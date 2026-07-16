"""Behavior tests for the repository layer running on PostgreSQL.

These exercise the queries that diverge between SQLite and PostgreSQL
(confidence distribution, daily counts) plus a basic round-trip smoke
test, all against the ``pg_repos`` fixture (function-scoped repositories
sharing the session-scoped ``pg_url`` PostgreSQL container). The whole
module skips when Docker is unavailable.
"""

import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from cq.models import Evidence, Insight, KnowledgeUnit, create_knowledge_unit
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from cq_server.migrations import _MIGRATION_LOCK_KEY, run_migrations
from cq_server.repositories._queries import SELECT_PROPOSED_DAILY

from .db_helpers import _RepoBundle


def _make_unit(*, confidence: float = 0.9, **overrides: Any) -> KnowledgeUnit:
    defaults: dict[str, Any] = {
        "domains": ["databases", "performance"],
        "insight": Insight(
            summary="Use connection pooling",
            detail="Database connections are expensive to create.",
            action="Configure a connection pool with a max size of 10.",
        ),
    }
    unit = create_knowledge_unit(**{**defaults, **overrides})
    unit.evidence = Evidence(confidence=confidence)
    return unit


async def _insert_and_review(repos: _RepoBundle, status: str, **kwargs: Any) -> KnowledgeUnit:
    unit = _make_unit(**kwargs)
    await repos.insert(unit)
    await repos.set_review_status(unit.id, status, "reviewer")
    return unit


async def _insert_and_approve(repos: _RepoBundle, **kwargs: Any) -> KnowledgeUnit:
    return await _insert_and_review(repos, "approved", **kwargs)


async def test_pg_insert_query_roundtrip(pg_repos: _RepoBundle) -> None:
    unit = await _insert_and_approve(pg_repos, domains=["databases"])
    results = await pg_repos.query(["databases"])
    assert [u.id for u in results] == [unit.id]


async def test_pg_knowledge_confidence_distribution(pg_repos: _RepoBundle) -> None:
    # Buckets: 0.0-0.3, 0.3-0.5, 0.5-0.7, 0.7-1.0
    await _insert_and_approve(pg_repos, confidence=0.2)
    await _insert_and_approve(pg_repos, confidence=0.6)
    await _insert_and_approve(pg_repos, confidence=0.95)
    dist = await pg_repos.knowledge.confidence_distribution()
    assert dist == {"0.0-0.3": 1, "0.3-0.5": 0, "0.5-0.7": 1, "0.7-1.0": 1}


async def test_pg_reviews_confidence_distribution(pg_repos: _RepoBundle) -> None:
    # Review dashboard buckets: 0.0-0.3, 0.3-0.6, 0.6-0.8, 0.8-1.0
    await _insert_and_approve(pg_repos, confidence=0.2)
    await _insert_and_approve(pg_repos, confidence=0.5)
    await _insert_and_approve(pg_repos, confidence=0.9)
    dist = await pg_repos.reviews.confidence_distribution()
    assert dist == {"0.0-0.3": 1, "0.3-0.6": 1, "0.6-0.8": 0, "0.8-1.0": 1}


async def test_pg_daily_counts(pg_repos: _RepoBundle) -> None:
    # One proposed+approved unit today; daily_counts must aggregate it by day
    # via ``to_char(created_at::timestamptz, 'YYYY-MM-DD')``, not ``date(text)``.
    await _insert_and_approve(pg_repos)
    rows = await pg_repos.daily_counts(days=7)
    today = datetime.now(UTC).date().isoformat()
    by_date = {r["date"]: r for r in rows}
    assert by_date[today]["proposed"] == 1
    assert by_date[today]["approved"] == 1


async def test_pg_daily_counts_rejected(pg_repos: _RepoBundle) -> None:
    # The rejected-daily query is a distinct dialect-keyed branch; cover it too.
    await _insert_and_review(pg_repos, "rejected")
    rows = await pg_repos.daily_counts(days=7)
    today = datetime.now(UTC).date().isoformat()
    by_date = {r["date"]: r for r in rows}
    assert by_date[today]["rejected"] == 1
    assert by_date[today]["approved"] == 0


async def test_pg_daily_counts_excludes_before_cutoff(pg_repos: _RepoBundle) -> None:
    # The ``>= :cutoff`` filter is copy-pasted into the PG variant of ``_daily``
    # (not shared with the SQLite one), and on PG it is a lexicographic text
    # comparison against the TEXT column. Pin it directly: a row dated exactly
    # on the cutoff is included, one a second before is excluded.
    now = datetime.now(UTC)
    cutoff_date = (now - timedelta(days=30)).date()
    midnight = datetime(cutoff_date.year, cutoff_date.month, cutoff_date.day, tzinfo=UTC)
    on_cutoff = await _insert_and_approve(pg_repos, domains=["on"])
    before_cutoff = await _insert_and_approve(pg_repos, domains=["before"])
    with pg_repos._engine.begin() as conn:
        update = text("UPDATE knowledge_units SET created_at = :when WHERE id = :id")
        conn.execute(update, {"when": midnight.isoformat(), "id": on_cutoff.id})
        conn.execute(update, {"when": (midnight - timedelta(seconds=1)).isoformat(), "id": before_cutoff.id})
    with pg_repos._engine.connect() as conn:
        rows = conn.execute(SELECT_PROPOSED_DAILY["postgresql"], {"cutoff": cutoff_date.isoformat()}).fetchall()
    counts = {row[0]: row[1] for row in rows}
    assert counts.get(cutoff_date.isoformat()) == 1  # on-cutoff row included
    assert sum(counts.values()) == 1  # before-cutoff row excluded entirely


async def test_pg_daily_counts_datestyle_independent(pg_repos: _RepoBundle) -> None:
    # The day key uses ``to_char(..., 'YYYY-MM-DD')`` rather than ``::date::text``
    # precisely so it stays ISO regardless of the session ``DateStyle``. Run the
    # proposed-daily query on a connection pinned to a non-ISO DateStyle and
    # assert the key is still ISO — a regression to ``::date::text`` would yield
    # e.g. '16.07.2026' and fail here.
    await _insert_and_approve(pg_repos)
    cutoff = (datetime.now(UTC) - timedelta(days=7)).date().isoformat()
    with pg_repos._engine.connect() as conn:
        conn.execute(text("SET DateStyle = 'German, DMY'"))
        rows = conn.execute(SELECT_PROPOSED_DAILY["postgresql"], {"cutoff": cutoff}).fetchall()
    today = datetime.now(UTC).date().isoformat()
    assert today in {row[0] for row in rows}


def test_pg_migration_serializes_on_advisory_lock(pg_url: str) -> None:
    """Concurrent pod startups must take turns on the migration.

    Simulate "pod A is mid-migration" by holding the migration advisory
    lock on an external session, then assert a second ``run_migrations``
    blocks on that same lock instead of racing into the schema work, and
    completes cleanly once we release it. Without the lock the migration
    would run DDL immediately even while another pod holds the lock.
    """
    holder = create_engine(pg_url)
    # AUTOCOMMIT so the session-level lock is held by the connection alone,
    # with no lingering transaction — session advisory locks outlive the
    # statement and are released only by unlock (or the session closing).
    conn = holder.connect().execution_options(isolation_level="AUTOCOMMIT")
    try:
        conn.execute(text("SELECT pg_advisory_lock(:k)"), {"k": _MIGRATION_LOCK_KEY})

        done = threading.Event()
        error: list[BaseException] = []

        def _run() -> None:
            try:
                run_migrations(pg_url)
            except BaseException as exc:  # noqa: BLE001 — surfaced to the test below
                error.append(exc)
            finally:
                done.set()

        threading.Thread(target=_run, daemon=True).start()

        # While we hold the lock, run_migrations must be stuck acquiring it.
        assert not done.wait(timeout=2.0), "run_migrations did not block on the advisory lock"

        # Release; the migration should now proceed and finish cleanly.
        conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": _MIGRATION_LOCK_KEY})

        assert done.wait(timeout=10.0), "run_migrations did not finish after lock release"
        assert not error, f"run_migrations raised after acquiring the lock: {error[0]!r}"
    finally:
        conn.close()
        holder.dispose()


def test_pg_migration_runs_ddl_once_under_concurrency(pg_url: str) -> None:
    """Two concurrent fresh-database startups serialize instead of racing on DDL.

    The blocking test above proves the lock is *taken*; this proves what
    the lock is *for*: on a brand-new database, two pods starting together
    must not both run the baseline ``CREATE TABLE`` / ``stamp``. Those
    DDL-emitting branches never fire against the session ``pg_url`` (it is
    already migrated), so they only get coverage here.

    Deterministic, no correctness-gating sleeps: an external session holds
    the migration lock, we start two ``run_migrations`` threads, then poll
    ``pg_locks`` until *both* are provably queued on that exact advisory
    lock (the wall clock is only a failure deadline). Only then do we
    release, so both are past ``create_engine`` and genuinely blocked on
    the lock — not merely slow to start — and the migration serializes
    them. Without the lock the threads never queue (poll never sees two
    waiters) and race into duplicate DDL, so this fails in both directions.

    Runs against a throwaway database on the same server; skips cleanly if
    the test role can't ``CREATE DATABASE``.
    """
    admin_url = make_url(pg_url).set(database="postgres")
    dbname = f"cq_mig_race_{uuid.uuid4().hex}"
    admin = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    created = False
    try:
        try:
            with admin.connect() as c:
                c.execute(text(f'CREATE DATABASE "{dbname}"'))
            created = True
        except Exception as exc:  # noqa: BLE001 — provisioning, not the thing under test
            pytest.skip(f"cannot CREATE DATABASE for concurrency test: {exc}")

        # render_as_string(hide_password=False): plain str() masks the
        # password as "***", which would then fail authentication.
        fresh_url = make_url(pg_url).set(database=dbname).render_as_string(hide_password=False)

        # Hold the migration lock so both threads must queue behind it.
        holder_engine = create_engine(fresh_url)
        holder = holder_engine.connect().execution_options(isolation_level="AUTOCOMMIT")
        errors: list[BaseException] = []

        def _run() -> None:
            try:
                run_migrations(fresh_url)
            except BaseException as exc:  # noqa: BLE001 — surfaced to the test below
                errors.append(exc)

        threads = [threading.Thread(target=_run, daemon=True) for _ in range(2)]
        try:
            holder.execute(text("SELECT pg_advisory_lock(:k)"), {"k": _MIGRATION_LOCK_KEY})
            for t in threads:
                t.start()

            def _waiters() -> int:
                # Sessions blocked acquiring an advisory lock on our fresh DB
                # show up as ungranted 'advisory' rows in pg_locks.
                with admin.connect() as c:
                    return c.execute(
                        text(
                            "SELECT count(*) FROM pg_locks l "
                            "JOIN pg_database d ON d.oid = l.database "
                            "WHERE d.datname = :db AND l.locktype = 'advisory' AND NOT l.granted"
                        ),
                        {"db": dbname},
                    ).scalar_one()

            deadline = time.monotonic() + 5.0
            while _waiters() < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            assert _waiters() >= 2, "both run_migrations did not queue on the advisory lock"

            # Release; the two runs now proceed one at a time.
            holder.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": _MIGRATION_LOCK_KEY})

            for t in threads:
                t.join(timeout=15.0)
            assert not any(t.is_alive() for t in threads), "run_migrations did not finish"
            assert not errors, f"concurrent migrations raced into duplicate DDL: {errors!r}"

            # Exactly one built the schema; the other found it already done.
            check = create_engine(fresh_url)
            try:
                with check.connect() as c:
                    got = c.execute(text("SELECT to_regclass('public.knowledge_units')")).scalar()
                assert got is not None, "schema was not created"
            finally:
                check.dispose()
        finally:
            holder.close()
            holder_engine.dispose()
    finally:
        if created:
            with admin.connect() as c:
                # Drop needs no other sessions on the DB; migration engines are
                # disposed by now, but terminate any stragglers to be safe.
                c.execute(
                    text(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE datname = :d AND pid <> pg_backend_pid()"
                    ),
                    {"d": dbname},
                )
                c.execute(text(f'DROP DATABASE IF EXISTS "{dbname}"'))
        admin.dispose()
