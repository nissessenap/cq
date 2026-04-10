# PostgreSQL Backend Support for cq Server

**Date:** 2026-04-10
**Status:** Draft
**Author:** Edvin Norling
**Issue:** [#257](https://github.com/mozilla-ai/cq/issues/257)

---

## Goal

Add PostgreSQL support to the cq remote server while keeping SQLite as the
default. SQLite remains the zero-config option for local and single-instance
use. PostgreSQL is opt-in for production deployments that need multi-process
access (e.g., Kubernetes).

**Scope:** Server only. The Go SDK, Python SDK `LocalStore`, and CLI local
store are explicitly out of scope — their SQLite usage is intentional (local
cache, not a shared database).

---

## Current State

The server stores all data in SQLite via a single class (`RemoteStore` in
`server/backend/src/cq_server/store.py`). There is no ORM, no query builder,
and no abstraction layer. The schema is three tables:

```sql
-- knowledge_units (core table)
id          TEXT PRIMARY KEY
data        TEXT NOT NULL          -- JSON-serialized KnowledgeUnit
status      TEXT NOT NULL DEFAULT 'pending'
reviewed_by TEXT
reviewed_at TEXT
created_at  TEXT
tier        TEXT NOT NULL DEFAULT 'private'

-- knowledge_unit_domains (junction table)
unit_id     TEXT NOT NULL  -> FK knowledge_units(id) ON DELETE CASCADE
domain      TEXT NOT NULL
PRIMARY KEY (unit_id, domain)
INDEX idx_domains_domain ON knowledge_unit_domains(domain)

-- users
id            INTEGER PRIMARY KEY AUTOINCREMENT
username      TEXT NOT NULL UNIQUE
password_hash TEXT NOT NULL
created_at    TEXT NOT NULL
```

Schema is applied at startup via `CREATE TABLE IF NOT EXISTS` and ad-hoc
`ALTER TABLE ADD COLUMN` with `PRAGMA table_info()` introspection. There is
no migration framework.

---

## Configuration

A new environment variable selects the backend:

```python
# In app.py lifespan:
database_url = os.environ.get(
    "CQ_DATABASE_URL",
    f"sqlite:///{os.environ.get('CQ_DB_PATH', '/data/cq.db')}"
)
```

- `CQ_DATABASE_URL` — standard connection string
  (`sqlite:///path` or `postgresql://user:pass@host/db`).
- Backward compatible — falls back to `CQ_DB_PATH` (existing env var).
- Default: SQLite at `/data/cq.db` (unchanged).

---

## Store Implementation: Two Options

Out of ~15 SQL queries in `RemoteStore`, almost all are standard SQL that works
identically on both SQLite and PostgreSQL. The only truly dialect-specific query
is `daily_counts()`, which uses SQLite's `date('now', '-30 days')` syntax.

The remaining differences are mechanical: parameter placeholders (`?` vs `%s`),
PRAGMAs (SQLite only), and connection model (single connection vs pool).

### Option A: SQLAlchemy Core — one shared implementation (recommended)

SQLAlchemy Core (not the ORM) is a query builder and connection manager. You
write queries using raw SQL via `text()`, and it handles dialect differences.
One store class works for both backends:

```text
cq_server/
  store.py               # Single Store class, takes a database URL
```

```python
from sqlalchemy import create_engine, text

class Store:
    def __init__(self, database_url: str):
        self._engine = create_engine(database_url)
        # SQLite PRAGMAs via connect event
        if self._engine.dialect.name == "sqlite":
            from sqlalchemy import event
            @event.listens_for(self._engine, "connect")
            def set_sqlite_pragmas(dbapi_conn, _):
                dbapi_conn.execute("PRAGMA foreign_keys = ON")
                dbapi_conn.execute("PRAGMA journal_mode = WAL")
                dbapi_conn.execute("PRAGMA synchronous = NORMAL")
                dbapi_conn.execute("PRAGMA busy_timeout = 5000")

    def get(self, unit_id: str) -> KnowledgeUnit | None:
        with self._engine.connect() as conn:
            row = conn.execute(
                text("SELECT data FROM knowledge_units "
                     "WHERE id = :id AND status = 'approved'"),
                {"id": unit_id},
            ).fetchone()
        if row is None:
            return None
        return KnowledgeUnit.model_validate_json(row[0])

    def close(self) -> None:
        self._engine.dispose()
```

The `:id` placeholder is converted to `?` for SQLite or `%s` for PostgreSQL
automatically. Same Python code, same SQL string, both backends.

**What you get:**

- One store class — change a query once, it applies to both backends.
- Connection pooling built-in (`QueuePool` for PostgreSQL, `StaticPool` for
  SQLite). Replaces the manual `threading.Lock` + single connection.
- Transaction management via `engine.begin()` / `conn.commit()`.
- Alembic pulls in SQLAlchemy as a dependency anyway, so using Core for
  queries adds zero extra dependency weight.

**What it costs:**

- Contributors need to know SQLAlchemy's `text()` and `create_engine()` API.
- The current raw sqlite3 code needs rewriting to use SQLAlchemy connections.
- One more abstraction layer between the code and the database.

### Option B: Raw SQL — two separate implementations

Keep using native drivers directly: sqlite3 for SQLite, psycopg for PostgreSQL.
Each backend is its own class with its own SQL strings, sharing an abstract base
class.

```text
cq_server/
  store/
    __init__.py          # re-export create_store()
    _base.py             # StoreBase ABC
    _sqlite.py           # SqliteStore(StoreBase) — current code
    _postgresql.py       # PostgresqlStore(StoreBase) — new
```

The ABC gives runtime enforcement: if an implementation forgets a method,
Python raises `TypeError` at instantiation. No type checker needed (the project
doesn't run one).

```python
class SqliteStore(StoreBase):
    """Basically the current RemoteStore code, renamed."""

    def get(self, unit_id: str) -> KnowledgeUnit | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT data FROM knowledge_units "
                "WHERE id = ? AND status = 'approved'",
                (unit_id,),
            ).fetchone()
        # ...

class PostgresqlStore(StoreBase):
    """Same SQL, different placeholder and connection model."""

    def get(self, unit_id: str) -> KnowledgeUnit | None:
        with self._pool.connection() as conn:
            row = conn.execute(
                "SELECT data FROM knowledge_units "
                "WHERE id = %s AND status = 'approved'",
                (unit_id,),
            ).fetchone()
        # ...
```

**What you get:**

- `SqliteStore` is the current code with minimal changes (rename + inherit ABC).
- Each implementation is self-contained and easy to read.
- No SQLAlchemy dependency for the store layer (only Alembic uses it).

**What it costs:**

- ~90% duplicated SQL across the two classes. Change a WHERE clause? Update
  it in two places.
- Connection pooling for PostgreSQL must be wired up manually via
  `psycopg_pool.ConnectionPool`.

### Recommendation

Option A is the stronger choice because Alembic already brings SQLAlchemy as a
dependency, one store class means one place to change queries, and connection
pooling is a solved problem. Option B is viable if minimal abstraction is
preferred.

---

## Migration Strategy: Alembic

### Why Alembic?

The current server has no migration framework. Schema is applied via
`CREATE TABLE IF NOT EXISTS` and ad-hoc `ALTER TABLE ADD COLUMN` with
`PRAGMA table_info()` to check if columns exist. This works for a single
backend but doesn't scale to two.

**Without Alembic**, migrations would need dialect-specific introspection.
The `ALTER TABLE ADD COLUMN` itself is portable, but checking whether a column
already exists is not:

```python
# SQLite — PRAGMA (SQLite-only syntax)
cursor = conn.execute("PRAGMA table_info(knowledge_units)")
existing = {row[1] for row in cursor.fetchall()}

# PostgreSQL — information_schema (SQL standard)
cursor = conn.execute(
    "SELECT column_name FROM information_schema.columns "
    "WHERE table_name = 'knowledge_units'"
)
existing = {row[0] for row in cursor.fetchall()}
```

`CREATE TABLE IF NOT EXISTS` works on both, so basic schema creation is fine.
The pain is in the "check before alter" migration logic — you'd maintain two
migration code paths, one per backend.

**Alembic** solves this. It tracks which schema changes have been applied and
runs new ones in order — like git for your database schema. One migration file,
both backends.

### How it works

1. Alembic creates an `alembic_version` table that stores the ID of the last
   migration applied.

2. Migrations live as Python files in `alembic/versions/`. Each has an
   `upgrade()` function (apply the change) and a `downgrade()` function
   (undo it).

3. At startup, the server calls `alembic.command.upgrade(cfg, "head")`
   programmatically — same automatic, zero-operator-intervention model as
   today. Production deployments never run the `alembic` CLI manually.

### What does a migration file look like?

Migrations use Alembic's operations API — not raw SQL, not ORM:

```python
"""Add tier column to knowledge_units."""
from alembic import op
import sqlalchemy as sa

revision = "a1b2c3d4e5f6"
down_revision = "000000000000"

def upgrade():
    with op.batch_alter_table("knowledge_units") as batch_op:
        batch_op.add_column(
            sa.Column("tier", sa.Text(), nullable=False, server_default="private")
        )

def downgrade():
    with op.batch_alter_table("knowledge_units") as batch_op:
        batch_op.drop_column("tier")
```

You can also drop to raw SQL: `op.execute(text("UPDATE ..."))`.

### Same migration for both backends?

Yes. A single migration file runs on both SQLite and PostgreSQL. The key is
`render_as_batch=True` in `env.py`:

- **PostgreSQL**: Generates standard `ALTER TABLE` statements. Batch mode has
  no effect.
- **SQLite**: Can't do most ALTER TABLE operations natively. Batch mode works
  around this by creating a temp table with the new schema, copying data,
  dropping the old table, and renaming. This happens automatically — the
  migration code looks the same.

For the rare case where SQL must differ between dialects:

```python
from alembic import op, context

def upgrade():
    if context.get_impl().dialect.name == "sqlite":
        ...
    else:
        ...
```

### Handling existing deployed SQLite databases

Current cq servers have databases with tables already created but no
`alembic_version` table. We need to bring them under Alembic's management
without re-running DDL.

```python
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect

def run_migrations(engine):
    alembic_cfg = Config("alembic.ini")
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    has_data_tables = "knowledge_units" in tables
    has_alembic = "alembic_version" in tables

    if has_data_tables and not has_alembic:
        # Existing database — stamp at baseline without running DDL
        command.stamp(alembic_cfg, "baseline_rev_id")

    # Run any pending migrations
    command.upgrade(alembic_cfg, "head")
```

This gives three cases:

- **New database**: Alembic runs the baseline migration to create everything.
- **Existing database** (tables exist, no `alembic_version`): Stamp at baseline,
  then run any subsequent migrations.
- **Already-migrated database**: Just run new migrations.

### File structure

```text
server/backend/
  alembic.ini
  alembic/
    env.py                       # render_as_batch=True, reads CQ_DATABASE_URL
    versions/
      001_baseline.py            # Full current schema
      002_next_change.py         # Future migrations
```

---

## SQL Dialect Differences

Most of the SQL is portable. Here are the differences and how they're resolved:

| SQLite | PostgreSQL | Resolution |
|--------|------------|------------|
| `?` placeholders | `%s` or `$1` | SQLAlchemy `text()` with `:name` (Option A) or separate SQL per class (Option B) |
| `date('now', '-30 days')` | `CURRENT_DATE - INTERVAL '30 days'` | Compute cutoff in Python (see below) |
| `DATE(column)` | `DATE(column)` | Portable |
| `COALESCE(a, b)` | `COALESCE(a, b)` | Portable |
| `PRAGMA foreign_keys = ON` | Default ON | Engine connect event (Option A) or `__init__` (Option B) |
| `PRAGMA journal_mode = WAL` | N/A | Same |
| `AUTOINCREMENT` | `SERIAL` / `IDENTITY` | SQLAlchemy handles in Alembic migration |

### daily_counts() — the one non-portable query

This method runs three queries that filter by a date cutoff. The fix is to
compute the cutoff in Python instead of in SQL:

```python
# Before (SQLite-specific):
cutoff = f"-{days} days"
"WHERE created_at >= date('now', ?)", (cutoff,)

# After (portable):
cutoff = (datetime.now(UTC) - timedelta(days=days)).date().isoformat()
"WHERE created_at >= :cutoff", {"cutoff": cutoff}
```

This does not move work from the database to Python. Python only computes a
single date string (e.g., `"2026-03-11"`). The filtering, grouping, and
counting still happen in the database — rows that don't match the cutoff never
leave the database. The current `date('now', ?)` does the exact same thing: it
computes a date string that the WHERE clause filters against. We're just
computing that string one step earlier.

### Column types — keep TEXT for parity

- **JSON data column**: Use `TEXT` on both backends. The application stores JSON
  via `model_dump_json()` (string) and reads via `model_validate_json()` (string).
  Neither backend does JSON operations in SQL. PostgreSQL `JSONB` is a future
  optimization if SQL-side JSON queries are ever needed.

- **Timestamp columns**: Use `TEXT` on both backends. The current code stores
  ISO strings and reads them back as strings. Using `TIMESTAMP` on PostgreSQL
  would cause psycopg to return `datetime` objects instead of strings, breaking
  parity. Native `TIMESTAMP` is a future optimization if date indexing is needed.

---

## Libraries

| Library | Version | Role |
|---------|---------|------|
| SQLAlchemy | 2.0.x | Core expression language + engine (Option A); Alembic dependency (both) |
| psycopg | 3.x | PostgreSQL driver |
| Alembic | 1.18.x | Schema migrations across both dialects |

**Why psycopg v3 (not psycopg2):** v3 is the recommended driver for new
projects — native async support, binary protocol, built-in connection pooling.
psycopg2 is maintenance-only. v3 integrates with SQLAlchemy via the
`postgresql+psycopg` URL prefix.

**Why NOT SQLAlchemy ORM:** Does anyone want to use an ORM?

**Why NOT async:** The server is fully synchronous. FastAPI runs sync handlers
in a threadpool — fine for moderate concurrency. `aiosqlite` is not truly async
(background thread wrapper). Going async would require changing every method
signature. Save for a follow-up if needed.

---

## Design Decisions

1. **Connection pooling**: Configurable via env vars, with sensible defaults.
   SQLAlchemy's `create_engine()` includes a pool by default.

2. **Transaction isolation**: Keep database defaults (SQLite WAL = snapshot
   isolation; PostgreSQL = read committed).

3. **Testing**: SQLite tests keep the current approach (tmp_path). PostgreSQL
   tests use **testcontainers-python** — starts a real PostgreSQL in Docker per
   test session. CI runs both backends.

4. **Backup scripts**: PostgreSQL backup tooling is out of scope. Operators
   bring their own strategy.

5. **AUTOINCREMENT vs SERIAL**: SQLAlchemy handles this automatically in
   Alembic migrations via `sa.Column("id", sa.Integer, primary_key=True,
   autoincrement=True)` — compiles to the right syntax per dialect.

---

## Implementation Plan

### Phase 1: Abstraction + Alembic (no new backend)

This phase is independently mergeable — it creates the extension point and
introduces proper migration tracking.

**If Option A:**

1. Rewrite `RemoteStore` to use SQLAlchemy `Engine` as `Store`
2. Update `app.py` lifespan to use `CQ_DATABASE_URL`
3. Add Alembic: `alembic.ini` + `env.py` with `render_as_batch=True`
4. Create baseline migration from current schema
5. Add startup logic to stamp existing databases + run migrations
6. Remove `_ensure_schema()` / `ensure_review_columns()` / `ensure_users_table()`
7. All existing tests pass unchanged

**If Option B:**

1. Extract `StoreBase` ABC to `store/_base.py`
2. Move current `RemoteStore` to `store/_sqlite.py` as `SqliteStore`
3. Add `create_store()` factory in `store/__init__.py`
4. Update `app.py` lifespan to use `CQ_DATABASE_URL` + factory
5. Same Alembic setup as Option A (steps 3-7)

### Phase 2: PostgreSQL backend

**If Option A:**

1. Add `psycopg` to dependencies
2. Handle dialect branch for `daily_counts()`
3. Add testcontainers-python fixtures
4. CI matrix: test both SQLite and PostgreSQL

**If Option B:**

1. Add `psycopg[pool]` to dependencies
2. Implement `PostgresqlStore` in `store/_postgresql.py`
3. Add testcontainers-python fixtures
4. CI matrix: test both SQLite and PostgreSQL

### Phase 3: Documentation

1. Update `docs/architecture.md` to reflect multi-backend support
2. Document `CQ_DATABASE_URL` in README / deployment docs
3. Update `docker-compose.yml` with optional PostgreSQL service

---

## Open Questions

1. **Seed scripts**: `server/scripts/seed-users.py` currently uses raw sqlite3.
   Should it use the store abstraction for both backends, or remain a
   SQLite-only development utility?
