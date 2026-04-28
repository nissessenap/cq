# cq-server backend

FastAPI service backing the cq remote store.

## Development

From the repository root:

```
make setup-server-backend   # uv sync
make dev-api                # run against a local SQLite DB
make test-server-backend    # pytest
make lint-server-backend    # pre-commit (ruff, ty, uv lock check)
```

## Database migrations (Alembic)

The server runs Alembic migrations on every start, before opening the
store. The runner (`cq_server.migrations.run_migrations`) handles
three cases:

1. **New database** (no tables) — applies the baseline migration,
   creating every table from scratch. The `alembic_version` row is
   written as part of that.
2. **Existing pre-Alembic database** (data tables present, no
   `alembic_version`) — *stamps* the baseline revision without
   re-running its DDL, then runs any later migrations. This is the
   first-restart-after-upgrade case for a server that's been running
   on the legacy `_ensure_schema()` path. **No DDL re-runs, no data
   touched, no downtime** — the change is a single insert into a new
   table.
3. **Already-managed database** — `upgrade head` is a no-op when
   there are no pending revisions, so restart is idempotent.

Database URL resolution (used by `alembic/env.py`, the migration
runner, and — in a later child issue — the runtime store factory)
lives in `cq_server.db_url.resolve_database_url`. Precedence:

1. `CQ_DATABASE_URL` — used verbatim. **Today this must be a SQLite
   URL** (e.g. `sqlite:////data/cq.db`); the runtime store is still
   SQLite-only and the server rejects non-SQLite URLs at startup.
   Postgres support lands with #309/#311.
2. `CQ_DB_PATH` — wrapped as `sqlite:///<path>` (back-compat with
   the existing env var).
3. Default — `sqlite:////data/cq.db`.

Schema is owned exclusively by Alembic. `SqliteStore` no longer creates
or alters tables — any schema change must land as a new Alembic
migration in `alembic/versions/`. There is no parallel `tables.py` /
`SqliteStore` DDL to keep in sync.

**Rollback.** If a new migration causes a bad deploy, redeploy the
previous server image. The previous version sees an `alembic_version`
ahead of its head and refuses to start (Alembic's normal behaviour),
which is the desired safeguard against silently downgrading data. To
recover, either re-deploy the version that wrote the newer
`alembic_version`, or hand-write a downgrade migration before
redeploying the older image.

To run Alembic commands against a local dev database (the path is
resolved relative to wherever `alembic` is invoked from — here,
`server/backend/`):

```
cd server/backend
CQ_DB_PATH=./dev.db uv run alembic current   # show current revision
CQ_DB_PATH=./dev.db uv run alembic upgrade head
```

Full environment-variable documentation will land alongside the
`CQ_DATABASE_URL` runtime wiring in a later phase-1 child issue.
