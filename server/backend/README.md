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

SQLAlchemy and Alembic are wired up but **currently unused at
runtime** — no migrations are defined, and `app.py` still creates
the SQLite schema directly. This is intentional; the framework is
staged so follow-up work in the [PostgreSQL-backend epic][epic] can
land the baseline migration, the async `Store` protocol, and the
Postgres backend incrementally.

Database URL resolution (used by `alembic/env.py` and, in a later
child issue, the runtime store factory) lives in
`cq_server.db_url.resolve_database_url`. Precedence:

1. `CQ_DATABASE_URL` — used verbatim (e.g. `postgresql+psycopg://…`).
2. `CQ_DB_PATH` — wrapped as `sqlite:///<path>` (back-compat with
   the existing env var).
3. Default — `sqlite:////data/cq.db`.

To run Alembic commands against a local dev database (the path is
resolved relative to wherever `alembic` is invoked from — here,
`server/backend/`):

```
cd server/backend
CQ_DB_PATH=./dev.db uv run alembic current
```

Full environment-variable documentation will land alongside the
`CQ_DATABASE_URL` runtime wiring in a later phase-1 child issue.

[epic]: https://github.com/mozilla-ai/cq/issues/257
