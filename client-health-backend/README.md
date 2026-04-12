# Client Health Backend

This backend powers the Client Health Dashboard frontend. It authenticates to Coursedog, fetches integration-health data, stores normalized snapshots in SQLite, and exposes read APIs plus background sync/backfill jobs for the UI.

## Responsibilities

- Authenticate against the Coursedog API
- Fetch school lists, merge history, merge errors, and user activity
- Persist school snapshots to SQLite
- Track sync and backfill job history
- Serve cached dashboard data to the frontend

## Stack

- FastAPI
- SQLAlchemy
- SQLite
- httpx
- Poetry

## Layout

- `app/main.py`: FastAPI app and lifespan hooks
- `app/routes.py`: API routes, sync orchestration, parsing helpers
- `app/models.py`: SQLAlchemy models
- `app/db.py`: SQLite setup and Coursedog client initialization
- `tests/test_app.py`: backend unit tests
- `client_health.db`: local snapshot and sync-run database

## Environment

Copy `.env.example` to `.env` before running the backend. The local `.env` file is gitignored and should stay out of version control.

Supported variables:

- `INTERNAL_API_KEY`
- `ALLOWED_ORIGINS`
- `CORS_ALLOW_CREDENTIALS`
- `COURSEDOG_BASE_URL`
- `COURSEDOG_EMAIL`
- `COURSEDOG_PASSWORD`
- `VITE_COURSEDOG_PRD_URL`
- `VITE_COURSEDOG_EMAIL`
- `VITE_COURSEDOG_PASSWORD`

The `COURSEDOG_*` names are preferred. The `VITE_*` names are still supported as fallbacks.
`INTERNAL_API_KEY` is required for all `/api/*` requests and should match the frontend `VITE_INTERNAL_API_KEY` value.

## Install

```bash
poetry install
```

## Run Locally

```bash
poetry run fastapi dev app/main.py
```

Default local URL:

- `http://localhost:8000`

Security defaults:

- `/api/*` routes require the `X-Internal-API-Key` header
- allowed CORS origins come from `ALLOWED_ORIGINS`
- credentials are disabled by default unless `CORS_ALLOW_CREDENTIALS=true`

## Test

Run tests from this directory:

```bash
python3 -m unittest tests.test_app -v
```

The test module imports the local `app` package directly, so running from the repo root may fail unless `PYTHONPATH` is adjusted.

## API Overview

Read endpoints:

- `GET /healthz`
- `GET /api/schools`
- `GET /api/client-health`
- `GET /api/client-health/history`
- `GET /api/client-health/active-users`
- `GET /api/client-health/sync-metadata`
- `GET /api/client-health/sync-runs`
- `GET /api/client-health/sync/{job_id}`

Mutation endpoints:

- `POST /api/client-health/sync`
- `POST /api/client-health/history/backfill`

## Data Model

`school_snapshots`

- one row per `school` and `snapshot_date`
- stores nightly, realtime, and manual merge aggregates
- includes granular issue/no-data fields
- includes merge-error counts and active users

`sync_runs`

- stores sync/backfill job metadata
- tracks job scope, status, timestamps, and error message

## Notes

- The backend creates tables automatically on startup.
- `ensure_schema_updates()` applies lightweight SQLite schema upgrades for older local DBs.
- Dashboard reads are intentionally served from SQLite rather than hitting Coursedog live on every page load.
- Demo/test tenants are filtered by name heuristics in `app/routes.py`.
- Sync/backfill triggers are throttled to reduce accidental repeated runs.
