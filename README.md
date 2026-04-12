# Client Health Dashboard

Client Health Dashboard is a full-stack internal dashboard for monitoring school integration health in Coursedog. It combines a Vue 3 frontend with a FastAPI backend and a local SQLite cache so the UI can show the latest fleet snapshot, historical trends, school-level drill-downs, and background sync activity.

## What It Does

- Shows the latest cached health snapshot for all schools
- Tracks nightly, realtime, and manual merge outcomes
- Distinguishes `finishedWithIssues` and `noData` from outright failures
- Displays open merge errors and 24-hour active-user counts
- Supports school detail drill-down with 30-day trend charts
- Runs on-demand bulk syncs and historical backfills
- Exposes recent sync/backfill job history in the UI

## Architecture

The app has two parts:

1. Frontend: Vue 3 + TypeScript + Vite
   - Lives in `src/`
   - Calls the backend through the Vite dev proxy at `/backend`
2. Backend: FastAPI + SQLAlchemy + SQLite
   - Lives in `client-health-backend/`
   - Authenticates to Coursedog, fetches API data, and persists snapshots to `client_health.db`

High-level flow:

1. The frontend requests cached snapshot and history data from the backend.
2. The backend reads from SQLite for normal dashboard loads.
3. When a user triggers sync or backfill, the backend fetches fresh data from Coursedog in the background and writes new snapshot rows plus sync-run records.
4. The frontend polls job status and refreshes its queries when work completes.

## Key Screens

- `/admin/client-health`: fleet overview, filters, trend charts, and the school table
- `/admin/client-health/:school`: school detail page with history and sync metadata
- `/admin/jobs`: recent sync/backfill job monitoring

## Tech Stack

Frontend:

- Vue 3
- TypeScript
- Vite
- Vue Router
- TanStack Vue Query
- ApexCharts
- Tailwind CSS
- Vitest

Backend:

- FastAPI
- SQLAlchemy
- SQLite
- httpx
- Python `unittest`

## Prerequisites

- Node.js 18+
- npm
- Python 3.9+
- Poetry for backend dependency management
- Valid Coursedog credentials for live syncs/backfills

## Environment Variables

Copy `.env.example` to `.env` in the repo root and `client-health-backend/.env.example` to `client-health-backend/.env`. These runtime files are intentionally gitignored and should never be committed.

Commonly used values:

- `BACKEND_URL`: frontend proxy target in development. Defaults to `http://localhost:8000`.
- `VITE_INTERNAL_API_KEY` and `INTERNAL_API_KEY`: shared secret used to authenticate frontend-to-backend requests. Use the same random value in both files.
- `VITE_ALLOWED_ORIGINS` and `ALLOWED_ORIGINS`: comma-separated browser origins allowed to access the backend if it is reached directly.
- `COURSEDOG_BASE_URL` or `VITE_COURSEDOG_PRD_URL`: Coursedog base URL. Defaults to `https://app.coursedog.com`.
- `COURSEDOG_EMAIL` or `VITE_COURSEDOG_EMAIL`: login email for Coursedog API access.
- `COURSEDOG_PASSWORD` or `VITE_COURSEDOG_PASSWORD`: login password for Coursedog API access.

If credentials are missing, the backend will still start, but live Coursedog fetches may fail or return limited data.
If `INTERNAL_API_KEY` is missing, the backend will reject `/api/*` requests until it is configured.

## Local Development

### 1. Install frontend dependencies

```bash
npm install
```

### 2. Install backend dependencies

```bash
cd client-health-backend
poetry install
cd ..
```

### 3. Start the backend

From `client-health-backend/`:

```bash
poetry run fastapi dev app/main.py
```

The backend runs on `http://localhost:8000` by default.

### 4. Start the frontend

From the repo root:

```bash
npm run dev
```

The frontend runs on `http://localhost:5173` and proxies `/backend/*` requests to the backend.

## Security Notes

- Treat the backend as an internal privileged service even on a private network.
- Keep the backend behind trusted network controls or a reverse proxy.
- Rotate any credentials that were previously committed in `.env` files or scratch scripts.
- Do not commit SQLite databases or `.env` files; use the example files for onboarding instead.

## Testing

Frontend:

```bash
npm test
```

Backend:

```bash
cd client-health-backend
python3 -m unittest tests.test_app -v
```

Note: backend tests should be run from `client-health-backend/`. Running `unittest discover` from the repository root will not resolve the backend `app` package correctly.

## Production Build

Build the frontend:

```bash
npm run build
```

Preview the production bundle:

```bash
npm run preview
```

Current note: the chart bundle is relatively large because ApexCharts is loaded into the main build. Vite may warn about chunk size during production builds.

## Database and Sync Model

The backend persists data in `client-health-backend/client_health.db`.

Important tables:

- `school_snapshots`: one row per school per snapshot date
- `sync_runs`: one row per sync or backfill attempt

Key behaviors:

- Normal dashboard reads come from SQLite, not directly from Coursedog
- `/api/client-health/sync` starts a fresh sync for all schools or a single school
- `/api/client-health/history/backfill` backfills snapshot dates across a date range
- The dashboard polls job status until completion

Because the SQLite DB is an active working artifact, snapshot counts may be uneven while a backfill is still running.

## Repository Layout

- `src/`: Vue frontend
- `client-health-backend/`: FastAPI backend, DB models, tests, and local SQLite DB
- `public/`: static frontend assets
- `analysis_results.md`: earlier project assessment; useful historical context, but parts are now outdated
- `coursedog-api-endpoints.md`: notes on upstream Coursedog endpoints and possible future enhancements

## Current State of the Project

The project is beyond prototype stage:

- The sidebar layout and multi-route app shell are implemented
- Background jobs and sync monitoring are implemented
- Granular merge status handling is implemented in both backend and frontend
- Frontend and backend tests exist and are passing with the correct commands

The biggest remaining work is operational rather than structural:

- keeping snapshot data complete and current
- clarifying database ownership/commit policy
- improving docs and developer onboarding
- reducing frontend bundle size if needed

## Contributing

1. Create a branch from `main`
2. Make your changes
3. Run frontend tests, backend tests, and a production build
4. Open a pull request with any setup or data assumptions called out clearly
