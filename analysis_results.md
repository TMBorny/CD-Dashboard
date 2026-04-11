# Project Assessment: CD Dashboard test

This assessment identifies the most critical problems currently hindering the performance, reliability, and security of the Client Health Dashboard.

## 1. Data Dissonance & Redundant Aggregation
The project currently has a "split brain" problem regarding data processing:

*   **Problem**: `src/api/index.ts` (Frontend) and `app/routes.py` (Backend) both implement logic to fetch and aggregate health data. The frontend performs heavy computation (bucketing merge history, calculating success rates) on raw data fetched directly from Coursedog.
*   **Impact**: Substantial performance lag on the client side, redundant API calls, and inconsistent metrics if the logic diverges between the two layers.
*   **Recommendation**: Move all data aggregation (history, averages, status counts) to the FastAPI backend. The frontend should only consume "display-ready" JSON from the local SQLite store.

## 2. Unstable "Monolithic" Sync Process
The current synchronization mechanism is the project's biggest stability risk.

*   **Problem**: The `/client-health/sync` endpoint tries to process ALL schools in a single HTTP request. For each school, it makes 4 separate API calls (`health`, `merge-errors`, `mergeReports`, `userActivity`).
*   **Impact**:
    *   **Timeouts**: 50 schools = 200 API calls. Even with batching, this will likely exceed 30s timeouts.
    *   **Fragility**: A single failure in a middle step can crash the entire sync or lead to incomplete data.
*   **Recommendation**: Implement an asynchronous task queue (e.g., using a background task in FastAPI) or break the sync into per-school granular requests that can be handled incrementally.

## 3. Session & Authentication Fragmentation
Authentication is handled independently by both the browser and the backend server.

*   **Problem**: The frontend uses an Axios interceptor to login to `/api/v1/sessions`, while the backend uses its own `httpx` client to do the same.
*   **Impact**: This results in "session fighting" where one client might invalidate the token of another, and doubles the load on the Coursedog authentication service.
*   **Recommendation**: Centralize authentication on the backend. The frontend should only authenticate against the *local* dashboard server, which then manages the secure connection to Coursedog.

## 4. Security & Proxy Exposure
The current Vite configuration is overly permissive.

*   **Problem**: `vite.config.ts` proxies `/api` directly to `https://app.coursedog.com`.
*   **Impact**:
    *   **Credential Leakage**: Sensitive credentials (like `VITE_COURSEDOG_PASSWORD`) are exposed to the browser.
    *   **Attack Surface**: Any user can essentially use the proxy to hit any Coursedog API endpoint, bypassing the dashboard's intended restrictions.
*   **Recommendation**: Remove the direct `/api` proxy. All Coursedog API calls should be routed through the FastAPI backend, which acts as a controlled gatekeeper.

## 5. Weak Error Handling
The backend suppresses critical failure signals.

*   **Problem**: `fetch_school_health` is filled with `try/except: pass` blocks.
*   **Impact**: If a specific school fails to sync, the system reports "Success" but leaves the database with empty or stale fields. This makes debugging "Ghost Errors" nearly impossible.
*   **Recommendation**: Implement robust logging and partial-failure status reporting. The UI should show exactly which schools failed to sync and why.

## Summary Checklist
- [ ] Move history aggregation to Backend.
- [ ] Refactor Sync to use background tasks.
- [ ] Centralize Auth on Backend.
- [ ] Remove direct `/api` proxy from Vite.
- [ ] Enhance Backend error logging.
