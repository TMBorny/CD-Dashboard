# Project Assessment: CD Dashboard test

This assessment identifies the most critical problems currently hindering the performance, reliability, and security of the Client Health Dashboard.

*Note: Many foundational scale and architecture issues (Data Dissonance, Monolithic Syncs, Authentication Fragmentation, Security Proxies) were successfully addressed in recent refactoring phases by introducing a robust FastAPI/SQLite persistance layer.*

## 1. Frontend-Backend Disconnect on Granular Merge Statuses
* **Problem**: The backend database schema (`models.py`) and sync logic (`routes.py`) were recently updated to track granular metrics for merge reports—specifically differentiating `finishedWithIssues` and `noData` from standard successes or failures. However, the Vue frontend (`ClientHealthTable.vue` and `ClientHealthDetail.vue`) was never updated to visualize these new stats. It still ignores these variables and calculates a simplistic success rate (`succeeded / total`).
* **Impact**: Users still cannot distinguish between a catastrophic total failure vs a merge that finished but lacked data. The "Health Score" algorithm is functioning on outdated assumptions.
* **Recommendation**:
  - Update `ClientHealthTable.vue` to display columns or badges for "Issues" and "No Data".
  - Adjust the frontend `getHealthScoreValue` algorithm to appropriately weight these granular statuses.
  - Add a stacked bar chart to `ClientHealthDetail.vue` to show a historical visual breakdown of detailed merge outcomes.

## 2. Lack of Centralized Background Job Monitoring (Observability)
* **Problem**: The dashboard executes asynchronous batches to sync schools and backfill data. Although backend models track a `sync_runs` table for all attempts, there is no UI in the frontend to view this data.
* **Impact**: If background scheduled syncs fail, or if a bulk backfill operation silently errors out on specific schools, administrators have absolutely no visibility into *which* schools failed or *why* (e.g., API rate limiting vs authentication errors). Debugging is nearly impossible without SQL terminal access.
* **Recommendation**: Create a "System Health" or "Jobs Activity" UI route that fetches all records from the `SyncRun` table to display a live feed of background operations, their timings, and associated error stacks.

## 3. Thin Testing Coverage on Complex Data Extraction
* **Problem**: The application heavily relies on parsing highly varied, untyped JSON structures from upstream APIs to derive critical metrics (e.g., extracting merge error counts, bucketing history entries, parsing schedules).
* **Impact**: Small changes in the upstream Coursedog API structure can easily cause silent metric aggregation failures. The current test suite (`test_app.py`) is minimal and lacks coverage on the complex parser functions.
* **Recommendation**: Introduce a suite of robust `pytest` cases specifically targeting data parsing features within `routes.py`, especially `extract_merge_error_count` and `build_snapshot_from_merge_history`.

## 4. Unused / Fragmented Layout Architecture
* **Problem**: The `Sidebar.vue` component exists but the dashboard largely operates as a single-page list view without proper sub-navigation structure.
* **Impact**: As new features are added (such as Jobs Monitoring or Auth Settings), the UI will lack a cohesive layout to mount them, resulting in a cluttered top-bar or hidden routes.
* **Recommendation**: Refactor `App.vue` and Vue Router settings to utilize a standard layout wrapper with a functional sidebar, routing between "Overview", "Job History", and "Settings".

## Summary Action Plan

- [ ] Update frontend tables and charts to properly visualize `finishedWithIssues` and `noData`.
- [ ] Build a **Jobs Monitoring** page to expose the `sync_runs` SQL table to the browser.
- [ ] Write unit tests for background sync parsing and metric extraction functions.
- [ ] Solidify the application layout utilizing `Sidebar.vue` to frame navigation cleanly.
