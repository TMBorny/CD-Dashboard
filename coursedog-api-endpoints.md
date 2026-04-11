# Coursedog API Endpoints Useful for Client Health Dashboard

All endpoints require authentication (session cookie from `POST /api/v1/login`).

---

## Currently Used by the Dashboard

| # | Endpoint | Method | Description |
|---|----------|--------|-------------|
| 1 | `/api/v1/login` | POST | Authenticate with `{ email, password }`. Returns session cookie for subsequent requests. |
| 2 | `/api/v1/admin/schools/products` | GET | Lists all schools with display names and enabled products. Requires `coursedog` role. |
| 3 | `/api/v1/int/:school/integrations-hub/overview/health` | GET | Returns nightly merge stats (48h window): `allNightlyMergesCount`, `succeededNightlyMergesCount`, and `recentFailedMerges` array (merges that failed with no subsequent success in 24h). |
| 4 | `/api/v1/int/:school/integrations-hub/overview/merge-errors` | GET | Returns entities with sync errors. Params: `page`, `size`, optional `entityTypes[]`, `termCodes[]`, `allTerms`. Response includes paginated error list with `totalCount`. |
| 5 | `/api/v1/:school/mergeReports` | GET | Returns merge report history. Params: `size`. Each report has `scheduleType` (nightly/realtime/manual), `status` (success/error), `timestampEnd`, `type` (entity type). Used to compute realtime/manual merge stats. |
| 6 | `/api/v1/:school/userActivity` | GET | Returns user activity records. Params: `after` (timestamp). Used to count distinct active users by email. |

---

## Additional Endpoints Worth Adding

### Integration Health & Monitoring

| # | Endpoint | Method | Description |
|---|----------|--------|-------------|
| 7 | `/api/v1/:school/integration/status` | GET | Returns current integration job status (`active` or `noData`). Params: `scheduleType` (nightly/realtime/manual), `jobId` or `type`+`year`+`semester`+`termCode`. Useful for showing **live integration status** (is a merge currently running?). |
| 8 | `/api/v1/:school/integration/manual/status` | GET | Returns manual integration status specifically. Shows if a manual merge is in progress. |
| 9 | `/api/v1/:school/integration/in-progress-status` | GET | Returns which entity types currently have an integration in progress. Useful for a **"currently running"** indicator per school. |
| 10 | `/api/v1/:school/integration/configuration` | GET | Returns the school's integration configuration/settings. Shows whether integrations are enabled and how they're configured. Useful for identifying **schools with integrations enabled vs disabled**. |

### Merge History & Analytics (Integrations Hub)

| # | Endpoint | Method | Description |
|---|----------|--------|-------------|
| 11 | `/api/v1/int/:school/integrations-hub/merge-history` | GET | Paginated, filterable merge history. Params: `page`, `size`, `dateFrom`, `dateTo`, `entityType`, `scheduleType`, `status`, `termCode`. More powerful than `/mergeReports` — supports date range filtering and status filtering. |
| 12 | `/api/v1/int/:school/integrations-hub/merge-history/:mergeReportId/coursedog-updates` | GET | Returns how many Coursedog records were updated by a specific merge. Useful for measuring **merge impact**. |
| 13 | `/api/v1/int/:school/integrations-hub/merge-history/:mergeReportId/entiy-merge-errors` | GET | Returns entity-level errors for a specific merge report. Params: `page`, `size`. Useful for **drill-down into why a merge failed**. |
| 14 | `/api/v1/int/:school/integrations-hub/merge-history/:mergeReportId/post-step-updates` | GET | Returns post-step updates for a merge report. Shows what happened after the merge completed. |
| 15 | `/api/v1/int/:school/integrations-hub/merge-report-analytics/:mergeReportId` | GET | Returns detailed analytics for a merge report. Useful for **deep-dive into merge performance**. |
| 16 | `/api/v1/int/:school/integrations-hub/merge-report-analytics/:mergeReportId/status` | GET | Returns the analytics processing status for a merge report. |

### Merge Error Details

| # | Endpoint | Method | Description |
|---|----------|--------|-------------|
| 17 | `/api/v1/int/:school/integrations-hub/overview/merge-errors/supported-entity-types` | GET | Returns which entity types are available for filtering merge errors. Useful for building **entity type filter dropdowns**. |
| 18 | `/api/v1/int/:school/integrations-hub/overview/merge-errors/supported-terms` | GET | Returns which terms are available for filtering merge errors. Useful for building **term filter dropdowns**. |

### Data Validation

| # | Endpoint | Method | Description |
|---|----------|--------|-------------|
| 19 | `/api/v1/int/:school/integrations-hub/data-validation/overview` | GET | Returns data validation overview — shows which entity types have validation issues and details about the enabled integration save state. Useful for a **"data quality"** metric per school. |

### Activity Log

| # | Endpoint | Method | Description |
|---|----------|--------|-------------|
| 20 | `/api/v1/int/:school/activity-log` | GET | Paginated integration activity log. Params: `page`, `size`, `actionTypes[]`, `entityTypes[]`, `sourceTypes[]`, `timestampFrom`, `timestampTo`, `search`. Shows all integration-related events (merges started, completed, failed, settings changed). Useful for an **audit trail / event timeline**. |
| 21 | `/api/v1/int/:school/activity-log/comments` | GET | Returns comments on activity log entries. Shows team communication about integration issues. |

### Global Settings & Related Schools

| # | Endpoint | Method | Description |
|---|----------|--------|-------------|
| 22 | `/api/v1/int/:school/integrations-hub/global-settings/related-schools` | GET | Returns schools that share integration configurations (parent/child relationships). Useful for understanding **school groupings**. |

### Merge Reports (Legacy Routes)

| # | Endpoint | Method | Description |
|---|----------|--------|-------------|
| 23 | `/api/v1/:school/mergeReports/:mergeReportId` | GET | Returns a single merge report by ID with full details. Useful for **drill-down** from failed merge lists. |

---

## Recommended Additions to the Dashboard

**High value, easy to integrate:**
- **#10** (`integration/configuration`) — Identify which schools have integrations enabled. Filter out schools without integrations to reduce noise.
- **#11** (`merge-history`) — Replace the current `mergeReports` call with this more powerful endpoint that supports date range and status filtering.
- **#19** (`data-validation/overview`) — Add a "data quality" column showing validation issues per school.
- **#9** (`in-progress-status`) — Show a live "merge running" indicator per school.

**Medium value, useful for drill-downs:**
- **#20** (`activity-log`) — Add an activity timeline to the school detail view.
- **#12** (`coursedog-updates`) — Show merge impact (records changed) alongside success/fail stats.
- **#13** (`entity-merge-errors`) — Enable drill-down into specific merge failures.

**Lower priority but nice-to-have:**
- **#17/#18** (supported entity types/terms) — Enable filtered views of merge errors.
- **#22** (related schools) — Group related schools in the dashboard.
