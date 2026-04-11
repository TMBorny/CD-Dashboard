"""API routes for the Client Health Dashboard backend.

All routes persist data to SQLite via SQLAlchemy models.
The /api/client-health/sync endpoint fetches live data from Coursedog
and stores it in the database.
"""

import asyncio
import time
from datetime import datetime, timezone, timedelta
from uuid import uuid4
from typing import Optional

from fastapi import APIRouter, Query, HTTPException

from app.db import api_get, get_db
from app.models import SchoolSnapshot, SyncRun

router = APIRouter(prefix="/api")
_sync_jobs: dict[str, dict] = {}
_active_sync_job_id: Optional[str] = None
TEMP_FULL_SYNC_LIMIT = 10
EXCLUDED_SCHOOL_TERMS = ("demo", "test", "sandbox", "baseline")


def school_matches_excluded_terms(value: Optional[str]) -> bool:
    """Return True when a school identifier/display name matches an excluded term."""
    if not value:
        return False
    lowered = value.lower()
    return any(term in lowered for term in EXCLUDED_SCHOOL_TERMS)


def is_demo_school(school: str, display_name: str) -> bool:
    """Return True when a school looks like a demo/testing tenant."""
    haystacks = [school, display_name]
    return any(school_matches_excluded_terms(value) for value in haystacks)


def exclude_demo_school_snapshots(query):
    """Exclude demo-school snapshots from SQLite-backed reads."""
    for term in EXCLUDED_SCHOOL_TERMS:
        query = query.filter(~SchoolSnapshot.school.ilike(f"%{term}%"))
        query = query.filter(~SchoolSnapshot.display_name.ilike(f"%{term}%"))
    return query


# ---------------------------------------------------------------------------
# Read endpoints — serve data from SQLite
# ---------------------------------------------------------------------------


@router.get("/schools")
async def list_schools():
    """List all schools from the Coursedog admin API."""
    try:
        data = await api_get("/api/v1/admin/schools/products")
        schools = []
        if isinstance(data, list):
            for item in data:
                school = item.get("_id", item.get("school", ""))
                display_name = item.get("displayName", item.get("_id", ""))
                if is_demo_school(school, display_name):
                    continue
                schools.append({
                    "school": school,
                    "displayName": display_name,
                    "products": item.get("products", []),
                })
        elif isinstance(data, dict):
            for key, val in data.items():
                if isinstance(val, dict):
                    display_name = val.get("displayName", key)
                    if is_demo_school(key, display_name):
                        continue
                    schools.append({
                        "school": key,
                        "displayName": display_name,
                        "products": val.get("products", []),
                    })
                else:
                    if is_demo_school(key, key):
                        continue
                    schools.append({"school": key, "displayName": key, "products": []})
        schools.sort(key=lambda s: s["school"])
        return {"schools": schools}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Coursedog API error: {e}")


@router.get("/client-health")
async def get_client_health():
    """Get the latest snapshot for all schools from the database."""
    db = get_db()
    try:
        # Find the most recent snapshot date
        latest = (
            db.query(SchoolSnapshot.snapshot_date)
            .order_by(SchoolSnapshot.snapshot_date.desc())
            .first()
        )
        if not latest:
            return {"snapshotDate": None, "schools": []}

        latest_date = latest[0]
        snapshots = (
            exclude_demo_school_snapshots(db.query(SchoolSnapshot))
            .filter(SchoolSnapshot.snapshot_date == latest_date)
            .order_by(SchoolSnapshot.school)
            .all()
        )
        return {
            "snapshotDate": latest_date,
            "schools": [s.to_dict() for s in snapshots],
        }
    finally:
        db.close()


@router.get("/client-health/history")
async def get_client_health_history(
    days: int = Query(default=30, ge=1, le=365),
    school: Optional[str] = Query(default=None),
):
    """Get historical snapshots from the database."""
    db = get_db()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
        query = exclude_demo_school_snapshots(db.query(SchoolSnapshot)).filter(
            SchoolSnapshot.snapshot_date >= cutoff
        )
        if school:
            query = query.filter(SchoolSnapshot.school == school)

        snapshots = query.order_by(SchoolSnapshot.snapshot_date).all()
        return {"snapshots": [s.to_dict() for s in snapshots]}
    finally:
        db.close()


@router.get("/client-health/active-users")
async def get_client_health_active_users(
    school: str = Query(...),
    after: Optional[str] = Query(default=None),
    before: Optional[str] = Query(default=None),
):
    """Get active users for a school via the Coursedog user activity API."""
    try:
        params: dict = {}
        normalized_after = normalize_time_param(after)
        normalized_before = normalize_time_param(before)
        if normalized_after:
            params["after"] = normalized_after
        if normalized_before:
            params["before"] = normalized_before
        data = await api_get(f"/api/v1/{school}/userActivity", params=params)
        users = extract_unique_activity_users(data)
        return {"count": len(users), "users": users}
    except Exception as e:
        return {"count": 0, "users": [], "error": str(e)}


@router.get("/client-health/sync-metadata")
async def get_client_health_sync_metadata(school: Optional[str] = Query(default=None)):
    """Return last attempted sync run and last successful snapshot timestamps."""
    db = get_db()
    try:
        latest_attempt_query = db.query(SyncRun)
        latest_success_query = exclude_demo_school_snapshots(db.query(SchoolSnapshot))

        if school:
            latest_attempt_query = latest_attempt_query.filter(SyncRun.school == school)
            latest_success_query = latest_success_query.filter(SchoolSnapshot.school == school)

        latest_attempt = latest_attempt_query.order_by(SyncRun.attempted_at.desc()).first()
        latest_success = latest_success_query.order_by(SchoolSnapshot.created_at.desc()).first()

        return {
            "lastAttemptedSync": latest_attempt.to_dict() if latest_attempt else None,
            "lastSuccessfulSync": {
                "school": latest_success.school,
                "snapshotDate": latest_success.snapshot_date,
                "createdAt": latest_success.created_at.isoformat() if latest_success.created_at else None,
            }
            if latest_success
            else None,
        }
    finally:
        db.close()

@router.get("/client-health/sync-runs")
async def get_client_health_sync_runs(
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
):
    """Return historical sync run events for background job monitoring."""
    db = get_db()
    try:
        runs = (
            db.query(SyncRun)
            .order_by(SyncRun.attempted_at.desc())
            .offset(offset)
            .limit(limit)
            .all()
        )
        return {"syncRuns": [r.to_dict() for r in runs]}
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Sync endpoint — fetch live data from Coursedog and persist to SQLite
# ---------------------------------------------------------------------------


@router.post("/client-health/sync")
async def trigger_sync(school: Optional[str] = Query(default=None)):
    """Start a background sync job for all schools or a single school."""
    global _active_sync_job_id

    if _active_sync_job_id:
        active_job = _sync_jobs.get(_active_sync_job_id)
        if active_job and active_job["status"] in {"queued", "running"}:
            return serialize_sync_job(active_job, already_running=True)

    job_id = str(uuid4())
    job = {
        "jobId": job_id,
        "status": "queued",
        "snapshotDate": None,
        "schoolsProcessed": 0,
        "totalSchools": 0,
        "errors": [],
        "timing": None,
        "error": None,
        "scope": "single-school" if school else "bulk",
        "school": school,
        "startedAt": None,
        "finishedAt": None,
    }
    _sync_jobs[job_id] = job
    _active_sync_job_id = job_id
    asyncio.create_task(run_sync_job(job_id, school=school))
    return serialize_sync_job(job)


@router.post("/client-health/history/backfill")
async def trigger_history_backfill(
    school: str = Query(...),
    days: int = Query(default=7, ge=1, le=30),
):
    """Backfill historical snapshots for a single school."""
    global _active_sync_job_id

    if _active_sync_job_id:
        active_job = _sync_jobs.get(_active_sync_job_id)
        if active_job and active_job["status"] in {"queued", "running"}:
            return serialize_sync_job(active_job, already_running=True)

    job_id = str(uuid4())
    job = {
        "jobId": job_id,
        "status": "queued",
        "snapshotDate": None,
        "schoolsProcessed": 0,
        "totalSchools": days,
        "errors": [],
        "timing": None,
        "error": None,
        "scope": "history-backfill",
        "school": school,
        "days": days,
        "startedAt": None,
        "finishedAt": None,
    }
    _sync_jobs[job_id] = job
    _active_sync_job_id = job_id
    asyncio.create_task(run_history_backfill_job(job_id, school=school, days=days))
    return serialize_sync_job(job)


@router.get("/client-health/sync/{job_id}")
async def get_sync_status(job_id: str):
    """Return status for a previously created sync job."""
    job = _sync_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Sync job not found")
    return serialize_sync_job(job)


def select_schools_for_sync(schools: list[dict], school: Optional[str]) -> list[dict]:
    """Select which schools should be synced for the requested scope."""
    if school:
        selected = [item for item in schools if item["school"] == school]
        if not selected:
            raise HTTPException(status_code=404, detail=f"School not found: {school}")
        return selected
    return schools[:TEMP_FULL_SYNC_LIMIT]


async def run_sync_job(job_id: str, school: Optional[str] = None):
    """Fetch live health data in the background and persist a daily snapshot."""
    global _active_sync_job_id
    job = _sync_jobs[job_id]
    started_at = datetime.now(timezone.utc)
    job["status"] = "running"
    job["startedAt"] = started_at.isoformat()

    start_time = time.time()
    db = get_db()
    sync_run = SyncRun(
        job_id=job_id,
        school=school,
        scope=job["scope"],
        status="running",
        attempted_at=started_at,
    )
    db.add(sync_run)
    db.commit()
    db.refresh(sync_run)
    db.close()

    try:
        # 1. Get all schools
        schools_resp = await list_schools()
        schools = select_schools_for_sync(schools_resp["schools"], school)
        job["totalSchools"] = len(schools)

        list_time = time.time()

        snapshot_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        new_snapshots: list[dict] = []
        errors: list[str] = []

        # 2. Process schools in batches of 10
        batch_size = 10
        for i in range(0, len(schools), batch_size):
            batch = schools[i: i + batch_size]
            tasks = [
                fetch_school_health(
                    s["school"],
                    s.get("displayName", s["school"]),
                    s.get("products", []),
                    snapshot_date,
                )
                for s in batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    errors.append(str(result))
                elif result is not None:
                    errors.extend(result.pop("_syncErrors", []))
                    new_snapshots.append(result)
            job["schoolsProcessed"] = len(new_snapshots)
            job["errors"] = errors[-20:]

        fetch_time = time.time()

        # 3. Persist to SQLite — upsert (delete old for today, insert new)
        db = get_db()
        try:
            delete_query = db.query(SchoolSnapshot).filter(
                SchoolSnapshot.snapshot_date == snapshot_date
            )
            if school:
                delete_query = delete_query.filter(SchoolSnapshot.school == school)
            delete_query.delete()
            for snap_data in new_snapshots:
                db.add(SchoolSnapshot.from_dict(snap_data))
            db.commit()
        except Exception as e:
            db.rollback()
            raise RuntimeError(f"Database write error: {e}") from e
        finally:
            db.close()

        persist_time = time.time()
        total_elapsed = persist_time - start_time

        job.update(
            {
                "status": "completed",
                "snapshotDate": snapshot_date,
                "schoolsProcessed": len(new_snapshots),
                "totalSchools": len(schools),
                "errors": errors[-20:],
                "timing": {
                    "listSchoolsSec": round(list_time - start_time, 2),
                    "fetchHealthSec": round(fetch_time - list_time, 2),
                    "persistDbSec": round(persist_time - fetch_time, 2),
                    "totalSec": round(total_elapsed, 2),
                },
                "finishedAt": datetime.now(timezone.utc).isoformat(),
            }
        )
        db = get_db()
        try:
            sync_run = db.query(SyncRun).filter(SyncRun.job_id == job_id).first()
            if sync_run:
                sync_run.status = "completed"
                sync_run.snapshot_date = snapshot_date
                sync_run.finished_at = datetime.now(timezone.utc)
                sync_run.error_message = errors[0] if errors else None
                db.commit()
        finally:
            db.close()
    except Exception as e:
        job.update(
            {
                "status": "failed",
                "error": str(e),
                "finishedAt": datetime.now(timezone.utc).isoformat(),
            }
        )
        db = get_db()
        try:
            sync_run = db.query(SyncRun).filter(SyncRun.job_id == job_id).first()
            if sync_run:
                sync_run.status = "failed"
                sync_run.finished_at = datetime.now(timezone.utc)
                sync_run.error_message = str(e)
                db.commit()
        finally:
            db.close()
    finally:
        if _active_sync_job_id == job_id:
            _active_sync_job_id = None


# Also keep the old endpoint name for backwards compat
@router.post("/client-health/snapshot")
async def trigger_snapshot():
    """Alias for /sync for backwards compatibility."""
    return await trigger_sync()


def serialize_sync_job(job: dict, already_running: bool = False) -> dict:
    """Return the stable JSON shape used by the frontend sync polling UI."""
    payload = dict(job)
    if already_running:
        payload["alreadyRunning"] = True
    if payload.get("scope") == "bulk":
        payload["limit"] = TEMP_FULL_SYNC_LIMIT
    return payload


def parse_count(value) -> Optional[int]:
    """Return an integer count from ints or numeric strings."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def normalize_time_param(value: Optional[str]) -> Optional[str]:
    """Normalize ISO or integer-ish timestamps to epoch milliseconds strings."""
    if not value:
        return None
    if value.isdigit():
        return value

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return str(int(parsed.timestamp() * 1000))
    except ValueError:
        return value


def extract_merge_error_count(payload) -> int:
    """Best-effort extraction of total merge errors from varied API response shapes."""
    if isinstance(payload, list):
        return len(payload)

    if not isinstance(payload, dict):
        return 0

    direct_keys = ("totalCount", "total", "count", "totalElements", "recordsTotal")
    for key in direct_keys:
        value = payload.get(key)
        parsed = parse_count(value)
        if parsed is not None:
            return parsed

    nested_keys = ("data", "meta", "pagination", "page", "result", "results")
    for key in nested_keys:
        value = payload.get(key)
        if isinstance(value, (dict, list)):
            nested_count = extract_merge_error_count(value)
            if nested_count:
                return nested_count

    collection_keys = ("content", "items", "rows", "mergeErrors", "errors")
    for key in collection_keys:
        value = payload.get(key)
        if isinstance(value, list):
            return len(value)
        if isinstance(value, dict):
            nested_count = extract_merge_error_count(value)
            if nested_count:
                return nested_count

    return 0


def extract_merge_history_entries(payload) -> list[dict]:
    """Best-effort extraction of merge history entries from varied API response shapes."""
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []

    for key in ("content", "items", "results", "mergeReports", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            normalized: list[dict] = []
            for item in value:
                if not isinstance(item, dict):
                    continue
                normalized.append(item.get("mergeReport", item))
            return normalized
        if isinstance(value, dict):
            nested = extract_merge_history_entries(value)
            if nested:
                return nested

    return []


def extract_activity_entries(payload) -> list[dict]:
    """Best-effort extraction of user activity rows from varied API response shapes."""
    if isinstance(payload, list):
        return [entry for entry in payload if isinstance(entry, dict)]
    if not isinstance(payload, dict):
        return []

    for key in ("data", "results", "items", "content", "rows", "activities", "userActivity"):
        value = payload.get(key)
        if isinstance(value, list):
            return [entry for entry in value if isinstance(entry, dict)]
        if isinstance(value, dict):
            nested = extract_activity_entries(value)
            if nested:
                return nested

    return []


def extract_unique_activity_users(payload) -> list[str]:
    """Return sorted unique user identifiers from a user activity payload."""
    entries = extract_activity_entries(payload)
    if entries:
        return sorted(
            {
                (entry.get("email") or entry.get("userId") or "").strip()
                for entry in entries
                if (entry.get("email") or entry.get("userId"))
            }
        )

    if isinstance(payload, dict):
        identifiers: set[str] = set()
        for key, value in payload.items():
            if isinstance(value, dict):
                identifier = (
                    (value.get("email") or value.get("userId") or key)
                    if key
                    else None
                )
                if identifier:
                    identifiers.add(str(identifier).strip())
            elif isinstance(key, str) and key:
                identifiers.add(key.strip())
        return sorted(identifier for identifier in identifiers if identifier)

    return []


def extract_sis_platform(payload) -> Optional[str]:
    """Extract the best SIS label from an integration configuration payload."""
    if not isinstance(payload, dict):
        return None

    for key in ("sisPlatform", "integrationBroker"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    return None


async def fetch_school_sis_platform(school: str) -> Optional[str]:
    """Fetch the configured SIS platform for a school."""
    config = await api_get(f"/api/v1/{school}/integration/configuration")
    return extract_sis_platform(config)


def build_snapshot_from_merge_history(
    school: str,
    display_name: str,
    sis_platform: Optional[str],
    products: list,
    snapshot_date: str,
    merge_entries: list[dict],
    active_users_24h: int,
) -> dict:
    """Build a historical snapshot from merge history plus user activity."""
    nightly_total = nightly_success = nightly_issues = nightly_nodata = 0
    realtime_total = realtime_success = realtime_issues = realtime_nodata = 0
    manual_total = manual_success = manual_issues = manual_nodata = 0
    failed_entries: list[dict] = []

    for report in merge_entries:
        schedule = str(report.get("scheduleType", "")).lower()
        status = str(report.get("status", "")).lower()
        succeeded = status == "success"
        has_issues = status == "finishedwithissues"
        no_data = status == "nodata"
        failed = not (succeeded or has_issues or no_data)

        if schedule == "nightly":
            nightly_total += 1
            if succeeded:
                nightly_success += 1
            elif has_issues:
                nightly_issues += 1
            elif no_data:
                nightly_nodata += 1
        elif schedule == "realtime":
            realtime_total += 1
            if succeeded:
                realtime_success += 1
            elif has_issues:
                realtime_issues += 1
            elif no_data:
                realtime_nodata += 1
        elif schedule == "manual":
            manual_total += 1
            if succeeded:
                manual_success += 1
            elif has_issues:
                manual_issues += 1
            elif no_data:
                manual_nodata += 1

        if failed:
            failed_entries.append(
                {
                    "id": report.get("id", report.get("_id", "unknown")),
                    "type": report.get("type"),
                    "scheduleType": report.get("scheduleType"),
                    "timestampEnd": report.get("timestampEnd"),
                    "status": report.get("status"),
                }
            )

    return {
        "snapshotDate": snapshot_date,
        "school": school,
        "displayName": display_name,
        "sisPlatform": sis_platform,
        "products": products,
        "merges": {
            "nightly": {
                "total": nightly_total,
                "succeeded": nightly_success,
                "failed": nightly_total - nightly_success - nightly_issues - nightly_nodata,
                "finishedWithIssues": nightly_issues,
                "noData": nightly_nodata,
            },
            "realtime": {
                "total": realtime_total,
                "succeeded": realtime_success,
                "failed": realtime_total - realtime_success - realtime_issues - realtime_nodata,
                "finishedWithIssues": realtime_issues,
                "noData": realtime_nodata,
            },
            "manual": {
                "total": manual_total,
                "succeeded": manual_success,
                "failed": manual_total - manual_success - manual_issues - manual_nodata,
                "finishedWithIssues": manual_issues,
                "noData": manual_nodata,
            },
        },
        # Historical approximation: the current-state merge-errors overview endpoint
        # does not expose per-day counts, so use failed merge history entries per day.
        "recentFailedMerges": failed_entries[:10],
        "mergeErrorsCount": len(failed_entries),
        "activeUsers24h": active_users_24h,
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Helper: fetch health data for a single school
# ---------------------------------------------------------------------------


def summarize_recent_merge_activity(merge_entries: list[dict], last_24h: int) -> tuple[int, int, int, int, int, int, int, int]:
    """Summarize realtime/manual merge activity in the last 24 hours."""
    realtime_total = realtime_success = realtime_issues = realtime_nodata = 0
    manual_total = manual_success = manual_issues = manual_nodata = 0

    for report in merge_entries:
        ts = report.get("timestampEnd", 0)
        schedule = str(report.get("scheduleType", "")).lower()
        status = str(report.get("status", "")).lower()

        if not ts or ts <= last_24h:
            continue

        if schedule == "realtime":
            realtime_total += 1
            if status == "success":
                realtime_success += 1
            elif status == "finishedwithissues":
                realtime_issues += 1
            elif status == "nodata":
                realtime_nodata += 1
        elif schedule == "manual":
            manual_total += 1
            if status == "success":
                manual_success += 1
            elif status == "finishedwithissues":
                manual_issues += 1
            elif status == "nodata":
                manual_nodata += 1

    return (
        realtime_total, realtime_success, realtime_issues, realtime_nodata,
        manual_total, manual_success, manual_issues, manual_nodata
    )


async def fetch_school_health(
    school: str, display_name: str, products: list, snapshot_date: str
) -> Optional[dict]:
    """Fetch health data for a single school from the Coursedog API."""
    try:
        sync_errors: list[str] = []

        # Fetch integration health (nightly merge stats + recent failed merges)
        health_data = {}
        try:
            health_data = await api_get(f"/api/v1/int/{school}/integrations-hub/overview/health")
        except Exception as e:
            sync_errors.append(f"{school}: health request failed ({e})")

        # Fetch merge errors
        merge_errors_data = {}
        try:
            merge_errors_data = await api_get(
                f"/api/v1/int/{school}/integrations-hub/overview/merge-errors",
                params={"page": "0", "size": "1"},
            )
        except Exception as e:
            sync_errors.append(f"{school}: merge errors request failed ({e})")

        # Fetch recent merge history to compute realtime/manual stats
        merge_reports: list = []
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        last_24h = now_ms - (24 * 60 * 60 * 1000)
        date_from = datetime.fromtimestamp(last_24h / 1000, tz=timezone.utc).isoformat()
        date_to = datetime.now(timezone.utc).isoformat()
        try:
            merge_reports_data = await api_get(
                f"/api/v1/int/{school}/integrations-hub/merge-history",
                params={
                    "page": "0",
                    "size": "500",
                    "dateFrom": date_from,
                    "dateTo": date_to,
                },
            )
            merge_reports = extract_merge_history_entries(merge_reports_data)
        except Exception as e:
            sync_errors.append(f"{school}: merge history request failed ({e})")

        # Parse health data
        nightly_total = health_data.get("allNightlyMergesCount", 0)
        nightly_success = health_data.get("succeededNightlyMergesCount", 0)
        recent_failed = health_data.get("recentFailedMerges", [])

        # Compute realtime/manual from merge reports
        (
            realtime_total, realtime_success, realtime_issues, realtime_nodata,
            manual_total, manual_success, manual_issues, manual_nodata
        ) = summarize_recent_merge_activity(merge_reports, last_24h)

        # Parse merge errors count
        merge_errors_count = extract_merge_error_count(merge_errors_data)

        # Active users — try to get from user activity endpoint
        active_users_24h = 0
        try:
            activity_data = await api_get(
                f"/api/v1/{school}/userActivity",
                params={"after": str(last_24h)},
            )
            active_users_24h = len(extract_unique_activity_users(activity_data))
        except Exception as e:
            sync_errors.append(f"{school}: user activity request failed ({e})")

        if len(sync_errors) == 4:
            raise RuntimeError(f"{school}: all upstream requests failed")

        sis_platform = None
        try:
            sis_platform = await fetch_school_sis_platform(school)
        except Exception as e:
            sync_errors.append(f"{school}: integration config request failed ({e})")

        return {
            "snapshotDate": snapshot_date,
            "school": school,
            "displayName": display_name,
            "sisPlatform": sis_platform,
            "products": products,
            "merges": {
                "nightly": {
                    "total": nightly_total,
                    "succeeded": nightly_success,
                    "failed": nightly_total - nightly_success,
                    "finishedWithIssues": 0,
                    "noData": 0,
                },
                "realtime": {
                    "total": realtime_total,
                    "succeeded": realtime_success,
                    "failed": realtime_total - realtime_success - realtime_issues - realtime_nodata,
                    "finishedWithIssues": realtime_issues,
                    "noData": realtime_nodata,
                },
                "manual": {
                    "total": manual_total,
                    "succeeded": manual_success,
                    "failed": manual_total - manual_success - manual_issues - manual_nodata,
                    "finishedWithIssues": manual_issues,
                    "noData": manual_nodata,
                },
            },
            "recentFailedMerges": recent_failed,
            "mergeErrorsCount": merge_errors_count,
            "activeUsers24h": active_users_24h,
            "createdAt": datetime.now(timezone.utc).isoformat(),
            "_syncErrors": sync_errors,
        }
    except Exception as e:
        raise RuntimeError(f"Error fetching health for {school}: {e}") from e


async def fetch_school_health_for_date(
    school: str, display_name: str, products: list, snapshot_date: str
) -> dict:
    """Fetch one day's historical health data for a school."""
    today_snapshot_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if snapshot_date == today_snapshot_date:
        return await fetch_school_health(
            school=school,
            display_name=display_name,
            products=products,
            snapshot_date=snapshot_date,
        )

    sync_errors: list[str] = []

    day_start = datetime.strptime(snapshot_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    merge_entries: list[dict] = []
    try:
        merge_history_data = await api_get(
            f"/api/v1/int/{school}/integrations-hub/merge-history",
            params={
                "page": "0",
                "size": "500",
                "dateFrom": day_start.isoformat(),
                "dateTo": day_end.isoformat(),
            },
        )
        merge_entries = extract_merge_history_entries(merge_history_data)
    except Exception as e:
        sync_errors.append(f"{school} {snapshot_date}: merge history request failed ({e})")

    active_users_24h = 0
    try:
        activity_data = await api_get(
            f"/api/v1/{school}/userActivity",
            params={
                "after": str(int(day_start.timestamp() * 1000)),
                "before": str(int(day_end.timestamp() * 1000)),
            },
        )
        active_users_24h = len(extract_unique_activity_users(activity_data))
    except Exception as e:
        sync_errors.append(f"{school} {snapshot_date}: user activity request failed ({e})")

    sis_platform = None
    try:
        sis_platform = await fetch_school_sis_platform(school)
    except Exception as e:
        sync_errors.append(f"{school} {snapshot_date}: integration config request failed ({e})")

    snapshot = build_snapshot_from_merge_history(
        school=school,
        display_name=display_name,
        sis_platform=sis_platform,
        products=products,
        snapshot_date=snapshot_date,
        merge_entries=merge_entries,
        active_users_24h=active_users_24h,
    )
    snapshot["_syncErrors"] = sync_errors
    return snapshot


async def run_history_backfill_job(job_id: str, school: str, days: int):
    """Backfill historical daily snapshots for a single school."""
    global _active_sync_job_id
    job = _sync_jobs[job_id]
    started_at = datetime.now(timezone.utc)
    job["status"] = "running"
    job["startedAt"] = started_at.isoformat()

    start_time = time.time()
    db = get_db()
    sync_run = SyncRun(
        job_id=job_id,
        school=school,
        scope=job["scope"],
        status="running",
        attempted_at=started_at,
    )
    db.add(sync_run)
    db.commit()
    db.refresh(sync_run)
    db.close()

    try:
        schools_resp = await list_schools()
        schools = select_schools_for_sync(schools_resp["schools"], school)
        school_info = schools[0]
        list_time = time.time()

        snapshot_dates = [
            (datetime.now(timezone.utc) - timedelta(days=offset)).strftime("%Y-%m-%d")
            for offset in range(days - 1, -1, -1)
        ]

        snapshots: list[dict] = []
        errors: list[str] = []
        for snapshot_date in snapshot_dates:
            result = await fetch_school_health_for_date(
                school=school_info["school"],
                display_name=school_info.get("displayName", school_info["school"]),
                products=school_info.get("products", []),
                snapshot_date=snapshot_date,
            )
            errors.extend(result.pop("_syncErrors", []))
            snapshots.append(result)
            job["schoolsProcessed"] = len(snapshots)
            job["errors"] = errors[-20:]

        fetch_time = time.time()

        db = get_db()
        try:
            db.query(SchoolSnapshot).filter(
                SchoolSnapshot.school == school_info["school"],
                SchoolSnapshot.snapshot_date.in_(snapshot_dates),
            ).delete(synchronize_session=False)
            for snapshot in snapshots:
                db.add(SchoolSnapshot.from_dict(snapshot))
            db.commit()
        except Exception as e:
            db.rollback()
            raise RuntimeError(f"Database write error: {e}") from e
        finally:
            db.close()

        persist_time = time.time()
        job.update(
            {
                "status": "completed",
                "snapshotDate": snapshot_dates[-1],
                "schoolsProcessed": len(snapshots),
                "totalSchools": len(snapshot_dates),
                "errors": errors[-20:],
                "timing": {
                    "listSchoolsSec": round(list_time - start_time, 2),
                    "fetchHealthSec": round(fetch_time - list_time, 2),
                    "persistDbSec": round(persist_time - fetch_time, 2),
                    "totalSec": round(persist_time - start_time, 2),
                },
                "finishedAt": datetime.now(timezone.utc).isoformat(),
            }
        )
        db = get_db()
        try:
            sync_run = db.query(SyncRun).filter(SyncRun.job_id == job_id).first()
            if sync_run:
                sync_run.status = "completed"
                sync_run.snapshot_date = snapshot_dates[-1]
                sync_run.finished_at = datetime.now(timezone.utc)
                sync_run.error_message = errors[0] if errors else None
                db.commit()
        finally:
            db.close()
    except Exception as e:
        job.update(
            {
                "status": "failed",
                "error": str(e),
                "finishedAt": datetime.now(timezone.utc).isoformat(),
            }
        )
        db = get_db()
        try:
            sync_run = db.query(SyncRun).filter(SyncRun.job_id == job_id).first()
            if sync_run:
                sync_run.status = "failed"
                sync_run.finished_at = datetime.now(timezone.utc)
                sync_run.error_message = str(e)
                db.commit()
        finally:
            db.close()
    finally:
        if _active_sync_job_id == job_id:
            _active_sync_job_id = None
