"""API routes for the Client Health Dashboard backend.

All routes persist data to SQLite via SQLAlchemy models.
The /api/client-health/sync endpoint fetches live data from Coursedog
and stores it in the database.
"""

import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from uuid import uuid4
from typing import Optional

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

from app.db import api_get, get_db
from app.models import ExcludedSchool, SchoolSnapshot, SyncRun

router = APIRouter(prefix="/api")
_sync_jobs: dict[str, dict] = {}
_active_sync_job_id: Optional[str] = None
_scheduled_sync_task: Optional[asyncio.Task] = None
_scheduled_sync_stop_event: Optional[asyncio.Event] = None
EXCLUDED_SCHOOL_TERMS = ("demo", "test", "sandbox", "baseline")


class SchoolExclusionPayload(BaseModel):
    school: str
SCHEDULE_TIMEZONE = ZoneInfo("America/New_York")
SCHEDULED_SYNC_HOUR = 7
SCHEDULED_SYNC_MINUTE = 30


def school_matches_excluded_terms(value: Optional[str]) -> bool:
    """Return True when a school identifier/display name matches an excluded term."""
    if not value:
        return False
    lowered = value.lower()
    return any(term in lowered for term in EXCLUDED_SCHOOL_TERMS)


def get_additional_excluded_schools(db) -> set[str]:
    """Return locally managed excluded school ids."""
    return {
        row.school.strip().lower()
        for row in db.query(ExcludedSchool).all()
        if row.school and row.school.strip()
    }


def normalize_school_catalog(data) -> list[dict]:
    """Normalize the admin schools response into a flat school list without exclusions."""
    schools: list[dict] = []
    if isinstance(data, list):
        for item in data:
            school = item.get("_id", item.get("school", ""))
            display_name = item.get("displayName", item.get("_id", ""))
            schools.append({
                "school": school,
                "displayName": display_name,
                "products": item.get("products", []),
            })
    elif isinstance(data, dict):
        for key, val in data.items():
            if isinstance(val, dict):
                schools.append({
                    "school": key,
                    "displayName": val.get("displayName", key),
                    "products": val.get("products", []),
                })
            else:
                schools.append({"school": key, "displayName": key, "products": []})
    schools.sort(key=lambda s: s["school"])
    return schools


def get_school_exclusion_reason(
    school: str, display_name: str, additional_excluded_schools: set[str]
) -> Optional[str]:
    """Return a human-readable reason when a school should be excluded."""
    normalized_school = school.strip().lower()
    if normalized_school in additional_excluded_schools:
        return "Manually excluded in Operations"

    for term in EXCLUDED_SCHOOL_TERMS:
        if term in normalized_school or term in (display_name or "").lower():
            return f"Matches excluded term: {term}"

    return None


def split_school_catalog(
    schools: list[dict], additional_excluded_schools: set[str]
) -> tuple[list[dict], list[dict]]:
    """Split a raw school catalog into included and excluded buckets."""
    included: list[dict] = []
    excluded: list[dict] = []

    for school in schools:
        reason = get_school_exclusion_reason(
            school.get("school", ""),
            school.get("displayName", ""),
            additional_excluded_schools,
        )
        if reason:
            excluded.append({**school, "reason": reason})
        else:
            included.append(school)

    return included, excluded


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
    db = get_db()
    try:
        data = await api_get("/api/v1/admin/schools/products")
        all_schools = normalize_school_catalog(data)
        additional_excluded_schools = get_additional_excluded_schools(db)
        included_schools, excluded_schools = split_school_catalog(
            all_schools, additional_excluded_schools
        )
        return {
            "schools": included_schools,
            "excludedSchools": excluded_schools,
            "excludedTerms": list(EXCLUDED_SCHOOL_TERMS),
            "additionalExcludedSchools": sorted(additional_excluded_schools),
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Coursedog API error: {e}")
    finally:
        db.close()


@router.post("/schools/exclusions")
async def add_school_exclusion(payload: SchoolExclusionPayload):
    """Persist an additional school exclusion for Operations and bulk syncs."""
    normalized_school = payload.school.strip().lower()
    if not normalized_school:
        raise HTTPException(status_code=422, detail="school is required")

    db = get_db()
    try:
        existing = db.query(ExcludedSchool).filter(ExcludedSchool.school == normalized_school).first()
        if not existing:
            db.add(ExcludedSchool(school=normalized_school))
            db.commit()
        return {"school": normalized_school}
    finally:
        db.close()


@router.delete("/schools/exclusions/{school}")
async def remove_school_exclusion(school: str):
    """Remove a previously added school exclusion."""
    normalized_school = school.strip().lower()
    db = get_db()
    try:
        existing = db.query(ExcludedSchool).filter(ExcludedSchool.school == normalized_school).first()
        if existing:
            db.delete(existing)
            db.commit()
        return {"school": normalized_school}
    finally:
        db.close()


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
    job, already_running = enqueue_sync_job(school=school)
    return serialize_sync_job(job, already_running=already_running)


@router.post("/client-health/history/backfill")
async def trigger_history_backfill(
    startDate: str = Query(...),
    endDate: Optional[str] = Query(default=None),
    school: Optional[str] = Query(default=None),
):
    """Backfill historical snapshots for one school or all schools."""
    global _active_sync_job_id

    if _active_sync_job_id:
        active_job = _sync_jobs.get(_active_sync_job_id)
        if active_job and active_job["status"] in {"queued", "running"}:
            return serialize_sync_job(active_job, already_running=True)

    normalized_start, normalized_end = resolve_backfill_date_range(startDate, endDate)
    snapshot_dates = build_snapshot_dates(normalized_start, normalized_end)

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
        "scope": "history-backfill-single" if school else "history-backfill-bulk",
        "school": school,
        "startDate": normalized_start,
        "endDate": normalized_end,
        "dateCount": len(snapshot_dates),
        "startedAt": None,
        "finishedAt": None,
    }
    _sync_jobs[job_id] = job
    _active_sync_job_id = job_id
    asyncio.create_task(
        run_history_backfill_job(
            job_id,
            school=school,
            start_date=normalized_start,
            end_date=normalized_end,
        )
    )
    return serialize_sync_job(job)


@router.get("/client-health/sync/{job_id}")
async def get_sync_status(job_id: str):
    """Return status for a previously created sync job."""
    job = _sync_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Sync job not found")
    return serialize_sync_job(job)


def current_new_york_time(now: Optional[datetime] = None) -> datetime:
    """Return the current time in the scheduler timezone."""
    base = now or datetime.now(timezone.utc)
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    return base.astimezone(SCHEDULE_TIMEZONE)


def get_new_york_snapshot_date(now: Optional[datetime] = None) -> str:
    """Return today's snapshot date in America/New_York."""
    return current_new_york_time(now).strftime("%Y-%m-%d")


def get_next_scheduled_sync_time(now: Optional[datetime] = None) -> datetime:
    """Return the next 7:30 AM America/New_York run time."""
    local_now = current_new_york_time(now)
    scheduled = local_now.replace(
        hour=SCHEDULED_SYNC_HOUR,
        minute=SCHEDULED_SYNC_MINUTE,
        second=0,
        microsecond=0,
    )
    if scheduled <= local_now:
        scheduled += timedelta(days=1)
    return scheduled


def get_active_sync_job() -> Optional[dict]:
    """Return the currently active queued/running job, if any."""
    if not _active_sync_job_id:
        return None
    active_job = _sync_jobs.get(_active_sync_job_id)
    if active_job and active_job["status"] in {"queued", "running"}:
        return active_job
    return None


def enqueue_sync_job(
    school: Optional[str] = None,
    *,
    scope: Optional[str] = None,
    snapshot_date_override: Optional[str] = None,
) -> tuple[dict, bool]:
    """Create and queue a sync job unless another sync is already active."""
    global _active_sync_job_id

    active_job = get_active_sync_job()
    if active_job:
        return active_job, True

    job_id = str(uuid4())
    job = {
        "jobId": job_id,
        "status": "queued",
        "snapshotDate": snapshot_date_override,
        "schoolsProcessed": 0,
        "totalSchools": 0,
        "errors": [],
        "timing": None,
        "error": None,
        "scope": scope or ("single-school" if school else "bulk"),
        "school": school,
        "startedAt": None,
        "finishedAt": None,
    }
    _sync_jobs[job_id] = job
    _active_sync_job_id = job_id
    asyncio.create_task(
        run_sync_job(
            job_id,
            school=school,
            snapshot_date_override=snapshot_date_override,
        )
    )
    return job, False


def has_successful_snapshot_for_date(snapshot_date: str) -> bool:
    """Return True when at least one non-demo snapshot exists for the date."""
    db = get_db()
    try:
        snapshot = (
            exclude_demo_school_snapshots(db.query(SchoolSnapshot))
            .filter(SchoolSnapshot.snapshot_date == snapshot_date)
            .first()
        )
        return snapshot is not None
    finally:
        db.close()


async def maybe_enqueue_catchup_sync(now: Optional[datetime] = None) -> Optional[dict]:
    """Queue one startup catch-up sync when today's local snapshot is missing."""
    snapshot_date = get_new_york_snapshot_date(now)
    if has_successful_snapshot_for_date(snapshot_date):
        return None

    job, _ = enqueue_sync_job(
        scope="scheduled-catchup-bulk",
        snapshot_date_override=snapshot_date,
    )
    return job


async def maybe_enqueue_scheduled_sync(now: Optional[datetime] = None) -> dict:
    """Queue the daily scheduled latest-only sync for today's New York date."""
    snapshot_date = get_new_york_snapshot_date(now)
    job, _ = enqueue_sync_job(
        scope="scheduled-bulk",
        snapshot_date_override=snapshot_date,
    )
    return job


async def run_scheduled_sync_loop(stop_event: asyncio.Event):
    """Sleep until the next 7:30 AM New York run, then queue a latest-only sync."""
    while not stop_event.is_set():
        now_utc = datetime.now(timezone.utc)
        next_run = get_next_scheduled_sync_time(now_utc)
        delay_seconds = max(0.0, (next_run.astimezone(timezone.utc) - now_utc).total_seconds())

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=delay_seconds)
            break
        except asyncio.TimeoutError:
            await maybe_enqueue_scheduled_sync()


async def start_scheduled_sync_service():
    """Start the daily scheduler and queue a same-day catch-up when needed."""
    global _scheduled_sync_stop_event, _scheduled_sync_task

    if _scheduled_sync_task and not _scheduled_sync_task.done():
        return

    _scheduled_sync_stop_event = asyncio.Event()
    await maybe_enqueue_catchup_sync()
    _scheduled_sync_task = asyncio.create_task(run_scheduled_sync_loop(_scheduled_sync_stop_event))


async def stop_scheduled_sync_service():
    """Stop the daily scheduler task cleanly."""
    global _scheduled_sync_stop_event, _scheduled_sync_task

    if _scheduled_sync_stop_event:
        _scheduled_sync_stop_event.set()

    if _scheduled_sync_task:
        await asyncio.gather(_scheduled_sync_task, return_exceptions=True)

    _scheduled_sync_stop_event = None
    _scheduled_sync_task = None


def select_schools_for_sync(schools: list[dict], school: Optional[str]) -> list[dict]:
    """Select which schools should be synced for the requested scope."""
    if school:
        selected = [item for item in schools if item["school"] == school]
        if not selected:
            raise HTTPException(status_code=404, detail=f"School not found: {school}")
        return selected
    return list(schools)


async def run_sync_job(
    job_id: str,
    school: Optional[str] = None,
    snapshot_date_override: Optional[str] = None,
):
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
        schools_processed=0,
        total_schools=0,
        attempted_at=started_at,
        started_at=started_at,
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
        db = get_db()
        try:
            sync_run = db.query(SyncRun).filter(SyncRun.job_id == job_id).first()
            if sync_run:
                hydrate_sync_run(sync_run, job)
                db.commit()
        finally:
            db.close()

        list_time = time.time()

        snapshot_date = snapshot_date_override or datetime.now(timezone.utc).strftime("%Y-%m-%d")
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
            db = get_db()
            try:
                sync_run = db.query(SyncRun).filter(SyncRun.job_id == job_id).first()
                if sync_run:
                    hydrate_sync_run(sync_run, job)
                    db.commit()
            finally:
                db.close()

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
                hydrate_sync_run(sync_run, job)
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
                hydrate_sync_run(sync_run, job)
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
    return payload


def hydrate_sync_run(sync_run: SyncRun, job: dict) -> None:
    """Copy in-memory job metadata onto the persisted sync-run row."""
    sync_run.status = job["status"]
    sync_run.snapshot_date = job.get("snapshotDate")
    sync_run.schools_processed = job.get("schoolsProcessed", 0)
    sync_run.total_schools = job.get("totalSchools", 0)
    sync_run.started_at = (
        datetime.fromisoformat(job["startedAt"]) if job.get("startedAt") else None
    )
    sync_run.finished_at = (
        datetime.fromisoformat(job["finishedAt"]) if job.get("finishedAt") else None
    )
    sync_run.start_date = job.get("startDate")
    sync_run.end_date = job.get("endDate")
    sync_run.date_count = job.get("dateCount")
    sync_run.errors_json = json.dumps(job.get("errors", []))
    sync_run.timing_json = json.dumps(job.get("timing")) if job.get("timing") else None
    sync_run.error_message = job.get("error") or (job.get("errors") or [None])[0]


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


def parse_snapshot_date(value: str, field_name: str) -> str:
    """Validate and normalize a YYYY-MM-DD snapshot date string."""
    try:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"{field_name} must be in YYYY-MM-DD format",
        ) from exc


def resolve_backfill_date_range(start_date: str, end_date: Optional[str]) -> tuple[str, str]:
    """Return an inclusive validated date range for history backfill."""
    normalized_start = parse_snapshot_date(start_date, "startDate")
    normalized_end = (
        parse_snapshot_date(end_date, "endDate")
        if end_date
        else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    )

    if normalized_start > normalized_end:
        raise HTTPException(status_code=422, detail="startDate must be on or before endDate")

    return normalized_start, normalized_end


def build_snapshot_dates(start_date: str, end_date: str) -> list[str]:
    """Return all snapshot dates in the inclusive range."""
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    snapshot_dates: list[str] = []

    while current <= end:
        snapshot_dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return snapshot_dates


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


# Suffixes in the school slug that map to a canonical SIS label.
# These are used as a fallback when the config API has no sisPlatform field
# and the integrationBroker value is the middleware name (e.g. "N2N") rather
# than the actual SIS.
_SLUG_SIS_MAP: dict[str, str] = {
    "workday": "Workday",
    "banner_ethos": "BannerEthos",
    "banner_sql": "BannerSQL",
    "banner_direct": "Banner",
    "banner": "Banner",
    "colleague_ethos": "ColleagueEthos",
    "colleague_direct": "Colleague",
    "colleague": "Colleague",
    "peoplesoft_direct": "PeopleSoftDirect",
    "peoplesoft": "PeopleSoft",
    "jenzabar": "Jenzabar",
    "powercampus": "PowerCampus",
    "campusnexus": "CampusNexusAnthology",
    "anthology": "CampusNexusAnthology",
    "homegrown": "Homegrown",
    "csv": "CSV",
}


def sis_from_slug(school: str) -> Optional[str]:
    """Derive a SIS label from the school slug by matching known suffix patterns."""
    slug = school.lower()
    # Try longest suffix match first so "banner_ethos" beats "banner"
    for suffix in sorted(_SLUG_SIS_MAP, key=len, reverse=True):
        if slug.endswith(suffix) or f"_{suffix}" in slug:
            return _SLUG_SIS_MAP[suffix]
    return None


def extract_sis_platform(payload) -> Optional[str]:
    """Extract the best SIS label from an integration configuration payload."""
    if not isinstance(payload, dict):
        return None

    # sisPlatform is the authoritative field — always prefer it.
    sis = payload.get("sisPlatform")
    if isinstance(sis, str) and sis.strip():
        return sis.strip()

    # integrationBroker is the middleware/transport, not the SIS.
    # "N2N" is a broker name and should never be surfaced as the SIS label.
    broker = payload.get("integrationBroker")
    if isinstance(broker, str) and broker.strip() and broker.strip().upper() != "N2N":
        return broker.strip()

    return None


async def fetch_school_sis_platform(school: str) -> Optional[str]:
    """Fetch the SIS platform for a school, falling back to slug-based inference."""
    config = await api_get(f"/api/v1/{school}/integration/configuration")
    platform = extract_sis_platform(config)
    if platform is None:
        platform = sis_from_slug(school)
    return platform


def calculate_nightly_merge_time(merge_entries: list[dict]) -> int:
    """Calculate the contiguous duration spanning all nightly merges in the provided entries.
    
    This merges overlapping execution intervals to prevent gaps between disconnected
    nightly runs (e.g. yesterday's run and today's run) from artificially inflating
    the total duration.
    """
    groups: dict[str, list] = {}
    fallback_intervals = []
    
    for report in merge_entries:
        if str(report.get("scheduleType", "")).lower() == "nightly":
            start_val = report.get("timestampStart") or report.get("startedAt") or report.get("timestampEnd")
            end_val = report.get("timestampEnd") or report.get("completedAt")
            
            if start_val and end_val and isinstance(start_val, (int, float)) and isinstance(end_val, (int, float)):
                if start_val <= end_val:
                    group_id = report.get("groupId")
                    if group_id:
                        if group_id not in groups:
                            groups[group_id] = {"starts": [], "ends": []}
                        groups[group_id]["starts"].append(start_val)
                        groups[group_id]["ends"].append(end_val)
                    else:
                        fallback_intervals.append([start_val, end_val])
    
    total_duration = 0
    
    # 1. Calculate duration for explicitly grouped merge events
    for g in groups.values():
        total_duration += max(0, int(max(g["ends"]) - min(g["starts"])))
        
    # 2. Handle legacy reports missing a groupId using the 2-hour sliding window tolerance
    if fallback_intervals:
        fallback_intervals.sort(key=lambda x: x[0])
        merged = [fallback_intervals[0]]
        GROUPING_TOLERANCE_MS = 2 * 60 * 60 * 1000
        
        for current in fallback_intervals[1:]:
            prev = merged[-1]
            if current[0] <= prev[1] + GROUPING_TOLERANCE_MS:
                prev[1] = max(prev[1], current[1])
            else:
                merged.append(current)
                
        total_duration += int(sum(end - start for start, end in merged))
        
    return max(0, total_duration)


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
    nightly_merge_time_ms = calculate_nightly_merge_time(merge_entries)

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
                "mergeTimeMs": nightly_merge_time_ms,
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

        # Compute nightly merge time from merge reports history
        nightly_merge_time = calculate_nightly_merge_time(merge_reports)

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
                    "mergeTimeMs": nightly_merge_time,
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


async def run_history_backfill_job(
    job_id: str,
    school: Optional[str],
    start_date: str,
    end_date: str,
):
    """Backfill historical daily snapshots for one school or all schools."""
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
        schools_processed=0,
        total_schools=0,
        attempted_at=started_at,
        started_at=started_at,
        start_date=job.get("startDate"),
        end_date=job.get("endDate"),
        date_count=job.get("dateCount"),
    )
    db.add(sync_run)
    db.commit()
    db.refresh(sync_run)
    db.close()

    try:
        schools_resp = await list_schools()
        schools = select_schools_for_sync(schools_resp["schools"], school)
        list_time = time.time()
        snapshot_dates = build_snapshot_dates(start_date, end_date)
        total_work_units = len(schools) * len(snapshot_dates)
        job["totalSchools"] = total_work_units
        db = get_db()
        try:
            sync_run = db.query(SyncRun).filter(SyncRun.job_id == job_id).first()
            if sync_run:
                hydrate_sync_run(sync_run, job)
                db.commit()
        finally:
            db.close()

        errors: list[str] = []
        processed_units = 0
        db = get_db()
        try:
            for school_info in schools:
                snapshots: list[dict] = []
                for snapshot_date in snapshot_dates:
                    result = await fetch_school_health_for_date(
                        school=school_info["school"],
                        display_name=school_info.get("displayName", school_info["school"]),
                        products=school_info.get("products", []),
                        snapshot_date=snapshot_date,
                    )
                    errors.extend(result.pop("_syncErrors", []))
                    snapshots.append(result)
                    processed_units += 1
                    job["schoolsProcessed"] = processed_units
                    job["errors"] = errors[-20:]
                    sync_run = db.query(SyncRun).filter(SyncRun.job_id == job_id).first()
                    if sync_run:
                        hydrate_sync_run(sync_run, job)
                        db.commit()

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
                    raise RuntimeError(
                        f"Database write error for {school_info['school']}: {e}"
                    ) from e
        finally:
            db.close()

        persist_time = time.time()
        fetch_time = persist_time
        job.update(
            {
                "status": "completed",
                "snapshotDate": snapshot_dates[-1],
                "schoolsProcessed": processed_units,
                "totalSchools": total_work_units,
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
                hydrate_sync_run(sync_run, job)
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
                hydrate_sync_run(sync_run, job)
                db.commit()
        finally:
            db.close()
    finally:
        if _active_sync_job_id == job_id:
            _active_sync_job_id = None
