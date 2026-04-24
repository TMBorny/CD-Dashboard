"""API routes for the Client Health Dashboard backend.

All routes persist data to SQLite via SQLAlchemy models.
The /api/client-health/sync endpoint fetches live data from Coursedog
and stores it in the database.
"""

import asyncio
import hashlib
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from uuid import uuid4
from typing import Any, Literal, Optional

from fastapi import APIRouter, Query, HTTPException, Response
from pydantic import BaseModel
from sqlalchemy import or_

from app.db import api_get, authenticate_api, get_db, get_error_analysis_export_path
from app.models import (
    BackfillWorkUnit,
    ErrorAnalysisDetail,
    ErrorAnalysisGroup,
    ExcludedSchool,
    SchedulerSettings,
    SchoolSnapshot,
    SyncRun,
    serialize_datetime,
)
from app.school_names import resolve_school_display_name

router = APIRouter(prefix="/api")
logger = logging.getLogger(__name__)
_sync_jobs: dict[str, dict] = {}
_active_sync_job_id: Optional[str] = None
_scheduled_sync_task: Optional[asyncio.Task] = None
_scheduled_sync_stop_event: Optional[asyncio.Event] = None
_backfill_recovery_task: Optional[asyncio.Task] = None
_last_job_trigger_monotonic: float = 0.0
EXCLUDED_SCHOOL_TERMS = ("demo", "test", "sandbox", "baseline")
JOB_TRIGGER_COOLDOWN_SECONDS = 15
BACKFILL_STALL_THRESHOLD_SECONDS = 10 * 60
BACKFILL_AUTO_RESUME_POLL_SECONDS = 60
FAILED_UNITS_SAMPLE_LIMIT = 10
MERGE_ERRORS_PAGE_SIZE = 250
MERGE_ERRORS_MAX_PAGES = 200
ERROR_ANALYSIS_SAMPLE_LIMIT = 3
SIGNATURE_EXPLORER_UNKNOWN_BUCKET = "__unknown__"
HALTED_CHANGE_THRESHOLD_STATUS = "Halted: Change Threshold Exceeded"
HALTED_STAGE_NAME = "Sync Coursedog Updates with SIS"
CHANGE_THRESHOLD_LIMIT = 10
ERROR_SIGNATURE_VERSION_V1 = "v1"
ERROR_SIGNATURE_VERSION_V2 = "v2"
ERROR_SIGNATURE_STRATEGY_CODE_TEMPLATE = "code_template"
ERROR_SIGNATURE_STRATEGY_TEMPLATE_FALLBACK = "template_fallback"
ERROR_SIGNATURE_CODE_TEMPLATE_CONFIDENCE = 0.92
ERROR_SIGNATURE_TEMPLATE_FALLBACK_CONFIDENCE = 0.58
GENERIC_ERROR_CODE_VALUES = {
    "400",
    "401",
    "403",
    "404",
    "409",
    "422",
    "429",
    "500",
    "502",
    "503",
    "504",
    "error",
    "errors",
    "failed",
    "failure",
    "unknown",
    "unknown_error",
    "internal_error",
    "validation",
    "warning",
}


class SchoolExclusionPayload(BaseModel):
    school: str


SCHEDULE_TIMEZONE = ZoneInfo("America/New_York")
DEFAULT_SCHEDULED_SYNC_TIME = "07:30"


class SchedulerSettingsPayload(BaseModel):
    syncEnabled: bool
    syncTime: str


def normalize_utc_datetime(value: Optional[datetime]) -> Optional[datetime]:
    """Treat naive SQLite datetimes as UTC so arithmetic stays safe."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def snapshot_has_measured_merge_errors(snapshot_date: str, created_at: Optional[datetime]) -> bool:
    """Return True when the snapshot was captured on the same New York calendar day."""
    normalized_created_at = normalize_utc_datetime(created_at)
    if normalized_created_at is None:
        return False
    return normalized_created_at.astimezone(SCHEDULE_TIMEZONE).strftime("%Y-%m-%d") == snapshot_date


def enforce_job_trigger_cooldown(action: str) -> None:
    """Prevent repeated sync/backfill triggers from overwhelming the service."""
    global _last_job_trigger_monotonic

    now_monotonic = time.monotonic()
    elapsed = now_monotonic - _last_job_trigger_monotonic
    if _last_job_trigger_monotonic and elapsed < JOB_TRIGGER_COOLDOWN_SECONDS:
        retry_after = max(1, int(JOB_TRIGGER_COOLDOWN_SECONDS - elapsed))
        logger.warning("Rejected %s trigger during cooldown; retry after %ss", action, retry_after)
        raise HTTPException(
            status_code=429,
            detail={
                "code": "job_trigger_rate_limited",
                "message": "A sync or backfill was triggered recently. Please wait before trying again.",
                "retryAfterSeconds": retry_after,
            },
        )

    _last_job_trigger_monotonic = now_monotonic


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


def get_new_york_year_start_date(now: Optional[datetime] = None) -> str:
    """Return January 1 of the current New York calendar year."""
    local_now = current_new_york_time(now)
    return f"{local_now.year}-01-01"


def _school_has_year_to_date_activity_from_snapshots(
    db,
    school: str,
    year_start_date: str,
) -> dict[str, bool]:
    """Check whether cached snapshots already prove a school is active this year."""
    snapshots = (
        db.query(SchoolSnapshot)
        .filter(SchoolSnapshot.school == school)
        .filter(SchoolSnapshot.snapshot_date >= year_start_date)
        .all()
    )

    signals = {
        "activeUsers": False,
        "realtimeMerge": False,
        "nightlyMerge": False,
    }
    for snapshot in snapshots:
        if (snapshot.active_users_24h or 0) > 0:
            signals["activeUsers"] = True
        if (snapshot.realtime_total or 0) > 0:
            signals["realtimeMerge"] = True
        if (snapshot.nightly_total or 0) > 0:
            signals["nightlyMerge"] = True

    return signals


async def _school_has_year_to_date_activity_from_coursedog(
    school: str,
    year_start_date: str,
    now: Optional[datetime] = None,
) -> dict[str, bool]:
    """Check live Coursedog activity when local snapshots are incomplete."""
    local_now = current_new_york_time(now)
    year_start = datetime.strptime(year_start_date, "%Y-%m-%d").replace(tzinfo=SCHEDULE_TIMEZONE)

    signals = {
        "activeUsers": False,
        "realtimeMerge": False,
        "nightlyMerge": False,
    }

    try:
        activity_payload = await api_get(
            f"/api/v1/{school}/userActivity",
            params={
                "after": str(int(year_start.astimezone(timezone.utc).timestamp() * 1000)),
                "before": str(int(local_now.astimezone(timezone.utc).timestamp() * 1000)),
            },
        )
        signals["activeUsers"] = bool(extract_unique_activity_users(activity_payload))
    except Exception as exc:
        logger.warning("Live active-user lookup failed for %s: %s", school, exc)

    try:
        page = 0
        page_size = 500
        while True:
            merge_history_payload = await api_get(
                f"/api/v1/int/{school}/integrations-hub/merge-history",
                params={
                    "page": str(page),
                    "size": str(page_size),
                    "dateFrom": year_start.astimezone(timezone.utc).isoformat(),
                    "dateTo": local_now.astimezone(timezone.utc).isoformat(),
                },
            )
            merge_entries = extract_merge_history_entries(merge_history_payload)
            if not merge_entries:
                break

            for entry in merge_entries:
                schedule_type = str(entry.get("scheduleType", "")).lower()
                if schedule_type == "realtime":
                    signals["realtimeMerge"] = True
                elif schedule_type == "nightly":
                    signals["nightlyMerge"] = True

                if signals["realtimeMerge"] and signals["nightlyMerge"]:
                    break

            if signals["realtimeMerge"] and signals["nightlyMerge"]:
                break
            if len(merge_entries) < page_size:
                break
            page += 1
    except Exception as exc:
        logger.warning("Live merge-history lookup failed for %s: %s", school, exc)

    return signals


async def evaluate_manual_exclusion_reinstatement(
    db,
    school: str,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Return whether a manually excluded school should be reinstated."""
    year_start_date = get_new_york_year_start_date(now)
    local_signals = _school_has_year_to_date_activity_from_snapshots(db, school, year_start_date)
    if any(local_signals.values()):
        return {
            "school": school,
            "yearStartDate": year_start_date,
            "qualifies": True,
            "source": "local",
            "signals": local_signals,
        }

    live_signals = await _school_has_year_to_date_activity_from_coursedog(school, year_start_date, now=now)
    qualifies = any(live_signals.values())
    return {
        "school": school,
        "yearStartDate": year_start_date,
        "qualifies": qualifies,
        "source": "live" if qualifies or any(live_signals.values()) else "live-empty",
        "signals": live_signals,
    }


async def reconcile_manual_exclusions(
    db,
    *,
    apply_changes: bool = False,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Evaluate the manual exclusion list and optionally remove reinstated schools."""
    excluded_rows = (
        db.query(ExcludedSchool)
        .order_by(ExcludedSchool.school.asc())
        .all()
    )
    results: list[dict[str, Any]] = []
    reinstated: list[str] = []
    retained: list[str] = []

    for row in excluded_rows:
        evaluation = await evaluate_manual_exclusion_reinstatement(db, row.school, now=now)
        results.append(evaluation)
        if evaluation["qualifies"]:
            reinstated.append(row.school)
            if apply_changes:
                db.delete(row)
        else:
            retained.append(row.school)

    if apply_changes:
        db.commit()

    return {
        "yearStartDate": get_new_york_year_start_date(now),
        "evaluatedCount": len(excluded_rows),
        "reinstatedSchools": reinstated,
        "retainedSchools": retained,
        "results": results,
    }


def normalize_school_catalog(products_data, display_names_data=None) -> list[dict]:
    """Normalize the admin schools response into a flat school list without exclusions."""
    schools: list[dict] = []
    display_name_map: dict[str, str] = {}
    if isinstance(display_names_data, list):
        for item in display_names_data:
            school = item.get("_id", item.get("id", item.get("school", "")))
            display_name = item.get("displayName") or item.get("fullName") or school
            if school:
                display_name_map[school] = display_name
    elif isinstance(display_names_data, dict):
        for key, val in display_names_data.items():
            if isinstance(val, dict):
                display_name_map[key] = val.get("displayName") or val.get("fullName") or key
            elif isinstance(val, str):
                display_name_map[key] = val

    if isinstance(products_data, list):
        for item in products_data:
            school = item.get("_id", item.get("school", ""))
            schools.append({
                "school": school,
                "displayName": resolve_school_display_name(
                    school,
                    verified_display_name=display_name_map.get(school),
                    fallback_display_name=item.get("displayName", item.get("_id", "")),
                ),
                "products": item.get("products", []),
            })
    elif isinstance(products_data, dict):
        for key, val in products_data.items():
            if isinstance(val, dict):
                schools.append({
                    "school": key,
                    "displayName": resolve_school_display_name(
                        key,
                        verified_display_name=display_name_map.get(key),
                        fallback_display_name=val.get("displayName", key),
                    ),
                    "products": val.get("products", []),
                })
            else:
                schools.append({
                    "school": key,
                    "displayName": resolve_school_display_name(
                        key,
                        verified_display_name=display_name_map.get(key),
                        fallback_display_name=key,
                    ),
                    "products": [],
                })
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


def write_error_analysis_export(db) -> Path:
    """Write all captured error-analysis groups to a standalone JSON file."""
    export_path = get_error_analysis_export_path()
    export_path.parent.mkdir(parents=True, exist_ok=True)

    groups = (
        db.query(ErrorAnalysisGroup)
        .order_by(
            ErrorAnalysisGroup.snapshot_date.desc(),
            ErrorAnalysisGroup.school.asc(),
            ErrorAnalysisGroup.signature_version.asc(),
            ErrorAnalysisGroup.signature_key.asc(),
        )
        .all()
    )
    payload = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "path": str(export_path),
        "totalGroups": len(groups),
        "totalErrorInstances": sum(group.count for group in groups),
        "groups": [group.to_dict() for group in groups],
    }

    temp_path = export_path.with_suffix(f"{export_path.suffix}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(export_path)
    return export_path


def apply_error_analysis_detail_filters(
    query,
    *,
    days: Optional[int],
    school: Optional[str],
    sis_platform: Optional[str],
    entity_type: Optional[str],
    signature: Optional[str],
    search: Optional[str],
):
    """Apply the shared detail-table filters to a SQLAlchemy query."""
    if days is not None:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
        query = query.filter(ErrorAnalysisDetail.snapshot_date >= cutoff)
    if school:
        query = query.filter(ErrorAnalysisDetail.school == school)
    if sis_platform:
        query = query.filter(ErrorAnalysisDetail.sis_platform == sis_platform)
    if entity_type:
        query = query.filter(ErrorAnalysisDetail.entity_type == entity_type)
    if signature:
        query = query.filter(ErrorAnalysisDetail.signature_key == signature)
    if search:
        pattern = f"%{search.strip()}%"
        query = query.filter(
            or_(
                ErrorAnalysisDetail.full_error_text.ilike(pattern),
                ErrorAnalysisDetail.signature_label.ilike(pattern),
                ErrorAnalysisDetail.normalized_message.ilike(pattern),
                ErrorAnalysisDetail.message_template.ilike(pattern),
                ErrorAnalysisDetail.entity_type.ilike(pattern),
                ErrorAnalysisDetail.error_code.ilike(pattern),
                ErrorAnalysisDetail.canonical_error_code.ilike(pattern),
                ErrorAnalysisDetail.operation_name.ilike(pattern),
                ErrorAnalysisDetail.school.ilike(pattern),
                ErrorAnalysisDetail.display_name.ilike(pattern),
                ErrorAnalysisDetail.sis_platform.ilike(pattern),
                ErrorAnalysisDetail.entity_display_name.ilike(pattern),
                ErrorAnalysisDetail.merge_report_id.ilike(pattern),
            )
        )
    return query


def resolve_latest_snapshot_date(query, snapshot_column) -> Optional[str]:
    """Return the newest snapshot date available for the current filtered query."""
    latest_row = query.order_by(snapshot_column.desc()).with_entities(snapshot_column).first()
    return latest_row[0] if latest_row else None


def get_primary_term_code(row: ErrorAnalysisDetail) -> Optional[str]:
    """Return one stable term code per detail row for signature drilldowns."""
    try:
        term_codes = json.loads(row.term_codes_json) if row.term_codes_json else []
    except json.JSONDecodeError:
        return None

    if not isinstance(term_codes, list):
        return None

    for value in term_codes:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def get_signature_explorer_bucket_value(
    row: ErrorAnalysisDetail,
    group_by: Literal["sis", "school", "term"],
) -> Optional[str]:
    """Return the bucket key for one detail row and grouping mode."""
    if group_by == "sis":
        return row.sis_platform.strip() if isinstance(row.sis_platform, str) and row.sis_platform.strip() else None
    if group_by == "school":
        return row.school.strip() if isinstance(row.school, str) and row.school.strip() else None
    return get_primary_term_code(row)


def get_signature_explorer_bucket_label(
    row: ErrorAnalysisDetail,
    group_by: Literal["sis", "school", "term"],
) -> str:
    """Return the display label for one drilldown bucket."""
    if group_by == "sis":
        return row.sis_platform.strip() if isinstance(row.sis_platform, str) and row.sis_platform.strip() else "Unknown"
    if group_by == "school":
        return row.display_name or row.school or "Unknown"
    return get_primary_term_code(row) or "Unknown"


def build_signature_explorer_breakdowns(rows: list[ErrorAnalysisDetail]) -> dict[str, list[dict]]:
    """Build SIS, school, and term drilldowns for a set of signature rows."""
    breakdowns: dict[str, dict[str, dict[str, Any]]] = {
        "sis": {},
        "school": {},
        "term": {},
    }

    for group_by in ("sis", "school", "term"):
        for row in rows:
            key = get_signature_explorer_bucket_value(row, group_by) or SIGNATURE_EXPLORER_UNKNOWN_BUCKET
            label = get_signature_explorer_bucket_label(row, group_by)
            existing = breakdowns[group_by].setdefault(
                key,
                {
                    "key": key,
                    "label": label,
                    "count": 0,
                },
            )
            existing["count"] += 1

    total = len(rows) or 1
    return {
        group_by: sorted(
            [
                {
                    **value,
                    "share": value["count"] / total,
                }
                for value in bucket_map.values()
            ],
            key=lambda item: (-item["count"], item["label"]),
        )
        for group_by, bucket_map in breakdowns.items()
    }


def build_signature_explorer_school_counts(rows: list[ErrorAnalysisDetail]) -> list[dict[str, Any]]:
    """Build distinct school counts for one grouped error message."""
    schools: dict[str, dict[str, Any]] = {}

    for row in rows:
        school_key = row.school.strip() if isinstance(row.school, str) and row.school.strip() else SIGNATURE_EXPLORER_UNKNOWN_BUCKET
        label = row.display_name or row.school or "Unknown"
        existing = schools.setdefault(
            school_key,
            {
                "school": school_key,
                "label": label,
                "count": 0,
            },
        )
        existing["count"] += 1

    return sorted(
        schools.values(),
        key=lambda item: (item["label"].lower(), item["school"]),
    )


def build_signature_explorer_grouped_rows(rows: list[ErrorAnalysisDetail]) -> list[dict[str, Any]]:
    """Collapse identical full error texts into grouped rows using the newest row as the representative sample."""
    grouped_rows: dict[str, dict[str, Any]] = {}

    sorted_rows = sorted(
        rows,
        key=lambda row: (
            row.snapshot_date or "",
            row.id or 0,
        ),
        reverse=True,
    )

    for row in sorted_rows:
        group_key = row.full_error_text
        existing = grouped_rows.get(group_key)
        if existing:
            existing["sourceRows"].append(row)
            continue

        grouped_rows[group_key] = {
            "key": group_key,
            "representative": row,
            "sourceRows": [row],
        }

    return [
        {
            **group["representative"].to_dict(),
            "key": group["key"],
            "instanceCount": len(group["sourceRows"]),
            "schools": build_signature_explorer_school_counts(group["sourceRows"]),
        }
        for group in grouped_rows.values()
    ]


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
        products_data, display_names_data = await asyncio.gather(
            api_get("/api/v1/admin/schools/products"),
            api_get("/api/v1/admin/schools/displayNames"),
        )
        all_schools = normalize_school_catalog(products_data, display_names_data)
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
    days: Optional[int] = Query(default=None, ge=1),
    school: Optional[str] = Query(default=None),
):
    """Get historical snapshots from the database."""
    db = get_db()
    try:
        query = exclude_demo_school_snapshots(db.query(SchoolSnapshot))
        if days is not None:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
            query = query.filter(SchoolSnapshot.snapshot_date >= cutoff)
        if school:
            query = query.filter(SchoolSnapshot.school == school)

        snapshots = query.order_by(SchoolSnapshot.snapshot_date).all()
        serialized_snapshots: list[dict] = []
        for snapshot in snapshots:
            payload = snapshot.to_dict()
            if not snapshot_has_measured_merge_errors(snapshot.snapshot_date, snapshot.created_at):
                payload["mergeErrorsCount"] = None
            serialized_snapshots.append(payload)

        return {"snapshots": serialized_snapshots}
    finally:
        db.close()


@router.get("/error-analysis")
async def get_error_analysis(
    days: Optional[int] = Query(default=None, ge=1),
    school: Optional[str] = Query(default=None),
    sis_platform: Optional[str] = Query(default=None, alias="sisPlatform"),
    latest_only: bool = Query(default=False, alias="latestOnly"),
):
    """Return aggregate error-analysis data from captured open merge-error groups."""
    db = get_db()
    try:
        latest_snapshot = (
            db.query(SchoolSnapshot.snapshot_date)
            .order_by(SchoolSnapshot.snapshot_date.desc())
            .first()
        )
        school_options: list[dict] = []
        sis_values: set[str] = set()
        if latest_snapshot:
            latest_rows = (
                exclude_demo_school_snapshots(db.query(SchoolSnapshot))
                .filter(SchoolSnapshot.snapshot_date == latest_snapshot[0])
                .order_by(SchoolSnapshot.display_name, SchoolSnapshot.school)
                .all()
            )
            school_options = [
                {
                    "value": row.school,
                    "label": row.display_name or row.school,
                    "sisPlatform": row.sis_platform,
                }
                for row in latest_rows
            ]
            for row in latest_rows:
                if row.sis_platform:
                    sis_values.add(row.sis_platform)

        history_start = (
            db.query(ErrorAnalysisGroup.snapshot_date)
            .order_by(ErrorAnalysisGroup.snapshot_date.asc())
            .first()
        )
        last_capture = (
            db.query(ErrorAnalysisGroup.created_at)
            .order_by(ErrorAnalysisGroup.created_at.desc())
            .first()
        )

        query = db.query(ErrorAnalysisGroup)
        if days is not None:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
            query = query.filter(ErrorAnalysisGroup.snapshot_date >= cutoff)
        if school:
            query = query.filter(ErrorAnalysisGroup.school == school)
        if sis_platform:
            query = query.filter(ErrorAnalysisGroup.sis_platform == sis_platform)
        resolved_snapshot_date = resolve_latest_snapshot_date(query, ErrorAnalysisGroup.snapshot_date) if latest_only else None
        if resolved_snapshot_date:
            query = query.filter(ErrorAnalysisGroup.snapshot_date == resolved_snapshot_date)

        groups = query.order_by(
            ErrorAnalysisGroup.snapshot_date.asc(),
            ErrorAnalysisGroup.school.asc(),
            ErrorAnalysisGroup.signature_version.asc(),
            ErrorAnalysisGroup.signature_key.asc(),
        ).all()

        response = {
            "metadata": {
                "historyStartsOn": history_start[0] if history_start else None,
                "lastCapturedAt": serialize_datetime(last_capture[0]) if last_capture else None,
                "appliedFilters": {
                    "days": days,
                    "school": school,
                    "sisPlatform": sis_platform,
                    "latestOnly": latest_only,
                },
                "hasCapturedData": bool(history_start),
                "filteredGroupCount": len(groups),
                "resolvedSnapshotDate": resolved_snapshot_date,
            },
            "filterOptions": {
                "schools": school_options,
                "sisPlatforms": sorted(sis_values),
            },
            "summary": {
                "totalGroupedErrors": len(groups),
                "totalErrorInstances": 0,
                "distinctSignatures": 0,
                "affectedSchools": 0,
                "affectedSisPlatforms": 0,
                "captureDays": 0,
                "latestSnapshotDate": None,
            },
            "trends": [],
            "signatures": [],
            "schoolBreakdowns": [],
            "sisBreakdowns": [],
        }

        if not groups:
            return response

        trends: dict[str, dict] = {}
        signatures: dict[str, dict] = {}
        school_breakdowns: dict[str, dict] = {}
        sis_breakdowns: dict[str, dict] = {}
        affected_schools: set[str] = set()
        affected_sis: set[str] = set()
        capture_days: set[str] = set()
        latest_snapshot_total_error_instances = 0

        def get_dominant_entry(counter: dict[str, int]) -> Optional[str]:
            if not counter:
                return None
            return max(counter.items(), key=lambda item: (item[1], item[0]))[0]

        for group in groups:
            sample_errors = json.loads(group.sample_errors_json) if group.sample_errors_json else []
            term_codes = json.loads(group.term_codes_json) if group.term_codes_json else []
            merge_report_reference = extract_merge_report_reference(sample_errors)
            signature_identity = get_row_signature_identity(group)
            signature_version = get_signature_version(group.signature_version)
            resolution_hint = build_resolution_hint(
                group.normalized_message,
                group.entity_type,
                group.canonical_error_code or group.error_code,
            )
            sis_label = group.sis_platform or "Unknown"
            affected_schools.add(group.school)
            affected_sis.add(sis_label)
            capture_days.add(group.snapshot_date)

            trend_row = trends.setdefault(
                group.snapshot_date,
                {
                    "snapshotDate": group.snapshot_date,
                    "totalErrors": 0,
                    "distinctSignatures": set(),
                    "affectedSchools": set(),
                },
            )
            trend_row["totalErrors"] += group.count
            trend_row["distinctSignatures"].add(signature_identity)
            trend_row["affectedSchools"].add(group.school)

            signature_row = signatures.setdefault(
                signature_identity,
                {
                    "signatureKey": group.signature_key,
                    "legacySignatureKey": group.legacy_signature_key,
                    "signatureVersion": signature_version,
                    "signatureStrategy": group.signature_strategy,
                    "signatureConfidence": (
                        None if group.signature_confidence is None else group.signature_confidence / 100
                    ),
                    "entityType": group.entity_type,
                    "errorCode": group.error_code,
                    "canonicalErrorCode": group.canonical_error_code,
                    "operationName": group.operation_name,
                    "signatureLabel": build_error_signature_label(
                        group.entity_type,
                        group.canonical_error_code or group.error_code,
                        group.normalized_message,
                    ),
                    "messageTemplate": group.message_template or group.normalized_message,
                    "normalizedMessage": group.normalized_message,
                    "sampleMessage": group.sample_message,
                    "totalCount": 0,
                    "affectedSchools": set(),
                    "affectedSisPlatforms": set(),
                    "firstSeen": group.snapshot_date,
                    "lastSeen": group.snapshot_date,
                    "recurrenceDays": set(),
                    "countsBySchool": {},
                    "countsBySis": {},
                    "schoolLabels": {},
                    "latestMergeReportBySchool": {},
                    "exampleMergeReports": [],
                    "sampleErrors": sample_errors,
                    "termCodes": set(term_codes),
                    "resolutionHint": resolution_hint,
                    "latestMergeReport": None,
                },
            )
            signature_row["totalCount"] += group.count
            signature_row["affectedSchools"].add(group.school)
            signature_row["affectedSisPlatforms"].add(sis_label)
            signature_row["firstSeen"] = min(signature_row["firstSeen"], group.snapshot_date)
            signature_row["lastSeen"] = max(signature_row["lastSeen"], group.snapshot_date)
            signature_row["recurrenceDays"].add(group.snapshot_date)
            signature_row["countsBySchool"][group.school] = signature_row["countsBySchool"].get(group.school, 0) + group.count
            signature_row["countsBySis"][sis_label] = signature_row["countsBySis"].get(sis_label, 0) + group.count
            signature_row["schoolLabels"][group.school] = group.display_name or group.school
            signature_row["termCodes"].update(term_codes)
            if signature_row["legacySignatureKey"] is None and group.legacy_signature_key:
                signature_row["legacySignatureKey"] = group.legacy_signature_key
            if signature_row["signatureStrategy"] is None and group.signature_strategy:
                signature_row["signatureStrategy"] = group.signature_strategy
            if signature_row["signatureConfidence"] is None and group.signature_confidence is not None:
                signature_row["signatureConfidence"] = group.signature_confidence / 100
            if signature_row["canonicalErrorCode"] is None and group.canonical_error_code:
                signature_row["canonicalErrorCode"] = group.canonical_error_code
            if signature_row["operationName"] is None and group.operation_name:
                signature_row["operationName"] = group.operation_name
            if merge_report_reference and (
                signature_row["latestMergeReport"] is None
                or group.snapshot_date >= signature_row["latestMergeReport"]["snapshotDate"]
            ):
                signature_row["latestMergeReport"] = {
                    **merge_report_reference,
                    "school": group.school,
                    "snapshotDate": group.snapshot_date,
                }
            if merge_report_reference:
                existing_school_report = signature_row["latestMergeReportBySchool"].get(group.school)
                if existing_school_report is None or group.snapshot_date >= existing_school_report["snapshotDate"]:
                    signature_row["latestMergeReportBySchool"][group.school] = {
                        **merge_report_reference,
                        "school": group.school,
                        "snapshotDate": group.snapshot_date,
                    }
                    signature_row["exampleMergeReports"] = sorted(
                        signature_row["latestMergeReportBySchool"].values(),
                        key=lambda item: (item["snapshotDate"], item["school"], item["mergeReportId"]),
                        reverse=True,
                    )[:5]

            school_row = school_breakdowns.setdefault(
                group.school,
                {
                    "key": group.school,
                    "label": group.display_name or group.school,
                    "sisPlatform": group.sis_platform,
                    "totalErrors": 0,
                    "distinctSignatures": set(),
                    "countsBySignature": {},
                    "resolutionBuckets": {},
                    "lastSeen": group.snapshot_date,
                    "recurrenceDays": set(),
                    "latestMergeReport": None,
                },
            )
            school_row["totalErrors"] += group.count
            school_row["distinctSignatures"].add(signature_identity)
            school_row["countsBySignature"][signature_identity] = school_row["countsBySignature"].get(signature_identity, 0) + group.count
            school_row["resolutionBuckets"][resolution_hint["bucket"]] = school_row["resolutionBuckets"].get(resolution_hint["bucket"], 0) + group.count
            school_row["lastSeen"] = max(school_row["lastSeen"], group.snapshot_date)
            school_row["recurrenceDays"].add(group.snapshot_date)
            if merge_report_reference and (
                school_row["latestMergeReport"] is None
                or group.snapshot_date >= school_row["latestMergeReport"]["snapshotDate"]
            ):
                school_row["latestMergeReport"] = {
                    **merge_report_reference,
                    "school": group.school,
                    "snapshotDate": group.snapshot_date,
                }

            sis_row = sis_breakdowns.setdefault(
                sis_label,
                {
                    "key": sis_label,
                    "label": sis_label,
                    "schoolCountSet": set(),
                    "totalErrors": 0,
                    "distinctSignatures": set(),
                    "countsBySignature": {},
                    "resolutionBuckets": {},
                    "lastSeen": group.snapshot_date,
                },
            )
            sis_row["schoolCountSet"].add(group.school)
            sis_row["totalErrors"] += group.count
            sis_row["distinctSignatures"].add(signature_identity)
            sis_row["countsBySignature"][signature_identity] = sis_row["countsBySignature"].get(signature_identity, 0) + group.count
            sis_row["resolutionBuckets"][resolution_hint["bucket"]] = sis_row["resolutionBuckets"].get(resolution_hint["bucket"], 0) + group.count
            sis_row["lastSeen"] = max(sis_row["lastSeen"], group.snapshot_date)

        serialized_trends = [
            {
                "snapshotDate": trend["snapshotDate"],
                "totalErrors": trend["totalErrors"],
                "distinctSignatures": len(trend["distinctSignatures"]),
                "affectedSchools": len(trend["affectedSchools"]),
            }
            for trend in trends.values()
        ]
        serialized_trends.sort(key=lambda item: item["snapshotDate"])

        serialized_signatures = []
        for signature in signatures.values():
            dominant_school = get_dominant_entry(signature["countsBySchool"])
            dominant_sis = get_dominant_entry(signature["countsBySis"])
            dominant_school_merge_report = (
                signature["latestMergeReportBySchool"].get(dominant_school)
                if dominant_school
                else None
            )
            serialized_signatures.append(
                {
                    "signatureKey": signature["signatureKey"],
                    "legacySignatureKey": signature["legacySignatureKey"],
                    "signatureVersion": signature["signatureVersion"],
                    "signatureStrategy": signature["signatureStrategy"],
                    "signatureConfidence": signature["signatureConfidence"],
                    "entityType": signature["entityType"],
                    "errorCode": signature["errorCode"],
                    "canonicalErrorCode": signature["canonicalErrorCode"],
                    "operationName": signature["operationName"],
                    "signatureLabel": signature["signatureLabel"],
                    "messageTemplate": signature["messageTemplate"],
                    "normalizedMessage": signature["normalizedMessage"],
                    "sampleMessage": signature["sampleMessage"],
                    "totalCount": signature["totalCount"],
                    "affectedSchools": len(signature["affectedSchools"]),
                    "affectedSisPlatforms": len(signature["affectedSisPlatforms"]),
                    "firstSeen": signature["firstSeen"],
                    "lastSeen": signature["lastSeen"],
                    "recurrenceDays": len(signature["recurrenceDays"]),
                    "dominantSchool": dominant_school,
                    "dominantSisPlatform": dominant_sis,
                    "sampleErrors": signature["sampleErrors"],
                    "termCodes": sorted(signature["termCodes"]),
                    "resolutionHint": signature["resolutionHint"],
                    "latestMergeReport": signature["latestMergeReport"],
                    "dominantSchoolMergeReport": dominant_school_merge_report,
                    "impactedSchools": [
                        {
                            "school": school_key,
                            "label": signature["schoolLabels"].get(school_key, school_key),
                            "count": count,
                        }
                        for school_key, count in sorted(
                            signature["countsBySchool"].items(),
                            key=lambda item: (-item[1], item[0]),
                        )
                    ],
                    "exampleMergeReports": signature["exampleMergeReports"],
                }
            )
        serialized_signatures.sort(
            key=lambda item: (-item["totalCount"], -item["recurrenceDays"], item["sampleMessage"])
        )

        serialized_school_breakdowns = []
        for school_row in school_breakdowns.values():
            dominant_signature_key = get_dominant_entry(school_row["countsBySignature"])
            dominant_signature = signatures.get(dominant_signature_key) if dominant_signature_key else None
            dominant_bucket = get_dominant_entry(school_row["resolutionBuckets"])
            serialized_school_breakdowns.append(
                {
                    "key": school_row["key"],
                    "label": school_row["label"],
                    "sisPlatform": school_row["sisPlatform"],
                    "totalErrors": school_row["totalErrors"],
                    "distinctSignatures": len(school_row["distinctSignatures"]),
                    "dominantSignature": dominant_signature["signatureLabel"] if dominant_signature else None,
                    "lastSeen": school_row["lastSeen"],
                    "recurrenceDays": len(school_row["recurrenceDays"]),
                    "likelyNextStep": dominant_signature["resolutionHint"]["action"] if dominant_signature else None,
                    "topResolutionTheme": dominant_bucket,
                    "resolutionBuckets": school_row["resolutionBuckets"],
                    "latestMergeReport": school_row["latestMergeReport"],
                }
            )
        serialized_school_breakdowns.sort(key=lambda item: (-item["totalErrors"], item["label"]))

        serialized_sis_breakdowns = []
        for sis_row in sis_breakdowns.values():
            dominant_signature_key = get_dominant_entry(sis_row["countsBySignature"])
            dominant_signature = signatures.get(dominant_signature_key) if dominant_signature_key else None
            dominant_bucket = get_dominant_entry(sis_row["resolutionBuckets"])
            associated_signatures = []
            for signature_key, count in sorted(
                sis_row["countsBySignature"].items(),
                key=lambda item: (-item[1], item[0]),
            ):
                signature = signatures.get(signature_key)
                if not signature:
                    continue
                associated_signatures.append(
                    {
                        "signatureKey": signature["signatureKey"],
                        "signatureVersion": signature["signatureVersion"],
                        "signatureLabel": signature["signatureLabel"],
                        "count": count,
                        "entityType": signature["entityType"],
                        "errorCode": signature["errorCode"],
                        "canonicalErrorCode": signature["canonicalErrorCode"],
                        "operationName": signature["operationName"],
                        "resolutionTitle": signature["resolutionHint"]["title"],
                        "sampleMessage": signature["sampleMessage"],
                    }
                )
            serialized_sis_breakdowns.append(
                {
                    "key": sis_row["key"],
                    "label": sis_row["label"],
                    "affectedSchools": len(sis_row["schoolCountSet"]),
                    "totalErrors": sis_row["totalErrors"],
                    "distinctSignatures": len(sis_row["distinctSignatures"]),
                    "dominantSignature": dominant_signature["signatureLabel"] if dominant_signature else None,
                    "lastSeen": sis_row["lastSeen"],
                    "commonResolutionTheme": dominant_bucket,
                    "associatedSignatures": associated_signatures,
                }
            )
        serialized_sis_breakdowns.sort(key=lambda item: (-item["totalErrors"], item["label"]))

        latest_snapshot_date = max(capture_days)
        latest_snapshot_by_school: dict[str, str] = {}
        for group in groups:
            latest_snapshot_by_school[group.school] = max(
                latest_snapshot_by_school.get(group.school, group.snapshot_date),
                group.snapshot_date,
            )
        latest_snapshot_total_error_instances = sum(
            group.count
            for group in groups
            if latest_snapshot_by_school.get(group.school) == group.snapshot_date
        )

        response["summary"] = {
            "totalGroupedErrors": len(groups),
            "totalErrorInstances": latest_snapshot_total_error_instances,
            "distinctSignatures": len(signatures),
            "affectedSchools": len(affected_schools),
            "affectedSisPlatforms": len(affected_sis),
            "captureDays": len(capture_days),
            "latestSnapshotDate": latest_snapshot_date,
        }
        response["trends"] = serialized_trends
        response["signatures"] = serialized_signatures
        response["schoolBreakdowns"] = serialized_school_breakdowns
        response["sisBreakdowns"] = serialized_sis_breakdowns
        return response
    finally:
        db.close()


@router.get("/error-analysis/export")
async def export_error_analysis(
    days: Optional[int] = Query(default=None, ge=1),
    school: Optional[str] = Query(default=None),
    sis_platform: Optional[str] = Query(default=None, alias="sisPlatform"),
    view: Optional[str] = Query(default="grouped"),
    category: Optional[str] = Query(default=None),
    entity_type: Optional[str] = Query(default=None, alias="entityType"),
    signature: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
):
    """Download the currently filtered error-analysis groups or details as JSON."""
    db = get_db()
    try:
        is_detail_export = view == "all-errors"
        model = ErrorAnalysisDetail if is_detail_export else ErrorAnalysisGroup
        history_start = (
            db.query(model.snapshot_date)
            .order_by(model.snapshot_date.asc())
            .first()
        )
        last_capture = (
            db.query(model.created_at)
            .order_by(model.created_at.desc())
            .first()
        )
        if is_detail_export:
            query = apply_error_analysis_detail_filters(
                db.query(ErrorAnalysisDetail),
                days=days,
                school=school,
                sis_platform=sis_platform,
                entity_type=entity_type,
                signature=signature,
                search=q,
            )
            ordered_rows = query.order_by(
                ErrorAnalysisDetail.snapshot_date.desc(),
                ErrorAnalysisDetail.school.asc(),
                ErrorAnalysisDetail.id.asc(),
            ).all()
            rows = [
                row
                for row in ordered_rows
                if not category
                or build_resolution_hint(
                    row.normalized_message,
                    row.entity_type,
                    row.error_code,
                )["bucket"] == category
            ]
        else:
            query = db.query(ErrorAnalysisGroup)
            if days is not None:
                cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
                query = query.filter(ErrorAnalysisGroup.snapshot_date >= cutoff)
            if school:
                query = query.filter(ErrorAnalysisGroup.school == school)
            if sis_platform:
                query = query.filter(ErrorAnalysisGroup.sis_platform == sis_platform)
            rows = query.order_by(
                ErrorAnalysisGroup.snapshot_date.desc(),
                ErrorAnalysisGroup.school.asc(),
                ErrorAnalysisGroup.signature_version.asc(),
                ErrorAnalysisGroup.signature_key.asc(),
            ).all()

        filename_parts = ["error-analysis"]
        if is_detail_export:
            filename_parts.append("all-errors")
        if school:
            filename_parts.append(school)
        if sis_platform:
            filename_parts.append(sis_platform.lower().replace(" ", "-"))
        if days is not None:
            filename_parts.append(f"{days}d")
        filename_parts.append(datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
        filename = "-".join(filename_parts) + ".json"

        payload = {
            "metadata": {
                "historyStartsOn": history_start[0] if history_start else None,
                "lastCapturedAt": serialize_datetime(last_capture[0]) if last_capture else None,
                "appliedFilters": {
                    "days": days,
                    "school": school,
                    "sisPlatform": sis_platform,
                    "category": category,
                    "entityType": entity_type,
                    "signature": signature,
                    "q": q,
                    "view": view,
                },
                "exportedAt": datetime.now(timezone.utc).isoformat(),
                ("rowCount" if is_detail_export else "groupCount"): len(rows),
                "totalErrorInstances": sum(getattr(row, "count", 1) for row in rows),
            },
            ("rows" if is_detail_export else "groups"): [row.to_dict() for row in rows],
        }

        return Response(
            content=json.dumps(payload, indent=2),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    finally:
        db.close()


@router.get("/error-analysis/errors")
async def get_error_analysis_errors(
    days: Optional[int] = Query(default=None, ge=1),
    school: Optional[str] = Query(default=None),
    sis_platform: Optional[str] = Query(default=None, alias="sisPlatform"),
    latest_only: bool = Query(default=False, alias="latestOnly"),
    category: Optional[str] = Query(default=None),
    entity_type: Optional[str] = Query(default=None, alias="entityType"),
    signature: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    sort_by: str = Query(default="snapshotDate", alias="sortBy"),
    sort_dir: str = Query(default="desc", alias="sortDir"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200, alias="pageSize"),
):
    """Return paginated individual captured merge errors for the all-errors table."""
    db = get_db()
    try:
        sort_columns = {
            "snapshotDate": ErrorAnalysisDetail.snapshot_date,
            "school": ErrorAnalysisDetail.school,
            "displayName": ErrorAnalysisDetail.display_name,
            "sisPlatform": ErrorAnalysisDetail.sis_platform,
            "entityType": ErrorAnalysisDetail.entity_type,
            "errorCode": ErrorAnalysisDetail.error_code,
            "signatureLabel": ErrorAnalysisDetail.signature_label,
            "mergeReportId": ErrorAnalysisDetail.merge_report_id,
        }
        order_column = sort_columns.get(sort_by, ErrorAnalysisDetail.snapshot_date)
        query = apply_error_analysis_detail_filters(
            db.query(ErrorAnalysisDetail),
            days=days,
            school=school,
            sis_platform=sis_platform,
            entity_type=entity_type,
            signature=signature,
            search=q,
        )
        resolved_snapshot_date = resolve_latest_snapshot_date(query, ErrorAnalysisDetail.snapshot_date) if latest_only else None
        if resolved_snapshot_date:
            query = query.filter(ErrorAnalysisDetail.snapshot_date == resolved_snapshot_date)
        ordered_rows = query.order_by(
            order_column.asc() if sort_dir == "asc" else order_column.desc(),
            ErrorAnalysisDetail.id.desc() if sort_dir == "desc" else ErrorAnalysisDetail.id.asc(),
        ).all()
        filtered_rows = [
            row
            for row in ordered_rows
            if not category
            or build_resolution_hint(
                row.normalized_message,
                row.entity_type,
                row.error_code,
            )["bucket"] == category
        ]
        total = len(filtered_rows)
        start = (page - 1) * page_size
        rows = filtered_rows[start : start + page_size]

        return {
            "rows": [row.to_dict() for row in rows],
            "total": total,
            "page": page,
            "pageSize": page_size,
            "sortBy": sort_by,
            "sortDir": sort_dir,
            "metadata": {
                "appliedFilters": {
                    "days": days,
                    "school": school,
                    "sisPlatform": sis_platform,
                    "latestOnly": latest_only,
                    "category": category,
                    "entityType": entity_type,
                    "signature": signature,
                    "q": q,
                },
                "resolvedSnapshotDate": resolved_snapshot_date,
            },
        }
    finally:
        db.close()


@router.get("/error-analysis/signature-explorer")
async def get_error_analysis_signature_explorer(
    signature: str = Query(..., min_length=1),
    group_by: Literal["sis", "school", "term"] = Query(default="sis", alias="groupBy"),
    bucket: Optional[str] = Query(default=None),
    days: Optional[int] = Query(default=None, ge=1),
    school: Optional[str] = Query(default=None),
    sis_platform: Optional[str] = Query(default=None, alias="sisPlatform"),
    latest_only: bool = Query(default=True, alias="latestOnly"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200, alias="pageSize"),
):
    """Return within-signature drilldowns and matching latest detail rows."""
    db = get_db()
    try:
        query = apply_error_analysis_detail_filters(
            db.query(ErrorAnalysisDetail),
            days=days,
            school=school,
            sis_platform=sis_platform,
            entity_type=None,
            signature=signature,
            search=None,
        )
        resolved_snapshot_date = resolve_latest_snapshot_date(query, ErrorAnalysisDetail.snapshot_date) if latest_only else None
        if resolved_snapshot_date:
            query = query.filter(ErrorAnalysisDetail.snapshot_date == resolved_snapshot_date)

        signature_rows = query.order_by(
            ErrorAnalysisDetail.snapshot_date.desc(),
            ErrorAnalysisDetail.school.asc(),
            ErrorAnalysisDetail.id.desc(),
        ).all()
        breakdowns = build_signature_explorer_breakdowns(signature_rows)
        filtered_rows = (
            [
                row
                for row in signature_rows
                if (get_signature_explorer_bucket_value(row, group_by) or SIGNATURE_EXPLORER_UNKNOWN_BUCKET) == bucket
            ]
            if bucket
            else signature_rows
        )
        grouped_rows = build_signature_explorer_grouped_rows(filtered_rows)
        start = (page - 1) * page_size
        rows = grouped_rows[start : start + page_size]

        return {
            "rows": rows,
            "total": len(grouped_rows),
            "page": page,
            "pageSize": page_size,
            "metadata": {
                "appliedFilters": {
                    "days": days,
                    "school": school,
                    "sisPlatform": sis_platform,
                    "latestOnly": latest_only,
                    "signature": signature,
                    "groupBy": group_by,
                    "bucket": bucket,
                },
                "resolvedSnapshotDate": resolved_snapshot_date,
                "signatureTotal": len(signature_rows),
                "bucketTotal": len(filtered_rows),
            },
            "breakdowns": breakdowns,
        }
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
        logger.warning("Active users lookup failed for %s: %s", school, e)
        raise HTTPException(
            status_code=502,
            detail={
                "code": "active_users_lookup_failed",
                "message": "Could not fetch active users from the upstream service",
                "school": school,
            },
        ) from e


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
            "lastAttemptedSync": serialize_persisted_sync_run(latest_attempt) if latest_attempt else None,
            "lastSuccessfulSync": {
                "school": latest_success.school,
                "snapshotDate": latest_success.snapshot_date,
                "createdAt": serialize_datetime(latest_success.created_at),
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
        total_count = db.query(SyncRun).count()
        runs = (
            db.query(SyncRun)
            .order_by(SyncRun.attempted_at.desc())
            .offset(offset)
            .limit(limit)
            .all()
        )
        return {
            "syncRuns": [serialize_persisted_sync_run(r) for r in runs],
            "totalCount": total_count,
            "limit": limit,
            "offset": offset,
        }
    finally:
        db.close()


@router.get("/client-health/sync-runs/{job_id}")
async def get_client_health_sync_run(job_id: str):
    """Return one persisted sync run by job id for the detail page."""
    db = get_db()
    try:
        run = get_sync_run_or_404(db, job_id)
        return {"syncRun": serialize_persisted_sync_run(run)}
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Sync endpoint — fetch live data from Coursedog and persist to SQLite
# ---------------------------------------------------------------------------


@router.post("/client-health/sync")
async def trigger_sync(school: Optional[str] = Query(default=None)):
    """Start a background sync job for all schools or a single school."""
    enforce_job_trigger_cooldown("sync")
    job, already_running = enqueue_sync_job(school=school)
    if already_running:
        logger.info("Sync trigger reused active job %s for school=%s", job["jobId"], school)
    else:
        logger.info("Accepted sync trigger job_id=%s school=%s", job["jobId"], school)
    return serialize_sync_job(job, already_running=already_running)


@router.post("/client-health/history/backfill")
async def trigger_history_backfill(
    startDate: str = Query(...),
    endDate: Optional[str] = Query(default=None),
    school: Optional[str] = Query(default=None),
):
    """Backfill historical snapshots for one school or all schools."""
    active_job = get_active_sync_job()
    if active_job:
        logger.info(
            "Backfill trigger reused active job %s for school=%s",
            active_job["jobId"],
            school,
        )
        return serialize_sync_job(active_job, already_running=True)

    enforce_job_trigger_cooldown("history-backfill")
    normalized_start, normalized_end = resolve_backfill_date_range(startDate, endDate)
    snapshot_dates = build_snapshot_dates(normalized_start, normalized_end)

    job = create_backfill_job_payload(
        job_id=str(uuid4()),
        school=school,
        start_date=normalized_start,
        end_date=normalized_end,
        date_count=len(snapshot_dates),
    )
    start_backfill_job_task(job)
    logger.info(
        "Accepted backfill trigger job_id=%s school=%s start=%s end=%s",
        job["jobId"],
        school,
        normalized_start,
        normalized_end,
    )
    return serialize_sync_job(job)


@router.post("/client-health/history/backfill/{job_id}/resume")
async def resume_history_backfill(job_id: str):
    """Resume a stalled or incomplete bulk backfill by retrying pending and failed units."""
    return await restart_backfill_job(job_id, mode="resume")


@router.post("/client-health/history/backfill/{job_id}/retry-failures")
async def retry_history_backfill_failures(job_id: str):
    """Retry only failed units for an existing backfill job."""
    return await restart_backfill_job(job_id, mode="retry_failures")


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


def validate_scheduler_time(sync_time: str) -> str:
    """Normalize and validate a scheduler time in HH:MM 24-hour format."""
    normalized = (sync_time or "").strip()
    try:
        parsed = datetime.strptime(normalized, "%H:%M")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="syncTime must use HH:MM in 24-hour format") from exc
    return parsed.strftime("%H:%M")


def get_scheduler_settings(db) -> SchedulerSettings:
    """Return the persisted scheduler settings row, creating defaults when missing."""
    settings = db.query(SchedulerSettings).order_by(SchedulerSettings.id.asc()).first()
    if settings:
        if not settings.sync_time:
            settings.sync_time = DEFAULT_SCHEDULED_SYNC_TIME
            settings.updated_at = datetime.now(timezone.utc)
            db.commit()
            db.refresh(settings)
        return settings

    settings = SchedulerSettings(
        sync_enabled=1,
        sync_time=DEFAULT_SCHEDULED_SYNC_TIME,
        updated_at=datetime.now(timezone.utc),
    )
    db.add(settings)
    db.commit()
    db.refresh(settings)
    return settings


def load_scheduler_settings() -> dict:
    """Load current scheduler settings into a frontend-friendly dict."""
    db = get_db()
    try:
        return get_scheduler_settings(db).to_dict()
    finally:
        db.close()


def get_next_scheduled_sync_time(
    now: Optional[datetime] = None,
    *,
    sync_time: str = DEFAULT_SCHEDULED_SYNC_TIME,
) -> datetime:
    """Return the next configured America/New_York run time."""
    local_now = current_new_york_time(now)
    normalized_time = validate_scheduler_time(sync_time)
    scheduled_hour, scheduled_minute = [int(part) for part in normalized_time.split(":")]
    scheduled = local_now.replace(
        hour=scheduled_hour,
        minute=scheduled_minute,
        second=0,
        microsecond=0,
    )
    if scheduled <= local_now:
        scheduled += timedelta(days=1)
    return scheduled


@router.get("/client-health/scheduler-settings")
async def get_client_health_scheduler_settings():
    """Return the persisted daily sync scheduler settings."""
    return load_scheduler_settings()


@router.put("/client-health/scheduler-settings")
async def update_client_health_scheduler_settings(payload: SchedulerSettingsPayload):
    """Update the daily sync scheduler settings and restart the scheduler loop."""
    normalized_time = validate_scheduler_time(payload.syncTime)
    db = get_db()
    try:
        settings = get_scheduler_settings(db)
        settings.sync_enabled = 1 if payload.syncEnabled else 0
        settings.sync_time = normalized_time
        settings.updated_at = datetime.now(timezone.utc)
        db.commit()
        db.refresh(settings)
        response = settings.to_dict()
    finally:
        db.close()

    await stop_scheduled_sync_service()
    await start_scheduled_sync_service()
    return response


def get_active_sync_job() -> Optional[dict]:
    """Return the currently active queued/running job, if any."""
    if not _active_sync_job_id:
        return None
    active_job = _sync_jobs.get(_active_sync_job_id)
    if active_job and active_job["status"] in {"queued", "running"}:
        return active_job
    return None


def is_backfill_scope(scope: Optional[str]) -> bool:
    return bool(scope and "backfill" in scope)


def append_failed_unit_sample(sample: list[dict], unit: dict) -> list[dict]:
    """Keep a bounded list of failed work-unit samples for operator debugging."""
    next_sample = [*sample, unit]
    return next_sample[-FAILED_UNITS_SAMPLE_LIMIT:]


def derive_sync_run_status(
    status: str,
    *,
    scope: Optional[str],
    finished_at: Optional[datetime],
    last_heartbeat_at: Optional[datetime],
    failed_units: int,
) -> tuple[str, Optional[str]]:
    """Return the user-facing status and detail for sync-run payloads."""
    if status == "completed" and is_backfill_scope(scope) and failed_units > 0:
        return "completed_with_failures", "completed_with_failures"

    if (
        status == "running"
        and is_backfill_scope(scope)
        and not normalize_utc_datetime(finished_at)
        and last_heartbeat_at
    ):
        normalized_heartbeat = normalize_utc_datetime(last_heartbeat_at)
        if normalized_heartbeat is None:
            return status, None
        age_seconds = (datetime.now(timezone.utc) - normalized_heartbeat).total_seconds()
        if age_seconds >= BACKFILL_STALL_THRESHOLD_SECONDS:
            return "stalled", "heartbeat_stale"

    return status, None


def _format_elapsed_for_reason(age_seconds: float) -> str:
    """Render elapsed time for operator-facing failure reasons."""
    rounded = max(0, int(round(age_seconds)))
    if rounded < 60:
        return f"{rounded}s"
    minutes, seconds = divmod(rounded, 60)
    if minutes < 60:
        return f"{minutes}m" if seconds == 0 else f"{minutes}m {seconds}s"
    hours, remaining_minutes = divmod(minutes, 60)
    return f"{hours}h" if remaining_minutes == 0 else f"{hours}h {remaining_minutes}m"


def build_stalled_backfill_reason(
    sync_run: SyncRun,
    *,
    now: Optional[datetime] = None,
) -> str:
    """Build the best-effort reason string for a stalled backfill run."""
    reference_now = now or datetime.now(timezone.utc)
    fragments: list[str] = []

    heartbeat_at = normalize_utc_datetime(sync_run.last_heartbeat_at)
    if heartbeat_at:
        heartbeat_age = (reference_now - heartbeat_at).total_seconds()
        fragments.append(
            f"No heartbeat for {_format_elapsed_for_reason(heartbeat_age)}"
        )
    else:
        fragments.append("Backfill stopped reporting heartbeats")

    if sync_run.current_school or sync_run.current_snapshot_date:
        unit_target = sync_run.current_school or "unknown school"
        if sync_run.current_snapshot_date:
            unit_target = f"{unit_target} on {sync_run.current_snapshot_date}"
        fragments.append(f"while processing {unit_target}")

    progress_at = normalize_utc_datetime(sync_run.last_progress_at)
    if progress_at:
        progress_age = (reference_now - progress_at).total_seconds()
        fragments.append(
            f"last progress {_format_elapsed_for_reason(progress_age)} ago"
        )

    # Only surface a "last error" that is an actual failure, not an
    # auto-resume recovery note (which starts with "Auto-resume triggered").
    # Including recovery notes here causes compounding verbose stall reasons
    # where each stall embeds the previous auto-resume message verbatim.
    def _is_recovery_note(msg: str) -> bool:
        return isinstance(msg, str) and msg.startswith("Auto-resume triggered")

    recent_error: Optional[str] = None
    if sync_run.error_message and not _is_recovery_note(sync_run.error_message):
        recent_error = sync_run.error_message
    if not recent_error and sync_run.errors_json:
        try:
            parsed_errors = json.loads(sync_run.errors_json)
            if isinstance(parsed_errors, list):
                # Walk from the end, skipping recovery notes
                for entry in reversed(parsed_errors):
                    if not _is_recovery_note(entry):
                        recent_error = entry
                        break
        except json.JSONDecodeError:
            pass

    if recent_error:
        fragments.append(f"last error: {recent_error}")

    return "; ".join(fragments)


def append_job_error(job: dict, message: str) -> None:
    """Append a bounded operator-facing issue entry to a job payload."""
    job["errors"] = [*(job.get("errors") or []), message][-20:]


def serialize_persisted_sync_run(sync_run: SyncRun) -> dict:
    """Serialize a DB-backed sync run with derived operator-facing status fields."""
    payload = sync_run.to_dict()
    derived_status, derived_detail = derive_sync_run_status(
        sync_run.status,
        scope=sync_run.scope,
        finished_at=sync_run.finished_at,
        last_heartbeat_at=sync_run.last_heartbeat_at,
        failed_units=sync_run.failed_units or 0,
    )
    payload["status"] = derived_status
    if derived_detail and not payload.get("statusDetail"):
        payload["statusDetail"] = derived_detail
    if derived_status == "stalled" and not payload.get("failureReason"):
        payload["failureReason"] = build_stalled_backfill_reason(sync_run)
    if sync_run.last_heartbeat_at:
        payload["stallThresholdSeconds"] = BACKFILL_STALL_THRESHOLD_SECONDS
    return payload


def create_backfill_job_payload(
    *,
    job_id: str,
    school: Optional[str],
    start_date: str,
    end_date: str,
    date_count: int,
    scope: Optional[str] = None,
) -> dict:
    """Create the in-memory job shape used for bulk backfills."""
    return {
        "jobId": job_id,
        "status": "queued",
        "snapshotDate": None,
        "schoolsProcessed": 0,
        "totalSchools": 0,
        "errors": [],
        "timing": None,
        "error": None,
        "scope": scope or ("history-backfill-single" if school else "history-backfill-bulk"),
        "school": school,
        "startDate": start_date,
        "endDate": end_date,
        "dateCount": date_count,
        "startedAt": None,
        "finishedAt": None,
        "lastHeartbeatAt": None,
        "lastProgressAt": None,
        "currentSchool": None,
        "currentSnapshotDate": None,
        "completedUnits": 0,
        "failedUnits": 0,
        "skippedUnits": 0,
        "statusDetail": None,
        "failureReason": None,
        "failedUnitsSample": [],
        "checkpointState": None,
    }


def start_backfill_job_task(job: dict, *, mode: str = "initial") -> None:
    """Register a backfill job as active and start its worker task."""
    global _active_sync_job_id
    _sync_jobs[job["jobId"]] = job
    _active_sync_job_id = job["jobId"]
    asyncio.create_task(
        run_history_backfill_job(
            job["jobId"],
            school=job.get("school"),
            start_date=job["startDate"],
            end_date=job["endDate"],
            mode=mode,
        )
    )


def get_sync_run_or_404(db, job_id: str) -> SyncRun:
    sync_run = db.query(SyncRun).filter(SyncRun.job_id == job_id).first()
    if not sync_run:
        raise HTTPException(status_code=404, detail="Backfill job not found")
    return sync_run


def build_backfill_job_from_sync_run(sync_run: SyncRun) -> dict:
    """Rehydrate an in-memory job payload from the persisted sync-run row."""
    job = create_backfill_job_payload(
        job_id=sync_run.job_id,
        school=sync_run.school,
        start_date=sync_run.start_date or "",
        end_date=sync_run.end_date or "",
        date_count=sync_run.date_count or 0,
        scope=sync_run.scope,
    )
    job.update(
        {
            "status": sync_run.status,
            "snapshotDate": sync_run.snapshot_date,
            "schoolsProcessed": sync_run.schools_processed or 0,
            "totalSchools": sync_run.total_schools or 0,
            "startedAt": serialize_datetime(sync_run.started_at),
            "finishedAt": serialize_datetime(sync_run.finished_at),
            "lastHeartbeatAt": serialize_datetime(sync_run.last_heartbeat_at),
            "lastProgressAt": serialize_datetime(sync_run.last_progress_at),
            "currentSchool": sync_run.current_school,
            "currentSnapshotDate": sync_run.current_snapshot_date,
            "completedUnits": sync_run.completed_units or 0,
            "failedUnits": sync_run.failed_units or 0,
            "skippedUnits": sync_run.skipped_units or 0,
            "statusDetail": sync_run.status_detail,
            "failureReason": sync_run.failure_reason,
            "failedUnitsSample": json.loads(sync_run.failed_units_sample_json)
            if sync_run.failed_units_sample_json
            else [],
            "checkpointState": json.loads(sync_run.checkpoint_state_json)
            if sync_run.checkpoint_state_json
            else None,
            "errors": json.loads(sync_run.errors_json) if sync_run.errors_json else [],
            "timing": json.loads(sync_run.timing_json) if sync_run.timing_json else None,
            "error": sync_run.error_message,
        }
    )
    return job


def summarize_backfill_work_unit_statuses(db, job_id: str) -> dict[str, int]:
    """Return cumulative work-unit counts for a backfill job."""
    counts = {"pending": 0, "running": 0, "completed": 0, "failed": 0, "skipped": 0}
    units = db.query(BackfillWorkUnit).filter(BackfillWorkUnit.job_id == job_id).all()
    for unit in units:
        if unit.status in counts:
            counts[unit.status] += 1
    return counts


def backfill_snapshot_exists(db, school: str, snapshot_date: str) -> bool:
    """Return whether a snapshot already exists for a school/date pair."""
    return (
        db.query(SchoolSnapshot.id)
        .filter(SchoolSnapshot.school == school, SchoolSnapshot.snapshot_date == snapshot_date)
        .first()
        is not None
    )


async def restart_backfill_job(job_id: str, mode: str) -> dict:
    """Resume or retry a historical backfill using the persisted work ledger."""
    active_job = get_active_sync_job()
    if active_job:
        logger.info("Backfill %s request reused active job %s", mode, active_job["jobId"])
        return serialize_sync_job(active_job, already_running=True)

    db = get_db()
    try:
        sync_run = get_sync_run_or_404(db, job_id)
        if not is_backfill_scope(sync_run.scope):
            raise HTTPException(status_code=422, detail="Only historical backfill jobs can be resumed")

        derived_status, _ = derive_sync_run_status(
            sync_run.status,
            scope=sync_run.scope,
            finished_at=sync_run.finished_at,
            last_heartbeat_at=sync_run.last_heartbeat_at,
            failed_units=sync_run.failed_units or 0,
        )
        if mode == "retry_failures":
            if (sync_run.failed_units or 0) == 0:
                raise HTTPException(status_code=409, detail="This backfill has no failed units to retry")
        elif derived_status not in {"failed", "stalled", "running"} and not (
            sync_run.completed_units or 0
        ) < (sync_run.total_schools or 0):
            raise HTTPException(status_code=409, detail="This backfill does not need resuming")

        job = build_backfill_job_from_sync_run(sync_run)
        start_backfill_job_task(job, mode=mode)
        logger.info("Accepted backfill %s request for job_id=%s", mode, job_id)
        return serialize_sync_job(job)
    finally:
        db.close()


def prepare_backfill_auto_resume(job: dict, stalled_reason: str, *, now: Optional[datetime] = None) -> str:
    """Annotate a stalled backfill job before automatically resuming it."""
    reference_now = now or datetime.now(timezone.utc)
    recovery_note = (
        f"Auto-resume triggered at {reference_now.isoformat()} after stall: {stalled_reason}"
    )
    # Record the event in the errors log (non-fatal) but do NOT write into
    # failureReason — that field is reserved for terminal failures.  Clearing
    # it here ensures a previously-failed job that is now recovering doesn't
    # keep showing a stale failure reason while it is actively running.
    append_job_error(job, recovery_note)
    job["recoveryNote"] = recovery_note
    job["failureReason"] = None
    job["statusDetail"] = "auto_resuming"
    job["finishedAt"] = None
    job["error"] = None
    return recovery_note


async def maybe_auto_resume_stalled_backfill(now: Optional[datetime] = None) -> Optional[dict]:
    """Restart one stalled backfill when the worker is otherwise idle."""
    if get_active_sync_job():
        return None

    reference_now = now or datetime.now(timezone.utc)
    db = get_db()
    try:
        candidates = (
            db.query(SyncRun)
            .filter(SyncRun.status == "running")
            .order_by(SyncRun.attempted_at.desc())
            .all()
        )
        for sync_run in candidates:
            derived_status, _ = derive_sync_run_status(
                sync_run.status,
                scope=sync_run.scope,
                finished_at=sync_run.finished_at,
                last_heartbeat_at=sync_run.last_heartbeat_at,
                failed_units=sync_run.failed_units or 0,
            )
            if derived_status != "stalled":
                continue

            stalled_reason = build_stalled_backfill_reason(sync_run, now=reference_now)
            job = build_backfill_job_from_sync_run(sync_run)
            recovery_note = prepare_backfill_auto_resume(job, stalled_reason, now=reference_now)
            start_backfill_job_task(job, mode="resume")
            persist_job_state(job["jobId"])
            logger.warning(
                "Auto-resuming stalled backfill job_id=%s reason=%s",
                job["jobId"],
                stalled_reason,
            )
            return {"jobId": job["jobId"], "note": recovery_note}
    finally:
        db.close()

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
    scheduler_settings = load_scheduler_settings()
    if not scheduler_settings["syncEnabled"]:
        return None

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
    scheduler_settings = load_scheduler_settings()
    if not scheduler_settings["syncEnabled"]:
        raise HTTPException(status_code=409, detail="Daily sync is disabled")

    snapshot_date = get_new_york_snapshot_date(now)
    job, _ = enqueue_sync_job(
        scope="scheduled-bulk",
        snapshot_date_override=snapshot_date,
    )
    return job


async def run_scheduled_sync_loop(stop_event: asyncio.Event):
    """Sleep until the next configured New York run, then queue a latest-only sync."""
    while not stop_event.is_set():
        scheduler_settings = load_scheduler_settings()
        if not scheduler_settings["syncEnabled"]:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=3600)
                break
            except asyncio.TimeoutError:
                continue

        now_utc = datetime.now(timezone.utc)
        next_run = get_next_scheduled_sync_time(now_utc, sync_time=scheduler_settings["syncTime"])
        delay_seconds = max(0.0, (next_run.astimezone(timezone.utc) - now_utc).total_seconds())

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=delay_seconds)
            break
        except asyncio.TimeoutError:
            try:
                await maybe_enqueue_scheduled_sync()
            except HTTPException:
                continue


async def run_backfill_recovery_loop(stop_event: asyncio.Event):
    """Periodically resume stalled backfills when no other job is active."""
    while not stop_event.is_set():
        try:
            await maybe_auto_resume_stalled_backfill()
        except Exception:
            logger.exception("Automatic stalled-backfill recovery failed")

        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=BACKFILL_AUTO_RESUME_POLL_SECONDS,
            )
            break
        except asyncio.TimeoutError:
            continue


async def start_scheduled_sync_service():
    """Start the daily scheduler and queue a same-day catch-up when needed."""
    global _scheduled_sync_stop_event, _scheduled_sync_task, _backfill_recovery_task

    if _scheduled_sync_task and not _scheduled_sync_task.done():
        return

    _scheduled_sync_stop_event = asyncio.Event()
    await maybe_enqueue_catchup_sync()
    _scheduled_sync_task = asyncio.create_task(run_scheduled_sync_loop(_scheduled_sync_stop_event))
    _backfill_recovery_task = asyncio.create_task(
        run_backfill_recovery_loop(_scheduled_sync_stop_event)
    )


async def stop_scheduled_sync_service():
    """Stop the daily scheduler task cleanly."""
    global _scheduled_sync_stop_event, _scheduled_sync_task, _backfill_recovery_task

    if _scheduled_sync_stop_event:
        _scheduled_sync_stop_event.set()

    if _scheduled_sync_task:
        await asyncio.gather(_scheduled_sync_task, return_exceptions=True)
    if _backfill_recovery_task:
        await asyncio.gather(_backfill_recovery_task, return_exceptions=True)

    _scheduled_sync_stop_event = None
    _scheduled_sync_task = None
    _backfill_recovery_task = None


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
        # Start each sync with a fresh upstream auth session when credentials
        # are configured. Mid-run expiry is still handled by api_get().
        await authenticate_api()
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
        new_error_groups: list[dict] = []
        new_error_details: list[dict] = []
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
                    new_error_groups.extend(result.pop("_errorGroups", []))
                    new_error_details.extend(result.pop("_errorDetails", []))
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
            delete_error_groups_query = db.query(ErrorAnalysisGroup).filter(
                ErrorAnalysisGroup.snapshot_date == snapshot_date
            )
            delete_error_details_query = db.query(ErrorAnalysisDetail).filter(
                ErrorAnalysisDetail.snapshot_date == snapshot_date
            )
            if school:
                delete_query = delete_query.filter(SchoolSnapshot.school == school)
                delete_error_groups_query = delete_error_groups_query.filter(ErrorAnalysisGroup.school == school)
                delete_error_details_query = delete_error_details_query.filter(ErrorAnalysisDetail.school == school)
            delete_query.delete()
            delete_error_groups_query.delete()
            delete_error_details_query.delete()
            for snap_data in new_snapshots:
                db.add(SchoolSnapshot.from_dict(snap_data))
            for group_data in new_error_groups:
                db.add(ErrorAnalysisGroup.from_dict(group_data))
            for detail_data in new_error_details:
                db.add(ErrorAnalysisDetail.from_dict(detail_data))
            db.commit()
            write_error_analysis_export(db)
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
    sync_run.last_heartbeat_at = (
        datetime.fromisoformat(job["lastHeartbeatAt"]) if job.get("lastHeartbeatAt") else None
    )
    sync_run.last_progress_at = (
        datetime.fromisoformat(job["lastProgressAt"]) if job.get("lastProgressAt") else None
    )
    sync_run.current_school = job.get("currentSchool")
    sync_run.current_snapshot_date = job.get("currentSnapshotDate")
    sync_run.completed_units = job.get("completedUnits", 0)
    sync_run.failed_units = job.get("failedUnits", 0)
    sync_run.skipped_units = job.get("skippedUnits", 0)
    sync_run.status_detail = job.get("statusDetail")
    sync_run.failure_reason = job.get("failureReason")
    sync_run.failed_units_sample_json = json.dumps(job.get("failedUnitsSample", []))
    sync_run.checkpoint_state_json = (
        json.dumps(job.get("checkpointState")) if job.get("checkpointState") else None
    )
    sync_run.errors_json = json.dumps(job.get("errors", []))
    sync_run.timing_json = json.dumps(job.get("timing")) if job.get("timing") else None
    sync_run.error_message = job.get("error") or (job.get("errors") or [None])[0]


def update_backfill_job_state(
    job: dict,
    *,
    now: Optional[datetime] = None,
    current_school: Optional[str] = None,
    current_snapshot_date: Optional[str] = None,
    completed_delta: int = 0,
    failed_delta: int = 0,
    skipped_delta: int = 0,
    error_message: Optional[str] = None,
    failed_unit: Optional[dict] = None,
    checkpoint_status: Optional[str] = None,
) -> None:
    """Update in-memory backfill metadata used for persistence and UI."""
    timestamp = (now or datetime.now(timezone.utc)).isoformat()
    job["lastHeartbeatAt"] = timestamp
    if current_school is not None:
        job["currentSchool"] = current_school
    if current_snapshot_date is not None:
        job["currentSnapshotDate"] = current_snapshot_date

    if completed_delta or failed_delta or skipped_delta:
        job["lastProgressAt"] = timestamp
        job["completedUnits"] = job.get("completedUnits", 0) + completed_delta
        job["failedUnits"] = job.get("failedUnits", 0) + failed_delta
        job["skippedUnits"] = job.get("skippedUnits", 0) + skipped_delta
        job["schoolsProcessed"] = job["completedUnits"] + job["failedUnits"] + job["skippedUnits"]

    if error_message:
        append_job_error(job, error_message)

    if failed_unit:
        job["failedUnitsSample"] = append_failed_unit_sample(
            job.get("failedUnitsSample", []),
            failed_unit,
        )

    remaining = max(0, job.get("totalSchools", 0) - job.get("schoolsProcessed", 0))
    job["checkpointState"] = {
        "currentSchool": job.get("currentSchool"),
        "currentSnapshotDate": job.get("currentSnapshotDate"),
        "completedUnits": job.get("completedUnits", 0),
        "failedUnits": job.get("failedUnits", 0),
        "skippedUnits": job.get("skippedUnits", 0),
        "remainingUnits": remaining,
        "status": checkpoint_status or job.get("status"),
    }


def persist_job_state(job_id: str) -> None:
    """Persist the current in-memory job metadata to the sync_runs table."""
    db = get_db()
    try:
        sync_run = db.query(SyncRun).filter(SyncRun.job_id == job_id).first()
        if sync_run:
            hydrate_sync_run(sync_run, _sync_jobs[job_id])
            db.commit()
    finally:
        db.close()


def reset_backfill_work_units(db, job_id: str, statuses: set[str]) -> int:
    """Reset the selected work units back to pending for resume/retry."""
    units = (
        db.query(BackfillWorkUnit)
        .filter(BackfillWorkUnit.job_id == job_id, BackfillWorkUnit.status.in_(list(statuses)))
        .all()
    )
    for unit in units:
        unit.status = "pending"
        unit.last_error = None
        unit.completed_at = None
    return len(units)


def seed_backfill_work_units(db, job_id: str, schools: list[dict], snapshot_dates: list[str]) -> None:
    """Create the per-school per-date work ledger for a new backfill job."""
    for school_info in schools:
        for snapshot_date in snapshot_dates:
            exists = (
                db.query(BackfillWorkUnit)
                .filter(
                    BackfillWorkUnit.job_id == job_id,
                    BackfillWorkUnit.school == school_info["school"],
                    BackfillWorkUnit.snapshot_date == snapshot_date,
                )
                .first()
            )
            if exists:
                continue
            db.add(
                BackfillWorkUnit(
                    job_id=job_id,
                    school=school_info["school"],
                    display_name=school_info.get("displayName", school_info["school"]),
                    snapshot_date=snapshot_date,
                    products_json=json.dumps(school_info.get("products", [])),
                    status="pending",
                )
            )


def get_backfill_work_units(db, job_id: str, statuses: set[str]) -> list[BackfillWorkUnit]:
    """Load work units in a deterministic processing order."""
    return (
        db.query(BackfillWorkUnit)
        .filter(BackfillWorkUnit.job_id == job_id, BackfillWorkUnit.status.in_(list(statuses)))
        .order_by(BackfillWorkUnit.school, BackfillWorkUnit.snapshot_date)
        .all()
    )


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
        else get_new_york_snapshot_date()
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


def extract_merge_error_rows(payload) -> list[dict]:
    """Best-effort extraction of merge-error rows from varied API response shapes."""
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if not isinstance(payload, dict):
        return []

    for key in ("content", "items", "rows", "mergeErrors", "errors", "data", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            rows: list[dict] = []
            for item in value:
                if not isinstance(item, dict):
                    continue
                rows.append(item.get("mergeError", item))
            return rows
        if isinstance(value, dict):
            nested = extract_merge_error_rows(value)
            if nested:
                return nested

    return []


def first_non_empty_string(*values) -> Optional[str]:
    """Return the first non-empty string-ish value."""
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


# XML tag names (namespace-stripped) that carry a human-readable SOAP fault message.
_SOAP_READABLE_TAGS = {"faultstring", "Message", "message", "detail", "Detail", "Text"}


def extract_message_from_xml_body(body: str) -> Optional[str]:
    """Try to pull a short, readable message out of a raw XML/SOAP body string.

    Priority:
    1. The text content of any <faultstring> (or other well-known readable) element.
    2. The concatenated leaf-node text of the first <detail> / <Detail> subtree.
    3. None — let the caller decide what to do next.
    """
    try:
        root = ET.fromstring(body.strip())
    except ET.ParseError:
        return None

    # Walk every element; strip namespace prefixes for comparison.
    readable_texts: list[str] = []
    detail_texts: list[str] = []

    for elem in root.iter():
        local = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
        text = (elem.text or "").strip()
        if not text:
            continue
        if local in ("faultstring", "message", "Message", "Text"):
            readable_texts.append(text)
        elif local in ("detail", "Detail"):
            detail_texts.append(text)

    if readable_texts:
        return " | ".join(dict.fromkeys(readable_texts))  # deduplicate, preserve order
    if detail_texts:
        return " | ".join(dict.fromkeys(detail_texts))
    return None


def collect_term_codes(payload) -> list[str]:
    """Extract stable term code strings from a merge-error row."""
    values: list[str] = []

    direct_term = payload.get("termCode")
    if isinstance(direct_term, str) and direct_term.strip():
        values.append(direct_term.strip())

    term_codes = payload.get("termCodes")
    if isinstance(term_codes, list):
        for term_code in term_codes:
            if isinstance(term_code, str) and term_code.strip():
                values.append(term_code.strip())

    term_like = payload.get("term") or payload.get("academicTerm")
    if isinstance(term_like, dict):
        for key in ("code", "termCode", "id"):
            term_code = term_like.get(key)
            if isinstance(term_code, str) and term_code.strip():
                values.append(term_code.strip())
                break
    elif isinstance(term_like, str) and term_like.strip():
        values.append(term_like.strip())

    return sorted(set(values))


def iterate_original_error_payloads(payload: dict):
    """Yield nested original-error payloads from a merge-error row."""
    nested_errors = payload.get("errors")
    if not isinstance(nested_errors, list):
        return

    for entry in nested_errors:
        if not isinstance(entry, dict):
            continue
        original_error = entry.get("originalError")
        if isinstance(original_error, dict):
            yield original_error


def extract_merge_error_message(payload: dict) -> str:
    """Extract the most human-readable message from a merge-error row."""
    message, _source = extract_merge_error_message_with_source(payload)
    return message


def normalize_error_message(message: str) -> str:
    """Reduce volatile values so recurring error signatures group together."""
    normalized = message.strip().lower()
    normalized = re.sub(r"https?://\S+", "<url>", normalized)
    normalized = re.sub(r"\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b", "<email>", normalized)
    normalized = re.sub(r"\b\d{4}-\d{2}-\d{2}[t\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:z|[+-]\d{2}:?\d{2})?\b", "<timestamp>", normalized)
    normalized = re.sub(r"\b\d{4}-\d{2}-\d{2}\b", "<date>", normalized)
    normalized = re.sub(r"\b\d{1,2}:\d{2}:\d{2}(?:\.\d+)?\b", "<time>", normalized)
    normalized = re.sub(r"\b[0-9a-f]{8}-[0-9a-f-]{27,}\b", "<uuid>", normalized)
    normalized = re.sub(r"\b[0-9a-f]{24,}\b", "<hex>", normalized)
    normalized = re.sub(r"\b[a-z0-9]{16,}\b", "<token>", normalized)
    normalized = re.sub(r"\b\d{2,}\b", "<num>", normalized)
    normalized = re.sub(r"['\"`][^'\"`]{2,}['\"`]", "<value>", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip() or "unspecified upstream merge error"


def get_signature_version(value: Optional[str]) -> str:
    """Return the explicit signature version, defaulting older rows to v1."""
    return value or ERROR_SIGNATURE_VERSION_V1


def build_signature_identity(signature_key: str, signature_version: Optional[str]) -> str:
    """Create an in-memory unique identity for mixed-version signature rows."""
    return f"{get_signature_version(signature_version)}:{signature_key}"


def get_row_signature_identity(row: Any) -> str:
    """Return a stable in-memory identity for an ORM row or dict-like signature payload."""
    signature_key = getattr(row, "signature_key", None) or getattr(row, "signatureKey", None)
    signature_version = getattr(row, "signature_version", None) or getattr(row, "signatureVersion", None)
    return build_signature_identity(signature_key, signature_version)


def is_operation_like_code(value: Optional[str]) -> bool:
    """Return True when a code looks like an operation/action name rather than an error class."""
    if not value:
        return False
    candidate = value.strip()
    if not candidate:
        return False
    if re.match(r"^(Create|Update|Delete|Set|Sync|Publish|Import|Export|Post|Patch|Put|Get|Merge)[A-Z0-9_].*$", candidate):
        return True
    lowered = candidate.lower()
    return lowered.endswith(("request", "operation", "action"))


def extract_operation_name(payload: dict) -> Optional[str]:
    """Extract an operation/action name without treating it as the semantic error code."""
    direct_operation = first_non_empty_string(
        payload.get("operationName"),
        payload.get("postType"),
        payload.get("action"),
    )
    if direct_operation:
        return direct_operation

    for original_error in iterate_original_error_payloads(payload):
        operation_name = first_non_empty_string(
            original_error.get("postType"),
            original_error.get("operationName"),
            original_error.get("action"),
        )
        if operation_name:
            return operation_name

    return None


def iter_merge_error_code_candidates(payload: dict):
    """Yield candidate error codes from strongest to weakest upstream locations."""
    direct_candidates = (
        ("topLevel.errorCode", payload.get("errorCode")),
        ("topLevel.code", payload.get("code")),
        ("topLevel.reasonCode", payload.get("reasonCode")),
        ("topLevel.statusCode", payload.get("statusCode")),
    )
    for source, value in direct_candidates:
        code = first_non_empty_string(value)
        if code:
            yield code, source

    for original_error in iterate_original_error_payloads(payload):
        body = original_error.get("body")
        if isinstance(body, dict):
            body_candidates = (
                ("originalError.body.code", body.get("code")),
                ("originalError.body.errorCode", body.get("errorCode")),
                ("originalError.body.reasonCode", body.get("reasonCode")),
                ("originalError.body.statusCode", body.get("statusCode")),
            )
            for source, value in body_candidates:
                code = first_non_empty_string(value)
                if code:
                    yield code, source

            body_errors = body.get("errors")
            if isinstance(body_errors, list):
                for entry in body_errors:
                    if not isinstance(entry, dict):
                        continue
                    nested_candidates = (
                        ("originalError.body.errors.code", entry.get("code")),
                        ("originalError.body.errors.errorCode", entry.get("errorCode")),
                        ("originalError.body.errors.reasonCode", entry.get("reasonCode")),
                        ("originalError.body.errors.statusCode", entry.get("statusCode")),
                    )
                    for source, value in nested_candidates:
                        code = first_non_empty_string(value)
                        if code:
                            yield code, source

        original_candidates = (
            ("originalError.code", original_error.get("code")),
            ("originalError.errorCode", original_error.get("errorCode")),
            ("originalError.reasonCode", original_error.get("reasonCode")),
            ("originalError.statusCode", original_error.get("statusCode")),
        )
        for source, value in original_candidates:
            code = first_non_empty_string(value)
            if code:
                yield code, source


def classify_canonical_error_code(payload: dict) -> dict[str, Optional[str]]:
    """Return the semantic error-code classification for one merge-error row."""
    operation_name = extract_operation_name(payload)
    for code, source in iter_merge_error_code_candidates(payload):
        lowered = code.strip().lower()
        if lowered in GENERIC_ERROR_CODE_VALUES:
            continue
        if operation_name and lowered == operation_name.strip().lower():
            continue
        if is_operation_like_code(code):
            continue
        return {
            "canonicalErrorCode": code,
            "canonicalCodeSource": source,
            "operationName": operation_name,
        }

    return {
        "canonicalErrorCode": None,
        "canonicalCodeSource": None,
        "operationName": operation_name,
    }


def extract_legacy_merge_error_code(payload: dict) -> Optional[str]:
    """Preserve the original broad code extraction for legacy key compatibility."""
    for code, _source in iter_merge_error_code_candidates(payload):
        return code

    for original_error in iterate_original_error_payloads(payload):
        code = first_non_empty_string(original_error.get("postType"))
        if code:
            return code

    return None


def extract_merge_error_message_with_source(payload: dict) -> tuple[str, str]:
    """Extract the best human-readable message and note where it came from."""
    message = first_non_empty_string(
        payload.get("message"),
        payload.get("errorMessage"),
        payload.get("detail"),
        payload.get("reason"),
        payload.get("error"),
        payload.get("description"),
        payload.get("title"),
    )
    if message:
        return message, "topLevel"

    for original_error in iterate_original_error_payloads(payload):
        body = original_error.get("body")

        if isinstance(body, dict):
            body_message = first_non_empty_string(
                body.get("message"),
                body.get("errorMessage"),
                body.get("detail"),
                body.get("reason"),
                body.get("description"),
                body.get("title"),
            )
            if body_message:
                return body_message, "originalError.body"

            body_errors = body.get("errors")
            if isinstance(body_errors, list):
                for entry in body_errors:
                    if not isinstance(entry, dict):
                        continue
                    nested_body_message = first_non_empty_string(
                        entry.get("message"),
                        entry.get("errorMessage"),
                        entry.get("detail"),
                        entry.get("reason"),
                        entry.get("description"),
                        entry.get("error"),
                    )
                    if nested_body_message:
                        return nested_body_message, "originalError.body.errors"
        elif isinstance(body, str) and body.strip():
            xml_message = extract_message_from_xml_body(body)
            if xml_message:
                return xml_message, "originalError.body.xml"
            if not body.strip().startswith("<"):
                return body.strip(), "originalError.body"

        original_error_message = first_non_empty_string(
            original_error.get("message"),
            original_error.get("errorMessage"),
            original_error.get("detail"),
            original_error.get("reason"),
            original_error.get("description"),
            original_error.get("error"),
        )
        if original_error_message:
            return original_error_message, "originalError"

    nested_errors = payload.get("errors")
    if isinstance(nested_errors, list):
        nested_messages = [
            first_non_empty_string(
                entry.get("message"),
                entry.get("errorMessage"),
                entry.get("detail"),
                entry.get("reason"),
                entry.get("error"),
            )
            for entry in nested_errors
            if isinstance(entry, dict)
        ]
        for nested_message in nested_messages:
            if nested_message:
                return nested_message, "payload.errors"

    return "Unspecified upstream merge error", "fallback"


def extract_merge_error_entity_type(payload: dict) -> Optional[str]:
    """Extract the entity type involved in a merge error."""
    entity_type = first_non_empty_string(
        payload.get("entityType"),
        payload.get("entityName"),
        payload.get("mergeType"),
        payload.get("objectType"),
    )
    if entity_type:
        return entity_type

    type_value = payload.get("type")
    if isinstance(type_value, str) and type_value.strip():
        lowered = type_value.strip().lower()
        if lowered not in {"error", "validation", "warning"}:
            return type_value.strip()

    return None


def extract_merge_error_code(payload: dict) -> Optional[str]:
    """Extract a stable machine-readable code when available."""
    return classify_canonical_error_code(payload)["canonicalErrorCode"]


def build_error_signature_label(
    entity_type: Optional[str],
    error_code: Optional[str],
    normalized_message: str,
) -> str:
    """Build a readable signature label for aggregate and breakdown views."""
    parts = [entity_type or "unknown-entity"]
    if error_code:
        parts.append(error_code)
    parts.append(normalized_message)
    return " | ".join(parts)


def build_legacy_signature_key(
    entity_type: Optional[str],
    legacy_error_code: Optional[str],
    normalized_message: str,
) -> str:
    """Preserve the legacy v1 signature basis for safe side-by-side history."""
    signature_basis = "|".join(
        [
            entity_type or "unknown-entity",
            legacy_error_code or "unknown-code",
            normalized_message,
        ]
    )
    return hashlib.sha1(signature_basis.encode("utf-8")).hexdigest()[:16]


def normalize_merge_error_row(payload: dict) -> dict:
    """Normalize a raw merge-error row into a grouping-friendly shape."""
    sample_message, message_source = extract_merge_error_message_with_source(payload)
    normalized_message = normalize_error_message(sample_message)
    entity_type = extract_merge_error_entity_type(payload)
    code_classification = classify_canonical_error_code(payload)
    canonical_error_code = code_classification["canonicalErrorCode"]
    operation_name = code_classification["operationName"]
    signature_strategy = (
        ERROR_SIGNATURE_STRATEGY_CODE_TEMPLATE
        if canonical_error_code
        else ERROR_SIGNATURE_STRATEGY_TEMPLATE_FALLBACK
    )
    signature_version = ERROR_SIGNATURE_VERSION_V2
    signature_confidence = (
        ERROR_SIGNATURE_CODE_TEMPLATE_CONFIDENCE
        if canonical_error_code
        else ERROR_SIGNATURE_TEMPLATE_FALLBACK_CONFIDENCE
    )
    signature_label = build_error_signature_label(entity_type, canonical_error_code, normalized_message)
    if canonical_error_code:
        signature_basis = "|".join(
            [
                entity_type or "unknown-entity",
                canonical_error_code,
                normalized_message,
            ]
        )
    else:
        signature_basis = "|".join(
            [
                entity_type or "unknown-entity",
                normalized_message,
            ]
        )
    signature_key = hashlib.sha1(signature_basis.encode("utf-8")).hexdigest()[:16]
    legacy_error_code = extract_legacy_merge_error_code(payload)
    return {
        "entityType": entity_type,
        "errorCode": canonical_error_code,
        "canonicalErrorCode": canonical_error_code,
        "canonicalCodeSource": code_classification["canonicalCodeSource"],
        "operationName": operation_name,
        "signatureKey": signature_key,
        "legacySignatureKey": build_legacy_signature_key(entity_type, legacy_error_code, normalized_message),
        "signatureVersion": signature_version,
        "signatureStrategy": signature_strategy,
        "signatureConfidence": signature_confidence,
        "signatureLabel": signature_label,
        "messageTemplate": normalized_message,
        "messageSource": message_source,
        "normalizedMessage": normalized_message,
        "sampleMessage": sample_message,
        "termCodes": collect_term_codes(payload),
        "rawError": payload,
    }


def build_error_analysis_details(
    snapshot_date: str,
    school: str,
    display_name: str,
    sis_platform: Optional[str],
    merge_error_rows: list[dict],
) -> list[dict]:
    """Normalize every raw merge-error row into a searchable persisted detail row."""
    details: list[dict] = []

    for row in merge_error_rows:
        normalized_row = normalize_merge_error_row(row)
        merge_report_reference = extract_merge_report_reference([row])
        details.append(
            {
                "snapshotDate": snapshot_date,
                "school": school,
                "displayName": display_name,
                "sisPlatform": sis_platform,
                "entityType": normalized_row["entityType"],
                "errorCode": normalized_row["errorCode"],
                "canonicalErrorCode": normalized_row["canonicalErrorCode"],
                "canonicalCodeSource": normalized_row["canonicalCodeSource"],
                "operationName": normalized_row["operationName"],
                "signatureKey": normalized_row["signatureKey"],
                "legacySignatureKey": normalized_row["legacySignatureKey"],
                "signatureVersion": normalized_row["signatureVersion"],
                "signatureStrategy": normalized_row["signatureStrategy"],
                "signatureConfidence": normalized_row["signatureConfidence"],
                "signatureLabel": normalized_row["signatureLabel"],
                "messageTemplate": normalized_row["messageTemplate"],
                "normalizedMessage": normalized_row["normalizedMessage"],
                "fullErrorText": normalized_row["sampleMessage"],
                "entityDisplayName": first_non_empty_string(row.get("entityDisplayName"), row.get("entityName")),
                "mergeReport": (
                    {
                        **merge_report_reference,
                        "school": school,
                        "snapshotDate": snapshot_date,
                    }
                    if merge_report_reference
                    else None
                ),
                "termCodes": normalized_row["termCodes"],
                "rawError": row,
            }
        )

    return details


def build_error_analysis_groups(
    snapshot_date: str,
    school: str,
    display_name: str,
    sis_platform: Optional[str],
    merge_error_rows: list[dict],
) -> list[dict]:
    """Group raw merge-error rows by normalized signature for one school snapshot."""
    grouped: dict[str, dict] = {}

    for row in merge_error_rows:
        normalized_row = normalize_merge_error_row(row)
        signature_key = normalized_row["signatureKey"]
        existing = grouped.get(signature_key)

        if existing is None:
            existing = {
                "snapshotDate": snapshot_date,
                "school": school,
                "displayName": display_name,
                "sisPlatform": sis_platform,
                "entityType": normalized_row["entityType"],
                "errorCode": normalized_row["errorCode"],
                "canonicalErrorCode": normalized_row["canonicalErrorCode"],
                "canonicalCodeSource": normalized_row["canonicalCodeSource"],
                "operationName": normalized_row["operationName"],
                "signatureKey": signature_key,
                "legacySignatureKey": normalized_row["legacySignatureKey"],
                "signatureVersion": normalized_row["signatureVersion"],
                "signatureStrategy": normalized_row["signatureStrategy"],
                "signatureConfidence": normalized_row["signatureConfidence"],
                "signatureLabel": normalized_row["signatureLabel"],
                "messageTemplate": normalized_row["messageTemplate"],
                "normalizedMessage": normalized_row["normalizedMessage"],
                "sampleMessage": normalized_row["sampleMessage"],
                "count": 0,
                "sampleErrors": [],
                "termCodes": set(),
            }
            grouped[signature_key] = existing

        existing["count"] += 1
        existing["termCodes"].update(normalized_row["termCodes"])
        if len(existing["sampleErrors"]) < ERROR_ANALYSIS_SAMPLE_LIMIT:
            existing["sampleErrors"].append(normalized_row["rawError"])

    normalized_groups: list[dict] = []
    for group in grouped.values():
        normalized_groups.append(
            {
                **group,
                "termCodes": sorted(group["termCodes"]),
            }
        )

    return sorted(normalized_groups, key=lambda group: (-group["count"], group["signatureKey"]))


async def fetch_all_merge_error_rows(school: str) -> tuple[list[dict], int]:
    """Fetch the full open merge-error inventory for a school via pagination."""
    all_rows: list[dict] = []
    total_count: Optional[int] = None

    for page in range(MERGE_ERRORS_MAX_PAGES):
        payload = await api_get(
            f"/api/v1/int/{school}/integrations-hub/overview/merge-errors",
            params={"page": str(page), "size": str(MERGE_ERRORS_PAGE_SIZE)},
        )
        page_rows = extract_merge_error_rows(payload)
        page_total = extract_merge_error_count(payload)
        if total_count is None:
            total_count = page_total
        all_rows.extend(page_rows)

        if not page_rows:
            break
        if total_count is not None and len(all_rows) >= total_count:
            break
        if len(page_rows) < MERGE_ERRORS_PAGE_SIZE:
            break

    return all_rows, (total_count if total_count is not None else len(all_rows))


def build_resolution_hint(normalized_message: str, entity_type: Optional[str], error_code: Optional[str]) -> dict:
    """Return a likely next-step recommendation for a normalized error signature."""
    haystack = " ".join(part for part in [normalized_message, entity_type or "", error_code or ""] if part).lower()

    if any(token in haystack for token in ("missing", "not found", "not.found", "unknown", "reference", "dependency", "prerequisite", "does not exist")):
        return {
            "bucket": "missing_reference",
            "title": "Missing dependency or reference",
            "action": "Verify the referenced records exist in the SIS and are synced before retrying dependent entities.",
            "rationale": "The signature reads like a missing upstream dependency or lookup reference.",
            "confidence": 0.82,
        }

    if any(token in haystack for token in ("duplicate", "already exists", "conflict", "unique", "overlap", "overlapping")):
        return {
            "bucket": "duplicate_conflict",
            "title": "Duplicate or conflicting record",
            "action": "Check for duplicate source records or conflicting identifiers and resolve the collision before rerunning the sync.",
            "rationale": "The signature points to a duplicate, uniqueness, or conflicting-write condition.",
            "confidence": 0.79,
        }

    if any(token in haystack for token in ("invalid", "validation", "required", "malformed", "format", "parse", "cannot parse", "type mismatch", "schema", "null")):
        return {
            "bucket": "validation_data_shape",
            "title": "Validation or data-shape issue",
            "action": "Review the source payload for missing required fields, invalid formats, or schema mismatches before the next sync.",
            "rationale": "The signature suggests the payload shape or field values do not satisfy validation.",
            "confidence": 0.77,
        }

    if any(token in haystack for token in ("auth", "permission", "unauthorized", "forbidden", "token", "credential", "config", "mapping", "disabled")):
        return {
            "bucket": "configuration_auth",
            "title": "Configuration or access problem",
            "action": "Review integration credentials, permissions, and mapping/configuration settings for the affected entity type.",
            "rationale": "The signature looks tied to authentication, permissions, or configuration drift.",
            "confidence": 0.74,
        }

    return {
        "bucket": "generic_investigation",
        "title": "General investigation recommended",
        "action": "Inspect a sample error in Integration Hub, compare recent changes for the entity type, and confirm whether the issue is isolated to one school or SIS.",
        "rationale": "The signature is not specific enough for a stronger automatic recommendation.",
        "confidence": 0.52,
    }


def extract_merge_report_reference(sample_errors: list[dict]) -> Optional[dict]:
    """Extract the best available merge-report reference from stored sample errors."""
    for sample_error in sample_errors:
        if not isinstance(sample_error, dict):
            continue

        merge_report_id = first_non_empty_string(
            sample_error.get("lastSyncMergeReportId"),
            sample_error.get("mergeReportId"),
            sample_error.get("mergeReport", {}).get("id")
            if isinstance(sample_error.get("mergeReport"), dict)
            else None,
        )
        if not merge_report_id:
            continue

        merge_report = sample_error.get("mergeReport")
        schedule_type = (
            merge_report.get("scheduleType")
            if isinstance(merge_report, dict)
            else None
        )
        entity_display_name = first_non_empty_string(
            sample_error.get("entityDisplayName"),
            sample_error.get("entityId"),
        )
        return {
            "mergeReportId": merge_report_id,
            "scheduleType": schedule_type,
            "entityDisplayName": entity_display_name,
        }

    return None


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


def _collect_nightly_merge_batches(merge_entries: list[dict]) -> list[dict[str, int]]:
    """Normalize nightly merge reports into batch spans with start/end timestamps."""
    groups: dict[str, dict[str, list[int]]] = {}
    fallback_days: dict[str, dict[str, list[int]]] = {}

    for report in merge_entries:
        if str(report.get("scheduleType", "")).lower() == "nightly":
            start_val = report.get("timestampStart") or report.get("startedAt")
            end_val = report.get("timestampEnd") or report.get("completedAt")

            if not isinstance(end_val, (int, float)):
                continue

            if isinstance(start_val, (int, float)) and start_val <= end_val:
                group_id = report.get("groupId")
                if group_id:
                    if group_id not in groups:
                        groups[group_id] = {"starts": [], "ends": []}
                    groups[group_id]["starts"].append(int(start_val))
                    groups[group_id]["ends"].append(int(end_val))
                    continue

            day_key = datetime.fromtimestamp(end_val / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            if day_key not in fallback_days:
                fallback_days[day_key] = {"starts": [], "ends": []}
            fallback_days[day_key]["starts"].append(int(start_val) if isinstance(start_val, (int, float)) else int(end_val))
            fallback_days[day_key]["ends"].append(int(end_val))

    batches: list[dict[str, int]] = []
    for g in groups.values():
        batches.append({
            "start": int(min(g["starts"])),
            "end": int(max(g["ends"])),
        })
    for batch in fallback_days.values():
        batches.append({
            "start": int(min(batch["starts"])),
            "end": int(max(batch["ends"])),
        })

    return batches


def calculate_nightly_merge_time(merge_entries: list[dict]) -> int:
    """Calculate the duration spanned by nightly merges in the provided entries.

    Prefer explicit start/end timestamps and group ids when present. When upstream only
    provides completion timestamps, fall back to the earliest and latest nightly event
    observed on the same UTC day so long-running nightly batches are still represented.
    """
    batches = _collect_nightly_merge_batches(merge_entries)
    total_duration = 0

    for batch in batches:
        total_duration += max(0, int(batch["end"] - batch["start"]))

    return max(0, total_duration)


def calculate_latest_nightly_merge_time(merge_entries: list[dict]) -> int:
    """Return the duration of the most recent nightly batch in the provided entries."""
    batches = _collect_nightly_merge_batches(merge_entries)
    if not batches:
        return 0
    latest_batch = max(batches, key=lambda batch: batch["end"])
    return max(0, int(latest_batch["end"] - latest_batch["start"]))


def _normalize_merge_identifier(report: dict) -> str:
    return str(report.get("id") or report.get("_id") or "unknown")


def _extract_stage_status_from_payload(payload: Any, stage_name: str) -> Optional[str]:
    """Search a merge-report payload recursively for the named stage status."""
    normalized_stage_name = stage_name.strip().lower()

    def walk(node: Any) -> Optional[str]:
        if isinstance(node, list):
            for item in node:
                status = walk(item)
                if status:
                    return status
            return None

        if not isinstance(node, dict):
            return None

        candidate_name = next(
            (
                str(node.get(key)).strip()
                for key in ("step", "stage", "name", "title", "label")
                if isinstance(node.get(key), str) and str(node.get(key)).strip()
            ),
            None,
        )
        candidate_status = node.get("status")
        if (
            candidate_name
            and candidate_name.lower() == normalized_stage_name
            and isinstance(candidate_status, str)
            and candidate_status.strip()
        ):
            return candidate_status.strip()

        for value in node.values():
            status = walk(value)
            if status:
                return status

        return None

    return walk(payload)


async def classify_nightly_merge_halt_status(school: str, merge_report_id: str) -> Optional[str]:
    """Return the halt status for the nightly sync stage when present."""
    try:
        payload = await api_get(f"/api/v1/{school}/mergeReports/{merge_report_id}")
    except Exception:
        payload = None

    stage_status = _extract_stage_status_from_payload(payload, HALTED_STAGE_NAME)
    if stage_status:
        return stage_status

    try:
        post_step_updates = await api_get(
            f"/api/v1/int/{school}/integrations-hub/merge-history/{merge_report_id}/post-step-updates",
            params={"page": "0", "size": "1"},
        )
    except Exception:
        return None

    update_count = post_step_updates.get("count")
    if isinstance(update_count, int) and update_count > CHANGE_THRESHOLD_LIMIT:
        return HALTED_CHANGE_THRESHOLD_STATUS

    return None


async def annotate_nightly_halted_merges(school: str, merge_entries: list[dict]) -> list[dict]:
    """Annotate nightly finished-with-issues merges with halt reason metadata."""
    annotated_entries = [dict(entry) for entry in merge_entries if isinstance(entry, dict)]
    if not annotated_entries:
        return []

    async def annotate_entry(index: int, entry: dict) -> None:
        schedule = str(entry.get("scheduleType", "")).lower()
        status = str(entry.get("status", "")).lower()
        merge_report_id = _normalize_merge_identifier(entry)
        if schedule != "nightly" or status != "finishedwithissues" or merge_report_id == "unknown":
            return

        try:
            stage_status = await classify_nightly_merge_halt_status(school, merge_report_id)
        except Exception as exc:
            logger.warning(
                "Failed to inspect merge report detail for halt classification school=%s merge_report_id=%s error=%s",
                school,
                merge_report_id,
                exc,
            )
            return

        if stage_status == HALTED_CHANGE_THRESHOLD_STATUS:
            annotated_entries[index]["haltReason"] = stage_status
            annotated_entries[index]["statusDetail"] = stage_status

    await asyncio.gather(*(annotate_entry(index, entry) for index, entry in enumerate(annotated_entries)))
    return annotated_entries


def summarize_nightly_outcomes(merge_entries: list[dict]) -> tuple[int, int, int]:
    """Count nightly merges that finished with issues, had no data, or halted."""
    nightly_issues = nightly_nodata = nightly_halted = 0

    for report in merge_entries:
        if str(report.get("scheduleType", "")).lower() != "nightly":
            continue

        if str(report.get("haltReason", "")).strip() == HALTED_CHANGE_THRESHOLD_STATUS:
            nightly_halted += 1
            continue

        status = str(report.get("status", "")).lower()
        if status == "finishedwithissues":
            nightly_issues += 1
        elif status == "nodata":
            nightly_nodata += 1

    return nightly_issues, nightly_nodata, nightly_halted


def merge_recent_merge_entries(primary_entries: list[dict], extra_entries: list[dict]) -> list[dict]:
    """Combine recent failed and halted merge entries without duplicating ids."""
    merged: list[dict] = []
    seen_ids: set[str] = set()

    for source in (primary_entries, extra_entries):
        for entry in source:
            if not isinstance(entry, dict):
                continue
            normalized = dict(entry)
            merge_id = _normalize_merge_identifier(normalized)
            if merge_id in seen_ids:
                continue
            seen_ids.add(merge_id)
            if "id" not in normalized:
                normalized["id"] = merge_id
            merged.append(normalized)

    merged.sort(
        key=lambda entry: str(entry.get("timestampEnd") or ""),
        reverse=True,
    )
    return merged[:10]


def build_snapshot_from_merge_history(
    school: str,
    display_name: str,
    sis_platform: Optional[str],
    products: list,
    snapshot_date: str,
    merge_entries: list[dict],
    active_users_24h: int,
    active_users: Optional[list[str]] = None,
) -> dict:
    """Build a historical snapshot from merge history plus user activity."""
    nightly_total = nightly_success = nightly_issues = nightly_nodata = nightly_halted = 0
    realtime_total = realtime_success = realtime_issues = realtime_nodata = 0
    manual_total = manual_success = manual_issues = manual_nodata = 0
    failed_entries: list[dict] = []
    halted_entries: list[dict] = []
    nightly_merge_time_ms = calculate_nightly_merge_time(merge_entries)

    for report in merge_entries:
        schedule = str(report.get("scheduleType", "")).lower()
        status = str(report.get("status", "")).lower()
        halt_reason = str(report.get("haltReason", "")).strip()
        succeeded = status == "success"
        has_issues = status == "finishedwithissues"
        no_data = status == "nodata"
        halted = halt_reason == HALTED_CHANGE_THRESHOLD_STATUS
        failed = not (succeeded or has_issues or no_data)

        if schedule == "nightly":
            nightly_total += 1
            if succeeded:
                nightly_success += 1
            elif halted:
                nightly_halted += 1
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

        merge_entry = {
            "id": _normalize_merge_identifier(report),
            "mergeReportId": _normalize_merge_identifier(report),
            "type": report.get("type"),
            "scheduleType": report.get("scheduleType"),
            "timestampEnd": report.get("timestampEnd"),
            "status": report.get("status"),
        }
        if report.get("statusDetail"):
            merge_entry["statusDetail"] = report.get("statusDetail")
        if halt_reason:
            merge_entry["haltReason"] = halt_reason
            merge_entry["statusDetail"] = report.get("statusDetail") or halt_reason

        if schedule == "nightly" and halted:
            halted_entries.append(merge_entry)
        elif failed:
            failed_entries.append(merge_entry)

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
                "failed": nightly_total - nightly_success - nightly_issues - nightly_nodata - nightly_halted,
                "finishedWithIssues": nightly_issues,
                "noData": nightly_nodata,
                "halted": nightly_halted,
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
        "recentFailedMerges": merge_recent_merge_entries(failed_entries, halted_entries),
        # Historical backfills do not have a trustworthy per-day open merge error total.
        "mergeErrorsCount": None,
        "activeUsers24h": active_users_24h,
        "activeUsers": active_users or [],
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
        merge_error_rows: list[dict] = []
        merge_errors_count = 0

        # Fetch integration health (nightly merge stats + recent failed merges)
        health_data = {}
        try:
            health_data = await api_get(f"/api/v1/int/{school}/integrations-hub/overview/health")
        except Exception as e:
            sync_errors.append(f"{school}: health request failed ({e})")

        # Fetch merge errors
        try:
            merge_error_rows, merge_errors_count = await fetch_all_merge_error_rows(school)
        except Exception as e:
            sync_errors.append(f"{school}: merge errors request failed ({e})")

        # Fetch recent merge history to compute realtime/manual stats
        merge_reports: list = []
        nightly_merge_reports: list = []
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        last_24h = now_ms - (24 * 60 * 60 * 1000)
        last_48h = now_ms - (48 * 60 * 60 * 1000)
        date_from = datetime.fromtimestamp(last_24h / 1000, tz=timezone.utc).isoformat()
        nightly_date_from = datetime.fromtimestamp(last_48h / 1000, tz=timezone.utc).isoformat()
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

        # Fetch nightly merge history across the same 48-hour window used by upstream health.
        try:
            nightly_merge_reports_data = await api_get(
                f"/api/v1/int/{school}/integrations-hub/merge-history",
                params={
                    "page": "0",
                    "size": "500",
                    "dateFrom": nightly_date_from,
                    "dateTo": date_to,
                    "scheduleType": "nightly",
                },
            )
            nightly_merge_reports = extract_merge_history_entries(nightly_merge_reports_data)
        except Exception as e:
            sync_errors.append(f"{school}: nightly merge history request failed ({e})")

        nightly_merge_reports = await annotate_nightly_halted_merges(school, nightly_merge_reports)

        # Parse health data
        nightly_total = health_data.get("allNightlyMergesCount", 0)
        nightly_success = health_data.get("succeededNightlyMergesCount", 0)
        nightly_issues, nightly_nodata, nightly_halted = summarize_nightly_outcomes(nightly_merge_reports)
        recent_failed = merge_recent_merge_entries(
            health_data.get("recentFailedMerges", []),
            [
                {
                    "id": _normalize_merge_identifier(report),
                    "type": report.get("type"),
                    "scheduleType": report.get("scheduleType"),
                    "timestampEnd": report.get("timestampEnd"),
                    "status": report.get("status"),
                    "haltReason": report.get("haltReason"),
                    "statusDetail": report.get("statusDetail") or report.get("haltReason"),
                }
                for report in nightly_merge_reports
                if str(report.get("haltReason", "")).strip() == HALTED_CHANGE_THRESHOLD_STATUS
            ],
        )

        # Compute realtime/manual from merge reports
        (
            realtime_total, realtime_success, realtime_issues, realtime_nodata,
            manual_total, manual_success, manual_issues, manual_nodata
        ) = summarize_recent_merge_activity(merge_reports, last_24h)

        # Compute nightly merge time from merge reports history
        nightly_merge_time = calculate_latest_nightly_merge_time(nightly_merge_reports)

        # Active users — try to get from user activity endpoint
        active_users_24h = 0
        active_users: list[str] = []
        try:
            activity_data = await api_get(
                f"/api/v1/{school}/userActivity",
                params={"after": str(last_24h)},
            )
            active_users = extract_unique_activity_users(activity_data)
            active_users_24h = len(active_users)
        except Exception as e:
            sync_errors.append(f"{school}: user activity request failed ({e})")

        if len(sync_errors) == 5:
            raise RuntimeError(f"{school}: all upstream requests failed")

        sis_platform = None
        try:
            sis_platform = await fetch_school_sis_platform(school)
        except Exception as e:
            sync_errors.append(f"{school}: integration config request failed ({e})")

        error_groups = build_error_analysis_groups(
            snapshot_date=snapshot_date,
            school=school,
            display_name=display_name,
            sis_platform=sis_platform,
            merge_error_rows=merge_error_rows,
        )
        error_details = build_error_analysis_details(
            snapshot_date=snapshot_date,
            school=school,
            display_name=display_name,
            sis_platform=sis_platform,
            merge_error_rows=merge_error_rows,
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
                    "failed": max(0, nightly_total - nightly_success - nightly_issues - nightly_nodata - nightly_halted),
                    "finishedWithIssues": nightly_issues,
                    "noData": nightly_nodata,
                    "halted": nightly_halted,
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
            "activeUsers": active_users,
            "createdAt": datetime.now(timezone.utc).isoformat(),
            "_errorGroups": error_groups,
            "_errorDetails": error_details,
            "_syncErrors": sync_errors,
        }
    except Exception as e:
        raise RuntimeError(f"Error fetching health for {school}: {e}") from e


async def fetch_school_health_for_date(
    school: str, display_name: str, products: list, snapshot_date: str
) -> dict:
    """Fetch one day's historical health data for a school."""
    today_snapshot_date = get_new_york_snapshot_date()
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
    if not merge_entries and sync_errors:
        raise RuntimeError("; ".join(sync_errors))
    merge_entries = await annotate_nightly_halted_merges(school, merge_entries)

    active_users_24h = 0
    active_users: list[str] = []
    try:
        activity_data = await api_get(
            f"/api/v1/{school}/userActivity",
            params={
                "after": str(int(day_start.timestamp() * 1000)),
                "before": str(int(day_end.timestamp() * 1000)),
            },
        )
        active_users = extract_unique_activity_users(activity_data)
        active_users_24h = len(active_users)
    except Exception as e:
        # Non-fatal: log the failure and continue with active_users_24h=0
        # so a bad activity endpoint doesn't abort the whole work unit.
        sync_errors.append(f"{school} {snapshot_date}: user activity request failed ({e})")
        logger.warning(
            "Backfill activity fetch failed school=%s snapshot_date=%s error=%s",
            school,
            snapshot_date,
            e,
        )

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
        active_users=active_users,
    )
    snapshot["_syncErrors"] = sync_errors
    return snapshot


async def run_history_backfill_job(
    job_id: str,
    school: Optional[str],
    start_date: str,
    end_date: str,
    mode: str = "initial",
):
    """Backfill historical daily snapshots for one school or all schools."""
    global _active_sync_job_id
    job = _sync_jobs[job_id]
    started_at = datetime.now(timezone.utc)
    job["status"] = "running"
    job["startedAt"] = started_at.isoformat()
    update_backfill_job_state(job, now=started_at, checkpoint_status="running")

    start_time = time.time()
    db = get_db()
    try:
        sync_run = db.query(SyncRun).filter(SyncRun.job_id == job_id).first()
        if sync_run is None:
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
    finally:
        db.close()

    try:
        # Start each backfill with a fresh upstream auth session when
        # credentials are configured. Mid-run expiry is still handled by api_get().
        await authenticate_api()
        schools_resp = await list_schools()
        schools = select_schools_for_sync(schools_resp["schools"], school)
        list_time = time.time()
        snapshot_dates = build_snapshot_dates(start_date, end_date)
        total_work_units = len(schools) * len(snapshot_dates)
        job["totalSchools"] = total_work_units
        job["schoolsProcessed"] = job.get("completedUnits", 0) + job.get("failedUnits", 0) + job.get("skippedUnits", 0)
        job["statusDetail"] = "running"
        db = get_db()
        try:
            if mode == "initial":
                seed_backfill_work_units(db, job_id, schools, snapshot_dates)
            elif mode == "resume":
                reset_backfill_work_units(db, job_id, {"pending", "failed", "running"})
            elif mode == "retry_failures":
                reset_backfill_work_units(db, job_id, {"failed"})
            db.commit()
        finally:
            db.close()

        db = get_db()
        try:
            status_counts = summarize_backfill_work_unit_statuses(db, job_id)
        finally:
            db.close()
        job["completedUnits"] = status_counts["completed"]
        job["failedUnits"] = status_counts["failed"]
        job["skippedUnits"] = status_counts["skipped"]
        job["schoolsProcessed"] = (
            job["completedUnits"] + job["failedUnits"] + job["skippedUnits"]
        )
        job["totalSchools"] = sum(status_counts.values())
        persist_job_state(job_id)

        db = get_db()
        try:
            work_units = get_backfill_work_units(db, job_id, {"pending"})
        finally:
            db.close()
        job["schoolsProcessed"] = job.get("completedUnits", 0) + job.get("failedUnits", 0) + job.get("skippedUnits", 0)
        persist_job_state(job_id)

        for unit in work_units:
            attempt_number = 1
            db = get_db()
            try:
                if backfill_snapshot_exists(db, unit.school, unit.snapshot_date):
                    db_unit = db.query(BackfillWorkUnit).filter(BackfillWorkUnit.id == unit.id).first()
                    if db_unit:
                        db_unit.status = "skipped"
                        db_unit.last_error = "Snapshot already exists; skipped re-fetch"
                        db_unit.completed_at = datetime.now(timezone.utc)
                    db.commit()
                    update_backfill_job_state(
                        job,
                        current_school=unit.school,
                        current_snapshot_date=unit.snapshot_date,
                        skipped_delta=1,
                        checkpoint_status="running",
                    )
                    persist_job_state(job_id)
                    logger.info(
                        "Backfill unit skipped job_id=%s school=%s snapshot_date=%s mode=%s",
                        job_id,
                        unit.school,
                        unit.snapshot_date,
                        mode,
                    )
                    continue
            finally:
                db.close()

            db = get_db()
            try:
                db_unit = db.query(BackfillWorkUnit).filter(BackfillWorkUnit.id == unit.id).first()
                if not db_unit:
                    continue
                db_unit.status = "running"
                db_unit.attempt_count += 1
                db_unit.last_attempted_at = datetime.now(timezone.utc)
                attempt_number = db_unit.attempt_count
                db.commit()
            finally:
                db.close()

            update_backfill_job_state(
                job,
                current_school=unit.school,
                current_snapshot_date=unit.snapshot_date,
                checkpoint_status="running",
            )
            persist_job_state(job_id)
            logger.info(
                "Backfill unit started job_id=%s school=%s snapshot_date=%s attempt=%s mode=%s",
                job_id,
                unit.school,
                unit.snapshot_date,
                attempt_number,
                mode,
            )

            try:
                result = await fetch_school_health_for_date(
                    school=unit.school,
                    display_name=unit.display_name,
                    products=json.loads(unit.products_json) if unit.products_json else [],
                    snapshot_date=unit.snapshot_date,
                )
                sync_errors = result.pop("_syncErrors", [])

                db = get_db()
                try:
                    db.query(SchoolSnapshot).filter(
                        SchoolSnapshot.school == unit.school,
                        SchoolSnapshot.snapshot_date == unit.snapshot_date,
                    ).delete(synchronize_session=False)
                    db.add(SchoolSnapshot.from_dict(result))
                    db_unit = db.query(BackfillWorkUnit).filter(BackfillWorkUnit.id == unit.id).first()
                    if db_unit:
                        db_unit.status = "completed"
                        db_unit.last_error = None
                        db_unit.completed_at = datetime.now(timezone.utc)
                    db.commit()
                finally:
                    db.close()

                for sync_error in sync_errors:
                    update_backfill_job_state(job, error_message=sync_error)

                update_backfill_job_state(
                    job,
                    current_school=unit.school,
                    current_snapshot_date=unit.snapshot_date,
                    completed_delta=1,
                    checkpoint_status="running",
                )
                persist_job_state(job_id)
            except Exception as e:
                failure_message = str(e)
                db = get_db()
                try:
                    db_unit = db.query(BackfillWorkUnit).filter(BackfillWorkUnit.id == unit.id).first()
                    if db_unit:
                        db_unit.status = "failed"
                        db_unit.last_error = failure_message
                    db.commit()
                finally:
                    db.close()

                update_backfill_job_state(
                    job,
                    current_school=unit.school,
                    current_snapshot_date=unit.snapshot_date,
                    failed_delta=1,
                    error_message=failure_message,
                    failed_unit={
                        "school": unit.school,
                        "snapshotDate": unit.snapshot_date,
                        "attemptCount": attempt_number,
                        "error": failure_message,
                    },
                    checkpoint_status="running",
                )
                persist_job_state(job_id)
                logger.warning(
                    "Backfill unit failed job_id=%s school=%s snapshot_date=%s mode=%s error=%s",
                    job_id,
                    unit.school,
                    unit.snapshot_date,
                    mode,
                    failure_message,
                )

        persist_time = time.time()
        fetch_time = persist_time
        job.update(
            {
                "status": "completed",
                "statusDetail": "completed_with_failures" if job.get("failedUnits", 0) else "completed",
                "snapshotDate": snapshot_dates[-1],
                "schoolsProcessed": job.get("completedUnits", 0) + job.get("failedUnits", 0) + job.get("skippedUnits", 0),
                "totalSchools": job.get("totalSchools", total_work_units),
                "timing": {
                    "listSchoolsSec": round(list_time - start_time, 2),
                    "fetchHealthSec": round(fetch_time - list_time, 2),
                    "persistDbSec": round(persist_time - fetch_time, 2),
                    "totalSec": round(persist_time - start_time, 2),
                },
                "finishedAt": datetime.now(timezone.utc).isoformat(),
            }
        )
        if job.get("failedUnits", 0):
            job["failureReason"] = f"{job['failedUnits']} work unit(s) failed"
        else:
            job["failureReason"] = None
        update_backfill_job_state(job, checkpoint_status=job["statusDetail"])
        persist_job_state(job_id)
        logger.info(
            "Backfill job completed job_id=%s mode=%s completed_units=%s failed_units=%s total_units=%s",
            job_id,
            mode,
            job.get("completedUnits", 0),
            job.get("failedUnits", 0),
            len(work_units),
        )
    except Exception as e:
        finished_at = datetime.now(timezone.utc)
        job.update(
            {
                "status": "failed",
                "statusDetail": "failed",
                "failureReason": str(e),
                "error": str(e),
                "finishedAt": finished_at.isoformat(),
            }
        )
        update_backfill_job_state(job, checkpoint_status="failed")
        persist_job_state(job_id)
        # Also mark the SyncRun DB record as failed so it isn't left in 'running'.
        db = get_db()
        try:
            sync_run = db.query(SyncRun).filter(SyncRun.job_id == job_id).first()
            if sync_run:
                sync_run.status = "failed"
                sync_run.finished_at = finished_at
                sync_run.error_message = str(e)
                db.commit()
        except Exception as db_err:
            logger.warning("Failed to update SyncRun on job failure job_id=%s: %s", job_id, db_err)
        finally:
            db.close()
        logger.exception("Backfill job failed job_id=%s mode=%s error=%s", job_id, mode, e)
    finally:
        if _active_sync_job_id == job_id:
            _active_sync_job_id = None
