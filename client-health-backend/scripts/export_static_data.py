#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = Path(__file__).resolve().parents[1]

if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db import api_get, close_api, connect_api, get_db, init_db  # noqa: E402
from app.models import ErrorAnalysisDetail, ErrorAnalysisGroup, SchoolSnapshot, SyncRun, serialize_datetime  # noqa: E402
from app.routes import (  # noqa: E402
    EXCLUDED_SCHOOL_TERMS,
    exclude_demo_school_snapshots,
    extract_unique_activity_users,
    get_additional_excluded_schools,
    normalize_school_catalog,
    serialize_persisted_sync_run,
    split_school_catalog,
    snapshot_has_measured_merge_errors,
)


def first_non_empty_string(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def extract_merge_report_reference(sample_errors: list[dict[str, Any]]) -> dict[str, Any] | None:
    for sample_error in sample_errors:
        merge_report = sample_error.get("mergeReport")
        merge_report_payload = merge_report if isinstance(merge_report, dict) else {}
        merge_report_id = first_non_empty_string(
            sample_error.get("lastSyncMergeReportId"),
            sample_error.get("mergeReportId"),
            merge_report_payload.get("id"),
        )
        if not merge_report_id:
            continue

        return {
            "school": "",
            "mergeReportId": merge_report_id,
            "scheduleType": merge_report_payload.get("scheduleType") if isinstance(merge_report_payload.get("scheduleType"), str) else None,
            "entityDisplayName": first_non_empty_string(
                sample_error.get("entityDisplayName"),
                sample_error.get("entityId"),
                sample_error.get("message"),
            ),
            "snapshotDate": "",
        }

    return None


def write_json(path: Path, payload: Any, bundle_root: Path) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, indent=2, sort_keys=True)
    path.write_text(serialized, encoding="utf-8")
    digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    return {
        "path": path.relative_to(bundle_root.parent).as_posix(),
        "sha256": digest,
        "sizeBytes": path.stat().st_size,
    }


async def build_latest_client_health(db) -> dict[str, Any]:
    latest = db.query(SchoolSnapshot.snapshot_date).order_by(SchoolSnapshot.snapshot_date.desc()).first()
    if not latest:
        return {
            "snapshotDate": None,
            "schools": [],
            "excludedSchools": [],
            "excludedTerms": list(EXCLUDED_SCHOOL_TERMS),
            "additionalExcludedSchools": [],
        }

    latest_date = latest[0]
    snapshots = (
        exclude_demo_school_snapshots(db.query(SchoolSnapshot))
        .filter(SchoolSnapshot.snapshot_date == latest_date)
        .order_by(SchoolSnapshot.school)
        .all()
    )
    await connect_api()
    try:
        products_data, display_names_data = await asyncio.gather(
            api_get("/api/v1/admin/schools/products"),
            api_get("/api/v1/admin/schools/displayNames"),
        )
        all_schools = normalize_school_catalog(products_data, display_names_data)
        additional_excluded_schools = get_additional_excluded_schools(db)
        _, excluded_schools = split_school_catalog(all_schools, additional_excluded_schools)
        return {
            "snapshotDate": latest_date,
            "schools": [snapshot.to_dict() for snapshot in snapshots],
            "excludedSchools": excluded_schools,
            "excludedTerms": list(EXCLUDED_SCHOOL_TERMS),
            "additionalExcludedSchools": sorted(additional_excluded_schools),
        }
    finally:
        await close_api()


def build_client_health_history(db) -> dict[str, Any]:
    snapshots = (
        exclude_demo_school_snapshots(db.query(SchoolSnapshot))
        .order_by(SchoolSnapshot.snapshot_date.asc(), SchoolSnapshot.school.asc())
        .all()
    )
    serialized_snapshots: list[dict[str, Any]] = []
    for snapshot in snapshots:
        payload = snapshot.to_dict()
        if not snapshot_has_measured_merge_errors(snapshot.snapshot_date, snapshot.created_at):
            payload["mergeErrorsCount"] = None
        serialized_snapshots.append(payload)

    return {"snapshots": serialized_snapshots}


def build_sync_metadata_payload(db, school: str | None = None) -> dict[str, Any]:
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


def build_active_users_payload(snapshot: SchoolSnapshot | None) -> dict[str, Any]:
    if snapshot is None:
        return {
            "count": 0,
            "users": [],
            "error": "No cached active-user snapshot is available for this school.",
        }

    users = json.loads(snapshot.active_users_json) if snapshot.active_users_json else []
    return {
        "count": snapshot.active_users_24h or 0,
        "users": users,
        **({"error": "The static site includes the latest cached count only. Individual active-user lists require the live backend."} if not users and (snapshot.active_users_24h or 0) > 0 else {}),
        "snapshotDate": snapshot.snapshot_date,
        "capturedAt": serialize_datetime(snapshot.created_at),
    }


async def hydrate_missing_active_users(latest_by_school: dict[str, SchoolSnapshot]) -> dict[str, list[str]]:
    snapshots_to_fetch = {
        school: snapshot
        for school, snapshot in latest_by_school.items()
        if snapshot is not None
        and (snapshot.active_users_24h or 0) > 0
        and not (snapshot.active_users_json or "").strip().strip("[]")
    }
    if not snapshots_to_fetch:
        return {}

    await connect_api()
    try:
        semaphore = asyncio.Semaphore(12)

        async def fetch_one(school: str, snapshot: SchoolSnapshot) -> tuple[str, list[str] | None]:
            async with semaphore:
                try:
                    captured_at = snapshot.created_at.replace(tzinfo=timezone.utc) if snapshot.created_at and snapshot.created_at.tzinfo is None else snapshot.created_at
                    if captured_at is None:
                        return school, None
                    window_end = int(captured_at.timestamp() * 1000)
                    window_start = int((captured_at.timestamp() - 86400) * 1000)
                    payload = await api_get(
                        f"/api/v1/{school}/userActivity",
                        params={
                            "after": str(window_start),
                            "before": str(window_end),
                        },
                    )
                    users = extract_unique_activity_users(payload)
                    return school, users or None
                except Exception:
                    return school, None

        results = await asyncio.gather(
            *(fetch_one(school, snapshot) for school, snapshot in snapshots_to_fetch.items())
        )
        hydrated: dict[str, list[str]] = {}
        for school, users in results:
            if users:
                hydrated[school] = users
        return hydrated
    finally:
        await close_api()


def build_error_summary_export(db, exported_at: str) -> dict[str, Any]:
    history_start = db.query(ErrorAnalysisGroup.snapshot_date).order_by(ErrorAnalysisGroup.snapshot_date.asc()).first()
    last_capture = db.query(ErrorAnalysisGroup.created_at).order_by(ErrorAnalysisGroup.created_at.desc()).first()
    groups = (
        db.query(ErrorAnalysisGroup)
        .order_by(
            ErrorAnalysisGroup.snapshot_date.asc(),
            ErrorAnalysisGroup.school.asc(),
            ErrorAnalysisGroup.signature_key.asc(),
        )
        .all()
    )

    groups_list = []
    for group in groups:
        data = group.to_dict()
        merge_report_reference = extract_merge_report_reference(data["sampleErrors"])
        data["sampleErrors"] = []
        data["latestMergeReport"] = (
            {
                **merge_report_reference,
                "school": group.school,
                "snapshotDate": group.snapshot_date,
            }
            if merge_report_reference
            else None
        )
        groups_list.append(data)

    return {
        "metadata": {
            "historyStartsOn": history_start[0] if history_start else None,
            "lastCapturedAt": serialize_datetime(last_capture[0]) if last_capture else None,
            "exportedAt": exported_at,
        },
        "groups": groups_list,
    }


def build_error_detail_export(db, exported_at: str) -> dict[str, Any]:
    history_start = db.query(ErrorAnalysisDetail.snapshot_date).order_by(ErrorAnalysisDetail.snapshot_date.asc()).first()
    last_capture = db.query(ErrorAnalysisDetail.created_at).order_by(ErrorAnalysisDetail.created_at.desc()).first()
    latest_snapshot = db.query(ErrorAnalysisDetail.snapshot_date).order_by(ErrorAnalysisDetail.snapshot_date.desc()).first()
    
    if latest_snapshot:
        rows = (
            db.query(ErrorAnalysisDetail)
            .filter(ErrorAnalysisDetail.snapshot_date == latest_snapshot[0])
            .order_by(
                ErrorAnalysisDetail.school.asc(),
                ErrorAnalysisDetail.id.asc(),
            )
            .all()
        )
    else:
        rows = []

    rows_list = []
    for row in rows:
        data = row.to_dict()
        rows_list.append(data)

    return {
        "metadata": {
            "historyStartsOn": history_start[0] if history_start else None,
            "lastCapturedAt": serialize_datetime(last_capture[0]) if last_capture else None,
            "exportedAt": exported_at,
        },
        "rows": rows_list,
    }


def build_sync_runs_export(db) -> dict[str, Any]:
    runs = db.query(SyncRun).order_by(SyncRun.attempted_at.desc()).all()
    return {
        "syncRuns": [serialize_persisted_sync_run(run) for run in runs],
        "totalCount": len(runs),
        "limit": len(runs),
        "offset": 0,
    }


def find_latest_snapshot_by_school(db) -> dict[str, SchoolSnapshot]:
    snapshots = (
        exclude_demo_school_snapshots(db.query(SchoolSnapshot))
        .order_by(SchoolSnapshot.school.asc(), SchoolSnapshot.snapshot_date.desc(), SchoolSnapshot.created_at.desc())
        .all()
    )
    latest_by_school: dict[str, SchoolSnapshot] = {}
    for snapshot in snapshots:
        latest_by_school.setdefault(snapshot.school, snapshot)
    return latest_by_school


def export_static_data(output_dir: Path) -> None:
    init_db()
    exported_at = datetime.now(timezone.utc).isoformat()
    output_dir = output_dir.resolve()
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    db = get_db()
    try:
        files: dict[str, dict[str, Any]] = {}
        latest_client_health = asyncio.run(build_latest_client_health(db))
        client_health_history = build_client_health_history(db)
        latest_by_school = find_latest_snapshot_by_school(db)
        hydrated_active_users = asyncio.run(hydrate_missing_active_users(latest_by_school))
        sync_runs = build_sync_runs_export(db)
        error_summary = build_error_summary_export(db, exported_at)
        error_details = build_error_detail_export(db, exported_at)

        files["clientHealthLatest"] = write_json(output_dir / "client-health" / "latest.json", latest_client_health, output_dir)
        files["clientHealthHistory"] = write_json(output_dir / "client-health" / "history.json", client_health_history, output_dir)
        files["errorAnalysisSummary"] = write_json(output_dir / "error-analysis" / "summary.json", error_summary, output_dir)
        files["errorAnalysisErrors"] = write_json(output_dir / "error-analysis" / "errors.json", error_details, output_dir)
        files["syncRuns"] = write_json(output_dir / "jobs" / "sync-runs.json", sync_runs, output_dir)

        all_sync_metadata = build_sync_metadata_payload(db)
        files["clientHealthSyncMetadataAll"] = write_json(
            output_dir / "client-health" / "sync-metadata" / "all.json",
            all_sync_metadata,
            output_dir,
        )

        for school, snapshot in latest_by_school.items():
            encoded_school = quote(school, safe="")
            files[f"clientHealthSyncMetadata:{school}"] = write_json(
                output_dir / "client-health" / "sync-metadata" / f"{encoded_school}.json",
                build_sync_metadata_payload(db, school=school),
                output_dir,
            )
            active_users_payload = build_active_users_payload(snapshot)
            hydrated_users = hydrated_active_users.get(school)
            if hydrated_users:
                active_users_payload["users"] = hydrated_users
                active_users_payload.pop("error", None)
            files[f"clientHealthActiveUsers:{school}"] = write_json(
                output_dir / "client-health" / "active-users" / f"{encoded_school}.json",
                active_users_payload,
                output_dir,
            )

        latest_snapshot_date = latest_client_health.get("snapshotDate")
        manifest = {
            "exportedAt": exported_at,
            "latestSnapshotDate": latest_snapshot_date,
            "referenceDate": latest_snapshot_date or exported_at[:10],
            "dataMode": "static",
            "files": files,
        }
        files["manifest"] = write_json(output_dir / "manifest.json", manifest, output_dir)
    finally:
        db.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export SQLite-backed dashboard data as static JSON assets.")
    parser.add_argument(
        "--output-dir",
        default=str(ROOT / "public" / "static-data"),
        help="Directory to write the static JSON bundle into.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    export_static_data(Path(args.output_dir))
