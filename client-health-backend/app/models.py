"""SQLAlchemy models for persisting client health snapshots."""

import json
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import Column, String, Integer, Text, DateTime, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


def serialize_datetime(value: Optional[datetime]) -> Optional[str]:
    """Serialize persisted datetimes as explicit UTC timestamps.

    SQLite drops tzinfo for DateTime columns, so naive values coming back out of the
    database should be treated as UTC instead of local wall time.
    """
    if value is None:
        return None

    normalized = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    return normalized.isoformat()


class SchoolSnapshot(Base):
    """A single health snapshot for one school on one date."""

    __tablename__ = "school_snapshots"
    __table_args__ = (
        UniqueConstraint("school", "snapshot_date", name="uq_school_date"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_date = Column(String(10), nullable=False, index=True)  # YYYY-MM-DD
    school = Column(String(255), nullable=False, index=True)
    display_name = Column(String(255), nullable=False)
    sis_platform = Column(String(255), nullable=True)
    products_json = Column(Text, default="[]")

    # Nightly merge stats
    nightly_total = Column(Integer, default=0)
    nightly_succeeded = Column(Integer, default=0)
    nightly_failed = Column(Integer, default=0)
    nightly_issues = Column(Integer, default=0)
    nightly_no_data = Column(Integer, default=0)
    nightly_merge_time_ms = Column(Integer, default=0)

    # Realtime merge stats
    realtime_total = Column(Integer, default=0)
    realtime_succeeded = Column(Integer, default=0)
    realtime_failed = Column(Integer, default=0)
    realtime_issues = Column(Integer, default=0)
    realtime_no_data = Column(Integer, default=0)

    # Manual merge stats
    manual_total = Column(Integer, default=0)
    manual_succeeded = Column(Integer, default=0)
    manual_failed = Column(Integer, default=0)
    manual_issues = Column(Integer, default=0)
    manual_no_data = Column(Integer, default=0)

    # Error and activity counts
    recent_failed_merges_json = Column(Text, default="[]")
    merge_errors_count = Column(Integer, default=0)
    active_users_24h = Column(Integer, default=0)

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        """Convert to the JSON shape that the frontend expects."""
        return {
            "snapshotDate": self.snapshot_date,
            "school": self.school,
            "displayName": self.display_name,
            "sisPlatform": self.sis_platform,
            "products": json.loads(self.products_json) if self.products_json else [],
            "merges": {
                "nightly": {
                    "total": self.nightly_total,
                    "succeeded": self.nightly_succeeded,
                    "failed": self.nightly_failed,
                    "finishedWithIssues": self.nightly_issues,
                    "noData": self.nightly_no_data,
                    "mergeTimeMs": self.nightly_merge_time_ms,
                },
                "realtime": {
                    "total": self.realtime_total,
                    "succeeded": self.realtime_succeeded,
                    "failed": self.realtime_failed,
                    "finishedWithIssues": self.realtime_issues,
                    "noData": self.realtime_no_data,
                },
                "manual": {
                    "total": self.manual_total,
                    "succeeded": self.manual_succeeded,
                    "failed": self.manual_failed,
                    "finishedWithIssues": self.manual_issues,
                    "noData": self.manual_no_data,
                },
            },
            "recentFailedMerges": json.loads(self.recent_failed_merges_json)
            if self.recent_failed_merges_json
            else [],
            "mergeErrorsCount": self.merge_errors_count,
            "activeUsers24h": self.active_users_24h,
            "createdAt": serialize_datetime(self.created_at),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SchoolSnapshot":
        """Create a SchoolSnapshot instance from a snapshot dict."""
        merges = data.get("merges", {})
        nightly = merges.get("nightly", {})
        realtime = merges.get("realtime", {})
        manual = merges.get("manual", {})

        return cls(
            snapshot_date=data["snapshotDate"],
            school=data["school"],
            display_name=data.get("displayName", data["school"]),
            sis_platform=data.get("sisPlatform"),
            products_json=json.dumps(data.get("products", [])),
            nightly_total=nightly.get("total", 0),
            nightly_succeeded=nightly.get("succeeded", 0),
            nightly_failed=nightly.get("failed", 0),
            nightly_issues=nightly.get("finishedWithIssues", 0),
            nightly_no_data=nightly.get("noData", 0),
            nightly_merge_time_ms=nightly.get("mergeTimeMs", 0),
            realtime_total=realtime.get("total", 0),
            realtime_succeeded=realtime.get("succeeded", 0),
            realtime_failed=realtime.get("failed", 0),
            realtime_issues=realtime.get("finishedWithIssues", 0),
            realtime_no_data=realtime.get("noData", 0),
            manual_total=manual.get("total", 0),
            manual_succeeded=manual.get("succeeded", 0),
            manual_failed=manual.get("failed", 0),
            manual_issues=manual.get("finishedWithIssues", 0),
            manual_no_data=manual.get("noData", 0),
            recent_failed_merges_json=json.dumps(
                data.get("recentFailedMerges", [])
            ),
            merge_errors_count=data.get("mergeErrorsCount", 0),
            active_users_24h=data.get("activeUsers24h", 0),
        )


class SyncRun(Base):
    """A persisted record of a sync/backfill attempt."""

    __tablename__ = "sync_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(64), nullable=False, unique=True, index=True)
    school = Column(String(255), nullable=True, index=True)
    scope = Column(String(64), nullable=False, index=True)
    status = Column(String(32), nullable=False, index=True)
    snapshot_date = Column(String(10), nullable=True)
    schools_processed = Column(Integer, default=0)
    total_schools = Column(Integer, default=0)
    attempted_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    start_date = Column(String(10), nullable=True)
    end_date = Column(String(10), nullable=True)
    date_count = Column(Integer, nullable=True)
    last_heartbeat_at = Column(DateTime, nullable=True)
    last_progress_at = Column(DateTime, nullable=True)
    current_school = Column(String(255), nullable=True)
    current_snapshot_date = Column(String(10), nullable=True)
    completed_units = Column(Integer, default=0)
    failed_units = Column(Integer, default=0)
    skipped_units = Column(Integer, default=0)
    status_detail = Column(String(64), nullable=True)
    failure_reason = Column(Text, nullable=True)
    failed_units_sample_json = Column(Text, nullable=True)
    checkpoint_state_json = Column(Text, nullable=True)
    errors_json = Column(Text, nullable=True)
    timing_json = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)

    def to_dict(self) -> dict:
        errors = json.loads(self.errors_json) if self.errors_json else []
        timing = json.loads(self.timing_json) if self.timing_json else None
        failed_units_sample = json.loads(self.failed_units_sample_json) if self.failed_units_sample_json else []
        checkpoint_state = json.loads(self.checkpoint_state_json) if self.checkpoint_state_json else None
        return {
            "jobId": self.job_id,
            "school": self.school,
            "scope": self.scope,
            "status": self.status,
            "snapshotDate": self.snapshot_date,
            "schoolsProcessed": self.schools_processed,
            "totalSchools": self.total_schools,
            "attemptedAt": serialize_datetime(self.attempted_at),
            "startedAt": serialize_datetime(self.started_at),
            "finishedAt": serialize_datetime(self.finished_at),
            "startDate": self.start_date,
            "endDate": self.end_date,
            "dateCount": self.date_count,
            "lastHeartbeatAt": serialize_datetime(self.last_heartbeat_at),
            "lastProgressAt": serialize_datetime(self.last_progress_at),
            "currentSchool": self.current_school,
            "currentSnapshotDate": self.current_snapshot_date,
            "completedUnits": self.completed_units,
            "failedUnits": self.failed_units,
            "skippedUnits": self.skipped_units,
            "statusDetail": self.status_detail,
            "failureReason": self.failure_reason,
            "failedUnitsSample": failed_units_sample,
            "checkpointState": checkpoint_state,
            "errors": errors,
            "errorCount": len(errors),
            "timing": timing,
            "errorMessage": self.error_message,
        }


class BackfillWorkUnit(Base):
    """One resumable work item within a historical backfill job."""

    __tablename__ = "backfill_work_units"
    __table_args__ = (
        UniqueConstraint("job_id", "school", "snapshot_date", name="uq_backfill_job_school_date"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(64), nullable=False, index=True)
    school = Column(String(255), nullable=False, index=True)
    display_name = Column(String(255), nullable=False)
    snapshot_date = Column(String(10), nullable=False, index=True)
    products_json = Column(Text, default="[]")
    status = Column(String(32), nullable=False, default="pending", index=True)
    attempt_count = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=True)
    last_attempted_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    def to_dict(self) -> dict:
        return {
            "jobId": self.job_id,
            "school": self.school,
            "displayName": self.display_name,
            "snapshotDate": self.snapshot_date,
            "products": json.loads(self.products_json) if self.products_json else [],
            "status": self.status,
            "attemptCount": self.attempt_count,
            "lastError": self.last_error,
            "lastAttemptedAt": serialize_datetime(self.last_attempted_at),
            "completedAt": serialize_datetime(self.completed_at),
        }


class ExcludedSchool(Base):
    """A locally managed school exclusion for operations and sync selection."""

    __tablename__ = "excluded_schools"

    id = Column(Integer, primary_key=True, autoincrement=True)
    school = Column(String(255), nullable=False, unique=True, index=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    def to_dict(self) -> dict:
        return {
            "school": self.school,
            "createdAt": serialize_datetime(self.created_at),
        }


class SchedulerSettings(Base):
    """Persisted scheduler configuration for the daily sync service."""

    __tablename__ = "scheduler_settings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sync_enabled = Column(Integer, nullable=False, default=1)
    sync_time = Column(String(5), nullable=False, default="07:30")
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), nullable=False)

    def to_dict(self) -> dict:
        return {
            "syncEnabled": bool(self.sync_enabled),
            "syncTime": self.sync_time,
            "updatedAt": serialize_datetime(self.updated_at),
        }
