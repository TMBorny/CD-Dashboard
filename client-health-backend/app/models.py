"""SQLAlchemy models for persisting client health snapshots."""

import json
from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, Text, DateTime, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


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
            "createdAt": self.created_at.isoformat() if self.created_at else None,
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
    errors_json = Column(Text, nullable=True)
    timing_json = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)

    def to_dict(self) -> dict:
        errors = json.loads(self.errors_json) if self.errors_json else []
        timing = json.loads(self.timing_json) if self.timing_json else None
        return {
            "jobId": self.job_id,
            "school": self.school,
            "scope": self.scope,
            "status": self.status,
            "snapshotDate": self.snapshot_date,
            "schoolsProcessed": self.schools_processed,
            "totalSchools": self.total_schools,
            "attemptedAt": self.attempted_at.isoformat() if self.attempted_at else None,
            "startedAt": self.started_at.isoformat() if self.started_at else None,
            "finishedAt": self.finished_at.isoformat() if self.finished_at else None,
            "startDate": self.start_date,
            "endDate": self.end_date,
            "dateCount": self.date_count,
            "errors": errors,
            "errorCount": len(errors),
            "timing": timing,
            "errorMessage": self.error_message,
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
            "createdAt": self.created_at.isoformat() if self.created_at else None,
        }
