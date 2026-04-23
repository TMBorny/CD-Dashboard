"""Database and API client management.

Manages two connections:
1. SQLite via SQLAlchemy for persisting health snapshots
2. httpx AsyncClient for fetching data from the Coursedog API
"""

import asyncio
import os
from pathlib import Path
from typing import Optional

import httpx
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker, Session

from app.models import Base

# All configuration lives in the single root .env.
# The backend reads VITE_* variable names as fallbacks throughout.
_root_env = Path(__file__).parent.parent.parent / ".env"
load_dotenv(_root_env)

# --- Coursedog API client ---

COURSEDOG_BASE_URL = os.getenv(
    "COURSEDOG_BASE_URL",
    os.getenv("VITE_COURSEDOG_PRD_URL", "https://app.coursedog.com"),
)
COURSEDOG_EMAIL = os.getenv("COURSEDOG_EMAIL", os.getenv("VITE_COURSEDOG_EMAIL", ""))
COURSEDOG_PASSWORD = os.getenv(
    "COURSEDOG_PASSWORD",
    os.getenv("VITE_COURSEDOG_PASSWORD", ""),
)

_http_client: Optional[httpx.AsyncClient] = None
_auth_cookie: Optional[str] = None
_auth_lock = asyncio.Lock()


def get_http_client() -> httpx.AsyncClient:
    if _http_client is None:
        raise RuntimeError("HTTP client not initialized")
    return _http_client


async def connect_api():
    """Initialize HTTP client and authenticate with Coursedog."""
    global _http_client, _auth_cookie
    _http_client = httpx.AsyncClient(
        base_url=COURSEDOG_BASE_URL,
        timeout=30.0,
        follow_redirects=True,
    )
    _auth_cookie = None

    await authenticate_api()


def _clear_auth_state(client: httpx.AsyncClient) -> None:
    global _auth_cookie
    _auth_cookie = None
    client.headers.pop("Authorization", None)
    client.headers.pop("cookie", None)
    client.cookies.clear()


async def authenticate_api() -> bool:
    """Authenticate the shared HTTP client with Coursedog."""
    if not COURSEDOG_EMAIL or not COURSEDOG_PASSWORD:
        print("Warning: No COURSEDOG_EMAIL/COURSEDOG_PASSWORD set. API calls will be unauthenticated.")
        return False

    client = get_http_client()

    async with _auth_lock:
        _clear_auth_state(client)

        try:
            resp = await client.post(
                "/api/v1/sessions",
                json={"email": COURSEDOG_EMAIL, "password": COURSEDOG_PASSWORD},
            )
            if resp.status_code in (200, 201):
                # Try token-based auth first (response has {"token": "..."})
                try:
                    resp_data = resp.json()
                except ValueError:
                    resp_data = {}
                token = resp_data.get("token") if isinstance(resp_data, dict) else None
                if token:
                    client.headers["Authorization"] = f"Bearer {token}"

                # Also grab cookies as fallback
                _auth_cookie = resp.headers.get("set-cookie", "")
                if _auth_cookie:
                    client.headers["cookie"] = _auth_cookie
                for cookie_name, cookie_value in resp.cookies.items():
                    client.cookies.set(cookie_name, cookie_value)
                print(f"Authenticated with Coursedog at {COURSEDOG_BASE_URL}")
                return True

            print(f"Warning: Login failed ({resp.status_code}): {resp.text[:200]}")
            return False
        except Exception as e:
            print(f"Warning: Could not authenticate with Coursedog: {e}")
            return False


async def close_api():
    global _http_client, _auth_cookie
    if _http_client:
        await _http_client.aclose()
        _http_client = None
    _auth_cookie = None


async def api_get(path: str, params: Optional[dict] = None) -> dict:
    """Make an authenticated GET request to the Coursedog API."""
    client = get_http_client()
    resp = await client.get(path, params=params)
    if resp.status_code == 401 and await authenticate_api():
        resp = await client.get(path, params=params)
    resp.raise_for_status()
    return resp.json()


# --- SQLite database ---

_engine = None
_SessionLocal = None
_current_database_url = None


def get_default_db_path() -> Path:
    return Path(__file__).parent.parent / "client_health.db"


def get_error_analysis_export_path() -> Path:
    configured_path = os.getenv("CLIENT_HEALTH_ERROR_EXPORT_PATH")
    if configured_path:
        return Path(configured_path).expanduser()

    return get_default_db_path().with_name("error_analysis_export.json")


def get_database_url() -> str:
    database_url = os.getenv("CLIENT_HEALTH_DATABASE_URL")
    if database_url:
        return database_url

    configured_path = os.getenv("CLIENT_HEALTH_DB_PATH")
    if configured_path:
        return f"sqlite:///{Path(configured_path).expanduser()}"

    return f"sqlite:///{get_default_db_path()}"


def init_db():
    """Create the SQLite engine, session factory, and tables."""
    global _engine, _SessionLocal, _current_database_url
    database_url = get_database_url()

    if _engine is not None and _current_database_url != database_url:
        _engine.dispose()
        _engine = None
        _SessionLocal = None

    if _engine is None:
        _engine = create_engine(database_url, echo=False)
        _SessionLocal = sessionmaker(bind=_engine)
        _current_database_url = database_url
    Base.metadata.create_all(_engine)
    ensure_schema_updates(_engine)
    print(f"SQLite database initialized at {database_url}")


def ensure_schema_updates(engine) -> None:
    """Apply lightweight schema updates for existing SQLite databases."""
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    if "school_snapshots" not in table_names:
        return

    columns = {column["name"] for column in inspector.get_columns("school_snapshots")}
    with engine.begin() as connection:
        if "sis_platform" not in columns:
            connection.execute(text("ALTER TABLE school_snapshots ADD COLUMN sis_platform VARCHAR(255)"))
        if "nightly_issues" not in columns:
            connection.execute(text("ALTER TABLE school_snapshots ADD COLUMN nightly_issues INTEGER DEFAULT 0"))
            connection.execute(text("ALTER TABLE school_snapshots ADD COLUMN nightly_no_data INTEGER DEFAULT 0"))
            connection.execute(text("ALTER TABLE school_snapshots ADD COLUMN realtime_issues INTEGER DEFAULT 0"))
            connection.execute(text("ALTER TABLE school_snapshots ADD COLUMN realtime_no_data INTEGER DEFAULT 0"))
            connection.execute(text("ALTER TABLE school_snapshots ADD COLUMN manual_issues INTEGER DEFAULT 0"))
            connection.execute(text("ALTER TABLE school_snapshots ADD COLUMN manual_no_data INTEGER DEFAULT 0"))
        if "nightly_merge_time_ms" not in columns:
            connection.execute(text("ALTER TABLE school_snapshots ADD COLUMN nightly_merge_time_ms INTEGER DEFAULT 0"))
        if "nightly_halted" not in columns:
            connection.execute(text("ALTER TABLE school_snapshots ADD COLUMN nightly_halted INTEGER DEFAULT 0"))
        if "active_users_json" not in columns:
            connection.execute(text("ALTER TABLE school_snapshots ADD COLUMN active_users_json TEXT DEFAULT '[]'"))

    if "sync_runs" not in table_names:
        return

    sync_run_columns = {column["name"] for column in inspector.get_columns("sync_runs")}
    with engine.begin() as connection:
        if "schools_processed" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN schools_processed INTEGER DEFAULT 0"))
        if "total_schools" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN total_schools INTEGER DEFAULT 0"))
        if "started_at" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN started_at DATETIME"))
        if "start_date" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN start_date VARCHAR(10)"))
        if "end_date" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN end_date VARCHAR(10)"))
        if "date_count" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN date_count INTEGER"))
        if "last_heartbeat_at" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN last_heartbeat_at DATETIME"))
        if "last_progress_at" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN last_progress_at DATETIME"))
        if "current_school" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN current_school VARCHAR(255)"))
        if "current_snapshot_date" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN current_snapshot_date VARCHAR(10)"))
        if "completed_units" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN completed_units INTEGER DEFAULT 0"))
        if "failed_units" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN failed_units INTEGER DEFAULT 0"))
        if "skipped_units" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN skipped_units INTEGER DEFAULT 0"))
        if "status_detail" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN status_detail VARCHAR(64)"))
        if "failure_reason" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN failure_reason TEXT"))
        if "failed_units_sample_json" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN failed_units_sample_json TEXT"))
        if "checkpoint_state_json" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN checkpoint_state_json TEXT"))
        if "errors_json" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN errors_json TEXT"))
        if "timing_json" not in sync_run_columns:
            connection.execute(text("ALTER TABLE sync_runs ADD COLUMN timing_json TEXT"))

    if "backfill_work_units" not in table_names:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE backfill_work_units (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        job_id VARCHAR(64) NOT NULL,
                        school VARCHAR(255) NOT NULL,
                        display_name VARCHAR(255) NOT NULL,
                        snapshot_date VARCHAR(10) NOT NULL,
                        products_json TEXT DEFAULT '[]',
                        status VARCHAR(32) NOT NULL DEFAULT 'pending',
                        attempt_count INTEGER NOT NULL DEFAULT 0,
                        last_error TEXT,
                        last_attempted_at DATETIME,
                        completed_at DATETIME,
                        CONSTRAINT uq_backfill_job_school_date UNIQUE (job_id, school, snapshot_date)
                    )
                    """
                )
            )
            connection.execute(text("CREATE INDEX ix_backfill_work_units_job_id ON backfill_work_units (job_id)"))
            connection.execute(text("CREATE INDEX ix_backfill_work_units_school ON backfill_work_units (school)"))
            connection.execute(text("CREATE INDEX ix_backfill_work_units_snapshot_date ON backfill_work_units (snapshot_date)"))
            connection.execute(text("CREATE INDEX ix_backfill_work_units_status ON backfill_work_units (status)"))

    if "scheduler_settings" not in table_names:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE scheduler_settings (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sync_enabled INTEGER NOT NULL DEFAULT 1,
                        sync_time VARCHAR(5) NOT NULL DEFAULT '07:30',
                        updated_at DATETIME NOT NULL
                    )
                    """
                )
            )

    if "error_analysis_groups" not in table_names:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE error_analysis_groups (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        snapshot_date VARCHAR(10) NOT NULL,
                        school VARCHAR(255) NOT NULL,
                        display_name VARCHAR(255) NOT NULL,
                        sis_platform VARCHAR(255),
                        entity_type VARCHAR(255),
                        error_code VARCHAR(255),
                        signature_key VARCHAR(64) NOT NULL,
                        normalized_message TEXT NOT NULL,
                        sample_message TEXT NOT NULL,
                        count INTEGER NOT NULL DEFAULT 0,
                        sample_errors_json TEXT NOT NULL DEFAULT '[]',
                        term_codes_json TEXT NOT NULL DEFAULT '[]',
                        created_at DATETIME NOT NULL,
                        CONSTRAINT uq_error_analysis_group UNIQUE (snapshot_date, school, signature_key)
                    )
                    """
                )
            )
            connection.execute(text("CREATE INDEX ix_error_analysis_groups_snapshot_date ON error_analysis_groups (snapshot_date)"))
            connection.execute(text("CREATE INDEX ix_error_analysis_groups_school ON error_analysis_groups (school)"))
            connection.execute(text("CREATE INDEX ix_error_analysis_groups_sis_platform ON error_analysis_groups (sis_platform)"))
            connection.execute(text("CREATE INDEX ix_error_analysis_groups_signature_key ON error_analysis_groups (signature_key)"))

    if "error_analysis_details" not in table_names:
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE error_analysis_details (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        snapshot_date VARCHAR(10) NOT NULL,
                        school VARCHAR(255) NOT NULL,
                        display_name VARCHAR(255) NOT NULL,
                        sis_platform VARCHAR(255),
                        entity_type VARCHAR(255),
                        error_code VARCHAR(255),
                        signature_key VARCHAR(64) NOT NULL,
                        signature_label TEXT NOT NULL,
                        normalized_message TEXT NOT NULL,
                        full_error_text TEXT NOT NULL,
                        entity_display_name VARCHAR(255),
                        merge_report_id VARCHAR(255),
                        schedule_type VARCHAR(255),
                        term_codes_json TEXT NOT NULL DEFAULT '[]',
                        raw_error_json TEXT NOT NULL,
                        created_at DATETIME NOT NULL
                    )
                    """
                )
            )
            connection.execute(text("CREATE INDEX ix_error_analysis_details_snapshot_date ON error_analysis_details (snapshot_date)"))
            connection.execute(text("CREATE INDEX ix_error_analysis_details_school ON error_analysis_details (school)"))
            connection.execute(text("CREATE INDEX ix_error_analysis_details_sis_platform ON error_analysis_details (sis_platform)"))
            connection.execute(text("CREATE INDEX ix_error_analysis_details_entity_type ON error_analysis_details (entity_type)"))
            connection.execute(text("CREATE INDEX ix_error_analysis_details_error_code ON error_analysis_details (error_code)"))
            connection.execute(text("CREATE INDEX ix_error_analysis_details_signature_key ON error_analysis_details (signature_key)"))
            connection.execute(text("CREATE INDEX ix_error_analysis_details_merge_report_id ON error_analysis_details (merge_report_id)"))


def get_db() -> Session:
    """Get a new database session. Caller is responsible for closing it."""
    if _SessionLocal is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _SessionLocal()
