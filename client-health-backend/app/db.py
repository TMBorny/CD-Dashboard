"""Database and API client management.

Manages two connections:
1. SQLite via SQLAlchemy for persisting health snapshots
2. httpx AsyncClient for fetching data from the Coursedog API
"""

import os
from pathlib import Path
from typing import Optional

import httpx
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker, Session

from app.models import Base

load_dotenv()

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

    if COURSEDOG_EMAIL and COURSEDOG_PASSWORD:
        try:
            resp = await _http_client.post(
                "/api/v1/sessions",
                json={"email": COURSEDOG_EMAIL, "password": COURSEDOG_PASSWORD},
            )
            if resp.status_code in (200, 201):
                # Try token-based auth first (response has {"token": "..."})
                resp_data = resp.json() if resp.text else {}
                token = resp_data.get("token") if isinstance(resp_data, dict) else None
                if token:
                    _http_client.headers["Authorization"] = f"Bearer {token}"

                # Also grab cookies as fallback
                _auth_cookie = resp.headers.get("set-cookie", "")
                if _auth_cookie:
                    _http_client.headers["cookie"] = _auth_cookie
                for cookie_name, cookie_value in resp.cookies.items():
                    _http_client.cookies.set(cookie_name, cookie_value)
                print(f"Authenticated with Coursedog at {COURSEDOG_BASE_URL}")
            else:
                print(f"Warning: Login failed ({resp.status_code}): {resp.text[:200]}")
        except Exception as e:
            print(f"Warning: Could not authenticate with Coursedog: {e}")
    else:
        print("Warning: No COURSEDOG_EMAIL/COURSEDOG_PASSWORD set. API calls will be unauthenticated.")


async def close_api():
    global _http_client
    if _http_client:
        await _http_client.aclose()
        _http_client = None


async def api_get(path: str, params: Optional[dict] = None) -> dict:
    """Make an authenticated GET request to the Coursedog API."""
    client = get_http_client()
    resp = await client.get(path, params=params)
    resp.raise_for_status()
    return resp.json()


# --- SQLite database ---

DB_PATH = Path(__file__).parent.parent / "client_health.db"
DATABASE_URL = f"sqlite:///{DB_PATH}"

_engine = None
_SessionLocal = None


def init_db():
    """Create the SQLite engine, session factory, and tables."""
    global _engine, _SessionLocal
    _engine = create_engine(DATABASE_URL, echo=False)
    _SessionLocal = sessionmaker(bind=_engine)
    Base.metadata.create_all(_engine)
    ensure_schema_updates(_engine)
    print(f"SQLite database initialized at {DB_PATH}")


def ensure_schema_updates(engine) -> None:
    """Apply lightweight schema updates for existing SQLite databases."""
    inspector = inspect(engine)
    if "school_snapshots" not in inspector.get_table_names():
        return

    columns = {column["name"] for column in inspector.get_columns("school_snapshots")}
    with engine.begin() as connection:
        if "sis_platform" not in columns:
            connection.execute(text("ALTER TABLE school_snapshots ADD COLUMN sis_platform VARCHAR(255)"))


def get_db() -> Session:
    """Get a new database session. Caller is responsible for closing it."""
    if _SessionLocal is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _SessionLocal()
