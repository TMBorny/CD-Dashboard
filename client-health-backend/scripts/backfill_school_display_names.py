#!/usr/bin/env python3
"""Backfill persisted school display names using the verified resolver."""

from __future__ import annotations

from pathlib import Path
import sys

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

import asyncio

from app.db import api_get, close_api, connect_api, get_db, init_db  # noqa: E402
from app.school_names import backfill_school_display_names  # noqa: E402


def build_verified_display_name_map(display_names_data) -> dict[str, str]:
    mapping: dict[str, str] = {}
    if isinstance(display_names_data, dict):
        for school, value in display_names_data.items():
            if isinstance(value, dict):
                display_name = value.get("displayName") or value.get("fullName")
            elif isinstance(value, str):
                display_name = value
            else:
                display_name = None
            if display_name:
                mapping[school] = display_name
    return mapping


def main() -> int:
    init_db()
    db = get_db()
    try:
        async def load_names():
            await connect_api()
            try:
                return await api_get("/api/v1/admin/schools/displayNames")
            finally:
                await close_api()

        display_names = asyncio.run(load_names())
        updated = backfill_school_display_names(db, build_verified_display_name_map(display_names))
        print(f"Updated {updated} persisted school display-name rows")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
