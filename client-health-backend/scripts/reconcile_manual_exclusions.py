#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db import close_api, connect_api, get_db, init_db  # noqa: E402
from app.routes import reconcile_manual_exclusions  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reconcile manually excluded schools against year-to-date activity."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Evaluate exclusions without removing any rows.",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    init_db()
    try:
        await connect_api()

        db = get_db()
        try:
            result = await reconcile_manual_exclusions(db, apply_changes=not args.dry_run)
        finally:
            db.close()
    finally:
        await close_api()

    action = "Dry run" if args.dry_run else "Applied"
    reinstated = result["reinstatedSchools"]
    retained = result["retainedSchools"]

    print(f"{action} reconciliation for {result['yearStartDate']} onward")
    print(f"Evaluated: {result['evaluatedCount']}")
    print(f"Reinstated ({len(reinstated)}): {', '.join(reinstated) if reinstated else 'none'}")
    print(f"Retained ({len(retained)}): {', '.join(retained) if retained else 'none'}")


if __name__ == "__main__":
    asyncio.run(main())
