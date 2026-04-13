import asyncio
import json
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db import connect_api, api_get, close_api

async def main():
    await connect_api()
    try:
        data = await api_get(
            f"/api/v1/int/mcc_colleague_ethos/integrations-hub/merge-history",
            params={
                "page": "0", "size": "100",
            }
        )
        reports = data.get("content", []) if isinstance(data, dict) else []
        nightly = [r for r in reports if str(r.get("scheduleType", "")).lower() == "nightly"]
        
        if nightly:
            print("First item keys:", nightly[0].keys())
            print("\nFirst item JSON:")
            print(json.dumps(nightly[0], indent=2))
        elif reports:
            print("No nightly, but found reports. First item JSON:")
            print(json.dumps(reports[0], indent=2))
        else:
            print("No reports found at all.")
    except Exception as e:
        print("Error", e)
    await close_api()

asyncio.run(main())
