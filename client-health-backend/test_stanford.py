import asyncio
from app.db import connect_api, api_get, close_api
from app.routes import extract_merge_history_entries

async def main():
    await connect_api()
    try:
        data = await api_get("/api/v1/int/stanford/integrations-hub/merge-history", params={"page": "0", "size": "500"})
        entries = extract_merge_history_entries(data)
        counts = {}
        for entry in entries:
            st = str(entry.get("status", ""))
            sch = str(entry.get("scheduleType", ""))
            k = f"{sch}_{st}"
            counts[k] = counts.get(k, 0) + 1
        print(f"Stanford latest statuses/schedules: {counts}")
    finally:
        await close_api()

asyncio.run(main())
