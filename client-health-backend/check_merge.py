import asyncio
import sys
from app.db import connect_api, api_get, close_api

async def main():
    await connect_api()
    school = 'stanford'
    
    try:
        data = await api_get(f"/api/v1/int/{school}/integrations-hub/merge-history", params={"page": "0", "size": "500"})
        entries = []
        for key in ("content", "items", "results", "mergeReports", "data"):
            val = data.get(key)
            if isinstance(val, list):
                entries = [item.get("mergeReport", item) if isinstance(item, dict) else item for item in val]
                break
                
        target_id = "GWzxXBG9bm2LjDuhLFUk"
        found = False
        counts = {}
        target_entry = None
        for entry in entries:
            st = str(entry.get("status", ""))
            counts[st] = counts.get(st, 0) + 1
            if entry.get("id") == target_id or entry.get("_id") == target_id:
                target_entry = entry
                found = True
        
        if target_entry:
            print(f"Found target report: {target_entry['id']} mode: {target_entry.get('mode')} status: {target_entry.get('status')}")
        else:
            print("Target not found.")
            
        print(f"Statuses found (raw case): {counts}")
        
    finally:
        await close_api()

if __name__ == "__main__":
    asyncio.run(main())
