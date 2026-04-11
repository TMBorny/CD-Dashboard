import asyncio
from app.db import connect_api, close_api, init_db
from app.routes import run_history_backfill_job, _sync_jobs

async def main():
    init_db()
    await connect_api()
    _sync_jobs["test_job2"] = {"scope": "single", "schoolsProcessed": 0, "totalSchools": 0, "errors": []}
    await run_history_backfill_job("test_job2", "stanford", 7)
    await close_api()

asyncio.run(main())
