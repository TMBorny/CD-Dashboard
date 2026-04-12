from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from app.db import connect_api, close_api, init_db
from app.routes import router, start_scheduled_sync_service, stop_scheduled_sync_service


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    # Initialize SQLite database (creates tables if they don't exist)
    init_db()
    # Connect to Coursedog API
    await connect_api()
    await start_scheduled_sync_service()
    yield
    await stop_scheduled_sync_service()
    await close_api()


app = FastAPI(lifespan=lifespan)

# Disable CORS. Do not remove this for full-stack development.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

app.include_router(router)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
