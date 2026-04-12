from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from app.db import connect_api, close_api, init_db
from app.routes import router, start_scheduled_sync_service, stop_scheduled_sync_service
from app.security import get_cors_settings, require_internal_auth


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


def create_app() -> FastAPI:
    app_instance = FastAPI(lifespan=lifespan)
    cors_settings = get_cors_settings()

    app_instance.add_middleware(
        CORSMiddleware,
        allow_origins=cors_settings["allow_origins"],
        allow_credentials=cors_settings["allow_credentials"],
        allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
        allow_headers=["X-Internal-API-Key", "Content-Type"],
    )

    app_instance.include_router(router, dependencies=[Depends(require_internal_auth)])
    return app_instance


app = create_app()


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
