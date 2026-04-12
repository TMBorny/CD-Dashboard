"""Security helpers for internal API access and CORS configuration."""

import logging
import os
from typing import Optional

from fastapi import Header, HTTPException, Request, status

logger = logging.getLogger(__name__)

INTERNAL_API_KEY_HEADER = "X-Internal-API-Key"
DEFAULT_ALLOWED_ORIGINS = (
    "http://localhost:5173",
    "http://127.0.0.1:5173",
)
LOOPBACK_CLIENTS = {"127.0.0.1", "::1", "localhost", "testclient"}


def _parse_csv_env(*names: str, default: tuple[str, ...] = ()) -> list[str]:
    for name in names:
        raw_value = os.getenv(name, "")
        if raw_value:
            return [item.strip() for item in raw_value.split(",") if item.strip()]
    return list(default)


def _parse_bool_env(*names: str, default: bool = False) -> bool:
    truthy_values = {"1", "true", "yes", "on"}
    falsey_values = {"0", "false", "no", "off"}
    for name in names:
        raw_value = os.getenv(name)
        if raw_value is None:
            continue
        normalized = raw_value.strip().lower()
        if normalized in truthy_values:
            return True
        if normalized in falsey_values:
            return False
    return default


def get_internal_api_key() -> str:
    return os.getenv("INTERNAL_API_KEY", os.getenv("VITE_INTERNAL_API_KEY", "")).strip()


def is_loopback_client(request: Request) -> bool:
    client_host = request.client.host if request.client else ""
    return client_host in LOOPBACK_CLIENTS


def get_cors_settings() -> dict:
    return {
        "allow_origins": _parse_csv_env(
            "ALLOWED_ORIGINS",
            "VITE_ALLOWED_ORIGINS",
            default=DEFAULT_ALLOWED_ORIGINS,
        ),
        "allow_credentials": _parse_bool_env(
            "CORS_ALLOW_CREDENTIALS",
            "VITE_CORS_ALLOW_CREDENTIALS",
            default=False,
        ),
    }


async def require_internal_auth(
    request: Request,
    x_internal_api_key: Optional[str] = Header(default=None, alias=INTERNAL_API_KEY_HEADER),
) -> None:
    expected_api_key = get_internal_api_key()
    if not expected_api_key:
        if is_loopback_client(request):
            logger.warning(
                "Internal API key is not configured; allowing loopback request to %s",
                request.url.path,
            )
            return

        logger.error("Internal API key is not configured; rejecting %s", request.url.path)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "internal_auth_not_configured",
                "message": "Internal API access is not configured",
            },
        )

    if x_internal_api_key != expected_api_key:
        client_host = request.client.host if request.client else "unknown"
        logger.warning("Unauthorized internal API request from %s to %s", client_host, request.url.path)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "code": "invalid_internal_api_key",
                "message": "A valid internal API key is required",
            },
        )
