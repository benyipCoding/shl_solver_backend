from typing import Any

import httpx

from app.core.config import settings


class FFLogsAPIError(Exception):
    def __init__(
        self,
        status_code: int,
        message: str,
        payload: Any | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.payload = payload


_client: httpx.AsyncClient | None = None


def _build_base_url() -> str:
    base_url = settings.ff14_base_url.rstrip("/")
    if not base_url.endswith("/v1"):
        base_url = f"{base_url}/v1"
    return base_url


def init_ff14_client():
    global _client
    _client = httpx.AsyncClient(
        base_url=_build_base_url(),
        headers={"Accept": "application/json"},
        timeout=httpx.Timeout(30.0, connect=10.0),
    )


def get_ff14_client() -> httpx.AsyncClient:
    if _client is None:
        raise RuntimeError("FF Logs client not initialized")
    return _client


async def close_ff14_client():
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
