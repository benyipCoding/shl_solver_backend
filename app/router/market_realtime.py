"""Local-only streaming gateway. It does not modify market data or sync state."""
import json
import logging

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.core.config import settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/market_master/realtime", tags=["Market realtime"])
INTERVALS = {"1min", "5min", "15min", "30min", "45min", "1h", "2h", "4h", "8h", "1day", "1week", "1month"}


@router.get("/capabilities")
async def capabilities():
    return {"enabled": settings.can_use_fxcm_realtime}


def normalize_event(event, interval):
    # Historical endpoints expose date-only strings for D/W/M candles. Use
    # their same display identity, keeping the provider timestamp for debugging.
    if interval in {"1day", "1week", "1month"}:
        for candle in event.get("candles", []):
            candle["datetime"] = candle["datetime"][:10]
    return event


@router.get("/stream")
async def stream(symbol: str = Query(..., min_length=1, max_length=64), interval: str = "1h",
                 snapshot_size: int = Query(256, ge=2, le=5000)):
    if not settings.can_use_fxcm_realtime:
        raise HTTPException(404, "Realtime is disabled outside local development")
    if interval not in INTERVALS:
        raise HTTPException(400, "Unsupported interval")
    # Use the application service after startup, avoiding the existing sync /
    # market service import cycle for capability checks and disabled endpoints.
    from app.services.market_master import market_master_service

    provider_symbol = market_master_service._resolve_fxcm_symbol(symbol)

    async def events():
        yield ": connected\n\n"
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(max(90, settings.fxcm_api_timeout_seconds), connect=10),
                trust_env=False,
            ) as client:
                async with client.stream(
                    "GET", settings.fxcm_api_base_url.rstrip("/") + "/realtime/stream",
                    params={"symbol": provider_symbol, "interval": interval, "snapshot_size": snapshot_size},
                ) as response:
                    if response.status_code != 200:
                        yield "data: " + json.dumps({"type": "error", "retryable": response.status_code >= 500,
                            "message": "FXCM 实时服务未就绪，请检查本地配置与品种订阅"}) + "\n\n"
                        return
                    async for line in response.aiter_lines():
                        if line.startswith("data: "):
                            event = normalize_event(json.loads(line[6:]), interval)
                            yield "data: " + json.dumps(event, allow_nan=False) + "\n\n"
                        elif line.startswith(":"):
                            yield line + "\n\n"
        except (httpx.HTTPError, ValueError, KeyError):
            logger.exception("FXCM realtime gateway disconnected")
            yield "data: " + json.dumps({"type": "error", "retryable": True,
                "message": "实时行情连接中断，正在重连并补齐快照"}) + "\n\n"

    return StreamingResponse(events(), media_type="text/event-stream", headers={
        "Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no"})
