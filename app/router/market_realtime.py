"""Local-only streaming gateway. It does not modify market data or sync state."""
import json
import logging
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.core.config import settings
from app.schemas.response import APIResponse

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/market_master/realtime", tags=["Market realtime"])
INTERVALS = {
    "1min",
    "5min",
    "15min",
    "30min",
    "45min",
    "1h",
    "2h",
    "3h",
    "4h",
    "6h",
    "8h",
    "1day",
    "1week",
    "1month",
}


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


@router.get("/history")
async def history(
    symbol: str = Query(..., min_length=1, max_length=64),
    interval: str = "1h",
    outputsize: int = Query(5000, ge=2, le=5000),
    end_date: str | None = Query(None),
):
    """Fetch an older FXCM window without reading or updating the database."""
    if not settings.can_use_fxcm_realtime:
        raise HTTPException(404, "Realtime is disabled outside local development")
    if interval not in INTERVALS:
        raise HTTPException(400, "Unsupported interval")

    from app.services.market_master import market_master_service

    provider_symbol = market_master_service._resolve_fxcm_symbol(symbol)
    params: dict[str, Any] = {
        "symbol": provider_symbol,
        "interval": interval,
        "outputsize": outputsize,
        "price_type": "mid",
    }
    if end_date:
        params["end_date"] = end_date

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(max(90, settings.fxcm_api_timeout_seconds), connect=10),
            trust_env=False,
        ) as client:
            response = await client.get(
                settings.fxcm_api_base_url.rstrip("/") + "/history",
                params=params,
            )
    except httpx.HTTPError as exc:
        logger.exception("FXCM realtime history gateway failed")
        raise HTTPException(502, "FXCM 历史 K 线服务暂时不可用") from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise HTTPException(502, "FXCM 历史 K 线返回格式无效") from exc

    if response.status_code != 200:
        detail = payload.get("detail") if isinstance(payload, dict) else None
        message = detail or "FXCM 历史 K 线请求失败"
        status_code = response.status_code if 400 <= response.status_code < 500 else 502
        raise HTTPException(status_code, message)

    raw_values = payload.get("values") if isinstance(payload, dict) else []
    values = [item for item in raw_values if isinstance(item, dict)]
    meta = payload.get("meta") if isinstance(payload, dict) else {}
    if not isinstance(meta, dict):
        meta = {}
    if interval in {"1day", "1week", "1month"}:
        for candle in values:
            datetime_value = candle.get("datetime")
            if isinstance(datetime_value, str):
                candle["datetime"] = datetime_value[:10]

    earliest = values[0].get("datetime") if values else None
    latest = values[-1].get("datetime") if values else None
    result_meta = {
        "symbol": meta.get("symbol") or symbol,
        "provider_symbol": meta.get("provider_symbol") or provider_symbol,
        "interval": interval,
        "requested_interval": meta.get("requested_interval") or interval,
        "provider_interval": meta.get("provider_interval") or interval,
        "price_type": meta.get("price_type") or "mid",
        "currency": meta.get("currency"),
        "exchange": meta.get("exchange") or "FXCM",
        "exchange_timezone": meta.get("exchange_timezone"),
        "asset_type": meta.get("asset_type"),
        "total": len(values),
        "offset": 0,
        "earliest": earliest,
        "latest": latest,
    }
    return APIResponse(
        data={
            "symbol": result_meta["symbol"],
            "interval": interval,
            "count": len(values),
            "total": len(values),
            "offset": 0,
            # A derived interval (for example 3h/6h) can yield fewer candles
            # than outputsize because the sidecar's raw H1 request is capped
            # at 5000 rows. An empty page is the reliable end-of-history mark.
            "has_more_history": bool(values),
            "has_more_future": False,
            "earliest": earliest,
            "latest": latest,
            "meta": result_meta,
            "values": values,
        }
    )


@router.get("/stream")
async def stream(symbol: str = Query(..., min_length=1, max_length=64), interval: str = "1h",
                 snapshot_size: int = Query(256, ge=2, le=5000),
                 seed_timestamp: float | None = Query(None, gt=0),
                 seed_price: float | None = Query(None, gt=0)):
    if not settings.can_use_fxcm_realtime:
        raise HTTPException(404, "Realtime is disabled outside local development")
    if interval not in INTERVALS:
        raise HTTPException(400, "Unsupported interval")
    # Use the application service after startup, avoiding the existing sync /
    # market service import cycle for capability checks and disabled endpoints.
    from app.services.market_master import market_master_service

    provider_symbol = market_master_service._resolve_fxcm_symbol(symbol)
    seed_params = (
        {"seed_timestamp": seed_timestamp, "seed_price": seed_price}
        if seed_timestamp is not None and seed_price is not None
        else {}
    )

    async def events():
        yield ": connected\n\n"
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(max(90, settings.fxcm_api_timeout_seconds), connect=10),
                trust_env=False,
            ) as client:
                async with client.stream(
                    "GET", settings.fxcm_api_base_url.rstrip("/") + "/realtime/stream",
                    params={
                        "symbol": provider_symbol,
                        "interval": interval,
                        "snapshot_size": snapshot_size,
                        **seed_params,
                    },
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
