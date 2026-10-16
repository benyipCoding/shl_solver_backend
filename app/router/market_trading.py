"""Authenticated local gateway to a single, explicitly enabled FXCM Demo account."""
import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response, StreamingResponse

from app.core.config import settings
from app.depends.jwt_guard import verify_superuser


def require_trading():
    if not (settings.can_use_fxcm_realtime and settings.fxcm_trading_enabled and settings.fxcm_trading_bridge_key):
        raise HTTPException(404, "本地福汇交易未启用")


router = APIRouter(prefix="/market_master/trading", tags=["Demo trading"],
                   dependencies=[Depends(require_trading), Depends(verify_superuser)])


@router.api_route("/{path:path}", methods=["GET", "POST"])
async def proxy(path: str, request: Request):
    from uuid import UUID
    valid = path in {"state", "stream", "history"} and request.method == "GET"
    valid = valid or path == "commands" and request.method == "POST"
    if path.startswith("commands/") and request.method == "GET":
        try:
            UUID(path.removeprefix("commands/"))
            valid = True
        except ValueError:
            pass
    if not valid:
        raise HTTPException(404)
    params = dict(request.query_params)
    if "symbol" in params:
        from app.services.market_master import market_master_service
        params["symbol"] = market_master_service._resolve_fxcm_symbol(params["symbol"])
    body = None
    if request.method == "POST":
        body = await request.json()
        from app.services.market_master import market_master_service
        if isinstance(body, dict) and isinstance(body.get("symbol"), str):
            body["symbol"] = market_master_service._resolve_fxcm_symbol(body["symbol"])
    client = httpx.AsyncClient(timeout=httpx.Timeout(90, connect=10), trust_env=False)
    try:
        response = await client.send(client.build_request(request.method,
            settings.fxcm_api_base_url.rstrip("/") + "/trading/" + path,
            params=params, json=body, headers={"X-FXCM-Bridge-Key": settings.fxcm_trading_bridge_key}), stream=True)
    except httpx.HTTPError:
        await client.aclose()
        raise HTTPException(502, "福汇交易连接中断；已提交的指令请按原编号查询结果，不要重复下单")
    if path != "stream" or response.status_code != 200:
        try:
            return Response(await response.aread(), status_code=response.status_code,
                            media_type=response.headers.get("content-type", "application/json"))
        finally:
            await response.aclose()
            await client.aclose()
    async def events():
        try:
            async for chunk in response.aiter_bytes():
                yield chunk
        finally:
            await response.aclose()
            await client.aclose()
    return StreamingResponse(events(), media_type="text/event-stream", headers={
        "Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no"})
