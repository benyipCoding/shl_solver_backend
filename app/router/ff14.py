from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from app.clients.ff14 import FFLogsAPIError
from app.depends.jwt_guard import verify_user
from app.schemas.response import APIResponse
from app.services.ff14 import ff14_service


router = APIRouter(
    prefix="/ff14_logs",
    tags=["FF14 Logs"],
    # dependencies=[Depends(verify_user)],
)


async def _proxy_request(path: str, request: Request):
    try:
        result = await ff14_service.get(path=path, params=request.query_params)
    except FFLogsAPIError as exc:
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "code": exc.status_code,
                "message": exc.message,
                "data": exc.payload,
            },
        )
    return APIResponse(data=result)


@router.get("/zones", response_model=APIResponse[Any])
async def get_zones(request: Request):
    return await _proxy_request("/zones", request)


@router.get("/classes", response_model=APIResponse[Any])
async def get_classes(request: Request):
    return await _proxy_request("/classes", request)


@router.get("/rankings/encounter/{encounterID}", response_model=APIResponse[Any])
async def get_encounter_rankings(encounterID: int, request: Request):
    return await _proxy_request(f"/rankings/encounter/{encounterID}", request)


@router.get(
    "/rankings/character/{characterName}/{serverName}/{serverRegion}",
    response_model=APIResponse[Any],
)
async def get_character_rankings(
    characterName: str,
    serverName: str,
    serverRegion: str,
    request: Request,
):
    return await _proxy_request(
        f"/rankings/character/{characterName}/{serverName}/{serverRegion}",
        request,
    )


@router.get(
    "/parses/character/{characterName}/{serverName}/{serverRegion}",
    response_model=APIResponse[Any],
)
async def get_character_parses(
    characterName: str,
    serverName: str,
    serverRegion: str,
    request: Request,
):
    return await _proxy_request(
        f"/parses/character/{characterName}/{serverName}/{serverRegion}",
        request,
    )


@router.get(
    "/reports/guild/{guildName}/{serverName}/{serverRegion}",
    response_model=APIResponse[Any],
)
async def get_guild_reports(
    guildName: str,
    serverName: str,
    serverRegion: str,
    request: Request,
):
    return await _proxy_request(
        f"/reports/guild/{guildName}/{serverName}/{serverRegion}",
        request,
    )


@router.get("/reports/user/{userName}", response_model=APIResponse[Any])
async def get_user_reports(userName: str, request: Request):
    return await _proxy_request(f"/reports/user/{userName}", request)


@router.get("/report/fights/{code}", response_model=APIResponse[Any])
async def get_report_fights(code: str, request: Request):
    return await _proxy_request(f"/report/fights/{code}", request)


@router.get("/report/events/{view}/{code}", response_model=APIResponse[Any])
async def get_report_events(view: str, code: str, request: Request):
    return await _proxy_request(f"/report/events/{view}/{code}", request)


@router.get("/report/tables/{view}/{code}", response_model=APIResponse[Any])
async def get_report_tables(view: str, code: str, request: Request):
    return await _proxy_request(f"/report/tables/{view}/{code}", request)
