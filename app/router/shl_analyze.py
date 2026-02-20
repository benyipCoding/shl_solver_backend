from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.clients.db import get_db
from app.schemas.shl_analyze import SHLAnalyzePayload
from app.services.shl_analyze import shl_service
from app.schemas.response import APIResponse
from app.schemas.shl_analyze import SHLAnalyzeResult
from fastapi_limiter.depends import RateLimiter
from fastapi import Request

router = APIRouter(prefix="/shl_analyze", tags=["SHL Analyze"])


async def ai_rate_limit_key(request: Request):
    user = getattr(request.state, "user", None)
    if user:
        return f"user:{user.id}"
    return f"ip:{request.client.host}"


@router.post(
    "/",
    response_model=APIResponse[SHLAnalyzeResult],
    dependencies=[
        # Depends(get_current_user),
        Depends(RateLimiter(times=3, seconds=60, identifier=ai_rate_limit_key)),
        # Depends(ai_guard),
    ],
)
async def process_shl_analyze(
    payload: SHLAnalyzePayload, db: AsyncSession = Depends(get_db)
):
    try:
        result = shl_service.analyze(payload)
        return APIResponse(data=result)
    except Exception as e:
        return APIResponse(message=str(e), code=500)
