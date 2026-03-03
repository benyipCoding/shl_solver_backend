from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.clients.db import get_db
from app.schemas.shl_analyze import SHLAnalyzePayload
from app.services.shl_analyze import shl_service
from app.schemas.response import APIResponse
from app.schemas.shl_analyze import SHLAnalyzeResult
from fastapi_limiter.depends import RateLimiter
from fastapi import Request
from app.services.llms import llms_service
from app.depends.jwt_guard import verify_user
from app.utils.helpers import ai_rate_limit_key


router = APIRouter(
    prefix="/shl_analyze", tags=["SHL Analyze"], dependencies=[Depends(verify_user)]
)


@router.post(
    "",
    response_model=APIResponse[SHLAnalyzeResult],
    dependencies=[
        Depends(RateLimiter(times=3, seconds=60, identifier=ai_rate_limit_key)),
    ],
)
async def process_shl_analyze(
    request: Request,
    payload: SHLAnalyzePayload,
    db: AsyncSession = Depends(get_db),
):
    llm = await llms_service.get_by_id(db, payload.llmId)
    if not llm or not llm.enabled:
        return APIResponse(message="LLM not found or disabled", code=404)

    result = await shl_service.analyze(request, payload, db, llm.key)
    return APIResponse(data=result)
