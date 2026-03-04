from fastapi import APIRouter
from app.schemas.excel_workbench import TransformRequest, AIResponseSchema
from app.schemas.response import APIResponse
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends
from app.clients.db import get_db
from fastapi_limiter.depends import RateLimiter
from fastapi import Request
from app.utils.helpers import ai_rate_limit_key
from app.depends.jwt_guard import verify_user
from app.services.excel_workbench import excel_workbench_service


router = APIRouter(
    prefix="/excel_wb", tags=["Excel Workbench"], dependencies=[Depends(verify_user)]
)


@router.post(
    "/transform",
    response_model=APIResponse[AIResponseSchema],
    dependencies=[
        Depends(RateLimiter(times=3, seconds=60, identifier=ai_rate_limit_key)),
    ],
)
async def process_transform(
    request: Request, payload: TransformRequest, db: AsyncSession = Depends(get_db)
):
    result = await excel_workbench_service.transform(request, payload, db)
    return APIResponse(data=result)
