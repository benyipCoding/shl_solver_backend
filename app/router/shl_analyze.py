from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.clients.db import get_db
from app.schemas.shl_analyze import SHLAnalyzePayload
from app.services.shl_analyze import shl_service
from app.schemas.response import APIResponse
from app.schemas.shl_analyze import SHLAnalyzeResult

router = APIRouter(prefix="/shl_analyze", tags=["SHL Analyze"])


@router.post("/", response_model=APIResponse[SHLAnalyzeResult])
async def process_shl_analyze(
    payload: SHLAnalyzePayload, db: AsyncSession = Depends(get_db)
):
    try:
        result = shl_service.analyze(payload)
        return APIResponse(data=result)
    except Exception as e:
        return APIResponse(message=str(e), code=500)
