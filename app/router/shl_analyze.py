from fastapi import APIRouter, Depends, BackgroundTasks, Request
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi_limiter.depends import RateLimiter

from app.clients.db import get_db
from app.schemas.shl_analyze import SHLAnalyzePayload, SHLAnalyzeResult
from app.services.shl_analyze import shl_service
from app.schemas.response import APIResponse
from app.services.llms import llms_service
from app.depends.jwt_guard import verify_user
from app.utils.helpers import ai_rate_limit_key
from app.utils.file_handler import save_images_to_disk  # 【新增】引入保存图片的函数


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
    background_tasks: BackgroundTasks,  # 【修改】注入 BackgroundTasks
    db: AsyncSession = Depends(get_db),
):
    llm = await llms_service.get_by_id(db, payload.llmId)
    if not llm or not llm.enabled:
        return APIResponse(message="LLM not found or disabled", code=404)

    # 1. 等待 AI 分析完成
    result = await shl_service.analyze(request, payload, db, llm.key)

    # 2. 【修改】分析成功后，将保存图片的任务挂载到后台执行
    # 这样代码会立刻执行下一步 return，不会在此处发生硬盘 I/O 阻塞
    background_tasks.add_task(save_images_to_disk, payload.images_data)

    return APIResponse(data=result)
