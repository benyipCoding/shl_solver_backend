from fastapi import APIRouter, Depends, BackgroundTasks, Request
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi_limiter.depends import RateLimiter

from app.clients.db import get_db
from app.schemas.shl_analyze import (
    SHLAnalyzePayload,
    SHLAnalyzeResult,
    SHLCodeVerifyPayload,
    SHLCodeVerifyResult,
)
from app.services.shl_analyze import shl_service
from app.schemas.response import APIResponse
from app.services.llms import llms_service
from app.depends.jwt_guard import verify_user
from app.utils.helpers import ai_rate_limit_key
from app.utils.file_handler import (
    handle_shl_analyze_background_task,
)
from typing import List


router = APIRouter(
    prefix="/shl_analyze", tags=["SHL Analyze"], dependencies=[Depends(verify_user)]
)


@router.post(
    "",
    response_model=APIResponse[SHLAnalyzeResult],
    dependencies=[
        Depends(RateLimiter(times=1, seconds=60, identifier=ai_rate_limit_key)),
    ],
)
async def process_shl_analyze(
    request: Request,
    payload: SHLAnalyzePayload,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    llm = await llms_service.get_by_id(db, payload.llmId)
    if not llm or not llm.enabled:
        return APIResponse(message="LLM not found or disabled", code=404)

    try:
        # 1. 等待 AI 分析完成
        result, token_count = await shl_service.analyze(request, payload, db, llm.key)
        # result, token_count = await shl_service.analyze_openrouter(
        #     request, payload, db, llm.key
        # )

        if isinstance(result, list):
            result = result[0] if result else {}

        # 2. 分析成功后，将保存图片的任务以及历史记录挂载到后台执行
        # 这样代码会立刻执行下一步 return，不会在此处发生硬盘 I/O 阻塞
        background_tasks.add_task(
            handle_shl_analyze_background_task,
            payload.images_data,
            request.state.user.id,
            llm.key,
            token_count,
            result,
            status="completed",
        )

        return APIResponse(data=result)

    except Exception as e:
        # 3. 如果分析失败，也要记录失败的历史
        background_tasks.add_task(
            handle_shl_analyze_background_task,
            payload.images_data,
            request.state.user.id,
            llm.key,
            0,
            {"error": str(e)},
            status="failed",
        )
        raise e


@router.post(
    "/verify-code",
    response_model=APIResponse[SHLCodeVerifyResult],
    dependencies=[
        Depends(RateLimiter(times=5, seconds=60, identifier=ai_rate_limit_key)),
    ],
)
async def process_code_verify(
    request: Request,
    payload: SHLCodeVerifyPayload,
    # background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    try:
        # 1. AI代码纠错
        # 不需要 llmId，内部固定使用 gemini-3-flash-preview
        result = await shl_service.verify_code(request, payload, db)
        # result = await shl_service.verify_code_openrouter(request, payload, db)

        # background_tasks.add_task(
        #     handle_shl_analyze_background_task,
        #     [payload.image_data] if payload.image_data else [],
        #     request.state.user.id,
        #     "gemini-3-flash-preview",
        #     token_count,
        #     result,
        #     status="completed",
        # )

        return APIResponse(data=result)

    except Exception as e:
        # 记录失败的历史
        # background_tasks.add_task(
        #     handle_shl_analyze_background_task,
        #     [payload.image_data] if payload.image_data else [],
        #     request.state.user.id,
        #     "gemini-3-flash-preview",
        #     0,
        #     {"error": str(e)},
        #     status="failed",
        # )
        raise e
