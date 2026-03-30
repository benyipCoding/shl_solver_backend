import traceback
import asyncio
from fastapi import APIRouter, Depends, BackgroundTasks, Request
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi_limiter.depends import RateLimiter

from app.utils.alert_utils import send_email_alert
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
from app.services.wallet_service import wallet_service, InsufficientCreditsException
from app.models.user import ActionType

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

    user_id = request.state.user.id

    # ================= 1. 动态计费与流水类型判断 =================
    # 根据 llm.key 判定是 Pro 还是 Flash，决定扣费金额和记录类型
    is_pro = "pro" in llm.key.lower()
    cost = 20 if is_pro else 5
    action_type = ActionType.USE_PRO_MODEL if is_pro else ActionType.USE_FLASH_MODEL

    # ================= 2. 事前扣费 =================
    try:
        await wallet_service.consume_credits(
            db, user_id=user_id, amount=cost, action_type=action_type
        )
    except InsufficientCreditsException:
        # 余额不足，直接返回 402 状态码，阻断后续所有 AI 请求
        return APIResponse(message="算力点数不足，请充值", code=402)

    try:
        # 1. 等待 AI 分析完成
        result, token_count = await shl_service.analyze(request, payload, db, llm.key)

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

        # AI 调用失败，务必将点数退还给用户
        await wallet_service.refund_credits(
            db, user_id=user_id, amount=cost, action_type=action_type
        )
        # 手动触发报警邮件逻辑
        error_msg = traceback.format_exc()
        alert_text = f"🚨 后端服务报警 (AI分析失败)\n\nURL: {request.url}\nMethod: {request.method}\nError: {str(e)}\n\nTraceback:\n{error_msg}"
        asyncio.create_task(asyncio.to_thread(send_email_alert, alert_text))

        return APIResponse(message=f"分析失败: {str(e)}", code=500)


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
    db: AsyncSession = Depends(get_db),
):
    user_id = request.state.user.id
    # 拍照纠错统一计费策略
    cost = 2
    action_type = ActionType.USE_VISION_DIFF

    # ================= 1. 事前扣费 =================
    try:
        await wallet_service.consume_credits(
            db, user_id=user_id, amount=cost, action_type=action_type
        )
    except InsufficientCreditsException:
        return APIResponse(message="算力点数不足，请充值", code=402)

    try:
        # 1. AI代码纠错
        # 不需要 llmId，内部固定使用 gemini-3-flash-preview
        result = await shl_service.verify_code(request, payload, db)
        return APIResponse(data=result)

    except Exception as e:
        # 如果 verify 失败，退还费用
        await wallet_service.refund_credits(
            db, user_id=user_id, amount=cost, action_type=action_type
        )
        # # 手动触发报警邮件逻辑
        # error_msg = traceback.format_exc()
        # alert_text = f"🚨 后端服务报警 (AI纠错失败)\n\nURL: {request.url}\nMethod: {request.method}\nError: {str(e)}\n\nTraceback:\n{error_msg}"
        # asyncio.create_task(asyncio.to_thread(send_email_alert, alert_text))

        return APIResponse(message=f"纠错失败: {str(e)}", code=500)
