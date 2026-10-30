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
    TaskSubmitResponse,
)
from app.services.shl_analyze import shl_service
from app.schemas.response import APIResponse
from app.services.llms import llms_service
from app.depends.jwt_guard import verify_user
from app.utils.helpers import ai_rate_limit_key

from app.services.wallet_service import wallet_service, InsufficientCreditsException
from app.models.shl_solver import ActionType

# 引入模型和枚举
from app.models.ai_task import AITask, TaskStatus
from app.services.task_worker import (
    background_shl_solver_task,
    background_code_verify_task,
)
from sqlalchemy.future import select
import traceback
from app.clients.posthog_client import capture_event
from app.core.shl_models import get_shl_model_config, VERIFY_CODE_COST


router = APIRouter(
    prefix="/shl_analyze", tags=["SHL Analyze"], dependencies=[Depends(verify_user)]
)

@router.post(
    "",
    response_model=APIResponse[TaskSubmitResponse],
    dependencies=[
        Depends(RateLimiter(times=2, seconds=60, identifier=ai_rate_limit_key)),
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
        model_config = get_shl_model_config(llm.key)
    except ValueError:
        return APIResponse(message="Unsupported SHL model", code=400)

    user_id = request.state.user.id
    client_ip = getattr(request.state, "real_ip", request.client.host)
    req_path = request.url.path

    # ================= 1. 动态计费与流水类型判断 =================
    cost = model_config.credits
    action_type = model_config.action_type

    # ================= 2. 事前扣费 =================
    try:
        await wallet_service.consume_credits(
            db, user_id=user_id, amount=cost, action_type=action_type
        )
    except InsufficientCreditsException:
        # 余额不足，直接返回 402 状态码，阻断后续所有 AI 请求
        return APIResponse(message="算力点数不足，请充值", code=402)

    try:
        # 3. 创建 PENDING 状态的任务记录
        new_task = AITask(
            user_id=user_id, task_type="SHL_ANALYZE", status=TaskStatus.PENDING
        )
        db.add(new_task)
        await db.commit()
        await db.refresh(new_task)

        # 4. 把耗时工作丢进后台
        background_tasks.add_task(
            background_shl_solver_task,
            task_id=new_task.task_id,
            user_id=user_id,
            payload=payload,
            llm_key=llm.key,
            cost=cost,
            action_type=action_type,
            ip=client_ip,
            request_path=req_path,
        )

        capture_event(
            str(user_id),
            "ai_task_started",
            properties={
                "task_type": "SHL_ANALYZE",
                "llm_key": llm.key,
                "cost": cost,
                "provider": model_config.provider,
                "is_pro": action_type == ActionType.USE_PRO_MODEL,
                "$ip": client_ip,
                "$user_agent": request.headers.get("user-agent", ""),
            },
        )

        return APIResponse(
            data={"task_id": new_task.task_id, "status": TaskStatus.PENDING.value}
        )

    except Exception as e:
        await db.rollback()
        # 创建任务失败时的兜底退款
        await wallet_service.refund_credits(db, user_id, cost, action_type)
        raise e


@router.post(
    "/verify-code",
    response_model=APIResponse[TaskSubmitResponse],
    dependencies=[
        Depends(RateLimiter(times=5, seconds=60, identifier=ai_rate_limit_key)),
    ],
)
async def process_code_verify(
    request: Request,
    payload: SHLCodeVerifyPayload,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    user_id = request.state.user.id
    client_ip = getattr(request.state, "real_ip", request.client.host)
    req_path = request.url.path

    # 拍照纠错统一计费策略
    cost = VERIFY_CODE_COST
    action_type = ActionType.USE_VISION_DIFF

    # ================= 1. 事前扣费 =================
    try:
        await wallet_service.consume_credits(
            db, user_id=user_id, amount=cost, action_type=action_type
        )
    except InsufficientCreditsException:
        return APIResponse(message="算力点数不足，请充值", code=402)

    try:
        # 2. 创建 PENDING 状态的任务记录
        new_task = AITask(
            user_id=user_id, task_type="CODE_VERIFY", status=TaskStatus.PENDING
        )
        db.add(new_task)
        await db.commit()
        await db.refresh(new_task)

        # 3. 把耗时工作丢进后台
        background_tasks.add_task(
            background_code_verify_task,
            task_id=new_task.task_id,
            user_id=user_id,
            payload=payload,
            cost=cost,
            action_type=action_type,
            ip=client_ip,
            request_path=req_path,
        )

        capture_event(
            str(user_id),
            "ai_task_started",
            properties={
                "task_type": "CODE_VERIFY",
                "cost": cost,
                "$ip": client_ip,
                "$user_agent": request.headers.get("user-agent", ""),
            },
        )

        return APIResponse(
            data={"task_id": new_task.task_id, "status": TaskStatus.PENDING.value}
        )

    except Exception as e:
        await db.rollback()
        # 创建任务失败时的兜底退款
        await wallet_service.refund_credits(db, user_id, cost, action_type)
        raise e


# ==========================================
# 3. 轮询查询任务进度 (GET)
# ==========================================
@router.get(
    "/task/{task_id}",
    dependencies=[
        Depends(
            RateLimiter(times=30, seconds=60, identifier=ai_rate_limit_key)
        ),  # 允许较高频率的轮询
    ],
)
async def get_task_status(
    request: Request,
    task_id: str,
    db: AsyncSession = Depends(get_db),
):
    user_id = request.state.user.id

    stmt = select(AITask).where(AITask.task_id == task_id)
    result = await db.execute(stmt)
    task = result.scalars().first()

    if not task:
        return APIResponse(message="任务不存在", code=404)

    # 安全隔离：自己的任务自己看
    if task.user_id != user_id:
        return APIResponse(message="无权访问该任务", code=403)

    response_data = {
        "task_id": task.task_id,
        "status": task.status.value,
        "task_type": task.task_type,
    }

    if task.status == TaskStatus.COMPLETED:
        response_data["result"] = task.result
    elif task.status == TaskStatus.FAILED:
        response_data["error"] = task.error_message

    return APIResponse(data=response_data)
