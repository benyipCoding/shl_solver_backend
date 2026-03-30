import json
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.clients import db as db_client
from app.models.ai_task import AITask, TaskStatus
from app.services.shl_analyze import shl_service
from app.services.wallet_service import wallet_service
from app.schemas.shl_analyze import SHLAnalyzePayload
from app.models.shl_solver import ActionType
from app.services.token_record import token_record_service
from app.utils.file_handler import (
    handle_shl_analyze_background_task,
)


async def background_shl_solver_task(
    task_id: str,
    user_id: int,
    payload: SHLAnalyzePayload,
    llm_key: str,
    cost: int,
    action_type: ActionType,
    ip: str,
    request_path: str,
):
    """
    纯后台执行的 AI 分析任务
    """
    # 🌟 关键：使用 db_client.async_session() 开启一个完全独立的生命周期 Session
    async with db_client.async_session() as db:
        assert isinstance(db, AsyncSession), "async_session 必须返回 AsyncSession 实例"
        try:
            # 1. 任务开始，更新状态为 PROCESSING (处理中)
            stmt = select(AITask).where(AITask.task_id == task_id)
            result = await db.execute(stmt)
            task = result.scalars().first()

            if not task:
                print(f"Error: 找不到任务 {task_id}")
                return

            task.status = TaskStatus.PROCESSING
            await db.commit()

            # 2. 调用真正的 AI 分析逻辑
            ai_result, token_count = await shl_service.analyze(
                payload=payload,
                llm_key=llm_key,
            )

            if isinstance(ai_result, list):
                ai_result = ai_result[0] if ai_result else {}

            # 3-1. AI 调用成功，保存结果并标记为 COMPLETED
            task.status = TaskStatus.COMPLETED
            task.result = ai_result
            await db.commit()

            # 4. 记录 token 使用情况，方便后续分析和统计
            await token_record_service.record_token_usage(
                ip=ip,
                request_path=request_path,
                user_id=user_id,
                db=db,
                token_count=token_count,
                model=llm_key,
            )

            # 5. 触发保存图片和历史记录的后台任务
            await handle_shl_analyze_background_task(
                images_data=payload.images_data,
                user_id=user_id,
                model=llm_key,
                token_count=token_count,
                result_data=ai_result,
                status="completed",
            )

        except Exception as e:
            print(f"后台任务 {task_id} 执行失败: {e}")

            # 3-2. 发生异常，记录错误信息并标记为 FAILED
            if task:
                task.status = TaskStatus.FAILED
                task.error_message = str(e)

            # 4. 执行退款兜底逻辑，把点数还给用户！
            await wallet_service.refund_credits(
                db=db, user_id=user_id, amount=cost, action_type=action_type
            )
            await db.commit()

            await handle_shl_analyze_background_task(
                images_data=payload.images_data,
                user_id=user_id,
                model=llm_key,
                token_count=0,
                result_data={"error": str(e)},
                status="failed",
            )
