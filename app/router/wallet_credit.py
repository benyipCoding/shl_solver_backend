from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.schemas.wallet_credit import RechargeRequest

from app.clients.db import get_db
from app.depends.jwt_guard import verify_user
from app.models.user import User
from app.schemas.response import APIResponse
from app.services.wallet_service import wallet_service

router = APIRouter(prefix="/wallet_credit", tags=["Wallet Credit"])


@router.post("/recharge", response_model=APIResponse[dict])
async def recharge_credit(
    req: RechargeRequest,
    current_user: User = Depends(verify_user),
    db: AsyncSession = Depends(get_db),
):
    """
    给对应邮箱的用户充值算力（仅限 is_staff 或 is_superuser 操作）
    """
    if not (current_user.is_staff or current_user.is_superuser):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="权限不足，仅管理员可充值算力"
        )

    if req.points <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="充值点数必须大于0"
        )

    balance_after = await wallet_service.recharge_credit(db, req.email, req.points)

    return APIResponse(
        code=200,
        message="充值成功",
        data={
            "email": req.email,
            "recharged_points": req.points,
            "balance_after": balance_after,
        },
    )
