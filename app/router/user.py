from fastapi import APIRouter, Depends, Request, Response
from sqlalchemy.ext.asyncio import AsyncSession
from app.clients.db import get_db
from app.services.user import user_service
from app.services.wallet_service import wallet_service
from app.depends.jwt_guard import verify_user
from app.schemas.response import APIResponse
from app.schemas.user import UserSerializer, UserCreditLogSerializer
from typing import List


router = APIRouter(prefix="/user", tags=["User"], dependencies=[Depends(verify_user)])


@router.get(
    "/me",
    response_model=APIResponse[UserSerializer],
)
async def read_current_user(request: Request, db: AsyncSession = Depends(get_db)):
    user = request.state.user
    return APIResponse(data=user)


@router.get("/balance")
async def read_user_balance(request: Request, db: AsyncSession = Depends(get_db)):
    user = request.state.user
    balance = await wallet_service.get_balance(db, user.id)
    return APIResponse(data=balance)


@router.get("/credit-logs", response_model=APIResponse[List[UserCreditLogSerializer]])
async def read_user_credit_logs(
    request: Request,
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
):
    user = request.state.user
    logs = await wallet_service.get_credit_logs(db, user.id, skip=skip, limit=limit)
    return APIResponse(data=logs)


@router.get("/{user_id}", response_model=APIResponse[UserSerializer])
async def read_user(user_id: int, db: AsyncSession = Depends(get_db)):
    user = await user_service.get_user_by_id(db, user_id)
    if not user:
        return APIResponse(message="User not found", code=404)
    return APIResponse(data=user)
