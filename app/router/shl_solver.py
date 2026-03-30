from fastapi import APIRouter, Depends, Query, Path, HTTPException, Body
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import update, select, func

from app.clients.db import get_db
from app.schemas.response import APIResponse
from app.schemas.shl_solver import (
    SHLSolverHistorySerializer,
    SHLSolverHistoryListResponse,
    SHLSolverHistoryPatch,
)
from app.services.shl_solver import shl_solver_service
from app.depends.jwt_guard import verify_user
from app.models.shl_solver import SHLSolverHistory

router = APIRouter(
    prefix="/shl_history",
    tags=["SHL Solver History"],
    dependencies=[Depends(verify_user)],
)


@router.get("", response_model=APIResponse[SHLSolverHistoryListResponse])
async def list_shl_history(
    page: int = Query(1, ge=1, description="Page number"),
    size: int = Query(10, ge=1, le=100, description="Page size"),
    db: AsyncSession = Depends(get_db),
    user=Depends(verify_user),
):
    """
    Batch retrieve SHL solver history.
    """
    items, total = await shl_solver_service.get_history_list(
        db, page, size, user_id=user.id
    )

    return APIResponse(
        data=SHLSolverHistoryListResponse(
            items=items, total=total, page=page, size=size
        )
    )


@router.get("/unread_count", response_model=APIResponse)
async def get_unread_count(
    db: AsyncSession = Depends(get_db),
    user=Depends(verify_user),
):
    """
    Get the count of unread SHL solver history records.
    """
    query = (
        select(func.count())
        .select_from(SHLSolverHistory)
        .where(SHLSolverHistory.is_readed == False)
    )

    if not getattr(user, "is_superuser", False) and not getattr(
        user, "is_staff", False
    ):
        query = query.where(SHLSolverHistory.user_id == user.id)

    result = await db.execute(query)
    count = result.scalar() or 0

    return APIResponse(data={"unread_count": count})


@router.get("/{id}", response_model=APIResponse[SHLSolverHistorySerializer])
async def get_shl_history_detail(
    id: int = Path(..., description="History ID"), db: AsyncSession = Depends(get_db)
):
    """
    Get a single SHL solver history record.
    """
    item = await shl_solver_service.get_history_detail(db, id)

    if not item:
        return APIResponse(code=404, message="SHL Solver History not found")

    return APIResponse(data=item)


@router.patch("/{id}", response_model=APIResponse)
async def update_shl_history(
    id: int = Path(..., description="History ID"),
    payload: SHLSolverHistoryPatch = Body(...),
    db: AsyncSession = Depends(get_db),
    user=Depends(verify_user),
):
    """
    Update a SHL solver history record.
    """
    # 确保记录存在且属于当前用户（或者用户为管理员也可以调整权限）
    item = await db.get(SHLSolverHistory, id)
    if not item:
        return APIResponse(code=404, message="SHL Solver History not found")

    if (
        getattr(item, "user_id", None) != user.id
        and not getattr(user, "is_superuser", False)
        and not getattr(user, "is_staff", False)
    ):
        return APIResponse(code=403, message="Permission denied")

    update_data = payload.model_dump(exclude_unset=True)
    if not update_data:
        return APIResponse(message="No fields to update")

    for key, value in update_data.items():
        setattr(item, key, value)

    await db.commit()
    await db.refresh(item)

    return APIResponse(message="SHL Solver History updated successfully")
