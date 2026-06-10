from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.clients.db import get_db
from app.depends.jwt_guard import verify_superuser
from app.models.shl_solver import ActionType
from app.models.user import CreditType, User
from app.schemas.admin import (
    AdminCreditLogItem,
    AdminCreditLogListResponse,
    AdminFXCMMarketSyncStatusResponse,
    AdminFXCMMarketSyncTriggerResponse,
    AdminTokenRecordItem,
    AdminTokenRecordListResponse,
    AdminUserDetail,
    AdminUserListResponse,
    AdminUserUpdateRequest,
    AdminWalletListResponse,
    AdminWalletRechargeRequest,
    AdminWalletRechargeResponse,
    AdminWalletSummary,
)
from app.schemas.response import APIResponse
from app.services.admin import admin_service
from app.services.fxcm_market_sync import (
    fxcm_market_sync_scheduler,
    fxcm_market_sync_service,
)


router = APIRouter(
    prefix="/admin",
    tags=["Admin"],
    dependencies=[Depends(verify_superuser)],
)


@router.get(
    "/users",
    response_model=APIResponse[AdminUserListResponse],
    summary="分页查询用户列表",
    description="支持按邮箱、用户名、手机号模糊查询，并返回每个用户的钱包汇总信息。",
)
async def list_admin_users(
    page: int = Query(1, ge=1, description="页码，从 1 开始"),
    page_size: int = Query(20, ge=1, le=100, description="每页条数，最大 100"),
    keyword: Optional[str] = Query(None, description="邮箱、用户名、手机号关键字"),
    is_active: Optional[bool] = Query(None, description="按账号启用状态过滤"),
    is_staff: Optional[bool] = Query(None, description="按员工账号状态过滤"),
    is_superuser: Optional[bool] = Query(None, description="按超级管理员状态过滤"),
    db: AsyncSession = Depends(get_db),
):
    data = await admin_service.list_users(
        db=db,
        page=page,
        page_size=page_size,
        keyword=keyword,
        is_active=is_active,
        is_staff=is_staff,
        is_superuser=is_superuser,
    )
    return APIResponse(data=data)


@router.get(
    "/users/{user_id}",
    response_model=APIResponse[AdminUserDetail],
    summary="获取用户详情",
    description="返回用户基础信息、钱包余额和关联流水统计。",
)
async def get_admin_user_detail(
    user_id: int,
    db: AsyncSession = Depends(get_db),
):
    data = await admin_service.get_user_detail(db=db, user_id=user_id)
    return APIResponse(data=data)


@router.patch(
    "/users/{user_id}",
    response_model=APIResponse[AdminUserDetail],
    summary="更新用户资料和权限",
    description="支持修改用户名、邮箱、手机号，以及 is_active、is_staff、is_superuser 等后台管理字段。",
)
async def update_admin_user(
    user_id: int,
    payload: AdminUserUpdateRequest,
    current_user: User = Depends(verify_superuser),
    db: AsyncSession = Depends(get_db),
):
    data = await admin_service.update_user(
        db=db,
        user_id=user_id,
        payload=payload,
        operator_user_id=current_user.id,
    )
    return APIResponse(message="用户信息更新成功", data=data)


@router.get(
    "/wallets",
    response_model=APIResponse[AdminWalletListResponse],
    summary="分页查询钱包列表",
    description="返回用户钱包余额，可按关键字过滤，并可只看已创建钱包的用户。",
)
async def list_admin_wallets(
    page: int = Query(1, ge=1, description="页码，从 1 开始"),
    page_size: int = Query(20, ge=1, le=100, description="每页条数，最大 100"),
    keyword: Optional[str] = Query(None, description="邮箱、用户名、手机号关键字"),
    only_with_wallet: bool = Query(False, description="是否只返回已创建钱包的用户"),
    db: AsyncSession = Depends(get_db),
):
    data = await admin_service.list_wallets(
        db=db,
        page=page,
        page_size=page_size,
        keyword=keyword,
        only_with_wallet=only_with_wallet,
    )
    return APIResponse(data=data)


@router.get(
    "/wallets/{user_id}",
    response_model=APIResponse[AdminWalletSummary],
    summary="获取用户钱包详情",
    description="按用户 ID 获取钱包余额、最近重置时间和钱包记录时间。",
)
async def get_admin_wallet_detail(
    user_id: int,
    db: AsyncSession = Depends(get_db),
):
    data = await admin_service.get_wallet_detail(db=db, user_id=user_id)
    return APIResponse(data=data)


@router.post(
    "/wallets/{user_id}/recharge",
    response_model=APIResponse[AdminWalletRechargeResponse],
    summary="给用户钱包充值",
    description="向指定用户的钱包增加付费算力，并同步写入 user_credit_log 流水。",
)
async def recharge_admin_wallet(
    user_id: int,
    payload: AdminWalletRechargeRequest,
    db: AsyncSession = Depends(get_db),
):
    data = await admin_service.recharge_wallet(
        db=db,
        user_id=user_id,
        amount=payload.amount,
    )
    return APIResponse(message="钱包充值成功", data=data)


@router.get(
    "/credit-logs",
    response_model=APIResponse[AdminCreditLogListResponse],
    summary="分页查询消费记录",
    description="消费记录基于 user_credit_log，可按用户、点数类型、动作类型和时间范围筛选。",
)
async def list_admin_credit_logs(
    page: int = Query(1, ge=1, description="页码，从 1 开始"),
    page_size: int = Query(20, ge=1, le=100, description="每页条数，最大 100"),
    user_id: Optional[int] = Query(None, description="按用户 ID 过滤"),
    keyword: Optional[str] = Query(None, description="按邮箱或用户名模糊查询"),
    credit_type: Optional[CreditType] = Query(None, description="点数类型过滤"),
    action_type: Optional[ActionType] = Query(None, description="动作类型过滤"),
    start_at: Optional[datetime] = Query(None, description="开始时间，ISO 8601 格式"),
    end_at: Optional[datetime] = Query(None, description="结束时间，ISO 8601 格式"),
    db: AsyncSession = Depends(get_db),
):
    data = await admin_service.list_credit_logs(
        db=db,
        page=page,
        page_size=page_size,
        user_id=user_id,
        keyword=keyword,
        credit_type=credit_type,
        action_type=action_type,
        start_at=start_at,
        end_at=end_at,
    )
    return APIResponse(data=data)


@router.get(
    "/credit-logs/{log_id}",
    response_model=APIResponse[AdminCreditLogItem],
    summary="获取单条消费记录详情",
)
async def get_admin_credit_log_detail(
    log_id: int,
    db: AsyncSession = Depends(get_db),
):
    data = await admin_service.get_credit_log_detail(db=db, log_id=log_id)
    return APIResponse(data=data)


@router.get(
    "/token-records",
    response_model=APIResponse[AdminTokenRecordListResponse],
    summary="分页查询 Token 记录",
    description="基于 token_record 表，可按用户、模型、IP、请求路径和时间范围筛选。",
)
async def list_admin_token_records(
    page: int = Query(1, ge=1, description="页码，从 1 开始"),
    page_size: int = Query(20, ge=1, le=100, description="每页条数，最大 100"),
    user_id: Optional[int] = Query(None, description="按用户 ID 过滤"),
    keyword: Optional[str] = Query(
        None, description="按邮箱、用户名、IP、请求路径或模型模糊查询"
    ),
    model: Optional[str] = Query(None, description="按模型名称精确过滤"),
    start_at: Optional[datetime] = Query(None, description="开始时间，ISO 8601 格式"),
    end_at: Optional[datetime] = Query(None, description="结束时间，ISO 8601 格式"),
    db: AsyncSession = Depends(get_db),
):
    data = await admin_service.list_token_records(
        db=db,
        page=page,
        page_size=page_size,
        user_id=user_id,
        keyword=keyword,
        model=model,
        start_at=start_at,
        end_at=end_at,
    )
    return APIResponse(data=data)


@router.get(
    "/token-records/{record_id}",
    response_model=APIResponse[AdminTokenRecordItem],
    summary="获取单条 Token 记录详情",
)
async def get_admin_token_record_detail(
    record_id: int,
    db: AsyncSession = Depends(get_db),
):
    data = await admin_service.get_token_record_detail(db=db, record_id=record_id)
    return APIResponse(data=data)


@router.get(
    "/market-data/status",
    response_model=APIResponse[AdminFXCMMarketSyncStatusResponse],
    summary="查看 FXCM 市场数据同步状态",
    description="返回本地 FXCM 品种、别名、K 线和同步状态数量，以及当前调度配置。",
)
async def get_market_data_sync_status(
    db: AsyncSession = Depends(get_db),
):
    payload = await fxcm_market_sync_service.get_status(db)
    payload["scheduler_running"] = fxcm_market_sync_scheduler.is_running()
    return APIResponse(data=AdminFXCMMarketSyncStatusResponse.model_validate(payload))


@router.post(
    "/market-data/sync",
    response_model=APIResponse[AdminFXCMMarketSyncTriggerResponse],
    summary="手动触发 FXCM 市场数据同步",
    description="mode 支持 all、metadata、bars；bars 默认会强制忽略 next_sync_from。",
)
async def trigger_market_data_sync(
    mode: str = Query(
        "all",
        pattern="^(all|metadata|bars)$",
        description="同步模式：all、metadata、bars",
    ),
    force_due: bool = Query(
        True,
        description="仅对 bars 或 all 有效，是否忽略 next_sync_from 直接执行到期状态。",
    ),
    db: AsyncSession = Depends(get_db),
):
    result = await fxcm_market_sync_service.run_manual(
        db,
        mode=mode,
        force_due=force_due,
    )
    return APIResponse(
        data=AdminFXCMMarketSyncTriggerResponse.model_validate(result.to_dict())
    )
