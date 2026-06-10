from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, EmailStr, Field, field_validator

from app.models.shl_solver import ActionType
from app.models.user import CreditType


class PaginationMeta(BaseModel):
    total: int
    page: int
    page_size: int
    total_pages: int


class AdminUserSummary(BaseModel):
    id: int
    username: str
    email: EmailStr
    mobile_phone: Optional[str] = None
    is_active: bool
    is_staff: bool
    is_superuser: bool
    total_token_count: int
    free_credits: int
    paid_credits: int
    wallet_total_credits: int
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class AdminUserDetail(AdminUserSummary):
    deleted_at: Optional[datetime] = None
    credit_log_count: int
    token_record_count: int


class AdminUserListResponse(BaseModel):
    items: List[AdminUserSummary]
    pagination: PaginationMeta


class AdminUserUpdateRequest(BaseModel):
    username: Optional[str] = Field(default=None, min_length=1, max_length=100)
    email: Optional[EmailStr] = None
    mobile_phone: Optional[str] = Field(default=None, max_length=20)
    is_active: Optional[bool] = None
    is_staff: Optional[bool] = None
    is_superuser: Optional[bool] = None

    @field_validator("username", "mobile_phone", mode="before")
    @classmethod
    def normalize_optional_text(cls, value):
        if value is None:
            return value
        if isinstance(value, str):
            value = value.strip()
            return value or None
        return value


class AdminWalletSummary(BaseModel):
    wallet_id: Optional[int] = None
    user_id: int
    username: str
    email: EmailStr
    mobile_phone: Optional[str] = None
    free_credits: int
    paid_credits: int
    total_credits: int
    last_reset_date: Optional[date] = None
    wallet_created_at: Optional[datetime] = None
    wallet_updated_at: Optional[datetime] = None


class AdminWalletListResponse(BaseModel):
    items: List[AdminWalletSummary]
    pagination: PaginationMeta


class AdminWalletRechargeRequest(BaseModel):
    amount: int = Field(..., gt=0, description="本次增加的付费算力点数")


class AdminWalletRechargeResponse(BaseModel):
    user_id: int
    username: str
    email: EmailStr
    recharged_points: int
    free_credits: int
    paid_credits: int
    balance_after: int


class AdminCreditLogItem(BaseModel):
    id: int
    user_id: int
    username: Optional[str] = None
    email: Optional[EmailStr] = None
    amount: int
    credit_type: CreditType
    action_type: ActionType
    balance_after: int
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class AdminCreditLogListResponse(BaseModel):
    items: List[AdminCreditLogItem]
    pagination: PaginationMeta


class AdminTokenRecordItem(BaseModel):
    id: int
    user_id: int
    username: Optional[str] = None
    email: Optional[EmailStr] = None
    ip: str
    request_path: Optional[str] = None
    model: Optional[str] = None
    token_count: int
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class AdminTokenRecordListResponse(BaseModel):
    items: List[AdminTokenRecordItem]
    pagination: PaginationMeta


class AdminFXCMMarketSyncTriggerResponse(BaseModel):
    reason: str
    skipped: bool
    metadata_synced: bool
    synced_instruments: int
    bootstrap_states: int
    processed_states: int
    succeeded_states: int
    failed_states: int
    rows_upserted: int
    errors: List[str]
    finished_at: Optional[datetime] = None


class AdminFXCMMarketSyncStatusResponse(BaseModel):
    scheduler_enabled: bool
    scheduler_running: bool
    lock_held: bool
    hot_symbols: List[str]
    metadata_interval_hours: int
    bar_intervals: List[str]
    instrument_count: int
    alias_count: int
    bar_count: int
    state_count: int
    enabled_state_count: int
    due_state_count: int
    failed_state_count: int
    last_metadata_sync_at: Optional[datetime] = None
