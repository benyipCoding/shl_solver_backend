from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class BacktestSessionCreate(BaseModel):
    model_config = ConfigDict(extra="ignore")

    client_session_id: str | None = Field(None, max_length=36)
    symbol: str = Field(..., min_length=1, max_length=64)
    interval: str = Field(..., min_length=1, max_length=16)
    timeframe: str | None = Field(None, max_length=16)
    price_type: str = Field("mid", max_length=16)
    start_bar_time: datetime | int | float | str
    start_bar_index: int | None = None
    initial_visible_bars: int = Field(200, ge=1)
    cursor_bar_time: datetime | int | float | str | None = None
    cursor_bar_index: int | None = None
    initial_balance: Decimal = Field(Decimal("10000"))


class BacktestEventCreate(BaseModel):
    model_config = ConfigDict(extra="ignore")

    event_type: Literal["OPEN", "MODIFY_SL", "MODIFY_TP", "CLOSE"]
    client_trade_id: str = Field(..., min_length=1, max_length=64)
    client_event_id: str | None = Field(None, min_length=1, max_length=128)
    bar_time: datetime | int | float | str
    bar_index: int | None = None
    side: str | None = Field(None, max_length=8)
    units: int | None = Field(None, ge=1)
    price: Decimal | None = Field(None, allow_inf_nan=False)
    sl_price: Decimal | None = None
    tp_price: Decimal | None = None
    close_reason: str | None = Field(None, max_length=32)


class BacktestSessionComplete(BaseModel):
    model_config = ConfigDict(extra="ignore")

    cursor_bar_time: datetime | int | float | str | None = None
    cursor_bar_index: int | None = None
    ending_balance: Decimal | None = None
    mark_price: Decimal | None = None


class BacktestSessionView(BaseModel):
    public_id: str
    client_session_id: str | None = None
    symbol: str
    interval: str
    timeframe: str | None = None
    status: str
    visibility: str
    start_bar_time: datetime
    start_bar_index: int | None = None
    initial_visible_bars: int
    cursor_bar_time: datetime | None = None
    cursor_bar_index: int | None = None
    initial_balance: Decimal
    ending_balance: Decimal | None = None
    trade_count: int
    closed_trade_count: int
    win_count: int
    realized_pnl: Decimal
    created_at: datetime | None = None
    ended_at: datetime | None = None


class BacktestSessionListResponse(BaseModel):
    items: list[BacktestSessionView]
    total: int
    page: int
    size: int


class BacktestEventResult(BaseModel):
    public_id: str
    client_trade_id: str
    event_type: str
    sequence_no: int
    trade_status: str
    extra: dict[str, Any] | None = None
