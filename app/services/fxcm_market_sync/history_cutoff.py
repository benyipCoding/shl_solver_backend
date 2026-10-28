from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.market_data import MarketBarSyncState, MarketSyncConfig
from app.services.fxcm_market_sync.constants import DEFAULT_HISTORY_CUTOFF
from app.services.fxcm_market_sync.intervals import interval_delta
from app.services.fxcm_market_sync.utils import parse_request_payload_datetime


HISTORY_CUTOFF_KEY = "history_cutoff_at"

# state.meta 中记录向前采集已推进到的时间点；无数据的时段（周末、停牌）也会推进，避免卡在空窗口。
FORWARD_CURSOR_KEY = "history_forward_until"


async def get_history_cutoff(db: AsyncSession) -> datetime:
    """读取历史数据截止时间，未配置时使用默认值。"""
    value = await db.scalar(
        select(MarketSyncConfig.value).where(
            MarketSyncConfig.key == HISTORY_CUTOFF_KEY
        )
    )
    return parse_request_payload_datetime(value) or DEFAULT_HISTORY_CUTOFF


async def save_history_cutoff(db: AsyncSession, cutoff: datetime) -> None:
    config = await db.get(MarketSyncConfig, HISTORY_CUTOFF_KEY)
    if config is None:
        db.add(MarketSyncConfig(key=HISTORY_CUTOFF_KEY, value=cutoff.isoformat()))
    else:
        config.value = cutoff.isoformat()


def get_forward_cursor(state: MarketBarSyncState) -> datetime | None:
    if not isinstance(state.meta, dict):
        return None
    return parse_request_payload_datetime(state.meta.get(FORWARD_CURSOR_KEY))


def set_forward_cursor(state: MarketBarSyncState, value: datetime) -> None:
    # JSON 列不追踪原地修改，需整体赋值。
    meta = dict(state.meta) if isinstance(state.meta, dict) else {}
    meta[FORWARD_CURSOR_KEY] = value.isoformat()
    state.meta = meta


def forward_reached(state: MarketBarSyncState, cutoff: datetime) -> bool:
    """截止时间前的最后一根 K 线是否已采集（或向前采集已推进到截止时间）。"""
    latest = state.latest_synced_bar_time
    if latest is not None and latest + interval_delta(state.interval) >= cutoff:
        return True
    cursor = get_forward_cursor(state)
    return cursor is not None and cursor >= cutoff


def is_history_complete(state: MarketBarSyncState, cutoff: datetime) -> bool:
    """向前覆盖到截止时间、向后回补到下限，即视为该品种周期的历史数据采集完成。"""
    if not forward_reached(state, cutoff):
        return False
    if state.latest_synced_bar_time is None:
        # 截止时间之前 FXCM 没有任何数据，无可采集。
        return True
    return bool(state.backfill_completed)
