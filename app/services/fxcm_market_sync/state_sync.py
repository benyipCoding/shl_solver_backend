import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.market_data import (
    MarketBarSyncState,
    MarketInstrument,
    MarketOHLCVBar,
    MarketSyncQueueItem,
)
from app.services.fxcm_market_sync.bar_sync import bar_sync_handler
from app.services.fxcm_market_sync.constants import (
    PROVIDER,
    SUPPORTED_INTERVALS,
    SYNC_MODE_BACKFILL,
    SYNC_MODE_COMPLETED,
)
from app.services.fxcm_market_sync.history_cutoff import is_history_complete
from app.services.fxcm_market_sync.intervals import state_priority
from app.services.fxcm_market_sync.types import FXCMMarketSyncResult, utc_now


logger = logging.getLogger(__name__)


class StateSyncHandler:
    """负责同步状态任务的初始化与到期调度执行。"""

    async def bootstrap_sync_states(
        self,
        db: AsyncSession,
        *,
        sync_intervals: list[str],
    ) -> int:
        """为采集队列中的品种按周期创建或修复同步状态任务。"""
        disable_stmt = select(MarketBarSyncState).where(
            MarketBarSyncState.provider == PROVIDER,
            MarketBarSyncState.enabled.is_(True),
            MarketBarSyncState.interval.not_in(sync_intervals),
        )
        stale_states = (await db.execute(disable_stmt)).scalars().all()
        for stale in stale_states:
            stale.enabled = False

        stmt = (
            select(MarketInstrument)
            .join(
                MarketSyncQueueItem,
                MarketSyncQueueItem.instrument_id == MarketInstrument.id,
            )
            .where(
                MarketInstrument.provider == PROVIDER,
                MarketInstrument.is_active.is_(True),
            )
        )
        instruments = (await db.execute(stmt)).scalars().all()

        created_count = 0
        for instrument in instruments:
            created_count += await self.ensure_instrument_states(
                db, instrument, sync_intervals=sync_intervals
            )
        return created_count

    async def ensure_instrument_states(
        self,
        db: AsyncSession,
        instrument: MarketInstrument,
        *,
        sync_intervals: list[str],
    ) -> int:
        """确保单个品种在每个同步周期都有启用的状态任务，返回新建数量。"""
        existing_states = (
            (
                await db.execute(
                    select(MarketBarSyncState).where(
                        MarketBarSyncState.instrument_id == instrument.id,
                        MarketBarSyncState.price_type == "mid",
                    )
                )
            )
            .scalars()
            .all()
        )
        existing_by_interval = {state.interval: state for state in existing_states}

        created_count = 0
        for interval in sync_intervals:
            existing = existing_by_interval.get(interval)
            if existing is not None:
                existing.enabled = True
                existing.target_history_bars = settings.fxcm_sync_backfill_bars
                existing.priority = state_priority(interval, sync_intervals)
                continue

            db.add(
                MarketBarSyncState(
                    instrument_id=instrument.id,
                    provider=PROVIDER,
                    interval=interval,
                    price_type="mid",
                    enabled=True,
                    priority=state_priority(interval, sync_intervals),
                    target_history_bars=settings.fxcm_sync_backfill_bars,
                    sync_mode="BACKFILL",
                    # 新加入队列的品种立即可调度，不等周期冷却（1week 冷却长达 6 小时）。
                    next_sync_from=utc_now(),
                    backfill_completed=False,
                    last_status="IDLE",
                )
            )
            created_count += 1
        return created_count

    async def reconcile_completion(
        self,
        db: AsyncSession,
        *,
        cutoff: datetime,
    ) -> dict[str, int]:
        """按截止时间重新判定全部状态：已覆盖的标记完成，不再覆盖的重新排入采集。"""
        states = (
            (
                await db.execute(
                    select(MarketBarSyncState).where(
                        MarketBarSyncState.provider == PROVIDER,
                        MarketBarSyncState.interval.in_(SUPPORTED_INTERVALS),
                    )
                )
            )
            .scalars()
            .all()
        )
        completed = 0
        reopened = 0
        for state in states:
            if is_history_complete(state, cutoff):
                if state.sync_mode != SYNC_MODE_COMPLETED:
                    state.sync_mode = SYNC_MODE_COMPLETED
                    state.next_sync_from = None
                    completed += 1
            elif state.sync_mode != SYNC_MODE_BACKFILL:
                state.sync_mode = SYNC_MODE_BACKFILL
                state.next_sync_from = utc_now()
                reopened += 1
        return {"completed": completed, "reopened": reopened}

    async def sync_due_states(
        self,
        db: AsyncSession,
        *,
        result: FXCMMarketSyncResult,
        force_due: bool,
        cutoff: datetime,
        before_batch: (
            Callable[[AsyncSession, FXCMMarketSyncResult], Awaitable[Any]] | None
        ) = None,
    ) -> int:
        """挑选采集队列内未完成的到期状态，逐个采集历史数据并记录成功失败统计。"""
        now = utc_now()
        if before_batch is not None:
            await before_batch(db, result)
        bar_count_subquery = (
            select(
                MarketOHLCVBar.instrument_id.label("instrument_id"),
                func.count().label("bar_count"),
            )
            .where(
                MarketOHLCVBar.provider == PROVIDER,
                MarketOHLCVBar.interval.in_(SUPPORTED_INTERVALS),
            )
            .group_by(MarketOHLCVBar.instrument_id)
            .subquery()
        )

        stmt = (
            select(MarketBarSyncState, MarketInstrument)
            .join(
                MarketInstrument,
                MarketInstrument.id == MarketBarSyncState.instrument_id,
            )
            .join(
                MarketSyncQueueItem,
                MarketSyncQueueItem.instrument_id == MarketBarSyncState.instrument_id,
            )
            .outerjoin(
                bar_count_subquery,
                bar_count_subquery.c.instrument_id == MarketBarSyncState.instrument_id,
            )
            .where(
                MarketBarSyncState.provider == PROVIDER,
                MarketBarSyncState.interval.in_(SUPPORTED_INTERVALS),
                MarketBarSyncState.enabled.is_(True),
                MarketBarSyncState.sync_mode != SYNC_MODE_COMPLETED,
                MarketInstrument.is_active.is_(True),
            )
            .order_by(
                func.coalesce(bar_count_subquery.c.bar_count, 0).asc(),
                MarketBarSyncState.last_attempt_at.asc().nullsfirst(),
                MarketBarSyncState.priority.asc(),
            )
            .limit(10)
        )
        if not force_due:
            stmt = stmt.where(
                or_(
                    MarketBarSyncState.next_sync_from.is_(None),
                    MarketBarSyncState.next_sync_from <= now,
                )
            )

        rows = (await db.execute(stmt)).all()
        seen_instrument_ids: set[int] = set()
        deduped_rows = []
        for row in rows:
            iid = row[1].id
            if iid not in seen_instrument_ids:
                seen_instrument_ids.add(iid)
                deduped_rows.append(row)
        rows = deduped_rows

        processed = 0
        for state, instrument in rows:
            if before_batch is not None:
                await before_batch(db, result)

            processed += 1
            state_id = state.id
            instrument_id = state.instrument_id
            interval = state.interval
            price_type = state.price_type
            instrument_symbol = instrument.symbol

            try:
                inserted = await bar_sync_handler.sync_history_state(
                    db, state, instrument, cutoff=cutoff
                )
                await db.commit()
                result.rows_upserted += inserted
                result.succeeded_states += 1
            except Exception as exc:
                await db.rollback()
                logger.exception(
                    "FXCM state sync failed",
                    extra={
                        "instrument_id": instrument_id,
                        "interval": interval,
                        "price_type": price_type,
                    },
                )
                persisted_state = await db.get(MarketBarSyncState, state_id)
                if persisted_state is not None:
                    persisted_state.last_status = "FAILED"
                    persisted_state.last_error = f"{type(exc).__name__}: {exc}"
                    persisted_state.last_attempt_at = utc_now()
                    persisted_state.retry_count = (persisted_state.retry_count or 0) + 1
                    persisted_state.next_sync_from = utc_now() + timedelta(
                        minutes=min(30, max(5, persisted_state.retry_count * 5))
                    )
                    await db.commit()
                result.failed_states += 1
                result.errors.append(
                    f"{instrument_symbol}/{interval}: {type(exc).__name__}: {exc}"
                )
        return processed


state_sync_handler = StateSyncHandler()
