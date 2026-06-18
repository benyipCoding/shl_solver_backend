import logging
from datetime import timedelta

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.market_data import (
    MarketBarSyncState,
    MarketInstrument,
    MarketOHLCVBar,
)
from app.services.fxcm_market_sync.bar_sync import bar_sync_handler
from app.services.fxcm_market_sync.constants import PROVIDER
from app.services.fxcm_market_sync.intervals import (
    calculate_next_sync_from,
    state_priority,
)
from app.services.fxcm_market_sync.scheduling_policy import (
    is_allowed_today,
    weekend_resume_at,
)
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
        """为热池品种按周期创建或修复同步状态任务。"""
        disable_stmt = select(MarketBarSyncState).where(
            MarketBarSyncState.provider == PROVIDER,
            MarketBarSyncState.enabled.is_(True),
            MarketBarSyncState.interval.not_in(sync_intervals),
        )
        stale_states = (await db.execute(disable_stmt)).scalars().all()
        for stale in stale_states:
            stale.enabled = False

        stmt = select(MarketInstrument).where(
            MarketInstrument.provider == PROVIDER,
            MarketInstrument.is_active.is_(True),
            MarketInstrument.is_hot.is_(True),
        )
        instruments = (await db.execute(stmt)).scalars().all()

        created_count = 0
        for instrument in instruments:
            for interval in sync_intervals:
                exists_stmt = select(MarketBarSyncState).where(
                    MarketBarSyncState.instrument_id == instrument.id,
                    MarketBarSyncState.interval == interval,
                    MarketBarSyncState.price_type == "mid",
                )
                existing = (await db.execute(exists_stmt)).scalars().first()
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
                        next_sync_from=calculate_next_sync_from(
                            interval=interval,
                            now=utc_now(),
                            skip_weekends=False,
                        ),
                        backfill_completed=False,
                        last_status="IDLE",
                    )
                )
                created_count += 1
        return created_count

    async def sync_due_states(
        self,
        db: AsyncSession,
        *,
        result: FXCMMarketSyncResult,
        force_due: bool,
    ) -> int:
        """挑选到期状态并逐个执行同步，记录成功失败统计。"""
        now = utc_now()
        bar_count_subquery = (
            select(
                MarketOHLCVBar.instrument_id.label("instrument_id"),
                func.count().label("bar_count"),
            )
            .where(MarketOHLCVBar.provider == PROVIDER)
            .group_by(MarketOHLCVBar.instrument_id)
            .subquery()
        )

        stmt = (
            select(MarketBarSyncState, MarketInstrument)
            .join(
                MarketInstrument,
                MarketInstrument.id == MarketBarSyncState.instrument_id,
            )
            .outerjoin(
                bar_count_subquery,
                bar_count_subquery.c.instrument_id == MarketBarSyncState.instrument_id,
            )
            .where(
                MarketBarSyncState.provider == PROVIDER,
                MarketBarSyncState.enabled.is_(True),
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
            processed += 1
            state_id = state.id
            instrument_id = state.instrument_id
            interval = state.interval
            price_type = state.price_type
            instrument_symbol = instrument.symbol

            if not is_allowed_today(instrument, now):
                tomorrow = (now + timedelta(days=1)).replace(
                    hour=0, minute=5, second=0, microsecond=0
                )
                state.next_sync_from = tomorrow
                state.last_status = "SKIPPED"
                state.last_error = "Skipped by daily category rotation policy"
                state.last_attempt_at = now
                await db.commit()
                continue

            if state.backfill_completed:
                resume_at = weekend_resume_at(instrument, now)
                if resume_at is not None:
                    state.next_sync_from = resume_at
                    state.last_status = "SKIPPED"
                    state.last_error = None
                    state.last_attempt_at = now
                    await db.commit()
                    continue

            try:
                inserted = await bar_sync_handler.sync_single_state(
                    db, state, instrument
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
