import asyncio
import logging
from collections import deque
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.market_data import (
    MarketBarSyncState,
    MarketInstrument,
    MarketInstrumentAlias,
    MarketOHLCVBar,
)
from app.services.fxcm_market_sync.bar_sync import bar_sync_handler
from app.services.fxcm_market_sync.constants import PROVIDER, SUPPORTED_INTERVALS
from app.services.fxcm_market_sync.instrument_sync import instrument_sync_handler
from app.services.fxcm_market_sync.intervals import (
    calculate_next_sync_from,
    normalize_sync_interval,
    resolve_sync_intervals,
)
from app.services.fxcm_market_sync.scheduling_policy import (
    infer_asset_category,
    is_allowed_today,
)
from app.services.fxcm_market_sync.state_sync import state_sync_handler
from app.services.fxcm_market_sync.types import (
    FXCMMarketSyncResult,
    PriorityForwardSyncError,
    PriorityForwardSyncJob,
    utc_now,
)
from app.services.fxcm_market_sync.utils import normalize_symbol
from app.services.market_master import market_master_service


logger = logging.getLogger(__name__)


class FXCMMarketSyncService:
    """FXCM 行情同步编排服务：协调元数据同步、状态调度与按需拉取。"""

    PROVIDER = PROVIDER
    SUPPORTED_INTERVALS = SUPPORTED_INTERVALS

    def __init__(self) -> None:
        """初始化同步服务运行状态和互斥锁。"""
        self._lock = asyncio.Lock()
        self._priority_run_lock = asyncio.Lock()
        self._last_metadata_sync_at: datetime | None = None
        self._priority_jobs: deque[PriorityForwardSyncJob] = deque()

    # --- 配置与状态查询 ---

    async def get_hot_symbols(self, db: AsyncSession) -> list[str]:
        return await instrument_sync_handler.get_hot_symbols(db)

    def _normalize_sync_interval(self, interval: str) -> str | None:
        return normalize_sync_interval(interval)

    @property
    def sync_intervals(self) -> list[str]:
        return resolve_sync_intervals(settings.fxcm_sync_intervals)

    def is_running(self) -> bool:
        return self._lock.locked()

    def get_last_metadata_sync_at(self) -> datetime | None:
        return self._last_metadata_sync_at

    # --- 调度入口 ---

    async def run_cycle(
        self,
        db: AsyncSession,
        *,
        reason: str,
        force_metadata: bool = False,
        force_due: bool = False,
    ) -> FXCMMarketSyncResult:
        """执行一次完整调度周期：必要时同步元数据，再处理到期状态任务。"""
        if self._lock.locked():
            return FXCMMarketSyncResult(
                reason=reason,
                skipped=True,
                errors=["sync task is already running"],
                finished_at=utc_now(),
            )

        async with self._lock:
            result = FXCMMarketSyncResult(reason=reason)
            try:
                await self._drain_priority_jobs(db, result)
                should_sync_metadata = force_metadata or self._metadata_sync_due()
                if should_sync_metadata:
                    synced_instruments = await self.sync_instruments(db)
                    bootstrap_states = await self.bootstrap_sync_states(db)
                    await db.commit()

                    result.metadata_synced = True
                    result.synced_instruments = synced_instruments
                    result.bootstrap_states = bootstrap_states
                    self._last_metadata_sync_at = utc_now()

                processed_states = await self.sync_due_states(
                    db,
                    result=result,
                    force_due=force_due,
                )
                result.processed_states += processed_states
                await self._drain_priority_jobs(db, result)
                await db.commit()
            except Exception as exc:
                await db.rollback()
                logger.exception("FXCM market sync cycle failed")
                result.errors.append(f"{type(exc).__name__}: {exc}")
            finally:
                result.finished_at = utc_now()

            return result

    async def run_manual(
        self,
        db: AsyncSession,
        *,
        mode: str,
        force_due: bool = True,
    ) -> FXCMMarketSyncResult:
        """执行手动触发同步，可按模式只跑 metadata、bars 或全部。"""
        normalized_mode = mode.strip().lower()
        if normalized_mode not in {"all", "metadata", "bars"}:
            normalized_mode = "all"

        if self._lock.locked():
            return FXCMMarketSyncResult(
                reason=f"manual:{normalized_mode}",
                skipped=True,
                errors=["sync task is already running"],
                finished_at=utc_now(),
            )

        async with self._lock:
            result = FXCMMarketSyncResult(reason=f"manual:{normalized_mode}")
            try:
                await self._drain_priority_jobs(db, result)
                if normalized_mode in {"all", "metadata"}:
                    result.synced_instruments = await self.sync_instruments(db)
                    result.bootstrap_states = await self.bootstrap_sync_states(db)
                    result.metadata_synced = True
                    self._last_metadata_sync_at = utc_now()
                    await db.commit()

                if normalized_mode in {"all", "bars"}:
                    result.processed_states += await self.sync_due_states(
                        db,
                        result=result,
                        force_due=force_due,
                    )
                    await self._drain_priority_jobs(db, result)
                    await db.commit()
            except Exception as exc:
                await db.rollback()
                logger.exception("FXCM market manual sync failed")
                result.errors.append(f"{type(exc).__name__}: {exc}")
            finally:
                result.finished_at = utc_now()

            return result

    async def fetch_on_demand_and_register(
        self,
        db: AsyncSession,
        symbol: str,
        interval: str,
        outputsize: int | None = None,
    ) -> bool:
        """本地缺数据时即时拉取，并把品种/周期注册到后台持续同步。"""
        normalized_interval = self._normalize_sync_interval(interval)
        if normalized_interval is None:
            logger.warning(
                "Unsupported FXCM sync interval requested",
                extra={"interval": interval, "supported": self.SUPPORTED_INTERVALS},
            )
            return False
        interval = normalized_interval

        instrument = await self._ensure_instrument(db, symbol)
        if instrument is None:
            return False

        target_bars = (
            outputsize
            if outputsize and outputsize > 0
            else settings.fxcm_sync_backfill_bars
        )
        state = await self._ensure_bar_state(
            db,
            instrument,
            interval,
            target_bars=target_bars,
        )

        try:
            await bar_sync_handler.sync_single_state(db, state, instrument)
            await db.commit()
            return True
        except Exception:
            await db.rollback()
            logger.exception(f"Fetch on demand failed for {symbol} {interval}")
            return False

    async def request_priority_forward_sync(
        self,
        db: AsyncSession,
        symbol: str,
        interval: str,
    ) -> dict[str, Any]:
        """立刻向前追赶指定品种/周期，不排队等待后台 metadata/bar 周期结束。"""
        normalized_symbol = (symbol or "").strip()
        if not normalized_symbol:
            raise PriorityForwardSyncError(400, "symbol is required")

        normalized_interval = self._normalize_sync_interval(interval)
        if normalized_interval is None:
            raise PriorityForwardSyncError(
                400,
                f"Unsupported interval: {interval}",
            )

        canonical_symbol = market_master_service._resolve_fxcm_symbol(normalized_symbol)
        job = self._enqueue_priority_forward_sync(canonical_symbol, normalized_interval)
        logger.info(
            "Priority forward sync starting immediately",
            extra={
                "symbol": canonical_symbol,
                "interval": normalized_interval,
                "scheduler_lock_held": self._lock.locked(),
            },
        )
        return await self._run_priority_job(db, job)

    def _enqueue_priority_forward_sync(
        self,
        symbol: str,
        interval: str,
    ) -> PriorityForwardSyncJob:
        """合并同一品种/周期的在途优先任务，避免重复抓取。"""
        for existing in self._priority_jobs:
            if (
                existing.symbol == symbol
                and existing.interval == interval
                and not existing.future.done()
            ):
                return existing

        job = PriorityForwardSyncJob(
            symbol=symbol,
            interval=interval,
            future=asyncio.get_running_loop().create_future(),
        )
        self._priority_jobs.append(job)
        return job

    async def _drain_priority_jobs(
        self,
        db: AsyncSession,
        result: FXCMMarketSyncResult,
    ) -> int:
        """立刻处理队列中的最高优先追赶任务，并跳过品类轮换限制。"""
        processed = 0
        while self._priority_jobs:
            job = self._priority_jobs.popleft()
            if job.future.done():
                continue
            try:
                payload = await self._run_priority_job(db, job)
                result.rows_upserted += int(payload.get("rows_upserted") or 0)
                result.succeeded_states += 1
                result.processed_states += 1
                processed += 1
            except PriorityForwardSyncError as exc:
                result.failed_states += 1
                result.errors.append(f"{job.symbol}/{job.interval}: {exc.message}")
            except Exception as exc:
                result.failed_states += 1
                result.errors.append(
                    f"{job.symbol}/{job.interval}: {type(exc).__name__}: {exc}"
                )
        return processed

    async def _run_priority_job(
        self,
        db: AsyncSession,
        job: PriorityForwardSyncJob,
    ) -> dict[str, Any]:
        """执行优先追赶任务；不占用后台调度锁，避免被 metadata 周期堵住。"""
        async with self._priority_run_lock:
            if job.future.done():
                return job.future.result()

            try:
                self._priority_jobs.remove(job)
            except ValueError:
                pass

            try:
                async with db.begin_nested():
                    payload = await self._execute_priority_forward_sync(db, job)
                await db.commit()
                if not job.future.done():
                    job.future.set_result(payload)
                return payload
            except Exception as exc:
                logger.exception(
                    "Priority forward sync failed",
                    extra={"symbol": job.symbol, "interval": job.interval},
                )
                if not job.future.done():
                    job.future.set_exception(exc)
                raise

    async def _execute_priority_forward_sync(
        self,
        db: AsyncSession,
        job: PriorityForwardSyncJob,
    ) -> dict[str, Any]:
        """执行单次向前追赶：从本地最新 K 线补到当前时间。"""
        instrument = await self._ensure_instrument(db, job.symbol)
        if instrument is None:
            raise PriorityForwardSyncError(
                404,
                f"Instrument not found: {job.symbol}",
            )

        state = await self._ensure_bar_state(db, instrument, job.interval)
        await db.flush()
        await bar_sync_handler._hydrate_state_from_storage(db, state)
        from_bar_time = state.latest_synced_bar_time
        inserted = await bar_sync_handler.sync_forward_only(db, state, instrument)
        return {
            "ok": True,
            "symbol": instrument.symbol,
            "provider_symbol": instrument.provider_symbol,
            "interval": job.interval,
            "rows_upserted": inserted,
            "from_bar_time": from_bar_time,
            "to_bar_time": state.latest_synced_bar_time,
            "backfill_skipped": True,
        }

    async def _ensure_instrument(
        self,
        db: AsyncSession,
        symbol: str,
    ) -> MarketInstrument | None:
        """按规范化符号查找品种；不存在时尝试从 sidecar 注册。"""
        canonical_symbol = market_master_service._resolve_fxcm_symbol(symbol)
        stmt = select(MarketInstrument).where(
            MarketInstrument.provider == self.PROVIDER,
            or_(
                MarketInstrument.normalized_symbol
                == normalize_symbol(canonical_symbol),
                MarketInstrument.normalized_provider_symbol
                == normalize_symbol(canonical_symbol),
            ),
        )
        instrument = (await db.execute(stmt)).scalars().first()
        if instrument is not None:
            return instrument

        instrument_payload = await instrument_sync_handler.build_instrument_payload(
            canonical_symbol, hot_symbols=None
        )
        if instrument_payload is None:
            return None
        instrument = await instrument_sync_handler.upsert_instrument(
            db, instrument_payload
        )
        await instrument_sync_handler.sync_aliases(
            db,
            instrument=instrument,
            aliases=instrument_payload.pop("aliases", []),
        )
        await db.flush()
        return instrument

    async def _ensure_bar_state(
        self,
        db: AsyncSession,
        instrument: MarketInstrument,
        interval: str,
        target_bars: int | None = None,
    ) -> MarketBarSyncState:
        """确保品种/周期同步状态存在；已存在时不降低目标历史条数。"""
        stmt_state = select(MarketBarSyncState).where(
            MarketBarSyncState.instrument_id == instrument.id,
            MarketBarSyncState.interval == interval,
            MarketBarSyncState.price_type == "mid",
        )
        state = (await db.execute(stmt_state)).scalars().first()
        resolved_target_bars = (
            target_bars
            if target_bars and target_bars > 0
            else settings.fxcm_sync_backfill_bars
        )
        if state is None:
            state = MarketBarSyncState(
                instrument_id=instrument.id,
                provider=self.PROVIDER,
                interval=interval,
                price_type="mid",
                enabled=True,
                priority=0,
                target_history_bars=resolved_target_bars,
                sync_mode="BACKFILL",
                next_sync_from=calculate_next_sync_from(
                    interval=interval,
                    now=utc_now(),
                    skip_weekends=False,
                ),
                backfill_completed=False,
                last_status="IDLE",
            )
            db.add(state)
            await db.flush()
            return state

        state.enabled = True
        if target_bars is not None and target_bars > (state.target_history_bars or 0):
            state.target_history_bars = target_bars
            state.backfill_completed = False
        return state

    # --- 子任务委托 ---

    async def sync_instruments(self, db: AsyncSession) -> int:
        async def preempt() -> None:
            dummy = FXCMMarketSyncResult(reason="priority-preempt")
            await self._drain_priority_jobs(db, dummy)

        return await instrument_sync_handler.sync_instruments(db, before_each=preempt)

    async def bootstrap_sync_states(self, db: AsyncSession) -> int:
        return await state_sync_handler.bootstrap_sync_states(
            db, sync_intervals=self.sync_intervals
        )

    async def sync_due_states(
        self,
        db: AsyncSession,
        *,
        result: FXCMMarketSyncResult,
        force_due: bool,
    ) -> int:
        return await state_sync_handler.sync_due_states(
            db,
            result=result,
            force_due=force_due,
            before_batch=self._drain_priority_jobs,
        )

    # --- 监控接口 ---

    async def get_status(self, db: AsyncSession) -> dict[str, Any]:
        """汇总当前同步系统状态与核心计数指标。"""
        now = utc_now()

        instrument_count = await db.scalar(
            select(func.count())
            .select_from(MarketInstrument)
            .where(MarketInstrument.provider == self.PROVIDER)
        )
        alias_count = await db.scalar(
            select(func.count()).select_from(MarketInstrumentAlias)
        )
        bar_count = await db.scalar(select(func.count()).select_from(MarketOHLCVBar))
        state_count = await db.scalar(
            select(func.count())
            .select_from(MarketBarSyncState)
            .where(MarketBarSyncState.provider == self.PROVIDER)
        )
        enabled_state_count = await db.scalar(
            select(func.count())
            .select_from(MarketBarSyncState)
            .where(
                MarketBarSyncState.provider == self.PROVIDER,
                MarketBarSyncState.enabled.is_(True),
            )
        )
        due_state_count = await db.scalar(
            select(func.count())
            .select_from(MarketBarSyncState)
            .where(
                MarketBarSyncState.provider == self.PROVIDER,
                MarketBarSyncState.enabled.is_(True),
                or_(
                    MarketBarSyncState.next_sync_from.is_(None),
                    MarketBarSyncState.next_sync_from <= now,
                ),
            )
        )
        failed_state_count = await db.scalar(
            select(func.count())
            .select_from(MarketBarSyncState)
            .where(
                MarketBarSyncState.provider == self.PROVIDER,
                MarketBarSyncState.last_status == "FAILED",
            )
        )

        hot_symbols = await self.get_hot_symbols(db)

        return {
            "scheduler_enabled": settings.fxcm_sync_enabled,
            "scheduler_running": False,
            "lock_held": self.is_running(),
            "hot_symbols": hot_symbols,
            "metadata_interval_hours": settings.fxcm_sync_metadata_interval_hours,
            "bar_intervals": self.sync_intervals,
            "instrument_count": int(instrument_count or 0),
            "alias_count": int(alias_count or 0),
            "bar_count": int(bar_count or 0),
            "state_count": int(state_count or 0),
            "enabled_state_count": int(enabled_state_count or 0),
            "due_state_count": int(due_state_count or 0),
            "failed_state_count": int(failed_state_count or 0),
            "last_metadata_sync_at": self._last_metadata_sync_at,
            "priority_queue_size": len(self._priority_jobs),
            "priority_jobs": [
                {
                    "symbol": job.symbol,
                    "interval": job.interval,
                    "requested_at": job.requested_at,
                }
                for job in self._priority_jobs
                if not job.future.done()
            ],
        }

    async def get_running_tasks(self, db: AsyncSession) -> list[dict[str, Any]]:
        """查询最近处于 RUNNING 状态的任务明细。"""
        stmt = (
            select(MarketBarSyncState, MarketInstrument)
            .join(
                MarketInstrument,
                MarketInstrument.id == MarketBarSyncState.instrument_id,
            )
            .where(MarketBarSyncState.last_status == "RUNNING")
            .order_by(MarketBarSyncState.last_attempt_at.desc())
            .limit(50)
        )
        rows = (await db.execute(stmt)).all()
        tasks = []
        for state, instrument in rows:
            tasks.append(
                {
                    "instrument_id": instrument.id,
                    "symbol": instrument.symbol,
                    "provider_symbol": instrument.provider_symbol,
                    "interval": state.interval,
                    "price_type": state.price_type,
                    "sync_mode": state.sync_mode,
                    "last_attempt_at": state.last_attempt_at,
                    "target_history_bars": state.target_history_bars,
                    "backfill_completed": state.backfill_completed,
                    "earliest_synced_bar_time": state.earliest_synced_bar_time,
                    "latest_synced_bar_time": state.latest_synced_bar_time,
                }
            )
        return tasks

    async def get_sync_states(self, db: AsyncSession) -> dict[str, Any]:
        """返回全部同步状态行及今日轮换策略上下文。"""
        now = utc_now()
        weekday = now.weekday()
        rotation_schedule = [
            {"weekday": 0, "category": "forex", "label": "外汇"},
            {"weekday": 1, "category": "index", "label": "指数"},
            {"weekday": 2, "category": "commodity", "label": "大宗商品"},
            {"weekday": 3, "category": "index", "label": "指数"},
            {"weekday": 4, "category": "crypto", "label": "加密货币"},
            {"weekday": 5, "category": "forex", "label": "外汇"},
            {"weekday": 6, "category": "commodity", "label": "大宗商品"},
        ]
        today_rotation = next(
            (item for item in rotation_schedule if item["weekday"] == weekday),
            None,
        )

        stmt = (
            select(MarketBarSyncState, MarketInstrument)
            .join(
                MarketInstrument,
                MarketInstrument.id == MarketBarSyncState.instrument_id,
            )
            .where(MarketBarSyncState.provider == self.PROVIDER)
            .order_by(
                MarketInstrument.symbol.asc(),
                MarketBarSyncState.priority.asc(),
                MarketBarSyncState.interval.asc(),
            )
        )
        rows = (await db.execute(stmt)).all()

        items: list[dict[str, Any]] = []
        for state, instrument in rows:
            category = infer_asset_category(instrument)
            allowed_today = is_allowed_today(instrument, now)
            next_sync_from = state.next_sync_from
            if next_sync_from is None:
                seconds_until_next_sync = 0
                is_due = True
            elif next_sync_from <= now:
                seconds_until_next_sync = 0
                is_due = True
            else:
                seconds_until_next_sync = int((next_sync_from - now).total_seconds())
                is_due = False

            items.append(
                {
                    "id": state.id,
                    "instrument_id": instrument.id,
                    "symbol": instrument.symbol,
                    "provider_symbol": instrument.provider_symbol,
                    "asset_type": instrument.asset_type,
                    "asset_category": category,
                    "allowed_today": allowed_today,
                    "interval": state.interval,
                    "price_type": state.price_type,
                    "enabled": state.enabled,
                    "priority": state.priority,
                    "target_history_bars": state.target_history_bars,
                    "sync_mode": state.sync_mode,
                    "earliest_synced_bar_time": state.earliest_synced_bar_time,
                    "latest_synced_bar_time": state.latest_synced_bar_time,
                    "last_requested_start_at": state.last_requested_start_at,
                    "last_requested_end_at": state.last_requested_end_at,
                    "next_sync_from": next_sync_from,
                    "seconds_until_next_sync": seconds_until_next_sync,
                    "is_due": is_due,
                    "backfill_completed": state.backfill_completed,
                    "last_status": state.last_status,
                    "last_attempt_at": state.last_attempt_at,
                    "last_success_at": state.last_success_at,
                    "retry_count": state.retry_count,
                    "last_error": state.last_error,
                    "meta": state.meta,
                    "created_at": state.created_at,
                    "updated_at": state.updated_at,
                }
            )

        return {
            "rotation": {
                "weekday": weekday,
                "today": today_rotation,
                "schedule": rotation_schedule,
                "server_time": now,
            },
            "items": items,
        }

    def _metadata_sync_due(self) -> bool:
        if self._last_metadata_sync_at is None:
            return True
        due_at = self._last_metadata_sync_at + timedelta(
            hours=settings.fxcm_sync_metadata_interval_hours
        )
        return utc_now() >= due_at


fxcm_market_sync_service = FXCMMarketSyncService()
