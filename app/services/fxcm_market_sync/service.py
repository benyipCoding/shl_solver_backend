import asyncio
import logging
import math
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.market_data import (
    MarketBarSyncState,
    MarketInstrument,
    MarketInstrumentAlias,
    MarketOHLCVBar,
)
from app.services.fxcm_market_sync.bar_sync import bar_sync_handler
from app.services.fxcm_market_sync.constants import (
    BACKFILL_EARLIEST_DATE,
    PRIORITY_REPAIR_MAX_ROUNDS,
    PROVIDER,
    SUPPORTED_INTERVALS,
)
from app.services.fxcm_market_sync.history_cutoff import (
    get_history_cutoff,
    is_history_complete,
    save_history_cutoff,
)
from app.services.fxcm_market_sync.instrument_sync import instrument_sync_handler
from app.services.fxcm_market_sync.intervals import (
    interval_delta,
    normalize_sync_interval,
    repair_outputsize,
    resolve_sync_intervals,
)
from app.services.fxcm_market_sync.scheduling_policy import infer_asset_category
from app.services.fxcm_market_sync.state_sync import state_sync_handler
from app.services.fxcm_market_sync.sync_queue import sync_queue_handler
from app.services.fxcm_market_sync.types import (
    FXCMMarketSyncResult,
    PriorityForwardSyncError,
    PrioritySyncJob,
    utc_now,
)
from app.services.fxcm_market_sync.utils import (
    normalize_symbol,
    parse_request_payload_datetime,
)
from app.services.fxcm_sidecar import FXCMSidecarError
from app.models.market_repair_outbox import MarketRepairOutbox
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
        self._priority_jobs: deque[PrioritySyncJob] = deque()

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
        """本地缺数据时即时拉取并登记同步状态；品种在采集队列中才会被后台持续同步。"""
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
        job = self._enqueue_priority_job(
            canonical_symbol,
            normalized_interval,
            kind="forward",
        )
        logger.info(
            "Priority forward sync starting immediately",
            extra={
                "symbol": canonical_symbol,
                "interval": normalized_interval,
                "scheduler_lock_held": self._lock.locked(),
            },
        )
        return await self._run_priority_job(db, job)

    async def request_priority_range_repair(
        self,
        db: AsyncSession,
        symbol: str,
        interval: str,
        start_date: str,
        end_date: str,
    ) -> dict[str, Any]:
        """立刻按时间段重采指定品种/周期，差异以 FXCM 新数据为准，并插到最高优先级。"""
        normalized_symbol = (symbol or "").strip()
        if not normalized_symbol:
            raise PriorityForwardSyncError(400, "symbol is required")

        normalized_interval = self._normalize_sync_interval(interval)
        if normalized_interval is None:
            raise PriorityForwardSyncError(
                400,
                f"Unsupported interval: {interval}",
            )

        start_at = parse_request_payload_datetime(start_date)
        end_at = parse_request_payload_datetime(end_date)
        if start_at is None or end_at is None:
            raise PriorityForwardSyncError(
                400,
                "start_date and end_date are required ISO datetimes",
            )
        if end_at < start_at:
            start_at, end_at = end_at, start_at
        if start_at == end_at:
            end_at = start_at + interval_delta(normalized_interval)

        estimated_windows = self._estimate_repair_windows(
            normalized_interval, start_at, end_at
        )
        if estimated_windows > PRIORITY_REPAIR_MAX_ROUNDS:
            raise PriorityForwardSyncError(
                400,
                "框选时间范围过大，请缩小后再修复",
            )

        canonical_symbol = market_master_service._resolve_fxcm_symbol(normalized_symbol)
        job = self._enqueue_priority_job(
            canonical_symbol,
            normalized_interval,
            kind="repair",
            start_at=start_at,
            end_at=end_at,
        )
        logger.info(
            "Priority range repair starting immediately",
            extra={
                "symbol": canonical_symbol,
                "interval": normalized_interval,
                "start_at": start_at.isoformat(),
                "end_at": end_at.isoformat(),
                "estimated_windows": estimated_windows,
                "scheduler_lock_held": self._lock.locked(),
            },
        )
        return await self._run_priority_job(db, job)

    def _estimate_repair_windows(
        self,
        interval: str,
        start_at: datetime,
        end_at: datetime,
    ) -> int:
        window = interval_delta(interval) * max(1, repair_outputsize(interval))
        window_seconds = max(window.total_seconds(), 1)
        span_seconds = max((end_at - start_at).total_seconds(), 0)
        return max(1, math.ceil(span_seconds / window_seconds))

    def _enqueue_priority_job(
        self,
        symbol: str,
        interval: str,
        *,
        kind: str = "forward",
        start_at: datetime | None = None,
        end_at: datetime | None = None,
    ) -> PrioritySyncJob:
        """合并同一任务的在途优先请求；修复任务插入队列最前。"""
        for existing in self._priority_jobs:
            if existing.future.done():
                continue
            if existing.kind != kind:
                continue
            if existing.symbol != symbol or existing.interval != interval:
                continue
            if kind == "repair" and (
                existing.start_at != start_at or existing.end_at != end_at
            ):
                continue
            return existing

        job = PrioritySyncJob(
            symbol=symbol,
            interval=interval,
            kind=kind,
            start_at=start_at,
            end_at=end_at,
            future=asyncio.get_running_loop().create_future(),
        )
        if kind == "repair":
            self._priority_jobs.appendleft(job)
        else:
            self._priority_jobs.append(job)
        return job

    def _enqueue_priority_forward_sync(
        self,
        symbol: str,
        interval: str,
    ) -> PrioritySyncJob:
        return self._enqueue_priority_job(symbol, interval, kind="forward")

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
        job: PrioritySyncJob,
    ) -> dict[str, Any]:
        """执行优先任务；不占用后台调度锁，避免被 metadata 周期堵住。"""
        async with self._priority_run_lock:
            if job.future.done():
                return job.future.result()

            try:
                self._priority_jobs.remove(job)
            except ValueError:
                pass

            try:
                async with db.begin_nested():
                    payload = await self._execute_priority_job(db, job)
                await db.commit()
                if not job.future.done():
                    job.future.set_result(payload)
                return payload
            except FXCMSidecarError as exc:
                wrapped = PriorityForwardSyncError(exc.status_code, exc.message)
                logger.exception(
                    "Priority sync job failed via sidecar",
                    extra={
                        "symbol": job.symbol,
                        "interval": job.interval,
                        "kind": job.kind,
                    },
                )
                if not job.future.done():
                    job.future.set_exception(wrapped)
                raise wrapped from exc
            except Exception as exc:
                logger.exception(
                    "Priority sync job failed",
                    extra={
                        "symbol": job.symbol,
                        "interval": job.interval,
                        "kind": job.kind,
                    },
                )
                if not job.future.done():
                    job.future.set_exception(exc)
                raise

    async def _execute_priority_job(
        self,
        db: AsyncSession,
        job: PrioritySyncJob,
    ) -> dict[str, Any]:
        if job.kind == "repair":
            return await self._execute_priority_range_repair(db, job)
        return await self._execute_priority_forward_sync(db, job)

    async def _execute_priority_forward_sync(
        self,
        db: AsyncSession,
        job: PrioritySyncJob,
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

    async def _execute_priority_range_repair(
        self,
        db: AsyncSession,
        job: PrioritySyncJob,
    ) -> dict[str, Any]:
        """按框选时间段重采 FXCM，并以新数据覆盖差异。"""
        if job.start_at is None or job.end_at is None:
            raise PriorityForwardSyncError(400, "repair range is required")

        instrument = await self._ensure_instrument(db, job.symbol)
        if instrument is None:
            raise PriorityForwardSyncError(
                404,
                f"Instrument not found: {job.symbol}",
            )

        state = await self._ensure_bar_state(db, instrument, job.interval)
        await db.flush()
        result = await bar_sync_handler.sync_range_repair(
            db,
            state,
            instrument,
            start_at=job.start_at,
            end_at=job.end_at,
        )
        if not result.get("ok"):
            raise PriorityForwardSyncError(
                502,
                "福汇未返回该时间段的 K 线，已取消覆盖以免误删本地数据",
            )
        # 与 K 线修复共用 _run_priority_job 的事务，避免修复成功但漏记同步任务。
        # 每次成功修复都追加，即使数据未变化，也允许修复生产端的历史差异。
        outbox = MarketRepairOutbox(
            instrument_id=instrument.id,
            provider=instrument.provider,
            symbol=instrument.symbol,
            interval=state.interval,
            price_type=state.price_type,
            start_at=job.start_at,
            end_at=job.end_at,
        )
        db.add(outbox)
        await db.flush()
        return {
            "ok": True,
            "kind": "repair",
            "replica_job_id": outbox.id,
            "symbol": instrument.symbol,
            "provider_symbol": instrument.provider_symbol,
            "interval": job.interval,
            "start_at": job.start_at,
            "end_at": job.end_at,
            **result,
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
                next_sync_from=utc_now(),
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

        return await instrument_sync_handler.sync_instruments(
            db,
            symbols=await sync_queue_handler.get_queued_symbols(db),
            before_each=preempt,
        )

    async def bootstrap_sync_states(self, db: AsyncSession) -> int:
        created = await state_sync_handler.bootstrap_sync_states(
            db, sync_intervals=self.sync_intervals
        )
        await state_sync_handler.reconcile_completion(
            db, cutoff=await get_history_cutoff(db)
        )
        return created

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
            cutoff=await get_history_cutoff(db),
            before_batch=self._drain_priority_jobs,
        )

    # --- 历史数据截止时间 ---

    async def set_history_cutoff(
        self,
        db: AsyncSession,
        cutoff: datetime,
    ) -> dict[str, Any]:
        """修改历史数据截止时间，并据此重新判定所有状态是否采集完成。"""
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=timezone.utc)
        if cutoff <= BACKFILL_EARLIEST_DATE:
            raise ValueError("截止时间必须晚于历史回补下限 1994-01-01")
        if cutoff > utc_now():
            raise ValueError("截止时间不能晚于当前时间，最新数据请使用实时模式或同步到最新")

        await save_history_cutoff(db, cutoff)
        counts = await state_sync_handler.reconcile_completion(db, cutoff=cutoff)
        await db.commit()
        return {"history_cutoff_at": cutoff, **counts}

    # --- 采集队列 ---

    async def get_sync_queue(self, db: AsyncSession) -> list[dict[str, Any]]:
        """返回历史数据采集队列中的品种。"""
        return [
            {
                "instrument_id": instrument.id,
                "symbol": instrument.symbol,
                "provider_symbol": instrument.provider_symbol,
                "name": instrument.name,
                "asset_type": instrument.asset_type,
                "asset_category": infer_asset_category(instrument),
                "is_active": instrument.is_active,
                "added_at": item.created_at,
            }
            for item, instrument in await sync_queue_handler.list_queued_instruments(db)
        ]

    async def list_sync_candidates(
        self,
        db: AsyncSession,
        *,
        keyword: str | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        return await sync_queue_handler.list_candidates(
            db, keyword=keyword, limit=limit
        )

    async def add_to_sync_queue(
        self,
        db: AsyncSession,
        *,
        symbols: list[str],
        instrument_ids: list[int],
    ) -> dict[str, Any]:
        """把品种加入采集队列并立即为其创建各周期同步状态。

        instrument_ids 指向本地已有品种；symbols 用于本地尚未落库、需经 sidecar 注册的品种。
        """
        instruments: list[MarketInstrument] = []
        not_found: list[str] = []

        if instrument_ids:
            found = {
                instrument.id: instrument
                for instrument in (
                    await db.execute(
                        select(MarketInstrument).where(
                            MarketInstrument.provider == self.PROVIDER,
                            MarketInstrument.id.in_(instrument_ids),
                        )
                    )
                )
                .scalars()
                .all()
            }
            for instrument_id in instrument_ids:
                instrument = found.get(instrument_id)
                if instrument is None:
                    not_found.append(str(instrument_id))
                else:
                    instruments.append(instrument)

        for symbol in symbols:
            normalized_symbol = (symbol or "").strip()
            if not normalized_symbol:
                continue
            instrument = await self._ensure_instrument(db, normalized_symbol)
            if instrument is None:
                not_found.append(normalized_symbol)
            else:
                instruments.append(instrument)

        added: list[str] = []
        already_queued: list[str] = []
        for instrument in instruments:
            if await sync_queue_handler.add_instrument(db, instrument):
                await state_sync_handler.ensure_instrument_states(
                    db, instrument, sync_intervals=self.sync_intervals
                )
                added.append(instrument.symbol)
            else:
                already_queued.append(instrument.symbol)
        await db.commit()

        return {
            "added": added,
            "already_queued": already_queued,
            "not_found": not_found,
        }

    async def remove_from_sync_queue(
        self,
        db: AsyncSession,
        *,
        instrument_ids: list[int],
    ) -> dict[str, Any]:
        """把品种移出采集队列；已采集的 K 线和同步状态保留，重新加入可续采。"""
        removed = await sync_queue_handler.remove_instruments(db, instrument_ids)
        await db.commit()
        return {"removed": removed}

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
        # K 线表数千万行，精确 count(*) 需全表扫描，监控页每 3 秒轮询会堆积；用统计估算值。
        bar_count = await db.scalar(
            text(
                "SELECT reltuples::bigint FROM pg_class "
                "WHERE oid = to_regclass(:table_name)"
            ),
            {"table_name": MarketOHLCVBar.__tablename__},
        )
        if bar_count is None or bar_count < 0:
            bar_count = await db.scalar(
                select(func.count()).select_from(MarketOHLCVBar)
            )
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
        queued_symbols = await sync_queue_handler.get_queued_symbols(db)
        history_cutoff = await get_history_cutoff(db)

        return {
            "scheduler_enabled": settings.fxcm_sync_enabled,
            "scheduler_running": False,
            "lock_held": self.is_running(),
            "hot_symbols": hot_symbols,
            "queued_symbols": queued_symbols,
            "history_cutoff_at": history_cutoff,
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
                    "kind": job.kind,
                    "start_at": job.start_at,
                    "end_at": job.end_at,
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
        """返回全部同步状态行，标记各行是否在采集队列中、是否已采集到截止时间。"""
        now = utc_now()
        queued_ids = await sync_queue_handler.get_queued_instrument_ids(db)
        cutoff = await get_history_cutoff(db)

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
            next_sync_from = state.next_sync_from
            if next_sync_from is None or next_sync_from <= now:
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
                    "asset_category": infer_asset_category(instrument),
                    "in_queue": instrument.id in queued_ids,
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
                    "history_complete": is_history_complete(state, cutoff),
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

        return {"server_time": now, "history_cutoff_at": cutoff, "items": items}

    def _metadata_sync_due(self) -> bool:
        if self._last_metadata_sync_at is None:
            return True
        due_at = self._last_metadata_sync_at + timedelta(
            hours=settings.fxcm_sync_metadata_interval_hours
        )
        return utc_now() >= due_at


fxcm_market_sync_service = FXCMMarketSyncService()
