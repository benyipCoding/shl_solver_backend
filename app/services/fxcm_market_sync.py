import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import and_, func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.clients import db as db_client
from app.core.config import settings
from app.models.market_data import (
    MarketBarSyncState,
    MarketInstrument,
    MarketInstrumentAlias,
    MarketOHLCVBar,
)
from app.services.fxcm_sidecar import FXCMSidecarError, fxcm_sidecar_service
from app.services.market_master import market_master_service


logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    """返回当前 UTC 时间，统一全模块的时间基准。"""
    return datetime.now(tz=timezone.utc)


@dataclass
class FXCMMarketSyncResult:
    reason: str
    skipped: bool = False
    metadata_synced: bool = False
    synced_instruments: int = 0
    bootstrap_states: int = 0
    processed_states: int = 0
    succeeded_states: int = 0
    failed_states: int = 0
    rows_upserted: int = 0
    errors: list[str] = field(default_factory=list)
    finished_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """将同步结果对象转换为可序列化字典。"""
        return {
            "reason": self.reason,
            "skipped": self.skipped,
            "metadata_synced": self.metadata_synced,
            "synced_instruments": self.synced_instruments,
            "bootstrap_states": self.bootstrap_states,
            "processed_states": self.processed_states,
            "succeeded_states": self.succeeded_states,
            "failed_states": self.failed_states,
            "rows_upserted": self.rows_upserted,
            "errors": list(self.errors),
            "finished_at": self.finished_at,
        }


class FXCMMarketSyncService:
    PROVIDER = "FXCM"
    SUPPORTED_INTERVALS = (
        "1min",
        "5min",
        "15min",
        "30min",
        "1h",
        "2h",
        "4h",
        "1day",
        "1week",
    )
    ALWAYS_OPEN_ASSET_TYPES = {"digital currency"}

    def __init__(self) -> None:
        """初始化同步服务运行状态和互斥锁。"""
        self._lock = asyncio.Lock()
        self._last_metadata_sync_at: datetime | None = None

    async def get_hot_symbols(self, db: AsyncSession) -> list[str]:
        """读取数据库热池品种，并去重后返回规范化符号列表。"""
        stmt = select(MarketInstrument.symbol).where(
            MarketInstrument.provider == self.PROVIDER,
            MarketInstrument.is_active.is_(True),
            MarketInstrument.is_hot.is_(True),
        )
        symbols = (await db.execute(stmt)).scalars().all()

        canonical_symbols: list[str] = []
        seen: set[str] = set()
        for symbol in symbols:
            canonical_symbol = market_master_service._resolve_fxcm_symbol(symbol)
            if canonical_symbol in seen:
                continue
            seen.add(canonical_symbol)
            canonical_symbols.append(canonical_symbol)
        return canonical_symbols

    @property
    def sync_intervals(self) -> list[str]:
        """解析并标准化配置中的同步周期，过滤不支持项。"""
        raw_intervals = [
            item.strip()
            for item in settings.fxcm_sync_intervals.split(",")
            if item.strip()
        ]
        normalized_intervals: list[str] = []
        seen: set[str] = set()
        for interval in raw_intervals or list(self.SUPPORTED_INTERVALS):
            normalized_interval = market_master_service._resolve_interval_name(interval)
            if normalized_interval not in self.SUPPORTED_INTERVALS:
                continue
            if normalized_interval in seen:
                continue
            seen.add(normalized_interval)
            normalized_intervals.append(normalized_interval)
        return normalized_intervals or list(self.SUPPORTED_INTERVALS)

    def is_running(self) -> bool:
        """判断当前是否已有同步任务在运行。"""
        return self._lock.locked()

    def get_last_metadata_sync_at(self) -> datetime | None:
        """返回最近一次元数据同步完成时间。"""
        return self._last_metadata_sync_at

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
                finished_at=_utc_now(),
            )

        async with self._lock:
            result = FXCMMarketSyncResult(reason=reason)
            try:
                should_sync_metadata = force_metadata or self._metadata_sync_due()
                if should_sync_metadata:
                    synced_instruments = await self.sync_instruments(db)
                    bootstrap_states = await self.bootstrap_sync_states(db)
                    await db.commit()

                    result.metadata_synced = True
                    result.synced_instruments = synced_instruments
                    result.bootstrap_states = bootstrap_states
                    self._last_metadata_sync_at = _utc_now()

                processed_states = await self.sync_due_states(
                    db,
                    result=result,
                    force_due=force_due,
                )
                result.processed_states = processed_states
                await db.commit()
            except Exception as exc:
                await db.rollback()
                logger.exception("FXCM market sync cycle failed")
                result.errors.append(f"{type(exc).__name__}: {exc}")
            finally:
                result.finished_at = _utc_now()

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
                finished_at=_utc_now(),
            )

        async with self._lock:
            result = FXCMMarketSyncResult(reason=f"manual:{normalized_mode}")
            try:
                if normalized_mode in {"all", "metadata"}:
                    result.synced_instruments = await self.sync_instruments(db)
                    result.bootstrap_states = await self.bootstrap_sync_states(db)
                    result.metadata_synced = True
                    self._last_metadata_sync_at = _utc_now()
                    await db.commit()

                if normalized_mode in {"all", "bars"}:
                    result.processed_states = await self.sync_due_states(
                        db,
                        result=result,
                        force_due=force_due,
                    )
                    await db.commit()
            except Exception as exc:
                await db.rollback()
                logger.exception("FXCM market manual sync failed")
                result.errors.append(f"{type(exc).__name__}: {exc}")
            finally:
                result.finished_at = _utc_now()

            return result

    async def fetch_on_demand_and_register(
        self,
        db: AsyncSession,
        symbol: str,
        interval: str,
        outputsize: int | None = None,
    ) -> bool:
        """本地缺数据时即时拉取，并把品种/周期注册到后台持续同步。"""
        canonical_symbol = market_master_service._resolve_fxcm_symbol(symbol)

        stmt = select(MarketInstrument).where(
            MarketInstrument.provider == self.PROVIDER,
            or_(
                MarketInstrument.normalized_symbol
                == self._normalize_symbol(canonical_symbol),
                MarketInstrument.normalized_provider_symbol
                == self._normalize_symbol(canonical_symbol),
            ),
        )
        instrument = (await db.execute(stmt)).scalars().first()

        if instrument is None:
            instrument_payload = await self._build_instrument_payload(
                canonical_symbol, hot_symbols=None
            )
            if instrument_payload is None:
                return False
            instrument = await self._upsert_instrument(db, instrument_payload)
            await self._sync_aliases(
                db, instrument=instrument, aliases=instrument_payload.pop("aliases", [])
            )
            await db.flush()

        stmt_state = select(MarketBarSyncState).where(
            MarketBarSyncState.instrument_id == instrument.id,
            MarketBarSyncState.interval == interval,
            MarketBarSyncState.price_type == "mid",
        )
        state = (await db.execute(stmt_state)).scalars().first()

        target_bars = (
            outputsize
            if outputsize and outputsize > 0
            else settings.fxcm_sync_backfill_bars
        )
        if state is None:
            state = MarketBarSyncState(
                instrument_id=instrument.id,
                provider=self.PROVIDER,
                interval=interval,
                price_type="mid",
                enabled=True,
                priority=self._state_priority(interval),
                target_history_bars=target_bars,
                sync_mode="BACKFILL",
                next_sync_from=self._calculate_next_sync_from(
                    interval=interval,
                    now=_utc_now(),
                    skip_weekends=False,
                ),
                backfill_completed=False,
                last_status="IDLE",
            )
            db.add(state)
            await db.flush()
        else:
            if target_bars > (state.target_history_bars or 0):
                state.target_history_bars = target_bars
                state.backfill_completed = False

        try:
            await self._sync_single_state(db, state, instrument)
            await db.commit()
            return True
        except Exception as exc:
            await db.rollback()
            logger.exception(f"Fetch on demand failed for {symbol} {interval}")
            return False

    async def sync_instruments(self, db: AsyncSession) -> int:
        """刷新热池品种元数据并同步别名映射。"""
        synced_count = 0
        hot_symbols = await self.get_hot_symbols(db)
        for symbol in hot_symbols:
            instrument_payload = await self._build_instrument_payload(
                symbol, hot_symbols=hot_symbols
            )
            if instrument_payload is None:
                continue

            instrument = await self._upsert_instrument(db, instrument_payload)
            await self._sync_aliases(
                db,
                instrument=instrument,
                aliases=instrument_payload.pop("aliases", []),
            )
            synced_count += 1
        return synced_count

    async def bootstrap_sync_states(self, db: AsyncSession) -> int:
        """为热池品种按周期创建或修复同步状态任务。"""
        # 先禁用所有不在当前支持列表中的旧周期状态，避免它们混入调度队列
        disable_stmt = select(MarketBarSyncState).where(
            MarketBarSyncState.provider == self.PROVIDER,
            MarketBarSyncState.enabled.is_(True),
            MarketBarSyncState.interval.not_in(self.sync_intervals),
        )
        stale_states = (await db.execute(disable_stmt)).scalars().all()
        for stale in stale_states:
            stale.enabled = False

        stmt = select(MarketInstrument).where(
            MarketInstrument.provider == self.PROVIDER,
            MarketInstrument.is_active.is_(True),
            MarketInstrument.is_hot.is_(True),
        )
        instruments = (await db.execute(stmt)).scalars().all()

        created_count = 0
        for instrument in instruments:
            for interval in self.sync_intervals:
                exists_stmt = select(MarketBarSyncState).where(
                    MarketBarSyncState.instrument_id == instrument.id,
                    MarketBarSyncState.interval == interval,
                    MarketBarSyncState.price_type == "mid",
                )
                existing = (await db.execute(exists_stmt)).scalars().first()
                if existing is not None:
                    existing.enabled = True
                    existing.target_history_bars = settings.fxcm_sync_backfill_bars
                    existing.priority = self._state_priority(interval)
                    continue

                db.add(
                    MarketBarSyncState(
                        instrument_id=instrument.id,
                        provider=self.PROVIDER,
                        interval=interval,
                        price_type="mid",
                        enabled=True,
                        priority=self._state_priority(interval),
                        target_history_bars=settings.fxcm_sync_backfill_bars,
                        sync_mode="BACKFILL",
                        next_sync_from=self._calculate_next_sync_from(
                            interval=interval,
                            now=_utc_now(),
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
        now = _utc_now()
        bar_count_subquery = (
            select(
                MarketOHLCVBar.instrument_id.label("instrument_id"),
                func.count().label("bar_count"),
            )
            .where(MarketOHLCVBar.provider == self.PROVIDER)
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
                MarketBarSyncState.provider == self.PROVIDER,
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
        # 每个品种每轮最多处理 1 个 interval，避免 BTC/ETH 等多周期品种独占全部槽位
        seen_instrument_ids: set[int] = set()
        deduped_rows = []
        for row in rows:
            iid = row[1].id  # MarketInstrument.id
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

            # 1. 每日硬性轮换策略：只允许特定品类在今天运行
            if not self._is_allowed_today(instrument, now):
                # 如果今天不轮到它，直接把下次调度时间推迟到明天凌晨
                tomorrow = (now + timedelta(days=1)).replace(
                    hour=0, minute=5, second=0, microsecond=0
                )
                state.next_sync_from = tomorrow
                state.last_status = "SKIPPED"
                state.last_error = "Skipped by daily category rotation policy"
                state.last_attempt_at = now
                await db.commit()
                continue

            # 2. 周末过滤策略：只有当历史回补已完成，进入增量监听模式时，传统品类才跳过周末
            if state.backfill_completed:
                weekend_resume_at = self._weekend_resume_at(instrument, now)
                if weekend_resume_at is not None:
                    state.next_sync_from = weekend_resume_at
                    state.last_status = "SKIPPED"
                    state.last_error = None
                    state.last_attempt_at = now
                    await db.commit()
                    continue

            try:
                inserted = await self._sync_single_state(db, state, instrument)
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
                    persisted_state.last_attempt_at = _utc_now()
                    persisted_state.retry_count = (persisted_state.retry_count or 0) + 1
                    persisted_state.next_sync_from = _utc_now() + timedelta(
                        minutes=min(30, max(5, persisted_state.retry_count * 5))
                    )
                    await db.commit()
                result.failed_states += 1
                result.errors.append(
                    f"{instrument_symbol}/{interval}: {type(exc).__name__}: {exc}"
                )
        return processed

    async def get_status(self, db: AsyncSession) -> dict[str, Any]:
        """汇总当前同步系统状态与核心计数指标。"""
        now = _utc_now()

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

    async def _build_instrument_payload(
        self, symbol: str, hot_symbols: list[str] | None = None
    ) -> dict[str, Any] | None:
        """聚合 sidecar 与本地画像，构建可落库的品种元数据载荷。"""
        canonical_symbol = market_master_service._resolve_fxcm_symbol(symbol)
        profile = market_master_service._find_symbol_profile(canonical_symbol)

        search_item: Mapping[str, Any] | None = None
        quote_payload: Mapping[str, Any] | None = None

        try:
            search_payload = await fxcm_sidecar_service.search_symbols(
                keyword=canonical_symbol,
                outputsize=10,
            )
            search_item = self._pick_best_search_item(
                search_payload.get("items"),
                canonical_symbol=canonical_symbol,
                profile=profile,
            )
        except FXCMSidecarError:
            logger.warning("FXCM symbol search failed", exc_info=True)

        try:
            quote_payload = await fxcm_sidecar_service.get_quote(
                symbol=canonical_symbol,
                interval="1day",
                price_type="mid",
            )
        except FXCMSidecarError:
            logger.warning(
                "FXCM quote fetch failed during metadata sync", exc_info=True
            )

        provider_symbol = self._coalesce_str(
            search_item.get("provider_symbol") if search_item else None,
            quote_payload.get("provider_symbol") if quote_payload else None,
            profile.get("provider_symbol") if profile else None,
            canonical_symbol,
        )
        symbol_value = self._coalesce_str(
            search_item.get("symbol") if search_item else None,
            quote_payload.get("symbol") if quote_payload else None,
            profile.get("symbol") if profile else None,
            canonical_symbol,
        )

        if provider_symbol is None or symbol_value is None:
            return None

        aliases = self._build_aliases(
            canonical_symbol=symbol_value,
            provider_symbol=provider_symbol,
            profile=profile,
            search_item=search_item,
        )

        market_value = self._coalesce_str(
            search_item.get("market") if search_item else None,
            profile.get("market") if profile else None,
        )
        asset_type = self._coalesce_str(
            search_item.get("asset_type") if search_item else None,
            profile.get("asset_type") if profile else None,
        )

        return {
            "provider": self.PROVIDER,
            "symbol": symbol_value,
            "normalized_symbol": self._normalize_symbol(symbol_value),
            "provider_symbol": provider_symbol,
            "normalized_provider_symbol": self._normalize_symbol(provider_symbol),
            "name": self._coalesce_str(
                search_item.get("name") if search_item else None,
                quote_payload.get("name") if quote_payload else None,
                profile.get("name") if profile else None,
                symbol_value,
            ),
            "display_label": self._coalesce_str(
                search_item.get("label") if search_item else None,
                profile.get("name") if profile else None,
            ),
            "exchange": self._coalesce_str(
                search_item.get("exchange") if search_item else None,
                quote_payload.get("exchange") if quote_payload else None,
                profile.get("exchange") if profile else None,
            ),
            "mic_code": self._coalesce_str(
                search_item.get("mic_code") if search_item else None,
                quote_payload.get("mic_code") if quote_payload else None,
            ),
            "market": market_value,
            "asset_type": asset_type,
            "country": self._coalesce_str(
                search_item.get("country") if search_item else None,
            ),
            "currency": self._coalesce_str(
                search_item.get("currency") if search_item else None,
                quote_payload.get("currency") if quote_payload else None,
                profile.get("currency") if profile else None,
            ),
            "exchange_timezone": self._coalesce_str(
                search_item.get("timezone") if search_item else None,
                profile.get("timezone") if profile else None,
            ),
            "provider_plan": self._coalesce_str(
                search_item.get("provider_plan") if search_item else None,
            ),
            "sort_weight": self._sort_weight(symbol_value, hot_symbols=hot_symbols),
            "is_active": True,
            "is_searchable": True,
            "is_hot": True,
            "supports_history": True,
            "supports_quote": True,
            "source_payload": {
                "search_item": (
                    dict(search_item) if isinstance(search_item, Mapping) else None
                ),
                "quote": (
                    dict(quote_payload) if isinstance(quote_payload, Mapping) else None
                ),
                "profile": dict(profile) if isinstance(profile, Mapping) else None,
            },
            "metadata_synced_at": _utc_now(),
            "last_seen_at": _utc_now(),
            "aliases": aliases,
        }

    async def _upsert_instrument(
        self,
        db: AsyncSession,
        payload: dict[str, Any],
    ) -> MarketInstrument:
        """按规范化符号插入或更新品种记录。"""
        aliases = payload.pop("aliases", [])
        stmt = select(MarketInstrument).where(
            and_(
                MarketInstrument.provider == payload["provider"],
                or_(
                    MarketInstrument.normalized_provider_symbol
                    == payload["normalized_provider_symbol"],
                    MarketInstrument.normalized_symbol == payload["normalized_symbol"],
                ),
            )
        )
        instrument = (await db.execute(stmt)).scalars().first()
        if instrument is None:
            instrument = MarketInstrument(**payload)
            db.add(instrument)
            await db.flush()
        else:
            for key, value in payload.items():
                setattr(instrument, key, value)
            await db.flush()

        payload["aliases"] = aliases
        return instrument

    async def _sync_aliases(
        self,
        db: AsyncSession,
        *,
        instrument: MarketInstrument,
        aliases: Sequence[dict[str, Any]],
    ) -> None:
        """同步品种别名表：不存在则新增，存在则更新绑定。"""
        for alias_payload in aliases:
            stmt = select(MarketInstrumentAlias).where(
                MarketInstrumentAlias.normalized_alias
                == alias_payload["normalized_alias"]
            )
            alias = (await db.execute(stmt)).scalars().first()
            if alias is None:
                db.add(
                    MarketInstrumentAlias(
                        instrument_id=instrument.id,
                        **alias_payload,
                    )
                )
                continue

            alias.instrument_id = instrument.id
            alias.alias = alias_payload["alias"]
            alias.alias_type = alias_payload["alias_type"]
            alias.priority = alias_payload["priority"]
            alias.is_active = True

    async def _sync_single_state(
        self,
        db: AsyncSession,
        state: MarketBarSyncState,
        instrument: MarketInstrument,
    ) -> int:
        """执行单个状态的增量拉取与历史回补，并更新状态机字段。"""
        state.last_attempt_at = _utc_now()
        state.last_status = "RUNNING"
        state.last_error = None

        inserted_count = 0

        # --- 第一阶段（前向/增量）：始终拉取最新数据，保持与实时行情同步 ---
        forward_payload = {
            "symbol": instrument.provider_symbol,
            "interval": state.interval,
            "outputsize": self._incremental_outputsize(state.interval),
            "start_date": None,
            "end_date": None,
            "price_type": state.price_type,
        }

        if state.latest_synced_bar_time is not None:
            # 与已有最新数据做部分重叠，避免漏掉跳动的数据
            overlap = self._interval_delta(state.interval) * max(
                1, settings.fxcm_sync_incremental_overlap_bars
            )
            incremental_start = state.latest_synced_bar_time - overlap
            forward_payload["start_date"] = incremental_start.isoformat()

        payload = await fxcm_sidecar_service.get_history(**forward_payload)
        values = payload.get("values") if isinstance(payload, Mapping) else []
        rows = self._build_bar_rows(
            instrument=instrument,
            state=state,
            values=values if isinstance(values, list) else [],
            meta=payload.get("meta") if isinstance(payload, Mapping) else None,
        )

        if rows:
            inserted = await self._upsert_bars(db, rows)
            inserted_count += inserted
            bar_times = [row["bar_time"] for row in rows]

            state.earliest_synced_bar_time = self._min_datetime(
                state.earliest_synced_bar_time, min(bar_times)
            )
            state.latest_synced_bar_time = self._max_datetime(
                state.latest_synced_bar_time, max(bar_times)
            )

            state.last_requested_start_at = self._parse_request_payload_datetime(
                forward_payload.get("start_date")
            )
            state.last_requested_end_at = self._parse_request_payload_datetime(
                forward_payload.get("end_date")
            )

        # --- 第二阶段（后向/回补）：用时间换空间，每次往后推进一小批，直至数据枯竭 ---
        if state.earliest_synced_bar_time is not None and not state.backfill_completed:
            batch_size = 2000  # 每次仅抓取一小批，不阻塞长连接
            backfill_payload = {
                "symbol": instrument.provider_symbol,
                "interval": state.interval,
                "outputsize": batch_size,
                "start_date": None,
                "end_date": state.earliest_synced_bar_time.isoformat(),
                "price_type": state.price_type,
            }

            b_payload = await fxcm_sidecar_service.get_history(**backfill_payload)
            b_values = b_payload.get("values") if isinstance(b_payload, Mapping) else []
            b_rows = self._build_bar_rows(
                instrument=instrument,
                state=state,
                values=b_values if isinstance(b_values, list) else [],
                meta=b_payload.get("meta") if isinstance(b_payload, Mapping) else None,
            )

            if b_rows:
                inserted = await self._upsert_bars(db, b_rows)
                inserted_count += inserted
                b_bar_times = [row["bar_time"] for row in b_rows]
                state.earliest_synced_bar_time = self._min_datetime(
                    state.earliest_synced_bar_time, min(b_bar_times)
                )

                # FXCM 的历史分页有时会不饱满，不以返回时间是否推进作为枯竭判断
                # 只有极少条（不足 5%）才认为到达数据底部
                if len(b_rows) < batch_size * 0.05:
                    state.backfill_completed = True
            else:
                state.backfill_completed = True

        state.last_success_at = _utc_now()
        state.last_status = "SUCCESS"
        state.retry_count = 0
        state.sync_mode = "INCREMENTAL" if state.backfill_completed else "BACKFILL"

        if state.backfill_completed:
            state.next_sync_from = self._calculate_next_sync_from(
                interval=state.interval,
                now=_utc_now(),
                skip_weekends=self._normalize_asset_type(instrument.asset_type)
                not in self.ALWAYS_OPEN_ASSET_TYPES,
            )
        else:
            # 回补未完成：与 INCREMENTAL 使用相同的延迟(120秒)，让所有品种公平竞争调度时间
            # 如果此处延迟远短于 INCREMENTAL 的 2 分钟，BTC/ETH 这类无限历史品种会霸占队列
            state.next_sync_from = _utc_now() + timedelta(seconds=120)

        state.updated_at = _utc_now()
        return inserted_count

    def _parse_request_payload_datetime(self, value: Any) -> datetime | None:
        """把请求载荷中的时间值解析为 UTC datetime。"""
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        if not isinstance(value, str) or not value.strip():
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _build_bar_rows(
        self,
        *,
        instrument: MarketInstrument,
        state: MarketBarSyncState,
        values: Sequence[Mapping[str, Any]],
        meta: Mapping[str, Any] | None,
    ) -> list[dict[str, Any]]:
        """将 sidecar 返回的 K 线数据转换为数据库行结构。"""
        source_interval = None
        if isinstance(meta, Mapping):
            source_interval = self._coalesce_str(meta.get("provider_interval"))

        rows: list[dict[str, Any]] = []
        for item in values:
            bar_time = self._parse_bar_time(item)
            close_price = self._to_decimal(item.get("close"))
            if bar_time is None or close_price is None:
                continue

            open_price = self._to_decimal(item.get("open")) or close_price
            high_price = self._to_decimal(item.get("high")) or close_price
            low_price = self._to_decimal(item.get("low")) or close_price

            rows.append(
                {
                    "instrument_id": instrument.id,
                    "provider": self.PROVIDER,
                    "provider_symbol": instrument.provider_symbol,
                    "interval": state.interval,
                    "price_type": state.price_type,
                    "data_origin": "PROVIDER",
                    "source_interval": source_interval,
                    "bar_time": bar_time,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": self._to_int(item.get("volume")),
                }
            )
        return rows

    async def _upsert_bars(
        self,
        db: AsyncSession,
        rows: Sequence[dict[str, Any]],
    ) -> int:
        """批量 UPSERT K 线数据，冲突时用新值覆盖。"""
        if not rows:
            return 0

        stmt = insert(MarketOHLCVBar).values(list(rows))
        stmt = stmt.on_conflict_do_update(
            constraint="uq_market_ohlcv_bar_instrument_interval_price_time",
            set_={
                "provider": stmt.excluded.provider,
                "provider_symbol": stmt.excluded.provider_symbol,
                "data_origin": stmt.excluded.data_origin,
                "source_interval": stmt.excluded.source_interval,
                "open": stmt.excluded.open,
                "high": stmt.excluded.high,
                "low": stmt.excluded.low,
                "close": stmt.excluded.close,
                "volume": stmt.excluded.volume,
            },
        )
        await db.execute(stmt)
        return len(rows)

    def _pick_best_search_item(
        self,
        items: Any,
        *,
        canonical_symbol: str,
        profile: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None:
        """从搜索结果中优先挑选与目标符号最匹配的条目。"""
        if not isinstance(items, Sequence):
            return None

        target_symbols = {
            self._normalize_symbol(canonical_symbol),
        }
        if profile is not None:
            for key in ("symbol", "provider_symbol"):
                value = self._coalesce_str(profile.get(key))
                if value:
                    target_symbols.add(self._normalize_symbol(value))

        for item in items:
            if not isinstance(item, Mapping):
                continue
            candidates = {
                self._normalize_symbol(self._coalesce_str(item.get("symbol")) or ""),
                self._normalize_symbol(
                    self._coalesce_str(item.get("provider_symbol")) or ""
                ),
            }
            if target_symbols & candidates:
                return item

        for item in items:
            if isinstance(item, Mapping):
                return item
        return None

    def _build_aliases(
        self,
        *,
        canonical_symbol: str,
        provider_symbol: str,
        profile: Mapping[str, Any] | None,
        search_item: Mapping[str, Any] | None,
    ) -> list[dict[str, Any]]:
        """生成去重后的别名集合，供别名表同步使用。"""
        alias_specs: list[tuple[str, str, int]] = [
            (canonical_symbol, "CANONICAL", 1),
            (provider_symbol, "PROVIDER", 2),
            (canonical_symbol.replace("/", ""), "SEARCH", 30),
        ]

        if search_item is not None:
            label = self._coalesce_str(search_item.get("label"))
            if label:
                alias_specs.append((label, "SEARCH", 40))

        if profile is not None:
            for alias in profile.get("aliases", ()):
                alias_value = self._coalesce_str(alias)
                if alias_value:
                    alias_specs.append((alias_value, "MANUAL", 10))

        aliases: list[dict[str, Any]] = []
        seen: set[str] = set()
        for alias, alias_type, priority in alias_specs:
            normalized_alias = self._normalize_symbol(alias)
            if not normalized_alias or normalized_alias in seen:
                continue
            seen.add(normalized_alias)
            aliases.append(
                {
                    "alias": alias,
                    "normalized_alias": normalized_alias,
                    "alias_type": alias_type,
                    "priority": priority,
                    "is_active": True,
                }
            )
        return aliases

    def _normalize_symbol(self, value: str) -> str:
        """规范化符号文本，便于一致匹配。"""
        return market_master_service._normalize_lookup(value)

    def _sort_weight(self, symbol: str, hot_symbols: list[str] | None = None) -> int:
        """根据热池顺序计算排序权重，越靠前权重越小。"""
        if not hot_symbols:
            return 999
        try:
            return hot_symbols.index(symbol)
        except ValueError:
            return 999

    def _metadata_sync_due(self) -> bool:
        """判断是否到了下一次元数据同步窗口。"""
        if self._last_metadata_sync_at is None:
            return True
        due_at = self._last_metadata_sync_at + timedelta(
            hours=settings.fxcm_sync_metadata_interval_hours
        )
        return _utc_now() >= due_at

    def _incremental_outputsize(self, interval: str) -> int:
        """按周期返回增量拉取建议条数。"""
        if interval in {"1month", "1week", "1day"}:
            return settings.fxcm_sync_1day_incremental_outputsize
        if interval in {"8h", "4h"}:
            return max(60, settings.fxcm_sync_1h_incremental_outputsize // 2)
        if interval == "2h":
            return max(100, settings.fxcm_sync_1h_incremental_outputsize)
        if interval in {"1h", "45min", "30min"}:
            return max(300, settings.fxcm_sync_1h_incremental_outputsize * 2)
        if interval in {"15min", "5min", "1min"}:
            return max(500, settings.fxcm_sync_1h_incremental_outputsize * 3)
        return settings.fxcm_sync_1h_incremental_outputsize

    def _state_priority(self, interval: str) -> int:
        """根据周期在配置中的顺序计算任务优先级。"""
        try:
            return (self.sync_intervals.index(interval) + 1) * 10
        except ValueError:
            return 100

    def _interval_delta(self, interval: str) -> timedelta:
        """把周期字符串转换为对应的时间跨度。"""
        if interval == "1month":
            return timedelta(days=30)
        if interval == "1week":
            return timedelta(weeks=1)
        if interval == "1day":
            return timedelta(days=1)
        if interval == "8h":
            return timedelta(hours=8)
        if interval == "4h":
            return timedelta(hours=4)
        if interval == "2h":
            return timedelta(hours=2)
        if interval == "1h":
            return timedelta(hours=1)
        if interval == "45min":
            return timedelta(minutes=45)
        if interval == "30min":
            return timedelta(minutes=30)
        if interval == "15min":
            return timedelta(minutes=15)
        if interval == "5min":
            return timedelta(minutes=5)
        if interval == "1min":
            return timedelta(minutes=1)
        return timedelta(hours=1)

    def _weekend_resume_at(
        self,
        instrument: MarketInstrument,
        current_time: datetime,
    ) -> datetime | None:
        """若非 7x24 品种且当前在周末，返回下周一恢复时间。"""
        asset_type = self._normalize_asset_type(instrument.asset_type)
        if asset_type in self.ALWAYS_OPEN_ASSET_TYPES:
            return None
        if current_time.weekday() < 5:
            return None

        days_until_monday = 7 - current_time.weekday()
        monday = (current_time + timedelta(days=days_until_monday)).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        return monday

    def _is_allowed_today(
        self, instrument: MarketInstrument, current_time: datetime
    ) -> bool:
        """根据硬性调度策略：不同日期采集不同品类，防止单一品类霸占队列。"""
        weekday = current_time.weekday()
        asset_type = self._normalize_asset_type(instrument.asset_type) or ""

        if "digital" in asset_type or "crypto" in asset_type:
            category = "crypto"
        elif "forex" in asset_type or "fx" in asset_type or "currency" in asset_type:
            category = "forex"
        elif "index" in asset_type or "stock" in asset_type or "equity" in asset_type:
            category = "index"
        elif (
            "metal" in asset_type
            or "commodity" in asset_type
            or "oil" in asset_type
            or "energy" in asset_type
        ):
            category = "commodity"
        else:
            # 通过 symbol 推断兜底
            symbol = (instrument.symbol or "").upper()
            if symbol in ("BTC/USD", "ETH/USD"):
                category = "crypto"
            elif symbol in (
                "US30",
                "NAS100",
                "SPX500",
                "UK100",
                "GER30",
                "FRA40",
                "USDOLLAR",
            ):
                category = "index"
            elif "XAU" in symbol or "XAG" in symbol or "OIL" in symbol:
                category = "commodity"
            elif len(symbol) == 7 and "/" in symbol:
                category = "forex"
            else:
                category = "other"

        if category == "other":
            return True

        # 每日品类轮换（保证覆盖不同周期，今天是什么品类就只采这一个类别）
        if weekday == 0:  # 周一
            return category == "forex"
        elif weekday == 1:  # 周二
            return category == "index"
        elif weekday == 2:  # 周三
            return category == "commodity"
        elif weekday == 3:  # 周四
            return category == "crypto"
        elif weekday == 4:  # 周五
            return category == "forex"
        elif weekday == 5:  # 周六
            return category == "index"
        elif weekday == 6:  # 周日
            return category == "commodity"

        return True

    def _calculate_next_sync_from(
        self,
        *,
        interval: str,
        now: datetime,
        skip_weekends: bool,
    ) -> datetime:
        """计算下一次调度时间，默认小间隔轮询并可跳过周末。"""
        # 为了让更多品种都能轮播抓取，不在特定整点集中触发
        # 统一设置一个较小间隔来排队等待下一轮（例如 2 分钟）
        next_sync_from = now + timedelta(minutes=2)

        while skip_weekends and next_sync_from.weekday() >= 5:
            next_sync_from = next_sync_from + timedelta(days=1)

        return next_sync_from

    def _normalize_asset_type(self, asset_type: str | None) -> str | None:
        """标准化资产类型文本，便于规则判断。"""
        if asset_type is None:
            return None
        normalized = asset_type.strip().casefold()
        return normalized or None

    def _parse_bar_time(self, item: Mapping[str, Any]) -> datetime | None:
        """从 bar 项中解析时间，优先 timestamp，其次 datetime。"""
        timestamp = self._to_int(item.get("timestamp"))
        if timestamp is not None:
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)

        raw_value = self._coalesce_str(item.get("datetime"))
        if raw_value is None:
            return None
        try:
            parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _coalesce_str(self, *values: Any) -> str | None:
        """返回第一个非空字符串值。"""
        for value in values:
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return None

    def _to_int(self, value: Any) -> int | None:
        """安全转换为 int，失败返回 None。"""
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _to_decimal(self, value: Any) -> Decimal | None:
        """安全转换为 Decimal，失败返回 None。"""
        if value in (None, ""):
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None

    def _min_datetime(
        self,
        current: datetime | None,
        new_value: datetime,
    ) -> datetime:
        """在 current 与 new_value 之间取更早时间。"""
        if current is None:
            return new_value
        return min(current, new_value)

    def _max_datetime(
        self,
        current: datetime | None,
        new_value: datetime,
    ) -> datetime:
        """在 current 与 new_value 之间取更晚时间。"""
        if current is None:
            return new_value
        return max(current, new_value)


fxcm_market_sync_service = FXCMMarketSyncService()


class FXCMMarketSyncScheduler:
    def __init__(self, service: FXCMMarketSyncService) -> None:
        """初始化调度器并绑定同步服务实例。"""
        self._service = service
        self._task: asyncio.Task[None] | None = None
        self._stop_event: asyncio.Event | None = None

    def is_running(self) -> bool:
        """判断调度循环任务是否仍在运行。"""
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        """启动后台调度循环（若开关开启且当前未运行）。"""
        if not settings.fxcm_sync_enabled or self.is_running():
            return
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(
            self._run_loop(),
            name="fxcm-market-sync-scheduler",
        )

    async def stop(self) -> None:
        """停止后台调度循环并回收任务句柄。"""
        if self._task is None:
            return
        assert self._stop_event is not None
        self._stop_event.set()
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None
        self._stop_event = None

    async def _run_loop(self) -> None:
        """调度主循环：周期性执行 run_cycle，并按负载调整等待时间。"""
        assert self._stop_event is not None
        while not self._stop_event.is_set():
            processed_states = 0
            try:
                async with db_client.async_session() as db:
                    result = await self._service.run_cycle(
                        db,
                        reason="scheduler",
                        force_metadata=False,
                        force_due=False,
                    )
                    processed_states = result.processed_states
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("FXCM market scheduler loop failed")

            try:
                delay = (
                    1.0
                    if processed_states > 0
                    else max(15, settings.fxcm_sync_poll_interval_seconds)
                )
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=delay,
                )
            except TimeoutError:
                continue


fxcm_market_sync_scheduler = FXCMMarketSyncScheduler(fxcm_market_sync_service)
