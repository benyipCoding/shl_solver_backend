"""本地 market 数据推送到远程 PostgreSQL（CLI 脚本专用，不参与主后端 lifespan）。"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Sequence, TypeVar

from sqlalchemy import create_engine, delete, func, or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session, sessionmaker

from app.models.market_data import (
    MarketInstrument,
    MarketInstrumentAlias,
    MarketOHLCVBar,
)
from app.services.fxcm_market_sync.constants import PROVIDER
from app.services.fxcm_market_sync.intervals import interval_delta, normalize_sync_interval
from app.services.fxcm_market_sync.utils import (
    normalize_symbol,
    parse_request_payload_datetime,
)
from app.services.market_data_replica.types import MarketReplicaResult
from app.models.market_repair_outbox import MarketRepairOutbox
from app.services.market_master import market_master_service


logger = logging.getLogger(__name__)

_T = TypeVar("_T")

# 连续多少次断连且期间没有任何批次提交成功，才放弃当前序列。
_SERIES_SYNC_ATTEMPTS = 3

# 沿唯一索引 (instrument_id, interval, price_type, bar_time) 跳跃扫描列出所有序列；
# 直接 SELECT DISTINCT 在千万行级别要全表扫描数分钟。
_LIST_BAR_SERIES_SQL = text(
    """
    WITH RECURSIVE series AS (
        (
            SELECT instrument_id, interval, price_type
            FROM market_ohlcv_bar
            WHERE provider = :provider
            ORDER BY instrument_id, interval, price_type
            LIMIT 1
        )
        UNION ALL
        SELECT nxt.instrument_id, nxt.interval, nxt.price_type
        FROM series, LATERAL (
            SELECT b.instrument_id, b.interval, b.price_type
            FROM market_ohlcv_bar b
            WHERE b.provider = :provider
              AND (b.instrument_id, b.interval, b.price_type)
                  > (series.instrument_id, series.interval, series.price_type)
            ORDER BY b.instrument_id, b.interval, b.price_type
            LIMIT 1
        ) nxt
    )
    SELECT instrument_id, interval, price_type FROM series
    """
)

# 一次往返取回全部序列的远程水位；每个序列仍走唯一索引首尾探测。
# 远程经 SSH 隧道单次往返约 0.6s，逐个序列查询时 400 个序列光规划就要十几分钟。
_REMOTE_WATERMARKS_SQL = text(
    """
    SELECT
        s.instrument_id,
        s.interval,
        s.price_type,
        (
            SELECT min(market_ohlcv_bar.bar_time) FROM market_ohlcv_bar
            WHERE market_ohlcv_bar.instrument_id = s.instrument_id
              AND market_ohlcv_bar.interval = s.interval
              AND market_ohlcv_bar.price_type = s.price_type
        ) AS min_bar_time,
        (
            SELECT max(market_ohlcv_bar.bar_time) FROM market_ohlcv_bar
            WHERE market_ohlcv_bar.instrument_id = s.instrument_id
              AND market_ohlcv_bar.interval = s.interval
              AND market_ohlcv_bar.price_type = s.price_type
        ) AS max_bar_time
    FROM unnest(
        CAST(:instrument_ids AS integer[]),
        CAST(:intervals AS text[]),
        CAST(:price_types AS text[])
    ) AS s(instrument_id, interval, price_type)
    """
)

# 规划阶段统计本地待推行数时，每处理多少个序列输出一次进度。
_PLAN_LOG_EVERY = 50


@dataclass
class _BarSeries:
    """一个待推送的 K 线序列（品种 × 周期 × 价格类型）。"""

    source_instrument_id: int
    target_instrument_id: int
    symbol: str
    interval: str
    price_type: str
    remote_min_bar_time: datetime | None
    remote_max_bar_time: datetime | None
    pending_rows: int

    @property
    def label(self) -> str:
        return f"{self.symbol} {self.interval} {self.price_type}"


class _BarSyncProgress:
    """增量推送的整体进度，每提交一批输出一行。"""

    def __init__(self, *, total_rows: int, total_series: int) -> None:
        self.total_rows = total_rows
        self.total_series = total_series
        self.pushed_rows = 0
        self.series_index = 0
        self.series_pushed_rows = 0
        self._series: _BarSeries | None = None
        self._started_at = time.monotonic()

    def start_series(self, series: _BarSeries) -> None:
        self.series_index += 1
        self.series_pushed_rows = 0
        self._series = series

    def batch_committed(self, rows: int, direction: str) -> None:
        self.pushed_rows += rows
        self.series_pushed_rows += rows
        series = self._series
        elapsed = max(time.monotonic() - self._started_at, 1e-6)
        rate = self.pushed_rows / elapsed
        remaining = max(0, self.total_rows - self.pushed_rows)
        eta = _format_duration(remaining / rate) if rate > 0 else "?"
        logger.info(
            "[%5.1f%%] %s/%s rows | series %s/%s %s (%s) %s/%s | %.0f rows/s | ETA %s",
            _percent(self.pushed_rows, self.total_rows),
            self.pushed_rows,
            self.total_rows,
            self.series_index,
            self.total_series,
            series.label if series else "?",
            direction,
            self.series_pushed_rows,
            series.pending_rows if series else "?",
            rate,
            eta,
        )

    def finish(self) -> None:
        logger.info(
            "Bar sync finished: %s rows in %s series, took %s",
            self.pushed_rows,
            self.series_index,
            _format_duration(time.monotonic() - self._started_at),
        )


def _percent(done: int, total: int) -> float:
    if total <= 0:
        return 100.0
    return min(100.0, done * 100.0 / total)


def _format_duration(seconds: float) -> str:
    seconds = int(max(0, seconds))
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def _is_retryable_disconnect(exc: DBAPIError) -> bool:
    """SSL 断开 / 超时等连接级错误可重连重试；约束冲突等业务错误直接抛出。"""
    return bool(exc.connection_invalidated) or isinstance(exc, OperationalError)

_REPLICA_TABLES = (
    "market_instrument",
    "market_instrument_alias",
    "market_ohlcv_bar",
)

_INSTRUMENT_UPSERT_COLUMNS = (
    "provider",
    "symbol",
    "provider_symbol",
    "normalized_provider_symbol",
    "name",
    "display_label",
    "exchange",
    "mic_code",
    "market",
    "asset_type",
    "country",
    "currency",
    "exchange_timezone",
    "provider_plan",
    "sort_weight",
    "is_active",
    "is_searchable",
    "is_hot",
    "supports_history",
    "supports_quote",
    "source_payload",
    "metadata_synced_at",
    "last_seen_at",
    "updated_at",
    "deleted_at",
)

_ALIAS_UPSERT_COLUMNS = (
    "instrument_id",
    "alias",
    "alias_type",
    "priority",
    "is_active",
    "updated_at",
    "deleted_at",
)

_BAR_UPSERT_COLUMNS = (
    "provider",
    "provider_symbol",
    "data_origin",
    "source_interval",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "updated_at",
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _model_row_dict(instance: object) -> dict[str, Any]:
    table = instance.__table__  # type: ignore[attr-defined]
    return {column.name: getattr(instance, column.name) for column in table.columns}


class MarketDataReplicaService:
    """将本地 market 元数据与 K 线增量/全量推送到远程库。"""

    def __init__(
        self,
        source_database_url: str,
        target_database_url: str,
        *,
        batch_size: int = 2000,
        overlap_bars: int = 2,
    ) -> None:
        self._source_engine = create_engine(source_database_url, future=True)
        self._target_engine = create_engine(target_database_url, future=True, pool_pre_ping=True)
        self._source_session_factory = sessionmaker(
            bind=self._source_engine, expire_on_commit=False
        )
        self._target_session_factory = sessionmaker(
            bind=self._target_engine, expire_on_commit=False
        )
        self._batch_size = max(100, batch_size)
        self._overlap_bars = max(0, overlap_bars)

    def close(self) -> None:
        self._source_engine.dispose()
        self._target_engine.dispose()

    def get_status(self) -> dict[str, Any]:
        """对比本地与远程 market 表行数及最新 K 线时间。"""
        with self._source_session() as source, self._target_session() as target:
            return {
                "source": self._collect_counts(source),
                "target": self._collect_counts(target),
                "repair_queue": self._repair_queue_counts(source),
            }

    def _repair_queue_counts(self, source: Session) -> dict[str, int]:
        pending, failed, synced = source.execute(
            select(
                func.count().filter(MarketRepairOutbox.synced_at.is_(None)),
                func.count().filter(
                    MarketRepairOutbox.synced_at.is_(None),
                    MarketRepairOutbox.last_error.is_not(None),
                ),
                func.count().filter(MarketRepairOutbox.synced_at.is_not(None)),
            ).where(MarketRepairOutbox.provider == PROVIDER)
        ).one()
        return {"pending": pending, "failed": failed, "synced": synced}

    def run_pending_repairs(self) -> MarketReplicaResult:
        """消费源库 outbox；生产先提交，本地后确认，崩溃后可安全重放。"""
        result = MarketReplicaResult(mode="repair")
        try:
            with self._source_session() as source:
                # 固定本轮上界，每条失败记录只尝试一次；新修复留给下次命令。
                upper_id = source.scalar(
                    select(func.max(MarketRepairOutbox.id)).where(
                        MarketRepairOutbox.provider == PROVIDER,
                        MarketRepairOutbox.synced_at.is_(None),
                    )
                ) or 0
            last_id = 0
            while last_id < upper_id:
                with self._source_session() as source:
                    job = source.scalar(
                        select(MarketRepairOutbox)
                        .where(
                            MarketRepairOutbox.provider == PROVIDER,
                            MarketRepairOutbox.synced_at.is_(None),
                            MarketRepairOutbox.id > last_id,
                            MarketRepairOutbox.id <= upper_id,
                        )
                        .order_by(MarketRepairOutbox.id)
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                    if job is None:
                        break
                    last_id = job.id
                    job.attempts += 1
                    job.last_attempt_at = _utc_now()
                    # 源库行锁一直持有到远端提交及本地确认完成；断连自动释放。
                    try:
                        upserted, deleted = self.sync_bars_range_repair(
                            symbol=job.symbol,
                            interval=job.interval,
                            price_type=job.price_type,
                            start_date=job.start_at.isoformat(),
                            end_date=job.end_at.isoformat(),
                        )
                    except Exception as exc:
                        job.last_error = f"{type(exc).__name__}: {exc}"
                        source.commit()
                        result.repair_jobs_failed += 1
                        result.errors.append(f"repair job {job.id}: {job.last_error}")
                        logger.exception("Repair replica job %s failed; retained for retry", job.id)
                    else:
                        job.synced_at = _utc_now()
                        job.last_error = None
                        job.bars_upserted = upserted
                        job.bars_deleted = deleted
                        source.commit()
                        result.repair_jobs_succeeded += 1
                        result.bars_upserted += upserted
                        result.bars_deleted += deleted
                        logger.info("Repair replica job %s synced", job.id)
            with self._source_session() as source:
                result.repair_jobs_pending = self._repair_queue_counts(source)["pending"]
        except Exception as exc:
            logger.exception("Market repair outbox processing failed")
            result.errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            result.finished_at = _utc_now()
        return result

    def run_bootstrap(self, *, force: bool = False) -> MarketReplicaResult:
        """一次性全量复制 instrument / alias / bar 到空远程库。"""
        result = MarketReplicaResult(mode="bootstrap")
        try:
            with self._target_session() as target:
                if not force and not self._target_market_tables_empty(target):
                    result.skipped = True
                    result.errors.append(
                        "target market tables are not empty; use --force to overwrite rows"
                    )
                    return result
                if force and not self._target_market_tables_empty(target):
                    self._truncate_target_market_tables(target)

            result.instruments_upserted = self._bootstrap_copy_table(
                MarketInstrument,
                provider_filter=True,
            )
            result.aliases_upserted = self._bootstrap_copy_table(
                MarketInstrumentAlias,
                provider_filter=False,
            )
            result.bars_upserted = self._bootstrap_copy_table(
                MarketOHLCVBar,
                provider_filter=True,
            )
            self._reset_target_sequences()
        except Exception as exc:
            logger.exception("Market data bootstrap failed")
            result.errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            result.finished_at = _utc_now()
        return result

    def run_incremental(self) -> MarketReplicaResult:
        """增量同步元数据与新 K 线（默认任务计划模式）。"""
        result = MarketReplicaResult(mode="incremental")
        try:
            metadata = self.sync_metadata()
            result.instruments_upserted = metadata["instruments"]
            result.aliases_upserted = metadata["aliases"]
            result.bars_upserted = self.sync_bars_incremental()
        except Exception as exc:
            logger.exception("Market data incremental sync failed")
            result.errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            result.finished_at = _utc_now()
        return result

    def sync_metadata(self) -> dict[str, int]:
        """Upsert 远程 instrument 与 alias（按 normalized_symbol / normalized_alias）。"""
        with self._source_session() as source, self._target_session() as target:
            instruments = source.scalars(
                select(MarketInstrument)
                .where(MarketInstrument.provider == PROVIDER)
                .order_by(MarketInstrument.id.asc())
            ).all()

            instrument_count = 0
            alias_count = 0

            for instrument in instruments:
                payload = _model_row_dict(instrument)
                payload.pop("id", None)
                payload["updated_at"] = _utc_now()

                stmt = insert(MarketInstrument).values(payload)
                stmt = stmt.on_conflict_do_update(
                    constraint="uq_market_instrument_normalized_symbol",
                    set_={
                        column: stmt.excluded[column]
                        for column in _INSTRUMENT_UPSERT_COLUMNS
                    },
                )
                target.execute(stmt)
                instrument_count += 1

                target_instrument = target.scalar(
                    select(MarketInstrument.id).where(
                        MarketInstrument.normalized_symbol
                        == instrument.normalized_symbol
                    )
                )
                if target_instrument is None:
                    continue

                aliases = source.scalars(
                    select(MarketInstrumentAlias).where(
                        MarketInstrumentAlias.instrument_id == instrument.id
                    )
                ).all()
                for alias in aliases:
                    alias_payload = _model_row_dict(alias)
                    alias_payload.pop("id", None)
                    alias_payload["instrument_id"] = int(target_instrument)
                    alias_payload["updated_at"] = _utc_now()

                    alias_stmt = insert(MarketInstrumentAlias).values(alias_payload)
                    alias_stmt = alias_stmt.on_conflict_do_update(
                        constraint="uq_market_instrument_alias_normalized_alias",
                        set_={
                            column: alias_stmt.excluded[column]
                            for column in _ALIAS_UPSERT_COLUMNS
                        },
                    )
                    target.execute(alias_stmt)
                    alias_count += 1

            target.commit()
            return {"instruments": instrument_count, "aliases": alias_count}

    def sync_bars_incremental(self) -> int:
        """按远程水位推送本地 K 线：比远程最新更新的、以及比远程最早更早的（回补出来的历史）。

        每批（batch_size 行）推完即提交：SSL 断连只回滚当前一批，重连后按远程新水位续推。
        向前部分按时间升序推、回补部分按时间降序推，已提交的数据始终与远程原有数据连续。
        """
        series_list = self._plan_bar_series()
        if not series_list:
            logger.info("No bar series to push")
            return 0

        progress = _BarSyncProgress(
            total_rows=sum(series.pending_rows for series in series_list),
            total_series=len(series_list),
        )
        logger.info(
            "Pushing %s rows across %s series (batch size %s)",
            progress.total_rows,
            progress.total_series,
            self._batch_size,
        )
        for series in series_list:
            progress.start_series(series)
            # 首次尝试沿用规划阶段读到的远程水位；重连后重新读取，从已提交的位置续推。
            self._retry_disconnects(
                lambda attempt: self._sync_bar_series(
                    series, progress, reuse_planned_watermarks=attempt == 1
                ),
                describe=series.label,
                progress_probe=lambda: progress.pushed_rows,
            )
        progress.finish()
        return progress.pushed_rows

    def _plan_bar_series(self) -> list[_BarSeries]:
        """列出本地全部序列，读取远程水位并统计每个序列待推送行数（用于整体进度）。"""
        started_at = time.monotonic()
        logger.info("Planning bar sync: listing local series")

        def build_id_map(_attempt: int) -> dict[int, int]:
            with self._source_session() as source, self._target_session() as target:
                return self._build_instrument_id_map(source, target)

        id_map = self._retry_disconnects(build_id_map, describe="instrument id map")
        if not id_map:
            return []

        planned: list[_BarSeries] = []
        with self._source_session() as source:
            symbols = {
                int(instrument_id): symbol
                for instrument_id, symbol in source.execute(
                    select(MarketInstrument.id, MarketInstrument.symbol).where(
                        MarketInstrument.provider == PROVIDER
                    )
                ).all()
            }
            groups = source.execute(_LIST_BAR_SERIES_SQL, {"provider": PROVIDER}).all()
            mapped = [
                (int(source_instrument_id), id_map[int(source_instrument_id)], interval, price_type)
                for source_instrument_id, interval, price_type in groups
                if int(source_instrument_id) in id_map
            ]
            unmapped = len(groups) - len(mapped)
            logger.info(
                "Planning: %s local series (%s without a remote instrument); reading remote watermarks",
                len(groups),
                unmapped,
            )
            watermarks = self._retry_disconnects(
                lambda _attempt: self._read_remote_watermarks_bulk(
                    [(target_id, interval, price_type) for _, target_id, interval, price_type in mapped]
                ),
                describe="remote watermarks",
            )
            logger.info(
                "Planning: remote watermarks read in %s; counting local rows to push",
                _format_duration(time.monotonic() - started_at),
            )

            for index, (source_instrument_id, target_instrument_id, interval, price_type) in enumerate(
                mapped, start=1
            ):
                if index % _PLAN_LOG_EVERY == 0:
                    logger.info("Planning: counted %s/%s series", index, len(mapped))
                remote_min, remote_max = watermarks.get(
                    (target_instrument_id, interval, price_type), (None, None)
                )
                pending = sum(
                    int(
                        source.scalar(
                            select(func.count())
                            .select_from(MarketOHLCVBar)
                            .where(
                                *self._series_filter(source_instrument_id, interval, price_type),
                                *conditions,
                            )
                        )
                        or 0
                    )
                    for _, conditions, _ in self._pending_ranges(interval, remote_min, remote_max)
                )
                if pending == 0:
                    continue
                planned.append(
                    _BarSeries(
                        source_instrument_id=source_instrument_id,
                        target_instrument_id=target_instrument_id,
                        symbol=symbols.get(source_instrument_id, str(source_instrument_id)),
                        interval=interval,
                        price_type=price_type,
                        remote_min_bar_time=remote_min,
                        remote_max_bar_time=remote_max,
                        pending_rows=pending,
                    )
                )

        logger.info(
            "Planned %s of %s local series in %s (%s without a remote instrument)",
            len(planned),
            len(groups),
            _format_duration(time.monotonic() - started_at),
            unmapped,
        )
        return planned

    def _read_remote_watermarks_bulk(
        self, series: Sequence[tuple[int, str, str]]
    ) -> dict[tuple[int, str, str], tuple[datetime | None, datetime | None]]:
        """一条查询读取多个序列的远程 (最早, 最新) bar_time。"""
        if not series:
            return {}
        with self._target_session() as target:
            rows = target.execute(
                _REMOTE_WATERMARKS_SQL,
                {
                    "instrument_ids": [int(instrument_id) for instrument_id, _, _ in series],
                    "intervals": [interval for _, interval, _ in series],
                    "price_types": [price_type for _, _, price_type in series],
                },
            ).all()
        return {
            (int(instrument_id), interval, price_type): (min_bar_time, max_bar_time)
            for instrument_id, interval, price_type, min_bar_time, max_bar_time in rows
        }

    def _retry_disconnects(
        self,
        action: Callable[[int], _T],
        *,
        describe: str,
        progress_probe: Callable[[], int] | None = None,
    ) -> _T:
        """断连后用新连接重试 action(attempt)；期间有批次提交成功则重新计数。"""
        attempt = 0
        failures = 0
        while True:
            attempt += 1
            marker = progress_probe() if progress_probe is not None else None
            try:
                return action(attempt)
            except DBAPIError as exc:
                if not _is_retryable_disconnect(exc):
                    raise
                if progress_probe is not None and progress_probe() != marker:
                    failures = 0
                failures += 1
                if failures >= _SERIES_SYNC_ATTEMPTS:
                    raise
                delay = 2 ** (failures - 1)
                logger.warning(
                    "Database connection lost while syncing %s (%s); retrying in %ss (attempt %s/%s)",
                    describe,
                    type(exc.orig).__name__ if exc.orig is not None else type(exc).__name__,
                    delay,
                    failures + 1,
                    _SERIES_SYNC_ATTEMPTS,
                )
                time.sleep(delay)

    def _read_remote_watermarks(
        self, target_instrument_id: int, interval: str, price_type: str
    ) -> tuple[datetime | None, datetime | None]:
        with self._target_session() as target:
            remote_min_bar_time, remote_max_bar_time = target.execute(
                select(func.min(MarketOHLCVBar.bar_time), func.max(MarketOHLCVBar.bar_time))
                .where(*self._series_filter(target_instrument_id, interval, price_type))
            ).one()
        return remote_min_bar_time, remote_max_bar_time

    @staticmethod
    def _series_filter(instrument_id: int, interval: str, price_type: str) -> tuple[Any, ...]:
        return (
            MarketOHLCVBar.instrument_id == instrument_id,
            MarketOHLCVBar.interval == interval,
            MarketOHLCVBar.price_type == price_type,
        )

    def _pending_ranges(
        self,
        interval: str,
        remote_min_bar_time: datetime | None,
        remote_max_bar_time: datetime | None,
    ) -> list[tuple[str, list[Any], bool]]:
        """返回 (方向, 过滤条件, 是否降序)：远程为空推全量，否则推新于远程最新的与早于远程最早的。"""
        if remote_max_bar_time is None:
            return [("full", [], False)]
        overlap = interval_delta(interval) * max(1, self._overlap_bars)
        return [
            ("newer", [MarketOHLCVBar.bar_time >= remote_max_bar_time - overlap], False),
            ("older", [MarketOHLCVBar.bar_time < remote_min_bar_time], True),
        ]

    def _sync_bar_series(
        self,
        series: _BarSeries,
        progress: _BarSyncProgress,
        *,
        reuse_planned_watermarks: bool,
    ) -> None:
        if reuse_planned_watermarks:
            remote_min, remote_max = series.remote_min_bar_time, series.remote_max_bar_time
        else:
            remote_min, remote_max = self._read_remote_watermarks(
                series.target_instrument_id, series.interval, series.price_type
            )
        with self._source_session() as source, self._target_session() as target:
            for direction, conditions, descending in self._pending_ranges(
                series.interval, remote_min, remote_max
            ):
                self._push_bar_pages(
                    source,
                    target,
                    series,
                    conditions,
                    descending=descending,
                    direction=direction,
                    progress=progress,
                )

    def _push_bar_pages(
        self,
        source: Session,
        target: Session,
        series: _BarSeries,
        conditions: list[Any],
        *,
        descending: bool,
        direction: str,
        progress: _BarSyncProgress,
    ) -> None:
        """按 bar_time 分页读取本地 K 线并 upsert 到远程，每页单独提交。"""
        order = MarketOHLCVBar.bar_time.desc() if descending else MarketOHLCVBar.bar_time.asc()
        last_bar_time: datetime | None = None
        while True:
            page_stmt = select(MarketOHLCVBar).where(
                *self._series_filter(series.source_instrument_id, series.interval, series.price_type),
                *conditions,
            )
            if last_bar_time is not None:
                page_stmt = page_stmt.where(
                    MarketOHLCVBar.bar_time < last_bar_time
                    if descending
                    else MarketOHLCVBar.bar_time > last_bar_time
                )
            rows = source.scalars(page_stmt.order_by(order).limit(self._batch_size)).all()
            if not rows:
                return

            payloads = []
            for row in rows:
                payload = _model_row_dict(row)
                payload.pop("id", None)
                payload["instrument_id"] = series.target_instrument_id
                payload["updated_at"] = _utc_now()
                payloads.append(payload)

            self._upsert_bars(target, payloads)
            target.commit()
            progress.batch_committed(len(rows), direction)
            last_bar_time = rows[-1].bar_time

    def run_range_repair(
        self,
        *,
        symbol: str,
        interval: str,
        start_date: str,
        end_date: str,
        price_type: str = "mid",
    ) -> MarketReplicaResult:
        """把本地已修复的历史 K 线按时间段覆盖到远程，并删除远程多余脏数据。"""
        result = MarketReplicaResult(mode="repair")
        try:
            upserted, deleted = self.sync_bars_range_repair(
                symbol=symbol,
                interval=interval,
                start_date=start_date,
                end_date=end_date,
                price_type=price_type,
            )
            result.bars_upserted = upserted
            result.bars_deleted = deleted
        except Exception as exc:
            logger.exception("Market data range repair replica failed")
            result.errors.append(f"{type(exc).__name__}: {exc}")
        finally:
            result.finished_at = _utc_now()
        return result

    def sync_bars_range_repair(
        self,
        *,
        symbol: str,
        interval: str,
        start_date: str,
        end_date: str,
        price_type: str = "mid",
    ) -> tuple[int, int]:
        """按品种/周期/时间段把本地 K 线 upsert 到远程，并以本地为源删除远程多余 K 线。"""
        normalized_interval = normalize_sync_interval(interval)
        if normalized_interval is None:
            raise ValueError(f"Unsupported interval: {interval}")

        start_at = parse_request_payload_datetime(start_date)
        end_at = parse_request_payload_datetime(end_date)
        if start_at is None or end_at is None:
            raise ValueError("start_date and end_date are required ISO datetimes")
        if end_at < start_at:
            start_at, end_at = end_at, start_at

        canonical_symbol = market_master_service._resolve_fxcm_symbol(symbol)
        lookup = normalize_symbol(canonical_symbol)
        normalized_price_type = (price_type or "mid").strip().lower() or "mid"

        with self._source_session() as source, self._target_session() as target:
            # 所有范围修复（含手动）先在目标库串行化，再读取本地快照。
            # 防止并发处理重叠范围时，较旧快照最后提交覆盖较新数据。
            target.execute(text("SELECT pg_advisory_xact_lock(724615, 1)"))
            source_instrument = source.scalars(
                select(MarketInstrument).where(
                    MarketInstrument.provider == PROVIDER,
                    or_(
                        MarketInstrument.normalized_symbol == lookup,
                        MarketInstrument.normalized_provider_symbol == lookup,
                    ),
                )
            ).first()
            if source_instrument is None:
                raise ValueError(f"Instrument not found in source: {symbol}")

            target_instrument_id = target.scalar(
                select(MarketInstrument.id).where(
                    MarketInstrument.provider == PROVIDER,
                    MarketInstrument.normalized_symbol
                    == source_instrument.normalized_symbol,
                )
            )
            if target_instrument_id is None:
                raise ValueError(
                    f"Instrument not found in target: {source_instrument.symbol}"
                )

            local_rows = source.scalars(
                select(MarketOHLCVBar)
                .where(
                    MarketOHLCVBar.instrument_id == source_instrument.id,
                    MarketOHLCVBar.interval == normalized_interval,
                    MarketOHLCVBar.price_type == normalized_price_type,
                    MarketOHLCVBar.provider == PROVIDER,
                    MarketOHLCVBar.bar_time >= start_at,
                    MarketOHLCVBar.bar_time <= end_at,
                )
                .order_by(MarketOHLCVBar.bar_time.asc())
            ).all()
            if not local_rows:
                raise ValueError(
                    "Local range has no bars; refuse to wipe remote data"
                )

            upserted = 0
            local_times = {_as_utc(row.bar_time) for row in local_rows}
            for offset in range(0, len(local_rows), self._batch_size):
                chunk = local_rows[offset : offset + self._batch_size]
                payloads = []
                for row in chunk:
                    payload = _model_row_dict(row)
                    payload.pop("id", None)
                    payload["instrument_id"] = int(target_instrument_id)
                    payload["updated_at"] = _utc_now()
                    payloads.append(payload)
                upserted += self._upsert_bars(target, payloads)

            remote_times = {
                _as_utc(bar_time)
                for bar_time in target.scalars(
                    select(MarketOHLCVBar.bar_time).where(
                        MarketOHLCVBar.instrument_id == int(target_instrument_id),
                        MarketOHLCVBar.interval == normalized_interval,
                        MarketOHLCVBar.price_type == normalized_price_type,
                        MarketOHLCVBar.provider == PROVIDER,
                        MarketOHLCVBar.bar_time >= start_at,
                        MarketOHLCVBar.bar_time <= end_at,
                    )
                ).all()
            }
            stale_times = [bar_time for bar_time in remote_times if bar_time not in local_times]
            deleted = 0
            for offset in range(0, len(stale_times), self._batch_size):
                chunk = stale_times[offset : offset + self._batch_size]
                result = target.execute(
                    delete(MarketOHLCVBar).where(
                        MarketOHLCVBar.instrument_id == int(target_instrument_id),
                        MarketOHLCVBar.interval == normalized_interval,
                        MarketOHLCVBar.price_type == normalized_price_type,
                        MarketOHLCVBar.provider == PROVIDER,
                        MarketOHLCVBar.bar_time.in_(chunk),
                    )
                )
                deleted += int(result.rowcount or 0)

            target.commit()
            logger.info(
                "Range repair replica finished",
                extra={
                    "symbol": source_instrument.symbol,
                    "interval": normalized_interval,
                    "start_at": start_at.isoformat(),
                    "end_at": end_at.isoformat(),
                    "upserted": upserted,
                    "deleted": deleted,
                },
            )
            return upserted, deleted

    def _source_session(self) -> Session:
        return self._source_session_factory()

    def _target_session(self) -> Session:
        return self._target_session_factory()

    def _collect_counts(self, session: Session) -> dict[str, Any]:
        instrument_count = session.scalar(
            select(func.count())
            .select_from(MarketInstrument)
            .where(MarketInstrument.provider == PROVIDER)
        )
        alias_count = session.scalar(
            select(func.count()).select_from(MarketInstrumentAlias)
        )
        bar_count = session.scalar(
            select(func.count())
            .select_from(MarketOHLCVBar)
            .where(MarketOHLCVBar.provider == PROVIDER)
        )
        latest_bar_time = session.scalar(
            select(func.max(MarketOHLCVBar.bar_time)).where(
                MarketOHLCVBar.provider == PROVIDER
            )
        )
        return {
            "instrument_count": int(instrument_count or 0),
            "alias_count": int(alias_count or 0),
            "bar_count": int(bar_count or 0),
            "latest_bar_time": latest_bar_time,
        }

    def _target_market_tables_empty(self, session: Session) -> bool:
        for model, filter_provider in (
            (MarketInstrument, True),
            (MarketInstrumentAlias, False),
            (MarketOHLCVBar, True),
        ):
            stmt = select(func.count()).select_from(model)
            if filter_provider and hasattr(model, "provider"):
                stmt = stmt.where(model.provider == PROVIDER)  # type: ignore[attr-defined]
            count = session.scalar(stmt)
            if int(count or 0) > 0:
                return False
        return True

    def _bootstrap_copy_table(
        self,
        model: (
            type[MarketInstrument] | type[MarketInstrumentAlias] | type[MarketOHLCVBar]
        ),
        *,
        provider_filter: bool,
    ) -> int:
        total = 0
        last_id = 0
        while True:
            with self._source_session() as source, self._target_session() as target:
                stmt = select(model).where(model.id > last_id).order_by(model.id.asc())  # type: ignore[attr-defined]
                if provider_filter and hasattr(model, "provider"):
                    stmt = stmt.where(model.provider == PROVIDER)  # type: ignore[attr-defined]
                rows = source.scalars(stmt.limit(self._batch_size)).all()
                if not rows:
                    break

                payloads = [_model_row_dict(row) for row in rows]
                target.execute(insert(model).values(payloads))
                target.commit()
                total += len(rows)
                last_id = int(rows[-1].id)  # type: ignore[attr-defined]
                logger.info(
                    "Bootstrap copied %s rows (total=%s, last_id=%s)",
                    len(rows),
                    total,
                    last_id,
                    extra={"table": model.__tablename__},
                )
        return total

    def _truncate_target_market_tables(self, session: Session) -> None:
        session.execute(
            text(
                "TRUNCATE TABLE market_ohlcv_bar, market_instrument_alias, "
                "market_instrument RESTART IDENTITY CASCADE"
            )
        )
        session.commit()

    def _reset_target_sequences(self) -> None:
        with self._target_engine.begin() as conn:
            for table in _REPLICA_TABLES:
                conn.execute(
                    text(
                        f"""
                        SELECT setval(
                            pg_get_serial_sequence('{table}', 'id'),
                            COALESCE((SELECT MAX(id) FROM {table}), 1),
                            true
                        )
                        """
                    )
                )

    def _build_instrument_id_map(
        self, source: Session, target: Session
    ) -> dict[int, int]:
        source_rows = source.execute(
            select(
                MarketInstrument.id,
                MarketInstrument.normalized_symbol,
            ).where(MarketInstrument.provider == PROVIDER)
        ).all()
        target_rows = target.execute(
            select(
                MarketInstrument.id,
                MarketInstrument.normalized_symbol,
            ).where(MarketInstrument.provider == PROVIDER)
        ).all()
        target_by_norm = {norm: int(row_id) for row_id, norm in target_rows}
        return {
            int(source_id): target_by_norm[norm]
            for source_id, norm in source_rows
            if norm in target_by_norm
        }

    def _upsert_bars(self, session: Session, rows: Sequence[dict[str, Any]]) -> int:
        if not rows:
            return 0
        stmt = insert(MarketOHLCVBar).values(list(rows))
        stmt = stmt.on_conflict_do_update(
            constraint="uq_market_ohlcv_bar_instrument_interval_price_time",
            set_={column: stmt.excluded[column] for column in _BAR_UPSERT_COLUMNS},
        )
        session.execute(stmt)
        return len(rows)
