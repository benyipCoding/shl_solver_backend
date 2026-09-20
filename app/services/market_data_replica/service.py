"""本地 market 数据推送到远程 PostgreSQL（CLI 脚本专用，不参与主后端 lifespan）。"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Sequence

from sqlalchemy import create_engine, delete, func, or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
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
from app.services.market_master import market_master_service


logger = logging.getLogger(__name__)

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
        self._target_engine = create_engine(target_database_url, future=True)
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
            }

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
        """按远程 max(bar_time) 水位推送本地新增/更新的 K 线。"""
        total = 0
        with self._source_session() as source, self._target_session() as target:
            id_map = self._build_instrument_id_map(source, target)
            if not id_map:
                target.commit()
                return 0

            groups = source.execute(
                select(
                    MarketOHLCVBar.instrument_id,
                    MarketOHLCVBar.interval,
                    MarketOHLCVBar.price_type,
                )
                .where(MarketOHLCVBar.provider == PROVIDER)
                .distinct()
                .order_by(
                    MarketOHLCVBar.instrument_id.asc(),
                    MarketOHLCVBar.interval.asc(),
                    MarketOHLCVBar.price_type.asc(),
                )
            ).all()

            for source_instrument_id, interval, price_type in groups:
                target_instrument_id = id_map.get(int(source_instrument_id))
                if target_instrument_id is None:
                    continue

                remote_max_bar_time = target.scalar(
                    select(func.max(MarketOHLCVBar.bar_time)).where(
                        MarketOHLCVBar.instrument_id == target_instrument_id,
                        MarketOHLCVBar.interval == interval,
                        MarketOHLCVBar.price_type == price_type,
                    )
                )

                stmt = (
                    select(MarketOHLCVBar)
                    .where(
                        MarketOHLCVBar.instrument_id == source_instrument_id,
                        MarketOHLCVBar.interval == interval,
                        MarketOHLCVBar.price_type == price_type,
                    )
                    .order_by(MarketOHLCVBar.bar_time.asc())
                )
                if remote_max_bar_time is not None:
                    overlap = interval_delta(interval) * max(1, self._overlap_bars)
                    watermark = remote_max_bar_time - overlap
                    stmt = stmt.where(MarketOHLCVBar.bar_time >= watermark)

                last_bar_time: datetime | None = None
                while True:
                    page_stmt = stmt
                    if last_bar_time is not None:
                        page_stmt = page_stmt.where(
                            MarketOHLCVBar.bar_time > last_bar_time
                        )
                    rows = source.scalars(page_stmt.limit(self._batch_size)).all()
                    if not rows:
                        break

                    payloads = []
                    for row in rows:
                        payload = _model_row_dict(row)
                        payload.pop("id", None)
                        payload["instrument_id"] = target_instrument_id
                        payload["updated_at"] = _utc_now()
                        payloads.append(payload)

                    total += self._upsert_bars(target, payloads)
                    last_bar_time = rows[-1].bar_time

            target.commit()
        return total

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
