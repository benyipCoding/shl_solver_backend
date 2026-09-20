from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Any, Mapping, Sequence

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.market_data import MarketBarSyncState, MarketInstrument, MarketOHLCVBar
from app.services.fxcm_market_sync.constants import (
    ALWAYS_OPEN_ASSET_TYPES,
    BACKFILL_EARLIEST_DATE,
    PRIORITY_FORWARD_MAX_ROUNDS,
    PRIORITY_REPAIR_MAX_ROUNDS,
    PROVIDER,
    REPAIR_DB_CHUNK_SIZE,
)
from app.services.fxcm_market_sync.intervals import (
    calculate_next_sync_from,
    catchup_outputsize,
    incremental_outputsize,
    interval_delta,
    repair_outputsize,
)
from app.services.fxcm_market_sync.scheduling_policy import normalize_asset_type
from app.services.fxcm_market_sync.types import utc_now
from app.services.fxcm_market_sync.utils import (
    coalesce_str,
    max_datetime,
    min_datetime,
    parse_bar_time,
    parse_request_payload_datetime,
    to_decimal,
    to_int,
)
from app.services.fxcm_sidecar import fxcm_sidecar_service


class BarSyncHandler:
    """负责 K 线数据的拉取、转换与落库，以及单状态同步状态机。"""

    async def sync_single_state(
        self,
        db: AsyncSession,
        state: MarketBarSyncState,
        instrument: MarketInstrument,
    ) -> int:
        """执行单个状态的增量拉取与历史回补，并更新状态机字段。"""
        state.last_attempt_at = utc_now()
        state.last_status = "RUNNING"
        state.last_error = None

        await self._hydrate_state_from_storage(db, state)

        inserted_count = await self._pull_forward_bars(
            db,
            state,
            instrument,
            outputsize=incremental_outputsize(state.interval),
            start_date=(
                (
                    state.latest_synced_bar_time
                    - interval_delta(state.interval)
                    * max(1, settings.fxcm_sync_incremental_overlap_bars)
                ).isoformat()
                if state.latest_synced_bar_time is not None
                else None
            ),
            end_date=None,
        )

        if (
            state.earliest_synced_bar_time is not None
            and not state.backfill_completed
            and state.earliest_synced_bar_time > BACKFILL_EARLIEST_DATE
        ):
            batch_size = 2000
            backfill_payload = {
                "symbol": instrument.provider_symbol,
                "interval": state.interval,
                "outputsize": batch_size,
                # 不请求 1994 年之前的历史，避免无意义采集。
                "start_date": BACKFILL_EARLIEST_DATE.isoformat(),
                "end_date": state.earliest_synced_bar_time.isoformat(),
                "price_type": state.price_type,
            }

            b_payload = await fxcm_sidecar_service.get_history(**backfill_payload)
            b_values = b_payload.get("values") if isinstance(b_payload, Mapping) else []
            b_rows = self.build_bar_rows(
                instrument=instrument,
                state=state,
                values=b_values if isinstance(b_values, list) else [],
                meta=b_payload.get("meta") if isinstance(b_payload, Mapping) else None,
            )

            if b_rows:
                inserted = await self.upsert_bars(db, b_rows)
                inserted_count += inserted
                b_bar_times = [row["bar_time"] for row in b_rows]
                state.earliest_synced_bar_time = min_datetime(
                    state.earliest_synced_bar_time, min(b_bar_times)
                )

                if (
                    state.earliest_synced_bar_time <= BACKFILL_EARLIEST_DATE
                    or len(b_rows) < batch_size * 0.05
                ):
                    state.backfill_completed = True
            else:
                state.backfill_completed = True
        elif (
            state.earliest_synced_bar_time is not None
            and state.earliest_synced_bar_time <= BACKFILL_EARLIEST_DATE
        ):
            state.backfill_completed = True

        self._finalize_state_success(state, instrument)
        return inserted_count

    async def sync_forward_only(
        self,
        db: AsyncSession,
        state: MarketBarSyncState,
        instrument: MarketInstrument,
    ) -> int:
        """只把本地最新 K 线追赶到当前时间，不回补更早的历史缺口。"""
        state.last_attempt_at = utc_now()
        state.last_status = "RUNNING"
        state.last_error = None

        await self._hydrate_state_from_storage(db, state)

        inserted_count = 0
        outputsize = catchup_outputsize(state.interval)
        delta = interval_delta(state.interval)
        overlap = delta * max(1, settings.fxcm_sync_incremental_overlap_bars)

        if state.latest_synced_bar_time is None:
            inserted_count += await self._pull_forward_bars(
                db,
                state,
                instrument,
                outputsize=outputsize,
                start_date=None,
                end_date=None,
            )
        else:
            now = utc_now()
            for _ in range(PRIORITY_FORWARD_MAX_ROUNDS):
                latest = state.latest_synced_bar_time
                if latest is None:
                    break
                if latest >= now - delta:
                    break

                window_start = latest - overlap
                window_end = min(now, window_start + delta * outputsize)
                if window_end <= window_start:
                    break

                previous_latest = latest
                inserted = await self._pull_forward_bars(
                    db,
                    state,
                    instrument,
                    outputsize=outputsize,
                    start_date=window_start.isoformat(),
                    end_date=window_end.isoformat(),
                )
                inserted_count += inserted

                if (
                    state.latest_synced_bar_time is None
                    or state.latest_synced_bar_time <= previous_latest
                ):
                    break

                now = utc_now()

        self._finalize_state_success(state, instrument)
        return inserted_count

    async def sync_range_repair(
        self,
        db: AsyncSession,
        state: MarketBarSyncState,
        instrument: MarketInstrument,
        *,
        start_at: datetime,
        end_at: datetime,
    ) -> dict[str, Any]:
        """按时间段重新采集 FXCM K 线，差异以新数据为准（含补缺与删除多余）。"""
        start_at = self._as_utc(start_at)
        end_at = self._as_utc(end_at)
        state.last_attempt_at = utc_now()
        state.last_status = "RUNNING"
        state.last_error = None

        fetched_rows = await self._fetch_range_bars(
            state,
            instrument,
            start_at=start_at,
            end_at=end_at,
        )
        fetched_by_time = {
            self._as_utc(row["bar_time"]): row for row in fetched_rows
        }
        if not fetched_by_time:
            state.last_status = "FAILED"
            state.last_error = "FXCM returned no bars for the selected range"
            state.last_requested_start_at = start_at
            state.last_requested_end_at = end_at
            state.updated_at = utc_now()
            return {
                "ok": False,
                "fetched_bars": 0,
                "local_bars": 0,
                "rows_inserted": 0,
                "rows_updated": 0,
                "rows_deleted": 0,
                "rows_upserted": 0,
                "unchanged_bars": 0,
            }

        local_bars = await self._load_local_bars(
            db,
            state,
            start_at=start_at,
            end_at=end_at,
        )
        local_by_time = {self._as_utc(bar.bar_time): bar for bar in local_bars}

        to_upsert: list[dict[str, Any]] = []
        inserted_count = 0
        updated_count = 0
        unchanged_count = 0
        for bar_time, row in fetched_by_time.items():
            existing = local_by_time.get(bar_time)
            if existing is None:
                to_upsert.append(row)
                inserted_count += 1
                continue
            if self._bars_equivalent(existing, row):
                unchanged_count += 1
                continue
            to_upsert.append(row)
            updated_count += 1

        stale_times = [
            bar_time
            for bar_time in local_by_time
            if bar_time not in fetched_by_time
        ]

        upserted = 0
        for offset in range(0, len(to_upsert), REPAIR_DB_CHUNK_SIZE):
            upserted += await self.upsert_bars(
                db, to_upsert[offset : offset + REPAIR_DB_CHUNK_SIZE]
            )

        deleted_count = await self._delete_bars_at_times(
            db,
            state,
            bar_times=stale_times,
        )

        await self._hydrate_state_from_storage(db, state)
        state.last_success_at = utc_now()
        state.last_status = "SUCCESS"
        state.retry_count = 0
        state.last_error = None
        state.last_requested_start_at = start_at
        state.last_requested_end_at = end_at
        state.updated_at = utc_now()

        return {
            "ok": True,
            "fetched_bars": len(fetched_by_time),
            "local_bars": len(local_by_time),
            "rows_inserted": inserted_count,
            "rows_updated": updated_count,
            "rows_deleted": deleted_count,
            "rows_upserted": upserted,
            "unchanged_bars": unchanged_count,
        }

    async def _fetch_range_bars(
        self,
        state: MarketBarSyncState,
        instrument: MarketInstrument,
        *,
        start_at: datetime,
        end_at: datetime,
    ) -> list[dict[str, Any]]:
        """按窗口分页拉取 [start_at, end_at] 的 FXCM K 线。"""
        outputsize = repair_outputsize(state.interval)
        delta = interval_delta(state.interval)
        cursor = start_at
        collected: dict[datetime, dict[str, Any]] = {}
        previous_latest: datetime | None = None

        for _ in range(PRIORITY_REPAIR_MAX_ROUNDS):
            if cursor > end_at:
                break

            window_end = min(end_at, cursor + delta * outputsize)
            if window_end <= cursor:
                window_end = end_at

            rows = await self._request_history_rows(
                state,
                instrument,
                outputsize=outputsize,
                start_date=cursor.isoformat(),
                end_date=window_end.isoformat(),
            )
            in_range = [
                row
                for row in rows
                if start_at <= self._as_utc(row["bar_time"]) <= end_at
            ]
            for row in in_range:
                collected[self._as_utc(row["bar_time"])] = row

            if not in_range:
                if window_end >= end_at:
                    break
                cursor = window_end + delta
                continue

            latest = max(self._as_utc(row["bar_time"]) for row in in_range)
            if previous_latest is not None and latest <= previous_latest:
                if window_end >= end_at:
                    break
                cursor = window_end + delta
                continue

            previous_latest = latest
            if latest >= end_at or window_end >= end_at:
                break
            if latest < window_end:
                cursor = window_end + delta
            else:
                cursor = latest + delta

        return list(collected.values())

    async def _request_history_rows(
        self,
        state: MarketBarSyncState,
        instrument: MarketInstrument,
        *,
        outputsize: int,
        start_date: str | None,
        end_date: str | None,
    ) -> list[dict[str, Any]]:
        """请求 sidecar 历史 K 线并转换为待落库行，不更新同步水位。"""
        payload = await fxcm_sidecar_service.get_history(
            symbol=instrument.provider_symbol,
            interval=state.interval,
            outputsize=outputsize,
            start_date=start_date,
            end_date=end_date,
            price_type=state.price_type,
        )
        if (
            isinstance(payload, Mapping)
            and "values" not in payload
            and isinstance(payload.get("data"), Mapping)
        ):
            payload = payload["data"]
        values = payload.get("values") if isinstance(payload, Mapping) else []
        return self.build_bar_rows(
            instrument=instrument,
            state=state,
            values=values if isinstance(values, list) else [],
            meta=payload.get("meta") if isinstance(payload, Mapping) else None,
        )

    async def _load_local_bars(
        self,
        db: AsyncSession,
        state: MarketBarSyncState,
        *,
        start_at: datetime,
        end_at: datetime,
    ) -> list[MarketOHLCVBar]:
        """读取本地指定时间段内的已有 K 线。"""
        stmt = (
            select(MarketOHLCVBar)
            .where(
                MarketOHLCVBar.instrument_id == state.instrument_id,
                MarketOHLCVBar.interval == state.interval,
                MarketOHLCVBar.price_type == state.price_type,
                MarketOHLCVBar.provider == PROVIDER,
                MarketOHLCVBar.bar_time >= start_at,
                MarketOHLCVBar.bar_time <= end_at,
            )
            .order_by(MarketOHLCVBar.bar_time.asc())
        )
        return list((await db.execute(stmt)).scalars().all())

    async def _delete_bars_at_times(
        self,
        db: AsyncSession,
        state: MarketBarSyncState,
        *,
        bar_times: Sequence[datetime],
    ) -> int:
        """删除指定时间戳上的本地 K 线。"""
        if not bar_times:
            return 0

        deleted = 0
        unique_times = list(dict.fromkeys(bar_times))
        for offset in range(0, len(unique_times), REPAIR_DB_CHUNK_SIZE):
            chunk = unique_times[offset : offset + REPAIR_DB_CHUNK_SIZE]
            result = await db.execute(
                delete(MarketOHLCVBar).where(
                    MarketOHLCVBar.instrument_id == state.instrument_id,
                    MarketOHLCVBar.interval == state.interval,
                    MarketOHLCVBar.price_type == state.price_type,
                    MarketOHLCVBar.provider == PROVIDER,
                    MarketOHLCVBar.bar_time.in_(chunk),
                )
            )
            deleted += int(result.rowcount or 0)
        return deleted

    @staticmethod
    def _as_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _bars_equivalent(existing: MarketOHLCVBar, row: Mapping[str, Any]) -> bool:
        """判断本地 K 线与新采集数据是否一致。"""
        return (
            BarSyncHandler._decimals_equal(existing.open, row.get("open"))
            and BarSyncHandler._decimals_equal(existing.high, row.get("high"))
            and BarSyncHandler._decimals_equal(existing.low, row.get("low"))
            and BarSyncHandler._decimals_equal(existing.close, row.get("close"))
            and (existing.volume or 0) == (row.get("volume") or 0)
        )

    @staticmethod
    def _decimals_equal(left: Any, right: Any) -> bool:
        if left is None and right is None:
            return True
        if left is None or right is None:
            return False
        try:
            return Decimal(str(left)) == Decimal(str(right))
        except Exception:
            return False

    async def _pull_forward_bars(
        self,
        db: AsyncSession,
        state: MarketBarSyncState,
        instrument: MarketInstrument,
        *,
        outputsize: int,
        start_date: str | None,
        end_date: str | None,
    ) -> int:
        """按给定窗口向前拉取并落库 K 线，更新同步水位。"""
        forward_payload = {
            "symbol": instrument.provider_symbol,
            "interval": state.interval,
            "outputsize": outputsize,
            "start_date": start_date,
            "end_date": end_date,
            "price_type": state.price_type,
        }
        payload = await fxcm_sidecar_service.get_history(**forward_payload)
        values = payload.get("values") if isinstance(payload, Mapping) else []
        rows = self.build_bar_rows(
            instrument=instrument,
            state=state,
            values=values if isinstance(values, list) else [],
            meta=payload.get("meta") if isinstance(payload, Mapping) else None,
        )
        if not rows:
            state.last_requested_start_at = parse_request_payload_datetime(start_date)
            state.last_requested_end_at = parse_request_payload_datetime(end_date)
            return 0

        inserted = await self.upsert_bars(db, rows)
        bar_times = [row["bar_time"] for row in rows]
        state.earliest_synced_bar_time = min_datetime(
            state.earliest_synced_bar_time, min(bar_times)
        )
        state.latest_synced_bar_time = max_datetime(
            state.latest_synced_bar_time, max(bar_times)
        )
        state.last_requested_start_at = parse_request_payload_datetime(start_date)
        state.last_requested_end_at = parse_request_payload_datetime(end_date)
        return inserted

    def _finalize_state_success(
        self,
        state: MarketBarSyncState,
        instrument: MarketInstrument,
    ) -> None:
        """将状态机标为成功，并按是否已完成历史回补设置下次调度时间。"""
        state.last_success_at = utc_now()
        state.last_status = "SUCCESS"
        state.retry_count = 0
        state.sync_mode = "INCREMENTAL" if state.backfill_completed else "BACKFILL"

        if state.backfill_completed:
            state.next_sync_from = calculate_next_sync_from(
                interval=state.interval,
                now=utc_now(),
                skip_weekends=normalize_asset_type(instrument.asset_type)
                not in ALWAYS_OPEN_ASSET_TYPES,
            )
        else:
            state.next_sync_from = utc_now() + timedelta(seconds=120)

        state.updated_at = utc_now()

    async def _hydrate_state_from_storage(
        self,
        db: AsyncSession,
        state: MarketBarSyncState,
    ) -> None:
        """根据本地存量 K 线修正同步水位；1994 年前数据不再回补。"""
        base_filters = (
            MarketOHLCVBar.instrument_id == state.instrument_id,
            MarketOHLCVBar.interval == state.interval,
            MarketOHLCVBar.price_type == state.price_type,
            MarketOHLCVBar.provider == PROVIDER,
        )

        # 同步水位只看 1994 及之后；更早的历史视为无效。
        min_relevant_bar_time = await db.scalar(
            select(func.min(MarketOHLCVBar.bar_time)).where(
                *base_filters,
                MarketOHLCVBar.bar_time >= BACKFILL_EARLIEST_DATE,
            )
        )
        max_bar_time = await db.scalar(
            select(func.max(MarketOHLCVBar.bar_time)).where(*base_filters)
        )
        # 若库里已有 1994 之前的数据，说明历史上已回补越过下限。
        has_pre_floor_bar = await db.scalar(
            select(func.min(MarketOHLCVBar.bar_time)).where(
                *base_filters,
                MarketOHLCVBar.bar_time < BACKFILL_EARLIEST_DATE,
            )
        )

        if min_relevant_bar_time is not None:
            state.earliest_synced_bar_time = min_datetime(
                state.earliest_synced_bar_time, min_relevant_bar_time
            )
        if max_bar_time is not None:
            state.latest_synced_bar_time = max_datetime(
                state.latest_synced_bar_time, max_bar_time
            )

        if state.backfill_completed:
            return

        # 1994 之后已有数据，且已触及/越过下限 → 可转增量。
        if min_relevant_bar_time is not None and (
            min_relevant_bar_time <= BACKFILL_EARLIEST_DATE
            or has_pre_floor_bar is not None
        ):
            state.backfill_completed = True

    def build_bar_rows(
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
            source_interval = coalesce_str(meta.get("provider_interval"))

        rows: list[dict[str, Any]] = []
        for item in values:
            bar_time = parse_bar_time(item)
            close_price = to_decimal(item.get("close"))
            if bar_time is None or close_price is None:
                continue
            if bar_time < BACKFILL_EARLIEST_DATE:
                continue

            open_price = to_decimal(item.get("open")) or close_price
            high_price = to_decimal(item.get("high")) or close_price
            low_price = to_decimal(item.get("low")) or close_price

            rows.append(
                {
                    "instrument_id": instrument.id,
                    "provider": PROVIDER,
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
                    "volume": to_int(item.get("volume")),
                }
            )
        return rows

    async def upsert_bars(
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


bar_sync_handler = BarSyncHandler()
