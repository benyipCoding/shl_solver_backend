from datetime import timedelta
from typing import Any, Mapping, Sequence

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.market_data import MarketBarSyncState, MarketInstrument, MarketOHLCVBar
from app.services.fxcm_market_sync.constants import ALWAYS_OPEN_ASSET_TYPES, PROVIDER
from app.services.fxcm_market_sync.intervals import (
    calculate_next_sync_from,
    incremental_outputsize,
    interval_delta,
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

        inserted_count = 0

        forward_payload = {
            "symbol": instrument.provider_symbol,
            "interval": state.interval,
            "outputsize": incremental_outputsize(state.interval),
            "start_date": None,
            "end_date": None,
            "price_type": state.price_type,
        }

        if state.latest_synced_bar_time is not None:
            overlap = interval_delta(state.interval) * max(
                1, settings.fxcm_sync_incremental_overlap_bars
            )
            incremental_start = state.latest_synced_bar_time - overlap
            forward_payload["start_date"] = incremental_start.isoformat()

        payload = await fxcm_sidecar_service.get_history(**forward_payload)
        values = payload.get("values") if isinstance(payload, Mapping) else []
        rows = self.build_bar_rows(
            instrument=instrument,
            state=state,
            values=values if isinstance(values, list) else [],
            meta=payload.get("meta") if isinstance(payload, Mapping) else None,
        )

        if rows:
            inserted = await self.upsert_bars(db, rows)
            inserted_count += inserted
            bar_times = [row["bar_time"] for row in rows]

            state.earliest_synced_bar_time = min_datetime(
                state.earliest_synced_bar_time, min(bar_times)
            )
            state.latest_synced_bar_time = max_datetime(
                state.latest_synced_bar_time, max(bar_times)
            )

            state.last_requested_start_at = parse_request_payload_datetime(
                forward_payload.get("start_date")
            )
            state.last_requested_end_at = parse_request_payload_datetime(
                forward_payload.get("end_date")
            )

        if state.earliest_synced_bar_time is not None and not state.backfill_completed:
            batch_size = 2000
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

                if len(b_rows) < batch_size * 0.05:
                    state.backfill_completed = True
            else:
                state.backfill_completed = True

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
        return inserted_count

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
