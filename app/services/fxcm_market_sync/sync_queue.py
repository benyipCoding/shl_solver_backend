import logging
from typing import Any

from sqlalchemy import delete, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.market_data import MarketInstrument, MarketSyncQueueItem
from app.services.fxcm_market_sync.constants import PROVIDER
from app.services.fxcm_market_sync.scheduling_policy import (
    infer_asset_category,
    infer_category_key,
)
from app.services.fxcm_market_sync.utils import normalize_symbol
from app.services.fxcm_sidecar import FXCMSidecarError, fxcm_sidecar_service


logger = logging.getLogger(__name__)


class SyncQueueHandler:
    """负责历史数据采集队列的读写，以及候选品种检索。"""

    async def list_queued_instruments(
        self,
        db: AsyncSession,
    ) -> list[tuple[MarketSyncQueueItem, MarketInstrument]]:
        """按加入顺序返回队列项及其品种。"""
        stmt = (
            select(MarketSyncQueueItem, MarketInstrument)
            .join(
                MarketInstrument,
                MarketInstrument.id == MarketSyncQueueItem.instrument_id,
            )
            .where(MarketInstrument.provider == PROVIDER)
            .order_by(MarketSyncQueueItem.created_at.asc(), MarketSyncQueueItem.id.asc())
        )
        return [(item, instrument) for item, instrument in (await db.execute(stmt)).all()]

    async def get_queued_symbols(self, db: AsyncSession) -> list[str]:
        """返回队列内启用品种的符号列表。"""
        return [
            instrument.symbol
            for _, instrument in await self.list_queued_instruments(db)
            if instrument.is_active
        ]

    async def get_queued_instrument_ids(self, db: AsyncSession) -> set[int]:
        stmt = select(MarketSyncQueueItem.instrument_id)
        return set((await db.execute(stmt)).scalars().all())

    async def add_instrument(
        self,
        db: AsyncSession,
        instrument: MarketInstrument,
    ) -> bool:
        """把品种加入队列；已在队列中返回 False。"""
        stmt = select(MarketSyncQueueItem).where(
            MarketSyncQueueItem.instrument_id == instrument.id
        )
        if (await db.execute(stmt)).scalars().first() is not None:
            return False
        db.add(MarketSyncQueueItem(instrument_id=instrument.id))
        await db.flush()
        return True

    async def remove_instruments(
        self,
        db: AsyncSession,
        instrument_ids: list[int],
    ) -> int:
        """把品种移出队列，保留其同步状态与已落库 K 线。"""
        if not instrument_ids:
            return 0
        result = await db.execute(
            delete(MarketSyncQueueItem).where(
                MarketSyncQueueItem.instrument_id.in_(instrument_ids)
            )
        )
        return int(result.rowcount or 0)

    async def list_candidates(
        self,
        db: AsyncSession,
        *,
        keyword: str | None,
        limit: int,
    ) -> dict[str, Any]:
        """返回不在队列中的候选品种；带关键字时补充 FXCM 中尚未落库的品种。"""
        queued_ids = await self.get_queued_instrument_ids(db)
        normalized_keyword = (keyword or "").strip()

        stmt = (
            select(MarketInstrument)
            .where(
                MarketInstrument.provider == PROVIDER,
                MarketInstrument.is_active.is_(True),
            )
            .order_by(MarketInstrument.symbol.asc())
        )
        if queued_ids:
            stmt = stmt.where(MarketInstrument.id.not_in(queued_ids))
        if normalized_keyword:
            pattern = f"%{normalized_keyword}%"
            stmt = stmt.where(
                or_(
                    MarketInstrument.symbol.ilike(pattern),
                    MarketInstrument.provider_symbol.ilike(pattern),
                    MarketInstrument.name.ilike(pattern),
                )
            )
        instruments = (await db.execute(stmt.limit(limit))).scalars().all()

        items = [
            {
                "instrument_id": instrument.id,
                "symbol": instrument.symbol,
                "provider_symbol": instrument.provider_symbol,
                "name": instrument.name,
                "asset_type": instrument.asset_type,
                "asset_category": infer_asset_category(instrument),
                "source": "local",
            }
            for instrument in instruments
        ]

        remote_error: str | None = None
        if normalized_keyword:
            try:
                remote_items = await self._search_remote(db, normalized_keyword)
                items.extend(remote_items)
            except FXCMSidecarError as exc:
                logger.warning("FXCM candidate search failed", exc_info=True)
                remote_error = exc.message

        return {"items": items, "remote_error": remote_error}

    async def _search_remote(
        self,
        db: AsyncSession,
        keyword: str,
    ) -> list[dict[str, Any]]:
        """在 sidecar 搜索品种，只保留本地 market_instrument 中还没有的。"""
        payload = await fxcm_sidecar_service.search_symbols(
            keyword=keyword,
            outputsize=20,
        )
        raw_items = payload.get("items") if isinstance(payload, dict) else None
        if not isinstance(raw_items, list):
            return []

        lookups = {
            normalize_symbol(str(value))
            for item in raw_items
            if isinstance(item, dict)
            for value in (item.get("symbol"), item.get("provider_symbol"))
            if value
        }
        if not lookups:
            return []
        known_stmt = select(
            MarketInstrument.normalized_symbol,
            MarketInstrument.normalized_provider_symbol,
        ).where(
            MarketInstrument.provider == PROVIDER,
            or_(
                MarketInstrument.normalized_symbol.in_(lookups),
                MarketInstrument.normalized_provider_symbol.in_(lookups),
            ),
        )
        known: set[str] = set()
        for normalized_symbol, normalized_provider_symbol in (
            await db.execute(known_stmt)
        ).all():
            known.add(normalized_symbol)
            known.add(normalized_provider_symbol)

        items: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or "").strip()
            provider_symbol = str(item.get("provider_symbol") or symbol).strip()
            if not symbol:
                continue
            keys = {normalize_symbol(symbol), normalize_symbol(provider_symbol)}
            if keys & known or keys & seen:
                continue
            seen.update(keys)
            items.append(
                {
                    "instrument_id": None,
                    "symbol": symbol,
                    "provider_symbol": provider_symbol,
                    "name": item.get("name"),
                    "asset_type": item.get("asset_type"),
                    "asset_category": infer_category_key(
                        item.get("asset_type"), symbol
                    ),
                    "source": "fxcm",
                }
            )
        return items


sync_queue_handler = SyncQueueHandler()
