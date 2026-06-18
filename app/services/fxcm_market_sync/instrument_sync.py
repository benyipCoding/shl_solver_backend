import logging
from typing import Any, Mapping, Sequence

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.market_data import MarketInstrument, MarketInstrumentAlias
from app.services.fxcm_market_sync.constants import PROVIDER
from app.services.fxcm_market_sync.types import utc_now
from app.services.fxcm_market_sync.utils import (
    coalesce_str,
    normalize_symbol,
)
from app.services.fxcm_sidecar import FXCMSidecarError, fxcm_sidecar_service
from app.services.market_master import market_master_service


logger = logging.getLogger(__name__)


class InstrumentSyncHandler:
    """负责品种元数据与别名的拉取、构建与落库。"""

    async def get_hot_symbols(self, db: AsyncSession) -> list[str]:
        """读取数据库热池品种，并去重后返回规范化符号列表。"""
        stmt = select(MarketInstrument.symbol).where(
            MarketInstrument.provider == PROVIDER,
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

    async def sync_instruments(self, db: AsyncSession) -> int:
        """刷新热池品种元数据并同步别名映射。"""
        synced_count = 0
        hot_symbols = await self.get_hot_symbols(db)
        for symbol in hot_symbols:
            instrument_payload = await self.build_instrument_payload(
                symbol, hot_symbols=hot_symbols
            )
            if instrument_payload is None:
                continue

            instrument = await self.upsert_instrument(db, instrument_payload)
            await self.sync_aliases(
                db,
                instrument=instrument,
                aliases=instrument_payload.pop("aliases", []),
            )
            synced_count += 1
        return synced_count

    async def build_instrument_payload(
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
            search_item = self.pick_best_search_item(
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

        provider_symbol = coalesce_str(
            search_item.get("provider_symbol") if search_item else None,
            quote_payload.get("provider_symbol") if quote_payload else None,
            profile.get("provider_symbol") if profile else None,
            canonical_symbol,
        )
        symbol_value = coalesce_str(
            search_item.get("symbol") if search_item else None,
            quote_payload.get("symbol") if quote_payload else None,
            profile.get("symbol") if profile else None,
            canonical_symbol,
        )

        if provider_symbol is None or symbol_value is None:
            return None

        aliases = self.build_aliases(
            canonical_symbol=symbol_value,
            provider_symbol=provider_symbol,
            profile=profile,
            search_item=search_item,
        )

        market_value = coalesce_str(
            search_item.get("market") if search_item else None,
            profile.get("market") if profile else None,
        )
        asset_type = coalesce_str(
            search_item.get("asset_type") if search_item else None,
            profile.get("asset_type") if profile else None,
        )

        return {
            "provider": PROVIDER,
            "symbol": symbol_value,
            "normalized_symbol": normalize_symbol(symbol_value),
            "provider_symbol": provider_symbol,
            "normalized_provider_symbol": normalize_symbol(provider_symbol),
            "name": coalesce_str(
                search_item.get("name") if search_item else None,
                quote_payload.get("name") if quote_payload else None,
                profile.get("name") if profile else None,
                symbol_value,
            ),
            "display_label": coalesce_str(
                search_item.get("label") if search_item else None,
                profile.get("name") if profile else None,
            ),
            "exchange": coalesce_str(
                search_item.get("exchange") if search_item else None,
                quote_payload.get("exchange") if quote_payload else None,
                profile.get("exchange") if profile else None,
            ),
            "mic_code": coalesce_str(
                search_item.get("mic_code") if search_item else None,
                quote_payload.get("mic_code") if quote_payload else None,
            ),
            "market": market_value,
            "asset_type": asset_type,
            "country": coalesce_str(
                search_item.get("country") if search_item else None,
            ),
            "currency": coalesce_str(
                search_item.get("currency") if search_item else None,
                quote_payload.get("currency") if quote_payload else None,
                profile.get("currency") if profile else None,
            ),
            "exchange_timezone": coalesce_str(
                search_item.get("timezone") if search_item else None,
                profile.get("timezone") if profile else None,
            ),
            "provider_plan": coalesce_str(
                search_item.get("provider_plan") if search_item else None,
            ),
            "sort_weight": self.sort_weight(symbol_value, hot_symbols=hot_symbols),
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
            "metadata_synced_at": utc_now(),
            "last_seen_at": utc_now(),
            "aliases": aliases,
        }

    async def upsert_instrument(
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

    async def sync_aliases(
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

    def pick_best_search_item(
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
            normalize_symbol(canonical_symbol),
        }
        if profile is not None:
            for key in ("symbol", "provider_symbol"):
                value = coalesce_str(profile.get(key))
                if value:
                    target_symbols.add(normalize_symbol(value))

        for item in items:
            if not isinstance(item, Mapping):
                continue
            candidates = {
                normalize_symbol(coalesce_str(item.get("symbol")) or ""),
                normalize_symbol(coalesce_str(item.get("provider_symbol")) or ""),
            }
            if target_symbols & candidates:
                return item

        for item in items:
            if isinstance(item, Mapping):
                return item
        return None

    def build_aliases(
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
            label = coalesce_str(search_item.get("label"))
            if label:
                alias_specs.append((label, "SEARCH", 40))

        if profile is not None:
            for alias in profile.get("aliases", ()):
                alias_value = coalesce_str(alias)
                if alias_value:
                    alias_specs.append((alias_value, "MANUAL", 10))

        aliases: list[dict[str, Any]] = []
        seen: set[str] = set()
        for alias, alias_type, priority in alias_specs:
            normalized_alias = normalize_symbol(alias)
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

    def sort_weight(self, symbol: str, hot_symbols: list[str] | None = None) -> int:
        """根据热池顺序计算排序权重，越靠前权重越小。"""
        if not hot_symbols:
            return 999
        try:
            return hot_symbols.index(symbol)
        except ValueError:
            return 999


instrument_sync_handler = InstrumentSyncHandler()
