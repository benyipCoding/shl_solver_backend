from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from threading import Lock
from time import monotonic
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.services.fxcm_sidecar import FXCMSidecarError, fxcm_sidecar_service


class TwelveDataAPIError(Exception):
    def __init__(
        self,
        status_code: int,
        message: str,
        payload: Any | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.payload = payload


@dataclass
class _TTLCacheEntry:
    expires_at: float
    value: Any


class _TTLCache:
    def __init__(self) -> None:
        self._entries: dict[tuple[Any, ...], _TTLCacheEntry] = {}
        self._lock = Lock()

    def get(self, key: tuple[Any, ...]) -> Any | None:
        now = monotonic()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.expires_at <= now:
                self._entries.pop(key, None)
                return None
            return deepcopy(entry.value)

    def set(self, key: tuple[Any, ...], value: Any, *, ttl_seconds: int) -> None:
        with self._lock:
            self._entries[key] = _TTLCacheEntry(
                expires_at=monotonic() + ttl_seconds,
                value=deepcopy(value),
            )


class MarketMasterService:
    IDENTIFIER_FIELDS = ("symbol", "figi", "isin", "cusip")
    MAX_WATCHLIST_SYMBOLS = 10
    DEFAULT_KLINE_OUTPUTSIZE = 120
    DEFAULT_TIME_SERIES_OUTPUTSIZE = 120
    DEFAULT_KLINE_TIMEZONE = "Exchange"
    DEFAULT_KLINE_ORDER = "desc"
    DEFAULT_KLINE_PREVIOUS_CLOSE = True
    DEFAULT_FILTER_NON_TRADING = False
    ALWAYS_OPEN_ASSET_TYPES = {"digital currency"}
    HOLIDAY_LIKE_STALE_MIN_CANDLES = 3
    MARKET_MOVER_DIRECTIONS = {"gainers", "losers"}
    FXCM_MARKET_MOVER_MARKETS = {
        "stocks",
        "etf",
        "mutual_funds",
        "forex",
        "crypto",
    }
    MARKET_MOVER_CANDIDATE_CAPS = {
        "stocks": 10,
        "etf": 0,
        "mutual_funds": 0,
        "forex": 8,
        "crypto": 8,
    }
    MARKET_MOVER_CACHE_TTL_SECONDS = 8
    SYMBOL_SEARCH_PROFILES = (
        {
            "symbol": "EUR/USD",
            "provider_symbol": "EUR/USD",
            "name": "EUR/USD",
            "exchange": "CCY",
            "market": "Forex",
            "asset_type": "Physical Currency",
            "currency": "USD",
            "timezone": "Europe/London",
            "aliases": ("EUR", "EUR/USD", "EURUSD"),
        },
        {
            "symbol": "GBP/USD",
            "provider_symbol": "GBP/USD",
            "name": "GBP/USD",
            "exchange": "CCY",
            "market": "Forex",
            "asset_type": "Physical Currency",
            "currency": "USD",
            "timezone": "Europe/London",
            "aliases": ("GBP", "GBP/USD", "GBPUSD"),
        },
        {
            "symbol": "BTC/USD",
            "provider_symbol": "BTC/USD",
            "name": "Bitcoin / US Dollar",
            "exchange": "CCC",
            "market": "Crypto",
            "asset_type": "Digital Currency",
            "currency": "USD",
            "timezone": "UTC",
            "aliases": ("BTC", "BTC/USD", "BTCUSD", "BITCOIN"),
        },
        {
            "symbol": "ETH/USD",
            "provider_symbol": "ETH/USD",
            "name": "Ethereum / US Dollar",
            "exchange": "CCC",
            "market": "Crypto",
            "asset_type": "Digital Currency",
            "currency": "USD",
            "timezone": "UTC",
            "aliases": ("ETH", "ETH/USD", "ETHUSD", "ETHEREUM"),
        },
        {
            "symbol": "XAU/USD",
            "provider_symbol": "XAU/USD",
            "name": "Gold / US Dollar",
            "exchange": "COMEX",
            "market": "Precious Metals",
            "asset_type": "Precious Metal",
            "currency": "USD",
            "timezone": "America/New_York",
            "aliases": ("XAU", "XAU/USD", "XAUUSD", "GOLD"),
        },
        {
            "symbol": "XAG/USD",
            "provider_symbol": "XAG/USD",
            "name": "Silver / US Dollar",
            "exchange": "COMEX",
            "market": "Precious Metals",
            "asset_type": "Precious Metal",
            "currency": "USD",
            "timezone": "America/New_York",
            "aliases": ("XAG", "XAG/USD", "XAGUSD", "SILVER"),
        },
        {
            "symbol": "USOil",
            "provider_symbol": "USOil",
            "name": "WTI Crude Oil",
            "exchange": "NYMEX",
            "market": "Commodities",
            "asset_type": "Commodity",
            "currency": "USD",
            "timezone": "America/New_York",
            "aliases": ("USOIL", "XTI", "XTI/USD", "XTIUSD", "WTI"),
        },
        {
            "symbol": "UKOil",
            "provider_symbol": "UKOil",
            "name": "Brent Crude Oil",
            "exchange": "ICE",
            "market": "Commodities",
            "asset_type": "Commodity",
            "currency": "USD",
            "timezone": "America/New_York",
            "aliases": ("UKOIL", "XBR", "XBR/USD", "XBRUSD", "BRENT"),
        },
        {
            "symbol": "USDOLLAR",
            "provider_symbol": "USDOLLAR",
            "name": "US Dollar Index",
            "exchange": "CFD",
            "market": "Indices",
            "asset_type": "Index",
            "currency": "USD",
            "timezone": "UTC",
            "aliases": ("USDOLLAR", "DXY", "US DOLLAR INDEX", "DOLLAR INDEX"),
        },
        {
            "symbol": "US30",
            "provider_symbol": "US30",
            "name": "Dow Jones Industrial Average",
            "exchange": "CFD",
            "market": "Indices",
            "asset_type": "Index",
            "currency": "USD",
            "timezone": "UTC",
            "aliases": ("US30", "DJI", "DOW", "DOW JONES"),
        },
        {
            "symbol": "NAS100",
            "provider_symbol": "NAS100",
            "name": "NASDAQ 100",
            "exchange": "CFD",
            "market": "Indices",
            "asset_type": "Index",
            "currency": "USD",
            "timezone": "UTC",
            "aliases": ("NAS100", "NDX", "NASDAQ 100", "NASDAQ"),
        },
        {
            "symbol": "SPX500",
            "provider_symbol": "SPX500",
            "name": "S&P 500",
            "exchange": "CFD",
            "market": "Indices",
            "asset_type": "Index",
            "currency": "USD",
            "timezone": "UTC",
            "aliases": ("SPX500", "SPX", "SP500", "S&P 500"),
        },
    )

    def __init__(self) -> None:
        self._market_movers_cache = _TTLCache()

    async def get_latest_price(self, params: Mapping[str, Any] | None = None) -> Any:
        query_params = self._build_query_params(params)
        symbol = self._extract_identifier(query_params)
        payload = await self._fetch_quote_payload(symbol, query_params)

        return {
            "symbol": payload.get("symbol"),
            "provider_symbol": payload.get("provider_symbol"),
            "price": payload.get("close"),
            "currency": payload.get("currency"),
            "datetime": payload.get("datetime"),
            "timestamp": payload.get("timestamp"),
            "exchange": payload.get("exchange"),
            "name": payload.get("name"),
        }

    async def get_quote(self, params: Mapping[str, Any] | None = None) -> Any:
        query_params = self._build_query_params(params)
        symbol = self._extract_identifier(query_params)
        return await self._fetch_quote_payload(symbol, query_params)

    async def get_time_series(
        self,
        params: Mapping[str, Any] | None = None,
        *,
        filter_non_trading: bool = False,
    ) -> Any:
        payload, _ = await self._get_time_series_payload(
            params,
            filter_non_trading=filter_non_trading,
        )
        return payload

    async def get_kline_defaults(
        self,
        *,
        symbol: str,
        interval: str = "1day",
        outputsize: int = DEFAULT_KLINE_OUTPUTSIZE,
        exchange: str | None = None,
        mic_code: str | None = None,
        country: str | None = None,
        asset_type: str | None = None,
        timezone: str = DEFAULT_KLINE_TIMEZONE,
        start_date: str | None = None,
        end_date: str | None = None,
        adjust: str | None = None,
        prepost: bool | None = None,
        dp: int | None = None,
        filter_non_trading: bool = DEFAULT_FILTER_NON_TRADING,
    ) -> dict[str, Any]:
        query_params = self._build_query_params(
            {
                "symbol": symbol,
                "interval": interval,
                "outputsize": outputsize,
                "exchange": exchange,
                "mic_code": mic_code,
                "country": country,
                "type": asset_type,
                "timezone": timezone,
                "start_date": start_date,
                "end_date": end_date,
                "adjust": adjust,
                "prepost": prepost,
                "dp": dp,
                "order": self.DEFAULT_KLINE_ORDER,
            }
        )
        payload, filter_info = await self._get_time_series_payload(
            query_params,
            filter_non_trading=filter_non_trading,
        )

        return self._normalize_kline(
            payload,
            requested_symbol=symbol,
            requested_interval=self._resolve_interval_name(interval),
            defaults_applied={
                "outputsize": self._to_int(query_params.get("outputsize"))
                or self.DEFAULT_KLINE_OUTPUTSIZE,
                "timezone": self._to_str(query_params.get("timezone"))
                or self.DEFAULT_KLINE_TIMEZONE,
                "order": self._to_str(query_params.get("order"))
                or self.DEFAULT_KLINE_ORDER,
                "previous_close": self.DEFAULT_KLINE_PREVIOUS_CLOSE,
                "filter_non_trading": filter_non_trading,
            },
            filter_info=filter_info,
        )

    async def search_symbols(self, params: Mapping[str, Any] | None = None) -> Any:
        query_params = self._build_query_params(params)
        keyword = self._to_str(query_params.get("symbol")) or self._to_str(
            query_params.get("keyword")
        )
        if keyword is None:
            raise TwelveDataAPIError(
                status_code=400,
                message="symbol is required",
                payload={"field": "symbol"},
            )

        outputsize = self._bounded_int(
            query_params.get("outputsize"),
            default=10,
            minimum=1,
            maximum=120,
        )
        items = await self._fetch_search_items(keyword, outputsize)

        return {
            "keyword": keyword,
            "count": len(items),
            "data": [self._build_symbol_search_record(item) for item in items],
        }

    async def search_unified(
        self,
        *,
        keyword: str,
        outputsize: int = 10,
        show_plan: bool = False,
    ) -> dict[str, Any]:
        normalized_keyword = (self._to_str(keyword) or "").strip()
        if not normalized_keyword:
            raise TwelveDataAPIError(
                status_code=400,
                message="keyword is required",
                payload={"field": "keyword"},
            )

        bounded_outputsize = self._bounded_int(
            outputsize,
            default=10,
            minimum=1,
            maximum=30,
        )
        items = await self._fetch_search_items(normalized_keyword, bounded_outputsize)
        normalized_items = [self._normalize_search_item(item) for item in items]

        if not show_plan:
            for item in normalized_items:
                item.pop("provider_plan", None)

        return {
            "keyword": normalized_keyword,
            "count": len(normalized_items),
            "items": normalized_items,
        }

    async def get_market_movers(
        self,
        market: str,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        query_params = self._build_query_params(params)
        market_key = market.casefold()
        direction = (
            self._to_str(query_params.get("direction")) or "gainers"
        ).casefold()

        if direction not in self.MARKET_MOVER_DIRECTIONS:
            raise TwelveDataAPIError(
                status_code=400,
                message="direction must be gainers or losers",
                payload={"field": "direction"},
            )

        outputsize = self._bounded_int(
            query_params.get("outputsize"),
            default=10,
            minimum=1,
            maximum=50,
        )
        country = self._to_str(query_params.get("country"))
        price_greater_than = self._to_float(query_params.get("price_greater_than"))
        cache_key = self._build_market_movers_cache_key(
            market=market_key,
            direction=direction,
            outputsize=outputsize,
            country=country,
            price_greater_than=price_greater_than,
        )
        cached = self._market_movers_cache.get(cache_key)
        if cached is not None:
            return cached

        if market_key in self.FXCM_MARKET_MOVER_MARKETS:
            items = await self._fetch_fxcm_market_movers(
                market=market_key,
                direction=direction,
                outputsize=outputsize,
                country=country,
                price_greater_than=price_greater_than,
            )
        else:
            raise TwelveDataAPIError(
                status_code=400,
                message="Unsupported market",
                payload={"market": market},
            )

        response = {
            "market": market_key,
            "direction": direction,
            "count": len(items),
            "items": items,
        }
        self._market_movers_cache.set(
            cache_key,
            response,
            ttl_seconds=self.MARKET_MOVER_CACHE_TTL_SECONDS,
        )
        return response

    async def get(
        self,
        path: str,
        params: Mapping[str, Any] | None = None,
        require_identifier: bool = False,
    ) -> Any:
        query_params = self._build_query_params(params)

        if require_identifier:
            self._ensure_identifier(query_params)

        normalized_path = path.rstrip("/")
        if normalized_path == "/price":
            return await self.get_latest_price(query_params)
        if normalized_path == "/quote":
            return await self.get_quote(query_params)
        if normalized_path == "/time_series":
            return await self.get_time_series(query_params)
        if normalized_path == "/symbol_search":
            return await self.search_symbols(query_params)
        if normalized_path.startswith("/market_movers/"):
            return await self.get_market_movers(
                normalized_path.rsplit("/", 1)[-1],
                query_params,
            )

        raise TwelveDataAPIError(
            status_code=400,
            message="Unsupported market master path",
            payload={"path": path},
        )

    async def get_watchlist_quotes(
        self,
        symbols: Sequence[str],
        *,
        interval: str | None = None,
        exchange: str | None = None,
        mic_code: str | None = None,
        country: str | None = None,
        asset_type: str | None = None,
        timezone: str | None = None,
        eod: bool | None = None,
        prepost: bool | None = None,
        dp: int | None = None,
    ) -> dict[str, Any]:
        normalized_symbols = self._normalize_symbols(symbols)

        if not normalized_symbols:
            raise TwelveDataAPIError(
                status_code=400,
                message="At least one symbol is required",
                payload={"field": "symbols"},
            )

        if len(normalized_symbols) > self.MAX_WATCHLIST_SYMBOLS:
            raise TwelveDataAPIError(
                status_code=400,
                message=(
                    f"Too many symbols requested. Maximum {self.MAX_WATCHLIST_SYMBOLS} symbols per request"
                ),
                payload={
                    "max_symbols": self.MAX_WATCHLIST_SYMBOLS,
                    "requested": len(normalized_symbols),
                },
            )

        base_params = {
            "interval": interval,
            "exchange": exchange,
            "mic_code": mic_code,
            "country": country,
            "type": asset_type,
            "timezone": timezone,
            "eod": eod,
            "prepost": prepost,
            "dp": dp,
        }

        requested_interval = self._resolve_interval_name(interval or "1day")
        resolved_symbols = [
            self._resolve_fxcm_symbol(symbol) for symbol in normalized_symbols
        ]
        original_by_requested: dict[str, str] = {}
        for original_symbol, requested_symbol in zip(
            normalized_symbols, resolved_symbols
        ):
            original_by_requested.setdefault(requested_symbol, original_symbol)

        try:
            payload = await fxcm_sidecar_service.get_quotes_batch(
                symbols=resolved_symbols,
                interval=requested_interval,
                price_type="mid",
            )
        except FXCMSidecarError as exc:
            raise self._to_twelve_error_from_fxcm(exc) from exc

        raw_items = payload.get("items") if isinstance(payload, Mapping) else []
        raw_errors = payload.get("errors") if isinstance(payload, Mapping) else []

        items = [
            self._normalize_quote(item)
            for item in raw_items
            if isinstance(item, Mapping)
        ]
        errors: list[dict[str, Any]] = []
        for error in raw_errors:
            if not isinstance(error, Mapping):
                continue
            requested_symbol = self._to_str(error.get("requested_symbol"))
            errors.append(
                {
                    "symbol": original_by_requested.get(
                        requested_symbol or "",
                        requested_symbol,
                    ),
                    "code": self._to_int(error.get("code")) or 502,
                    "message": self._to_str(error.get("message"))
                    or "FXCM sidecar request failed",
                    "data": error.get("data"),
                }
            )

        return {
            "requested_symbols": normalized_symbols,
            "count": len(normalized_symbols),
            "succeeded": len(items),
            "failed": len(errors),
            "items": items,
            "errors": errors,
        }

    async def _fetch_quote_payload(
        self,
        symbol: str,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        query_params = self._build_query_params(params)
        requested_interval = self._resolve_interval_name(
            self._to_str(query_params.get("interval")) or "1day"
        )
        fxcm_symbol = self._resolve_fxcm_symbol(symbol)
        try:
            return await fxcm_sidecar_service.get_quote(
                symbol=fxcm_symbol,
                interval=requested_interval,
                price_type="mid",
            )
        except FXCMSidecarError as exc:
            raise self._to_twelve_error_from_fxcm(exc) from exc

    async def _fetch_search_items(
        self,
        keyword: str,
        outputsize: int,
    ) -> list[dict[str, Any]]:
        alias_items = self._search_alias_items(keyword)

        try:
            payload = await fxcm_sidecar_service.search_symbols(
                keyword=keyword,
                outputsize=min(120, max(outputsize * 4, outputsize)),
            )
        except FXCMSidecarError as exc:
            raise self._to_twelve_error_from_fxcm(exc) from exc

        fxcm_items = payload.get("items") if isinstance(payload, Mapping) else []
        normalized_items = [
            self._overlay_search_profile(item)
            for item in fxcm_items
            if isinstance(item, Mapping)
        ]

        return self._dedupe_search_items(
            normalized_items + alias_items, limit=outputsize
        )

    async def _fetch_fxcm_time_series_payload(
        self,
        query_params: Mapping[str, Any],
    ) -> dict[str, Any]:
        symbol = self._resolve_fxcm_symbol(self._extract_identifier(query_params))
        requested_interval = self._resolve_interval_name(
            self._to_str(query_params.get("interval")) or "1day"
        )
        outputsize = self._bounded_int(
            query_params.get("outputsize"),
            default=self.DEFAULT_TIME_SERIES_OUTPUTSIZE,
            minimum=1,
            maximum=5000,
        )
        history_params = self._build_fxcm_history_params(query_params)

        try:
            payload = await fxcm_sidecar_service.get_history(
                symbol=symbol,
                interval=requested_interval,
                outputsize=outputsize,
                start_date=history_params.get("start_date"),
                end_date=history_params.get("end_date"),
                price_type="mid",
            )
        except FXCMSidecarError as exc:
            raise self._to_twelve_error_from_fxcm(exc) from exc

        return self._build_time_series_payload_from_fxcm(payload, query_params)

    async def _get_time_series_payload(
        self,
        params: Mapping[str, Any] | None = None,
        *,
        filter_non_trading: bool = False,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        query_params = self._build_query_params(params)
        self._ensure_identifier(query_params)

        payload = await self._fetch_fxcm_time_series_payload(query_params)
        if not filter_non_trading:
            values_count = self._extract_values_count(payload)
            return payload, self._build_filter_info(
                requested=False,
                applied=False,
                original_count=values_count,
                filtered_count=values_count,
                reason="filter_not_requested",
            )

        filtered_payload, filter_info = self._filter_time_series_payload(
            payload,
            params=query_params,
        )
        if not isinstance(filtered_payload, Mapping):
            return dict(payload), filter_info
        return dict(filtered_payload), filter_info

    def _build_fxcm_history_params(
        self,
        query_params: Mapping[str, Any],
    ) -> dict[str, str | None]:
        date_value = self._to_str(query_params.get("date"))
        if date_value is not None:
            now = datetime.now(tz=timezone.utc)
            if date_value.casefold() == "today":
                start_dt = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
            elif date_value.casefold() == "yesterday":
                today = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
                start_dt = today - timedelta(days=1)
            else:
                start_dt = self._parse_request_datetime(date_value)
            if start_dt is None:
                raise TwelveDataAPIError(
                    status_code=400,
                    message="Invalid date parameter",
                    payload={"field": "date", "value": date_value},
                )
            end_dt = start_dt + timedelta(days=1)
            return {
                "start_date": start_dt.isoformat(),
                "end_date": end_dt.isoformat(),
            }

        return {
            "start_date": self._to_str(query_params.get("start_date")),
            "end_date": self._to_str(query_params.get("end_date")),
        }

    def _build_time_series_payload_from_fxcm(
        self,
        payload: Mapping[str, Any],
        query_params: Mapping[str, Any],
    ) -> dict[str, Any]:
        meta = payload.get("meta") if isinstance(payload.get("meta"), Mapping) else {}
        raw_values = (
            payload.get("values") if isinstance(payload.get("values"), list) else []
        )

        requested_interval = self._resolve_interval_name(
            self._to_str(query_params.get("interval")) or "1day"
        )
        outputsize = self._bounded_int(
            query_params.get("outputsize"),
            default=self.DEFAULT_TIME_SERIES_OUTPUTSIZE,
            minimum=1,
            maximum=5000,
        )
        order = (
            self._to_str(query_params.get("order")) or self.DEFAULT_KLINE_ORDER
        ).casefold()
        dp = self._to_int(query_params.get("dp"))
        response_timezone = self._resolve_output_timezone_name(
            self._to_str(query_params.get("timezone")),
            self._to_str(meta.get("exchange_timezone")) or "UTC",
        )

        rows = [
            {
                "timestamp": self._to_int(item.get("timestamp")),
                "open": self._to_float(item.get("open")),
                "high": self._to_float(item.get("high")),
                "low": self._to_float(item.get("low")),
                "close": self._to_float(item.get("close")),
                "volume": self._to_int(item.get("volume")),
            }
            for item in raw_values
            if isinstance(item, Mapping)
        ]
        rows = [
            row
            for row in rows
            if row.get("timestamp") is not None and row.get("close") is not None
        ]
        rows = self._append_previous_close(rows)
        rows = rows[-outputsize:]

        if order != "asc":
            rows = list(reversed(rows))

        intraday = self._is_intraday_interval(requested_interval)
        values = [
            self._format_time_series_row(
                row,
                timezone_name=response_timezone,
                intraday=intraday,
                dp=dp,
            )
            for row in rows
        ]

        return {
            "meta": {
                "symbol": self._to_str(meta.get("symbol"))
                or self._extract_identifier(query_params),
                "provider_symbol": self._to_str(meta.get("provider_symbol"))
                or self._extract_identifier(query_params),
                "interval": requested_interval,
                "currency": self._to_str(meta.get("currency")),
                "exchange": self._to_str(meta.get("exchange")),
                "mic_code": None,
                "exchange_timezone": self._to_str(meta.get("exchange_timezone"))
                or "UTC",
                "type": self._to_str(meta.get("asset_type")),
            },
            "values": values,
        }

    async def _fetch_fxcm_market_movers(
        self,
        *,
        market: str,
        direction: str,
        outputsize: int,
        country: str | None,
        price_greater_than: float | None,
    ) -> list[dict[str, Any]]:
        try:
            candidates_payload = await fxcm_sidecar_service.get_market_symbols(
                market=market,
                outputsize=self._resolve_market_mover_candidate_count(
                    market,
                    outputsize,
                ),
                country=country,
            )
        except FXCMSidecarError as exc:
            raise self._to_twelve_error_from_fxcm(exc) from exc

        candidates = (
            candidates_payload.get("items")
            if isinstance(candidates_payload, Mapping)
            else []
        )
        if not isinstance(candidates, list):
            candidates = []

        batch_symbols = [
            self._to_str(item.get("provider_symbol"))
            or self._to_str(item.get("symbol"))
            for item in candidates
            if isinstance(item, Mapping)
        ]
        batch_symbols = [symbol for symbol in batch_symbols if symbol is not None]
        if not batch_symbols:
            return []

        try:
            quotes_payload = await fxcm_sidecar_service.get_quotes_batch(
                symbols=batch_symbols,
                interval="1day",
                price_type="mid",
            )
        except FXCMSidecarError as exc:
            raise self._to_twelve_error_from_fxcm(exc) from exc

        candidate_by_symbol: dict[str, Mapping[str, Any]] = {}
        for candidate in candidates:
            if not isinstance(candidate, Mapping):
                continue
            for key in (
                self._normalize_lookup(self._to_str(candidate.get("provider_symbol"))),
                self._normalize_lookup(self._to_str(candidate.get("symbol"))),
            ):
                if key and key not in candidate_by_symbol:
                    candidate_by_symbol[key] = candidate

        raw_items = (
            quotes_payload.get("items") if isinstance(quotes_payload, Mapping) else []
        )
        items: list[dict[str, Any]] = []
        for payload in raw_items:
            if not isinstance(payload, Mapping):
                continue
            item = self._build_market_mover_from_quote(payload)
            candidate = candidate_by_symbol.get(
                self._normalize_lookup(self._to_str(payload.get("provider_symbol")))
            ) or candidate_by_symbol.get(
                self._normalize_lookup(self._to_str(payload.get("symbol")))
            )
            item = self._overlay_market_mover_candidate(item, candidate)
            if price_greater_than is not None:
                price = self._to_float(item.get("price"))
                if price is None or price <= price_greater_than:
                    continue
            items.append(item)

        items.sort(
            key=lambda item: self._market_mover_sort_key(item, direction),
            reverse=direction == "gainers",
        )
        return items[:outputsize]

    def _append_previous_close(
        self,
        rows: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        items = [dict(row) for row in rows if isinstance(row, Mapping)]
        items.sort(key=lambda item: self._to_int(item.get("timestamp")) or 0)

        previous_close: float | None = None
        for item in items:
            item["previous_close"] = previous_close
            current_close = self._to_float(item.get("close"))
            if current_close is not None:
                previous_close = current_close

        return items

    def _format_time_series_row(
        self,
        row: Mapping[str, Any],
        *,
        timezone_name: str | None,
        intraday: bool,
        dp: int | None,
    ) -> dict[str, Any]:
        timestamp = self._to_int(row.get("timestamp")) or 0
        return {
            "datetime": self._format_output_datetime(
                timestamp,
                timezone_name=timezone_name,
                intraday=intraday,
            ),
            "open": self._round_number(self._to_float(row.get("open")), dp),
            "high": self._round_number(self._to_float(row.get("high")), dp),
            "low": self._round_number(self._to_float(row.get("low")), dp),
            "close": self._round_number(self._to_float(row.get("close")), dp),
            "volume": self._to_int(row.get("volume")),
            "previous_close": self._round_number(
                self._to_float(row.get("previous_close")),
                dp,
            ),
        }

    def _format_output_datetime(
        self,
        timestamp: int,
        *,
        timezone_name: str | None,
        intraday: bool,
    ) -> str:
        output_datetime = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        if timezone_name is not None:
            try:
                output_datetime = output_datetime.astimezone(ZoneInfo(timezone_name))
            except ZoneInfoNotFoundError:
                pass

        if intraday:
            return output_datetime.replace(tzinfo=None).isoformat(timespec="seconds")

        return output_datetime.date().isoformat()

    def _resolve_output_timezone_name(
        self,
        request_timezone: str | None,
        exchange_timezone: str | None,
    ) -> str | None:
        if request_timezone is None or request_timezone.casefold() == "exchange":
            return exchange_timezone
        return request_timezone

    def _resolve_interval_name(self, interval: str) -> str:
        aliases = {
            "1d": "1day",
            "1wk": "1week",
            "1mo": "1month",
            "60m": "1h",
            "60min": "1h",
        }
        return aliases.get(interval, interval)

    def _build_symbol_search_record(
        self,
        item: Mapping[str, Any],
    ) -> dict[str, Any]:
        return {
            "symbol": self._to_str(item.get("symbol")),
            "provider_symbol": self._to_str(item.get("provider_symbol")),
            "instrument_name": self._to_str(item.get("name")),
            "label": self._to_str(item.get("label")),
            "exchange": self._to_str(item.get("exchange")),
            "mic_code": self._to_str(item.get("mic_code")),
            "exchange_timezone": self._to_str(item.get("timezone")),
            "market": self._to_str(item.get("market")),
            "instrument_type": self._to_str(item.get("asset_type")),
            "country": self._to_str(item.get("country")),
            "currency": self._to_str(item.get("currency")),
            "plan": item.get("provider_plan"),
        }

    def _search_alias_items(self, keyword: str) -> list[dict[str, Any]]:
        normalized_keyword = self._normalize_lookup(keyword)
        exact_items: list[dict[str, Any]] = []
        fuzzy_items: list[dict[str, Any]] = []

        for profile in self.SYMBOL_SEARCH_PROFILES:
            haystacks = [
                self._normalize_lookup(profile["symbol"]),
                self._normalize_lookup(profile["provider_symbol"]),
                self._normalize_lookup(profile["name"]),
                *[
                    self._normalize_lookup(alias)
                    for alias in profile.get("aliases", ())
                ],
            ]
            if any(
                normalized_keyword == haystack for haystack in haystacks if haystack
            ):
                exact_items.append(self._build_search_profile_item(profile))
                continue
            if any(
                normalized_keyword in haystack or haystack in normalized_keyword
                for haystack in haystacks
                if haystack
            ):
                fuzzy_items.append(self._build_search_profile_item(profile))

        return exact_items + fuzzy_items

    def _build_search_profile_item(
        self,
        profile: Mapping[str, Any],
    ) -> dict[str, Any]:
        symbol = self._to_str(profile.get("symbol"))
        name = self._to_str(profile.get("name"))
        label = name or symbol
        if symbol and name and symbol != name:
            label = f"{name} ({symbol})"

        return {
            "symbol": symbol,
            "provider_symbol": self._to_str(profile.get("provider_symbol")),
            "name": name,
            "label": label,
            "exchange": self._to_str(profile.get("exchange")),
            "mic_code": None,
            "market": self._to_str(profile.get("market")),
            "asset_type": self._to_str(profile.get("asset_type")),
            "country": self._to_str(profile.get("country")),
            "currency": self._to_str(profile.get("currency")),
            "timezone": self._to_str(profile.get("timezone")),
            "provider_plan": None,
        }

    def _overlay_search_profile(
        self,
        item: Mapping[str, Any],
    ) -> dict[str, Any]:
        profile = self._find_symbol_profile(
            self._to_str(item.get("provider_symbol"))
            or self._to_str(item.get("symbol"))
        )
        if profile is None:
            return dict(item)

        merged = self._build_search_profile_item(profile)
        for key, value in item.items():
            if value not in (None, ""):
                merged[key] = value

        merged["symbol"] = self._to_str(profile.get("symbol")) or merged.get("symbol")
        merged["provider_symbol"] = self._to_str(
            item.get("provider_symbol")
        ) or self._to_str(profile.get("provider_symbol"))
        merged["market"] = self._to_str(profile.get("market")) or merged.get("market")
        merged["asset_type"] = self._to_str(profile.get("asset_type")) or merged.get(
            "asset_type"
        )
        merged["exchange"] = self._to_str(item.get("exchange")) or self._to_str(
            profile.get("exchange")
        )
        merged["name"] = self._to_str(item.get("name")) or self._to_str(
            profile.get("name")
        )
        name = merged.get("name")
        symbol = merged.get("symbol")
        merged["label"] = name or symbol
        if name and symbol and name != symbol:
            merged["label"] = f"{name} ({symbol})"

        return merged

    def _dedupe_search_items(
        self,
        items: Sequence[Mapping[str, Any]],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()

        for item in items:
            symbol = self._normalize_lookup(self._to_str(item.get("symbol")))
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            deduped.append(dict(item))
            if len(deduped) >= limit:
                break

        return deduped

    def _build_market_mover_from_quote(
        self,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        symbol = self._to_str(payload.get("symbol"))
        provider_symbol = self._to_str(payload.get("provider_symbol"))
        asset_type = self._map_provider_asset_type(
            None,
            provider_symbol=provider_symbol,
            canonical_symbol=symbol,
        )
        return {
            "symbol": symbol,
            "provider_symbol": provider_symbol,
            "name": self._to_str(payload.get("name")) or symbol,
            "exchange": self._to_str(payload.get("exchange")),
            "market": self._market_label_from_asset_type(
                asset_type,
                self._to_str(payload.get("exchange")),
            ),
            "asset_type": asset_type,
            "currency": self._to_str(payload.get("currency")),
            "price": self._to_float(payload.get("close")),
            "change": {
                "value": self._to_float(payload.get("change")),
                "percent": self._to_float(payload.get("percent_change")),
            },
            "volume": self._to_int(payload.get("volume")),
            "market_state": ("REGULAR" if payload.get("is_market_open") else "CLOSED"),
            "datetime": self._to_str(payload.get("datetime")),
        }

    def _overlay_market_mover_candidate(
        self,
        item: Mapping[str, Any],
        candidate: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        merged = dict(item)
        if not isinstance(candidate, Mapping):
            return merged

        merged["symbol"] = self._to_str(candidate.get("symbol")) or merged.get("symbol")
        merged["provider_symbol"] = self._to_str(
            candidate.get("provider_symbol")
        ) or merged.get("provider_symbol")
        merged["name"] = self._to_str(candidate.get("name")) or merged.get("name")
        merged["exchange"] = self._to_str(candidate.get("exchange")) or merged.get(
            "exchange"
        )
        merged["market"] = self._to_str(candidate.get("market")) or merged.get("market")
        merged["asset_type"] = self._to_str(candidate.get("asset_type")) or merged.get(
            "asset_type"
        )
        merged["currency"] = self._to_str(candidate.get("currency")) or merged.get(
            "currency"
        )
        return merged

    def _market_mover_sort_key(
        self,
        item: Mapping[str, Any],
        direction: str,
    ) -> float:
        change = item.get("change")
        percent = (
            self._to_float(change.get("percent"))
            if isinstance(change, Mapping)
            else None
        )
        if percent is not None:
            return percent
        return float("-inf") if direction == "gainers" else float("inf")

    def _build_market_movers_cache_key(
        self,
        *,
        market: str,
        direction: str,
        outputsize: int,
        country: str | None,
        price_greater_than: float | None,
    ) -> tuple[Any, ...]:
        return (
            "market_movers",
            market,
            direction,
            outputsize,
            (country or "").casefold(),
            price_greater_than,
        )

    def _resolve_market_mover_candidate_count(
        self,
        market: str,
        outputsize: int,
    ) -> int:
        cap = self.MARKET_MOVER_CANDIDATE_CAPS.get(market, 0)
        if cap <= 0:
            return 1
        return min(cap, max(outputsize * 2, 6))

    def _matches_country_filter(
        self,
        item: Mapping[str, Any],
        country: str,
    ) -> bool:
        normalized_country = country.strip().casefold()
        candidates = [
            self._to_str(item.get("region")),
            self._to_str(item.get("exchange")),
            self._to_str(item.get("fullExchangeName")),
        ]
        return any(
            candidate is not None and normalized_country in candidate.casefold()
            for candidate in candidates
        )

    def _extract_identifier(self, params: Mapping[str, Any]) -> str:
        for field in self.IDENTIFIER_FIELDS:
            value = self._to_str(params.get(field))
            if value is not None:
                return value
        self._ensure_identifier(params)
        raise TwelveDataAPIError(
            status_code=400,
            message="One of symbol, figi, isin or cusip is required",
            payload={"required_any_of": list(self.IDENTIFIER_FIELDS)},
        )

    def _resolve_fxcm_symbol(self, symbol: str) -> str:
        normalized_symbol = symbol.strip().upper()
        profile = self._find_symbol_profile(symbol)
        if profile is not None:
            canonical_symbol = self._to_str(profile.get("symbol"))
            if canonical_symbol:
                return canonical_symbol

        if "/" in normalized_symbol or "." in normalized_symbol:
            return normalized_symbol

        if len(normalized_symbol) == 6 and normalized_symbol.isalpha():
            return f"{normalized_symbol[:3]}/{normalized_symbol[3:]}"

        return normalized_symbol

    def _find_symbol_profile(
        self,
        symbol: str | None,
    ) -> Mapping[str, Any] | None:
        normalized_symbol = self._normalize_lookup(symbol)
        if not normalized_symbol:
            return None

        for profile in self.SYMBOL_SEARCH_PROFILES:
            candidates = [
                self._normalize_lookup(profile.get("symbol")),
                self._normalize_lookup(profile.get("provider_symbol")),
                *[
                    self._normalize_lookup(alias)
                    for alias in profile.get("aliases", ())
                ],
            ]
            if normalized_symbol in candidates:
                return profile

        return None

    def _map_provider_asset_type(
        self,
        provider_type: str | None,
        *,
        provider_symbol: str | None,
        canonical_symbol: str | None,
    ) -> str | None:
        profile = self._find_symbol_profile(provider_symbol or canonical_symbol)
        if profile is not None:
            return self._to_str(profile.get("asset_type"))
        mapping = {
            "CURRENCY": "Physical Currency",
            "CRYPTOCURRENCY": "Digital Currency",
            "ETF": "ETF",
            "MUTUALFUND": "Mutual Fund",
            "EQUITY": "Common Stock",
            "FUTURE": "Futures",
        }
        if provider_type in mapping:
            return mapping[provider_type]
        if canonical_symbol in {"XAU/USD", "XAG/USD"}:
            return "Precious Metal"
        return provider_type

    def _market_label_from_asset_type(
        self,
        asset_type: str | None,
        exchange: str | None,
    ) -> str | None:
        if asset_type == "Physical Currency":
            return "Forex"
        if asset_type == "Digital Currency":
            return "Crypto"
        if asset_type == "Precious Metal":
            return "Precious Metals"
        return exchange

    def _parse_request_datetime(self, value: str | None) -> datetime | None:
        if value is None:
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _bounded_int(
        self,
        value: Any,
        *,
        default: int,
        minimum: int,
        maximum: int,
    ) -> int:
        number = self._to_int(value)
        if number is None:
            return default
        return max(minimum, min(maximum, number))

    def _coerce_bool(self, value: Any) -> bool | None:
        if isinstance(value, bool):
            return value
        if value in (None, ""):
            return None
        if isinstance(value, str):
            normalized = value.strip().casefold()
            if normalized in {"1", "true", "yes", "on"}:
                return True
            if normalized in {"0", "false", "no", "off"}:
                return False
        return None

    def _round_number(self, value: float | None, dp: int | None) -> float | None:
        if value is None or dp is None:
            return value
        return round(value, dp)

    def _normalize_lookup(self, value: Any) -> str:
        if value in (None, ""):
            return ""
        return "".join(ch for ch in str(value).upper() if ch.isalnum())

    def _timestamp_to_iso(self, timestamp: int | None) -> str | None:
        if timestamp is None:
            return None
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()

    def _to_twelve_error_from_fxcm(
        self,
        exc: FXCMSidecarError,
    ) -> TwelveDataAPIError:
        return TwelveDataAPIError(
            status_code=exc.status_code,
            message=exc.message,
            payload=exc.payload,
        )

    def _build_query_params(
        self,
        params: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        return {
            key: value
            for key, value in (params or {}).items()
            if value is not None and value != "" and key != "apikey"
        }

    def _ensure_identifier(self, params: Mapping[str, Any]) -> None:
        if any(params.get(field) for field in self.IDENTIFIER_FIELDS):
            return

        raise TwelveDataAPIError(
            status_code=400,
            message="One of symbol, figi, isin or cusip is required",
            payload={"required_any_of": list(self.IDENTIFIER_FIELDS)},
        )

    def _normalize_symbols(self, symbols: Sequence[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()

        for raw_symbol in symbols:
            symbol = raw_symbol.strip().upper()
            if not symbol or symbol in seen:
                continue
            normalized.append(symbol)
            seen.add(symbol)

        return normalized

    def _normalize_quote(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, Mapping):
            return {"raw": payload}

        week_52 = payload.get("fifty_two_week")
        if not isinstance(week_52, Mapping):
            week_52 = {}

        last_price = self._to_float(payload.get("close"))

        return {
            "symbol": payload.get("symbol"),
            "name": payload.get("name"),
            "exchange": payload.get("exchange"),
            "mic_code": payload.get("mic_code"),
            "currency": payload.get("currency"),
            "datetime": payload.get("datetime"),
            "timestamp": self._to_int(payload.get("timestamp")),
            "last_quote_at": self._to_int(payload.get("last_quote_at")),
            "last_price": last_price,
            "ohlc": {
                "open": self._to_float(payload.get("open")),
                "high": self._to_float(payload.get("high")),
                "low": self._to_float(payload.get("low")),
                "close": last_price,
            },
            "change": {
                "value": self._to_float(payload.get("change")),
                "percent": self._to_float(payload.get("percent_change")),
                "previous_close": self._to_float(payload.get("previous_close")),
            },
            "volume": self._to_int(payload.get("volume")),
            "average_volume": self._to_int(payload.get("average_volume")),
            "is_market_open": payload.get("is_market_open"),
            "week_52": {
                "low": self._to_float(week_52.get("low")),
                "high": self._to_float(week_52.get("high")),
                "range": week_52.get("range"),
            },
        }

    def _normalize_search_item(self, item: Mapping[str, Any]) -> dict[str, Any]:
        symbol = self._to_str(item.get("symbol"))
        instrument_name = self._to_str(item.get("instrument_name")) or self._to_str(
            item.get("name")
        )
        exchange = self._to_str(item.get("exchange"))
        mic_code = self._to_str(item.get("mic_code"))
        provided_label = self._to_str(item.get("label"))
        label = provided_label or instrument_name or symbol
        if (
            provided_label is None
            and symbol
            and instrument_name
            and symbol != instrument_name
        ):
            label = f"{instrument_name} ({symbol})"

        provided_market = self._to_str(item.get("market"))
        market = provided_market or " / ".join(
            part for part in (exchange, mic_code) if part
        )

        return {
            "symbol": symbol,
            "provider_symbol": self._to_str(item.get("provider_symbol")),
            "name": instrument_name,
            "label": label,
            "exchange": exchange,
            "mic_code": mic_code,
            "market": market,
            "timezone": self._to_str(item.get("exchange_timezone"))
            or self._to_str(item.get("timezone")),
            "asset_type": self._to_str(item.get("instrument_type"))
            or self._to_str(item.get("asset_type")),
            "country": self._to_str(item.get("country")),
            "currency": self._to_str(item.get("currency")),
            "provider_plan": item.get("plan") or item.get("available_on"),
        }

    def _normalize_kline(
        self,
        payload: Any,
        *,
        requested_symbol: str,
        requested_interval: str,
        defaults_applied: Mapping[str, Any],
        filter_info: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not isinstance(payload, Mapping):
            return {
                "symbol": requested_symbol,
                "interval": requested_interval,
                "count": 0,
                "defaults_applied": dict(defaults_applied),
                "filtering": dict(filter_info or {}),
                "meta": {},
                "candles": [],
                "raw": payload,
            }

        meta = payload.get("meta")
        values = payload.get("values")
        if not isinstance(meta, Mapping):
            meta = {}
        if not isinstance(values, list):
            values = []

        candles = [
            self._normalize_candle(item) for item in values if isinstance(item, Mapping)
        ]

        return {
            "symbol": meta.get("symbol") or requested_symbol,
            "interval": meta.get("interval") or requested_interval,
            "count": len(candles),
            "defaults_applied": dict(defaults_applied),
            "filtering": dict(filter_info or {}),
            "meta": {
                "symbol": meta.get("symbol") or requested_symbol,
                "interval": meta.get("interval") or requested_interval,
                "currency": meta.get("currency"),
                "exchange": meta.get("exchange"),
                "mic_code": meta.get("mic_code"),
                "exchange_timezone": meta.get("exchange_timezone"),
                "asset_type": meta.get("type"),
            },
            "candles": candles,
        }

    def _normalize_candle(self, item: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "datetime": item.get("datetime"),
            "open": self._to_float(item.get("open")),
            "high": self._to_float(item.get("high")),
            "low": self._to_float(item.get("low")),
            "close": self._to_float(item.get("close")),
            "volume": self._to_int(item.get("volume")),
            "previous_close": self._to_float(item.get("previous_close")),
        }

    def _filter_time_series_payload(
        self,
        payload: Any,
        *,
        params: Mapping[str, Any] | None,
    ) -> tuple[Any, dict[str, Any]]:
        original_count = self._extract_values_count(payload)
        base_info = self._build_filter_info(
            requested=True,
            applied=False,
            original_count=original_count,
            filtered_count=original_count,
            reason=None,
        )

        if not isinstance(payload, Mapping):
            return payload, self._with_filter_reason(base_info, "payload_not_mapping")

        raw_meta = payload.get("meta")
        raw_values = payload.get("values")
        meta = raw_meta if isinstance(raw_meta, Mapping) else {}
        values = raw_values if isinstance(raw_values, list) else []

        interval = self._to_str(meta.get("interval")) or self._to_str(
            (params or {}).get("interval")
        )
        asset_type = self._normalize_asset_type(
            self._to_str(meta.get("type")) or self._to_str((params or {}).get("type"))
        )

        if not self._is_intraday_interval(interval):
            return payload, self._with_filter_reason(base_info, "non_intraday_interval")

        if asset_type in self.ALWAYS_OPEN_ASSET_TYPES:
            return payload, self._with_filter_reason(base_info, "always_open_asset")

        entries = self._build_candle_entries(values, meta=meta, params=params)
        if not entries:
            return payload, self._with_filter_reason(base_info, "datetime_parse_failed")

        holiday_like_dates = self._detect_holiday_like_dates(entries)
        filtered_values: list[Mapping[str, Any]] = []
        dropped_weekend = 0
        dropped_holiday_like = 0

        for entry in entries:
            is_weekend_stale = (
                entry["is_stale"]
                and entry["exchange_date"] is not None
                and entry["exchange_date"].weekday() >= 5
            )
            is_holiday_like = entry["exchange_date"] in holiday_like_dates

            if is_weekend_stale:
                dropped_weekend += 1
                continue

            if is_holiday_like:
                dropped_holiday_like += 1
                continue

            filtered_values.append(entry["item"])

        filtered_count = len(filtered_values)
        dropped_count = original_count - filtered_count
        filter_info = self._build_filter_info(
            requested=True,
            applied=dropped_count > 0,
            original_count=original_count,
            filtered_count=filtered_count,
            dropped_weekend=dropped_weekend,
            dropped_holiday_like=dropped_holiday_like,
            reason=(
                "filtered_closed_session_candles"
                if dropped_count > 0
                else "no_closed_session_candles_detected"
            ),
        )

        if dropped_count == 0:
            return payload, filter_info

        filtered_payload = dict(payload)
        filtered_payload["values"] = filtered_values
        return filtered_payload, filter_info

    def _build_candle_entries(
        self,
        values: Sequence[Any],
        *,
        meta: Mapping[str, Any],
        params: Mapping[str, Any] | None,
    ) -> list[dict[str, Any]]:
        response_timezone = self._resolve_response_timezone_name(
            interval=self._to_str(meta.get("interval"))
            or self._to_str((params or {}).get("interval")),
            request_timezone=self._to_str((params or {}).get("timezone")),
            exchange_timezone=self._to_str(meta.get("exchange_timezone")),
        )
        exchange_timezone = self._to_str(meta.get("exchange_timezone"))

        entries: list[dict[str, Any]] = []
        for item in values:
            if not isinstance(item, Mapping):
                continue

            raw_datetime = self._to_str(item.get("datetime"))
            parsed_datetime = self._parse_candle_datetime(raw_datetime)
            if raw_datetime is None or parsed_datetime is None:
                return []

            exchange_datetime = self._to_exchange_datetime(
                parsed_datetime,
                response_timezone=response_timezone,
                exchange_timezone=exchange_timezone,
            )
            entries.append(
                {
                    "item": item,
                    "raw_datetime": raw_datetime,
                    "sort_key": raw_datetime,
                    "exchange_date": exchange_datetime.date(),
                    "is_stale": False,
                }
            )

        previous_close: float | None = None
        sorted_entries = sorted(entries, key=lambda item: item["sort_key"])
        for index, entry in enumerate(sorted_entries):
            close_price = self._to_float(entry["item"].get("close"))
            explicit_previous_close = self._to_float(
                entry["item"].get("previous_close")
            )
            next_close = None
            if index + 1 < len(sorted_entries):
                next_close = self._to_float(
                    sorted_entries[index + 1]["item"].get("close")
                )

            entry["is_stale"] = self._is_stale_candle(
                entry["item"],
                previous_close=(
                    explicit_previous_close
                    if explicit_previous_close is not None
                    else previous_close
                ),
                next_close=next_close,
            )
            if close_price is not None:
                previous_close = close_price

        return entries

    def _detect_holiday_like_dates(
        self,
        entries: Sequence[Mapping[str, Any]],
    ) -> set[date]:
        grouped: dict[date, list[Mapping[str, Any]]] = {}

        for entry in entries:
            exchange_date = entry.get("exchange_date")
            if not isinstance(exchange_date, date):
                continue
            grouped.setdefault(exchange_date, []).append(entry)

        holiday_like_dates: set[date] = set()
        for exchange_date, grouped_entries in grouped.items():
            if exchange_date.weekday() >= 5:
                continue
            if len(grouped_entries) < self.HOLIDAY_LIKE_STALE_MIN_CANDLES:
                continue
            if all(entry.get("is_stale") for entry in grouped_entries):
                holiday_like_dates.add(exchange_date)

        return holiday_like_dates

    def _is_stale_candle(
        self,
        item: Mapping[str, Any],
        *,
        previous_close: float | None,
        next_close: float | None,
    ) -> bool:
        open_price = self._to_float(item.get("open"))
        high_price = self._to_float(item.get("high"))
        low_price = self._to_float(item.get("low"))
        close_price = self._to_float(item.get("close"))
        volume = self._to_int(item.get("volume"))

        if None in (open_price, high_price, low_price, close_price):
            return False

        if (
            open_price != high_price
            or high_price != low_price
            or low_price != close_price
        ):
            return False

        if previous_close is not None and close_price == previous_close:
            return volume in (None, 0)

        if next_close is not None and close_price == next_close:
            return volume in (None, 0)

        return False

    def _resolve_response_timezone_name(
        self,
        *,
        interval: str | None,
        request_timezone: str | None,
        exchange_timezone: str | None,
    ) -> str | None:
        if not self._is_intraday_interval(interval):
            return exchange_timezone

        if request_timezone is None or request_timezone.casefold() == "exchange":
            return exchange_timezone

        return request_timezone

    def _to_exchange_datetime(
        self,
        parsed_datetime: datetime,
        *,
        response_timezone: str | None,
        exchange_timezone: str | None,
    ) -> datetime:
        localized_datetime = parsed_datetime

        if parsed_datetime.tzinfo is None and response_timezone is not None:
            try:
                localized_datetime = parsed_datetime.replace(
                    tzinfo=ZoneInfo(response_timezone)
                )
            except ZoneInfoNotFoundError:
                localized_datetime = parsed_datetime

        if localized_datetime.tzinfo is None or exchange_timezone is None:
            return localized_datetime

        try:
            return localized_datetime.astimezone(ZoneInfo(exchange_timezone))
        except ZoneInfoNotFoundError:
            return localized_datetime

    def _is_intraday_interval(self, interval: str | None) -> bool:
        if interval is None:
            return False
        normalized_interval = interval.casefold()
        return not normalized_interval.endswith(("day", "week", "month"))

    def _normalize_asset_type(self, asset_type: str | None) -> str | None:
        if asset_type is None:
            return None
        return asset_type.strip().casefold()

    def _extract_values_count(self, payload: Any) -> int:
        if not isinstance(payload, Mapping):
            return 0
        values = payload.get("values")
        if not isinstance(values, list):
            return 0
        return len(values)

    def _build_filter_info(
        self,
        *,
        requested: bool,
        applied: bool,
        original_count: int,
        filtered_count: int,
        reason: str | None,
        dropped_weekend: int = 0,
        dropped_holiday_like: int = 0,
    ) -> dict[str, Any]:
        return {
            "requested": requested,
            "applied": applied,
            "original_count": original_count,
            "filtered_count": filtered_count,
            "dropped_count": original_count - filtered_count,
            "dropped_weekend": dropped_weekend,
            "dropped_holiday_like": dropped_holiday_like,
            "reason": reason,
        }

    def _with_filter_reason(
        self,
        filter_info: Mapping[str, Any],
        reason: str,
    ) -> dict[str, Any]:
        updated_filter_info = dict(filter_info)
        updated_filter_info["reason"] = reason
        return updated_filter_info

    def _parse_candle_datetime(self, value: str | None) -> datetime | None:
        if value is None:
            return None

        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    def _to_float(self, value: Any) -> float | None:
        if value in (None, ""):
            return None

        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _to_int(self, value: Any) -> int | None:
        if value in (None, ""):
            return None

        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _to_str(self, value: Any) -> str | None:
        if value in (None, ""):
            return None
        return str(value)


market_master_service = MarketMasterService()
