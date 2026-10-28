from app.models.market_data import MarketInstrument


def normalize_asset_type(asset_type: str | None) -> str | None:
    """标准化资产类型文本，便于规则判断。"""
    if asset_type is None:
        return None
    normalized = asset_type.strip().casefold()
    return normalized or None


def infer_category_key(asset_type: str | None, symbol: str | None = None) -> str:
    """根据资产类型或符号推断品类键。"""
    normalized_asset_type = normalize_asset_type(asset_type) or ""

    if "digital" in normalized_asset_type or "crypto" in normalized_asset_type:
        return "crypto"
    if (
        "forex" in normalized_asset_type
        or "fx" in normalized_asset_type
        or "currency" in normalized_asset_type
    ):
        return "forex"
    if (
        "index" in normalized_asset_type
        or "stock" in normalized_asset_type
        or "equity" in normalized_asset_type
    ):
        return "index"
    if (
        "metal" in normalized_asset_type
        or "commodity" in normalized_asset_type
        or "oil" in normalized_asset_type
        or "energy" in normalized_asset_type
    ):
        return "commodity"

    normalized_symbol = (symbol or "").upper()
    if normalized_symbol in ("BTC/USD", "ETH/USD"):
        return "crypto"
    if normalized_symbol in (
        "US30",
        "NAS100",
        "SPX500",
        "UK100",
        "GER30",
        "FRA40",
        "USDOLLAR",
    ):
        return "index"
    if (
        "XAU" in normalized_symbol
        or "XAG" in normalized_symbol
        or "OIL" in normalized_symbol
    ):
        return "commodity"
    if len(normalized_symbol) == 7 and "/" in normalized_symbol:
        return "forex"
    return "other"


def infer_asset_category(instrument: MarketInstrument) -> str:
    """根据资产类型或符号推断品类，用于页面分组展示。"""
    return infer_category_key(instrument.asset_type, instrument.symbol)
