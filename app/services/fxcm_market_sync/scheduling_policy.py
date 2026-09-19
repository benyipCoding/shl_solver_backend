from datetime import datetime, timedelta

from app.models.market_data import MarketInstrument
from app.services.fxcm_market_sync.constants import ALWAYS_OPEN_ASSET_TYPES


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
    """根据资产类型或符号推断品类，用于每日轮换策略。"""
    return infer_category_key(instrument.asset_type, instrument.symbol)


def is_allowed_today(instrument: MarketInstrument, current_time: datetime) -> bool:
    """根据硬性调度策略：不同日期采集不同品类，防止单一品类霸占队列。"""
    weekday = current_time.weekday()
    category = infer_asset_category(instrument)

    if category == "other":
        return True

    if weekday == 0:
        return category == "forex"
    if weekday == 1:
        return category == "index"
    if weekday == 2:
        return category == "commodity"
    if weekday == 3:
        return category == "index"
    if weekday == 4:
        return category == "crypto"
    if weekday == 5:
        return category == "forex"
    if weekday == 6:
        return category == "commodity"

    return True


def weekend_resume_at(
    instrument: MarketInstrument,
    current_time: datetime,
) -> datetime | None:
    """若非 7x24 品种且当前在周末，返回下周一恢复时间。"""
    asset_type = normalize_asset_type(instrument.asset_type)
    if asset_type in ALWAYS_OPEN_ASSET_TYPES:
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
