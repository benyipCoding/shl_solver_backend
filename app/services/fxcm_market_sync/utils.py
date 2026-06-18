from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping

from app.services.market_master import market_master_service


def normalize_symbol(value: str) -> str:
    """规范化符号文本，便于一致匹配。"""
    return market_master_service._normalize_lookup(value)


def coalesce_str(*values: Any) -> str | None:
    """返回第一个非空字符串值。"""
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def to_int(value: Any) -> int | None:
    """安全转换为 int，失败返回 None。"""
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_decimal(value: Any) -> Decimal | None:
    """安全转换为 Decimal，失败返回 None。"""
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def min_datetime(current: datetime | None, new_value: datetime) -> datetime:
    """在 current 与 new_value 之间取更早时间。"""
    if current is None:
        return new_value
    return min(current, new_value)


def max_datetime(current: datetime | None, new_value: datetime) -> datetime:
    """在 current 与 new_value 之间取更晚时间。"""
    if current is None:
        return new_value
    return max(current, new_value)


def parse_request_payload_datetime(value: Any) -> datetime | None:
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


def parse_bar_time(item: Mapping[str, Any]) -> datetime | None:
    """从 bar 项中解析时间，优先 timestamp，其次 datetime。"""
    timestamp = to_int(item.get("timestamp"))
    if timestamp is not None:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)

    raw_value = coalesce_str(item.get("datetime"))
    if raw_value is None:
        return None
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
