from datetime import datetime, timedelta

from app.core.config import settings
from app.services.fxcm_market_sync.constants import SUPPORTED_INTERVALS
from app.services.market_master import market_master_service


def normalize_sync_interval(interval: str) -> str | None:
    """标准化周期名，且必须在 SUPPORTED_INTERVALS 内才返回。"""
    normalized = market_master_service._resolve_interval_name(interval.strip())
    if normalized not in SUPPORTED_INTERVALS:
        return None
    return normalized


def resolve_sync_intervals(raw_intervals_config: str) -> list[str]:
    """解析并标准化配置中的同步周期，过滤不支持项。"""
    raw_intervals = [
        item.strip()
        for item in raw_intervals_config.split(",")
        if item.strip()
    ]
    normalized_intervals: list[str] = []
    seen: set[str] = set()
    for interval in raw_intervals or list(SUPPORTED_INTERVALS):
        normalized_interval = normalize_sync_interval(interval)
        if normalized_interval is None or normalized_interval in seen:
            continue
        seen.add(normalized_interval)
        normalized_intervals.append(normalized_interval)
    return normalized_intervals or list(SUPPORTED_INTERVALS)


def incremental_outputsize(interval: str) -> int:
    """按周期返回增量拉取建议条数。"""
    if interval in {"1week", "1day"}:
        return settings.fxcm_sync_1day_incremental_outputsize
    if interval in {"8h", "4h"}:
        return max(60, settings.fxcm_sync_1h_incremental_outputsize // 2)
    if interval == "2h":
        return max(100, settings.fxcm_sync_1h_incremental_outputsize)
    if interval in {"1h", "30min"}:
        return max(300, settings.fxcm_sync_1h_incremental_outputsize * 2)
    if interval in {"15min", "5min", "1min"}:
        return max(500, settings.fxcm_sync_1h_incremental_outputsize * 3)
    return settings.fxcm_sync_1h_incremental_outputsize


def state_priority(interval: str, sync_intervals: list[str]) -> int:
    """根据周期在配置中的顺序计算任务优先级。"""
    try:
        return (sync_intervals.index(interval) + 1) * 10
    except ValueError:
        return 100


def interval_delta(interval: str) -> timedelta:
    """把周期字符串转换为对应的时间跨度。"""
    if interval == "1week":
        return timedelta(weeks=1)
    if interval == "1day":
        return timedelta(days=1)
    if interval == "8h":
        return timedelta(hours=8)
    if interval == "4h":
        return timedelta(hours=4)
    if interval == "2h":
        return timedelta(hours=2)
    if interval == "1h":
        return timedelta(hours=1)
    if interval == "30min":
        return timedelta(minutes=30)
    if interval == "15min":
        return timedelta(minutes=15)
    if interval == "5min":
        return timedelta(minutes=5)
    if interval == "1min":
        return timedelta(minutes=1)
    return timedelta(hours=1)


def sync_poll_delay(interval: str) -> timedelta:
    """增量模式下，各周期再次进入调度队列前的冷却时间。"""
    poll_delays = {
        "1min": timedelta(minutes=2),
        "5min": timedelta(minutes=5),
        "15min": timedelta(minutes=15),
        "30min": timedelta(minutes=30),
        "1h": timedelta(hours=1),
        "2h": timedelta(hours=2),
        "4h": timedelta(hours=4),
        "8h": timedelta(hours=8),
        "1day": timedelta(hours=1),
        "1week": timedelta(hours=6),
    }
    return poll_delays.get(interval, timedelta(minutes=2))


def calculate_next_sync_from(
    *,
    interval: str,
    now: datetime,
    skip_weekends: bool,
) -> datetime:
    """计算下一次调度时间，按 K 线周期设置冷却并可跳过周末。"""
    next_sync_from = now + sync_poll_delay(interval)

    while skip_weekends and next_sync_from.weekday() >= 5:
        next_sync_from = next_sync_from + timedelta(days=1)

    return next_sync_from
