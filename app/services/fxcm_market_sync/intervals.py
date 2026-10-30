from datetime import timedelta

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
    if interval in {"8h", "6h", "4h"}:
        return max(60, settings.fxcm_sync_1h_incremental_outputsize // 2)
    if interval in {"3h", "2h"}:
        return max(100, settings.fxcm_sync_1h_incremental_outputsize)
    if interval in {"1h", "30min"}:
        return max(300, settings.fxcm_sync_1h_incremental_outputsize * 2)
    if interval in {"15min", "5min"}:
        return max(500, settings.fxcm_sync_1h_incremental_outputsize * 3)
    return settings.fxcm_sync_1h_incremental_outputsize


def catchup_outputsize(interval: str) -> int:
    """手动追赶到最新时，单次向前拉取的条数。只覆盖最新缺口，不回补更早历史。"""
    if interval == "1week":
        return 80
    if interval == "1day":
        return max(400, settings.fxcm_sync_1day_incremental_outputsize)
    if interval in {"8h", "6h", "4h"}:
        return 600
    if interval in {"3h", "2h", "1h"}:
        return 1000
    return 2000


def repair_outputsize(interval: str) -> int:
    """框选修复时单次拉取条数。聚合周期要压在 sidecar 5000 根原始 K 线上限内。"""
    if interval == "1week":
        return 80
    if interval == "1day":
        return 400
    if interval == "8h":
        return 250
    if interval == "6h":
        return 300
    if interval == "4h":
        return 400
    if interval == "3h":
        return 600
    if interval == "2h":
        return 1000
    if interval == "1h":
        return 1500
    return 2000


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
    if interval == "6h":
        return timedelta(hours=6)
    if interval == "4h":
        return timedelta(hours=4)
    if interval == "3h":
        return timedelta(hours=3)
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
