from datetime import datetime, timezone

PROVIDER = "FXCM"

# 历史回补最早日期：不采集此日期之前的数据；已覆盖到该日期则可转增量同步。
BACKFILL_EARLIEST_DATE = datetime(1994, 1, 1, tzinfo=timezone.utc)

SUPPORTED_INTERVALS = (
    "1min",
    "5min",
    "15min",
    "30min",
    "1h",
    "2h",
    "4h",
    "8h",
    "1day",
    "1week",
)

# 手动追赶到最新时，单次向前分页的上限轮数。
PRIORITY_FORWARD_MAX_ROUNDS = 20

ALWAYS_OPEN_ASSET_TYPES = {"digital currency"}
