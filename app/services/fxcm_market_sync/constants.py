from datetime import datetime, timezone

PROVIDER = "FXCM"

# 历史回补最早日期：不采集此日期之前的数据。
BACKFILL_EARLIEST_DATE = datetime(1994, 1, 1, tzinfo=timezone.utc)

SUPPORTED_INTERVALS = (
    "1min",
    "5min",
    "15min",
    "30min",
    "1h",
    "2h",
    "3h",
    "4h",
    "6h",
    "8h",
    "1day",
    "1week",
)

# 手动追赶到最新时，单次向前分页的上限轮数。
PRIORITY_FORWARD_MAX_ROUNDS = 20

# 框选修复时，按时间窗口分页拉取的上限轮数。
PRIORITY_REPAIR_MAX_ROUNDS = 120

# 框选修复单次 UPSERT / DELETE 的批大小。
REPAIR_DB_CHUNK_SIZE = 1000

# 历史数据截止时间默认值（可在 sync-status 页面修改）：品种周期向前采集到截止时间、
# 向后回补到 BACKFILL_EARLIEST_DATE，即视为采集完成，定时任务不再采集。
DEFAULT_HISTORY_CUTOFF = datetime(2026, 10, 1, tzinfo=timezone.utc)

# 定时历史采集每次向前或回补拉取的 K 线条数。
HISTORY_BATCH_SIZE = 2000

SYNC_MODE_BACKFILL = "BACKFILL"
SYNC_MODE_COMPLETED = "COMPLETED"
