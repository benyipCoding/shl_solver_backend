import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def utc_now() -> datetime:
    """返回当前 UTC 时间，统一全模块的时间基准。"""
    return datetime.now(tz=timezone.utc)


@dataclass
class FXCMMarketSyncResult:
    reason: str
    skipped: bool = False
    metadata_synced: bool = False
    synced_instruments: int = 0
    bootstrap_states: int = 0
    processed_states: int = 0
    succeeded_states: int = 0
    failed_states: int = 0
    rows_upserted: int = 0
    errors: list[str] = field(default_factory=list)
    finished_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """将同步结果对象转换为可序列化字典。"""
        return {
            "reason": self.reason,
            "skipped": self.skipped,
            "metadata_synced": self.metadata_synced,
            "synced_instruments": self.synced_instruments,
            "bootstrap_states": self.bootstrap_states,
            "processed_states": self.processed_states,
            "succeeded_states": self.succeeded_states,
            "failed_states": self.failed_states,
            "rows_upserted": self.rows_upserted,
            "errors": list(self.errors),
            "finished_at": self.finished_at,
        }


class PriorityForwardSyncError(Exception):
    """手动优先同步（追赶 / 框选修复）的业务错误，携带 HTTP 状态码。"""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message


@dataclass
class PrioritySyncJob:
    """最高优先级同步任务：向前追赶，或按时间段修复已有 K 线。"""

    symbol: str
    interval: str
    future: asyncio.Future
    requested_at: datetime = field(default_factory=utc_now)
    kind: str = "forward"
    start_at: datetime | None = None
    end_at: datetime | None = None


# 兼容旧名称，避免外部 import 断裂。
PriorityForwardSyncJob = PrioritySyncJob
