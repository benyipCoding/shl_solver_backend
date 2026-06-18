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
