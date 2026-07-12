from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class MarketReplicaResult:
    """本地 → 远程 market 数据同步结果。"""

    mode: str
    instruments_upserted: int = 0
    aliases_upserted: int = 0
    bars_upserted: int = 0
    skipped: bool = False
    errors: list[str] = field(default_factory=list)
    finished_at: datetime | None = None

    def to_summary(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "skipped": self.skipped,
            "instruments_upserted": self.instruments_upserted,
            "aliases_upserted": self.aliases_upserted,
            "bars_upserted": self.bars_upserted,
            "errors": self.errors,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
        }
