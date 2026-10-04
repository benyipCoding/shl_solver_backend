"""本地修复 → 生产同步的事务性 outbox，仅保存在源库，不参与行情复制。"""

from sqlalchemy import (
    CheckConstraint, Column, DateTime, ForeignKey, Index, Integer, String, Text,
    func, text,
)

from app.models.base import Base


class MarketRepairOutbox(Base):
    __tablename__ = "market_repair_outbox"
    __table_args__ = (
        CheckConstraint("end_at >= start_at", name="ck_market_repair_outbox_range"),
        Index(
            "ix_market_repair_outbox_pending", "provider", "id",
            postgresql_where=text("synced_at IS NULL"),
        ),
    )

    id = Column(Integer, primary_key=True)
    instrument_id = Column(
        Integer, ForeignKey("market_instrument.id", ondelete="RESTRICT"), nullable=False,
    )
    provider = Column(String(20), nullable=False)
    symbol = Column(String(64), nullable=False)
    interval = Column(String(16), nullable=False)
    price_type = Column(String(16), nullable=False)
    start_at = Column(DateTime(timezone=True), nullable=False)
    end_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    attempts = Column(Integer, nullable=False, server_default="0")
    last_attempt_at = Column(DateTime(timezone=True))
    last_error = Column(Text)
    synced_at = Column(DateTime(timezone=True))
    bars_upserted = Column(Integer, nullable=False, server_default="0")
    bars_deleted = Column(Integer, nullable=False, server_default="0")
