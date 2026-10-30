from sqlalchemy import CheckConstraint, Column, DateTime, ForeignKey, Integer, func

from app.models.base import Base


class HomepageConfig(Base):
    __tablename__ = "homepage_config"
    __table_args__ = (
        CheckConstraint("id = 1", name="ck_homepage_singleton"),
        CheckConstraint("active_version IN (1, 2)", name="ck_homepage_version"),
    )

    id = Column(Integer, primary_key=True)
    active_version = Column(Integer, nullable=False, server_default="1")
    revision = Column(Integer, nullable=False, server_default="0")
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_by = Column(Integer, ForeignKey("user.id", ondelete="SET NULL"), nullable=True)
