from sqlalchemy import Column, String, Text, Boolean
from app.models.base import Base
from app.models.mixins import TimestampMixin


class LLMs(Base, TimestampMixin):
    __tablename__ = "llms"

    key = Column(String(100), unique=True, index=True, nullable=False)
    name = Column(String(200), nullable=False)
    tag = Column(String(100), nullable=True)
    desc = Column(Text, nullable=True)
    enabled = Column(Boolean, default=True)
