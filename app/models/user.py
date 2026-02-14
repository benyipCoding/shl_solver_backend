from sqlalchemy import Column, String, Boolean, Integer
from app.models.base import Base
from app.models.mixins import TimestampMixin


class User(Base, TimestampMixin):
    __tablename__ = "user"

    username = Column(String(100), unique=True, index=True, nullable=False)
    email = Column(String(200), unique=True, index=True, nullable=False)
    password = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True)
    mobile_phone = Column(String(20), unique=True, index=True, nullable=True)
    total_token_count = Column(Integer, default=0, nullable=False)
