from sqlalchemy import Column, String, Boolean, Integer
from app.models.base import Base
from app.models.mixins import TimestampMixin


class TokenRecord(Base, TimestampMixin):
    __tablename__ = "token_record"

    ip = Column(String(45), nullable=False)  # supports IPv6
    token_count = Column(Integer, default=0, nullable=False)
    model = Column(String(100), nullable=True)  # 可选字段，记录使用的模型名称
    user_id = Column(Integer, nullable=False)  # 关联用户 ID
    request_path = Column(String(200), nullable=True)  # 可选字段，记录请求路径
