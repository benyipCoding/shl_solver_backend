from sqlalchemy import Column, String, Boolean, Integer, Date
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
    is_staff = Column(Boolean, default=False)  # 是否是员工账号
    is_superuser = Column(Boolean, default=False)  # 是否是超级管理员账号

    def __repr__(self):
        return f"<User id={self.id} username={self.username} email={self.email}>"


class UserCredit(Base, TimestampMixin):
    __tablename__ = "user_credit"

    user_id = Column(
        Integer,
        unique=True,
        index=True,
        nullable=False,
        comment="关联用户",
    )
    free_credits = Column(
        Integer,
        default=0,
        nullable=False,
        comment="免费点数（比如每日重置的额度，或一次性赠送额度）",
    )
    paid_credits = Column(
        Integer,
        default=0,
        nullable=False,
        comment="付费点数（用户真金白银买的，永不过期）",
    )
    last_reset_date = Column(Date, nullable=True, comment="记录上次重置免费点数的日期")

    def __repr__(self):
        return f"<UserCredit id={self.id} user_id={self.user_id}>"
