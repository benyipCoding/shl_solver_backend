import enum
from sqlalchemy import Column, String, Boolean, Integer, Date, BigInteger, Enum
from app.models.base import Base
from app.models.mixins import TimestampMixin
from app.models.shl_solver import ActionType


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


class CreditType(str, enum.Enum):
    FREE = "FREE"
    PAID = "PAID"


class UserCreditLog(Base, TimestampMixin):
    __tablename__ = "user_credit_log"

    user_id = Column(
        BigInteger,
        index=True,
        nullable=False,
        comment="关联用户 ID",
    )
    amount = Column(
        Integer,
        nullable=False,
        comment="变动额度（正数代表增加，负数代表消耗，如 -5）",
    )
    credit_type = Column(
        Enum(CreditType),
        nullable=False,
        comment="消耗的是哪种点数：FREE 还是 PAID",
    )
    action_type = Column(
        Enum(ActionType),
        nullable=False,
        comment="动作类型：SIGNUP_BONUS, DAILY_REFILL, USE_FLASH_MODEL, USE_PRO_MODEL, USE_VISION_DIFF, TOP_UP (充值)",
    )
    balance_after = Column(
        Integer,
        nullable=False,
        comment="变动后的钱包总余额（免费+付费），用于快速核对账目防篡改",
    )

    def __repr__(self):
        return (
            f"<UserCreditLog id={self.id} user_id={self.user_id} amount={self.amount}>"
        )
