from sqlalchemy import Column, String, Boolean, Integer
from app.models.base import Base
from app.models.mixins import TimestampMixin
import enum


class SHLSolverHistory(Base, TimestampMixin):
    __tablename__ = "shl_solver_history"

    image_urls = Column(String, nullable=False)  # 存储图片 URL，多个 URL 可用逗号分隔
    token_count = Column(Integer, default=0, nullable=False)
    model = Column(String(100), nullable=True)  # 可选字段，记录使用的模型名称
    user_id = Column(Integer, nullable=False)  # 关联用户 ID
    result_json = Column(String, nullable=True)  # 存储 SHL 求解结果的 JSON 字符串
    total_test_cases = Column(
        Integer, default=0, nullable=False
    )  # 可选字段，记录测试用例总数
    passed_test_cases = Column(
        Integer, default=0, nullable=False
    )  # 可选字段，记录通过的测试用例数量
    status = Column(
        String(50), default="pending", nullable=False
    )  # 记录求解状态，如 pending, completed, failed
    error_message = Column(
        String, nullable=True
    )  # 可选字段，记录求解过程中出现的错误信息
    is_readed = Column(Boolean, default=False, nullable=True)  # 记录用户是否已查看结果


class ActionType(str, enum.Enum):
    SIGNUP_BONUS = "SIGNUP_BONUS"
    DAILY_REFILL = "DAILY_REFILL"
    USE_FLASH_MODEL = "USE_FLASH_MODEL"
    USE_PRO_MODEL = "USE_PRO_MODEL"
    USE_VISION_DIFF = "USE_VISION_DIFF"
    TOP_UP = "TOP_UP"
