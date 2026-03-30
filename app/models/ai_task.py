import enum
import uuid
from sqlalchemy import Column, String, BigInteger, Enum, JSON, Text
from app.models.base import Base
from app.models.mixins import TimestampMixin


class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"  # 任务刚创建，排队中/准备执行
    PROCESSING = "PROCESSING"  # 正在请求大模型厂商
    COMPLETED = "COMPLETED"  # AI 执行成功，结果已落库
    FAILED = "FAILED"  # 执行失败（需触发兜底退款）


class AITask(Base, TimestampMixin):
    __tablename__ = "ai_task"

    # 使用 UUID 作为对外暴露的任务 ID（非常重要！）
    task_id = Column(
        String(36),
        default=lambda: str(uuid.uuid4()),
        unique=True,
        index=True,
        nullable=False,
        comment="对外暴露的唯一任务ID",
    )

    user_id = Column(
        BigInteger,
        index=True,
        nullable=False,
        comment="关联用户 ID",
    )

    task_type = Column(
        String(50),
        nullable=False,
        comment="任务类型，例如：SHL_ANALYZE, CODE_VERIFY",
    )

    status = Column(
        Enum(TaskStatus),
        default=TaskStatus.PENDING,
        nullable=False,
        index=True,
        comment="任务当前状态",
    )

    result = Column(
        JSON,
        nullable=True,
        comment="任务成功时的返回结果（存放结构化的 JSON 数据）",
    )

    error_message = Column(
        Text,
        nullable=True,
        comment="任务失败时的错误信息（用于排错）",
    )

    def __repr__(self):
        return f"<AITask task_id={self.task_id} type={self.task_type} status={self.status}>"
