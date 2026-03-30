from pydantic import BaseModel, EmailStr, field_validator
from datetime import datetime
from typing import Optional
from app.models.user import CreditType
from app.models.shl_solver import ActionType


class UserSerializer(BaseModel):
    id: int
    username: str
    email: EmailStr
    is_staff: bool
    is_superuser: bool
    # is_active: bool
    model_config = {"from_attributes": True}


class UserCreditLogSerializer(BaseModel):
    id: int
    user_id: int
    amount: int
    credit_type: CreditType
    action_type: str
    balance_after: int
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    deleted_at: Optional[datetime]

    @field_validator("action_type", mode="before")
    @classmethod
    def map_action_type(cls, v):
        mapping = {
            "SIGNUP_BONUS": "注册赠送",
            "DAILY_REFILL": "每日自动重置",
            "USE_FLASH_MODEL": "Flash模型提问",
            "USE_PRO_MODEL": "Pro模型提问",
            "USE_VISION_DIFF": "拍照纠错",
            "TOP_UP": "点数充值",
        }
        val_str = v.value if hasattr(v, "value") else str(v)
        return mapping.get(val_str, val_str)

    model_config = {"from_attributes": True}
