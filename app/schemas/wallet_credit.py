from pydantic import BaseModel, EmailStr


class RechargeRequest(BaseModel):
    email: EmailStr
    points: int
