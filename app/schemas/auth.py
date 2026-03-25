from pydantic import BaseModel, EmailStr, Field, ConfigDict


class AuthRequest(BaseModel):
    email: EmailStr
    password: str


class ForgotPasswordRequest(BaseModel):
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str
    confirm_password: str


class CaptchaRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    captcha_id: str = Field(..., alias="captchaId")
    user_input: str = Field(..., alias="userInput")
