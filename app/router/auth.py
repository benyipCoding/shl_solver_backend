from fastapi import APIRouter, Depends, Response, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from app.clients.db import get_db
from app.schemas.response import APIResponse
from app.schemas.auth import AuthRequest, ForgotPasswordRequest, ResetPasswordRequest
from app.services.auth import auth_service
from app.core.config import settings
from app.clients.redis_client import get_redis
import redis.asyncio as redis
from fastapi import Request
from app.schemas.user import UserSerializer
from fastapi import HTTPException, status
import uuid
from app.utils.email_helper import send_password_reset_email


router = APIRouter(prefix="/auth", tags=["Auth"])

ACCESS_TOKEN_KEY = "access_token"
REFRESH_TOKEN_KEY = "refresh_token"


@router.post("/register", response_model=APIResponse[UserSerializer])
async def register(
    payload: AuthRequest, response: Response, db: AsyncSession = Depends(get_db)
):
    # Only email & password provided by frontend. Use email local part as username.
    existing = await auth_service.get_by_email(db, payload.email)
    if existing:
        return APIResponse(code=409, message="Email already registered")

    username = payload.email.split("@")[0]
    user = await auth_service.create_user(
        db, username=username, email=payload.email, password=payload.password
    )
    return APIResponse(data=user)


@router.post("/login", response_model=APIResponse[UserSerializer])
async def login(
    payload: AuthRequest,
    response: Response,
    db: AsyncSession = Depends(get_db),
    redis: redis.Redis = Depends(get_redis),
):
    user = await auth_service.authenticate_user(db, payload.email, payload.password)

    data = {"sub": str(user.id), "email": user.email}
    access_token = auth_service.create_access_token(data)
    refresh_token = auth_service.create_refresh_token(data)

    await redis.set(
        f"refresh_token:{refresh_token}",
        str(user.id),
        ex=settings.jwt_refresh_token_expires_days * 24 * 3600,
    )

    response.set_cookie(
        key=ACCESS_TOKEN_KEY,
        value=access_token,
        httponly=settings.cookie_httponly,
        secure=settings.cookie_secure,
        samesite=settings.cookie_samesite,
        max_age=settings.jwt_access_token_expires_minutes * 60,
    )
    response.set_cookie(
        key=REFRESH_TOKEN_KEY,
        value=refresh_token,
        httponly=settings.cookie_httponly,
        secure=settings.cookie_secure,
        samesite=settings.cookie_samesite,
        max_age=settings.jwt_refresh_token_expires_days * 24 * 3600,
    )

    return APIResponse(data=user)


@router.post("/logout", response_model=APIResponse)
async def logout(
    request: Request, response: Response, redis: redis.Redis = Depends(get_redis)
):
    refresh_token = request.cookies.get(REFRESH_TOKEN_KEY)

    # 如果找到了 refresh_token，将其从 Redis 中作废
    if refresh_token:
        await redis.delete(f"refresh_token:{refresh_token}")

    # 清除客户端的 Cookie
    response.delete_cookie(
        key=ACCESS_TOKEN_KEY,
        secure=settings.cookie_secure,
        httponly=settings.cookie_httponly,
        samesite=settings.cookie_samesite,
    )
    response.delete_cookie(
        key=REFRESH_TOKEN_KEY,
        secure=settings.cookie_secure,
        httponly=settings.cookie_httponly,
        samesite=settings.cookie_samesite,
    )

    return APIResponse(message="Successfully logged out")


@router.post("/refresh", response_model=APIResponse)
async def refresh_token(
    request: Request,
    response: Response,
    redis: redis.Redis = Depends(get_redis),
):
    refresh_token = request.cookies.get(REFRESH_TOKEN_KEY)
    if not refresh_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token失效",
        )
    new_access_token = await auth_service.refresh_access_token(refresh_token, redis)
    if not new_access_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token失败",
        )

    response.set_cookie(
        key=ACCESS_TOKEN_KEY,
        value=new_access_token,
        httponly=settings.cookie_httponly,
        secure=settings.cookie_secure,
        samesite=settings.cookie_samesite,
        max_age=settings.jwt_access_token_expires_minutes * 60,
    )

    return APIResponse(message="Access token refreshed")


@router.post("/forgot-password", response_model=APIResponse)
async def forgot_password(
    payload: ForgotPasswordRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    redis: redis.Redis = Depends(get_redis),
):
    """
    发送密码重置邮件
    """
    # 查找用户
    user = await auth_service.get_by_email(db, payload.email)

    # 即使用户不存在，也返回成功，避免邮箱暴力枚举
    if user:
        # 生成一次性 Token (UUID)
        token = str(uuid.uuid4())

        # 将 Token 存入 Redis，有效期 15 分钟 (900秒)
        # Key: reset_token:{token} -> Value: user_id
        await redis.set(f"reset_token:{token}", str(user.id), ex=15 * 60)

        # 构建重置链接 (前端路由)
        reset_link = f"{settings.frontend_base_url}/reset-password?token={token}"

        # 异步发送邮件啊
        background_tasks.add_task(send_password_reset_email, user.email, reset_link)

    return APIResponse(message="如果该邮箱已注册，我们已发送重置密码链接，请查收邮件。")


@router.post("/reset-password", response_model=APIResponse)
async def reset_password(
    payload: ResetPasswordRequest,
    db: AsyncSession = Depends(get_db),
    redis: redis.Redis = Depends(get_redis),
):
    """
    使用 Token 重置密码
    """
    if payload.new_password != payload.confirm_password:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="两次输入的密码不一致",
        )

    # 验证 Token
    user_id = await redis.get(f"reset_token:{payload.token}")
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="重置链接无效或已过期",
        )

    # 获取用户并更新密码
    user = await auth_service.get_by_id(db, int(user_id))
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="用户不存在",
        )

    await auth_service.update_password(db, user, payload.new_password)

    # 消费掉 Token
    await redis.delete(f"reset_token:{payload.token}")

    return APIResponse(message="密码重置成功，请使用新密码登录")
