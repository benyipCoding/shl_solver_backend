from fastapi import Request
from fastapi import HTTPException, status


async def verify_user(request: Request):
    user = getattr(request.state, "user", None)
    if not user or not user.is_active:
        # 必须通过抛出异常来中断请求
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized: User is not logged in",
        )
    return user
