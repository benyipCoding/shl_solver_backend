from fastapi import APIRouter, Depends, Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.clients.db import get_db
from app.schemas.response import APIResponse
from app.schemas.auth import AuthRequest, UserSerializer
from app.services.auth import auth_service


router = APIRouter(prefix="/auth", tags=["Auth"])

ACCESS_TOKEN_KEY = "access_token"


@router.post("/register", response_model=APIResponse[UserSerializer])
async def register(
    payload: AuthRequest, response: Response, db: AsyncSession = Depends(get_db)
):
    # Only email & password provided by frontend. Use email local part as username.
    existing = await auth_service.get_by_email(db, payload.email)
    if existing:
        return APIResponse(code=400, message="Email already registered")

    username = payload.email.split("@")[0]
    user = await auth_service.create_user(
        db, username=username, email=payload.email, password=payload.password
    )
    token = auth_service.create_access_token({"sub": str(user.id), "email": user.email})
    response.set_cookie(
        key=ACCESS_TOKEN_KEY,
        value=token,
        httponly=True,
        secure=False,
        samesite="lax",
    )
    return APIResponse(data=user)


@router.post("/login", response_model=APIResponse[UserSerializer])
async def login(
    payload: AuthRequest, response: Response, db: AsyncSession = Depends(get_db)
):
    user = await auth_service.authenticate_user(db, payload.email, payload.password)
    if not user:
        return APIResponse(code=401, message="Invalid credentials")
    token = auth_service.create_access_token({"sub": str(user.id), "email": user.email})
    response.set_cookie(
        key=ACCESS_TOKEN_KEY,
        value=token,
        httponly=True,
        secure=False,
        samesite="lax",
    )
    return APIResponse(data=user)
