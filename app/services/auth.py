from datetime import datetime, timedelta
from typing import Optional
import hashlib

import jwt
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.user import User


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class AuthService:
    async def get_by_email(self, db: AsyncSession, email: str) -> Optional[User]:
        stmt = select(User).where(User.email == email)
        result = await db.execute(stmt)
        return result.scalars().first()

    async def create_user(
        self, db: AsyncSession, username: str, email: str, password: str
    ) -> User:
        # Avoid bcrypt 72 bytes limitation by hashing the password first
        hashed_input = hashlib.sha256(password.encode("utf-8")).hexdigest()
        hashed = pwd_context.hash(hashed_input)
        user = User(username=username, email=email, password=hashed)
        db.add(user)
        await db.commit()
        await db.refresh(user)
        return user

    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        # Try verifying with new method (hashed input)
        try:
            hashed_input = hashlib.sha256(plain_password.encode("utf-8")).hexdigest()
            if pwd_context.verify(hashed_input, hashed_password):
                return True
        except Exception:
            pass

        # Fallback for old passwords (if short enough)
        try:
            return pwd_context.verify(plain_password, hashed_password)
        except ValueError:
            # If standard verify fails due to long password, it's definitely incorrect for old scheme
            return False

    async def authenticate_user(
        self, db: AsyncSession, email: str, password: str
    ) -> Optional[User]:
        user = await self.get_by_email(db, email)
        if user is None:
            return None
        if not self.verify_password(password, user.password):
            return None
        return user

    def create_access_token(self, data: dict) -> str:
        to_encode = data.copy()
        expire = datetime.now() + timedelta(
            minutes=settings.jwt_access_token_expires_minutes
        )
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(
            to_encode, settings.jwt_secret_key, algorithm=settings.jwt_algorithm
        )
        return encoded_jwt


auth_service = AuthService()
