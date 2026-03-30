from typing import AsyncGenerator, Optional

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    create_async_engine,
    async_sessionmaker,
)
from sqlalchemy.orm import declarative_base
from app.core.config import settings

engine: Optional[AsyncEngine] = None
async_session: Optional[async_sessionmaker[AsyncSession]] = None

Base = declarative_base()


def init_db(database_url: str) -> None:
    """Create async engine and sessionmaker. Call this at app startup."""
    global engine, async_session
    engine = create_async_engine(settings.database_url_async, future=True, echo=False)
    async_session = async_sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )
    return async_session


async def close_db() -> None:
    """Dispose engine. Call this at app shutdown."""
    global engine
    if engine:
        await engine.dispose()


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency that yields an `AsyncSession`."""
    assert async_session is not None, "DB not initialized. Call init_db first."
    async with async_session() as session:
        yield session
