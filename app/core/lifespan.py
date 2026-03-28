from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.clients.gemini import init_gemini_client
from app.core.config import settings
from app.clients.db import init_db, close_db
from app.clients.redis_client import init_redis, close_redis

# from app.clients.openrouter import init_openrouter_client


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ===== startup =====
    init_gemini_client()
    print("✅ Gemini client initialized")
    # init_openrouter_client()
    # print("✅ OpenRouter client initialized")
    # 初始化数据库（如果配置了 DATABASE_URL ）
    if settings.database_url_async:
        init_db(settings.database_url_async)
        print("✅ Database engine initialized")
    # 初始化 Redis（如果配置了）
    if settings.redis_host:
        try:
            await init_redis()
        except Exception:
            print("⚠️ Redis init failed, continuing startup")

    yield

    # ===== shutdown =====
    # 关闭数据库连接
    try:
        await close_db()
        print("🛑 Database engine disposed")
    except Exception:
        pass

    # 关闭 Redis
    try:
        await close_redis()
        print("🛑 Redis connection closed")
    except Exception:
        pass

    print("👋 Application shutdown")
