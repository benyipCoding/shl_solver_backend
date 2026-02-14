from fastapi import FastAPI, APIRouter
from app.core.lifespan import lifespan
from app.router import auth, captcha


app = FastAPI(
    title="SHL Solver API",
    version="0.1.0",
    lifespan=lifespan,
)

# 创建一个总的 API 路由，并设置前缀
api_router = APIRouter()
api_router.include_router(auth.router)
api_router.include_router(captcha.router)

# 将总路由挂载到 app，配置公共前缀 /api
app.include_router(api_router, prefix="/api")
