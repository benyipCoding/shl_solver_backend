from fastapi import FastAPI, APIRouter, Request
from app.core.lifespan import lifespan
from app.core.config import settings
from app.router import (
    auth,
    captcha,
    shl_analyze,
    llms,
    user,
    ai_doctor,
    excel_workbench,
    shl_solver,
    wallet_credit,
)
from app.middlewares.auth import UserAuthMiddleware
from app.middlewares.real_ip import RealIPMiddleware
from app.core.exceptions import global_exception_handler


app = FastAPI(
    title="SHL Solver API",
    version="0.1.0",
    lifespan=lifespan,
)


# ==========================================
# 全局异常捕获 - 注册
# ==========================================
app.add_exception_handler(Exception, global_exception_handler)


app.add_middleware(RealIPMiddleware)
# 添加中间件，解析 JWT 并注入 user 到 request.state
app.add_middleware(UserAuthMiddleware)


# 创建一个总的 API 路由，并设置前缀
api_router = APIRouter()
api_router.include_router(auth.router)
api_router.include_router(captcha.router)
api_router.include_router(shl_analyze.router)
api_router.include_router(llms.router)
api_router.include_router(user.router)
api_router.include_router(ai_doctor.router)
api_router.include_router(excel_workbench.router)
api_router.include_router(shl_solver.router)
api_router.include_router(wallet_credit.router)

# 将总路由挂载到 app，配置公共前缀 /api_v1
app.include_router(api_router, prefix="/api_v1")
