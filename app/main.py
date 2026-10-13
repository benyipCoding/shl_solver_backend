from fastapi import FastAPI, APIRouter, Request
from app.core.lifespan import lifespan
from app.core.config import settings
from app.router import (
    admin,
    auth,
    captcha,
    shl_analyze,
    llms,
    ff14,
    ff14_v2,
    user,
    ai_doctor,
    excel_workbench,
    market_master,
    market_realtime,
    shl_solver,
    wallet_credit,
)
from app.middlewares.auth import UserAuthMiddleware
from app.middlewares.real_ip import RealIPMiddleware
from app.core.exceptions import global_exception_handler, validation_exception_handler
from fastapi.exceptions import RequestValidationError


openapi_tags = [
    {
        "name": "Admin",
        "description": "仅超级管理员可访问的后台管理接口。认证支持 Cookie 中的 access_token，或 Authorization: Bearer <token> 请求头。",
    }
]


app = FastAPI(
    title="SHL Solver API",
    version="0.1.0",
    description=(
        "SHL Solver 后端接口文档。\n\n"
        "- 全部接口统一挂载在 /api_v1 前缀下。\n"
        "- 需要登录的接口支持两种认证方式：浏览器 Cookie(access_token) 或 Authorization: Bearer <token>。\n"
        "- /api_v1/admin 下的接口仅允许 is_superuser=true 的用户访问。"
    ),
    openapi_tags=openapi_tags,
    lifespan=lifespan,
)


# ==========================================
# 全局异常捕获 - 注册
# ==========================================
app.add_exception_handler(Exception, global_exception_handler)
app.add_exception_handler(RequestValidationError, validation_exception_handler)

app.add_middleware(RealIPMiddleware)
# 添加中间件，解析 JWT 并注入 user 到 request.state
app.add_middleware(UserAuthMiddleware)


# 创建一个总的 API 路由，并设置前缀
api_router = APIRouter()
api_router.include_router(auth.router)
api_router.include_router(captcha.router)
api_router.include_router(admin.router)
api_router.include_router(shl_analyze.router)
api_router.include_router(llms.router)
api_router.include_router(ff14.router)
api_router.include_router(ff14_v2.router)
api_router.include_router(user.router)
api_router.include_router(ai_doctor.router)
api_router.include_router(excel_workbench.router)
api_router.include_router(market_master.router)
api_router.include_router(market_realtime.router)
api_router.include_router(shl_solver.router)
api_router.include_router(wallet_credit.router)

# 将总路由挂载到 app，配置公共前缀 /api_v1
app.include_router(api_router, prefix="/api_v1")
