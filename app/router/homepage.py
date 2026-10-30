from fastapi import APIRouter, Depends, Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.clients.db import get_db
from app.depends.jwt_guard import verify_superuser
from app.schemas.homepage import AdminHomepageConfig, PublicHomepageConfig, PublishHomepageRequest
from app.schemas.response import APIResponse
from app.services.homepage import get_homepage_config, publish_homepage

router = APIRouter(tags=["Homepage"])


@router.get("/site/homepage", response_model=APIResponse[PublicHomepageConfig])
async def public_homepage(response: Response, db: AsyncSession = Depends(get_db)):
    response.headers["Cache-Control"] = "no-store"
    config = await get_homepage_config(db)
    return APIResponse(data={"active_version": config.active_version})


@router.get("/admin/homepage", response_model=APIResponse[AdminHomepageConfig])
async def admin_homepage(
    response: Response,
    db: AsyncSession = Depends(get_db),
    user=Depends(verify_superuser),
):
    response.headers["Cache-Control"] = "no-store"
    config = await get_homepage_config(db)
    return APIResponse(data={key: getattr(config, key) for key in ("active_version", "revision", "updated_at", "updated_by")})


@router.patch("/admin/homepage", response_model=APIResponse[AdminHomepageConfig])
async def update_homepage(
    payload: PublishHomepageRequest,
    db: AsyncSession = Depends(get_db),
    user=Depends(verify_superuser),
):
    data = await publish_homepage(db, payload, user.id)
    return APIResponse(message="首页版本已发布", data=data)
