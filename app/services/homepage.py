from fastapi import HTTPException
from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.homepage import HomepageConfig
from app.schemas.homepage import PublishHomepageRequest


async def get_homepage_config(db: AsyncSession):
    config = (await db.execute(select(HomepageConfig).where(HomepageConfig.id == 1))).scalar_one_or_none()
    if config is None:
        raise HTTPException(503, "首页版本配置尚未初始化，请先执行数据库迁移")
    return config


async def publish_homepage(db: AsyncSession, payload: PublishHomepageRequest, operator_id: int):
    # Compare-and-swap prevents a stale admin tab from overwriting another publication.
    result = await db.execute(
        update(HomepageConfig)
        .where(HomepageConfig.id == 1, HomepageConfig.revision == payload.expected_revision)
        .values(
            active_version=payload.active_version,
            revision=HomepageConfig.revision + 1,
            updated_at=func.now(),
            updated_by=operator_id,
        )
        .returning(HomepageConfig)
    )
    config = result.scalar_one_or_none()
    if config is None:
        await db.rollback()
        raise HTTPException(409, "首页版本已被其他管理员修改，请刷新后重试")
    # Capture values before commit even for sessions configured with expire_on_commit.
    data = {key: getattr(config, key) for key in ("active_version", "revision", "updated_at", "updated_by")}
    await db.commit()
    return data
