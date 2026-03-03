from sqlalchemy.ext.asyncio import AsyncSession
from app.models.token_record import TokenRecord
from fastapi import Request

# from sqlalchemy import select


class TokenRecordService:
    async def record_token_usage(
        self,
        request: Request,
        db: AsyncSession,
        token_count: int,
        model: str = None,
    ):
        try:
            record = TokenRecord(
                ip=getattr(request.state, "real_ip", request.client.host),
                token_count=token_count,
                model=model,
                user_id=(
                    getattr(request.state, "user", None).id
                    if getattr(request.state, "user", None)
                    else None
                ),
                request_path=request.url.path,
            )
            db.add(record)
            await db.commit()
        except Exception as e:
            print(f"Error recording token usage: {e}")
            await db.rollback()


# 导出一个模块级实例，方便在其他地方直接使用
token_record_service = TokenRecordService()
