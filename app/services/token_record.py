from sqlalchemy.ext.asyncio import AsyncSession
from app.models.token_record import TokenRecord


class TokenRecordService:
    async def record_token_usage(
        self,
        ip: str,
        request_path: str,
        user_id: int,
        db: AsyncSession,
        token_count: int,
        model: str = None,
    ):
        try:
            record = TokenRecord(
                ip=ip,
                token_count=token_count,
                model=model,
                user_id=user_id,
                request_path=request_path,
            )
            db.add(record)
            await db.commit()
        except Exception as e:
            print(f"Error recording token usage: {e}")
            await db.rollback()


# 导出一个模块级实例，方便在其他地方直接使用
token_record_service = TokenRecordService()
