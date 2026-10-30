from typing import Optional

from fastapi import HTTPException, status
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.shl_solver import ActionType
from app.models.token_record import TokenRecord
from app.models.user import CreditType, User, UserCredit, UserCreditLog
from app.schemas.admin import AdminUserUpdateRequest
from app.services.wallet_service import wallet_service


class AdminService:
    def _build_pagination(self, total: int, page: int, page_size: int) -> dict:
        total_pages = (total + page_size - 1) // page_size if total else 0
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        }

    def _wallet_values(self, wallet: Optional[UserCredit]) -> tuple[int, int, int]:
        if not wallet:
            return 0, 0, 0
        free_credits = wallet.free_credits or 0
        paid_credits = wallet.paid_credits or 0
        return free_credits, paid_credits, free_credits + paid_credits

    def _user_keyword_filter(self, keyword: Optional[str]):
        if not keyword:
            return None
        normalized = keyword.strip()
        if not normalized:
            return None
        pattern = f"%{normalized}%"
        return or_(
            User.username.ilike(pattern),
            User.email.ilike(pattern),
            User.mobile_phone.ilike(pattern),
        )

    async def list_users(
        self,
        db: AsyncSession,
        page: int,
        page_size: int,
        keyword: Optional[str] = None,
        is_active: Optional[bool] = None,
        is_staff: Optional[bool] = None,
        is_superuser: Optional[bool] = None,
    ):
        filters = []
        keyword_filter = self._user_keyword_filter(keyword)
        if keyword_filter is not None:
            filters.append(keyword_filter)
        if is_active is not None:
            filters.append(User.is_active == is_active)
        if is_staff is not None:
            filters.append(User.is_staff == is_staff)
        if is_superuser is not None:
            filters.append(User.is_superuser == is_superuser)

        total_stmt = select(func.count()).select_from(User).where(*filters)
        total = (await db.execute(total_stmt)).scalar_one()

        stmt = (
            select(User, UserCredit)
            .outerjoin(UserCredit, UserCredit.user_id == User.id)
            .where(*filters)
            .order_by(User.created_at.desc(), User.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        rows = (await db.execute(stmt)).all()

        items = []
        for user, wallet in rows:
            free_credits, paid_credits, total_credits = self._wallet_values(wallet)
            items.append(
                {
                    "id": user.id,
                    "username": user.username,
                    "email": user.email,
                    "mobile_phone": user.mobile_phone,
                    "is_active": user.is_active,
                    "is_staff": user.is_staff,
                    "is_superuser": user.is_superuser,
                    "total_token_count": user.total_token_count,
                    "free_credits": free_credits,
                    "paid_credits": paid_credits,
                    "wallet_total_credits": total_credits,
                    "created_at": user.created_at,
                    "updated_at": user.updated_at,
                }
            )

        return {
            "items": items,
            "pagination": self._build_pagination(total, page, page_size),
        }

    async def get_user_detail(self, db: AsyncSession, user_id: int):
        stmt = (
            select(User, UserCredit)
            .outerjoin(UserCredit, UserCredit.user_id == User.id)
            .where(User.id == user_id)
        )
        row = (await db.execute(stmt)).first()
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="用户不存在"
            )

        user, wallet = row
        free_credits, paid_credits, total_credits = self._wallet_values(wallet)
        credit_log_count = (
            await db.execute(
                select(func.count())
                .select_from(UserCreditLog)
                .where(UserCreditLog.user_id == user_id)
            )
        ).scalar_one()
        token_record_count = (
            await db.execute(
                select(func.count())
                .select_from(TokenRecord)
                .where(TokenRecord.user_id == user_id)
            )
        ).scalar_one()

        return {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "mobile_phone": user.mobile_phone,
            "is_active": user.is_active,
            "is_staff": user.is_staff,
            "is_superuser": user.is_superuser,
            "total_token_count": user.total_token_count,
            "free_credits": free_credits,
            "paid_credits": paid_credits,
            "wallet_total_credits": total_credits,
            "created_at": user.created_at,
            "updated_at": user.updated_at,
            "deleted_at": user.deleted_at,
            "credit_log_count": credit_log_count,
            "token_record_count": token_record_count,
        }

    async def update_user(
        self,
        db: AsyncSession,
        user_id: int,
        payload: AdminUserUpdateRequest,
        operator_user_id: Optional[int] = None,
    ):
        user = await db.get(User, user_id)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="用户不存在"
            )

        updates = payload.model_dump(exclude_unset=True)

        username = updates.get("username")
        if username is not None and username != user.username:
            exists = (
                await db.execute(
                    select(User.id).where(User.username == username, User.id != user_id)
                )
            ).scalar_one_or_none()
            if exists:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="用户名已被其他用户占用",
                )
            user.username = username

        email = updates.get("email")
        if email is not None and email != user.email:
            exists = (
                await db.execute(
                    select(User.id).where(User.email == email, User.id != user_id)
                )
            ).scalar_one_or_none()
            if exists:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="邮箱已被其他用户占用",
                )
            user.email = email

        if "mobile_phone" in updates:
            mobile_phone = updates.get("mobile_phone")
            if mobile_phone and mobile_phone != user.mobile_phone:
                exists = (
                    await db.execute(
                        select(User.id).where(
                            User.mobile_phone == mobile_phone,
                            User.id != user_id,
                        )
                    )
                ).scalar_one_or_none()
                if exists:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="手机号已被其他用户占用",
                    )
            user.mobile_phone = mobile_phone

        if "is_active" in updates:
            user.is_active = updates["is_active"]
        if "is_staff" in updates:
            user.is_staff = updates["is_staff"]
        if "is_superuser" in updates:
            new_is_superuser = updates["is_superuser"]
            if (
                operator_user_id == user_id
                and not new_is_superuser
                and user.is_superuser
            ):
                remaining_superuser_count = (
                    await db.execute(
                        select(func.count())
                        .select_from(User)
                        .where(User.is_superuser.is_(True), User.id != user_id)
                    )
                ).scalar_one()
                if remaining_superuser_count == 0:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="不能取消最后一个超级管理员自己的超级管理员权限",
                    )
            user.is_superuser = new_is_superuser

        await db.commit()
        return await self.get_user_detail(db, user_id)

    async def list_wallets(
        self,
        db: AsyncSession,
        page: int,
        page_size: int,
        keyword: Optional[str] = None,
        only_with_wallet: bool = False,
    ):
        filters = []
        keyword_filter = self._user_keyword_filter(keyword)
        if keyword_filter is not None:
            filters.append(keyword_filter)
        if only_with_wallet:
            filters.append(UserCredit.id.is_not(None))

        total_stmt = (
            select(func.count())
            .select_from(User)
            .outerjoin(UserCredit, UserCredit.user_id == User.id)
            .where(*filters)
        )
        total = (await db.execute(total_stmt)).scalar_one()

        stmt = (
            select(User, UserCredit)
            .outerjoin(UserCredit, UserCredit.user_id == User.id)
            .where(*filters)
            .order_by(User.created_at.desc(), User.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        rows = (await db.execute(stmt)).all()

        items = []
        for user, wallet in rows:
            free_credits, paid_credits, total_credits = self._wallet_values(wallet)
            items.append(
                {
                    "wallet_id": wallet.id if wallet else None,
                    "user_id": user.id,
                    "username": user.username,
                    "email": user.email,
                    "mobile_phone": user.mobile_phone,
                    "free_credits": free_credits,
                    "paid_credits": paid_credits,
                    "total_credits": total_credits,
                    "last_reset_date": wallet.last_reset_date if wallet else None,
                    "wallet_created_at": wallet.created_at if wallet else None,
                    "wallet_updated_at": wallet.updated_at if wallet else None,
                }
            )

        return {
            "items": items,
            "pagination": self._build_pagination(total, page, page_size),
        }

    async def get_wallet_detail(self, db: AsyncSession, user_id: int):
        stmt = (
            select(User, UserCredit)
            .outerjoin(UserCredit, UserCredit.user_id == User.id)
            .where(User.id == user_id)
        )
        row = (await db.execute(stmt)).first()
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="用户不存在"
            )

        user, wallet = row
        free_credits, paid_credits, total_credits = self._wallet_values(wallet)
        return {
            "wallet_id": wallet.id if wallet else None,
            "user_id": user.id,
            "username": user.username,
            "email": user.email,
            "mobile_phone": user.mobile_phone,
            "free_credits": free_credits,
            "paid_credits": paid_credits,
            "total_credits": total_credits,
            "last_reset_date": wallet.last_reset_date if wallet else None,
            "wallet_created_at": wallet.created_at if wallet else None,
            "wallet_updated_at": wallet.updated_at if wallet else None,
        }

    async def recharge_wallet(self, db: AsyncSession, user_id: int, amount: int):
        balance_after = await wallet_service.recharge_credit_by_user_id(
            db=db, user_id=user_id, points=amount
        )
        wallet_detail = await self.get_wallet_detail(db, user_id)
        return {
            "user_id": wallet_detail["user_id"],
            "username": wallet_detail["username"],
            "email": wallet_detail["email"],
            "recharged_points": amount,
            "free_credits": wallet_detail["free_credits"],
            "paid_credits": wallet_detail["paid_credits"],
            "balance_after": balance_after,
        }

    async def list_credit_logs(
        self,
        db: AsyncSession,
        page: int,
        page_size: int,
        user_id: Optional[int] = None,
        keyword: Optional[str] = None,
        credit_type: Optional[CreditType] = None,
        action_type: Optional[ActionType] = None,
        start_at=None,
        end_at=None,
    ):
        filters = []
        if user_id is not None:
            filters.append(UserCreditLog.user_id == user_id)
        if credit_type is not None:
            filters.append(UserCreditLog.credit_type == credit_type)
        if action_type is not None:
            filters.append(UserCreditLog.action_type == action_type)
        if start_at is not None:
            filters.append(UserCreditLog.created_at >= start_at)
        if end_at is not None:
            filters.append(UserCreditLog.created_at <= end_at)
        keyword_filter = self._user_keyword_filter(keyword)
        if keyword_filter is not None:
            filters.append(keyword_filter)

        total_stmt = (
            select(func.count())
            .select_from(UserCreditLog)
            .outerjoin(User, User.id == UserCreditLog.user_id)
            .where(*filters)
        )
        total = (await db.execute(total_stmt)).scalar_one()

        stmt = (
            select(UserCreditLog, User.username, User.email)
            .outerjoin(User, User.id == UserCreditLog.user_id)
            .where(*filters)
            .order_by(UserCreditLog.created_at.desc(), UserCreditLog.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        rows = (await db.execute(stmt)).all()
        items = []
        for record, username, email in rows:
            items.append(
                {
                    "id": record.id,
                    "user_id": record.user_id,
                    "username": username,
                    "email": email,
                    "amount": record.amount,
                    "credit_type": record.credit_type,
                    "action_type": record.action_type,
                    "balance_after": record.balance_after,
                    "operator_user_id": record.operator_user_id,
                    "reason": record.reason,
                    "created_at": record.created_at,
                    "updated_at": record.updated_at,
                }
            )

        return {
            "items": items,
            "pagination": self._build_pagination(total, page, page_size),
        }

    async def get_credit_log_detail(self, db: AsyncSession, log_id: int):
        stmt = (
            select(UserCreditLog, User.username, User.email)
            .outerjoin(User, User.id == UserCreditLog.user_id)
            .where(UserCreditLog.id == log_id)
        )
        row = (await db.execute(stmt)).first()
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="消费记录不存在"
            )

        record, username, email = row
        return {
            "id": record.id,
            "user_id": record.user_id,
            "username": username,
            "email": email,
            "amount": record.amount,
            "credit_type": record.credit_type,
            "action_type": record.action_type,
            "balance_after": record.balance_after,
            "operator_user_id": record.operator_user_id,
            "reason": record.reason,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
        }

    async def list_token_records(
        self,
        db: AsyncSession,
        page: int,
        page_size: int,
        user_id: Optional[int] = None,
        keyword: Optional[str] = None,
        model: Optional[str] = None,
        start_at=None,
        end_at=None,
    ):
        filters = []
        if user_id is not None:
            filters.append(TokenRecord.user_id == user_id)
        if model:
            filters.append(TokenRecord.model == model)
        if start_at is not None:
            filters.append(TokenRecord.created_at >= start_at)
        if end_at is not None:
            filters.append(TokenRecord.created_at <= end_at)
        if keyword:
            normalized = keyword.strip()
            if normalized:
                pattern = f"%{normalized}%"
                filters.append(
                    or_(
                        User.username.ilike(pattern),
                        User.email.ilike(pattern),
                        TokenRecord.ip.ilike(pattern),
                        TokenRecord.request_path.ilike(pattern),
                        TokenRecord.model.ilike(pattern),
                    )
                )

        total_stmt = (
            select(func.count())
            .select_from(TokenRecord)
            .outerjoin(User, User.id == TokenRecord.user_id)
            .where(*filters)
        )
        total = (await db.execute(total_stmt)).scalar_one()

        stmt = (
            select(TokenRecord, User.username, User.email)
            .outerjoin(User, User.id == TokenRecord.user_id)
            .where(*filters)
            .order_by(TokenRecord.created_at.desc(), TokenRecord.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
        rows = (await db.execute(stmt)).all()
        items = []
        for record, username, email in rows:
            items.append(
                {
                    "id": record.id,
                    "user_id": record.user_id,
                    "username": username,
                    "email": email,
                    "ip": record.ip,
                    "request_path": record.request_path,
                    "model": record.model,
                    "token_count": record.token_count,
                    "created_at": record.created_at,
                    "updated_at": record.updated_at,
                }
            )

        return {
            "items": items,
            "pagination": self._build_pagination(total, page, page_size),
        }

    async def get_token_record_detail(self, db: AsyncSession, record_id: int):
        stmt = (
            select(TokenRecord, User.username, User.email)
            .outerjoin(User, User.id == TokenRecord.user_id)
            .where(TokenRecord.id == record_id)
        )
        row = (await db.execute(stmt)).first()
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Token 记录不存在"
            )

        record, username, email = row
        return {
            "id": record.id,
            "user_id": record.user_id,
            "username": username,
            "email": email,
            "ip": record.ip,
            "request_path": record.request_path,
            "model": record.model,
            "token_count": record.token_count,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
        }


admin_service = AdminService()
