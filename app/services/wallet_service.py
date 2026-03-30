from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException
from app.models.user import User, UserCredit, UserCreditLog, CreditType, ActionType
from datetime import datetime, timezone


class InsufficientCreditsException(Exception):
    """自定义异常：算力点数不足"""

    pass


class WalletService:
    async def recharge_credit(self, db: AsyncSession, email: str, points: int) -> int:
        """
        根据邮箱给用户充值算力
        返回充值后的总余额
        """
        # 1. 查找目标用户
        stmt = select(User).where(User.email == email)
        result = await db.execute(stmt)
        target_user = result.scalar_one_or_none()

        if not target_user:
            raise HTTPException(status_code=404, detail="找不到该邮箱对应的用户")

        # 2. 查钱包（加悲观锁防止并发问题）
        c_stmt = (
            select(UserCredit)
            .where(UserCredit.user_id == target_user.id)
            .with_for_update()
        )
        result = await db.execute(c_stmt)
        wallet = result.scalar_one_or_none()

        if not wallet:
            # 如果没有钱包但需要充值，直接新建
            wallet = UserCredit(
                user_id=target_user.id, free_credits=0, paid_credits=points
            )
            db.add(wallet)
            balance_after = points
        else:
            wallet.paid_credits += points
            balance_after = wallet.free_credits + wallet.paid_credits

        # 3. 记录流水
        log = UserCreditLog(
            user_id=target_user.id,
            amount=points,
            credit_type=CreditType.PAID,
            action_type=ActionType.TOP_UP,
            balance_after=balance_after,
        )
        db.add(log)

        await db.commit()
        return balance_after

    async def create_wallet_with_bonus(
        self, db: AsyncSession, user_id: int, bonus_amount: int
    ):
        """
        为用户创建钱包并赠送免费算力
        """
        wallet = UserCredit(user_id=user_id, free_credits=bonus_amount, paid_credits=0)
        db.add(wallet)

        log = UserCreditLog(
            user_id=user_id,
            amount=bonus_amount,
            credit_type=CreditType.FREE,
            action_type=ActionType.SIGNUP_BONUS,
            balance_after=bonus_amount,
        )
        db.add(log)
        await db.flush()
        return wallet

    async def consume_credits(
        self, db: AsyncSession, user_id: int, amount: int, action_type: ActionType
    ):
        """
        消耗算力点数 (带有悲观锁，防并发超卖)
        """
        if amount <= 0:
            return

        # 1. 开启事务并加上行锁 (FOR UPDATE)
        stmt = select(UserCredit).where(UserCredit.user_id == user_id).with_for_update()
        result = await db.execute(stmt)
        wallet = result.scalar_one_or_none()

        if not wallet:
            raise InsufficientCreditsException("算力点数不足")  # 没有钱包，视为没有点数

        # 2. 检查总余额
        if wallet.free_credits + wallet.paid_credits < amount:
            raise InsufficientCreditsException("算力点数不足")

        # 3. 计算扣费分配 (优先扣免费，不够再扣付费)
        free_to_deduct = 0
        paid_to_deduct = 0

        if wallet.free_credits >= amount:
            free_to_deduct = amount
            wallet.free_credits -= amount
        else:
            free_to_deduct = wallet.free_credits
            paid_to_deduct = amount - wallet.free_credits
            wallet.free_credits = 0
            wallet.paid_credits -= paid_to_deduct

        # 4. 记录流水 (为了严格遵守 Enum，如果是混合扣费，拆分为两条流水记录)

        # 记录免费点数扣除流水
        if free_to_deduct > 0:
            log_free = UserCreditLog(
                user_id=user_id,
                amount=-free_to_deduct,
                credit_type=CreditType.FREE,
                action_type=action_type,
                # 注意：如果是混合扣费，这里的 balance_after 是扣完免费后的中间态余额
                balance_after=wallet.free_credits
                + wallet.paid_credits
                + paid_to_deduct,
            )
            db.add(log_free)

        # 记录付费点数扣除流水
        if paid_to_deduct > 0:
            log_paid = UserCreditLog(
                user_id=user_id,
                amount=-paid_to_deduct,
                credit_type=CreditType.PAID,
                action_type=action_type,
                # 最终余额
                balance_after=wallet.free_credits + wallet.paid_credits,
            )
            db.add(log_paid)

        # 5. 提交事务释放锁
        await db.commit()

    async def refund_credits(
        self, db: AsyncSession, user_id: int, amount: int, action_type: ActionType
    ):
        """
        退还算力点数 (用于 API 调用失败时的兜底补偿/冲正)
        """
        if amount <= 0:
            return

        stmt = select(UserCredit).where(UserCredit.user_id == user_id).with_for_update()
        result = await db.execute(stmt)
        wallet = result.scalar_one_or_none()

        if wallet:
            # 为了简化逻辑，退款一律原路增加到免费点数池中
            # （因为如果大模型报错，对用户来说这笔开销就是没发生，退到 free 也是合理的）
            wallet.free_credits += amount

            # 生成一条正数流水，action_type 保持一致（例如 USE_FLASH_MODEL），金额为正，代表冲正
            refund_log = UserCreditLog(
                user_id=user_id,
                amount=amount,  # 正数
                credit_type=CreditType.FREE,
                action_type=action_type,
                balance_after=wallet.free_credits + wallet.paid_credits,
            )
            db.add(refund_log)
            await db.commit()

    async def get_balance(self, db: AsyncSession, user_id: int):
        """
        根据用户ID查询算力余额
        """
        stmt = select(UserCredit).where(UserCredit.user_id == user_id)
        result = await db.execute(stmt)
        wallet = result.scalar_one_or_none()

        if not wallet:
            # 查找该用户，判断其注册时间
            u_stmt = select(User).where(User.id == user_id)
            u_result = await db.execute(u_stmt)
            target_user = u_result.scalar_one_or_none()

            if target_user and target_user.created_at:
                created_at = target_user.created_at
                # 为了比较安全，处理 tzinfo
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)

                cutoff_date = datetime(2026, 4, 15, tzinfo=timezone.utc)
                if created_at < cutoff_date:
                    # 如果是26年4月15日前注册的，则赠送50点
                    wallet = await self.create_wallet_with_bonus(db, user_id, 50)
                    await db.commit()
                    return {
                        "free_credits": wallet.free_credits,
                        "paid_credits": wallet.paid_credits,
                        "total": wallet.free_credits + wallet.paid_credits,
                    }

            return {
                "free_credits": 0,
                "paid_credits": 0,
                "total": 0,
            }

        return {
            "free_credits": wallet.free_credits,
            "paid_credits": wallet.paid_credits,
            "total": wallet.free_credits + wallet.paid_credits,
        }

    async def get_credit_logs(
        self, db: AsyncSession, user_id: int, skip: int = 0, limit: int = 100
    ):
        """
        根据用户ID查询该用户的所有算力使用记录
        """
        stmt = (
            select(UserCreditLog)
            .where(UserCreditLog.user_id == user_id)
            .order_by(UserCreditLog.created_at.desc())
            .offset(skip)
            .limit(limit)
        )
        result = await db.execute(stmt)
        return result.scalars().all()


# 实例化为单例供其他模块引入
wallet_service = WalletService()
