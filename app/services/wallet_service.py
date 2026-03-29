from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from fastapi import HTTPException
from app.models.user import UserCredit, UserCreditLog, CreditType, ActionType


class InsufficientCreditsException(Exception):
    """自定义异常：算力点数不足"""

    pass


class WalletService:
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
            wallet = await self.create_wallet_with_bonus(db, user_id, 50)

        return {
            "free_credits": wallet.free_credits,
            "paid_credits": wallet.paid_credits,
            "total": wallet.free_credits + wallet.paid_credits,
        }


# 实例化为单例供其他模块引入
wallet_service = WalletService()
