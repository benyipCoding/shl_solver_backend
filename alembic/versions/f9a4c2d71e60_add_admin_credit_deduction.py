"""Add manual deductions and administrator audit metadata.

Revision ID: f9a4c2d71e60
Revises: b1c8d4e6f209
"""
from alembic import op
import sqlalchemy as sa

revision = "f9a4c2d71e60"
down_revision = "b1c8d4e6f209"
branch_labels = None
depends_on = None

OLD_ACTIONS = ("SIGNUP_BONUS", "DAILY_REFILL", "USE_FLASH_MODEL", "USE_PRO_MODEL", "USE_VISION_DIFF", "TOP_UP")


def upgrade():
    dialect = op.get_bind().dialect.name
    if dialect == "postgresql":
        with op.get_context().autocommit_block():
            op.execute("ALTER TYPE actiontype ADD VALUE IF NOT EXISTS 'ADMIN_DEDUCT'")
    elif dialect in ("mysql", "mariadb"):
        op.alter_column("user_credit_log", "action_type",
                        existing_type=sa.Enum(*OLD_ACTIONS, name="actiontype"),
                        type_=sa.Enum(*OLD_ACTIONS, "ADMIN_DEDUCT", name="actiontype"),
                        existing_nullable=False)
    op.add_column("user_credit_log", sa.Column("operator_user_id", sa.Integer(), nullable=True))
    op.add_column("user_credit_log", sa.Column("reason", sa.String(500), nullable=True))


def downgrade():
    # Do not silently discard audit history when rolling back an in-use feature.
    used = op.get_bind().execute(sa.text("SELECT COUNT(*) FROM user_credit_log WHERE action_type = 'ADMIN_DEDUCT'")).scalar_one()
    if used:
        raise RuntimeError("Cannot remove deduction audit fields while ADMIN_DEDUCT records exist")
    op.drop_column("user_credit_log", "reason")
    op.drop_column("user_credit_log", "operator_user_id")
    if op.get_bind().dialect.name in ("mysql", "mariadb"):
        op.alter_column("user_credit_log", "action_type",
                        existing_type=sa.Enum(*OLD_ACTIONS, "ADMIN_DEDUCT", name="actiontype"),
                        type_=sa.Enum(*OLD_ACTIONS, name="actiontype"), existing_nullable=False)
    # PostgreSQL cannot drop individual enum values; the unused value is harmless.
