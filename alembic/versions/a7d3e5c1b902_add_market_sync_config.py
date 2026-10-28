"""Add history sync config with a default history cutoff.

Revision ID: a7d3e5c1b902
Revises: f3b7c2d914ae
"""

from alembic import op
import sqlalchemy as sa

revision = "a7d3e5c1b902"
down_revision = "f3b7c2d914ae"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "market_sync_config",
        sa.Column("key", sa.String(64), primary_key=True, comment="配置键"),
        sa.Column("value", sa.Text(), nullable=False, comment="配置值"),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    # 历史数据截止时间：品种周期采集到此即视为完成。已有状态的完成判定在调度器首轮或修改截止时间时执行。
    op.execute(
        "INSERT INTO market_sync_config (key, value) "
        "VALUES ('history_cutoff_at', '2026-10-01T00:00:00+00:00')"
    )


def downgrade() -> None:
    op.drop_table("market_sync_config")
