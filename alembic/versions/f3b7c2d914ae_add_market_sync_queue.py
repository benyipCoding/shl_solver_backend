"""Add manual historical sync queue.

Revision ID: f3b7c2d914ae
Revises: e2f4a91c608b
"""

from alembic import op
import sqlalchemy as sa

revision = "f3b7c2d914ae"
down_revision = "e2f4a91c608b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "market_sync_queue_item",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("instrument_id", sa.Integer(), nullable=False, comment="品种 ID"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["instrument_id"], ["market_instrument.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("instrument_id", name="uq_market_sync_queue_item_instrument"),
    )
    op.create_index(
        op.f("ix_market_sync_queue_item_id"), "market_sync_queue_item", ["id"],
    )
    # 保留升级前的采集范围：已有启用同步状态的品种进入队列。
    op.execute(
        """
        INSERT INTO market_sync_queue_item (instrument_id)
        SELECT DISTINCT s.instrument_id
        FROM market_bar_sync_state s
        JOIN market_instrument i ON i.id = s.instrument_id
        WHERE s.provider = 'FXCM' AND s.enabled IS TRUE AND i.is_active IS TRUE
        """
    )
    # 每日品类轮换已移除，被轮换推迟到次日的状态立即恢复调度。
    op.execute(
        """
        UPDATE market_bar_sync_state
        SET next_sync_from = now(), last_status = 'IDLE', last_error = NULL
        WHERE last_status = 'SKIPPED'
          AND last_error = 'Skipped by daily category rotation policy'
        """
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_market_sync_queue_item_id"), table_name="market_sync_queue_item")
    op.drop_table("market_sync_queue_item")
