"""Add saved references to shared backtest sessions.

Revision ID: e2f4a91c608b
Revises: d8a2f679b031
"""

from alembic import op
import sqlalchemy as sa

revision = "e2f4a91c608b"
down_revision = "d8a2f679b031"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "market_backtest_bookmark",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("session_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        sa.Column("deleted_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(["user_id"], ["user.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["session_id"], ["market_backtest_session.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("user_id", "session_id", name="uq_market_backtest_bookmark_user_session"),
    )
    op.create_index(
        "ix_market_backtest_bookmark_user_created", "market_backtest_bookmark", ["user_id", "created_at"],
    )
    op.create_index("ix_market_backtest_bookmark_id", "market_backtest_bookmark", ["id"])


def downgrade() -> None:
    op.drop_index("ix_market_backtest_bookmark_id", table_name="market_backtest_bookmark")
    op.drop_index("ix_market_backtest_bookmark_user_created", table_name="market_backtest_bookmark")
    op.drop_table("market_backtest_bookmark")
