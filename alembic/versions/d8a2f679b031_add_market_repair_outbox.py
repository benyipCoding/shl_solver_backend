"""Add durable market repair replication outbox.

Revision ID: d8a2f679b031
Revises: c4e8f1a09b73
"""

from alembic import op
import sqlalchemy as sa

revision = "d8a2f679b031"
down_revision = "c4e8f1a09b73"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "market_repair_outbox",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("provider", sa.String(20), nullable=False),
        sa.Column("symbol", sa.String(64), nullable=False),
        sa.Column("interval", sa.String(16), nullable=False),
        sa.Column("price_type", sa.String(16), nullable=False),
        sa.Column("start_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.Text()),
        sa.Column("synced_at", sa.DateTime(timezone=True)),
        sa.Column("bars_upserted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("bars_deleted", sa.Integer(), nullable=False, server_default="0"),
        sa.ForeignKeyConstraint(["instrument_id"], ["market_instrument.id"], ondelete="RESTRICT"),
        sa.CheckConstraint("end_at >= start_at", name="ck_market_repair_outbox_range"),
    )
    op.create_index(
        "ix_market_repair_outbox_pending", "market_repair_outbox", ["provider", "id"],
        postgresql_where=sa.text("synced_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_market_repair_outbox_pending", table_name="market_repair_outbox")
    op.drop_table("market_repair_outbox")
