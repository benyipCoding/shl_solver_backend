"""Persist the published homepage version; preserve V1 on upgrade.

Revision ID: b1c8d4e6f209
Revises: a7d3e5c1b902
"""
from alembic import op
import sqlalchemy as sa

revision = "b1c8d4e6f209"
down_revision = "a7d3e5c1b902"
branch_labels = None
depends_on = None


def upgrade():
    table = op.create_table(
        "homepage_config",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("active_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("revision", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_by", sa.Integer(), sa.ForeignKey("user.id", ondelete="SET NULL"), nullable=True),
        sa.CheckConstraint("id = 1", name="ck_homepage_singleton"),
        sa.CheckConstraint("active_version IN (1, 2)", name="ck_homepage_version"),
    )
    op.bulk_insert(table, [{"id": 1, "active_version": 1, "revision": 0}])


def downgrade():
    op.drop_table("homepage_config")
