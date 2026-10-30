"""Add OpenAI SHL models and replace Gemini Flash.

Revision ID: a31f6c8d902e
Revises: f9a4c2d71e60
"""
from alembic import op
import sqlalchemy as sa


revision = "a31f6c8d902e"
down_revision = "f9a4c2d71e60"
branch_labels = None
depends_on = None

OLD_ACTIONS = (
    "SIGNUP_BONUS", "DAILY_REFILL", "USE_FLASH_MODEL", "USE_PRO_MODEL",
    "USE_VISION_DIFF", "TOP_UP", "ADMIN_DEDUCT",
)
OLD_FLASH = "gemini-3-flash-preview"
NEW_FLASH = "gemini-3.8-flash"
MODELS = (
    (NEW_FLASH, "Gemini 3.8 Flash", "默认 · 10点", "SHL 分析默认模型，每次分析 10 算力点数。"),
    ("gemini-4-argon", "Gemini 4 Argon", "30点", "每次分析 30 算力点数。"),
    ("gpt-5.6-sol", "GPT-5.6 Sol", "OpenAI · 60点", "每次分析 60 算力点数。"),
    ("gpt-6-astra", "GPT-6 Astra", "OpenAI · 100点", "每次分析 100 算力点数。"),
)
llms = sa.table(
    "llms", sa.column("id", sa.Integer), sa.column("key", sa.String),
    sa.column("name", sa.String), sa.column("tag", sa.String),
    sa.column("desc", sa.Text), sa.column("enabled", sa.Boolean),
)


def upgrade():
    db = op.get_bind()
    if db.dialect.name == "postgresql":
        with op.get_context().autocommit_block():
            op.execute("ALTER TYPE actiontype ADD VALUE IF NOT EXISTS 'USE_OPENAI_MODEL'")
    elif db.dialect.name in ("mysql", "mariadb"):
        op.alter_column(
            "user_credit_log", "action_type",
            existing_type=sa.Enum(*OLD_ACTIONS, name="actiontype"),
            type_=sa.Enum(*OLD_ACTIONS, "USE_OPENAI_MODEL", name="actiontype"),
            existing_nullable=False,
        )

    # Keep the previous Flash ID so saved selections still resolve after upgrade.
    new_flash_id = db.scalar(sa.select(llms.c.id).where(llms.c.key == NEW_FLASH))
    if new_flash_id is None:
        db.execute(llms.update().where(llms.c.key == OLD_FLASH).values(key=NEW_FLASH))
    else:
        db.execute(llms.update().where(llms.c.key == OLD_FLASH).values(enabled=False))

    for key, name, tag, desc in MODELS:
        values = dict(name=name, tag=tag, desc=desc, enabled=True)
        model_id = db.scalar(sa.select(llms.c.id).where(llms.c.key == key))
        if model_id is None:
            db.execute(llms.insert().values(key=key, **values))
        else:
            db.execute(llms.update().where(llms.c.id == model_id).values(**values))


def downgrade():
    db = op.get_bind()
    used = db.scalar(sa.text("SELECT COUNT(*) FROM user_credit_log WHERE action_type = 'USE_OPENAI_MODEL'"))
    if used:
        raise RuntimeError("Cannot downgrade while OpenAI credit records exist")

    # Retain model IDs and history; an upgrade can re-enable these rows later.
    db.execute(llms.update().where(llms.c.key.in_([row[0] for row in MODELS])).values(enabled=False))
    old_flash_id = db.scalar(sa.select(llms.c.id).where(llms.c.key == OLD_FLASH))
    flash_filter = llms.c.key == (OLD_FLASH if old_flash_id is not None else NEW_FLASH)
    db.execute(llms.update().where(flash_filter).values(
        key=OLD_FLASH, name="Gemini 3 Flash", tag="Gemini 3 Flash (速度快)",
        desc="每次分析 10 算力点数。", enabled=True,
    ))
    if db.dialect.name in ("mysql", "mariadb"):
        op.alter_column(
            "user_credit_log", "action_type",
            existing_type=sa.Enum(*OLD_ACTIONS, "USE_OPENAI_MODEL", name="actiontype"),
            type_=sa.Enum(*OLD_ACTIONS, name="actiontype"), existing_nullable=False,
        )
    # PostgreSQL cannot remove one enum value; leave the unused label in place.
