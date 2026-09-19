"""add market backtest tables

Revision ID: c4e8f1a09b73
Revises: 2438e4b862c4
Create Date: 2026-09-19 21:30:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c4e8f1a09b73"
down_revision: Union[str, Sequence[str], None] = "2438e4b862c4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "market_backtest_session",
        sa.Column(
            "public_id",
            sa.String(length=36),
            nullable=False,
            comment="对外暴露的场次 ID，分享链接复用此字段",
        ),
        sa.Column(
            "client_session_id",
            sa.String(length=36),
            nullable=True,
            comment="前端生成的场次 ID，用于断线续传和幂等写入",
        ),
        sa.Column("user_id", sa.Integer(), nullable=False, comment="回测用户 ID"),
        sa.Column(
            "instrument_id", sa.Integer(), nullable=False, comment="回测标的 ID"
        ),
        sa.Column(
            "symbol",
            sa.String(length=64),
            nullable=False,
            comment="开场时的规范代码快照，避免标的改名后历史不可读",
        ),
        sa.Column(
            "interval",
            sa.String(length=16),
            nullable=False,
            comment="K 线周期，与 market_ohlcv_bar.interval 一致，如 1day、1h",
        ),
        sa.Column(
            "timeframe",
            sa.String(length=16),
            nullable=True,
            comment="前端周期标签快照，如 D1、H1",
        ),
        sa.Column(
            "provider",
            sa.String(length=20),
            nullable=False,
            comment="数据供应商快照",
        ),
        sa.Column(
            "price_type",
            sa.String(length=16),
            nullable=False,
            comment="价格类型快照，如 mid、bid、ask",
        ),
        sa.Column(
            "source",
            sa.String(length=16),
            nullable=False,
            comment="来源，当前为 BACKTEST，预留实盘模拟扩展",
        ),
        sa.Column(
            "status",
            sa.String(length=16),
            nullable=False,
            comment="场次状态：RUNNING、COMPLETED、ABANDONED",
        ),
        sa.Column(
            "visibility",
            sa.String(length=16),
            nullable=False,
            comment="分享可见性：PRIVATE、UNLISTED、PUBLIC",
        ),
        sa.Column(
            "title", sa.String(length=255), nullable=True, comment="用户命名或分享标题"
        ),
        sa.Column(
            "start_bar_time",
            sa.DateTime(timezone=True),
            nullable=False,
            comment="回测起点对应的最后一根可见 K 线时间，UTC",
        ),
        sa.Column(
            "start_bar_index",
            sa.Integer(),
            nullable=True,
            comment="回测起点在当时数据集中的下标，仅作辅助，还原以时间为准",
        ),
        sa.Column(
            "initial_visible_bars",
            sa.Integer(),
            nullable=False,
            comment="回测开始时图表向左露出的 K 线数量",
        ),
        sa.Column(
            "cursor_bar_time",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="用户播放到的最远 K 线时间，UTC",
        ),
        sa.Column(
            "cursor_bar_index",
            sa.Integer(),
            nullable=True,
            comment="用户播放到的最远 K 线下标，仅作辅助",
        ),
        sa.Column(
            "initial_balance",
            sa.Numeric(precision=20, scale=4),
            nullable=False,
            comment="开场模拟资金",
        ),
        sa.Column(
            "ending_balance",
            sa.Numeric(precision=20, scale=4),
            nullable=True,
            comment="结束时模拟资金，未结束则为空",
        ),
        sa.Column(
            "trade_count", sa.Integer(), nullable=False, comment="本场开仓笔数"
        ),
        sa.Column(
            "closed_trade_count",
            sa.Integer(),
            nullable=False,
            comment="本场已平仓笔数",
        ),
        sa.Column("win_count", sa.Integer(), nullable=False, comment="本场盈利笔数"),
        sa.Column(
            "realized_pnl",
            sa.Numeric(precision=20, scale=10),
            nullable=False,
            comment="本场已实现盈亏合计，便于列表展示",
        ),
        sa.Column(
            "ended_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="退出回测的墙钟时间",
        ),
        sa.Column(
            "meta", sa.JSON(), nullable=True, comment="扩展上下文，如指标配置快照"
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["instrument_id"], ["market_instrument.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["user_id"], ["user.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("public_id", name="uq_market_backtest_session_public_id"),
    )
    op.create_index(
        op.f("ix_market_backtest_session_id"),
        "market_backtest_session",
        ["id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_market_backtest_session_user_id"),
        "market_backtest_session",
        ["user_id"],
        unique=False,
    )
    op.create_index(
        "ix_market_backtest_session_user_created",
        "market_backtest_session",
        ["user_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_market_backtest_session_user_instrument_interval",
        "market_backtest_session",
        ["user_id", "instrument_id", "interval"],
        unique=False,
    )
    op.create_index(
        "ix_market_backtest_session_visibility_public_id",
        "market_backtest_session",
        ["visibility", "public_id"],
        unique=False,
    )
    op.create_index(
        "ix_market_backtest_session_status_ended",
        "market_backtest_session",
        ["status", "ended_at"],
        unique=False,
    )
    op.create_index(
        "uq_market_backtest_session_user_client",
        "market_backtest_session",
        ["user_id", "client_session_id"],
        unique=True,
        postgresql_where=sa.text("client_session_id IS NOT NULL"),
    )

    op.create_table(
        "market_backtest_trade",
        sa.Column(
            "session_id", sa.Integer(), nullable=False, comment="所属回测场次"
        ),
        sa.Column(
            "user_id",
            sa.Integer(),
            nullable=False,
            comment="冗余用户 ID，便于不 join 场次做统计",
        ),
        sa.Column(
            "instrument_id",
            sa.Integer(),
            nullable=False,
            comment="冗余标的 ID，便于按品种聚合",
        ),
        sa.Column(
            "client_trade_id",
            sa.String(length=64),
            nullable=True,
            comment="前端本地成交 ID，用于会话内幂等映射",
        ),
        sa.Column(
            "sequence_no",
            sa.Integer(),
            nullable=False,
            comment="本场开仓序号，同一根 K 线上多笔单时用于稳定排序",
        ),
        sa.Column(
            "symbol",
            sa.String(length=64),
            nullable=False,
            comment="开仓时的规范代码快照",
        ),
        sa.Column(
            "interval",
            sa.String(length=16),
            nullable=False,
            comment="开仓时的 K 线周期快照",
        ),
        sa.Column("side", sa.String(length=8), nullable=False, comment="方向：BUY、SELL"),
        sa.Column("units", sa.Integer(), nullable=False, comment="手数/下单数量"),
        sa.Column(
            "status",
            sa.String(length=16),
            nullable=False,
            comment="持仓状态：OPEN、CLOSED",
        ),
        sa.Column(
            "entry_price",
            sa.Numeric(precision=20, scale=10),
            nullable=False,
            comment="开仓价",
        ),
        sa.Column(
            "entry_bar_time",
            sa.DateTime(timezone=True),
            nullable=False,
            comment="开仓所在 K 线时间，UTC，分享还原的主锚点",
        ),
        sa.Column(
            "entry_bar_index",
            sa.Integer(),
            nullable=True,
            comment="开仓时数据集下标，仅作辅助",
        ),
        sa.Column(
            "initial_sl_price",
            sa.Numeric(precision=20, scale=10),
            nullable=True,
            comment="开仓时的止损价，用于对比是否中途改止损",
        ),
        sa.Column(
            "initial_tp_price",
            sa.Numeric(precision=20, scale=10),
            nullable=True,
            comment="开仓时的止盈价，用于对比是否中途改止盈",
        ),
        sa.Column(
            "sl_price",
            sa.Numeric(precision=20, scale=10),
            nullable=True,
            comment="当前或平仓时的止损价",
        ),
        sa.Column(
            "tp_price",
            sa.Numeric(precision=20, scale=10),
            nullable=True,
            comment="当前或平仓时的止盈价",
        ),
        sa.Column(
            "close_price",
            sa.Numeric(precision=20, scale=10),
            nullable=True,
            comment="平仓价",
        ),
        sa.Column(
            "close_bar_time",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="平仓所在 K 线时间，UTC",
        ),
        sa.Column(
            "close_bar_index",
            sa.Integer(),
            nullable=True,
            comment="平仓时数据集下标，仅作辅助",
        ),
        sa.Column(
            "close_reason",
            sa.String(length=32),
            nullable=True,
            comment="平仓原因：SL_HIT、TP_HIT、MARKET_CLOSE、FORCED_MARKET_CLOSE",
        ),
        sa.Column(
            "realized_pnl",
            sa.Numeric(precision=20, scale=10),
            nullable=True,
            comment="已实现盈亏金额，平仓后写入",
        ),
        sa.Column(
            "realized_points",
            sa.Numeric(precision=20, scale=10),
            nullable=True,
            comment="已实现价差（点数口径），排除手数后便于跨仓比较",
        ),
        sa.Column(
            "closed_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment="平仓墙钟时间",
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["instrument_id"], ["market_instrument.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["session_id"], ["market_backtest_session.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["user_id"], ["user.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_market_backtest_trade_id"),
        "market_backtest_trade",
        ["id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_market_backtest_trade_session_id"),
        "market_backtest_trade",
        ["session_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_market_backtest_trade_user_id"),
        "market_backtest_trade",
        ["user_id"],
        unique=False,
    )
    op.create_index(
        "ix_market_backtest_trade_session_entry_time",
        "market_backtest_trade",
        ["session_id", "entry_bar_time"],
        unique=False,
    )
    op.create_index(
        "ix_market_backtest_trade_user_instrument_interval_status",
        "market_backtest_trade",
        ["user_id", "instrument_id", "interval", "status"],
        unique=False,
    )
    op.create_index(
        "ix_market_backtest_trade_user_symbol_interval",
        "market_backtest_trade",
        ["user_id", "symbol", "interval"],
        unique=False,
    )
    op.create_index(
        "ix_market_backtest_trade_user_closed",
        "market_backtest_trade",
        ["user_id", "closed_at"],
        unique=False,
    )
    op.create_index(
        "uq_market_backtest_trade_session_client",
        "market_backtest_trade",
        ["session_id", "client_trade_id"],
        unique=True,
        postgresql_where=sa.text("client_trade_id IS NOT NULL"),
    )

    op.create_table(
        "market_backtest_event",
        sa.Column(
            "session_id", sa.Integer(), nullable=False, comment="所属回测场次"
        ),
        sa.Column("trade_id", sa.Integer(), nullable=False, comment="关联持仓"),
        sa.Column(
            "sequence_no",
            sa.Integer(),
            nullable=False,
            comment="场次内单调序号，同一根 K 线上多事件时以此排序",
        ),
        sa.Column(
            "event_type",
            sa.String(length=16),
            nullable=False,
            comment="事件类型：OPEN、MODIFY_SL、MODIFY_TP、CLOSE",
        ),
        sa.Column(
            "bar_time",
            sa.DateTime(timezone=True),
            nullable=False,
            comment="事件对应的 K 线时间，UTC",
        ),
        sa.Column(
            "bar_index",
            sa.Integer(),
            nullable=True,
            comment="事件发生时的数据集下标，仅作辅助",
        ),
        sa.Column(
            "side",
            sa.String(length=8),
            nullable=True,
            comment="开仓方向快照：BUY、SELL",
        ),
        sa.Column("units", sa.Integer(), nullable=True, comment="该事件相关手数"),
        sa.Column(
            "price",
            sa.Numeric(precision=20, scale=10),
            nullable=True,
            comment="事件价格：开仓价、新止损/止盈价或平仓价",
        ),
        sa.Column(
            "sl_price",
            sa.Numeric(precision=20, scale=10),
            nullable=True,
            comment="事件发生后的止损价",
        ),
        sa.Column(
            "tp_price",
            sa.Numeric(precision=20, scale=10),
            nullable=True,
            comment="事件发生后的止盈价",
        ),
        sa.Column(
            "close_reason",
            sa.String(length=32),
            nullable=True,
            comment="平仓事件原因，取值同成交表 close_reason",
        ),
        sa.Column("payload", sa.JSON(), nullable=True, comment="预留扩展负载"),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=True,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["session_id"], ["market_backtest_session.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["trade_id"], ["market_backtest_trade.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "session_id",
            "sequence_no",
            name="uq_market_backtest_event_session_sequence",
        ),
    )
    op.create_index(
        op.f("ix_market_backtest_event_id"),
        "market_backtest_event",
        ["id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_market_backtest_event_session_id"),
        "market_backtest_event",
        ["session_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_market_backtest_event_trade_id"),
        "market_backtest_event",
        ["trade_id"],
        unique=False,
    )
    op.create_index(
        "ix_market_backtest_event_session_bar_time",
        "market_backtest_event",
        ["session_id", "bar_time"],
        unique=False,
    )
    op.create_index(
        "ix_market_backtest_event_trade_sequence",
        "market_backtest_event",
        ["trade_id", "sequence_no"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_market_backtest_event_trade_sequence",
        table_name="market_backtest_event",
    )
    op.drop_index(
        "ix_market_backtest_event_session_bar_time",
        table_name="market_backtest_event",
    )
    op.drop_index(
        op.f("ix_market_backtest_event_trade_id"),
        table_name="market_backtest_event",
    )
    op.drop_index(
        op.f("ix_market_backtest_event_session_id"),
        table_name="market_backtest_event",
    )
    op.drop_index(op.f("ix_market_backtest_event_id"), table_name="market_backtest_event")
    op.drop_table("market_backtest_event")

    op.drop_index(
        "uq_market_backtest_trade_session_client",
        table_name="market_backtest_trade",
        postgresql_where=sa.text("client_trade_id IS NOT NULL"),
    )
    op.drop_index(
        "ix_market_backtest_trade_user_closed", table_name="market_backtest_trade"
    )
    op.drop_index(
        "ix_market_backtest_trade_user_symbol_interval",
        table_name="market_backtest_trade",
    )
    op.drop_index(
        "ix_market_backtest_trade_user_instrument_interval_status",
        table_name="market_backtest_trade",
    )
    op.drop_index(
        "ix_market_backtest_trade_session_entry_time",
        table_name="market_backtest_trade",
    )
    op.drop_index(
        op.f("ix_market_backtest_trade_user_id"), table_name="market_backtest_trade"
    )
    op.drop_index(
        op.f("ix_market_backtest_trade_session_id"),
        table_name="market_backtest_trade",
    )
    op.drop_index(op.f("ix_market_backtest_trade_id"), table_name="market_backtest_trade")
    op.drop_table("market_backtest_trade")

    op.drop_index(
        "uq_market_backtest_session_user_client",
        table_name="market_backtest_session",
        postgresql_where=sa.text("client_session_id IS NOT NULL"),
    )
    op.drop_index(
        "ix_market_backtest_session_status_ended",
        table_name="market_backtest_session",
    )
    op.drop_index(
        "ix_market_backtest_session_visibility_public_id",
        table_name="market_backtest_session",
    )
    op.drop_index(
        "ix_market_backtest_session_user_instrument_interval",
        table_name="market_backtest_session",
    )
    op.drop_index(
        "ix_market_backtest_session_user_created",
        table_name="market_backtest_session",
    )
    op.drop_index(
        op.f("ix_market_backtest_session_user_id"),
        table_name="market_backtest_session",
    )
    op.drop_index(
        op.f("ix_market_backtest_session_id"), table_name="market_backtest_session"
    )
    op.drop_table("market_backtest_session")
