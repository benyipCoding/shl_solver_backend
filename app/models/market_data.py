import uuid

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)

from app.models.base import Base
from app.models.mixins import TimestampMixin


class MarketInstrument(Base, TimestampMixin):
    __tablename__ = "market_instrument"
    __table_args__ = (
        UniqueConstraint(
            "normalized_symbol",
            name="uq_market_instrument_normalized_symbol",
        ),
        UniqueConstraint(
            "provider",
            "normalized_provider_symbol",
            name="uq_market_instrument_provider_symbol",
        ),
        Index(
            "ix_market_instrument_provider_market_asset_active",
            "provider",
            "market",
            "asset_type",
            "is_active",
        ),
        Index(
            "ix_market_instrument_is_hot_active",
            "is_hot",
            "is_active",
        ),
        Index("ix_market_instrument_name", "name"),
    )

    provider = Column(String(20), nullable=False, default="FXCM", comment="数据供应商")
    symbol = Column(
        String(64), nullable=False, index=True, comment="对外暴露的规范代码"
    )
    normalized_symbol = Column(
        String(64),
        nullable=False,
        index=True,
        comment="按服务层规则归一化后的 symbol",
    )
    provider_symbol = Column(
        String(64),
        nullable=False,
        comment="供应商原始或主标的代码",
    )
    normalized_provider_symbol = Column(
        String(64),
        nullable=False,
        index=True,
        comment="按服务层规则归一化后的 provider symbol",
    )
    name = Column(String(255), nullable=False, comment="标的名称")
    display_label = Column(String(255), nullable=True, comment="前端展示标签")
    exchange = Column(String(64), nullable=True, comment="交易所")
    mic_code = Column(String(32), nullable=True, comment="MIC 代码")
    market = Column(String(64), nullable=True, comment="市场分类")
    asset_type = Column(String(64), nullable=True, comment="资产类型")
    country = Column(String(64), nullable=True, comment="国家或地区")
    currency = Column(String(16), nullable=True, comment="计价货币")
    exchange_timezone = Column(String(64), nullable=True, comment="交易所时区")
    provider_plan = Column(
        String(64),
        nullable=True,
        comment="供应商可用性或套餐字段",
    )
    sort_weight = Column(
        Integer,
        nullable=False,
        default=0,
        comment="搜索排序权重，越小越靠前",
    )
    is_active = Column(Boolean, nullable=False, default=True, comment="是否启用")
    is_searchable = Column(
        Boolean,
        nullable=False,
        default=True,
        comment="是否参与搜索",
    )
    is_hot = Column(Boolean, nullable=False, default=False, comment="是否热池标的")
    supports_history = Column(
        Boolean,
        nullable=False,
        default=True,
        comment="是否支持历史 K 线",
    )
    supports_quote = Column(
        Boolean,
        nullable=False,
        default=True,
        comment="是否支持报价",
    )
    source_payload = Column(
        JSON,
        nullable=True,
        comment="最近一次同步的原始或归一化主数据负载",
    )
    metadata_synced_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="主数据最近同步时间",
    )
    last_seen_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="最近一次在供应商返回中出现的时间",
    )


class MarketInstrumentAlias(Base, TimestampMixin):
    __tablename__ = "market_instrument_alias"
    __table_args__ = (
        UniqueConstraint(
            "normalized_alias",
            name="uq_market_instrument_alias_normalized_alias",
        ),
        Index("ix_market_instrument_alias_instrument_id", "instrument_id"),
        Index(
            "ix_market_instrument_alias_priority_active",
            "priority",
            "is_active",
        ),
    )

    instrument_id = Column(
        Integer,
        ForeignKey("market_instrument.id", ondelete="CASCADE"),
        nullable=False,
        comment="关联主标的 ID",
    )
    alias = Column(String(128), nullable=False, comment="原始别名")
    normalized_alias = Column(
        String(128),
        nullable=False,
        index=True,
        comment="归一化后的别名",
    )
    alias_type = Column(
        String(32),
        nullable=False,
        default="MANUAL",
        comment="别名类型，如 LEGACY、MANUAL、SEARCH",
    )
    priority = Column(
        Integer,
        nullable=False,
        default=100,
        comment="冲突时优先级，越小越优先",
    )
    is_active = Column(Boolean, nullable=False, default=True, comment="是否启用")


class MarketOHLCVBar(Base, TimestampMixin):
    __tablename__ = "market_ohlcv_bar"
    __table_args__ = (
        UniqueConstraint(
            "instrument_id",
            "interval",
            "price_type",
            "bar_time",
            name="uq_market_ohlcv_bar_instrument_interval_price_time",
        ),
        Index(
            "ix_market_ohlcv_bar_instrument_interval_time",
            "instrument_id",
            "interval",
            "bar_time",
        ),
        Index("ix_market_ohlcv_bar_interval_time", "interval", "bar_time"),
    )

    instrument_id = Column(
        Integer,
        ForeignKey("market_instrument.id", ondelete="CASCADE"),
        nullable=False,
        comment="关联主标的 ID",
    )
    provider = Column(String(20), nullable=False, default="FXCM", comment="数据供应商")
    provider_symbol = Column(
        String(64),
        nullable=False,
        comment="抓取时使用的 provider symbol",
    )
    interval = Column(String(16), nullable=False, comment="K 线周期，如 1day、1h")
    price_type = Column(
        String(16),
        nullable=False,
        default="mid",
        comment="价格类型，如 mid、bid、ask",
    )
    data_origin = Column(
        String(16),
        nullable=False,
        default="PROVIDER",
        comment="数据来源，如 PROVIDER 或 AGGREGATED",
    )
    source_interval = Column(
        String(16),
        nullable=True,
        comment="若为聚合数据，则记录来源周期",
    )
    bar_time = Column(
        DateTime(timezone=True),
        nullable=False,
        comment="K 线起始时间，统一存 UTC",
    )
    open = Column(Numeric(20, 10), nullable=False, comment="开盘价")
    high = Column(Numeric(20, 10), nullable=False, comment="最高价")
    low = Column(Numeric(20, 10), nullable=False, comment="最低价")
    close = Column(Numeric(20, 10), nullable=False, comment="收盘价")
    volume = Column(BigInteger, nullable=True, comment="成交量")


class MarketBarSyncState(Base, TimestampMixin):
    __tablename__ = "market_bar_sync_state"
    __table_args__ = (
        UniqueConstraint(
            "instrument_id",
            "interval",
            "price_type",
            name="uq_market_bar_sync_state_instrument_interval_price",
        ),
        Index(
            "ix_market_bar_sync_state_enabled_priority",
            "enabled",
            "priority",
        ),
        Index(
            "ix_market_bar_sync_state_status_attempt",
            "last_status",
            "last_attempt_at",
        ),
        Index(
            "ix_market_bar_sync_state_next_sync_from",
            "next_sync_from",
        ),
    )

    instrument_id = Column(
        Integer,
        ForeignKey("market_instrument.id", ondelete="CASCADE"),
        nullable=False,
        comment="关联主标的 ID",
    )
    provider = Column(String(20), nullable=False, default="FXCM", comment="数据供应商")
    interval = Column(String(16), nullable=False, comment="同步周期")
    price_type = Column(
        String(16),
        nullable=False,
        default="mid",
        comment="价格类型",
    )
    enabled = Column(Boolean, nullable=False, default=True, comment="是否启用同步")
    priority = Column(
        Integer,
        nullable=False,
        default=100,
        comment="调度优先级，越小越优先",
    )
    target_history_bars = Column(
        Integer,
        nullable=True,
        comment="目标回补 K 线数量",
    )
    sync_mode = Column(
        String(16),
        nullable=False,
        default="INCREMENTAL",
        comment="同步模式，如 BACKFILL 或 INCREMENTAL",
    )
    earliest_synced_bar_time = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="已同步最早 K 线时间",
    )
    latest_synced_bar_time = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="已同步最新 K 线时间",
    )
    last_requested_start_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="最近一次请求的开始时间",
    )
    last_requested_end_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="最近一次请求的结束时间",
    )
    next_sync_from = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="下次同步起点",
    )
    backfill_completed = Column(
        Boolean,
        nullable=False,
        default=False,
        comment="历史回补是否完成",
    )
    last_status = Column(
        String(20),
        nullable=False,
        default="IDLE",
        comment="最近一次同步状态",
    )
    last_attempt_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="最近一次尝试同步时间",
    )
    last_success_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="最近一次同步成功时间",
    )
    retry_count = Column(
        Integer,
        nullable=False,
        default=0,
        comment="连续重试次数",
    )
    last_error = Column(Text, nullable=True, comment="最近一次错误信息")
    meta = Column(JSON, nullable=True, comment="同步上下文和诊断信息")


class MarketBacktestSession(Base, TimestampMixin):
    """一次「开启逐K回测 → 退出回测」对应一行。

    既是按品种/周期做盈亏统计的分组单元，也是分享时间轴时
    一比一还原图表起点、可见窗口和成交序列的入口。
    """

    __tablename__ = "market_backtest_session"
    __table_args__ = (
        UniqueConstraint(
            "public_id",
            name="uq_market_backtest_session_public_id",
        ),
        Index(
            "uq_market_backtest_session_user_client",
            "user_id",
            "client_session_id",
            unique=True,
            postgresql_where=text("client_session_id IS NOT NULL"),
        ),
        Index(
            "ix_market_backtest_session_user_created",
            "user_id",
            "created_at",
        ),
        Index(
            "ix_market_backtest_session_user_instrument_interval",
            "user_id",
            "instrument_id",
            "interval",
        ),
        Index(
            "ix_market_backtest_session_visibility_public_id",
            "visibility",
            "public_id",
        ),
        Index(
            "ix_market_backtest_session_status_ended",
            "status",
            "ended_at",
        ),
    )

    public_id = Column(
        String(36),
        nullable=False,
        default=lambda: str(uuid.uuid4()),
        comment="对外暴露的场次 ID，分享链接复用此字段",
    )
    client_session_id = Column(
        String(36),
        nullable=True,
        comment="前端生成的场次 ID，用于断线续传和幂等写入",
    )
    user_id = Column(
        Integer,
        ForeignKey("user.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
        comment="回测用户 ID",
    )
    instrument_id = Column(
        Integer,
        ForeignKey("market_instrument.id", ondelete="RESTRICT"),
        nullable=False,
        comment="回测标的 ID",
    )
    symbol = Column(
        String(64),
        nullable=False,
        comment="开场时的规范代码快照，避免标的改名后历史不可读",
    )
    interval = Column(
        String(16),
        nullable=False,
        comment="K 线周期，与 market_ohlcv_bar.interval 一致，如 1day、1h",
    )
    timeframe = Column(
        String(16),
        nullable=True,
        comment="前端周期标签快照，如 D1、H1",
    )
    provider = Column(
        String(20),
        nullable=False,
        default="FXCM",
        comment="数据供应商快照",
    )
    price_type = Column(
        String(16),
        nullable=False,
        default="mid",
        comment="价格类型快照，如 mid、bid、ask",
    )
    source = Column(
        String(16),
        nullable=False,
        default="BACKTEST",
        comment="来源，当前为 BACKTEST，预留实盘模拟扩展",
    )
    status = Column(
        String(16),
        nullable=False,
        default="RUNNING",
        comment="场次状态：RUNNING、COMPLETED、ABANDONED",
    )
    visibility = Column(
        String(16),
        nullable=False,
        default="PRIVATE",
        comment="分享可见性：PRIVATE、UNLISTED、PUBLIC",
    )
    title = Column(
        String(255),
        nullable=True,
        comment="用户命名或分享标题",
    )
    start_bar_time = Column(
        DateTime(timezone=True),
        nullable=False,
        comment="回测起点对应的最后一根可见 K 线时间，UTC",
    )
    start_bar_index = Column(
        Integer,
        nullable=True,
        comment="回测起点在当时数据集中的下标，仅作辅助，还原以时间为准",
    )
    initial_visible_bars = Column(
        Integer,
        nullable=False,
        default=200,
        comment="回测开始时图表向左露出的 K 线数量",
    )
    cursor_bar_time = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="用户播放到的最远 K 线时间，UTC",
    )
    cursor_bar_index = Column(
        Integer,
        nullable=True,
        comment="用户播放到的最远 K 线下标，仅作辅助",
    )
    initial_balance = Column(
        Numeric(20, 4),
        nullable=False,
        default=10000,
        comment="开场模拟资金",
    )
    ending_balance = Column(
        Numeric(20, 4),
        nullable=True,
        comment="结束时模拟资金，未结束则为空",
    )
    trade_count = Column(
        Integer,
        nullable=False,
        default=0,
        comment="本场开仓笔数",
    )
    closed_trade_count = Column(
        Integer,
        nullable=False,
        default=0,
        comment="本场已平仓笔数",
    )
    win_count = Column(
        Integer,
        nullable=False,
        default=0,
        comment="本场盈利笔数",
    )
    realized_pnl = Column(
        Numeric(20, 10),
        nullable=False,
        default=0,
        comment="本场已实现盈亏合计，便于列表展示",
    )
    ended_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="退出回测的墙钟时间",
    )
    meta = Column(
        JSON,
        nullable=True,
        comment="扩展上下文，如指标配置快照",
    )


class MarketBacktestTrade(Base, TimestampMixin):
    """回测中的一笔持仓，从开仓到平仓占一行。

    用于品种/周期盈亏统计；入场/离场 K 线时间用于分享时在图表上还原仓位。
    """

    __tablename__ = "market_backtest_trade"
    __table_args__ = (
        Index(
            "uq_market_backtest_trade_session_client",
            "session_id",
            "client_trade_id",
            unique=True,
            postgresql_where=text("client_trade_id IS NOT NULL"),
        ),
        Index(
            "ix_market_backtest_trade_session_entry_time",
            "session_id",
            "entry_bar_time",
        ),
        Index(
            "ix_market_backtest_trade_user_instrument_interval_status",
            "user_id",
            "instrument_id",
            "interval",
            "status",
        ),
        Index(
            "ix_market_backtest_trade_user_symbol_interval",
            "user_id",
            "symbol",
            "interval",
        ),
        Index(
            "ix_market_backtest_trade_user_closed",
            "user_id",
            "closed_at",
        ),
    )

    session_id = Column(
        Integer,
        ForeignKey("market_backtest_session.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="所属回测场次",
    )
    user_id = Column(
        Integer,
        ForeignKey("user.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
        comment="冗余用户 ID，便于不 join 场次做统计",
    )
    instrument_id = Column(
        Integer,
        ForeignKey("market_instrument.id", ondelete="RESTRICT"),
        nullable=False,
        comment="冗余标的 ID，便于按品种聚合",
    )
    client_trade_id = Column(
        String(64),
        nullable=True,
        comment="前端本地成交 ID，用于会话内幂等映射",
    )
    sequence_no = Column(
        Integer,
        nullable=False,
        default=0,
        comment="本场开仓序号，同一根 K 线上多笔单时用于稳定排序",
    )
    symbol = Column(
        String(64),
        nullable=False,
        comment="开仓时的规范代码快照",
    )
    interval = Column(
        String(16),
        nullable=False,
        comment="开仓时的 K 线周期快照",
    )
    side = Column(
        String(8),
        nullable=False,
        comment="方向：BUY、SELL",
    )
    units = Column(
        Integer,
        nullable=False,
        comment="手数/下单数量",
    )
    status = Column(
        String(16),
        nullable=False,
        default="OPEN",
        comment="持仓状态：OPEN、CLOSED",
    )
    entry_price = Column(
        Numeric(20, 10),
        nullable=False,
        comment="开仓价",
    )
    entry_bar_time = Column(
        DateTime(timezone=True),
        nullable=False,
        comment="开仓所在 K 线时间，UTC，分享还原的主锚点",
    )
    entry_bar_index = Column(
        Integer,
        nullable=True,
        comment="开仓时数据集下标，仅作辅助",
    )
    initial_sl_price = Column(
        Numeric(20, 10),
        nullable=True,
        comment="开仓时的止损价，用于对比是否中途改止损",
    )
    initial_tp_price = Column(
        Numeric(20, 10),
        nullable=True,
        comment="开仓时的止盈价，用于对比是否中途改止盈",
    )
    sl_price = Column(
        Numeric(20, 10),
        nullable=True,
        comment="当前或平仓时的止损价",
    )
    tp_price = Column(
        Numeric(20, 10),
        nullable=True,
        comment="当前或平仓时的止盈价",
    )
    close_price = Column(
        Numeric(20, 10),
        nullable=True,
        comment="平仓价",
    )
    close_bar_time = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="平仓所在 K 线时间，UTC",
    )
    close_bar_index = Column(
        Integer,
        nullable=True,
        comment="平仓时数据集下标，仅作辅助",
    )
    close_reason = Column(
        String(32),
        nullable=True,
        comment="平仓原因：SL_HIT、TP_HIT、MARKET_CLOSE、FORCED_MARKET_CLOSE",
    )
    realized_pnl = Column(
        Numeric(20, 10),
        nullable=True,
        comment="已实现盈亏金额，平仓后写入",
    )
    realized_points = Column(
        Numeric(20, 10),
        nullable=True,
        comment="已实现价差（点数口径），排除手数后便于跨仓比较",
    )
    closed_at = Column(
        DateTime(timezone=True),
        nullable=True,
        comment="平仓墙钟时间",
    )


class MarketBacktestEvent(Base, TimestampMixin):
    """回测场次内的只追加事件流。

    成交表保存仓位终态便于统计；本表按 K 线时间记录开仓、改止盈止损、
    平仓，分享时按 sequence_no 回放即可还原操作顺序。
    """

    __tablename__ = "market_backtest_event"
    __table_args__ = (
        UniqueConstraint(
            "session_id",
            "sequence_no",
            name="uq_market_backtest_event_session_sequence",
        ),
        Index(
            "ix_market_backtest_event_session_bar_time",
            "session_id",
            "bar_time",
        ),
        Index(
            "ix_market_backtest_event_trade_sequence",
            "trade_id",
            "sequence_no",
        ),
    )

    session_id = Column(
        Integer,
        ForeignKey("market_backtest_session.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="所属回测场次",
    )
    trade_id = Column(
        Integer,
        ForeignKey("market_backtest_trade.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="关联持仓",
    )
    sequence_no = Column(
        Integer,
        nullable=False,
        comment="场次内单调序号，同一根 K 线上多事件时以此排序",
    )
    event_type = Column(
        String(16),
        nullable=False,
        comment="事件类型：OPEN、MODIFY_SL、MODIFY_TP、CLOSE",
    )
    bar_time = Column(
        DateTime(timezone=True),
        nullable=False,
        comment="事件对应的 K 线时间，UTC",
    )
    bar_index = Column(
        Integer,
        nullable=True,
        comment="事件发生时的数据集下标，仅作辅助",
    )
    side = Column(
        String(8),
        nullable=True,
        comment="开仓方向快照：BUY、SELL",
    )
    units = Column(
        Integer,
        nullable=True,
        comment="该事件相关手数",
    )
    price = Column(
        Numeric(20, 10),
        nullable=True,
        comment="事件价格：开仓价、新止损/止盈价或平仓价",
    )
    sl_price = Column(
        Numeric(20, 10),
        nullable=True,
        comment="事件发生后的止损价",
    )
    tp_price = Column(
        Numeric(20, 10),
        nullable=True,
        comment="事件发生后的止盈价",
    )
    close_reason = Column(
        String(32),
        nullable=True,
        comment="平仓事件原因，取值同成交表 close_reason",
    )
    payload = Column(
        JSON,
        nullable=True,
        comment="预留扩展负载",
    )
