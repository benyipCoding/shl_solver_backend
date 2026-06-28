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
