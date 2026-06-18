"""FXCM 行情同步包。

模块划分：
- service: 编排入口（run_cycle / run_manual / 监控接口）
- scheduler: 后台定时调度循环
- instrument_sync: 品种元数据与别名同步
- bar_sync: K 线拉取、转换与落库
- state_sync: 同步状态任务初始化与到期调度
- intervals: 周期解析与调度时间计算
- scheduling_policy: 每日品类轮换与周末跳过策略
- utils / types / constants: 公共工具与类型定义
"""

from app.services.fxcm_market_sync.scheduler import FXCMMarketSyncScheduler
from app.services.fxcm_market_sync.service import (
    FXCMMarketSyncService,
    fxcm_market_sync_service,
)
from app.services.fxcm_market_sync.types import FXCMMarketSyncResult


fxcm_market_sync_scheduler = FXCMMarketSyncScheduler(fxcm_market_sync_service)

__all__ = [
    "FXCMMarketSyncResult",
    "FXCMMarketSyncService",
    "FXCMMarketSyncScheduler",
    "fxcm_market_sync_service",
    "fxcm_market_sync_scheduler",
]
