"""FXCM 行情同步包。

模块划分：
- service: 编排入口（run_cycle / run_manual / 优先追赶 / 框选修复 / 监控接口）
- scheduler: 后台定时调度循环（页面手动启停）
- sync_queue: 历史数据采集队列与候选品种
- instrument_sync: 品种元数据与别名同步
- bar_sync: K 线拉取、转换、落库与时间段修复
- state_sync: 同步状态任务初始化、到期调度与完成判定
- history_cutoff: 历史数据截止时间配置与采集完成判定
- intervals: 周期解析与调度时间计算
- scheduling_policy: 品类推断
- utils / types / constants: 公共工具与类型定义
"""

from app.services.fxcm_market_sync.scheduler import (
    FXCMMarketSyncScheduler,
    FXCMSchedulerControlError,
)
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
    "FXCMSchedulerControlError",
    "fxcm_market_sync_service",
    "fxcm_market_sync_scheduler",
]
