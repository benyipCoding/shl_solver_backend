import asyncio
import contextlib
import logging
from datetime import datetime
from typing import Any

from app.clients import db as db_client
from app.core.config import settings
from app.services.fxcm_market_sync.service import FXCMMarketSyncService
from app.services.fxcm_market_sync.types import utc_now


logger = logging.getLogger(__name__)


class FXCMSchedulerControlError(Exception):
    """调度器无法按请求启停时抛出，携带 HTTP 状态码。"""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message


class FXCMMarketSyncScheduler:
    """历史数据后台定时调度器：由 sync-status 页面手动启停，周期性触发 run_cycle。

    不随服务启动自动运行；进程重启后回到停止状态。实时模式不经过此调度器。
    """

    def __init__(self, service: FXCMMarketSyncService) -> None:
        self._service = service
        self._task: asyncio.Task[None] | None = None
        self._stop_event: asyncio.Event | None = None
        self._started_at: datetime | None = None
        self._cycle_count = 0
        self._current_cycle_started_at: datetime | None = None
        self._last_cycle: dict[str, Any] | None = None

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start_blocked_reason(self) -> str | None:
        if not settings.fxcm_sync_enabled:
            return "FXCM_SYNC_ENABLED 未开启，历史数据定时采集不可用"
        if not settings.is_local_development:
            return "历史数据定时采集仅允许在本地开发环境开启"
        return None

    def snapshot(self) -> dict[str, Any]:
        """返回调度器运行状态，供 sync-status 页面展示。"""
        return {
            "running": self.is_running(),
            "blocked_reason": self.start_blocked_reason(),
            "started_at": self._started_at,
            "cycle_count": self._cycle_count,
            "current_cycle_started_at": self._current_cycle_started_at,
            "last_cycle": self._last_cycle,
            "poll_interval_seconds": max(15, settings.fxcm_sync_poll_interval_seconds),
        }

    async def start(self) -> None:
        if self.is_running():
            return
        blocked_reason = self.start_blocked_reason()
        if blocked_reason is not None:
            raise FXCMSchedulerControlError(409, blocked_reason)
        self._stop_event = asyncio.Event()
        self._started_at = utc_now()
        self._cycle_count = 0
        self._task = asyncio.create_task(
            self._run_loop(),
            name="fxcm-market-sync-scheduler",
        )
        logger.info("FXCM market sync scheduler started")

    async def stop(self) -> None:
        if self._task is None:
            return
        assert self._stop_event is not None
        self._stop_event.set()
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None
        self._stop_event = None
        self._started_at = None
        self._current_cycle_started_at = None
        logger.info("FXCM market sync scheduler stopped")

    async def _run_loop(self) -> None:
        assert self._stop_event is not None
        while not self._stop_event.is_set():
            processed_states = 0
            self._current_cycle_started_at = utc_now()
            try:
                async with db_client.async_session() as db:
                    result = await self._service.run_cycle(
                        db,
                        reason="scheduler",
                        force_metadata=False,
                        force_due=False,
                    )
                    processed_states = result.processed_states
                    self._last_cycle = result.to_dict()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("FXCM market scheduler loop failed")
                self._last_cycle = {
                    "reason": "scheduler",
                    "errors": [f"{type(exc).__name__}: {exc}"],
                    "finished_at": utc_now(),
                }
            finally:
                self._current_cycle_started_at = None
            self._cycle_count += 1

            try:
                delay = (
                    1.0
                    if processed_states > 0
                    else max(15, settings.fxcm_sync_poll_interval_seconds)
                )
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=delay,
                )
            except TimeoutError:
                continue
