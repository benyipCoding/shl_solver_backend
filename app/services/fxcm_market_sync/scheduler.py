import asyncio
import contextlib
import logging

from app.clients import db as db_client
from app.core.config import settings
from app.services.fxcm_market_sync.service import FXCMMarketSyncService


logger = logging.getLogger(__name__)


class FXCMMarketSyncScheduler:
    """后台定时调度器：周期性触发 FXCMMarketSyncService.run_cycle。"""

    def __init__(self, service: FXCMMarketSyncService) -> None:
        self._service = service
        self._task: asyncio.Task[None] | None = None
        self._stop_event: asyncio.Event | None = None

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if not settings.fxcm_sync_enabled or self.is_running():
            return
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(
            self._run_loop(),
            name="fxcm-market-sync-scheduler",
        )

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

    async def _run_loop(self) -> None:
        assert self._stop_event is not None
        while not self._stop_event.is_set():
            processed_states = 0
            try:
                async with db_client.async_session() as db:
                    result = await self._service.run_cycle(
                        db,
                        reason="scheduler",
                        force_metadata=False,
                        force_due=False,
                    )
                    processed_states = result.processed_states
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("FXCM market scheduler loop failed")

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
