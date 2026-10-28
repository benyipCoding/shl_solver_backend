"""历史数据采集队列与手动启停调度器。

数据库用例在独立 PostgreSQL schema 中运行；设置 MARKET_SYNC_TEST_DATABASE_URL 指向隔离的测试库。
"""

import asyncio
import importlib.util
import os
import sys
import unittest
import uuid
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.models.market_data import (
    MarketBarSyncState, MarketInstrument, MarketOHLCVBar, MarketSyncQueueItem,
)
from app.services.fxcm_market_sync.scheduler import (
    FXCMMarketSyncScheduler, FXCMSchedulerControlError,
)
from app.services.fxcm_market_sync.service import FXCMMarketSyncService
from app.services.fxcm_market_sync.types import FXCMMarketSyncResult, utc_now


if sys.platform == "win32":
    # psycopg 异步模式不支持 Windows 默认的 ProactorEventLoop。
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class SchedulerControlTest(unittest.IsolatedAsyncioTestCase):
    async def test_start_refused_when_sync_disabled(self):
        scheduler = FXCMMarketSyncScheduler(FXCMMarketSyncService())
        with patch("app.services.fxcm_market_sync.scheduler.settings") as settings:
            settings.fxcm_sync_enabled = False
            with self.assertRaises(FXCMSchedulerControlError) as error:
                await scheduler.start()
        self.assertEqual(error.exception.status_code, 409)
        self.assertFalse(scheduler.is_running())

    async def test_start_runs_cycles_until_stopped(self):
        service = FXCMMarketSyncService()
        service.run_cycle = AsyncMock(return_value=FXCMMarketSyncResult(reason="scheduler"))
        scheduler = FXCMMarketSyncScheduler(service)

        class _Session:
            async def __aenter__(self):
                return object()

            async def __aexit__(self, *exc):
                return False

        with patch("app.services.fxcm_market_sync.scheduler.settings") as settings, \
                patch("app.services.fxcm_market_sync.scheduler.db_client.async_session", _Session):
            settings.fxcm_sync_enabled = True
            settings.is_local_development = True
            settings.fxcm_sync_poll_interval_seconds = 60
            await scheduler.start()
            await asyncio.sleep(0.05)
            snapshot = scheduler.snapshot()
            self.assertTrue(snapshot["running"])
            self.assertEqual(snapshot["cycle_count"], 1)
            self.assertEqual(snapshot["last_cycle"]["reason"], "scheduler")
            await scheduler.stop()

        self.assertFalse(scheduler.snapshot()["running"])
        self.assertIsNone(scheduler.snapshot()["started_at"])
        service.run_cycle.assert_awaited_once()


@unittest.skipUnless(os.getenv("MARKET_SYNC_TEST_DATABASE_URL"), "isolated PostgreSQL URL required")
class SyncQueuePostgresTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        base_url = make_url(os.environ["MARKET_SYNC_TEST_DATABASE_URL"])
        self.schema = f"sync_queue_test_{uuid.uuid4().hex}"
        self.admin = create_engine(base_url)
        with self.admin.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{self.schema}"'))
        sync_engine = create_engine(
            base_url, connect_args={"options": f"-csearch_path={self.schema}"},
        )
        for model in (MarketInstrument, MarketOHLCVBar, MarketBarSyncState):
            model.__table__.create(sync_engine)
        with sync_engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO market_instrument (id, provider, symbol, normalized_symbol, provider_symbol,"
                " normalized_provider_symbol, name, asset_type, is_active, is_searchable, is_hot,"
                " supports_history, supports_quote, sort_weight) VALUES"
                " (1, 'FXCM', 'EUR/USD', 'EURUSD', 'EUR/USD', 'EURUSD', 'Euro', 'Forex', true, true, true, true, true, 0),"
                " (2, 'FXCM', 'XAU/USD', 'XAUUSD', 'XAU/USD', 'XAUUSD', 'Gold', 'Metal', true, true, true, true, true, 0),"
                " (3, 'FXCM', 'US30', 'US30', 'US30', 'US30', 'Dow', 'Index', true, true, true, true, true, 0)"
            ))
            conn.execute(text(
                "INSERT INTO market_bar_sync_state (instrument_id, provider, interval, price_type, enabled,"
                " priority, target_history_bars, sync_mode, backfill_completed, last_status, last_error,"
                " retry_count, next_sync_from) VALUES"
                " (1, 'FXCM', '1h', 'mid', true, 10, 1000, 'BACKFILL', false, 'SKIPPED',"
                "  'Skipped by daily category rotation policy', 0, now() + interval '1 day')"
            ))
            spec = importlib.util.spec_from_file_location(
                "sync_queue_migration",
                Path(__file__).resolve().parents[1] / "alembic/versions/f3b7c2d914ae_add_market_sync_queue.py",
            )
            migration = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(migration)
            with Operations.context(MigrationContext.configure(conn)):
                migration.upgrade()
        sync_engine.dispose()

        async_url = base_url.set(drivername="postgresql+psycopg")
        self.engine = create_async_engine(
            async_url, connect_args={"options": f"-csearch_path={self.schema}"},
        )
        self.service = FXCMMarketSyncService()

    async def asyncTearDown(self):
        await self.engine.dispose()
        with self.admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{self.schema}" CASCADE'))
        self.admin.dispose()

    def session(self):
        return AsyncSession(self.engine, expire_on_commit=False)

    async def test_migration_seeds_queue_and_releases_rotation_skips(self):
        async with self.session() as db:
            queued = (await db.execute(select(MarketSyncQueueItem.instrument_id))).scalars().all()
            state = (await db.execute(select(MarketBarSyncState))).scalars().one()
        self.assertEqual(queued, [1])
        self.assertEqual(state.last_status, "IDLE")
        self.assertIsNone(state.last_error)
        self.assertLessEqual(state.next_sync_from, utc_now())

    async def test_add_creates_due_states_and_remove_keeps_them(self):
        with patch.object(FXCMMarketSyncService, "sync_intervals", ["1h", "1day"]):
            async with self.session() as db:
                result = await self.service.add_to_sync_queue(db, symbols=[], instrument_ids=[2, 1, 404])
        self.assertEqual(result, {"added": ["XAU/USD"], "already_queued": ["EUR/USD"], "not_found": ["404"]})

        async with self.session() as db:
            states = (await db.execute(
                select(MarketBarSyncState).where(MarketBarSyncState.instrument_id == 2)
            )).scalars().all()
            self.assertEqual(sorted(s.interval for s in states), ["1day", "1h"])
            self.assertTrue(all(s.next_sync_from <= utc_now() for s in states))
            queue = await self.service.get_sync_queue(db)
            self.assertEqual([item["symbol"] for item in queue], ["EUR/USD", "XAU/USD"])
            candidates = await self.service.list_sync_candidates(db)
            self.assertEqual([item["symbol"] for item in candidates["items"]], ["US30"])

            await self.service.remove_from_sync_queue(db, instrument_ids=[2])
            synced = await self.service.get_sync_states(db)
            in_queue = {(item["symbol"], item["in_queue"]) for item in synced["items"]}
            self.assertIn(("XAU/USD", False), in_queue)
            self.assertIn(("EUR/USD", True), in_queue)

    async def test_due_states_only_come_from_queue(self):
        async with self.session() as db:
            db.add(MarketBarSyncState(
                instrument_id=3, provider="FXCM", interval="1h", price_type="mid", enabled=True,
                priority=10, target_history_bars=1000, sync_mode="BACKFILL", backfill_completed=False,
                last_status="IDLE", retry_count=0, next_sync_from=utc_now() - timedelta(minutes=1),
            ))
            await db.commit()

        synced_symbols = []

        async def fake_sync(db, state, instrument):
            synced_symbols.append(instrument.symbol)
            return 0

        with patch(
            "app.services.fxcm_market_sync.state_sync.bar_sync_handler.sync_single_state",
            side_effect=fake_sync,
        ):
            async with self.session() as db:
                result = FXCMMarketSyncResult(reason="test")
                await self.service.sync_due_states(db, result=result, force_due=True)
        self.assertEqual(synced_symbols, ["EUR/USD"])
