"""历史数据采集队列与手动启停调度器。

数据库用例在独立 PostgreSQL schema 中运行；设置 MARKET_SYNC_TEST_DATABASE_URL 指向隔离的测试库。
"""

import asyncio
import importlib.util
import os
import sys
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.models.market_data import (
    MarketBarSyncState, MarketInstrument, MarketOHLCVBar, MarketSyncConfig, MarketSyncQueueItem,
)
from app.services.fxcm_market_sync.bar_sync import BarSyncHandler
from app.services.fxcm_market_sync.history_cutoff import (
    get_forward_cursor, is_history_complete, set_forward_cursor,
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


CUTOFF = datetime(2026, 10, 1, tzinfo=timezone.utc)


def history_state(interval="1h", latest=None, earliest=None, backfill_completed=True, sync_mode="BACKFILL"):
    return MarketBarSyncState(
        instrument_id=1, provider="FXCM", interval=interval, price_type="mid", enabled=True,
        latest_synced_bar_time=latest, earliest_synced_bar_time=earliest,
        backfill_completed=backfill_completed, sync_mode=sync_mode, meta=None,
    )


class HistoryCompletionTest(unittest.TestCase):
    def test_complete_needs_last_bar_before_cutoff_and_backfill(self):
        self.assertTrue(is_history_complete(history_state(latest=CUTOFF - timedelta(hours=1)), CUTOFF))
        self.assertFalse(is_history_complete(
            history_state(latest=CUTOFF - timedelta(hours=1), backfill_completed=False), CUTOFF))
        self.assertTrue(is_history_complete(history_state(latest=CUTOFF + timedelta(days=20)), CUTOFF))

    def test_forward_cursor_covers_closed_market_gap(self):
        # 截止时间落在周末：最后一根 K 线早于截止时间数十小时，靠游标判定已覆盖。
        state = history_state(latest=CUTOFF - timedelta(hours=50))
        self.assertFalse(is_history_complete(state, CUTOFF))
        set_forward_cursor(state, CUTOFF)
        self.assertTrue(is_history_complete(state, CUTOFF))
        self.assertFalse(is_history_complete(state, CUTOFF + timedelta(days=7)))

    def test_no_data_before_cutoff_counts_as_complete(self):
        state = history_state(backfill_completed=False)
        self.assertFalse(is_history_complete(state, CUTOFF))
        set_forward_cursor(state, CUTOFF)
        self.assertTrue(is_history_complete(state, CUTOFF))


class SyncHistoryStateTest(unittest.IsolatedAsyncioTestCase):
    async def run_state(self, state):
        handler = BarSyncHandler()
        handler._hydrate_state_from_storage = AsyncMock()
        handler._pull_forward_bars = AsyncMock(return_value=0)
        handler._backfill_step = AsyncMock(return_value=0)
        await handler.sync_history_state(object(), state, object(), cutoff=CUTOFF)
        return handler

    async def test_new_state_takes_latest_batch_before_cutoff(self):
        state = history_state(backfill_completed=False)
        handler = await self.run_state(state)
        kwargs = handler._pull_forward_bars.await_args.kwargs
        self.assertIsNone(kwargs["start_date"])
        self.assertEqual(kwargs["end_date"], CUTOFF.isoformat())
        self.assertEqual(get_forward_cursor(state), CUTOFF)
        handler._backfill_step.assert_awaited_once()

    async def test_forward_window_is_capped_by_batch_and_cutoff(self):
        latest = CUTOFF - timedelta(hours=5000)
        state = history_state(latest=latest)
        handler = await self.run_state(state)
        window_start = latest - timedelta(hours=2)
        window_end = window_start + timedelta(hours=2000)
        kwargs = handler._pull_forward_bars.await_args.kwargs
        self.assertEqual((kwargs["start_date"], kwargs["end_date"]), (window_start.isoformat(), window_end.isoformat()))
        self.assertEqual(get_forward_cursor(state), window_end)
        self.assertEqual(state.sync_mode, "BACKFILL")
        self.assertIsNotNone(state.next_sync_from)

        # 下一轮从游标继续，即使这段时间没有新 K 线（休市）也不会卡在原地。
        handler = await self.run_state(state)
        self.assertEqual(handler._pull_forward_bars.await_args.kwargs["start_date"], window_end.isoformat())

    async def test_reaching_cutoff_marks_completed_without_more_pulls(self):
        state = history_state(latest=CUTOFF - timedelta(hours=30))
        handler = await self.run_state(state)
        self.assertEqual(handler._pull_forward_bars.await_args.kwargs["end_date"], CUTOFF.isoformat())
        self.assertEqual(state.sync_mode, "COMPLETED")
        self.assertIsNone(state.next_sync_from)

        handler = await self.run_state(state)
        handler._pull_forward_bars.assert_not_awaited()


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
        for model in (MarketInstrument, MarketOHLCVBar, MarketBarSyncState, MarketSyncConfig):
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

        async def fake_sync(db, state, instrument, cutoff):
            synced_symbols.append(instrument.symbol)
            return 0

        with patch(
            "app.services.fxcm_market_sync.state_sync.bar_sync_handler.sync_history_state",
            side_effect=fake_sync,
        ):
            async with self.session() as db:
                result = FXCMMarketSyncResult(reason="test")
                await self.service.sync_due_states(db, result=result, force_due=True)
        self.assertEqual(synced_symbols, ["EUR/USD"])

    async def test_cutoff_change_reconciles_and_completed_states_are_skipped(self):
        async with self.session() as db:
            db.add_all([
                MarketBarSyncState(
                    instrument_id=1, provider="FXCM", interval="5min", price_type="mid", enabled=True,
                    priority=10, target_history_bars=1000, sync_mode="INCREMENTAL", backfill_completed=True,
                    latest_synced_bar_time=datetime(2026, 9, 30, 23, 55, tzinfo=timezone.utc),
                    earliest_synced_bar_time=datetime(2001, 1, 1, tzinfo=timezone.utc),
                    last_status="SUCCESS", retry_count=0, next_sync_from=utc_now() + timedelta(hours=6),
                ),
            ])
            await db.commit()

        async with self.session() as db:
            with self.assertRaises(ValueError):
                await self.service.set_history_cutoff(db, utc_now() + timedelta(days=1))
            result = await self.service.set_history_cutoff(db, CUTOFF)
        self.assertEqual(result["completed"], 1)

        async with self.session() as db:
            state = (await db.execute(
                select(MarketBarSyncState).where(MarketBarSyncState.interval == "5min")
            )).scalars().one()
            self.assertEqual(state.sync_mode, "COMPLETED")
            self.assertIsNone(state.next_sync_from)
            items = (await self.service.get_sync_states(db))["items"]
            self.assertTrue(next(i for i in items if i["interval"] == "5min")["history_complete"])

        synced = []

        async def fake_sync(db, state, instrument, cutoff):
            synced.append(state.interval)
            return 0

        with patch(
            "app.services.fxcm_market_sync.state_sync.bar_sync_handler.sync_history_state",
            side_effect=fake_sync,
        ):
            async with self.session() as db:
                await self.service.sync_due_states(db, result=FXCMMarketSyncResult(reason="test"), force_due=True)
        self.assertNotIn("5min", synced)

        async with self.session() as db:
            result = await self.service.set_history_cutoff(db, datetime(2026, 10, 15, tzinfo=timezone.utc))
            self.assertEqual(result["reopened"], 1)
            state = (await db.execute(
                select(MarketBarSyncState).where(MarketBarSyncState.interval == "5min")
            )).scalars().one()
            self.assertEqual(state.sync_mode, "BACKFILL")
            self.assertLessEqual(state.next_sync_from, utc_now())
