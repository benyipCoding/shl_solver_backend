"""Integration tests in unique PostgreSQL schemas; never use application DB URLs.

Set MARKET_REPAIR_TEST_DATABASE_URL to an isolated PostgreSQL test database.
"""

import asyncio
import importlib.util
import os
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Event
from pathlib import Path
from unittest.mock import AsyncMock, patch

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, event, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.models.market_data import (
    MarketBarSyncState, MarketInstrument, MarketInstrumentAlias, MarketOHLCVBar,
)
from app.models.market_repair_outbox import MarketRepairOutbox
from app.services.fxcm_market_sync.bar_sync import bar_sync_handler
from app.services.fxcm_market_sync.service import FXCMMarketSyncService
from app.services.market_data_replica import MarketDataReplicaService
from app.services.market_data_replica.types import MarketReplicaResult
from scripts.sync_market_to_prod import main


class RepairCliTest(unittest.TestCase):
    def test_no_arguments_consumes_queue_and_failure_sets_exit_code(self):
        with patch("sys.argv", ["sync_market_to_prod.py", "repair", "--json"]), \
                patch("scripts.sync_market_to_prod.resolve_database_urls", return_value=("source", "target")), \
                patch("app.services.market_data_replica.MarketDataReplicaService") as factory, \
                patch("builtins.print"):
            factory.return_value.run_pending_repairs.return_value = MarketReplicaResult(
                mode="repair", errors=["failed"],
            )
            self.assertEqual(main(), 1)
            factory.return_value.run_pending_repairs.assert_called_once_with()
            factory.return_value.close.assert_called_once_with()

    def test_partial_range_rejected_before_database_access(self):
        with patch("sys.argv", ["sync_market_to_prod.py", "repair", "--symbol", "XAU/USD"]), \
                patch("scripts.sync_market_to_prod.resolve_database_urls") as urls, \
                patch("sys.stderr"), self.assertRaises(SystemExit) as error:
            main()
        self.assertEqual(error.exception.code, 2)
        urls.assert_not_called()

    def test_explicit_range_keeps_legacy_behavior(self):
        with patch("sys.argv", ["sync_market_to_prod.py", "repair", "--symbol", "XAU/USD",
                               "--interval", "5min", "--start-date", "2026-07-01",
                               "--end-date", "2026-08-31"]), \
                patch("scripts.sync_market_to_prod.resolve_database_urls", return_value=("source", "target")), \
                patch("app.services.market_data_replica.MarketDataReplicaService") as factory, \
                patch("builtins.print"):
            factory.return_value.run_range_repair.return_value = MarketReplicaResult(mode="repair")
            self.assertEqual(main(), 0)
            factory.return_value.run_range_repair.assert_called_once()
            factory.return_value.run_pending_repairs.assert_not_called()


@unittest.skipUnless(os.getenv("MARKET_REPAIR_TEST_DATABASE_URL"), "isolated PostgreSQL URL required")
class RepairOutboxPostgresTest(unittest.TestCase):
    def setUp(self):
        self.base_url = make_url(os.environ["MARKET_REPAIR_TEST_DATABASE_URL"])
        self.admin = create_engine(self.base_url)
        suffix = uuid.uuid4().hex
        self.schemas = [f"repair_test_source_{suffix}", f"repair_test_target_{suffix}"]
        with self.admin.begin() as conn:
            for schema in self.schemas:
                conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        self.addCleanup(self.cleanup_schemas)
        urls = [self.base_url.update_query_dict({"options": f"-csearch_path={schema}"})
                for schema in self.schemas]
        self.service = MarketDataReplicaService(*urls)
        self.addCleanup(self.service.close)
        for engine in (self.service._source_engine, self.service._target_engine):
            for model in (MarketInstrument, MarketInstrumentAlias, MarketOHLCVBar, MarketBarSyncState):
                model.__table__.create(engine)
        spec = importlib.util.spec_from_file_location(
            "repair_outbox_migration",
            Path(__file__).resolve().parents[1] / "alembic/versions/d8a2f679b031_add_market_repair_outbox.py",
        )
        self.migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.migration)
        with self.service._source_engine.begin() as conn:
            with Operations.context(MigrationContext.configure(conn)):
                self.migration.upgrade()
        self.start = datetime(2026, 7, 1, tzinfo=timezone.utc)
        self.end = self.start + timedelta(minutes=10)
        for factory, instrument_id in ((self.service._source_session, 1), (self.service._target_session, 99)):
            with factory() as session:
                session.add(MarketInstrument(
                    id=instrument_id, provider="FXCM", symbol="XAU/USD", normalized_symbol="XAUUSD",
                    provider_symbol="XAU/USD", normalized_provider_symbol="XAUUSD", name="Gold",
                ))
                session.commit()
        with self.service._source_session() as session:
            session.add(self.bar(1, self.start, 2000))
            session.commit()

    def cleanup_schemas(self):
        with self.admin.begin() as conn:
            for schema in self.schemas:
                conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        self.admin.dispose()

    def bar(self, instrument_id, when, close):
        return MarketOHLCVBar(
            instrument_id=instrument_id, provider="FXCM", provider_symbol="XAU/USD",
            interval="5min", price_type="mid", bar_time=when,
            open=close, high=close, low=close, close=close, volume=100,
        )

    def enqueue(self, interval="5min"):
        with self.service._source_session() as source:
            job = MarketRepairOutbox(
                instrument_id=1, provider="FXCM", symbol="XAU/USD", interval=interval,
                price_type="mid", start_at=self.start, end_at=self.end,
            )
            source.add(job)
            source.commit()
            return job.id

    def test_sync_delete_extras_preserve_outside_range_and_skip_completed(self):
        job_id = self.enqueue()
        outside = self.end + timedelta(minutes=5)
        with self.service._target_session() as target:
            target.add_all([self.bar(99, self.start, 1000),
                            self.bar(99, self.start + timedelta(minutes=5), 1000),
                            self.bar(99, outside, 1000)])
            target.commit()
        result = self.service.run_pending_repairs()
        self.assertFalse(result.errors)
        self.assertEqual((result.repair_jobs_succeeded, result.bars_upserted, result.bars_deleted), (1, 1, 1))
        with self.service._target_session() as target:
            rows = target.scalars(select(MarketOHLCVBar).order_by(MarketOHLCVBar.bar_time)).all()
            self.assertEqual([(r.bar_time, int(r.close)) for r in rows], [(self.start, 2000), (outside, 1000)])
        with self.service._source_session() as source:
            job = source.get(MarketRepairOutbox, job_id)
            self.assertIsNotNone(job.synced_at)
            self.assertEqual(job.attempts, 1)
        self.assertEqual(self.service.run_pending_repairs().repair_jobs_succeeded, 0)

    def test_failed_job_does_not_block_others_and_retries_next_run(self):
        failed_id = self.enqueue("1h")  # Empty local range must not wipe remote bars.
        self.enqueue()
        result = self.service.run_pending_repairs()
        self.assertEqual((result.repair_jobs_failed, result.repair_jobs_succeeded, result.repair_jobs_pending), (1, 1, 1))
        with self.service._source_session() as source:
            job = source.get(MarketRepairOutbox, failed_id)
            self.assertIsNone(job.synced_at)
            self.assertIn("no bars", job.last_error)
            row = self.bar(1, self.start, 2000)
            row.interval = "1h"
            source.add(row)
            source.commit()
        result = self.service.run_pending_repairs()
        self.assertEqual((result.repair_jobs_succeeded, result.repair_jobs_pending), (1, 0))
        with self.service._source_session() as source:
            job = source.get(MarketRepairOutbox, failed_id)
            self.assertEqual(job.attempts, 2)
            self.assertIsNone(job.last_error)

    def test_remote_failure_rolls_back_and_post_commit_crash_replays(self):
        self.enqueue()
        original = self.service._upsert_bars

        def fail_after_upsert(*args):
            original(*args)
            raise RuntimeError("remote failure before commit")

        with patch.object(self.service, "_upsert_bars", side_effect=fail_after_upsert):
            self.assertEqual(self.service.run_pending_repairs().repair_jobs_failed, 1)
        with self.service._target_session() as target:
            self.assertEqual(target.scalar(select(func.count()).select_from(MarketOHLCVBar)), 0)

        original_repair = self.service.sync_bars_range_repair

        def crash_after_target_commit(**kwargs):
            original_repair(**kwargs)
            raise KeyboardInterrupt("process stopped before local acknowledgement")

        with patch.object(self.service, "sync_bars_range_repair", side_effect=crash_after_target_commit):
            with self.assertRaises(KeyboardInterrupt):
                self.service.run_pending_repairs()
        result = self.service.run_pending_repairs()
        self.assertEqual(result.repair_jobs_succeeded, 1)
        with self.service._target_session() as target:
            self.assertEqual(target.scalar(select(func.count()).select_from(MarketOHLCVBar)), 1)

    def test_concurrent_worker_skips_claimed_job_and_defers_new_jobs(self):
        self.enqueue()
        started, release = Event(), Event()
        original = self.service.sync_bars_range_repair

        def slow_repair(**kwargs):
            started.set()
            if not release.wait(10):
                raise RuntimeError("test worker timeout")
            return original(**kwargs)

        with ThreadPoolExecutor(max_workers=1) as pool:
            with patch.object(self.service, "sync_bars_range_repair", side_effect=slow_repair):
                future = pool.submit(self.service.run_pending_repairs)
                try:
                    self.assertTrue(started.wait(10))
                    other = self.service.run_pending_repairs()
                    self.assertEqual((other.repair_jobs_succeeded, other.repair_jobs_pending), (0, 1))
                    self.enqueue()
                finally:
                    release.set()
                result = future.result(timeout=10)
        self.assertEqual((result.repair_jobs_succeeded, result.repair_jobs_pending), (1, 1))
        self.assertEqual(self.service.run_pending_repairs().repair_jobs_succeeded, 1)

    def test_overlapping_repairs_read_fresh_bars_after_target_lock(self):
        self.enqueue()
        self.enqueue()
        first_wrote, second_waiting, release = Event(), Event(), Event()
        original = self.service._upsert_bars

        def pause_first_write(target, rows):
            count = original(target, rows)
            if not first_wrote.is_set():
                first_wrote.set()
                if not release.wait(10):
                    raise RuntimeError("test writer timeout")
            return count

        def detect_wait(conn, cursor, statement, parameters, context, executemany):
            if "pg_advisory_xact_lock" in statement and first_wrote.is_set():
                second_waiting.set()

        event.listen(self.service._target_engine, "before_cursor_execute", detect_wait)
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                with patch.object(self.service, "_upsert_bars", side_effect=pause_first_write):
                    first = pool.submit(self.service.run_pending_repairs)
                    try:
                        self.assertTrue(first_wrote.wait(10))
                        second = pool.submit(self.service.run_pending_repairs)
                        self.assertTrue(second_waiting.wait(10))
                        with self.service._source_session() as source:
                            source.scalar(select(MarketOHLCVBar)).close = 2100
                            source.commit()
                    finally:
                        release.set()
                    self.assertFalse(first.result(timeout=10).errors)
                    self.assertFalse(second.result(timeout=10).errors)
            with self.service._target_session() as target:
                self.assertEqual(int(target.scalar(select(MarketOHLCVBar.close))), 2100)
        finally:
            event.remove(self.service._target_engine, "before_cursor_execute", detect_wait)

    def test_successful_local_repairs_append_and_outbox_failure_rolls_back_bars(self):
        async def repair(close, fail_outbox=False):
            engine = create_async_engine(
                self.base_url.set(drivername="postgresql+asyncpg", query={}),
                connect_args={"server_settings": {"search_path": self.schemas[0]}},
            )
            try:
                async with AsyncSession(engine, expire_on_commit=False) as db:
                    service = FXCMMarketSyncService()
                    job = service._enqueue_priority_job("XAU/USD", "5min", kind="repair",
                                                        start_at=self.start, end_at=self.end)
                    row = self.bar(1, self.start, close)
                    payload = {key: getattr(row, key) for key in (
                        "instrument_id", "provider", "provider_symbol", "interval", "price_type",
                        "bar_time", "open", "high", "low", "close", "volume",
                    )}
                    add = db.add

                    def maybe_fail(value):
                        if fail_outbox and isinstance(value, MarketRepairOutbox):
                            raise RuntimeError("outbox unavailable")
                        add(value)

                    with patch.object(bar_sync_handler, "_fetch_range_bars", AsyncMock(return_value=[payload])), \
                            patch.object(db, "add", side_effect=maybe_fail):
                        try:
                            return await service._run_priority_job(db, job)
                        finally:
                            if job.future.done():
                                job.future.exception()  # Consume expected failure in this test.
            finally:
                await engine.dispose()

        first = asyncio.run(repair(2100))
        second = asyncio.run(repair(2100))  # Even unchanged bars produce a new outbox entry.
        self.assertNotEqual(first["replica_job_id"], second["replica_job_id"])
        with self.assertRaisesRegex(RuntimeError, "outbox unavailable"):
            asyncio.run(repair(2200, fail_outbox=True))
        with self.service._source_session() as source:
            self.assertEqual(source.scalar(select(func.count()).select_from(MarketRepairOutbox)), 2)
            self.assertEqual(int(source.scalar(select(MarketOHLCVBar.close))), 2100)

    def test_migration_downgrade_and_upgrade(self):
        with self.service._source_engine.begin() as conn:
            with Operations.context(MigrationContext.configure(conn)):
                self.migration.downgrade()
                self.migration.upgrade()
        self.assertEqual(self.service.run_pending_repairs().repair_jobs_pending, 0)


if __name__ == "__main__":
    unittest.main()
