"""sync_bars_incremental 在独立 PostgreSQL schema 中的测试；不使用应用数据库地址。

设置 MARKET_REPAIR_TEST_DATABASE_URL 指向隔离的测试库。
"""

import os
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from sqlalchemy import create_engine, event, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from app.models.market_data import MarketInstrument, MarketInstrumentAlias, MarketOHLCVBar
from app.services.market_data_replica import MarketDataReplicaService


START = datetime(2025, 1, 6, tzinfo=timezone.utc)


@unittest.skipUnless(os.getenv("MARKET_REPAIR_TEST_DATABASE_URL"), "isolated PostgreSQL URL required")
class ReplicaIncrementalTest(unittest.TestCase):
    def setUp(self):
        base_url = make_url(os.environ["MARKET_REPAIR_TEST_DATABASE_URL"])
        self.admin = create_engine(base_url)
        suffix = uuid.uuid4().hex
        self.schemas = [f"replica_inc_source_{suffix}", f"replica_inc_target_{suffix}"]
        with self.admin.begin() as conn:
            for schema in self.schemas:
                conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        self.addCleanup(self.cleanup)
        urls = [base_url.update_query_dict({"options": f"-csearch_path={schema}"})
                for schema in self.schemas]
        self.service = MarketDataReplicaService(*urls, batch_size=100)
        self.addCleanup(self.service.close)
        for engine in (self.service._source_engine, self.service._target_engine):
            for model in (MarketInstrument, MarketInstrumentAlias, MarketOHLCVBar):
                model.__table__.create(engine)
        for factory, instrument_id in ((self.service._source_session, 1), (self.service._target_session, 99)):
            with factory() as session:
                session.add(MarketInstrument(
                    id=instrument_id, provider="FXCM", symbol="XAU/USD", normalized_symbol="XAUUSD",
                    provider_symbol="XAU/USD", normalized_provider_symbol="XAUUSD", name="Gold",
                ))
                session.commit()

    def cleanup(self):
        with self.admin.begin() as conn:
            for schema in self.schemas:
                conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        self.admin.dispose()

    def bar(self, instrument_id, interval, hours, close=1000):
        return MarketOHLCVBar(
            instrument_id=instrument_id, provider="FXCM", provider_symbol="XAU/USD", interval=interval,
            price_type="mid", bar_time=START + timedelta(hours=hours),
            open=close, high=close, low=close, close=close, volume=1,
        )

    def target_hours(self, interval):
        with self.service._target_session() as target:
            times = target.scalars(
                select(MarketOHLCVBar.bar_time)
                .where(MarketOHLCVBar.interval == interval)
                .order_by(MarketOHLCVBar.bar_time)
            ).all()
        return [int((t - START).total_seconds() // 3600) for t in times]

    def test_pushes_newer_and_backfilled_older_bars(self):
        # 本地：向前补到 300、向更早回补到 0；远程只有中间的 100~200。
        with self.service._source_session() as source:
            source.add_all([self.bar(1, "1h", h) for h in range(0, 301)])
            source.commit()
        with self.service._target_session() as target:
            target.add_all([self.bar(99, "1h", h) for h in range(100, 201)])
            target.commit()

        self.service.sync_bars_incremental()

        self.assertEqual(self.target_hours("1h"), list(range(0, 301)))

    def test_empty_remote_series_is_copied_whole(self):
        with self.service._source_session() as source:
            source.add_all([self.bar(1, "4h", h * 4) for h in range(250)])
            source.commit()

        self.service.sync_bars_incremental()

        self.assertEqual(len(self.target_hours("4h")), 250)

    def test_each_series_is_committed_before_the_next(self):
        with self.service._source_session() as source:
            source.add_all([self.bar(1, "1h", h) for h in range(10)])
            source.add_all([self.bar(1, "5min", h) for h in range(10)])
            source.commit()

        original = self.service._upsert_bars

        def fail_on_5min(session, rows):
            if rows and rows[0]["interval"] == "5min":
                raise RuntimeError("target went away")
            return original(session, rows)

        with patch.object(self.service, "_upsert_bars", side_effect=fail_on_5min), \
                self.assertRaises(RuntimeError):
            self.service.sync_bars_incremental()

        # "1h" 排在 "5min" 前面，已推送并提交，不会随后一个序列的失败回滚。
        self.assertEqual(self.target_hours("1h"), list(range(10)))
        self.assertEqual(self.target_hours("5min"), [])

    def disconnect_before_statement(self, should_disconnect):
        # 只终止隔离测试库中本用例正在使用的目标连接。
        def listener(conn, cursor, statement, parameters, context, executemany):
            if should_disconnect(statement):
                pid = conn.connection.driver_connection.info.backend_pid
                with self.admin.connect() as admin:
                    self.assertTrue(admin.scalar(
                        text("SELECT pg_terminate_backend(:pid, 5000)"), {"pid": pid},
                    ))

        engine = self.service._target_engine
        event.listen(engine, "before_cursor_execute", listener)
        self.addCleanup(event.remove, engine, "before_cursor_execute", listener)

    def test_reconnects_after_watermark_query_disconnect(self):
        with self.service._source_session() as source:
            source.add(self.bar(1, "15min", 0))
            source.commit()

        def disconnect_first_watermark(statement):
            if "min(market_ohlcv_bar.bar_time)" in statement:
                watermarks.append(statement)
                return len(watermarks) == 1
            return False

        watermarks = []
        self.disconnect_before_statement(disconnect_first_watermark)
        self.assertEqual(self.service.sync_bars_incremental(), 1)
        self.assertEqual(len(watermarks), 2)
        self.assertEqual(self.target_hours("15min"), [0])

    def nth_bar_write_disconnects(self, n):
        writes = []

        def should_disconnect(statement):
            if statement.startswith("INSERT INTO market_ohlcv_bar"):
                writes.append(statement)
                return len(writes) == n
            return False

        self.disconnect_before_statement(should_disconnect)
        return writes

    def test_resumes_from_committed_batch_after_disconnect_on_second_page(self):
        with self.service._source_session() as source:
            source.add_all([self.bar(1, "1h", h) for h in range(105)])
            source.commit()

        writes = self.nth_bar_write_disconnects(2)
        with patch("app.services.market_data_replica.service.time.sleep"):
            # 第一批 100 根已提交；重连后只补推剩余部分（含 2 根重叠）。
            self.assertEqual(self.service.sync_bars_incremental(), 108)
        self.assertEqual(len(writes), 3)
        self.assertEqual(self.target_hours("1h"), list(range(105)))

    def test_backfilled_history_resumes_without_gap_after_disconnect(self):
        # 远程只有 250~260；本地回补到 0。回补按时间降序分批推，断连后从远程新的最早处续推。
        with self.service._source_session() as source:
            source.add_all([self.bar(1, "1h", h) for h in range(0, 261)])
            source.commit()
        with self.service._target_session() as target:
            target.add_all([self.bar(99, "1h", h) for h in range(250, 261)])
            target.commit()

        writes = self.nth_bar_write_disconnects(3)
        with patch("app.services.market_data_replica.service.time.sleep"):
            self.service.sync_bars_incremental()
        self.assertEqual(self.target_hours("1h"), list(range(0, 261)))

    def test_logs_overall_progress_per_batch(self):
        with self.service._source_session() as source:
            source.add_all([self.bar(1, "1h", h) for h in range(250)])
            source.commit()

        with self.assertLogs("app.services.market_data_replica.service", level="INFO") as logs:
            self.service.sync_bars_incremental()

        progress = [line for line in logs.output if "rows | series" in line]
        self.assertEqual(len(progress), 3)
        self.assertIn("250/250 rows | series 1/1 XAU/USD 1h mid (full) 250/250", progress[-1])
        self.assertIn("100.0%", progress[-1])

    def test_persistent_disconnect_is_reported(self):
        with self.service._source_session() as source:
            source.add(self.bar(1, "1h", 0))
            source.commit()
        self.disconnect_before_statement(lambda stmt: "min(market_ohlcv_bar.bar_time)" in stmt)
        with patch("app.services.market_data_replica.service.time.sleep") as backoff:
            with self.assertRaises(DBAPIError) as raised:
                self.service.sync_bars_incremental()
        self.assertTrue(raised.exception.connection_invalidated)
        self.assertEqual(backoff.call_count, 2)
        self.assertEqual(self.target_hours("1h"), [])
