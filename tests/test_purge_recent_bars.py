"""scripts/purge_recent_bars.py 在独立 PostgreSQL schema 中的端到端测试。

设置 MARKET_SYNC_TEST_DATABASE_URL 指向隔离的测试库。
"""

import os
import unittest
import uuid
from datetime import datetime, timezone
from unittest.mock import patch

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from app.models.market_data import MarketBarSyncState, MarketInstrument, MarketOHLCVBar
from scripts.purge_recent_bars import main


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


@unittest.skipUnless(os.getenv("MARKET_SYNC_TEST_DATABASE_URL"), "isolated PostgreSQL URL required")
class PurgeRecentBarsTest(unittest.TestCase):
    def setUp(self):
        base_url = make_url(os.environ["MARKET_SYNC_TEST_DATABASE_URL"])
        self.schema = f"purge_test_{uuid.uuid4().hex}"
        self.admin = create_engine(base_url)
        with self.admin.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{self.schema}"'))
        self.addCleanup(self.cleanup)
        self.url = base_url.update_query_dict(
            {"options": f"-csearch_path={self.schema}"}
        ).render_as_string(hide_password=False)
        self.engine = create_engine(self.url)
        for model in (MarketInstrument, MarketOHLCVBar, MarketBarSyncState):
            model.__table__.create(self.engine)
        with self.engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO market_instrument (id, provider, symbol, normalized_symbol, provider_symbol,"
                " normalized_provider_symbol, name, is_active, is_searchable, is_hot, supports_history,"
                " supports_quote, sort_weight) VALUES"
                " (1, 'FXCM', 'EUR/USD', 'EURUSD', 'EUR/USD', 'EURUSD', 'Euro', true, true, true, true, true, 0)"
            ))
            for interval, when in (("1h", utc(2025, 1, 1)), ("1h", utc(2026, 6, 1)), ("1h", utc(2026, 10, 27)),
                                   ("5min", utc(2026, 9, 1)), ("1day", utc(2024, 1, 1))):
                conn.execute(text(
                    "INSERT INTO market_ohlcv_bar (instrument_id, provider, provider_symbol, interval, price_type,"
                    " data_origin, bar_time, open, high, low, close) VALUES"
                    " (1, 'FXCM', 'EUR/USD', :interval, 'mid', 'provider', :when, 1, 1, 1, 1)"
                ), {"interval": interval, "when": when})
            conn.execute(text(
                "INSERT INTO market_bar_sync_state (instrument_id, provider, interval, price_type, enabled,"
                " priority, sync_mode, backfill_completed, last_status, retry_count, latest_synced_bar_time,"
                " earliest_synced_bar_time, next_sync_from, meta) VALUES"
                " (1, 'FXCM', '1h', 'mid', true, 10, 'COMPLETED', true, 'SUCCESS', 0,"
                "  '2026-10-27T00:00:00Z', '2025-01-01T00:00:00Z', NULL,"
                "  '{\"history_forward_until\": \"2026-10-01T00:00:00+00:00\", \"other\": 1}'),"
                " (1, 'FXCM', '5min', 'mid', true, 20, 'BACKFILL', true, 'SUCCESS', 0,"
                "  '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z', NULL, NULL),"
                " (1, 'FXCM', '1day', 'mid', true, 30, 'BACKFILL', false, 'SUCCESS', 0,"
                "  '2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z', '2030-01-01T00:00:00Z', NULL)"
            ))

    def cleanup(self):
        self.engine.dispose()
        with self.admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{self.schema}" CASCADE'))
        self.admin.dispose()

    def run_script(self, *extra):
        argv = ["purge_recent_bars.py", "--database-url", self.url, "--since", "2025-10-28", *extra]
        with patch("sys.argv", argv), patch("builtins.print"):
            return main()

    def bars(self):
        with self.engine.connect() as conn:
            return conn.execute(text(
                "SELECT interval, bar_time FROM market_ohlcv_bar ORDER BY interval, bar_time"
            )).all()

    def states(self):
        with self.engine.connect() as conn:
            rows = conn.execute(text("SELECT * FROM market_bar_sync_state")).mappings().all()
        return {row["interval"]: row for row in rows}

    def test_dry_run_changes_nothing(self):
        self.assertEqual(self.run_script(), 0)
        self.assertEqual(len(self.bars()), 5)
        self.assertEqual(self.states()["1h"]["sync_mode"], "COMPLETED")

    def test_execute_deletes_recent_bars_and_rewinds_states(self):
        self.assertEqual(self.run_script("--execute", "--yes"), 0)
        self.assertEqual(self.bars(), [("1day", utc(2024, 1, 1)), ("1h", utc(2025, 1, 1))])

        states = self.states()
        hourly = states["1h"]
        self.assertEqual(hourly["latest_synced_bar_time"], utc(2025, 1, 1))
        self.assertEqual(hourly["earliest_synced_bar_time"], utc(2025, 1, 1))
        self.assertTrue(hourly["backfill_completed"])
        self.assertEqual(hourly["sync_mode"], "BACKFILL")
        self.assertIsNotNone(hourly["next_sync_from"])
        self.assertEqual(hourly["meta"], {"other": 1})

        five = states["5min"]
        self.assertIsNone(five["latest_synced_bar_time"])
        self.assertIsNone(five["earliest_synced_bar_time"])
        self.assertFalse(five["backfill_completed"])

        daily = states["1day"]
        self.assertEqual(daily["next_sync_from"], utc(2030, 1, 1))
        self.assertEqual(daily["last_status"], "SUCCESS")

    def test_execute_requires_typed_confirmation(self):
        with patch("builtins.input", return_value="no"):
            self.assertEqual(self.run_script("--execute"), 1)
        self.assertEqual(len(self.bars()), 5)
