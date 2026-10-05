"""Exercise the sharing migration against an isolated database, without app DB access."""
import importlib.util
from pathlib import Path
import unittest

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect, text


class BacktestBookmarkMigrationTest(unittest.TestCase):
    def test_upgrade_and_downgrade_preserve_existing_records(self):
        path = Path(__file__).parents[1] / "alembic/versions/e2f4a91c608b_add_backtest_bookmarks.py"
        spec = importlib.util.spec_from_file_location("bookmark_migration", path)
        migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)
        engine = create_engine("sqlite:///:memory:")
        try:
            with engine.begin() as connection:
                connection.execute(text('CREATE TABLE "user" (id INTEGER PRIMARY KEY)'))
                connection.execute(text("CREATE TABLE market_backtest_session (id INTEGER PRIMARY KEY)"))
                connection.execute(text("INSERT INTO market_backtest_session (id) VALUES (1)"))
                with Operations.context(MigrationContext.configure(connection)):
                    migration.upgrade()
                    schema = inspect(connection)
                    self.assertIn("market_backtest_bookmark", schema.get_table_names())
                    self.assertEqual(len(schema.get_foreign_keys("market_backtest_bookmark")), 2)
                    self.assertEqual(schema.get_unique_constraints("market_backtest_bookmark")[0]["column_names"], ["user_id", "session_id"])
                    migration.downgrade()
                    self.assertNotIn("market_backtest_bookmark", inspect(connection).get_table_names())
                    self.assertEqual(connection.scalar(text("SELECT COUNT(*) FROM market_backtest_session")), 1)
        finally:
            engine.dispose()
