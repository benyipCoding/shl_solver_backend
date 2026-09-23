"""Persistence regressions using isolated in-memory SQLite; no application DB calls."""
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.models.market_data import MarketBacktestEvent, MarketBacktestSession, MarketBacktestTrade
from app.schemas.market_backtest import BacktestEventCreate
from app.services.market_backtest import BacktestPersistError, MarketBacktestService


class AsyncTestSession:
    """Async facade over SQLite's bundled synchronous driver."""
    def __init__(self, session):
        self.session = session

    def add(self, value):
        self.session.add(value)

    async def execute(self, statement):
        return self.session.execute(statement)

    async def flush(self):
        self.session.flush()

    async def commit(self):
        self.session.commit()


class BacktestPersistenceTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        for model in (MarketBacktestSession, MarketBacktestTrade, MarketBacktestEvent):
            model.__table__.create(self.engine)
        self.sql = Session(self.engine, expire_on_commit=False)
        self.db = AsyncTestSession(self.sql)
        self.service = MarketBacktestService()
        self.user = SimpleNamespace(id=1)
        self.session = MarketBacktestSession(
            public_id="test-session", user_id=1, instrument_id=1, symbol="TEST",
            interval="1h", start_bar_time=datetime.now(timezone.utc),
        )
        self.sql.add(self.session)
        self.sql.commit()

    def tearDown(self):
        self.sql.close()
        self.engine.dispose()

    async def event(self, kind, **kwargs):
        return await self.service.record_event(self.db, self.user, "test-session", BacktestEventCreate(
            event_type=kind, client_trade_id="position-1", bar_time=1790121600, **kwargs,
        ))

    def trade(self):
        return self.sql.scalars(select(MarketBacktestTrade)).one()

    def events(self):
        return self.sql.scalars(select(MarketBacktestEvent).order_by(MarketBacktestEvent.sequence_no)).all()

    async def test_partial_close_retry_and_final_close(self):
        await self.event("OPEN", side="BUY", units=100, price=100, sl_price=90, tp_price=120)
        await self.event("CLOSE", units=50, price=110, client_event_id="fill-1")
        self.assertEqual(self.trade().status, "OPEN")
        self.assertEqual(self.trade().sl_price, Decimal(90))
        self.assertEqual(self.session.realized_pnl, Decimal(500))
        self.assertEqual(self.session.closed_trade_count, 0)
        await self.event("CLOSE", units=50, price=110, client_event_id="fill-1")
        self.assertEqual(len(self.events()), 2)
        self.assertEqual(self.session.realized_pnl, Decimal(500))
        await self.event("CLOSE", units=20, price=95, client_event_id="fill-2")
        await self.event("CLOSE", price=120, close_reason="TP_HIT")
        self.assertEqual(self.trade().status, "CLOSED")
        self.assertEqual(self.trade().realized_pnl, Decimal(1000))
        self.assertEqual(self.trade().close_price, Decimal(110))
        self.assertEqual(self.session.realized_pnl, Decimal(1000))
        self.assertEqual(self.session.closed_trade_count, 1)
        self.assertEqual(self.session.win_count, 1)
        self.assertEqual([e.units for e in self.events() if e.event_type == "CLOSE"], [50, 20, 30])

    async def test_risk_can_be_added_removed_and_serialized(self):
        await self.event("OPEN", side="SELL", units=100, price=100)
        await self.event("MODIFY_SL", price=110)
        await self.event("MODIFY_TP", price=80)
        await self.event("MODIFY_SL", price=None)
        await self.event("MODIFY_TP", price=None)
        self.assertIsNone(self.trade().sl_price)
        self.assertIsNone(self.trade().tp_price)
        serialized = self.service._serialize_event(self.events()[-1], "position-1")
        self.assertIsNone(serialized["price"])
        with self.assertRaises(BacktestPersistError):
            await self.event("MODIFY_SL")

    async def test_rejects_over_close_and_force_closes_only_remainder(self):
        await self.event("OPEN", side="SELL", units=100, price=100)
        await self.event("CLOSE", units=40, price=90)
        with self.assertRaises(BacktestPersistError):
            await self.event("CLOSE", units=61, price=110)
        self.assertEqual(self.session.realized_pnl, Decimal(400))
        await self.service._force_close_open_trades(
            self.db, self.session, mark_price=Decimal(110),
            bar_time=datetime.now(timezone.utc), bar_index=2,
        )
        self.assertEqual(self.events()[-1].units, 60)
        self.assertEqual(self.trade().realized_pnl, Decimal(-200))
        self.assertEqual(self.trade().close_price, Decimal(102))
        self.assertEqual(self.session.win_count, 0)
        self.assertEqual(self.session.closed_trade_count, 1)


if __name__ == "__main__":
    unittest.main()
