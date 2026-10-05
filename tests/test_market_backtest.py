"""Persistence regressions using isolated in-memory SQLite; no application DB calls."""
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from sqlalchemy import create_engine, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.market_data import MarketBacktestBookmark, MarketBacktestEvent, MarketBacktestSession, MarketBacktestTrade
from app.schemas.market_backtest import BacktestEventCreate, BacktestSessionComplete
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
        for model in (MarketBacktestSession, MarketBacktestTrade, MarketBacktestEvent, MarketBacktestBookmark):
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

    async def share_with(self, user_id=2):
        await self.event("OPEN", side="BUY", units=100, price=100)
        await self.event("CLOSE", price=110)
        await self.service.share_session(self.db, self.user, "test-session")
        recipient = SimpleNamespace(id=user_id)
        result = await self.service.save_shared_session(self.db, recipient, "test-session")
        return recipient, result

    async def test_share_import_preserves_replay_and_is_idempotent(self):
        recipient, imported = await self.share_with()
        owner_detail = await self.service.get_session_detail(self.db, self.user, "test-session")
        self.assertFalse(imported["already_saved"])
        self.assertTrue(imported["session"]["is_shared"])
        self.assertNotIn("client_session_id", imported["session"])
        self.assertEqual(imported["session"]["events"], owner_detail["events"])
        self.assertEqual(imported["session"]["trades"], owner_detail["trades"])
        again = await self.service.save_shared_session(self.db, recipient, "test-session")
        self.assertTrue(again["already_saved"])
        listing = await self.service.list_sessions(self.db, recipient, 1, 20)
        self.assertEqual(listing["total"], 1)
        self.assertTrue(listing["items"][0]["is_shared"])
        self.assertTrue(listing["items"][0]["is_available"])
        self.assertIsNotNone(listing["items"][0]["saved_at"])
        self.assertEqual(self.sql.scalar(select(func.count()).select_from(MarketBacktestSession)), 1)
        self.assertEqual(self.sql.scalar(select(func.count()).select_from(MarketBacktestBookmark)), 1)

    async def test_private_records_and_unknown_links_cannot_be_imported(self):
        recipient = SimpleNamespace(id=2)
        for public_id in ("test-session", "unknown"):
            with self.assertRaises(BacktestPersistError) as error:
                await self.service.save_shared_session(self.db, recipient, public_id)
            self.assertEqual(error.exception.status_code, 404)
        with self.assertRaises(BacktestPersistError):
            await self.service.share_session(self.db, recipient, "test-session")
        self.assertEqual(self.session.visibility, "PRIVATE")
        self.assertEqual(self.sql.scalar(select(func.count()).select_from(MarketBacktestBookmark)), 0)

    async def test_shared_replays_do_not_grant_owner_write_permissions(self):
        recipient, _ = await self.share_with()
        stranger = SimpleNamespace(id=3)
        with self.assertRaises(BacktestPersistError):
            await self.service.get_session_detail(self.db, stranger, "test-session")
        with self.assertRaises(BacktestPersistError):
            await self.service.share_session(self.db, stranger, "test-session")
        with self.assertRaises(BacktestPersistError):
            await self.service.record_event(self.db, recipient, "test-session", BacktestEventCreate(
                event_type="OPEN", client_trade_id="intruder", bar_time=1790121600, side="BUY", units=1, price=100,
            ))
        with self.assertRaises(BacktestPersistError):
            await self.service.complete_session(self.db, recipient, "test-session", BacktestSessionComplete())
        with self.assertRaises(BacktestPersistError):
            await self.service.revoke_share(self.db, recipient, "test-session")
        # A recipient can forward the same existing link, without changing ownership.
        forwarded = await self.service.share_session(self.db, recipient, "test-session")
        self.assertEqual(forwarded["public_id"], "test-session")
        self.assertEqual(self.session.user_id, 1)
        self.assertEqual(len(self.events()), 2)

    async def test_removing_bookmark_preserves_source_and_can_be_saved_again(self):
        recipient, _ = await self.share_with()
        recipient.is_superuser = True  # List actions must still only remove their bookmark.
        removed = await self.service.delete_session(self.db, recipient, "test-session")
        self.assertTrue(removed["removed_bookmark"])
        self.assertIsNone(self.session.deleted_at)
        self.assertTrue(all(event.deleted_at is None for event in self.events()))
        self.assertEqual((await self.service.list_sessions(self.db, recipient, 1, 20))["total"], 0)
        with self.assertRaises(BacktestPersistError):
            await self.service.get_session_detail(self.db, recipient, "test-session")
        imported = await self.service.save_shared_session(self.db, recipient, "test-session")
        self.assertFalse(imported["already_saved"])
        self.assertEqual(self.sql.scalar(select(func.count()).select_from(MarketBacktestBookmark)), 1)

    async def test_revoked_share_is_unavailable_until_owner_shares_again(self):
        recipient, _ = await self.share_with()
        await self.service.revoke_share(self.db, self.user, "test-session")
        with self.assertRaises(BacktestPersistError):
            await self.service.get_session_detail(self.db, recipient, "test-session")
        with self.assertRaises(BacktestPersistError):
            await self.service.save_shared_session(self.db, recipient, "test-session")
        listing = await self.service.list_sessions(self.db, recipient, 1, 20)
        self.assertFalse(listing["items"][0]["is_available"])
        await self.service.share_session(self.db, self.user, "test-session")
        imported = await self.service.save_shared_session(self.db, recipient, "test-session")
        self.assertTrue(imported["already_saved"])
        self.assertTrue(imported["session"]["is_available"])

    async def test_deleted_source_leaves_removable_unavailable_bookmark(self):
        recipient, _ = await self.share_with()
        await self.service.delete_session(self.db, self.user, "test-session")
        self.assertEqual((await self.service.list_sessions(self.db, self.user, 1, 20))["total"], 0)
        listing = await self.service.list_sessions(self.db, recipient, 1, 20)
        self.assertEqual(listing["total"], 1)
        self.assertFalse(listing["items"][0]["is_available"])
        with self.assertRaises(BacktestPersistError):
            await self.service.save_shared_session(self.db, recipient, "test-session")
        await self.service.delete_session(self.db, recipient, "test-session")
        self.assertEqual((await self.service.list_sessions(self.db, recipient, 1, 20))["total"], 0)

    async def test_owner_link_does_not_create_duplicate_and_lists_are_paginated(self):
        recipient, _ = await self.share_with()
        own_import = await self.service.save_shared_session(self.db, self.user, "test-session")
        self.assertTrue(own_import["already_saved"])
        self.assertFalse(own_import["session"]["is_shared"])
        self.sql.add(MarketBacktestSession(
            public_id="recipient-own", user_id=recipient.id, instrument_id=1, symbol="OTHER",
            interval="1h", start_bar_time=datetime.now(timezone.utc),
        ))
        self.sql.commit()
        first = await self.service.list_sessions(self.db, recipient, 1, 1)
        second = await self.service.list_sessions(self.db, recipient, 2, 1)
        self.assertEqual(first["total"], 2)
        self.assertEqual(second["total"], 2)
        self.assertNotEqual(first["items"][0]["public_id"], second["items"][0]["public_id"])
        self.assertEqual((await self.service.list_sessions(self.db, SimpleNamespace(id=3), 1, 20))["total"], 0)
        self.sql.add(MarketBacktestBookmark(user_id=recipient.id, session_id=self.session.id))
        with self.assertRaises(IntegrityError):
            self.sql.flush()
        self.sql.rollback()

    async def test_share_http_routes_enforce_login_and_connect_to_saved_history(self):
        from fastapi import FastAPI
        from httpx import ASGITransport, AsyncClient
        from app.clients.db import get_db
        from app.router.market_master import router

        app = FastAPI()
        app.include_router(router)

        @app.middleware("http")
        async def test_auth(request, call_next):
            if request.headers.get("x-test-user"):
                request.state.user = SimpleNamespace(id=int(request.headers["x-test-user"]), is_active=True)
            return await call_next(request)

        async def test_db():
            yield self.db

        app.dependency_overrides[get_db] = test_db
        base = "/market_master/backtest"
        share = f"{base}/sessions/test-session/share"
        imported = f"{base}/shared/test-session"
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            for method, path in (("POST", share), ("DELETE", share), ("POST", imported)):
                self.assertEqual((await client.request(method, path)).status_code, 401)
            owner = {"x-test-user": "1"}
            recipient = {"x-test-user": "2"}
            self.assertEqual((await client.post(imported, headers=recipient)).status_code, 404)
            response = await client.post(share, headers=owner)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["data"]["visibility"], "UNLISTED")
            response = await client.post(imported, headers=recipient)
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.json()["data"]["session"]["is_shared"])
            listing = await client.get(f"{base}/sessions", headers=recipient)
            self.assertEqual(listing.json()["data"]["total"], 1)
            self.assertEqual((await client.delete(share, headers=recipient)).status_code, 404)
            self.assertEqual((await client.delete(share, headers=owner)).status_code, 200)
            self.assertEqual((await client.get(f"{base}/sessions/test-session", headers=recipient)).status_code, 404)
            self.assertEqual((await client.delete(f"{base}/sessions/test-session", headers=recipient)).status_code, 200)
            self.assertIsNone(self.session.deleted_at)


if __name__ == "__main__":
    unittest.main()
