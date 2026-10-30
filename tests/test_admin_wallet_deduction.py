"""Exercise the real admin routes and wallet SQL against an isolated SQLite database."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from alembic.migration import MigrationContext
from alembic.operations import Operations
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select, text, inspect
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.clients.db import get_db
from app.models.user import User, UserCredit, UserCreditLog, CreditType
from app.models.shl_solver import ActionType
from app.router.admin import router


class AsyncSessionAdapter:
    def __init__(self, session):
        self.session = session

    async def execute(self, statement):
        return self.session.execute(statement)

    def add(self, item):
        self.session.add(item)

    async def commit(self):
        self.session.commit()

    async def rollback(self):
        self.session.rollback()


class AdminWalletDeductionTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
        for table in (User.__table__, UserCredit.__table__, UserCreditLog.__table__):
            table.create(self.engine)
        with Session(self.engine) as db:
            db.add_all([
                User(id=1, username="admin", email="admin@example.com", password="unused", is_superuser=True),
                User(id=2, username="target", email="target@example.com", password="unused"),
                User(id=3, username="empty", email="empty@example.com", password="unused"),
                UserCredit(user_id=2, free_credits=40, paid_credits=100),
            ])
            db.commit()
        self.app = FastAPI()
        self.app.include_router(router)

        @self.app.middleware("http")
        async def identity(request: Request, call_next):
            role = request.headers.get("x-test-role")
            request.state.user = SimpleNamespace(id=1, is_active=role != "inactive", is_superuser=role in ("admin", "inactive")) if role else None
            return await call_next(request)

        async def isolated_db():
            with Session(self.engine) as db:
                yield AsyncSessionAdapter(db)

        self.app.dependency_overrides[get_db] = isolated_db
        self.client = TestClient(self.app, raise_server_exceptions=False)

    def tearDown(self):
        self.client.close()
        self.engine.dispose()

    def deduct(self, amount=25, credit_type="PAID", user_id=2, reason="  撤回重复赠送  ", role="admin"):
        return self.client.post(f"/admin/wallets/{user_id}/deduct", headers={"x-test-role": role} if role else {},
                                json={"amount": amount, "credit_type": credit_type, "reason": reason})

    def balances(self):
        with Session(self.engine) as db:
            wallet = db.scalar(select(UserCredit).where(UserCredit.user_id == 2))
            logs = db.scalars(select(UserCreditLog)).all()
            return wallet.free_credits, wallet.paid_credits, len(logs)

    def test_paid_deduction_persists_and_exposes_audit_fields(self):
        response = self.deduct()
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["data"]["balance_after"], 115)
        self.assertEqual(self.balances(), (40, 75, 1))
        logs = self.client.get("/admin/credit-logs?action_type=ADMIN_DEDUCT", headers={"x-test-role": "admin"}).json()["data"]["items"]
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0]["amount"], -25)
        self.assertEqual(logs[0]["operator_user_id"], 1)
        self.assertEqual(logs[0]["reason"], "撤回重复赠送")
        detail = self.client.get(f'/admin/credit-logs/{logs[0]["id"]}', headers={"x-test-role": "admin"}).json()["data"]
        self.assertEqual(detail["reason"], logs[0]["reason"])

    def test_free_deduction_and_exact_balance_leave_other_pool_untouched(self):
        self.assertEqual(self.deduct(40, "FREE").status_code, 200)
        self.assertEqual(self.balances(), (0, 100, 1))
        self.assertEqual(self.deduct(100, "PAID").json()["data"]["balance_after"], 0)
        self.assertEqual(self.balances(), (0, 0, 2))

    def test_insufficient_pool_does_not_borrow_from_other_pool(self):
        response = self.deduct(41, "FREE")
        self.assertEqual(response.status_code, 409)
        self.assertIn("40", response.json()["detail"])
        self.assertEqual(self.balances(), (40, 100, 0))

    def test_repeated_requests_recheck_current_balance(self):
        self.assertEqual(self.deduct(80).status_code, 200)
        self.assertEqual(self.deduct(80).status_code, 409)
        self.assertEqual(self.balances(), (40, 20, 1))

    def test_existing_recharge_still_works_after_deduction(self):
        self.assertEqual(self.deduct(25).status_code, 200)
        response = self.client.post("/admin/wallets/2/recharge", headers={"x-test-role": "admin"}, json={"amount": 10})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["data"]["balance_after"], 125)
        self.assertEqual(self.balances(), (40, 85, 2))

    def test_missing_wallet_or_user_does_not_create_records(self):
        self.assertEqual(self.deduct(user_id=3).status_code, 409)
        self.assertEqual(self.deduct(user_id=999).status_code, 404)
        self.assertEqual(self.balances(), (40, 100, 0))

    def test_invalid_amount_type_reason_or_pool_is_rejected(self):
        for amount in (0, -1, 1.5, True, "10", 2147483648):
            with self.subTest(amount=amount):
                self.assertEqual(self.deduct(amount).status_code, 422)
        for reason in ("", "  ", "x" * 501):
            self.assertEqual(self.deduct(reason=reason).status_code, 422)
        self.assertEqual(self.deduct(credit_type="BOTH").status_code, 422)
        self.assertEqual(self.balances(), (40, 100, 0))

    def test_only_active_superuser_can_deduct(self):
        for role, status in ((None, 401), ("user", 403), ("staff", 403), ("inactive", 403)):
            self.assertEqual(self.deduct(role=role).status_code, status)
        self.assertEqual(self.balances(), (40, 100, 0))

    def test_commit_failure_rolls_back_balance_and_log(self):
        async def fail_commit(adapter):
            adapter.session.flush()
            raise RuntimeError("simulated commit failure")
        with patch.object(AsyncSessionAdapter, "commit", fail_commit):
            self.assertEqual(self.deduct().status_code, 500)
        self.assertEqual(self.balances(), (40, 100, 0))


class DeductionMigrationTest(unittest.TestCase):
    def test_upgrade_preserves_existing_logs_and_downgrade_protects_audit_history(self):
        path = Path(__file__).parents[1] / "alembic/versions/f9a4c2d71e60_add_admin_credit_deduction.py"
        spec = importlib.util.spec_from_file_location("deduction_migration", path)
        migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)
        engine = create_engine("sqlite://")
        with engine.begin() as connection:
            connection.execute(text("CREATE TABLE user_credit_log (id INTEGER PRIMARY KEY, action_type VARCHAR(30), amount INTEGER)"))
            connection.execute(text("INSERT INTO user_credit_log VALUES (1, 'TOP_UP', 50)"))
            with Operations.context(MigrationContext.configure(connection)):
                migration.upgrade()
                self.assertIn("reason", {column["name"] for column in inspect(connection).get_columns("user_credit_log")})
                self.assertEqual(connection.execute(text("SELECT amount FROM user_credit_log WHERE id=1")).scalar_one(), 50)
                connection.execute(text("INSERT INTO user_credit_log VALUES (2, 'ADMIN_DEDUCT', -10, 1, 'correction')"))
                with self.assertRaises(RuntimeError):
                    migration.downgrade()
                connection.execute(text("DELETE FROM user_credit_log WHERE id=2"))
                migration.downgrade()
                self.assertEqual(connection.execute(text("SELECT amount FROM user_credit_log WHERE id=1")).scalar_one(), 50)
        engine.dispose()


if __name__ == "__main__":
    unittest.main()
