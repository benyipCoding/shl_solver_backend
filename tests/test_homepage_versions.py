"""Exercise publication, persistence, concurrency and permissions on an isolated DB."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import unittest

from alembic.migration import MigrationContext
from alembic.operations import Operations
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.clients.db import get_db
from app.router.homepage import router


class AsyncSessionAdapter:
    """Run the service's SQL against SQLite without an async driver dependency."""
    def __init__(self, session):
        self.session = session

    async def execute(self, statement):
        return self.session.execute(statement)

    async def commit(self):
        self.session.commit()

    async def rollback(self):
        self.session.rollback()


def load_migration():
    path = Path(__file__).parents[1] / "alembic/versions/b1c8d4e6f209_add_homepage_versions.py"
    spec = importlib.util.spec_from_file_location("homepage_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


class HomepageVersionsTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
        with self.engine.begin() as connection:
            connection.execute(text('CREATE TABLE "user" (id INTEGER PRIMARY KEY)'))
            connection.execute(text('INSERT INTO "user" (id) VALUES (1)'))
            with Operations.context(MigrationContext.configure(connection)):
                load_migration().upgrade()
        self.app = FastAPI()
        self.app.include_router(router)

        @self.app.middleware("http")
        async def identity(request: Request, call_next):
            role = request.headers.get("x-test-role")
            request.state.user = SimpleNamespace(id=1, is_active=role != "inactive", is_superuser=role in ("superuser", "inactive"), is_staff=role == "staff") if role else None
            return await call_next(request)

        async def isolated_db():
            with Session(self.engine) as session:
                yield AsyncSessionAdapter(session)

        self.app.dependency_overrides[get_db] = isolated_db
        self.client = TestClient(self.app)
        self.admin = {"x-test-role": "superuser"}

    def tearDown(self):
        self.client.close()
        self.engine.dispose()

    def publish(self, version, revision=0, headers=None):
        return self.client.patch("/admin/homepage", json={"active_version": version, "expected_revision": revision}, headers=headers or self.admin)

    def test_public_default_is_v1_and_contains_no_admin_metadata(self):
        response = self.client.get("/site/homepage")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"], {"active_version": 1})
        self.assertEqual(response.headers["cache-control"], "no-store")

    def test_only_active_superuser_can_read_or_publish_admin_config(self):
        for role, status in [(None, 401), ("user", 403), ("staff", 403), ("inactive", 403)]:
            headers = {"x-test-role": role} if role else {}
            with self.subTest(role=role):
                self.assertEqual(self.client.get("/admin/homepage", headers=headers).status_code, status)
                self.assertEqual(self.client.patch("/admin/homepage", headers=headers, json={"active_version": 2, "expected_revision": 0}).status_code, status)
        self.assertEqual(self.client.get("/site/homepage").json()["data"]["active_version"], 1)

    def test_publish_is_persistent_for_new_sessions_and_can_revert(self):
        self.assertEqual(self.publish(2).status_code, 200)
        self.assertEqual(self.client.get("/site/homepage").json()["data"]["active_version"], 2)
        config = self.client.get("/admin/homepage", headers=self.admin).json()["data"]
        self.assertEqual(config["revision"], 1)
        self.assertEqual(config["updated_by"], 1)
        self.assertTrue(config["updated_at"])
        self.assertEqual(self.publish(1, config["revision"]).status_code, 200)
        self.assertEqual(self.client.get("/site/homepage").json()["data"]["active_version"], 1)

    def test_stale_publish_does_not_overwrite_newer_selection(self):
        self.assertEqual(self.publish(2).status_code, 200)
        self.assertEqual(self.publish(1, 0).status_code, 409)
        self.assertEqual(self.client.get("/site/homepage").json()["data"]["active_version"], 2)

    def test_unknown_and_coerced_versions_are_rejected(self):
        for version in [0, 3, -1, "2", True, 2.0, None]:
            with self.subTest(version=version):
                self.assertEqual(self.publish(version).status_code, 422)
        for revision in [-1, "0", True, 0.5]:
            with self.subTest(revision=revision):
                self.assertEqual(self.publish(2, revision).status_code, 422)
        self.assertEqual(self.client.get("/site/homepage").json()["data"]["active_version"], 1)

    def test_migration_reverts_only_its_own_table(self):
        with self.engine.begin() as connection:
            self.assertIn("homepage_config", inspect(connection).get_table_names())
            with Operations.context(MigrationContext.configure(connection)):
                load_migration().downgrade()
            self.assertNotIn("homepage_config", inspect(connection).get_table_names())
            self.assertEqual(connection.scalar(text('SELECT COUNT(*) FROM "user"')), 1)


if __name__ == "__main__":
    unittest.main()
