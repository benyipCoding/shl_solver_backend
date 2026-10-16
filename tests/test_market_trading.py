import json
import sys
import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core.config import settings
from app.depends.jwt_guard import verify_superuser
from app.router.market_trading import router


class TradingGatewayTest(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(router)
        self.client = TestClient(self.app)

    def enabled(self):
        stack = ExitStack()
        for name, value in [("app_env", "local"), ("fxcm_realtime_enabled", True),
                            ("fxcm_trading_enabled", True), ("fxcm_trading_bridge_key", "test-key")]:
            stack.enter_context(patch.object(settings, name, value))
        return stack

    def test_production_and_anonymous_never_reach_broker(self):
        with self.enabled(), patch("app.router.market_trading.httpx.AsyncClient") as upstream:
            self.assertEqual(self.client.get("/market_master/trading/state?symbol=EUR/USD").status_code, 401)
            self.app.dependency_overrides[verify_superuser] = lambda: object()
            for env in ["prod", "production", "staging", "unknown"]:
                with patch.object(settings, "app_env", env):
                    self.assertEqual(self.client.post("/market_master/trading/commands", json={}).status_code, 404)
            upstream.assert_not_called()

    def test_bridge_preserves_command_identity_and_resolves_symbol(self):
        requests = []
        def respond(request):
            requests.append(request)
            return httpx.Response(200, json={"status": "confirmed"})
        upstream = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        self.app.dependency_overrides[verify_superuser] = lambda: object()
        module = SimpleNamespace(market_master_service=SimpleNamespace(_resolve_fxcm_symbol=lambda _: "XAU/USD"))
        payload = {"client_id": "uuid", "symbol": "GOLD", "action": "open", "amount": 1}
        with self.enabled(), patch.dict(sys.modules, {"app.services.market_master": module}), \
                patch("app.router.market_trading.httpx.AsyncClient", return_value=upstream):
            result = self.client.post("/market_master/trading/commands", json=payload)
        self.assertEqual(result.json()["status"], "confirmed")
        self.assertEqual(requests[0].headers["x-fxcm-bridge-key"], "test-key")
        self.assertEqual(json.loads(requests[0].content), dict(payload, symbol="XAU/USD"))

    def test_trading_flag_and_route_allowlist(self):
        self.app.dependency_overrides[verify_superuser] = lambda: object()
        with self.enabled(), patch("app.router.market_trading.httpx.AsyncClient") as upstream:
            self.assertEqual(self.client.post("/market_master/trading/state", json={}).status_code, 404)
            self.assertEqual(self.client.get("/market_master/trading/commands/not-a-uuid").status_code, 404)
            with patch.object(settings, "fxcm_trading_enabled", False):
                self.assertEqual(self.client.post("/market_master/trading/commands", json={}).status_code, 404)
            upstream.assert_not_called()

    def test_history_query_is_read_only_and_forwards_account_filters(self):
        requests = []

        def respond(request):
            requests.append(request)
            return httpx.Response(200, json={"type": "history", "items": []})

        upstream = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        self.app.dependency_overrides[verify_superuser] = lambda: object()
        with self.enabled(), patch("app.router.market_trading.httpx.AsyncClient", return_value=upstream):
            result = self.client.get(
                "/market_master/trading/history?account_id=demo&from_ts=1&to_ts=2"
            )
        self.assertEqual(result.status_code, 200)
        self.assertEqual(requests[0].url.params.get("account_id"), "demo")
        self.assertEqual(requests[0].url.params.get("from_ts"), "1")
        self.assertEqual(requests[0].url.params.get("to_ts"), "2")
