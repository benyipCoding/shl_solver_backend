import unittest
import json
import sys
from types import SimpleNamespace
from unittest.mock import patch

import httpx

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core.config import settings
from app.router.market_realtime import normalize_event, router


class RealtimeGatewayTest(unittest.TestCase):
    def setUp(self):
        app = FastAPI()
        app.include_router(router)
        self.client = TestClient(app)

    def test_production_gate_does_not_contact_sidecar(self):
        for environment in ("prod", "production", "staging", "unknown"):
            with patch.object(settings, "app_env", environment), patch.object(settings, "fxcm_realtime_enabled", True), \
                    patch("app.router.market_realtime.httpx.AsyncClient") as upstream:
                self.assertEqual(self.client.get("/market_master/realtime/capabilities").json(), {"enabled": False})
                self.assertEqual(self.client.get("/market_master/realtime/stream?symbol=EUR/USD").status_code, 404)
                upstream.assert_not_called()

    def test_explicit_flag_required_and_unsupported_interval_rejected(self):
        with patch.object(settings, "app_env", "local"), patch.object(settings, "fxcm_realtime_enabled", False):
            self.assertFalse(self.client.get("/market_master/realtime/capabilities").json()["enabled"])
        with patch.object(settings, "app_env", "local"), patch.object(settings, "fxcm_realtime_enabled", True):
            self.assertTrue(self.client.get("/market_master/realtime/capabilities").json()["enabled"])
            self.assertEqual(self.client.get("/market_master/realtime/stream?symbol=EUR/USD&interval=2seconds").status_code, 400)

    def test_native_periods_use_same_date_identity_as_historical_frontend(self):
        for interval in ("1day", "1week", "1month"):
            event = {"type": "candles", "candles": [{"datetime": "2026-10-12T21:00:00+00:00", "timestamp": 1791838800}]}
            result = normalize_event(event, interval)
            self.assertEqual(result["candles"][0]["datetime"], "2026-10-12")
            self.assertEqual(result["candles"][0]["timestamp"], 1791838800)

    def test_intraday_timestamp_is_preserved(self):
        event = {"type": "candles", "candles": [{"datetime": "2026-10-12T21:00:00+00:00"}]}
        self.assertEqual(normalize_event(event, "4h"), event)

    def test_stream_forwards_snapshot_and_uses_provider_symbol_without_database(self):
        event = {"type": "snapshot", "candles": [{"datetime": "2026-10-12T21:00:00+00:00"}]}
        requests = []
        def respond(request):
            requests.append(request)
            return httpx.Response(200, text="data: " + json.dumps(event) + "\n\n")
        upstream = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        service = SimpleNamespace(_resolve_fxcm_symbol=lambda symbol: "XAU/USD")
        module = SimpleNamespace(market_master_service=service)
        with patch.object(settings, "app_env", "local"), patch.object(settings, "fxcm_realtime_enabled", True), \
                patch.dict(sys.modules, {"app.services.market_master": module}), \
                patch("app.router.market_realtime.httpx.AsyncClient", return_value=upstream):
            response = self.client.get("/market_master/realtime/stream?symbol=GOLD&interval=1day")
        self.assertIn("text/event-stream", response.headers["content-type"])
        data = json.loads(next(line[6:] for line in response.text.splitlines() if line.startswith("data: ")))
        self.assertEqual(data["candles"][0]["datetime"], "2026-10-12")
        self.assertEqual(requests[0].url.params["symbol"], "XAU/USD")
        self.assertEqual(requests[0].url.params["snapshot_size"], "256")

    def test_history_window_forwards_to_fxcm_without_database(self):
        values = [
            {
                "datetime": "2026-10-12",
                "timestamp": 1791763200,
                "open": 4100,
                "high": 4200,
                "low": 4050,
                "close": 4150,
            },
            {
                "datetime": "2026-10-13",
                "timestamp": 1791849600,
                "open": 4150,
                "high": 4250,
                "low": 4100,
                "close": 4200,
            },
        ]
        requests = []

        def respond(request):
            requests.append(request)
            return httpx.Response(
                200,
                json={
                    "meta": {
                        "symbol": "XAU/USD",
                        "provider_symbol": "XAU/USD",
                        "requested_interval": "1day",
                        "provider_interval": "D1",
                        "price_type": "mid",
                    },
                    "values": values,
                },
            )

        upstream = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        module = SimpleNamespace(
            market_master_service=SimpleNamespace(
                _resolve_fxcm_symbol=lambda symbol: "XAU/USD"
            )
        )
        with patch.object(settings, "app_env", "local"), patch.object(settings, "fxcm_realtime_enabled", True), \
                patch.dict(sys.modules, {"app.services.market_master": module}), \
                patch("app.router.market_realtime.httpx.AsyncClient", return_value=upstream):
            response = self.client.get(
                "/market_master/realtime/history?symbol=GOLD&interval=1day"
                "&outputsize=2&end_date=2026-10-11T00:00:00Z"
            )

        self.assertEqual(response.status_code, 200)
        data = response.json()["data"]
        self.assertEqual(data["values"], values)
        self.assertTrue(data["has_more_history"])
        self.assertEqual(requests[0].url.params["symbol"], "XAU/USD")
        self.assertEqual(requests[0].url.params["interval"], "1day")
        self.assertEqual(requests[0].url.params["end_date"], "2026-10-11T00:00:00Z")

    def test_snapshot_size_is_bounded_and_deep_recovery_is_forwarded(self):
        requests = []
        upstream = httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: (requests.append(request) or httpx.Response(200, text=": connected\n\n"))))
        module = SimpleNamespace(market_master_service=SimpleNamespace(_resolve_fxcm_symbol=lambda symbol: symbol))
        with patch.object(settings, "app_env", "local"), patch.object(settings, "fxcm_realtime_enabled", True), \
                patch.dict(sys.modules, {"app.services.market_master": module}), \
                patch("app.router.market_realtime.httpx.AsyncClient", return_value=upstream):
            for size in (0, 20001):
                self.assertEqual(self.client.get("/market_master/realtime/stream?symbol=EUR/USD&snapshot_size=" + str(size)).status_code, 422)
            self.assertEqual(self.client.get("/market_master/realtime/stream?symbol=EUR/USD&snapshot_size=20000").status_code, 200)
        self.assertEqual(requests[0].url.params["snapshot_size"], "20000")

    def test_chart_seed_is_forwarded_to_sidecar(self):
        requests = []
        upstream = httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: (requests.append(request) or httpx.Response(200, text="data: {\"type\":\"snapshot\"}\n\n"))))
        module = SimpleNamespace(market_master_service=SimpleNamespace(_resolve_fxcm_symbol=lambda symbol: symbol))
        with patch.object(settings, "app_env", "local"), patch.object(settings, "fxcm_realtime_enabled", True), \
                patch.dict(sys.modules, {"app.services.market_master": module}), \
                patch("app.router.market_realtime.httpx.AsyncClient", return_value=upstream):
            response = self.client.get(
                "/market_master/realtime/stream?symbol=EUR/USD&interval=1min"
                "&seed_timestamp=1792184280&seed_price=1.1239"
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(requests[0].url.params["seed_timestamp"], "1792184280.0")
        self.assertEqual(requests[0].url.params["seed_price"], "1.1239")
