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
