import logging
from typing import Any, Mapping, Sequence

import httpx

from app.core.config import settings


logger = logging.getLogger(__name__)


class FXCMSidecarError(Exception):
    def __init__(self, status_code: int, message: str, payload: Any | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.payload = payload


class FXCMSidecarService:
    DEFAULT_TIMEOUT = httpx.Timeout(settings.fxcm_api_timeout_seconds, connect=10.0)
    MAX_REQUEST_ATTEMPTS = 2

    async def get_history(
        self,
        *,
        symbol: str,
        interval: str,
        outputsize: int,
        start_date: str | None = None,
        end_date: str | None = None,
        price_type: str = "mid",
    ) -> dict[str, Any]:
        return await self._request_json(
            "/history",
            params={
                "symbol": symbol,
                "interval": interval,
                "outputsize": outputsize,
                "start_date": start_date,
                "end_date": end_date,
                "price_type": price_type,
            },
        )

    async def get_quote(
        self,
        *,
        symbol: str,
        interval: str = "1day",
        price_type: str = "mid",
    ) -> dict[str, Any]:
        return await self._request_json(
            "/quote",
            params={
                "symbol": symbol,
                "interval": interval,
                "price_type": price_type,
            },
        )

    async def get_quotes_batch(
        self,
        *,
        symbols: Sequence[str],
        interval: str = "1day",
        price_type: str = "mid",
    ) -> dict[str, Any]:
        return await self._request_json(
            "/quotes/batch",
            params={
                "symbols": ",".join(symbols),
                "interval": interval,
                "price_type": price_type,
            },
        )

    async def get_market_symbols(
        self,
        *,
        market: str,
        outputsize: int,
        country: str | None = None,
    ) -> dict[str, Any]:
        return await self._request_json(
            "/symbols/market",
            params={
                "market": market,
                "outputsize": outputsize,
                "country": country,
            },
        )

    async def search_symbols(
        self,
        *,
        keyword: str,
        outputsize: int,
    ) -> dict[str, Any]:
        return await self._request_json(
            "/symbols/search",
            params={
                "keyword": keyword,
                "outputsize": outputsize,
            },
        )

    async def _request_json(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = f"{settings.fxcm_api_base_url.rstrip('/')}{path}"
        request_params = {
            key: value for key, value in (params or {}).items() if value is not None
        }

        response: httpx.Response | None = None
        last_request_error: httpx.RequestError | None = None

        for attempt in range(1, self.MAX_REQUEST_ATTEMPTS + 1):
            try:
                async with httpx.AsyncClient(
                    timeout=self.DEFAULT_TIMEOUT,
                    trust_env=False,
                ) as client:
                    response = await client.get(url, params=request_params)
                break
            except httpx.RequestError as exc:
                last_request_error = exc
                logger.warning(
                    "FXCM sidecar request attempt failed",
                    extra={
                        "path": path,
                        "attempt": attempt,
                        "max_attempts": self.MAX_REQUEST_ATTEMPTS,
                        "params": dict(request_params),
                        "error_type": type(exc).__name__,
                        "error_repr": repr(exc),
                    },
                )
                if attempt >= self.MAX_REQUEST_ATTEMPTS:
                    break

        if response is None:
            exc = last_request_error
            detail = None
            if exc is not None:
                detail = f"{type(exc).__name__}: {exc!r}"
                logger.error(
                    "FXCM sidecar request failed after retries",
                    extra={
                        "path": path,
                        "max_attempts": self.MAX_REQUEST_ATTEMPTS,
                        "params": dict(request_params),
                        "error_type": type(exc).__name__,
                        "error_repr": repr(exc),
                    },
                )
            raise FXCMSidecarError(
                status_code=502,
                message="FXCM sidecar service unavailable",
                payload={
                    "detail": detail,
                    "url": url,
                    "params": request_params,
                    "attempts": self.MAX_REQUEST_ATTEMPTS,
                },
            ) from exc

        try:
            payload = response.json()
        except ValueError:
            payload = {"detail": response.text}

        if response.is_error:
            if isinstance(payload, Mapping):
                raise FXCMSidecarError(
                    status_code=response.status_code,
                    message=str(
                        payload.get("message") or "FXCM sidecar request failed"
                    ),
                    payload=payload.get("data") if "data" in payload else dict(payload),
                )

            raise FXCMSidecarError(
                status_code=response.status_code,
                message="FXCM sidecar request failed",
                payload=payload,
            )

        if not isinstance(payload, Mapping):
            raise FXCMSidecarError(
                status_code=502,
                message="FXCM sidecar returned an invalid response",
                payload=payload,
            )

        return dict(payload)


fxcm_sidecar_service = FXCMSidecarService()
