from typing import Any, Mapping

import httpx

from app.clients.ff14 import FFLogsAPIError, get_ff14_client
from app.core.config import settings


class FF14Service:
    async def get(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        client = get_ff14_client()
        query_params = self._build_query_params(params)

        try:
            response = await client.get(path, params=query_params)
        except httpx.RequestError as exc:
            raise FFLogsAPIError(
                status_code=502,
                message="FF Logs service unavailable",
                payload={"detail": str(exc)},
            ) from exc

        if response.is_error:
            raise FFLogsAPIError(
                status_code=response.status_code,
                message=self._extract_error_message(response),
                payload=self._extract_response_payload(response),
            )

        if not response.content:
            return None

        return response.json()

    def _build_query_params(self, params: Mapping[str, Any] | None) -> dict[str, Any]:
        query_params = {
            key: value
            for key, value in (params or {}).items()
            if value is not None and value != "" and key != "api_key"
        }
        query_params["api_key"] = settings.ff14_api_key
        return query_params

    def _extract_error_message(self, response: httpx.Response) -> str:
        payload = self._extract_response_payload(response)
        if isinstance(payload, dict):
            message = payload.get("error") or payload.get("message")
            if isinstance(message, str) and message:
                return message
        if response.text:
            return response.text
        return "FF Logs request failed"

    def _extract_response_payload(self, response: httpx.Response) -> Any:
        try:
            return response.json()
        except ValueError:
            if response.text:
                return {"detail": response.text}
            return None


ff14_service = FF14Service()
