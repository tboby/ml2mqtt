from __future__ import annotations

import asyncio
from typing import Any

from aiohttp import ClientError, ClientSession


class Ml2MqttApiError(Exception):
    pass


class Ml2MqttApiClient:
    def __init__(self, session: ClientSession, app_url: str) -> None:
        self._session = session
        self._app_url = app_url.rstrip("/")

    async def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        try:
            async with self._session.request(method, f"{self._app_url}{path}", json=payload, timeout=10) as response:
                data = await response.json(content_type=None)
                if response.status >= 400:
                    raise Ml2MqttApiError(data.get("error", f"API request failed with status {response.status}"))
                if not isinstance(data, dict):
                    raise Ml2MqttApiError("API returned an unexpected response")
                return data
        except (ClientError, asyncio.TimeoutError) as err:
            raise Ml2MqttApiError(str(err)) from err

    async def async_list_models(self) -> list[dict[str, Any]]:
        data = await self._request("GET", "/api/v1/models")
        models = data.get("models", [])
        return models if isinstance(models, list) else []

    async def async_get_model(self, model_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/api/v1/models/{model_id}")

    async def async_create_model(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._request("POST", "/api/v1/models", payload)

    async def async_get_binding(self, model_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/api/v1/models/{model_id}/binding")

    async def async_set_binding(self, model_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._request("PUT", f"/api/v1/models/{model_id}/binding", payload)

    async def async_clear_binding(self, model_id: str) -> dict[str, Any]:
        return await self._request("DELETE", f"/api/v1/models/{model_id}/binding")

    async def async_get_bridge_status(self, model_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/api/v1/models/{model_id}/bridge-status")
