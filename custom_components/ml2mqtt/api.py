from __future__ import annotations

import asyncio
import json
from typing import Any

from aiohttp import ClientError, ClientSession


class Ml2MqttApiError(Exception):
    pass


class Ml2MqttApiClient:
    def __init__(self, session: ClientSession, app_url: str) -> None:
        self._session = session
        self._app_url = app_url.rstrip("/")

    @staticmethod
    def _normalize_model_payload(data: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(data)
        if "model_id" not in normalized and "id" in normalized:
            normalized["model_id"] = str(normalized["id"])
        return normalized

    async def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        try:
            async with self._session.request(method, f"{self._app_url}{path}", json=payload, timeout=10) as response:
                raw_body = await response.text()
                try:
                    data = json.loads(raw_body) if raw_body else {}
                except json.JSONDecodeError as err:
                    if response.status >= 400:
                        raise Ml2MqttApiError(
                            f"API request failed with status {response.status}: {raw_body[:200].strip() or 'non-JSON response'}"
                        ) from err
                    raise Ml2MqttApiError("API returned a non-JSON response") from err

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
        if not isinstance(models, list):
            return []
        return [self._normalize_model_payload(model) for model in models if isinstance(model, dict)]

    async def async_get_model(self, model_id: str) -> dict[str, Any]:
        return self._normalize_model_payload(await self._request("GET", f"/api/v1/models/{model_id}"))

    async def async_create_model(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._normalize_model_payload(await self._request("POST", "/api/v1/models", payload))

    async def async_get_binding(self, model_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/api/v1/models/{model_id}/binding")

    async def async_set_binding(self, model_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._request("PUT", f"/api/v1/models/{model_id}/binding", payload)

    async def async_clear_binding(self, model_id: str) -> dict[str, Any]:
        return await self._request("DELETE", f"/api/v1/models/{model_id}/binding")

    async def async_get_bridge_status(self, model_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/api/v1/models/{model_id}/bridge-status")

    async def async_set_learning_type(self, model_id: str, learning_type: str) -> dict[str, Any]:
        return self._normalize_model_payload(
            await self._request(
                "PUT",
                f"/api/v1/models/{model_id}/learning-type",
                {"learning_type": learning_type},
            )
        )
