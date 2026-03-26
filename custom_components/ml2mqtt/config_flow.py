from __future__ import annotations

from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResult, section
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.selector import selector

from .api import Ml2MqttApiClient, Ml2MqttApiError
from .const import CONF_APP_URL, CONF_MODEL_ID, CONF_MODEL_SLUG, DEFAULT_APP_URL, DOMAIN
from .helpers import build_helper_entity_metadata, safe_slug


class Ml2MqttConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1

    def __init__(self) -> None:
        self._app_url = DEFAULT_APP_URL
        self._api: Ml2MqttApiClient | None = None
        self._models: list[dict[str, Any]] = []
        self._create_new_defaults: dict[str, Any] = {
            "model_name": "",
            "labels": "",
            "source_entities": [],
            "advanced": {},
        }

    def _default_mqtt_topic(self, model_name: str) -> str:
        return f"ml2mqtt/{safe_slug(model_name)}"

    def _parse_label_input(self, raw_labels: str) -> list[str]:
        labels: list[str] = []
        seen: set[str] = set()

        for line in str(raw_labels).replace("\r", "\n").split("\n"):
            for part in line.split(","):
                label = part.strip()
                if not label or label in seen:
                    continue
                labels.append(label)
                seen.add(label)

        return labels

    def _build_create_new_schema(self) -> vol.Schema:
        schema: dict[Any, Any] = {
            vol.Required("model_name", default=self._create_new_defaults["model_name"]): str,
            vol.Required("labels", default=self._create_new_defaults["labels"]): selector({"text": {"multiline": True}}),
            vol.Required("source_entities", default=self._create_new_defaults["source_entities"]): selector({"entity": {"multiple": True}}),
        }

        if self.show_advanced_options:
            advanced = self._create_new_defaults.get("advanced", {})
            suggested_topic = str(advanced.get("mqtt_topic") or self._default_mqtt_topic(self._create_new_defaults["model_name"]))
            schema[vol.Optional("advanced")] = section(
                vol.Schema({
                    vol.Optional("mqtt_topic", description={"suggested_value": suggested_topic}): str,
                    vol.Optional("default_value", default=float(advanced.get("default_value", 9999))): vol.Coerce(float),
                }),
                {"collapsed": True},
            )

        return vol.Schema(schema)

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        errors = {}

        if user_input is not None:
            self._app_url = user_input[CONF_APP_URL].rstrip("/")
            self._api = Ml2MqttApiClient(async_get_clientsession(self.hass), self._app_url)
            try:
                self._models = await self._api.async_list_models()
                return await self.async_step_mode()
            except Ml2MqttApiError:
                errors["base"] = "cannot_connect"

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({
                vol.Required(CONF_APP_URL, default=self._app_url): str,
            }),
            errors=errors,
        )

    async def async_step_mode(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        if user_input is not None:
            if user_input["mode"] == "bind_existing":
                return await self.async_step_bind_existing()
            return await self.async_step_create_new()

        can_bind_existing = len(self._models) > 0
        options = {"create_new": "Create a new model"}
        if can_bind_existing:
            options = {"bind_existing": "Bind an existing model", **options}

        return self.async_show_form(
            step_id="mode",
            data_schema=vol.Schema({
                vol.Required("mode", default="bind_existing" if can_bind_existing else "create_new"): vol.In(options),
            }),
        )

    async def async_step_bind_existing(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        if self._api is None:
            return await self.async_step_user()

        errors = {}
        if user_input is not None:
            selected_model = next(model for model in self._models if model["id"] == user_input[CONF_MODEL_ID])
            await self.async_set_unique_id(selected_model["slug"])
            self._abort_if_unique_id_configured()

            try:
                await self._api.async_set_binding(
                    selected_model["id"],
                    {
                        "source_entities": user_input["source_entities"],
                        "binding": build_helper_entity_metadata(selected_model["name"]),
                    },
                )
                return self.async_create_entry(
                    title=selected_model["name"],
                    data={
                        CONF_APP_URL: self._app_url,
                        CONF_MODEL_ID: selected_model["id"],
                        CONF_MODEL_SLUG: selected_model["slug"],
                    },
                )
            except Ml2MqttApiError:
                errors["base"] = "cannot_connect"

        model_options = {model["id"]: model["name"] for model in self._models}
        return self.async_show_form(
            step_id="bind_existing",
            data_schema=vol.Schema({
                vol.Required(CONF_MODEL_ID): vol.In(model_options),
                vol.Required("source_entities"): selector({"entity": {"multiple": True}}),
            }),
            errors=errors,
        )

    async def async_step_create_new(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        if self._api is None:
            return await self.async_step_user()

        errors = {}
        if user_input is not None:
            advanced = user_input.get("advanced") if isinstance(user_input.get("advanced"), dict) else {}
            self._create_new_defaults = {
                "model_name": str(user_input["model_name"]).strip(),
                "labels": str(user_input["labels"]),
                "source_entities": user_input["source_entities"],
                "advanced": {
                    "mqtt_topic": str(advanced.get("mqtt_topic", "")).strip(),
                    "default_value": advanced.get("default_value", 9999),
                },
            }

            labels = self._parse_label_input(self._create_new_defaults["labels"])
            if len(labels) < 2:
                errors["labels"] = "invalid_labels"

        if user_input is not None and not errors:
            try:
                model = await self._api.async_create_model(
                    {
                        "model_name": self._create_new_defaults["model_name"],
                        "labels": labels,
                        "mqtt_topic": self._create_new_defaults["advanced"].get("mqtt_topic") or None,
                        "default_value": self._create_new_defaults["advanced"].get("default_value", 9999),
                        "source_entities": self._create_new_defaults["source_entities"],
                        "binding": build_helper_entity_metadata(self._create_new_defaults["model_name"]),
                    }
                )
                await self.async_set_unique_id(model["slug"])
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title=model["name"],
                    data={
                        CONF_APP_URL: self._app_url,
                        CONF_MODEL_ID: model["id"],
                        CONF_MODEL_SLUG: model["slug"],
                    },
                )
            except Ml2MqttApiError:
                errors["base"] = "cannot_connect"

        return self.async_show_form(
            step_id="create_new",
            data_schema=self._build_create_new_schema(),
            errors=errors,
        )
