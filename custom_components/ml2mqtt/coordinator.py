from __future__ import annotations

import json
import logging
from copy import deepcopy
from datetime import timedelta
from typing import Any

from homeassistant.components import mqtt
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import Event, HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.event import async_track_state_change_event
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import Ml2MqttApiClient, Ml2MqttApiError
from .const import CONF_APP_URL, CONF_MODEL_ID, CONF_MODEL_SLUG, DISABLED_LABEL, DOMAIN
from .helpers import build_device_identifier, build_model_edit_url, build_snapshot_payload

_LOGGER = logging.getLogger(__name__)


class Ml2MqttCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    def __init__(self, hass: HomeAssistant, entry: ConfigEntry, api: Ml2MqttApiClient, model_reference: dict[str, Any]) -> None:
        super().__init__(hass, _LOGGER, name=f"{DOMAIN}_{model_reference[CONF_MODEL_ID]}", update_interval=timedelta(seconds=15))
        self.entry = entry
        self.api = api
        self.app_url = entry.data[CONF_APP_URL]
        self.model_id = str(model_reference[CONF_MODEL_ID])
        self.model_slug = str(model_reference.get(CONF_MODEL_SLUG) or self.model_id)
        self.model_name = str(model_reference.get("name") or self.model_id)
        self.legacy_unique_prefix = str(model_reference.get("legacy_unique_prefix") or "")
        self.active_label = DISABLED_LABEL
        self.current_prediction: str | None = None
        self.current_confidence: float | None = None
        self.runtime_status = "initializing"
        self.source_states: dict[str, Any] = {}
        self._unsubscribe_prediction = None
        self._unsubscribe_state_listener = None

    async def _async_update_data(self) -> dict[str, Any]:
        try:
            model_data = await self.api.async_get_model(self.model_id)
        except Ml2MqttApiError as err:
            raise UpdateFailed(str(err)) from err
        await self._async_apply_model_state(model_data, push_update=False)
        return model_data

    async def async_initialize(self) -> None:
        try:
            await self.async_config_entry_first_refresh()
        except UpdateFailed as err:
            raise ConfigEntryNotReady(str(err)) from err

        await self._async_apply_model_state(self.data)

    async def _async_apply_model_state(self, model_data: dict[str, Any], push_update: bool = True) -> None:
        self.model_name = str(model_data.get("name") or self.model_name)
        self.model_slug = str(model_data.get("slug") or self.model_slug)
        binding = model_data.get("binding") or {}
        sources = binding.get("sources", []) if isinstance(binding, dict) else []
        bridge_status = model_data.get("bridge_status") or {}

        self.source_states = {
            source["entity_id"]: self._read_state(source["entity_id"])
            for source in sources
            if source.get("entity_id")
        }
        self.current_prediction = bridge_status.get("last_prediction")
        self.current_confidence = bridge_status.get("last_confidence")
        if self.active_label not in self.label_options:
            self.active_label = DISABLED_LABEL
        
        await self._async_setup_runtime_subscriptions()
        self.runtime_status = "warning" if self.compatibility_status.get("state") == "warning" else "ready"
        if push_update:
            self.async_set_updated_data(model_data)

    @property
    def device_identifier(self) -> str:
        return build_device_identifier(self.entry.entry_id, self.model_slug)

    @property
    def edit_url(self) -> str:
        return build_model_edit_url(self.app_url, self.model_slug)

    @property
    def observation_count(self) -> int:
        if not self.data:
            return 0
        return int(self.data.get("observation_count", 0))

    @property
    def learning_type(self) -> str:
        if not self.data:
            return "UNKNOWN"
        return str(self.data.get("learning_type", "UNKNOWN"))

    @property
    def model_type(self) -> str:
        if not self.data:
            return "UNKNOWN"
        return str(self.data.get("model_type", "UNKNOWN"))

    @property
    def label_counts(self) -> dict[str, int]:
        if not self.data:
            return {}
        raw_counts = self.data.get("label_counts", {})
        if not isinstance(raw_counts, dict):
            return {}
        return {str(label): int(count) for label, count in raw_counts.items()}

    @property
    def label_options(self) -> list[str]:
        labels = self.data.get("labels", []) if self.data else []
        normalized = [label for label in labels if label and label != DISABLED_LABEL]
        return [DISABLED_LABEL, *normalized]

    @property
    def compatibility_status(self) -> dict[str, Any]:
        if not self.data:
            return {"state": "unbound", "warnings": []}
        return self.data.get("compatibility_status", {"state": "unbound", "warnings": []})

    @property
    def binding(self) -> dict[str, Any]:
        if not self.data:
            return {}
        return self.data.get("binding") or {}

    @property
    def command_topic(self) -> str:
        mqtt_topic = self.data.get("mqtt_topic", "") if self.data else ""
        return f"{mqtt_topic}/set" if mqtt_topic else ""

    @property
    def state_topic(self) -> str:
        mqtt_topic = self.data.get("mqtt_topic", "") if self.data else ""
        return f"{mqtt_topic}/state" if mqtt_topic else ""

    @property
    def source_entities(self) -> list[str]:
        return [source["entity_id"] for source in self.binding.get("sources", []) if source.get("entity_id")]

    @property
    def source_details(self) -> list[dict[str, Any]]:
        details: list[dict[str, Any]] = []
        for source in self.binding.get("sources", []):
            entity_id = source.get("entity_id")
            if not entity_id:
                continue

            state = self.hass.states.get(entity_id)
            name = source.get("name") or entity_id
            if state is not None:
                name = state.attributes.get("friendly_name") or name

            current_state = state.state if state is not None else self.source_states.get(entity_id)
            attributes = state.attributes if state is not None else {}

            details.append({
                "entity_id": entity_id,
                "name": name,
                "state": current_state,
                "unit_of_measurement": attributes.get("unit_of_measurement"),
                "icon": attributes.get("icon"),
                "device_class": attributes.get("device_class"),
                "original_domain": entity_id.split(".", 1)[0],
            })
        return details

    def get_source_detail(self, entity_id: str) -> dict[str, Any] | None:
        for detail in self.source_details:
            if detail.get("entity_id") == entity_id:
                return detail
        return None

    @property
    def source_summary(self) -> str:
        details = self.source_details
        if not details:
            return "No sensors bound"

        summary = ", ".join(detail["name"] for detail in details)
        if len(summary) <= 255:
            return summary
        return f"{len(details)} sensors bound"

    @property
    def bridge_status(self) -> dict[str, Any]:
        status = deepcopy(self.data.get("bridge_status", {}) if self.data else {})
        status["runtime_status"] = self.runtime_status
        status["active_label"] = self.active_label
        status["source_entities"] = self.source_entities
        status["sources"] = self.source_details
        status["learning_type"] = self.learning_type
        status["model_type"] = self.model_type
        status["observation_count"] = self.observation_count
        status["label_counts"] = self.label_counts
        status["edit_url"] = self.edit_url
        return status

    @callback
    def _read_state(self, entity_id: str) -> Any:
        state = self.hass.states.get(entity_id)
        return state.state if state is not None else None

    async def _async_setup_runtime_subscriptions(self) -> None:
        if self._unsubscribe_prediction is not None:
            self._unsubscribe_prediction()
            self._unsubscribe_prediction = None

        if self._unsubscribe_state_listener is not None:
            self._unsubscribe_state_listener()
            self._unsubscribe_state_listener = None

        if self.state_topic:
            self._unsubscribe_prediction = await mqtt.async_subscribe(
                self.hass,
                self.state_topic,
                self._async_handle_prediction,
                qos=0,
            )

        if self.source_entities:
            self._unsubscribe_state_listener = async_track_state_change_event(
                self.hass,
                self.source_entities,
                self._async_handle_source_change,
            )

    @callback
    def _async_handle_prediction(self, message) -> None:
        try:
            payload = json.loads(message.payload)
        except (TypeError, json.JSONDecodeError):
            _LOGGER.warning("Invalid ML2MQTT prediction payload: %s", message.payload)
            self.runtime_status = "prediction_error"
            self.async_update_listeners()
            return

        self.current_prediction = payload.get("state")
        self.current_confidence = payload.get("confidence")
        if self.compatibility_status.get("state") == "warning":
            self.runtime_status = "warning"
        else:
            self.runtime_status = "ready"

        updated = deepcopy(self.data or {})
        bridge_status = updated.setdefault("bridge_status", {})
        bridge_status["last_prediction"] = self.current_prediction
        bridge_status["last_confidence"] = self.current_confidence
        updated["bridge_status"] = bridge_status
        self.async_set_updated_data(updated)
        self.hass.async_create_task(self.async_request_refresh())

    @callback
    def _async_handle_source_change(self, event: Event) -> None:
        new_state = event.data.get("new_state")
        entity_id = event.data.get("entity_id")
        if entity_id is None:
            return
        self.source_states[entity_id] = new_state.state if new_state is not None else None
        self.hass.async_create_task(self.async_publish_snapshot())

    async def async_publish_snapshot(self) -> None:
        if not self.command_topic:
            self.runtime_status = "mqtt_unavailable"
            self.async_update_listeners()
            return

        payload = build_snapshot_payload(self.binding.get("sources", []), self.source_states, self.active_label)
        await mqtt.async_publish(self.hass, self.command_topic, json.dumps(payload), qos=0, retain=False)
        self.runtime_status = "warning" if self.compatibility_status.get("state") == "warning" else "ready"
        self.async_update_listeners()

    async def async_set_trainer(self, label: str) -> None:
        self.active_label = label if label in self.label_options else DISABLED_LABEL
        await self.async_publish_snapshot()

    async def async_set_learning_type(self, learning_type: str) -> None:
        model_data = await self.api.async_set_learning_type(self.model_id, learning_type)
        await self._async_apply_model_state(model_data)

    async def async_unload(self) -> None:
        if self._unsubscribe_prediction is not None:
            self._unsubscribe_prediction()
            self._unsubscribe_prediction = None
        if self._unsubscribe_state_listener is not None:
            self._unsubscribe_state_listener()
            self._unsubscribe_state_listener = None
