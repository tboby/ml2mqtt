from __future__ import annotations

import time

from homeassistant.components.binary_sensor import BinarySensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN, MANUFACTURER
from .coordinator import Ml2MqttCoordinator
from .helpers import safe_slug

CLASS_PRESENCE_WINDOW_SECONDS = 5


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    coordinators: dict[str, Ml2MqttCoordinator] = hass.data[DOMAIN][entry.entry_id]["coordinators"]
    entities: list[BinarySensorEntity] = []

    for coordinator in coordinators.values():
        labels = coordinator.data.get("labels", []) if coordinator.data else []
        for label in labels:
            entities.append(Ml2MqttClassPresenceSensor(coordinator, entry, label))
    async_add_entities(entities)


class Ml2MqttClassPresenceSensor(CoordinatorEntity[Ml2MqttCoordinator], BinarySensorEntity):
    def __init__(self, coordinator: Ml2MqttCoordinator, entry: ConfigEntry, label: str) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._label = label
        unique_prefix = coordinator.legacy_unique_prefix or f"{entry.entry_id}_{coordinator.model_slug}"
        self._attr_unique_id = f"{unique_prefix}_class_{safe_slug(label)}"
        self._attr_name = f"{coordinator.model_name} {label}"
        self._attr_device_class = "presence"

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self.coordinator.device_identifier)},
            manufacturer=MANUFACTURER,
            name=self.coordinator.model_name,
            model="ML2MQTT Model Bridge",
            configuration_url=self.coordinator.edit_url,
        )

    @property
    def is_on(self) -> bool | None:
        last_seen = self.coordinator.class_last_seen.get(self._label)
        if last_seen is None:
            return False
        return (time.time() - last_seen) <= CLASS_PRESENCE_WINDOW_SECONDS

    @property
    def extra_state_attributes(self):
        last_seen = self.coordinator.class_last_seen.get(self._label)
        return {
            "label": self._label,
            "model_id": self.coordinator.model_id,
            "last_seen": last_seen,
            "seconds_ago": time.time() - last_seen if last_seen is not None else None,
        }
