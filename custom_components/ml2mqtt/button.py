from __future__ import annotations

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN, MANUFACTURER
from .coordinator import Ml2MqttCoordinator
from .helpers import safe_slug


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    coordinators: dict[str, Ml2MqttCoordinator] = hass.data[DOMAIN][entry.entry_id]["coordinators"]
    async_add_entities([Ml2MqttCaptureSampleButton(coordinator, entry) for coordinator in coordinators.values()])


class Ml2MqttCaptureSampleButton(CoordinatorEntity[Ml2MqttCoordinator], ButtonEntity):
    def __init__(self, coordinator: Ml2MqttCoordinator, entry: ConfigEntry) -> None:
        super().__init__(coordinator)
        self._entry = entry
        unique_prefix = coordinator.legacy_unique_prefix or f"{entry.entry_id}_{coordinator.model_slug}"
        self._attr_unique_id = f"{unique_prefix}_capture_sample"
        model_name = coordinator.model_name
        self._attr_name = f"{model_name} Capture Sample"
        self.entity_id = f"button.ml2mqtt_{safe_slug(model_name)}_capture_sample"
        self._attr_icon = "mdi:camera-plus"

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self.coordinator.device_identifier)},
            manufacturer=MANUFACTURER,
            name=self.coordinator.model_name,
            model="ML2MQTT Model Bridge",
            configuration_url=self.coordinator.edit_url,
        )

    async def async_press(self) -> None:
        await self.coordinator.async_publish_snapshot()
