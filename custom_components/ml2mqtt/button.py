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
    coordinator: Ml2MqttCoordinator = hass.data[DOMAIN][entry.entry_id]["coordinator"]
    async_add_entities([Ml2MqttCaptureSampleButton(coordinator, entry)])


class Ml2MqttCaptureSampleButton(CoordinatorEntity[Ml2MqttCoordinator], ButtonEntity):
    def __init__(self, coordinator: Ml2MqttCoordinator, entry: ConfigEntry) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_capture_sample"
        model_name = coordinator.data.get("name", entry.title)
        self._attr_name = f"{model_name} Capture Sample"
        self.entity_id = f"button.ml2mqtt_{safe_slug(model_name)}_capture_sample"
        self._attr_icon = "mdi:camera-plus"

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._entry.entry_id)},
            manufacturer=MANUFACTURER,
            name=self.coordinator.data.get("name", self._entry.title),
            model="ML2MQTT Model Bridge",
        )

    async def async_press(self) -> None:
        await self.coordinator.async_publish_snapshot()
