from __future__ import annotations

from homeassistant.components.select import SelectEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN, MANUFACTURER
from .coordinator import Ml2MqttCoordinator


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    coordinator: Ml2MqttCoordinator = hass.data[DOMAIN][entry.entry_id]["coordinator"]
    async_add_entities([Ml2MqttTrainerSelect(coordinator, entry)])


class Ml2MqttTrainerSelect(CoordinatorEntity[Ml2MqttCoordinator], SelectEntity):
    def __init__(self, coordinator: Ml2MqttCoordinator, entry: ConfigEntry) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_trainer"
        self._attr_name = f"{coordinator.data.get('name', entry.title)} Trainer"
        trainer = coordinator.binding.get("trainer", {})
        self.entity_id = trainer.get("entity_id")

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._entry.entry_id)},
            manufacturer=MANUFACTURER,
            name=self.coordinator.data.get("name", self._entry.title),
            model="ML2MQTT Model Bridge",
        )

    @property
    def options(self) -> list[str]:
        return self.coordinator.label_options

    @property
    def current_option(self) -> str:
        return self.coordinator.active_label

    async def async_select_option(self, option: str) -> None:
        await self.coordinator.async_set_trainer(option)
