from __future__ import annotations

from homeassistant.components.select import SelectEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo, EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN, MANUFACTURER
from .coordinator import Ml2MqttCoordinator
from .helpers import safe_slug

LEARNING_MODE_LABELS = {
    "Off": "DISABLED",
    "Lazy": "LAZY",
    "Eager": "EAGER",
}
LEARNING_MODE_VALUES = {value: label for label, value in LEARNING_MODE_LABELS.items()}


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    coordinators: dict[str, Ml2MqttCoordinator] = hass.data[DOMAIN][entry.entry_id]["coordinators"]
    entities: list[SelectEntity] = []
    for coordinator in coordinators.values():
        entities.append(Ml2MqttTrainerSelect(coordinator, entry))
        entities.append(Ml2MqttLearningModeSelect(coordinator, entry))
    async_add_entities(entities)


class Ml2MqttTrainerSelect(CoordinatorEntity[Ml2MqttCoordinator], SelectEntity):
    def __init__(self, coordinator: Ml2MqttCoordinator, entry: ConfigEntry) -> None:
        super().__init__(coordinator)
        self._entry = entry
        unique_prefix = coordinator.legacy_unique_prefix or f"{entry.entry_id}_{coordinator.model_slug}"
        self._attr_unique_id = f"{unique_prefix}_trainer"
        self._attr_name = f"{coordinator.model_name} Trainer"
        trainer = coordinator.binding.get("trainer", {})
        self.entity_id = trainer.get("entity_id")

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
    def options(self) -> list[str]:
        return self.coordinator.label_options

    @property
    def current_option(self) -> str:
        return self.coordinator.active_label

    async def async_select_option(self, option: str) -> None:
        await self.coordinator.async_set_trainer(option)


class Ml2MqttLearningModeSelect(CoordinatorEntity[Ml2MqttCoordinator], SelectEntity):
    def __init__(self, coordinator: Ml2MqttCoordinator, entry: ConfigEntry) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_{coordinator.model_slug}_learning_mode"
        self._attr_name = f"{coordinator.model_name} Learning Mode"
        self.entity_id = f"select.ml2mqtt_{safe_slug(coordinator.model_name)}_learning_mode"
        self._attr_icon = "mdi:brain"
        self._attr_entity_category = EntityCategory.CONFIG

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
    def options(self) -> list[str]:
        return list(LEARNING_MODE_LABELS)

    @property
    def current_option(self) -> str:
        return LEARNING_MODE_VALUES.get(self.coordinator.learning_type, "Off")

    async def async_select_option(self, option: str) -> None:
        await self.coordinator.async_set_learning_type(LEARNING_MODE_LABELS[option])
