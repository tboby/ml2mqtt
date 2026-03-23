from __future__ import annotations

from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN, MANUFACTURER
from .coordinator import Ml2MqttCoordinator


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    coordinator: Ml2MqttCoordinator = hass.data[DOMAIN][entry.entry_id]["coordinator"]
    async_add_entities([
        Ml2MqttPredictionSensor(coordinator, entry),
        Ml2MqttConfidenceSensor(coordinator, entry),
        Ml2MqttBridgeStatusSensor(coordinator, entry),
    ])


class Ml2MqttBaseEntity(CoordinatorEntity[Ml2MqttCoordinator]):
    def __init__(self, coordinator: Ml2MqttCoordinator, entry: ConfigEntry, key: str, default_name: str) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._key = key
        self._attr_unique_id = f"{entry.entry_id}_{key}"
        self._attr_name = f"{coordinator.data.get('name', entry.title)} {default_name}"

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self._entry.entry_id)},
            manufacturer=MANUFACTURER,
            name=self.coordinator.data.get("name", self._entry.title),
            model="ML2MQTT Model Bridge",
        )


class Ml2MqttPredictionSensor(Ml2MqttBaseEntity, SensorEntity):
    def __init__(self, coordinator: Ml2MqttCoordinator, entry: ConfigEntry) -> None:
        super().__init__(coordinator, entry, "prediction", "Prediction")
        prediction = coordinator.binding.get("outputs", {}).get("prediction", {})
        self.entity_id = prediction.get("entity_id")

    @property
    def native_value(self):
        return self.coordinator.current_prediction

    @property
    def extra_state_attributes(self):
        return {
            "model_id": self.coordinator.model_id,
            "compatibility_status": self.coordinator.compatibility_status,
        }


class Ml2MqttConfidenceSensor(Ml2MqttBaseEntity, SensorEntity):
    def __init__(self, coordinator: Ml2MqttCoordinator, entry: ConfigEntry) -> None:
        super().__init__(coordinator, entry, "confidence", "Confidence")
        confidence = coordinator.binding.get("outputs", {}).get("confidence", {})
        self.entity_id = confidence.get("entity_id")

    @property
    def native_value(self):
        return self.coordinator.current_confidence


class Ml2MqttBridgeStatusSensor(Ml2MqttBaseEntity, SensorEntity):
    def __init__(self, coordinator: Ml2MqttCoordinator, entry: ConfigEntry) -> None:
        super().__init__(coordinator, entry, "bridge_status", "Bridge Status")
        status = coordinator.binding.get("outputs", {}).get("status", {})
        self.entity_id = status.get("entity_id")

    @property
    def native_value(self):
        return self.coordinator.runtime_status

    @property
    def extra_state_attributes(self):
        return self.coordinator.bridge_status
