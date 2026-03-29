from __future__ import annotations

from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo, EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN, MANUFACTURER
from .coordinator import Ml2MqttCoordinator
from .helpers import safe_slug


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback) -> None:
    coordinators: dict[str, Ml2MqttCoordinator] = hass.data[DOMAIN][entry.entry_id]["coordinators"]
    entities: list[SensorEntity] = []

    for coordinator in coordinators.values():
        entities.extend([
            Ml2MqttPredictionSensor(coordinator, entry),
            Ml2MqttConfidenceSensor(coordinator, entry),
            Ml2MqttBridgeStatusSensor(coordinator, entry),
            Ml2MqttTrainingSamplesSensor(coordinator, entry),
            Ml2MqttIngestedSensorsSensor(coordinator, entry),
        ])
    async_add_entities(entities)


class Ml2MqttBaseEntity(CoordinatorEntity[Ml2MqttCoordinator]):
    def __init__(self, coordinator: Ml2MqttCoordinator, entry: ConfigEntry, key: str, default_name: str) -> None:
        super().__init__(coordinator)
        self._entry = entry
        self._key = key
        unique_prefix = coordinator.legacy_unique_prefix or f"{entry.entry_id}_{coordinator.model_slug}"
        self._attr_unique_id = f"{unique_prefix}_{key}"
        self._attr_name = f"{coordinator.model_name} {default_name}"

    @property
    def device_info(self) -> DeviceInfo:
        return DeviceInfo(
            identifiers={(DOMAIN, self.coordinator.device_identifier)},
            manufacturer=MANUFACTURER,
            name=self.coordinator.model_name,
            model="ML2MQTT Model Bridge",
            configuration_url=self.coordinator.edit_url,
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
            "edit_url": self.coordinator.edit_url,
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
        self._attr_entity_category = EntityCategory.DIAGNOSTIC

    @property
    def native_value(self):
        return self.coordinator.runtime_status

    @property
    def extra_state_attributes(self):
        return self.coordinator.bridge_status


class Ml2MqttTrainingSamplesSensor(Ml2MqttBaseEntity, SensorEntity):
    def __init__(self, coordinator: Ml2MqttCoordinator, entry: ConfigEntry) -> None:
        super().__init__(coordinator, entry, "training_samples", "Training Samples")
        self._attr_unique_id = f"{entry.entry_id}_{coordinator.model_slug}_training_samples"
        self.entity_id = f"sensor.ml2mqtt_{safe_slug(coordinator.model_name)}_training_samples"
        self._attr_entity_category = EntityCategory.DIAGNOSTIC
        self._attr_icon = "mdi:database"

    @property
    def native_value(self):
        return self.coordinator.observation_count

    @property
    def extra_state_attributes(self):
        return {
            "learning_type": self.coordinator.learning_type,
            "model_type": self.coordinator.model_type,
            "label_counts": self.coordinator.label_counts,
            "raw_observation_count": self.coordinator.raw_observation_count,
            "edit_url": self.coordinator.edit_url,
        }


class Ml2MqttIngestedSensorsSensor(Ml2MqttBaseEntity, SensorEntity):
    def __init__(self, coordinator: Ml2MqttCoordinator, entry: ConfigEntry) -> None:
        super().__init__(coordinator, entry, "ingested_sensors", "Ingested Sensors")
        self.entity_id = f"sensor.ml2mqtt_{safe_slug(coordinator.data.get('name', entry.title))}_ingested_sensors"
        self._attr_entity_category = EntityCategory.DIAGNOSTIC
        self._attr_icon = "mdi:database-arrow-right"

    @property
    def native_value(self):
        return self.coordinator.source_summary

    @property
    def extra_state_attributes(self):
        details = self.coordinator.source_details
        return {
            "source_count": len(details),
            "source_entities": [detail["entity_id"] for detail in details],
            "source_values": {detail["entity_id"]: detail.get("state") for detail in details},
            "sources": details,
        }
