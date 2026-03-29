from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers import entity_registry as er

from .api import Ml2MqttApiClient, Ml2MqttApiError
from .const import CONF_APP_URL, CONF_MODELS, CONF_MODEL_ID, CONF_MODEL_SLUG, DOMAIN, PLATFORMS
from .coordinator import Ml2MqttCoordinator
from .helpers import build_entry_title, get_configured_models, normalize_app_url, safe_slug

_LOGGER = logging.getLogger(__name__)


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    return True


def _normalize_model_references(entry: ConfigEntry) -> list[dict[str, str]]:
    models = get_configured_models(entry)
    normalized_models: list[dict[str, str]] = []

    for model in models:
        normalized_model = dict(model)
        legacy_prefix = str(normalized_model.get("legacy_unique_prefix") or "")
        legacy_flag = bool(normalized_model.pop("legacy_unique_ids", False))

        if not legacy_prefix and (legacy_flag or len(models) == 1):
            normalized_model["legacy_unique_prefix"] = entry.entry_id

        normalized_models.append(normalized_model)

    return normalized_models


def _merge_model_references(entries: list[ConfigEntry]) -> list[dict[str, str]]:
    merged: list[dict[str, str]] = []
    seen_ids: set[str] = set()

    for candidate in entries:
        for model in _normalize_model_references(candidate):
            model_id = str(model.get(CONF_MODEL_ID) or "").strip()
            if not model_id or model_id in seen_ids:
                continue
            merged.append(model)
            seen_ids.add(model_id)

    return merged


def _cleanup_training_sample_entities(hass: HomeAssistant, entry: ConfigEntry, model_references: list[dict[str, str]]) -> None:
    entity_registry = er.async_get(hass)

    for model in model_references:
        model_slug = str(model.get(CONF_MODEL_SLUG) or safe_slug(str(model.get(CONF_MODEL_ID) or "model")))
        model_name = str(model.get("name") or model.get(CONF_MODEL_ID) or model_slug)
        desired_unique_id = f"{entry.entry_id}_{model_slug}_training_samples"
        desired_entity_id = f"sensor.ml2mqtt_{safe_slug(model_name)}_training_samples"

        existing_conflict = entity_registry.async_get(desired_entity_id)
        if (
            existing_conflict is not None
            and existing_conflict.platform == DOMAIN
            and existing_conflict.unique_id != desired_unique_id
        ):
            entity_registry.async_remove(desired_entity_id)

        current_entity_id = entity_registry.async_get_entity_id("sensor", DOMAIN, desired_unique_id)
        if current_entity_id and current_entity_id != desired_entity_id and entity_registry.async_get(desired_entity_id) is None:
            entity_registry.async_update_entity(current_entity_id, new_entity_id=desired_entity_id)


def _cleanup_source_mirror_entities(hass: HomeAssistant) -> None:
    entity_registry = er.async_get(hass)
    removed = 0

    for entity_entry in list(entity_registry.entities.values()):
        if entity_entry.platform != DOMAIN:
            continue
        if not entity_entry.entity_id.startswith("sensor."):
            continue
        if "_source_" not in entity_entry.entity_id and "_source_" not in entity_entry.unique_id:
            continue
        entity_registry.async_remove(entity_entry.entity_id)
        removed += 1

    if removed:
        _LOGGER.info("Removed %s legacy ML2MQTT source mirror entities", removed)


async def async_migrate_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    current_entry = hass.config_entries.async_get_entry(entry.entry_id)
    if current_entry is None:
        return False

    if entry.version > 4:
        return False

    if entry.version == 1:
        options = dict(entry.options)
        if CONF_MODELS not in options and CONF_MODEL_ID in entry.data:
            options[CONF_MODELS] = [{
                CONF_MODEL_ID: str(entry.data[CONF_MODEL_ID]),
                CONF_MODEL_SLUG: str(entry.data.get(CONF_MODEL_SLUG) or entry.data[CONF_MODEL_ID]),
                "name": entry.title,
                "legacy_unique_prefix": entry.entry_id,
            }]

        hass.config_entries.async_update_entry(
            current_entry,
            data={CONF_APP_URL: current_entry.data[CONF_APP_URL]},
            options=options,
            title=build_entry_title(current_entry.data[CONF_APP_URL]),
            version=4,
        )

    elif entry.version in (2, 3):
        options = dict(entry.options)
        options[CONF_MODELS] = _normalize_model_references(current_entry)
        hass.config_entries.async_update_entry(
            current_entry,
            data={CONF_APP_URL: current_entry.data[CONF_APP_URL]},
            options=options,
            title=build_entry_title(current_entry.data[CONF_APP_URL]),
            version=4,
        )

    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    normalized_url = normalize_app_url(entry.data[CONF_APP_URL])
    related_entries = [
        candidate
        for candidate in hass.config_entries.async_entries(DOMAIN)
        if normalize_app_url(candidate.data.get(CONF_APP_URL, "")) == normalized_url
    ]
    primary_entry = min(related_entries, key=lambda candidate: candidate.entry_id)
    merged_models = _merge_model_references(related_entries)

    if (
        primary_entry.options.get(CONF_MODELS) != merged_models
        or primary_entry.title != build_entry_title(primary_entry.data[CONF_APP_URL])
        or primary_entry.unique_id != normalized_url
    ):
        hass.config_entries.async_update_entry(
            primary_entry,
            data={CONF_APP_URL: primary_entry.data[CONF_APP_URL]},
            options={CONF_MODELS: merged_models},
            title=build_entry_title(primary_entry.data[CONF_APP_URL]),
            unique_id=normalized_url,
        )

    if entry.entry_id != primary_entry.entry_id:
        hass.async_create_task(hass.config_entries.async_remove(entry.entry_id))
        return False

    for candidate in related_entries:
        if candidate.entry_id != primary_entry.entry_id:
            hass.async_create_task(hass.config_entries.async_remove(candidate.entry_id))

    entry = primary_entry
    api = Ml2MqttApiClient(async_get_clientsession(hass), entry.data[CONF_APP_URL])
    model_references = get_configured_models(entry)
    _cleanup_training_sample_entities(hass, entry, model_references)
    _cleanup_source_mirror_entities(hass)
    coordinators: dict[str, Ml2MqttCoordinator] = {}

    try:
        if not model_references:
            await api.async_list_models()
        for model_reference in model_references:
            coordinator = Ml2MqttCoordinator(hass, entry, api, model_reference)
            try:
                await coordinator.async_initialize()
            except ConfigEntryNotReady as err:
                if "404" in str(err) or "not found" in str(err).lower():
                    _LOGGER.warning("Skipping missing ML2MQTT model '%s': %s", model_reference.get(CONF_MODEL_ID), err)
                    continue
                raise
            coordinators[coordinator.model_id] = coordinator
    except Ml2MqttApiError as err:
        raise ConfigEntryNotReady(str(err)) from err

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = {
        "api": api,
        "coordinators": coordinators,
    }
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        coordinators = hass.data[DOMAIN][entry.entry_id]["coordinators"]
        for coordinator in coordinators.values():
            await coordinator.async_unload()
        hass.data[DOMAIN].pop(entry.entry_id)
    return unload_ok
