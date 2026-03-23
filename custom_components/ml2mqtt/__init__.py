from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import Ml2MqttApiClient, Ml2MqttApiError
from .const import CONF_APP_URL, DOMAIN, PLATFORMS
from .coordinator import Ml2MqttCoordinator


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    api = Ml2MqttApiClient(async_get_clientsession(hass), entry.data[CONF_APP_URL])
    coordinator = Ml2MqttCoordinator(hass, entry, api)

    try:
        await coordinator.async_initialize()
    except Ml2MqttApiError as err:
        raise ConfigEntryNotReady(str(err)) from err

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = {
        "api": api,
        "coordinator": coordinator,
    }
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        coordinator = hass.data[DOMAIN][entry.entry_id]["coordinator"]
        await coordinator.async_unload()
        hass.data[DOMAIN].pop(entry.entry_id)
    return unload_ok
