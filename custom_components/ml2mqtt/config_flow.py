from __future__ import annotations

from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResult, section
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.selector import selector

from .api import Ml2MqttApiClient, Ml2MqttApiError
from .const import CONF_APP_URL, CONF_MODEL_ID, CONF_MODELS, DEFAULT_APP_URL, DOMAIN
from .helpers import (
    build_entry_title,
    build_entity_aliases,
    build_helper_entity_metadata,
    get_configured_models,
    get_binding_feature_names,
    normalize_model_id,
    normalize_app_url,
    remap_observations_to_binding,
    safe_slug,
    serialize_model_reference,
)


class Ml2MqttFlowBase:
    def __init__(self) -> None:
        self._app_url = DEFAULT_APP_URL
        self._api: Ml2MqttApiClient | None = None
        self._models: list[dict[str, Any]] = []
        self._clone_defaults: dict[str, Any] = {
            "source_model_id": "",
            "model_name": "",
            "source_inputs": [],
            "target_entities": {},
        }
        self._create_new_defaults: dict[str, Any] = {
            "model_name": "",
            "labels": "",
            "source_entities": [],
            "advanced": {},
        }

    async def _async_connect(self, app_url: str) -> None:
        self._app_url = normalize_app_url(app_url)
        self._api = Ml2MqttApiClient(async_get_clientsession(self.hass), self._app_url)
        self._models = await self._api.async_list_models()

    def _default_mqtt_topic(self, model_name: str) -> str:
        return f"ml2mqtt/{safe_slug(model_name)}"

    def _parse_label_input(self, raw_labels: str) -> list[str]:
        labels: list[str] = []
        seen: set[str] = set()

        for line in str(raw_labels).replace("\r", "\n").split("\n"):
            for part in line.split(","):
                label = part.strip()
                if not label or label in seen:
                    continue
                labels.append(label)
                seen.add(label)

        return labels

    def _build_create_new_schema(self) -> vol.Schema:
        schema: dict[Any, Any] = {
            vol.Required("model_name", default=self._create_new_defaults["model_name"]): str,
            vol.Required("labels", default=self._create_new_defaults["labels"]): selector({"text": {"multiline": True}}),
            vol.Required("source_entities", default=self._create_new_defaults["source_entities"]): selector({"entity": {"multiple": True}}),
        }

        if self.show_advanced_options:
            advanced = self._create_new_defaults.get("advanced", {})
            suggested_topic = str(advanced.get("mqtt_topic") or self._default_mqtt_topic(self._create_new_defaults["model_name"]))
            schema[vol.Optional("advanced")] = section(
                vol.Schema({
                    vol.Optional("mqtt_topic", description={"suggested_value": suggested_topic}): str,
                    vol.Optional("default_value", default=float(advanced.get("default_value", 9999))): vol.Coerce(float),
                }),
                {"collapsed": True},
            )

        return vol.Schema(schema)

    def _build_model_schema(self, models: list[dict[str, Any]], field_name: str = CONF_MODEL_ID) -> vol.Schema:
        model_options = {
            self._model_identifier(model): str(model.get("name") or self._model_identifier(model))
            for model in models
            if self._model_identifier(model)
        }
        return vol.Schema({
            vol.Required(field_name): vol.In(model_options),
            vol.Required("source_entities"): selector({"entity": {"multiple": True}}),
        })

    def _build_remove_schema(self, models: list[dict[str, Any]]) -> vol.Schema:
        model_options = {model[CONF_MODEL_ID]: model["name"] for model in models}
        return vol.Schema({
            vol.Required(CONF_MODEL_ID): vol.In(model_options),
        })

    def _build_clone_schema(self, models: list[dict[str, Any]]) -> vol.Schema:
        model_options = {
            self._model_identifier(model): str(model.get("name") or self._model_identifier(model))
            for model in models
            if self._model_identifier(model)
        }
        return vol.Schema({
            vol.Required("source_model_id", default=self._clone_defaults.get("source_model_id", "")): vol.In(model_options),
            vol.Required("model_name", default=self._clone_defaults.get("model_name", "")): str,
        })

    def _build_clone_mapping_schema(self) -> vol.Schema:
        schema: dict[Any, Any] = {}
        for source_entry in self._clone_defaults.get("source_inputs", []):
            entity_id = str(source_entry.get("entity_id") or "").strip()
            if not entity_id:
                continue
            schema[vol.Required(entity_id, default=self._clone_defaults.get("target_entities", {}).get(entity_id))] = selector({"entity": {}})
        return vol.Schema(schema)

    def _build_import_schema(self, source_models: list[dict[str, Any]], target_models: list[dict[str, Any]]) -> vol.Schema:
        source_options = {
            self._model_identifier(model): str(model.get("name") or self._model_identifier(model))
            for model in source_models
            if self._model_identifier(model)
        }
        target_options = {
            self._model_identifier(model): str(model.get("name") or self._model_identifier(model))
            for model in target_models
            if self._model_identifier(model)
        }
        return vol.Schema({
            vol.Required("source_model_id"): vol.In(source_options),
            vol.Required("target_model_id"): vol.In(target_options),
            vol.Required("replace_existing", default=False): bool,
        })

    def _build_app_url_schema(self) -> vol.Schema:
        return vol.Schema({
            vol.Required(CONF_APP_URL, default=self._app_url): str,
        })

    def _get_model_by_id(self, model_id: str) -> dict[str, Any] | None:
        for model in self._models:
            if self._model_identifier(model) == model_id:
                return model
        return None

    def _model_identifier(self, model: dict[str, Any]) -> str:
        return normalize_model_id(model.get(CONF_MODEL_ID) or model.get("id"))

    async def _async_import_model_observations(
        self,
        source_model_id: str,
        target_model_id: str,
        *,
        replace_existing: bool,
    ) -> None:
        if self._api is None:
            raise Ml2MqttApiError("API client is not initialized")

        source_model = await self._api.async_get_model(source_model_id)
        target_model = await self._api.async_get_model(target_model_id)
        source_observations = await self._api.async_list_raw_observations(source_model_id)
        remapped_observations = remap_observations_to_binding(
            source_observations,
            source_model.get("binding"),
            target_model.get("binding"),
        )
        previous_learning_type = str(target_model.get("learning_type") or "EAGER")
        if previous_learning_type == "DISABLED":
            await self._api.async_set_learning_type(target_model_id, "EAGER")

        try:
            await self._api.async_import_raw_observations(
                target_model_id,
                {
                    "observations": remapped_observations,
                    "replace_existing": replace_existing,
                    "rebuild_after_import": True,
                },
            )
        finally:
            if previous_learning_type == "DISABLED":
                await self._api.async_set_learning_type(target_model_id, previous_learning_type)


class Ml2MqttConfigFlow(Ml2MqttFlowBase, config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 4

    def __init__(self) -> None:
        Ml2MqttFlowBase.__init__(self)

    @staticmethod
    def async_get_options_flow(config_entry: config_entries.ConfigEntry) -> config_entries.OptionsFlow:
        return Ml2MqttOptionsFlow(config_entry)

    def _get_existing_entry_for_url(self, app_url: str) -> config_entries.ConfigEntry | None:
        normalized = normalize_app_url(app_url)
        for entry in self._async_current_entries():
            if normalize_app_url(entry.data.get(CONF_APP_URL, "")) == normalized:
                return entry
        return None

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        errors = {}

        if user_input is not None:
            requested_url = user_input[CONF_APP_URL]
            if self._get_existing_entry_for_url(requested_url) is not None:
                return self.async_abort(reason="already_configured_service")

            try:
                await self._async_connect(requested_url)
                return await self.async_step_mode()
            except Ml2MqttApiError:
                errors["base"] = "cannot_connect"

        return self.async_show_form(
            step_id="user",
            data_schema=self._build_app_url_schema(),
            errors=errors,
        )

    async def async_step_mode(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        if user_input is not None:
            if user_input["mode"] == "bind_existing":
                return await self.async_step_bind_existing()
            return await self.async_step_create_new()

        can_bind_existing = len(self._models) > 0
        options = {"create_new": "Create a new model"}
        if can_bind_existing:
            options = {"bind_existing": "Bind an existing model", **options}

        return self.async_show_form(
            step_id="mode",
            data_schema=vol.Schema({
                vol.Required("mode", default="bind_existing" if can_bind_existing else "create_new"): vol.In(options),
            }),
        )

    async def async_step_bind_existing(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        if self._api is None:
            return await self.async_step_user()

        if not self._models:
            return self.async_abort(reason="no_available_models")

        errors = {}
        if user_input is not None:
            selected_model = self._get_model_by_id(user_input[CONF_MODEL_ID])
            selected_model_id = self._model_identifier(selected_model) if selected_model is not None else ""
            if selected_model is None or not selected_model_id:
                errors["base"] = "model_not_found"
            else:
                try:
                    await self._api.async_set_binding(
                        selected_model_id,
                        {
                            "source_entities": user_input["source_entities"],
                            "entity_aliases": build_entity_aliases(user_input["source_entities"]),
                            "binding": build_helper_entity_metadata(selected_model["name"]),
                        },
                    )
                    return self.async_create_entry(
                        title=build_entry_title(self._app_url),
                        data={CONF_APP_URL: self._app_url},
                        options={CONF_MODELS: [serialize_model_reference(selected_model)]},
                    )
                except Ml2MqttApiError:
                    errors["base"] = "cannot_connect"

        return self.async_show_form(
            step_id="bind_existing",
            data_schema=self._build_model_schema(self._models),
            errors=errors,
        )

    async def async_step_create_new(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        if self._api is None:
            return await self.async_step_user()

        errors = {}
        labels: list[str] = []

        if user_input is not None:
            advanced = user_input.get("advanced") if isinstance(user_input.get("advanced"), dict) else {}
            self._create_new_defaults = {
                "model_name": str(user_input["model_name"]).strip(),
                "labels": str(user_input["labels"]),
                "source_entities": user_input["source_entities"],
                "advanced": {
                    "mqtt_topic": str(advanced.get("mqtt_topic", "")).strip(),
                    "default_value": advanced.get("default_value", 9999),
                },
            }

            labels = self._parse_label_input(self._create_new_defaults["labels"])
            if len(labels) < 2:
                errors["labels"] = "invalid_labels"

        if user_input is not None and not errors:
            try:
                model = await self._api.async_create_model(
                    {
                        "model_name": self._create_new_defaults["model_name"],
                        "labels": labels,
                        "mqtt_topic": self._create_new_defaults["advanced"].get("mqtt_topic") or None,
                        "default_value": self._create_new_defaults["advanced"].get("default_value", 9999),
                        "source_entities": self._create_new_defaults["source_entities"],
                        "entity_aliases": build_entity_aliases(self._create_new_defaults["source_entities"]),
                        "binding": build_helper_entity_metadata(self._create_new_defaults["model_name"]),
                    }
                )
                return self.async_create_entry(
                    title=build_entry_title(self._app_url),
                    data={CONF_APP_URL: self._app_url},
                    options={CONF_MODELS: [serialize_model_reference(model)]},
                )
            except Ml2MqttApiError:
                errors["base"] = "cannot_connect"

        return self.async_show_form(
            step_id="create_new",
            data_schema=self._build_create_new_schema(),
            errors=errors,
        )


class Ml2MqttOptionsFlow(Ml2MqttFlowBase, config_entries.OptionsFlowWithReload):
    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        Ml2MqttFlowBase.__init__(self)
        self._entry = config_entry
        self._app_url = config_entry.data.get(CONF_APP_URL, DEFAULT_APP_URL)

    def _configured_models(self) -> list[dict[str, Any]]:
        current_entry = self.hass.config_entries.async_get_entry(self._entry.entry_id) if self.hass else self._entry
        return get_configured_models(current_entry or self._entry)

    def _options_entry(self, models: list[dict[str, Any]]) -> FlowResult:
        current_entry = self.hass.config_entries.async_get_entry(self._entry.entry_id) or self._entry
        self.hass.config_entries.async_update_entry(
            current_entry,
            data={CONF_APP_URL: self._app_url},
            title=build_entry_title(self._app_url),
        )
        return self.async_create_entry(title="", data={CONF_MODELS: models})

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        try:
            await self._async_connect(self._app_url)
        except Ml2MqttApiError:
            return await self.async_step_update_url()

        configured_models = self._configured_models()
        configured_ids = {model[CONF_MODEL_ID] for model in configured_models}
        available_ids = {self._model_identifier(model) for model in self._models if self._model_identifier(model)}
        available_actions = {"create_new": "Create and bind new model", "update_url": "Update app URL"}

        if available_ids - configured_ids:
            available_actions = {"add_existing": "Add existing model", **available_actions}
        if self._models:
            available_actions["clone_model"] = "Clone model with observations"
        if configured_models:
            if len(self._models) >= 1:
                available_actions["import_observations"] = "Import observations"
            available_actions["rebind_model"] = "Update model inputs"
            available_actions["remove_model"] = "Delete model"

        if user_input is not None:
            return await getattr(self, f"async_step_{user_input['action']}")()

        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema({
                vol.Required("action", default=next(iter(available_actions))): vol.In(available_actions),
            }),
        )

    async def async_step_update_url(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        errors = {}

        if user_input is not None:
            try:
                await self._async_connect(user_input[CONF_APP_URL])
                return self._options_entry(self._configured_models())
            except Ml2MqttApiError:
                errors["base"] = "cannot_connect"

        return self.async_show_form(
            step_id="update_url",
            data_schema=self._build_app_url_schema(),
            errors=errors,
        )

    async def async_step_add_existing(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        if self._api is None:
            return await self.async_step_update_url()

        configured_models = self._configured_models()
        configured_ids = {model[CONF_MODEL_ID] for model in configured_models}
        available_models = [model for model in self._models if self._model_identifier(model) not in configured_ids]
        if not available_models:
            return self.async_abort(reason="no_available_models")

        errors = {}
        if user_input is not None:
            selected_model = self._get_model_by_id(user_input[CONF_MODEL_ID])
            selected_model_id = self._model_identifier(selected_model) if selected_model is not None else ""
            if selected_model is None or not selected_model_id:
                errors["base"] = "model_not_found"
            else:
                try:
                    await self._api.async_set_binding(
                        selected_model_id,
                        {
                            "source_entities": user_input["source_entities"],
                            "entity_aliases": build_entity_aliases(user_input["source_entities"]),
                            "binding": build_helper_entity_metadata(selected_model["name"]),
                        },
                    )
                    return self._options_entry([*configured_models, serialize_model_reference(selected_model)])
                except Ml2MqttApiError:
                    errors["base"] = "cannot_connect"

        return self.async_show_form(
            step_id="add_existing",
            data_schema=self._build_model_schema(available_models),
            errors=errors,
        )

    async def async_step_create_new(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        if self._api is None:
            return await self.async_step_update_url()

        errors = {}
        labels: list[str] = []

        if user_input is not None:
            advanced = user_input.get("advanced") if isinstance(user_input.get("advanced"), dict) else {}
            self._create_new_defaults = {
                "model_name": str(user_input["model_name"]).strip(),
                "labels": str(user_input["labels"]),
                "source_entities": user_input["source_entities"],
                "advanced": {
                    "mqtt_topic": str(advanced.get("mqtt_topic", "")).strip(),
                    "default_value": advanced.get("default_value", 9999),
                },
            }

            labels = self._parse_label_input(self._create_new_defaults["labels"])
            if len(labels) < 2:
                errors["labels"] = "invalid_labels"

        if user_input is not None and not errors:
            try:
                model = await self._api.async_create_model(
                    {
                        "model_name": self._create_new_defaults["model_name"],
                        "labels": labels,
                        "mqtt_topic": self._create_new_defaults["advanced"].get("mqtt_topic") or None,
                        "default_value": self._create_new_defaults["advanced"].get("default_value", 9999),
                        "source_entities": self._create_new_defaults["source_entities"],
                        "entity_aliases": build_entity_aliases(self._create_new_defaults["source_entities"]),
                        "binding": build_helper_entity_metadata(self._create_new_defaults["model_name"]),
                    }
                )
                return self._options_entry([*self._configured_models(), serialize_model_reference(model)])
            except Ml2MqttApiError:
                errors["base"] = "cannot_connect"

        return self.async_show_form(
            step_id="create_new",
            data_schema=self._build_create_new_schema(),
            errors=errors,
        )

    async def async_step_rebind_model(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        if self._api is None:
            return await self.async_step_update_url()

        configured_models = self._configured_models()
        if not configured_models:
            return self.async_abort(reason="no_configured_models")

        errors = {}
        if user_input is not None:
            try:
                await self._api.async_set_binding(
                    user_input[CONF_MODEL_ID],
                    {
                        "source_entities": user_input["source_entities"],
                        "entity_aliases": build_entity_aliases(user_input["source_entities"]),
                    },
                )
                return self._options_entry(configured_models)
            except Ml2MqttApiError:
                errors["base"] = "cannot_connect"

        return self.async_show_form(
            step_id="rebind_model",
            data_schema=self._build_model_schema(configured_models),
            errors=errors,
        )

    async def async_step_clone_model(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        if self._api is None:
            return await self.async_step_update_url()

        if not self._models:
            return self.async_abort(reason="no_available_models")

        errors = {}
        if user_input is not None:
            self._clone_defaults["source_model_id"] = str(user_input["source_model_id"])
            self._clone_defaults["model_name"] = str(user_input["model_name"]).strip()

            source_model = self._get_model_by_id(user_input["source_model_id"])
            source_model_id = self._model_identifier(source_model) if source_model is not None else ""
            if source_model is None or not source_model_id:
                errors["base"] = "model_not_found"
            else:
                created_model_id: str | None = None
                try:
                    source_detail = await self._api.async_get_model(source_model_id)
                    binding = source_detail.get("binding") if isinstance(source_detail.get("binding"), dict) else {}
                    source_inputs = binding.get("sources", []) if isinstance(binding.get("sources"), list) else []
                    source_features = get_binding_feature_names(binding)
                    if not source_inputs or not source_features:
                        errors["base"] = "incompatible_bindings"
                    else:
                        self._clone_defaults["source_inputs"] = [entry for entry in source_inputs if isinstance(entry, dict)]
                        self._clone_defaults["target_entities"] = {}
                        return await self.async_step_clone_model_inputs()
                except ValueError:
                    if created_model_id is not None:
                        try:
                            await self._api.async_delete_model(created_model_id)
                        except Ml2MqttApiError:
                            pass
                    errors["base"] = "incompatible_bindings"
                except Ml2MqttApiError:
                    if created_model_id is not None:
                        try:
                            await self._api.async_delete_model(created_model_id)
                        except Ml2MqttApiError:
                            pass
                    errors["base"] = "cannot_connect"

        return self.async_show_form(
            step_id="clone_model",
            data_schema=self._build_clone_schema(self._models),
            errors=errors,
        )

    async def async_step_clone_model_inputs(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        if self._api is None:
            return await self.async_step_update_url()

        errors = {}
        source_model_id = str(self._clone_defaults.get("source_model_id") or "")
        model_name = str(self._clone_defaults.get("model_name") or "").strip()
        source_inputs = self._clone_defaults.get("source_inputs", [])
        if not source_model_id or not model_name or not source_inputs:
            return await self.async_step_clone_model()

        if user_input is not None:
            target_entities = {str(key): str(value) for key, value in user_input.items() if str(value).strip()}
            self._clone_defaults["target_entities"] = target_entities
            selected_entities = list(target_entities.values())
            if len(selected_entities) != len(source_inputs) or len(set(selected_entities)) != len(selected_entities):
                errors["base"] = "invalid_clone_mapping"
            else:
                created_model_id: str | None = None
                try:
                    source_detail = await self._api.async_get_model(source_model_id)
                    ordered_source_entities = [
                        str(entry.get("entity_id") or "").strip()
                        for entry in source_inputs
                        if isinstance(entry, dict) and str(entry.get("entity_id") or "").strip()
                    ]
                    ordered_target_entities = [target_entities[entity_id] for entity_id in ordered_source_entities]
                    model = await self._api.async_create_model(
                        {
                            "model_name": model_name,
                            "labels": source_detail.get("labels", []),
                            "source_entities": ordered_target_entities,
                            "entity_aliases": build_entity_aliases(ordered_target_entities),
                            "model_settings": source_detail.get("model_settings"),
                            "preprocessors": source_detail.get("preprocessors"),
                            "postprocessors": source_detail.get("postprocessors"),
                            "binding": build_helper_entity_metadata(model_name),
                        }
                    )
                    created_model_id = model[CONF_MODEL_ID]
                    await self._async_import_model_observations(
                        source_model_id,
                        created_model_id,
                        replace_existing=True,
                    )
                    learning_type = source_detail.get("learning_type")
                    if isinstance(learning_type, str) and learning_type:
                        await self._api.async_set_learning_type(created_model_id, learning_type)
                    return self._options_entry([*self._configured_models(), serialize_model_reference(model)])
                except ValueError:
                    if created_model_id is not None:
                        try:
                            await self._api.async_delete_model(created_model_id)
                        except Ml2MqttApiError:
                            pass
                    errors["base"] = "incompatible_bindings"
                except Ml2MqttApiError:
                    if created_model_id is not None:
                        try:
                            await self._api.async_delete_model(created_model_id)
                        except Ml2MqttApiError:
                            pass
                    errors["base"] = "cannot_connect"

        return self.async_show_form(
            step_id="clone_model_inputs",
            data_schema=self._build_clone_mapping_schema(),
            errors=errors,
        )

    async def async_step_import_observations(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        if self._api is None:
            return await self.async_step_update_url()

        configured_models = self._configured_models()
        if not configured_models:
            return self.async_abort(reason="no_configured_models")

        errors = {}
        if user_input is not None:
            source_model_id = str(user_input["source_model_id"])
            target_model_id = str(user_input["target_model_id"])
            if source_model_id == target_model_id:
                errors["base"] = "same_model_import"
            else:
                try:
                    await self._async_import_model_observations(
                        source_model_id,
                        target_model_id,
                        replace_existing=bool(user_input.get("replace_existing", False)),
                    )
                    return self._options_entry(configured_models)
                except ValueError:
                    errors["base"] = "incompatible_bindings"
                except Ml2MqttApiError:
                    errors["base"] = "cannot_connect"

        return self.async_show_form(
            step_id="import_observations",
            data_schema=self._build_import_schema(self._models, configured_models),
            errors=errors,
        )

    async def async_step_remove_model(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        if self._api is None:
            return await self.async_step_update_url()

        configured_models = self._configured_models()
        if not configured_models:
            return self.async_abort(reason="no_configured_models")

        errors = {}
        if user_input is not None:
            try:
                await self._api.async_delete_model(user_input[CONF_MODEL_ID])
                remaining_models = [
                    model for model in configured_models if model[CONF_MODEL_ID] != user_input[CONF_MODEL_ID]
                ]
                return self._options_entry(remaining_models)
            except Ml2MqttApiError:
                errors["base"] = "cannot_connect"

        return self.async_show_form(
            step_id="remove_model",
            data_schema=self._build_remove_schema(configured_models),
            errors=errors,
        )
