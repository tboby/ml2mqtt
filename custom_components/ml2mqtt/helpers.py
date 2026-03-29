from __future__ import annotations

import re
from urllib.parse import urlsplit, urlunsplit
from typing import Any, Mapping, Sequence

from .const import CONF_APP_URL, CONF_MODEL_ID, CONF_MODEL_SLUG, CONF_MODELS, DISABLED_LABEL


def safe_slug(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized or "model"


def build_helper_entity_metadata(model_name: str) -> dict[str, Any]:
    object_id = safe_slug(model_name)
    prefix = f"ml2mqtt_{object_id}"
    return {
        "trainer": {
            "entity_id": f"select.{prefix}_trainer",
            "name": f"{model_name} Current Label",
        },
        "outputs": {
            "prediction": {
                "entity_id": f"sensor.{prefix}_prediction",
                "name": f"{model_name} Prediction",
            },
            "confidence": {
                "entity_id": f"sensor.{prefix}_confidence",
                "name": f"{model_name} Confidence",
            },
            "status": {
                "entity_id": f"sensor.{prefix}_bridge_status",
                "name": f"{model_name} Bridge Status",
            },
        },
    }


def normalize_app_url(app_url: str) -> str:
    return app_url.strip().rstrip("/")


def build_entry_title(app_url: str) -> str:
    normalized = normalize_app_url(app_url)
    parsed = urlsplit(normalized)
    location = parsed.netloc or parsed.path or normalized
    return f"ML2MQTT ({location})"


def serialize_model_reference(model: Mapping[str, Any], legacy_unique_prefix: str | None = None) -> dict[str, Any]:
    model_id = str(model.get(CONF_MODEL_ID) or model.get("id") or "").strip()
    reference = {
        CONF_MODEL_ID: model_id,
        CONF_MODEL_SLUG: str(model.get("slug") or safe_slug(model_id)),
        "name": str(model.get("name") or model_id),
    }
    if legacy_unique_prefix:
        reference["legacy_unique_prefix"] = legacy_unique_prefix
    return reference


def get_configured_models(entry: Any) -> list[dict[str, str]]:
    options = getattr(entry, "options", {}) or {}
    if CONF_MODELS in options:
        models = options.get(CONF_MODELS, [])
        if isinstance(models, list):
            return [dict(model) for model in models if isinstance(model, dict)]
        return []

    data = getattr(entry, "data", {}) or {}
    legacy_model_id = data.get(CONF_MODEL_ID)
    if not legacy_model_id:
        return []

    return [{
        CONF_MODEL_ID: str(legacy_model_id),
        CONF_MODEL_SLUG: str(data.get(CONF_MODEL_SLUG) or safe_slug(str(legacy_model_id))),
        "name": str(getattr(entry, "title", legacy_model_id)),
    }]


def build_model_edit_url(app_url: str, model_slug: str) -> str:
    parsed = urlsplit(normalize_app_url(app_url))
    if parsed.hostname == "workspace" and parsed.port == 5000:
        parsed = parsed._replace(netloc="localhost:15000")
    return f"{urlunsplit(parsed)}/edit-model/{model_slug}/settings"


def build_device_identifier(entry_id: str, model_slug: str) -> str:
    return f"{entry_id}_{safe_slug(model_slug)}"


def build_snapshot_payload(
    sources: Sequence[Mapping[str, Any]],
    source_states: Mapping[str, Any],
    active_label: str | None,
) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for source in sources:
        entity_id = source.get("entity_id")
        if not entity_id:
            continue
        payload.append({
            "entity_id": entity_id,
            "state": source_states.get(entity_id),
        })

    if active_label and active_label != DISABLED_LABEL:
        payload.append({"label": active_label})

    return payload
