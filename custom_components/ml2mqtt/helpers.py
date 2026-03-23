from __future__ import annotations

import re
from typing import Any, Mapping, Sequence

from .const import DISABLED_LABEL


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
            "name": f"{model_name} Trainer",
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
