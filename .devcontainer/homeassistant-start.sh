#!/bin/sh
set -eu

rm -f /config/.ha_run.lock

python - <<'PY'
import json
from pathlib import Path

registry_path = Path("/config/.storage/core.entity_registry")
if not registry_path.exists():
    raise SystemExit(0)

data = json.loads(registry_path.read_text())
registry = data.get("data", {})
removed = 0

def keep_entry(entry):
    global removed
    if entry.get("platform") != "ml2mqtt":
        return True
    entity_id = entry.get("entity_id", "")
    unique_id = entry.get("unique_id", "")
    if "_source_" not in entity_id and "_source_" not in unique_id:
        return True
    removed += 1
    return False

entities = registry.get("entities")
if isinstance(entities, list):
    registry["entities"] = [entry for entry in entities if keep_entry(entry)]

deleted_entities = registry.get("deleted_entities")
if isinstance(deleted_entities, list):
    registry["deleted_entities"] = [entry for entry in deleted_entities if keep_entry(entry)]

if removed:
    registry_path.write_text(json.dumps(data, separators=(",", ":")))
PY

exec /init
