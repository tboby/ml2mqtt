## Why

The current Home Assistant setup depends on manually editing generated Node-RED flows, duplicating source entities, and managing helper entities outside the app. That makes the system fragile and hard to evolve, even though the central ML and MQTT runtime paths are already usable and should stay close to upstream.

## What Changes

- Add app-managed model binding metadata so each model can declare its Home Assistant source entities, trainer control, and output mappings without changing the existing ML core behavior.
- Add a small adapter-facing app API for listing models, reading model details, reading and writing bindings, and reporting bridge/runtime status.
- Add a Home Assistant custom integration distributed through HACS that lets users bind Home Assistant entities to existing app models, automatically creates helper entities, and replaces manual Node-RED orchestration for normal Home Assistant use.
- Preserve the current MQTT runtime contract and training behavior so existing ML storage, prediction flow, and Node-RED or raw MQTT usage can continue as alternate adapters.
- Update Node-RED generation and documentation to align with the new binding metadata where practical, while keeping manual MQTT-based operation possible in theory.

## Capabilities

### New Capabilities
- `model-bindings`: Store and manage per-model binding metadata for source entities, trainer controls, output entities, and compatibility status.
- `adapter-config-api`: Provide an app API that adapters can use to discover models, inspect labels and topics, manage bindings, and read bridge health.
- `home-assistant-bridge`: Provide a Home Assistant integration that binds Home Assistant entities to app models, publishes runtime snapshots over MQTT, and exposes prediction and control entities in Home Assistant.

### Modified Capabilities

None.

## Impact

- Affects Flask routes and API surface in `ml2mqtt/routes/`.
- Adds binding and status metadata handling around the existing model configuration and runtime flow in `ml2mqtt/ModelService.py`, `ml2mqtt/ModelStore.py`, and related templates.
- Adds a new Home Assistant custom integration package and HACS metadata.
- Touches Node-RED generation, setup UX, and repository documentation so Home Assistant users no longer rely on manual flow editing as the primary path.
