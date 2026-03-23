## Context

The current Home Assistant path is driven by generated Node-RED flows that users must import and edit by hand. That flow duplicates source entity configuration, creates helper entities outside the app, and leaves model-to-entity bindings implicit even though the core ML path already accepts a generic MQTT snapshot payload and should remain close to upstream.

This repo already has most of the runtime pieces needed for prediction and training: the app creates models, the existing MQTT contract publishes snapshots to `<topic>/set` and predictions to `<topic>/state`, and `ModelService.predictLabel()` handles current training semantics. The redesign therefore needs to improve the control plane and Home Assistant UX without rewriting the ML engine.

Key constraints from discovery:

- Keep the central ML runtime and storage behavior as unchanged as practical for upstream mergeability.
- Keep the current MQTT runtime contract and current training behavior where every relevant source change becomes a sample when a label is active.
- Make Home Assistant the primary day-to-day UX, but preserve raw MQTT and Node-RED operation as alternate adapters.
- Let source binding changes warn instead of hard-blocking or auto-resetting models.
- Auto-create helper entities in Home Assistant.
- Prefer an entity-first Home Assistant flow where selected entities can drive model creation, while the app remains the canonical model service.

## Goals / Non-Goals

**Goals:**
- Preserve the existing snapshot-based MQTT runtime and `ModelService.predictLabel()` behavior.
- Add explicit per-model binding metadata for source entities, trainer controls, outputs, and compatibility warnings.
- Add a small app API that adapters can use to discover models, create models, and manage bindings.
- Add a Home Assistant integration that provides entity selection, helper entities, MQTT orchestration, and status reporting.
- Keep existing Node-RED and raw MQTT paths working against the same runtime contract.

**Non-Goals:**
- Rework classifiers, preprocessors, postprocessors, or training algorithms.
- Replace the MQTT runtime with an HTTP- or WebSocket-only runtime.
- Guarantee that changing source entities is safe without retraining.
- Replace the entire existing app UI in the first implementation wave.

## Decisions

### 1. Preserve the existing MQTT snapshot runtime contract

The Home Assistant integration will publish the same snapshot payload shape that the app already accepts today: an ordered set of `{ "entity_id": ..., "state": ... }` objects plus an optional `{ "label": ... }` object on the existing model topic.

Why:
- It keeps `ModelService.predictLabel()` and current training behavior intact.
- It minimizes churn in classifier, preprocessor, and persistence code.
- It preserves compatibility with raw MQTT publishers and Node-RED.

Alternative considered:
- A new delta-based core runtime was rejected for this change because it would force a deeper ML-engine redesign and make upstream sync harder.

### 2. Keep the app as the canonical model service, but let Home Assistant initiate creation

The app remains the source of truth for model definitions and stored configuration. The integration may still offer an entity-first creation flow by collecting selected Home Assistant entities and calling the app API to create the model with the matching binding metadata and `input_count`.

Why:
- It resolves the tension between "app creates" and "select entities then create from that" by treating Home Assistant as the UX and the app as the actual model registry.
- It keeps non-Home-Assistant usage aligned with existing app-driven behavior.

Alternative considered:
- Making the integration own model definitions was rejected because it would push too much non-HA logic into Home Assistant and weaken standalone support.

### 3. Introduce binding metadata as control-plane state, separate from learned entity keys

Each model will gain versioned binding metadata that records:
- ordered source bindings
- trainer control binding
- prediction and helper entity metadata
- bridge status / compatibility warnings

This metadata should be stored alongside existing model config rather than replacing learned sensor keys or observation storage.

Why:
- Existing learned entity keys are retrospective ML data, not a safe source of truth for UI bindings.
- Keeping bindings separate avoids invasive changes to current ML storage.
- A versioned metadata document gives room for later adapters without locking the design to Home Assistant internals.

Alternative considered:
- Adding fully normalized new database tables for bindings was deferred to keep the first implementation closer to existing code paths.

### 4. Add a versioned adapter configuration API in the app

The app will expose a small JSON API for adapters to:
- list models and key metadata
- fetch model details, labels, topics, and current binding state
- create models from adapter-provided selections
- update or clear bindings for an existing model
- read runtime and compatibility status relevant to the bridge

Why:
- Home Assistant selectors and config flows work best with a request/response API.
- MQTT remains the runtime transport, but request/response config avoids topic sprawl and state reconstruction complexity.

Alternative considered:
- MQTT-only configuration was rejected because it would re-create the same orchestration complexity this change is trying to remove.

### 5. Keep all Home Assistant-specific behavior inside the custom integration

The integration owns entity selection, state subscriptions, automatic helper entity creation, runtime snapshot assembly, and Home Assistant availability behavior. The app remains Home Assistant-agnostic and only understands adapter metadata and MQTT payloads.

Why:
- It preserves the app as a reusable standalone service.
- It matches Home Assistant's native strengths: config flows, selectors, registries, and push-style entity updates.

Alternative considered:
- Having the app call Home Assistant APIs directly was rejected because it would blur the boundary between core logic and the HA adapter.

### 6. Binding changes warn, but do not auto-reset model data

If a user changes the set or order of bound source entities for an existing model, the app saves the new binding metadata and marks the model as needing review or retraining. It does not silently delete observations, reset the model, or hard-block the change.

Why:
- This matches the user's preference for warning-only behavior.
- It acknowledges that source changes are often model-breaking while still allowing deliberate experimentation.

Alternative considered:
- Automatic retraining or automatic resets were rejected because they would be both destructive and misleading.

### 7. Preserve legacy adapters; treat Node-RED generation as optional follow-through

Manual MQTT and Node-RED-based operation remain supported by keeping the current runtime contract. Node-RED generation may later consume binding metadata to reduce manual edits, but that is not required to make the new architecture useful.

Why:
- It keeps the change focused on the new adapter path without removing the existing one.
- It avoids coupling initial success to a full Node-RED generator rewrite.

## Risks / Trade-offs

- [Changing bound sources can invalidate existing training data] -> Persist compatibility warnings and show retraining guidance in both the app and the integration.
- [Binding metadata stored near existing config may become loosely structured] -> Version the metadata document and funnel access through helper methods rather than ad hoc dict mutation.
- [Two control surfaces can drift] -> Make the app the only write authority and have the integration read back the saved state after every mutation.
- [The new app API introduces local network exposure] -> Keep the API versioned and limited to adapter concerns; document deployment assumptions and leave stronger auth hardening as a tracked follow-up if needed.
- [Supporting legacy and new adapter paths increases test scope] -> Add contract tests for snapshot compatibility and adapter smoke tests around binding creation and prediction updates.

## Migration Plan

- Existing model databases remain valid and continue accepting the current MQTT snapshot contract.
- Existing models begin with no binding metadata and can continue using Node-RED or raw MQTT unchanged.
- Home Assistant users can bind existing models after the fact, or create a new model from selected entities through the integration.
- When a new binding disagrees with existing learned entities, the app warns that retraining may be required but preserves stored model data.
- Rollout can be phased: add binding metadata and API first, then add the Home Assistant integration, then update docs and optional Node-RED generation improvements.
- Rollback remains low-risk: disable the integration and continue driving the model through the existing MQTT contract.

## Open Questions

- Should the first release require adapter API authentication, or should API hardening be a follow-up once add-on discovery and deployment assumptions are finalized?
