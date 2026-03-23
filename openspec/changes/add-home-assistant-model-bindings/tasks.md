## 1. App binding metadata

- [x] 1.1 Add versioned per-model binding metadata helpers for source bindings, trainer binding, output metadata, and compatibility status.
- [x] 1.2 Update model create and update flows so bindings can be absent, created later, or initialized from selected source entities.
- [x] 1.3 Compute and persist warning-only compatibility state when binding order or source membership changes.

## 2. Adapter configuration API

- [x] 2.1 Add versioned JSON endpoints for adapter model list and model detail reads.
- [x] 2.2 Add create-model API support that derives or validates `input_count` from adapter-provided source selections.
- [x] 2.3 Add binding read/write/clear endpoints plus a bridge-status response for each model.
- [x] 2.4 Define and document the initial adapter API access model for Home Assistant deployments.

## 3. Home Assistant integration

- [x] 3.1 Scaffold the custom integration package and HACS metadata for `ml2mqtt`.
- [x] 3.2 Implement the Home Assistant flow for binding an existing model or creating a new model from selected entities.
- [x] 3.3 Implement the MQTT runtime coordinator that watches selected entities, builds compatible snapshot payloads, and subscribes to prediction updates.
- [x] 3.4 Create the trainer select, prediction sensor, confidence sensor, and bridge status entities for each bound model.
- [x] 3.5 Handle app API and MQTT unavailability using Home Assistant retry and unload patterns.

## 4. Compatibility and UX follow-through

- [x] 4.1 Verify that raw MQTT and existing Node-RED snapshot publishers still work with bound and unbound models.
- [x] 4.2 Decide whether Node-RED generation should read binding metadata now or remain a documented compatibility path for this change.
- [x] 4.3 Update app and repository docs for the new Home Assistant-first setup flow and legacy fallback flow.

## 5. Verification

- [x] 5.1 Add app-level tests for binding metadata, compatibility warnings, and adapter API validation.
- [x] 5.2 Add integration-level tests or scripted verification for bind, predict, and training flows through Home Assistant.
