## ADDED Requirements

### Requirement: Home Assistant provides a model binding flow
The Home Assistant integration SHALL provide a user flow that lets a user either bind Home Assistant entities to an existing app model or create a new app model from selected entities.

#### Scenario: User binds entities to existing model
- **WHEN** a user selects an existing app model and chooses Home Assistant source entities in the integration
- **THEN** the integration saves the binding through the app API and exposes the resulting bound model in Home Assistant

#### Scenario: User creates a new model from selected entities
- **WHEN** a user selects source entities and creates a new model through the integration
- **THEN** the integration requests model creation from the app API using those selected entities as the initial binding definition

### Requirement: Home Assistant auto-creates helper entities
For each bound model, the Home Assistant integration SHALL create the helper entities needed for normal operation, including a trainer select, prediction sensor, confidence sensor, and bridge status entity.

#### Scenario: Helper entities appear after binding
- **WHEN** a model is successfully bound through the integration
- **THEN** the trainer select, prediction sensor, confidence sensor, and bridge status entity are created for that model without manual YAML or Node-RED setup

### Requirement: Home Assistant publishes compatible runtime snapshots
The Home Assistant integration SHALL watch the bound source entities and trainer control, and SHALL publish the existing snapshot-style MQTT payload to the model topic whenever a relevant source state changes or the trainer selection changes. The integration SHALL preserve the current behavior where every relevant source change becomes a training sample while a non-disabled trainer label is active.

#### Scenario: Source change triggers inference snapshot
- **WHEN** a bound source entity changes while the trainer select is disabled
- **THEN** the integration publishes a full snapshot payload for inference on the configured model topic

#### Scenario: Source change triggers training sample
- **WHEN** a bound source entity changes while a non-disabled trainer label is selected
- **THEN** the integration publishes a full snapshot payload that includes the active label so the app records a training observation using the current behavior

### Requirement: Home Assistant handles backend unavailability gracefully
The Home Assistant integration SHALL surface backend unavailability through its bridge status entity and SHALL retry setup or reconnection instead of silently failing when the app API or MQTT runtime is unavailable.

#### Scenario: App unavailable during setup
- **WHEN** the integration cannot reach the app API during setup or reload
- **THEN** the integration reports the model bridge as unavailable and retries using Home Assistant's standard recovery behavior
