## ADDED Requirements

### Requirement: Models support explicit binding metadata
The system SHALL allow each model to store explicit binding metadata separate from learned observations and discovered sensor keys. Binding metadata SHALL support an ordered list of source entities, an optional trainer control binding, output/helper entity metadata, and a compatibility status.

#### Scenario: Existing model has no binding metadata
- **WHEN** an existing model that was created before bindings are introduced is loaded
- **THEN** the model remains valid for raw MQTT and Node-RED use without requiring a binding document

#### Scenario: Adapter saves binding metadata
- **WHEN** an adapter saves a source list, trainer binding, and output metadata for a model
- **THEN** the app persists that binding metadata independently from learned entity keys and returns it on later reads

### Requirement: Binding changes warn without deleting model data
The system SHALL allow the source bindings for an existing model to be updated without automatically deleting observations or resetting the trained model. If the set or order of bound sources changes, the system SHALL mark the model with a compatibility warning that retraining may be required.

#### Scenario: Source binding changes after training
- **WHEN** a user changes the bound source entities for a model that already has observations or a trained classifier
- **THEN** the app saves the new binding metadata and records a warning that the model may need retraining

#### Scenario: Binding update does not force reset
- **WHEN** a binding update is saved for an existing model
- **THEN** the app does not automatically delete observations, labels, or stored model artifacts

### Requirement: Models remain compatible with legacy MQTT adapters
The system SHALL continue to accept the existing snapshot-style MQTT runtime payload for both bound and unbound models so that raw MQTT publishers and Node-RED flows remain usable.

#### Scenario: Raw MQTT client publishes snapshot payload
- **WHEN** a non-Home-Assistant client publishes the existing snapshot payload structure to a model topic
- **THEN** the model processes the payload using the current prediction and training flow without requiring Home Assistant binding metadata
