## ADDED Requirements

### Requirement: Adapters can discover model and binding state
The system SHALL provide a versioned app API that allows an adapter to list models and inspect model details needed for binding and runtime setup. Model details SHALL include the model identifier, labels, MQTT topic, input count, current binding metadata, and compatibility status.

#### Scenario: Adapter lists available models
- **WHEN** an adapter requests the model list from the app API
- **THEN** the response includes enough metadata to let the adapter present existing models as bindable targets

#### Scenario: Adapter reads model detail
- **WHEN** an adapter requests details for a specific model
- **THEN** the response includes the model's labels, MQTT topic, input count, binding metadata, and compatibility warning state

### Requirement: Adapters can create models from selected sources
The system SHALL allow an adapter to request model creation with an initial binding definition derived from selected source entities. When a model is created this way, the app SHALL set the model input count from the selected source count or reject the request if an explicitly supplied count does not match.

#### Scenario: Home Assistant creates model from selected entities
- **WHEN** an adapter submits a create-model request with four selected source entities and no explicit input count
- **THEN** the app creates the model with an input count of four and stores the initial binding metadata

#### Scenario: Mismatched explicit count is rejected
- **WHEN** an adapter submits a create-model request whose explicit input count does not match the number of selected source entities
- **THEN** the app rejects the request with a validation error instead of creating an inconsistent model

### Requirement: Adapters can update bindings and read bridge status
The system SHALL allow an adapter to update or clear a model binding and to read bridge-oriented status for that model, including the current compatibility state and recent runtime health indicators relevant to the adapter.

#### Scenario: Adapter updates an existing model binding
- **WHEN** an adapter saves a revised binding for an existing model
- **THEN** the app persists the binding, recalculates compatibility state, and returns the saved binding document

#### Scenario: Adapter reads bridge status
- **WHEN** an adapter requests bridge status for a model
- **THEN** the app returns the model's current compatibility warning state and the latest available bridge/runtime status fields defined by the API
