# Home Assistant Bridge Verification

Use this checklist to verify the custom integration against a running ML2MQTT app.

## 1. Bind or create a model

1. Install the custom integration from HACS.
2. Add the integration in Home Assistant.
3. Enter the reachable ML2MQTT app URL.
4. Either:
   - bind an existing model to selected Home Assistant entities, or
   - create a new model from selected Home Assistant entities.
5. Confirm the config entry finishes loading without `ConfigEntryNotReady` errors.

Expected result:
- the app returns a saved binding from `/api/v1/models/<model>/binding`
- Home Assistant creates trainer, prediction, confidence, and bridge status entities

## 2. Verify inference flow

1. Set the trainer select to `Disabled`.
2. Change one of the bound source entities.
3. Watch the model's MQTT command topic `<topic>/set`.
4. Watch the prediction topic `<topic>/state`.

Expected result:
- Home Assistant publishes a full snapshot payload to `<topic>/set`
- ML2MQTT publishes a prediction JSON payload to `<topic>/state`
- the prediction and confidence entities update in Home Assistant

## 3. Verify training flow

1. Set the trainer select to a non-disabled label.
2. Change one of the bound source entities.
3. Open the model's Observations page in ML2MQTT.

Expected result:
- Home Assistant publishes the same full snapshot payload plus the active label
- ML2MQTT stores a new observation using the current learning behavior
- recent MQTT history in the ML2MQTT UI shows the incoming payload

## 4. Verify compatibility warnings

1. Change the bound source entity order or membership from the integration.
2. Read `/api/v1/models/<model>/binding` or `/api/v1/models/<model>/bridge-status`.

Expected result:
- the binding saves successfully
- the app reports a warning-only compatibility state
- existing observations remain intact until the user chooses to retrain or clean up data

## 5. Verify legacy compatibility path

1. Publish a snapshot payload manually or from the existing Node-RED export.
2. Do this once for a model with bindings and once for a model without bindings.

Expected result:
- both models continue to accept the existing snapshot MQTT contract
- prediction publishing continues to use `<topic>/state`
