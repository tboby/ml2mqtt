import json
import tempfile
import unittest
from pathlib import Path

from flask import Flask

from ModelService import ModelService, MISSING_SENSOR_RECENCY_SECONDS, build_recency_feature_name
from ModelStore import ModelStore
from ModelManager import ModelManager
from routes.model_routes import init_model_routes


class FakeMqttClient:
    def __init__(self):
        self._connected = True
        self.topics = {}
        self.published = []

    def subscribe(self, topic, callback):
        self.topics.setdefault(topic, []).append(callback)

    def unsubscribe(self, topic, callback):
        callbacks = self.topics.get(topic, [])
        if callback in callbacks:
            callbacks.remove(callback)
        if not callbacks and topic in self.topics:
            del self.topics[topic]

    def publish(self, topic, message):
        self.published.append((topic, message))


class ModelBindingServiceTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.mqtt = FakeMqttClient()
        self.store = ModelStore(str(Path(self.tempdir.name) / "binding.db"))
        self.model = ModelService(self.mqtt, self.store)
        self.model.setName("Presence Model")
        self.model.setMqttTopic("ml2mqtt/presence-model")
        self.model.setModelConfig("input_count", 2)

    def tearDown(self):
        self.store.close()
        self.tempdir.cleanup()

    def test_binding_changes_create_warning_without_reset(self):
        initial = self.model.setModelBinding({
            "sources": ["sensor.one", "sensor.two"],
        })
        self.assertEqual(initial["compatibility_status"]["state"], "ready")

        updated = self.model.setModelBinding({
            "sources": ["sensor.one", "sensor.three"],
        })

        self.assertEqual(updated["compatibility_status"]["state"], "warning")
        warning_codes = {warning["code"] for warning in updated["compatibility_status"]["warnings"]}
        self.assertIn("source_membership_changed", warning_codes)
        self.assertEqual(self.model.getObservationCount(), 0)

    def test_bound_model_rejects_payload_without_age_seconds(self):
        self.model.setModelBinding({
            "sources": ["sensor.one", "sensor.two"],
        })

        self.model.predictLabel(json.dumps([
            {"entity_id": "sensor.one", "state": 12.5},
            {"entity_id": "sensor.two", "state": 3.1},
        ]))

        bridge_status = self.model.getBridgeStatus()
        self.assertIn("last_input_at", bridge_status)
        self.assertEqual(bridge_status["last_error"], "missing_age_seconds")
        self.assertFalse(self.mqtt.published)
        self.assertEqual(self.model.getRawObservationCount(), 0)
        self.assertEqual(self.model.getObservationCount(), 0)

    def test_unbound_model_rejects_payload_without_age_seconds(self):
        self.model.predictLabel(json.dumps([
            {"entity_id": "sensor.one", "state": 12.5},
            {"entity_id": "sensor.two", "state": 3.1},
        ]))

        bridge_status = self.model.getBridgeStatus()
        self.assertEqual(bridge_status["compatibility_status"]["state"], "unbound")
        self.assertEqual(bridge_status["last_error"], "missing_age_seconds")
        self.assertFalse(self.mqtt.published)
        self.assertEqual(self.model.getRawObservationCount(), 0)
        self.assertEqual(self.model.getObservationCount(), 0)

    def test_disabled_label_keeps_predictions_live_but_skips_learning_history(self):
        self.model.setLearningType("EAGER")
        self.model.addPreprocessor("rolling_average", {
            "sensor": [{"SELECT_ALL": True}],
            "windowSize": 2,
        })

        self.model.predictLabel(json.dumps([
            {"label": "Kitchen"},
            {"entity_id": "sensor.one", "state": 10.0, "age_seconds": 0.0},
            {"entity_id": "sensor.two", "state": 2.0, "age_seconds": 0.0},
        ]))
        processor_storage = self.store.getDict("processor_storage")

        self.mqtt.published.clear()
        self.model.predictLabel(json.dumps([
            {"label": "Disabled"},
            {"entity_id": "sensor.one", "state": 99.0, "age_seconds": 0.0},
            {"entity_id": "sensor.two", "state": 50.0, "age_seconds": 0.0},
        ]))

        self.assertTrue(self.mqtt.published)
        self.assertEqual(self.model.getRawObservationCount(), 1)
        self.assertEqual(self.model.getObservationCount(), 1)
        self.assertEqual(self.store.getDict("processor_storage"), processor_storage)

    def test_replay_does_not_publish_predictions(self):
        self.model.setLearningType("EAGER")
        self.model.replayRawObservations([
            {
                "label": "Kitchen",
                "sensorValues": {
                    "sensor.one": 12.5,
                    build_recency_feature_name("sensor.one"): 0.0,
                    "sensor.two": 3.1,
                    build_recency_feature_name("sensor.two"): 0.0,
                },
            }
        ])

        self.assertEqual(self.model.getObservationCount(), 1)
        self.assertFalse(self.mqtt.published)

    def test_import_raw_observations_writes_raw_blob_once(self):
        raw_save_count = 0
        original_save_dict = self.store.saveDict

        def counting_save_dict(name, value):
            nonlocal raw_save_count
            if name == "raw_observations":
                raw_save_count += 1
            return original_save_dict(name, value)

        self.store.saveDict = counting_save_dict

        imported = self.model.importRawObservations([
            {
                "time": 100.0,
                "label": "Kitchen",
                "sensorValues": {
                    "sensor.one": 12.5,
                    build_recency_feature_name("sensor.one"): 0.0,
                    "sensor.two": 3.1,
                    build_recency_feature_name("sensor.two"): 0.0,
                },
            },
            {
                "time": 105.0,
                "label": "Office",
                "sensorValues": {
                    "sensor.one": 4.2,
                    build_recency_feature_name("sensor.one"): 0.0,
                    "sensor.two": 8.9,
                    build_recency_feature_name("sensor.two"): 0.0,
                },
            },
        ], replace_existing=True)

        self.assertEqual(imported, 2)
        self.assertEqual(raw_save_count, 1)
        self.assertEqual(self.model.getRawObservationCount(), 2)

    def test_eager_replay_rebuilds_model_once_after_batch(self):
        self.model.setLearningType("EAGER")

        populate_count = 0
        original_populate = self.model._populateModel

        def counting_populate():
            nonlocal populate_count
            populate_count += 1
            original_populate()

        self.model._populateModel = counting_populate

        self.model.replayRawObservations([
            {
                "label": "Kitchen",
                "sensorValues": {
                    "sensor.one": 12.5,
                    build_recency_feature_name("sensor.one"): 0.0,
                    "sensor.two": 3.1,
                    build_recency_feature_name("sensor.two"): 0.0,
                },
            },
            {
                "label": "Office",
                "sensorValues": {
                    "sensor.one": 4.2,
                    build_recency_feature_name("sensor.one"): 0.0,
                    "sensor.two": 8.9,
                    build_recency_feature_name("sensor.two"): 0.0,
                },
            },
        ])

        self.assertEqual(self.model.getObservationCount(), 2)
        self.assertEqual(populate_count, 1)

    def test_eager_replay_saves_processor_storage_once(self):
        self.model.setLearningType("EAGER")
        self.model.addPreprocessor("rolling_average", {
            "sensor": [{"SELECT_ALL": True}],
            "windowSize": 2,
        })

        processor_storage_save_count = 0
        original_save_dict = self.store.saveDict

        def counting_save_dict(name, value):
            nonlocal processor_storage_save_count
            if name == "processor_storage":
                processor_storage_save_count += 1
            return original_save_dict(name, value)

        self.store.saveDict = counting_save_dict

        self.model.replayRawObservations([
            {
                "label": "Kitchen",
                "sensorValues": {
                    "sensor.one": 12.5,
                    build_recency_feature_name("sensor.one"): 0.0,
                    "sensor.two": 3.1,
                    build_recency_feature_name("sensor.two"): 0.0,
                },
            },
            {
                "label": "Kitchen",
                "sensorValues": {
                    "sensor.one": 13.5,
                    build_recency_feature_name("sensor.one"): 0.0,
                    "sensor.two": 4.1,
                    build_recency_feature_name("sensor.two"): 0.0,
                },
            },
        ])

        self.assertEqual(processor_storage_save_count, 1)

    def test_replay_does_not_rebuild_when_learning_is_disabled(self):
        self.model.replayRawObservations([
            {
                "label": "Kitchen",
                "sensorValues": {
                    "sensor.one": 12.5,
                    build_recency_feature_name("sensor.one"): 0.0,
                    "sensor.two": 3.1,
                    build_recency_feature_name("sensor.two"): 0.0,
                },
            }
        ])

        self.assertEqual(self.model.getObservationCount(), 0)
        self.assertFalse(self.mqtt.published)

    def test_replay_preserves_sensor_recency_features(self):
        self.model.setLearningType("EAGER")

        self.model.replayRawObservations([
            {
                "time": 100.0,
                "label": "Kitchen",
                "sensorValues": {
                    "sensor.one": 12.5,
                    build_recency_feature_name("sensor.one"): 0.0,
                    "sensor.two": 3.1,
                    build_recency_feature_name("sensor.two"): 0.0,
                },
            },
            {
                "time": 105.0,
                "label": "Kitchen",
                "sensorValues": {
                    "sensor.one": 12.5,
                    build_recency_feature_name("sensor.one"): 7.0,
                    "sensor.two": 4.2,
                    build_recency_feature_name("sensor.two"): 0.0,
                },
            },
        ])

        observations = self.model.getObservations()
        latest = observations[0]
        self.assertAlmostEqual(latest.sensorValues[build_recency_feature_name("sensor.one")], 7.0)
        self.assertAlmostEqual(latest.sensorValues[build_recency_feature_name("sensor.two")], 0.0)

    def test_live_predictions_publish_sensor_recency_metadata(self):
        self.model.setLearningType("EAGER")
        self.model.setModelBinding({
            "sources": ["sensor.one", "sensor.two"],
        })

        self.model.predictLabel(json.dumps([
            {"label": "Kitchen"},
            {"entity_id": "sensor.one", "state": 12.5, "age_seconds": 0.0},
            {"entity_id": "sensor.two", "state": 3.1, "age_seconds": 0.0},
        ]))

        self.model.predictLabel(json.dumps([
            {"label": "Kitchen"},
            {"entity_id": "sensor.one", "state": 12.5, "age_seconds": 7.0},
            {"entity_id": "sensor.two", "state": 4.2, "age_seconds": 0.0},
        ]))

        self.assertEqual(self.model.getBindingStatus()["state"], "ready")

        observations = self.model.getObservations()
        latest = observations[0]
        self.assertAlmostEqual(latest.sensorValues[build_recency_feature_name("sensor.one")], 7.0)
        self.assertAlmostEqual(latest.sensorValues[build_recency_feature_name("sensor.two")], 0.0)

        published_topic, published_payload = self.mqtt.published[-1]
        self.assertEqual(published_topic, "ml2mqtt/presence-model/state")

        payload = json.loads(published_payload)
        self.assertEqual(payload["sensor_values"], {
            "sensor.one": 12.5,
            "sensor.two": 4.2,
        })
        self.assertEqual(payload["sensor_recency_seconds"], {
            "sensor.one": 7.0,
            "sensor.two": 0.0,
        })

        bridge_status = self.model.getBridgeStatus()
        self.assertEqual(bridge_status["last_sensor_recency_seconds"], {
            "sensor.one": 7.0,
            "sensor.two": 0.0,
        })

    def test_missing_sensor_values_use_max_recency(self):
        self.model.setLearningType("EAGER")
        self.model.setModelBinding({
            "sources": ["sensor.one", "sensor.two"],
        })
        self.model.addPreprocessor("null_handler", {
            "sensor": [{"SELECT_ALL": True}],
            "replacementType": "float",
            "nullReplacement": -1,
        })

        self.model.predictLabel(json.dumps([
            {"label": "Kitchen"},
            {"entity_id": "sensor.one", "state": 12.5, "age_seconds": 0.0},
            {"entity_id": "sensor.two", "state": 3.1, "age_seconds": 0.0},
        ]))

        self.model.predictLabel(json.dumps([
            {"label": "Kitchen"},
            {"entity_id": "sensor.one", "state": 12.5, "age_seconds": 7.0},
            {"entity_id": "sensor.two", "state": None, "age_seconds": None},
        ]))

        observations = self.model.getObservations()
        latest = observations[0]
        self.assertAlmostEqual(latest.sensorValues[build_recency_feature_name("sensor.one")], 7.0)
        self.assertAlmostEqual(
            latest.sensorValues[build_recency_feature_name("sensor.two")],
            MISSING_SENSOR_RECENCY_SECONDS,
        )

        published_topic, published_payload = self.mqtt.published[-1]
        self.assertEqual(published_topic, "ml2mqtt/presence-model/state")

        payload = json.loads(published_payload)
        self.assertEqual(payload["sensor_values"], {
            "sensor.one": 12.5,
            "sensor.two": None,
        })
        self.assertEqual(payload["sensor_recency_seconds"], {
            "sensor.one": 7.0,
            "sensor.two": MISSING_SENSOR_RECENCY_SECONDS,
        })

    def test_analysis_summary_highlights_label_and_source_gaps(self):
        self.model.setLearningType("EAGER")
        self.model.setModelBinding({
            "sources": ["sensor.one", "sensor.two"],
        })
        self.model.addPreprocessor("null_handler", {
            "sensor": [{"SELECT_ALL": True}],
            "replacementType": "float",
            "nullReplacement": -1,
        })

        observations = []
        for index in range(6):
            observations.append({
                "time": float(index + 1),
                "label": "Kitchen",
                "sensorValues": {
                    "sensor.one": 10.0 + index,
                    build_recency_feature_name("sensor.one"): 0.0,
                    "sensor.two": None if index < 3 else 2.0 + index,
                    build_recency_feature_name("sensor.two"): MISSING_SENSOR_RECENCY_SECONDS if index < 3 else 0.0,
                },
            })

        observations.append({
            "time": 7.0,
            "label": "Office",
            "sensorValues": {
                "sensor.one": 25.0,
                build_recency_feature_name("sensor.one"): 0.0,
                "sensor.two": None,
                build_recency_feature_name("sensor.two"): MISSING_SENSOR_RECENCY_SECONDS,
            },
        })

        self.model.importRawObservations(observations, replace_existing=True)
        self.model.replayRawObservations(clear_training_data=True, reset_processor_storage=True)

        summary = self.model.getAnalysisSummary()
        self.assertEqual(summary["overview"]["observation_count"], 7)
        self.assertEqual(summary["coverage"]["underrepresented_labels"][0]["label"], "Office")
        self.assertIn("confusion_matrix", summary["quality"])
        self.assertIn("sequence_evaluation", summary["quality"])

        sensor_two = next(source for source in summary["sources"]["by_source"] if source["source"] == "sensor.two")
        self.assertGreater(sensor_two["missing_rate"], 0.5)
        self.assertGreater(sensor_two["stale_rate"], 0.5)
        self.assertEqual(sensor_two["dominant_label"], "Kitchen")

        recommendation_codes = {item["code"] for item in summary["recommendations"]}
        self.assertIn("label_needs_more_samples", recommendation_codes)

    def test_sequence_evaluation_tracks_stationary_and_transition_behavior(self):
        self.model.setLearningType("EAGER")
        self.model.setModelBinding({
            "sources": ["sensor.one", "sensor.two"],
        })

        raw_observations = []
        samples = [
            (1.0, "Kitchen", 10.0, 1.0),
            (2.0, "Kitchen", 10.5, 1.5),
            (3.0, "Office", 90.0, 100.0),
            (4.0, "Office", 91.0, 101.0),
            (5.0, "Kitchen", 11.0, 2.0),
            (6.0, "Kitchen", 11.5, 2.5),
            (7.0, "Office", 92.0, 102.0),
            (8.0, "Office", 93.0, 103.0),
            (9.0, "Kitchen", 12.0, 3.0),
            (10.0, "Kitchen", 12.5, 3.5),
            (11.0, "Office", 94.0, 104.0),
            (12.0, "Office", 95.0, 105.0),
        ]
        for time_value, label, one_value, two_value in samples:
            raw_observations.append({
                "time": time_value,
                "label": label,
                "sensorValues": {
                    "sensor.one": one_value,
                    build_recency_feature_name("sensor.one"): 0.0,
                    "sensor.two": two_value,
                    build_recency_feature_name("sensor.two"): 0.0,
                },
            })

        self.model.importRawObservations(raw_observations, replace_existing=True)
        self.model.replayRawObservations(clear_training_data=True, reset_processor_storage=True)

        sequence = self.model.getAnalysisSummary()["quality"]["sequence_evaluation"]
        self.assertEqual(sequence["method"], "chronological_run_split")
        self.assertGreater(sequence["train_observations"], 0)
        self.assertGreater(sequence["test_observations"], 0)
        self.assertGreaterEqual(sequence["transition_count"], 1)
        self.assertIsNotNone(sequence["stationary_accuracy"])
        self.assertIn("labels", sequence["confusion_matrix"])


class AdapterApiRoutesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.TemporaryDirectory()
        cls.mqtt = FakeMqttClient()
        cls.manager = ModelManager(cls.mqtt, cls.tempdir.name)
        cls.app = Flask(__name__)
        cls.app.register_blueprint(init_model_routes(cls.manager))
        cls.client = cls.app.test_client()

    @classmethod
    def tearDownClass(cls):
        for model_name in list(cls.manager.getModels().keys()):
            cls.manager.removeModel(model_name)
        cls.tempdir.cleanup()

    def tearDown(self):
        for model_name in list(self.manager.getModels().keys()):
            self.manager.removeModel(model_name)
        self.mqtt.published.clear()

    def test_create_model_derives_input_count_from_selected_sources(self):
        response = self.client.post(
            "/api/v1/models",
            json={
                "model_name": "Room Presence",
                "labels": ["Kitchen", "Office"],
                "source_entities": ["sensor.rssi_1", "sensor.rssi_2"],
            },
        )

        self.assertEqual(response.status_code, 201)
        payload = response.get_json()
        self.assertEqual(payload["input_count"], 2)
        self.assertEqual(
            [source["entity_id"] for source in payload["binding"]["sources"]],
            ["sensor.rssi_1", "sensor.rssi_2"],
        )
        self.assertEqual(
            payload["binding"]["outputs"]["prediction"]["entity_id"],
            "sensor.ml2mqtt_room_presence_prediction",
        )
        self.assertEqual(payload["id"], "room presence")

    def test_create_model_rejects_mismatched_input_count(self):
        response = self.client.post(
            "/api/v1/models",
            json={
                "model_name": "Mismatch Model",
                "labels": ["On", "Off"],
                "source_entities": ["sensor.one", "sensor.two"],
                "input_count": 1,
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("input_count", response.get_json()["error"])

    def test_binding_endpoints_report_compatibility_status(self):
        create_response = self.client.post(
            "/api/v1/models",
            json={
                "model_name": "BindingModel",
                "labels": ["A", "B"],
                "source_entities": ["sensor.one", "sensor.two"],
            },
        )
        self.assertEqual(create_response.status_code, 201)

        update_response = self.client.put(
            "/api/v1/models/BindingModel/binding",
            json={
                "source_entities": ["sensor.two", "sensor.one"],
            },
        )
        self.assertEqual(update_response.status_code, 200)
        update_payload = update_response.get_json()
        self.assertEqual(update_payload["compatibility_status"]["state"], "warning")
        warning_codes = {warning["code"] for warning in update_payload["compatibility_status"]["warnings"]}
        self.assertIn("source_order_changed", warning_codes)

        bridge_response = self.client.get("/api/v1/models/BindingModel/bridge-status")
        self.assertEqual(bridge_response.status_code, 200)
        self.assertEqual(bridge_response.get_json()["compatibility_status"]["state"], "warning")

        delete_response = self.client.delete("/api/v1/models/BindingModel/binding")
        self.assertEqual(delete_response.status_code, 200)
        self.assertEqual(delete_response.get_json()["compatibility_status"]["state"], "unbound")

    def test_delete_model_allows_recreating_same_name(self):
        create_response = self.client.post(
            "/api/v1/models",
            json={
                "model_name": "Reusable Model",
                "labels": ["A", "B"],
                "source_entities": ["sensor.one", "sensor.two"],
            },
        )
        self.assertEqual(create_response.status_code, 201)

        delete_response = self.client.delete("/api/v1/models/Reusable Model")
        self.assertEqual(delete_response.status_code, 200)
        self.assertEqual(delete_response.get_json()["id"], "reusable model")
        self.assertTrue(delete_response.get_json()["deleted"])

        list_response = self.client.get("/api/v1/models")
        self.assertEqual(list_response.status_code, 200)
        self.assertEqual(list_response.get_json()["models"], [])

        recreate_response = self.client.post(
            "/api/v1/models",
            json={
                "model_name": "Reusable Model",
                "labels": ["A", "B"],
                "source_entities": ["sensor.one", "sensor.two"],
            },
        )
        self.assertEqual(recreate_response.status_code, 201)
        self.assertEqual(recreate_response.get_json()["id"], "reusable model")

    def test_analysis_endpoint_returns_model_summary(self):
        create_response = self.client.post(
            "/api/v1/models",
            json={
                "model_name": "Analysis Model",
                "labels": ["Kitchen", "Office"],
                "source_entities": ["sensor.one", "sensor.two"],
            },
        )
        self.assertEqual(create_response.status_code, 201)

        model = self.manager.getModel("analysis model")
        model.setLearningType("EAGER")
        raw_observations = [
            {
                "time": 1.0,
                "label": "Kitchen",
                "sensorValues": {
                    "sensor.one": 10.0,
                    build_recency_feature_name("sensor.one"): 0.0,
                    "sensor.two": 3.0,
                    build_recency_feature_name("sensor.two"): 0.0,
                },
            },
            {
                "time": 2.0,
                "label": "Office",
                "sensorValues": {
                    "sensor.one": 2.0,
                    build_recency_feature_name("sensor.one"): 0.0,
                    "sensor.two": 9.0,
                    build_recency_feature_name("sensor.two"): 0.0,
                },
            },
        ]
        model.importRawObservations(raw_observations, replace_existing=True)
        model.replayRawObservations(clear_training_data=True, reset_processor_storage=True)

        response = self.client.get("/api/v1/models/Analysis Model/analysis")
        self.assertEqual(response.status_code, 200)

        payload = response.get_json()
        self.assertEqual(payload["overview"]["raw_observation_count"], 2)
        self.assertIn("coverage", payload)
        self.assertIn("quality", payload)
        self.assertIn("confusion_matrix", payload["quality"])
        self.assertIn("sequence_evaluation", payload["quality"])
        self.assertIn("features", payload)
        self.assertIn("sources", payload)
        self.assertIn("recommendations", payload)


if __name__ == "__main__":
    unittest.main()
