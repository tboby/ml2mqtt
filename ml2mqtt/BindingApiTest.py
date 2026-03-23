import json
import tempfile
import unittest
from pathlib import Path

from flask import Flask

from ModelService import ModelService
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

    def test_bound_model_accepts_legacy_snapshot_payload(self):
        self.model.setModelBinding({
            "sources": ["sensor.one", "sensor.two"],
        })

        self.model.predictLabel(json.dumps([
            {"entity_id": "sensor.one", "state": 12.5},
            {"entity_id": "sensor.two", "state": 3.1},
        ]))

        bridge_status = self.model.getBridgeStatus()
        self.assertIn("last_input_at", bridge_status)
        self.assertTrue(self.mqtt.published)
        published_topic, published_payload = self.mqtt.published[-1]
        self.assertEqual(published_topic, "ml2mqtt/presence-model/state")
        self.assertEqual(json.loads(published_payload)["confidence"], 0)

    def test_unbound_model_accepts_legacy_snapshot_payload(self):
        self.model.predictLabel(json.dumps([
            {"entity_id": "sensor.one", "state": 12.5},
            {"entity_id": "sensor.two", "state": 3.1},
        ]))

        bridge_status = self.model.getBridgeStatus()
        self.assertEqual(bridge_status["compatibility_status"]["state"], "unbound")
        self.assertTrue(self.mqtt.published)


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


if __name__ == "__main__":
    unittest.main()
