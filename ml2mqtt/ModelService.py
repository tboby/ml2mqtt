import logging
import json
import time
from collections import Counter
from copy import deepcopy
from typing import Any, Dict, List, Optional, Union

from ModelStore import ModelStore, ModelObservation, EntityKey
from classifiers.RandomForest import RandomForest, RandomForestParams
from classifiers.KNNClassifier import KNNClassifier, KNNParams
from MqttClient import MqttClient
from postprocessors.PostprocessorFactory import PostprocessorFactory
from postprocessors.base import BasePostprocessor
from preprocessors.base import BasePreprocessor
from preprocessors.PreprocessorFactory import PreprocessorFactory
from nodered.nodered_generator import NodeRedGenerator
DISABLED_LABEL = "Disabled"


class ModelService:
    def __init__(self, mqttClient: MqttClient, modelstore: ModelStore):
        self._mqttClient = mqttClient
        self._modelstore: ModelStore = modelstore
        self._model = None
        self._logger = logging.getLogger(__name__)
        self._postProcessorFactory = PostprocessorFactory()
        self._postprocessors: List[BasePostprocessor] = []

        self._preprocessorFactory = PreprocessorFactory()
        self._preprocessors: List[BasePreprocessor] = []
        self._runtimeProcessorStorage: Dict[str, Dict[str, Any]] = {}
        
        self._modelType: str
        self._allParams: Dict[str, Dict[str, Any]] = {}
        self._recentMqtt = []
        self._populateModel()
        self._loadPostprocessors()
        self._loadPreprocessors()

    def dispose(self) -> None:
        topic = self.getMqttTopic()
        self._mqttClient.unsubscribe(f"{topic}/set", self.predictLabel)
        self._modelstore.close()

    def subscribeToMqttTopics(self) -> None:
        topic = self.getMqttTopic()
        self._logger.info("Subscribing to MQTT topic: %s/set", topic)
        self._mqttClient.subscribe(f"{topic}/set", self.predictLabel)

    def _populateModel(self) -> None:
        settings = self._modelstore.getDict('model_settings') or {}
        self._modelType = settings.get("model_type", "RandomForest")
        self._allParams = settings.get("model_parameters", {})

        paramsForThisModel = self._allParams.get(self._modelType, {})

        self._logger.info(f"Loading with settings {settings}")

        if self._modelType == "KNN":
            self._model = KNNClassifier(params=paramsForThisModel)
        else:
            self._model = RandomForest(params=paramsForThisModel)

        observations = self._modelstore.getObservations()
        self._model.populateDataframe(observations)

    def _loadPostprocessors(self) -> None:
        """Load postprocessors from model settings."""
        postProcessors = self._modelstore.getPostprocessors()
        self._postprocessors = []
        
        for postprocessor_data in postProcessors:
            try:
                postprocessor = self._postProcessorFactory.create(postprocessor_data.type, postprocessor_data.id, postprocessor_data.params)
                self._postprocessors.append(postprocessor)
            except ValueError as e:
                self._logger.warning(f"Failed to load postprocessor: {e}")


    def _loadPreprocessors(self) -> None:
        """Load postprocessors from model settings."""
        preprocessors = self._modelstore.getPreprocessors()
        self._preprocessors = []
        
        for preprocessor_data in preprocessors:
            try:
                postprocessor = self._preprocessorFactory.create(preprocessor_data.type, preprocessor_data.id, preprocessor_data.params)
                self._preprocessors.append(postprocessor)
            except ValueError as e:
                self._logger.warning(f"Failed to load preprocessor: {e}")

        self._runtimeProcessorStorage = self._getStoredProcessorStorage()


    def getEntityKeys(self) -> List[EntityKey]:
        features = self._model.getFeatureImportance() or {}
        entities = self._modelstore.getEntityKeys()
        for entity in entities:
            entity.significance = features.get(entity.name, 0.0)
        return entities

    def getAccuracy(self) -> Optional[float]:
        return self._model.getAccuracy()

    def _recordRecentMqttHistory(self, entityMap: Dict[str, Any]) -> None:
        previousEntityMap = self._modelstore.getDict("mqtt_observations")
        if "history" in previousEntityMap:
            previousEntityMap["history"].append(entityMap)
            if len(previousEntityMap["history"]) > 10:
                previousEntityMap["history"].pop(0)
        else:
            previousEntityMap["history"] = [entityMap]
        self._modelstore.saveDict("mqtt_observations", previousEntityMap)

    def _getStoredProcessorStorage(self) -> Dict[str, Dict[str, Any]]:
        processorStorage = self._modelstore.getDict("processor_storage")
        if isinstance(processorStorage, dict):
            return deepcopy(processorStorage)
        return {}

    def _applyPreprocessors(
        self,
        entityMap: Dict[str, Any],
        processorStorage: Dict[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        processedEntityMap = dict(entityMap)
        for preprocessor in self._preprocessors:
            state = processorStorage.setdefault(str(preprocessor.dbId), {})
            processedEntityMap = preprocessor.process(processedEntityMap, state)
            if not processedEntityMap:
                self._logger.debug("No entity values to process.")
                return None
        return processedEntityMap

    def _processRawObservation(
        self,
        label: str,
        entityMap: Dict[str, Any],
        assignedTime: Optional[float] = None,
        persist_raw: bool = True,
        publish_prediction: bool = True,
        rebuild_model: bool = True,
    ) -> None:
        observationTime = assignedTime if assignedTime is not None else time.time()
        learningType = self.getLearningType()
        shouldUpdateLearningHistory = learningType != "DISABLED" and label != DISABLED_LABEL

        if publish_prediction:
            self.updateBridgeStatus({
                "last_input_at": time.time(),
                "last_label": label,
                "last_error": None,
                "mqtt_connected": self._mqttClient._connected,
            })
            self._recordRecentMqttHistory(entityMap)

        liveEntityValues: Optional[Dict[str, Any]] = None
        if publish_prediction:
            liveProcessedEntityMap = self._applyPreprocessors(entityMap, self._runtimeProcessorStorage)
            if liveProcessedEntityMap:
                liveEntityValues = {k: v for k, v in liveProcessedEntityMap.items() if v is not None}

        if persist_raw and shouldUpdateLearningHistory:
            self._modelstore.addRawObservation(label, entityMap, observationTime)

        if shouldUpdateLearningHistory:
            processorStorage = self._getStoredProcessorStorage()
            processedEntityMap = self._applyPreprocessors(entityMap, processorStorage)
            self._modelstore.saveDict("processor_storage", processorStorage)

            if processedEntityMap:
                entityValues = {k: v for k, v in processedEntityMap.items() if v is not None}
            else:
                entityValues = None

            if learningType == "LAZY":
                prediction, confidence = self._model.predictLabel(entityValues or {})
                if processedEntityMap and (prediction != label or confidence < 0.8):
                    entityValues = self._modelstore.sortEntityValues(processedEntityMap, True)
                    self._logger.info("Adding training observation for label: %s", label)
                    self._modelstore.addObservation(label, entityValues, observationTime)
                    if rebuild_model:
                        self._populateModel()
            elif learningType == "EAGER":
                if processedEntityMap:
                    entityValues = self._modelstore.sortEntityValues(processedEntityMap, True)
                    self._logger.info("Adding training observation for label: %s", label)
                    self._modelstore.addObservation(label, entityValues, observationTime)
                    if rebuild_model:
                        self._populateModel()
        elif learningType == "DISABLED":
            self._logger.info("Learning is disabled; skipping observation persistence")
        else:
            self._logger.info("Trainer label is disabled; ignoring observation for learning")

        if not publish_prediction:
            return

        if not liveEntityValues:
            return

        prediction, confidence = self._model.predictLabel(liveEntityValues)
        confidence = round(confidence, 4)
        observation = liveEntityValues
        for postprocessor in self._postprocessors:
            observation, prediction = postprocessor.process(observation, prediction, confidence)
            if prediction is None:
                return

        topic = self.getMqttTopic()
        self._mqttClient.publish(f"{topic}/state", json.dumps({"state": prediction, "confidence": confidence}))
        self.updateBridgeStatus({
            "last_prediction_at": time.time(),
            "last_prediction": prediction,
            "last_confidence": confidence,
            "mqtt_connected": self._mqttClient._connected,
        })
        self._logger.info(f"Predicted label: {prediction} with confidence {confidence}")

    def _normalizeRawObservation(self, observation: Dict[str, Any]) -> ModelObservation:
        if not isinstance(observation, dict):
            raise ValueError("Each observation must be a JSON object")

        sensorValues = observation.get("sensorValues", observation.get("sensor_values", observation.get("sensors")))
        if not isinstance(sensorValues, dict) or not sensorValues:
            raise ValueError("Each observation must include a non-empty sensorValues object")

        rawTime = observation.get("time", observation.get("timestamp"))
        if rawTime in (None, ""):
            observedAt = time.time()
        else:
            observedAt = float(rawTime)

        label = str(observation.get("label") or DISABLED_LABEL)
        normalizedValues = {str(key): value for key, value in sensorValues.items() if str(key).strip()}
        if not normalizedValues:
            raise ValueError("Each observation must include at least one sensor value")

        return ModelObservation(observedAt, label, normalizedValues)

    def predictLabel(self, msg: Any) -> None:
        self._recentMqtt.append(msg)
        if len(self._recentMqtt) > 10:
            self._recentMqtt.pop(0)

        messageStr: str
        if hasattr(msg, "payload"):
            try:
                messageStr = msg.payload.decode()
            except Exception as e:
                self._logger.warning("Could not decode MQTT payload: %s", e)
                return
        else:
            messageStr = str(msg)

        try:
            entities: List[Dict[str, Any]] = json.loads(messageStr)
        except json.JSONDecodeError:
            self._logger.warning("Invalid JSON: %s", messageStr)
            self.updateBridgeStatus({
                "last_input_at": time.time(),
                "last_error": "invalid_json",
                "mqtt_connected": self._mqttClient._connected,
            })
            return

        label: str = DISABLED_LABEL
        entityMap: Dict[str, Any] = {}

        for entity in entities:
            if "label" in entity:
                label = entity["label"]
            elif "entity_id" in entity and "state" in entity:
                entityMap[entity["entity_id"]] = entity["state"]

        self._processRawObservation(label, entityMap)

    def getMqttTopic(self) -> str:
        return self._modelstore.getMqttTopic() or ""

    def setMqttTopic(self, mqttTopic: str) -> None:
        self._modelstore.setMqttTopic(mqttTopic)

    def getName(self) -> str:
        return self._modelstore.getName() or ""

    def setName(self, modelName: str) -> None:
        self._modelstore.setName(modelName)

    def getObservations(self) -> List[ModelObservation]:
        return self._modelstore.getObservations()

    def getModelSize(self) -> int:
        return self._modelstore.getModelSize()

    def getObservationCount(self) -> int:
        return self._modelstore.getObservationCount()

    def getRawObservations(self) -> List[ModelObservation]:
        return self._modelstore.getRawObservations()

    def getRawObservationCount(self) -> int:
        return self._modelstore.getRawObservationCount()

    def deleteRawObservations(self) -> None:
        self._modelstore.deleteRawObservations()

    def importRawObservations(self, observations: List[Dict[str, Any]], replace_existing: bool = False) -> int:
        normalized = [self._normalizeRawObservation(observation) for observation in observations]

        if replace_existing:
            self._modelstore.deleteRawObservations()

        for observation in normalized:
            self._modelstore.addRawObservation(observation.label, observation.sensorValues, observation.time)
        return len(normalized)

    def replayRawObservations(
        self,
        observations: Optional[List[Dict[str, Any]]] = None,
        clear_training_data: bool = False,
        reset_processor_storage: bool = True,
    ) -> int:
        if observations is None:
            normalized = self.getRawObservations()
        else:
            normalized = [self._normalizeRawObservation(observation) for observation in observations]

        if clear_training_data:
            self.deleteObservationsSince(0)

        if reset_processor_storage:
            self._modelstore.saveDict("processor_storage", {})

        learningType = self.getLearningType()
        rebuild_during_replay = learningType != "EAGER"

        for observation in normalized:
            self._processRawObservation(
                observation.label,
                observation.sensorValues,
                observation.time,
                persist_raw=False,
                publish_prediction=False,
                rebuild_model=rebuild_during_replay,
            )

        if normalized and not rebuild_during_replay:
            self._populateModel()

        return len(normalized)

    def getObservationCountsByLabel(self) -> Dict[str, int]:
        counts = Counter(observation.label for observation in self.getObservations())
        labels = sorted(set(self.getLabels()) | set(counts.keys()))
        return {label: int(counts.get(label, 0)) for label in labels}

    def getLabels(self) -> List[str]:
        return self._modelstore.getLabels() + self.getModelConfig("labels", [])

    def deleteEntity(self, entityName: str) -> None:
        self._modelstore.deleteEntity(entityName)
        # Rebuild the model after entity deletion
        self._populateModel()

    def getLabelStats(self) -> Optional[Dict[str, Any]]:
        labelStats = self._model.getLabelStats() or {}
        for extraLabel in self.getLabels():
            if not extraLabel in labelStats.keys():   
                labelStats[extraLabel] = {
                    "support":0,
                    "precision": 0,
                    "recall": 0,
                    "f1": 0,
                }        
        return labelStats

    def deleteObservationsByLabel(self, label: str) -> None:
        """Delete all observations with the given label."""
        self._modelstore.deleteObservationsByLabel(label)

        presavedLabels = self.getModelConfig("labels", [])
        presavedLabels.remove(label)
        self.setModelConfig("labels", presavedLabels)

        # Rebuild the model after deletion
        self._populateModel()

    def deleteObservation(self, time: int) -> None:
        """Delete an observation by its timestamp."""
        self._modelstore.deleteObservation(time)
        # Rebuild the model after deletion
        self._populateModel()

    def deleteObservationsSince(self, timestamp: int) -> None:
            """Delete an observation by its timestamp."""
            self._modelstore.deleteObservationsSince(timestamp)
            # Rebuild the model after deletion
            self._populateModel()

    def optimizeParameters(self) -> None:
        best_params = self._model.optimizeParameters(self._modelstore.getObservations())

        modelSettings = self.getModelSettings()
        modelSettings["model_parameters"] = modelSettings.get("model_parameters", {})
        modelSettings["model_parameters"][self._modelType] = best_params
        self._modelstore.saveDict("model_settings", modelSettings)

    def getModelSettings(self) -> Dict[str, Any]:
        settings = self._modelstore.getDict('model_settings') or {}
        if not settings:
            settings = {
                "model_type": "RandomForest",
                "model_parameters": {
                    "RandomForest": {
                        "n_estimators": 100,
                        "max_depth": None,
                        "min_samples_split": 2,
                        "min_samples_leaf": 1,
                        "max_features": "sqrt",
                        "class_weight": None,
                        "bootstrap": True,
                        "oob_score": False
                    }, "KNN": {
                        "n_neighbors": 5,
                        "weights": "uniform",
                        "algorithm": "auto",
                        "leaf_size": 30,
                        "metric": "minkowski",
                        "p": 2
                    }
                }
            }
        return settings

    def setModelSettings(self, settings: Dict[str, Any]) -> None:
        self._logger.info(f"Setting model settings: {settings}");
        self._modelType = settings.get("model_type", "RandomForest")
        self._allParams = settings.get("model_parameters", {})
        self._modelstore.saveDict("model_settings", settings)
        self._populateModel()

    def getPostprocessors(self) -> List[BasePostprocessor]:
        """Get list of postprocessors."""
        return self._postprocessors

    def getPreprocessors(self) -> List[BasePreprocessor]:
        """Get list of preprocessors."""
        return self._preprocessors

    def addPostprocessor(self, type: str, params: Dict[str, Any]) -> None:
        """Add a new postprocessor."""
        try:
            # First add to database to get the ID
            dbId = self._modelstore.addPostprocessor(type, params)
            # Then create the postprocessor instance
            postprocessor = self._postProcessorFactory.create(type, dbId, params)
            self._postprocessors.append(postprocessor)
        except Exception as e:
            # If postprocessor creation fails, delete from database
            if 'dbId' in locals():
                self._modelstore.deletePostprocessor(dbId)
            raise e

    def addPreprocessor(self, type: str, params: Dict[str, Any]) -> None:
        """Add a new preprocessor."""
        try:
            # First add to database to get the ID
            dbId = self._modelstore.addPreprocessor(type, params)
            # Then create the postprocessor instance
            preprocessor = self._preprocessorFactory.create(type, dbId, params)
            self._preprocessors.append(preprocessor)
            self.deleteObservationsSince(0)
        except Exception as e:
            # If postprocessor creation fails, delete from database.
            if 'dbId' in locals():
                self._modelstore.deletePreprocessor(dbId)
            raise e

    def removePostprocessor(self, index: int) -> None:
        """Remove a postprocessor by index."""
        if 0 <= index < len(self._postprocessors):
            deletedProcessor = self._postprocessors.pop(index)
            
            self._modelstore.deletePostprocessor(deletedProcessor.dbId)

    def removePreprocessor(self, index: int) -> None:
        """Remove a preprocessor by index."""
        if 0 <= index < len(self._preprocessors):
            deletedProcessor = self._preprocessors.pop(index)
            
            self._modelstore.deletePreprocessor(deletedProcessor.dbId)
            self.deleteObservationsSince(0)

    def reorderPreprocessors(self, from_index: int, to_index: int) -> None:
        """Reorder preprocessors."""
        if 0 <= from_index < len(self._preprocessors) and 0 <= to_index < len(self._preprocessors):
            self._logger.info("Previous preprocessors: %s", list(map(lambda p: p, self._preprocessors)))
            preprocessor = self._preprocessors.pop(from_index)
            self._preprocessors.insert(to_index, preprocessor)
            self._logger.info("Reordering preprocessors: %s", list(map(lambda p: p, self._preprocessors)))
            self._modelstore.reorderPreprocessors(map(lambda p: p.dbId, self._preprocessors))
            self.deleteObservationsSince(0)

    def reorderPostprocessors(self, from_index: int, to_index: int) -> None:
        """Reorder postprocessors."""
        if 0 <= from_index < len(self._postprocessors) and 0 <= to_index < len(self._postprocessors):
            self._logger.info("Previous postprocessors: %s", list(map(lambda p: p, self._postprocessors)))
            postprocessor = self._postprocessors.pop(from_index)
            self._postprocessors.insert(to_index, postprocessor)
            self._logger.info("Reordering postprocessors: %s", list(map(lambda p: p, self._postprocessors)))
            self._modelstore.reorderPostprocessors(map(lambda p: p.dbId, self._postprocessors))

    def getLearningType(self):
        settings = self.getModelSettings() or {}
        learningType = settings.get("learning_type", "DISABLED")
        self._logger.info(f"Getting learning type: {learningType}")
        return learningType

    def getModelType(self) -> str:
        settings = self.getModelSettings() or {}
        return str(settings.get("model_type", "RandomForest"))
    
    def setLearningType(self, learningType: str) -> None:
        settings = self.getModelSettings() or {}
        settings["learning_type"] = learningType
        self._logger.info(f"Setting learning type: {learningType}")
        self._modelstore.saveDict("model_settings", settings)

    def getMostRecentMqttObservations(self):
        previousObservations = self._modelstore.getDict("mqtt_observations")
        if 'history' in previousObservations:
            return previousObservations['history']
        else:
            return []

    def _normalizeBindingEntity(self, entity: Any) -> Optional[Dict[str, Any]]:
        if entity is None:
            return None

        if isinstance(entity, str):
            entity_id = entity.strip()
            if not entity_id:
                return None
            return {"entity_id": entity_id, "name": entity_id}

        if not isinstance(entity, dict):
            return None

        entity_id = str(entity.get("entity_id") or entity.get("id") or "").strip()
        if not entity_id:
            return None

        normalized = {
            "entity_id": entity_id,
            "name": str(entity.get("name") or entity_id),
        }
        for key in ("device_id", "area_id", "attribute", "domain"):
            if entity.get(key) is not None:
                normalized[key] = entity[key]
        return normalized

    def _normalizeBindingSources(self, sources: Any) -> List[Dict[str, Any]]:
        if not isinstance(sources, list):
            return []

        normalized: List[Dict[str, Any]] = []
        seen = set()
        for source in sources:
            sourceEntry = self._normalizeBindingEntity(source)
            if sourceEntry is None:
                continue
            entityId = sourceEntry["entity_id"]
            if entityId in seen:
                continue
            seen.add(entityId)
            normalized.append(sourceEntry)
        return normalized

    def _normalizeBindingOutputs(self, outputs: Any) -> Dict[str, Dict[str, Any]]:
        if not isinstance(outputs, dict):
            return {}

        normalized: Dict[str, Dict[str, Any]] = {}
        for name, entry in outputs.items():
            normalizedEntry = self._normalizeBindingEntity(entry)
            if normalizedEntry is not None:
                normalized[name] = normalizedEntry
        return normalized

    def _getBindingSourceIds(self, binding: Optional[Dict[str, Any]]) -> List[str]:
        if not binding:
            return []
        return [source["entity_id"] for source in binding.get("sources", []) if source.get("entity_id")]

    def _buildCompatibilityStatus(self, previousBinding: Optional[Dict[str, Any]], binding: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not binding:
            return {
                "state": "unbound",
                "warnings": [],
                "retraining_required": False,
                "updated_at": time.time(),
            }

        newSources = self._getBindingSourceIds(binding)
        previousSources = self._getBindingSourceIds(previousBinding)
        learnedSources = [entity.name for entity in self._modelstore.getEntityKeys()]

        warnings: List[Dict[str, str]] = []

        def add_warning(code: str, message: str) -> None:
            if code not in {warning["code"] for warning in warnings}:
                warnings.append({"code": code, "message": message})

        if previousSources and previousSources != newSources:
            if set(previousSources) != set(newSources):
                add_warning(
                    "source_membership_changed",
                    "Bound source entities changed. Existing training data may no longer match the selected inputs.",
                )
            else:
                add_warning(
                    "source_order_changed",
                    "Bound source entity order changed. Existing training data may need retraining to stay aligned.",
                )

        if learnedSources and newSources and learnedSources != newSources:
            if set(learnedSources) != set(newSources):
                add_warning(
                    "learned_sources_mismatch",
                    "Bound source entities do not match the entities seen in stored observations.",
                )
            else:
                add_warning(
                    "learned_source_order_mismatch",
                    "Bound source entity order differs from the entity order learned by the stored model.",
                )

        return {
            "state": "warning" if warnings else "ready",
            "warnings": warnings,
            "retraining_required": len(warnings) > 0,
            "updated_at": time.time(),
        }

    def _normalizeBinding(self, binding: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if binding is None:
            return None
        if not isinstance(binding, dict):
            raise ValueError("Binding payload must be a JSON object")

        normalized = {
            "version": int(binding.get("version", 1)),
            "sources": self._normalizeBindingSources(binding.get("sources", [])),
            "trainer": self._normalizeBindingEntity(binding.get("trainer")),
            "outputs": self._normalizeBindingOutputs(binding.get("outputs", {})),
            "adapter": binding.get("adapter", {}) if isinstance(binding.get("adapter", {}), dict) else {},
            "updated_at": time.time(),
        }
        return normalized

    def getModelBinding(self) -> Optional[Dict[str, Any]]:
        binding = self.getModelConfig("binding", None)
        if isinstance(binding, dict) and binding:
            return deepcopy(binding)
        return None

    def setModelBinding(self, binding: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        normalized = self._normalizeBinding(binding)
        previousBinding = self.getModelBinding()

        if normalized is None:
            current = self._modelstore.getDict("config")
            current.pop("binding", None)
            self._modelstore.saveDict("config", current)
            return None

        normalized["compatibility_status"] = self._buildCompatibilityStatus(previousBinding, normalized)
        self.setModelConfig("binding", normalized)
        return deepcopy(normalized)

    def clearModelBinding(self) -> None:
        self.setModelBinding(None)

    def getBindingStatus(self) -> Dict[str, Any]:
        binding = self.getModelBinding()
        if not binding:
            return {
                "state": "unbound",
                "warnings": [],
                "retraining_required": False,
            }
        return deepcopy(self._buildCompatibilityStatus(binding, binding))

    def updateBridgeStatus(self, updates: Dict[str, Any]) -> Dict[str, Any]:
        current = self._modelstore.getDict("bridge_status")
        current.update(updates)
        current["mqtt_connected"] = self._mqttClient._connected
        current["updated_at"] = time.time()
        self._modelstore.saveDict("bridge_status", current)
        return deepcopy(current)

    def getBridgeStatus(self) -> Dict[str, Any]:
        bridgeStatus = self._modelstore.getDict("bridge_status")
        bridgeStatus["mqtt_connected"] = self._mqttClient._connected
        bridgeStatus["binding_present"] = self.getModelBinding() is not None
        bridgeStatus["compatibility_status"] = self.getBindingStatus()
        bridgeStatus["topics"] = {
            "command": f"{self.getMqttTopic()}/set",
            "state": f"{self.getMqttTopic()}/state",
        }
        return deepcopy(bridgeStatus)

    def getModelSummary(self) -> Dict[str, Any]:
        binding = self.getModelBinding()
        inputCount = self.getModelConfig("input_count", len(self._getBindingSourceIds(binding)) or 1)
        return {
            "id": self.getName().lower(),
            "name": self.getName(),
            "mqtt_topic": self.getMqttTopic(),
            "input_count": inputCount,
            "labels": sorted(set(self.getLabels())),
            "binding": binding,
            "compatibility_status": self.getBindingStatus(),
        }

    def getModelDetail(self) -> Dict[str, Any]:
        summary = self.getModelSummary()
        summary.update({
            "learning_type": self.getLearningType(),
            "model_type": self.getModelType(),
            "observation_count": self.getObservationCount(),
            "raw_observation_count": self.getRawObservationCount(),
            "label_counts": self.getObservationCountsByLabel(),
            "bridge_status": self.getBridgeStatus(),
        })
        return summary
    
    def setModelConfig(self, key, value):
        current = self._modelstore.getDict("config")
        current[key] = value
        self._modelstore.saveDict("config", current)
    
    def getModelConfig(self, key, default):
        config = self._modelstore.getDict("config")
        if key in config:
            return config[key]
        else:
            return default

    def generateNodeRed(self) -> str:
        nodeRedGenerator = NodeRedGenerator(self)
        return nodeRedGenerator.generate()
    
    def getRecentMqtt(self) -> str:
        return self._recentMqtt
