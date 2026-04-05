import logging
import json
import time
from collections import Counter
from copy import deepcopy
from typing import Any, Dict, List, Optional, Union
from sklearn.metrics import accuracy_score, confusion_matrix, f1_score

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
RECENCY_FEATURE_SUFFIX = "__ml2mqtt_age_seconds"
MISSING_SENSOR_RECENCY_SECONDS = 3600.0
ANALYSIS_MIN_LABEL_SAMPLES = 5
ANALYSIS_STALE_AGE_SECONDS = 300.0
ANALYSIS_SEQUENCE_TEST_SHARE = 0.3


def build_recency_feature_name(entityName: str) -> str:
    return f"{entityName}{RECENCY_FEATURE_SUFFIX}"


def is_recency_feature_name(entityName: str) -> bool:
    return entityName.endswith(RECENCY_FEATURE_SUFFIX)


def get_source_entity_name_from_recency_feature(entityName: str) -> Optional[str]:
    if not is_recency_feature_name(entityName):
        return None
    return entityName[:-len(RECENCY_FEATURE_SUFFIX)]


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
        entities = [
            entity
            for entity in self._modelstore.getEntityKeys()
            if not is_recency_feature_name(entity.name)
        ]
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

    def _splitObservationFeatures(self, entityMap: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, float]]:
        sensorValues: Dict[str, Any] = {}
        sensorRecency: Dict[str, float] = {}

        for rawName, value in entityMap.items():
            entityName = str(rawName)
            if is_recency_feature_name(entityName):
                sourceEntity = get_source_entity_name_from_recency_feature(entityName)
                if sourceEntity is None:
                    continue
                try:
                    sensorRecency[sourceEntity] = max(0.0, float(value))
                except (TypeError, ValueError):
                    sensorRecency[sourceEntity] = 0.0
                continue
            sensorValues[entityName] = value

        return sensorValues, sensorRecency

    def _encodeObservationRecency(
        self,
        entityMap: Dict[str, Any],
        entityAgeMap: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        entity_aliases = self._getEntityAliases()
        if entity_aliases:
            entityMap = self._applyEntityAliases(entityMap, entity_aliases)
            if entityAgeMap:
                entityAgeMap = self._applyEntityAliases(entityAgeMap, entity_aliases)

        sensorValues, sensorRecency = self._splitObservationFeatures(entityMap)

        def normalize_age(value: Any, default: float) -> float:
            try:
                return max(0.0, float(value))
            except (TypeError, ValueError):
                return default

        if entityAgeMap:
            for entityName, ageSeconds in entityAgeMap.items():
                normalizedName = str(entityName).strip()
                if not normalizedName:
                    continue
                sensorRecency[normalizedName] = normalize_age(ageSeconds, MISSING_SENSOR_RECENCY_SECONDS)

        orderedEntities: List[str] = []
        seenEntities = set()
        binding = self.getModelBinding()
        canonicalSourceIds = self._getCanonicalSourceIds(binding, entity_aliases)
        for entityName in [
            *canonicalSourceIds,
            *sensorValues.keys(),
            *sensorRecency.keys(),
        ]:
            normalizedName = str(entityName).strip()
            if not normalizedName or normalizedName in seenEntities or is_recency_feature_name(normalizedName):
                continue
            seenEntities.add(normalizedName)
            orderedEntities.append(normalizedName)

        encoded: Dict[str, Any] = {}

        for entityName in orderedEntities:
            value = sensorValues.get(entityName)
            if value is None:
                encoded[entityName] = None
                encoded[build_recency_feature_name(entityName)] = normalize_age(
                    sensorRecency.get(entityName),
                    MISSING_SENSOR_RECENCY_SECONDS,
                )
                continue

            if entityName not in sensorRecency:
                raise ValueError(f"missing_age_seconds:{entityName}")

            encoded[entityName] = value
            encoded[build_recency_feature_name(entityName)] = normalize_age(
                sensorRecency.get(entityName),
                0.0,
            )

        return encoded

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
        entityAgeMap: Optional[Dict[str, Any]] = None,
        assignedTime: Optional[float] = None,
        persist_raw: bool = True,
        publish_prediction: bool = True,
        rebuild_model: bool = True,
        learning_type: Optional[str] = None,
        processor_storage: Optional[Dict[str, Dict[str, Any]]] = None,
        persist_processor_storage: bool = True,
    ) -> None:
        observationTime = assignedTime if assignedTime is not None else time.time()
        encodedEntityMap = self._encodeObservationRecency(entityMap, entityAgeMap)
        rawSensorValues, rawSensorRecency = self._splitObservationFeatures(encodedEntityMap)

        learningType = learning_type if learning_type is not None else self.getLearningType()
        shouldUpdateLearningHistory = learningType != "DISABLED" and label != DISABLED_LABEL

        if publish_prediction:
            self.updateBridgeStatus({
                "last_input_at": observationTime,
                "last_label": label,
                "last_error": None,
                "last_sensor_values": rawSensorValues,
                "last_sensor_recency_seconds": rawSensorRecency,
                "mqtt_connected": self._mqttClient._connected,
            })
            self._recordRecentMqttHistory(encodedEntityMap)

        liveEntityValues: Optional[Dict[str, Any]] = None
        if publish_prediction:
            liveProcessedEntityMap = self._applyPreprocessors(encodedEntityMap, self._runtimeProcessorStorage)
            if liveProcessedEntityMap:
                liveEntityValues = {k: v for k, v in liveProcessedEntityMap.items() if v is not None}

        if persist_raw and shouldUpdateLearningHistory:
            self._modelstore.addRawObservation(label, encodedEntityMap, observationTime)

        if shouldUpdateLearningHistory:
            processorStorage = processor_storage if processor_storage is not None else self._getStoredProcessorStorage()
            processedEntityMap = self._applyPreprocessors(encodedEntityMap, processorStorage)
            if persist_processor_storage:
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
        self._mqttClient.publish(
            f"{topic}/state",
            json.dumps({
                "state": prediction,
                "confidence": confidence,
                "observed_at": observationTime,
                "sensor_values": rawSensorValues,
                "sensor_recency_seconds": rawSensorRecency,
            }),
        )
        self.updateBridgeStatus({
            "last_prediction_at": observationTime,
            "last_prediction": prediction,
            "last_confidence": confidence,
            "last_sensor_values": rawSensorValues,
            "last_sensor_recency_seconds": rawSensorRecency,
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
        entityAgeMap: Dict[str, Any] = {}
        missingAgeEntities: List[str] = []

        for entity in entities:
            if "label" in entity:
                label = entity["label"]
            elif "entity_id" in entity and "state" in entity:
                entityId = entity["entity_id"]
                entityMap[entityId] = entity["state"]
                if "age_seconds" in entity:
                    entityAgeMap[entityId] = entity["age_seconds"]
                else:
                    missingAgeEntities.append(entityId)

        if missingAgeEntities:
            self._logger.warning("Missing age_seconds for entities: %s", ", ".join(missingAgeEntities))
            self.updateBridgeStatus({
                "last_input_at": time.time(),
                "last_error": "missing_age_seconds",
                "mqtt_connected": self._mqttClient._connected,
            })
            return

        self._processRawObservation(label, entityMap, entityAgeMap=entityAgeMap)

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
            combined = normalized
        else:
            combined = self.getRawObservations() + normalized

        combined.sort(key=lambda observation: observation.time)
        self._modelstore.saveRawObservations(combined)
        return len(normalized)

    def _replayEagerObservations(
        self,
        observations: List[ModelObservation],
        processorStorage: Dict[str, Dict[str, Any]],
    ) -> int:
        trainingObservations: List[ModelObservation] = []

        for observation in observations:
            if observation.label == DISABLED_LABEL:
                continue

            encodedEntityMap = self._encodeObservationRecency(observation.sensorValues)
            processedEntityMap = self._applyPreprocessors(encodedEntityMap, processorStorage)
            if not processedEntityMap:
                continue

            entityValues = {k: v for k, v in processedEntityMap.items() if v is not None}
            if not entityValues:
                continue

            trainingObservations.append(ModelObservation(observation.time, observation.label, entityValues))

        self._modelstore.saveDict("processor_storage", processorStorage)
        if trainingObservations:
            self._modelstore.addObservations(trainingObservations)
        self._populateModel()
        return len(trainingObservations)

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
            self._modelstore.clearObservations()

        learningType = self.getLearningType()
        processorStorage = {} if reset_processor_storage else self._getStoredProcessorStorage()

        if learningType == "EAGER":
            self._replayEagerObservations(normalized, processorStorage)
            return len(normalized)

        if reset_processor_storage:
            self._modelstore.saveDict("processor_storage", processorStorage)

        rebuild_during_replay = learningType != "EAGER"

        for observation in normalized:
            self._processRawObservation(
                observation.label,
                observation.sensorValues,
                assignedTime=observation.time,
                persist_raw=False,
                publish_prediction=False,
                rebuild_model=rebuild_during_replay,
                learning_type=learningType,
                processor_storage=processorStorage,
                persist_processor_storage=False,
            )

        if normalized and learningType != "DISABLED":
            self._modelstore.saveDict("processor_storage", processorStorage)

        if normalized and not rebuild_during_replay:
            self._populateModel()
        elif clear_training_data and (not normalized or learningType == "DISABLED"):
            self._populateModel()

        return len(normalized)

    def getObservationCountsByLabel(self) -> Dict[str, int]:
        counts = Counter(observation.label for observation in self.getObservations())
        labels = sorted(set(self.getLabels()) | set(counts.keys()))
        return {label: int(counts.get(label, 0)) for label in labels}

    def getLabels(self) -> List[str]:
        return self._modelstore.getLabels() + self.getModelConfig("labels", [])

    def deleteEntity(self, entityName: str) -> None:
        availableEntities = {entity.name for entity in self._modelstore.getEntityKeys()}
        entitiesToDelete = [entityName]

        if is_recency_feature_name(entityName):
            sourceEntity = get_source_entity_name_from_recency_feature(entityName)
            if sourceEntity and sourceEntity in availableEntities:
                entitiesToDelete.append(sourceEntity)
        else:
            recencyFeature = build_recency_feature_name(entityName)
            if recencyFeature in availableEntities:
                entitiesToDelete.append(recencyFeature)

        deleted = False
        for name in entitiesToDelete:
            if name not in availableEntities:
                continue
            self._modelstore.deleteEntity(name)
            availableEntities.remove(name)
            deleted = True

        if not deleted:
            raise ValueError("Entity not found")

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

    def _getAnalysisSources(self, rawObservations: List[ModelObservation]) -> List[str]:
        orderedSources: List[str] = []
        seen = set()

        for sourceName in self._getBindingSourceIds(self.getModelBinding()):
            if sourceName not in seen:
                seen.add(sourceName)
                orderedSources.append(sourceName)

        for observation in rawObservations:
            for featureName in observation.sensorValues.keys():
                if is_recency_feature_name(featureName):
                    sourceName = get_source_entity_name_from_recency_feature(featureName)
                    if sourceName and sourceName not in seen:
                        seen.add(sourceName)
                        orderedSources.append(sourceName)
                    continue
                if featureName not in seen:
                    seen.add(featureName)
                    orderedSources.append(featureName)

        return orderedSources

    def _createAnalysisModel(self) -> Any:
        paramsForThisModel = self._allParams.get(self._modelType, {})
        if self._modelType == "KNN":
            return KNNClassifier(params=paramsForThisModel)
        return RandomForest(params=paramsForThisModel)

    def _getUniqueChronologicalObservations(self, observations: List[ModelObservation]) -> List[ModelObservation]:
        ordered = sorted(observations, key=lambda observation: observation.time)
        unique: List[ModelObservation] = []
        seenTimes = set()
        for observation in ordered:
            if observation.time in seenTimes:
                continue
            seenTimes.add(observation.time)
            unique.append(observation)
        return unique

    def _groupObservationRuns(self, observations: List[ModelObservation]) -> List[List[ModelObservation]]:
        if not observations:
            return []

        runs: List[List[ModelObservation]] = [[observations[0]]]
        for observation in observations[1:]:
            if observation.label == runs[-1][-1].label:
                runs[-1].append(observation)
            else:
                runs.append([observation])
        return runs

    def _buildEvaluationConfusions(self, labels: List[str], matrix: List[List[int]]) -> List[Dict[str, Any]]:
        confusionEntries: List[Dict[str, Any]] = []
        for rowIndex, actualLabel in enumerate(labels):
            if rowIndex >= len(matrix) or not isinstance(matrix[rowIndex], list):
                continue
            row = matrix[rowIndex]
            for colIndex, predictedLabel in enumerate(labels):
                if rowIndex == colIndex or colIndex >= len(row):
                    continue
                count = int(row[colIndex] or 0)
                if count <= 0:
                    continue
                confusionEntries.append({
                    "actual": str(actualLabel),
                    "predicted": str(predictedLabel),
                    "count": count,
                })
        confusionEntries.sort(key=lambda item: (-item["count"], item["actual"], item["predicted"]))
        return confusionEntries

    def _buildSequenceEvaluation(self, observations: List[ModelObservation]) -> Dict[str, Any]:
        uniqueObservations = self._getUniqueChronologicalObservations(observations)
        runs = self._groupObservationRuns(uniqueObservations)
        if len(uniqueObservations) < 4 or len(runs) < 2:
            return {}

        totalObservations = len(uniqueObservations)
        targetTrainSize = max(1, int(totalObservations * (1.0 - ANALYSIS_SEQUENCE_TEST_SHARE)))
        trainRuns: List[List[ModelObservation]] = []
        trainSize = 0

        for index, run in enumerate(runs):
            remainingRuns = len(runs) - index - 1
            if trainRuns and trainSize >= targetTrainSize and remainingRuns >= 1:
                break
            if remainingRuns == 0:
                break
            trainRuns.append(run)
            trainSize += len(run)

        if not trainRuns or len(trainRuns) == len(runs):
            return {}

        trainObservations = [observation for run in trainRuns for observation in run]
        testObservations = [observation for run in runs[len(trainRuns):] for observation in run]
        if not trainObservations or not testObservations:
            return {}

        candidateModel = self._createAnalysisModel()
        candidateModel.populateDataframe(trainObservations)

        predictedLabels: List[str] = []
        actualLabels: List[str] = []
        for observation in testObservations:
            predicted, _confidence = candidateModel.predictLabel(observation.sensorValues)
            predictedLabels.append(str(predicted) if predicted is not None else "__unpredicted__")
            actualLabels.append(observation.label)

        labelOrder = sorted(set(actualLabels) | set(predictedLabels))
        matrix = confusion_matrix(actualLabels, predictedLabels, labels=labelOrder).astype(int).tolist()
        topConfusions = self._buildEvaluationConfusions(labelOrder, matrix)

        stationaryTotal = 0
        stationaryCorrect = 0
        transitionCount = 0
        settledTransitionCount = 0
        immediateTransitionCount = 0
        missedTransitionCount = 0
        lagSteps: List[int] = []
        lagSeconds: List[float] = []

        for index in range(1, len(testObservations)):
            current = testObservations[index]
            previous = testObservations[index - 1]
            if current.label == previous.label:
                stationaryTotal += 1
                if predictedLabels[index] == current.label:
                    stationaryCorrect += 1
                continue

            transitionCount += 1
            newLabel = current.label
            previousLabel = previous.label
            runEnd = index
            while runEnd + 1 < len(testObservations) and testObservations[runEnd + 1].label == newLabel:
                runEnd += 1

            matchedIndex: Optional[int] = None
            for candidateIndex in range(index, runEnd + 1):
                if predictedLabels[candidateIndex] == newLabel:
                    matchedIndex = candidateIndex
                    break

            if matchedIndex is None:
                missedTransitionCount += 1
            else:
                settledTransitionCount += 1
                lagSteps.append(matchedIndex - index)
                lagSeconds.append(float(testObservations[matchedIndex].time - current.time))
                if matchedIndex == index:
                    immediateTransitionCount += 1

        return {
            "method": "chronological_run_split",
            "train_observations": len(trainObservations),
            "test_observations": len(testObservations),
            "train_runs": len(trainRuns),
            "test_runs": len(runs) - len(trainRuns),
            "accuracy": round(float(accuracy_score(actualLabels, predictedLabels)), 4),
            "macro_f1": round(float(f1_score(actualLabels, predictedLabels, average="macro", zero_division=0)), 4),
            "stationary_accuracy": round((stationaryCorrect / stationaryTotal), 4) if stationaryTotal else None,
            "stationary_samples": stationaryTotal,
            "transition_count": transitionCount,
            "settled_transition_count": settledTransitionCount,
            "missed_transition_count": missedTransitionCount,
            "immediate_transition_rate": round((immediateTransitionCount / transitionCount), 4) if transitionCount else None,
            "delayed_transition_rate": round(((transitionCount - immediateTransitionCount) / transitionCount), 4) if transitionCount else None,
            "average_transition_lag_steps": round((sum(lagSteps) / len(lagSteps)), 2) if lagSteps else None,
            "average_transition_lag_seconds": round((sum(lagSeconds) / len(lagSeconds)), 2) if lagSeconds else None,
            "confusion_matrix": {
                "labels": labelOrder,
                "matrix": matrix,
            },
            "top_confusions": topConfusions[:8],
        }

    def _buildAnalysisRecommendations(
        self,
        totalObservations: int,
        labels: List[str],
        underrepresentedLabels: List[Dict[str, Any]],
        emptyLabels: List[Dict[str, Any]],
        imbalanceRatio: Optional[float],
        weakLabels: List[Dict[str, Any]],
        sourceStats: List[Dict[str, Any]],
        topFeatures: List[Dict[str, Any]],
        confusionEntries: List[Dict[str, Any]],
        sequenceEvaluation: Dict[str, Any],
    ) -> List[Dict[str, str]]:
        recommendations: List[Dict[str, str]] = []

        recommendedMinimum = max(ANALYSIS_MIN_LABEL_SAMPLES * max(len(labels), 1), ANALYSIS_MIN_LABEL_SAMPLES)
        if totalObservations < recommendedMinimum:
            recommendations.append({
                "severity": "high",
                "code": "dataset_small",
                "title": "Collect more training samples overall",
                "detail": f"This model has {totalObservations} derived observations. Aim for at least {recommendedMinimum} so each label has enough examples.",
            })

        if emptyLabels:
            labelNames = ", ".join(item["label"] for item in emptyLabels[:4])
            recommendations.append({
                "severity": "high",
                "code": "empty_labels",
                "title": "Some labels have no training data",
                "detail": f"Add observations for {labelNames} before relying on predictions for those labels.",
            })

        if underrepresentedLabels:
            weakest = underrepresentedLabels[0]
            recommendations.append({
                "severity": "medium",
                "code": "label_needs_more_samples",
                "title": "Add more samples for weak labels",
                "detail": f"{weakest['label']} only has {weakest['count']} observations. Low-count labels are usually the first place the model struggles.",
            })

        if imbalanceRatio and imbalanceRatio >= 3:
            recommendations.append({
                "severity": "medium",
                "code": "label_imbalance",
                "title": "Balance the label distribution",
                "detail": f"The largest label has {imbalanceRatio:.1f}x more samples than the smallest non-empty label. Try collecting more examples for the smaller classes.",
            })

        if weakLabels:
            weakest = weakLabels[0]
            recommendations.append({
                "severity": "medium",
                "code": "weak_label_metrics",
                "title": "Review low-performing labels",
                "detail": f"{weakest['label']} has recall {weakest['recall']:.2f} and F1 {weakest['f1']:.2f}. Add more representative samples around that room's transitions and edge cases.",
            })

        stationaryAccuracy = sequenceEvaluation.get("stationary_accuracy")
        if stationaryAccuracy is not None and stationaryAccuracy < 0.9:
            recommendations.append({
                "severity": "high",
                "code": "stationary_errors",
                "title": "The model is weak even while staying in one room",
                "detail": f"Chronological evaluation only gets {stationaryAccuracy * 100:.0f}% of steady-state samples right. Add more stationary examples in the confused rooms before tuning settings.",
            })

        delayedTransitionRate = sequenceEvaluation.get("delayed_transition_rate")
        if delayedTransitionRate is not None and delayedTransitionRate > 0.25:
            lagSeconds = sequenceEvaluation.get("average_transition_lag_seconds")
            lagText = f" Average lag is about {lagSeconds:.1f}s." if lagSeconds is not None else ""
            recommendations.append({
                "severity": "medium",
                "code": "transition_lag",
                "title": "Room transitions are too sticky",
                "detail": f"{delayedTransitionRate * 100:.0f}% of room changes do not switch immediately in chronological evaluation.{lagText} Add transition samples and review stale-reading handling.",
            })

        staleImportantSource = next(
            (
                source
                for source in sourceStats
                if source["stale_rate"] >= 0.3
                and source["combined_importance"] > 0
                and not source["room_specific"]
            ),
            None,
        )
        if staleImportantSource is not None:
            recommendations.append({
                "severity": "medium",
                "code": "stale_sensor_signal",
                "title": "A useful source is stale too often",
                "detail": f"{staleImportantSource['source']} is stale in {staleImportantSource['stale_rate'] * 100:.0f}% of raw snapshots without being strongly tied to one room. Faster updates or better placement could help this model.",
            })

        if confusionEntries:
            topConfusion = confusionEntries[0]
            recommendations.append({
                "severity": "info",
                "code": "top_confusion_pair",
                "title": "Collect more boundary samples for the most confused rooms",
                "detail": f"The biggest holdout confusion is {topConfusion['actual']} -> {topConfusion['predicted']} ({topConfusion['count']} times). Add samples during transitions and edge positions between those rooms.",
            })

        if topFeatures:
            strongestFeature = topFeatures[0]
            if strongestFeature["kind"] == "recency":
                recommendations.append({
                    "severity": "info",
                    "code": "recency_matters",
                    "title": "Recency is a major part of the model",
                    "detail": f"{strongestFeature['source']} recency is currently the strongest signal. Keep an eye on sensor freshness and stale values when evaluating predictions.",
                })

        if not recommendations:
            recommendations.append({
                "severity": "info",
                "code": "analysis_healthy",
                "title": "No obvious weak spots detected",
                "detail": "Coverage, label metrics, and source freshness look broadly healthy from the current stored data.",
            })

        return recommendations[:6]

    def getAnalysisSummary(self) -> Dict[str, Any]:
        rawObservations = self.getRawObservations()
        observations = self.getObservations()
        labelCounts = self.getObservationCountsByLabel()
        totalObservations = sum(labelCounts.values())
        labels = sorted(set(self.getLabels()) | set(labelCounts.keys()))
        labelShareMap = {
            label: ((labelCounts.get(label, 0) / totalObservations) if totalObservations else 0.0)
            for label in labels
        }
        sequenceEvaluation = self._buildSequenceEvaluation(observations)
        accuracy = self.getAccuracy()
        labelStats = self.getLabelStats() or {}
        confusionMatrix = self._model.getConfusionMatrix() or {"labels": [], "matrix": []}
        featureImportance = self._model.getFeatureImportance() or {}
        sourceNames = self._getAnalysisSources(rawObservations)

        labelCoverage: List[Dict[str, Any]] = []
        underrepresentedLabels: List[Dict[str, Any]] = []
        emptyLabels: List[Dict[str, Any]] = []
        nonZeroCounts: List[int] = []

        for label in labels:
            count = int(labelCounts.get(label, 0))
            share = (count / totalObservations) if totalObservations else 0.0
            entry = {
                "label": label,
                "count": count,
                "share": round(share, 4),
                "target_minimum": ANALYSIS_MIN_LABEL_SAMPLES,
                "needs_more": count < ANALYSIS_MIN_LABEL_SAMPLES,
            }
            labelCoverage.append(entry)
            if count == 0:
                emptyLabels.append(entry)
            elif count < ANALYSIS_MIN_LABEL_SAMPLES:
                underrepresentedLabels.append(entry)
                nonZeroCounts.append(count)
            else:
                nonZeroCounts.append(count)

        imbalanceRatio: Optional[float] = None
        if len(nonZeroCounts) >= 2 and min(nonZeroCounts) > 0:
            imbalanceRatio = round(max(nonZeroCounts) / min(nonZeroCounts), 3)

        labelQuality: List[Dict[str, Any]] = []
        for label in labels:
            stats = labelStats.get(label, {})
            labelQuality.append({
                "label": label,
                "count": int(labelCounts.get(label, 0)),
                "support": int(stats.get("support", 0)),
                "precision": float(stats.get("precision", 0.0)),
                "recall": float(stats.get("recall", 0.0)),
                "f1": float(stats.get("f1", 0.0)),
            })

        weakLabels = [
            label
            for label in sorted(labelQuality, key=lambda item: (item["f1"], item["recall"], item["count"], item["label"]))
            if label["count"] > 0 and (label["f1"] < 0.6 or label["recall"] < 0.6)
        ]

        confusionEntries: List[Dict[str, Any]] = []
        matrixLabels = confusionMatrix.get("labels", []) if isinstance(confusionMatrix, dict) else []
        matrixRows = confusionMatrix.get("matrix", []) if isinstance(confusionMatrix, dict) else []
        for rowIndex, rowLabel in enumerate(matrixLabels):
            if rowIndex >= len(matrixRows) or not isinstance(matrixRows[rowIndex], list):
                continue
            row = matrixRows[rowIndex]
            for colIndex, predictedLabel in enumerate(matrixLabels):
                if rowIndex == colIndex or colIndex >= len(row):
                    continue
                count = int(row[colIndex] or 0)
                if count <= 0:
                    continue
                confusionEntries.append({
                    "actual": str(rowLabel),
                    "predicted": str(predictedLabel),
                    "count": count,
                })
        confusionEntries.sort(key=lambda item: (-item["count"], item["actual"], item["predicted"]))

        topFeatures: List[Dict[str, Any]] = []
        for featureName, importance in sorted(featureImportance.items(), key=lambda item: item[1], reverse=True):
            normalizedImportance = float(importance)
            if normalizedImportance <= 0:
                continue
            sourceName = get_source_entity_name_from_recency_feature(featureName) if is_recency_feature_name(featureName) else featureName
            topFeatures.append({
                "name": featureName,
                "source": sourceName,
                "kind": "recency" if is_recency_feature_name(featureName) else "value",
                "importance": round(normalizedImportance, 4),
            })
        topFeatures = topFeatures[:10]

        sourceStats: List[Dict[str, Any]] = []
        for sourceName in sourceNames:
            recencyFeatureName = build_recency_feature_name(sourceName)
            rawValues: List[Any] = []
            ages: List[float] = []
            missingCount = 0
            staleCount = 0
            presentByLabel: Counter[str] = Counter()

            for observation in rawObservations:
                value = observation.sensorValues.get(sourceName)
                rawValues.append(value)
                if value is None:
                    missingCount += 1
                else:
                    presentByLabel[observation.label] += 1

                rawAge = observation.sensorValues.get(recencyFeatureName)
                try:
                    ageSeconds = max(0.0, float(rawAge))
                    ages.append(ageSeconds)
                    if ageSeconds >= ANALYSIS_STALE_AGE_SECONDS:
                        staleCount += 1
                except (TypeError, ValueError):
                    pass

            snapshotCount = len(rawObservations)
            presentCount = snapshotCount - missingCount
            missingRate = (missingCount / snapshotCount) if snapshotCount else 0.0
            staleRate = (staleCount / snapshotCount) if snapshotCount else 0.0
            dominantLabel = None
            dominantLabelShare = 0.0
            dominantLabelLift = None
            if presentByLabel:
                dominantLabel, dominantCount = presentByLabel.most_common(1)[0]
                dominantLabelShare = dominantCount / max(presentCount, 1)
                baselineShare = labelShareMap.get(dominantLabel, 0.0)
                if baselineShare > 0:
                    dominantLabelLift = dominantLabelShare / baselineShare
            sourceStats.append({
                "source": sourceName,
                "snapshots": snapshotCount,
                "present_count": presentCount,
                "present_rate": round((presentCount / snapshotCount), 4) if snapshotCount else 0.0,
                "missing_rate": round(missingRate, 4),
                "avg_age_seconds": round(sum(ages) / len(ages), 1) if ages else None,
                "max_age_seconds": round(max(ages), 1) if ages else None,
                "stale_rate": round(staleRate, 4),
                "dominant_label": dominantLabel,
                "dominant_label_share": round(dominantLabelShare, 4) if dominantLabel is not None else None,
                "dominant_label_lift": round(dominantLabelLift, 2) if dominantLabelLift is not None else None,
                "room_specific": dominantLabel is not None and presentCount >= ANALYSIS_MIN_LABEL_SAMPLES and (dominantLabelLift or 0.0) >= 2.0,
                "value_importance": round(float(featureImportance.get(sourceName, 0.0)), 4),
                "recency_importance": round(float(featureImportance.get(recencyFeatureName, 0.0)), 4),
                "combined_importance": round(
                    float(featureImportance.get(sourceName, 0.0)) + float(featureImportance.get(recencyFeatureName, 0.0)),
                    4,
                ),
            })

        sourceStats.sort(key=lambda item: (-item["combined_importance"], -item["stale_rate"], item["source"]))

        recommendationConfusions = sequenceEvaluation.get("top_confusions") or confusionEntries

        recommendations = self._buildAnalysisRecommendations(
            totalObservations=totalObservations,
            labels=labels,
            underrepresentedLabels=underrepresentedLabels,
            emptyLabels=emptyLabels,
            imbalanceRatio=imbalanceRatio,
            weakLabels=weakLabels,
            sourceStats=sourceStats,
            topFeatures=topFeatures,
            confusionEntries=recommendationConfusions,
            sequenceEvaluation=sequenceEvaluation,
        )

        return {
            "overview": {
                "observation_count": len(observations),
                "raw_observation_count": len(rawObservations),
                "source_count": len(sourceNames),
                "label_count": len(labels),
                "learning_type": self.getLearningType(),
                "model_type": self.getModelType(),
                "accuracy": round(float(accuracy), 4) if accuracy is not None else None,
                "feature_importance_available": bool(featureImportance),
            },
            "coverage": {
                "labels": labelCoverage,
                "total_observations": totalObservations,
                "underrepresented_labels": underrepresentedLabels,
                "empty_labels": emptyLabels,
                "imbalance_ratio": imbalanceRatio,
                "recommended_min_per_label": ANALYSIS_MIN_LABEL_SAMPLES,
            },
            "quality": {
                "labels": sorted(labelQuality, key=lambda item: (item["f1"], item["recall"], item["label"])),
                "weak_labels": weakLabels[:5],
                "confusion_matrix": {
                    "labels": [str(label) for label in matrixLabels],
                    "matrix": matrixRows,
                },
                "top_confusions": confusionEntries[:8],
                "sequence_evaluation": sequenceEvaluation,
            },
            "features": {
                "top": topFeatures,
                "top_recency": [feature for feature in topFeatures if feature["kind"] == "recency"][:5],
                "top_value": [feature for feature in topFeatures if feature["kind"] == "value"][:5],
            },
            "sources": {
                "by_source": sourceStats,
                "stale_threshold_seconds": ANALYSIS_STALE_AGE_SECONDS,
            },
            "recommendations": recommendations,
        }

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

    def _getEntityAliases(self) -> Dict[str, str]:
        binding = self.getModelBinding()
        if not binding:
            return {}
        aliases = binding.get("entity_aliases", {})
        if isinstance(aliases, dict):
            return {str(k): str(v) for k, v in aliases.items() if str(k).strip() and str(v).strip()}
        return {}

    def _applyEntityAliases(self, entityMap: Dict[str, Any], aliases: Dict[str, str]) -> Dict[str, Any]:
        if not aliases:
            return entityMap
        remapped: Dict[str, Any] = {}
        for key, value in entityMap.items():
            remapped[aliases.get(key, key)] = value
        return remapped

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

        aliases = binding.get("entity_aliases", {})
        newSources = self._getCanonicalSourceIds(binding, aliases)
        previousSources = self._getCanonicalSourceIds(previousBinding, aliases) if previousBinding else []
        learnedSources = [
            entity.name
            for entity in self._modelstore.getEntityKeys()
            if not is_recency_feature_name(entity.name)
        ]

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

    def _getCanonicalSourceIds(self, binding: Optional[Dict[str, Any]], aliases: Dict[str, str]) -> List[str]:
        raw_ids = self._getBindingSourceIds(binding)
        if not aliases:
            return raw_ids
        return [aliases.get(eid, eid) for eid in raw_ids]

    def _normalizeBinding(self, binding: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if binding is None:
            return None
        if not isinstance(binding, dict):
            raise ValueError("Binding payload must be a JSON object")

        entity_aliases = binding.get("entity_aliases", {})
        if isinstance(entity_aliases, dict):
            entity_aliases = {str(k): str(v) for k, v in entity_aliases.items() if str(k).strip() and str(v).strip()}
        else:
            entity_aliases = {}

        normalized = {
            "version": int(binding.get("version", 1)),
            "sources": self._normalizeBindingSources(binding.get("sources", [])),
            "trainer": self._normalizeBindingEntity(binding.get("trainer")),
            "outputs": self._normalizeBindingOutputs(binding.get("outputs", {})),
            "adapter": binding.get("adapter", {}) if isinstance(binding.get("adapter", {}), dict) else {},
            "entity_aliases": entity_aliases,
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

        learnedSources = [
            entity.name
            for entity in self._modelstore.getEntityKeys()
            if not is_recency_feature_name(entity.name)
        ]
        if learnedSources:
            return deepcopy(self._buildCompatibilityStatus(None, binding))

        storedStatus = binding.get("compatibility_status")
        if isinstance(storedStatus, dict):
            status = deepcopy(storedStatus)
            status["updated_at"] = time.time()
            return status

        return deepcopy(self._buildCompatibilityStatus(None, binding))

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
