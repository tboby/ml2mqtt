from flask import Blueprint, render_template, request, url_for, redirect, abort, Response, jsonify
from jinja2 import TemplateNotFound
from typing import Dict, Any, List, Optional
import json
import math
import logging
from ModelStore import ModelObservation, EntityKey
from classifiers.RandomForest import RandomForestParams
from classifiers.KNNClassifier import KNNParams
from utils.helpers import slugify
from postprocessors.PostprocessorFactory import PostprocessorFactory
from preprocessors.PreprocessorFactory import PreprocessorFactory
from ModelManager import ModelManager
from PreprocessorEvaluator import PreprocessorEvaluator
from datetime import timedelta, datetime

logger = logging.getLogger("ml2mqtt.routes.model")
model_bp = Blueprint('model', __name__)
API_VERSION = 1


def _parse_labels(raw_labels: Any) -> List[str]:
    labels: List[str] = []

    if isinstance(raw_labels, list):
        labels = raw_labels
    elif isinstance(raw_labels, str):
        raw_labels = raw_labels.strip()
        if raw_labels:
            try:
                parsed = json.loads(raw_labels)
                if isinstance(parsed, list):
                    labels = parsed
                else:
                    labels = [item.strip() for item in raw_labels.split(",") if item.strip()]
            except json.JSONDecodeError:
                labels = [item.strip() for item in raw_labels.split(",") if item.strip()]

    return sorted(set(str(label).strip() for label in labels if str(label).strip()))


def _coerce_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value in [None, "", []]:
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    raise ValueError("Expected an integer value")


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return bool(value)


def _coerce_learning_type(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    aliases = {
        "OFF": "DISABLED",
    }
    normalized = aliases.get(normalized, normalized)
    valid_values = {"DISABLED", "LAZY", "EAGER"}
    if normalized not in valid_values:
        raise ValueError("learning_type must be one of DISABLED, LAZY, or EAGER")
    return normalized


def _default_binding_entities(model_name: str) -> Dict[str, Any]:
    safe_slug = slugify(model_name or "model").replace("-", "_")
    prefix = f"ml2mqtt_{safe_slug}"
    return {
        "trainer": {
            "entity_id": f"select.{prefix}_trainer",
            "name": f"{model_name} Trainer",
        },
        "outputs": {
            "prediction": {
                "entity_id": f"sensor.{prefix}_prediction",
                "name": f"{model_name} Prediction",
            },
            "confidence": {
                "entity_id": f"sensor.{prefix}_confidence",
                "name": f"{model_name} Confidence",
            },
            "status": {
                "entity_id": f"sensor.{prefix}_bridge_status",
                "name": f"{model_name} Bridge Status",
            },
        },
    }


def _build_binding_payload(model_name: str, payload: Dict[str, Any], existing: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    binding: Dict[str, Any] = {}
    if isinstance(existing, dict):
        binding.update(existing)

    explicit_binding = payload.get("binding")
    if isinstance(explicit_binding, dict):
        binding.update(explicit_binding)

    if isinstance(payload.get("source_entities"), list):
        binding["sources"] = payload["source_entities"]

    if not binding:
        return None

    defaults = _default_binding_entities(model_name)
    binding.setdefault("trainer", defaults["trainer"])

    outputs = binding.get("outputs", {}) if isinstance(binding.get("outputs"), dict) else {}
    for output_name, output_value in defaults["outputs"].items():
        outputs.setdefault(output_name, output_value)
    binding["outputs"] = outputs

    adapter = binding.get("adapter", {}) if isinstance(binding.get("adapter"), dict) else {}
    adapter.setdefault("kind", payload.get("adapter_kind", "home_assistant"))
    binding["adapter"] = adapter

    return binding


def _serialize_model(model_name: str, model: Any, include_bridge: bool = False) -> Dict[str, Any]:
    payload = model.getModelDetail() if include_bridge else model.getModelSummary()
    payload["id"] = model_name
    payload["slug"] = slugify(model_name)
    payload["name"] = model.getName() or model_name
    return payload


def _canonical_model_id(model: Any, fallback: str = "") -> str:
    model_name = str(model.getName() or fallback or "").strip()
    return model_name.lower()


def _serialize_observation(observation: ModelObservation) -> Dict[str, Any]:
    return {
        "time": observation.time,
        "display_time": observation.display_time,
        "label": observation.label,
        "sensorValues": observation.sensorValues,
    }

def init_model_routes(model_manager: ModelManager):
    def createOrConfigureModel(
        modelName: str,
        mqttTopic: str,
        labels: List[str],
        inputCount: int,
        defaultValue: Any,
        binding: Optional[Dict[str, Any]] = None,
    ):
        newModel = model_manager.addModel(modelName)
        newModel.setMqttTopic(mqttTopic)
        newModel.setName(modelName)
        newModel.setModelConfig("labels", sorted(list(set(labels))))
        newModel.setModelConfig("input_count", inputCount)
        if binding is not None:
            newModel.setModelBinding(binding)
        newModel.addPreprocessor("type_caster", { 'sensor': [{"SELECT_ALL": True }]})
        newModel.addPreprocessor("null_handler", { 'sensor': [{"SELECT_ALL": True }], 'replacementType': 'float', 'nullReplacement': defaultValue})
        newModel.addPostprocessor("only_diff", {})
        newModel.setLearningType("EAGER")
        newModel.subscribeToMqttTopics()
        return newModel

    def getInputCount(rawInputCount: Any, sourceCount: int) -> int:
        parsedInputCount = _coerce_int(rawInputCount, None)
        if parsedInputCount is None:
            return sourceCount if sourceCount > 0 else 1
        if sourceCount > 0 and parsedInputCount != sourceCount:
            raise ValueError("input_count must match the number of selected source entities")
        return parsedInputCount

    @model_bp.route("/")
    def home() -> str:
        models = []
        modelMap = model_manager.getModels()
        for model in modelMap:
            modelName = modelMap[model].getName()
            models.append({
                "name": modelName,
                "mqtt_topic": modelMap[model].getMqttTopic()
            })
        return render_template("home.html", title="Home", active_page="home", models=models, mqtt_connected=model_manager._mqttClient._connected)

    @model_bp.route("/check-model-name")
    def checkModel() -> str:
        model_name = request.args.get("name", "").strip().lower()
        slug = slugify(model_name)
        is_taken = model_manager.modelExists(slug)
        return json.dumps({"exists": is_taken})

    @model_bp.route("/create-model", methods=["GET", "POST"])
    def createModel() -> Response:
        if request.method == "POST":
            modelName = request.form.get("model_name")
            defaultValue = request.form.get("default_value")
            mqttTopic = request.form.get("mqtt_topic")
            labels = _parse_labels(request.form.get("labels", "[]"))

            if modelName is None:
                abort(400, "Missing model name")
            modelName = modelName.strip()
            if not modelName:
                abort(400, "Missing model name")

            if mqttTopic is None or not mqttTopic.strip():
                mqttTopic = f"ml2mqtt/{slugify(modelName)}"

            inputCount = getInputCount(request.form.get("input_count"), 0)
            createOrConfigureModel(modelName, mqttTopic.strip(), labels, inputCount, defaultValue)

            return redirect(url_for("model.home"))

        return render_template("create-model.html", title="Add Model", active_page="create_model")

    @model_bp.route(f"/api/v{API_VERSION}/models", methods=["GET"])
    def apiListModels() -> Response:
        models = []
        for modelName, model in model_manager.getModels().items():
            models.append(_serialize_model(_canonical_model_id(model, modelName), model, include_bridge=False))
        return jsonify({"models": models, "version": API_VERSION})

    @model_bp.route(f"/api/v{API_VERSION}/models", methods=["POST"])
    def apiCreateModel() -> Response:
        try:
            data = request.get_json()
            if not isinstance(data, dict):
                return jsonify({"error": "Missing or invalid JSON payload"}), 400

            modelName = str(data.get("model_name") or "").strip()
            if not modelName:
                return jsonify({"error": "model_name is required"}), 400
            if model_manager.modelExists(modelName):
                return jsonify({"error": f"Model '{modelName}' already exists"}), 409

            mqttTopic = str(data.get("mqtt_topic") or f"ml2mqtt/{slugify(modelName)}").strip()
            labels = _parse_labels(data.get("labels", []))
            defaultValue = data.get("default_value", 9999)
            binding = _build_binding_payload(modelName, data)
            sourceCount = len(binding.get("sources", [])) if binding else 0
            inputCount = getInputCount(data.get("input_count"), sourceCount)

            model = createOrConfigureModel(modelName, mqttTopic, labels, inputCount, defaultValue, binding)
            return jsonify(_serialize_model(_canonical_model_id(model, modelName), model, include_bridge=True)), 201
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            logger.exception("Error creating model from adapter API")
            return jsonify({"error": str(e)}), 500

    @model_bp.route(f"/api/v{API_VERSION}/models/<string:modelName>", methods=["DELETE"])
    def apiDeleteModel(modelName: str) -> Response:
        if not model_manager.modelExists(modelName):
            return jsonify({"error": f"Model '{modelName}' not found"}), 404

        model = model_manager.getModel(modelName)
        canonicalModelId = _canonical_model_id(model, modelName)
        deletedName = model.getName() or modelName
        model_manager.removeModel(modelName)
        return jsonify({
            "deleted": True,
            "id": canonicalModelId,
            "name": deletedName,
        })

    @model_bp.route(f"/api/v{API_VERSION}/models/<string:modelName>", methods=["GET"])
    def apiGetModel(modelName: str) -> Response:
        if not model_manager.modelExists(modelName):
            return jsonify({"error": f"Model '{modelName}' not found"}), 404
        model = model_manager.getModel(modelName)
        return jsonify(_serialize_model(_canonical_model_id(model, modelName), model, include_bridge=True))

    @model_bp.route(f"/api/v{API_VERSION}/models/<string:modelName>/binding", methods=["GET"])
    def apiGetModelBinding(modelName: str) -> Response:
        if not model_manager.modelExists(modelName):
            return jsonify({"error": f"Model '{modelName}' not found"}), 404
        model = model_manager.getModel(modelName)
        canonicalModelId = _canonical_model_id(model, modelName)
        return jsonify({
            "id": canonicalModelId,
            "binding": model.getModelBinding(),
            "compatibility_status": model.getBindingStatus(),
            "bridge_status": model.getBridgeStatus(),
        })

    @model_bp.route(f"/api/v{API_VERSION}/models/<string:modelName>/binding", methods=["PUT"])
    def apiSetModelBinding(modelName: str) -> Response:
        if not model_manager.modelExists(modelName):
            return jsonify({"error": f"Model '{modelName}' not found"}), 404

        data = request.get_json()
        if not isinstance(data, dict):
            return jsonify({"error": "Missing or invalid JSON payload"}), 400

        model = model_manager.getModel(modelName)
        try:
            binding = _build_binding_payload(modelName, data, existing=model.getModelBinding())
            if binding is None:
                return jsonify({"error": "binding or source_entities is required"}), 400

            savedBinding = model.setModelBinding(binding)
            canonicalModelId = _canonical_model_id(model, modelName)
            return jsonify({
                "id": canonicalModelId,
                "binding": savedBinding,
                "compatibility_status": model.getBindingStatus(),
                "bridge_status": model.getBridgeStatus(),
            })
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

    @model_bp.route(f"/api/v{API_VERSION}/models/<string:modelName>/binding", methods=["DELETE"])
    def apiClearModelBinding(modelName: str) -> Response:
        if not model_manager.modelExists(modelName):
            return jsonify({"error": f"Model '{modelName}' not found"}), 404

        model = model_manager.getModel(modelName)
        model.clearModelBinding()
        canonicalModelId = _canonical_model_id(model, modelName)
        return jsonify({
            "id": canonicalModelId,
            "binding": None,
            "compatibility_status": model.getBindingStatus(),
            "bridge_status": model.getBridgeStatus(),
        })

    @model_bp.route(f"/api/v{API_VERSION}/models/<string:modelName>/bridge-status", methods=["GET"])
    def apiGetBridgeStatus(modelName: str) -> Response:
        if not model_manager.modelExists(modelName):
            return jsonify({"error": f"Model '{modelName}' not found"}), 404
        model = model_manager.getModel(modelName)
        return jsonify(model.getBridgeStatus())

    @model_bp.route(f"/api/v{API_VERSION}/models/<string:modelName>/learning-type", methods=["PUT"])
    def apiSetModelLearningType(modelName: str) -> Response:
        if not model_manager.modelExists(modelName):
            return jsonify({"error": f"Model '{modelName}' not found"}), 404

        data = request.get_json()
        if not isinstance(data, dict):
            return jsonify({"error": "Missing or invalid JSON payload"}), 400

        try:
            learningType = _coerce_learning_type(data.get("learning_type"))
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

        model = model_manager.getModel(modelName)
        model.setLearningType(learningType)
        return jsonify(_serialize_model(_canonical_model_id(model, modelName), model, include_bridge=True))

    @model_bp.route(f"/api/v{API_VERSION}/models/<string:modelName>/raw-observations", methods=["GET"])
    def apiListRawObservations(modelName: str) -> Response:
        if not model_manager.modelExists(modelName):
            return jsonify({"error": f"Model '{modelName}' not found"}), 404

        model = model_manager.getModel(modelName)
        observations = [_serialize_observation(observation) for observation in model.getRawObservations()]
        return jsonify({
            "observations": observations,
            "count": len(observations),
        })

    @model_bp.route(f"/api/v{API_VERSION}/models/<string:modelName>/raw-observations", methods=["DELETE"])
    def apiDeleteRawObservations(modelName: str) -> Response:
        if not model_manager.modelExists(modelName):
            return jsonify({"error": f"Model '{modelName}' not found"}), 404

        model = model_manager.getModel(modelName)
        model.deleteRawObservations()
        return jsonify({
            "raw_observation_count": model.getRawObservationCount(),
            "observation_count": model.getObservationCount(),
            "learning_type": model.getLearningType(),
        })

    @model_bp.route(f"/api/v{API_VERSION}/models/<string:modelName>/raw-observations/import", methods=["POST"])
    def apiImportRawObservations(modelName: str) -> Response:
        if not model_manager.modelExists(modelName):
            return jsonify({"error": f"Model '{modelName}' not found"}), 404

        data = request.get_json()
        if not isinstance(data, dict):
            return jsonify({"error": "Missing or invalid JSON payload"}), 400

        observations = data.get("observations")
        if not isinstance(observations, list):
            return jsonify({"error": "observations must be a list"}), 400

        model = model_manager.getModel(modelName)
        try:
            importedCount = model.importRawObservations(
                observations,
                replace_existing=_coerce_bool(data.get("replace_existing"), False),
            )

            rebuiltCount = 0
            if _coerce_bool(data.get("rebuild_after_import", data.get("replay_after_import")), False):
                rebuiltCount = model.replayRawObservations(
                    observations=None,
                    clear_training_data=True,
                    reset_processor_storage=True,
                )
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            logger.exception("Failed to import raw observations for model '%s'", modelName)
            return jsonify({"error": str(e)}), 500

        return jsonify({
            "imported": importedCount,
            "rebuilt": rebuiltCount,
            "observation_count": model.getObservationCount(),
            "raw_observation_count": model.getRawObservationCount(),
            "learning_type": model.getLearningType(),
        })

    @model_bp.route(f"/api/v{API_VERSION}/models/<string:modelName>/raw-observations/replay", methods=["POST"])
    def apiReplayRawObservations(modelName: str) -> Response:
        if not model_manager.modelExists(modelName):
            return jsonify({"error": f"Model '{modelName}' not found"}), 404

        data = request.get_json(silent=True)
        if data is None:
            data = {}
        if not isinstance(data, dict):
            return jsonify({"error": "Missing or invalid JSON payload"}), 400

        observations = data.get("observations")
        if observations is not None and not isinstance(observations, list):
            return jsonify({"error": "observations must be a list when provided"}), 400

        model = model_manager.getModel(modelName)
        try:
            replayedCount = model.replayRawObservations(
                observations=observations,
                clear_training_data=_coerce_bool(data.get("clear_training_data"), False),
                reset_processor_storage=_coerce_bool(data.get("reset_processor_storage"), True),
            )
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            logger.exception("Failed to replay raw observations for model '%s'", modelName)
            return jsonify({"error": str(e)}), 500

        return jsonify({
            "replayed": replayedCount,
            "observation_count": model.getObservationCount(),
            "raw_observation_count": model.getRawObservationCount(),
            "learning_type": model.getLearningType(),
        })

    @model_bp.route("/delete-model/<string:modelName>/", methods=["POST"])
    def deleteModel(modelName: str) -> Response:
        model_manager.removeModel(modelName)
        return redirect(url_for("model.home"))

    @model_bp.route("/edit-model/<string:modelName>/<section>")
    def editModel(modelName: str, section: str = "settings") -> str:
        validSections = ["settings", "entities", "observations", "preprocessors", "postprocessors", "mqtt", "nodered"]

        if section not in validSections:
            abort(404)

        class ViewModel:
            def __init__(self):
                self.name: str = modelName
                self.params: Dict[str, Any] = {}
                self.observations: List[ModelObservation] = []
                self.entities: List[EntityKey] = []
                self.labels: List[str] = []
                self.currentPage: int = 0
                self.totalPages: int = 0
                self.rawObservationCount: int = 0
                self.observationCount: int = 0
                self.learningType: str = "DISABLED"

        model = ViewModel()
        modelService = model_manager.getModel(modelName)
        model.rawObservationCount = modelService.getRawObservationCount()
        model.observationCount = modelService.getObservationCount()
        model.learningType = modelService.getLearningType()

        if section == "observations":
            page = int(request.args.get("page", 1))
            pageSize = 50
            allObservations = modelService.getObservations()
            total = len(allObservations)

            start = (page - 1) * pageSize
            end = start + pageSize
            paginated = allObservations[start:end]

            model.observations = paginated
            model.currentPage = page
            model.labels = modelService.getLabels()
            model.totalPages = math.ceil(total / pageSize)

        elif section == "settings":
            logger.info(f"Model settings: {model_manager.getModel(modelName).getModelSettings()}")
            model.params = { 
                "accuracy": model_manager.getModel(modelName).getAccuracy(),
                "observationCount": len(model_manager.getModel(modelName).getObservations()),
                "modelSize": model_manager.getModel(modelName).getModelSize(),
                "modelParameters": model_manager.getModel(modelName).getModelSettings(),
                "labelStats": model_manager.getModel(modelName).getLabelStats(),
                "learningType": model_manager.getModel(modelName).getLearningType(),
            }
        elif section == "postprocessors":
            logger.info(f"{list(map(lambda processor: processor.to_dict(),model_manager.getModel(modelName).getPostprocessors()))}")
            model.postprocessors = map(lambda processor: processor.to_dict(),model_manager.getModel(modelName).getPostprocessors())
        elif section == "preprocessors":
            logger.info(f"{list(map(lambda processor: processor.to_dict(),model_manager.getModel(modelName).getPreprocessors()))}")
            recentObservations = model_manager.getModel(modelName).getMostRecentMqttObservations()
            model.recentMqtt = None if len(recentObservations) == 0 else recentObservations[-1]
            evaluator = PreprocessorEvaluator(model_manager.getModel(modelName).getPreprocessors())
            model.preprocessors = evaluator.evaluate(recentObservations)
            if len(model.preprocessors) == 0:
                model.lastSensors = model.recentMqtt
            else:
                model.lastSensors = model.preprocessors[-1]['produces']

        elif section == "entities":
            model.entities = model_manager.getModel(modelName).getEntityKeys()
        elif section == "mqtt":
            model.params = {
                "mqttTopic": model_manager.getModel(modelName).getMqttTopic(),
            }
        elif section == "nodered":
            model.params = {
                "noderedConfig": model_manager.getModel(modelName).generateNodeRed()
            }

        sectionTemplate = f"edit_model/{section}.html"
        return render_template(
            "edit_model.html",
            title=f"Edit Model: {model.name}",
            activePage=None,
            activeSection=section,
            model=model,
            sectionTemplate=sectionTemplate,
            availablePostprocessors=PostprocessorFactory().get_available_postprocessors(),
            availablePreprocessors=PreprocessorFactory().get_available_preprocessors(),
        )

    @model_bp.route("/edit-model/<string:modelName>/change-model", methods=["POST"])
    def changeModel(modelName: str) -> str:
        try:
            modelType = request.form.get("modelType", "RandomForest")
            currentSettings = model_manager.getModel(modelName).getModelSettings()
            currentSettings["model_type"] = modelType
            model_manager.getModel(modelName).setModelSettings(currentSettings)
            return jsonify(success=True)
        except Exception as e:
            return jsonify(success=False, error=str(e)), 400
    
    @model_bp.route("/model/<modelName>/changeLearning", methods=["POST"])
    def changeLearning(modelName):
        try:
            learningType = _coerce_learning_type(request.form.get("learningType")) # 'DISABLED', 'LAZY', 'EAGER'
        except ValueError as e:
            return jsonify(success=False, error=str(e)), 400
        logger.info(f"Changing learning type for model '{modelName}' to {learningType}")
        model_manager.getModel(modelName).setLearningType(learningType)

        return jsonify(success=True)

    @model_bp.route("/edit-model/<string:modelName>/settings/update", methods=["POST"])
    def updateModelSettings(modelName: str) -> str:
        def get_int(name: str, default: Optional[int] = None) -> Optional[int]:
            val = request.form.get(name)
            return int(val) if val and val.isdigit() else default

        def get_optional_int(name: str) -> Optional[int]:
            val = request.form.get(name)
            return int(val) if val and val.isdigit() else None

        def get_bool(name: str) -> bool:
            return request.form.get(name) in ["true", "on", "1"]

        def get_str_or_none(name: str) -> Optional[str]:
            val = request.form.get(name)
            return val if val not in ["None", "", None] else None

        try:
            modelType = request.form.get("modelType", "RandomForest")

            settings: Dict[str, Any] = model_manager.getModel(modelName).getModelSettings()

            if modelType == "RandomForest":
                rfParams: RandomForestParams = {
                    "n_estimators": get_int("nEstimators", 100),
                    "max_depth": get_optional_int("maxDepth"),
                    "min_samples_split": get_int("minSamplesSplit", 2),
                    "min_samples_leaf": get_int("minSamplesLeaf", 1),
                    "max_features": get_str_or_none("maxFeatures"),
                    "class_weight": get_str_or_none("classWeight"),
                    "bootstrap": get_bool("bootstrap"),
                    "oob_score": get_bool("oobScore"),
                }
                settings["model_parameters"]["RandomForest"] = rfParams

            elif modelType == "KNN":
                knnParams: KNNParams = {
                    "n_neighbors": get_int("nNeighbors", 5),
                    "weights": request.form.get("weights", "uniform"),
                    "metric": request.form.get("metric", "minkowski"),
                }
                settings["model_parameters"]["KNN"] = knnParams

            else:
                return jsonify(success=False, error=f"Unknown model type '{modelType}'"), 400

            model_manager.getModel(modelName).setModelSettings(settings)
            return jsonify(success=True)

        except Exception as e:
            return jsonify(success=False, error=str(e)), 400

    @model_bp.route("/edit-model/<string:modelName>/settings/autotune", methods=["POST"])
    def autoTuneModel(modelName: str) -> str:
        model_manager.getModel(modelName).optimizeParameters()
        return json.dumps({"success": True})

    @model_bp.route("/api/model/<string:modelName>/observation/<float:observationTime>/delete", methods=["POST"])
    def apiDeleteObservation(modelName: str, observationTime: float) -> str:
        try:
            model_manager.getModel(modelName).deleteObservation(observationTime)
            return jsonify({"success": True})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        
    @model_bp.route("/api/model/<string:modelName>/observations/delete", methods=["POST"])
    def apiDeleteObservations(modelName: str) -> str:
        try:
            data = request.get_json()
            scope = data.get("scope")

            if not scope:
                return jsonify({"error": "Scope parameter is required"}), 400

            now = datetime.utcnow()

            if scope == "all":
                timestamp = 0  # Effectively deletes all observations
            elif scope == "hour":
                timestamp = (now - timedelta(hours=1)).timestamp()
            elif scope == "day":
                timestamp = (now - timedelta(days=1)).timestamp()
            elif scope == "week":
                timestamp = (now - timedelta(weeks=1)).timestamp()
            else:
                return jsonify({"error": "Invalid scope parameter"}), 400

            model_manager.getModel(modelName).deleteObservationsSince(timestamp)
            return jsonify({"success": True})
            
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    @model_bp.route("/edit-model/<string:modelName>/model-settings/<string:modelType>")
    def getModelSettingsTemplate(modelName: str, modelType: str) -> str:
        if modelType not in ["RandomForest", "KNN"]:
            abort(404)
        model = ViewModel(modelName)
        settings = model_manager.getModel(modelName).getModelSettings()
        
        # Initialize default parameters if they don't exist
        if "model_parameters" not in settings:
            settings["model_parameters"] = {}
        
        if modelType == "KNN":
            if "KNN" not in settings["model_parameters"]:
                settings["model_parameters"]["KNN"] = {
                    "n_neighbors": 5,
                    "weights": "uniform",
                    "metric": "minkowski"
                }
        
        model.params = {
            "modelParameters": {
                "model_type": modelType,
                "model_parameters": settings["model_parameters"]
            }
        }
        return render_template(f"edit_model/partials/model_settings/{modelType.lower()}.html", model=model)

    @model_bp.route("/api/model/<string:modelName>/entity/<string:entityName>/delete", methods=["POST"])
    def deleteEntity(modelName: str, entityName: str) -> Response:
        try:
            model_manager.getModel(modelName).deleteEntity(entityName)
            return jsonify({"success": True})
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            return jsonify({"error": "Internal server error"}), 500

    @model_bp.route("/delete-label/<string:modelName>/<string:label>", methods=["POST"])
    def deleteLabel(modelName: str, label: str) -> Response:
        try:
            model_manager.getModel(modelName).deleteObservationsByLabel(label)
            return redirect(url_for("model.editModel", modelName=modelName, section="settings"))
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @model_bp.route("/edit-model/<string:modelName>/postprocessor/add", methods=["POST"])
    def addPostprocessor(modelName: str) -> Response:
        try:
            data = request.get_json()
            if data is None:
                return jsonify({"error": "Missing or invalid JSON payload"}), 400
            logger.info(f"Adding postprocessor: {data}")
            model_manager.getModel(modelName).addPostprocessor(data['type'], data['params'])
            return jsonify({"success": True})
        except ValueError as e:
            logger.error(f"Error adding postprocessor: {e}")
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            logger.error(f"Error adding postprocessor: {e}")
            return jsonify({"error": "Internal server error"}), 500

    @model_bp.route("/edit-model/<string:modelName>/preprocessor/add", methods=["POST"])
    def addPreprocessor(modelName: str) -> Response:
        try:
            data = request.get_json()
            if data is None:
                return jsonify({"error": "Missing or invalid JSON payload"}), 400
            logger.info(f"Adding preprocessor: {data}")
            model_manager.getModel(modelName).addPreprocessor(data['type'], data['params'])
            return jsonify({"success": True})
        except ValueError as e:
            logger.error(f"Error adding preprocessor: {e}")
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            logger.error(f"Error adding preprocessor: {e}")
            return jsonify({"error": "Internal server error"}), 500

    @model_bp.route("/edit-model/<string:modelName>/postprocessor/delete", methods=["POST"])
    def deletePostprocessor(modelName: str) -> Response:
        try:
            data = request.get_json()
            if data is None or "index" not in data:
                return jsonify({"error": "Missing index in payload"}), 400
                
            model_manager.getModel(modelName).removePostprocessor(data["index"])
            return jsonify({"success": True})
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            return jsonify({"error": "Internal server error"}), 500

    @model_bp.route("/edit-model/<string:modelName>/preprocessor/delete", methods=["POST"])
    def deletePreprocessor(modelName: str) -> Response:
        try:
            data = request.get_json()
            if not data or "index" not in data:
                return jsonify({"success": False, "error": "Missing 'index' in payload"}), 400
            
            index = data["index"]

            if not isinstance(index, int):
                return jsonify({"success": False, "error": "'index' must be an integer"}), 400

            model = model_manager.getModel(modelName)
            if not model:
                return jsonify({"success": False, "error": f"Model '{modelName}' not found"}), 404

            try:
                model.removePreprocessor(index)
            except IndexError:
                return jsonify({"success": False, "error": f"Index '{index}' out of range"}), 400
            except ValueError as e:
                return jsonify({"success": False, "error": str(e)}), 400

            return jsonify({"success": True})

        except Exception as e:
            app.logger.exception(f"Error deleting preprocessor for model '{modelName}': {e}")
            return jsonify({"success": False, "error": "Internal server error"}), 500

    @model_bp.route("/edit-model/<string:modelName>/postprocessor/reorder", methods=["POST"])
    def reorderPostprocessors(modelName: str) -> Response:
        try:
            data = request.get_json()
            if data is None or "fromIndex" not in data or "toIndex" not in data:
                return jsonify({"error": "Missing fromIndex or toIndex in payload"}), 400
                
            model_manager.getModel(modelName).reorderPostprocessors(data["fromIndex"], data["toIndex"])
            return jsonify({"success": True})
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            return jsonify({"error": "Internal server error"}), 500

    @model_bp.route("/edit-model/<string:modelName>/preprocessor/reorder", methods=["POST"])
    def reorderPreprocessors(modelName: str) -> Response:
        try:
            data = request.get_json()
            if data is None or "fromIndex" not in data or "toIndex" not in data:
                return jsonify({"error": "Missing fromIndex or toIndex in payload"}), 400
                    
            model_manager.getModel(modelName).reorderPreprocessors(data["fromIndex"], data["toIndex"])
            return jsonify({"success": True})
        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            return jsonify({"error": "Internal server error"}), 500

    @model_bp.route("/render_preprocessor/<string:preprocessor_type>", methods=["POST"])
    def render_preprocessor(preprocessor_type):
        data = request.get_json()
        sensors = data.get("sensors", [])

        try:
            return render_template(f"preprocessors/{preprocessor_type}.html", sensors=sensors)
        except TemplateNotFound:
            return jsonify({"error": "Template not found"}), 404

    @model_bp.route("/render_postprocessor/<string:postprocessor_type>", methods=["POST"])
    def render_postprocessor(postprocessor_type):
        # In the future, we might want to pass data to the template,
        # similar to how 'sensors' are passed to preprocessor templates.
        # For now, an empty context is sufficient.
        # data = request.get_json()
        # context = data.get("context", {}) 
        try:
            return render_template(f"postprocessors/{postprocessor_type}.html") #, **context)
        except TemplateNotFound:
            return jsonify({"error": "Template not found"}), 404

    @model_bp.route("/mqtt_history/<string:modelName>", methods=["GET"])
    def render_mqtt(modelName: str) -> Response:
        return jsonify(model_manager.getModel(modelName).getRecentMqtt())
    
    @model_bp.route("/mqtt_topic/<string:modelName>", methods=["PUT"])
    def set_mqtt_base_topic(modelName: str) -> Response:
        try:
            data = request.get_json()
            if not data or "mqttTopic" not in data:
                return jsonify({"error": "mqttTopic parameter is required"}), 400

            mqttTopic = data["mqttTopic"]

            # Update the model's MQTT topic
            model = model_manager.getModel(modelName)
            if not model:
                return jsonify({"error": f"Model '{modelName}' not found"}), 404

            model.setMqttTopic(mqttTopic)

            return jsonify({"success": True, "mqttTopic": mqttTopic})

        except Exception as e:
            logger.exception(f"Error setting MQTT topic for model '{modelName}': {e}")
            return jsonify({"error": str(e)}), 500
    
    return model_bp 
