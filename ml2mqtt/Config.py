import json
import os
from pathlib import Path
from typing import Any, Dict, Optional


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


class Config:
    def __init__(self):
        self._isHomeAssistant = False
        self._configSource = ""
        self.config = {}
        self._load()

    def _load(self):
        explicit_config = os.getenv("ML2MQTT_CONFIG_FILE")
        options_path = Path("/data/options.json")
        settings_path = Path("settings.json")

        if explicit_config:
            config_path = Path(explicit_config)
            if not config_path.exists():
                raise FileNotFoundError(f"Configured ML2MQTT_CONFIG_FILE was not found: {config_path}")
            self.config = self._load_json(config_path)
            self._configSource = str(config_path)
        elif self._has_env_mqtt_config():
            self.config = self._build_env_config()
            self._configSource = "environment"
        elif options_path.exists():
            self._isHomeAssistant = True
            self.config = self._build_home_assistant_config(self._load_json(options_path))
            self._configSource = str(options_path)
        elif settings_path.exists():
            self.config = self._load_json(settings_path)
            self._configSource = str(settings_path)
        else:
            raise FileNotFoundError(
                "No configuration found. Provide ML2MQTT_CONFIG_FILE, MQTT_SERVER, /data/options.json, or settings.json"
            )

        self._apply_defaults()

    def _load_json(self, path: Path) -> Dict[str, Any]:
        with path.open("r", encoding="utf-8") as file:
            return json.load(file)

    def _has_env_mqtt_config(self) -> bool:
        return os.getenv("MQTT_SERVER") is not None

    def _build_env_config(self) -> Dict[str, Any]:
        return {
            "mqtt": {
                "server": os.getenv("MQTT_SERVER", "localhost"),
                "port": int(os.getenv("MQTT_PORT", "1883")),
                "username": os.getenv("MQTT_USERNAME", ""),
                "password": os.getenv("MQTT_PASSWORD", ""),
            },
            "app": {
                "data_path": os.getenv("ML2MQTT_DATA_DIR", "data"),
                "enable_ingress": _env_bool("ML2MQTT_ENABLE_INGRESS", False),
                "host": os.getenv("ML2MQTT_HOST", "0.0.0.0"),
                "port": int(os.getenv("ML2MQTT_PORT", "5000")),
            },
        }

    def _build_home_assistant_config(self, options: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "mqtt": {
                "server": options.get("mqtt-server", "core-mosquitto"),
                "port": options.get("mqtt-port", 1883),
                "username": options.get("mqtt-username", "mqtt"),
                "password": options.get("mqtt-password", "mqtt"),
            },
            "app": {
                "data_path": "/data",
                "enable_ingress": True,
                "host": "0.0.0.0",
                "port": 5000,
            },
        }

    def _apply_defaults(self) -> None:
        mqtt = self.config.setdefault("mqtt", {})
        mqtt["server"] = os.getenv("MQTT_SERVER", mqtt.get("server", "localhost"))
        mqtt["port"] = int(os.getenv("MQTT_PORT", str(mqtt.get("port", 1883))))
        mqtt["username"] = os.getenv("MQTT_USERNAME", mqtt.get("username", ""))
        mqtt["password"] = os.getenv("MQTT_PASSWORD", mqtt.get("password", ""))

        app = self.config.setdefault("app", {})
        default_data_path = "/data" if self._isHomeAssistant else "data"
        app["data_path"] = os.getenv(
            "ML2MQTT_DATA_DIR",
            app.get("data_path", self.config.get("data_path", default_data_path)),
        )
        app["host"] = os.getenv("ML2MQTT_HOST", str(app.get("host", "0.0.0.0")))
        app["port"] = int(os.getenv("ML2MQTT_PORT", str(app.get("port", 5000))))
        app["enable_ingress"] = _env_bool(
            "ML2MQTT_ENABLE_INGRESS",
            bool(app.get("enable_ingress", self._isHomeAssistant)),
        )

    def getValue(self, keyName: str, valueName: Optional[str] = None) -> Any:
        if valueName is None:
            return self.config.get(keyName, {})
        else:
            return self.config.get(keyName, {}).get(valueName)
        
    def isHomeAssistant(self) -> bool:
        return self._isHomeAssistant

    def isIngressEnabled(self) -> bool:
        return bool(self.config.get("app", {}).get("enable_ingress", False))

    def getHost(self) -> str:
        return str(self.config.get("app", {}).get("host", "0.0.0.0"))

    def getPort(self) -> int:
        return int(self.config.get("app", {}).get("port", 5000))

    def getConfigSource(self) -> str:
        return self._configSource
    
    def getDataPath(self) -> str:
        app_config = self.config.get("app", {})
        return str(app_config.get("data_path") or self.config.get("data_path") or ".")

