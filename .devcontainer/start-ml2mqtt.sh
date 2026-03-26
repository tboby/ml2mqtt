#!/usr/bin/env bash
set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${UV_PROJECT_ENVIRONMENT:-/home/vscode/.local/share/ml2mqtt-venv}"

export MQTT_SERVER="${MQTT_SERVER:-mosquitto}"
export MQTT_PORT="${MQTT_PORT:-1883}"
export MQTT_USERNAME="${MQTT_USERNAME:-}"
export MQTT_PASSWORD="${MQTT_PASSWORD:-}"
export ML2MQTT_DATA_DIR="${ML2MQTT_DATA_DIR:-${REPO_ROOT}/data}"
export ML2MQTT_ENABLE_INGRESS="${ML2MQTT_ENABLE_INGRESS:-false}"
export ML2MQTT_HOST="${ML2MQTT_HOST:-0.0.0.0}"
export ML2MQTT_PORT="${ML2MQTT_PORT:-5000}"
export ML2MQTT_DEBUG="${ML2MQTT_DEBUG:-false}"

if [ ! -x "${VENV_DIR}/bin/python" ]; then
  printf 'Python environment not found at %s. Rebuild the devcontainer.\n' "${VENV_DIR}" >&2
  exit 1
fi

mkdir -p "${ML2MQTT_DATA_DIR}/models"

cd "${REPO_ROOT}/ml2mqtt"

if [ "${ML2MQTT_DEBUG}" = "1" ] || [ "${ML2MQTT_DEBUG}" = "true" ] || [ "${ML2MQTT_DEBUG}" = "yes" ]; then
  exec "${VENV_DIR}/bin/python" -m flask --app app run --debug --host "${ML2MQTT_HOST}" --port "${ML2MQTT_PORT}"
fi

exec "${VENV_DIR}/bin/python" app.py
