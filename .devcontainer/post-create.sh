#!/usr/bin/env bash
set -eu

VENV_DIR="${UV_PROJECT_ENVIRONMENT:-/home/vscode/.local/share/ml2mqtt-venv}"

if [ ! -x "${VENV_DIR}/bin/python" ]; then
  printf 'Expected prebuilt Python environment at %s. Rebuild the devcontainer.\n' "${VENV_DIR}" >&2
  exit 1
fi

mkdir -p data/models
