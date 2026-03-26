#!/usr/bin/env bash
set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="/tmp/ml2mqtt.pid"
LOG_FILE="/tmp/ml2mqtt.log"

is_ready() {
  python - <<'PY'
import sys
import urllib.request

try:
    with urllib.request.urlopen("http://127.0.0.1:5000/api/v1/models", timeout=1):
        pass
except Exception:
    sys.exit(1)

sys.exit(0)
PY
}

if [ -f "${PID_FILE}" ]; then
  pid="$(cat "${PID_FILE}" 2>/dev/null || true)"
  if [ -n "${pid}" ] && kill -0 "${pid}" 2>/dev/null; then
    exit 0
  fi
  rm -f "${PID_FILE}"
fi

if is_ready; then
  exit 0
fi

nohup bash "${SCRIPT_DIR}/start-ml2mqtt.sh" >"${LOG_FILE}" 2>&1 </dev/null &
pid="$!"
printf '%s\n' "${pid}" >"${PID_FILE}"

for _ in $(seq 1 30); do
  if is_ready; then
    exit 0
  fi

  if ! kill -0 "${pid}" 2>/dev/null; then
    printf 'ML2MQTT exited during startup.\n' >&2
    cat "${LOG_FILE}" >&2
    exit 1
  fi

  sleep 1
done

printf 'ML2MQTT did not become ready within 30 seconds.\n' >&2
cat "${LOG_FILE}" >&2
exit 1
