#!/usr/bin/env bash
set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="/tmp/ml2mqtt.pid"
LOG_FILE="/tmp/ml2mqtt.log"

cleanup_pid_file() {
  rm -f "${PID_FILE}"
}

stop_tracked_process() {
  pid="$1"
  if [ -z "${pid}" ]; then
    return 0
  fi

  if ! kill -0 "${pid}" 2>/dev/null; then
    return 0
  fi

  kill "${pid}" 2>/dev/null || true
  for _ in $(seq 1 10); do
    if ! kill -0 "${pid}" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done

  kill -9 "${pid}" 2>/dev/null || true
}

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
    if is_ready; then
      exit 0
    fi

    printf 'ML2MQTT process %s is alive but not ready; restarting.\n' "${pid}" >&2
    stop_tracked_process "${pid}"
  fi
  cleanup_pid_file
fi

if is_ready; then
  cleanup_pid_file
  exit 0
fi

rm -f "${LOG_FILE}.previous"
if [ -f "${LOG_FILE}" ]; then
  mv "${LOG_FILE}" "${LOG_FILE}.previous"
fi

nohup bash "${SCRIPT_DIR}/start-ml2mqtt.sh" >"${LOG_FILE}" 2>&1 </dev/null &
pid="$!"
printf '%s\n' "${pid}" >"${PID_FILE}"

for _ in $(seq 1 30); do
  if is_ready; then
    cleanup_pid_file
    printf '%s\n' "${pid}" >"${PID_FILE}"
    exit 0
  fi

  if ! kill -0 "${pid}" 2>/dev/null; then
    printf 'ML2MQTT exited during startup.\n' >&2
    cleanup_pid_file
    cat "${LOG_FILE}" >&2
    exit 1
  fi

  sleep 1
done

printf 'ML2MQTT did not become ready within 30 seconds.\n' >&2
cleanup_pid_file
cat "${LOG_FILE}" >&2
exit 1
