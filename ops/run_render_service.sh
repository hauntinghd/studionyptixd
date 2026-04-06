#!/usr/bin/env sh
set -eu

APP_ROOT="${RENDER_APP_ROOT:-/app}"
if [ ! -f "$APP_ROOT/backend.py" ]; then
  APP_ROOT="/app"
fi

if [ -n "${RENDER_ENV_FILE_PATH:-}" ] && [ -f "${RENDER_ENV_FILE_PATH}" ]; then
  set -a
  # shellcheck disable=SC1090
  . "${RENDER_ENV_FILE_PATH}"
  set +a
fi

cd "$APP_ROOT"

if [ -z "${FRONTEND_DIST_DIR:-}" ] && [ -d "$APP_ROOT/ViralShorts-App/dist" ]; then
  export FRONTEND_DIST_DIR="$APP_ROOT/ViralShorts-App/dist"
fi

export PYTHONPATH="$APP_ROOT${PYTHONPATH:+:$PYTHONPATH}"

API_CMD="uvicorn backend:app --host 0.0.0.0 --port ${PORT:-10000}"
RUN_EMBEDDED_WORKER="${RUN_EMBEDDED_WORKER:-0}"

WORKER_PID=""
if [ "$RUN_EMBEDDED_WORKER" = "1" ] || [ "$RUN_EMBEDDED_WORKER" = "true" ] || [ "$RUN_EMBEDDED_WORKER" = "yes" ] || [ "$RUN_EMBEDDED_WORKER" = "on" ]; then
  echo "Starting embedded queue worker"
  python -u backend_worker.py &
  WORKER_PID="$!"
fi

cleanup() {
  if [ -n "$WORKER_PID" ]; then
    kill "$WORKER_PID" 2>/dev/null || true
    wait "$WORKER_PID" 2>/dev/null || true
  fi
  if [ -n "${API_PID:-}" ]; then
    kill "$API_PID" 2>/dev/null || true
    wait "$API_PID" 2>/dev/null || true
  fi
}

trap cleanup INT TERM EXIT

echo "Starting API service"
sh -c "$API_CMD" &
API_PID="$!"
wait "$API_PID"
