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

# RUN_EMBEDDED_WORKER is consumed by backend.py's FastAPI lifecycle. Starting
# backend_worker.py here as well creates two consumers that can recover and run
# the same in-flight billable job concurrently.
echo "Starting API service with in-process production consumer"
exec uvicorn backend:app --host 0.0.0.0 --port "${PORT:-10000}" --workers 1
