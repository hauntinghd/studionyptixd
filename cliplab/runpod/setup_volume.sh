#!/usr/bin/env bash
# Initialize ClipLab paths on the shared RunPod network volume.
set -euo pipefail

ROOT="${STUDIO_APP_DATA_DIR:-}"
if [ -z "${ROOT}" ]; then
  if [ -d "/runpod-volume/studio" ]; then
    ROOT="/runpod-volume/studio"
  elif [ -d "/workspace/studio" ]; then
    ROOT="/workspace/studio"
  else
    ROOT="/runpod-volume/studio"
  fi
fi
export STUDIO_APP_DATA_DIR="${ROOT}"
CLIPLAB="${ROOT}/cliplab"

mkdir -p "${CLIPLAB}/datasets" "${CLIPLAB}/models/virality/v1" "${CLIPLAB}/models/reframe/v1" "${CLIPLAB}/exports"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cp "${SCRIPT_DIR}/model_registry.json" "${CLIPLAB}/models/model_registry.json"

echo "ClipLab volume ready at ${CLIPLAB}"
ls -la "${CLIPLAB}/models"
