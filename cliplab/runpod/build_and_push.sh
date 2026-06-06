#!/usr/bin/env bash
# Build and push ClipLab inference image (run from repo root).
set -euo pipefail

REGISTRY="${DOCKER_REGISTRY:?set DOCKER_REGISTRY e.g. ghcr.io/yourorg}"
TAG="${CLIPLAB_IMAGE_TAG:-cliplab-inference:v1.0}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

docker build -f "${SCRIPT_DIR}/Dockerfile" -t "${REGISTRY}/${TAG}" "${REPO_ROOT}"
docker push "${REGISTRY}/${TAG}"

echo "Pushed ${REGISTRY}/${TAG}"
echo "Next: cd runpod-serverless && python upsert_cliplab_endpoint.py"
