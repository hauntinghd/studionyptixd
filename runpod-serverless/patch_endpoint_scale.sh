#!/usr/bin/env bash
# Patch the live RunPod Serverless endpoint to keep at least one warm worker,
# allow bursts up to 10, and stop killing warm workers after 5 seconds.
#
# What changes:
#   workersMin:   0  -> 1     (one worker always warm, avoids cold-boot on every first request)
#   workersMax:   3  -> 10    (burst capacity; we don't pay when not running)
#   idleTimeout:  5  -> 60    (keep warm workers around for a minute between requests)
#
# Why: diag showed 1684 completed / 0 failed but 342 in-queue with only 1 worker
# initializing. The handler works fine; the endpoint just can't scale fast enough
# because every request pays a 60-90s cold boot. Warm pool + burst cap solves it.
#
# Usage:
#   cd D:/Games/asd
#   bash runpod-serverless/patch_endpoint_scale.sh
#
# Requires RUNPOD_API_KEY and RUNPOD_ENDPOINT_ID to be exported (or in .env / .runpod.env).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

set -o allexport
[[ -f "${REPO_ROOT}/.env" ]] && source "${REPO_ROOT}/.env"
[[ -f "${SCRIPT_DIR}/.runpod.env" ]] && source "${SCRIPT_DIR}/.runpod.env"
set +o allexport

: "${RUNPOD_API_KEY:?RUNPOD_API_KEY required}"
: "${RUNPOD_ENDPOINT_ID:?RUNPOD_ENDPOINT_ID required (the studio-api-ada24 endpoint id)}"

echo "Patching endpoint ${RUNPOD_ENDPOINT_ID}..."
echo "  workersMin  0 -> 1"
echo "  workersMax  3 -> 10"
echo "  idleTimeout 5 -> 60"

PAYLOAD=$(cat <<EOF
{
  "query": "mutation SaveEndpoint(\$input: SaveEndpointInput!) { saveEndpoint(input: \$input) { id name workersMin workersMax idleTimeout } }",
  "variables": {
    "input": {
      "id": "${RUNPOD_ENDPOINT_ID}",
      "workersMin": 1,
      "workersMax": 10,
      "idleTimeout": 60
    }
  }
}
EOF
)

RESPONSE=$(curl -sS -X POST "https://api.runpod.io/graphql?api_key=${RUNPOD_API_KEY}" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD")

echo ""
echo "Response:"
echo "$RESPONSE"

if echo "$RESPONSE" | grep -q '"errors"'; then
  echo ""
  echo "✗ Patch FAILED — see errors above. The SaveEndpointInput schema may have changed,"
  echo "  or the endpoint may need additional fields passed. Check RunPod GraphQL docs."
  exit 1
fi

echo ""
echo "✓ Endpoint patched. Give it ~60-120s to spin up the new warm worker,"
echo "  then re-run the health curl to confirm idle>=1."
