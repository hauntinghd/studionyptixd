#!/usr/bin/env bash
# Deploy Studio backend as RunPod Serverless endpoint.
#
# Steps:
#   1. Build + push Docker image to Docker Hub (or GHCR)
#   2. Create/update RunPod serverless template via GraphQL
#      (template carries image name + env vars)
#   3. Create/update RunPod serverless endpoint (scales 0 -> N workers,
#      attaches network volume for persistent state).
#
# ─── Setup (one-time) ─────────────────────────────────────────
# Copy runpod-serverless/.runpod.env.example → .runpod.env and fill in:
#   RUNPOD_API_KEY        (runpod.io/console/user/settings)
#   DOCKER_REGISTRY       (e.g. docker.io/caseynyptid)
#   IMAGE_TAG             (e.g. studio-backend:v1.0)
#   NETWORK_VOLUME_ID     (create one in RunPod dashboard, copy the ID)
#   STUDIO_APP_DATA_DIR   (where inside the volume state lives, default /runpod-volume/studio)
#
# ─── Run ──────────────────────────────────────────────────────
# bash runpod-serverless/deploy.sh
#
# ─── Outputs ──────────────────────────────────────────────────
# RUNPOD_ENDPOINT_ID (saved to .runpod.env for the CF Worker step)
set -euo pipefail

# ─── Load env files ───────────────────────────────────────────
# Order: repo-root .env (shared backend secrets) → runpod-serverless/.runpod.env
# (RunPod-specific overrides + secrets Casey doesn't want in the regular .env).
# This way we don't duplicate PAYPAL_CLIENT_ID, SUPABASE_URL, etc. into two files.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

set -o allexport
[[ -f "${REPO_ROOT}/.env" ]] && source "${REPO_ROOT}/.env"
ENV_FILE="${SCRIPT_DIR}/.runpod.env"
[[ -f "$ENV_FILE" ]] && source "$ENV_FILE"
set +o allexport

: "${RUNPOD_API_KEY:?RUNPOD_API_KEY required (runpod.io/console/user/settings). Set in runpod-serverless/.runpod.env}"
: "${DOCKER_REGISTRY:?DOCKER_REGISTRY required (e.g. docker.io/caseynyptid). Set in runpod-serverless/.runpod.env}"
: "${IMAGE_TAG:=studio-backend:v1.0}"
: "${NETWORK_VOLUME_ID:=}"
: "${STUDIO_APP_DATA_DIR:=/runpod-volume/studio}"

FULL_IMAGE="${DOCKER_REGISTRY}/${IMAGE_TAG}"
TEMPLATE_NAME="studio-backend-serverless"
ENDPOINT_NAME="studio-backend"

echo "══════════════════════════════════════════════"
echo "  Studio → RunPod Serverless Deploy"
echo "══════════════════════════════════════════════"
echo "  Image:      $FULL_IMAGE"
echo "  Template:   $TEMPLATE_NAME"
echo "  Endpoint:   $ENDPOINT_NAME"
echo "  Volume ID:  ${NETWORK_VOLUME_ID:-<none — state will NOT persist>}"
echo "  AppData:    $STUDIO_APP_DATA_DIR"
echo ""

if [[ -z "$NETWORK_VOLUME_ID" ]]; then
  echo "⚠  WARNING: NETWORK_VOLUME_ID is empty. File-based state (PayPal orders,"
  echo "   topup wallets, longform sessions, projects) will be lost between"
  echo "   cold-starts. Highly recommend creating a volume first."
  echo ""
  read -rp "   Continue anyway? [y/N] " CONFIRM
  [[ "$CONFIRM" =~ ^[Yy] ]] || { echo "Aborted."; exit 1; }
fi

# ─── 1. Build + push ──────────────────────────────────────────
echo "→ Building Docker image..."
cd "${SCRIPT_DIR}/.."  # ASD repo root
docker build -f runpod-serverless/Dockerfile -t "$FULL_IMAGE" .

echo "→ Pushing to registry..."
docker push "$FULL_IMAGE"

# ─── 2. Build env vars list ───────────────────────────────────
# These get baked into the RunPod template. Each one is pulled from the current
# shell (loaded via .runpod.env above). Missing keys are skipped silently — that
# way the template will still run if a provider is disabled.
echo "→ Building template env vars..."

ENV_KEYS=(
  # Core infra — state persistence
  "APP_DATA_DIR:$STUDIO_APP_DATA_DIR"
  "SITE_URL"
  # Backend AI providers
  "XAI_API_KEY"
  "ELEVENLABS_API_KEY"
  "FAL_AI_KEY"
  "PIKZELS_API_KEY"
  "ALGROW_API_KEY"
  "COMFYUI_URL"
  "RUNWAYML_API_SECRET"
  # Supabase (auth + profiles + storage)
  "SUPABASE_URL"
  "SUPABASE_ANON_KEY"
  "SUPABASE_JWT_SECRET"
  "SUPABASE_SERVICE_KEY"
  # Google / YouTube
  "YOUTUBE_API_KEY"
  "YOUTUBE_API_KEYS"
  "GOOGLE_CLIENT_ID"
  "GOOGLE_CLIENT_SECRET"
  "GOOGLE_REDIRECT_URI"
  # Billing
  "STRIPE_SECRET_KEY"
  "STRIPE_WEBHOOK_SECRET"
  "STRIPE_TOPUP_PUBLIC_ENABLED"
  "PAYPAL_CLIENT_ID"
  "PAYPAL_CLIENT_SECRET"
  "PAYPAL_ENV:live"
  "PAYPAL_WEBHOOK_ID"
  # Feature flags / model defaults
  "IMAGE_PROVIDER_ORDER:fal"
  "XAI_IMAGE_FALLBACK_ENABLED:true"
  "HIDREAM_ENABLED:false"
  "HIDREAM_EDIT_ENABLED:false"
  "SKELETON_REQUIRE_WAN22:false"
  "RUNPOD_IMAGE_FEEDBACK_ENABLED:false"
  "RUNPOD_COMPOSITOR_ENABLED:false"
  "CHATSTORY_FORCE_LOCAL_TTS:false"
  "CHATSTORY_ELEVENLABS_MODEL_ID:eleven_multilingual_v2"
  "FAL_IMAGE_BACKUP_MODEL:imagen4_fast"
  "XAI_IMAGE_MODEL:grok-imagine-image-pro"
  "XAI_VIDEO_MODEL:grok-imagine-video"
  "PIKZELS_THUMBNAIL_MODEL:pkz-3"
  "PIKZELS_RECREATE_MODEL:pkz-3"
  "PIKZELS_TITLE_MODEL:pkz-3"
  # RunPod internal (if the backend calls out to RunPod compositor)
  "RUNPOD_API_KEY"
  "RUNPOD_COMPOSITOR_ENDPOINT_ID"
)

ENV_JSON="["
FIRST=1
for ENTRY in "${ENV_KEYS[@]}"; do
  KEY="${ENTRY%%:*}"
  DEFAULT="${ENTRY#*:}"
  [[ "$DEFAULT" == "$ENTRY" ]] && DEFAULT=""   # no default
  VALUE="${!KEY:-$DEFAULT}"
  [[ -z "$VALUE" ]] && continue                # skip empty
  ESCAPED=$(python -c "import json,sys; print(json.dumps(sys.argv[1]))" "$VALUE")
  [[ $FIRST -eq 1 ]] && FIRST=0 || ENV_JSON+=","
  ENV_JSON+="{\"key\":\"${KEY}\",\"value\":${ESCAPED}}"
done
ENV_JSON+="]"
echo "  Env var count: $(grep -o '"key":' <<<"$ENV_JSON" | wc -l)"

# ─── 3. Upsert template ───────────────────────────────────────
echo "→ Upserting RunPod template..."
TEMPLATE_PAYLOAD=$(python -c "
import json, sys
name, image, env_str = sys.argv[1], sys.argv[2], sys.argv[3]
body = {
    'query': 'mutation(\$input: SaveTemplateInput!) { saveTemplate(input: \$input) { id name } }',
    'variables': {
        'input': {
            'name': name,
            'imageName': image,
            'dockerArgs': '',
            'ports': '',
            'volumeInGb': 0,
            'containerDiskInGb': 50,
            'env': json.loads(env_str),
        }
    }
}
print(json.dumps(body))
" "$TEMPLATE_NAME" "$FULL_IMAGE" "$ENV_JSON")

TEMPLATE_RESP=$(curl -s -X POST "https://api.runpod.io/graphql?api_key=${RUNPOD_API_KEY}" \
  -H "Content-Type: application/json" \
  -d "$TEMPLATE_PAYLOAD")

TEMPLATE_ID=$(python -c "
import json, sys
d = json.loads(sys.stdin.read())
if 'errors' in d:
    print('ERROR:', json.dumps(d['errors']), file=sys.stderr)
    sys.exit(2)
print(d['data']['saveTemplate']['id'])
" <<<"$TEMPLATE_RESP")
echo "  template id: $TEMPLATE_ID"

# ─── 4. Upsert endpoint ───────────────────────────────────────
echo "→ Upserting serverless endpoint..."
ENDPOINT_PAYLOAD=$(python -c "
import json, sys
name, tid, vid = sys.argv[1], sys.argv[2], sys.argv[3]
body = {
    'query': 'mutation(\$input: SaveEndpointInput!) { saveEndpoint(input: \$input) { id name } }',
    'variables': {
        'input': {
            'name': name,
            'templateId': tid,
            'gpuIds': 'CPU3C,CPU5C',
            'networkVolumeId': vid,
            'locations': '',
            'idleTimeout': 5,
            'scalerType': 'QUEUE_DELAY',
            'scalerValue': 4,
            'workersMin': 0,
            'workersMax': 3,
            'flashboot': True,
        }
    }
}
print(json.dumps(body))
" "$ENDPOINT_NAME" "$TEMPLATE_ID" "$NETWORK_VOLUME_ID")

ENDPOINT_RESP=$(curl -s -X POST "https://api.runpod.io/graphql?api_key=${RUNPOD_API_KEY}" \
  -H "Content-Type: application/json" \
  -d "$ENDPOINT_PAYLOAD")

ENDPOINT_ID=$(python -c "
import json, sys
d = json.loads(sys.stdin.read())
if 'errors' in d:
    print('ERROR:', json.dumps(d['errors']), file=sys.stderr)
    sys.exit(2)
print(d['data']['saveEndpoint']['id'])
" <<<"$ENDPOINT_RESP")

# Save endpoint ID back to .runpod.env so wrangler step can read it
if [[ -f "$ENV_FILE" ]] && ! grep -q "^RUNPOD_ENDPOINT_ID=" "$ENV_FILE"; then
  echo "RUNPOD_ENDPOINT_ID=${ENDPOINT_ID}" >> "$ENV_FILE"
  echo "  (appended RUNPOD_ENDPOINT_ID to .runpod.env)"
fi

echo ""
echo "══════════════════════════════════════════════"
echo "  ✓ DEPLOYED"
echo "══════════════════════════════════════════════"
echo "  Endpoint ID:  $ENDPOINT_ID"
echo "  Endpoint URL: https://api.runpod.ai/v2/${ENDPOINT_ID}/runsync"
echo ""
echo "Next steps:"
echo "  1. cd runpod-serverless"
echo "     wrangler login   (OAuth to the CF account on nyptidindustries.com)"
echo "     wrangler secret put RUNPOD_API_KEY"
echo "     wrangler secret put RUNPOD_ENDPOINT_ID   # paste: $ENDPOINT_ID"
echo "     wrangler deploy"
echo ""
echo "  2. Test: curl https://api.studio.nyptidindustries.com/api/health"
echo ""
echo "  3. Patch PayPal webhook URL:"
echo "     The webhook is registered at https://api.nyptidindustries.com/api/paypal/webhook"
echo "     Run: python runpod-serverless/patch_paypal_webhook.py"
echo ""
