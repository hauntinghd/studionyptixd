#!/usr/bin/env bash
# Bulk-import Studio secrets to Fly.io.
#
# Usage:
#   1. Copy each value from Render dashboard (or your local secret store) and
#      replace <PASTE_*_HERE> below.
#   2. Run:  bash ops/fly_migrate_secrets.sh
#   3. Fly will trigger a single deploy after all secrets are set (we use
#      `--stage` to defer + `flyctl deploy` at the end).
#
# Note: Once secrets are set on Fly they DO NOT need to live in this script.
# Empty <PASTE_*> strings are SKIPPED (so partial migrations are safe).
set -eu

APP="${APP:-nyptid-studio}"
FLYCTL="${FLYCTL:-/c/Users/casey/.fly/bin/flyctl.exe}"

# ---- Paste real values here, then re-run. Empty lines are skipped. ----
declare -A SECRETS=(
  [XAI_API_KEY]="<PASTE_XAI_API_KEY_HERE>"
  [ALGROW_API_KEY]="<PASTE_ALGROW_API_KEY_HERE>"
  [YOUTUBE_API_KEY]="<PASTE_YOUTUBE_API_KEY_HERE>"
  [GOOGLE_CLIENT_ID]="<PASTE_GOOGLE_CLIENT_ID_HERE>"
  [GOOGLE_CLIENT_SECRET]="<PASTE_GOOGLE_CLIENT_SECRET_HERE>"
  [GOOGLE_REDIRECT_URI]="<PASTE_GOOGLE_REDIRECT_URI_HERE>"
  [ELEVENLABS_API_KEY]="<PASTE_ELEVENLABS_API_KEY_HERE>"
  [FAL_AI_KEY]="<PASTE_FAL_AI_KEY_HERE>"
  [FAL_AI_KEY_2]="<PASTE_FAL_AI_KEY_2_HERE>"
  [FAL_AI_KEY_3]="<PASTE_FAL_AI_KEY_3_HERE>"
  [FAL_AI_KEY_4]="<PASTE_FAL_AI_KEY_4_HERE>"
  [FAL_AI_KEY_5]="<PASTE_FAL_AI_KEY_5_HERE>"
  [FAL_AI_KEY_6]="<PASTE_FAL_AI_KEY_6_HERE>"
  [PIKZELS_API_KEY]="<PASTE_PIKZELS_API_KEY_HERE>"
  [SUPABASE_URL]="<PASTE_SUPABASE_URL_HERE>"
  [SUPABASE_ANON_KEY]="<PASTE_SUPABASE_ANON_KEY_HERE>"
  [SUPABASE_JWT_SECRET]="<PASTE_SUPABASE_JWT_SECRET_HERE>"
  [SUPABASE_SERVICE_KEY]="<PASTE_SUPABASE_SERVICE_KEY_HERE>"
  [STRIPE_SECRET_KEY]="<PASTE_STRIPE_SECRET_KEY_HERE>"
  [STRIPE_WEBHOOK_SECRET]="<PASTE_STRIPE_WEBHOOK_SECRET_HERE>"
  [PAYPAL_CLIENT_ID]="<PASTE_PAYPAL_CLIENT_ID_HERE>"
  [PAYPAL_CLIENT_SECRET]="<PASTE_PAYPAL_CLIENT_SECRET_HERE>"
  [PAYPAL_WEBHOOK_ID]="<PASTE_PAYPAL_WEBHOOK_ID_HERE>"
  [COMFYUI_URL]="<PASTE_COMFYUI_URL_HERE>"
  # Redis URL is set automatically by `fly redis create` or by Upstash setup.
  # Uncomment if you're providing your own Redis:
  # [REDIS_URL]="<PASTE_REDIS_URL_HERE>"
)

# ---- Set them in one batched call (single deploy at the end). ----
args=()
for k in "${!SECRETS[@]}"; do
  v="${SECRETS[$k]}"
  if [[ -z "$v" || "$v" == "<PASTE_"*"_HERE>" ]]; then
    echo "[skip] $k (empty / placeholder)"
    continue
  fi
  args+=("$k=$v")
done

if [[ ${#args[@]} -eq 0 ]]; then
  echo "No real secrets provided. Edit the script and rerun."
  exit 1
fi

echo "[fly] setting ${#args[@]} secret(s) on $APP (staged — deploy will trigger)"
"$FLYCTL" secrets set --app "$APP" "${args[@]}"
echo "[fly] done. Run \`flyctl deploy --app $APP\` to apply."
