#!/usr/bin/env bash

# Guard the SHARED Caddy edge that fronts Studio's public origin.
#
# Why this exists: the Studio API container can be perfectly healthy while
# Studio is completely unreachable. TLS termination belongs to a Caddy owned by
# a DIFFERENT product's compose project (ClipLab), and that product's deploy
# used to ship its own single-site Caddyfile over the shared multi-tenant one.
# When it did, Studio's site block vanished, `docker compose up -d` reloaded
# Caddy without it, and Cloudflare answered 525 — while every container still
# reported healthy. watchdog.sh could not see that, because it only inspects
# the API container and returns early when the container is fine.
#
# This guard checks the edge the way a customer does (through Cloudflare) and
# repairs the two things that can break it: a missing Studio site block and a
# missing Worker-to-origin token. It only ever APPENDS Studio's own block; it
# never edits or removes another product's site.
#
# Safe to run every minute. Idempotent. Exits non-zero only when the edge is
# broken in a way it could not repair.

set -Eeuo pipefail
# shellcheck source=ops/contabo/lib.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/lib.sh"

require_commands docker curl flock grep install

RELEASE_DIR_SELF="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)"
# Resolve the site template whether we are running from inside a release
# checkout or from a standalone install directory (e.g. shared/edge) that has no
# ops/ tree above it. Falling back to the active release keeps a bare
# `bash edge_guard.sh` working without the caller exporting anything.
SITE_TEMPLATE="${STUDIO_SITE_TEMPLATE:-}"
if [[ -z "${SITE_TEMPLATE}" ]]; then
  for candidate in \
    "${RELEASE_DIR_SELF}/ops/contabo/Caddyfile.studio" \
    "${STUDIO_INSTALL_ROOT}/current/ops/contabo/Caddyfile.studio"
  do
    if [[ -f "${candidate}" ]]; then
      SITE_TEMPLATE="${candidate}"
      break
    fi
  done
fi
SHARED_CADDYFILE="${STUDIO_SHARED_CADDYFILE:-/opt/cliplab/deploy/Caddyfile}"
CADDY_PROJECT_DIR="${STUDIO_CADDY_PROJECT_DIR:-$(dirname -- "${SHARED_CADDYFILE}")}"
CADDY_CONTAINER="${STUDIO_CADDY_CONTAINER:-cliplab-caddy}"
CADDY_IMAGE="${STUDIO_CADDY_IMAGE:-caddy:2-alpine}"
PUBLIC_HEALTH_URL="${STUDIO_PUBLIC_HEALTH_URL:-https://api-studio.nyptidindustries.com/api/health}"
CADDY_ENV_FILE="${STUDIO_CADDY_ENV_FILE:-${STUDIO_SHARED_DIR}/caddy.env}"
EDGE_DIR="${STUDIO_SHARED_DIR}/edge"
GUARD_LOCK="${EDGE_DIR}/edge-guard.lock"
ALERT_STATE="${EDGE_DIR}/last-alert"
ALERT_WINDOW_SEC="${STUDIO_EDGE_ALERT_WINDOW_SEC:-1800}"
DRY_RUN=0
[[ "${1:-}" != "--check-only" ]] || DRY_RUN=1

install -d -m 700 -- "${EDGE_DIR}"

# Never compete with a deploy, rollback, or another guard tick.
exec 8>"${GUARD_LOCK}"
flock -n 8 || exit 0

[[ -f "${SITE_TEMPLATE}" ]] || die "edge guard cannot find the Studio site template: ${SITE_TEMPLATE}"
[[ -f "${SHARED_CADDYFILE}" ]] || die "edge guard cannot find the shared Caddyfile: ${SHARED_CADDYFILE}"

# The origin hostname is whatever the release template declares — never hardcoded,
# so a site rename stays a single-file change.
ORIGIN_HOST="$(grep -m1 -oE '^[A-Za-z0-9][A-Za-z0-9.:-]*[[:space:]]*\{' -- "${SITE_TEMPLATE}" | sed -e 's/[[:space:]]*{$//')"
[[ -n "${ORIGIN_HOST}" ]] || die "edge guard could not read the site address from ${SITE_TEMPLATE}"

# ---------------------------------------------------------------- alerting ---
# curl, not studio_alerts.py: the host python has no httpx (the app runs in a
# container), and an alerter that needs a dependency is an alerter that fails
# exactly when it is needed.
alert() {
  local kind="$1" title="$2" description="$3" color icon now last
  [[ -n "${STUDIO_ERROR_WEBHOOK_URL:-}" ]] || return 0
  case "${kind}" in
    error) color=15158332; icon="🔴" ;;
    warn) color=15965202; icon="🟡" ;;
    success) color=3066993; icon="🟢" ;;
    *) color=3447003; icon="🔵" ;;
  esac
  # Dedup so a persistent condition does not post once a minute forever.
  now="$(date +%s)"
  if [[ "${kind}" != "success" && -f "${ALERT_STATE}" ]]; then
    last="$(cat -- "${ALERT_STATE}" 2>/dev/null || echo 0)"
    (( now - last >= ALERT_WINDOW_SEC )) || return 0
  fi
  printf '%s' "${now}" >"${ALERT_STATE}" 2>/dev/null || true
  local payload
  payload="$(
    ORIGIN_HOST="${ORIGIN_HOST}" TITLE="${icon} ${title}" DESC="${description}" \
    COLOR="${color}" HOSTN="$(uname -n)" python3 -c '
import json, os
print(json.dumps({
    "username": "Studio Alerts",
    "embeds": [{
        "title": os.environ["TITLE"][:256],
        "description": os.environ["DESC"][:2000],
        "color": int(os.environ["COLOR"]),
        "fields": [
            {"name": "component", "value": "edge_guard", "inline": True},
            {"name": "origin", "value": os.environ["ORIGIN_HOST"], "inline": True},
        ],
        "footer": {"text": os.environ["HOSTN"]},
    }],
}))'
  )" || return 0
  curl -fsS -m 15 -H 'Content-Type: application/json' \
    -X POST -d "${payload}" -- "${STUDIO_ERROR_WEBHOOK_URL}" >/dev/null 2>&1 || true
}

# Pull only the webhook out of the secret env; never echo it.
if [[ -z "${STUDIO_ERROR_WEBHOOK_URL:-}" && -f "${STUDIO_SHARED_DIR}/studio.env" ]]; then
  STUDIO_ERROR_WEBHOOK_URL="$(env_value "${STUDIO_SHARED_DIR}/studio.env" STUDIO_ERROR_WEBHOOK_URL 2>/dev/null || true)"
  export STUDIO_ERROR_WEBHOOK_URL
fi

# ----------------------------------------------------------------- probes ----
site_block_present() { grep -qF -- "${ORIGIN_HOST}" "${SHARED_CADDYFILE}"; }

token_present() {
  docker inspect "${CADDY_CONTAINER}" --format '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null |
    grep -qE '^STUDIO_ORIGIN_TOKEN=.+'
}

public_health_code() {
  curl -s -o /dev/null -w '%{http_code}' -m 25 -- "${PUBLIC_HEALTH_URL}" 2>/dev/null || echo 000
}

# ----------------------------------------------------------------- repair ----
repair_site_block() {
  local candidate="${EDGE_DIR}/Caddyfile.candidate" backup
  backup="${EDGE_DIR}/Caddyfile.pre-repair.$(date -u +%Y%m%dT%H%M%SZ)"

  # Preserve every other product's block verbatim; append ours.
  { cat -- "${SHARED_CADDYFILE}"; printf '\n'; cat -- "${SITE_TEMPLATE}"; } >"${candidate}"

  # Refuse to install a config Caddy itself rejects. The dummy token keeps the
  # real one out of process arguments; syntax validity does not depend on it.
  if ! docker run --rm \
    -e STUDIO_ORIGIN_TOKEN=validate-only-dummy-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
    -e STUDIO_ORIGIN_TOKEN_PREVIOUS= \
    -v "${candidate}":/etc/caddy/Caddyfile:ro "${CADDY_IMAGE}" \
    caddy validate --config /etc/caddy/Caddyfile --adapter caddyfile >/dev/null 2>&1; then
    rm -f -- "${candidate}"
    return 1
  fi

  cp -p -- "${SHARED_CADDYFILE}" "${backup}"
  install -m 644 -- "${candidate}" "${SHARED_CADDYFILE}"
  rm -f -- "${candidate}"
  cp -- "${SHARED_CADDYFILE}" "${EDGE_DIR}/Caddyfile.expected"
  info "Edge guard re-appended the ${ORIGIN_HOST} site block (backup: ${backup})"
}

ensure_token_override() {
  # Compose auto-loads docker-compose.override.yml. It is not part of the other
  # product's source tree, so `tar -xzf` during their deploy cannot remove it
  # (extraction only touches archived paths). That makes the token injection
  # survive deploys we do not control.
  local override="${CADDY_PROJECT_DIR}/docker-compose.override.yml"
  [[ -f "${CADDY_ENV_FILE}" ]] || return 1
  if [[ -f "${override}" ]] && grep -qF -- "${CADDY_ENV_FILE}" "${override}"; then
    return 0
  fi
  cat >"${override}" <<OVERRIDE
# NYPTID Studio edge integration - written by ops/contabo/edge_guard.sh.
# Gives the SHARED Caddy the Worker-to-origin token. The Studio API never sees it.
services:
  caddy:
    env_file:
      - ${CADDY_ENV_FILE}
OVERRIDE
  chmod 644 -- "${override}"
  info "Edge guard restored the Caddy origin-token override"
}

reload_caddy() {
  ( cd -- "${CADDY_PROJECT_DIR}" && docker compose up -d caddy >/dev/null 2>&1 )
}

# ------------------------------------------------------------------- main ----
repaired=()
block_ok=1; token_ok=1
site_block_present || block_ok=0
token_present || token_ok=0

if (( DRY_RUN )); then
  printf 'site_block=%s token=%s public_health=%s\n' \
    "$(( block_ok ? 1 : 0 ))" "$(( token_ok ? 1 : 0 ))" "$(public_health_code)"
  (( block_ok && token_ok )) || exit 1
  exit 0
fi

if (( ! block_ok )); then
  if repair_site_block; then repaired+=("site block"); else
    alert error "Studio edge: site block missing and unrepairable" \
      "The ${ORIGIN_HOST} block is absent from ${SHARED_CADDYFILE} and the rebuilt config failed \`caddy validate\`. Studio is serving Cloudflare 525 until this is fixed by hand."
    die "edge guard could not build a valid Caddyfile"
  fi
fi

if (( ! token_ok )); then
  if ensure_token_override; then repaired+=("origin token"); else
    alert error "Studio edge: origin token unavailable" \
      "${CADDY_CONTAINER} has no STUDIO_ORIGIN_TOKEN and ${CADDY_ENV_FILE} is missing, so every request would fail closed with direct_origin_forbidden."
    die "edge guard cannot restore the origin token"
  fi
fi

if (( ${#repaired[@]} > 0 )); then
  reload_caddy || true
  sleep 5
  code="$(public_health_code)"
  detail="Repaired: $(IFS=', '; echo "${repaired[*]}"). Public health now HTTP ${code}."
  cause="A deploy of another product sharing this Caddy most likely overwrote ${SHARED_CADDYFILE}."
  if [[ "${code}" == "200" ]]; then
    alert success "Studio edge auto-repaired" "${detail} ${cause}"
    info "Edge guard repaired the edge; public health is 200"
  else
    alert error "Studio edge repaired but still unhealthy" "${detail} ${cause}"
    die "edge guard repaired config but public health is ${code}"
  fi
  exit 0
fi

# Config is intact. Confirm customers can actually reach Studio.
code="$(public_health_code)"
if [[ "${code}" != "200" ]]; then
  alert error "Studio unreachable through Cloudflare" \
    "Public health returned HTTP ${code} while the ${ORIGIN_HOST} site block and origin token are both present, so this is NOT the shared-Caddyfile failure. Check the API container, Cloudflare, and the origin certificate."
  die "public health is ${code} with an intact edge configuration"
fi
exit 0
