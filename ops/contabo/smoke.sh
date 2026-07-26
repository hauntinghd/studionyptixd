#!/usr/bin/env bash

set -Eeuo pipefail
# shellcheck source=ops/contabo/lib.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/lib.sh"

candidate=""
url=""
expected_sha=""
expected_build=""
attempts=1
check_container_count=0
origin_token_file=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --candidate)
      candidate="${2:-}"
      shift 2
      ;;
    --url)
      url="${2:-}"
      shift 2
      ;;
    --expected-sha)
      expected_sha="${2:-}"
      shift 2
      ;;
    --expected-build)
      expected_build="${2:-}"
      shift 2
      ;;
    --attempts)
      attempts="${2:-}"
      shift 2
      ;;
    --check-container-count)
      check_container_count=1
      shift
      ;;
    --origin-token-file)
      origin_token_file="${2:-}"
      shift 2
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

require_commands curl python3 mktemp
[[ "${attempts}" =~ ^[1-9][0-9]*$ ]] || die "--attempts must be a positive integer"

if [[ -n "${candidate}" ]]; then
  candidate="$(readlink -f -- "${candidate}")"
  assert_candidate_env "${candidate}"
  expected_sha="$(env_value "${candidate}" EXPECTED_GIT_SHA)"
  expected_build="$(env_value "${candidate}" EXPECTED_BUILD_ID)"
  if [[ -z "${url}" ]]; then
    loopback_port="$(env_value "${candidate}" STUDIO_LOOPBACK_PORT)"
    url="http://127.0.0.1:${loopback_port}"
  fi
fi

[[ "${expected_sha}" =~ ^[0-9a-f]{40}$ ]] || die "a full --expected-sha or --candidate is required"
[[ "${expected_build}" =~ ^studio-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$ ]] ||
  die "a valid --expected-build or --candidate is required"
[[ "${url}" == http://* || "${url}" == https://* ]] || die "--url must be HTTP(S)"
url="${url%/}"
url_host="$(
  python3 - "${url}" <<'PY'
import sys
from urllib.parse import urlsplit

parsed = urlsplit(sys.argv[1])
print((parsed.hostname or "").lower())
PY
)"
host_header_args=()
case "${url_host}" in
  127.0.0.1|localhost|::1)
    # Loopback has no public virtual-host identity, so exercise the canonical
    # backend Host contract explicitly. Public Caddy/sslip/canonical URLs must
    # retain their actual Host so ingress routing itself is tested.
    host_header_args=(--header 'Host: api-studio.nyptidindustries.com')
    ;;
esac

tmp_dir="$(mktemp -d)"
cleanup() {
  rm -rf -- "${tmp_dir}"
}
trap cleanup EXIT
origin_auth_args=()
if [[ -n "${origin_token_file}" ]]; then
  [[ "${url}" == https://* ]] || die "--origin-token-file requires an HTTPS URL"
  origin_token_file="$(readlink -f -- "${origin_token_file}")"
  [[ -f "${origin_token_file}" && -r "${origin_token_file}" ]] ||
    die "--origin-token-file must name a readable regular file"
  origin_token="$(
    python3 - "${origin_token_file}" <<'PY'
import re
import sys

path = sys.argv[1]
matches = []
with open(path, "r", encoding="utf-8") as handle:
    for raw_line in handle:
        line = raw_line.strip()
        if line.startswith("STUDIO_ORIGIN_TOKEN="):
            matches.append(line.split("=", 1)[1])
if len(matches) != 1 or re.fullmatch(r"[A-Za-z0-9_-]{43,128}", matches[0]) is None:
    raise SystemExit("origin token file must contain one valid STUDIO_ORIGIN_TOKEN")
print(matches[0])
PY
  )"
  origin_curl_config="${tmp_dir}/origin-auth.curl"
  printf 'header = "X-NYPTID-Studio-Origin-Token: v1.%s"\n' \
    "${origin_token}" >"${origin_curl_config}"
  chmod 600 "${origin_curl_config}"
  unset origin_token
  origin_auth_args=(--config "${origin_curl_config}")
fi
payload_file="${tmp_dir}/health.json"
last_error="health endpoint did not return HTTP 200"

for ((attempt = 1; attempt <= attempts; attempt += 1)); do
  http_code="$(
    curl \
      --silent \
      --show-error \
      --connect-timeout 5 \
      --max-time 20 \
      "${origin_auth_args[@]}" \
      "${host_header_args[@]}" \
      --output "${payload_file}" \
      --write-out '%{http_code}' \
      "${url}/api/health" 2>"${tmp_dir}/curl.err" || true
  )"
  if [[ "${http_code}" == "200" ]]; then
    if last_error="$(
      python3 - "${payload_file}" "${expected_sha}" "${expected_build}" <<'PY'
import json
import sys

path, expected_sha, expected_build = sys.argv[1:]
try:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
except Exception as exc:
    print(f"invalid health JSON ({type(exc).__name__})")
    raise SystemExit(1)

checks = (
    (payload.get("status") == "online", "status is not online"),
    (payload.get("backend_commit") == expected_sha, "backend_commit does not match"),
    (payload.get("frontend_bundle") == expected_build, "frontend_bundle does not match"),
    (payload.get("deployment_target") == "contabo", "deployment target is not Contabo"),
    (payload.get("release_id") == expected_build, "release ID does not match"),
    (payload.get("queue_mode") == "redis", "queue_mode is not redis"),
    (payload.get("queue_consumer_ready") is True, "queue consumer is not ready"),
    (payload.get("queue_consumer_running") is True, "queue consumer is not running"),
    (payload.get("youtube_token_storage_ready") is True, "YouTube token storage is not ready"),
    (payload.get("xai_image_fallback_enabled") is False, "xAI image fallback is enabled"),
    (payload.get("runpod_production_enabled") is False, "unverified RunPod production is enabled"),
    (payload.get("runpod_longform_enabled") is False, "unverified RunPod long-form is enabled"),
    (payload.get("runpod_control_configured") is False, "RunPod control remains configured"),
    (payload.get("runpod_storage_configured") is False, "RunPod storage remains configured"),
    (payload.get("runpod_configured") is False, "RunPod remains configured"),
    (payload.get("cliplab_virality_backend") == "local_llm", "ClipLab virality is not local"),
    (payload.get("cliplab_reframe_backend") == "opencv_face", "ClipLab reframe is not local"),
    (payload.get("cliplab_runpod_configured") is False, "ClipLab RunPod remains configured"),
)
for ok, message in checks:
    if not ok:
        print(message)
        raise SystemExit(1)

consumer = payload.get("queue_consumer")
if not isinstance(consumer, dict) or consumer.get("workers") != 1:
    print("health does not prove exactly one embedded worker")
    raise SystemExit(1)
provider_order = payload.get("image_provider_order")
if provider_order != ["fal"]:
    print("effective Studio image provider order is not FAL-only")
    raise SystemExit(1)
print("ok")
PY
    )"; then
      break
    fi
  else
    last_error="$(tr '\n' ' ' <"${tmp_dir}/curl.err")"
  fi
  if (( attempt < attempts )); then
    sleep 2
  fi
done

[[ "${last_error}" == "ok" ]] || die "health/provenance smoke failed: ${last_error}"

cors_headers="${tmp_dir}/cors.headers"
cors_code="$(
  curl \
    --silent \
    --show-error \
    --connect-timeout 5 \
    --max-time 20 \
    "${origin_auth_args[@]}" \
    --request OPTIONS \
    "${host_header_args[@]}" \
    --header 'Origin: https://studio.nyptidindustries.com' \
    --header 'Access-Control-Request-Method: GET' \
    --dump-header "${cors_headers}" \
    --output /dev/null \
    --write-out '%{http_code}' \
    "${url}/api/health"
)"
[[ "${cors_code}" == "200" || "${cors_code}" == "204" ]] || die "CORS preflight returned HTTP ${cors_code}"
tr -d '\r' <"${cors_headers}" |
  grep -qi '^access-control-allow-origin: https://studio\.nyptidindustries\.com$' ||
  die "CORS does not authorize the exact Studio frontend origin"

curl \
  --fail \
  --silent \
  --show-error \
  --connect-timeout 5 \
  --max-time 20 \
  "${origin_auth_args[@]}" \
  "${host_header_args[@]}" \
  --output /dev/null \
  "${url}/"

if (( check_container_count )); then
  require_commands docker
  count="$(running_api_container_count)"
  (( count == 1 )) || die "expected exactly one running Studio API container, found ${count}"
  container_id="$(
    docker ps \
      --filter "label=com.docker.compose.project=${STUDIO_PROJECT_NAME}" \
      --filter "label=com.docker.compose.service=studio-api" \
      --format '{{.ID}}'
  )"
  running_image="$(docker inspect --format '{{.Config.Image}}' "${container_id}")"
  expected_image="$(env_value "${candidate}" STUDIO_IMAGE)"
  [[ "${running_image}" == "${expected_image}" ]] ||
    die "running container image does not match the candidate"
  running_image_id="$(docker inspect --format '{{.Image}}' "${container_id}")"
  expected_image_id="$(env_value "${candidate}" STUDIO_IMAGE_ID)"
  [[ "${running_image_id}" == "${expected_image_id}" ]] ||
    die "running container image ID does not match the immutable staged artifact"
fi

info "Smoke passed: ${expected_sha} / ${expected_build}; one ready Redis consumer"
