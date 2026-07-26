#!/usr/bin/env bash

set -Eeuo pipefail

CONTABO_OPS_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(cd -- "${CONTABO_OPS_DIR}/../.." && pwd -P)"
STUDIO_INSTALL_ROOT="${STUDIO_INSTALL_ROOT:-/opt/studio}"
STUDIO_SHARED_DIR="${STUDIO_SHARED_DIR:-${STUDIO_INSTALL_ROOT}/shared}"
STUDIO_BASE_ENV="${STUDIO_BASE_ENV:-${STUDIO_SHARED_DIR}/base.env}"
STUDIO_LOCK_FILE="${STUDIO_LOCK_FILE:-${STUDIO_SHARED_DIR}/lifecycle.lock}"
STUDIO_PROJECT_NAME="${STUDIO_PROJECT_NAME:-nyptid-studio}"

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

info() {
  printf '==> %s\n' "$*"
}

require_commands() {
  local command_name
  for command_name in "$@"; do
    command -v "${command_name}" >/dev/null 2>&1 || die "required command not found: ${command_name}"
  done
}

assert_safe_install_root() {
  [[ "${STUDIO_INSTALL_ROOT}" == /* ]] || die "STUDIO_INSTALL_ROOT must be absolute"
  [[ "${STUDIO_INSTALL_ROOT}" != "/" ]] || die "STUDIO_INSTALL_ROOT cannot be /"
  case "${STUDIO_INSTALL_ROOT}" in
    /opt/studio|/srv/studio|/var/lib/studio) ;;
    *) die "refusing unexpected install root: ${STUDIO_INSTALL_ROOT}" ;;
  esac
}

assert_private_file() {
  local path="$1"
  local mode owner
  [[ -f "${path}" ]] || die "required file is missing: ${path}"
  [[ ! -L "${path}" ]] || die "secret-bearing file cannot be a symlink: ${path}"
  mode="$(stat -c '%a' -- "${path}")"
  owner="$(stat -c '%u' -- "${path}")"
  [[ "${mode}" =~ ^[0-7]00$ ]] || die "${path} must not grant group/other permissions (mode=${mode})"
  [[ "${owner}" == "0" ]] || die "${path} must be owned by root (uid=${owner})"
}

assert_private_dir() {
  local path="$1"
  local mode owner
  [[ -d "${path}" ]] || die "required directory is missing: ${path}"
  [[ ! -L "${path}" ]] || die "private directory cannot be a symlink: ${path}"
  mode="$(stat -c '%a' -- "${path}")"
  owner="$(stat -c '%u' -- "${path}")"
  [[ "${mode}" =~ ^[0-7]00$ ]] ||
    die "${path} must not grant group/other permissions (mode=${mode})"
  [[ "${owner}" == "0" ]] || die "${path} must be owned by root (uid=${owner})"
}

env_value() {
  local path="$1"
  local key="$2"
  awk -v wanted="${key}" '
    index($0, wanted "=") == 1 {
      print substr($0, length(wanted) + 2)
      found = 1
      exit
    }
    END {
      if (!found) exit 1
    }
  ' "${path}"
}

build_timestamp_from_id() {
  local build_id="$1"
  [[ "${build_id}" =~ ^studio-([0-9]{8}T[0-9]{6}Z)-[0-9a-f]{12}$ ]] ||
    die "cannot extract timestamp from invalid Studio build ID"
  printf '%s\n' "${BASH_REMATCH[1]}"
}

assert_candidate_env() {
  local candidate="$1"
  local sha build image image_id image_digest_ref release_dir compose_file
  assert_private_file "${candidate}"
  sha="$(env_value "${candidate}" EXPECTED_GIT_SHA)" || die "candidate lacks EXPECTED_GIT_SHA"
  build="$(env_value "${candidate}" EXPECTED_BUILD_ID)" || die "candidate lacks EXPECTED_BUILD_ID"
  image="$(env_value "${candidate}" STUDIO_IMAGE)" || die "candidate lacks STUDIO_IMAGE"
  image_id="$(env_value "${candidate}" STUDIO_IMAGE_ID)" || die "candidate lacks STUDIO_IMAGE_ID"
  image_digest_ref="$(env_value "${candidate}" STUDIO_IMAGE_DIGEST_REF 2>/dev/null || true)"
  release_dir="$(env_value "${candidate}" RELEASE_DIR)" || die "candidate lacks RELEASE_DIR"
  compose_file="$(env_value "${candidate}" STUDIO_COMPOSE_FILE)" || die "candidate lacks STUDIO_COMPOSE_FILE"
  [[ "${sha}" =~ ^[0-9a-f]{40}$ ]] || die "candidate Git SHA is invalid"
  [[ "${build}" =~ ^studio-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$ ]] || die "candidate build ID is invalid"
  [[ "${image}" == "nyptid-studio:${build}" ]] || die "candidate image is not tied to its build ID"
  [[ "${image_id}" =~ ^sha256:[0-9a-f]{64}$ ]] || die "candidate image ID is invalid"
  if [[ -n "${image_digest_ref}" ]]; then
    [[ "${image_digest_ref}" =~ ^docker\.io/nyptid/nyptid-studio-api@sha256:[0-9a-f]{64}$ ]] ||
      die "candidate CI image digest reference is invalid"
  fi
  [[ "${release_dir}" == /* && -d "${release_dir}" ]] || die "candidate release directory is unavailable"
  [[ "${compose_file}" == "${release_dir}/ops/contabo/docker-compose.yml" ]] || die "candidate Compose path is not release-owned"
  [[ -f "${compose_file}" ]] || die "candidate Compose file is unavailable"
}

compose_for() {
  local candidate="$1"
  shift
  local compose_file
  compose_file="$(env_value "${candidate}" STUDIO_COMPOSE_FILE)"
  docker compose \
    --project-name "${STUDIO_PROJECT_NAME}" \
    --env-file "${candidate}" \
    --file "${compose_file}" \
    "$@"
}

atomic_symlink() {
  local target="$1"
  local link_path="$2"
  local tmp_link="${link_path}.tmp.$$"
  if [[ ( -e "${link_path}" || -L "${link_path}" ) && ! -L "${link_path}" ]]; then
    die "refusing to replace non-symlink lifecycle path: ${link_path}"
  fi
  ln -s -- "${target}" "${tmp_link}"
  mv -Tf -- "${tmp_link}" "${link_path}"
}

acquire_lifecycle_lock() {
  require_commands flock
  install -d -m 700 -- "${STUDIO_SHARED_DIR}"
  exec 9>"${STUDIO_LOCK_FILE}"
  flock -x 9
}

running_api_container_count() {
  docker ps \
    --filter "label=com.docker.compose.project=${STUDIO_PROJECT_NAME}" \
    --filter "label=com.docker.compose.service=studio-api" \
    --format '{{.ID}}' | awk 'NF { count += 1 } END { print count + 0 }'
}

assert_candidate_image_binding() {
  local candidate="$1"
  local image expected_image_id actual_image_id
  image="$(env_value "${candidate}" STUDIO_IMAGE)"
  expected_image_id="$(env_value "${candidate}" STUDIO_IMAGE_ID)"
  actual_image_id="$(docker image inspect --format '{{.Id}}' "${image}" 2>/dev/null || true)"
  [[ "${actual_image_id}" == "${expected_image_id}" ]] ||
    die "candidate image tag no longer resolves to its attested image ID"
}

create_verified_api_container() {
  local candidate="$1"
  local container_id expected_image_id actual_image_id running
  assert_candidate_image_binding "${candidate}"
  compose_for "${candidate}" create --force-recreate --no-deps studio-api
  container_id="$(
    compose_for "${candidate}" ps --all --quiet studio-api | awk 'NF { print; exit }'
  )"
  [[ -n "${container_id}" ]] || die "Compose did not create the Studio API container"
  running="$(docker inspect --format '{{.State.Running}}' "${container_id}")"
  [[ "${running}" == "false" ]] ||
    die "candidate container ran before immutable image verification"
  expected_image_id="$(env_value "${candidate}" STUDIO_IMAGE_ID)"
  actual_image_id="$(docker inspect --format '{{.Image}}' "${container_id}")"
  [[ "${actual_image_id}" == "${expected_image_id}" ]] ||
    die "stopped candidate container image ID does not match the attested image"
}

queue_drain_snapshot() {
  local -a counts
  mapfile -t counts < <(
    docker exec nyptid-studio-redis sh -ec '
      REDISCLI_AUTH="$REDIS_PASSWORD" exec redis-cli --raw EVAL "
        local queued =
          redis.call(\"LLEN\", \"studio:queue:p0\") +
          redis.call(\"LLEN\", \"studio:queue:p1\") +
          redis.call(\"LLEN\", \"studio:queue:p2\")
        local inflight = redis.call(\"LLEN\", \"studio:queue:processing\")
        local admitted = #redis.call(\"KEYS\", \"studio:queue:admitted:*\")
        local agent_active = tonumber(redis.call(\"GET\", \"studio:studio_agent:active\") or \"0\")
        local agent_waiting = tonumber(redis.call(\"GET\", \"studio:studio_agent:waiting\") or \"0\")
        local agent_leases = redis.call(\"ZCARD\", \"studio:studio_agent:leases\")
        local slots_active = 0
        local slots_waiting = 0
        local slots_leases = 0
        for _, lane in ipairs({\"render\", \"stills\", \"i2v\", \"i2v_premium\", \"audio\", \"compose\"}) do
          local base = \"studio:production_slots:\" .. lane
          slots_active = slots_active + tonumber(redis.call(\"GET\", base .. \":active\") or \"0\")
          slots_waiting = slots_waiting + tonumber(redis.call(\"GET\", base .. \":waiting\") or \"0\")
          slots_leases = slots_leases + redis.call(\"ZCARD\", base .. \":leases\")
        end
        return {
          queued,
          inflight,
          admitted,
          agent_active,
          agent_waiting,
          agent_leases,
          slots_active,
          slots_waiting,
          slots_leases
        }
      " 0
    '
  )
  [[ "${#counts[@]}" -eq 9 ]] || die "could not read the complete Redis drain snapshot"
  local value
  for value in "${counts[@]}"; do
    [[ "${value}" =~ ^[0-9]+$ ]] || die "Redis drain snapshot returned a non-integer"
  done
  printf 'BACKEND_QUEUED=%s\n' "${counts[0]}"
  printf 'BACKEND_INFLIGHT=%s\n' "${counts[1]}"
  printf 'BACKEND_ADMITTED=%s\n' "${counts[2]}"
  printf 'AGENT_ACTIVE=%s\n' "${counts[3]}"
  printf 'AGENT_WAITING=%s\n' "${counts[4]}"
  printf 'AGENT_LEASES=%s\n' "${counts[5]}"
  printf 'SLOTS_ACTIVE=%s\n' "${counts[6]}"
  printf 'SLOTS_WAITING=%s\n' "${counts[7]}"
  printf 'SLOTS_LEASES=%s\n' "${counts[8]}"
}

assert_queue_snapshot_drained() {
  local snapshot="$1"
  local line key value seen=0
  while IFS= read -r line; do
    [[ "${line}" == *=* ]] || die "malformed queue drain snapshot"
    key="${line%%=*}"
    value="${line#*=}"
    [[ "${key}" =~ ^[A-Z_]+$ && "${value}" =~ ^[0-9]+$ ]] ||
      die "invalid queue drain snapshot field"
    [[ "${value}" == "0" ]] || die "queue/in-flight drain failed: ${key}=${value}"
    seen=$((seen + 1))
  done <<<"${snapshot}"
  (( seen == 9 )) || die "queue drain snapshot is incomplete"
}

capture_file_quiescence() {
  local candidate="$1"
  local output="$2"
  local data_dir sessions_dir
  data_dir="$(env_value "${candidate}" STUDIO_DATA_DIR)"
  sessions_dir="${data_dir}/studio_agent_sessions"
  [[ -d "${sessions_dir}" ]] ||
    die "Studio Agent sessions directory is unavailable: ${sessions_dir}"
  python3 "${CONTABO_OPS_DIR}/file_quiescence.py" \
    --sessions-dir "${sessions_dir}" \
    --output "${output}" \
    --require-drained
}

validate_studio_secrets() {
  local candidate="$1"
  local secret_file
  local key value
  secret_file="$(env_value "${candidate}" STUDIO_ENV_FILE)" || die "candidate lacks STUDIO_ENV_FILE"
  assert_private_file "${secret_file}"
  for key in \
    ANTHROPIC_API_KEY \
    FAL_AI_KEY \
    YOUTUBE_API_KEY \
    GOOGLE_CLIENT_ID \
    GOOGLE_CLIENT_SECRET \
    GOOGLE_REDIRECT_URI \
    SUPABASE_URL \
    SUPABASE_ANON_KEY \
    SUPABASE_JWT_SECRET \
    SUPABASE_SERVICE_KEY \
    STRIPE_SECRET_KEY \
    STRIPE_WEBHOOK_SECRET \
    PAYPAL_CLIENT_ID \
    PAYPAL_CLIENT_SECRET \
    PAYPAL_WEBHOOK_ID; do
    value="$(env_value "${secret_file}" "${key}" 2>/dev/null || true)"
    [[ -n "${value}" && "${value}" != *"<PASTE_"* ]] || die "required Studio secret/config is empty: ${key}"
  done
}
