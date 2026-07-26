#!/usr/bin/env bash

set -Eeuo pipefail
# shellcheck source=ops/contabo/lib.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/lib.sh"

mode="previous"
confirmation=""
queue_attestation="${STUDIO_SHARED_DIR}/queue-drained.attestation"
reverse_data_attestation=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --to-previous)
      mode="previous"
      shift
      ;;
    --stop-for-legacy)
      mode="prepare_legacy"
      shift
      ;;
    --authorize-legacy)
      mode="authorize_legacy"
      shift
      ;;
    --queue-attestation)
      queue_attestation="${2:-}"
      shift 2
      ;;
    --reverse-data-attestation)
      reverse_data_attestation="${2:-}"
      shift 2
      ;;
    --confirm)
      confirmation="${2:-}"
      shift 2
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

assert_safe_install_root
require_commands docker stat python3 sha256sum install curl mktemp
acquire_lifecycle_lock

active_link="${STUDIO_SHARED_DIR}/active.env"
previous_link="${STUDIO_SHARED_DIR}/previous.env"
[[ -L "${active_link}" ]] || die "no active Contabo release is recorded"
active="$(readlink -f -- "${active_link}")"
assert_candidate_env "${active}"

summary_value() {
  local summary="$1"
  local key="$2"
  printf '%s\n' "${summary}" | awk -F= -v wanted="${key}" '$1 == wanted { print $2; found = 1; exit } END { if (!found) exit 1 }'
}

json_value() {
  local path="$1"
  local key="$2"
  python3 - "${path}" "${key}" <<'PY'
import json
import sys
with open(sys.argv[1], "r", encoding="utf-8") as handle:
    payload = json.load(handle)
value = payload
for segment in sys.argv[2].split("."):
    if not isinstance(value, dict) or segment not in value:
        raise SystemExit(1)
    value = value[segment]
if isinstance(value, bool):
    print("true" if value else "false")
else:
    print(value)
PY
}

if [[ "${mode}" == "prepare_legacy" ]]; then
  [[ "${confirmation}" == "prepare-legacy-after-ingress-is-blocked" ]] ||
    die "pass --confirm prepare-legacy-after-ingress-is-blocked only after blocking new Contabo mutations"
  [[ "${queue_attestation}" == "${STUDIO_SHARED_DIR}/queue-drained.attestation" ]] ||
    die "queue drain attestation must use the managed root-only path"

  pre_stop_snapshot="$(queue_drain_snapshot)"
  assert_queue_snapshot_drained "${pre_stop_snapshot}"
  pre_file_quiescence="$(mktemp "${STUDIO_SHARED_DIR}/.pre-file-quiescence.XXXXXX")"
  capture_file_quiescence "${active}" "${pre_file_quiescence}"
  rm -- "${pre_file_quiescence}"

  disabled_marker="${STUDIO_SHARED_DIR}/consumer.disabled"
  tmp_marker="${disabled_marker}.tmp.$$"
  umask 077
  {
    printf 'CONSUMER_DISABLED=1\n'
    printf 'STATE=preparing-legacy-reverse-sync\n'
    printf 'DISABLED_AT_EPOCH=%s\n' "$(date -u +%s)"
  } >"${tmp_marker}"
  chmod 600 "${tmp_marker}"
  mv -f -- "${tmp_marker}" "${disabled_marker}"
  if [[ -e "${STUDIO_SHARED_DIR}/legacy-start.ready" || -L "${STUDIO_SHARED_DIR}/legacy-start.ready" ]]; then
    [[ -f "${STUDIO_SHARED_DIR}/legacy-start.ready" && ! -L "${STUDIO_SHARED_DIR}/legacy-start.ready" ]] ||
      die "legacy authorization path is not a regular file"
    rm -- "${STUDIO_SHARED_DIR}/legacy-start.ready"
  fi

  info "Stopping the Contabo API/consumer before creating the reverse-sync source proof"
  compose_for "${active}" stop -t 120 studio-api
  count="$(running_api_container_count)"
  (( count == 0 )) || die "Contabo consumer is still running; do not start the legacy platform"
  loopback_port="$(env_value "${active}" STUDIO_LOOPBACK_PORT)"
  set +e
  retired_http_code="$(
    curl --silent --show-error --connect-timeout 2 --max-time 5 \
      --output /dev/null --write-out '%{http_code}' \
      "http://127.0.0.1:${loopback_port}/api/health"
  )"
  retired_curl_exit="$?"
  set -e
  [[ "${retired_curl_exit}" -ne 0 && ! "${retired_http_code}" =~ ^[23][0-9][0-9]$ ]] ||
    die "Contabo ingress still reaches an API after the consumer stop"

  post_stop_snapshot="$(queue_drain_snapshot)"
  assert_queue_snapshot_drained "${post_stop_snapshot}"
  file_quiescence_attestation="${STUDIO_SHARED_DIR}/file-quiescence.attestation.json"
  capture_file_quiescence "${active}" "${file_quiescence_attestation}"
  assert_private_file "${file_quiescence_attestation}"
  file_quiescence_sha="$(sha256sum "${file_quiescence_attestation}" | awk '{print $1}')"
  timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
  reverse_manifest="${STUDIO_SHARED_DIR}/reverse-manifests/contabo-${timestamp}.manifest"
  install -d -m 700 -- "${STUDIO_SHARED_DIR}/reverse-manifests"
  manifest_summary="$(
    python3 "${CONTABO_OPS_DIR}/data_manifest.py" create \
      --data-dir "$(env_value "${active}" STUDIO_DATA_DIR)" \
      --manifest "${reverse_manifest}"
  )"
  manifest_sha="$(summary_value "${manifest_summary}" MANIFEST_SHA256)"
  manifest_files="$(summary_value "${manifest_summary}" FILE_COUNT)"
  manifest_bytes="$(summary_value "${manifest_summary}" TOTAL_BYTES)"

  tmp_attestation="${queue_attestation}.tmp.$$"
  {
    printf 'ATTESTATION_FORMAT=1\n'
    printf 'QUEUE_DRAINED=1\n'
    printf 'CREATED_AT_EPOCH=%s\n' "$(date -u +%s)"
    printf 'ACTIVE_BUILD_ID=%s\n' "$(env_value "${active}" EXPECTED_BUILD_ID)"
    printf 'DATA_MANIFEST_PATH=%s\n' "${reverse_manifest}"
    printf 'DATA_MANIFEST_SHA256=%s\n' "${manifest_sha}"
    printf 'DATA_FILE_COUNT=%s\n' "${manifest_files}"
    printf 'DATA_TOTAL_BYTES=%s\n' "${manifest_bytes}"
    printf 'FILE_QUIESCENCE_ATTESTATION_PATH=%s\n' "${file_quiescence_attestation}"
    printf 'FILE_QUIESCENCE_ATTESTATION_SHA256=%s\n' "${file_quiescence_sha}"
    printf 'FILE_QUIESCENCE_SNAPSHOT_SHA256=%s\n' \
      "$(json_value "${file_quiescence_attestation}" snapshot_sha256)"
    printf '%s\n' "${post_stop_snapshot}"
  } >"${tmp_attestation}"
  chmod 600 "${tmp_attestation}"
  mv -f -- "${tmp_attestation}" "${queue_attestation}"
  queue_attestation_sha="$(sha256sum "${queue_attestation}" | awk '{print $1}')"

  tmp_marker="${disabled_marker}.tmp.$$"
  {
    printf 'CONSUMER_DISABLED=1\n'
    printf 'STATE=awaiting-verified-reverse-sync\n'
    printf 'DISABLED_AT_EPOCH=%s\n' "$(date -u +%s)"
    printf 'QUEUE_DRAIN_ATTESTATION_PATH=%s\n' "${queue_attestation}"
    printf 'QUEUE_DRAIN_ATTESTATION_SHA256=%s\n' "${queue_attestation_sha}"
    printf 'FILE_QUIESCENCE_ATTESTATION_SHA256=%s\n' "${file_quiescence_sha}"
  } >"${tmp_marker}"
  chmod 600 "${tmp_marker}"
  mv -f -- "${tmp_marker}" "${disabled_marker}"

  info "Contabo has zero production consumers and a zero-count queue/in-flight attestation"
  info "Reverse-sync source manifest: ${reverse_manifest}"
  info "Fly is NOT authorized to run yet; sync and verify /var/data, then use --authorize-legacy"
  exit 0
fi

if [[ "${mode}" == "authorize_legacy" ]]; then
  [[ "${confirmation}" == "reverse-data-sync-is-verified-and-fly-is-still-copy-only" ]] ||
    die "pass --confirm reverse-data-sync-is-verified-and-fly-is-still-copy-only only after destination verification"
  [[ "${queue_attestation}" == "${STUDIO_SHARED_DIR}/queue-drained.attestation" ]] ||
    die "queue drain attestation must use the managed root-only path"
  [[ -n "${reverse_data_attestation}" ]] || die "--reverse-data-attestation is required"
  disabled_marker="${STUDIO_SHARED_DIR}/consumer.disabled"
  assert_private_file "${disabled_marker}"
  assert_private_file "${queue_attestation}"
  assert_private_file "${reverse_data_attestation}"
  count="$(running_api_container_count)"
  (( count == 0 )) || die "Contabo consumer is running; legacy authorization is forbidden"

  live_snapshot="$(queue_drain_snapshot)"
  assert_queue_snapshot_drained "${live_snapshot}"
  file_quiescence_attestation="$(
    env_value "${queue_attestation}" FILE_QUIESCENCE_ATTESTATION_PATH || true
  )"
  [[ "${file_quiescence_attestation}" == "${STUDIO_SHARED_DIR}/file-quiescence.attestation.json" ]] ||
    die "queue drain attestation references unmanaged file quiescence evidence"
  assert_private_file "${file_quiescence_attestation}"
  expected_file_quiescence_sha="$(
    env_value "${queue_attestation}" FILE_QUIESCENCE_ATTESTATION_SHA256 || true
  )"
  actual_file_quiescence_sha="$(
    sha256sum "${file_quiescence_attestation}" | awk '{print $1}'
  )"
  [[ "${expected_file_quiescence_sha}" =~ ^[0-9a-f]{64}$ &&
     "${actual_file_quiescence_sha}" == "${expected_file_quiescence_sha}" ]] ||
    die "file quiescence evidence changed after the Contabo consumer stopped"
  current_file_quiescence="$(mktemp "${STUDIO_SHARED_DIR}/.current-file-quiescence.XXXXXX")"
  capture_file_quiescence "${active}" "${current_file_quiescence}"
  actual_file_snapshot_sha="$(
    json_value "${current_file_quiescence}" snapshot_sha256
  )"
  expected_file_snapshot_sha="$(
    env_value "${queue_attestation}" FILE_QUIESCENCE_SNAPSHOT_SHA256 || true
  )"
  [[ "${actual_file_snapshot_sha}" == "${expected_file_snapshot_sha}" ]] ||
    die "file-backed resumable production changed after the drain proof"
  rm -- "${current_file_quiescence}"
  [[ "$(env_value "${queue_attestation}" ATTESTATION_FORMAT || true)" == "1" ]] ||
    die "queue drain attestation format is invalid"
  [[ "$(env_value "${queue_attestation}" QUEUE_DRAINED || true)" == "1" ]] ||
    die "queue drain attestation is not verified"
  attested_snapshot=""
  for key in \
    BACKEND_QUEUED BACKEND_INFLIGHT BACKEND_ADMITTED \
    AGENT_ACTIVE AGENT_WAITING AGENT_LEASES \
    SLOTS_ACTIVE SLOTS_WAITING SLOTS_LEASES; do
    attested_snapshot+="${key}=$(env_value "${queue_attestation}" "${key}" || true)"$'\n'
  done
  attested_snapshot="${attested_snapshot%$'\n'}"
  assert_queue_snapshot_drained "${attested_snapshot}"

  expected_queue_sha="$(env_value "${disabled_marker}" QUEUE_DRAIN_ATTESTATION_SHA256 || true)"
  actual_queue_sha="$(sha256sum "${queue_attestation}" | awk '{print $1}')"
  [[ "${expected_queue_sha}" =~ ^[0-9a-f]{64}$ && "${actual_queue_sha}" == "${expected_queue_sha}" ]] ||
    die "queue drain attestation changed after the Contabo consumer was stopped"

  source_manifest="$(env_value "${queue_attestation}" DATA_MANIFEST_PATH || true)"
  [[ "${source_manifest}" == "${STUDIO_SHARED_DIR}/reverse-manifests/"* ]] ||
    die "queue drain attestation references an unmanaged reverse manifest"
  source_summary="$(
    python3 "${CONTABO_OPS_DIR}/data_manifest.py" verify \
      --data-dir "$(env_value "${active}" STUDIO_DATA_DIR)" \
      --manifest "${source_manifest}"
  )"
  source_sha="$(summary_value "${source_summary}" MANIFEST_SHA256)"
  source_files="$(summary_value "${source_summary}" FILE_COUNT)"
  source_bytes="$(summary_value "${source_summary}" TOTAL_BYTES)"
  [[ "${source_sha}" == "$(env_value "${queue_attestation}" DATA_MANIFEST_SHA256 || true)" ]] ||
    die "reverse source manifest hash changed"
  [[ "${source_files}" == "$(env_value "${queue_attestation}" DATA_FILE_COUNT || true)" ]] ||
    die "reverse source manifest file count changed"
  [[ "${source_bytes}" == "$(env_value "${queue_attestation}" DATA_TOTAL_BYTES || true)" ]] ||
    die "reverse source manifest byte count changed"

  reverse_summary="$(
    python3 "${CONTABO_OPS_DIR}/data_manifest.py" inspect \
      --attestation "${reverse_data_attestation}" \
      --role reverse-destination
  )"
  [[ "$(env_value "${reverse_data_attestation}" DATA_DIR || true)" == "/var/data" ]] ||
    die "reverse data attestation does not target Fly /var/data"
  [[ "$(summary_value "${reverse_summary}" MANIFEST_SHA256)" == "${source_sha}" ]] ||
    die "Fly reverse-sync manifest hash does not match Contabo"
  [[ "$(summary_value "${reverse_summary}" FILE_COUNT)" == "${source_files}" ]] ||
    die "Fly reverse-sync file count does not match Contabo"
  [[ "$(summary_value "${reverse_summary}" TOTAL_BYTES)" == "${source_bytes}" ]] ||
    die "Fly reverse-sync byte count does not match Contabo"

  queue_created="$(env_value "${queue_attestation}" CREATED_AT_EPOCH || true)"
  reverse_verified="$(env_value "${reverse_data_attestation}" VERIFIED_AT_EPOCH || true)"
  [[ "${queue_created}" =~ ^[0-9]+$ && "${reverse_verified}" =~ ^[0-9]+$ ]] ||
    die "legacy rollback attestation timestamps are invalid"
  now="$(date -u +%s)"
  (( reverse_verified >= queue_created && reverse_verified <= now && now - reverse_verified <= 86400 )) ||
    die "reverse data verification is stale or predates the queue drain"

  reverse_attestation_sha="$(sha256sum "${reverse_data_attestation}" | awk '{print $1}')"
  ready_path="${STUDIO_SHARED_DIR}/legacy-start.ready"
  tmp_ready="${ready_path}.tmp.$$"
  {
    printf 'AUTHORIZATION_FORMAT=1\n'
    printf 'LEGACY_START_AUTHORIZED=1\n'
    printf 'AUTHORIZED_AT_EPOCH=%s\n' "${now}"
    printf 'QUEUE_DRAIN_ATTESTATION_SHA256=%s\n' "${actual_queue_sha}"
    printf 'REVERSE_DATA_ATTESTATION_SHA256=%s\n' "${reverse_attestation_sha}"
    printf 'DATA_MANIFEST_SHA256=%s\n' "${source_sha}"
    printf 'DATA_FILE_COUNT=%s\n' "${source_files}"
    printf 'DATA_TOTAL_BYTES=%s\n' "${source_bytes}"
  } >"${tmp_ready}"
  chmod 600 "${tmp_ready}"
  mv -f -- "${tmp_ready}" "${ready_path}"
  info "Legacy start authorization written: ${ready_path}"
  info "No platform was started; reverify Fly is copy-only before deliberately restoring its API config"
  exit 0
fi

[[ ! -f "${STUDIO_SHARED_DIR}/consumer.disabled" ]] ||
  die "Contabo consumer is fenced off for legacy rollback; create a new cutover fence before restarting it"
[[ -L "${previous_link}" ]] || die "no previously verified Contabo candidate is recorded"
previous="$(readlink -f -- "${previous_link}")"
assert_candidate_env "${previous}"
[[ "${previous}" != "${active}" ]] || die "previous candidate equals active candidate"

count="$(running_api_container_count)"
(( count <= 1 )) || die "more than one Studio API container is running; refusing rollback"

info "Replacing the active single owner with the previous immutable candidate"
create_verified_api_container "${previous}"
compose_for "${previous}" start studio-api
previous_release="$(env_value "${previous}" RELEASE_DIR)"
if ! bash "${previous_release}/ops/contabo/smoke.sh" \
  --candidate "${previous}" \
  --attempts 60 \
  --check-container-count; then
  info "Previous candidate failed; restoring the release that was active"
  create_verified_api_container "${active}"
  compose_for "${active}" start studio-api
  active_release="$(env_value "${active}" RELEASE_DIR)"
  bash "${active_release}/ops/contabo/smoke.sh" \
    --candidate "${active}" \
    --attempts 60 \
    --check-container-count ||
    die "rollback target and restoration candidate both failed"
  die "rollback target failed health verification"
fi

atomic_symlink "${active}" "${previous_link}"
atomic_symlink "${previous}" "${active_link}"
atomic_symlink "${previous_release}" "${STUDIO_INSTALL_ROOT}/current"
info "Rollback complete: $(env_value "${previous}" EXPECTED_BUILD_ID)"
