#!/usr/bin/env bash

set -Eeuo pipefail
# shellcheck source=ops/contabo/lib.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/lib.sh"

candidate=""
legacy_app="nyptid-studio"
legacy_machine_id=""
legacy_origin=""
evidence_dir=""
data_attestation="${STUDIO_SHARED_DIR}/data-ready.attestation"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --candidate) candidate="${2:-}"; shift 2 ;;
    --legacy-app) legacy_app="${2:-}"; shift 2 ;;
    --legacy-machine-id) legacy_machine_id="${2:-}"; shift 2 ;;
    --legacy-origin) legacy_origin="${2:-}"; shift 2 ;;
    --evidence-dir) evidence_dir="${2:-}"; shift 2 ;;
    --data-attestation) data_attestation="${2:-}"; shift 2 ;;
    *) die "unknown argument: $1" ;;
  esac
done

[[ "${legacy_app}" =~ ^[a-z0-9][a-z0-9-]{1,62}$ ]] ||
  die "--legacy-app is invalid"
[[ "${legacy_machine_id}" =~ ^[A-Za-z0-9]+$ ]] ||
  die "--legacy-machine-id is required"
[[ "${legacy_origin}" == https://* ]] || die "legacy origin must be HTTPS"
[[ "${evidence_dir}" == "${STUDIO_SHARED_DIR}/fly-evidence/"* ]] ||
  die "Fly evidence must be installed under the managed root-only directory"

assert_safe_install_root
require_commands python3 sha256sum install stat
acquire_lifecycle_lock

if [[ -z "${candidate}" ]]; then
  [[ -L "${STUDIO_SHARED_DIR}/staged.env" ]] || die "no staged candidate is available"
  candidate="$(readlink -f -- "${STUDIO_SHARED_DIR}/staged.env")"
fi
assert_candidate_env "${candidate}"
if [[ -L "${STUDIO_SHARED_DIR}/active.env" ]]; then
  [[ -f "${STUDIO_SHARED_DIR}/consumer.disabled" ]] ||
    die "Contabo is already the enabled owner; a cutover fence is not needed"
fi

assert_private_dir "${evidence_dir}"
for evidence_name in \
  machine-list-before.json machine-before.json app-config.json \
  origin-probe.json legacy-queue.json file-quiescence.json \
  machine-after.json machine-list-after.json; do
  assert_private_file "${evidence_dir}/${evidence_name}"
done
evidence_summary="$(
  python3 "${CONTABO_OPS_DIR}/fly_cutover_evidence.py" \
    --evidence-dir "${evidence_dir}" \
    --app "${legacy_app}" \
    --machine-id "${legacy_machine_id}" \
    --origin "${legacy_origin}" \
    --max-age-seconds 1800
)"
summary_value() {
  local key="$1"
  printf '%s\n' "${evidence_summary}" |
    awk -F= -v wanted="${key}" '$1 == wanted { print $2; found=1; exit } END { if (!found) exit 1 }'
}
evidence_bundle_sha="$(summary_value BUNDLE_SHA256)"
[[ "${evidence_bundle_sha}" =~ ^[0-9a-f]{64}$ ]] ||
  die "Fly evidence bundle hash is invalid"

[[ "${data_attestation}" == "${STUDIO_SHARED_DIR}/data-ready.attestation" ]] ||
  die "data readiness attestation must use the managed root-only path"
assert_private_file "${data_attestation}"
data_dir="$(env_value "${candidate}" STUDIO_DATA_DIR)"
manifest_path="$(env_value "${data_attestation}" MANIFEST_PATH || true)"
[[ "${manifest_path}" == "${STUDIO_SHARED_DIR}/data-manifests/"* ]] ||
  die "data readiness attestation does not reference a managed source manifest"
python3 "${CONTABO_OPS_DIR}/data_manifest.py" check \
  --data-dir "${data_dir}" \
  --attestation "${data_attestation}" \
  --role migrated-data-ready
data_manifest_sha="$(env_value "${data_attestation}" MANIFEST_SHA256 || true)"
data_file_count="$(env_value "${data_attestation}" FILE_COUNT || true)"
data_total_bytes="$(env_value "${data_attestation}" TOTAL_BYTES || true)"
[[ "${data_manifest_sha}" =~ ^[0-9a-f]{64}$ ]] || die "data readiness manifest hash is invalid"
[[ "${data_file_count}" =~ ^[1-9][0-9]*$ ]] || die "data readiness file count must be positive"
[[ "${data_total_bytes}" =~ ^[1-9][0-9]*$ ]] || die "data readiness byte count must be positive"
data_attestation_sha="$(sha256sum "${data_attestation}" | awk '{print $1}')"

build_id="$(env_value "${candidate}" EXPECTED_BUILD_ID)"
target_sha="$(env_value "${candidate}" EXPECTED_GIT_SHA)"
fence_dir="${STUDIO_SHARED_DIR}/fences"
fence_path="${fence_dir}/${build_id}.fence"
tmp_fence="${fence_path}.tmp.$$"
install -d -m 700 -- "${fence_dir}"
umask 077
{
  printf 'FENCE_FORMAT=2\n'
  printf 'TARGET_GIT_SHA=%s\n' "${target_sha}"
  printf 'CREATED_AT_EPOCH=%s\n' "$(date -u +%s)"
  printf 'LEGACY_APP=%s\n' "${legacy_app}"
  printf 'LEGACY_MACHINE_ID=%s\n' "${legacy_machine_id}"
  printf 'LEGACY_ORIGIN=%s\n' "${legacy_origin}"
  printf 'LEGACY_EVIDENCE_DIR=%s\n' "${evidence_dir}"
  printf 'LEGACY_EVIDENCE_BUNDLE_SHA256=%s\n' "${evidence_bundle_sha}"
  for key in \
    MACHINE_BEFORE_SHA256 APP_CONFIG_SHA256 ORIGIN_PROBE_SHA256 \
    QUEUE_PROBE_SHA256 FILE_QUIESCENCE_SHA256 MACHINE_AFTER_SHA256; do
    printf 'LEGACY_%s=%s\n' "${key}" "$(summary_value "${key}")"
  done
  printf 'DATA_READY_ATTESTATION_PATH=%s\n' "${data_attestation}"
  printf 'DATA_READY_ATTESTATION_SHA256=%s\n' "${data_attestation_sha}"
  printf 'DATA_MANIFEST_SHA256=%s\n' "${data_manifest_sha}"
  printf 'DATA_FILE_COUNT=%s\n' "${data_file_count}"
  printf 'DATA_TOTAL_BYTES=%s\n' "${data_total_bytes}"
} >"${tmp_fence}"
chmod 600 "${tmp_fence}"
mv -f -- "${tmp_fence}" "${fence_path}"

info "Wrote machine-verified short-lived cutover fence: ${fence_path}"
info "Activation will re-read and revalidate every bound raw evidence file"
