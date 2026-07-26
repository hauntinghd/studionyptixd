#!/usr/bin/env bash

set -Eeuo pipefail
# shellcheck source=ops/contabo/lib.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/lib.sh"

usage() {
  cat >&2 <<'EOF'
Usage:
  deploy.sh stage [--build-id studio-YYYYMMDDTHHMMSSZ-<12hex>]
                  [--image-ref docker.io/nyptid/nyptid-studio-api@sha256:<64hex>]
  deploy.sh activate [--candidate /opt/studio/shared/candidates/<id>.env]
                     [--fence /opt/studio/shared/fences/<id>.fence]

stage pulls the exact CI-produced digest when --image-ref is supplied. The
source-build fallback is retained for an explicitly local recovery stage. It
starts/prepares Redis only and never starts another API/consumer. The first
activate requires a recent cutover fence made only after the legacy production
consumer has been stopped and its traffic autostart path has been disabled and
verified.
EOF
  exit 2
}

mode="${1:-}"
[[ -n "${mode}" ]] || usage
shift

build_id=""
image_ref=""
candidate=""
fence=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --build-id)
      [[ $# -ge 2 ]] || usage
      build_id="$2"
      shift 2
      ;;
    --image-ref)
      [[ $# -ge 2 ]] || usage
      image_ref="$2"
      shift 2
      ;;
    --candidate)
      [[ $# -ge 2 ]] || usage
      candidate="$2"
      shift 2
      ;;
    --fence)
      [[ $# -ge 2 ]] || usage
      fence="$2"
      shift 2
      ;;
    *)
      usage
      ;;
  esac
done

assert_safe_install_root
require_commands docker git install stat awk python3 sha256sum
acquire_lifecycle_lock
assert_private_file "${STUDIO_BASE_ENV}"

stage_candidate() {
  local sha short_sha image image_id existing_image_id
  local existing_revision existing_version source_image_id
  local candidate_dir candidate_path tmp_candidate active_path redis_health

  [[ -z "${candidate}" && -z "${fence}" ]] || die "stage does not accept --candidate or --fence"
  cd "${REPO_ROOT}"
  [[ -z "$(git status --porcelain)" ]] ||
    die "refusing to build a dirty worktree; commit the exact release first"
  sha="$(git rev-parse HEAD)"
  [[ "${sha}" =~ ^[0-9a-f]{40}$ ]] || die "could not resolve a full Git SHA"
  short_sha="${sha:0:12}"
  if [[ -z "${build_id}" ]]; then
    build_id="studio-$(date -u +%Y%m%dT%H%M%SZ)-${short_sha}"
  fi
  [[ "${build_id}" =~ ^studio-[0-9]{8}T[0-9]{6}Z-${short_sha}$ ]] ||
    die "build ID must contain the current 12-character Git SHA"
  image="nyptid-studio:${build_id}"

  if [[ -n "${image_ref}" ]]; then
    [[ "${image_ref}" =~ ^docker\.io/nyptid/nyptid-studio-api@sha256:[0-9a-f]{64}$ ]] ||
      die "--image-ref must be the exact trusted Docker Hub repository digest"
    info "Pulling exact CI-produced Studio image ${image_ref}"
    docker pull "${image_ref}"
    source_image_id="$(docker image inspect --format '{{.Id}}' "${image_ref}")"
    existing_revision="$(
      docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "${image_ref}"
    )"
    existing_version="$(
      docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.version" }}' "${image_ref}"
    )"
    [[ "${existing_revision}" == "${sha}" && "${existing_version}" == "${build_id}" ]] ||
      die "CI image digest labels do not match the checked-out release"
    [[ "${source_image_id}" =~ ^sha256:[0-9a-f]{64}$ ]] ||
      die "CI image digest did not resolve to an immutable local image ID"

    if docker image inspect "${image}" >/dev/null 2>&1; then
      existing_image_id="$(docker image inspect --format '{{.Id}}' "${image}")"
      [[ "${existing_image_id}" == "${source_image_id}" ]] ||
        die "existing candidate tag points at a different image ID: ${image}"
      info "Reusing candidate tag already bound to the exact CI image"
    else
      docker tag "${image_ref}" "${image}"
    fi
  elif docker image inspect "${image}" >/dev/null 2>&1; then
    existing_revision="$(
      docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "${image}"
    )"
    existing_version="$(
      docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.version" }}' "${image}"
    )"
    [[ "${existing_revision}" == "${sha}" && "${existing_version}" == "${build_id}" ]] ||
      die "existing image tag has conflicting provenance: ${image}"
    info "Reusing already-attested local source-build image ${image}"
  else
    info "Building explicit local-recovery Studio candidate ${build_id} (${sha})"
    docker build \
      --build-arg "GIT_SHA=${sha}" \
      --build-arg "FRONTEND_BUILD_ID=${build_id}" \
      --label "org.opencontainers.image.revision=${sha}" \
      --label "org.opencontainers.image.version=${build_id}" \
      --tag "${image}" \
      "${REPO_ROOT}"
  fi

  existing_revision="$(
    docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "${image}"
  )"
  existing_version="$(
    docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.version" }}' "${image}"
  )"
  image_id="$(docker image inspect --format '{{.Id}}' "${image}")"
  [[ "${existing_revision}" == "${sha}" && "${existing_version}" == "${build_id}" ]] ||
    die "built image failed provenance attestation"
  [[ "${image_id}" =~ ^sha256:[0-9a-f]{64}$ ]] ||
    die "built image does not have an immutable local image ID"

  candidate_dir="${STUDIO_SHARED_DIR}/candidates"
  install -d -m 700 -- "${candidate_dir}"
  candidate_path="${candidate_dir}/${build_id}.env"
  tmp_candidate="${candidate_path}.tmp.$$"
  cp -- "${STUDIO_BASE_ENV}" "${tmp_candidate}"
  {
    printf '\nSTUDIO_IMAGE=%s\n' "${image}"
    printf 'STUDIO_IMAGE_ID=%s\n' "${image_id}"
    printf 'STUDIO_IMAGE_DIGEST_REF=%s\n' "${image_ref}"
    printf 'EXPECTED_GIT_SHA=%s\n' "${sha}"
    printf 'EXPECTED_BUILD_ID=%s\n' "${build_id}"
    printf 'RELEASE_DIR=%s\n' "${REPO_ROOT}"
    printf 'STUDIO_COMPOSE_FILE=%s\n' "${CONTABO_OPS_DIR}/docker-compose.yml"
    printf 'CREATED_AT_EPOCH=%s\n' "$(date -u +%s)"
  } >>"${tmp_candidate}"
  chmod 600 "${tmp_candidate}"
  mv -f -- "${tmp_candidate}" "${candidate_path}"
  assert_candidate_env "${candidate_path}"
  validate_studio_secrets "${candidate_path}"
  compose_for "${candidate_path}" config --quiet

  active_path=""
  if [[ -L "${STUDIO_SHARED_DIR}/active.env" ]]; then
    active_path="$(readlink -f -- "${STUDIO_SHARED_DIR}/active.env")"
  fi
  if [[ -n "${active_path}" ]]; then
    redis_health="$(docker inspect --format '{{ if .State.Health }}{{ .State.Health.Status }}{{ end }}' nyptid-studio-redis 2>/dev/null || true)"
    [[ "${redis_health}" == "healthy" ]] ||
      die "active Contabo release exists but its Redis is not healthy; refusing an implicit Redis replacement"
  else
    info "Starting local Redis only; Studio API/consumer remains stopped"
    compose_for "${candidate_path}" up -d redis
    for _ in $(seq 1 30); do
      redis_health="$(docker inspect --format '{{ if .State.Health }}{{ .State.Health.Status }}{{ end }}' nyptid-studio-redis 2>/dev/null || true)"
      [[ "${redis_health}" == "healthy" ]] && break
      sleep 2
    done
    [[ "${redis_health}" == "healthy" ]] || die "local Redis did not become healthy"
  fi

  atomic_symlink "${candidate_path}" "${STUDIO_SHARED_DIR}/staged.env"
  info "Staged ${candidate_path}"
  info "No Studio API/consumer was started"
}

validate_first_cutover_fence() {
  local fence_path="$1"
  local target_sha created now age
  local data_attestation data_attestation_sha expected_attestation_sha
  local data_manifest_sha data_file_count data_total_bytes manifest_path data_dir
  [[ -n "${fence_path}" ]] ||
    die "first activation requires a machine-verified Fly cutover fence"
  assert_private_file "${fence_path}"
  [[ "$(env_value "${fence_path}" FENCE_FORMAT || true)" == "2" ]] ||
    die "cutover fence format is obsolete or invalid"
  target_sha="$(env_value "${fence_path}" TARGET_GIT_SHA || true)"
  created="$(env_value "${fence_path}" CREATED_AT_EPOCH || true)"
  data_attestation="$(env_value "${fence_path}" DATA_READY_ATTESTATION_PATH || true)"
  expected_attestation_sha="$(env_value "${fence_path}" DATA_READY_ATTESTATION_SHA256 || true)"
  data_manifest_sha="$(env_value "${fence_path}" DATA_MANIFEST_SHA256 || true)"
  data_file_count="$(env_value "${fence_path}" DATA_FILE_COUNT || true)"
  data_total_bytes="$(env_value "${fence_path}" DATA_TOTAL_BYTES || true)"
  [[ "${target_sha}" == "$(env_value "${candidate}" EXPECTED_GIT_SHA)" ]] ||
    die "cutover fence targets a different Git SHA"
  [[ "${created}" =~ ^[0-9]+$ ]] || die "cutover fence timestamp is invalid"
  reverify_cutover_evidence "${fence_path}"
  [[ "${data_attestation}" == "${STUDIO_SHARED_DIR}/data-ready.attestation" ]] ||
    die "cutover fence does not reference the managed data readiness attestation"
  assert_private_file "${data_attestation}"
  data_attestation_sha="$(sha256sum "${data_attestation}" | awk '{print $1}')"
  [[ "${expected_attestation_sha}" =~ ^[0-9a-f]{64}$ &&
     "${data_attestation_sha}" == "${expected_attestation_sha}" ]] ||
    die "data readiness attestation changed after the cutover fence was issued"
  [[ "${data_manifest_sha}" =~ ^[0-9a-f]{64}$ ]] || die "cutover fence data manifest hash is invalid"
  [[ "${data_file_count}" =~ ^[1-9][0-9]*$ ]] || die "cutover fence data file count is not positive"
  [[ "${data_total_bytes}" =~ ^[1-9][0-9]*$ ]] || die "cutover fence data byte count is not positive"
  [[ "$(env_value "${data_attestation}" MANIFEST_SHA256 || true)" == "${data_manifest_sha}" ]] ||
    die "cutover fence is not bound to the attested data manifest"
  [[ "$(env_value "${data_attestation}" FILE_COUNT || true)" == "${data_file_count}" ]] ||
    die "cutover fence data file count does not match the attestation"
  [[ "$(env_value "${data_attestation}" TOTAL_BYTES || true)" == "${data_total_bytes}" ]] ||
    die "cutover fence data byte count does not match the attestation"
  manifest_path="$(env_value "${data_attestation}" MANIFEST_PATH || true)"
  [[ "${manifest_path}" == "${STUDIO_SHARED_DIR}/data-manifests/"* ]] ||
    die "data readiness attestation references an unmanaged manifest"
  data_dir="$(env_value "${candidate}" STUDIO_DATA_DIR)"
  python3 "${CONTABO_OPS_DIR}/data_manifest.py" check \
    --data-dir "${data_dir}" \
    --attestation "${data_attestation}" \
    --role migrated-data-ready
  now="$(date -u +%s)"
  age=$((now - created))
  (( age >= 0 && age <= 1800 )) || die "cutover fence is stale; stop/reverify the legacy consumer again"
}

reverify_cutover_evidence() {
  local fence_path="$1"
  local legacy_app legacy_machine_id legacy_origin evidence_dir
  local expected_bundle actual_bundle evidence_summary evidence_name
  legacy_app="$(env_value "${fence_path}" LEGACY_APP || true)"
  legacy_machine_id="$(env_value "${fence_path}" LEGACY_MACHINE_ID || true)"
  legacy_origin="$(env_value "${fence_path}" LEGACY_ORIGIN || true)"
  evidence_dir="$(env_value "${fence_path}" LEGACY_EVIDENCE_DIR || true)"
  expected_bundle="$(env_value "${fence_path}" LEGACY_EVIDENCE_BUNDLE_SHA256 || true)"
  [[ "${evidence_dir}" == "${STUDIO_SHARED_DIR}/fly-evidence/"* ]] ||
    die "cutover fence references an unmanaged Fly evidence directory"
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
  actual_bundle="$(
    printf '%s\n' "${evidence_summary}" |
      awk -F= '$1 == "BUNDLE_SHA256" { print $2; found=1; exit } END { if (!found) exit 1 }'
  )"
  [[ "${expected_bundle}" =~ ^[0-9a-f]{64}$ && "${actual_bundle}" == "${expected_bundle}" ]] ||
    die "bound Fly cutover evidence changed or no longer verifies"
}

activate_candidate() {
  local active_link previous_link current_link old_active="" candidate_release
  local count smoke_script rollback_smoke fence_archive disabled_marker
  local old_build candidate_build old_timestamp candidate_timestamp
  local first_fence_required=0

  [[ -z "${build_id}" ]] || die "activate does not accept --build-id"
  [[ -z "${image_ref}" ]] || die "activate does not accept --image-ref"
  if [[ -z "${candidate}" ]]; then
    [[ -L "${STUDIO_SHARED_DIR}/staged.env" ]] || die "no staged candidate is available"
    candidate="$(readlink -f -- "${STUDIO_SHARED_DIR}/staged.env")"
  fi
  assert_candidate_env "${candidate}"
  validate_studio_secrets "${candidate}"
  compose_for "${candidate}" config --quiet

  active_link="${STUDIO_SHARED_DIR}/active.env"
  previous_link="${STUDIO_SHARED_DIR}/previous.env"
  current_link="${STUDIO_INSTALL_ROOT}/current"
  disabled_marker="${STUDIO_SHARED_DIR}/consumer.disabled"
  for managed_link in "${active_link}" "${previous_link}" "${current_link}"; do
    if [[ ( -e "${managed_link}" || -L "${managed_link}" ) && ! -L "${managed_link}" ]]; then
      die "activation preflight found a non-symlink lifecycle path: ${managed_link}"
    fi
  done
  if [[ -L "${active_link}" ]]; then
    old_active="$(readlink -f -- "${active_link}")"
    assert_candidate_env "${old_active}"
    old_build="$(env_value "${old_active}" EXPECTED_BUILD_ID)"
    candidate_build="$(env_value "${candidate}" EXPECTED_BUILD_ID)"
    if [[ "${candidate}" != "${old_active}" ]]; then
      old_timestamp="$(build_timestamp_from_id "${old_build}")"
      candidate_timestamp="$(build_timestamp_from_id "${candidate_build}")"
      [[ "${candidate_timestamp}" > "${old_timestamp}" ]] ||
        die "normal activation cannot downgrade or reuse an older build timestamp; use rollback.sh"
    fi
  fi
  if [[ -z "${old_active}" || -f "${disabled_marker}" ]]; then
    validate_first_cutover_fence "${fence}"
    first_fence_required=1
  fi

  count="$(running_api_container_count)"
  (( count <= 1 )) || die "more than one Studio API container is running; refusing activation"

  info "Creating the candidate stopped, then verifying its immutable image ID"
  create_verified_api_container "${candidate}"
  count="$(running_api_container_count)"
  (( count == 0 )) || die "candidate preflight unexpectedly left an API container running"
  if (( first_fence_required )); then
    # This is intentionally the last external-owner check before the candidate
    # process receives production secrets and starts.
    reverify_cutover_evidence "${fence}"
  fi

  info "Starting the single verified Studio API/consumer owner"
  [[ ! -f "${disabled_marker}" ]] || rm -- "${disabled_marker}"
  compose_for "${candidate}" start studio-api
  count="$(running_api_container_count)"
  (( count == 1 )) || die "activation did not leave exactly one Studio API container"

  candidate_release="$(env_value "${candidate}" RELEASE_DIR)"
  smoke_script="${candidate_release}/ops/contabo/smoke.sh"
  if ! bash "${smoke_script}" --candidate "${candidate}" --attempts 60 --check-container-count; then
    info "Candidate smoke failed; restoring the prior single owner"
    if [[ -n "${old_active}" ]]; then
      rollback_smoke="$(env_value "${old_active}" RELEASE_DIR)/ops/contabo/smoke.sh"
      create_verified_api_container "${old_active}"
      compose_for "${old_active}" start studio-api
      bash "${rollback_smoke}" --candidate "${old_active}" --attempts 60 --check-container-count ||
        die "candidate and automatic rollback both failed health verification"
    else
      compose_for "${candidate}" stop -t 120 studio-api || true
      compose_for "${candidate}" rm -f studio-api || true
    fi
    die "candidate failed health verification and was not activated"
  fi

  if [[ -n "${old_active}" && "${old_active}" != "${candidate}" ]]; then
    atomic_symlink "${old_active}" "${previous_link}"
  fi
  atomic_symlink "${candidate}" "${active_link}"
  atomic_symlink "${candidate_release}" "${current_link}"

  if [[ -n "${fence}" ]]; then
    install -d -m 700 -- "${STUDIO_SHARED_DIR}/fences/used"
    fence_archive="${STUDIO_SHARED_DIR}/fences/used/$(basename -- "${fence}").used.$(date -u +%s)"
    mv -- "${fence}" "${fence_archive}"
  fi
  if [[ -L "${STUDIO_SHARED_DIR}/staged.env" ]] &&
     [[ "$(readlink -f -- "${STUDIO_SHARED_DIR}/staged.env")" == "${candidate}" ]]; then
    rm -- "${STUDIO_SHARED_DIR}/staged.env"
  fi

  info "Activated $(env_value "${candidate}" EXPECTED_BUILD_ID)"
  info "Health proves one Redis-backed consumer and immutable backend/frontend provenance"
}

case "${mode}" in
  stage) stage_candidate ;;
  activate) activate_candidate ;;
  *) usage ;;
esac
