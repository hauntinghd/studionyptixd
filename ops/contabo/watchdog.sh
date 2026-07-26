#!/usr/bin/env bash

set -Eeuo pipefail
# shellcheck source=ops/contabo/lib.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/lib.sh"

assert_safe_install_root
require_commands docker flock stat
install -d -m 700 -- "${STUDIO_SHARED_DIR}"

# A deploy, rollback, or backup already holding this lock owns lifecycle
# decisions. A watchdog tick skips instead of competing with it.
exec 9>"${STUDIO_LOCK_FILE}"
flock -n 9 || exit 0

[[ ! -f "${STUDIO_SHARED_DIR}/consumer.disabled" ]] || exit 0
[[ -L "${STUDIO_SHARED_DIR}/active.env" ]] || exit 0
active="$(readlink -f -- "${STUDIO_SHARED_DIR}/active.env")"
assert_candidate_env "${active}"

count="$(running_api_container_count)"
(( count <= 1 )) || die "watchdog found multiple Studio API containers and will not guess an owner"

if (( count == 0 )); then
  info "Watchdog found no enabled Studio API; starting the recorded single owner"
  create_verified_api_container "${active}"
  compose_for "${active}" start studio-api
else
  container_id="$(
    docker ps \
      --filter "label=com.docker.compose.project=${STUDIO_PROJECT_NAME}" \
      --filter "label=com.docker.compose.service=studio-api" \
      --format '{{.ID}}'
  )"
  expected_image_id="$(env_value "${active}" STUDIO_IMAGE_ID)"
  actual_image_id="$(docker inspect --format '{{.Image}}' "${container_id}")"
  [[ "${actual_image_id}" == "${expected_image_id}" ]] ||
    die "watchdog refuses to restart a container with an unattested image ID"
  health="$(docker inspect --format '{{ if .State.Health }}{{ .State.Health.Status }}{{ end }}' "${container_id}")"
  case "${health}" in
    healthy|starting)
      exit 0
      ;;
    *)
      info "Watchdog is gracefully restarting the unhealthy single owner"
      compose_for "${active}" restart -t 120 studio-api
      ;;
  esac
fi

release_dir="$(env_value "${active}" RELEASE_DIR)"
bash "${release_dir}/ops/contabo/smoke.sh" \
  --candidate "${active}" \
  --attempts 60 \
  --check-container-count
