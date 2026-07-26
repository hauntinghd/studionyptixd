#!/usr/bin/env bash

set -Eeuo pipefail
# shellcheck source=ops/contabo/lib.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/lib.sh"

assert_safe_install_root
require_commands docker rsync tar gzip sha256sum mktemp stat find sort tail cut
acquire_lifecycle_lock

active_link="${STUDIO_SHARED_DIR}/active.env"
[[ -L "${active_link}" ]] || die "no active Contabo release is recorded"
active="$(readlink -f -- "${active_link}")"
assert_candidate_env "${active}"

data_dir="$(env_value "${active}" STUDIO_DATA_DIR)"
retention="$(env_value "${active}" BACKUP_RETENTION_COUNT)"
[[ "${data_dir}" == "${STUDIO_INSTALL_ROOT}/data" && -d "${data_dir}" ]] ||
  die "unexpected or missing Studio data directory"
[[ "${retention}" =~ ^[1-9][0-9]*$ ]] ||
  die "BACKUP_RETENTION_COUNT must be positive"
[[ ! -f "${STUDIO_SHARED_DIR}/consumer.disabled" ]] ||
  die "backup is forbidden while the production consumer is disabled"

count="$(running_api_container_count)"
(( count == 1 )) || die "consistent backup requires exactly one API owner before quiesce"
pre_queue="$(queue_drain_snapshot)"
assert_queue_snapshot_drained "${pre_queue}"
pre_files="$(mktemp "${STUDIO_SHARED_DIR}/.backup-pre-files.XXXXXX")"
capture_file_quiescence "${active}" "${pre_files}"
rm -- "${pre_files}"

backup_root="${STUDIO_INSTALL_ROOT}/backups"
install -d -m 700 -- "${backup_root}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
snapshot_dir="${backup_root}/.snapshot-${timestamp}-$$"
archive_partial="${backup_root}/studio-${timestamp}.tar.gz.partial"
archive="${backup_root}/studio-${timestamp}.tar.gz"
archive_name="$(basename -- "${archive}")"
install -d -m 700 -- "${snapshot_dir}/data" "${snapshot_dir}/redis"
api_stopped=0

restore_api() {
  if (( api_stopped )); then
    info "Restoring the verified API owner after backup quiesce"
    create_verified_api_container "${active}" &&
      compose_for "${active}" start studio-api &&
      bash "$(env_value "${active}" RELEASE_DIR)/ops/contabo/smoke.sh" \
        --candidate "${active}" --attempts 60 --check-container-count
    api_stopped=0
  fi
}

cleanup() {
  restore_api || true
  if [[ -d "${snapshot_dir}" && "${snapshot_dir}" == "${backup_root}/.snapshot-"* ]]; then
    rm -rf -- "${snapshot_dir}"
  fi
  [[ ! -f "${archive_partial}" ]] || rm -- "${archive_partial}"
}
trap cleanup EXIT

info "Stopping admission and the sole consumer for a cross-store-consistent snapshot"
compose_for "${active}" stop -t 120 studio-api
api_stopped=1
count="$(running_api_container_count)"
(( count == 0 )) || die "API owner did not stop for consistent backup"

# This post-stop proof closes the small precheck-to-stop admission race. If a
# request entered during that interval, the backup aborts and the API is
# restored; no skewed archive is published.
post_queue="$(queue_drain_snapshot)"
assert_queue_snapshot_drained "${post_queue}"
post_files="${snapshot_dir}/file-quiescence.json"
capture_file_quiescence "${active}" "${post_files}"

info "Copying immutable /var/data while every application writer is stopped"
rsync --archive --numeric-ids --delete -- "${data_dir}/" "${snapshot_dir}/data/"

compose_for "${active}" exec -T redis sh -ec \
  'REDISCLI_AUTH="$REDIS_PASSWORD" redis-cli SAVE >/dev/null'
source_rdb_sha="$(
  docker exec nyptid-studio-redis sha256sum /data/dump.rdb | awk '{print $1}'
)"
[[ "${source_rdb_sha}" =~ ^[0-9a-f]{64}$ ]] ||
  die "Redis RDB source hash is invalid"
docker cp nyptid-studio-redis:/data/dump.rdb "${snapshot_dir}/redis/dump.rdb"
redis_rdb_sha="$(sha256sum "${snapshot_dir}/redis/dump.rdb" | awk '{print $1}')"
[[ "${redis_rdb_sha}" == "${source_rdb_sha}" ]] ||
  die "copied Redis RDB does not match the stopped-writer SAVE"
chmod 600 "${snapshot_dir}/redis/dump.rdb"

git_sha="$(env_value "${active}" EXPECTED_GIT_SHA)"
build_id="$(env_value "${active}" EXPECTED_BUILD_ID)"
cat >"${snapshot_dir}/metadata.json" <<EOF
{"format":2,"created_at":"${timestamp}","git_sha":"${git_sha}","build_id":"${build_id}","data_path":"/var/data","redis_snapshot":"dump.rdb","redis_rdb_sha256":"${redis_rdb_sha}","cross_store_alignment":"api-stopped-and-post-stop-quiescence-verified","recovery_class":"local-host-only-not-disaster-recovery"}
EOF
chmod 600 "${snapshot_dir}/metadata.json"

# Downtime ends once both immutable snapshot inputs exist. Compression cannot
# alter their source state.
restore_api

info "Compressing the quiesced local recovery snapshot"
tar --numeric-owner -C "${snapshot_dir}" -czf "${archive_partial}" .
chmod 600 "${archive_partial}"
mv -- "${archive_partial}" "${archive}"
(
  cd "${backup_root}"
  sha256sum "${archive_name}" >"${archive_name}.sha256"
)
chmod 600 "${archive}.sha256"
printf '%s\n' \
  'This unencrypted same-host archive is LOCAL RECOVERY ONLY and is not disaster recovery.' \
  >"${archive}.NOT_DISASTER_RECOVERY"
chmod 600 "${archive}.NOT_DISASTER_RECOVERY"

mapfile -t expired < <(
  find "${backup_root}" -maxdepth 1 -type f -name 'studio-*.tar.gz' -printf '%T@ %p\n' |
    sort -nr |
    tail -n "+$((retention + 1))" |
    cut -d' ' -f2-
)
for expired_archive in "${expired[@]}"; do
  [[ "${expired_archive}" == "${backup_root}/studio-"*.tar.gz ]] ||
    die "retention selected an unsafe path"
  rm -- "${expired_archive}"
  for sidecar in \
    "${expired_archive}.sha256" \
    "${expired_archive}.NOT_DISASTER_RECOVERY"; do
    [[ ! -f "${sidecar}" ]] || rm -- "${sidecar}"
  done
done

info "Consistent local recovery snapshot complete: ${archive}"
info "NOT DR: configure encrypted, verified off-host storage before claiming disaster recovery"
