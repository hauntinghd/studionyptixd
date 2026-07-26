#!/usr/bin/env bash

set -Eeuo pipefail
# shellcheck source=ops/contabo/lib.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/lib.sh"

usage() {
  cat >&2 <<'EOF'
Usage:
  restic_offsite.sh init [--config /opt/studio/shared/restic-s3.env]
  restic_offsite.sh backup --snapshot-dir <quiesced-snapshot> --build-id <studio-build-id>
                           [--config /opt/studio/shared/restic-s3.env]
  restic_offsite.sh check [--config /opt/studio/shared/restic-s3.env]
  restic_offsite.sh restore-check [--config /opt/studio/shared/restic-s3.env]
  restic_offsite.sh maintenance [--config /opt/studio/shared/restic-s3.env]

The configuration and repository password files must be root-owned mode 0600.
Only HTTPS S3-compatible repositories are accepted. Repository credentials and
the repository password are never accepted as command-line arguments.
EOF
  exit 2
}

mode="${1:-}"
[[ -n "${mode}" ]] || usage
shift

config="${STUDIO_RESTIC_CONFIG:-${STUDIO_SHARED_DIR}/restic-s3.env}"
snapshot_dir=""
build_id=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      [[ $# -ge 2 ]] || usage
      config="$2"
      shift 2
      ;;
    --snapshot-dir)
      [[ $# -ge 2 ]] || usage
      snapshot_dir="$2"
      shift 2
      ;;
    --build-id)
      [[ $# -ge 2 ]] || usage
      build_id="$2"
      shift 2
      ;;
    *)
      usage
      ;;
  esac
done

[[ "${EUID}" -eq 0 ]] || die "restic_offsite.sh must run as root"
assert_safe_install_root
require_commands awk find flock install mktemp python3 readlink restic sha256sum stat
assert_private_file "${config}"

repository="$(env_value "${config}" RESTIC_REPOSITORY 2>/dev/null || true)"
password_file="$(env_value "${config}" RESTIC_PASSWORD_FILE 2>/dev/null || true)"
access_key="$(env_value "${config}" AWS_ACCESS_KEY_ID 2>/dev/null || true)"
secret_key="$(env_value "${config}" AWS_SECRET_ACCESS_KEY 2>/dev/null || true)"
region="$(env_value "${config}" AWS_DEFAULT_REGION 2>/dev/null || true)"
instance_id="$(env_value "${STUDIO_BASE_ENV}" STUDIO_INSTANCE_ID 2>/dev/null || true)"
instance_id="${instance_id:-studio-contabo-primary}"

[[ "${repository}" =~ ^s3:https://[^[:space:]]+$ ]] ||
  die "RESTIC_REPOSITORY must be a non-empty HTTPS S3-compatible repository"
[[ "${password_file}" == /* ]] ||
  die "RESTIC_PASSWORD_FILE must be an absolute path"
assert_private_file "${password_file}"
[[ -n "${access_key}" ]] || die "AWS_ACCESS_KEY_ID is missing"
[[ -n "${secret_key}" ]] || die "AWS_SECRET_ACCESS_KEY is missing"
region="${region:-default}"
[[ "${region}" != *[$' \t\r\n']* ]] || die "AWS_DEFAULT_REGION contains whitespace"

backup_root="${STUDIO_INSTALL_ROOT}/backups"
cache_dir="${backup_root}/.restic-cache"
install -d -m 700 -- "${backup_root}" "${cache_dir}"
exec 8>"${STUDIO_SHARED_DIR}/restic-offsite.lock"
flock -x 8

export RESTIC_REPOSITORY="${repository}"
export RESTIC_PASSWORD_FILE="${password_file}"
export RESTIC_CACHE_DIR="${cache_dir}"
export AWS_ACCESS_KEY_ID="${access_key}"
export AWS_SECRET_ACCESS_KEY="${secret_key}"
export AWS_DEFAULT_REGION="${region}"
export AWS_REGION="${region}"

write_success_marker() {
  local marker_name="$1"
  local marker="${STUDIO_SHARED_DIR}/${marker_name}"
  local tmp="${marker}.tmp.$$"
  umask 077
  {
    printf 'COMPLETED_AT_EPOCH=%s\n' "$(date -u +%s)"
    printf 'INSTANCE_ID=%s\n' "${instance_id}"
    if [[ -n "${build_id}" ]]; then
      printf 'BUILD_ID=%s\n' "${build_id}"
    fi
  } >"${tmp}"
  chmod 600 "${tmp}"
  mv -f -- "${tmp}" "${marker}"
}

case "${mode}" in
  init)
    [[ -z "${snapshot_dir}" && -z "${build_id}" ]] || usage
    restic init
    info "Initialized encrypted off-host restic repository"
    ;;
  backup)
    [[ -n "${snapshot_dir}" && -n "${build_id}" ]] || usage
    [[ "${build_id}" =~ ^studio-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$ ]] ||
      die "invalid Studio build ID"
    snapshot_dir="$(readlink -f -- "${snapshot_dir}")"
    case "${snapshot_dir}" in
      "${backup_root}/.snapshot-"*) ;;
      *) die "snapshot directory is outside the managed backup root" ;;
    esac
    [[ -d "${snapshot_dir}/data" ]] || die "snapshot data directory is missing"
    [[ -f "${snapshot_dir}/redis/dump.rdb" ]] || die "snapshot Redis dump is missing"
    [[ -f "${snapshot_dir}/metadata.json" ]] || die "snapshot metadata is missing"
    metadata_build="$(
      python3 - "${snapshot_dir}/metadata.json" <<'PY'
import json
import sys

payload = json.loads(open(sys.argv[1], encoding="utf-8").read())
print(str(payload.get("build_id") or ""))
PY
    )"
    [[ "${metadata_build}" == "${build_id}" ]] ||
      die "snapshot metadata build does not match requested release"
    (
      cd "${snapshot_dir}"
      restic backup . \
        --host "${instance_id}" \
        --tag nyptid-studio \
        --tag cross-store-consistent \
        --tag "release:${build_id}"
    )
    write_success_marker "offsite-last-success"
    info "Encrypted off-host snapshot completed for ${build_id}"
    ;;
  check)
    [[ -z "${snapshot_dir}" && -z "${build_id}" ]] || usage
    restic check
    write_success_marker "offsite-check-last-success"
    info "Off-host restic repository metadata and pack structure verified"
    ;;
  restore-check)
    [[ -z "${snapshot_dir}" && -z "${build_id}" ]] || usage
    restore_root="$(mktemp -d "${backup_root}/.restic-restore-check.XXXXXX")"
    cleanup_restore() {
      if [[ -d "${restore_root}" && "${restore_root}" == "${backup_root}/.restic-restore-check."* ]]; then
        rm -rf -- "${restore_root}"
      fi
    }
    trap cleanup_restore EXIT
    restic restore latest \
      --host "${instance_id}" \
      --tag nyptid-studio \
      --tag cross-store-consistent \
      --target "${restore_root}"
    [[ -f "${restore_root}/metadata.json" ]] ||
      die "restored snapshot metadata is missing"
    [[ -f "${restore_root}/redis/dump.rdb" ]] ||
      die "restored Redis dump is missing"
    [[ -d "${restore_root}/data" ]] ||
      die "restored data directory is missing"
    [[ -n "$(find "${restore_root}/data" -type f -print -quit)" ]] ||
      die "restored data directory contains no files"
    expected_rdb_sha="$(
      python3 - "${restore_root}/metadata.json" <<'PY'
import json
import re
import sys

payload = json.loads(open(sys.argv[1], encoding="utf-8").read())
if int(payload.get("format") or 0) != 2:
    raise SystemExit("unsupported backup metadata format")
value = str(payload.get("redis_rdb_sha256") or "")
if not re.fullmatch(r"[0-9a-f]{64}", value):
    raise SystemExit("invalid Redis digest in backup metadata")
print(value)
PY
    )"
    actual_rdb_sha="$(sha256sum "${restore_root}/redis/dump.rdb" | awk '{print $1}')"
    [[ "${actual_rdb_sha}" == "${expected_rdb_sha}" ]] ||
      die "restored Redis dump failed its recorded SHA-256"
    write_success_marker "offsite-restore-check-last-success"
    info "Latest encrypted off-host snapshot restored and verified in isolation"
    ;;
  maintenance)
    [[ -z "${snapshot_dir}" && -z "${build_id}" ]] || usage
    keep_daily="$(env_value "${config}" RESTIC_KEEP_DAILY 2>/dev/null || true)"
    keep_weekly="$(env_value "${config}" RESTIC_KEEP_WEEKLY 2>/dev/null || true)"
    keep_monthly="$(env_value "${config}" RESTIC_KEEP_MONTHLY 2>/dev/null || true)"
    keep_daily="${keep_daily:-7}"
    keep_weekly="${keep_weekly:-5}"
    keep_monthly="${keep_monthly:-12}"
    for value in "${keep_daily}" "${keep_weekly}" "${keep_monthly}"; do
      [[ "${value}" =~ ^[1-9][0-9]*$ ]] ||
        die "restic retention values must be positive integers"
    done
    restic forget \
      --host "${instance_id}" \
      --tag nyptid-studio \
      --keep-daily "${keep_daily}" \
      --keep-weekly "${keep_weekly}" \
      --keep-monthly "${keep_monthly}" \
      --prune
    restic check
    write_success_marker "offsite-maintenance-last-success"
    info "Off-host retention, prune, and repository verification completed"
    ;;
  *)
    usage
    ;;
esac
