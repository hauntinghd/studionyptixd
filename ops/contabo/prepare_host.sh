#!/usr/bin/env bash

set -Eeuo pipefail
# shellcheck source=ops/contabo/lib.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)/lib.sh"

install_backup_timer=0
install_watchdog_timer=0
case "${1:-}" in
  "")
    ;;
  --install-backup-timer)
    install_backup_timer=1
    ;;
  --install-systemd-timers)
    install_backup_timer=1
    install_watchdog_timer=1
    ;;
  *)
    die "usage: $0 [--install-backup-timer|--install-systemd-timers]"
    ;;
esac
[[ $# -le 1 ]] || die "usage: $0 [--install-backup-timer|--install-systemd-timers]"

[[ "${EUID}" -eq 0 ]] || die "prepare_host.sh must run as root"
assert_safe_install_root
require_commands docker install openssl stat

caddy_network="${CADDY_NETWORK:-deploy_default}"
[[ "${caddy_network}" =~ ^[A-Za-z0-9_.-]+$ ]] || die "invalid CADDY_NETWORK"
docker network inspect "${caddy_network}" >/dev/null 2>&1 ||
  die "external Caddy network does not exist: ${caddy_network}"

info "Creating the isolated Studio host layout"
install -d -m 700 -- \
  "${STUDIO_INSTALL_ROOT}" \
  "${STUDIO_SHARED_DIR}" \
  "${STUDIO_SHARED_DIR}/candidates" \
  "${STUDIO_SHARED_DIR}/data-manifests" \
  "${STUDIO_SHARED_DIR}/fences" \
  "${STUDIO_SHARED_DIR}/fly-evidence" \
  "${STUDIO_SHARED_DIR}/reverse-manifests" \
  "${STUDIO_INSTALL_ROOT}/backups" \
  "${STUDIO_INSTALL_ROOT}/releases" \
  "${STUDIO_INSTALL_ROOT}/data"
install -d -m 770 -o 999 -g 999 -- "${STUDIO_INSTALL_ROOT}/redis"

studio_env="${STUDIO_SHARED_DIR}/studio.env"
if [[ ! -e "${studio_env}" ]]; then
  install -m 600 -- "${CONTABO_OPS_DIR}/studio.env.example" "${studio_env}"
  info "Created ${studio_env}; populate it before staging"
else
  assert_private_file "${studio_env}"
fi

if [[ ! -e "${STUDIO_BASE_ENV}" ]]; then
  redis_password="$(openssl rand -hex 32)"
  tmp_env="${STUDIO_BASE_ENV}.tmp.$$"
  umask 077
  {
    printf 'COMPOSE_PROJECT_NAME=%s\n' "${STUDIO_PROJECT_NAME}"
    printf 'STUDIO_DATA_DIR=%s\n' "${STUDIO_INSTALL_ROOT}/data"
    printf 'STUDIO_REDIS_DIR=%s\n' "${STUDIO_INSTALL_ROOT}/redis"
    printf 'STUDIO_ENV_FILE=%s\n' "${studio_env}"
    printf 'CADDY_NETWORK=%s\n' "${caddy_network}"
    printf 'STUDIO_LOOPBACK_PORT=%s\n' "${STUDIO_LOOPBACK_PORT:-10000}"
    printf 'STUDIO_MEMORY_LIMIT=%s\n' "${STUDIO_MEMORY_LIMIT:-4g}"
    printf 'STUDIO_CPU_LIMIT=%s\n' "${STUDIO_CPU_LIMIT:-4.0}"
    printf 'REDIS_MEMORY_LIMIT=%s\n' "${REDIS_MEMORY_LIMIT:-768m}"
    printf 'REDIS_CPU_LIMIT=%s\n' "${REDIS_CPU_LIMIT:-1.0}"
    printf 'STUDIO_INSTANCE_ID=%s\n' "${STUDIO_INSTANCE_ID:-studio-contabo-primary}"
    printf 'BACKUP_RETENTION_COUNT=%s\n' "${BACKUP_RETENTION_COUNT:-7}"
    printf 'REDIS_PASSWORD=%s\n' "${redis_password}"
  } >"${tmp_env}"
  chmod 600 "${tmp_env}"
  mv -f -- "${tmp_env}" "${STUDIO_BASE_ENV}"
else
  assert_private_file "${STUDIO_BASE_ENV}"
  redis_password="$(env_value "${STUDIO_BASE_ENV}" REDIS_PASSWORD || true)"
  [[ "${redis_password}" =~ ^[0-9a-f]{64}$ ]] || die "base.env has an invalid Redis password"
fi

if (( install_backup_timer )); then
  require_commands systemctl
  install -m 644 -- "${CONTABO_OPS_DIR}/studio-backup.service" /etc/systemd/system/studio-backup.service
  install -m 644 -- "${CONTABO_OPS_DIR}/studio-backup.timer" /etc/systemd/system/studio-backup.timer
  systemctl daemon-reload
  systemctl enable --now studio-backup.timer
  info "Enabled the daily quiesced local-recovery snapshot timer (not off-host DR)"
fi

if (( install_watchdog_timer )); then
  require_commands systemctl
  install -m 644 -- "${CONTABO_OPS_DIR}/studio-watchdog.service" /etc/systemd/system/studio-watchdog.service
  install -m 644 -- "${CONTABO_OPS_DIR}/studio-watchdog.timer" /etc/systemd/system/studio-watchdog.timer
  systemctl daemon-reload
  systemctl enable --now studio-watchdog.timer
  info "Enabled the single-consumer Studio health watchdog"
fi

info "Host layout ready; no Studio API process was started"
