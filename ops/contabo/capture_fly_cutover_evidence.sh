#!/usr/bin/env bash

set -Eeuo pipefail

usage() {
  cat >&2 <<'EOF'
Usage:
  capture_fly_cutover_evidence.sh \
    --app nyptid-studio \
    --machine-id <machine-id> \
    --origin https://nyptid-studio.fly.dev \
    --queue-evidence <legacy-queue.json> \
    --file-evidence <file-quiescence.json> \
    --output-dir <new-empty-directory>

Run only after the copy-only Fly machine has been stopped. The queue and file
proofs must have been captured after public services were removed.
EOF
  exit 2
}

app=""
machine_id=""
origin=""
queue_evidence=""
file_evidence=""
output_dir=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --app) app="${2:-}"; shift 2 ;;
    --machine-id) machine_id="${2:-}"; shift 2 ;;
    --origin) origin="${2:-}"; shift 2 ;;
    --queue-evidence) queue_evidence="${2:-}"; shift 2 ;;
    --file-evidence) file_evidence="${2:-}"; shift 2 ;;
    --output-dir) output_dir="${2:-}"; shift 2 ;;
    *) usage ;;
  esac
done

[[ "${app}" =~ ^[a-z0-9][a-z0-9-]{1,62}$ ]] || usage
[[ "${machine_id}" =~ ^[A-Za-z0-9]+$ ]] || usage
[[ "${origin}" == https://* && "${origin#https://}" != */* ]] || usage
[[ -f "${queue_evidence}" && ! -L "${queue_evidence}" ]] || usage
[[ -f "${file_evidence}" && ! -L "${file_evidence}" ]] || usage
[[ -n "${output_dir}" && ! -e "${output_dir}" ]] || usage
for command_name in fly curl python3 install; do
  command -v "${command_name}" >/dev/null 2>&1 ||
    { printf 'ERROR: required command not found: %s\n' "${command_name}" >&2; exit 1; }
done

umask 077
install -d -m 700 -- "${output_dir}"
install -m 600 -- "${queue_evidence}" "${output_dir}/legacy-queue.json"
install -m 600 -- "${file_evidence}" "${output_dir}/file-quiescence.json"

extract_machine() {
  local list_path="$1"
  local machine_path="$2"
  python3 - "${list_path}" "${machine_path}" "${machine_id}" <<'PY'
import json
import os
import sys

source, destination, expected_id = sys.argv[1:]
with open(source, "r", encoding="utf-8") as handle:
    machines = json.load(handle)
if not isinstance(machines, list):
    raise SystemExit("Fly machine list is not an array")
matches = [
    machine
    for machine in machines
    if isinstance(machine, dict) and str(machine.get("id") or "") == expected_id
]
if len(matches) != 1:
    raise SystemExit(
        f"expected exactly one Fly machine {expected_id}; found {len(matches)}"
    )
temporary = destination + ".tmp"
with open(temporary, "w", encoding="utf-8") as handle:
    json.dump(matches[0], handle, sort_keys=True, separators=(",", ":"))
    handle.write("\n")
    handle.flush()
    os.fsync(handle.fileno())
os.chmod(temporary, 0o600)
os.replace(temporary, destination)
PY
}

fly machine list --app "${app}" --json \
  >"${output_dir}/machine-list-before.json"
extract_machine \
  "${output_dir}/machine-list-before.json" \
  "${output_dir}/machine-before.json"
# flyctl 0.4.73 emits remote application config as JSON by default and does
# not accept a --json flag for this command.
fly config show --app "${app}" >"${output_dir}/app-config.json"

attempts_tsv="${output_dir}/.origin-attempts.tsv"
: >"${attempts_tsv}"
for attempt in 1 2 3; do
  started="$(date -u +%s)"
  set +e
  http_status="$(
    curl \
      --silent \
      --show-error \
      --fail-with-body \
      --connect-timeout 5 \
      --max-time 15 \
      --output /dev/null \
      --write-out '%{http_code}' \
      "${origin}/api/health" 2>"${output_dir}/.origin-${attempt}.stderr"
  )"
  curl_exit="$?"
  set -e
  [[ "${http_status}" =~ ^[0-9]{3}$ ]] || http_status="000"
  printf '%s\t%s\t%s\t%s\n' \
    "${attempt}" "${started}" "${curl_exit}" "$((10#${http_status}))" \
    >>"${attempts_tsv}"
  sleep 2
done

captured="$(date -u +%s)"
python3 - \
  "${attempts_tsv}" "${output_dir}/origin-probe.json" \
  "${app}" "${machine_id}" "${origin}" "${captured}" <<'PY'
import json
import os
import sys

source, destination, app, machine_id, origin, captured = sys.argv[1:]
attempts = []
with open(source, "r", encoding="utf-8") as handle:
    for line in handle:
        index, started, curl_exit, http_status = line.rstrip("\n").split("\t")
        attempts.append(
            {
                "attempt": int(index),
                "started_at_epoch": int(started),
                "curl_exit_code": int(curl_exit),
                "http_status": int(http_status),
            }
        )
payload = {
    "format": 1,
    "app": app,
    "machine_id": machine_id,
    "origin": origin,
    "captured_at_epoch": int(captured),
    "attempts": attempts,
}
temporary = destination + ".tmp"
with open(temporary, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
    handle.write("\n")
    handle.flush()
    os.fsync(handle.fileno())
os.chmod(temporary, 0o600)
os.replace(temporary, destination)
PY

sleep 5
fly machine list --app "${app}" --json \
  >"${output_dir}/machine-list-after.json"
extract_machine \
  "${output_dir}/machine-list-after.json" \
  "${output_dir}/machine-after.json"
rm -- "${attempts_tsv}" "${output_dir}"/.origin-*.stderr
chmod 600 "${output_dir}"/*.json

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
python3 "${script_dir}/fly_cutover_evidence.py" \
  --evidence-dir "${output_dir}" \
  --app "${app}" \
  --machine-id "${machine_id}" \
  --origin "${origin}" \
  --max-age-seconds 1800 >/dev/null
printf 'Verified Fly evidence directory: %s\n' "${output_dir}"
