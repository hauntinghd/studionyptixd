#!/usr/bin/env bash
set -Eeuo pipefail

printf '%s\n' \
  "RETIRED: RunPod setup is disabled; no credentials were read and no network or host mutation was attempted." \
  >&2
exit 78
