#!/usr/bin/env bash
set -Eeuo pipefail

printf '%s\n' \
  "RETIRED: ClipLab RunPod image publication is disabled; no registry mutation was attempted." \
  >&2
exit 78
