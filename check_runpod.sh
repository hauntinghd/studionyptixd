#!/usr/bin/env bash
set -Eeuo pipefail

printf '%s\n' \
  "RETIRED: RunPod operator checks are disabled; Studio production is Contabo-owned." \
  >&2
exit 78
