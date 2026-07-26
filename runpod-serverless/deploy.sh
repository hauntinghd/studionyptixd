#!/usr/bin/env bash

set -Eeuo pipefail

printf '%s\n' \
  "RETIRED: Studio backend deployment is Contabo-owned. No RunPod credentials were read and no network mutation was attempted." \
  >&2
exit 78
