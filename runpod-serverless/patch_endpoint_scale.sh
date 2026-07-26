#!/usr/bin/env bash

set -Eeuo pipefail

printf '%s\n' \
  "RETIRED: Studio scaling is Contabo-owned. No RunPod credentials were read and no endpoint mutation was attempted." \
  >&2
exit 78
