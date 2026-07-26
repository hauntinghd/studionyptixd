#!/usr/bin/env bash

set -Eeuo pipefail

cat >&2 <<'EOF'
ERROR: Fly secret migration is retired.

Contabo owns the canonical NYPTID Studio production backend. Changing Fly
secrets can restart the rollback machine and create a second production
consumer. Store production values in the root-owned mode-0600 file
/opt/studio/shared/studio.env and use the reviewed ops/contabo release flow.

If an emergency Fly rollback is required, first stop and fence the Contabo
consumer, reverse-sync persistent data, drain/attest queue state, and follow
ops/contabo/README.md. This script intentionally performs no Fly mutation.
EOF
exit 1
