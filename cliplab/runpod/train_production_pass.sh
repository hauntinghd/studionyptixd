#!/usr/bin/env bash
# ClipLab production training — escalating passes (v1 foundation → v5 frontier)
set -euo pipefail

PASS="${1:-v2}"
ROOT="${STUDIO_APP_DATA_DIR:-/workspace/studio}"
RUNPOD_DIR="$(cd "$(dirname "$0")" && pwd)"
PY="${PYTHON:-python3}"
VENV_PY="/workspace/JJRR/.venv/bin/python"
[[ -x "$VENV_PY" ]] && PY="$VENV_PY"

export STUDIO_APP_DATA_DIR="$ROOT"
export PYTHONPATH="${RUNPOD_DIR}:${PYTHONPATH:-}"
export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES:-}"

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) [cliplab-$PASS] $*"; }

case "$PASS" in
  v1)
    FB_COUNT=8000; RF_COUNT=1200; V_EPOCHS=50; R_EPOCHS=40; V_LR=5e-4; R_LR=5e-4; V_BATCH=64; R_BATCH=16
    OUT_V="$ROOT/cliplab/models/virality/v1"; OUT_R="$ROOT/cliplab/models/reframe/v1"
    ;;
  v2)
    FB_COUNT=15000; RF_COUNT=2500; V_EPOCHS=80; R_EPOCHS=60; V_LR=2e-4; R_LR=2e-4; V_BATCH=64; R_BATCH=16
    OUT_V="$ROOT/cliplab/models/virality/v2"; OUT_R="$ROOT/cliplab/models/reframe/v2"
    ;;
  v3)
    FB_COUNT=20000; RF_COUNT=4000; V_EPOCHS=100; R_EPOCHS=80; V_LR=1e-4; R_LR=1e-4; V_BATCH=48; R_BATCH=12
    OUT_V="$ROOT/cliplab/models/virality/v3"; OUT_R="$ROOT/cliplab/models/reframe/v3"
    ;;
  v4|v5)
    FB_COUNT=25000; RF_COUNT=5000; V_EPOCHS=120; R_EPOCHS=100; V_LR=5e-5; R_LR=5e-5; V_BATCH=32; R_BATCH=8
    OUT_V="$ROOT/cliplab/models/virality/${PASS#v}"; OUT_R="$ROOT/cliplab/models/reframe/${PASS#v}"
    ;;
  *)
    echo "Usage: train_production_pass.sh [v1|v2|v3|v4|v5]" >&2
    exit 1
    ;;
esac

log "=== ClipLab PASS $PASS ==="
mkdir -p "$ROOT/cliplab/datasets" "$OUT_V" "$OUT_R"

log "Bootstrap feedback ($FB_COUNT rows, merge)"
"$PY" "$RUNPOD_DIR/bootstrap_feedback_mass.py" --count "$FB_COUNT" --merge --seed "$((42 + ${PASS#v}))"

log "Bootstrap reframe ($RF_COUNT trajectories, merge)"
"$PY" "$RUNPOD_DIR/bootstrap_reframe_mass.py" --count "$RF_COUNT" --merge --seed "$((99 + ${PASS#v}))"

log "Train virality ($V_EPOCHS epochs lr=$V_LR)"
"$PY" "$RUNPOD_DIR/train_virality_scorer.py" \
  --dataset "$ROOT/cliplab/datasets/cliplab_feedback.jsonl" \
  --out "$OUT_V" \
  --epochs "$V_EPOCHS" \
  --batch-size "$V_BATCH" \
  --lr "$V_LR"

log "Train reframe ($R_EPOCHS epochs lr=$R_LR)"
"$PY" "$RUNPOD_DIR/train_face_reframe.py" \
  --dataset "$ROOT/cliplab/datasets/cliplab_reframe.jsonl" \
  --out "$OUT_R" \
  --epochs "$R_EPOCHS" \
  --batch-size "$R_BATCH" \
  --lr "$R_LR"

# Flip registry to latest pass
REG="$ROOT/cliplab/models/model_registry.json"
if [[ -f "$REG" ]]; then
  "$PY" -c "
import json
from pathlib import Path
p = Path('$REG')
d = json.loads(p.read_text())
d['virality_scorer']['active'] = 'runpod_custom_${PASS}'
d['face_reframe']['active'] = 'runpod_face_${PASS}'
d['virality_scorer']['checkpoints']['runpod_custom_${PASS}'] = {
    'path': 'virality/${PASS#v}/model.pt', 'config': 'virality/${PASS#v}/config.json', 'status': 'ready'
}
d['face_reframe']['checkpoints']['runpod_face_${PASS}'] = {
    'path': 'reframe/${PASS#v}/tracker.pt', 'config': 'reframe/${PASS#v}/config.json', 'status': 'ready'
}
p.write_text(json.dumps(d, indent=2) + '\n')
print('registry -> ${PASS}')
"
fi

log "=== ClipLab PASS $PASS COMPLETE ==="
ls -lh "$OUT_V/model.pt" "$OUT_R/tracker.pt"
