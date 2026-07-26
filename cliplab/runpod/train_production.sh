#!/usr/bin/env bash
# ClipLab production training — mass bootstrap + max epochs
set -euo pipefail

ROOT="${STUDIO_APP_DATA_DIR:-/workspace/studio}"
RUNPOD_DIR="$(cd "$(dirname "$0")" && pwd)"
PY="${PYTHON:-python3}"
VENV_PY="/workspace/JJRR/.venv/bin/python"
[[ -x "$VENV_PY" ]] && PY="$VENV_PY"

export STUDIO_APP_DATA_DIR="$ROOT"
export PYTHONPATH="${RUNPOD_DIR}:${PYTHONPATH:-}"

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*"; }

log "=== ClipLab PRODUCTION TRAINING ==="
mkdir -p "$ROOT/cliplab/datasets" "$ROOT/cliplab/models/virality/v1" "$ROOT/cliplab/models/reframe/v1"

log "Mass bootstrap feedback (8000 rows)"
"$PY" "$RUNPOD_DIR/bootstrap_feedback_mass.py" --count 8000 --merge

log "Mass bootstrap reframe (1200 trajectories)"
"$PY" "$RUNPOD_DIR/bootstrap_reframe_mass.py" --count 1200 --merge

FEEDBACK_ROWS=$(wc -l < "$ROOT/cliplab/datasets/cliplab_feedback.jsonl")
REFRAME_ROWS=$(wc -l < "$ROOT/cliplab/datasets/cliplab_reframe.jsonl")
log "Datasets: feedback=$FEEDBACK_ROWS reframe=$REFRAME_ROWS"

log "Train virality reranker (50 epochs, batch 64)"
"$PY" "$RUNPOD_DIR/train_virality_scorer.py" \
  --dataset "$ROOT/cliplab/datasets/cliplab_feedback.jsonl" \
  --out "$ROOT/cliplab/models/virality/v1" \
  --epochs 50 \
  --batch-size 64 \
  --lr 5e-4

log "Train face reframe tracker (40 epochs, batch 16)"
"$PY" "$RUNPOD_DIR/train_face_reframe.py" \
  --dataset "$ROOT/cliplab/datasets/cliplab_reframe.jsonl" \
  --out "$ROOT/cliplab/models/reframe/v1" \
  --epochs 40 \
  --batch-size 16 \
  --lr 5e-4

log "=== ClipLab OFFLINE TRAINING COMPLETE (artifacts not activated) ==="
ls -lh "$ROOT/cliplab/models/virality/v1/model.pt" "$ROOT/cliplab/models/reframe/v1/tracker.pt"
