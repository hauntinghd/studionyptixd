#!/usr/bin/env bash
# Full ClipLab training pipeline on a RunPod GPU pod with volume mounted.
set -euo pipefail

export STUDIO_APP_DATA_DIR="${STUDIO_APP_DATA_DIR:-/runpod-volume/studio}"
REPO_ROOT="${REPO_ROOT:-/workspace/repo}"
export PYTHONPATH="${REPO_ROOT}:${PYTHONPATH:-}"
cd "${REPO_ROOT}"

pip install -q torch torchvision opencv-python-headless sentence-transformers

bash cliplab/runpod/setup_volume.sh

python cliplab/runpod/bootstrap_feedback.py
python cliplab/runpod/bootstrap_opencv_reframe.py

python cliplab/runpod/train_virality_scorer.py \
  --dataset "${STUDIO_APP_DATA_DIR}/cliplab/datasets/cliplab_feedback.jsonl" \
  --out "${STUDIO_APP_DATA_DIR}/cliplab/models/virality/v1" \
  --epochs "${VIRALITY_EPOCHS:-5}"

python cliplab/runpod/train_face_reframe.py \
  --dataset "${STUDIO_APP_DATA_DIR}/cliplab/datasets/cliplab_reframe.jsonl" \
  --out "${STUDIO_APP_DATA_DIR}/cliplab/models/reframe/v1" \
  --epochs "${REFRAME_EPOCHS:-10}"

echo "Offline training complete. Artifacts were not activated for production."
echo "Studio and ClipLab production execution remain Contabo-owned."
