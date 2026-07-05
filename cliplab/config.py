"""ClipLab paths and tunables."""
from __future__ import annotations

import os
from pathlib import Path

from backend_settings import TEMP_DIR

CLIPLAB_DIR = TEMP_DIR / "cliplab"
CLIPLAB_UPLOAD_DIR = CLIPLAB_DIR / "uploads"
CLIPLAB_TRANSCRIPT_DIR = CLIPLAB_DIR / "transcripts"
CLIPLAB_RENDER_DIR = CLIPLAB_DIR / "renders"
CLIPLAB_JOBS_DIR = CLIPLAB_DIR / "jobs"

for _d in (CLIPLAB_DIR, CLIPLAB_UPLOAD_DIR, CLIPLAB_TRANSCRIPT_DIR, CLIPLAB_RENDER_DIR, CLIPLAB_JOBS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# RunPod network volume mount (training + custom inference weights)
RUNPOD_VOLUME_ROOT = Path(os.getenv("STUDIO_APP_DATA_DIR", "/runpod-volume/studio"))
CLIPLAB_MODELS_DIR = RUNPOD_VOLUME_ROOT / "cliplab" / "models"
CLIPLAB_DATASETS_DIR = RUNPOD_VOLUME_ROOT / "cliplab" / "datasets"

# Billing: 1 credit per minute of source video (rounded up)
CLIPLAB_CREDITS_PER_MINUTE = int(os.getenv("CLIPLAB_CREDITS_PER_MINUTE", "1"))
CLIPLAB_MIN_CREDITS = int(os.getenv("CLIPLAB_MIN_CREDITS", "3"))
CLIPLAB_MAX_RENDER_CLIPS = int(os.getenv("CLIPLAB_MAX_RENDER_CLIPS", "20"))

# Segment limits
CLIPLAB_MAX_SEGMENT_SEC = float(os.getenv("CLIPLAB_MAX_SEGMENT_SEC", "60"))
CLIPLAB_MIN_SEGMENT_SEC = float(os.getenv("CLIPLAB_MIN_SEGMENT_SEC", "8"))

# Reframe output
CLIPLAB_OUTPUT_WIDTH = 1080
CLIPLAB_OUTPUT_HEIGHT = 1920
CLIPLAB_OUTPUT_FPS = 30

# Model backend selection (swap when RunPod weights are trained)
VIRALITY_BACKEND = os.getenv("CLIPLAB_VIRALITY_BACKEND", "local_llm")  # local_llm | runpod_custom_v1
REFRAME_BACKEND = os.getenv("CLIPLAB_REFRAME_BACKEND", "opencv_face")  # opencv_face | runpod_face_v1

RUNPOD_CLIPLAB_ENDPOINT = os.getenv("RUNPOD_CLIPLAB_ENDPOINT_ID", "").strip()
RUNPOD_CLIPLAB_URL = (
    f"https://api.runpod.ai/v2/{RUNPOD_CLIPLAB_ENDPOINT}/runsync"
    if RUNPOD_CLIPLAB_ENDPOINT
    else ""
)

# Temporary internal provider bridge while Catalyst learns enough to replace it.
# Keep the key server-side only. Never expose this to the frontend bundle.
CLIPLAB_PROVIDER = os.getenv("CLIPLAB_PROVIDER", "local").strip().lower()  # local | opus | hybrid
OPUSCLIP_API_BASE = os.getenv("OPUSCLIP_API_BASE", "https://api.opus.pro/api").rstrip("/")
OPUSCLIP_API_KEY = os.getenv("OPUSCLIP_API_KEY", "").strip()
OPUSCLIP_ORG_ID = os.getenv("OPUSCLIP_ORG_ID", "").strip()
OPUSCLIP_POLL_INTERVAL_SEC = float(os.getenv("OPUSCLIP_POLL_INTERVAL_SEC", "20"))
OPUSCLIP_POLL_TIMEOUT_SEC = float(os.getenv("OPUSCLIP_POLL_TIMEOUT_SEC", "2400"))
