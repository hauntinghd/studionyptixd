"""ClipLab paths and tunables."""
from __future__ import annotations

import os
from pathlib import Path

from backend_settings import APP_DATA_DIR, TEMP_DIR

CLIPLAB_DIR = TEMP_DIR / "cliplab"
CLIPLAB_UPLOAD_DIR = CLIPLAB_DIR / "uploads"
CLIPLAB_TRANSCRIPT_DIR = CLIPLAB_DIR / "transcripts"
CLIPLAB_RENDER_DIR = CLIPLAB_DIR / "renders"
CLIPLAB_JOBS_DIR = CLIPLAB_DIR / "jobs"

for _d in (CLIPLAB_DIR, CLIPLAB_UPLOAD_DIR, CLIPLAB_TRANSCRIPT_DIR, CLIPLAB_RENDER_DIR, CLIPLAB_JOBS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# Persistent Studio data root. The API's canonical APP_DATA_DIR owns ClipLab;
# legacy RunPod volume variables are intentionally ignored.
STUDIO_DATA_ROOT = Path(APP_DATA_DIR)
CLIPLAB_MODELS_DIR = STUDIO_DATA_ROOT / "cliplab" / "models"
CLIPLAB_DATASETS_DIR = STUDIO_DATA_ROOT / "cliplab" / "datasets"

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

# Effective production policy. Saved backend preferences and endpoint keys are
# legacy data, not authorization to recreate the retired RunPod plane.
VIRALITY_BACKEND = "local_llm"
REFRAME_BACKEND = "opencv_face"
RUNPOD_CLIPLAB_ENDPOINT = ""
RUNPOD_CLIPLAB_URL = ""
