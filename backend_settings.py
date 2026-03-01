import os
from pathlib import Path
import stripe as stripe_lib


env_path = Path(__file__).parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip())


XAI_API_KEY = os.getenv("XAI_API_KEY", "")
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY", "")
COMFYUI_URL = os.getenv("COMFYUI_URL", "https://came-drop-energy-ryan.trycloudflare.com")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "")
SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET", "")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
SITE_URL = os.getenv("SITE_URL", "https://studio.nyptidindustries.com")
FAL_AI_KEY = os.getenv("FAL_AI_KEY", "")
XAI_IMAGE_MODEL = os.getenv("XAI_IMAGE_MODEL", "grok-imagine-image-pro")
XAI_VIDEO_MODEL = os.getenv("XAI_VIDEO_MODEL", "grok-imagine-video")


def _resolve_runway_api_key() -> tuple[str, str]:
    candidates = (
        ("RUNWAY_API_KEY", os.getenv("RUNWAY_API_KEY", "")),
        ("RUNWAYML_API_KEY", os.getenv("RUNWAYML_API_KEY", "")),
        ("RUNWAY_KEY", os.getenv("RUNWAY_KEY", "")),
    )
    for source, raw in candidates:
        val = (raw or "").strip().strip('"').strip("'")
        if val:
            return val, source
    return "", ""


RUNWAY_API_KEY, RUNWAY_API_KEY_SOURCE = _resolve_runway_api_key()
RUNWAY_VIDEO_MODEL = os.getenv("RUNWAY_VIDEO_MODEL", "gen4.5")
RUNWAY_API_VERSION = os.getenv("RUNWAY_API_VERSION", "2024-11-06")
XAI_IMAGE_ASPECT_RATIO = os.getenv("XAI_IMAGE_ASPECT_RATIO", "9:16")
XAI_IMAGE_RESOLUTION = os.getenv("XAI_IMAGE_RESOLUTION", "2k")
USE_XAI_VIDEO = os.getenv("USE_XAI_VIDEO", "1").lower() in ("1", "true", "yes", "on")
PRODUCT_DEMO_PUBLIC_ENABLED = os.getenv("PRODUCT_DEMO_PUBLIC_ENABLED", "0").lower() in ("1", "true", "yes", "on")
SKELETON_GLOBAL_REFERENCE_IMAGE_URL = os.getenv("SKELETON_GLOBAL_REFERENCE_IMAGE_URL", "")
USE_FAL_GROK_IMAGE = os.getenv("USE_FAL_GROK_IMAGE", "0").lower() in ("1", "true", "yes", "on")
RUNPOD_IMAGE_FEEDBACK_ENABLED = os.getenv("RUNPOD_IMAGE_FEEDBACK_ENABLED", "1").lower() in ("1", "true", "yes", "on")
RUNPOD_IMAGE_FEEDBACK_SSH = os.getenv(
    "RUNPOD_IMAGE_FEEDBACK_SSH",
    "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p 22092 root@69.30.85.41",
)
RUNPOD_IMAGE_FEEDBACK_BASE_DIR = os.getenv("RUNPOD_IMAGE_FEEDBACK_BASE_DIR", "/workspace/image_training")
RUNPOD_COMPOSITOR_ENABLED = os.getenv("RUNPOD_COMPOSITOR_ENABLED", "1").lower() in ("1", "true", "yes", "on")
RUNPOD_COMPOSITOR_FALLBACK_LOCAL = os.getenv("RUNPOD_COMPOSITOR_FALLBACK_LOCAL", "1").lower() in ("1", "true", "yes", "on")
RUNPOD_COMPOSITOR_HOST = os.getenv("RUNPOD_COMPOSITOR_HOST", "root@69.30.85.41")
RUNPOD_COMPOSITOR_SSH_PORT = os.getenv("RUNPOD_COMPOSITOR_SSH_PORT", "22118")
RUNPOD_COMPOSITOR_BASE_DIR = os.getenv("RUNPOD_COMPOSITOR_BASE_DIR", "/workspace/nyptid_compositor")
JOB_QUEUE_WORKERS = max(1, int(os.getenv("JOB_QUEUE_WORKERS", "2")))
JOB_MAX_QUEUE_DEPTH = max(1, int(os.getenv("JOB_MAX_QUEUE_DEPTH", "300")))
REDIS_URL = os.getenv("REDIS_URL", "").strip()
REDIS_QUEUE_ENABLED = os.getenv("REDIS_QUEUE_ENABLED", "0").lower() in ("1", "true", "yes", "on")
REDIS_QUEUE_PREFIX = os.getenv("REDIS_QUEUE_PREFIX", "studio")
FORCE_720P_ONLY = os.getenv("FORCE_720P_ONLY", "1").lower() in ("1", "true", "yes", "on")

stripe_lib.api_key = STRIPE_SECRET_KEY

STRIPE_PRICE_TO_PLAN = {
    "price_1T4eT7BL8lRmwao2hHcUbcny": "starter",
    "price_1T4eTUBL8lRmwao2EK3JDOpy": "creator",
    "price_1T4eTjBL8lRmwao2q6WkoZLH": "pro",
    "price_1T4wZLBL8lRmwao2SyYRfHdQ": "demo_pro",
}

DEMO_PRO_PRICE_ID = "price_1T4wZLBL8lRmwao2SyYRfHdQ"
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

OUTPUT_DIR = Path("generated_videos")
OUTPUT_DIR.mkdir(exist_ok=True)
TEMP_DIR = Path("temp_assets")
TEMP_DIR.mkdir(exist_ok=True)
TRAINING_DATA_DIR = Path("training_data")
TRAINING_DATA_DIR.mkdir(exist_ok=True)
CREATIVE_SESSIONS_FILE = TEMP_DIR / "creative_sessions_store.json"
CREATIVE_SESSION_PERSISTENCE_ENABLED = os.getenv("CREATIVE_SESSION_PERSISTENCE_ENABLED", "1").lower() in ("1", "true", "yes", "on")
PROJECTS_STORE_FILE = TEMP_DIR / "projects_store.json"
