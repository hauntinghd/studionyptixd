import os
from pathlib import Path
import stripe as stripe_lib


env_path = Path(__file__).parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ[key.strip()] = val.strip().strip('"').strip("'")


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
# Default to lower-cost model lane unless explicitly overridden.
RUNWAY_VIDEO_MODEL = os.getenv("RUNWAY_VIDEO_MODEL", "gen4_turbo")
RUNWAY_API_VERSION = os.getenv("RUNWAY_API_VERSION", "2024-11-06")
XAI_IMAGE_ASPECT_RATIO = os.getenv("XAI_IMAGE_ASPECT_RATIO", "9:16")
XAI_IMAGE_RESOLUTION = os.getenv("XAI_IMAGE_RESOLUTION", "2k")
USE_XAI_VIDEO = os.getenv("USE_XAI_VIDEO", "1").lower() in ("1", "true", "yes", "on")
PRODUCT_DEMO_PUBLIC_ENABLED = os.getenv("PRODUCT_DEMO_PUBLIC_ENABLED", "0").lower() in ("1", "true", "yes", "on")
SKELETON_GLOBAL_REFERENCE_IMAGE_URL = os.getenv("SKELETON_GLOBAL_REFERENCE_IMAGE_URL", "")
STORY_GLOBAL_REFERENCE_IMAGE_URL = os.getenv("STORY_GLOBAL_REFERENCE_IMAGE_URL", "")
MOTIVATION_GLOBAL_REFERENCE_IMAGE_URL = os.getenv("MOTIVATION_GLOBAL_REFERENCE_IMAGE_URL", "")
USE_FAL_GROK_IMAGE = os.getenv("USE_FAL_GROK_IMAGE", "0").lower() in ("1", "true", "yes", "on")
IMAGE_QUALITY_BESTOF_ENABLED = os.getenv("IMAGE_QUALITY_BESTOF_ENABLED", "1").lower() in ("1", "true", "yes", "on")
IMAGE_QUALITY_BESTOF_COUNT = max(1, min(8, int(os.getenv("IMAGE_QUALITY_BESTOF_COUNT", "4"))))
IMAGE_QUALITY_MIN_SCORE = float(os.getenv("IMAGE_QUALITY_MIN_SCORE", "62"))
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
FORCE_720P_ONLY = os.getenv("FORCE_720P_ONLY", "0").lower() in ("1", "true", "yes", "on")
SCRIPT_TO_SHORT_ENABLED = os.getenv("SCRIPT_TO_SHORT_ENABLED", "1").lower() in ("1", "true", "yes", "on")
STORY_ADVANCED_CONTROLS_ENABLED = os.getenv("STORY_ADVANCED_CONTROLS_ENABLED", "1").lower() in ("1", "true", "yes", "on")
STORY_RETENTION_TUNING_ENABLED = os.getenv("STORY_RETENTION_TUNING_ENABLED", "1").lower() in ("1", "true", "yes", "on")
DISABLE_ALL_SFX = os.getenv("DISABLE_ALL_SFX", "1").lower() in ("1", "true", "yes", "on")
MAINTENANCE_BANNER_ENABLED = os.getenv("MAINTENANCE_BANNER_ENABLED", "0").lower() in ("1", "true", "yes", "on")
MAINTENANCE_BANNER_MESSAGE = os.getenv(
    "MAINTENANCE_BANNER_MESSAGE",
    "Studio is under high load. Some generations may queue longer than usual while we scale capacity.",
).strip()

stripe_lib.api_key = STRIPE_SECRET_KEY

ELITE_PRICE_ID = os.getenv("PLAN_PRICE_ID_ELITE", "price_1T9uMwBL8lRmwao2Lk89pxiz").strip()

STRIPE_PRICE_TO_PLAN = {
    "price_1T4eT7BL8lRmwao2hHcUbcny": "starter",
    "price_1T4eTUBL8lRmwao2EK3JDOpy": "creator",
    "price_1T4eTjBL8lRmwao2q6WkoZLH": "pro",
    ELITE_PRICE_ID: "elite",
    "price_1T4wZLBL8lRmwao2SyYRfHdQ": "demo_pro",
}

PLAN_PRICE_USD = {
    "starter": float(os.getenv("PLAN_PRICE_STARTER_USD", "14")),
    "creator": float(os.getenv("PLAN_PRICE_CREATOR_USD", "30")),
    "pro": float(os.getenv("PLAN_PRICE_PRO_USD", "39")),
    "elite": float(os.getenv("PLAN_PRICE_ELITE_USD", "300")),
    "demo_pro": float(os.getenv("PLAN_PRICE_DEMO_PRO_USD", "150")),
}

DEMO_PRO_PRICE_ID = "price_1T4wZLBL8lRmwao2SyYRfHdQ"
TOPUP_PACK_PRICE_IDS = {
    "small": os.getenv("TOPUP_PRICE_SMALL", "price_1T6mJLBL8lRmwao2jPo0DGdq").strip(),
    "medium": os.getenv("TOPUP_PRICE_MEDIUM", "price_1T6mJXBL8lRmwao2E9djNfMA").strip(),
    "large": os.getenv("TOPUP_PRICE_LARGE", "price_1T6mJmBL8lRmwao24qTiaau5").strip(),
}
TOPUP_PACKS = {}
if TOPUP_PACK_PRICE_IDS["small"]:
    TOPUP_PACKS[TOPUP_PACK_PRICE_IDS["small"]] = {"pack": "small", "credits": 25, "price_usd": 7.99}
if TOPUP_PACK_PRICE_IDS["medium"]:
    TOPUP_PACKS[TOPUP_PACK_PRICE_IDS["medium"]] = {"pack": "medium", "credits": 150, "price_usd": 27.99}
if TOPUP_PACK_PRICE_IDS["large"]:
    TOPUP_PACKS[TOPUP_PACK_PRICE_IDS["large"]] = {"pack": "large", "credits": 400, "price_usd": 64.99}
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
