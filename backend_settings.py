import os
import json
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
PIKZELS_API_KEY = os.getenv("PIKZELS_API_KEY", "")
ALGROW_API_KEY = os.getenv("ALGROW_API_KEY", "")
ALGROW_API_BASE_URL = str(os.getenv("ALGROW_API_BASE_URL", "https://api.algrow.online") or "https://api.algrow.online").strip().rstrip("/")
COMFYUI_URL = os.getenv("COMFYUI_URL", "https://came-drop-energy-ryan.trycloudflare.com")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "")
SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET", "")


def _split_env_values(raw: str) -> list[str]:
    values: list[str] = []
    for chunk in str(raw or "").replace("\r", "\n").replace(";", ",").split("\n"):
        for value in str(chunk or "").split(","):
            cleaned = str(value or "").strip().strip('"').strip("'")
            if cleaned:
                values.append(cleaned)
    return values


def _collect_youtube_api_keys() -> list[str]:
    seen: set[str] = set()
    keys: list[str] = []

    def _append(raw: str) -> None:
        cleaned = str(raw or "").strip().strip('"').strip("'")
        if not cleaned or cleaned in seen:
            return
        seen.add(cleaned)
        keys.append(cleaned)

    for env_name in ("YOUTUBE_API_KEYS", "GOOGLE_YOUTUBE_API_KEYS"):
        for value in _split_env_values(os.getenv(env_name, "")):
            _append(value)
    for env_name in (
        "YOUTUBE_API_KEY",
        "GOOGLE_YOUTUBE_API_KEY",
        "YOUTUBE_API_KEY_1",
        "YOUTUBE_API_KEY_2",
        "YOUTUBE_API_KEY_3",
        "YOUTUBE_API_KEY_4",
        "YOUTUBE_API_KEY_5",
        "GOOGLE_YOUTUBE_API_KEY_1",
        "GOOGLE_YOUTUBE_API_KEY_2",
        "GOOGLE_YOUTUBE_API_KEY_3",
    ):
        _append(os.getenv(env_name, ""))
    return keys


YOUTUBE_API_KEYS = _collect_youtube_api_keys()
YOUTUBE_API_KEY = YOUTUBE_API_KEYS[0] if YOUTUBE_API_KEYS else ""
GOOGLE_DEFAULT_REDIRECT_URI = "https://api.nyptidindustries.com/api/oauth/google/youtube/callback"
GOOGLE_CLIENT_SECRETS_PATH = Path(__file__).parent / "client_secrets.json"
YOUTUBE_OAUTH_MODE = str(os.getenv("YOUTUBE_OAUTH_MODE", "auto") or "auto").strip().lower()
if YOUTUBE_OAUTH_MODE not in {"auto", "web", "installed"}:
    YOUTUBE_OAUTH_MODE = "auto"


def _load_google_client_secrets_payload() -> tuple[dict | None, str]:
    if not GOOGLE_CLIENT_SECRETS_PATH.exists():
        return None, "missing_google_oauth_credentials"
    try:
        payload = json.loads(GOOGLE_CLIENT_SECRETS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None, "client_secrets_invalid_json"
    if not isinstance(payload, dict):
        return None, "client_secrets_invalid_json"
    return payload, ""


def _extract_google_client_settings(payload: dict | None, client_kind: str) -> dict:
    if not isinstance(payload, dict):
        return {
            "client_id": "",
            "client_secret": "",
            "source": "missing",
            "client_kind": client_kind,
            "registered_redirect_uris": [],
        }
    client_config = payload.get(client_kind)
    if not isinstance(client_config, dict):
        return {
            "client_id": "",
            "client_secret": "",
            "source": "missing",
            "client_kind": client_kind,
            "registered_redirect_uris": [],
        }
    return {
        "client_id": str(client_config.get("client_id", "") or "").strip(),
        "client_secret": str(client_config.get("client_secret", "") or "").strip(),
        "source": f"client_secrets:{client_kind}",
        "client_kind": client_kind,
        "registered_redirect_uris": [
            str(value or "").strip()
            for value in list(client_config.get("redirect_uris") or [])
            if str(value or "").strip()
        ],
    }


def _load_google_oauth_settings() -> dict:
    env_client_id = str(os.getenv("GOOGLE_CLIENT_ID", "") or "").strip()
    env_client_secret = str(os.getenv("GOOGLE_CLIENT_SECRET", "") or "").strip()
    env_redirect_uri = str(os.getenv("GOOGLE_REDIRECT_URI", GOOGLE_DEFAULT_REDIRECT_URI) or GOOGLE_DEFAULT_REDIRECT_URI).strip()
    if env_client_id and env_client_secret:
        return {
            "client_id": env_client_id,
            "client_secret": env_client_secret,
            "redirect_uri": env_redirect_uri,
            "source": "env",
            "client_kind": "web",
            "config_issue": "",
            "registered_redirect_uris": [env_redirect_uri] if env_redirect_uri else [],
        }

    payload, payload_issue = _load_google_client_secrets_payload()
    if payload_issue:
        return {
            "client_id": "",
            "client_secret": "",
            "redirect_uri": env_redirect_uri,
            "source": "client_secrets" if payload_issue == "client_secrets_invalid_json" else "missing",
            "client_kind": "unknown",
            "config_issue": payload_issue,
            "registered_redirect_uris": [],
        }

    web_settings = _extract_google_client_settings(payload, "web")
    installed_settings = _extract_google_client_settings(payload, "installed")
    selected = web_settings if str(web_settings.get("client_id", "") or "").strip() else installed_settings
    client_id = str(selected.get("client_id", "") or "").strip()
    client_secret = str(selected.get("client_secret", "") or "").strip()
    client_kind = str(selected.get("client_kind", "unknown") or "unknown").strip()
    registered_redirect_uris = list(selected.get("registered_redirect_uris") or [])
    config_issue = ""
    if not client_id or not client_secret:
        config_issue = "client_secrets_missing_client_credentials"
    elif client_kind != "web":
        config_issue = "" if YOUTUBE_OAUTH_MODE == "installed" else "desktop_client_not_supported_for_backend_oauth"
    elif registered_redirect_uris and env_redirect_uri not in registered_redirect_uris:
        config_issue = "redirect_uri_not_listed_in_google_client"

    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": env_redirect_uri,
        "source": str(selected.get("source", f"client_secrets:{client_kind}") or f"client_secrets:{client_kind}").strip(),
        "client_kind": client_kind,
        "config_issue": config_issue,
        "registered_redirect_uris": registered_redirect_uris,
    }


def _load_google_installed_oauth_settings() -> dict:
    payload, payload_issue = _load_google_client_secrets_payload()
    if payload_issue:
        return {
            "client_id": "",
            "client_secret": "",
            "redirect_uri": "",
            "source": "client_secrets" if payload_issue == "client_secrets_invalid_json" else "missing",
            "client_kind": "installed",
            "config_issue": payload_issue,
            "registered_redirect_uris": [],
        }
    installed_settings = _extract_google_client_settings(payload, "installed")
    client_id = str(installed_settings.get("client_id", "") or "").strip()
    client_secret = str(installed_settings.get("client_secret", "") or "").strip()
    registered_redirect_uris = list(installed_settings.get("registered_redirect_uris") or [])
    config_issue = ""
    if not client_id or not client_secret:
        config_issue = "client_secrets_missing_client_credentials"
    elif not registered_redirect_uris:
        config_issue = "client_secrets_missing_redirect_uri"
    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": str(registered_redirect_uris[0] if registered_redirect_uris else "").strip(),
        "source": str(installed_settings.get("source", "client_secrets:installed") or "client_secrets:installed").strip(),
        "client_kind": "installed",
        "config_issue": config_issue,
        "registered_redirect_uris": registered_redirect_uris,
    }


_google_oauth_settings = _load_google_oauth_settings()
GOOGLE_CLIENT_ID = str(_google_oauth_settings.get("client_id", "") or "").strip()
GOOGLE_CLIENT_SECRET = str(_google_oauth_settings.get("client_secret", "") or "").strip()
GOOGLE_REDIRECT_URI = str(_google_oauth_settings.get("redirect_uri", GOOGLE_DEFAULT_REDIRECT_URI) or GOOGLE_DEFAULT_REDIRECT_URI).strip()
GOOGLE_OAUTH_SOURCE = str(_google_oauth_settings.get("source", "") or "").strip()
GOOGLE_OAUTH_CLIENT_KIND = str(_google_oauth_settings.get("client_kind", "") or "").strip()
GOOGLE_OAUTH_CONFIG_ISSUE = str(_google_oauth_settings.get("config_issue", "") or "").strip()
GOOGLE_OAUTH_REGISTERED_REDIRECT_URIS = tuple(
    str(value or "").strip()
    for value in list(_google_oauth_settings.get("registered_redirect_uris") or [])
    if str(value or "").strip()
)
_google_installed_oauth_settings = _load_google_installed_oauth_settings()
GOOGLE_INSTALLED_CLIENT_ID = str(_google_installed_oauth_settings.get("client_id", "") or "").strip()
GOOGLE_INSTALLED_CLIENT_SECRET = str(_google_installed_oauth_settings.get("client_secret", "") or "").strip()
GOOGLE_INSTALLED_REDIRECT_URI = str(_google_installed_oauth_settings.get("redirect_uri", "") or "").strip()
GOOGLE_INSTALLED_OAUTH_SOURCE = str(_google_installed_oauth_settings.get("source", "") or "").strip()
GOOGLE_INSTALLED_OAUTH_CONFIG_ISSUE = str(_google_installed_oauth_settings.get("config_issue", "") or "").strip()
GOOGLE_INSTALLED_REGISTERED_REDIRECT_URIS = tuple(
    str(value or "").strip()
    for value in list(_google_installed_oauth_settings.get("registered_redirect_uris") or [])
    if str(value or "").strip()
)
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
STRIPE_TOPUP_PUBLIC_ENABLED = os.getenv("STRIPE_TOPUP_PUBLIC_ENABLED", "0").lower() in ("1", "true", "yes", "on")
PAYPAL_CLIENT_ID = os.getenv("PAYPAL_CLIENT_ID", "")
PAYPAL_CLIENT_SECRET = os.getenv("PAYPAL_CLIENT_SECRET", "")
PAYPAL_ENV = str(os.getenv("PAYPAL_ENV", "live") or "live").strip().lower()
SITE_URL = os.getenv("SITE_URL", "https://studio.nyptidindustries.com")
FAL_AI_KEY = os.getenv("FAL_AI_KEY", "")
FAL_IMAGE_BACKUP_MODEL = str(os.getenv("FAL_IMAGE_BACKUP_MODEL", "grok_imagine") or "grok_imagine").strip().lower()
XAI_IMAGE_MODEL = os.getenv("XAI_IMAGE_MODEL", "grok-imagine-image-pro")
XAI_VIDEO_MODEL = os.getenv("XAI_VIDEO_MODEL", "grok-imagine-video")
PIKZELS_THUMBNAIL_MODEL = os.getenv("PIKZELS_THUMBNAIL_MODEL", "pkz-3")
PIKZELS_RECREATE_MODEL = os.getenv("PIKZELS_RECREATE_MODEL", "pkz-3")
PIKZELS_TITLE_MODEL = os.getenv("PIKZELS_TITLE_MODEL", "pkz-3")


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
WAITLIST_ONLY_MODE = os.getenv("WAITLIST_ONLY_MODE", "0").lower() in ("1", "true", "yes", "on")
WAITLIST_REQUIRE_STRIPE_PAYMENT = os.getenv("WAITLIST_REQUIRE_STRIPE_PAYMENT", "0").lower() in ("1", "true", "yes", "on")
SKELETON_GLOBAL_REFERENCE_IMAGE_URL = os.getenv(
    "SKELETON_GLOBAL_REFERENCE_IMAGE_URL",
    f"{SITE_URL.rstrip('/')}/default-skeleton-style-lock.png",
)
STORY_GLOBAL_REFERENCE_IMAGE_URL = os.getenv("STORY_GLOBAL_REFERENCE_IMAGE_URL", "")
MOTIVATION_GLOBAL_REFERENCE_IMAGE_URL = os.getenv("MOTIVATION_GLOBAL_REFERENCE_IMAGE_URL", "")
USE_FAL_GROK_IMAGE = os.getenv("USE_FAL_GROK_IMAGE", "0").lower() in ("1", "true", "yes", "on")
IMAGE_PROVIDER_ORDER = os.getenv("IMAGE_PROVIDER_ORDER", "fal")
XAI_IMAGE_FALLBACK_ENABLED = os.getenv("XAI_IMAGE_FALLBACK_ENABLED", "0").lower() in ("1", "true", "yes", "on")
HIDREAM_ENABLED = os.getenv("HIDREAM_ENABLED", "1").lower() in ("1", "true", "yes", "on")
HIDREAM_MODEL = os.getenv("HIDREAM_MODEL", "hidream_i1_full_fp8.safetensors")
HIDREAM_EDIT_ENABLED = os.getenv("HIDREAM_EDIT_ENABLED", "1").lower() in ("1", "true", "yes", "on")
HIDREAM_EDIT_MODEL = os.getenv("HIDREAM_EDIT_MODEL", "hidream_e1_1_bf16.safetensors")
HIDREAM_EDIT_WEIGHT_DTYPE = os.getenv("HIDREAM_EDIT_WEIGHT_DTYPE", "fp8_e4m3fn_fast")
HIDREAM_CLIP_L = os.getenv("HIDREAM_CLIP_L", "clip_l_hidream.safetensors")
HIDREAM_CLIP_G = os.getenv("HIDREAM_CLIP_G", "clip_g_hidream.safetensors")
HIDREAM_T5 = os.getenv("HIDREAM_T5", "t5xxl_fp8_e4m3fn_scaled.safetensors")
HIDREAM_LLAMA = os.getenv("HIDREAM_LLAMA", "llama_3.1_8b_instruct_fp8_scaled.safetensors")
HIDREAM_VAE = os.getenv("HIDREAM_VAE", "ae.safetensors")
HIDREAM_SHIFT = float(os.getenv("HIDREAM_SHIFT", "3.0") or 3.0)
HIDREAM_STEPS = max(8, int(os.getenv("HIDREAM_STEPS", "40") or 40))
HIDREAM_CFG = float(os.getenv("HIDREAM_CFG", "5.0") or 5.0)
HIDREAM_SAMPLER = os.getenv("HIDREAM_SAMPLER", "euler")
HIDREAM_SCHEDULER = os.getenv("HIDREAM_SCHEDULER", "simple")
WAN22_T2I_CHECKPOINT = os.getenv("WAN22_T2I_CHECKPOINT", "wan2.2_t2i_14b_fp8_scaled.safetensors")
WAN22_T2I_CLIP = os.getenv("WAN22_T2I_CLIP", "umt5_xxl_fp8_e4m3fn_scaled.safetensors")
WAN22_T2I_VAE = os.getenv("WAN22_T2I_VAE", "wan2.2_vae.safetensors")
WAN22_T2I_UNET = os.getenv("WAN22_T2I_UNET", "wan2.2_ti2v_5B_fp16.safetensors")
WAN22_T2I_UNET_FP8 = os.getenv("WAN22_T2I_UNET_FP8", "wan2.2_ti2v_5B_fp8.safetensors")
TEMPLATE_ADAPTER_ROUTING_ENABLED = os.getenv("TEMPLATE_ADAPTER_ROUTING_ENABLED", "1").lower() in ("1", "true", "yes", "on")
_TEMPLATE_ADAPTER_ROUTING_DEFAULT = {
    "default": {
        "checkpoint": "sd_xl_base_1.0.safetensors",
        "prepend_trigger": "",
        "prompt_suffix": "",
        "negative_suffix": "",
        "loras": [],
    },
    "skeleton": {
        "checkpoint": "sd_xl_base_1.0.safetensors",
        "prepend_trigger": "",
        "prompt_suffix": "",
        "negative_suffix": "",
        "loras": [],
    },
    "story": {
        "checkpoint": "sd_xl_base_1.0.safetensors",
        "prepend_trigger": "",
        "prompt_suffix": "cinematic photoreal composition, high-detail environmental storytelling",
        "negative_suffix": "",
        "loras": [],
    },
    "motivation": {
        "checkpoint": "sd_xl_base_1.0.safetensors",
        "prepend_trigger": "",
        "prompt_suffix": "cinematic golden-hour lighting, premium inspirational framing",
        "negative_suffix": "",
        "loras": [],
    },
    "daytrading": {
        "checkpoint": "sd_xl_base_1.0.safetensors",
        "prepend_trigger": "",
        "prompt_suffix": "ultra realistic professional trading desk, photoreal market screens, tradingview or bloomberg style chart hierarchy, order-flow and level-2 realism, cinematic financial explainer composition, polished 3d finance graphics only when they look like real market tools",
        "negative_suffix": "",
        "loras": [],
    },
}


def _parse_template_adapter_routing() -> dict:
    raw = os.getenv("TEMPLATE_ADAPTER_ROUTING", "").strip()
    if not raw:
        return dict(_TEMPLATE_ADAPTER_ROUTING_DEFAULT)
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            merged = dict(_TEMPLATE_ADAPTER_ROUTING_DEFAULT)
            for k, v in parsed.items():
                if isinstance(v, dict):
                    base = dict(_TEMPLATE_ADAPTER_ROUTING_DEFAULT.get(str(k).lower(), {}))
                    base.update(v)
                    merged[str(k).lower()] = base
            return merged
    except Exception:
        pass
    return dict(_TEMPLATE_ADAPTER_ROUTING_DEFAULT)


TEMPLATE_ADAPTER_ROUTING = _parse_template_adapter_routing()
IMAGE_LOCAL_PROVIDER_RETRIES = max(1, min(5, int(os.getenv("IMAGE_LOCAL_PROVIDER_RETRIES", "3"))))
IMAGE_PROVIDER_FAILURE_COOLDOWN_SEC = max(0, int(os.getenv("IMAGE_PROVIDER_FAILURE_COOLDOWN_SEC", "90")))
IMAGE_PROVIDER_WAN_SKIP_IF_UNAVAILABLE = os.getenv("IMAGE_PROVIDER_WAN_SKIP_IF_UNAVAILABLE", "1").lower() in ("1", "true", "yes", "on")
SKELETON_REQUIRE_WAN22 = os.getenv("SKELETON_REQUIRE_WAN22", "1").lower() in ("1", "true", "yes", "on")
SKELETON_SDXL_LORA_ENABLED = os.getenv("SKELETON_SDXL_LORA_ENABLED", "0").lower() in ("1", "true", "yes", "on")
IMAGE_LOCAL_MIN_FILE_BYTES = max(1024, int(os.getenv("IMAGE_LOCAL_MIN_FILE_BYTES", "8192")))
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
LONGFORM_BETA_ENABLED = os.getenv("LONGFORM_BETA_ENABLED", "0").lower() in ("1", "true", "yes", "on")
LONGFORM_DEFAULT_TARGET_MINUTES = float(os.getenv("LONGFORM_DEFAULT_TARGET_MINUTES", "8"))
LONGFORM_MIN_TARGET_MINUTES = float(os.getenv("LONGFORM_MIN_TARGET_MINUTES", "2"))
LONGFORM_MAX_TARGET_MINUTES = float(os.getenv("LONGFORM_MAX_TARGET_MINUTES", "30"))
LONGFORM_MAX_SCENE_RETRIES = max(1, int(os.getenv("LONGFORM_MAX_SCENE_RETRIES", "4")))
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
    "free": 0.0,
    "starter": float(os.getenv("PLAN_PRICE_STARTER_USD", "14")),
    "creator": float(os.getenv("PLAN_PRICE_CREATOR_USD", "24")),
    "pro": float(os.getenv("PLAN_PRICE_PRO_USD", "39")),
    "elite": float(os.getenv("PLAN_PRICE_ELITE_USD", "300")),
    "demo_pro": float(os.getenv("PLAN_PRICE_DEMO_PRO_USD", "150")),
}

# Animation usage pricing baseline:
# Kling 2.1 Standard I2V observed market API cost (5s) ~= $0.28.
# Studio applies a configurable multiplier for margin + platform overhead.
KLING21_STANDARD_I2V_5S_USD = float(os.getenv("KLING21_STANDARD_I2V_5S_USD", "0.28"))
ANIMATION_MARKUP_MULTIPLIER = float(os.getenv("ANIMATION_MARKUP_MULTIPLIER", "3.0"))
ANIMATION_CREDIT_UNIT_USD = round(
    max(0.01, KLING21_STANDARD_I2V_5S_USD) * max(1.0, ANIMATION_MARKUP_MULTIPLIER),
    2,
)

DEMO_PRO_PRICE_ID = "price_1T4wZLBL8lRmwao2SyYRfHdQ"
TOPUP_PACK_SPECS = [
    {"id": "ac_trial", "pack": "trial", "credits": 1, "price_usd": 0.60},
    {"id": "ac_starter", "pack": "starter", "credits": 3, "price_usd": 1.80},
    {"id": "ac_mini", "pack": "mini", "credits": 5, "price_usd": 3.00},
    {"id": "ac_lite", "pack": "lite", "credits": 10, "price_usd": 6.00},
    {"id": "ac_boost", "pack": "boost", "credits": 15, "price_usd": 9.00},
    {"id": "ac_basic", "pack": "basic", "credits": 25, "price_usd": 15.00},
    {"id": "ac_runner", "pack": "runner", "credits": 40, "price_usd": 24.00},
    {"id": "ac_creator", "pack": "creator", "credits": 50, "price_usd": 30.00},
    {"id": "ac_operator", "pack": "operator", "credits": 75, "price_usd": 45.00},
    {"id": "ac_growth", "pack": "growth", "credits": 100, "price_usd": 60.00},
    {"id": "ac_power", "pack": "power", "credits": 150, "price_usd": 90.00},
    {"id": "ac_scale", "pack": "scale", "credits": 250, "price_usd": 150.00},
    {"id": "ac_studio", "pack": "studio", "credits": 500, "price_usd": 300.00},
    {"id": "ac_agency", "pack": "agency", "credits": 1000, "price_usd": 600.00},
]
TOPUP_PACKS = {
    str(spec["id"]): {
        "pack": str(spec["pack"]),
        "credits": int(spec["credits"]),
        "price_usd": float(spec["price_usd"]),
        "stripe_price_id": str(spec.get("stripe_price_id", "") or "").strip(),
    }
    for spec in TOPUP_PACK_SPECS
}
PUBLIC_PLAN_IDS = ("free", "starter", "creator", "pro")
PUBLIC_TOPUP_PACK_IDS = tuple(str(spec["id"]) for spec in TOPUP_PACK_SPECS)
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

APP_ROOT = Path(__file__).resolve().parent


def _resolve_data_path(raw_value: str, default_path: Path) -> Path:
    cleaned = str(raw_value or "").strip()
    if cleaned:
        candidate = Path(cleaned).expanduser()
        if not candidate.is_absolute():
            candidate = (APP_ROOT / candidate).resolve()
        return candidate
    return default_path.resolve()


APP_DATA_DIR = _resolve_data_path(os.getenv("APP_DATA_DIR", ""), APP_ROOT)
APP_DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_DIR = _resolve_data_path(os.getenv("OUTPUT_DIR", ""), APP_DATA_DIR / "generated_videos")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
TEMP_DIR = _resolve_data_path(os.getenv("TEMP_DIR", ""), APP_DATA_DIR / "temp_assets")
TEMP_DIR.mkdir(parents=True, exist_ok=True)
TRAINING_DATA_DIR = _resolve_data_path(os.getenv("TRAINING_DATA_DIR", ""), APP_DATA_DIR / "training_data")
TRAINING_DATA_DIR.mkdir(parents=True, exist_ok=True)
THUMBNAIL_DIR = _resolve_data_path(os.getenv("THUMBNAIL_DIR", ""), APP_DATA_DIR / "thumbnails")
THUMBNAIL_DIR.mkdir(parents=True, exist_ok=True)
DEMO_UPLOAD_DIR = _resolve_data_path(os.getenv("DEMO_UPLOAD_DIR", ""), APP_DATA_DIR / "demo_uploads")
DEMO_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
CREATIVE_SESSIONS_FILE = _resolve_data_path(
    os.getenv("CREATIVE_SESSIONS_FILE", ""),
    TEMP_DIR / "creative_sessions_store.json",
)
CREATIVE_SESSION_PERSISTENCE_ENABLED = os.getenv("CREATIVE_SESSION_PERSISTENCE_ENABLED", "1").lower() in ("1", "true", "yes", "on")
PROJECTS_STORE_FILE = _resolve_data_path(
    os.getenv("PROJECTS_STORE_FILE", ""),
    TEMP_DIR / "projects_store.json",
)
YOUTUBE_CONNECTIONS_FILE = _resolve_data_path(
    os.getenv("YOUTUBE_CONNECTIONS_FILE", ""),
    TEMP_DIR / "youtube_connections.json",
)
YOUTUBE_OAUTH_STATES_FILE = _resolve_data_path(
    os.getenv("YOUTUBE_OAUTH_STATES_FILE", ""),
    TEMP_DIR / "youtube_oauth_states.json",
)
YOUTUBE_SIGNAL_LOG_FILE = _resolve_data_path(
    os.getenv("YOUTUBE_SIGNAL_LOG_FILE", ""),
    TRAINING_DATA_DIR / "youtube_channel_signals.jsonl",
)
