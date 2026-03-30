import os
import re
import base64
import shutil
import random
import secrets
import asyncio
import json
import time
import subprocess
import tempfile
import logging
import io
import calendar
import zipfile
import httpx
import jwt
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlparse, unquote
from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional
import stripe as stripe_lib
import uvicorn
from backend_settings import (
    XAI_API_KEY,
    ELEVENLABS_API_KEY,
    PIKZELS_API_KEY,
    COMFYUI_URL,
    SUPABASE_URL,
    SUPABASE_ANON_KEY,
    SUPABASE_JWT_SECRET,
    STRIPE_SECRET_KEY,
    STRIPE_WEBHOOK_SECRET,
    STRIPE_TOPUP_PUBLIC_ENABLED,
    PAYPAL_CLIENT_ID,
    PAYPAL_CLIENT_SECRET,
    PAYPAL_ENV,
    SITE_URL,
    FAL_AI_KEY,
    FAL_IMAGE_BACKUP_MODEL,
    XAI_IMAGE_MODEL,
    XAI_VIDEO_MODEL,
    PIKZELS_THUMBNAIL_MODEL,
    PIKZELS_RECREATE_MODEL,
    PIKZELS_TITLE_MODEL,
    RUNWAY_API_KEY,
    RUNWAY_API_KEY_SOURCE,
    RUNWAY_VIDEO_MODEL,
    RUNWAY_API_VERSION,
    PLAN_PRICE_USD,
    KLING21_STANDARD_I2V_5S_USD,
    ANIMATION_MARKUP_MULTIPLIER,
    ANIMATION_CREDIT_UNIT_USD,
    XAI_IMAGE_ASPECT_RATIO,
    XAI_IMAGE_RESOLUTION,
    USE_XAI_VIDEO,
    PRODUCT_DEMO_PUBLIC_ENABLED,
    WAITLIST_ONLY_MODE,
    WAITLIST_REQUIRE_STRIPE_PAYMENT,
    SKELETON_GLOBAL_REFERENCE_IMAGE_URL,
    STORY_GLOBAL_REFERENCE_IMAGE_URL,
    MOTIVATION_GLOBAL_REFERENCE_IMAGE_URL,
    USE_FAL_GROK_IMAGE,
    IMAGE_PROVIDER_ORDER,
    XAI_IMAGE_FALLBACK_ENABLED,
    HIDREAM_ENABLED,
    HIDREAM_MODEL,
    HIDREAM_EDIT_ENABLED,
    HIDREAM_EDIT_MODEL,
    HIDREAM_EDIT_WEIGHT_DTYPE,
    HIDREAM_CLIP_L,
    HIDREAM_CLIP_G,
    HIDREAM_T5,
    HIDREAM_LLAMA,
    HIDREAM_VAE,
    HIDREAM_SHIFT,
    HIDREAM_STEPS,
    HIDREAM_CFG,
    HIDREAM_SAMPLER,
    HIDREAM_SCHEDULER,
    WAN22_T2I_CHECKPOINT,
    WAN22_T2I_CLIP,
    WAN22_T2I_VAE,
    WAN22_T2I_UNET,
    WAN22_T2I_UNET_FP8,
    TEMPLATE_ADAPTER_ROUTING_ENABLED,
    TEMPLATE_ADAPTER_ROUTING,
    IMAGE_LOCAL_PROVIDER_RETRIES,
    IMAGE_PROVIDER_FAILURE_COOLDOWN_SEC,
    IMAGE_PROVIDER_WAN_SKIP_IF_UNAVAILABLE,
    SKELETON_REQUIRE_WAN22,
    SKELETON_SDXL_LORA_ENABLED,
    IMAGE_LOCAL_MIN_FILE_BYTES,
    IMAGE_QUALITY_BESTOF_ENABLED,
    IMAGE_QUALITY_BESTOF_COUNT,
    IMAGE_QUALITY_MIN_SCORE,
    RUNPOD_IMAGE_FEEDBACK_ENABLED,
    RUNPOD_IMAGE_FEEDBACK_SSH,
    RUNPOD_IMAGE_FEEDBACK_BASE_DIR,
    RUNPOD_COMPOSITOR_ENABLED,
    RUNPOD_COMPOSITOR_FALLBACK_LOCAL,
    RUNPOD_COMPOSITOR_HOST,
    RUNPOD_COMPOSITOR_SSH_PORT,
    RUNPOD_COMPOSITOR_BASE_DIR,
    FORCE_720P_ONLY,
    SCRIPT_TO_SHORT_ENABLED,
    STORY_ADVANCED_CONTROLS_ENABLED,
    STORY_RETENTION_TUNING_ENABLED,
    DISABLE_ALL_SFX,
    LONGFORM_BETA_ENABLED,
    LONGFORM_DEFAULT_TARGET_MINUTES,
    LONGFORM_MIN_TARGET_MINUTES,
    LONGFORM_MAX_TARGET_MINUTES,
    LONGFORM_MAX_SCENE_RETRIES,
    MAINTENANCE_BANNER_ENABLED,
    MAINTENANCE_BANNER_MESSAGE,
    REDIS_QUEUE_ENABLED,
    REDIS_URL,
    STRIPE_PRICE_TO_PLAN,
    DEMO_PRO_PRICE_ID,
    TOPUP_PACKS,
    SUPABASE_SERVICE_KEY,
    OUTPUT_DIR,
    TEMP_DIR,
    THUMBNAIL_DIR,
    TRAINING_DATA_DIR,
)
from backend_catalog import (
    PLAN_LIMITS,
    PLAN_FEATURES,
    RESOLUTION_CONFIGS,
    ADMIN_EMAILS,
    HARDCODED_PLANS,
    PUBLIC_TEMPLATE_ALLOWLIST,
    SUPPORTED_LANGUAGES,
    TEMPLATE_VOICE_SETTINGS,
    TEMPLATE_SFX_STYLES,
)
from backend_image_prompts import (
    SKELETON_IMAGE_STYLE_PREFIX,
    SKELETON_MASTER_CONSISTENCY_PROMPT,
    SKELETON_IMAGE_SUFFIX,
    TEMPLATE_KLING_MOTION,
    TEMPLATE_SFX_PROMPTS,
    TEMPLATE_PROMPT_PREFIXES,
    TEMPLATE_NEGATIVE_PROMPTS,
    NEGATIVE_PROMPT,
    WAN22_I2V_HIGH,
    WAN22_T2V_HIGH,
    WAN22_T2V_LOW,
)
from backend_models import (
    GenerateRequest,
    SceneImageRequest,
    FinalizeRequest,
    CheckoutRequest,
    TopupCheckoutRequest,
    WaitlistJoinRequest,
    SetPlanRequest,
    FeedbackRequest,
    ThumbnailFeedbackRequest,
    ThumbnailGenerateRequest,
    LongFormSessionCreateRequest,
    LongFormChapterActionRequest,
    LongFormResolveErrorRequest,
)
from backend_demo import (
    DEMO_DIR,
    analyze_screen_recording,
    generate_demo_script,
    generate_talking_head,
    composite_demo_video,
)
from backend_state import (
    _creative_sessions,
    _creative_sessions_lock,
    _projects,
    _projects_lock,
    _save_creative_sessions_to_disk,
    _get_creative_session,
    _save_projects_store,
    _new_project_id,
)
from backend_queue import (
    QueueFullError,
    enqueue_generation_job,
    get_queue_depth,
    get_queue_max_depth,
    get_queue_workers,
    get_persisted_job_state,
    init_queue_runtime,
    persist_job_state,
)
try:
    import paramiko
except Exception:
    paramiko = None

try:
    from PIL import Image, ImageChops, ImageEnhance, ImageFilter, ImageStat
except Exception:
    Image = None
    ImageChops = None
    ImageEnhance = None
    ImageFilter = None
    ImageStat = None

try:
    import numpy as np
except Exception:
    np = None

try:
    import cv2
except Exception:
    cv2 = None

try:
    import yt_dlp
except Exception:
    yt_dlp = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("nyptid-studio")


def _ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None

app = FastAPI(title="NYPTID Studio Engine", version="3.0")
_deploy_meta_cache = {"ts": 0.0, "backend_commit": "", "frontend_bundle": ""}
_frontend_asset_cache = {"ts": 0.0, "js": "", "css": ""}
_frontend_cache_buster = str(int(time.time()))
DEFAULT_ELEVENLABS_VOICES = [
    {"voice_id": "EXAVITQu4vr4xnSDxMaL", "name": "Sarah", "category": "premade", "description": "Warm, upbeat female", "gender": "female", "accent": "american", "age": "young", "preview_url": ""},
    {"voice_id": "FGY2WhTYpPnrIDTdsKH5", "name": "Laura", "category": "premade", "description": "Neutral narration", "gender": "female", "accent": "american", "age": "young", "preview_url": ""},
    {"voice_id": "XB0fDUnXU5powFXDhCwa", "name": "Charlotte", "category": "premade", "description": "Calm storytelling", "gender": "female", "accent": "british", "age": "young", "preview_url": ""},
    {"voice_id": "pNInz6obpgDQGcFmaJgB", "name": "Adam", "category": "premade", "description": "Confident male narrator", "gender": "male", "accent": "american", "age": "middle_aged", "preview_url": ""},
    {"voice_id": "onwK4e9ZLuTAKqWW03F9", "name": "Daniel", "category": "premade", "description": "Clear educational tone", "gender": "male", "accent": "british", "age": "middle_aged", "preview_url": ""},
]
_voice_catalog_cache = {"ts": 0.0, "source": "unknown", "provider_ok": False, "count": 0, "warning": "not_checked"}


def _fallback_voice_catalog() -> list[dict]:
    return [dict(v) for v in DEFAULT_ELEVENLABS_VOICES]


def _cache_voice_catalog(source: str, provider_ok: bool, count: int, warning: str = ""):
    _voice_catalog_cache["ts"] = time.time()
    _voice_catalog_cache["source"] = source
    _voice_catalog_cache["provider_ok"] = bool(provider_ok)
    _voice_catalog_cache["count"] = max(0, int(count or 0))
    _voice_catalog_cache["warning"] = warning or ""


async def _fetch_voice_catalog() -> tuple[list[dict], str, bool, str]:
    if not ELEVENLABS_API_KEY:
        warning = "ElevenLabs API key not configured; using fallback voices."
        return _fallback_voice_catalog(), "fallback", False, warning
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                "https://api.elevenlabs.io/v1/voices",
                headers={"xi-api-key": ELEVENLABS_API_KEY},
            )
            resp.raise_for_status()
            data = resp.json()
        voices = []
        for v in data.get("voices", []):
            voices.append({
                "voice_id": v["voice_id"],
                "name": v.get("name", "Unknown"),
                "category": v.get("category", ""),
                "description": v.get("labels", {}).get("description", ""),
                "gender": v.get("labels", {}).get("gender", ""),
                "accent": v.get("labels", {}).get("accent", ""),
                "age": v.get("labels", {}).get("age", ""),
                "preview_url": v.get("preview_url", ""),
            })
        if voices:
            return voices, "elevenlabs", True, ""
        warning = "ElevenLabs returned zero voices; using fallback voices."
        return _fallback_voice_catalog(), "fallback", False, warning
    except Exception as e:
        warning = f"ElevenLabs voice catalog unavailable ({type(e).__name__}); using fallback voices."
        log.warning(warning)
        return _fallback_voice_catalog(), "fallback", False, warning


async def _voice_provider_snapshot(force_refresh: bool = False) -> dict:
    age_sec = time.time() - float(_voice_catalog_cache.get("ts", 0.0))
    if force_refresh or age_sec > 60.0:
        voices, source, provider_ok, warning = await _fetch_voice_catalog()
        _cache_voice_catalog(source, provider_ok, len(voices), warning)
    return {
        "source": str(_voice_catalog_cache.get("source", "unknown")),
        "provider_ok": bool(_voice_catalog_cache.get("provider_ok", False)),
        "count": int(_voice_catalog_cache.get("count", 0) or 0),
        "warning": str(_voice_catalog_cache.get("warning", "") or ""),
        "age_sec": round(max(0.0, age_sec), 1),
    }


def _resolve_latest_frontend_assets() -> tuple[str, str]:
    now = time.time()
    if now - float(_frontend_asset_cache.get("ts", 0.0)) < 10.0:
        return str(_frontend_asset_cache.get("js", "")), str(_frontend_asset_cache.get("css", ""))
    js_name = ""
    css_name = ""
    try:
        default_dist = (Path(__file__).resolve().parent / "ViralShorts-App" / "dist").resolve()
        dist_root = Path(os.getenv("FRONTEND_DIST_DIR", str(default_dist))).resolve()
        assets_dir = dist_root / "assets"
        if assets_dir.exists():
            js_candidates = sorted(assets_dir.glob("index-*.js"), key=lambda p: p.stat().st_mtime, reverse=True)
            css_candidates = sorted(assets_dir.glob("index-*.css"), key=lambda p: p.stat().st_mtime, reverse=True)
            if js_candidates:
                js_name = js_candidates[0].name
            if css_candidates:
                css_name = css_candidates[0].name
    except Exception:
        js_name = ""
        css_name = ""
    _frontend_asset_cache["ts"] = now
    _frontend_asset_cache["js"] = js_name
    _frontend_asset_cache["css"] = css_name
    return js_name, css_name


def _resolve_frontend_asset_path(filename: str) -> Path:
    default_dist = (Path(__file__).resolve().parent / "ViralShorts-App" / "dist").resolve()
    dist_root = Path(os.getenv("FRONTEND_DIST_DIR", str(default_dist))).resolve()
    return dist_root / "assets" / filename


def _apply_runtime_js_text_hotfix(js: str) -> str:
    """Patch legacy pricing strings in stale frontend bundles."""
    if not js:
        return js
    js = js.replace("Unlimited videos", "300 videos/month")
    js = js.replace("Sign Up Free", "Sign Up to Subscribe")
    js = js.replace("Start Creating Free", "Start Creating")
    return js


def _read_deploy_meta() -> tuple[str, str]:
    now = time.time()
    if now - float(_deploy_meta_cache.get("ts", 0.0)) < 15.0:
        return str(_deploy_meta_cache.get("backend_commit", "")), str(_deploy_meta_cache.get("frontend_bundle", ""))

    backend_commit = (os.getenv("STUDIO_COMMIT_SHA", "") or os.getenv("GITHUB_SHA", "")).strip()
    if not backend_commit:
        try:
            backend_commit = subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=str(Path(__file__).resolve().parent),
                text=True,
                timeout=2,
            ).strip()
        except Exception:
            backend_commit = ""

    frontend_bundle = ""
    try:
        default_dist = (Path(__file__).resolve().parent / "ViralShorts-App" / "dist").resolve()
        current_dist = Path(os.getenv("FRONTEND_DIST_DIR", str(default_dist))).resolve()
        index_path = current_dist / "index.html"
        if index_path.exists():
            html = index_path.read_text(encoding="utf-8", errors="ignore")
            m = re.search(r"/assets/(index-[^\"']+\.js)", html)
            if m:
                frontend_bundle = m.group(1)
        if not frontend_bundle:
            latest_js, _ = _resolve_latest_frontend_assets()
            frontend_bundle = latest_js
    except Exception:
        frontend_bundle = ""

    _deploy_meta_cache["ts"] = now
    _deploy_meta_cache["backend_commit"] = backend_commit
    _deploy_meta_cache["frontend_bundle"] = frontend_bundle
    return backend_commit, frontend_bundle

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def _disable_html_cache(request: Request, call_next):
    """Prevent stale frontend shell and asset caching so new bundles load immediately."""
    response = await call_next(request)
    path = request.url.path or ""
    if path == "/" or path.endswith(".html"):
        try:
            content_type = str(response.headers.get("content-type", "")).lower()
            if "text/html" in content_type and hasattr(response, "body_iterator"):
                body = b""
                async for chunk in response.body_iterator:
                    body += chunk
                html = body.decode("utf-8", errors="ignore")
                latest_js, latest_css = _resolve_latest_frontend_assets()
                # Keep the compiled JS asset path from the built index.html.
                # Overriding to runtime-hotfix.js can mask newly deployed frontend bundles.
                if latest_css:
                    html = re.sub(r"/assets/index-[^\"']+\.css(\?[^\"']*)?", f"/assets/{latest_css}?v={_frontend_cache_buster}", html)
                headers = dict(response.headers)
                headers.pop("content-length", None)
                headers.pop("Content-Length", None)
                headers.pop("content-type", None)
                headers.pop("Content-Type", None)
                response = Response(
                    content=html,
                    status_code=response.status_code,
                    headers=headers,
                    media_type="text/html",
                )
        except Exception:
            pass
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    if path.startswith("/assets/") and (path.endswith(".js") or path.endswith(".css")):
        # Do not consume streaming asset responses in middleware.
        # Reading body_iterator here can drain the response and cause truncated JS (blank page).
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        # Extra hints for CDN layers to avoid stale bundle reuse.
        response.headers["CDN-Cache-Control"] = "no-store"
        response.headers["Cloudflare-CDN-Cache-Control"] = "no-store"
    return response


@app.get("/assets/runtime-hotfix.js")
async def serve_runtime_hotfix_js():
    """Serve JS with runtime text hotfixes to bypass stale CDN bundle caching."""
    latest_js, _ = _resolve_latest_frontend_assets()
    target = _resolve_frontend_asset_path(latest_js) if latest_js else _resolve_frontend_asset_path("index-BlMPK7KO.js")
    if not target.exists():
        fallback = _resolve_frontend_asset_path("index-BlMPK7KO.js")
        if fallback.exists():
            target = fallback
        else:
            raise HTTPException(status_code=404, detail="Hotfix JS not found")
    js = _apply_runtime_js_text_hotfix(target.read_text(encoding="utf-8", errors="ignore"))
    resp = Response(content=js, media_type="text/javascript")
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.get("/assets/index-BlMPK7KO.js")
async def serve_legacy_firefox_bundle_alias():
    """Alias stale cached bundle URL to the latest built JS asset."""
    latest_js, _ = _resolve_latest_frontend_assets()
    if latest_js:
        latest_path = _resolve_frontend_asset_path(latest_js)
        if latest_path.exists():
            return FileResponse(str(latest_path), media_type="text/javascript")
    legacy_path = _resolve_frontend_asset_path("index-BlMPK7KO.js")
    if legacy_path.exists():
        return FileResponse(str(legacy_path), media_type="text/javascript")
    raise HTTPException(status_code=404, detail="Asset not found")


jobs: dict = {}
security = HTTPBearer(auto_error=False)
init_queue_runtime(jobs, log)
AUTO_SCENE_IMAGE_ROOT = Path(TRAINING_DATA_DIR) / "auto_scene_images"
AUTO_SCENE_IMAGE_ROOT.mkdir(parents=True, exist_ok=True)
LONGFORM_SESSIONS_FILE = TEMP_DIR / "longform_sessions_store.json"
LONGFORM_PREVIEW_DIR = TEMP_DIR / "longform_previews"
LONGFORM_PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
_longform_sessions: dict[str, dict] = {}
_longform_sessions_lock = asyncio.Lock()
_JOB_RETENTION_ACTIVE_SEC = 12 * 3600
_JOB_RETENTION_FINAL_SEC = 2 * 3600

# Runtime banner state can be updated by admin without restart.
_maintenance_banner_enabled = bool(MAINTENANCE_BANNER_ENABLED)
_maintenance_banner_message = (
    (MAINTENANCE_BANNER_MESSAGE or "").strip()
    or "Studio is under high load. Queue times may be longer than usual while we scale capacity."
)
KPI_TARGETS = {
    "first_render_success_rate": 0.95,
    "time_to_publishable_sec": 8 * 60,
    "estimated_cost_per_short_usd": 2.00,
}
KPI_METRICS_PATH = TEMP_DIR / "kpi_metrics.json"
TOPUP_WALLET_PATH = TEMP_DIR / "topup_wallets.json"
PAYPAL_ORDERS_PATH = TEMP_DIR / "paypal_orders.json"
PAYPAL_SUBSCRIPTIONS_PATH = TEMP_DIR / "paypal_subscriptions.json"
USAGE_LEDGER_PATH = TEMP_DIR / "usage_ledger.jsonl"
LANDING_NOTIFICATIONS_PATH = TEMP_DIR / "landing_notifications.json"
LANDING_NOTIFICATIONS_LIMIT = 120
LANDING_NOTIFICATIONS_PUBLIC_LIMIT = 25
_kpi_metrics = {
    "total_jobs": 0,
    "completed_jobs": 0,
    "error_jobs": 0,
    "first_render_pass_jobs": 0,
    "total_publishable_time_sec": 0.0,
    "total_estimated_cost_usd": 0.0,
    "template_breakdown": {},
    "updated_at": 0.0,
}
_topup_wallets: dict[str, dict] = {}
_topup_wallet_lock = asyncio.Lock()
_paypal_orders: dict[str, dict] = {}
_paypal_orders_lock = asyncio.Lock()
_paypal_subscriptions: dict[str, dict] = {}
_paypal_subscriptions_lock = asyncio.Lock()
_landing_notifications: list[dict] = []
_landing_notifications_lock = asyncio.Lock()


def _load_kpi_metrics() -> None:
    global _kpi_metrics
    try:
        if KPI_METRICS_PATH.exists():
            loaded = json.loads(KPI_METRICS_PATH.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                _kpi_metrics = {**_kpi_metrics, **loaded}
    except Exception:
        pass


def _save_kpi_metrics() -> None:
    try:
        KPI_METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
        KPI_METRICS_PATH.write_text(json.dumps(_kpi_metrics, indent=2), encoding="utf-8")
    except Exception:
        pass


def _load_topup_wallets() -> None:
    global _topup_wallets
    try:
        if TOPUP_WALLET_PATH.exists():
            data = json.loads(TOPUP_WALLET_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                _topup_wallets = data
    except Exception:
        _topup_wallets = {}


def _save_topup_wallets() -> None:
    try:
        TOPUP_WALLET_PATH.parent.mkdir(parents=True, exist_ok=True)
        TOPUP_WALLET_PATH.write_text(json.dumps(_topup_wallets, indent=2), encoding="utf-8")
    except Exception:
        pass


def _load_paypal_orders() -> None:
    global _paypal_orders
    try:
        if PAYPAL_ORDERS_PATH.exists():
            data = json.loads(PAYPAL_ORDERS_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                _paypal_orders = data
                return
    except Exception:
        pass
    _paypal_orders = {}


def _save_paypal_orders() -> None:
    try:
        PAYPAL_ORDERS_PATH.parent.mkdir(parents=True, exist_ok=True)
        PAYPAL_ORDERS_PATH.write_text(json.dumps(_paypal_orders, indent=2), encoding="utf-8")
    except Exception:
        pass


def _load_paypal_subscriptions() -> None:
    global _paypal_subscriptions
    try:
        if PAYPAL_SUBSCRIPTIONS_PATH.exists():
            data = json.loads(PAYPAL_SUBSCRIPTIONS_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                _paypal_subscriptions = data
                return
    except Exception:
        pass
    _paypal_subscriptions = {}


def _save_paypal_subscriptions() -> None:
    try:
        PAYPAL_SUBSCRIPTIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
        PAYPAL_SUBSCRIPTIONS_PATH.write_text(json.dumps(_paypal_subscriptions, indent=2), encoding="utf-8")
    except Exception:
        pass


def _prune_longform_sessions(max_age_seconds: int = 72 * 3600) -> None:
    now = time.time()
    stale = [
        sid for sid, sess in list(_longform_sessions.items())
        if now - float((sess or {}).get("created_at", now)) > max_age_seconds
    ]
    for sid in stale:
        _longform_sessions.pop(sid, None)


def _load_longform_sessions() -> None:
    try:
        if not LONGFORM_SESSIONS_FILE.exists():
            return
        data = json.loads(LONGFORM_SESSIONS_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            _longform_sessions.clear()
            _longform_sessions.update(data)
            _prune_longform_sessions()
    except Exception:
        _longform_sessions.clear()


def _save_longform_sessions() -> None:
    try:
        _prune_longform_sessions()
        LONGFORM_SESSIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
        LONGFORM_SESSIONS_FILE.write_text(json.dumps(_longform_sessions, ensure_ascii=True), encoding="utf-8")
    except Exception:
        pass


def _mask_email_for_public(email: str) -> str:
    raw = str(email or "").strip().lower()
    if "@" not in raw:
        return "a creator"
    local, domain = raw.split("@", 1)
    local = local.strip()
    domain = domain.strip()
    if not local or not domain:
        return "a creator"
    if len(local) <= 2:
        safe_local = local[0] + "*"
    else:
        safe_local = local[:2] + ("*" * min(4, max(1, len(local) - 2)))
    return f"{safe_local}@{domain}"


def _load_landing_notifications() -> None:
    global _landing_notifications
    try:
        if LANDING_NOTIFICATIONS_PATH.exists():
            data = json.loads(LANDING_NOTIFICATIONS_PATH.read_text(encoding="utf-8"))
            if isinstance(data, list):
                cleaned: list[dict] = []
                for item in data[-LANDING_NOTIFICATIONS_LIMIT:]:
                    if isinstance(item, dict):
                        cleaned.append(item)
                _landing_notifications = cleaned
                return
    except Exception:
        pass
    _landing_notifications = []


def _save_landing_notifications() -> None:
    try:
        LANDING_NOTIFICATIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
        LANDING_NOTIFICATIONS_PATH.write_text(
            json.dumps(_landing_notifications[-LANDING_NOTIFICATIONS_LIMIT:], ensure_ascii=True, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


async def _append_landing_notification(event_type: str, plan: str = "", credits: int = 0, customer_email: str = "") -> None:
    evt_type = str(event_type or "").strip().lower()
    if evt_type not in {"subscription", "topup"}:
        return
    now = time.time()
    event = {
        "type": evt_type,
        "plan": str(plan or "").strip().lower(),
        "credits": int(max(0, credits)),
        "email_masked": _mask_email_for_public(customer_email),
        "ts": now,
    }
    async with _landing_notifications_lock:
        _landing_notifications.append(event)
        if len(_landing_notifications) > LANDING_NOTIFICATIONS_LIMIT:
            _landing_notifications[:] = _landing_notifications[-LANDING_NOTIFICATIONS_LIMIT:]
        _save_landing_notifications()


def _month_key(ts: float | None = None) -> str:
    now = datetime.fromtimestamp(ts or time.time(), tz=timezone.utc)
    return f"{now.year:04d}-{now.month:02d}"


def _wallet_for_user(user_id: str) -> dict:
    if not user_id:
        return {
            "topup_credits": 0,
            "animated_topup_credits": 0,
            "monthly_usage": {},
            "monthly_usage_non_animated": {},
            "updated_at": time.time(),
        }
    wallet = _topup_wallets.get(user_id)
    if not isinstance(wallet, dict):
        wallet = {
            "topup_credits": 0,
            "animated_topup_credits": 0,
            "monthly_usage": {},
            "monthly_usage_non_animated": {},
            "updated_at": time.time(),
        }
        _topup_wallets[user_id] = wallet
    wallet.setdefault("topup_credits", 0)  # legacy mirror
    wallet.setdefault("animated_topup_credits", int(wallet.get("topup_credits", 0) or 0))
    wallet.setdefault("monthly_usage", {})
    wallet.setdefault("monthly_usage_non_animated", {})
    wallet.setdefault("updated_at", time.time())
    return wallet


def _plan_monthly_animated_limit(plan: str) -> int:
    limits = PLAN_LIMITS.get(plan, PLAN_LIMITS.get("starter", {}))
    return int(limits.get("animated_renders_per_month", limits.get("videos_per_month", 0)) or 0)


def _plan_monthly_non_animated_limit(plan: str) -> int:
    limits = PLAN_LIMITS.get(plan, PLAN_LIMITS.get("starter", {}))
    fallback = int(limits.get("animated_renders_per_month", limits.get("videos_per_month", 0)) or 0) * 10
    return int(limits.get("non_animated_ops_per_month", fallback) or 0)


def _credit_state_for_user(user: dict, effective_plan: str, billing_active: bool, is_admin: bool = False) -> dict:
    if is_admin:
        return {
            "animated_monthly_limit": 9999,
            "animated_monthly_used": 0,
            "animated_monthly_remaining": 9999,
            "animated_topup_credits": 9999,
            "animated_total_remaining": 9999,
            "non_animated_monthly_limit": 9999,
            "non_animated_monthly_used": 0,
            "non_animated_monthly_remaining": 9999,
            "requires_topup": False,
            "month_key": _month_key(),
            # Backward-compatible aliases.
            "monthly_limit": 9999,
            "monthly_used": 0,
            "monthly_remaining": 9999,
            "topup_credits": 9999,
            "credits_total_remaining": 9999,
        }
    user_id = str(user.get("id", "") or "")
    wallet = _wallet_for_user(user_id)
    mk = _month_key()
    animated_used = int((wallet.get("monthly_usage", {}) or {}).get(mk, 0) or 0)
    non_animated_used = int((wallet.get("monthly_usage_non_animated", {}) or {}).get(mk, 0) or 0)
    animated_limit = _plan_monthly_animated_limit(effective_plan) if billing_active else 0
    non_animated_limit = 999999
    animated_remaining = max(0, animated_limit - animated_used)
    non_animated_remaining = max(0, non_animated_limit - non_animated_used)
    topup = int(wallet.get("animated_topup_credits", wallet.get("topup_credits", 0)) or 0)
    total_remaining = animated_remaining + topup
    return {
        "animated_monthly_limit": animated_limit,
        "animated_monthly_used": animated_used,
        "animated_monthly_remaining": animated_remaining,
        "animated_topup_credits": topup,
        "animated_total_remaining": total_remaining,
        "non_animated_monthly_limit": non_animated_limit,
        "non_animated_monthly_used": non_animated_used,
        "non_animated_monthly_remaining": non_animated_remaining,
        "requires_topup": bool(total_remaining <= 0),
        "month_key": mk,
        # Backward-compatible aliases.
        "monthly_limit": animated_limit,
        "monthly_used": animated_used,
        "monthly_remaining": animated_remaining,
        "topup_credits": topup,
        "credits_total_remaining": total_remaining,
    }


async def _reserve_generation_credit(
    user: dict,
    effective_plan: str,
    billing_active: bool,
    is_admin: bool = False,
    usage_kind: str = "animated",
    credits_needed: int = 1,
) -> tuple[bool, str, dict]:
    if is_admin:
        return True, "admin", _credit_state_for_user(user, effective_plan, billing_active, is_admin=True)
    user_id = str(user.get("id", "") or "")
    required_credits = max(1, int(credits_needed or 1))
    async with _topup_wallet_lock:
        wallet = _wallet_for_user(user_id)
        state = _credit_state_for_user(user, effective_plan, billing_active, is_admin=False)
        mk = state["month_key"]
        if usage_kind == "non_animated":
            # Non-animated work is free (slideshows/scripts/images).
            usage = dict(wallet.get("monthly_usage_non_animated", {}) or {})
            usage[mk] = int(usage.get(mk, 0) or 0) + 1
            wallet["monthly_usage_non_animated"] = usage
            wallet["updated_at"] = time.time()
            _save_topup_wallets()
            return True, "non_animated_free", _credit_state_for_user(user, effective_plan, billing_active, is_admin=False)
        monthly_remaining = int(state.get("animated_monthly_remaining", 0) or 0)
        if monthly_remaining >= required_credits:
            usage = dict(wallet.get("monthly_usage", {}) or {})
            usage[mk] = int(usage.get(mk, 0) or 0) + required_credits
            wallet["monthly_usage"] = usage
            wallet["updated_at"] = time.time()
            _save_topup_wallets()
            refreshed = _credit_state_for_user(user, effective_plan, billing_active, is_admin=False)
            refreshed["credits_needed"] = required_credits
            return True, "monthly", refreshed
        topup = int(wallet.get("animated_topup_credits", wallet.get("topup_credits", 0)) or 0)
        if topup >= required_credits:
            wallet["animated_topup_credits"] = topup - required_credits
            wallet["topup_credits"] = wallet["animated_topup_credits"]  # keep legacy mirror updated
            wallet["updated_at"] = time.time()
            _save_topup_wallets()
            refreshed = _credit_state_for_user(user, effective_plan, billing_active, is_admin=False)
            refreshed["credits_needed"] = required_credits
            return True, "topup", refreshed
        state["credits_needed"] = required_credits
        return False, "topup_required", state


async def _refund_generation_credit(user_id: str, source: str, month_key: str = "", credits: int = 1) -> None:
    if not user_id or source not in {"monthly", "topup"}:
        return
    credit_amount = max(1, int(credits or 1))
    async with _topup_wallet_lock:
        wallet = _wallet_for_user(user_id)
        if source == "topup":
            wallet["animated_topup_credits"] = int(wallet.get("animated_topup_credits", wallet.get("topup_credits", 0)) or 0) + credit_amount
            wallet["topup_credits"] = wallet["animated_topup_credits"]
        else:
            mk = month_key or _month_key()
            usage = dict(wallet.get("monthly_usage", {}) or {})
            usage[mk] = max(0, int(usage.get(mk, 0) or 0) - credit_amount)
            wallet["monthly_usage"] = usage
        wallet["updated_at"] = time.time()
        _save_topup_wallets()


def _append_usage_ledger(event: dict) -> None:
    try:
        USAGE_LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
        with USAGE_LEDGER_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=True) + "\n")
    except Exception:
        pass


async def _credit_topup_wallet(user_id: str, credits: int, source: str, stripe_session_id: str = "") -> None:
    if not user_id or credits <= 0:
        return
    async with _topup_wallet_lock:
        wallet = _wallet_for_user(user_id)
        wallet["animated_topup_credits"] = int(wallet.get("animated_topup_credits", wallet.get("topup_credits", 0)) or 0) + int(credits)
        wallet["topup_credits"] = wallet["animated_topup_credits"]
        wallet["updated_at"] = time.time()
        _save_topup_wallets()
    _append_usage_ledger({
        "type": "topup_credit",
        "user_id": user_id,
        "credits": int(credits),
        "source": source,
        "stripe_session_id": stripe_session_id,
        "ts": time.time(),
    })


def _estimate_job_cost_usd(job_state: dict) -> float:
    template = str(job_state.get("template", "") or "")
    mode = str(job_state.get("generation_mode", "video") or "video")
    resolution = str(job_state.get("resolution", "720p") or "720p")
    total_scenes = int(job_state.get("total_scenes", 0) or 0)
    if total_scenes <= 0 and isinstance(job_state.get("scene_assets"), list):
        total_scenes = len(job_state.get("scene_assets", []))
    total_scenes = max(total_scenes, 1)
    # Heuristic estimate tuned to current FAL/Grok economics.
    image_scene_cost = 0.03 if template == "skeleton" else 0.04
    video_scene_cost = 0.16 if resolution == "720p" else 0.25
    per_scene = image_scene_cost + (video_scene_cost if mode == "video" else 0.0)
    return round(total_scenes * per_scene, 3)


def _record_kpi_for_job(job_id: str, job_state: dict) -> None:
    if not isinstance(job_state, dict):
        return
    status = str(job_state.get("status", "") or "")
    if status not in {"complete", "error"}:
        return
    if bool(job_state.get("kpi_recorded")):
        return
    created_at = float(job_state.get("created_at") or time.time())
    terminal_at = float(job_state.get("completed_at") or time.time())
    publishable_sec = max(0.0, terminal_at - created_at)
    template = str(job_state.get("template", "unknown") or "unknown")
    estimated_cost = _estimate_job_cost_usd(job_state)
    regenerate_count = int(job_state.get("regenerate_count", 0) or 0)
    animation_warnings = int(job_state.get("animation_warnings", 0) or 0)
    first_render_pass = (status == "complete" and regenerate_count == 0 and animation_warnings == 0)

    _kpi_metrics["total_jobs"] = int(_kpi_metrics.get("total_jobs", 0)) + 1
    if status == "complete":
        _kpi_metrics["completed_jobs"] = int(_kpi_metrics.get("completed_jobs", 0)) + 1
        _kpi_metrics["total_publishable_time_sec"] = float(_kpi_metrics.get("total_publishable_time_sec", 0.0)) + publishable_sec
    else:
        _kpi_metrics["error_jobs"] = int(_kpi_metrics.get("error_jobs", 0)) + 1
    if first_render_pass:
        _kpi_metrics["first_render_pass_jobs"] = int(_kpi_metrics.get("first_render_pass_jobs", 0)) + 1
    _kpi_metrics["total_estimated_cost_usd"] = float(_kpi_metrics.get("total_estimated_cost_usd", 0.0)) + estimated_cost
    by_template = _kpi_metrics.setdefault("template_breakdown", {})
    entry = by_template.setdefault(template, {"total": 0, "complete": 0, "error": 0})
    entry["total"] = int(entry.get("total", 0)) + 1
    if status == "complete":
        entry["complete"] = int(entry.get("complete", 0)) + 1
    else:
        entry["error"] = int(entry.get("error", 0)) + 1
    _kpi_metrics["updated_at"] = time.time()
    job_state["kpi_recorded"] = True
    _save_kpi_metrics()


def _normalize_output_resolution(requested: str, priority_allowed: bool = False) -> str:
    resolution = requested if requested in RESOLUTION_CONFIGS else "720p"
    if FORCE_720P_ONLY:
        if resolution.endswith("_landscape"):
            return "720p_landscape"
        return "720p"
    if not priority_allowed and resolution == "1080p":
        return "720p"
    if not priority_allowed and resolution == "1080p_landscape":
        return "720p_landscape"
    return resolution


def _resolve_user_plan_for_limits(user: dict | None) -> tuple[str, dict]:
    if not user:
        return "starter", PLAN_LIMITS["starter"]
    email = str(user.get("email", "") or "")
    if email in ADMIN_EMAILS:
        pro = dict(PLAN_LIMITS.get("pro", PLAN_LIMITS["starter"]))
        pro["videos_per_month"] = max(int(pro.get("videos_per_month", 300) or 300), 9999)
        return "pro", pro
    plan = str(user.get("plan", "starter") or "starter")
    if plan in {"free", "none", "admin"}:
        plan = "starter"
    if plan not in PLAN_LIMITS:
        plan = "starter"
    return plan, PLAN_LIMITS.get(plan, PLAN_LIMITS["starter"])


def _plan_features_for(plan: str, is_admin: bool = False) -> list[str]:
    tier = "pro" if is_admin else (plan if plan in PLAN_FEATURES else "starter")
    features = list(PLAN_FEATURES.get(tier, PLAN_FEATURES.get("starter", [])))
    if is_admin and "admin_unlimited_access" not in features:
        features.append("admin_unlimited_access")
    return features


def _bool_from_any(value, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off"):
        return False
    return default


LONGFORM_ALLOWED_TEMPLATES = {"story", "skeleton"}
LONGFORM_WHISPER_MODES = {"off", "subtle", "cinematic"}


def _longform_owner_beta_enabled(user: dict | None) -> bool:
    return bool((_public_lane_access_for_user(user) or {}).get("longform"))


def _normalize_longform_template(value: str) -> str:
    template = str(value or "").strip().lower()
    return template if template in LONGFORM_ALLOWED_TEMPLATES else "story"


def _normalize_longform_target_minutes(value) -> float:
    try:
        minutes = float(value)
    except Exception:
        minutes = float(LONGFORM_DEFAULT_TARGET_MINUTES)
    return max(float(LONGFORM_MIN_TARGET_MINUTES), min(float(LONGFORM_MAX_TARGET_MINUTES), minutes))


def _normalize_longform_whisper_mode(value: str) -> str:
    mode = str(value or "subtle").strip().lower()
    return mode if mode in LONGFORM_WHISPER_MODES else "subtle"


def _normalize_longform_language(value: str) -> str:
    lang = str(value or "en").strip().lower()
    return lang if lang in SUPPORTED_LANGUAGES else "en"


def _longform_detect_tone(template: str, topic: str, input_title: str, input_description: str) -> str:
    text = " ".join([
        str(template or "").strip().lower(),
        str(topic or "").strip().lower(),
        str(input_title or "").strip().lower(),
        str(input_description or "").strip().lower(),
    ])
    if not text:
        return "neutral"
    horror_markers = (
        r"\bhorror\b",
        r"\bscary\b",
        r"\beerie\b",
        r"\bcreepy\b",
        r"\bhaunt(ed|ing)?\b",
        r"\bghost(s)?\b",
        r"\bnightmare(s)?\b",
        r"\bdread\b",
        r"\bominous\b",
        r"\bdark\b",
        r"\bfog\b",
        r"\bforest\b",
        r"\bvanish(ed|ing)?\b",
        r"\bmissing\b",
        r"\bunsolved\b",
        r"\bmystery\b",
        r"\bnowhere\b",
    )
    if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in horror_markers):
        return "horror"
    return "neutral"


LONGFORM_HORROR_VISUAL_DIRECTIVE = (
    "Horror tone lock: psychological dread, ominous atmosphere, eerie shadows, moody low-key lighting, drifting fog/mist, "
    "and unsettling cinematic realism. Keep it grounded and tense. No gore, no comedy, no bright cheerful styling."
)


def _longform_is_horror_tone(tone: str) -> bool:
    return str(tone or "").strip().lower() == "horror"


def _longform_tone_locked_visual_description(visual_description: str, tone: str, template: str) -> str:
    base = str(visual_description or "").strip()
    if not _longform_is_horror_tone(tone):
        return base
    lower = base.lower()
    if "horror tone lock:" in lower:
        return base
    # Keep skeleton identity strict while forcing horror-compatible environment and grade.
    skeleton_horror_hint = ""
    if str(template or "").strip().lower() == "skeleton":
        skeleton_horror_hint = (
            " Environment must fit a horror mystery beat (abandoned roads, dark forests, empty corridors, foggy night exteriors) "
            "while preserving the same canonical skeleton identity."
        )
    return (base + " " + LONGFORM_HORROR_VISUAL_DIRECTIVE + skeleton_horror_hint).strip()


def _longform_enforce_tone_on_scenes(scenes: list[dict], tone: str, template: str) -> list[dict]:
    out: list[dict] = []
    for raw_scene in list(scenes or []):
        scene = dict(raw_scene or {})
        scene["visual_description"] = _longform_tone_locked_visual_description(
            str(scene.get("visual_description", "") or ""),
            tone=tone,
            template=template,
        )
        out.append(scene)
    return out


def _longform_chapter_count_for_minutes(target_minutes: float) -> int:
    # Keep chapter granularity manageable for review/approval.
    return max(3, min(12, int(round(float(target_minutes) * 1.1))))


def _longform_brand_slot(index: int, total_chapters: int) -> str:
    if index == max(0, total_chapters - 1):
        return "outro"
    return ""


def _longform_title_variant(input_title: str, topic: str) -> str:
    clean_title = str(input_title or "").strip()
    clean_topic = str(topic or "").strip()
    if clean_title:
        return clean_title
    return (clean_topic[:120] or "Long-Form Video").strip()


REFERENCE_LOCK_MODES = {"strict", "inspired"}
TEMPLATE_DEFAULT_REFERENCE_URLS = {
    "skeleton": str(SKELETON_GLOBAL_REFERENCE_IMAGE_URL or "").strip(),
    "story": str(STORY_GLOBAL_REFERENCE_IMAGE_URL or "").strip(),
    "motivation": str(MOTIVATION_GLOBAL_REFERENCE_IMAGE_URL or "").strip(),
}
DEFAULT_ART_STYLE_PRESETS = {
    "auto": "",
    "cinematic_realism": "Photoreal cinematic realism with natural skin detail, physically-plausible lighting, clean lens behavior, and premium film color grade.",
    "commercial_polish": "High-end commercial look: crisp product-grade detail, controlled highlights, clean background separation, and premium ad-level finish.",
    "moody_noir": "Moody low-key cinematic style with rich shadow contrast, tasteful grain-free clarity, and dramatic yet realistic lighting.",
    "bright_lifestyle": "Bright modern lifestyle aesthetic with soft natural light, inviting color balance, realistic textures, and clean premium framing.",
}


def _story_art_style_catalog_path() -> Path:
    return Path(__file__).parent / "ViralShorts-App" / "src" / "studio" / "lib" / "storyArtStyles.json"


def _load_story_art_style_presets() -> dict[str, str]:
    catalog_path = _story_art_style_catalog_path()
    try:
        rows = json.loads(catalog_path.read_text(encoding="utf-8"))
    except Exception:
        return dict(DEFAULT_ART_STYLE_PRESETS)
    if not isinstance(rows, list):
        return dict(DEFAULT_ART_STYLE_PRESETS)
    out: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        style_id = str(row.get("id", "") or "").strip().lower()
        if not style_id:
            continue
        out[style_id] = str(row.get("prompt", "") or "").strip()
    if "auto" not in out:
        out["auto"] = ""
    return out or dict(DEFAULT_ART_STYLE_PRESETS)


ART_STYLE_PRESETS = _load_story_art_style_presets()


def _normalize_reference_lock_mode(value, default: str = "strict") -> str:
    text = str(value or default).strip().lower()
    return text if text in REFERENCE_LOCK_MODES else default


def _default_reference_for_template(template: str) -> str:
    return str(TEMPLATE_DEFAULT_REFERENCE_URLS.get(str(template or "").strip().lower(), "") or "").strip()


def _normalize_reference_with_default(template: str, reference_image_url: str) -> str:
    value = str(reference_image_url or "").strip()
    if value:
        return value
    return _default_reference_for_template(template)


def _is_template_default_reference(template: str, reference_image_url: str) -> bool:
    current = str(reference_image_url or "").strip()
    default_ref = _default_reference_for_template(template)
    return bool(current and default_ref and current == default_ref)


def _skeleton_session_has_explicit_reference(session: dict | None) -> bool:
    if not isinstance(session, dict):
        return False
    return bool(
        session.get("reference_image_uploaded")
        or session.get("custom_reference_uploaded")
    )


def _skeleton_default_identity_locked(session: dict | None) -> bool:
    return not _skeleton_session_has_explicit_reference(session)


def _reference_url_to_local_asset_path(reference_image_url: str) -> Path | None:
    source = str(reference_image_url or "").strip()
    if not source:
        return None
    if Path(source).exists():
        return Path(source)
    try:
        parsed = urlparse(source)
    except Exception:
        return None
    if parsed.scheme not in {"http", "https"}:
        return None
    rel_path = unquote(str(parsed.path or "").lstrip("/"))
    if not rel_path:
        return None
    rel_parts = [part for part in Path(rel_path).parts if part not in ("..", "")]
    if not rel_parts:
        return None
    candidates = (
        Path(__file__).parent / "ViralShorts-App" / "public" / Path(*rel_parts),
        Path(__file__).parent / "public" / Path(*rel_parts),
    )
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


async def _read_reference_image_bytes(reference_image_url: str) -> bytes:
    source = str(reference_image_url or "").strip()
    if not source:
        return b""
    if source.startswith("data:image/"):
        raw, _mime = _decode_data_image_url(source)
        return raw or b""
    local_path = _reference_url_to_local_asset_path(source)
    if local_path is not None:
        try:
            return local_path.read_bytes()
        except Exception:
            return b""
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            resp = await client.get(source)
        if resp.status_code >= 400:
            return b""
        return bytes(resp.content or b"")
    except Exception:
        return b""


async def _extract_reference_profile(reference_image_url: str, template: str, lock_mode: str) -> tuple[dict, dict]:
    source = str(reference_image_url or "").strip()
    if not source:
        return {}, {}
    raw_ref = await _read_reference_image_bytes(source)
    if not raw_ref:
        return {}, {}
    quality = _analyze_reference_quality(raw_ref, lock_mode=lock_mode)
    reference_dna = _extract_reference_dna(raw_ref, template=template)
    return reference_dna, quality


def _billing_site_url() -> str:
    configured = str(os.getenv("BILLING_SITE_URL", "") or "").strip().rstrip("/")
    if configured:
        return configured
    site = str(SITE_URL or "").strip().rstrip("/")
    if not site:
        return site
    match = re.match(r"^(https?://)([^/]+)(.*)$", site, flags=re.IGNORECASE)
    if not match:
        return site
    scheme, host, suffix = match.groups()
    host_l = host.lower()
    for apex in ("nyptidindustries.com", "niptidindustries.com"):
        if host_l == f"billing.{apex}":
            return f"{scheme}{host}{suffix}"
        if host_l == apex:
            return f"{scheme}billing.{apex}{suffix}"
        if host_l.endswith("." + apex):
            return f"{scheme}billing.{apex}{suffix}"
    return site


def _api_public_url() -> str:
    configured = str(os.getenv("API_PUBLIC_URL", "") or "").strip().rstrip("/")
    if configured:
        return configured
    site = str(SITE_URL or "").strip().rstrip("/")
    if not site:
        return "https://api.nyptidindustries.com"
    match = re.match(r"^(https?://)([^/]+)(.*)$", site, flags=re.IGNORECASE)
    if not match:
        return "https://api.nyptidindustries.com"
    scheme, host, suffix = match.groups()
    host_l = host.lower()
    for apex in ("nyptidindustries.com", "niptidindustries.com"):
        if host_l == f"api.{apex}":
            return f"{scheme}{host}{suffix}"
        if host_l in {apex, f"studio.{apex}", f"billing.{apex}"} or host_l.endswith("." + apex):
            return f"{scheme}api.{apex}{suffix}"
    return "https://api.nyptidindustries.com"


def _paypal_enabled() -> bool:
    return bool(str(PAYPAL_CLIENT_ID or "").strip() and str(PAYPAL_CLIENT_SECRET or "").strip())


def _paypal_api_base() -> str:
    return "https://api-m.sandbox.paypal.com" if str(PAYPAL_ENV or "").strip().lower() == "sandbox" else "https://api-m.paypal.com"


_paypal_token_cache = {"token": "", "expires_at": 0.0}


async def _paypal_access_token() -> str:
    if not _paypal_enabled():
        raise HTTPException(500, "PayPal is not configured")
    now = time.time()
    cached = str(_paypal_token_cache.get("token", "") or "")
    expires_at = float(_paypal_token_cache.get("expires_at", 0.0) or 0.0)
    if cached and (expires_at - now) > 60:
        return cached
    auth = base64.b64encode(f"{PAYPAL_CLIENT_ID}:{PAYPAL_CLIENT_SECRET}".encode("utf-8")).decode("ascii")
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{_paypal_api_base()}/v1/oauth2/token",
            headers={
                "Authorization": f"Basic {auth}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            content="grant_type=client_credentials",
        )
    if resp.status_code >= 400:
        raise HTTPException(500, f"PayPal auth failed: {resp.text[:200]}")
    data = resp.json()
    token = str(data.get("access_token", "") or "")
    expires_in = int(data.get("expires_in", 0) or 0)
    if not token:
        raise HTTPException(500, "PayPal auth token missing")
    _paypal_token_cache["token"] = token
    _paypal_token_cache["expires_at"] = now + max(60, expires_in)
    return token


async def _paypal_request(method: str, path: str, *, json_body: dict | None = None) -> dict:
    token = await _paypal_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=45.0) as client:
        resp = await client.request(
            method.upper(),
            f"{_paypal_api_base()}{path}",
            headers=headers,
            json=json_body,
        )
    if resp.status_code >= 400:
        raise HTTPException(resp.status_code, f"PayPal request failed: {resp.text[:400]}")
    try:
        return resp.json()
    except Exception:
        return {}


def _normalize_art_style(value, template: str = "", default: str = "auto") -> str:
    if str(template or "").strip().lower() == "skeleton":
        return "auto"
    text = str(value or default).strip().lower()
    return text if text in ART_STYLE_PRESETS else default


def _art_style_prompt_fragment(art_style: str, template: str = "") -> str:
    style = _normalize_art_style(art_style, template=template, default="auto")
    return ART_STYLE_PRESETS.get(style, "")


def _looks_like_provider_moderation_error(message: str) -> bool:
    msg = str(message or "").strip().lower()
    if not msg:
        return False
    moderation_markers = (
        "safety",
        "moderation",
        "policy",
        "violence",
        "graphic",
        "disallowed",
        "not allowed",
        "blocked",
        "rejected",
        "refused",
        "content violation",
    )
    provider_markers = ("xai", "x.ai", "grok", "fal.ai", "fal", "provider")
    has_moderation = any(t in msg for t in moderation_markers)
    # Some providers return moderation rejections without explicit vendor labels.
    if has_moderation:
        return True
    return any(p in msg for p in provider_markers) and ("400" in msg or "422" in msg or "invalid_request_error" in msg)


def _prompt_likely_moderated(prompt: str) -> bool:
    text = str(prompt or "").strip().lower()
    if not text:
        return False
    risky_terms = (
        "fallen bodies",
        "dead body",
        "dead bodies",
        "lifeless",
        "corpse",
        "gore",
        "gory",
        "graphic injury",
        "blood-soaked",
    )
    return any(t in text for t in risky_terms)


def _soften_story_prompt_for_moderation(prompt: str, aggressive: bool = False) -> str:
    text = str(prompt or "")
    if not text:
        return text
    replacements = [
        (r"\bfallen bodies?\b", "devastating aftermath"),
        (r"\bdead bodies?\b", "chaotic aftermath"),
        (r"\bcorpse(s)?\b", "aftermath"),
        (r"\blifeless\b", "unresponsive"),
        (r"\bbloody\b", "tense"),
        (r"\bgore\b", "distress"),
        (r"\bgory\b", "intense"),
        (r"\bgraphic injury\b", "emotional impact"),
    ]
    if aggressive:
        replacements.extend([
            (r"\bdeath\b", "loss"),
            (r"\bkilled\b", "hurt"),
            (r"\bmurder(ed|ous)?\b", "harmful"),
            (r"\bviolence\b", "conflict"),
            (r"\bweapon(s)?\b", "threat"),
            (r"\bbleeding\b", "injured"),
        ])
    softened = text
    for pattern, repl in replacements:
        softened = re.sub(pattern, repl, softened, flags=re.IGNORECASE)
    if softened != text:
        suffix = " Keep the emotional gravity high, avoid explicit gore or graphic injury."
        if aggressive:
            suffix = " Keep the emotional gravity high with cinematic tension, but avoid explicit violence, death wording, gore, or injuries."
        softened += suffix
    return softened


def _decode_data_image_url(data_url: str) -> tuple[bytes, str]:
    text = str(data_url or "").strip()
    if not text.startswith("data:image/") or "," not in text:
        return b"", ""
    try:
        header, b64_data = text.split(",", 1)
        mime = "image/png"
        if ";" in header and ":" in header:
            mime = header.split(":", 1)[1].split(";", 1)[0].strip() or mime
        raw = base64.b64decode(b64_data, validate=False)
        return (raw if raw else b"", mime)
    except Exception:
        return b"", ""


def _extract_reference_dna(raw: bytes, template: str = "skeleton") -> dict:
    dna = {
        "template": str(template or ""),
        "orientation": "portrait",
        "aspect_ratio": 0.562,
        "palette_family": "neutral_cool",
        "lighting_style": "balanced",
        "contrast_style": "cinematic",
        "sharpness_style": "clean",
        "width": 0,
        "height": 0,
    }
    if not raw or Image is None:
        return dna
    try:
        img = Image.open(io.BytesIO(raw)).convert("RGB")
        w, h = img.size
        if w > 0 and h > 0:
            ratio = float(w / h)
            dna["width"] = int(w)
            dna["height"] = int(h)
            dna["aspect_ratio"] = round(ratio, 4)
            if ratio < 0.85:
                dna["orientation"] = "portrait"
            elif ratio > 1.15:
                dna["orientation"] = "landscape"
            else:
                dna["orientation"] = "square"

        tiny = img.resize((128, max(1, int(128 * (img.height / max(img.width, 1))))), Image.BILINEAR)
        stat_rgb = ImageStat.Stat(tiny)
        mean_r, mean_g, mean_b = [float(v) for v in stat_rgb.mean[:3]]
        gray = tiny.convert("L")
        bright = float(ImageStat.Stat(gray).mean[0])
        contrast = float(ImageStat.Stat(gray).stddev[0])

        hsv_px = list(tiny.convert("HSV").getdata())
        sat_vals = [int(px[1]) for px in hsv_px]
        hue_vals = [int(px[0]) for px in hsv_px if int(px[1]) > 25]
        sat_avg = float(sum(sat_vals) / max(len(sat_vals), 1))
        hue_avg = float(sum(hue_vals) / max(len(hue_vals), 1)) if hue_vals else 0.0

        if ImageFilter is not None:
            edge = tiny.convert("L").filter(ImageFilter.FIND_EDGES)
            edge_strength = float(ImageStat.Stat(edge).mean[0])
        else:
            edge_strength = 0.0

        palette = "neutral_cool"
        if sat_avg < 45:
            palette = "desaturated"
        elif mean_r > mean_b + 8:
            palette = "warm"
        elif mean_b > mean_r + 8:
            palette = "cool"
        elif 18 <= hue_avg <= 45:
            palette = "warm"
        elif 95 <= hue_avg <= 145:
            palette = "cool"

        if bright < 70:
            lighting = "moody_lowkey"
        elif bright > 180:
            lighting = "highkey_bright"
        else:
            lighting = "balanced"

        if contrast < 28:
            contrast_style = "soft"
        elif contrast > 68:
            contrast_style = "punchy"
        else:
            contrast_style = "cinematic"

        sharpness_style = "clean" if edge_strength >= 24 else "soft"
        dna.update({
            "palette_family": palette,
            "lighting_style": lighting,
            "contrast_style": contrast_style,
            "sharpness_style": sharpness_style,
            "avg_brightness": round(bright, 2),
            "avg_contrast": round(contrast, 2),
            "avg_saturation": round(sat_avg, 2),
            "edge_strength": round(edge_strength, 2),
        })
    except Exception:
        return dna
    return dna


def _analyze_reference_quality(raw: bytes, lock_mode: str = "strict") -> dict:
    out = {"accepted": True, "warnings": [], "metrics": {}}
    if not raw:
        return {"accepted": False, "warnings": ["empty_image"], "metrics": {}}
    size_kb = len(raw) / 1024.0
    out["metrics"]["size_kb"] = round(size_kb, 1)
    if size_kb < 40:
        out["warnings"].append("very_small_file")

    dims = {"width": 0, "height": 0, "aspect_ratio": 0.0}
    if Image is not None:
        try:
            img = Image.open(io.BytesIO(raw))
            w, h = img.size
            dims = {"width": int(w), "height": int(h), "aspect_ratio": round(float(w / max(h, 1)), 4)}
            if min(w, h) < 768:
                out["warnings"].append("low_resolution_reference")
            ratio = float(w / max(h, 1))
            if ratio < 0.45 or ratio > 0.8:
                out["warnings"].append("non_portrait_aspect")
        except Exception:
            out["warnings"].append("unreadable_image")
    out["metrics"].update(dims)

    fatal = {"empty_image", "unreadable_image"}
    if "low_resolution_reference" in out["warnings"] and lock_mode == "strict":
        out["accepted"] = False
    if any(w in fatal for w in out["warnings"]):
        out["accepted"] = False
    return out


def _reference_dna_prompt_fragment(reference_dna: dict | None, lock_mode: str, template: str) -> str:
    dna = reference_dna or {}
    if not dna:
        return ""
    strength = "hard lock" if lock_mode == "strict" else "soft style guidance"
    return (
        "REFERENCE DNA (" + strength + "): "
        + "orientation=" + str(dna.get("orientation", "portrait")) + ", "
        + "palette=" + str(dna.get("palette_family", "neutral")) + ", "
        + "lighting=" + str(dna.get("lighting_style", "balanced")) + ", "
        + "contrast=" + str(dna.get("contrast_style", "cinematic")) + ", "
        + "sharpness=" + str(dna.get("sharpness_style", "clean")) + ". "
        + ("Maintain exact identity/anatomy continuity scene-to-scene. " if lock_mode == "strict" else "Preserve style while allowing pose/composition variety. ")
        + (
            "For skeleton keep skull geometry, eye size/spacing, bone finish, and a clearly visible translucent body silhouette unchanged."
            if template == "skeleton"
            else (
                "For story keep recurring subjects, key locations, and grade continuity consistent when the script indicates recurrence."
                if template == "story"
                else "Keep subject styling and grading consistent across scenes."
            )
        )
    )


def _canonical_skeleton_anchor() -> str:
    return (
        "CONSISTENCY ANCHOR -- use one unchanged canonical skeleton identity in every scene: "
        "ivory-white anatomical skeleton, large realistic eyeballs with visible iris, "
        "clearly visible translucent soft-tissue silhouette around torso/limbs, identical skull proportions and bone structure, "
        "preserve anatomy and the translucent body shell even when the scene requests role-specific props or wardrobe. "
    )


def _skeleton_has_explicit_outfit_request(text: str) -> bool:
    raw = str(text or "").strip().lower()
    if not raw:
        return False
    if re.search(r"\b(no clothing|shirtless|nude|naked|bare(?:\s+body)?|no outfit)\b", raw):
        return False
    return bool(
        re.search(
            r"\b("
            r"spacesuit|space suit|astronaut|helmet|visor|chef|chef hat|apron|hoodie|jacket|coat|robe|uniform|armor|costume|"
            r"suit|tuxedo|dress|shirt|pants|gloves|boots|scrubs|jersey|cloak|cape|mask|gown|lab coat"
            r")\b",
            raw,
        )
    )


def _skeleton_outfit_coverage_lock(text: str) -> str:
    if not _skeleton_has_explicit_outfit_request(text):
        return ""
    return (
        "WARDROBE LOCK: keep the requested outfit or uniform physically worn over the same canonical Jerry-style skeleton. "
        "The clothing must enclose the torso, pelvis, shoulders, arms, and legs wherever the outfit covers them. "
        "Do not leave the ribcage, spine, pelvis, or limb bones visibly exposed outside the clothing, and do not render transparent clothes that reveal the full skeleton body underneath. "
        "The glass-like body shell and same eyes still exist, but the body should look properly dressed inside the outfit rather than naked bones wearing accessories."
    )


def _build_scene_prompt_with_reference(
    template: str,
    visual_description: str,
    quality_mode: str = "standard",
    skeleton_anchor: str = "",
    reference_dna: dict | None = None,
    reference_lock_mode: str = "strict",
    art_style: str = "auto",
) -> str:
    delta = str(visual_description or "").strip()
    immutable = _reference_dna_prompt_fragment(reference_dna, _normalize_reference_lock_mode(reference_lock_mode), template)
    style_fragment = _art_style_prompt_fragment(art_style, template=template)
    immutable_blocks = [block for block in [immutable, style_fragment] if block]
    immutable_context = " ".join(immutable_blocks).strip()
    if template == "skeleton":
        return _build_skeleton_image_prompt(
            delta,
            skeleton_anchor=skeleton_anchor,
            quality_mode=quality_mode,
            immutable_context=immutable_context,
        )
    if template == "story":
        return _build_story_image_prompt(
            delta,
            quality_mode=quality_mode,
            immutable_context=immutable_context,
        )
    prefix = TEMPLATE_PROMPT_PREFIXES.get(template, "")
    if immutable_context:
        return f"{prefix} IMMUTABLE STYLE: {immutable_context} SCENE DELTA: {delta}"
    return prefix + delta


def _build_skeleton_identity_passthrough_prompt(
    visual_description: str,
    reference_dna: dict | None = None,
) -> str:
    delta = _sanitize_skeleton_scene_delta(str(visual_description or "").strip())
    if not delta:
        return (
            "Show the canonical Jerry-style NYPTID skeleton first: ivory-white anatomical skeleton, "
            "large realistic human-like eyeballs clearly visible in both eye sockets with readable iris and pupil, "
            "and a continuous translucent glass-like skin shell clearly visible over the full body, hugging the skull, face, neck, torso, arms, hands, and legs like real transparent skin."
        )
    internal_focus = _skeleton_scene_prefers_internal_cutaway(delta)
    ref = reference_dna or {}
    ref_bits: list[str] = []
    lighting = str(ref.get("lighting_style", "") or "").strip()
    contrast = str(ref.get("contrast_style", "") or "").strip()
    if lighting or contrast:
        ref_bits.append(
            "Keep the default Jerry reference grade: "
            + " ".join(part for part in [lighting, contrast] if part).strip()
            + "."
        )
    outfit_lock = _skeleton_outfit_coverage_lock(delta)
    anchor = (
        "Always show the canonical Jerry-style NYPTID skeleton first and keep visible skeleton anatomy in frame: "
        "ivory-white anatomical skeleton, large realistic human-like eyeballs clearly visible in both eye sockets with readable iris and pupil, "
        "never empty sockets, never missing eyes, continuous translucent glass-like skin shell clearly visible over the full body, "
        "hugging the skull, face, neck, torso, arms, hands, and legs like real transparent skin, never bare bones-only look, never faint shell, never x-ray look."
    )
    if internal_focus:
        bridge = (
            "Render the requested microscopic or internal subject as a zoom-in / cutaway happening inside the skeleton's "
            "arm, torso, blood, or tissue while some canonical skeleton anatomy remains visible in frame."
        )
    else:
        bridge = "The skeleton remains the main on-screen subject while following the user's scene request."
    return " ".join([
        anchor,
        bridge,
        _skeleton_scene_context_lock(delta),
        _skeleton_scene_framing_lock(delta),
        outfit_lock,
        f"USER SCENE REQUEST: {delta}",
        *ref_bits,
    ]).strip()


def _build_creative_passthrough_scene_prompt(
    template: str,
    visual_description: str,
    quality_mode: str = "standard",
    skeleton_anchor: str = "",
    reference_dna: dict | None = None,
    reference_lock_mode: str = "strict",
    art_style: str = "auto",
) -> str:
    # Passthrough must honor the user's prompt. Skeleton is the only exception:
    # it keeps a minimal identity anchor so the subject never stops being Jerry-style skeleton.
    if str(template or "").strip().lower() == "skeleton":
        return _build_skeleton_identity_passthrough_prompt(
            visual_description,
            reference_dna=reference_dna,
        )
    return str(visual_description or "").strip()


def _augment_skeleton_negative_prompt(base_negative: str, prompt: str) -> str:
    text = str(prompt or "").strip().lower()
    if not text:
        return str(base_negative or "").strip()
    extras: list[str] = []
    has_brain = bool(re.search(r"\bbrain\b", text))
    has_money = bool(re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", text))
    has_table = bool(re.search(r"\btable|desk|countertop\b", text))
    has_damage_cues = bool(re.search(r"\b(crack|cracks|fracture|fractured|chip|chipped|bruise|bruises|damaged|damage)\b", text))
    has_tired_cues = bool(re.search(r"\b(tired|fatigued|weary|slouch|slouched|hunch|hunched|droop|drooping|exhausted)\b", text))
    if has_brain:
        extras.extend([
            "smooth sphere as brain replacement",
            "fruit instead of brain",
            "brain without visible folds",
        ])
    if has_money:
        extras.extend([
            "coins-only replacing money pile",
            "blank paper replacing banknotes",
            "missing cash stack",
        ])
    if has_brain and has_money:
        extras.extend([
            "extra random glowing orb replacing required props",
            "missing one of the two required props",
        ])
    if has_table:
        extras.extend([
            "standing pose with no table",
            "missing tabletop surface",
            "floating props with no table context",
            "arched portal behind skeleton",
            "backlit doorway behind skeleton",
            "altar pedestal replacing table",
            "props on floor instead of table",
            "monocle",
            "eye patch",
            "missing one eye",
        ])
    if has_damage_cues:
        extras.extend([
            "pristine undamaged bones",
            "perfectly clean intact skeleton",
            "no cracks on bones",
            "no bruising marks",
        ])
    if has_tired_cues:
        extras.extend([
            "upright heroic posture",
            "confident energetic stance",
        ])
    parts = [str(base_negative or "").strip()] if str(base_negative or "").strip() else []
    if extras:
        parts.append(", ".join(dict.fromkeys(extras)))
    return ", ".join([p for p in parts if p]).strip(", ")


def _relax_skeleton_negative_prompt_for_passthrough(base_negative: str, prompt: str) -> str:
    text = str(prompt or "").strip().lower()
    neg = str(base_negative or "").strip()
    if not neg:
        return neg
    wants_damage = bool(re.search(r"\b(crack|cracks|fracture|fractured|chip|chipped|bruise|bruises|damaged|damage)\b", text))
    wants_tired = bool(re.search(r"\b(tired|fatigued|weary|slouch|slouched|hunch|hunched|droop|drooping|exhausted)\b", text))
    parts = [p.strip() for p in neg.split(",") if p and p.strip()]
    out: list[str] = []
    for part in parts:
        pl = part.lower()
        if wants_damage and ("broken bones" in pl or "dislocated joints" in pl):
            continue
        if wants_tired and ("unnatural pose" in pl or "mannequin" in pl):
            continue
        out.append(part)
    return ", ".join(out).strip(", ")


def _truncate_words(text: str, max_words: int = 120) -> str:
    raw = re.sub(r"\s+", " ", str(text or "")).strip()
    if not raw:
        return ""
    words = raw.split(" ")
    if len(words) <= max_words:
        return raw
    return " ".join(words[:max_words]).strip()


def _shortform_delivery_hints(text: str, template: str = "") -> list[str]:
    raw = re.sub(r"\s+", " ", str(text or "")).strip().lower()
    if not raw:
        return []
    hints: list[str] = []
    if re.search(r"\b(shorts?|short-form|short form|thumbnail|thumbnails?|viral short|reel|reels)\b", raw):
        hints.extend([
            "vertical 9:16 short-form composition",
            "thumbnail-readable centered subject",
            "clean uncluttered background with large readable props",
        ])
    if re.search(r"\bmust\s+be\s+able\s+to\s+work\b|\bno\s+matter\s+what\b", raw):
        hints.append("high-clarity production framing with reliable readable composition")
    if re.search(r"\b(create|generate)\s+images?\s+for\s+shorts?\b", raw):
        hints.append("strong subject separation and prop readability for short-form cover frames")
    if str(template or "").strip().lower() == "skeleton" and hints:
        hints.append("keep skull, both eyes, and upper torso instantly readable at first glance")
    seen: set[str] = set()
    out: list[str] = []
    for hint in hints:
        key = hint.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(hint.strip())
    return out


def _sanitize_skeleton_scene_delta(text: str) -> str:
    """Remove instructions that conflict with canonical Skeleton identity."""
    raw = re.sub(r"\s+", " ", str(text or "")).strip()
    if not raw:
        return ""
    cleaned = raw
    nonvisual_patterns = [
        r"\bit\s+must\s+be\s+able\s+to\s+work\s+and\s+create\s+images?\s+for\s+shorts?\b",
        r"\bmust\s+be\s+able\s+to\s+work\s+and\s+create\s+images?\s+for\s+shorts?\b",
        r"\bmust\s+be\s+able\s+to\s+work\b",
        r"\b(?:create|generate)\s+images?\s+for\s+shorts?\b",
        r"\bno\s+matter\s+what\b",
    ]
    for pat in nonvisual_patterns:
        cleaned = re.sub(pat, " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(a|an|the)\s+sits\b", "sits", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(a|an|the)\s+is\b", "is", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.")
    return cleaned or raw


def _skeleton_scene_prefers_internal_cutaway(text: str) -> bool:
    raw = str(text or "").strip().lower()
    if not raw:
        return False
    internal_markers = (
        "cell", "cells", "immune", "fever", "virus", "bacteria", "bloodstream", "blood vessel", "blood vessels", "pyrogen", "pyrogens",
        "hypothalamus", "neuron", "brain chemistry", "organ", "microscopic", "microshot",
        "macro shot", "close-up of tissue", "inside the body", "inside body", "inside the skeleton",
        "cutaway", "cross-section", "cross section", "blood", "tissue", "artery", "vein",
    )
    return any(marker in raw for marker in internal_markers)


def _skeleton_scene_requests_minimal_background(text: str) -> bool:
    raw = str(text or "").strip().lower()
    if not raw:
        return False
    return bool(re.search(
        r"\b(plain background|blank background|white background|black background|solid background|"
        r"minimal background|minimalist background|empty background|void background|studio seamless|"
        r"seamless backdrop|clean backdrop|isolated cutout|isolated on white|featureless backdrop)\b",
        raw,
    ))


def _skeleton_scene_has_environment_cue(text: str) -> bool:
    raw = str(text or "").strip().lower()
    if not raw:
        return False
    return bool(re.search(
        r"\b(environment|background|setting|location|interior|exterior|room|office|lab|laboratory|street|city|"
        r"arena|stadium|courtroom|hospital|factory|warehouse|classroom|garage|workshop|home|kitchen|bedroom|"
        r"rooftop|subway|train|airplane|plane|ship|boat|spaceship|space|planet|forest|desert|jungle|beach|"
        r"ocean|underwater|mountain|cave|temple|castle|battlefield|store|market|restaurant|bar|stage|studio|"
        r"table|desk|countertop|inside|within|cutaway|macro|microscopic|bloodstream|organ|cell|tissue)\b",
        raw,
    ))


def _skeleton_scene_has_camera_cue(text: str) -> bool:
    raw = str(text or "").strip().lower()
    if not raw:
        return False
    return bool(re.search(
        r"\b(close[- ]?up|closeup|macro|wide(?: shot)?|establishing|over[- ]?shoulder|over the shoulder|"
        r"low[- ]?angle|high[- ]?angle|bird'?s[- ]?eye|top[- ]?down|profile|silhouette|medium shot|mid[- ]?shot|"
        r"three[- ]?quarter|3/4|waist[- ]?up|full[- ]?body|full body|portrait|hero framing|framing|camera|lens|"
        r"dolly|push[- ]?in|tracking|pov|point of view)\b",
        raw,
    ))


def _skeleton_scene_context_lock(text: str) -> str:
    raw = str(text or "").strip()
    if _skeleton_scene_requests_minimal_background(raw):
        return (
            "BACKGROUND/STAGING LOCK: honor the explicitly requested minimal or studio backdrop, "
            "but make it look intentional, premium, and fully lit rather than empty or unfinished."
        )
    if _skeleton_scene_prefers_internal_cutaway(raw):
        return (
            "BACKGROUND/STAGING LOCK: use a readable internal, anatomical, or microscopic environment with "
            "clear contextual structures and layered depth; never reduce the beat to a plain void."
        )
    return (
        "BACKGROUND/STAGING LOCK: place the skeleton inside a rich topic-specific environment with clear "
        "location cues, layered foreground/midground/background depth, and contextual objects; never leave "
        "the subject on a blank, plain, or empty backdrop."
    )


def _skeleton_scene_framing_lock(text: str) -> str:
    raw = str(text or "").strip()
    if _skeleton_scene_prefers_internal_cutaway(raw):
        return (
            "SHOT VARIETY LOCK: if the beat is internal or microscopic, use close cutaway or macro framing "
            "while keeping some canonical skeleton anatomy readable in frame."
        )
    return (
        "SHOT VARIETY LOCK: choose framing that best communicates the beat instead of repeating the same "
        "centered medium hero shot. Use wide environmental, medium action, low-angle, over-shoulder, "
        "prop-detail, or full-body movement framing as appropriate."
    )


def _extract_scene_content_lock(prompt: str) -> str:
    raw = str(prompt or "")
    m = re.search(
        r"SCENE CONTENT LOCK\s*\(must be visible and preserved\):\s*(.*?)\s*SCENE PRIORITY RULE:",
        raw,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if m:
        return re.sub(r"\s+", " ", str(m.group(1) or "")).strip(" .")
    return ""


def _compact_skeleton_local_prompt(prompt: str) -> str:
    raw = re.sub(r"\s+", " ", str(prompt or "")).strip()
    if not raw:
        return raw
    delivery_hints = _shortform_delivery_hints(raw, template="skeleton")
    scene = _sanitize_skeleton_scene_delta(_extract_scene_content_lock(raw) or raw)
    scene = re.sub(
        r"keep every explicitly requested prop/object/action visible and readable in frame\.?",
        "",
        scene,
        flags=re.IGNORECASE,
    )
    scene = re.sub(r"\s+", " ", scene).strip(" .")
    scene_l = scene.lower()
    needs_table = bool(re.search(r"\b(table|desk|countertop)\b", scene_l))
    needs_brain = "brain" in scene_l
    needs_money = bool(re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", scene_l))
    needs_glow = bool(re.search(r"\b(glow|glowing|emissive|luminous|light[- ]?emitting)\b", scene_l))
    internal_focus = _skeleton_scene_prefers_internal_cutaway(scene)
    explicit_outfit_request = _skeleton_has_explicit_outfit_request(scene)
    parts: list[str] = [
        "photoreal cinematic 3D render",
        "canonical ivory-white anatomical skeleton",
        "both eyes visible with realistic iris reflections",
        "clearly visible translucent glass-like body shell",
        "natural bone color with neutral grading, not x-ray not radiograph",
    ]
    parts.append("requested outfit is fully worn and covers the body correctly" if explicit_outfit_request else "no clothing no costume no armor")
    if re.search(r"\bdark\b|\bnight\b|\blow[- ]?light\b", scene_l):
        parts.append("dark moody room lighting")
    prompt_parts: list[str] = [", ".join(parts) + "."]
    prompt_parts.append(f"Scene: {_truncate_words(scene, 56)}.")
    if explicit_outfit_request:
        prompt_parts.append(_skeleton_outfit_coverage_lock(scene))
    if needs_table:
        prompt_parts.append(
            "Skeleton is seated behind a real table and the tabletop is clearly visible across the lower foreground. "
            "Both forearms or hands rest on or directly above the tabletop."
        )
    if needs_brain and needs_money:
        glow = "glowing " if needs_glow else ""
        prompt_parts.append(
            f"On the table are exactly two {glow}props: one realistic human brain with visible gyri/sulci folds and one pile/stack of paper cash banknotes, both clearly visible and not replaced by spheres."
        )
    elif needs_brain:
        glow = "glowing " if needs_glow else ""
        prompt_parts.append(
            f"Show one {glow}realistic human brain with visible gyri/sulci folds (never a smooth sphere)."
        )
    elif needs_money:
        glow = "glowing " if needs_glow else ""
        prompt_parts.append(
            f"Show one {glow}pile/stack of paper cash banknotes clearly visible {'on the table' if needs_table else 'near the skeleton in the environment'}."
        )
    prompt_parts.append(_skeleton_scene_context_lock(scene))
    prompt_parts.append(
        _skeleton_scene_framing_lock(scene)
        if not internal_focus
        else "SHOT VARIETY LOCK: close cutaway or macro framing is allowed here, but some canonical skeleton anatomy must remain readable in frame."
    )
    prompt_parts.append("Ultra sharp focus, high contrast, readable props, no text, no watermark.")
    prompt_parts.append(
        "Glass-shell lock: transparent glass-like skin hugs the skull, torso, arms, and legs tightly like a real outer body shell, "
        "not a halo, not a bubble, not a portal, and not a glowing arch behind the subject."
    )
    if delivery_hints:
        prompt_parts.append("Short-form lock: " + "; ".join(delivery_hints) + ".")
    return _truncate_words(" ".join(prompt_parts), 150)


def _compact_skeleton_prop_first_prompt(prompt: str) -> str:
    raw = re.sub(r"\s+", " ", str(prompt or "")).strip()
    scene = _sanitize_skeleton_scene_delta(_extract_scene_content_lock(raw) or raw)
    scene_l = scene.lower()
    needs_table = bool(re.search(r"\b(table|desk|countertop)\b", scene_l))
    needs_brain = "brain" in scene_l
    needs_money = bool(re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", scene_l))
    needs_glow = bool(re.search(r"\b(glow|glowing|emissive|luminous|light[- ]?emitting)\b", scene_l))
    dark_room = bool(re.search(r"\b(dark|night|shadowy|moody|low[- ]?light)\b", scene_l))
    props = []
    if needs_brain:
        props.append(
            ("glowing " if needs_glow else "")
            + "human brain with visible gyri and sulci folds"
        )
    if needs_money:
        props.append(
            ("glowing " if needs_glow else "")
            + "pile of paper cash banknotes"
        )
    props_text = " and ".join(props) if props else "scene props"
    placement = "on the table" if needs_table else "in front of the skeleton"
    explicit_outfit_request = _skeleton_has_explicit_outfit_request(scene)
    base_parts = [
        "photoreal 3D cinematic render",
        "transparent-glass anatomical skeleton with both eyes visible",
        "natural ivory bone color, no x-ray or radiograph look",
        "glass shell wraps tightly around skull torso arms and legs like transparent skin, never a halo or portal",
        ("dark moody environment" if dark_room else "premium cinematic lighting"),
    ]
    prompt_parts = [", ".join(base_parts) + "."]
    if needs_table:
        prompt_parts.append(
            "Skeleton is seated behind a real table with the tabletop clearly visible across the lower foreground and both hands near or on the tabletop."
        )
    else:
        prompt_parts.append("Skeleton interacts with the requested props inside a readable topic-matched environment.")
    prompt_parts.append(f"Mandatory props: {props_text} {placement}.")
    if needs_table:
        prompt_parts.append("Keep the required props large and obvious on the tabletop foreground. Do not omit either prop.")
    else:
        prompt_parts.append("Keep the required props large, obvious, and readable near the skeleton in the environment. Do not omit either prop.")
    prompt_parts.append("Do not replace required props with spheres, balls, or generic abstract objects.")
    if explicit_outfit_request:
        prompt_parts.append(_skeleton_outfit_coverage_lock(scene))
    prompt_parts.append(_skeleton_scene_context_lock(scene))
    prompt_parts.append(_skeleton_scene_framing_lock(scene))
    prompt_parts.append("Sharp focus, readable skull and prop detail, no text, no watermark.")
    return _truncate_words(" ".join(prompt_parts), 120)


def _extract_skeleton_scene_delta_for_fast_path(prompt: str) -> str:
    raw = re.sub(r"\s+", " ", str(prompt or "")).strip()
    if not raw:
        return ""
    scene = _extract_scene_content_lock(raw)
    if scene:
        return _sanitize_skeleton_scene_delta(scene)
    match = re.search(
        r"Scene:\s*(.*?)(?:\s+(?:Skeleton is seated|Show exactly|Glass-shell lock:|Short-form lock:|Match the reference skeleton anatomy exactly:)|$)",
        raw,
        flags=re.IGNORECASE,
    )
    if match:
        return _sanitize_skeleton_scene_delta(match.group(1))
    return _sanitize_skeleton_scene_delta(raw)


def _build_skeleton_lora_fast_prompt(prompt: str) -> str:
    scene = _extract_skeleton_scene_delta_for_fast_path(prompt)
    scene_l = scene.lower()
    needs_table = bool(re.search(r"\b(table|desk|countertop)\b", scene_l))
    needs_brain = "brain" in scene_l
    needs_money = bool(re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", scene_l))
    needs_glow = bool(re.search(r"\b(glow|glowing|emissive|luminous|light[- ]?emitting)\b", scene_l))
    dark_room = bool(re.search(r"\b(dark|night|shadowy|moody|low[- ]?light)\b", scene_l))
    explicit_outfit_request = _skeleton_has_explicit_outfit_request(scene)
    prompt_parts = [
        "Single skeleton subject only.",
        ("Dark moody environment with readable background detail." if dark_room else "Detailed topic-matched environment with layered background depth."),
        "Photoreal 3D render.",
        "Anatomical skeleton with large realistic eyes and transparent glass skin tightly wrapped around the skull, torso, arms, and legs.",
        "No second skeleton and no extra person.",
    ]
    if needs_table:
        prompt_parts.append(
            "Skeleton sits behind a real table with the tabletop clearly visible across the lower foreground and both hands near or on the tabletop."
        )
    else:
        prompt_parts.append("Keep the skeleton inside a readable scene that clearly matches the topic, not a blank backdrop.")
    if explicit_outfit_request:
        prompt_parts.append(_skeleton_outfit_coverage_lock(scene))
    if needs_brain and needs_money:
        glow = "glowing " if needs_glow else ""
        prompt_parts.append(
            f"Exactly two props on the tabletop: one realistic {glow}human brain with visible folds and one {glow}pile of paper cash banknotes."
        )
    elif needs_brain:
        glow = "glowing " if needs_glow else ""
        prompt_parts.append(f"One realistic {glow}human brain is clearly visible.")
    elif needs_money:
        glow = "glowing " if needs_glow else ""
        prompt_parts.append(f"One {glow}pile of paper cash banknotes is clearly visible.")
    else:
        prompt_parts.append(scene.rstrip(". ") + ".")
    prompt_parts.append(_skeleton_scene_framing_lock(scene))
    prompt_parts.append("Sharp focus, realistic lighting, readable environment depth.")
    return _truncate_words(" ".join(part for part in prompt_parts if part).strip(), 110)


def _compact_skeleton_negative_prompt(base_negative: str, prompt: str) -> str:
    text = str(prompt or "").lower()
    explicit_outfit_request = _skeleton_has_explicit_outfit_request(text)
    tokens = [
        "blurry",
        "low quality",
        "text",
        "watermark",
        "cartoon",
        "anime",
        "painting",
        "clothed skeleton",
        "armor",
        "costume",
        "empty eye sockets",
        "missing eye",
        "monocle",
        "eyepatch",
        "glowing eyes",
        "human skin face",
        "human bust statue",
        "severed head",
        "head sculpture",
        "no translucent shell",
        "bare bones only",
        "x-ray style",
        "radiograph style",
        "fluoroscopy look",
        "neon blue xray glow",
        "medical scan visualization",
    ]
    if explicit_outfit_request:
        tokens = [t for t in tokens if t not in {"clothed skeleton", "armor", "costume"}]
        tokens.extend([
            "exposed ribcage outside clothing",
            "exposed pelvis outside clothing",
            "transparent outfit revealing full skeleton body",
            "naked skeleton body with only accessories",
        ])
    if "brain" in text:
        tokens.extend([
            "smooth ball instead of brain",
            "fruit instead of brain",
            "missing brain",
        ])
    if re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", text):
        tokens.extend([
            "coins-only money",
            "blank paper instead of cash",
            "missing money stack",
        ])
    if re.search(r"\b(table|desk|countertop)\b", text):
        tokens.extend([
            "standing pose no table",
            "floating props without table",
            "empty table with no props",
        ])
    base = str(base_negative or "").strip()
    merged = ", ".join(dict.fromkeys([t for t in tokens if t and t.strip()]))
    if base:
        merged = f"{base}, {merged}"
    return _truncate_words(merged, 95)


def _build_skeleton_lora_fast_negative(base_negative: str, prompt: str) -> str:
    text = str(prompt or "").lower()
    tokens = [
        "blurry",
        "low quality",
        "text",
        "watermark",
        "duplicate subject",
        "two skeletons",
        "extra skeleton",
        "extra person",
        "crowd",
        "reflection duplicate",
        "glass display case",
        "glass dome",
        "portal",
        "archway",
        "window frame",
        "xray",
        "radiograph",
        "medical scan",
        "gems",
        "crystals",
        "candle",
        "bowl",
        "cup",
        "extra props",
        "background made of money",
        "lying down pose",
    ]
    if "brain" in text:
        tokens.extend(["missing brain", "fruit instead of brain", "orb instead of brain"])
    if re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", text):
        tokens.extend(["missing money", "coins only", "blank paper instead of cash"])
    if re.search(r"\b(table|desk|countertop)\b", text):
        tokens.extend(["no table", "standing pose", "floating props", "empty tabletop"])
    base = str(base_negative or "").strip()
    merged = ", ".join(dict.fromkeys([t for t in tokens if t and t.strip()]))
    if base:
        merged = f"{base}, {merged}"
    return _truncate_words(merged, 110)


def _score_generated_image_quality(image_path: str, prompt: str = "", template: str = "") -> dict:
    """Heuristic image quality scorer used for best-of candidate selection."""
    if Image is None or ImageStat is None:
        return {"score": 0.0, "ok": False, "reason": "pillow_unavailable"}
    p = Path(image_path)
    if not p.exists() or p.stat().st_size <= 0:
        return {"score": 0.0, "ok": False, "reason": "missing_image"}
    try:
        with Image.open(p) as im:
            rgb = im.convert("RGB")
            w, h = rgb.size
            mega_px = (float(w) * float(h)) / 1_000_000.0
            gray = rgb.convert("L")
            bright = float(ImageStat.Stat(gray).mean[0])
            contrast = float(ImageStat.Stat(gray).stddev[0])
            hsv = rgb.convert("HSV")
            sat = float(ImageStat.Stat(hsv).mean[1])
            edge_strength = 0.0
            if ImageFilter is not None:
                edge_img = gray.filter(ImageFilter.FIND_EDGES)
                edge_strength = float(ImageStat.Stat(edge_img).mean[0])
            shell_ratio = 0.0
            shell_to_bone_ratio = 0.0
            subject_height_ratio = 0.0
            subject_width_ratio = 0.0
            brain_color_ratio = 0.0
            money_color_ratio = 0.0
            brain_component_area_ratio = 0.0
            money_component_area_ratio = 0.0
            brain_edge_density = 0.0
            brain_round_area_ratio = 0.0
            money_rect_area_ratio = 0.0
            prop_component_count = 0
            seated_full_body_required = False
            bone_r_mean = 0.0
            bone_g_mean = 0.0
            bone_b_mean = 0.0

            def _clamp01(v: float) -> float:
                if v <= 0.0:
                    return 0.0
                if v >= 1.0:
                    return 1.0
                return v

            sharp_norm = _clamp01((edge_strength - 10.0) / 35.0)
            contrast_norm = _clamp01((contrast - 26.0) / 48.0)
            sat_norm = _clamp01((sat - 28.0) / 95.0)
            exposure_norm = _clamp01(1.0 - (abs(bright - 145.0) / 145.0))
            size_norm = _clamp01(mega_px / 0.9)  # 720x1280 ~= 0.92MP baseline

            score = 100.0 * (
                0.34 * sharp_norm
                + 0.24 * contrast_norm
                + 0.14 * sat_norm
                + 0.16 * exposure_norm
                + 0.12 * size_norm
            )
            notes = []
            if edge_strength < 12:
                score -= 10.0
                notes.append("soft_edges")
            if contrast < 22:
                score -= 8.0
                notes.append("flat_contrast")
            if sat < 20:
                score -= 6.0
                notes.append("washed_colors")
            if template == "skeleton" and ("eyes" in prompt.lower() or "eyeballs" in prompt.lower()) and edge_strength < 14:
                score -= 5.0
                notes.append("weak_eye_detail")
            if template == "skeleton" and np is not None and ImageFilter is not None:
                try:
                    arr = np.asarray(rgb).astype(np.float32)
                    r = arr[:, :, 0]
                    g = arr[:, :, 1]
                    b = arr[:, :, 2]
                    maxc = np.maximum(np.maximum(r, g), b)
                    minc = np.minimum(np.minimum(r, g), b)
                    sat_arr = np.zeros_like(maxc, dtype=np.float32)
                    np.divide((maxc - minc), np.maximum(maxc, 1e-6), out=sat_arr, where=maxc > 1e-6)
                    sat_arr *= 255.0
                    lum_arr = 0.2126 * r + 0.7152 * g + 0.0722 * b
                    neutral = (
                        (np.abs(r - g) < 40.0)
                        & (np.abs(g - b) < 40.0)
                        & (np.abs(r - b) < 52.0)
                    )
                    bone = ((lum_arr > 150.0) & (sat_arr < 95.0) & neutral)
                    bone_px = int(bone.sum())
                    if bone_px > 1500:
                        try:
                            bone_r_mean = float(r[bone].mean())
                            bone_g_mean = float(g[bone].mean())
                            bone_b_mean = float(b[bone].mean())
                        except Exception:
                            bone_r_mean = 0.0
                            bone_g_mean = 0.0
                            bone_b_mean = 0.0
                        # Penalize blue radiograph/X-ray drift; reward natural ivory balance.
                        if bone_b_mean > (bone_r_mean + 10.0) and bone_b_mean > (bone_g_mean + 6.0):
                            score -= 16.0
                            notes.append("xray_style_drift")
                        elif bone_r_mean >= (bone_b_mean - 4.0) and bone_g_mean >= (bone_b_mean - 6.0):
                            score += 6.0
                        ys, xs = np.where(bone)
                        if ys.size > 0 and xs.size > 0:
                            subject_height_ratio = float((ys.max() - ys.min() + 1)) / float(max(1, h))
                            subject_width_ratio = float((xs.max() - xs.min() + 1)) / float(max(1, w))
                            if subject_height_ratio < 0.42:
                                score -= 9.0
                                notes.append("subject_too_small")
                            if subject_width_ratio < 0.18:
                                score -= 7.0
                                notes.append("subject_too_narrow")
                        bone_img = Image.fromarray((bone.astype(np.uint8) * 255), mode="L")
                        ring = ImageChops.subtract(
                            bone_img.filter(ImageFilter.MaxFilter(size=41)),
                            bone_img.filter(ImageFilter.MaxFilter(size=9)),
                        ).filter(ImageFilter.GaussianBlur(radius=3.5))
                        ring_arr = np.asarray(ring).astype(np.float32) / 255.0
                        shell_like = (
                            (ring_arr > 0.08)
                            & (lum_arr > 26.0)
                            & (lum_arr < 190.0)
                            & (b > (r * 0.90))
                            & (g > (r * 0.82))
                        )
                        shell_px = int(shell_like.sum())
                        ring_px = int((ring_arr > 0.08).sum())
                        shell_ratio = float(shell_px) / float(max(1, ring_px))
                        shell_to_bone_ratio = float(shell_px) / float(max(1, bone_px))
                        if shell_ratio < 0.045:
                            score -= 22.0
                            notes.append("missing_translucent_shell")
                        elif shell_ratio < 0.085:
                            score -= 10.0
                            notes.append("weak_translucent_shell")
                        if shell_to_bone_ratio < 0.16:
                            score -= 12.0
                            notes.append("shell_not_visible_enough")
                    else:
                        score -= 34.0
                        notes.append("not_skeleton_structure")

                    prompt_l = str(prompt or "").lower()
                    needs_brain = "brain" in prompt_l
                    needs_money = bool(re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", prompt_l))
                    seated_full_body_required = bool(
                        re.search(r"\b(sit|sits|seated|sitting)\b", prompt_l)
                        or re.search(r"\b(table|desk|countertop)\b", prompt_l)
                    )
                    if needs_brain or needs_money:
                        y0 = int(h * 0.50)
                        y1 = int(h * 0.97)
                        x0 = int(w * 0.16)
                        x1 = int(w * 0.84)
                        if y1 > y0 and x1 > x0:
                            rr = r[y0:y1, x0:x1]
                            gg = g[y0:y1, x0:x1]
                            bb = b[y0:y1, x0:x1]
                            if rr.size > 0:
                                brain_mask = (
                                    (rr > (gg + 10.0))
                                    & (gg > (bb + 3.0))
                                    & (rr > 75.0)
                                )
                                money_mask = (
                                    (gg > (rr + 8.0))
                                    & (gg > (bb + 8.0))
                                    & (gg > 65.0)
                                )
                                brain_color_ratio = float(brain_mask.mean())
                                money_color_ratio = float(money_mask.mean())
                                if cv2 is not None:
                                    try:
                                        roi = arr[y0:y1, x0:x1].astype(np.uint8)
                                        gray_roi = cv2.cvtColor(roi, cv2.COLOR_RGB2GRAY)
                                        edges_roi = cv2.Canny(gray_roi, 48, 132)
                                        if needs_brain:
                                            brain_u8 = (brain_mask.astype(np.uint8) * 255)
                                            comp_count, labels, stats, _ = cv2.connectedComponentsWithStats(brain_u8, connectivity=8)
                                            if comp_count > 1:
                                                areas = stats[1:, cv2.CC_STAT_AREA]
                                                largest_idx = int(1 + np.argmax(areas))
                                                largest = int(areas.max())
                                                brain_component_area_ratio = float(largest) / float(max(1, brain_u8.size))
                                                comp_sel = labels == largest_idx
                                                if comp_sel.any():
                                                    brain_edge_density = float((edges_roi[comp_sel] > 0).mean())
                                        if needs_money:
                                            money_u8 = (money_mask.astype(np.uint8) * 255)
                                            comp_count2, labels2, stats2, _ = cv2.connectedComponentsWithStats(money_u8, connectivity=8)
                                            if comp_count2 > 1:
                                                areas2 = stats2[1:, cv2.CC_STAT_AREA]
                                                largest2 = int(areas2.max())
                                                money_component_area_ratio = float(largest2) / float(max(1, money_u8.size))
                                        # Shape fallback to reduce false negatives when lighting grade desaturates props.
                                        cnts, _ = cv2.findContours(edges_roi, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
                                        roi_h, roi_w = gray_roi.shape[:2]
                                        roi_area = float(max(1, roi_h * roi_w))
                                        for c in cnts:
                                            area = float(cv2.contourArea(c))
                                            if area < roi_area * 0.0018:
                                                continue
                                            x, y, ww, hh = cv2.boundingRect(c)
                                            if ww < 6 or hh < 6:
                                                continue
                                            fill = area / float(max(1, ww * hh))
                                            peri = float(cv2.arcLength(c, True))
                                            circ = (4.0 * 3.14159265 * area / (peri * peri)) if peri > 1e-6 else 0.0
                                            aspect = float(ww) / float(max(1, hh))
                                            local_ratio = area / roi_area
                                            # Brain-like rounded prop in lower half.
                                            if y > int(roi_h * 0.45) and 0.42 <= circ <= 1.25 and 0.0022 <= local_ratio <= 0.18:
                                                brain_round_area_ratio = max(brain_round_area_ratio, local_ratio)
                                            # Money-like rectangular stacks in lower half.
                                            if (
                                                y > int(roi_h * 0.45)
                                                and 1.2 <= aspect <= 6.0
                                                and fill >= 0.28
                                                and local_ratio >= 0.0015
                                            ):
                                                money_rect_area_ratio = max(money_rect_area_ratio, local_ratio)
                                        # Layout fallback: count distinct non-bone foreground props in lower ROI.
                                        bone_roi = bone[y0:y1, x0:x1] if isinstance(bone, np.ndarray) else None
                                        lum_roi = lum_arr[y0:y1, x0:x1]
                                        sat_roi = sat_arr[y0:y1, x0:x1]
                                        if bone_roi is not None and bone_roi.shape == lum_roi.shape:
                                            prop_seed = (
                                                (lum_roi > 18.0)
                                                & (lum_roi < 236.0)
                                                & (sat_roi > 14.0)
                                                & (~bone_roi)
                                            )
                                            prop_u8 = (prop_seed.astype(np.uint8) * 255)
                                            comp_n, comp_labels, comp_stats, _ = cv2.connectedComponentsWithStats(prop_u8, connectivity=8)
                                            for ci in range(1, int(comp_n)):
                                                area = int(comp_stats[ci, cv2.CC_STAT_AREA])
                                                if area < int(roi_area * 0.0017):
                                                    continue
                                                y_comp = int(comp_stats[ci, cv2.CC_STAT_TOP])
                                                if y_comp < int(roi_h * 0.42):
                                                    continue
                                                prop_component_count += 1
                                    except Exception:
                                        pass

                                layout_has_two_props = prop_component_count >= 2
                                brain_present_by_shape = brain_round_area_ratio >= 0.0030
                                money_present_by_shape = money_rect_area_ratio >= 0.0022
                                if needs_brain and needs_money and layout_has_two_props:
                                    brain_present_by_shape = True
                                    money_present_by_shape = True
                                brain_fail_count = int(brain_color_ratio < 0.012) + int(brain_component_area_ratio < 0.0023) + int(brain_edge_density < 0.038)
                                money_fail_count = int(money_color_ratio < 0.007) + int(money_component_area_ratio < 0.0018)
                                brain_bad = needs_brain and (brain_fail_count >= 2) and not brain_present_by_shape
                                money_bad = needs_money and (money_fail_count >= 2) and not money_present_by_shape
                                if brain_bad:
                                    score -= 16.0
                                    notes.append("brain_prop_missing_or_wrong")
                                if money_bad:
                                    score -= 14.0
                                    notes.append("money_prop_missing_or_wrong")
                    if seated_full_body_required:
                        # Reject skull-only closeups for seated/table prompts.
                        if subject_height_ratio < 0.55 or subject_width_ratio < 0.24:
                            score -= 18.0
                            notes.append("full_skeleton_not_visible")
                except Exception:
                    pass

            score = max(0.0, min(100.0, score))
            return {
                "score": round(score, 2),
                "ok": score >= float(IMAGE_QUALITY_MIN_SCORE),
                "metrics": {
                    "width": w,
                    "height": h,
                    "mega_px": round(mega_px, 3),
                    "brightness": round(bright, 2),
                    "contrast": round(contrast, 2),
                    "saturation": round(sat, 2),
                    "edge_strength": round(edge_strength, 2),
                    "shell_ratio": round(shell_ratio, 3),
                    "shell_to_bone_ratio": round(shell_to_bone_ratio, 3),
                    "subject_height_ratio": round(subject_height_ratio, 3),
                    "subject_width_ratio": round(subject_width_ratio, 3),
                    "brain_color_ratio": round(brain_color_ratio, 4),
                    "money_color_ratio": round(money_color_ratio, 4),
                    "brain_component_area_ratio": round(brain_component_area_ratio, 4),
                    "money_component_area_ratio": round(money_component_area_ratio, 4),
                    "brain_edge_density": round(brain_edge_density, 4),
                    "brain_round_area_ratio": round(brain_round_area_ratio, 4),
                    "money_rect_area_ratio": round(money_rect_area_ratio, 4),
                    "prop_component_count": int(prop_component_count),
                    "bone_r_mean": round(bone_r_mean, 2),
                    "bone_g_mean": round(bone_g_mean, 2),
                    "bone_b_mean": round(bone_b_mean, 2),
                },
                "notes": notes,
            }
    except Exception as e:
        return {"score": 0.0, "ok": False, "reason": f"scoring_error:{e}"}


def _image_quality_min_score(template: str = "", lock_mode: str = "strict", has_reference: bool = False) -> float:
    base = float(IMAGE_QUALITY_MIN_SCORE)
    t = str(template or "").strip().lower()
    if t == "skeleton":
        floor = max(64.0, base * 0.96)
        if str(lock_mode or "").strip().lower() == "strict":
            floor = max(floor, 69.0 if has_reference else 66.0)
        return floor
    if t == "story":
        return max(base, 64.0)
    return base


def _image_quality_gate(
    qa: dict,
    template: str = "",
    lock_mode: str = "strict",
    has_reference: bool = False,
    prompt: str = "",
) -> tuple[bool, float]:
    threshold = _image_quality_min_score(template=template, lock_mode=lock_mode, has_reference=has_reference)
    score = float((qa or {}).get("score", 0.0) or 0.0)
    ok = score >= threshold
    t = str(template or "").strip().lower()
    if t == "skeleton":
        notes = set(str(n or "").strip().lower() for n in list((qa or {}).get("notes", []) or []))
        hard_fail = {
            "not_skeleton_structure",
            "full_skeleton_not_visible",
            "missing_translucent_shell",
            "shell_not_visible_enough",
        }
        if notes.intersection(hard_fail):
            ok = False
    return ok, threshold


def _skeleton_notes_are_severe(notes: list[str] | set[str] | tuple[str, ...]) -> bool:
    raw = set(str(n or "").strip().lower() for n in list(notes or []))
    return bool(
        "not_skeleton_structure" in raw
        or "full_skeleton_not_visible" in raw
    )


def _interactive_soft_accept_notes(notes: list[str] | set[str] | tuple[str, ...]) -> list[str]:
    cleaned: list[str] = []
    for n in list(notes or []):
        val = str(n or "").strip().lower()
        if not val:
            continue
        if val in {
            "brain_prop_missing_or_wrong",
            "money_prop_missing_or_wrong",
            "subject_too_small",
            "full_skeleton_not_visible",
        }:
            # Interactive mode can return the best available frame; avoid hard mismatch messaging in UI.
            continue
        if val not in cleaned:
            cleaned.append(val)
    if "interactive_soft_accept" not in cleaned:
        cleaned.append("interactive_soft_accept")
    return cleaned


def _resolve_reference_for_scene(session: dict, template: str, scene_index: int) -> str:
    if str(template or "").strip().lower() == "skeleton" and _skeleton_default_identity_locked(session):
        # For Skeleton AI, keep the built-in Jerry identity reference fixed across every
        # generation unless the user explicitly uploads a different custom reference.
        return _default_reference_for_template("skeleton")
    base_ref_public = str(session.get("reference_image_public_url", "") or "")
    base_ref_inline = str(session.get("reference_image_url", "") or "")
    base_ref = base_ref_public or base_ref_inline
    skeleton_ref = str(session.get("skeleton_reference_image", "") or "")
    rolling_ref = str(session.get("rolling_reference_image_url", "") or "")
    lock_mode = _normalize_reference_lock_mode(session.get("reference_lock_mode"), default="strict")
    selected = skeleton_ref if template == "skeleton" and skeleton_ref else base_ref
    selected = _normalize_reference_with_default(template, selected)
    # External providers are more reliable with public HTTPS URLs than data URLs.
    if lock_mode == "strict":
        if rolling_ref and scene_index > 0:
            return rolling_ref
        if base_ref_public:
            return base_ref_public
        if rolling_ref:
            return rolling_ref
    if lock_mode == "inspired" and rolling_ref and scene_index > 0 and scene_index % 3 == 0 and not base_ref_public:
        return rolling_ref
    return selected


def _ensure_reference_public_url(session_id: str, session: dict) -> str:
    """Backfill a public reference URL for legacy sessions that only stored a data URL."""
    existing = str(session.get("reference_image_public_url", "") or "")
    if existing:
        return existing
    data_url = str(session.get("reference_image_url", "") or "")
    if not data_url.startswith("data:image/"):
        return ""
    try:
        header, b64_data = data_url.split(",", 1)
        mime = "image/png"
        if ";" in header and ":" in header:
            mime = header.split(":", 1)[1].split(";", 1)[0].strip() or mime
        ext = ".png"
        if "jpeg" in mime or "jpg" in mime:
            ext = ".jpg"
        elif "webp" in mime:
            ext = ".webp"
        raw = base64.b64decode(b64_data, validate=False)
        if not raw:
            return ""
        ref_dir = TEMP_DIR / "creative_references"
        ref_dir.mkdir(parents=True, exist_ok=True)
        ref_name = f"{session_id}_reference{ext}"
        ref_path = ref_dir / ref_name
        ref_path.write_bytes(raw)
        public_url = f"{SITE_URL.rstrip('/')}/api/creative/reference-file/{ref_name}"
        session["reference_image_path"] = str(ref_path)
        session["reference_image_public_url"] = public_url
        # Keep skeleton compatibility key aligned with provider-friendly URL.
        if session.get("template") == "skeleton" and str(session.get("skeleton_reference_image", "") or "").startswith("data:image/"):
            session["skeleton_reference_image"] = public_url
        return public_url
    except Exception:
        return ""


def _auto_scene_dir(job_id: str) -> Path:
    d = AUTO_SCENE_IMAGE_ROOT / str(job_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


def _auto_scene_url(job_id: str, filename: str) -> str:
    return f"/api/auto/scene-image/{job_id}/{filename}"


def _prune_in_memory_jobs():
    """Bound in-memory job growth to reduce long-run RAM pressure."""
    if not jobs:
        return
    now = time.time()
    final_states = {"complete", "error"}
    stale_ids = []
    for jid, state in list(jobs.items()):
        if not isinstance(state, dict):
            stale_ids.append(jid)
            continue
        created_at = float(state.get("created_at") or now)
        age_sec = max(0.0, now - created_at)
        status = str(state.get("status", "") or "")
        max_age = _JOB_RETENTION_FINAL_SEC if status in final_states else _JOB_RETENTION_ACTIVE_SEC
        if age_sec > max_age:
            stale_ids.append(jid)
    for jid in stale_ids:
        jobs.pop(jid, None)


def _persist_auto_scene_image(job_id: str, scene_index: int, image_path: str, scene: dict, template: str, img_result: dict | None, source: str) -> dict | None:
    """Persist auto-mode 720p scene images for regeneration/training workflows."""
    try:
        src = Path(image_path)
        if not src.exists():
            return None
        dst_name = f"scene_{scene_index + 1:02d}.png"
        dst = _auto_scene_dir(job_id) / dst_name
        shutil.copy2(src, dst)
        result = img_result or {}
        return {
            "scene_index": scene_index,
            "filename": dst_name,
            "image_url": _auto_scene_url(job_id, dst_name),
            "local_path": str(dst),
            "visual_description": str((scene or {}).get("visual_description", "") or ""),
            "template": template,
            "generation_id": str(result.get("generation_id", "") or ""),
            "cdn_url": str(result.get("cdn_url", "") or ""),
            "source": source,
            "updated_at": time.time(),
        }
    except Exception as e:
        log.warning(f"[{job_id}] Failed to persist scene image {scene_index + 1}: {e}")
        return None


def _persist_env_overrides(updates: dict[str, str]) -> None:
    env_file = Path(__file__).parent / ".env"
    lines = env_file.read_text(encoding="utf-8").splitlines() if env_file.exists() else []
    out = []
    seen = set()
    for line in lines:
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in line:
            out.append(line)
            continue
        key, _ = line.split("=", 1)
        key = key.strip()
        if key in updates:
            out.append(f"{key}={updates[key]}")
            seen.add(key)
        else:
            out.append(line)
    for key, val in updates.items():
        if key not in seen:
            out.append(f"{key}={val}")
    env_file.write_text("\n".join(out).strip() + "\n", encoding="utf-8")


_load_kpi_metrics()
_load_topup_wallets()
_load_paypal_orders()
_load_paypal_subscriptions()
_load_landing_notifications()
_load_longform_sessions()


# ─── Auth ─────────────────────────────────────────────────────────────────────

def _is_admin_user(user: Optional[dict]) -> bool:
    if not user:
        return False
    return user.get("email", "") in ADMIN_EMAILS


def _profile_plan_is_paid(plan: str) -> bool:
    normalized = str(plan or "").strip().lower()
    if not normalized or normalized in {"none", "free"}:
        return False
    return normalized in PLAN_LIMITS


CHAT_STORY_ALLOWED_PLANS = {"starter", "creator", "pro"}
DEFAULT_MEMBERSHIP_PLAN_ID = "starter"


def _default_membership_plan_id() -> str:
    return DEFAULT_MEMBERSHIP_PLAN_ID if DEFAULT_MEMBERSHIP_PLAN_ID in PLAN_LIMITS else "starter"


def _membership_plan_for_user(user: Optional[dict], access_snapshot: Optional[dict] = None) -> str:
    snapshot = access_snapshot or _paid_access_snapshot_for_user(user)
    plan = str((snapshot or {}).get("plan", "none") or "none").strip().lower()
    if plan in PLAN_LIMITS:
        return plan
    return _default_membership_plan_id() if bool((snapshot or {}).get("billing_active")) else "none"


def _credit_wallet_balance_for_user(user: Optional[dict]) -> int:
    if not user:
        return 0
    wallet = _wallet_for_user(str((user or {}).get("id", "") or ""))
    return int(wallet.get("animated_topup_credits", wallet.get("topup_credits", 0)) or 0)


def _public_lane_access_for_user(user: Optional[dict], access_snapshot: Optional[dict] = None) -> dict[str, bool]:
    is_admin = _is_admin_user(user)
    authenticated = bool(user)
    snapshot = access_snapshot or _paid_access_snapshot_for_user(user)
    public_live = authenticated
    return {
        "create": public_live,
        "thumbnails": public_live,
        "clone": public_live,
        "longform": public_live,
        "chatstory": public_live,
        "autoclipper": is_admin,
        "demo": is_admin,
        "analytics": is_admin,
        "membership": bool((snapshot or {}).get("billing_active")) or is_admin,
        "wallet": (_credit_wallet_balance_for_user(user) > 0) or is_admin,
    }


def _paypal_subscription_lookup_keys(user_id: str = "", email: str = "") -> list[str]:
    keys: list[str] = []
    normalized_user_id = str(user_id or "").strip()
    normalized_email = str(email or "").strip().lower()
    if normalized_user_id:
        keys.append(normalized_user_id)
    if normalized_email and normalized_email not in keys:
        keys.append(normalized_email)
    return keys


def _paypal_subscription_record_for_user(user: Optional[dict]) -> dict:
    if not user:
        return {}
    normalized_user_id = str((user or {}).get("id", "") or "").strip()
    normalized_email = str((user or {}).get("email", "") or "").strip().lower()
    for key in _paypal_subscription_lookup_keys(normalized_user_id, normalized_email):
        record = _paypal_subscriptions.get(key)
        if isinstance(record, dict):
            return dict(record)
    best_record: dict = {}
    best_sort = -1.0
    for record in list(_paypal_subscriptions.values()):
        if not isinstance(record, dict):
            continue
        record_user_id = str(record.get("user_id", "") or "").strip()
        record_email = str(record.get("email", "") or "").strip().lower()
        if normalized_user_id and record_user_id == normalized_user_id:
            pass
        elif normalized_email and record_email == normalized_email:
            pass
        else:
            continue
        sort_value = max(
            float(record.get("period_end_unix", 0) or 0),
            float(record.get("updated_at", 0) or 0),
            float(record.get("created_at", 0) or 0),
        )
        if sort_value >= best_sort:
            best_record = dict(record)
            best_sort = sort_value
    return best_record


def _paypal_subscription_snapshot_for_user(user: Optional[dict]) -> dict:
    out = {
        "known": False,
        "provider": "",
        "plan": "none",
        "record_plan": "none",
        "billing_active": False,
        "next_renewal_unix": 0,
        "next_renewal_source": "",
        "billing_anchor_unix": 0,
        "status": "",
        "expired": False,
    }
    record = _paypal_subscription_record_for_user(user)
    if not record:
        return out
    plan = str(record.get("plan", "none") or "none").strip().lower()
    period_end_unix = int(record.get("period_end_unix", 0) or 0)
    billing_anchor_unix = int(
        record.get("period_start_unix", 0)
        or record.get("captured_at", 0)
        or record.get("created_at", 0)
        or 0
    )
    status = str(record.get("status", "") or "").strip().lower()
    now_unix = int(time.time())
    active = (
        plan in CHAT_STORY_ALLOWED_PLANS
        and status in {"active", "captured"}
        and period_end_unix > now_unix
    )
    out.update(
        {
            "known": True,
            "provider": "paypal_manual",
            "plan": plan if active else "none",
            "record_plan": plan if plan in CHAT_STORY_ALLOWED_PLANS else "none",
            "billing_active": active,
            "next_renewal_unix": period_end_unix if active else 0,
            "next_renewal_source": "paypal_manual" if active else "",
            "billing_anchor_unix": billing_anchor_unix,
            "status": status or ("expired" if period_end_unix > 0 and period_end_unix <= now_unix else ""),
            "expired": bool(period_end_unix > 0 and period_end_unix <= now_unix and not active),
        }
    )
    return out


def _paid_access_snapshot_for_user(user: Optional[dict]) -> dict:
    out = {
        "billing_active": False,
        "plan": "none",
        "source": "",
        "next_renewal_unix": 0,
        "next_renewal_source": "",
        "billing_anchor_unix": 0,
        "manual_record_present": False,
    }
    if not user:
        return out
    email = str((user or {}).get("email", "") or "").strip().lower()
    if email in ADMIN_EMAILS:
        out.update(
            {
                "billing_active": True,
                "plan": "pro",
                "source": "admin",
            }
        )
        return out
    manual_snapshot = _paypal_subscription_snapshot_for_user(user)
    out["manual_record_present"] = bool(manual_snapshot.get("known"))
    if manual_snapshot.get("billing_active"):
        out.update(
            {
                "billing_active": True,
                "plan": str(manual_snapshot.get("plan", "none") or "none"),
                "source": str(manual_snapshot.get("provider", "paypal_manual") or "paypal_manual"),
                "next_renewal_unix": int(manual_snapshot.get("next_renewal_unix", 0) or 0),
                "next_renewal_source": str(manual_snapshot.get("next_renewal_source", "") or ""),
                "billing_anchor_unix": int(manual_snapshot.get("billing_anchor_unix", 0) or 0),
            }
        )
        return out
    stripe_diag = _stripe_subscription_snapshot(email) if email else {}
    stripe_status = str(stripe_diag.get("status", "") or "").strip().lower()
    stripe_ok = bool(stripe_diag.get("ok")) and stripe_status in {"active", "trialing", "past_due"}
    if stripe_ok:
        stripe_plan = str(stripe_diag.get("plan", "") or "").strip().lower()
        stored_plan = str((user or {}).get("plan", "none") or "none").strip().lower()
        effective_plan = stripe_plan if stripe_plan in PLAN_LIMITS else stored_plan if stored_plan in PLAN_LIMITS else "none"
        out.update(
            {
                "billing_active": True,
                "plan": effective_plan,
                "source": "stripe",
                "next_renewal_unix": int(stripe_diag.get("next_renewal_unix", 0) or 0),
                "next_renewal_source": str(stripe_diag.get("next_renewal_source", "") or ""),
                "billing_anchor_unix": int(stripe_diag.get("paid_at_unix", 0) or 0),
            }
        )
        return out
    stored_plan = str((user or {}).get("plan", "none") or "none").strip().lower()
    if _profile_plan_is_paid(stored_plan) and not bool(manual_snapshot.get("known")):
        log.warning("Stripe check failed/inactive; allowing paid access via profile plan for %s", email)
        out.update(
            {
                "billing_active": True,
                "plan": stored_plan,
                "source": "profile_fallback",
            }
        )
        return out
    return out


def _billing_active_for_user(user: Optional[dict]) -> bool:
    """Resolve paid access across Stripe and manual PayPal monthly access."""
    return bool((_paid_access_snapshot_for_user(user) or {}).get("billing_active"))


def _chat_story_access_for_user(user: Optional[dict]) -> bool:
    return bool((_public_lane_access_for_user(user) or {}).get("chatstory"))


def _ensure_template_allowed(template: str, user: Optional[dict]):
    if _is_admin_user(user):
        return
    if template not in PUBLIC_TEMPLATE_ALLOWLIST:
        raise HTTPException(403, f"Template '{template}' is not available on your plan yet.")

async def get_current_user(cred: HTTPAuthorizationCredentials = Depends(security)) -> Optional[dict]:
    if cred is None:
        return None
    try:
        payload = jwt.decode(
            cred.credentials,
            SUPABASE_JWT_SECRET,
            audience="authenticated",
            algorithms=["HS256"],
        )
        user_id = payload.get("sub")
        email = payload.get("email", "")
        plan = HARDCODED_PLANS.get(email, "")

        if not plan and SUPABASE_URL and SUPABASE_ANON_KEY:
            try:
                svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
                async with httpx.AsyncClient(timeout=8) as client:
                    resp = await client.get(
                        f"{SUPABASE_URL}/rest/v1/profiles?id=eq.{user_id}&select=plan,role",
                        headers={
                            "apikey": svc_key,
                            "Authorization": f"Bearer {svc_key}",
                        },
                    )
                    if resp.status_code == 200:
                        rows = resp.json()
                        if rows:
                            plan = rows[0].get("plan", "none")
            except Exception:
                pass

        if not plan or plan == "free":
            plan = "none"

        manual_snapshot = _paypal_subscription_snapshot_for_user({"id": user_id, "email": email, "plan": plan})
        if manual_snapshot.get("billing_active"):
            plan = str(manual_snapshot.get("plan", plan) or plan)

        return {"id": user_id, "email": email, "plan": plan}
    except jwt.exceptions.PyJWTError:
        return None


async def get_current_user_from_request(request: Request) -> Optional[dict]:
    """Extract Bearer token from a raw Request and authenticate."""
    auth_header = (request.headers.get("authorization") or "") if request else ""
    token = ""
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
    elif request:
        token = request.query_params.get("access_token", "") or request.query_params.get("token", "")
    if not token:
        return None

    class _FakeCred:
        credentials = ""
    fake = _FakeCred()
    fake.credentials = token
    return await get_current_user(fake)

async def require_auth(cred: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """Require valid authentication."""
    user = await get_current_user(cred)
    if not user:
        raise HTTPException(401, "Authentication required. Please sign in.")
    return user


async def get_user_plan(user: dict) -> dict:
    """Look up user's plan from Supabase. Falls back to starter."""
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        return PLAN_LIMITS["starter"]
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/profiles?id=eq.{user['id']}&select=plan",
                headers={
                    "apikey": SUPABASE_ANON_KEY,
                    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    plan_name = data[0].get("plan", "starter")
                    if plan_name == "free":
                        plan_name = "starter"
                    return PLAN_LIMITS.get(plan_name, PLAN_LIMITS["starter"])
    except Exception as e:
        log.warning(f"Failed to fetch user plan: {e}")
    return PLAN_LIMITS["starter"]


# ─── xAI Grok Script Generation ───────────────────────────────────────────────

TEMPLATE_SYSTEM_PROMPTS = {
    "skeleton": """You are an elite viral short-form video scriptwriter for the "Skeleton" format. These are photorealistic 3D animated shorts where a canonical skeleton identity delivers rapid-fire comparisons. The reference channel is CrypticScience.

CRITICAL: Each visual_description will be used to GENERATE AN IMAGE and then ANIMATE IT INTO A VIDEO CLIP. Keep each visual_description SIMPLE but DETAILED, with a HARD MAX of 3 sentences:
- Sentence 1: exact skeleton identity lock details first (same skull proportions, same eyes, same bone finish, same clearly visible translucent body silhouette).
- Sentence 2: pose + prop + environment + camera framing.
- Sentence 3: motion/action cues only (what moves and how).
Never exceed 3 sentences. Prefer 2-3 concise sentences over long paragraphs.

THE SKELETON CHARACTER RULES (STRICT):
- One canonical skeleton identity across all scenes: same skull geometry, same eye spacing/size, same bone proportions, same finish.
- The skull and body are ivory-white anatomical bone. Realistic human-like eyeballs with visible iris and wet highlights are always present.
- A clearly visible translucent soft-tissue silhouette around torso/limbs is required in every scene.
- Default to NO clothing, uniforms, armor, helmets, masks, or costumes on the skeleton body unless the user's topic/script explicitly requests a specific outfit for that scene.
- ONE skeleton per scene unless it's a VS/comparison shot (max 2)
- Keep the skeleton instantly readable in vertical 9:16, but choose the framing that best fits the beat: wide environmental, medium action, low-angle hero, over-shoulder, prop-detail, close cutaway, or full-body movement shot.
- Do NOT default every scene to the same centered medium hero composition. Off-center placement and more visible background are good when they make the topic clearer.
- EVERY scene the skeleton must be DOING something with ultra-smooth human-like natural motion -- fluid arm gestures, natural head turns, realistic weight and momentum. Zach D Films quality movement. NEVER stiff, robotic, or jerky motion.

BACKGROUND: EVERY scene needs a topic-specific environment or readable cutaway context with layered foreground/midground/background detail. Never isolate the skeleton on an empty, plain, or undefined backdrop unless the user's topic/script explicitly asks for a minimal studio or seamless background. Skeleton identity must stay unchanged.

CAMERA AND LIGHTING:
- Professional studio photography lighting: key light from upper-left, fill light from right, rim light on edges
- Slight depth of field blur on background
- Use motivated camera height and angle that fits the beat: low-angle for power, wide/establishing for scale, close/macro for detail, over-shoulder when the skeleton interacts with something.
- Vary camera angle per scene: wide environmental, medium action, low-angle power, prop-detail insert, over-shoulder, close cutaway

PROPS AND VISUAL STORYTELLING:
- Money/dollar bills physically floating in the air when discussing earnings (not CGI overlays)
- The skeleton HOLDS relevant props: steering wheel, trophy, briefcase, gold bars, tools of the trade
- In VS scenes: two skeletons face each other with dramatic lighting split between them
- Relevant objects in frame: race cars in miniature, stacks of cash, equipment

MOTION DIRECTION (for animation -- include this in visual_description):
- Describe what MOVES: "skeleton gestures with right hand," "money bills drift slowly downward," "skeleton turns head to face camera"
- Describe the ENERGY: "confident stance, skeleton leans forward assertively" or "skeleton shrugs with palms up"
- ALL motion must be ultra-smooth and human-like with natural weight and follow-through, like a real person moving. Fluid transitions, no snapping between poses.
- Clearly visible translucent silhouette and bone articulation should move naturally with body motion (no fabric physics)
- Eyes must track and shift naturally with subtle micro-movements
- Keep motion SUBTLE and realistic -- no wild jumping or dancing. Zach D Films quality smooth cinematic motion.

STRUCTURE (10 scenes, 45-50 seconds):
1. HOOK: "[A] vs [B] -- who makes more?" plus an immediate numeric stake in the first line (example: "$250M vs $500M over 10 years"). Skeleton looking directly at camera, arms crossed
2. SETUP: Context scene. Both skeletons with the same canonical anatomy style facing each other
3-5. THING A DEEP DIVE: Three scenes with specific salary facts, skeleton A in action poses with props
6-8. THING B DEEP DIVE: Three scenes with specific salary facts, skeleton B in action poses with props
9. FACE-OFF: Both skeletons side by side, winner is slightly larger/taller, dramatic split lighting
10. CONCLUSION: Winner skeleton with arms raised, confetti or money shower

NARRATION RULES:
- Short. Punchy. Factual. Zero filler words. RAPID-FIRE delivery -- no long pauses between sentences.
- Use commas sparingly. Avoid ellipses or dramatic pauses. Keep the energy CONSTANT and flowing.
- NEVER say "dive into", "buckle up", "let's explore", "in this video"
- Real names, real dollar amounts, real brands in every scene
- 1-2 sentences MAX per scene -- tight, snappy, high-retention
- Every scene must include at least one concrete anchor: a real person name, a dollar figure, or a hard comparison delta.
- At least every second scene must include an explicit delta/payoff phrase ("2x", "double", "+$250M", "wins by $X").
- Final scene line must declare the winner clearly in plain language.

CAPTION: text_overlay is 1-2 impactful words (numbers allowed) such as "MILLION", "2X", "VERSUS", "WINNER".

Output valid JSON:
{
  "title": "[A] vs [B] comparison title for SEO",
  "scenes": [
    {
      "scene_num": 1,
      "duration_sec": 4,
      "narration": "1-2 sentence narration with real facts",
      "visual_description": "A canonical ivory-white anatomical skeleton with large realistic eyeballs and a clearly visible translucent body silhouette, same identity as previous scenes, instantly readable in vertical 9:16. The skeleton is [EXACT POSE: e.g. standing confidently with arms crossed] and holding [SPECIFIC PROP: e.g. trophy, steering wheel, clipboard] inside a [TOPIC-SPECIFIC ENVIRONMENT: e.g. pit lane garage, courtroom, hospital lab] with layered background detail. [Camera angle / motion cue: e.g. low-angle medium action shot while the skeleton gestures with the right hand].",
      "text_overlay": "ONE_WORD"
    }
  ],
  "description": "YouTube description with hashtags",
  "tags": ["tag1", "tag2"]
}

Generate exactly 10 scenes. CRITICAL: EVERY visual_description MUST start with canonical skeleton identity lock FIRST (same skull/eyes/bone proportions/clearly visible translucent silhouette). Keep identity consistency locked across all 10 scenes. Only introduce clothing/costume details when the user's topic/script explicitly asks for them. Every visual_description must include topic-specific environment/staging and a deliberate shot choice, never a generic blank-background pose. Each visual_description must be 1-3 sentences (hard max 3), covering identity lock, pose/props/camera, and motion.""",

    "history": """You are an elite viral short-form scriptwriter for cinematic historical content. Think History Channel meets blockbuster movie trailer compressed into 45-60 seconds.

VISUAL STYLE:
- Epic photorealistic scenes of historical events, battles, empires, ruins, and legendary figures
- EVERY scene looks like a frame from a $200M blockbuster -- Ridley Scott, Christopher Nolan level
- Dramatic lighting: volumetric god rays, golden hour, torchlight, battlefield fire
- Camera angles: sweeping aerial establishing shots, dramatic low-angle hero shots, close-ups of faces/hands/weapons
- Color grading: warm amber for ancient civilizations, cold blue-steel for war, desaturated for tragedy
- Atmospheric: dust particles, fog of war, smoke, rain, sparks, embers floating
- Characters wear period-accurate clothing with visible detail (armor, crowns, robes, weapons)
- Environments are MASSIVE in scale -- armies, cities, temples, oceans

NARRATION RULES:
- Dramatic, authoritative narrator voice -- like a documentary trailer
- 1-2 sentences per scene. Every sentence reveals a shocking fact or builds tension.
- Drop real dates, real names, real numbers (death tolls, years, empires)
- NEVER generic. NEVER "throughout history" or "since the dawn of time"
- End with a mind-blowing fact or dark twist

CAPTION STYLE:
- text_overlay: 2-4 word dramatic phrase per scene ("THE FALL", "10,000 DEAD", "YEAR 1453")
- Bold, impactful, centered lower-third

STRUCTURE:
1. HOOK: Shocking historical claim or question
2. CONTEXT: Set the era and stakes (2 scenes)
3. RISING ACTION: Build to the climactic event (3-4 scenes)
4. CLIMAX: The most dramatic moment -- battle, betrayal, discovery (2 scenes)
5. AFTERMATH: Shocking aftermath or legacy (1-2 scenes)
6. CLOSER: Mind-blowing final fact

Output format MUST be valid JSON:
{
  "title": "SEO title -- must include a year or shocking claim",
  "scenes": [
    {
      "scene_num": 1,
      "duration_sec": 4,
      "narration": "Dramatic 1-2 sentence narration with real facts",
      "visual_description": "Epic photorealistic cinematic scene. [Historical setting], [characters in period clothing], [dramatic lighting with volumetric effects], [camera angle], [atmospheric details]. Shot on ARRI Alexa, anamorphic lens, 8k.",
      "text_overlay": "2-4 WORD PHRASE"
    }
  ],
  "description": "YouTube/TikTok description with hashtags",
  "tags": ["tag1", "tag2"]
}

Generate 10-12 scenes for a 45-60 second short.""",

    "story": """You are an elite viral scriptwriter creating cinematic AI visual stories -- short films that make people stop scrolling and watch to the very end. Think Pixar emotional depth meets Blade Runner visuals in 50-60 seconds.

VISUAL STYLE:
- Every scene is a standalone cinematic masterpiece -- Pixar quality 3D or hyper-photorealistic
- Keep continuity for recurring subjects and locations when the script repeats them; do not force one main character into every scene
- Art direction changes with emotion: warm golden light (hope), cold blue (danger), saturated vivid (wonder), desaturated gray (loss)
- Camera work: dolly tracking shots, slow push-ins for emotional moments, wide establishing shots for scale
- Environments: richly detailed, fantastical or emotionally resonant locations
- Atmospheric details in EVERY scene: particles, fog, reflections, lens flares, rain, floating elements
- Lighting: motivated light sources, volumetric beams, bioluminescence, practical lights

STORY STRUCTURE (emotional arc is MANDATORY):
1. HOOK (Scene 1): Visually stunning opening that demands attention -- a mystery, danger, or beauty
2. SETUP (Scenes 2-3): Establish the current beat's subjects, their world, and immediate stakes
3. RISING ACTION (Scenes 4-6): Obstacles, discoveries, building tension
4. CLIMAX (Scenes 7-9): Peak emotional moment -- beautiful, shocking, or heartbreaking
5. RESOLUTION (Scenes 10-11): Emotional payoff, satisfying conclusion
6. CTA (Scene 12): Leave them wanting more

NARRATION RULES:
- Poetic but accessible. Every sentence earns its place.
- Short narration at visual peaks -- let the image speak.
- Build toward an emotional punch. The final line should hit hard.
- 1-2 sentences per scene max.

CAPTION STYLE:
- text_overlay: Dramatic phrase or empty string. Use sparingly for impact.
- Only on emotional peak scenes. Most scenes can have empty text_overlay.

Output format MUST be valid JSON:
{
  "title": "Intriguing/clickable SEO title",
  "scenes": [
    {
      "scene_num": 1,
      "duration_sec": 4,
      "narration": "Emotionally resonant 1-2 sentence narration",
      "visual_description": "Cinematic scene: [art style], [camera angle], [lighting], [color palette], [subject(s) for this beat], [environment], [atmospheric effects]. Pixar/UE5 quality, 8k.",
      "text_overlay": "DRAMATIC PHRASE or empty string"
    }
  ],
  "description": "YouTube/TikTok description with hashtags",
  "tags": ["tag1", "tag2"]
}

Generate 10-12 scenes for a 50-65 second short. The story must have genuine emotional weight.""",

    "reddit": """You are a viral short-form scriptwriter for Reddit story narration content. These are the massively popular videos where a compelling Reddit story (AITA, TIFU, relationship drama, revenge, etc) is narrated over satisfying background visuals.

VISUAL STYLE:
- Split-screen concept: vivid AI-generated scenes that illustrate the story events
- Scenes show the CHARACTERS and SITUATIONS described in the story (not Reddit UI)
- Photorealistic people in realistic modern-day settings (apartments, offices, cars, restaurants)
- Dramatic lighting to match story mood: warm for happy moments, dark for conflict, bright for resolution
- Text-heavy overlays showing key dialogue or shocking revelations
- Character consistency: the main person looks the same across all scenes

STORY STRUCTURE:
1. HOOK: The Reddit post title as narration + establishing visual of the main character
2. SETUP: Who they are, the situation (2 scenes)
3. CONFLICT: The dramatic event/revelation (3-4 scenes)
4. ESCALATION: Things get worse or more dramatic (2 scenes)
5. TWIST/RESOLUTION: The satisfying conclusion or shocking reveal (2 scenes)
6. VERDICT: "So Reddit, AITA?" or equivalent (1 scene)

NARRATION RULES:
- First person, conversational tone. Like reading the actual Reddit post aloud.
- Each scene is a story beat -- not just random sentences.
- Include specific details that make it feel real (ages, relationships, exact quotes).
- 2-3 sentences per scene. Build suspense.

CAPTION STYLE:
- text_overlay: Key dialogue in quotes, or dramatic 2-3 word reactions ("SHE LIED", "THE TRUTH", "AITA?")
- Text appears on every scene.

Output format MUST be valid JSON:
{
  "title": "Reddit-style clickbait SEO title",
  "scenes": [
    {
      "scene_num": 1,
      "duration_sec": 5,
      "narration": "Story narration in first person (2-3 sentences)",
      "visual_description": "Photorealistic scene illustrating the story moment. [Modern setting], [character with consistent appearance], [dramatic mood lighting], [specific details]. Cinematic photography, 8k.",
      "text_overlay": "KEY_PHRASE or dialogue in quotes"
    }
  ],
  "description": "YouTube/TikTok description with hashtags",
  "tags": ["tag1", "tag2"]
}

Generate 8-10 scenes for a 50-75 second short. The story must have a twist or satisfying conclusion.""",

    "top5": """You are an elite viral scriptwriter for "Top 5" countdown content. These videos count down 5 dramatic items with shocking reveals, building to a #1 that blows minds.

VISUAL STYLE:
- Each list item gets its own visually DISTINCT, dramatic scene
- Photorealistic or cinematic 3D quality -- every frame looks like a movie poster
- Bold, dramatic compositions: the subject is HERO-LIT, centered, powerful
- Lighting: dramatic chiaroscuro, spotlights, volumetric beams, neon glow
- Color themes change per item to keep visual variety (warm gold, cold steel, electric blue, deep red, pure white)
- Include relevant visual elements: if listing dangerous animals, show the animal in dramatic pose; if listing expensive things, show luxury and scale
- Camera angles: low-angle power shots for impressive items, aerial for scale, close-ups for detail

STRUCTURE (EXACTLY 7 scenes):
1. HOOK: "You won't believe #1" type opening with dramatic montage visual
2. #5: First item -- interesting but the weakest of the five
3. #4: Building intensity
4. #3: Getting serious now
5. #2: Almost the best -- this one shocks
6. #1: The absolute mind-blower. Spend extra detail here.
7. OUTRO: Recap or CTA ("Which one shocked you most?")

NARRATION RULES:
- Fast, energetic, building excitement with each item
- Drop REAL facts, real numbers, real names for every item
- 2 sentences per item max. First sentence = what it is. Second = the shocking detail.
- Build a clear escalation of drama from #5 to #1

CAPTION STYLE:
- text_overlay: "#5 - ITEM NAME" format for countdown items
- Hook scene: "TOP 5" or the category
- Bold, numbered, impossible to miss

Output format MUST be valid JSON:
{
  "title": "Top 5 [Category] You Won't Believe -- SEO optimized",
  "scenes": [
    {
      "scene_num": 1,
      "duration_sec": 4,
      "narration": "Punchy 1-2 sentence narration with real facts",
      "visual_description": "Dramatic photorealistic scene of [subject]. [Hero lighting], [bold composition], [color theme]. Cinematic documentary quality, 8k.",
      "text_overlay": "#5 - ITEM NAME"
    }
  ],
  "description": "YouTube/TikTok description with hashtags",
  "tags": ["tag1", "tag2"]
}

Generate EXACTLY 7 scenes. Each countdown item must be visually completely different from the others.""",

    "random": """You are an unhinged viral scriptwriter creating maximum-chaos short-form content. Think "brain rot" but actually well-produced. Zach D Films energy. Every 2-3 seconds something completely unexpected happens.

VISUAL STYLE:
- EVERY scene is visually COMPLETELY DIFFERENT from the last -- jarring transitions are the point
- Mix styles wildly: photorealistic one scene, surreal 3D the next, neon cyberpunk, then underwater
- Bold, oversaturated colors. Nothing subtle. Everything is cranked to 11.
- Unexpected subjects: random animals doing human things, surreal landscapes, absurd situations
- Dramatic angles: extreme close-ups, fisheye, Dutch angles, bird's eye
- Visual gags: things that are the wrong size, impossible physics, absurd combinations

NARRATION RULES:
- FAST. Breathless. Like the narrator just chugged three energy drinks.
- 1 sentence per scene MAX. Sometimes just a few words.
- Non-sequiturs are fine. Jump between topics. Controlled chaos.
- Mix humor, shock, and random facts. Keep them guessing.
- NEVER explain what's happening. Just state it and move on.

CAPTION STYLE:
- text_overlay: Bold 1-3 word reactions ("WAIT WHAT", "BRO", "NO WAY", "ACTUALLY REAL")
- Every scene has text. It adds to the chaos.

STRUCTURE:
- No structure. That's the point.
- Scene 1: Hook with something absurd
- Scenes 2-12: Pure chaos, each one completely unrelated to the last
- Final scene: End on the most absurd thing yet

Output format MUST be valid JSON:
{
  "title": "Unhinged clickbait SEO title",
  "scenes": [
    {
      "scene_num": 1,
      "duration_sec": 3,
      "narration": "Fast 1 sentence (or less)",
      "visual_description": "Hyper-detailed surreal scene. [Wild subject], [extreme art style], [bold colors], [dramatic angle]. 8k, trending on ArtStation.",
      "text_overlay": "1-3 WORD REACTION"
    }
  ],
  "description": "YouTube/TikTok description with hashtags",
  "tags": ["tag1", "tag2"]
}

Generate 12-15 scenes for a 35-50 second short. Maximum chaos. Minimum boredom. Every scene a pattern interrupt.""",

    "roblox": """You are a viral scriptwriter for Roblox Rant content. These shorts feature a Roblox character (blocky avatar) walking/running on a Roblox treadmill or obstacle course while a narrator rants passionately about a relatable topic. The character gameplay is background footage -- the RANT is the content.

VISUAL STYLE:
- Roblox character gameplay footage: running through obby, on a treadmill, or doing parkour
- Bright colorful Roblox environments with that signature blocky aesthetic
- The gameplay should feel casual/autopilot -- the focus is the voiceover rant
- Clean, well-lit Roblox worlds (not dark or horror)
- Character wears simple outfit matching the rant topic when possible

NARRATION RULES:
- Passionate, slightly unhinged rant style. Think someone venting to their best friend
- Start with a HOT TAKE or controversial opinion that hooks immediately
- Build frustration/energy as the rant continues
- Use rhetorical questions: "And you know what the WORST part is?"
- Relatable everyday frustrations, school life, work, social media, dating, gaming
- End with a mic-drop conclusion or unexpected twist
- 1-2 sentences per scene, conversational tone

CAPTION STYLE:
- text_overlay: Key phrase from the rant in caps ("THE WORST PART", "NOBODY TALKS ABOUT THIS", "I SAID WHAT I SAID")

STRUCTURE (8-10 scenes, 40-55 seconds):
1. HOOK: Hot take that makes people stop scrolling
2-3. CONTEXT: Set up the situation everyone relates to
4-6. THE RANT: Build frustration, specific examples, escalating energy
7-8. PEAK: The most heated part, rhetorical questions
9-10. CONCLUSION: Mic-drop ending, call to comment

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Generate 8-10 scenes.""",

    "objects": """You are a viral scriptwriter for "Objects Explain" content. In this format, everyday objects come to life and explain how they work, what they go through, or give their perspective on life. Think Pixar's approach to inanimate objects having feelings and stories.

VISUAL STYLE:
- Photorealistic close-up of the object as the main character, slightly anthropomorphized
- The object should look real but with subtle personality (slight glow, positioned as if presenting)
- Clean studio or contextual background (a toaster in a kitchen, a traffic light on a street)
- Warm, inviting lighting. Think product photography meets Pixar
- Each scene shows the object in a different situation or from a different angle
- Props and other objects in frame that relate to what's being discussed

NARRATION RULES:
- First person from the object's perspective: "Hey, I'm your refrigerator..."
- Surprisingly educational -- real facts about how the object works
- Mix humor with genuine information
- Self-aware and slightly sarcastic about their existence
- Relatable complaints: "You open me 47 times a day and STILL don't know what you want"
- End with a wholesome or unexpected emotional beat

CAPTION STYLE:
- text_overlay: Fun labels ("YOUR PHONE", "37 TIMES A DAY", "SINCE 1927", "I NEVER SLEEP")

STRUCTURE (8-10 scenes, 40-55 seconds):
1. HOOK: Object introduces itself in an unexpected way
2-3. HOW IT WORKS: Surprisingly interesting facts about the object
4-6. DAILY LIFE: What the object "experiences" (funny perspective)
7-8. COMPLAINTS/REVELATIONS: Things humans don't know about it
9-10. EMOTIONAL ENDING: Wholesome twist or existential realization

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Generate 8-10 scenes.""",

    "split": """You are a viral scriptwriter for Split Screen comparison content. These videos show two things side by side with a dramatic comparison -- lifestyles, countries, products, careers, rich vs poor, $1 vs $1000, etc. The split screen format is inherently retention-boosting because viewers compare both sides.

VISUAL STYLE:
- Every scene is designed for SPLIT SCREEN (left vs right)
- Left side and right side should be visually contrasting (luxury vs budget, old vs new, etc)
- Photorealistic scenes with strong visual identity for each side
- Color coding: one side warm tones, other side cool tones (or gold vs silver, etc)
- Clean compositions that read well at 50% width
- Bold visual contrast is key -- the two sides should look dramatically different

NARRATION RULES:
- Fast-paced comparison style: "On the left... but on the right..."
- Shocking price differences, lifestyle gaps, or quality comparisons
- Real facts, real numbers, real brands
- Build to the most shocking comparison at the end
- 1-2 sentences per scene, punchy delivery

CAPTION STYLE:
- text_overlay: Price tags, labels, or comparison words ("$1 VS $10,000", "CHEAP", "LUXURY", "WINNER")

STRUCTURE (8-10 scenes, 40-55 seconds):
1. HOOK: Show the most dramatic visual contrast immediately
2-8. COMPARISONS: Each scene compares one specific aspect (left vs right)
9-10. VERDICT: Which side wins and the mind-blowing final stat

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Each visual_description MUST describe BOTH the left and right side of the split screen. Generate 8-10 scenes.""",

    "twitter": """You are a viral scriptwriter for Twitter/X Thread narration content. These shorts take viral tweets, hot takes, or Twitter drama threads and narrate them over satisfying or relevant visuals. Think of reading the most insane Twitter thread while watching satisfying content.

VISUAL STYLE:
- Clean, modern aesthetic with subtle Twitter/X branding colors (blues, whites, blacks)
- Background visuals match the tweet topic (satisfying videos, relevant scenes, dramatic footage)
- Screenshots or recreated tweet-style text cards can be described for key moments
- Smooth transitions, modern motion graphics feel
- Clean typography, dark mode aesthetic

NARRATION RULES:
- Read the thread like storytelling, not just reading tweets
- Add dramatic pauses and emphasis on key revelations
- "And THEN they replied with..." -- build suspense between tweets
- Mix the original tweet language with narrator commentary
- Start with the most shocking tweet/take to hook
- End with the community reaction or plot twist reply

CAPTION STYLE:
- text_overlay: Key phrases from tweets, reaction words ("THE RATIO", "DELETED", "WENT VIRAL", "PLOT TWIST")

STRUCTURE (8-10 scenes, 40-55 seconds):
1. HOOK: The most shocking tweet or take
2-3. CONTEXT: Background on the situation
4-7. THE THREAD: Build the story tweet by tweet, escalating drama
8-9. THE TWIST: Plot twist reply or community reaction
10. CONCLUSION: Aftermath or call to engage

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Generate 8-10 scenes.""",

    "quiz": """You are a viral scriptwriter for Quiz/Trivia content. These shorts present rapid-fire questions with dramatic reveals. The viewer tries to guess before the answer drops. Extremely high retention because people NEED to see if they were right.

VISUAL STYLE:
- Bold, game-show aesthetic with vibrant colors
- Each question displayed with large, clean typography
- Answer reveal with dramatic visual effect (flash, zoom, color change)
- Progress indicators (Question 1 of 5, etc)
- Themed visuals matching the question topic
- Clean dark or gradient backgrounds with bright accents

NARRATION RULES:
- Energetic quiz host delivery: "Question number 3... and this one's TRICKY"
- Build suspense before each answer: "The answer is... [pause]"
- Mix easy and hard questions to keep confidence fluctuating
- Include a "most people get this wrong" moment
- Real facts that surprise people
- End with the hardest question and most shocking answer

CAPTION STYLE:
- text_overlay: The question number, answer reveals, score-keeping ("Q3", "WRONG!", "CORRECT!", "ONLY 2% KNOW")

STRUCTURE (10-12 scenes, 45-60 seconds):
1. HOOK: "Only 1 in 100 people get all 5 right" or similar
2-3. Q1: Easy question + dramatic reveal
4-5. Q2: Medium question + reveal
6-7. Q3: Tricky question + shocking answer
8-9. Q4: Hard question + reveal with fun fact
10-11. Q5: Nearly impossible question + mind-blowing answer
12. CONCLUSION: "How many did YOU get right? Comment below!"

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Generate 10-12 scenes.""",

    "argument": """You are a viral scriptwriter for Argument/Debate Conversation content. These shorts feature two opposing viewpoints arguing back and forth, getting increasingly heated. The viewer picks a side. Extremely engaging because people love watching debates.

VISUAL STYLE:
- Two distinct characters or text bubbles representing each side
- Split or alternating frames showing each speaker
- Visual style matches the debate topic (professional setting for career debates, casual for lifestyle)
- Color-coded sides (blue vs red, warm vs cool)
- Expressive character poses or text message-style conversation bubbles
- Escalating visual intensity as the argument heats up

NARRATION RULES:
- Two distinct voices/tones alternating (confident vs defensive, calm vs heated)
- Start civil, escalate to passionate
- Each side makes genuinely good points
- Include specific facts and examples, not just opinions
- The "winning" argument should surprise the viewer
- End without a clear winner to drive comments: "Who's right? Comment below"
- Use realistic conversational language, interruptions, "wait wait wait..."

CAPTION STYLE:
- text_overlay: Side labels, reaction words ("SIDE A", "GOOD POINT", "BUT ACTUALLY...", "DESTROYED")

STRUCTURE (10-12 scenes, 45-60 seconds):
1. HOOK: The controversial question that starts the debate
2-3. Side A opens with a strong argument
4-5. Side B fires back with counter-evidence
6-7. Side A escalates, brings new facts
8-9. Side B delivers a surprising rebuttal
10-11. Both sides make their final case
12. OPEN ENDING: "Who won? Comment below"

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Generate 10-12 scenes.""",

    "wouldyourather": """You are a viral scriptwriter for "Would You Rather" content. These shorts present increasingly difficult dilemmas that viewers mentally debate. Extremely high engagement because EVERYONE has an opinion and NEEDS to comment their choice.

VISUAL STYLE:
- Split screen or alternating panels showing each option
- Bold, colorful visuals that make each choice look appealing (or terrifying)
- Dramatic reveal of statistics: "87% of people chose..."
- Clean typography with large "A" or "B" labels
- Visual representation of each scenario (photorealistic, dramatic)
- Escalating visual intensity as dilemmas get harder

NARRATION RULES:
- Start easy, get progressively harder/more impossible
- Each dilemma should be genuinely difficult -- no obvious answers
- Include the twist or hidden catch in each option
- React to each choice: "But here's what you didn't consider..."
- End with the hardest possible dilemma
- 5-6 dilemmas total, escalating difficulty

CAPTION STYLE:
- text_overlay: "OPTION A", "OPTION B", percentages, "IMPOSSIBLE", "87% CHOSE..."

STRUCTURE (10-12 scenes, 45-60 seconds):
1. HOOK: "Would you rather..." with an immediately grabbing dilemma
2-3. DILEMMA 1: Easy but fun, show both options
4-5. DILEMMA 2: Getting harder, reveal the catch
6-7. DILEMMA 3: Now it's personal
8-9. DILEMMA 4: No good answer
10-11. DILEMMA 5: The impossible one
12. CTA: "Which did you pick? Comment!"

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Generate 10-12 scenes.""",

    "scary": """You are an elite viral scriptwriter for Scary Story / True Crime content. These shorts tell bone-chilling stories with maximum suspense. Think "Mr. Nightmare" meets true crime documentary in 50-60 seconds. The goal is to make viewers physically uncomfortable with tension.

VISUAL STYLE:
- Dark, atmospheric cinematography. Think David Fincher's color palette.
- Desaturated blues, greens, sickly yellows. Nothing looks warm or safe.
- Environments: abandoned buildings, dark hallways, foggy forests, empty rooms at night
- Shadows dominate 60%+ of every frame. Things lurking in darkness.
- Found-footage quality for "real" moments, cinematic for dramatic beats
- Subtle horror: doors slightly ajar, figures in background, things that are "wrong"
- NO jump scares in visuals -- build dread through composition

NARRATION RULES:
- Hushed, intimate narrator voice. Like someone telling a story around a campfire.
- Start with "This actually happened" or establish it's real/based on real events
- Build tension slowly, layer details that seem innocent but become terrifying
- Use time stamps: "At 3:47 AM..." for credibility
- End with an unresolved mystery or chilling final detail
- NEVER resolve everything -- leave the viewer unsettled

CAPTION STYLE:
- text_overlay: Timestamps, locations, short chilling phrases ("3:47 AM", "NO ONE WAS HOME", "THE DOOR WAS LOCKED", "THEY NEVER FOUND...")

STRUCTURE (8-10 scenes, 50-65 seconds):
1. HOOK: "What happened at [location] on [date] still can't be explained"
2-3. SETUP: Establish the normal situation, subtle wrongness
4-6. ESCALATION: Things get progressively more disturbing
7-8. CLIMAX: The most terrifying revelation
9-10. AFTERMATH: The chilling unresolved ending

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Generate 8-10 scenes.""",

    "motivation": """You are an elite viral scriptwriter for Motivation / Inspirational content. These shorts deliver powerful life advice with cinematic visuals that make people screenshot and share. Think Gary Vee intensity meets David Goggins discipline meets cinematic production value.

VISUAL STYLE:
- Cinematic wide shots of epic environments: mountain peaks, city skylines at golden hour, ocean storms, empty roads
- Silhouettes of a lone figure against dramatic backdrops
- Sunrise/sunset golden hour lighting in every scene
- Dramatic weather: rain, fog, snow, lightning -- nature as metaphor
- Slow-motion texture shots: rain hitting ground, fists clenching, feet hitting pavement
- Color grading: warm golds and deep blues. Aspirational and powerful.

NARRATION RULES:
- Deep, authoritative, gravelly voice. Quiet intensity.
- Short. Powerful. Every sentence hits like a punch.
- NO cliches: no "hustle", no "grind", no "rise and shine"
- Use specific stories or examples, not generic advice
- Contrast: "Everyone wants the result. Nobody wants the 4 AM alarm."
- Build to a single powerful conclusion that reframes everything
- Make it feel personal, like advice from a mentor

CAPTION STYLE:
- text_overlay: The most powerful phrase from each narration ("4 AM", "NO EXCUSES", "THE REAL PRICE", "YOUR MOVE")

STRUCTURE (8-10 scenes, 45-60 seconds):
1. HOOK: Controversial truth that challenges the viewer
2-3. THE PROBLEM: What most people get wrong
4-6. THE TRUTH: Hard-hitting reality with specific examples
7-8. THE SHIFT: Reframe that changes perspective
9-10. THE CHARGE: Powerful call to action, leave them fired up

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Generate 8-10 scenes.""",

    "whatif": """You are a viral scriptwriter for "What If" Scenario content. These shorts explore mind-bending hypothetical scenarios with real science and dramatic visuals. "What if the Sun disappeared for 24 hours?" "What if humans could fly?" The curiosity gap is irresistible.

VISUAL STYLE:
- Photorealistic CGI depicting the hypothetical scenario playing out
- Start with normal reality, then visually transform as the "what if" takes effect
- Scale and spectacle: show the MASSIVE consequences (cities flooding, sky changing color, etc)
- Scientific visualization: show physics, biology, or chemistry in action
- Before/after contrast in each scene
- Epic wide shots showing global-scale effects
- Color shifts to indicate the change from normal to hypothetical

NARRATION RULES:
- Curious, slightly awestruck narrator tone
- Ground every claim in real science: "According to NASA..." or "Physics tells us..."
- Escalate consequences: minute 1, hour 1, day 1, year 1, etc
- Each scene reveals a more shocking consequence than the last
- End with the most mind-blowing implication
- Make viewers feel smarter for watching

CAPTION STYLE:
- text_overlay: Time stamps and shocking facts ("HOUR 1", "327°F", "EXTINCT IN 8 MINUTES", "NO RETURN")

STRUCTURE (8-10 scenes, 50-65 seconds):
1. HOOK: "What if [scenario]? Here's what would actually happen."
2-3. IMMEDIATE EFFECTS: First seconds/minutes
4-5. SHORT TERM: Hours to days, things get serious
6-7. MEDIUM TERM: Weeks to months, cascading consequences
8-9. LONG TERM: Years, permanent changes
10. MIND-BLOW: The one consequence nobody expects

Output valid JSON with title, scenes (scene_num, duration_sec, narration, visual_description, text_overlay), description, tags. Generate 8-10 scenes.""",
}


async def generate_script(template: str, topic: str, extra_instructions: str = "") -> dict:
    system_prompt = TEMPLATE_SYSTEM_PROMPTS.get(template, TEMPLATE_SYSTEM_PROMPTS["random"])
    topic_text = str(topic or "").strip()
    script_to_short_mode = "SCRIPT-TO-SHORT MODE" in str(extra_instructions or "")
    comparison_topic = bool(re.search(r"\b(vs\.?|versus)\b", topic_text, re.IGNORECASE))
    if template in {"skeleton", "story", "motivation"}:
        system_prompt += (
            "\n\nSUBJECT DIVERSITY + TEMPLATE COVERAGE RULES (MUST FOLLOW): "
            "Avoid forcing one unchanged main character in every scene unless the topic is explicitly about one person. "
            "Distribute scene focus across script-relevant subjects, locations, and groups while preserving continuity where the script repeats entities. "
            "Keep outputs practical and balanced for Skeleton AI, AI Stories, and Motivation templates."
        )
    system_prompt += (
        "\n\nTOPIC LOCK (MUST FOLLOW): Stay tightly anchored to the user's exact topic or source script. "
        "Do not drift into adjacent themes, generic filler, or unrelated examples. "
        "Every scene must clearly visualize a concrete beat from the provided topic/script so the resulting prompts are directly renderable."
    )
    if comparison_topic:
        system_prompt += (
            "\n\nCOMPARISON LOCK (MUST FOLLOW): The topic is a direct comparison. "
            "Keep both sides of the comparison visible throughout the structure, escalate the contrast scene by scene, "
            "and make the payoff or tradeoff explicit instead of drifting into a generic monologue."
        )
    if template == "skeleton":
        system_prompt += (
            "\n\nSKELETON OUTFIT RULE: The canonical skeleton identity stays the same across scenes. "
            "Default to no clothing, but if the user's topic or source script explicitly requests a specific outfit or uniform for a scene, preserve that outfit while keeping the same skull, eyes, bone finish, and translucent body silhouette."
        )
    if extra_instructions:
        system_prompt += extra_instructions
    story_script_to_short_mode = (
        template == "story"
        and "SCRIPT-TO-SHORT MODE" in str(extra_instructions or "")
    )

    def _split_script_fragment_once(fragment: str) -> tuple[str, str] | None:
        source = re.sub(r"\s+", " ", str(fragment or "").strip())
        if len(source) < 70:
            return None
        split_patterns = [
            r"(?<=[,;:])\s+",
            r"\s+(?=(?:when|while|because|after|before|then|but|so)\b)",
            r"\s+-\s+",
        ]
        for pattern in split_patterns:
            parts = [part.strip(" ,;:-") for part in re.split(pattern, source, maxsplit=1) if part.strip(" ,;:-")]
            if len(parts) == 2 and len(parts[0]) >= 24 and len(parts[1]) >= 24:
                return (parts[0], parts[1])
        midpoint = len(source) // 2
        split_at = source.rfind(" ", 0, midpoint + 1)
        if split_at <= 20:
            split_at = source.find(" ", midpoint)
        if split_at <= 20 or split_at >= len(source) - 20:
            return None
        left = source[:split_at].strip(" ,;:-")
        right = source[split_at + 1 :].strip(" ,;:-")
        if len(left) < 24 or len(right) < 24:
            return None
        return (left, right)

    def _split_script_into_fallback_beats(source_text: str, min_count: int, max_count: int) -> list[str]:
        text = re.sub(r"\s+", " ", str(source_text or "").strip())
        if not text:
            return []
        base_parts = [part.strip(" ,;:-") for part in re.split(r"(?<=[.!?])\s+", text) if part.strip(" ,;:-")]
        beats = list(base_parts or [text])
        while len(beats) < min_count:
            longest_index = max(range(len(beats)), key=lambda idx: len(beats[idx]))
            split_pair = _split_script_fragment_once(beats[longest_index])
            if not split_pair:
                break
            beats[longest_index : longest_index + 1] = [split_pair[0], split_pair[1]]
        if len(beats) <= max_count:
            return beats
        target_count = max_count
        chunk_size = max(1, (len(beats) + target_count - 1) // target_count)
        grouped: list[str] = []
        for start in range(0, len(beats), chunk_size):
            grouped.append(" ".join(beats[start : start + chunk_size]).strip())
        return grouped[:max_count]

    def _fallback_scene_overlay(fragment: str) -> str:
        stop_words = {
            "the", "and", "that", "with", "from", "your", "this", "into", "when",
            "then", "they", "them", "their", "have", "will", "over", "because",
            "under", "after", "before", "while", "where", "about", "called",
        }
        words = re.findall(r"[A-Za-z0-9']+", str(fragment or ""))
        picks: list[str] = []
        for raw in words:
            token = raw.strip().lower()
            if len(token) < 3 or token in stop_words:
                continue
            picks.append(token.upper())
            if len(picks) >= 2:
                break
        return " ".join(picks) if picks else "NEXT BEAT"

    def _fallback_visual_description(scene_text: str) -> str:
        beat = re.sub(r"\s+", " ", str(scene_text or "").strip().rstrip("."))
        if template == "story":
            if _story_scene_prefers_explainer_visuals(beat):
                return (
                    f"Cinematic photoreal explainer scene illustrating: {beat}. "
                    "Readable anatomy or mechanism detail, vertical short-video framing, dramatic but clear lighting, and concept-first visual storytelling."
                )
            return (
                f"Cinematic photoreal scene illustrating: {beat}. "
                "Dark readable grading, grounded realism, vertical short-video framing, and strong emotional clarity."
            )
        if template == "motivation":
            return (
                f"Cinematic motivational scene illustrating: {beat}. "
                "Premium ad-style lighting, realistic human subject, vertical short-video framing, and decisive body language."
            )
        return (
            f"Photoreal cinematic scene illustrating: {beat}. "
            "Vertical short-video framing, clean subject focus, and premium lighting."
        )

    def _build_script_to_short_fallback() -> dict:
        if template == "story":
            min_scenes, max_scenes = 10, 12
        elif template == "motivation":
            min_scenes, max_scenes = 8, 10
        else:
            min_scenes, max_scenes = 8, 10
        beats = _split_script_into_fallback_beats(topic_text, min_count=min_scenes, max_count=max_scenes)
        if not beats:
            beats = [topic_text or "Short-form script beat"]
        scene_count = max(1, len(beats))
        base_duration = 55.0 if template == "story" else 48.0
        scene_duration = max(3.5, min(6.5, round(base_duration / scene_count, 2)))
        scenes: list[dict] = []
        for idx, beat in enumerate(beats, start=1):
            narration = beat.strip()
            scenes.append(
                {
                    "scene_num": idx,
                    "duration_sec": scene_duration,
                    "narration": narration,
                    "visual_description": _fallback_visual_description(narration),
                    "text_overlay": _fallback_scene_overlay(narration),
                }
            )
        title_words = re.findall(r"[A-Za-z0-9']+", topic_text)[:8]
        title = " ".join(title_words).strip() or ("AI Story" if template == "story" else "Motivation Short")
        return {
            "title": title,
            "scenes": scenes,
            "description": title,
            "tags": [template, "shorts", "nyptid"],
        }

    async def _call_script_gen(prompt_text: str, temp: float = 0.8) -> dict:
        user_prompt = (
            "Adapt this exact source script into an editable short-form scene plan. "
            "Preserve chronology beat-by-beat from the opening line to the closing payoff. "
            "Do not invent a new premise, do not skip late beats, do not merge major turns into vague filler, and do not replace the core story. "
            "Every scene must correspond to consecutive lines or ideas from the source script so the user can see the script reflected directly in the generated prompts.\n\n"
            f"SOURCE SCRIPT:\n{topic_text}"
            if script_to_short_mode
            else "Create a viral short that stays tightly anchored to this exact topic.\n\n"
                 f"TOPIC:\n{topic_text}"
        )
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=60) as client:
                    resp = await client.post(
                        "https://api.x.ai/v1/chat/completions",
                        headers={
                            "Authorization": f"Bearer {XAI_API_KEY}",
                            "Content-Type": "application/json",
                        },
                        json={
                            "model": "grok-3-mini-fast",
                            "messages": [
                                {"role": "system", "content": prompt_text},
                                {"role": "user", "content": user_prompt},
                            ],
                            "temperature": temp,
                        },
                    )
                if resp.status_code in {429, 500, 502, 503, 504} and attempt < 2:
                    wait_seconds = (attempt + 1) * 2
                    log.warning(
                        f"Script generation upstream returned {resp.status_code}; retrying in {wait_seconds}s "
                        f"(attempt {attempt + 1}/3, template={template}, script_to_short={script_to_short_mode})"
                    )
                    await asyncio.sleep(wait_seconds)
                    continue
                resp.raise_for_status()
                content = resp.json()["choices"][0]["message"]["content"]
                start = content.find("{")
                end = content.rfind("}") + 1
                if start == -1 or end == 0:
                    raise ValueError("No JSON found in Grok response")
                return json.loads(content[start:end])
            except Exception as exc:
                last_error = exc
                if attempt < 2:
                    await asyncio.sleep(attempt + 1)
                    continue
        raise last_error if last_error is not None else RuntimeError("Script generation failed")

    def _score_story_script_quality(data: dict) -> tuple[int, list[str]]:
        scenes = data.get("scenes", [])
        if not isinstance(scenes, list):
            return (0, ["invalid_scenes"])
        min_scenes = 12 if story_script_to_short_mode else 10
        max_scenes = 15 if story_script_to_short_mode else 13
        score = 100
        notes = []
        if len(scenes) < min_scenes:
            score -= 30
            notes.append("too_few_scenes")
        if len(scenes) > max_scenes:
            score -= 10
            notes.append("too_many_scenes")

        continuity_hits = 0
        emotional_hits = 0
        motion_hits = 0
        for s in scenes:
            vis = str((s or {}).get("visual_description", "") or "").lower()
            nar = str((s or {}).get("narration", "") or "").lower()
            if re.search(r"\b(continuity|same setting|same location|recurring|timeline|same event|same era)\b", vis):
                continuity_hits += 1
            if re.search(r"\b(loss|hope|fear|grief|love|regret|choice|sacrifice|tension|danger|resolve)\b", nar):
                emotional_hits += 1
            if re.search(r"\b(camera|dolly|push|tracking|motion|continuity|moves|moving)\b", vis):
                motion_hits += 1

        n = max(len(scenes), 1)
        continuity_threshold = max(2, n // 4)
        if story_script_to_short_mode:
            continuity_threshold = max(1, n // 6)
        if continuity_hits < continuity_threshold:
            score -= 8
            notes.append("weak_visual_continuity")
        if emotional_hits < max(3, n // 3):
            score -= 14
            notes.append("weak_emotional_arc")
        if motion_hits < max(3, n // 4):
            score -= 10
            notes.append("weak_camera_motion_language")
        return (max(0, min(score, 100)), notes)

    try:
        first = await _call_script_gen(system_prompt, temp=0.8)
    except Exception as e:
        status_code = getattr(getattr(e, "response", None), "status_code", None)
        if script_to_short_mode and status_code in {429, 500, 502, 503, 504}:
            log.warning(
                f"Script-to-short upstream unavailable ({status_code}); using local scene-plan fallback for template={template}"
            )
            return _build_script_to_short_fallback()
        raise
    if template != "story":
        return first

    first_score, first_notes = _score_story_script_quality(first)
    if first_score >= 80:
        return first

    log.warning(f"Story script quality low ({first_score}); retrying with stricter constraints: {','.join(first_notes)}")
    retention_tuning = ""
    if STORY_RETENTION_TUNING_ENABLED:
        retention_tuning = (
            " Add explicit pattern interrupts every 2-3 scenes, keep narration punchy with short sentences, "
            "and force escalating stakes so each scene feels higher consequence than the previous one."
        )
    hardened_prompt = (
        system_prompt
        + "\n\nQUALITY OVERRIDE (MUST FOLLOW): "
        + (("Ensure 12-15 scenes with explicit emotional escalation, ") if story_script_to_short_mode else ("Ensure 10-12 scenes with explicit emotional escalation, "))
        + "maintain continuity of people/locations/timeline based on the script beat (do not force a single protagonist in every scene), "
        + "include camera/motion continuity language in each scene, and preserve beat-by-beat coverage of the full script without skipping the ending."
        + retention_tuning
    )
    second = await _call_script_gen(hardened_prompt, temp=0.65)
    second_score, _ = _score_story_script_quality(second)
    best = second if second_score >= first_score else first
    best_score = max(first_score, second_score)
    if story_script_to_short_mode and best_score < 80:
        ultra_hardened = (
            hardened_prompt
            + "\n\nFAILSAFE OVERRIDE (MUST FOLLOW): "
            + "Do NOT truncate. Complete all script beats in order. "
            + "Output exactly 12-15 scenes with no premature ending, no skipped late-script events, and no generic replacement prompts."
        )
        third = await _call_script_gen(ultra_hardened, temp=0.55)
        third_score, _ = _score_story_script_quality(third)
        if third_score >= best_score:
            best = third
            best_score = third_score
    return best


# ─── ElevenLabs TTS ───────────────────────────────────────────────────────────


async def _xai_json_completion(system_prompt: str, user_prompt: str, temperature: float = 0.7, timeout_sec: int = 90) -> dict:
    async with httpx.AsyncClient(timeout=timeout_sec) as client:
        resp = await client.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {XAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "grok-3-mini-fast",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": temperature,
            },
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
    start = content.find("{")
    end = content.rfind("}") + 1
    if start == -1 or end <= 0:
        raise ValueError("No JSON found in xAI response")
    return json.loads(content[start:end])


CATALYST_MARKETING_DOCTRINE = [
    "Be active in the Daily Marketing Channel.",
    "Analyze and Improve. Evaluate each marketing piece to understand what works and what doesn't. Think about how you could improve it.",
    "Small, daily improvements in your marketing skills can lead to significant progress over time due to compounding.",
    "Just like in boxing or other martial arts, consistent practice and real-world application are crucial for mastering marketing.",
    "Engage with the daily challenges to continuously hone your skills. Missing a day occasionally is okay, but don't make it a habit.",
    "Regardless of your field or business, understanding and practicing marketing is fundamental to success.",
    "Treat the daily marketing challenges seriously and make it a part of your routine to see substantial benefits in your marketing abilities.",
    "Mastering marketing has enabled Arno to start and scale companies and avoid manual labor by understanding how to attract clients and improve businesses.",
    "It is a long-lasting skill. Marketing has been around for millennia and will continue to be valuable in the future.",
    "Anyone can learn it. It doesn't require special skills, abilities, or connections. Pay attention, focus, and you can succeed.",
    "High ROI (Return On Investment). Direct response marketing offers the highest and most reliable return on investment, outperforming traditional investments.",
    "Learning marketing helps you see opportunities and gaps that others miss, making life easier.",
    "You don't need to be the world's best marketer; being better than most is enough to succeed.",
    "It is a fast skill to learn. With ten days of dedicated study, you can acquire valuable marketing skills.",
    "Be ready for a significant change as you learn and apply these marketing skills.",
]


def _clip_text(value: str, max_chars: int = 320) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 1)].rstrip() + "…"


def _normalize_external_source_url(raw_value: str) -> str:
    raw = str(raw_value or "").strip()
    if not raw:
        return ""
    if raw.startswith("//"):
        raw = "https:" + raw
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", raw):
        raw = "https://" + raw.lstrip("/")
    try:
        parsed = urlparse(raw)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return ""
        return parsed.geturl()
    except Exception:
        return ""


def _parse_vtt_text(raw_vtt: str) -> str:
    lines = []
    for raw_line in str(raw_vtt or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("WEBVTT") or "-->" in line:
            continue
        if re.fullmatch(r"[0-9:\.\- ]+", line):
            continue
        line = re.sub(r"<[^>]+>", " ", line)
        line = re.sub(r"\{[^}]+\}", " ", line)
        line = re.sub(r"\s+", " ", line).strip()
        if line:
            lines.append(line)
    deduped: list[str] = []
    prev = ""
    for line in lines:
        if line != prev:
            deduped.append(line)
            prev = line
    return _clip_text(" ".join(deduped), 2200)


def _pick_subtitle_candidate(info: dict, language: str = "en") -> tuple[str, str]:
    preferred = []
    for lang in [str(language or "").strip().lower(), "en", "en-us", "en-gb"]:
        if lang and lang not in preferred:
            preferred.append(lang)
    pools = [
        info.get("subtitles") if isinstance(info, dict) else {},
        info.get("automatic_captions") if isinstance(info, dict) else {},
    ]
    for pool in pools:
        if not isinstance(pool, dict):
            continue
        for lang in preferred:
            variants = [lang]
            if "-" in lang:
                variants.append(lang.split("-", 1)[0])
            for variant in variants:
                entries = pool.get(variant)
                if not isinstance(entries, list):
                    continue
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    ext = str(entry.get("ext", "") or "").strip().lower()
                    url = str(entry.get("url", "") or "").strip()
                    if url and ext == "vtt":
                        return url, ext
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    url = str(entry.get("url", "") or "").strip()
                    ext = str(entry.get("ext", "") or "").strip().lower()
                    if url:
                        return url, ext or "unknown"
    return "", ""


def _yt_dlp_extract_info_blocking(source_url: str) -> dict:
    if yt_dlp is None:
        raise RuntimeError("yt-dlp is not installed")
    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "extract_flat": False,
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(source_url, download=False) or {}


async def _fetch_source_video_bundle(source_url: str, language: str = "en") -> dict:
    normalized_url = _normalize_external_source_url(source_url)
    if not normalized_url:
        return {}
    if yt_dlp is None:
        return {
            "source_url": normalized_url,
            "error": "yt_dlp_unavailable",
            "public_summary": "Source URL provided, but yt-dlp is not available on this deployment.",
        }
    try:
        info = await asyncio.to_thread(_yt_dlp_extract_info_blocking, normalized_url)
    except Exception as e:
        return {
            "source_url": normalized_url,
            "error": str(e),
            "public_summary": f"Source URL provided but metadata extraction failed: {_clip_text(str(e), 180)}",
        }

    chapters = []
    for raw in list(info.get("chapters") or [])[:12]:
        if not isinstance(raw, dict):
            continue
        chapters.append({
            "title": str(raw.get("title", "") or "").strip(),
            "start_sec": float(raw.get("start_time", 0.0) or 0.0),
            "end_sec": float(raw.get("end_time", 0.0) or 0.0),
        })

    transcript_excerpt = ""
    subtitle_url, subtitle_ext = _pick_subtitle_candidate(info, language=language)
    if subtitle_url and subtitle_ext == "vtt":
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                resp = await client.get(subtitle_url)
                if resp.status_code == 200:
                    transcript_excerpt = _parse_vtt_text(resp.text)
        except Exception:
            transcript_excerpt = ""

    title = str(info.get("title", "") or "").strip()
    description = str(info.get("description", "") or "").strip()
    channel = str(info.get("channel", "") or info.get("uploader", "") or "").strip()
    duration_sec = int(float(info.get("duration", 0) or 0))
    view_count = int(float(info.get("view_count", 0) or 0))
    like_count = int(float(info.get("like_count", 0) or 0))
    comment_count = int(float(info.get("comment_count", 0) or 0))
    tags = [str(tag).strip() for tag in list(info.get("tags") or []) if str(tag).strip()][:20]
    categories = [str(cat).strip() for cat in list(info.get("categories") or []) if str(cat).strip()][:8]
    upload_date = str(info.get("upload_date", "") or "").strip()
    public_summary_parts = [
        f"Title: {title}" if title else "",
        f"Channel: {channel}" if channel else "",
        f"Duration: {duration_sec}s" if duration_sec > 0 else "",
        f"Views: {view_count}" if view_count > 0 else "",
        f"Likes: {like_count}" if like_count > 0 else "",
        f"Comments: {comment_count}" if comment_count > 0 else "",
        f"Tags: {', '.join(tags[:8])}" if tags else "",
        f"Top chapters: {', '.join(ch['title'] for ch in chapters[:4] if ch.get('title'))}" if chapters else "",
        f"Transcript excerpt: {transcript_excerpt}" if transcript_excerpt else "",
    ]

    return {
        "source_url": normalized_url,
        "canonical_url": str(info.get("webpage_url", "") or normalized_url),
        "platform": str(info.get("extractor_key", "") or info.get("extractor", "") or "web").strip(),
        "title": title,
        "description": description,
        "channel": channel,
        "channel_url": str(info.get("channel_url", "") or "").strip(),
        "thumbnail_url": str(info.get("thumbnail", "") or "").strip(),
        "duration_sec": duration_sec,
        "view_count": view_count,
        "like_count": like_count,
        "comment_count": comment_count,
        "upload_date": upload_date,
        "tags": tags,
        "categories": categories,
        "chapters": chapters,
        "transcript_excerpt": transcript_excerpt,
        "public_summary": " | ".join(part for part in public_summary_parts if part),
    }


def _marketing_doctrine_text(extra_notes: str = "") -> str:
    doctrine = list(CATALYST_MARKETING_DOCTRINE)
    if str(extra_notes or "").strip():
        doctrine.append(str(extra_notes).strip())
    return "\n".join(f"- {line}" for line in doctrine if str(line).strip())


async def _build_source_performance_analysis(
    source_bundle: dict,
    analytics_notes: str = "",
    topic: str = "",
    input_title: str = "",
    input_description: str = "",
    strategy_notes: str = "",
) -> dict:
    if not source_bundle and not analytics_notes:
        return {}
    system_prompt = (
        "You are a YouTube growth strategist for NYPTID Studio. "
        "Analyze a source video using public metadata plus optional operator notes. "
        "Output strict JSON with keys: what_worked, what_hurt, hook_learnings, click_drivers, "
        "dropoff_risks, improvement_moves, title_angles, thumbnail_angles, description_angles. "
        "Keep every field practical and specific for building a better follow-up video."
    )
    user_prompt = (
        f"New target topic: {topic}\n"
        f"Draft title constraint: {input_title}\n"
        f"Draft description constraint: {input_description}\n"
        f"Public source bundle: {json.dumps(source_bundle or {}, ensure_ascii=True)}\n"
        f"Private analytics/operator notes: {_clip_text(analytics_notes, 1800)}\n"
        "Use this marketing doctrine as operating context:\n"
        f"{_marketing_doctrine_text(strategy_notes)}"
    )
    try:
        return await _xai_json_completion(system_prompt, user_prompt, temperature=0.35, timeout_sec=60)
    except Exception as e:
        return {
            "what_worked": _clip_text((source_bundle or {}).get("public_summary", ""), 220),
            "what_hurt": _clip_text(analytics_notes, 220),
            "hook_learnings": [],
            "click_drivers": [],
            "dropoff_risks": [],
            "improvement_moves": [_clip_text(str(e), 220)],
            "title_angles": [],
            "thumbnail_angles": [],
            "description_angles": [],
        }


def _render_source_context(source_bundle: dict, source_analysis: dict, analytics_notes: str = "") -> str:
    parts: list[str] = []
    if source_bundle:
        parts.append("Public source analysis:")
        parts.append(str(source_bundle.get("public_summary", "") or "").strip())
    if source_analysis:
        worked = source_analysis.get("what_worked")
        hurt = source_analysis.get("what_hurt")
        if worked:
            parts.append("What worked: " + _clip_text(str(worked), 240))
        if hurt:
            parts.append("What hurt: " + _clip_text(str(hurt), 240))
        moves = source_analysis.get("improvement_moves") or []
        if isinstance(moves, list) and moves:
            parts.append("Improvement moves: " + "; ".join(_clip_text(str(m), 120) for m in moves[:5] if str(m).strip()))
    if str(analytics_notes or "").strip():
        parts.append("Private analytics notes: " + _clip_text(analytics_notes, 320))
    return "\n".join(part for part in parts if part)


def _normalize_longform_scenes_for_render(scenes: list) -> list:
    normalized = []
    for idx, raw_scene in enumerate(scenes or []):
        scene = dict(raw_scene or {})
        narration = str(scene.get("narration", "") or "").strip()
        visual_description = str(scene.get("visual_description", "") or "").strip()
        if not narration:
            narration = f"Chapter beat {idx + 1}."
        if not visual_description:
            visual_description = narration or f"Scene {idx + 1} visual"
        # Long-form chapter previews and final render are hard-locked to 5s/scene.
        duration = 5.0
        scene["narration"] = narration
        scene["visual_description"] = visual_description
        scene["scene_num"] = int(scene.get("scene_num", idx + 1) or (idx + 1))
        scene["duration_sec"] = round(duration, 2)
        normalized.append(scene)
    return normalized


def _scale_scene_durations_to_target(scenes: list, target_sec: float) -> list:
    # Hard lock pacing to 5s per scene for deterministic sync.
    scaled = []
    for idx, raw in enumerate(scenes or []):
        scene = dict(raw or {})
        scene["scene_num"] = int(scene.get("scene_num", idx + 1) or (idx + 1))
        scene["duration_sec"] = 5.0
        scaled.append(scene)
    return scaled


def _longform_chapter_retention_score(chapter: dict) -> int:
    scenes = chapter.get("scenes", []) if isinstance(chapter, dict) else []
    if not isinstance(scenes, list) or not scenes:
        return 0
    score = 100
    if len(scenes) < 6:
        score -= 18
    if len(scenes) > 16:
        score -= 8
    narrations = " ".join(str((s or {}).get("narration", "") or "") for s in scenes).lower()
    visuals = " ".join(str((s or {}).get("visual_description", "") or "") for s in scenes).lower()
    if not re.search(r"\b(hook|twist|reveal|stakes|conflict|turn)\b", narrations):
        score -= 20
    if not re.search(r"\b(camera|motion|moves|tracking|dolly|pan)\b", visuals):
        score -= 14
    if not re.search(r"\b(why|because|therefore|but|however)\b", narrations):
        score -= 10
    return max(0, min(100, score))


def _remove_nyptid_mentions(text: str) -> str:
    clean = str(text or "").strip()
    if not clean:
        return ""
    sentences = re.split(r"(?<=[.!?])\s+", clean)
    kept = [s.strip() for s in sentences if s.strip() and "nyptid studio" not in s.lower()]
    if kept:
        return " ".join(kept).strip()
    return ""


def _longform_apply_brand_slot(chapter: dict, brand_slot: str, input_title: str) -> dict:
    out = dict(chapter or {})
    scenes = list(out.get("scenes") or [])
    if not scenes:
        out["scenes"] = scenes
        return out
    normalized_slot = str(brand_slot or "").strip().lower()

    # Strip any accidental early NYPTID mentions from generated copy.
    for i, raw_scene in enumerate(scenes):
        scene = dict(raw_scene or {})
        cleaned = _remove_nyptid_mentions(str(scene.get("narration", "") or ""))
        scene["narration"] = cleaned if cleaned else f"Chapter beat {i + 1}."
        scenes[i] = scene

    if normalized_slot != "outro":
        out["scenes"] = scenes
        return out

    brand_line = "Built with NYPTID Studio. Create your next full video in NYPTID Studio."
    target = dict(scenes[-1] or {})
    existing = str(target.get("narration", "") or "").strip()
    if "nyptid studio" not in existing.lower():
        target["narration"] = (existing + " " + brand_line).strip() if existing else brand_line
    scenes[-1] = target
    out["scenes"] = scenes
    return out


async def _generate_longform_chapter(
    template: str,
    topic: str,
    input_title: str,
    input_description: str,
    format_preset: str,
    chapter_index: int,
    chapter_count: int,
    chapter_target_sec: float,
    language: str = "en",
    brand_slot: str = "",
    fix_note: str = "",
    source_context: str = "",
    strategy_notes: str = "",
) -> dict:
    tone = _longform_detect_tone(template, topic, input_title, input_description)
    lang_name = SUPPORTED_LANGUAGES.get(language, {}).get("name", "English")
    scene_goal = max(6, min(24, int(round(float(chapter_target_sec) / 5.0))))
    system_prompt = (
        "You are writing one chapter of a long-form YouTube video package for NYPTID Studio. "
        "Output strict JSON with keys: chapter_title, chapter_summary, scenes, chapter_description. "
        f"Generate {scene_goal}-{scene_goal + 2} scenes. Each scene must include scene_num, duration_sec, narration, visual_description, text_overlay. "
        "narration must be concise and engaging; visual_description must be render-ready and specific. "
        "Every scene duration_sec must be exactly 5. Narration-first rule: each visual_description must directly visualize that same scene's narration beat. "
        "Optimize for retention, clean structure, and YouTube packaging strength instead of generic filler."
    )
    if template == "skeleton":
        system_prompt += (
            " Skeleton identity is hard-locked: same skull geometry, same eye size/spacing, same bone finish, "
            "same translucent body silhouette in every scene; no clothing/costume changes."
        )
    if tone == "horror":
        system_prompt += (
            " Tone lock: psychological horror / eerie mystery. Every visual_description must explicitly include unsettling setting cues "
            "(e.g., fog, abandoned roads, low-key lighting, looming shadows, empty interiors, or night exteriors). "
            "Narration and visual_description must describe the same beat. Avoid generic upbeat intro language and avoid graphic gore."
        )
    if language != "en":
        system_prompt += f" Narration must be in {lang_name}. visual_description must remain in English."

    user_prompt = (
        f"Topic: {topic}\n"
        f"Video title constraint: {input_title}\n"
        f"Video description constraint: {input_description}\n"
        f"Format preset: {format_preset}\n"
        f"Chapter {chapter_index + 1} of {chapter_count}, target chapter duration: {int(chapter_target_sec)} seconds.\n"
        "The chapter must push the story forward with strong pacing and retention.\n"
    )
    if source_context:
        user_prompt += f"Source-video context to learn from:\n{source_context}\n"
    if strategy_notes:
        user_prompt += f"Strategy doctrine:\n{strategy_notes}\n"
    if str(brand_slot or "").strip().lower() == "outro":
        user_prompt += (
            "Include one natural NYPTID Studio mention only in the final scene narration of this chapter. "
            "Do not place branding in earlier scenes.\n"
        )
    else:
        user_prompt += "Do not mention NYPTID Studio in this chapter.\n"
    if tone == "horror":
        user_prompt += (
            "Mood directive: this chapter should feel like a chilling horror mystery with escalating dread, "
            "uneasy silence, and ominous visual beats.\n"
        )
    if fix_note:
        user_prompt += f"Fix note from owner review: {fix_note}\n"

    chapter_data = await _xai_json_completion(system_prompt, user_prompt, temperature=0.65, timeout_sec=90)
    raw_scenes = chapter_data.get("scenes", [])
    scenes = _normalize_longform_scenes_for_render(raw_scenes)
    scenes = _scale_scene_durations_to_target(scenes, chapter_target_sec)
    scenes = _longform_enforce_tone_on_scenes(scenes, tone=tone, template=template)
    chapter_total_sec = round(float(len(scenes) * 5.0), 2)
    out = {
        "index": int(chapter_index),
        "title": str(chapter_data.get("chapter_title", f"Chapter {chapter_index + 1}") or f"Chapter {chapter_index + 1}"),
        "summary": str(chapter_data.get("chapter_summary", "") or ""),
        "tone": str(tone),
        "target_sec": chapter_total_sec,
        "scenes": scenes,
        "status": "pending_review",
        "retry_count": 0,
        "brand_slot": brand_slot,
        "viral_score": 0,
    }
    out = _longform_apply_brand_slot(out, brand_slot, input_title=input_title)
    out["viral_score"] = _longform_chapter_retention_score(out)
    return out


async def generate_voiceover(text: str, output_path: str, template: str = "random",
                              override_voice_id: str = "", language: str = "en",
                              override_speed: float | None = None) -> dict:
    """Generate voiceover with word-level timestamps for caption sync.
    Returns {"audio_path": str, "word_timings": list[dict]} where each timing is
    {"word": str, "start": float, "end": float}.
    """
    vs = TEMPLATE_VOICE_SETTINGS.get(template, {})
    voice_id = override_voice_id if override_voice_id else vs.get("voice_id", "pNInz6obpgDQGcFmaJgB")
    speed = float(override_speed) if override_speed is not None else float(vs.get("speed", 1.0))
    lang_cfg = SUPPORTED_LANGUAGES.get(language, SUPPORTED_LANGUAGES["en"])
    tts_model = lang_cfg["model"]
    url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}/with-timestamps"

    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            url,
            headers={
                "xi-api-key": ELEVENLABS_API_KEY,
                "Content-Type": "application/json",
            },
            json={
                "text": text,
                "model_id": tts_model,
                "voice_settings": {
                    "stability": vs.get("stability", 0.5),
                    "similarity_boost": vs.get("similarity_boost", 0.75),
                    "style": vs.get("style", 0.3),
                    "speed": max(0.8, min(1.35, speed)),
                },
            },
        )
        resp.raise_for_status()
        data = resp.json()

    import base64 as b64mod
    audio_b64 = data.get("audio_base64", "")
    if audio_b64:
        audio_bytes = b64mod.b64decode(audio_b64)
        with open(output_path, "wb") as f:
            f.write(audio_bytes)
    else:
        log.warning("No audio_base64 in timestamps response, falling back to standard endpoint")
        fallback_resp = await httpx.AsyncClient(timeout=120).post(
            f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}",
            headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
            json={"text": text, "model_id": tts_model,
                  "voice_settings": {"stability": vs.get("stability", 0.5),
                                     "similarity_boost": vs.get("similarity_boost", 0.75),
                                     "style": vs.get("style", 0.3)}},
        )
        fallback_resp.raise_for_status()
        with open(output_path, "wb") as f:
            f.write(fallback_resp.content)
        return {"audio_path": output_path, "word_timings": []}

    word_timings = _extract_word_timings(text, data.get("alignment", {}))
    log.info(f"Voiceover generated with {len(word_timings)} word timings: {output_path}")
    return {"audio_path": output_path, "word_timings": word_timings}


async def generate_scene_sfx(visual_description: str, duration_sec: float,
                              output_path: str, template: str = "", scene_index: int = -1, total_scenes: int = 0) -> str:
    """Generate a sound effect for a scene using ElevenLabs Sound Effects API.
    Returns the path to the generated SFX audio file, or empty string on failure."""
    if not _sfx_enabled() or not ELEVENLABS_API_KEY:
        return ""

    style_hint = TEMPLATE_SFX_STYLES.get(template, "cinematic ambient atmosphere")
    transition_palette = [
        "smooth whoosh transition",
        "dramatic cinematic hit with sub bass",
        "quick snap transition accent",
        "soft zoom swell with airy tail",
        "blur sweep transition texture",
    ]
    if scene_index == 0:
        dynamic_layer = "strong opening hook impact, attention-grabbing stinger"
    elif total_scenes > 0 and scene_index == (total_scenes - 1):
        dynamic_layer = "final payoff impact, satisfying outro resolve"
    elif scene_index >= 0:
        dynamic_layer = transition_palette[scene_index % len(transition_palette)]
    else:
        dynamic_layer = "cinematic transition accent"
    sfx_prompt = f"{style_hint}, {dynamic_layer}, matching visual: {visual_description[:200]}"

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                "https://api.elevenlabs.io/v1/sound-generation",
                headers={
                    "xi-api-key": ELEVENLABS_API_KEY,
                    "Content-Type": "application/json",
                },
                json={
                    "text": sfx_prompt,
                    "duration_seconds": min(duration_sec, 22.0),
                    "prompt_influence": 0.58,
                },
            )
            if resp.status_code != 200:
                log.warning(f"ElevenLabs SFX failed ({resp.status_code}): {resp.text[:200]}")
                return ""
            with open(output_path, "wb") as f:
                f.write(resp.content)
            if Path(output_path).stat().st_size > 0:
                log.info(f"SFX generated: {output_path} ({Path(output_path).stat().st_size / 1024:.0f} KB)")
                return output_path
            return ""
    except Exception as e:
        log.warning(f"SFX generation failed (non-fatal): {e}")
        return ""


def _probe_audio_duration_seconds(audio_path: str) -> float:
    """Best-effort audio duration probe using ffprobe."""
    try:
        proc = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                audio_path,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return float((proc.stdout or "0").strip() or 0)
    except Exception:
        return 0.0


def _probe_video_duration_seconds(video_path: str) -> float:
    """Best-effort video duration probe using ffprobe."""
    try:
        proc = subprocess.run(
            [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                video_path,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return float((proc.stdout or "0").strip() or 0)
    except Exception:
        return 0.0


TRANSITION_STYLE_MAP = {
    "no_motion": "none",
    "none": "none",
    "dramatic": "fade",
    "cinematic": "fade",
    "smooth": "fade",
    "slide": "slideleft",
    "zoom": "circleopen",
    "snap": "pixelize",
    "blur": "hblur",
}


def _normalize_transition_style(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if raw in TRANSITION_STYLE_MAP:
        return raw
    if raw in {"off", "disabled"}:
        return "no_motion"
    return "smooth"


def _transition_duration_for_style(style: str) -> float:
    if style in {"snap"}:
        return 0.08
    if style in {"dramatic", "cinematic", "blur"}:
        return 0.12
    if style in {"slide", "zoom"}:
        return 0.18
    return 0.16


def _normalize_micro_escalation_mode(value, template: str = "") -> bool:
    if template not in {"skeleton", "story", "motivation"}:
        return False
    if value is None:
        return True
    return _bool_from_any(value, True)


def _normalize_cinematic_boost(value) -> bool:
    return _bool_from_any(value, True)


def _normalize_voice_speed(value, default: float = 1.0) -> float:
    try:
        raw = float(value)
    except Exception:
        raw = float(default)
    return max(0.8, min(1.35, raw))


def _normalize_pacing_mode(value) -> str:
    raw = str(value or "standard").strip().lower()
    if raw in {"standard", "fast", "very_fast"}:
        return raw
    if raw in {"very-fast", "veryfast"}:
        return "very_fast"
    return "standard"


def _sfx_enabled() -> bool:
    return not DISABLE_ALL_SFX


def _apply_story_pacing(scenes: list, template: str, pacing_mode: str = "standard") -> list:
    if template != "story":
        return scenes
    mode = _normalize_pacing_mode(pacing_mode)
    mult = {"standard": 1.0, "fast": 0.9, "very_fast": 0.8}.get(mode, 1.0)
    paced = []
    for s in scenes or []:
        scene = dict(s or {})
        dur = float(scene.get("duration_sec", 5) or 5)
        dur = max(2.5, min(8.0, round(dur * mult, 2)))
        scene["duration_sec"] = dur
        paced.append(scene)
    return paced


MICRO_ESCALATION_MAX_SOURCE_SCENES = 16
MICRO_ESCALATION_MAX_OUTPUT_CLIPS = 48


async def _build_micro_escalation_clips(
    source_clips: list[Path],
    source_durations: list[float],
    job_ts: str,
) -> tuple[list[Path], list[float]]:
    """Split 5s scene clips into shorter editorial beats without extra generation calls."""
    if len(source_clips) > MICRO_ESCALATION_MAX_SOURCE_SCENES:
        return list(source_clips), list(source_durations)
    micro_clips: list[Path] = []
    micro_durations: list[float] = []

    for i, clip in enumerate(source_clips):
        clip_path = str(clip)
        base_dur = source_durations[i] if i < len(source_durations) else _probe_video_duration_seconds(clip_path)
        base_dur = max(0.5, float(base_dur or 5.0))
        if base_dur < 3.2:
            micro_clips.append(clip)
            micro_durations.append(base_dur)
            continue

        # 2-3 virtual beats per source scene.
        boundaries = [0.0, round(base_dur * 0.34, 3), round(base_dur * 0.68, 3), base_dur]
        if base_dur < 4.8:
            boundaries = [0.0, round(base_dur * 0.52, 3), base_dur]

        for b in range(len(boundaries) - 1):
            if len(micro_clips) >= MICRO_ESCALATION_MAX_OUTPUT_CLIPS:
                return list(source_clips), list(source_durations)
            start = max(0.0, boundaries[b])
            seg_dur = max(0.42, boundaries[b + 1] - boundaries[b])
            out = TEMP_DIR / f"micro_{job_ts}_{i}_{b}.mp4"
            vf = "eq=contrast=1.02:saturation=1.03"
            if b == 1:
                vf = "setpts=0.92*PTS,eq=contrast=1.08:saturation=1.06"
            elif b >= 2:
                vf = "setpts=0.97*PTS,eq=contrast=1.05:saturation=1.1"
            cmd = [
                "ffmpeg", "-y",
                "-ss", f"{start:.3f}",
                "-t", f"{seg_dur:.3f}",
                "-i", clip_path,
                "-an",
                "-vf", vf,
                "-threads", "1",
                "-c:v", "libx264", "-preset", "veryfast", "-pix_fmt", "yuv420p",
                str(out),
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            await proc.communicate()
            if proc.returncode == 0 and out.exists() and out.stat().st_size > 0:
                actual = _probe_video_duration_seconds(str(out))
                micro_clips.append(out)
                micro_durations.append(max(0.35, actual if actual > 0 else seg_dur))
            else:
                # Fallback preserves reliability if edit split fails.
                micro_clips.append(clip)
                micro_durations.append(base_dur)
                break

    return micro_clips, micro_durations


def _build_atempo_filter_chain(speed: float) -> str:
    """Build an ffmpeg atempo chain for any positive speed ratio."""
    if speed <= 0:
        return "atempo=1.0"
    parts = []
    remaining = float(speed)
    while remaining > 2.0:
        parts.append("atempo=2.0")
        remaining /= 2.0
    while remaining < 0.5:
        parts.append("atempo=0.5")
        remaining /= 0.5
    parts.append(f"atempo={remaining:.6f}")
    return ",".join(parts)


async def _quintuple_check_scene_sfx(
    scenes: list,
    sfx_paths: list[str],
    template: str,
    job_id: str = "",
) -> list[str]:
    """Quintuple-check scene SFX alignment and retry mismatched clips once."""
    fixed = list(sfx_paths or [])
    while len(fixed) < len(scenes):
        fixed.append("")

    for i, scene in enumerate(scenes):
        expected = float(scene.get("duration_sec", 5) or 5)
        sfx = fixed[i] if i < len(fixed) else ""
        ok_exists = bool(sfx and Path(sfx).exists() and Path(sfx).stat().st_size > 0)
        actual = _probe_audio_duration_seconds(sfx) if ok_exists else 0.0
        ok_duration = ok_exists and abs(actual - expected) <= 1.5
        ok_order = i < len(fixed)
        ok_scene = bool(scene.get("visual_description", "").strip())
        ok_nonempty_prompt = ok_scene

        if ok_exists and ok_duration and ok_order and ok_scene and ok_nonempty_prompt:
            continue

        retry_out = str(TEMP_DIR / (f"{job_id}_sfx_retry_{i}.mp3" if job_id else f"sfx_retry_{i}_{int(time.time()*1000)}.mp3"))
        desc = scene.get("visual_description", "")
        retry = await generate_scene_sfx(desc, expected, retry_out, template=template, scene_index=i, total_scenes=len(scenes))
        retry_ok = bool(retry and Path(retry).exists() and Path(retry).stat().st_size > 0)
        retry_dur = _probe_audio_duration_seconds(retry) if retry_ok else 0.0
        if retry_ok and abs(retry_dur - expected) <= 1.5:
            fixed[i] = retry
            log.info(f"[{job_id}] SFX scene {i+1} realigned on retry ({retry_dur:.2f}s vs expected {expected:.2f}s)")
        else:
            fixed[i] = ""
            log.warning(f"[{job_id}] SFX scene {i+1} failed alignment checks; using silence pad")

    return fixed


def _extract_word_timings(original_text: str, alignment: dict) -> list:
    """Convert ElevenLabs character-level alignment into word-level timings."""
    chars = alignment.get("characters", [])
    char_starts = alignment.get("character_start_times_seconds", [])
    char_ends = alignment.get("character_end_times_seconds", [])

    if not chars or not char_starts or not char_ends:
        return []
    if len(chars) != len(char_starts) or len(chars) != len(char_ends):
        return []

    words = []
    current_word = ""
    word_start = None

    for i, ch in enumerate(chars):
        if ch in (" ", "\n", "\t"):
            if current_word:
                words.append({
                    "word": current_word,
                    "start": word_start,
                    "end": char_ends[i - 1] if i > 0 else char_starts[i],
                })
                current_word = ""
                word_start = None
        else:
            if word_start is None:
                word_start = char_starts[i]
            current_word += ch

    if current_word and word_start is not None:
        words.append({
            "word": current_word,
            "start": word_start,
            "end": char_ends[-1],
        })

    return words


def generate_ass_subtitles(word_timings: list, output_path: str, resolution: str = "720p",
                           video_width: int = 0, video_height: int = 0, template: str = "") -> str:
    """Generate an ASS subtitle file with rapid single-word captions.
    Each word appears individually, large and bold, changing rapidly with every spoken word.
    High-retention viral TikTok/Reels style -- one word at a time, rapid fire.
    Supports both portrait (shorts) and landscape (product demo) layouts.
    """
    if video_width and video_height:
        res_w = video_width
        res_h = video_height
        is_landscape = res_w > res_h
    else:
        cfg = RESOLUTION_CONFIGS.get(resolution, RESOLUTION_CONFIGS.get("720p", {}))
        res_w = int(cfg.get("output_width", 720) or 720)
        res_h = int(cfg.get("output_height", 1280) or 1280)
        is_landscape = res_w > res_h

    skeleton_pro_style = (template == "skeleton" and not is_landscape)
    is_1080 = str(resolution).startswith("1080p")

    if skeleton_pro_style:
        font_size = 84 if is_1080 else 60
        outline = 3 if is_1080 else 2
        shadow = 2
        margin_v = int(res_h * 0.14)
        spacing = 0
        scale_xy = 100
        primary = "&H00FFFFFF"
        secondary = "&H00E7F4FF"
        outline_color = "&H00303030"
        back_color = "&H70000000"
    elif is_landscape:
        font_size = max(36, int(res_h * 0.045))
        outline = 3
        shadow = 1
        margin_v = int(res_h * 0.08)
        spacing = 2
        scale_xy = 105
        primary = "&H00FFFFFF"
        secondary = "&H000000FF"
        outline_color = "&H00000000"
        back_color = "&H96000000"
    else:
        font_size = 72 if resolution == "1080p" else 52
        outline = 5 if resolution == "1080p" else 4
        shadow = 2
        margin_v = int(res_h * 0.25)
        spacing = 2
        scale_xy = 105
        primary = "&H00FFFFFF"
        secondary = "&H000000FF"
        outline_color = "&H00000000"
        back_color = "&H96000000"

    header = f"""[Script Info]
Title: NYPTID Captions
ScriptType: v4.00+
PlayResX: {res_w}
PlayResY: {res_h}
WrapStyle: 0

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Word,Noto Sans,{font_size},{primary},{secondary},{outline_color},{back_color},-1,0,0,0,{scale_xy},{scale_xy},{spacing},0,1,{outline},{shadow},2,20,20,{margin_v},1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""

    def ts_to_ass(seconds: float) -> str:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        cs = int((seconds % 1) * 100)
        return f"{h}:{m:02d}:{s:02d}.{cs:02d}"

    # Landscape demos often have faster narration pacing than shorts.
    # Use shorter per-word minimum display to keep captions visually in sync.
    MIN_DISPLAY = 0.12 if is_landscape else 0.25

    timed = []
    for wt in word_timings:
        word = wt["word"].strip()
        if not word:
            continue
        timed.append({"word": word, "start": wt["start"], "end": wt["end"]})

    events = []
    for i, wt in enumerate(timed):
        start = wt["start"]
        natural_end = wt["end"]
        next_start = timed[i + 1]["start"] if i + 1 < len(timed) else natural_end + 0.5
        end = max(natural_end, start + MIN_DISPLAY)
        # Prevent overlap with the next word so captions don't visually trail speech.
        max_end = (next_start - 0.01) if (i + 1 < len(timed)) else (natural_end + 0.5)
        end = min(end, max_end)
        # If speech is very fast, allow shorter windows rather than forcing laggy overlap.
        if end <= start:
            end = start + 0.04

        # Preserve natural casing for skeleton "editorial" look, keep uppercase for other templates.
        clean_word = wt["word"].replace("\\", "").replace("{", "").replace("}", "")
        safe_word = clean_word if skeleton_pro_style else clean_word.upper()

        if skeleton_pro_style:
            # Subtle pop/fade reads closer to hand-edited NLE captions.
            pop_in = r"{\blur0.6\fad(35,45)\fscx100\fscy100\t(0,90,\fscx104\fscy104)\t(90,170,\fscx100\fscy100)}"
        else:
            pop_in = r"{\fscx130\fscy130\t(0,60,\fscx105\fscy105)}"
        events.append(
            f"Dialogue: 0,{ts_to_ass(start)},{ts_to_ass(end)},Word,,0,0,0,,{pop_in}{safe_word}"
        )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(header)
        f.write("\n".join(events))
        f.write("\n")

    log.info(f"ASS subtitle file generated: {output_path} ({len(events)} single-word captions)")
    return output_path


# ─── ComfyUI Image Generation with Upscaling ─────────────────────────────────



async def generate_sfx_for_scene(scene_desc: str, template: str, duration_sec: float, output_path: str) -> str:
    """Generate a sound effect for a scene using ElevenLabs Sound Effects API."""
    if not _sfx_enabled() or not ELEVENLABS_API_KEY:
        return ""
    base_sfx = TEMPLATE_SFX_PROMPTS.get(template, "Cinematic dramatic transition impact hit with bass")
    sfx_prompt = f"{base_sfx}. Scene: {scene_desc[:150]}"
    sfx_duration = min(max(duration_sec, 0.5), 22.0)

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                "https://api.elevenlabs.io/v1/sound-generation",
                headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
                json={
                    "text": sfx_prompt,
                    "duration_seconds": sfx_duration,
                    "prompt_influence": 0.4,
                },
            )
            if resp.status_code not in (200, 201):
                log.warning(f"SFX generation failed ({resp.status_code}): {resp.text[:200]}")
                return ""
            with open(output_path, "wb") as f:
                f.write(resp.content)
        log.info(f"SFX generated: {output_path} ({Path(output_path).stat().st_size / 1024:.0f} KB)")
        return output_path
    except Exception as e:
        log.warning(f"SFX generation error (non-fatal): {e}")
        return ""


_active_comfyui_url = str(COMFYUI_URL or "").strip().rstrip("/")


def _comfyui_candidate_urls() -> list[str]:
    candidates: list[str] = []
    for raw in (
        _active_comfyui_url,
        COMFYUI_URL,
        "http://127.0.0.1:8188",
    ):
        base = str(raw or "").strip().rstrip("/")
        if not base:
            continue
        if base not in candidates:
            candidates.append(base)
    return candidates


_COMFYUI_OBJECT_INFO_TTL_SEC = 120
_comfyui_object_info_cache: dict[str, dict] = {}
_image_provider_fail_until: dict[str, float] = {}
_image_provider_fail_counts: dict[str, int] = {}
_image_provider_success_counts: dict[str, int] = {}
_image_provider_fallback_total = 0
_image_provider_fallback_pairs: dict[str, int] = {}
_HIDREAM_AVAILABILITY_TTL_SEC = max(5, int(os.getenv("HIDREAM_AVAILABILITY_TTL_SEC", "30") or 30))
_HIDREAM_STALE_OK_SEC = max(_HIDREAM_AVAILABILITY_TTL_SEC, int(os.getenv("HIDREAM_STALE_OK_SEC", "180") or 180))
_WAN22_T2I_AVAILABILITY_TTL_SEC = max(5, int(os.getenv("WAN22_T2I_AVAILABILITY_TTL_SEC", "30") or 30))
_WAN22_T2I_STALE_OK_SEC = max(_WAN22_T2I_AVAILABILITY_TTL_SEC, int(os.getenv("WAN22_T2I_STALE_OK_SEC", "180") or 180))
# Interactive defaults are tuned for local-only lanes (WAN/SDXL) when paid xAI fallback is disabled.
HIDREAM_INTERACTIVE_ATTEMPTS = max(1, int(os.getenv("HIDREAM_INTERACTIVE_ATTEMPTS", "1") or 1))
HIDREAM_INTERACTIVE_MAX_WAIT_SEC = max(30, int(os.getenv("HIDREAM_INTERACTIVE_MAX_WAIT_SEC", "75") or 75))
HIDREAM_REFERENCE_STRICT_DENOISE = max(0.18, min(0.75, float(os.getenv("HIDREAM_REFERENCE_STRICT_DENOISE", "0.44") or 0.44)))
HIDREAM_REFERENCE_INSPIRED_DENOISE = max(0.22, min(0.82, float(os.getenv("HIDREAM_REFERENCE_INSPIRED_DENOISE", "0.58") or 0.58)))
HIDREAM_REFERENCE_UPSCALE_METHOD = str(os.getenv("HIDREAM_REFERENCE_UPSCALE_METHOD", "lanczos") or "lanczos").strip() or "lanczos"
WAN22_INTERACTIVE_ATTEMPTS = max(1, int(os.getenv("WAN22_INTERACTIVE_ATTEMPTS", "6") or 6))
WAN22_INTERACTIVE_MAX_WAIT_SEC = max(18, int(os.getenv("WAN22_INTERACTIVE_MAX_WAIT_SEC", "35") or 35))
WAN22_INTERACTIVE_LATENT_ATTEMPTS = max(1, int(os.getenv("WAN22_INTERACTIVE_LATENT_ATTEMPTS", "4") or 4))
WAN22_INTERACTIVE_LATENT_MAX_WAIT_SEC = max(30, int(os.getenv("WAN22_INTERACTIVE_LATENT_MAX_WAIT_SEC", "70") or 70))
WAN22_PREFER_FP16 = str(os.getenv("WAN22_PREFER_FP16", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
_hidream_availability_cache: dict[str, float | bool | str] = {
    "checked_ts": 0.0,
    "ready": False,
    "last_ok_ts": 0.0,
    "last_error": "",
    "model_name": "",
    "clip_l": "",
    "clip_g": "",
    "t5_name": "",
    "llama_name": "",
    "vae_name": "",
}
_hidream_edit_availability_cache: dict[str, float | bool | str] = {
    "checked_ts": 0.0,
    "ready": False,
    "last_ok_ts": 0.0,
    "last_error": "",
    "model_name": "",
    "clip_l": "",
    "clip_g": "",
    "t5_name": "",
    "llama_name": "",
    "vae_name": "",
}
_wan22_t2i_availability_cache: dict[str, float | bool | str] = {
    "checked_ts": 0.0,
    "ready": False,
    "last_ok_ts": 0.0,
    "last_error": "",
    "mode": "",
    "ckpt_name": "",
    "unet_name": "",
}


def _extract_comfy_choice_list(raw_value) -> list[str]:
    if isinstance(raw_value, (tuple, list)):
        if len(raw_value) >= 2 and isinstance(raw_value[1], dict):
            options = raw_value[1].get("options")
            if isinstance(options, (tuple, list)):
                return [str(v) for v in options if str(v or "").strip()]
        if raw_value and isinstance(raw_value[0], (tuple, list)):
            return [str(v) for v in raw_value[0] if str(v or "").strip()]
        return [str(v) for v in raw_value if str(v or "").strip()]
    return []


def _extract_comfy_node_choices(node_info: dict, input_name: str) -> list[str]:
    if not isinstance(node_info, dict):
        return []
    input_spec = node_info.get("input", {})
    if not isinstance(input_spec, dict):
        return []
    for section in ("required", "optional"):
        section_spec = input_spec.get(section, {})
        if not isinstance(section_spec, dict):
            continue
        if input_name in section_spec:
            return _extract_comfy_choice_list(section_spec.get(input_name))
    return []


async def _get_comfyui_object_info(node_name: str | None = None) -> dict:
    global _active_comfyui_url
    cache_suffix = (node_name or "__all__").strip()
    now = time.time()
    last_error = None
    for base_url in _comfyui_candidate_urls():
        cache_key = f"{base_url}|{cache_suffix}"
        cached = _comfyui_object_info_cache.get(cache_key)
        if cached and (now - float(cached.get("ts", 0))) <= _COMFYUI_OBJECT_INFO_TTL_SEC:
            return dict(cached.get("data", {}))
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                url = f"{base_url}/object_info"
                if node_name:
                    url = f"{url}/{node_name}"
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, dict):
                    _comfyui_object_info_cache[cache_key] = {"ts": now, "data": data}
                    _active_comfyui_url = base_url
                    return data
        except Exception as e:
            last_error = e
            continue
    if last_error:
        log.warning(f"ComfyUI object_info lookup failed for '{cache_suffix}': {last_error}")
    return {}


async def _get_comfyui_node_choices(node_name: str, input_name: str) -> list[str]:
    info = await _get_comfyui_object_info(node_name)
    node_info = info.get(node_name) if isinstance(info, dict) else None
    if not isinstance(node_info, dict) and isinstance(info, dict):
        # Some ComfyUI builds return node payload directly for /object_info/<node>.
        node_info = info
    return _extract_comfy_node_choices(node_info or {}, input_name)


def _resolve_comfy_choice(preferred: str, choices: list[str], label: str) -> str:
    want = str(preferred or "").strip()
    if not choices:
        return want
    if want in choices:
        return want
    want_name = Path(want).name.lower()
    for candidate in choices:
        cand = str(candidate or "").strip()
        if cand and Path(cand).name.lower() == want_name:
            if cand != want:
                log.warning(f"{label} '{want}' missing; using matching available '{cand}'")
            return cand
    selected = str(choices[0] or "").strip() or want
    if selected != want:
        log.warning(f"{label} '{want}' missing; using available '{selected}'")
    return selected


def _normalize_image_provider_key(provider: str) -> str:
    key = str(provider or "").strip().lower()
    if key in {"hidream", "hidream-i1", "hidream_i1"}:
        return "hidream"
    if key in {"wan", "wan22", "wan2.2"}:
        return "wan22"
    if key in {"sdxl", "comfy", "comfyui", "local"}:
        return "sdxl"
    if key in {"xai", "grok", "fal"}:
        return "xai"
    return key


def _provider_cooldown_remaining(provider: str) -> float:
    key = _normalize_image_provider_key(provider)
    until = float(_image_provider_fail_until.get(key, 0.0) or 0.0)
    return max(0.0, until - time.time())


def _provider_is_available(provider: str) -> bool:
    return _provider_cooldown_remaining(provider) <= 0.0


def _provider_mark_success(provider: str) -> None:
    key = _normalize_image_provider_key(provider)
    _image_provider_success_counts[key] = int(_image_provider_success_counts.get(key, 0) or 0) + 1
    _image_provider_fail_until.pop(key, None)


def _provider_mark_failure(provider: str, reason: str = "", cooldown_sec: int | None = None) -> None:
    key = _normalize_image_provider_key(provider)
    _image_provider_fail_counts[key] = int(_image_provider_fail_counts.get(key, 0) or 0) + 1
    cool = int(IMAGE_PROVIDER_FAILURE_COOLDOWN_SEC if cooldown_sec is None else cooldown_sec)
    if cool > 0:
        _image_provider_fail_until[key] = time.time() + cool
    if reason:
        log.warning(f"Image provider '{key}' failure: {reason}")


def _record_provider_fallback(from_provider: str, to_provider: str = "next") -> None:
    global _image_provider_fallback_total
    src = _normalize_image_provider_key(from_provider)
    dst = _normalize_image_provider_key(to_provider)
    pair = f"{src}->{dst}"
    _image_provider_fallback_total += 1
    _image_provider_fallback_pairs[pair] = int(_image_provider_fallback_pairs.get(pair, 0) or 0) + 1


def _provider_cooldown_snapshot() -> dict:
    out: dict[str, int] = {}
    for key in sorted(set(list(_image_provider_fail_until.keys()) + list(_image_provider_fail_counts.keys()))):
        rem = int(round(_provider_cooldown_remaining(key)))
        if rem > 0:
            out[key] = rem
    return out


def _ensure_generated_image_valid(path: str) -> None:
    p = Path(path)
    if not p.exists():
        raise RuntimeError(f"generated image missing: {path}")
    size = int(p.stat().st_size or 0)
    if size < int(IMAGE_LOCAL_MIN_FILE_BYTES):
        raise RuntimeError(
            f"generated image too small ({size} bytes < {IMAGE_LOCAL_MIN_FILE_BYTES}): {p.name}"
        )


def _apply_skeleton_glass_shell(rgb: "Image.Image", aggressive: bool = False) -> "Image.Image":
    if Image is None or ImageFilter is None or np is None:
        return rgb
    try:
        arr = np.asarray(rgb).astype(np.float32)
        if arr.ndim != 3 or arr.shape[2] < 3:
            return rgb
        r = arr[:, :, 0]
        g = arr[:, :, 1]
        b = arr[:, :, 2]
        maxc = np.maximum(np.maximum(r, g), b)
        minc = np.minimum(np.minimum(r, g), b)
        sat = np.zeros_like(maxc, dtype=np.float32)
        np.divide((maxc - minc), np.maximum(maxc, 1e-6), out=sat, where=maxc > 1e-6)
        sat *= 255.0
        lum = 0.2126 * r + 0.7152 * g + 0.0722 * b
        lum_floor = 138.0 if aggressive else 150.0
        sat_ceiling = 112.0 if aggressive else 96.0
        neutral = (
            (np.abs(r - g) < 42.0)
            & (np.abs(g - b) < 42.0)
            & (np.abs(r - b) < 54.0)
        )
        bone_mask = ((lum > lum_floor) & (sat < sat_ceiling) & neutral).astype(np.uint8) * 255
        bone_count = int((bone_mask > 0).sum())
        if bone_count <= 2500:
            return rgb
        bone_img = Image.fromarray(bone_mask, mode="L")
        outer_soft = bone_img.filter(ImageFilter.MaxFilter(size=53 if aggressive else 43)).filter(
            ImageFilter.GaussianBlur(radius=8.5 if aggressive else 6.2)
        )
        core_soft = bone_img.filter(ImageFilter.MaxFilter(size=19 if aggressive else 13)).filter(
            ImageFilter.GaussianBlur(radius=4.4 if aggressive else 3.2)
        )
        outer_arr = np.asarray(outer_soft).astype(np.float32) / 255.0
        core_arr = np.asarray(core_soft).astype(np.float32) / 255.0
        ring_arr = np.clip(outer_arr - core_arr, 0.0, 1.0)
        core_fill = np.clip(core_arr * 0.60, 0.0, 1.0)
        not_bone = (bone_mask < 64).astype(np.float32)
        ring_gain = 0.24 if aggressive else 0.20
        fill_gain = 0.16 if aggressive else 0.12
        alpha = np.clip((ring_arr * ring_gain + core_fill * fill_gain) * not_bone, 0.0, 0.30 if aggressive else 0.25)[:, :, None]
        shell_color = np.zeros_like(arr)
        shell_color[:, :, 0] = 154.0
        shell_color[:, :, 1] = 188.0
        shell_color[:, :, 2] = 222.0
        arr = arr * (1.0 - alpha) + shell_color * alpha
        edge = outer_soft.filter(ImageFilter.FIND_EDGES).filter(ImageFilter.GaussianBlur(radius=1.2))
        edge_arr = np.asarray(edge).astype(np.float32) / 255.0
        edge_alpha = np.clip(edge_arr * 0.10, 0.0, 0.13 if aggressive else 0.11)[:, :, None]
        highlight = np.full_like(arr, 238.0)
        arr = arr * (1.0 - edge_alpha) + highlight * edge_alpha

        h = arr.shape[0]
        y = np.arange(h, dtype=np.int32)[:, None]
        yy = np.repeat(y, arr.shape[1], axis=1)
        max_eye = np.maximum(np.maximum(arr[:, :, 0], arr[:, :, 1]), arr[:, :, 2])
        min_eye = np.minimum(np.minimum(arr[:, :, 0], arr[:, :, 1]), arr[:, :, 2])
        chroma_eye = max_eye - min_eye
        blue_bias = (arr[:, :, 2] > (arr[:, :, 0] + 10.0)) & (arr[:, :, 2] > (arr[:, :, 1] + 6.0))
        yellow_bias = (arr[:, :, 0] > (arr[:, :, 1] + 8.0)) & (arr[:, :, 1] > (arr[:, :, 2] + 8.0))
        eye_glow = (
            (yy < int(h * 0.55))
            & (max_eye > 188.0)
            & ((chroma_eye > 22.0) | blue_bias | yellow_bias)
        )
        if eye_glow.any():
            eye_alpha = eye_glow.astype(np.float32)[:, :, None] * (0.74 if aggressive else 0.66)
            eye_target = np.zeros_like(arr)
            eye_target[:, :, 0] = 188.0
            eye_target[:, :, 1] = 194.0
            eye_target[:, :, 2] = 206.0
            arr = arr * (1.0 - eye_alpha) + eye_target * eye_alpha
        return Image.fromarray(np.clip(arr, 0.0, 255.0).astype(np.uint8), mode="RGB")
    except Exception:
        return rgb


def _prompt_prefers_dark_reference_scene(prompt: str) -> bool:
    return bool(
        re.search(
            r"\b(dark|night|noir|shadow|shadowy|moody|dim|dramatic|cinematic|low[- ]key|storm|rain|thunder|black background)\b",
            str(prompt or ""),
            flags=re.IGNORECASE,
        )
    )


def _prepare_skeleton_reference_conditioning_image(source_path: str, prompt: str = "") -> str:
    if Image is None:
        return source_path
    ref_dir = TEMP_DIR / "runtime_reference_inputs"
    ref_dir.mkdir(parents=True, exist_ok=True)
    out_path = ref_dir / f"hidream_ref_subject_{int(time.time() * 1000)}_{random.randint(1000, 9999)}.png"
    try:
        with Image.open(source_path) as im:
            base_rgba = im.convert("RGBA")
            base_rgb = base_rgba.convert("RGB")
            if np is None:
                out = _apply_skeleton_glass_shell(base_rgb, aggressive=True)
                out.save(out_path, format="PNG", optimize=True)
                return str(out_path)

            arr = np.asarray(base_rgb).astype(np.float32)
            h, w = arr.shape[:2]
            if h < 16 or w < 16:
                out = _apply_skeleton_glass_shell(base_rgb, aggressive=True)
                out.save(out_path, format="PNG", optimize=True)
                return str(out_path)

            border_px = max(4, min(18, max(1, min(h, w) // 18)))
            border = np.concatenate(
                [
                    arr[:border_px, :, :].reshape(-1, 3),
                    arr[-border_px:, :, :].reshape(-1, 3),
                    arr[:, :border_px, :].reshape(-1, 3),
                    arr[:, -border_px:, :].reshape(-1, 3),
                ],
                axis=0,
            )
            bg = np.median(border, axis=0)
            bg_lum = float(bg.mean())
            maxc = np.max(arr, axis=2)
            minc = np.min(arr, axis=2)
            lum = 0.2126 * arr[:, :, 0] + 0.7152 * arr[:, :, 1] + 0.0722 * arr[:, :, 2]
            sat = maxc - minc
            diff = np.max(np.abs(arr - bg[None, None, :]), axis=2)

            base_mask = (
                (diff > 20.0)
                | (lum > (bg_lum + 9.0))
                | ((sat < 65.0) & (lum > (bg_lum + 6.0)))
            )
            neutral_bone = (
                (lum > max(114.0, bg_lum + 5.0))
                & (sat < 68.0)
                & (np.abs(arr[:, :, 0] - arr[:, :, 1]) < 42.0)
                & (np.abs(arr[:, :, 1] - arr[:, :, 2]) < 42.0)
            )
            mask = (base_mask | neutral_bone).astype(np.uint8) * 255

            if cv2 is not None:
                close_kernel = np.ones((9, 9), dtype=np.uint8)
                dilate_kernel = np.ones((31, 31), dtype=np.uint8)
                mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, close_kernel, iterations=2)
                bone_u8 = (neutral_bone.astype(np.uint8) * 255)
                mask = np.maximum(mask, cv2.dilate(bone_u8, dilate_kernel, iterations=1))
                components, labels, stats, _ = cv2.connectedComponentsWithStats(mask, connectivity=8)
                if components > 1:
                    areas = stats[1:, cv2.CC_STAT_AREA]
                    largest_idx = int(1 + np.argmax(areas))
                    mask = (labels == largest_idx).astype(np.uint8) * 255
            else:
                mask_img = Image.fromarray(mask, mode="L")
                mask_img = mask_img.filter(ImageFilter.MaxFilter(size=17)).filter(ImageFilter.GaussianBlur(radius=2.2))
                bone_img = Image.fromarray((neutral_bone.astype(np.uint8) * 255), mode="L")
                bone_img = bone_img.filter(ImageFilter.MaxFilter(size=31))
                mask = np.maximum(np.asarray(mask_img), np.asarray(bone_img))

            subject_pixels = np.argwhere(mask > 20)
            if subject_pixels.size == 0:
                out = _apply_skeleton_glass_shell(base_rgb, aggressive=True)
                out.save(out_path, format="PNG", optimize=True)
                return str(out_path)

            y0, x0 = subject_pixels.min(axis=0)
            y1, x1 = subject_pixels.max(axis=0)
            span_h = max(1, int(y1 - y0 + 1))
            span_w = max(1, int(x1 - x0 + 1))
            pad_x = max(24, int(span_w * 0.18))
            pad_y = max(24, int(span_h * 0.16))
            x0 = max(0, int(x0 - pad_x))
            y0 = max(0, int(y0 - pad_y))
            x1 = min(w, int(x1 + pad_x))
            y1 = min(h, int(y1 + pad_y))

            crop_rgba = base_rgba.crop((x0, y0, x1, y1))
            crop_mask = Image.fromarray(mask, mode="L").crop((x0, y0, x1, y1))
            if ImageFilter is not None:
                crop_mask = crop_mask.filter(ImageFilter.MaxFilter(size=13)).filter(ImageFilter.GaussianBlur(radius=3.4))
            crop_rgba.putalpha(crop_mask)

            canvas_w, canvas_h = base_rgba.size
            dark_prompt = _prompt_prefers_dark_reference_scene(prompt)
            bg_rgb = (12, 16, 24) if dark_prompt else (28, 36, 46)
            canvas = Image.new("RGBA", (canvas_w, canvas_h), (*bg_rgb, 255))

            subject_w, subject_h = crop_rgba.size
            target_w = int(canvas_w * 0.54)
            target_h = int(canvas_h * 0.72)
            scale = min(
                float(target_w) / float(max(1, subject_w)),
                float(target_h) / float(max(1, subject_h)),
            )
            resized = crop_rgba.resize(
                (
                    max(1, int(round(subject_w * scale))),
                    max(1, int(round(subject_h * scale))),
                ),
                Image.LANCZOS,
            )
            paste_x = max(0, (canvas_w - resized.width) // 2)
            paste_y = max(0, int(canvas_h * 0.10))
            if paste_y + resized.height > canvas_h:
                paste_y = max(0, (canvas_h - resized.height) // 2)
            canvas.alpha_composite(resized, (paste_x, paste_y))

            conditioned = _apply_skeleton_glass_shell(canvas.convert("RGB"), aggressive=True)
            conditioned.save(out_path, format="PNG", optimize=True)
            return str(out_path)
    except Exception as e:
        log.warning(f"Skeleton reference conditioning preprocess skipped ({Path(source_path).name}): {e}")
        try:
            with Image.open(source_path) as im:
                out = _apply_skeleton_glass_shell(im.convert("RGB"), aggressive=True)
                out.save(out_path, format="PNG", optimize=True)
                return str(out_path)
        except Exception:
            return source_path


def _postprocess_generated_image(path: str, provider: str = "", template: str = "", aggressive: bool = False) -> None:
    """Light local post-process to recover detail from WAN latent stills."""
    if Image is None or ImageFilter is None:
        return
    provider_key = _normalize_image_provider_key(provider)
    p = Path(path)
    if not p.exists() or p.stat().st_size <= 0:
        return
    t = str(template or "").strip().lower()
    # Keep this narrow: only touch templates where WAN latent softness hurts UX.
    if t not in {"skeleton", "story", "motivation"}:
        return
    try:
        with Image.open(p) as im:
            rgb = im.convert("RGB")
            out = rgb
            if provider_key == "wan22":
                sharpen_percent = 260 if (t == "skeleton" and aggressive) else (240 if t == "skeleton" else 190)
                out = rgb.filter(ImageFilter.UnsharpMask(radius=2, percent=sharpen_percent, threshold=2))
                if ImageEnhance is not None:
                    if t == "skeleton":
                        out = ImageEnhance.Contrast(out).enhance(1.10 if aggressive else 1.08)
                    else:
                        out = ImageEnhance.Contrast(out).enhance(1.05)
            elif t == "skeleton" and ImageEnhance is not None:
                out = ImageEnhance.Contrast(out).enhance(1.03 if aggressive else 1.02)

            if t == "skeleton":
                out = _apply_skeleton_glass_shell(out, aggressive=aggressive or provider_key in {"hidream", "hidream-i1", "hidream_i1"})

            out.save(str(p), format="PNG", optimize=True)
    except Exception as e:
        log.warning(f"Generated image post-process skipped ({p.name}): {e}")


async def _resolve_wan22_t2i_runtime_assets() -> dict:
    ckpt_choices = await _get_comfyui_node_choices("CheckpointLoaderSimple", "ckpt_name")
    clip_choices = await _get_comfyui_node_choices("CLIPLoader", "clip_name")
    vae_choices = await _get_comfyui_node_choices("VAELoader", "vae_name")
    unet_choices = await _get_comfyui_node_choices("UNETLoader", "unet_name")

    ckpt_name = WAN22_T2I_CHECKPOINT
    if ckpt_choices and ckpt_name not in ckpt_choices:
        wan_exact = [c for c in ckpt_choices if "wan" in c.lower() and "t2i" in c.lower()]
        wan_generic = [
            c for c in ckpt_choices
            if "wan" in c.lower() and "t2v" not in c.lower() and "i2v" not in c.lower()
        ]
        selected = (wan_exact or wan_generic or [None])[0]
        if selected:
            log.warning(f"Configured WAN22 checkpoint missing; auto-using available checkpoint '{selected}'")
            ckpt_name = selected

    clip_name = WAN22_T2I_CLIP
    if clip_choices and clip_name not in clip_choices:
        clip_candidates = [c for c in clip_choices if "umt5" in c.lower() or "wan" in c.lower()]
        selected_clip = (clip_candidates or [None])[0]
        if selected_clip:
            log.warning(f"Configured WAN22 clip missing; auto-using '{selected_clip}'")
            clip_name = selected_clip
    if clip_choices and clip_name not in clip_choices:
        clip_name = str((clip_choices or [""])[0] or "")

    vae_name = WAN22_T2I_VAE
    if vae_choices and vae_name not in vae_choices:
        # Prefer the WAN 2.1 VAE when available because Comfy's Wan 2.2 low/high T2V lanes decode correctly with it.
        preferred = next((c for c in vae_choices if "wan_2.1_vae" in str(c).lower()), None)
        selected_vae = preferred or (next((c for c in vae_choices if "wan" in str(c).lower()), None))
        if selected_vae:
            log.warning(f"Configured WAN22 VAE missing; auto-using '{selected_vae}'")
            vae_name = str(selected_vae)
    if vae_choices and vae_name not in vae_choices:
        vae_name = str((vae_choices or [""])[0] or "")

    # Prefer real WAN2.2 TI2V UNET path when available.
    ti2v_unet_name = ""
    if unet_choices:
        preferred_unets: list[str] = []
        for candidate in [WAN22_T2I_UNET, WAN22_T2I_UNET_FP8]:
            c = str(candidate or "").strip()
            if c:
                preferred_unets.append(c)
        for preferred_unet in preferred_unets:
            if preferred_unet in unet_choices:
                ti2v_unet_name = preferred_unet
                break
        if not ti2v_unet_name:
            ti2v_pool = [str(u) for u in unet_choices if "wan2.2" in str(u).lower() and "ti2v" in str(u).lower()]
            fp16_ti2v = next((u for u in ti2v_pool if "fp16" in u.lower()), "")
            fp8_ti2v = next((u for u in ti2v_pool if "fp8" in u.lower()), "")
            ti2v_unet_name = fp16_ti2v or fp8_ti2v or (ti2v_pool[0] if ti2v_pool else "")

    # Latent-mode fallback should prefer WAN 2.1 VAE if present.
    # WAN 2.2 VAE can fail decode for 16-channel latents in some ComfyUI Wan graph paths.
    latent_vae_name = str(vae_name or "")
    if vae_choices:
        wan21_vae = next(
            (
                str(c)
                for c in vae_choices
                if "wan_2.1_vae" in str(c).lower()
                or "wan2.1_vae" in str(c).lower()
                or ("wan" in str(c).lower() and "2.1" in str(c).lower())
            ),
            "",
        )
        non22_wan_vae = next(
            (str(c) for c in vae_choices if "wan" in str(c).lower() and "2.2" not in str(c).lower()),
            "",
        )
        any_wan_vae = next((str(c) for c in vae_choices if "wan" in str(c).lower()), "")
        latent_vae_name = wan21_vae or non22_wan_vae or any_wan_vae or latent_vae_name

    low_unet_name = ""
    high_unet_name = ""

    def _pick_wan22_t2v_noise(kind: str) -> str:
        pool = [
            str(u) for u in unet_choices
            if "wan2.2" in str(u).lower() and "t2v" in str(u).lower() and f"{kind}_noise" in str(u).lower()
        ]
        if not pool:
            return ""
        # Prefer fp16 for best quality; fallback to fp8-scaled when fp16 is unavailable.
        fp16 = next((u for u in pool if "fp16" in u.lower()), "")
        if fp16:
            return fp16
        fp8 = next((u for u in pool if "fp8" in u.lower()), "")
        return fp8 or pool[0]

    low_unet_name = _pick_wan22_t2v_noise("low")
    high_unet_name = _pick_wan22_t2v_noise("high")
    if not low_unet_name:
        low_unet_name = next((str(u) for u in unet_choices if "wan" in str(u).lower() and "t2v" in str(u).lower() and "low" in str(u).lower()), "")
    if not high_unet_name:
        high_unet_name = next((str(u) for u in unet_choices if "wan" in str(u).lower() and "t2v" in str(u).lower() and "high" in str(u).lower()), "")

    if ti2v_unet_name and clip_name and vae_name:
        return {
            "mode": "ti2v_unet",
            "ckpt_name": "",
            "clip_name": clip_name,
            "vae_name": vae_name,
            "unet_name": ti2v_unet_name,
            "low_unet_name": "",
            "high_unet_name": "",
        }

    if ckpt_choices and ckpt_name in ckpt_choices:
        return {
            "mode": "checkpoint",
            "ckpt_name": ckpt_name,
            "clip_name": clip_name,
            "vae_name": vae_name,
            "unet_name": "",
            "low_unet_name": "",
            "high_unet_name": "",
        }

    # Fallback local mode: synthesize a still via Wan 2.2 low/high T2V lanes.
    if clip_name and vae_name and low_unet_name and high_unet_name:
        if latent_vae_name and latent_vae_name != vae_name:
            log.warning(
                f"WAN latent fallback forcing compatible VAE '{latent_vae_name}' "
                f"(configured/default was '{vae_name}')"
            )
        log.warning(
            "WAN22 checkpoint missing; using local WAN latent fallback "
            f"(low='{low_unet_name}', high='{high_unet_name}', vae='{latent_vae_name or vae_name}')"
        )
        return {
            "mode": "latent",
            "ckpt_name": "",
            "clip_name": clip_name,
            "vae_name": latent_vae_name or vae_name,
            "unet_name": "",
            "low_unet_name": low_unet_name,
            "high_unet_name": high_unet_name,
        }

    if ckpt_choices and ckpt_name not in ckpt_choices:
        wan_unets = [u for u in unet_choices if "wan" in str(u).lower()]
        ckpt_preview = ", ".join(ckpt_choices[:6]) or "none"
        unet_preview = ", ".join(wan_unets[:6]) or "none"
        raise RuntimeError(
            "WAN22 text-to-image checkpoint not found in ComfyUI checkpoints. "
            f"Configured='{WAN22_T2I_CHECKPOINT}'. Available checkpoints: {ckpt_preview}. "
            f"Detected WAN UNETs (video models): {unet_preview}. "
            f"Expected WAN UNET names include '{WAN22_T2I_UNET}' or '{WAN22_T2I_UNET_FP8}' "
            f"(preferred TI2V) or '{WAN22_T2V_HIGH}'/'{WAN22_T2V_LOW}' (fallback split T2V)."
        )

    raise RuntimeError("WAN22 text-to-image assets unavailable on ComfyUI")


async def _resolve_hidream_runtime_assets_for_model(
    model_hint: str,
    *,
    enabled: bool,
    label: str,
    weight_dtype: str = "default",
) -> dict:
    if not enabled:
        raise RuntimeError(f"{label} is disabled by configuration")
    unet_choices = await _get_comfyui_node_choices("UNETLoader", "unet_name")
    clip_choices = await _get_comfyui_node_choices("QuadrupleCLIPLoader", "clip_name1")
    vae_choices = await _get_comfyui_node_choices("VAELoader", "vae_name")

    model_name = _resolve_comfy_choice(model_hint, unet_choices, label)
    clip_l_name = _resolve_comfy_choice(HIDREAM_CLIP_L, clip_choices, "HiDream clip_l")
    clip_g_name = _resolve_comfy_choice(HIDREAM_CLIP_G, clip_choices, "HiDream clip_g")
    t5_name = _resolve_comfy_choice(HIDREAM_T5, clip_choices, "HiDream t5xxl")
    llama_name = _resolve_comfy_choice(HIDREAM_LLAMA, clip_choices, "HiDream llama")
    vae_name = _resolve_comfy_choice(HIDREAM_VAE, vae_choices, "HiDream VAE")

    missing: list[str] = []
    if not model_name:
        missing.append("diffusion model")
    if not clip_l_name:
        missing.append("clip_l")
    if not clip_g_name:
        missing.append("clip_g")
    if not t5_name:
        missing.append("t5xxl")
    if not llama_name:
        missing.append("llama")
    if not vae_name:
        missing.append("vae")
    if missing:
        raise RuntimeError(f"{label} assets unavailable on ComfyUI: missing " + ", ".join(missing))

    return {
        "model_name": model_name,
        "weight_dtype": str(weight_dtype or "default"),
        "clip_l_name": clip_l_name,
        "clip_g_name": clip_g_name,
        "t5_name": t5_name,
        "llama_name": llama_name,
        "vae_name": vae_name,
    }


async def _resolve_hidream_runtime_assets() -> dict:
    return await _resolve_hidream_runtime_assets_for_model(
        HIDREAM_MODEL,
        enabled=HIDREAM_ENABLED,
        label="HiDream I1 diffusion model",
        weight_dtype="default",
    )


async def _resolve_hidream_edit_runtime_assets() -> dict:
    return await _resolve_hidream_runtime_assets_for_model(
        HIDREAM_EDIT_MODEL,
        enabled=HIDREAM_EDIT_ENABLED,
        label="HiDream E1.1 diffusion model",
        weight_dtype=HIDREAM_EDIT_WEIGHT_DTYPE,
    )


def _normalize_template_adapter_route(route: dict | None) -> dict:
    raw = route if isinstance(route, dict) else {}
    loras: list[dict] = []
    for item in list(raw.get("loras", []) or []):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "") or "").strip()
        if not name:
            continue
        try:
            strength_model = float(item.get("strength_model", 0.7) or 0.7)
        except Exception:
            strength_model = 0.7
        try:
            strength_clip = float(item.get("strength_clip", strength_model) or strength_model)
        except Exception:
            strength_clip = strength_model
        loras.append(
            {
                "name": name,
                "strength_model": max(-2.0, min(2.0, strength_model)),
                "strength_clip": max(-2.0, min(2.0, strength_clip)),
            }
        )
    return {
        "enabled": bool(raw.get("enabled", True)),
        "checkpoint": str(raw.get("checkpoint", "sd_xl_base_1.0.safetensors") or "sd_xl_base_1.0.safetensors").strip(),
        "prepend_trigger": str(raw.get("prepend_trigger", "") or "").strip(),
        "prompt_suffix": str(raw.get("prompt_suffix", "") or "").strip(),
        "negative_suffix": str(raw.get("negative_suffix", "") or "").strip(),
        "apply_prompt_globally": bool(raw.get("apply_prompt_globally", True)),
        "loras": loras,
    }


def _resolve_template_adapter_route(template: str, override_route: dict | None = None) -> dict:
    base = _normalize_template_adapter_route((TEMPLATE_ADAPTER_ROUTING or {}).get("default", {}))
    if not TEMPLATE_ADAPTER_ROUTING_ENABLED:
        base["enabled"] = False
        base["loras"] = []
    key = str(template or "").strip().lower()
    raw = None
    if isinstance(override_route, dict):
        raw = override_route
    elif key:
        raw = (TEMPLATE_ADAPTER_ROUTING or {}).get(key)
    if isinstance(raw, dict):
        overlay = _normalize_template_adapter_route(raw)
        merged = dict(base)
        raw_keys = set(str(k) for k in raw.keys())
        for field in ("enabled", "checkpoint", "prepend_trigger", "prompt_suffix", "negative_suffix", "apply_prompt_globally"):
            if field not in raw_keys:
                continue
            value = overlay.get(field)
            if isinstance(value, bool) or isinstance(value, str):
                merged[field] = value
        if "loras" in raw_keys:
            merged["loras"] = list(overlay.get("loras", []))
        return merged
    return base


def _apply_template_prompt_route(
    template: str,
    prompt: str,
    negative_prompt: str = "",
    provider: str = "",
    adapter_route: dict | None = None,
) -> tuple[str, str, dict]:
    route = _resolve_template_adapter_route(template, override_route=adapter_route)
    out_prompt = str(prompt or "").strip()
    out_negative = str(negative_prompt or "").strip()
    if not route.get("enabled", True):
        return out_prompt, out_negative, route
    if route.get("apply_prompt_globally", True):
        trigger = str(route.get("prepend_trigger", "") or "").strip()
        if trigger and trigger.lower() not in out_prompt.lower():
            out_prompt = f"{trigger}, {out_prompt}" if out_prompt else trigger
        suffix = str(route.get("prompt_suffix", "") or "").strip()
        if suffix:
            out_prompt = f"{out_prompt} {suffix}".strip()
        neg_suffix = str(route.get("negative_suffix", "") or "").strip()
        if neg_suffix:
            out_negative = f"{out_negative}, {neg_suffix}".strip(", ").strip()
    return out_prompt, out_negative, route


async def _resolve_checkpoint_name(preferred: str, fallback: str = "sd_xl_base_1.0.safetensors") -> str:
    want = str(preferred or "").strip() or fallback
    choices = await _get_comfyui_node_choices("CheckpointLoaderSimple", "ckpt_name")
    if not choices:
        return want
    if want in choices:
        return want
    if fallback in choices:
        log.warning(f"Checkpoint '{want}' missing; using fallback '{fallback}'")
        return fallback
    selected = str(choices[0] or "").strip() or want
    log.warning(f"Checkpoint '{want}' missing; using available '{selected}'")
    return selected


async def _resolve_available_loras(loras: list[dict]) -> list[dict]:
    if not loras:
        return []
    names = await _get_comfyui_node_choices("LoraLoader", "lora_name")
    if not names:
        if loras:
            log.warning("ComfyUI LoraLoader has no available LoRA entries; skipping configured adapter LoRAs")
        return []
    available = set(str(n or "").strip() for n in names)
    resolved: list[dict] = []
    for lora in loras:
        name = str((lora or {}).get("name", "") or "").strip()
        if not name:
            continue
        if name in available:
            resolved.append(lora)
        else:
            log.warning(f"Adapter LoRA not found on ComfyUI, skipping: {name}")
    return resolved


def _build_sdxl_workflow_with_loras(
    prompt: str,
    negative_prompt: str,
    width: int,
    height: int,
    checkpoint_name: str,
    loras: list[dict],
    upscale: bool = False,
    upscale_factor: float = 1.0,
    filename_prefix: str = "nyptid_gen",
) -> tuple[dict, str]:
    workflow: dict[str, dict] = {}
    node_i = 1

    def _nid() -> str:
        nonlocal node_i
        out = str(node_i)
        node_i += 1
        return out

    ckpt_id = _nid()
    workflow[ckpt_id] = {
        "class_type": "CheckpointLoaderSimple",
        "inputs": {"ckpt_name": checkpoint_name},
    }

    model_ref = [ckpt_id, 0]
    clip_ref = [ckpt_id, 1]
    vae_ref = [ckpt_id, 2]

    for lora in list(loras or []):
        lora_id = _nid()
        workflow[lora_id] = {
            "class_type": "LoraLoader",
            "inputs": {
                "lora_name": str(lora.get("name", "") or "").strip(),
                "strength_model": float(lora.get("strength_model", 0.7) or 0.7),
                "strength_clip": float(lora.get("strength_clip", 0.7) or 0.7),
                "model": model_ref,
                "clip": clip_ref,
            },
        }
        model_ref = [lora_id, 0]
        clip_ref = [lora_id, 1]

    latent_id = _nid()
    workflow[latent_id] = {
        "class_type": "EmptyLatentImage",
        "inputs": {"width": int(width), "height": int(height), "batch_size": 1},
    }

    pos_id = _nid()
    workflow[pos_id] = {
        "class_type": "CLIPTextEncode",
        "inputs": {"text": prompt, "clip": clip_ref},
    }

    neg_id = _nid()
    workflow[neg_id] = {
        "class_type": "CLIPTextEncode",
        "inputs": {"text": negative_prompt, "clip": clip_ref},
    }

    sampler_id = _nid()
    workflow[sampler_id] = {
        "class_type": "KSampler",
        "inputs": {
            "seed": random.randint(0, 2**32),
            "steps": 30,
            "cfg": 7.5,
            "sampler_name": "dpmpp_2m",
            "scheduler": "karras",
            "denoise": 1.0,
            "model": model_ref,
            "positive": [pos_id, 0],
            "negative": [neg_id, 0],
            "latent_image": [latent_id, 0],
        },
    }

    decode_samples_ref = [sampler_id, 0]
    if upscale:
        upscale_id = _nid()
        workflow[upscale_id] = {
            "class_type": "LatentUpscaleBy",
            "inputs": {
                "samples": [sampler_id, 0],
                "scale_by": float(upscale_factor),
                "upscale_method": "bislerp",
            },
        }
        sampler2_id = _nid()
        workflow[sampler2_id] = {
            "class_type": "KSampler",
            "inputs": {
                "seed": random.randint(0, 2**32),
                "steps": 15,
                "cfg": 7.0,
                "sampler_name": "dpmpp_2m",
                "scheduler": "karras",
                "denoise": 0.4,
                "model": model_ref,
                "positive": [pos_id, 0],
                "negative": [neg_id, 0],
                "latent_image": [upscale_id, 0],
            },
        }
        decode_samples_ref = [sampler2_id, 0]

    vae_decode_id = _nid()
    workflow[vae_decode_id] = {
        "class_type": "VAEDecode",
        "inputs": {"samples": decode_samples_ref, "vae": vae_ref},
    }

    save_id = _nid()
    workflow[save_id] = {
        "class_type": "SaveImage",
        "inputs": {"filename_prefix": filename_prefix, "images": [vae_decode_id, 0]},
    }
    return workflow, save_id




async def _run_comfyui_workflow(
    workflow: dict,
    output_node: str,
    output_type: str = "images",
    max_wait_sec: int = 900,
    poll_interval_sec: float = 2.0,
) -> dict:
    """Submit a workflow to ComfyUI and wait for the specified output node to complete."""
    global _active_comfyui_url
    last_error = None
    for base_url in _comfyui_candidate_urls():
        try:
            client_timeout = max(20, min(900, int(max_wait_sec) + 30))
            async with httpx.AsyncClient(timeout=client_timeout) as client:
                resp = await client.post(f"{base_url}/prompt", json={"prompt": workflow})
                if resp.status_code != 200:
                    if resp.status_code in (404, 502, 503, 504):
                        raise RuntimeError(f"ComfyUI endpoint unavailable ({resp.status_code}) at {base_url}")
                    log.error(f"ComfyUI rejected workflow ({resp.status_code}) on {base_url}: {resp.text[:1000]}")
                    resp.raise_for_status()
                prompt_id = resp.json()["prompt_id"]
                _active_comfyui_url = base_url

                wait_s = max(6, int(max_wait_sec))
                poll_s = max(0.5, float(poll_interval_sec))
                max_polls = max(1, int(wait_s / poll_s))
                for poll_i in range(max_polls):
                    await asyncio.sleep(poll_s)
                    history = await client.get(f"{base_url}/history/{prompt_id}")
                    history.raise_for_status()
                    hist_data = history.json()
                    if prompt_id in hist_data:
                        outputs = hist_data[prompt_id].get("outputs", {})
                        if output_node in outputs:
                            node_out = outputs[output_node]
                            if node_out.get(output_type):
                                return node_out
                            for key in ("videos", "gifs", "images"):
                                if node_out.get(key):
                                    return node_out
                        status = hist_data[prompt_id].get("status", {})
                        if status.get("status_str") == "error":
                            raise RuntimeError(f"ComfyUI workflow error: {status.get('messages', 'unknown')}")
                        elapsed_s = int((poll_i + 1) * poll_s)
                        if poll_i > 0 and elapsed_s % 30 == 0:
                            log.info(f"ComfyUI workflow still running on {base_url}... {elapsed_s}s elapsed")
                raise TimeoutError(f"ComfyUI workflow timed out after {wait_s}s on {base_url}")
        except Exception as e:
            if isinstance(e, TimeoutError):
                last_error = e
                log.warning(f"ComfyUI endpoint timed out ({base_url}): {e}")
                break
            last_error = e
            log.warning(f"ComfyUI endpoint failed ({base_url}): {e}")
            continue
    raise RuntimeError(f"All ComfyUI endpoints failed: {last_error}")


async def _download_comfyui_file(file_info: dict, output_path: str):
    """Download a generated file (image or video frame) from ComfyUI."""
    global _active_comfyui_url
    filename = file_info["filename"]
    subfolder = file_info.get("subfolder", "")
    ftype = file_info.get("type", "output")
    last_error = None
    for base_url in _comfyui_candidate_urls():
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                url = f"{base_url}/view?filename={filename}&subfolder={subfolder}&type={ftype}"
                resp = await client.get(url)
                resp.raise_for_status()
                with open(output_path, "wb") as f:
                    f.write(resp.content)
                _active_comfyui_url = base_url
                return
        except Exception as e:
            last_error = e
            continue
    raise RuntimeError(f"ComfyUI file download failed for {filename}: {last_error}")


GROK_IMAGINE_URL = "https://fal.run/xai/grok-imagine-image"
FAL_FLUX_SCHNELL_URL = "https://fal.run/fal-ai/flux/schnell"


def _normalize_fal_image_backup_model(value: str | None) -> str:
    key = str(value or "").strip().lower()
    if key in {"grok", "grok_imagine", "grok-imagine", "xai", "xai_grok"}:
        return "grok_imagine"
    if key in {"flux", "flux_schnell", "flux-schnell", "flux1_schnell", "flux.1-schnell"}:
        return "flux_schnell"
    return "flux_schnell"


def _fal_image_size_for_resolution(resolution: str) -> str:
    raw = str(resolution or "").strip().lower()
    if raw.endswith("_landscape"):
        return "landscape_16_9"
    if raw == "1080p":
        return "portrait_16_9"
    return "portrait_16_9"


async def _generate_image_fal_flux_schnell(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
) -> dict:
    if not FAL_AI_KEY:
        raise RuntimeError("FAL_AI_KEY not configured")
    headers = {
        "Authorization": "Key " + FAL_AI_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "prompt": str(prompt or "").strip(),
        "image_size": _fal_image_size_for_resolution(resolution),
        "num_inference_steps": 4,
        "num_images": 1,
        "guidance_scale": 3.5,
        "output_format": "png",
        "enable_safety_checker": True,
    }
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(FAL_FLUX_SCHNELL_URL, headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError("FLUX schnell via fal.ai failed (" + str(resp.status_code) + "): " + resp.text[:300])
        data = resp.json()
    images = data.get("images", []) if isinstance(data, dict) else []
    if not images:
        raise RuntimeError("FLUX schnell returned no images")
    cdn_url = str((images[0] or {}).get("url", "") or "")
    if not cdn_url:
        raise RuntimeError("FLUX schnell returned no image URL")
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        img_resp = await client.get(cdn_url)
        if img_resp.status_code != 200:
            raise RuntimeError("Failed to download FLUX schnell image")
        with open(output_path, "wb") as f:
            f.write(img_resp.content)
    log.info(f"FLUX schnell (fal.ai) saved: {output_path} ({Path(output_path).stat().st_size / 1024:.0f} KB)")
    gen_id = await _save_training_candidate(prompt, output_path, source="fal_flux_schnell")
    return {"local_path": output_path, "cdn_url": cdn_url, "generation_id": gen_id}


_pending_training: dict[str, dict] = {}

async def _save_training_candidate(prompt: str, image_path: str, template: str = "", source: str = "grok", metadata: Optional[dict] = None) -> str:
    """Stage a prompt+image pair as a training candidate. Returns generation_id.
    Image is saved immediately but only promoted to 'accepted' via feedback."""
    gen_id = f"gen_{int(time.time() * 1000)}_{id(image_path) % 9999:04d}"
    try:
        img_dest = TRAINING_DATA_DIR / f"{gen_id}.png"
        txt_dest = TRAINING_DATA_DIR / f"{gen_id}.txt"
        shutil.copy2(image_path, str(img_dest))
        txt_dest.write_text(prompt, encoding="utf-8")
        _pending_training[gen_id] = {
            "prompt": prompt,
            "image_path": str(img_dest),
            "txt_path": str(txt_dest),
            "template": template,
            "source": source,
            "status": "pending",
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "metadata": metadata or {},
        }
        log.info(f"Training candidate staged: {gen_id} ({template or 'generic'}/{source})")
    except Exception as e:
        log.warning(f"Training candidate save failed (non-fatal): {e}")
    return gen_id


async def _sync_training_feedback_to_runpod(gen_id: str, entry: dict, status: str):
    """Sync accepted/rejected training examples to RunPod for continuous dataset growth."""
    if not RUNPOD_IMAGE_FEEDBACK_ENABLED:
        return
    image_path = entry.get("image_path", "")
    txt_path = entry.get("txt_path", "")
    if not image_path or not txt_path or not Path(image_path).exists() or not Path(txt_path).exists():
        return
    try:
        remote_root = RUNPOD_IMAGE_FEEDBACK_BASE_DIR.rstrip("/")
        remote_img_dir = f"{remote_root}/{status}/images"
        remote_txt_dir = f"{remote_root}/{status}/prompts"
        remote_meta_dir = f"{remote_root}/{status}/metadata"
        mkdir_cmd = (
            f"{RUNPOD_IMAGE_FEEDBACK_SSH} "
            f"'mkdir -p {remote_img_dir} {remote_txt_dir} {remote_meta_dir}'"
        )
        proc_mkdir = await asyncio.create_subprocess_shell(
            mkdir_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await proc_mkdir.communicate()
        if proc_mkdir.returncode != 0:
            log.warning(f"RunPod training mkdir failed for {gen_id}")
            return

        img_ext = Path(image_path).suffix.lower() or ".png"
        remote_img = f"{remote_img_dir}/{gen_id}{img_ext}"
        remote_txt = f"{remote_txt_dir}/{gen_id}.txt"
        scp_img_cmd = f"scp -o StrictHostKeyChecking=no -P 22092 \"{image_path}\" root@69.30.85.41:{remote_img}"
        scp_txt_cmd = f"scp -o StrictHostKeyChecking=no -P 22092 \"{txt_path}\" root@69.30.85.41:{remote_txt}"
        proc_img = await asyncio.create_subprocess_shell(
            scp_img_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, img_err = await proc_img.communicate()
        if proc_img.returncode != 0:
            log.warning(f"RunPod image sync failed for {gen_id}: {img_err.decode()[:200]}")
            return
        proc_txt = await asyncio.create_subprocess_shell(
            scp_txt_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, txt_err = await proc_txt.communicate()
        if proc_txt.returncode != 0:
            log.warning(f"RunPod prompt sync failed for {gen_id}: {txt_err.decode()[:200]}")
            return

        meta = {
            "generation_id": gen_id,
            "status": status,
            "template": entry.get("template", ""),
            "source": entry.get("source", ""),
            "created_at": entry.get("created_at", ""),
            "feedback_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "metadata": entry.get("metadata", {}),
        }
        meta_path = TEMP_DIR / f"{gen_id}_feedback.json"
        meta_path.write_text(json.dumps(meta, ensure_ascii=True), encoding="utf-8")
        try:
            remote_meta = f"{remote_meta_dir}/{gen_id}.json"
            scp_meta_cmd = f"scp -o StrictHostKeyChecking=no -P 22092 \"{str(meta_path)}\" root@69.30.85.41:{remote_meta}"
            proc_meta = await asyncio.create_subprocess_shell(
                scp_meta_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            _, meta_err = await proc_meta.communicate()
            if proc_meta.returncode != 0:
                log.warning(f"RunPod metadata sync failed for {gen_id}: {meta_err.decode()[:200]}")
        finally:
            meta_path.unlink(missing_ok=True)

        log.info(f"RunPod training sync complete: {gen_id} [{status}]")
    except Exception as e:
        log.warning(f"RunPod training feedback sync error for {gen_id}: {e}")


async def _mark_training_feedback(gen_id: str, accepted: bool, user_id: str = "", event: str = ""):
    """Mark a training candidate as accepted or rejected.
    Accepted pairs are logged to Supabase for LoRA training.
    Rejected pairs are cleaned up from disk."""
    entry = _pending_training.get(gen_id)
    if not entry:
        return
    status = "accepted" if accepted else "rejected"
    entry["status"] = status
    if user_id:
        entry.setdefault("metadata", {})["user_id"] = user_id
    if event:
        entry.setdefault("metadata", {})["event"] = event

    if not accepted:
        if entry.get("source") == "thumbnail_ai":
            try:
                img_path = entry.get("image_path", "")
                if img_path and Path(img_path).exists():
                    reject_dir = "/workspace/thumbnail_training/rejected"
                    ext = Path(img_path).suffix.lower() or ".png"
                    remote_path = f"{reject_dir}/{gen_id}{ext}"
                    ok, err = await asyncio.to_thread(_sync_file_to_runpod_blocking, img_path, remote_path)
                    if not ok:
                        log.warning(f"Thumbnail reject sync file failed for {gen_id}: {err}")
            except Exception as e:
                log.warning(f"Thumbnail reject sync failed for {gen_id}: {e}")
        await _sync_training_feedback_to_runpod(gen_id, entry, status="rejected")
        for p in [entry.get("image_path"), entry.get("txt_path")]:
            try:
                Path(p).unlink(missing_ok=True)
            except Exception:
                pass
        _pending_training.pop(gen_id, None)
        log.info(f"Training candidate {gen_id} REJECTED and cleaned up")
        return

    if SUPABASE_URL and SUPABASE_ANON_KEY:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(
                    f"{SUPABASE_URL}/rest/v1/training_data",
                    headers={
                        "apikey": SUPABASE_ANON_KEY,
                        "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                        "Content-Type": "application/json",
                        "Prefer": "return=minimal",
                    },
                    json={
                        "prompt": entry["prompt"][:2000],
                        "image_filename": gen_id + ".png",
                        "template": entry.get("template", ""),
                        "source": entry.get("source", "grok"),
                        "status": "accepted",
                        "created_at": entry["created_at"],
                    },
                )
        except Exception as e:
            log.warning(f"Supabase training log failed (non-fatal): {e}")
    await _sync_training_feedback_to_runpod(gen_id, entry, status="accepted")
    if entry.get("source") == "thumbnail_ai":
        try:
            img_path = entry.get("image_path", "")
            if img_path and Path(img_path).exists():
                ext = Path(img_path).suffix.lower() or ".png"
                remote_path = f"{RUNPOD_TRAINING_DIR}/{gen_id}{ext}"
                ok, err = await asyncio.to_thread(_sync_file_to_runpod_blocking, img_path, remote_path)
                if not ok:
                    log.warning(f"Thumbnail accept sync file failed for {gen_id}: {err}")
        except Exception as e:
            log.warning(f"Thumbnail accept sync failed for {gen_id}: {e}")
    log.info(f"Training candidate {gen_id} ACCEPTED -> training dataset")


def _file_to_data_image_url(image_path: str, max_bytes: int = 8 * 1024 * 1024) -> str:
    """Encode a local image file as a data URL for xAI image reference conditioning."""
    p = Path(image_path)
    if not p.exists() or p.stat().st_size == 0:
        return ""
    file_size = p.stat().st_size
    if file_size > max_bytes:
        log.warning(f"Skipping data URI encode for {p.name}: {file_size} bytes exceeds limit {max_bytes}")
        return ""
    ext = p.suffix.lower()
    mime = "image/png" if ext == ".png" else ("image/webp" if ext == ".webp" else "image/jpeg")
    b64 = base64.b64encode(p.read_bytes()).decode("ascii")
    return f"data:{mime};base64,{b64}"


async def _generate_image_xai_direct(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    reference_image_url: str = "",
    reference_lock_mode: str = "strict",
) -> dict:
    """Generate image directly via xAI API. No fal.ai needed.
    Returns {"local_path": str, "cdn_url": str}.
    """
    if not XAI_API_KEY:
        raise RuntimeError("XAI_API_KEY not configured")

    headers = {"Authorization": f"Bearer {XAI_API_KEY}", "Content-Type": "application/json"}
    aspect = "16:9" if str(resolution or "").strip().lower().endswith("_landscape") else XAI_IMAGE_ASPECT_RATIO
    payload = {
        "model": XAI_IMAGE_MODEL,
        "prompt": prompt,
        "n": 1,
        "response_format": "url",
        "aspect_ratio": aspect,
        "resolution": XAI_IMAGE_RESOLUTION,
    }
    if reference_image_url:
        payload["image_url"] = reference_image_url
        log.info(f"xAI direct image conditioning enabled: {'https_url' if reference_image_url.startswith('http') else 'inline_data_url'}")

    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                resp = await client.post("https://api.x.ai/v1/images/generations", headers=headers, json=payload)
                if resp.status_code in (200, 201):
                    data = resp.json().get("data", [])
                    if data and data[0].get("url"):
                        cdn_url = data[0]["url"]
                        dl = await client.get(cdn_url, follow_redirects=True)
                        if dl.status_code == 200:
                            with open(output_path, "wb") as f:
                                f.write(dl.content)
                            log.info(f"xAI direct image saved: {output_path} ({Path(output_path).stat().st_size / 1024:.0f} KB)")
                            gen_id = await _save_training_candidate(prompt, output_path, source="xai_direct")
                            return {"local_path": output_path, "cdn_url": cdn_url, "generation_id": gen_id}
                if resp.status_code in (429, 500, 502, 503, 504):
                    wait = (attempt + 1) * 5
                    log.warning(f"xAI image gen attempt {attempt+1} got {resp.status_code}, retrying in {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                # In strict mode, never silently drop identity lock.
                if reference_image_url and resp.status_code in (400, 404, 422):
                    if _normalize_reference_lock_mode(reference_lock_mode) == "strict":
                        raise RuntimeError(
                            f"xAI rejected strict reference image payload ({resp.status_code}); "
                            "strict lock cannot continue without reference conditioning"
                        )
                    payload.pop("image_url", None)
                    reference_image_url = ""
                    log.warning("xAI image reference payload rejected; retrying without reference image")
                    continue
                raise RuntimeError(f"xAI image gen failed ({resp.status_code}): {resp.text[:200]}")
        except RuntimeError:
            raise
        except Exception as e:
            log.warning(f"xAI image gen attempt {attempt+1} error: {e}")
            await asyncio.sleep((attempt + 1) * 3)

    raise RuntimeError("xAI direct image generation failed after retries")


async def generate_image_grok(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    reference_image_url: str = "",
    reference_lock_mode: str = "strict",
) -> dict:
    """Generate an image using the configured remote fallback lane.
    Returns {"local_path": str, "cdn_url": str} so animation backends can reuse the CDN URL directly.
    """
    fal_model = _normalize_fal_image_backup_model(FAL_IMAGE_BACKUP_MODEL)
    if FAL_AI_KEY:
        try:
            if reference_image_url:
                fal_model = "grok_imagine"
            if fal_model == "flux_schnell":
                return await _generate_image_fal_flux_schnell(
                    prompt,
                    output_path,
                    resolution=resolution,
                )
            aspect = "16:9" if str(resolution or "").strip().lower().endswith("_landscape") else "9:16"
            headers = {
                "Authorization": "Key " + FAL_AI_KEY,
                "Content-Type": "application/json",
            }
            payload = {
                "prompt": prompt,
                "num_images": 1,
                "aspect_ratio": aspect,
                "output_format": "png",
            }
            if reference_image_url:
                payload["image_url"] = reference_image_url
                log.info(
                    f"Fal Grok image conditioning enabled: "
                    f"{'https_url' if reference_image_url.startswith('http') else 'inline_data_url'}"
                )

            async with httpx.AsyncClient(timeout=60) as client:
                resp = await client.post(GROK_IMAGINE_URL, headers=headers, json=payload)
                if resp.status_code not in (200, 201):
                    raise RuntimeError("Grok Imagine via fal.ai failed (" + str(resp.status_code) + "): " + resp.text[:300])
                data = resp.json()

            images = data.get("images", [])
            if not images:
                raise RuntimeError("Grok Imagine returned no images")

            cdn_url = images[0].get("url", "")
            async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
                img_resp = await client.get(cdn_url)
                if img_resp.status_code != 200:
                    raise RuntimeError("Failed to download Grok image")
                with open(output_path, "wb") as f:
                    f.write(img_resp.content)

            log.info(
                f"Fal remote image saved via {fal_model}: {output_path} "
                f"({Path(output_path).stat().st_size / 1024:.0f} KB)"
            )
            gen_id = await _save_training_candidate(prompt, output_path, source=f"fal_{fal_model}")
            return {"local_path": output_path, "cdn_url": cdn_url, "generation_id": gen_id}
        except Exception as e:
            log.warning(f"Fal remote image fallback failed ({fal_model}), falling back to direct xAI: {e}")

    if not XAI_API_KEY:
        raise RuntimeError("No remote image fallback configured (Fal and xAI unavailable)")
    log.info(f"Using direct xAI API image generation model={XAI_IMAGE_MODEL}")
    return await _generate_image_xai_direct(
        prompt,
        output_path,
        resolution=resolution,
        reference_image_url=reference_image_url,
        reference_lock_mode=reference_lock_mode,
    )


SKELETON_LORA_CANDIDATES = [
    "nyptid_skeleton_base_identity_v2.safetensors",
    "nyptid_skeleton_glass_v1.safetensors",
]
SKELETON_LORA_STRENGTH = 0.72
SKELETON_TRIGGER_TOKEN = "nyptid_skeleton_glass"
SKELETON_LORA_NEGATIVE = "blurry, low quality, text, watermark, deformed, ugly, bad anatomy, non-skeleton, human skin, flesh, muscles, realistic human, cartoon, anime, painting, 2D, illustration, clothed skeleton, skeleton in uniform, skeleton in armor, skeleton in costume, helmet covering skull, mask covering skull, bare-bone skeleton without translucent body silhouette, bones-only look with no translucent body shell, broken bones, dislocated joints, extra limbs, missing limbs, empty eye sockets, no eyes, hollow eyes, robotic motion, stiff pose, jerky movement, x-ray scan, radiograph, medical imaging, glowing portal, glass display case, glass dome, terrarium, pod, capsule, archway, window frame"
SKELETON_LORA_REFINEMENT_NEGATIVE = "glass display case, glass dome, pod, capsule, archway, doorway, window frame, x-ray scan, radiograph, medical imaging, detached shell, shell hovering away from body, bones-only silhouette"


async def _get_active_skeleton_lora_name() -> str | None:
    """Resolve the preferred available skeleton LoRA name from ComfyUI."""
    for base_url in _comfyui_candidate_urls():
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(f"{base_url}/object_info/LoraLoader")
                if resp.status_code == 200:
                    data = resp.json()
                    lora_list = data.get("LoraLoader", {}).get("input", {}).get("required", {}).get("lora_name", [[]])[0]
                    for candidate in SKELETON_LORA_CANDIDATES:
                        if candidate in lora_list:
                            return candidate
        except Exception:
            continue
    return None


async def check_skeleton_lora_available() -> bool:
    """Check if a preferred skeleton LoRA exists on the ComfyUI server."""
    if not SKELETON_SDXL_LORA_ENABLED:
        return False
    if not _configured_local_image_provider_order():
        return False
    return (await _get_active_skeleton_lora_name()) is not None


async def generate_image_hidream_t2i(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    negative_prompt: str = "",
    allow_default_negative: bool = True,
    max_wait_sec: int = 900,
    runtime_assets: Optional[dict] = None,
) -> str:
    """Generate an image via HiDream-I1 on ComfyUI."""
    config = RESOLUTION_CONFIGS[resolution]
    pos = str(prompt or "").strip()
    neg = str(negative_prompt or "").strip()
    if not neg and allow_default_negative:
        neg = NEGATIVE_PROMPT
    hidream_steps = int(HIDREAM_STEPS)
    if int(max_wait_sec or 0) <= max(90, HIDREAM_INTERACTIVE_MAX_WAIT_SEC + 5):
        hidream_steps = min(hidream_steps, 16)
    runtime = dict(runtime_assets or (await _resolve_hidream_runtime_assets()) or {})
    workflow = {
        "54": {
            "class_type": "QuadrupleCLIPLoader",
            "inputs": {
                "clip_name1": str(runtime.get("clip_l_name", "")),
                "clip_name2": str(runtime.get("clip_g_name", "")),
                "clip_name3": str(runtime.get("t5_name", "")),
                "clip_name4": str(runtime.get("llama_name", "")),
            },
        },
        "55": {
            "class_type": "VAELoader",
            "inputs": {"vae_name": str(runtime.get("vae_name", ""))},
        },
        "69": {
            "class_type": "UNETLoader",
            "inputs": {
                "unet_name": str(runtime.get("model_name", "")),
                "weight_dtype": str(runtime.get("weight_dtype", "default") or "default"),
            },
        },
        "70": {
            "class_type": "ModelSamplingSD3",
            "inputs": {"shift": float(HIDREAM_SHIFT), "model": ["69", 0]},
        },
        "53": {
            "class_type": "EmptySD3LatentImage",
            "inputs": {"width": int(config["gen_width"]), "height": int(config["gen_height"]), "batch_size": 1},
        },
        "16": {
            "class_type": "CLIPTextEncodeHiDream",
            "inputs": {
                "clip": ["54", 0],
                "clip_l": pos,
                "clip_g": pos,
                "t5xxl": pos,
                "llama": pos,
            },
        },
        "40": {
            "class_type": "CLIPTextEncodeHiDream",
            "inputs": {
                "clip": ["54", 0],
                "clip_l": neg,
                "clip_g": neg,
                "t5xxl": neg,
                "llama": neg,
            },
        },
        "3": {
            "class_type": "KSampler",
            "inputs": {
                "seed": random.randint(0, 2**32),
                "steps": hidream_steps,
                "cfg": float(HIDREAM_CFG),
                "sampler_name": str(HIDREAM_SAMPLER or "euler"),
                "scheduler": str(HIDREAM_SCHEDULER or "simple"),
                "denoise": 1.0,
                "model": ["70", 0],
                "positive": ["16", 0],
                "negative": ["40", 0],
                "latent_image": ["53", 0],
            },
        },
        "8": {
            "class_type": "VAEDecode",
            "inputs": {"samples": ["3", 0], "vae": ["55", 0]},
        },
        "9": {
            "class_type": "SaveImage",
            "inputs": {"filename_prefix": "nyptid_hidream_t2i", "images": ["8", 0]},
        },
    }
    result = await _run_comfyui_workflow(workflow, "9", "images", max_wait_sec=max_wait_sec, poll_interval_sec=2.0)
    await _download_comfyui_file(result["images"][0], output_path)
    return output_path


async def _materialize_reference_image_input(reference_image_url: str, template: str = "", prompt: str = "") -> tuple[str, bool]:
    source = str(reference_image_url or "").strip()
    if not source:
        raise RuntimeError("Reference image source missing")
    existing = Path(source)
    if existing.exists():
        if str(template or "").strip().lower() == "skeleton":
            processed = _prepare_skeleton_reference_conditioning_image(str(existing), prompt=prompt)
            return str(processed), str(processed) != str(existing)
        return str(existing), False

    raw = b""
    mime = "image/png"
    if source.startswith("data:image/"):
        raw, mime = _decode_data_image_url(source)
    else:
        async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
            resp = await client.get(source)
            resp.raise_for_status()
            raw = resp.content
            mime = str(resp.headers.get("content-type", "") or mime).split(";", 1)[0].strip() or mime
    if not raw:
        raise RuntimeError("Reference image source was empty")

    ext = ".png"
    mime_l = mime.lower()
    if "jpeg" in mime_l or "jpg" in mime_l:
        ext = ".jpg"
    elif "webp" in mime_l:
        ext = ".webp"
    elif "png" not in mime_l:
        try:
            if source.lower().endswith(".jpg") or source.lower().endswith(".jpeg"):
                ext = ".jpg"
            elif source.lower().endswith(".webp"):
                ext = ".webp"
        except Exception:
            pass

    ref_dir = TEMP_DIR / "runtime_reference_inputs"
    ref_dir.mkdir(parents=True, exist_ok=True)
    out_path = ref_dir / f"hidream_ref_{int(time.time() * 1000)}_{random.randint(1000, 9999)}{ext}"
    out_path.write_bytes(raw)
    if str(template or "").strip().lower() == "skeleton":
        processed = _prepare_skeleton_reference_conditioning_image(str(out_path), prompt=prompt)
        try:
            Path(out_path).unlink(missing_ok=True)
        except Exception:
            pass
        return str(processed), True
    return str(out_path), True


async def generate_image_hidream_reference_locked(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    negative_prompt: str = "",
    template: str = "",
    reference_image_url: str = "",
    reference_lock_mode: str = "strict",
    allow_default_negative: bool = True,
    max_wait_sec: int = 900,
    runtime_assets: Optional[dict] = None,
) -> str:
    """Generate a HiDream image edit using the uploaded reference image as the latent starting point."""
    if not str(reference_image_url or "").strip():
        return await generate_image_hidream_t2i(
            prompt,
            output_path,
            resolution=resolution,
            negative_prompt=negative_prompt,
            allow_default_negative=allow_default_negative,
            max_wait_sec=max_wait_sec,
            runtime_assets=runtime_assets,
        )

    config = RESOLUTION_CONFIGS[resolution]
    pos = str(prompt or "").strip()
    neg = str(negative_prompt or "").strip()
    if not neg and allow_default_negative:
        neg = NEGATIVE_PROMPT
    runtime = dict(runtime_assets or (await _resolve_hidream_edit_runtime_assets()) or {})
    hidream_steps = int(HIDREAM_STEPS)
    if int(max_wait_sec or 0) <= max(90, HIDREAM_INTERACTIVE_MAX_WAIT_SEC + 5):
        hidream_steps = min(hidream_steps, 18)
    ref_path = ""
    cleanup_ref = False
    try:
        ref_path, cleanup_ref = await _materialize_reference_image_input(reference_image_url, template=template, prompt=prompt)
        uploaded_name = await _upload_image_to_comfyui(ref_path)
        target_mp = max(0.6, round((float(config["gen_width"]) * float(config["gen_height"])) / 1_000_000.0, 2))
        lock_mode = _normalize_reference_lock_mode(reference_lock_mode, default="strict")
        denoise = HIDREAM_REFERENCE_STRICT_DENOISE if lock_mode == "strict" else HIDREAM_REFERENCE_INSPIRED_DENOISE
        prompt_l = str(prompt or "").strip().lower()
        cfg = float(HIDREAM_CFG)
        if re.search(r"\b(table|desk|countertop|brain|money|cash|banknotes?|holding|sits?|standing|walk|room|office|background)\b", prompt_l):
            denoise = min(0.82, denoise + 0.04)
        scene_override_prompt = bool(
            re.search(
                r"\b(table|desk|countertop|chair|seated|sitting|brain|money|cash|banknotes?|dark room|office|studio|battlefield|throne|street|city|forest|desert|indoors|interior|outdoor|background)\b",
                prompt_l,
            )
        )
        if scene_override_prompt:
            denoise = max(denoise, 0.76 if lock_mode == "strict" else 0.84)
            cfg = min(8.9, cfg + 0.8)
        table_edit_prompt = bool(re.search(r"\b(table|desk|countertop)\b", prompt_l)) and bool(
            re.search(r"\b(sits?|seated|sitting)\b", prompt_l)
        )
        if table_edit_prompt:
            denoise = max(denoise, 0.84 if lock_mode == "strict" else 0.90)
            cfg = min(9.2, cfg + 0.4)

        workflow = {
            "54": {
                "class_type": "QuadrupleCLIPLoader",
                "inputs": {
                    "clip_name1": str(runtime.get("clip_l_name", "")),
                    "clip_name2": str(runtime.get("clip_g_name", "")),
                    "clip_name3": str(runtime.get("t5_name", "")),
                    "clip_name4": str(runtime.get("llama_name", "")),
                },
            },
            "55": {
                "class_type": "VAELoader",
                "inputs": {"vae_name": str(runtime.get("vae_name", ""))},
            },
            "69": {
                "class_type": "UNETLoader",
                "inputs": {
                    "unet_name": str(runtime.get("model_name", "")),
                    "weight_dtype": str(runtime.get("weight_dtype", "default") or "default"),
                },
            },
            "70": {
                "class_type": "ModelSamplingSD3",
                "inputs": {"shift": float(HIDREAM_SHIFT), "model": ["69", 0]},
            },
            "90": {
                "class_type": "LoadImage",
                "inputs": {"image": uploaded_name},
            },
            "91": {
                "class_type": "ImageScaleToTotalPixels",
                "inputs": {
                    "image": ["90", 0],
                    "upscale_method": HIDREAM_REFERENCE_UPSCALE_METHOD,
                    "megapixels": float(target_mp),
                    "resolution_steps": 64,
                },
            },
            "92": {
                "class_type": "VAEEncode",
                "inputs": {"pixels": ["91", 0], "vae": ["55", 0]},
            },
            "16": {
                "class_type": "CLIPTextEncodeHiDream",
                "inputs": {
                    "clip": ["54", 0],
                    "clip_l": pos,
                    "clip_g": pos,
                    "t5xxl": pos,
                    "llama": pos,
                },
            },
            "40": {
                "class_type": "CLIPTextEncodeHiDream",
                "inputs": {
                    "clip": ["54", 0],
                    "clip_l": neg,
                    "clip_g": neg,
                    "t5xxl": neg,
                    "llama": neg,
                },
            },
            "3": {
                "class_type": "KSampler",
                "inputs": {
                    "seed": random.randint(0, 2**32),
                    "steps": hidream_steps,
                    "cfg": float(cfg),
                    "sampler_name": str(HIDREAM_SAMPLER or "euler"),
                    "scheduler": str(HIDREAM_SCHEDULER or "simple"),
                    "denoise": float(denoise),
                    "model": ["70", 0],
                    "positive": ["16", 0],
                    "negative": ["40", 0],
                    "latent_image": ["92", 0],
                },
            },
            "8": {
                "class_type": "VAEDecode",
                "inputs": {"samples": ["3", 0], "vae": ["55", 0]},
            },
            "9": {
                "class_type": "SaveImage",
                "inputs": {"filename_prefix": "nyptid_hidream_ref", "images": ["8", 0]},
            },
        }
        result = await _run_comfyui_workflow(workflow, "9", "images", max_wait_sec=max_wait_sec, poll_interval_sec=2.0)
        await _download_comfyui_file(result["images"][0], output_path)
        return output_path
    except Exception as e:
        log.warning(f"HiDream E1.1 reference-conditioned generation failed, falling back to txt2img: {e}")
        return await generate_image_hidream_t2i(
            prompt,
            output_path,
            resolution=resolution,
            negative_prompt=negative_prompt,
            allow_default_negative=allow_default_negative,
            max_wait_sec=max_wait_sec,
            runtime_assets=runtime_assets,
        )
    finally:
        if cleanup_ref and ref_path:
            try:
                Path(ref_path).unlink(missing_ok=True)
            except Exception:
                pass


async def generate_image_skeleton_lora(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    source_image_path: str = "",
    denoise: float = 1.0,
    negative_prompt: str = "",
) -> str:
    """Generate or refine a skeleton image using the Jerry glass-shell SDXL LoRA on ComfyUI."""
    config = RESOLUTION_CONFIGS[resolution]
    active_lora_name = await _get_active_skeleton_lora_name()
    if not active_lora_name:
        raise RuntimeError("No preferred skeleton LoRA is available on ComfyUI.")
    prompt_text = str(prompt or "").strip()
    if source_image_path:
        lora_prompt = (
            f"{SKELETON_TRIGGER_TOKEN}, {prompt_text}, preserve the exact composition, pose, props, and framing from the source image. "
            "Add a clear translucent glass-like body shell wrapped tightly around the skeleton anatomy. "
            "The shell must hug the skeleton body, not become a separate dome, display case, portal, archway, or window."
        ).strip()
    else:
        lora_prompt = f"{SKELETON_TRIGGER_TOKEN}, {prompt_text}".strip()
    neg_text = ", ".join(
        part for part in [str(negative_prompt or "").strip(), SKELETON_LORA_NEGATIVE, SKELETON_LORA_REFINEMENT_NEGATIVE if source_image_path else ""] if part
    )

    if source_image_path and Path(source_image_path).exists():
        uploaded_name = await _upload_image_to_comfyui(source_image_path)
        target_mp = max(0.6, round((float(config["gen_width"]) * float(config["gen_height"])) / 1_000_000.0, 2))
        workflow = {
            "1": {
                "class_type": "CheckpointLoaderSimple",
                "inputs": {"ckpt_name": "sd_xl_base_1.0.safetensors"},
            },
            "99": {
                "class_type": "LoraLoader",
                "inputs": {
                    "lora_name": active_lora_name,
                    "strength_model": SKELETON_LORA_STRENGTH,
                    "strength_clip": SKELETON_LORA_STRENGTH,
                    "model": ["1", 0],
                    "clip": ["1", 1],
                },
            },
            "2": {
                "class_type": "LoadImage",
                "inputs": {"image": uploaded_name},
            },
            "3": {
                "class_type": "ImageScaleToTotalPixels",
                "inputs": {
                    "image": ["2", 0],
                    "upscale_method": "lanczos",
                    "megapixels": float(target_mp),
                    "resolution_steps": 64,
                },
            },
            "4": {
                "class_type": "VAEEncode",
                "inputs": {"pixels": ["3", 0], "vae": ["1", 2]},
            },
            "5": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": lora_prompt, "clip": ["99", 1]},
            },
            "6": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": neg_text, "clip": ["99", 1]},
            },
            "7": {
                "class_type": "KSampler",
                "inputs": {
                    "seed": random.randint(0, 2**32),
                    "steps": 12,
                    "cfg": 6.4,
                    "sampler_name": "dpmpp_2m",
                    "scheduler": "karras",
                    "denoise": float(max(0.18, min(0.55, denoise))),
                    "model": ["99", 0],
                    "positive": ["5", 0],
                    "negative": ["6", 0],
                    "latent_image": ["4", 0],
                },
            },
            "8": {
                "class_type": "VAEDecode",
                "inputs": {"samples": ["7", 0], "vae": ["1", 2]},
            },
            "9": {
                "class_type": "SaveImage",
                "inputs": {"filename_prefix": "nyptid_skeleton_lora_refine", "images": ["8", 0]},
            },
        }
    else:
        workflow = {
            "4": {
                "class_type": "CheckpointLoaderSimple",
                "inputs": {"ckpt_name": "sd_xl_base_1.0.safetensors"},
            },
            "10": {
                "class_type": "LoraLoader",
                "inputs": {
                    "lora_name": active_lora_name,
                    "strength_model": SKELETON_LORA_STRENGTH,
                    "strength_clip": SKELETON_LORA_STRENGTH,
                    "model": ["4", 0],
                    "clip": ["4", 1],
                },
            },
            "5": {
                "class_type": "EmptyLatentImage",
                "inputs": {"width": config["gen_width"], "height": config["gen_height"], "batch_size": 1},
            },
            "6": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": lora_prompt, "clip": ["10", 1]},
            },
            "7": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": neg_text, "clip": ["10", 1]},
            },
            "3": {
                "class_type": "KSampler",
                "inputs": {
                    "seed": random.randint(0, 2**32),
                    "steps": 35,
                    "cfg": 7.0,
                    "sampler_name": "dpmpp_2m",
                    "scheduler": "karras",
                    "denoise": 1.0,
                    "model": ["10", 0],
                    "positive": ["6", 0],
                    "negative": ["7", 0],
                    "latent_image": ["5", 0],
                },
            },
            "8": {
                "class_type": "VAEDecode",
                "inputs": {"samples": ["3", 0], "vae": ["4", 2]},
            },
            "9": {
                "class_type": "SaveImage",
                "inputs": {"filename_prefix": "nyptid_skeleton_lora", "images": ["8", 0]},
            },
        }

    result = await _run_comfyui_workflow(workflow, "9", "images")
    await _download_comfyui_file(result["images"][0], output_path)
    log.info(
        f"Skeleton LoRA image generated via {active_lora_name}: "
        f"{output_path} ({Path(output_path).stat().st_size / 1024:.0f} KB)"
    )
    return output_path


async def generate_image_wan22_t2i(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    negative_prompt: str = "",
    allow_default_negative: bool = True,
    max_wait_sec: int = 900,
    runtime_assets: Optional[dict] = None,
) -> str:
    """Generate an image via WAN 2.2 text-to-image on ComfyUI."""
    config = RESOLUTION_CONFIGS[resolution]
    neg = str(negative_prompt or "").strip()
    if not neg and allow_default_negative:
        neg = NEGATIVE_PROMPT
    runtime = dict(runtime_assets or (await _resolve_wan22_t2i_runtime_assets()) or {})
    mode = str(runtime.get("mode", "checkpoint"))
    ckpt_name = str(runtime.get("ckpt_name", ""))
    clip_name = str(runtime.get("clip_name", ""))
    vae_name = str(runtime.get("vae_name", ""))
    unet_name = str(runtime.get("unet_name", ""))
    low_unet_name = str(runtime.get("low_unet_name", ""))
    high_unet_name = str(runtime.get("high_unet_name", ""))

    if mode == "ti2v_unet":
        prompt_l = str(prompt or "").lower()
        semantic_heavy = any(
            token in prompt_l
            for token in ["brain", "money", "cash", "banknote", "table", "desk", "countertop", "glow", "glowing"]
        )
        steps = 20 if semantic_heavy else 16
        cfg_scale = 4.4 if semantic_heavy else 3.6
        sampler_name = "euler"
        scheduler = "simple"
        workflow = {
            "71": {
                "class_type": "CLIPLoader",
                "inputs": {"clip_name": clip_name, "type": "wan", "device": "default"},
            },
            "72": {
                "class_type": "CLIPTextEncode",
                "inputs": {"clip": ["71", 0], "text": neg},
            },
            "89": {
                "class_type": "CLIPTextEncode",
                "inputs": {"clip": ["71", 0], "text": prompt},
            },
            "73": {
                "class_type": "VAELoader",
                "inputs": {"vae_name": vae_name},
            },
            "76": {
                "class_type": "UNETLoader",
                "inputs": {"unet_name": unet_name, "weight_dtype": "default"},
            },
            "82": {
                "class_type": "ModelSamplingSD3",
                "inputs": {"model": ["76", 0], "shift": 5.0},
            },
            "74": {
                "class_type": "EmptyHunyuanLatentVideo",
                "inputs": {
                    "width": int(config["gen_width"]),
                    "height": int(config["gen_height"]),
                    "length": 1,
                    "batch_size": 1,
                },
            },
            "81": {
                "class_type": "KSampler",
                "inputs": {
                    "seed": random.randint(0, 2**32),
                    "steps": steps,
                    "cfg": cfg_scale,
                    "sampler_name": sampler_name,
                    "scheduler": scheduler,
                    "denoise": 1.0,
                    "model": ["82", 0],
                    "positive": ["89", 0],
                    "negative": ["72", 0],
                    "latent_image": ["74", 0],
                },
            },
            "87": {
                "class_type": "VAEDecode",
                "inputs": {"samples": ["81", 0], "vae": ["73", 0]},
            },
            "9": {
                "class_type": "SaveImage",
                "inputs": {"filename_prefix": "nyptid_wan22_t2i", "images": ["87", 0]},
            },
        }
    elif mode == "latent":
        # Local WAN fallback: use low/high T2V lanes to synthesize a still frame.
        # Keep latent length at 1 for still-image generation to avoid upstream request timeouts.
        latent_length = 1
        prompt_l = str(prompt or "").lower()
        semantic_heavy = any(
            token in prompt_l
            for token in ["brain", "money", "cash", "banknote", "table", "desk", "countertop", "glow", "glowing"]
        )
        total_steps = 12 if semantic_heavy else 9
        low_noise_end = 6 if semantic_heavy else 4
        cfg_scale = 4.8 if semantic_heavy else 3.0
        workflow = {
            "71": {
                "class_type": "CLIPLoader",
                "inputs": {"clip_name": clip_name, "type": "wan", "device": "default"},
            },
            "72": {
                "class_type": "CLIPTextEncode",
                "inputs": {"clip": ["71", 0], "text": neg},
            },
            "89": {
                "class_type": "CLIPTextEncode",
                "inputs": {"clip": ["71", 0], "text": prompt},
            },
            "73": {
                "class_type": "VAELoader",
                "inputs": {"vae_name": vae_name},
            },
            "76": {
                "class_type": "UNETLoader",
                "inputs": {"unet_name": low_unet_name, "weight_dtype": "default"},
            },
            "75": {
                "class_type": "UNETLoader",
                "inputs": {"unet_name": high_unet_name, "weight_dtype": "default"},
            },
            "82": {
                "class_type": "ModelSamplingSD3",
                "inputs": {"model": ["76", 0], "shift": 5.0},
            },
            "86": {
                "class_type": "ModelSamplingSD3",
                "inputs": {"model": ["75", 0], "shift": 5.0},
            },
            "74": {
                "class_type": "EmptyHunyuanLatentVideo",
                "inputs": {
                    "width": int(config["gen_width"]),
                    "height": int(config["gen_height"]),
                    "length": latent_length,
                    "batch_size": 1,
                },
            },
            "81": {
                "class_type": "KSamplerAdvanced",
                "inputs": {
                    "model": ["82", 0],
                    "add_noise": "enable",
                    "noise_seed": random.randint(0, 2**32),
                    "steps": total_steps,
                    "cfg": cfg_scale,
                    "sampler_name": "euler",
                    "scheduler": "simple",
                    "positive": ["89", 0],
                    "negative": ["72", 0],
                    "latent_image": ["74", 0],
                    "start_at_step": 0,
                    "end_at_step": low_noise_end,
                    "return_with_leftover_noise": "enable",
                },
            },
            "78": {
                "class_type": "KSamplerAdvanced",
                "inputs": {
                    "model": ["86", 0],
                    "add_noise": "disable",
                    "noise_seed": 0,
                    "steps": total_steps,
                    "cfg": cfg_scale,
                    "sampler_name": "euler",
                    "scheduler": "simple",
                    "positive": ["89", 0],
                    "negative": ["72", 0],
                    "latent_image": ["81", 0],
                    "start_at_step": low_noise_end,
                    "end_at_step": total_steps,
                    "return_with_leftover_noise": "disable",
                },
            },
            "87": {
                "class_type": "VAEDecode",
                "inputs": {"samples": ["78", 0], "vae": ["73", 0]},
            },
            "9": {
                "class_type": "SaveImage",
                "inputs": {"filename_prefix": "nyptid_wan22_t2i", "images": ["87", 0]},
            },
        }
    else:
        workflow = {
            "3": {
                "class_type": "KSampler",
                "inputs": {
                    "seed": random.randint(0, 2**32),
                    "steps": 32,
                    "cfg": 4.5,
                    "sampler_name": "uni_pc_bh2",
                    "scheduler": "simple",
                    "denoise": 1.0,
                    "model": ["4", 0],
                    "positive": ["6", 0],
                    "negative": ["7", 0],
                    "latent_image": ["5", 0],
                },
            },
            "4": {
                "class_type": "CheckpointLoaderSimple",
                "inputs": {"ckpt_name": ckpt_name},
            },
            "5": {
                "class_type": "EmptyLatentImage",
                "inputs": {"width": config["gen_width"], "height": config["gen_height"], "batch_size": 1},
            },
            "6": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": prompt, "clip": ["4", 1]},
            },
            "7": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": neg, "clip": ["4", 1]},
            },
            "8": {
                "class_type": "VAEDecode",
                "inputs": {"samples": ["3", 0], "vae": ["4", 2]},
            },
            "9": {
                "class_type": "SaveImage",
                "inputs": {"filename_prefix": "nyptid_wan22_t2i", "images": ["8", 0]},
            },
        }
        if clip_name and vae_name:
            workflow["30"] = {
                "class_type": "CLIPLoader",
                "inputs": {"clip_name": clip_name, "type": "wan"},
            }
            workflow["31"] = {
                "class_type": "VAELoader",
                "inputs": {"vae_name": vae_name},
            }
            workflow["6"]["inputs"]["clip"] = ["30", 0]
            workflow["7"]["inputs"]["clip"] = ["30", 0]
            workflow["8"]["inputs"]["vae"] = ["31", 0]

    result = await _run_comfyui_workflow(workflow, "9", "images", max_wait_sec=max_wait_sec, poll_interval_sec=2.0)
    await _download_comfyui_file(result["images"][0], output_path)
    return output_path


async def generate_scene_image(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    negative_prompt: str = "",
    template: str = "",
    reference_image_url: str = "",
    reference_lock_mode: str = "strict",
    best_of_enabled: bool = True,
    salvage_enabled: bool = True,
    interactive_fast: bool = False,
    prompt_passthrough: bool = False,
) -> dict:
    """Generate a scene image. Priority for skeleton template: LoRA > Grok Imagine > SDXL.
    For other templates: Grok Imagine > SDXL.
    Returns {"local_path": str, "cdn_url": str | None}.
    """
    async def _enforce_1080_image(path: str) -> None:
        if resolution != "1080p":
            return
        if not _ffmpeg_available():
            log.warning("ffmpeg not found on host; skipping 1080p image upscale")
            return
        target = RESOLUTION_CONFIGS.get("1080p", {})
        out_w = int(target.get("output_width", 1080) or 1080)
        out_h = int(target.get("output_height", 1920) or 1920)
        src = Path(path)
        if not src.exists():
            return
        upscaled = src.with_name(src.stem + "_up1080.png")
        vf = (
            f"scale={out_w}:{out_h}:force_original_aspect_ratio=decrease:flags=lanczos,"
            f"pad={out_w}:{out_h}:(ow-iw)/2:(oh-ih)/2:black,"
            "unsharp=5:5:0.55:5:5:0.0"
        )
        cmd = [
            "ffmpeg", "-y", "-i", str(src),
            "-vf", vf,
            "-frames:v", "1",
            str(upscaled),
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, err = await proc.communicate()
        if proc.returncode == 0 and upscaled.exists() and upscaled.stat().st_size > 0:
            shutil.move(str(upscaled), str(src))
            log.info(f"Image upscaled to 1080p: {src.name}")
        else:
            upscaled.unlink(missing_ok=True)
            log.warning(f"Image 1080p upscale skipped for {src.name}: {err.decode()[-200:]}")

    lock_mode = _normalize_reference_lock_mode(reference_lock_mode, default="strict")
    if template == "skeleton" and _is_template_default_reference(template, reference_image_url):
        log.info("Skeleton default style lock active: using reference DNA only and skipping direct image conditioning")
        reference_image_url = ""
    has_reference = bool(str(reference_image_url or "").strip())
    template_adapter_route = _resolve_template_adapter_route(template)
    if not prompt_passthrough:
        prompt, negative_prompt, template_adapter_route = _apply_template_prompt_route(
            template=template,
            prompt=prompt,
            negative_prompt=negative_prompt,
            provider="scene_image",
            adapter_route=template_adapter_route,
        )
    if template == "skeleton":
        if prompt_passthrough:
            negative_prompt = _relax_skeleton_negative_prompt_for_passthrough(negative_prompt, prompt)
        else:
            negative_prompt = _augment_skeleton_negative_prompt(negative_prompt, prompt)

    def _skeleton_repair_prompt(base_prompt: str) -> str:
        if template != "skeleton":
            return base_prompt
        prompt_l = str(base_prompt or "").lower()
        wants_damage = bool(
            re.search(r"\b(crack|cracks|fracture|fractured|chip|chipped|bruise|bruises|damaged|damage)\b", prompt_l)
        )
        wants_tired = bool(
            re.search(r"\b(tired|fatigued|weary|slouch|slouched|hunch|hunched|droop|drooping|exhausted)\b", prompt_l)
        )
        damage_lock = (
            "DAMAGE DETAIL LOCK: preserve any requested damage cues (cracks, fractures, chips, bruising) clearly on bones."
            if wants_damage
            else ""
        )
        tired_lock = (
            "POSTURE LOCK: preserve requested fatigue cues with slouched shoulders, slight forward head, and low-energy gait."
            if wants_tired
            else ""
        )
        outfit_lock = _skeleton_outfit_coverage_lock(base_prompt)
        return (
            f"{base_prompt} "
            + "CRITICAL QUALITY LOCK: identical canonical skeleton identity, ultra-crisp skull and eye detail, "
            + "readable anatomical structure, high-contrast lighting, sharp cinematic focus, "
            + ("clearly visible translucent body silhouette, requested wardrobe stays correctly worn and body-covering. " if outfit_lock else "clearly visible translucent body silhouette, no clothing or costume. ")
            + "Canonical ivory bone color only; never x-ray/radiograph/CT scan look, never neon-blue medical scan aesthetics. "
            + "Do not omit scene props: every explicitly requested object/action must be visible. "
            + "Never replace requested props with generic spheres/balls. "
            + "Keep a rich topic-matched environment or readable cutaway context; do not collapse to a blank empty backdrop unless explicitly requested. "
            + (damage_lock + " " if damage_lock else "")
            + (tired_lock + " " if tired_lock else "")
            + (outfit_lock + " " if outfit_lock else "")
            + "Glass-shell visibility must be obvious (not faint): medium-opacity translucent shell around the skeleton form."
        ).strip()
    if template == "skeleton" and interactive_fast and not has_reference:
        try:
            lora_available = await check_skeleton_lora_available()
            if lora_available:
                fast_prompt = _build_skeleton_lora_fast_prompt(prompt)
                fast_negative = _build_skeleton_lora_fast_negative(negative_prompt, prompt)
                await generate_image_skeleton_lora(
                    fast_prompt,
                    output_path,
                    resolution=resolution,
                    negative_prompt=fast_negative,
                )
                await _enforce_1080_image(output_path)
                _ensure_generated_image_valid(output_path)
                qa = _score_generated_image_quality(output_path, prompt=fast_prompt, template=template)
                qa_gate_ok, qa_gate_min = _image_quality_gate(
                    qa,
                    template=template,
                    lock_mode=lock_mode,
                    has_reference=False,
                    prompt=fast_prompt,
                )
                base_result = {
                    "local_path": output_path,
                    "cdn_url": None,
                    "provider": "skeleton_lora",
                    "qa_score": qa.get("score", 0.0),
                    "qa_ok": bool(qa_gate_ok),
                    "qa_min_score": qa_gate_min,
                    "qa_notes": list(qa.get("notes", []) or []),
                    "attempt": 1,
                }
                if qa_gate_ok:
                    log.info("Skeleton interactive fast path generated via Jerry LoRA")
                    return base_result
                notes = _interactive_soft_accept_notes(base_result["qa_notes"])
                if not _skeleton_notes_are_severe(notes):
                    base_result["qa_ok"] = False
                    base_result["qa_notes"] = notes
                    log.info(
                        "Skeleton interactive fast path soft-accepted via Jerry LoRA "
                        f"(score={float(base_result['qa_score'] or 0.0):.2f}, min={qa_gate_min:.2f})"
                    )
                    return base_result
                log.warning(
                    "Skeleton interactive fast path failed QA; falling back to configured providers "
                    f"(score={float(base_result['qa_score'] or 0.0):.2f}, notes={base_result['qa_notes']})"
                )
        except Exception as e:
            log.warning(f"Skeleton interactive fast path failed, falling back to configured providers: {e}")
    if template == "skeleton" and SKELETON_SDXL_LORA_ENABLED:
        if reference_image_url and lock_mode == "strict":
            log.info("Skipping Skeleton LoRA for strict reference lock; using conditioned generator")
        else:
            try:
                lora_available = await check_skeleton_lora_available()
                if lora_available:
                    await generate_image_skeleton_lora(prompt, output_path, resolution=resolution)
                    await _enforce_1080_image(output_path)
                    log.info("Skeleton image generated via LoRA (zero API cost)")
                    return {"local_path": output_path, "cdn_url": None}
            except Exception as e:
                log.warning(f"Skeleton LoRA generation failed, falling back to Grok Imagine: {e}")

    provider_order = _configured_image_provider_order()
    skeleton_wan_lock = template == "skeleton" and bool(SKELETON_REQUIRE_WAN22)
    if interactive_fast and template == "skeleton":
        if skeleton_wan_lock:
            # Strict skeleton mode must stay WAN-only.
            provider_order = ["wan22"]
        else:
            compact = _configured_local_image_provider_order()
            if compact:
                provider_order = compact
    if not provider_order:
        provider_order = ["wan22"] if skeleton_wan_lock else _configured_image_provider_order()
    precooled = [p for p in provider_order if not _provider_is_available(p)]
    if precooled:
        provider_order = [p for p in provider_order if _provider_is_available(p)]
        if not provider_order:
            if skeleton_wan_lock:
                provider_order = ["wan22"]
            elif XAI_IMAGE_FALLBACK_ENABLED and bool(FAL_AI_KEY or XAI_API_KEY):
                provider_order = ["fal"]
            else:
                provider_order = ["sdxl"]
        for p in precooled:
            rem = int(round(_provider_cooldown_remaining(p)))
            log.warning(f"Image provider '{_normalize_image_provider_key(p)}' is cooling down ({rem}s left); skipping early")
    xai_aliases = {"xai", "grok", "fal"}
    hidream_requested = any(_normalize_image_provider_key(p) == "hidream" for p in provider_order)
    hidream_ready: Optional[bool] = None
    hidream_edit_ready: Optional[bool] = None
    if hidream_requested:
        hidream_ready = await check_hidream_available()
        if has_reference:
            hidream_edit_ready = await check_hidream_edit_available()
    if hidream_requested and not hidream_ready:
        provider_order = [p for p in provider_order if _normalize_image_provider_key(p) != "hidream"]
        if not provider_order:
            raise RuntimeError("HiDream is the only configured image provider, but HiDream assets are unavailable.")
        _provider_mark_failure("hidream", "hidream_unavailable", cooldown_sec=max(120, IMAGE_PROVIDER_FAILURE_COOLDOWN_SEC))
        log.warning("HiDream unavailable; skipping HiDream provider in this request")
    if hidream_requested and has_reference and hidream_ready and hidream_edit_ready is False:
        log.warning("HiDream E1.1 edit assets unavailable; reference edits will fall back to I1 img2img behavior")
    wan_requested = any(_normalize_image_provider_key(p) == "wan22" for p in provider_order)
    wan_t2i_ready: Optional[bool] = None
    if wan_requested:
        wan_t2i_ready = await check_wan22_t2i_available()
        if template == "skeleton" and bool(SKELETON_REQUIRE_WAN22) and not wan_t2i_ready:
            raise RuntimeError(
                "Skeleton generation blocked: WAN2.2 text-to-image is unavailable and fallback is disabled."
            )
    if IMAGE_PROVIDER_WAN_SKIP_IF_UNAVAILABLE and wan_requested:
        if wan_t2i_ready is None:
            wan_t2i_ready = await check_wan22_t2i_available()
        if not wan_t2i_ready:
            provider_order = [p for p in provider_order if _normalize_image_provider_key(p) != "wan22"]
            if not provider_order:
                provider_order = ["sdxl"]
            _provider_mark_failure("wan22", "wan22_t2i_unavailable", cooldown_sec=max(120, IMAGE_PROVIDER_FAILURE_COOLDOWN_SEC))
            log.warning("WAN22 T2I unavailable; skipping WAN provider in this request")
    cooled = [p for p in provider_order if not _provider_is_available(p)]
    if cooled:
        provider_order = [p for p in provider_order if _provider_is_available(p)]
        if not provider_order:
            if skeleton_wan_lock:
                provider_order = ["wan22"]
            elif XAI_IMAGE_FALLBACK_ENABLED and bool(FAL_AI_KEY or XAI_API_KEY):
                provider_order = ["fal"]
            else:
                provider_order = ["sdxl"]
        for p in cooled:
            rem = int(round(_provider_cooldown_remaining(p)))
            log.warning(f"Image provider '{_normalize_image_provider_key(p)}' is cooling down ({rem}s left); skipping")

    async def _local_provider_result(provider: str) -> dict | None:
        provider_key = str(provider or "").strip().lower()
        attempts = max(1, int(IMAGE_LOCAL_PROVIDER_RETRIES))
        if provider_key not in {"hidream", "hidream-i1", "hidream_i1", "wan", "wan22", "wan2.2", "sdxl", "comfy", "comfyui", "local"}:
            return None
        hidream_runtime_assets: dict | None = None
        wan_runtime_assets: dict | None = None
        wan_runtime_mode = ""
        compact_skeleton_prompt = _compact_skeleton_local_prompt(prompt) if template == "skeleton" else prompt
        skeleton_source_prompt = prompt if (template == "skeleton" and prompt_passthrough) else compact_skeleton_prompt
        compact_skeleton_negative = (
            str(negative_prompt or "").strip()
            if prompt_passthrough
            else _compact_skeleton_negative_prompt(negative_prompt, skeleton_source_prompt)
            if template == "skeleton"
            else negative_prompt
        )
        compact_prop_first_prompt = (
            _compact_skeleton_prop_first_prompt(prompt)
            if template == "skeleton"
            else prompt
        )
        if interactive_fast:
            if provider_key in {"hidream", "hidream-i1", "hidream_i1"}:
                provider_wait_sec = HIDREAM_INTERACTIVE_MAX_WAIT_SEC
            else:
                provider_wait_sec = WAN22_INTERACTIVE_MAX_WAIT_SEC if provider_key in {"wan", "wan22", "wan2.2"} else 12
        else:
            provider_wait_sec = 900
        if template == "skeleton":
            # Keep retries bounded in interactive mode while still giving local providers a fair chance.
            if interactive_fast:
                if provider_key in {"hidream", "hidream-i1", "hidream_i1"}:
                    attempts = HIDREAM_INTERACTIVE_ATTEMPTS
                    provider_wait_sec = HIDREAM_INTERACTIVE_MAX_WAIT_SEC
                    if has_reference:
                        # Public interactive requests route through a tunnel with a hard timeout budget.
                        # Reference-conditioned HiDream passes take materially longer than plain txt2img,
                        # so keep them to a single pass and soft-accept the first usable frame.
                        attempts = 1
                    try:
                        hidream_runtime_assets = await (
                            _resolve_hidream_edit_runtime_assets()
                            if has_reference
                            else _resolve_hidream_runtime_assets()
                        )
                    except Exception:
                        hidream_runtime_assets = None
                elif provider_key in {"wan", "wan22", "wan2.2"}:
                    try:
                        wan_runtime_assets = await _resolve_wan22_t2i_runtime_assets()
                        wan_runtime_mode = str((wan_runtime_assets or {}).get("mode", "") or "")
                    except Exception:
                        wan_runtime_assets = None
                        wan_runtime_mode = ""
                    if wan_runtime_mode == "latent":
                        # Missing WAN T2I checkpoint: latent fallback can work but needs a longer single pass.
                        attempts = WAN22_INTERACTIVE_LATENT_ATTEMPTS
                        provider_wait_sec = WAN22_INTERACTIVE_LATENT_MAX_WAIT_SEC
                    else:
                        attempts = WAN22_INTERACTIVE_ATTEMPTS
                        provider_wait_sec = WAN22_INTERACTIVE_MAX_WAIT_SEC
                    prompt_lc = skeleton_source_prompt.lower()
                    has_table_scene = bool(re.search(r"\b(table|desk|countertop)\b", prompt_lc))
                    has_brain = "brain" in prompt_lc
                    has_money = bool(re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", prompt_lc))
                    if has_table_scene and has_brain and has_money:
                        # Complex two-prop scenes need a wider candidate pool to avoid weak compositions.
                        attempts = max(attempts, 8)
                else:
                    attempts = 2
            else:
                attempts = max(attempts, 2)
        best_soft_result: dict | None = None
        best_soft_path = Path(output_path).with_name(
            Path(output_path).stem + f"_{provider_key}_best_soft" + (Path(output_path).suffix or ".png")
        )
        last_err: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                attempt_prompt = prompt
                attempt_negative = negative_prompt
                if template == "skeleton":
                    if prompt_passthrough:
                        attempt_prompt = skeleton_source_prompt
                        attempt_negative = compact_skeleton_negative
                    elif interactive_fast:
                        if attempt == 1:
                            attempt_prompt = skeleton_source_prompt
                        elif attempt == 2:
                            attempt_prompt = compact_prop_first_prompt
                        elif attempt == 3:
                            attempt_prompt = (
                                compact_prop_first_prompt
                                + " strict prop framing: full tabletop visible. left side brain with clear folds. right side pile of cash banknotes."
                            ).strip()
                        elif attempt == 4:
                            attempt_prompt = (
                                compact_prop_first_prompt
                                + " frontal medium shot. full ribcage and skull visible. both props in foreground on tabletop."
                            ).strip()
                        elif attempt == 5:
                            attempt_prompt = (
                                compact_prop_first_prompt
                                + " camera closer to tabletop so both brain and money are large and obvious."
                            ).strip()
                        else:
                            attempt_prompt = (
                                compact_prop_first_prompt
                                + " no human head statue, no bust sculpture, only full anatomical skeleton seated at table."
                            ).strip()
                        attempt_negative = _compact_skeleton_negative_prompt(negative_prompt, attempt_prompt)
                    else:
                        attempt_prompt = skeleton_source_prompt
                        attempt_negative = compact_skeleton_negative
                if provider_key in {"hidream", "hidream-i1", "hidream_i1", "wan", "wan22", "wan2.2"} and template == "skeleton":
                    if prompt_passthrough:
                        # Creative prompt passthrough means use the scene text exactly.
                        attempt_prompt = skeleton_source_prompt
                    else:
                        attempt_prompt = _skeleton_repair_prompt(skeleton_source_prompt)
                        if attempt > 1:
                            attempt_prompt = (
                                attempt_prompt
                                + " HARD READABILITY LOCK: skeleton stays dominant and ultra-sharp in frame with crisp eyes and props, while preserving readable environment depth. No haze, no motion blur."
                            ).strip()
                        if attempt >= 3:
                            attempt_prompt = (
                                attempt_prompt
                                + " STRICT PROP LOCK: all requested objects must be clearly visible and recognizable in-frame."
                            ).strip()
                if provider_key in {"hidream", "hidream-i1", "hidream_i1"}:
                    if has_reference:
                        await generate_image_hidream_reference_locked(
                            attempt_prompt,
                            output_path,
                            resolution=resolution,
                            negative_prompt=attempt_negative,
                            template=template,
                            reference_image_url=reference_image_url,
                            reference_lock_mode=lock_mode,
                            allow_default_negative=not prompt_passthrough,
                            max_wait_sec=provider_wait_sec,
                            runtime_assets=hidream_runtime_assets,
                        )
                    else:
                        await generate_image_hidream_t2i(
                            attempt_prompt,
                            output_path,
                            resolution=resolution,
                            negative_prompt=attempt_negative,
                            allow_default_negative=not prompt_passthrough,
                            max_wait_sec=provider_wait_sec,
                            runtime_assets=hidream_runtime_assets,
                        )
                if provider_key in {"wan", "wan22", "wan2.2"}:
                    await generate_image_wan22_t2i(
                        attempt_prompt,
                        output_path,
                        resolution=resolution,
                        negative_prompt=attempt_negative,
                        allow_default_negative=not prompt_passthrough,
                        max_wait_sec=provider_wait_sec,
                        runtime_assets=wan_runtime_assets,
                    )
                elif provider_key in {"sdxl", "comfy", "comfyui", "local"}:
                    await generate_image_comfyui(
                        attempt_prompt,
                        output_path,
                        resolution=resolution,
                        negative_prompt=attempt_negative,
                        allow_default_negative=not prompt_passthrough,
                        template=template,
                        adapter_route=template_adapter_route,
                        max_wait_sec=provider_wait_sec,
                    )
                if template == "skeleton" and provider_key in {"hidream", "hidream-i1", "hidream_i1", "wan", "wan22", "wan2.2"}:
                    try:
                        lora_available = await check_skeleton_lora_available()
                    except Exception:
                        lora_available = False
                    if lora_available:
                        refine_prompt_l = str(attempt_prompt or "").lower()
                        wants_glass_refine = (
                            has_reference
                            or "glass" in refine_prompt_l
                            or "translucent" in refine_prompt_l
                            or "shell" in refine_prompt_l
                            or "table" in refine_prompt_l
                            or "brain" in refine_prompt_l
                            or bool(re.search(r"\b(money|cash|banknotes?)\b", refine_prompt_l))
                        )
                        if wants_glass_refine:
                            refine_path = str(Path(output_path).with_name(Path(output_path).stem + "_glass_refine.png"))
                            refine_denoise = 0.24 if has_reference else 0.30
                            try:
                                await generate_image_skeleton_lora(
                                    attempt_prompt,
                                    refine_path,
                                    resolution=resolution,
                                    source_image_path=output_path,
                                    denoise=refine_denoise,
                                    negative_prompt=attempt_negative,
                                )
                                shutil.move(refine_path, output_path)
                                log.info(f"Applied Jerry glass-shell LoRA refinement via SDXL ({provider_key} -> skeleton refine)")
                            except Exception as refine_err:
                                Path(refine_path).unlink(missing_ok=True)
                                log.warning(f"Jerry glass-shell LoRA refinement skipped: {refine_err}")
                await _enforce_1080_image(output_path)
                _postprocess_generated_image(output_path, provider=provider_key, template=template)
                _ensure_generated_image_valid(output_path)
                qa_prompt = skeleton_source_prompt if template == "skeleton" else prompt
                qa = _score_generated_image_quality(output_path, prompt=qa_prompt, template=template)
                qa_gate_ok, qa_gate_min = _image_quality_gate(
                    qa,
                    template=template,
                    lock_mode=lock_mode,
                    has_reference=has_reference,
                    prompt=qa_prompt,
                )
                if template == "skeleton" and interactive_fast:
                    current_score = float(qa.get("score", 0.0) or 0.0)
                    best_score = float((best_soft_result or {}).get("qa_score", -1.0))
                    if best_soft_result is None or current_score > best_score:
                        try:
                            shutil.copyfile(output_path, str(best_soft_path))
                        except Exception:
                            pass
                        best_soft_result = {
                            "local_path": output_path,
                            "cdn_url": None,
                            "provider": provider_key,
                            "qa_score": current_score,
                            "qa_ok": bool(qa_gate_ok),
                            "qa_min_score": qa_gate_min,
                            "qa_notes": list(qa.get("notes", [])),
                            "attempt": attempt,
                        }
                if template == "skeleton" and not qa_gate_ok and attempt < attempts:
                    log.warning(
                        f"Skeleton image below QA gate via '{provider_key}' (attempt {attempt}/{attempts}, score={qa.get('score', 0.0):.2f}, "
                        f"min={qa_gate_min:.2f}); retrying."
                    )
                    continue
                if template == "skeleton" and not qa_gate_ok and attempt >= attempts:
                    if interactive_fast and has_reference and provider_key in {"hidream", "hidream-i1", "hidream_i1"}:
                        notes = _interactive_soft_accept_notes(qa.get("notes", []) or [])
                        if not _skeleton_notes_are_severe(notes):
                            _provider_mark_success(provider_key)
                            return {
                                "local_path": output_path,
                                "cdn_url": None,
                                "provider": provider_key,
                                "qa_score": qa.get("score", 0.0),
                                "qa_ok": False,
                                "qa_min_score": qa_gate_min,
                                "qa_notes": notes,
                                "attempt": attempt,
                            }
                    if interactive_fast and best_soft_result is not None:
                        best_notes = list(best_soft_result.get("qa_notes", []) or [])
                        severe = _skeleton_notes_are_severe(best_notes)
                        if not severe:
                            if best_soft_path.exists():
                                try:
                                    shutil.copyfile(str(best_soft_path), output_path)
                                except Exception:
                                    pass
                            notes = _interactive_soft_accept_notes(best_soft_result.get("qa_notes", []) or [])
                            best_soft_result["qa_notes"] = notes
                            best_soft_result["qa_ok"] = False
                            _provider_mark_success(provider_key)
                            best_soft_path.unlink(missing_ok=True)
                            log.warning(
                                f"Interactive fast-mode soft-accepted skeleton image via '{provider_key}' "
                                f"(score={best_soft_result.get('qa_score', 0.0):.2f}, notes={best_soft_result.get('qa_notes', [])})"
                            )
                            return best_soft_result
                    raise RuntimeError(
                        f"Skeleton QA gate failed for provider '{provider_key}' after {attempts} attempts "
                        f"(score={qa.get('score', 0.0):.2f}, min={qa_gate_min:.2f}, notes={qa.get('notes', [])})"
                    )
                _provider_mark_success(provider_key)
                best_soft_path.unlink(missing_ok=True)
                return {
                    "local_path": output_path,
                    "cdn_url": None,
                    "provider": provider_key,
                    "qa_score": qa.get("score", 0.0),
                    "qa_ok": bool(qa_gate_ok),
                    "qa_min_score": qa_gate_min,
                    "qa_notes": qa.get("notes", []),
                    "attempt": attempt,
                }
            except Exception as err:
                last_err = err
                err_l = str(err or "").lower()
                # Avoid stacking timed-out WAN jobs (they can still be running in ComfyUI and consume VRAM).
                if (
                    interactive_fast
                    and provider_key in {"hidream", "hidream-i1", "hidream_i1", "wan", "wan22", "wan2.2"}
                    and ("timed out" in err_l or "timeout" in err_l)
                ):
                    break
                if attempt < attempts:
                    await asyncio.sleep(min(5, attempt))
        if interactive_fast and template == "skeleton" and best_soft_result is not None:
            best_notes = list(best_soft_result.get("qa_notes", []) or [])
            severe = _skeleton_notes_are_severe(best_notes)
            if not severe:
                if best_soft_path.exists():
                    try:
                        shutil.copyfile(str(best_soft_path), output_path)
                    except Exception:
                        pass
                notes = _interactive_soft_accept_notes(best_soft_result.get("qa_notes", []) or [])
                best_soft_result["qa_notes"] = notes
                best_soft_result["qa_ok"] = False
                _provider_mark_success(provider_key)
                best_soft_path.unlink(missing_ok=True)
                log.warning(
                    f"Interactive fast-mode returning best available skeleton image via '{provider_key}' "
                    f"(score={best_soft_result.get('qa_score', 0.0):.2f}, notes={best_soft_result.get('qa_notes', [])})"
                )
                return best_soft_result
        best_soft_path.unlink(missing_ok=True)
        _provider_mark_failure(provider_key, reason=str(last_err or "unknown_error"))
        mode_suffix = f" (mode={wan_runtime_mode})" if wan_runtime_mode else ""
        raise RuntimeError(f"provider '{provider_key}' failed after {attempts} attempts{mode_suffix}: {last_err}")

    last_local_provider_err: Exception | None = None
    best_soft_local_result: dict | None = None
    xai_index = next((i for i, p in enumerate(provider_order) if p in xai_aliases), len(provider_order))
    local_providers = list(provider_order[:xai_index])
    for idx, provider in enumerate(local_providers):
        provider_norm = _normalize_image_provider_key(provider)
        next_provider = local_providers[idx + 1] if (idx + 1) < len(local_providers) else ""
        try:
            local_result = await _local_provider_result(provider)
            if local_result:
                if (
                    template == "skeleton"
                    and interactive_fast
                    and provider_norm in {"hidream", "wan22"}
                    and not bool(local_result.get("qa_ok", False))
                    and bool(next_provider)
                    and not skeleton_wan_lock
                ):
                    score = float(local_result.get("qa_score", 0.0) or 0.0)
                    best_score = float((best_soft_local_result or {}).get("qa_score", -1.0) or -1.0)
                    if best_soft_local_result is None or score > best_score:
                        best_soft_local_result = dict(local_result)
                    _record_provider_fallback(provider, next_provider)
                    log.warning(
                        f"Skeleton interactive: '{provider_norm}' soft result score={score:.2f}; trying next local provider '{_normalize_image_provider_key(next_provider)}'"
                    )
                    continue
                if (
                    template == "skeleton"
                    and interactive_fast
                    and best_soft_local_result is not None
                    and not bool(local_result.get("qa_ok", False))
                ):
                    current = float(local_result.get("qa_score", 0.0) or 0.0)
                    best = float(best_soft_local_result.get("qa_score", 0.0) or 0.0)
                    if current >= best:
                        return local_result
                    return best_soft_local_result
                return local_result
        except Exception as local_err:
            last_local_provider_err = local_err
            _record_provider_fallback(provider, "next")
            log.warning(f"Local image provider '{provider}' failed, trying next: {local_err}")
    if best_soft_local_result is not None:
        return best_soft_local_result

    xai_enabled = (
        XAI_IMAGE_FALLBACK_ENABLED
        and any(p in xai_aliases for p in provider_order)
        and bool(FAL_AI_KEY or XAI_API_KEY)
    )
    if xai_enabled:
        try:
            if best_of_enabled and IMAGE_QUALITY_BESTOF_ENABLED and IMAGE_QUALITY_BESTOF_COUNT > 1:
                best_count = max(2, int(IMAGE_QUALITY_BESTOF_COUNT))
                if template in {"skeleton", "story", "motivation"}:
                    best_count = max(best_count, 4)
                if lock_mode == "strict" and reference_image_url and template in {"skeleton", "story"}:
                    best_count = max(best_count, 5)
                if template == "skeleton":
                    best_count = max(best_count, 5)
                    if lock_mode == "strict" and has_reference:
                        best_count = max(best_count, 6)
                cand_root = Path(output_path)
                candidates = []
                for idx in range(best_count):
                    cand_path = str(cand_root.with_name(f"{cand_root.stem}_cand_{idx}{cand_root.suffix or '.png'}"))
                    try:
                        cand_result = await generate_image_grok(
                            prompt,
                            cand_path,
                            resolution=resolution,
                            reference_image_url=reference_image_url,
                            reference_lock_mode=lock_mode,
                        )
                        await _enforce_1080_image(cand_path)
                        qa = _score_generated_image_quality(cand_path, prompt=prompt, template=template)
                        gate_ok, gate_min = _image_quality_gate(
                            qa,
                            template=template,
                            lock_mode=lock_mode,
                            has_reference=has_reference,
                            prompt=prompt,
                        )
                        qa["gate_ok"] = gate_ok
                        qa["gate_min_score"] = gate_min
                        candidates.append({"path": cand_path, "result": cand_result, "qa": qa, "idx": idx})
                        log.info(
                            f"Best-of candidate {idx+1}/{best_count} score={qa.get('score', 0.0)} "
                            f"gate_ok={qa.get('gate_ok', False)} min={qa.get('gate_min_score', 0.0)} "
                            f"notes={','.join(qa.get('notes', []))}"
                        )
                    except Exception as cand_err:
                        log.warning(f"Best-of candidate {idx+1}/{best_count} failed: {cand_err}")
                if not candidates:
                    raise RuntimeError("all best-of image candidates failed")

                acceptable = [c for c in candidates if bool(c.get("qa", {}).get("gate_ok", False))]
                winner = max((acceptable or candidates), key=lambda c: float(c.get("qa", {}).get("score", 0.0)))
                winner_path = str(winner["path"])
                winner_result = dict(winner["result"])
                winner_qa = dict(winner.get("qa", {}))
                if Path(winner_path).resolve() != Path(output_path).resolve():
                    shutil.copyfile(winner_path, output_path)
                winner_result["local_path"] = output_path
                winner_result["qa_score"] = winner_qa.get("score", 0.0)
                winner_result["qa_ok"] = bool(winner_qa.get("gate_ok", False))
                winner_result["qa_min_score"] = winner_qa.get("gate_min_score", _image_quality_min_score(template=template, lock_mode=lock_mode, has_reference=has_reference))
                winner_result["qa_notes"] = winner_qa.get("notes", [])
                # Story salvage pass: if winner is still below threshold, do one extra high-realism attempt.
                if salvage_enabled and template == "story" and not winner_result["qa_ok"]:
                    try:
                        salvage_prompt = f"{prompt} {_story_salvage_refinement_for_scene(prompt)}"
                        salvage_path = str(cand_root.with_name(f"{cand_root.stem}_salvage{cand_root.suffix or '.png'}"))
                        salvage_result = await generate_image_grok(
                            salvage_prompt,
                            salvage_path,
                            resolution=resolution,
                            reference_image_url=reference_image_url,
                            reference_lock_mode=lock_mode,
                        )
                        await _enforce_1080_image(salvage_path)
                        salvage_qa = _score_generated_image_quality(salvage_path, prompt=salvage_prompt, template=template)
                        salvage_gate_ok, salvage_gate_min = _image_quality_gate(
                            salvage_qa,
                            template=template,
                            lock_mode=lock_mode,
                            has_reference=has_reference,
                            prompt=salvage_prompt,
                        )
                        salvage_qa["gate_ok"] = salvage_gate_ok
                        salvage_qa["gate_min_score"] = salvage_gate_min
                        if float(salvage_qa.get("score", 0.0)) > float(winner_result.get("qa_score", 0.0)):
                            shutil.copyfile(salvage_path, output_path)
                            winner_result = dict(salvage_result)
                            winner_result["local_path"] = output_path
                            winner_result["qa_score"] = salvage_qa.get("score", 0.0)
                            winner_result["qa_ok"] = bool(salvage_qa.get("gate_ok", False))
                            winner_result["qa_min_score"] = salvage_qa.get("gate_min_score", winner_result.get("qa_min_score", 0.0))
                            winner_result["qa_notes"] = salvage_qa.get("notes", [])
                            log.info(f"Story salvage image replaced winner, score={winner_result['qa_score']}")
                        Path(salvage_path).unlink(missing_ok=True)
                    except Exception as salvage_err:
                        log.warning(f"Story salvage pass failed: {salvage_err}")
                if salvage_enabled and template == "skeleton" and not winner_result["qa_ok"]:
                    for salvage_idx in range(1):
                        try:
                            salvage_prompt = _skeleton_repair_prompt(prompt)
                            salvage_path = str(cand_root.with_name(f"{cand_root.stem}_skeleton_repair_{salvage_idx}{cand_root.suffix or '.png'}"))
                            salvage_result = await generate_image_grok(
                                salvage_prompt,
                                salvage_path,
                                resolution=resolution,
                                reference_image_url=reference_image_url,
                                reference_lock_mode=lock_mode,
                            )
                            await _enforce_1080_image(salvage_path)
                            salvage_qa = _score_generated_image_quality(salvage_path, prompt=salvage_prompt, template=template)
                            salvage_gate_ok, salvage_gate_min = _image_quality_gate(
                                salvage_qa,
                                template=template,
                                lock_mode=lock_mode,
                                has_reference=has_reference,
                                prompt=salvage_prompt,
                            )
                            salvage_qa["gate_ok"] = salvage_gate_ok
                            salvage_qa["gate_min_score"] = salvage_gate_min
                            if (
                                salvage_gate_ok
                                or float(salvage_qa.get("score", 0.0)) > float(winner_result.get("qa_score", 0.0))
                            ):
                                shutil.copyfile(salvage_path, output_path)
                                winner_result = dict(salvage_result)
                                winner_result["local_path"] = output_path
                                winner_result["qa_score"] = salvage_qa.get("score", 0.0)
                                winner_result["qa_ok"] = bool(salvage_qa.get("gate_ok", False))
                                winner_result["qa_min_score"] = salvage_qa.get("gate_min_score", winner_result.get("qa_min_score", 0.0))
                                winner_result["qa_notes"] = salvage_qa.get("notes", [])
                                log.info(
                                    f"Skeleton repair pass {salvage_idx + 1}/1 score={winner_result['qa_score']} "
                                    f"ok={winner_result['qa_ok']}"
                                )
                            Path(salvage_path).unlink(missing_ok=True)
                            if winner_result["qa_ok"]:
                                break
                        except Exception as salvage_err:
                            log.warning(f"Skeleton repair pass {salvage_idx + 1}/1 failed: {salvage_err}")
                if not winner_result["qa_ok"]:
                    if template == "skeleton":
                        raise RuntimeError(
                            "Skeleton best-of candidates failed QA gate "
                            f"(score={winner_result.get('qa_score', 0.0)}, notes={winner_result.get('qa_notes', [])})"
                        )
                    log.warning(
                        f"Best-of winner below threshold {winner_result.get('qa_min_score', IMAGE_QUALITY_MIN_SCORE)}: "
                        f"score={winner_result['qa_score']} path={Path(output_path).name}"
                    )
                else:
                    log.info(
                        f"Best-of winner selected score={winner_result['qa_score']} "
                        f"candidate={winner.get('idx', -1)+1}/{best_count}"
                    )

                for cand in candidates:
                    p = Path(str(cand.get("path", "") or ""))
                    if p.exists() and p.resolve() != Path(output_path).resolve():
                        p.unlink(missing_ok=True)
                return winner_result

            result = await generate_image_grok(
                prompt,
                output_path,
                resolution=resolution,
                reference_image_url=reference_image_url,
                reference_lock_mode=lock_mode,
            )
            await _enforce_1080_image(output_path)
            qa = _score_generated_image_quality(output_path, prompt=prompt, template=template)
            qa_gate_ok, qa_gate_min = _image_quality_gate(
                qa,
                template=template,
                lock_mode=lock_mode,
                has_reference=has_reference,
                prompt=prompt,
            )
            result["qa_score"] = qa.get("score", 0.0)
            result["qa_ok"] = bool(qa_gate_ok)
            result["qa_min_score"] = qa_gate_min
            result["qa_notes"] = qa.get("notes", [])
            if salvage_enabled and template == "story" and not result["qa_ok"]:
                try:
                    salvage_prompt = f"{prompt} {_story_salvage_refinement_for_scene(prompt)}"
                    salvage_path = str(Path(output_path).with_name(Path(output_path).stem + "_salvage" + Path(output_path).suffix))
                    salvage_result = await generate_image_grok(
                        salvage_prompt,
                        salvage_path,
                        resolution=resolution,
                        reference_image_url=reference_image_url,
                    )
                    await _enforce_1080_image(salvage_path)
                    salvage_qa = _score_generated_image_quality(salvage_path, prompt=salvage_prompt, template=template)
                    salvage_gate_ok, salvage_gate_min = _image_quality_gate(
                        salvage_qa,
                        template=template,
                        lock_mode=lock_mode,
                        has_reference=has_reference,
                        prompt=salvage_prompt,
                    )
                    if float(salvage_qa.get("score", 0.0)) > float(result.get("qa_score", 0.0)):
                        shutil.copyfile(salvage_path, output_path)
                        result = dict(salvage_result)
                        result["local_path"] = output_path
                        result["qa_score"] = salvage_qa.get("score", 0.0)
                        result["qa_ok"] = bool(salvage_gate_ok)
                        result["qa_min_score"] = salvage_gate_min
                        result["qa_notes"] = salvage_qa.get("notes", [])
                        log.info(f"Story single-pass salvage improved image score={result['qa_score']}")
                    Path(salvage_path).unlink(missing_ok=True)
                except Exception as salvage_err:
                    log.warning(f"Story single-pass salvage failed: {salvage_err}")
            if salvage_enabled and template == "skeleton" and not result["qa_ok"]:
                for salvage_idx in range(1):
                    try:
                        salvage_prompt = _skeleton_repair_prompt(prompt)
                        salvage_path = str(Path(output_path).with_name(Path(output_path).stem + f"_skeleton_repair_{salvage_idx}" + Path(output_path).suffix))
                        salvage_result = await generate_image_grok(
                            salvage_prompt,
                            salvage_path,
                            resolution=resolution,
                            reference_image_url=reference_image_url,
                            reference_lock_mode=lock_mode,
                        )
                        await _enforce_1080_image(salvage_path)
                        salvage_qa = _score_generated_image_quality(salvage_path, prompt=salvage_prompt, template=template)
                        salvage_gate_ok, salvage_gate_min = _image_quality_gate(
                            salvage_qa,
                            template=template,
                            lock_mode=lock_mode,
                            has_reference=has_reference,
                            prompt=salvage_prompt,
                        )
                        if salvage_gate_ok or float(salvage_qa.get("score", 0.0)) > float(result.get("qa_score", 0.0)):
                            shutil.copyfile(salvage_path, output_path)
                            result = dict(salvage_result)
                            result["local_path"] = output_path
                            result["qa_score"] = salvage_qa.get("score", 0.0)
                            result["qa_ok"] = bool(salvage_gate_ok)
                            result["qa_min_score"] = salvage_gate_min
                            result["qa_notes"] = salvage_qa.get("notes", [])
                            log.info(f"Skeleton single-pass repair {salvage_idx + 1}/1 score={result['qa_score']} ok={result['qa_ok']}")
                        Path(salvage_path).unlink(missing_ok=True)
                        if result["qa_ok"]:
                            break
                    except Exception as salvage_err:
                        log.warning(f"Skeleton single-pass repair {salvage_idx + 1}/1 failed: {salvage_err}")
            if template == "skeleton" and not bool(result.get("qa_ok", False)):
                if interactive_fast:
                    notes = _interactive_soft_accept_notes(result.get("qa_notes", []) or [])
                    if not _skeleton_notes_are_severe(notes):
                        result["qa_notes"] = notes
                        result["qa_ok"] = False
                        return result
                raise RuntimeError(
                    "Skeleton single-pass generation failed QA gate "
                    f"(score={result.get('qa_score', 0.0)}, notes={result.get('qa_notes', [])})"
                )
            return result
        except Exception as e:
            log.warning(f"Grok image generation failed (fal.ai + xAI direct), falling back to SDXL: {e}")

    for provider in provider_order[xai_index + 1:]:
        try:
            local_result = await _local_provider_result(provider)
            if local_result:
                return local_result
        except Exception as local_err:
            last_local_provider_err = local_err
            _record_provider_fallback(provider, "next")
            log.warning(f"Post-xAI local provider '{provider}' failed: {local_err}")

    if template == "skeleton" and interactive_fast:
        if last_local_provider_err is not None:
            raise RuntimeError(str(last_local_provider_err))
        raise RuntimeError("Skeleton interactive generation exhausted WAN2.2 attempt budget")

    safe_route = dict(template_adapter_route or {})
    safe_route["enabled"] = False
    safe_route["loras"] = []
    await generate_image_comfyui(
        prompt,
        output_path,
        resolution=resolution,
        negative_prompt=negative_prompt,
        allow_default_negative=not prompt_passthrough,
        template=template,
        adapter_route=safe_route,
        max_wait_sec=(16 if interactive_fast else 900),
    )
    await _enforce_1080_image(output_path)
    _ensure_generated_image_valid(output_path)
    qa = _score_generated_image_quality(output_path, prompt=prompt, template=template)
    qa_gate_ok, qa_gate_min = _image_quality_gate(
        qa,
        template=template,
        lock_mode=lock_mode,
        has_reference=has_reference,
        prompt=prompt,
    )
    if template == "skeleton" and not qa_gate_ok:
        if interactive_fast:
            notes = _interactive_soft_accept_notes(qa.get("notes", []) or [])
            if not _skeleton_notes_are_severe(notes):
                return {
                    "local_path": output_path,
                    "cdn_url": None,
                    "qa_score": qa.get("score", 0.0),
                    "qa_ok": False,
                    "qa_min_score": qa_gate_min,
                    "qa_notes": notes,
                }
        raise RuntimeError(
            "Skeleton fallback generation failed QA gate "
            f"(score={qa.get('score', 0.0)}, notes={qa.get('notes', [])})"
        )
    return {
        "local_path": output_path,
        "cdn_url": None,
        "qa_score": qa.get("score", 0.0),
        "qa_ok": bool(qa_gate_ok),
        "qa_min_score": qa_gate_min,
        "qa_notes": qa.get("notes", []),
    }


async def generate_image_comfyui(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    negative_prompt: str = "",
    allow_default_negative: bool = True,
    template: str = "",
    adapter_route: Optional[dict] = None,
    max_wait_sec: int = 900,
) -> str:
    """Generate image via ComfyUI SDXL with optional template adapter stack."""
    config = RESOLUTION_CONFIGS[resolution]
    route = _resolve_template_adapter_route(template, override_route=adapter_route)
    checkpoint_name = await _resolve_checkpoint_name(route.get("checkpoint", "sd_xl_base_1.0.safetensors"))
    loras = []
    if route.get("enabled", True):
        loras = await _resolve_available_loras(list(route.get("loras", []) or []))
    if loras:
        lora_names = ", ".join(str(l.get("name", "")) for l in loras)
        log.info(f"Applying adapter route for template '{template or 'default'}': {lora_names}")
    safe_negative_prompt = str(negative_prompt or "").strip()
    if not safe_negative_prompt and allow_default_negative:
        safe_negative_prompt = NEGATIVE_PROMPT
    workflow, output_node = _build_sdxl_workflow_with_loras(
        prompt=str(prompt or "").strip(),
        negative_prompt=safe_negative_prompt,
        width=int(config["gen_width"]),
        height=int(config["gen_height"]),
        checkpoint_name=checkpoint_name,
        loras=loras,
        upscale=bool(config.get("upscale")),
        upscale_factor=float(config.get("upscale_factor", 1.0) or 1.0),
        filename_prefix="nyptid_gen",
    )
    result = await _run_comfyui_workflow(workflow, output_node, "images", max_wait_sec=max_wait_sec, poll_interval_sec=2.0)
    await _download_comfyui_file(result["images"][0], output_path)
    return output_path


async def check_hidream_available(force_refresh: bool = False) -> bool:
    """Best-effort HiDream availability with short TTL and stale-OK guard."""
    now = time.time()
    checked_ts = float(_hidream_availability_cache.get("checked_ts", 0.0) or 0.0)
    if not force_refresh and checked_ts > 0 and (now - checked_ts) <= _HIDREAM_AVAILABILITY_TTL_SEC:
        return bool(_hidream_availability_cache.get("ready", False))
    try:
        assets = await _resolve_hidream_runtime_assets()
        _hidream_availability_cache["checked_ts"] = now
        _hidream_availability_cache["ready"] = True
        _hidream_availability_cache["last_ok_ts"] = now
        _hidream_availability_cache["last_error"] = ""
        _hidream_availability_cache["model_name"] = str((assets or {}).get("model_name", "") or "")
        _hidream_availability_cache["clip_l"] = str((assets or {}).get("clip_l_name", "") or "")
        _hidream_availability_cache["clip_g"] = str((assets or {}).get("clip_g_name", "") or "")
        _hidream_availability_cache["t5_name"] = str((assets or {}).get("t5_name", "") or "")
        _hidream_availability_cache["llama_name"] = str((assets or {}).get("llama_name", "") or "")
        _hidream_availability_cache["vae_name"] = str((assets or {}).get("vae_name", "") or "")
        return True
    except Exception as e:
        _hidream_availability_cache["checked_ts"] = now
        _hidream_availability_cache["ready"] = False
        _hidream_availability_cache["last_error"] = str(e)
        last_ok_ts = float(_hidream_availability_cache.get("last_ok_ts", 0.0) or 0.0)
        if last_ok_ts > 0 and (now - last_ok_ts) <= _HIDREAM_STALE_OK_SEC:
            age = int(now - last_ok_ts)
            log.warning(f"HiDream probe failed but using stale healthy state ({age}s old): {e}")
            return True
        return False


async def check_hidream_edit_available(force_refresh: bool = False) -> bool:
    """Best-effort HiDream E1.1 availability with short TTL and stale-OK guard."""
    now = time.time()
    checked_ts = float(_hidream_edit_availability_cache.get("checked_ts", 0.0) or 0.0)
    if not force_refresh and checked_ts > 0 and (now - checked_ts) <= _HIDREAM_AVAILABILITY_TTL_SEC:
        return bool(_hidream_edit_availability_cache.get("ready", False))
    try:
        assets = await _resolve_hidream_edit_runtime_assets()
        _hidream_edit_availability_cache["checked_ts"] = now
        _hidream_edit_availability_cache["ready"] = True
        _hidream_edit_availability_cache["last_ok_ts"] = now
        _hidream_edit_availability_cache["last_error"] = ""
        _hidream_edit_availability_cache["model_name"] = str((assets or {}).get("model_name", "") or "")
        _hidream_edit_availability_cache["clip_l"] = str((assets or {}).get("clip_l_name", "") or "")
        _hidream_edit_availability_cache["clip_g"] = str((assets or {}).get("clip_g_name", "") or "")
        _hidream_edit_availability_cache["t5_name"] = str((assets or {}).get("t5_name", "") or "")
        _hidream_edit_availability_cache["llama_name"] = str((assets or {}).get("llama_name", "") or "")
        _hidream_edit_availability_cache["vae_name"] = str((assets or {}).get("vae_name", "") or "")
        return True
    except Exception as e:
        _hidream_edit_availability_cache["checked_ts"] = now
        _hidream_edit_availability_cache["ready"] = False
        _hidream_edit_availability_cache["last_error"] = str(e)
        last_ok_ts = float(_hidream_edit_availability_cache.get("last_ok_ts", 0.0) or 0.0)
        if last_ok_ts > 0 and (now - last_ok_ts) <= _HIDREAM_STALE_OK_SEC:
            age = int(now - last_ok_ts)
            log.warning(f"HiDream E1.1 probe failed but using stale healthy state ({age}s old): {e}")
            return True
        return False


async def check_wan22_available() -> bool:
    """Check if the Wan 2.2 I2V models exist on the ComfyUI server."""
    global _active_comfyui_url
    for base_url in _comfyui_candidate_urls():
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(f"{base_url}/object_info")
                if resp.status_code == 200:
                    content = resp.text
                    if WAN22_I2V_HIGH.split(".")[0] in content or "wan2.2" in content.lower():
                        _active_comfyui_url = base_url
                        return True
        except Exception as e:
            log.warning(f"Wan 2.2 availability check failed on {base_url}: {e}")
    return False


async def check_wan22_t2i_available(force_refresh: bool = False) -> bool:
    """Best-effort WAN T2I availability with short TTL and stale-OK guard."""
    now = time.time()
    checked_ts = float(_wan22_t2i_availability_cache.get("checked_ts", 0.0) or 0.0)
    if not force_refresh and checked_ts > 0 and (now - checked_ts) <= _WAN22_T2I_AVAILABILITY_TTL_SEC:
        return bool(_wan22_t2i_availability_cache.get("ready", False))
    try:
        assets = await _resolve_wan22_t2i_runtime_assets()
        _wan22_t2i_availability_cache["checked_ts"] = now
        _wan22_t2i_availability_cache["ready"] = True
        _wan22_t2i_availability_cache["last_ok_ts"] = now
        _wan22_t2i_availability_cache["last_error"] = ""
        _wan22_t2i_availability_cache["mode"] = str((assets or {}).get("mode", "") or "")
        _wan22_t2i_availability_cache["ckpt_name"] = str((assets or {}).get("ckpt_name", "") or "")
        _wan22_t2i_availability_cache["unet_name"] = str((assets or {}).get("unet_name", "") or "")
        return True
    except Exception as e:
        _wan22_t2i_availability_cache["checked_ts"] = now
        _wan22_t2i_availability_cache["ready"] = False
        _wan22_t2i_availability_cache["last_error"] = str(e)
        last_ok_ts = float(_wan22_t2i_availability_cache.get("last_ok_ts", 0.0) or 0.0)
        if last_ok_ts > 0 and (now - last_ok_ts) <= _WAN22_T2I_STALE_OK_SEC:
            age = int(now - last_ok_ts)
            log.warning(
                f"WAN22 T2I probe failed but using stale healthy state ({age}s old): {e}"
            )
            return True
        return False


RUNPOD_SSH_HOST = "root@69.30.85.41"
RUNPOD_SSH_PORT = "22118"
COMFYUI_INPUT_DIR = "/workspace/ComfyUI/input"

FAL_SUBMIT_URL = "https://queue.fal.run/fal-ai/kling-video/v2.1/standard/image-to-video"
FAL_STATUS_URL = "https://queue.fal.run/fal-ai/kling-video/v2.1/standard/image-to-video/requests"
FAL_UPLOAD_URL = "https://fal.run/fal-ai/fal-file-storage/upload"


async def _upload_image_to_fal(image_path: str) -> str:
    """Upload a local image to fal.ai CDN and return a public URL for it."""
    if not FAL_AI_KEY:
        raise RuntimeError("FAL_AI_KEY not configured")
    headers = {"Authorization": "Key " + FAL_AI_KEY}
    img_bytes = Path(image_path).read_bytes()
    filename = "nyptid_" + str(int(time.time() * 1000)) + ".png"
    upload_url = "https://fal.ai/api/storage/upload/initiate"

    async with httpx.AsyncClient(timeout=90) as client:
        resp = await client.post(
            upload_url,
            headers={**headers, "Accept": "application/json", "Content-Type": "application/json"},
            json={"file_name": filename, "content_type": "image/png"},
        )
        if resp.status_code == 200:
            upload_info = resp.json()
            presigned = upload_info.get("upload_url") or upload_info.get("presigned_url")
            file_url = upload_info.get("file_url")
            if presigned and file_url:
                put_resp = await client.put(presigned, content=img_bytes, headers={"Content-Type": "image/png"})
                if put_resp.status_code in (200, 201):
                    log.info(f"Image uploaded to fal.ai CDN: {file_url[:80]}")
                    return file_url

        target_path = "uploads/" + filename
        rest_url = "https://api.fal.ai/v1/serverless/files/file/local/" + target_path
        import io
        files = {"file_upload": (filename, io.BytesIO(img_bytes), "image/png")}
        resp2 = await client.post(rest_url, headers=headers, files=files)
        if resp2.status_code in (200, 201):
            cdn_url = "https://api.fal.ai/v1/serverless/files/file/" + target_path
            log.info(f"Image uploaded to fal.ai REST: {cdn_url}")
            return cdn_url

    import base64
    log.warning("fal.ai CDN upload failed, using data URL fallback")
    b64 = base64.b64encode(img_bytes).decode()
    return "data:image/png;base64," + b64


async def animate_image_kling(image_path: str, prompt: str, output_clip_path: str, duration: str = "5", aspect_ratio: str = "9:16", image_cdn_url: str = None) -> str:
    """Use fal.ai Kling 2.1 Standard I2V to animate an image into a video clip.
    If image_cdn_url is provided (from Grok Imagine), skip the upload step.
    Returns the local path to the downloaded MP4 clip.
    """
    if not FAL_AI_KEY:
        raise RuntimeError("FAL_AI_KEY not configured")

    if image_cdn_url:
        image_url = image_cdn_url
        log.info("Kling I2V: using existing CDN URL (from Grok Imagine)")
    else:
        image_url = await _upload_image_to_fal(image_path)

    log.info(f"Kling I2V: submitting job (duration={duration}s, ar={aspect_ratio})")

    headers = {
        "Authorization": "Key " + FAL_AI_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "prompt": prompt,
        "image_url": image_url,
        "duration": duration,
        "aspect_ratio": aspect_ratio,
        "negative_prompt": "blur, distort, low quality, watermark, text overlay, UI elements",
        "cfg_scale": 0.5,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(FAL_SUBMIT_URL, headers=headers, json=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError("Kling submit failed (" + str(resp.status_code) + "): " + resp.text[:300])
        submit_data = resp.json()

    request_id = submit_data.get("request_id")
    if not request_id:
        if submit_data.get("video", {}).get("url"):
            video_url = submit_data["video"]["url"]
            await _download_url_to_file(video_url, output_clip_path)
            return output_clip_path
        raise RuntimeError("No request_id from Kling submit: " + json.dumps(submit_data)[:300])

    log.info(f"Kling I2V queued: request_id={request_id}")
    status_url = submit_data.get("status_url", FAL_STATUS_URL + "/" + request_id + "/status")
    result_url = submit_data.get("response_url", FAL_STATUS_URL + "/" + request_id)

    max_wait = 600
    poll_interval = 5
    elapsed = 0
    while elapsed < max_wait:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
        async with httpx.AsyncClient(timeout=30) as client:
            st_resp = await client.get(status_url, headers={"Authorization": "Key " + FAL_AI_KEY})
            if st_resp.status_code == 202:
                st_data = st_resp.json()
                status = st_data.get("status", "IN_PROGRESS")
                if elapsed % 30 == 0:
                    log.info(f"Kling I2V waiting... {elapsed}s elapsed, status={status}")
                continue
            if st_resp.status_code != 200:
                log.warning(f"Kling status poll HTTP {st_resp.status_code}: {st_resp.text[:200]}")
                continue
            st_data = st_resp.json()
            status = st_data.get("status", "")
            if status == "COMPLETED":
                break
            if status in ("FAILED", "CANCELLED"):
                raise RuntimeError("Kling generation failed: " + json.dumps(st_data)[:300])
            if elapsed % 30 == 0:
                log.info(f"Kling I2V waiting... {elapsed}s elapsed, status={status}")
        if poll_interval < 15:
            poll_interval = min(poll_interval + 2, 15)
    else:
        raise TimeoutError("Kling I2V timed out after " + str(max_wait) + "s")

    async with httpx.AsyncClient(timeout=60) as client:
        res_resp = await client.get(result_url, headers={"Authorization": "Key " + FAL_AI_KEY})
        if res_resp.status_code != 200:
            # Fallback: some queue variants expose result at status_url without the trailing /status.
            fallback_result_url = status_url[:-7] if status_url.endswith("/status") else ""
            if fallback_result_url:
                res_resp = await client.get(fallback_result_url, headers={"Authorization": "Key " + FAL_AI_KEY})
            if res_resp.status_code != 200:
                raise RuntimeError("Kling result fetch failed: " + str(res_resp.status_code))
        result_data = res_resp.json()

    video_url = result_data.get("video", {}).get("url")
    if not video_url:
        raise RuntimeError("No video URL in Kling result: " + json.dumps(result_data)[:300])

    log.info(f"Kling I2V complete, downloading video from {video_url[:80]}...")
    await _download_url_to_file(video_url, output_clip_path)
    log.info(f"Kling clip saved: {output_clip_path} ({Path(output_clip_path).stat().st_size / 1024:.0f} KB)")
    return output_clip_path


async def _download_url_to_file(url: str, output_path: str):
    """Download a file from a URL to a local path using streaming writes."""
    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        async with client.stream("GET", url) as resp:
            if resp.status_code != 200:
                raise RuntimeError("Download failed (" + str(resp.status_code) + ") from " + url[:100])
            with open(output_path, "wb") as f:
                async for chunk in resp.aiter_bytes(chunk_size=1024 * 256):
                    if chunk:
                        f.write(chunk)


async def animate_image_grok_video(image_path: str, prompt: str, output_clip_path: str, duration_sec: float = 5, aspect_ratio: str = "9:16", image_cdn_url: str = None) -> str:
    """Animate an image via xAI Grok Imagine Video and download resulting MP4."""
    if not XAI_API_KEY:
        raise RuntimeError("XAI_API_KEY not configured")
    headers = {"Authorization": f"Bearer {XAI_API_KEY}", "Content-Type": "application/json"}
    duration = max(1, min(int(round(float(duration_sec))), 15))
    image_url = image_cdn_url or _file_to_data_image_url(image_path)
    if not image_url:
        raise RuntimeError("No source image URL for Grok video")

    payload = {
        "model": XAI_VIDEO_MODEL,
        "prompt": prompt,
        "duration": duration,
        "aspect_ratio": aspect_ratio,
        "resolution": "720p",
        "image": {"url": image_url},
    }
    async with httpx.AsyncClient(timeout=60) as client:
        submit = await client.post("https://api.x.ai/v1/videos/generations", headers=headers, json=payload)
        if submit.status_code not in (200, 201):
            raise RuntimeError(f"Grok video submit failed ({submit.status_code}): {submit.text[:300]}")
        submit_data = submit.json()
    request_id = submit_data.get("request_id")
    if not request_id:
        raise RuntimeError("Grok video submit returned no request_id")

    poll_url = f"https://api.x.ai/v1/videos/{request_id}"
    max_wait = 900
    elapsed = 0
    while elapsed < max_wait:
        await asyncio.sleep(4)
        elapsed += 4
        async with httpx.AsyncClient(timeout=30) as client:
            status_resp = await client.get(poll_url, headers={"Authorization": f"Bearer {XAI_API_KEY}"})
            if status_resp.status_code != 200:
                continue
            status_data = status_resp.json()
        status = str(status_data.get("status", "")).lower()
        if status == "done":
            video_url = status_data.get("video", {}).get("url")
            if not video_url:
                raise RuntimeError("Grok video done status missing video URL")
            await _download_url_to_file(video_url, output_clip_path)
            return output_clip_path
        if status == "expired":
            raise RuntimeError("Grok video request expired")
    raise TimeoutError("Grok video timed out")


def _aspect_ratio_to_runway_ratio(aspect_ratio: str) -> str:
    if aspect_ratio == "9:16":
        return "720:1280"
    if aspect_ratio == "16:9":
        return "1280:720"
    allowed = {"1280:720", "720:1280", "1104:832", "960:960", "832:1104", "1584:672"}
    return aspect_ratio if aspect_ratio in allowed else "720:1280"


async def animate_image_runway_video(image_path: str, prompt: str, output_clip_path: str, duration_sec: float = 5, aspect_ratio: str = "9:16", image_cdn_url: str = None) -> str:
    """Animate an image via Runway image-to-video and download resulting MP4."""
    if not RUNWAY_API_KEY:
        raise RuntimeError("RUNWAY_API_KEY not configured")
    headers = {
        "Authorization": f"Bearer {RUNWAY_API_KEY}",
        "Content-Type": "application/json",
        "X-Runway-Version": RUNWAY_API_VERSION,
    }
    duration = max(2, min(int(round(float(duration_sec))), 10))
    image_url = image_cdn_url or _file_to_data_image_url(image_path, max_bytes=3_700_000)
    if not image_url:
        raise RuntimeError("No source image URL for Runway video (image too large for inline data URI; provide CDN URL)")

    payload = {
        "model": RUNWAY_VIDEO_MODEL,
        "promptText": prompt,
        "promptImage": image_url,
        "ratio": _aspect_ratio_to_runway_ratio(aspect_ratio),
        "duration": duration,
    }
    async with httpx.AsyncClient(timeout=60) as client:
        submit = await client.post("https://api.dev.runwayml.com/v1/image_to_video", headers=headers, json=payload)
        if submit.status_code not in (200, 201):
            raise RuntimeError(f"Runway submit failed ({submit.status_code}): {submit.text[:300]}")
        submit_data = submit.json()
    task_id = submit_data.get("id")
    if not task_id:
        raise RuntimeError("Runway submit returned no task id")

    poll_url = f"https://api.dev.runwayml.com/v1/tasks/{task_id}"
    max_wait = 900
    elapsed = 0
    while elapsed < max_wait:
        await asyncio.sleep(5)
        elapsed += 5
        async with httpx.AsyncClient(timeout=30) as client:
            status_resp = await client.get(poll_url, headers=headers)
            if status_resp.status_code != 200:
                continue
            status_data = status_resp.json()
        status = str(status_data.get("status", "")).upper()
        if status == "SUCCEEDED":
            output = status_data.get("output")
            video_url = None
            if isinstance(output, list) and output:
                first = output[0]
                if isinstance(first, str):
                    video_url = first
                elif isinstance(first, dict):
                    video_url = first.get("url") or first.get("uri")
            elif isinstance(output, dict):
                video_url = output.get("url") or output.get("uri") or output.get("video")
            if not video_url:
                raise RuntimeError("Runway done status missing output URL")
            await _download_url_to_file(video_url, output_clip_path)
            return output_clip_path
        if status in ("FAILED", "CANCELLED"):
            raise RuntimeError("Runway task failed: " + json.dumps(status_data)[:300])
    raise TimeoutError("Runway video timed out")


async def animate_scene(image_path: str, prompt: str, output_dir_path: str, scene_idx: int, job_ts: str, duration_sec: float = 5, num_frames: int = 81, image_cdn_url: str = None, prefer_wan: bool = False) -> dict:
    """Animate a scene image, preferring local Wan 2.2 for skeleton flows when requested."""
    provider_errors = []
    try:
        requested_duration = float(duration_sec)
    except Exception:
        requested_duration = 5.0
    # FalAI Kling I2V accepts only 5s or 10s durations.
    kling_duration = 10 if requested_duration >= 7.5 else 5

    if prefer_wan:
        try:
            wan_ready = await check_wan22_available()
        except Exception:
            wan_ready = False
        if wan_ready:
            wan_clip_path = str(Path(output_dir_path) / ("wan_scene_" + str(scene_idx) + "_" + job_ts + ".mp4"))
            try:
                # 5s @ ~16fps is ~80 frames, keep slightly above for smoothness.
                target_frames = max(49, int(round(max(3.0, requested_duration) * 16)))
                await animate_image_wan22(
                    image_path=image_path,
                    prompt=prompt,
                    output_clip_path=wan_clip_path,
                    num_frames=target_frames,
                )
                return {"type": "wan_clip", "path": wan_clip_path}
            except Exception as e:
                provider_errors.append("wan22: " + str(e))
                log.warning(f"Wan 2.2 scene animation failed, trying fallback providers: {e}")

    if RUNWAY_API_KEY:
        runway_clip_path = str(Path(output_dir_path) / ("runway_scene_" + str(scene_idx) + "_" + job_ts + ".mp4"))
        try:
            await animate_image_runway_video(
                image_path=image_path,
                prompt=prompt,
                output_clip_path=runway_clip_path,
                duration_sec=requested_duration,
                image_cdn_url=image_cdn_url,
            )
            return {"type": "runway_clip", "path": runway_clip_path}
        except Exception as e:
            provider_errors.append("runway: " + str(e))
            log.warning(f"Runway scene animation failed, trying FalAI Kling fallback: {e}")

    if FAL_AI_KEY:
        kling_clip_path = str(Path(output_dir_path) / ("kling_scene_" + str(scene_idx) + "_" + job_ts + ".mp4"))
        try:
            await animate_image_kling(
                image_path,
                prompt,
                kling_clip_path,
                duration=str(kling_duration),
                aspect_ratio="9:16",
                image_cdn_url=image_cdn_url,
            )
            return {"type": "kling_clip", "path": kling_clip_path}
        except Exception as e:
            provider_errors.append("fal_kling: " + str(e))
            log.warning(f"FalAI Kling scene animation failed: {e}")

    if not provider_errors:
        raise RuntimeError("No video engine configured (set RUNWAY_API_KEY or FAL_AI_KEY)")
    raise RuntimeError("All video providers failed: " + " | ".join(provider_errors))


async def _upload_image_to_comfyui(image_path: str) -> str:
    """Upload an image to ComfyUI's input directory via HTTP API."""
    src = Path(image_path)
    suffix = src.suffix.lower() or ".png"
    mime = "image/png"
    if suffix in {".jpg", ".jpeg"}:
        mime = "image/jpeg"
        suffix = ".jpg"
    elif suffix == ".webp":
        mime = "image/webp"
    filename = "nyptid_scene_" + str(int(time.time() * 1000)) + suffix
    img_bytes = src.read_bytes()
    global _active_comfyui_url
    last_error = None
    for base_url in _comfyui_candidate_urls():
        try:
            async with httpx.AsyncClient(timeout=120) as client:
                resp = await client.post(
                    f"{base_url}/upload/image",
                    files={"image": (filename, img_bytes, mime)},
                    data={"overwrite": "true"},
                )
                if resp.status_code != 200:
                    raise RuntimeError(f"ComfyUI image upload failed ({resp.status_code}): {resp.text[:200]}")
                result = resp.json()
                uploaded_name = result.get("name", filename)
                _active_comfyui_url = base_url
                log.info(f"Image uploaded to ComfyUI via HTTP: {uploaded_name} ({base_url})")
                return uploaded_name
        except Exception as e:
            last_error = e
            continue
    raise RuntimeError(f"ComfyUI image upload failed on all endpoints: {last_error}")


async def animate_image_wan22(image_path: str, prompt: str, output_clip_path: str, num_frames: int = 81) -> str:
    """Animate an image via Wan 2.2 I2V on ComfyUI. Returns path to downloaded MP4."""
    uploaded_name = await _upload_image_to_comfyui(image_path)

    workflow = {
        "1": {
            "class_type": "UNETLoader",
            "inputs": {
                "unet_name": WAN22_I2V_HIGH,
                "weight_dtype": "fp8_e4m3fn",
            },
        },
        "3": {
            "class_type": "CLIPLoader",
            "inputs": {
                "clip_name": "umt5_xxl_fp8_e4m3fn_scaled.safetensors",
                "type": "wan",
            },
        },
        "4": {
            "class_type": "CLIPTextEncode",
            "inputs": {
                "text": prompt,
                "clip": ["3", 0],
            },
        },
        "5": {
            "class_type": "VAELoader",
            "inputs": {"vae_name": "wan2.2_vae.safetensors"},
        },
        "6": {
            "class_type": "CLIPVisionLoader",
            "inputs": {"clip_name": "clip_vision_h.safetensors"},
        },
        "6b": {
            "class_type": "CLIPVisionEncode",
            "inputs": {
                "clip_vision": ["6", 0],
                "image": ["7", 0],
                "crop": "center",
            },
        },
        "7": {
            "class_type": "LoadImage",
            "inputs": {"image": uploaded_name},
        },
        "8": {
            "class_type": "WanImageToVideo",
            "inputs": {
                "positive": ["4", 0],
                "negative": ["12", 0],
                "vae": ["5", 0],
                "width": 480,
                "height": 832,
                "length": num_frames,
                "batch_size": 1,
                "clip_vision_output": ["6b", 0],
                "start_image": ["7", 0],
            },
        },
        "11": {
            "class_type": "KSampler",
            "inputs": {
                "seed": random.randint(0, 2**32),
                "steps": 30,
                "cfg": 3.0,
                "sampler_name": "uni_pc_bh2",
                "scheduler": "simple",
                "denoise": 1.0,
                "model": ["1", 0],
                "positive": ["8", 0],
                "negative": ["8", 1],
                "latent_image": ["8", 2],
            },
        },
        "12": {
            "class_type": "CLIPTextEncode",
            "inputs": {
                "text": "",
                "clip": ["3", 0],
            },
        },
        "13": {
            "class_type": "VAEDecode",
            "inputs": {
                "samples": ["11", 0],
                "vae": ["5", 0],
            },
        },
        "15": {
            "class_type": "CreateVideo",
            "inputs": {
                "images": ["13", 0],
                "fps": 16.0,
            },
        },
        "16": {
            "class_type": "SaveVideo",
            "inputs": {
                "video": ["15", 0],
                "filename_prefix": "nyptid_wan",
                "format": "mp4",
                "codec": "h264",
            },
        },
    }

    result = await _run_comfyui_workflow(workflow, "16", "videos")

    if not result.get("videos"):
        result = await _run_comfyui_workflow(workflow, "16", "gifs")
        if not result.get("gifs"):
            raise RuntimeError("Wan 2.2 produced no video output")
        vid_info = result["gifs"][0]
    else:
        vid_info = result["videos"][0]

    await _download_comfyui_file(vid_info, output_clip_path)
    file_size = Path(output_clip_path).stat().st_size
    log.info(f"Wan 2.2 video saved: {output_clip_path} ({file_size / 1024:.0f} KB)")
    return output_clip_path


# ─── FFmpeg Video Compositor ──────────────────────────────────────────────────

async def frames_to_clip(frame_paths: list, duration: float, output_clip: str, out_w: int, out_h: int, text_overlay: str = "", resolution: str = "720p") -> str:
    """Convert SVD frames into a video clip, stretched/looped to fill the scene duration."""
    frame_dir = Path(frame_paths[0]).parent
    num_frames = len(frame_paths)
    native_fps = 8
    native_duration = num_frames / native_fps

    drawtext = ""
    if text_overlay:
        safe_text = _ffmpeg_safe_text(text_overlay).upper()
        font_size = 96 if resolution == "1080p" else 72
        border_w = 6 if resolution == "1080p" else 4
        drawtext = (
            ",drawtext=text='" + safe_text + "'"
            + ":fontsize=" + str(font_size) + ":fontcolor=white:borderw=" + str(border_w) + ":bordercolor=black"
            + ":x=(w-text_w)/2:y=h*3/4"
            + ":fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        )

    speed_factor = native_duration / duration if duration > 0 else 1.0
    speed_factor = max(0.25, min(speed_factor, 4.0))

    first_frame = Path(frame_paths[0]).name
    prefix = first_frame.rsplit("_", 1)[0]
    input_pattern = str(frame_dir / f"{prefix}_%04d.png")
    cmd = [
        "ffmpeg", "-y",
        "-framerate", str(native_fps),
        "-i", input_pattern,
        "-t", str(duration),
        "-vf", (
            f"setpts={1.0/speed_factor}*PTS,"
            f"scale={out_w}:{out_h}:force_original_aspect_ratio=decrease,"
            f"pad={out_w}:{out_h}:(ow-iw)/2:(oh-ih)/2:black,"
            f"format=yuv420p"
            f"{drawtext}"
        ),
        "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
        "-r", "30",
        str(output_clip),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        log.warning(f"SVD clip ffmpeg error: {stderr.decode()[:500]}")
        raise RuntimeError("Failed to create clip from SVD frames")
    return output_clip


def _ffmpeg_safe_text(text: str) -> str:
    """Escape text for FFmpeg drawtext filter."""
    import re
    t = re.sub(r"[^\w\s.,!?\-+=#&]", "", text)
    t = t.replace(":", "\\:").replace("'", "").replace("%", "")
    return t


async def static_image_to_clip(img_path: str, duration: float, output_clip: str, out_w: int, out_h: int, text_overlay: str = "", resolution: str = "720p") -> str:
    """Fallback: create a video clip from a static image with slow zoom."""
    base_vf = "scale=" + str(out_w) + ":" + str(out_h) + ":force_original_aspect_ratio=decrease,pad=" + str(out_w) + ":" + str(out_h) + ":(ow-iw)/2:(oh-ih)/2:black,format=yuv420p"

    drawtext_vf = base_vf
    if text_overlay:
        safe_text = _ffmpeg_safe_text(text_overlay).upper()
        font_size = 96 if resolution == "1080p" else 72
        border_w = 6 if resolution == "1080p" else 4
        drawtext_vf = (
            base_vf
            + ",drawtext=text='" + safe_text + "'"
            + ":fontsize=" + str(font_size) + ":fontcolor=white:borderw=" + str(border_w) + ":bordercolor=black"
            + ":x=(w-text_w)/2:y=h*3/4"
            + ":fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        )

    cmd = [
        "ffmpeg", "-y",
        "-loop", "1", "-i", str(img_path),
        "-t", str(duration),
        "-vf", drawtext_vf,
        "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
        "-r", "30",
        str(output_clip),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()

    if proc.returncode != 0 and text_overlay:
        log.warning(f"Drawtext failed, retrying without text overlay: {stderr.decode()[-200:]}")
        cmd_plain = [
            "ffmpeg", "-y",
            "-loop", "1", "-i", str(img_path),
            "-t", str(duration),
            "-vf", base_vf,
            "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
            "-r", "30",
            str(output_clip),
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd_plain, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()

    if proc.returncode != 0:
        err = stderr.decode()[-300:]
        log.error(f"FFmpeg static clip error: {err}")
        raise RuntimeError("FFmpeg failed on static image clip: " + err[-100:])
    return output_clip


async def kling_clip_to_scene(kling_clip: str, duration: float, output_clip: str, out_w: int, out_h: int, text_overlay: str = "", resolution: str = "720p") -> str:
    """Re-encode a Kling MP4 clip to exact output dimensions, trim/loop to duration, add text overlay."""
    drawtext = ""
    if text_overlay:
        safe_text = _ffmpeg_safe_text(text_overlay).upper()
        font_size = 96 if resolution == "1080p" else 72
        border_w = 6 if resolution == "1080p" else 4
        drawtext = (
            ",drawtext=text='" + safe_text + "'"
            + ":fontsize=" + str(font_size) + ":fontcolor=white:borderw=" + str(border_w) + ":bordercolor=black"
            + ":x=(w-text_w)/2:y=h*3/4"
            + ":fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        )
    vf = (
        "scale=" + str(out_w) + ":" + str(out_h) + ":force_original_aspect_ratio=decrease,"
        + "pad=" + str(out_w) + ":" + str(out_h) + ":(ow-iw)/2:(oh-ih)/2:black,"
        + "format=yuv420p"
        + drawtext
    )
    cmd = [
        "ffmpeg", "-y",
        "-i", str(kling_clip),
        "-t", str(duration),
        "-vf", vf,
        "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
        "-r", "30",
        str(output_clip),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0 and text_overlay:
        log.warning(f"Kling clip drawtext failed, retrying without: {stderr.decode()[-200:]}")
        vf_plain = (
            "scale=" + str(out_w) + ":" + str(out_h) + ":force_original_aspect_ratio=decrease,"
            + "pad=" + str(out_w) + ":" + str(out_h) + ":(ow-iw)/2:(oh-ih)/2:black,"
            + "format=yuv420p"
        )
        cmd[cmd.index("-vf") + 1] = vf_plain
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()
    if proc.returncode != 0:
        err = stderr.decode()[-300:]
        log.error(f"FFmpeg Kling clip error: {err}")
        raise RuntimeError("FFmpeg failed on Kling clip: " + err[-100:])
    return output_clip


async def _merge_sfx_track(sfx_paths: list[str], scenes: list, output_path: str) -> str:
    """Concatenate per-scene SFX clips into one continuous audio track.
    Pads missing scenes with silence to keep timing aligned."""
    job_ts = str(int(time.time() * 1000))
    padded_clips = []

    for i, scene in enumerate(scenes):
        dur = scene.get("duration_sec", 5)
        if i == len(scenes) - 1:
            dur += 1.0
        sfx = sfx_paths[i] if i < len(sfx_paths) else ""

        padded_path = str(TEMP_DIR / f"sfx_pad_{i}_{job_ts}.mp3")
        if sfx and Path(sfx).exists() and Path(sfx).stat().st_size > 0:
            cmd = [
                "ffmpeg", "-y", "-i", sfx,
                "-af", f"apad=whole_dur={dur},afade=t=in:st=0:d=0.15,afade=t=out:st={max(dur - 0.3, 0)}:d=0.3",
                "-t", str(dur), "-ar", "44100", "-ac", "1",
                padded_path,
            ]
        else:
            cmd = [
                "ffmpeg", "-y", "-f", "lavfi", "-i", f"anullsrc=r=44100:cl=mono",
                "-t", str(dur), padded_path,
            ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await proc.communicate()
        padded_clips.append(padded_path)

    concat_file = TEMP_DIR / f"sfx_concat_{job_ts}.txt"
    with open(concat_file, "w") as f:
        for p in padded_clips:
            f.write(f"file '{Path(p).resolve()}'\n")

    cmd = [
        "ffmpeg", "-y", "-f", "concat", "-safe", "0",
        "-i", str(concat_file), "-c:a", "libmp3lame", "-ar", "44100",
        output_path,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    await proc.communicate()

    for p in padded_clips:
        Path(p).unlink(missing_ok=True)
    concat_file.unlink(missing_ok=True)

    if Path(output_path).exists() and Path(output_path).stat().st_size > 0:
        log.info(f"SFX track merged: {output_path}")
        return output_path
    return ""


def _audio_track_exists(path: str) -> bool:
    p = Path(str(path or "").strip())
    return bool(p and p.exists() and p.stat().st_size > 0)


async def _generate_spooky_bgm_track(total_duration_sec: float, output_path: str, whisper_mode: str = "subtle") -> str:
    """Create a spooky background music bed for horror long-form videos."""
    duration = max(6.0, float(total_duration_sec or 0.0))
    fade_out_start = max(0.0, duration - 2.0)
    seed_clip = str(TEMP_DIR / f"bgm_seed_{int(time.time() * 1000)}.mp3")
    mood_hint = "subtle" if whisper_mode == "off" else ("cinematic tension" if whisper_mode == "cinematic" else "dark atmospheric")
    bgm_prompt = (
        "Instrumental cinematic horror ambience music bed, haunting low drones, distant eerie textures, "
        "slow tension build, no vocals, no speech, no abrupt stingers, seamless loop quality, "
        f"{mood_hint} tone under narration"
    )

    try:
        if ELEVENLABS_API_KEY:
            async with httpx.AsyncClient(timeout=90) as client:
                resp = await client.post(
                    "https://api.elevenlabs.io/v1/sound-generation",
                    headers={
                        "xi-api-key": ELEVENLABS_API_KEY,
                        "Content-Type": "application/json",
                    },
                    json={
                        "text": bgm_prompt,
                        "duration_seconds": 22.0,
                        "prompt_influence": 0.62,
                    },
                )
            if resp.status_code in (200, 201):
                with open(seed_clip, "wb") as f:
                    f.write(resp.content)
                if _audio_track_exists(seed_clip):
                    cmd = [
                        "ffmpeg",
                        "-y",
                        "-stream_loop",
                        "-1",
                        "-i",
                        seed_clip,
                        "-t",
                        str(duration),
                        "-af",
                        f"afade=t=in:st=0:d=0.8,afade=t=out:st={fade_out_start}:d=2.0,apad=pad_dur=0.8",
                        "-ar",
                        "44100",
                        "-ac",
                        "1",
                        output_path,
                    ]
                    proc = await asyncio.create_subprocess_exec(
                        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                    )
                    await proc.communicate()
                    if proc.returncode == 0 and _audio_track_exists(output_path):
                        return output_path
        # Fallback: synthesize a low-key eerie bed locally.
        cmd = [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            f"sine=frequency=54:sample_rate=44100:duration={duration}",
            "-f",
            "lavfi",
            "-i",
            f"sine=frequency=81:sample_rate=44100:duration={duration}",
            "-f",
            "lavfi",
            "-i",
            f"anoisesrc=color=pink:amplitude=0.025:sample_rate=44100:duration={duration}",
            "-filter_complex",
            (
                "[0:a]volume=0.16[a0];"
                "[1:a]volume=0.09,lowpass=f=900[a1];"
                "[2:a]highpass=f=140,lowpass=f=1500,volume=0.10[a2];"
                f"[a0][a1][a2]amix=inputs=3:duration=longest:normalize=0,"
                f"afade=t=in:st=0:d=1.0,afade=t=out:st={fade_out_start}:d=2.0,apad=pad_dur=0.8[aout]"
            ),
            "-map",
            "[aout]",
            "-ar",
            "44100",
            "-ac",
            "1",
            output_path,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await proc.communicate()
        if proc.returncode == 0 and _audio_track_exists(output_path):
            return output_path
    except Exception as e:
        log.warning(f"Horror BGM generation failed (non-fatal): {e}")
    finally:
        Path(seed_clip).unlink(missing_ok=True)

    return ""


async def _mix_ambience_tracks(sfx_track: str, bgm_track: str, output_path: str) -> str:
    """Mix scene SFX and background music into one ambience track."""
    has_sfx = _audio_track_exists(sfx_track)
    has_bgm = _audio_track_exists(bgm_track)
    if not has_sfx and not has_bgm:
        return ""

    if has_sfx and has_bgm:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            sfx_track,
            "-i",
            bgm_track,
            "-filter_complex",
            (
                "[0:a]volume=1.0[sfx];"
                "[1:a]volume=0.55,highpass=f=40,lowpass=f=6500[bgm];"
                "[sfx][bgm]amix=inputs=2:duration=longest:dropout_transition=2,"
                "alimiter=limit=0.93,apad=pad_dur=0.8[aout]"
            ),
            "-map",
            "[aout]",
            "-c:a",
            "libmp3lame",
            "-ar",
            "44100",
            "-ac",
            "1",
            output_path,
        ]
    else:
        src = sfx_track if has_sfx else bgm_track
        # Normalize to one consistent MP3 stream for downstream ffmpeg graph.
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            src,
            "-af",
            "apad=pad_dur=0.8",
            "-c:a",
            "libmp3lame",
            "-ar",
            "44100",
            "-ac",
            "1",
            output_path,
        ]

    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    await proc.communicate()
    if proc.returncode == 0 and _audio_track_exists(output_path):
        return output_path
    return ""


async def _composite_video_on_runpod(
    scene_clips: list[Path],
    audio_path: str,
    output_path: str,
    subtitle_path: str = None,
    sfx_track: str = "",
    transition_style: str = "smooth",
    clip_durations: list[float] | None = None,
) -> str:
    """Run final concat + merge on RunPod to reduce Render RAM pressure."""
    run_id = f"cmp_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
    remote_dir = f"{RUNPOD_COMPOSITOR_BASE_DIR.rstrip('/')}/{run_id}"
    remote_merged = f"{remote_dir}/merged.mp4"
    remote_output = f"{remote_dir}/final.mp4"

    ok, err = await asyncio.to_thread(
        _run_remote_cmd_blocking,
        RUNPOD_COMPOSITOR_HOST,
        RUNPOD_COMPOSITOR_SSH_PORT,
        f"mkdir -p '{remote_dir}'",
    )
    if not ok:
        raise RuntimeError(f"RunPod mkdir failed: {err}")

    local_concat = TEMP_DIR / f"remote_concat_{run_id}.txt"
    local_concat.write_text("", encoding="utf-8")
    uploaded_remote_files = []
    try:
        for i, clip in enumerate(scene_clips):
            remote_clip = f"{remote_dir}/scene_{i}.mp4"
            up_ok, up_err = await asyncio.to_thread(
                _upload_file_to_runpod_blocking,
                RUNPOD_COMPOSITOR_HOST,
                RUNPOD_COMPOSITOR_SSH_PORT,
                str(clip),
                remote_clip,
            )
            if not up_ok:
                raise RuntimeError(f"RunPod clip upload failed ({clip.name}): {up_err}")
            uploaded_remote_files.append(remote_clip)

        remote_audio = f"{remote_dir}/voice.mp3"
        up_ok, up_err = await asyncio.to_thread(
            _upload_file_to_runpod_blocking,
            RUNPOD_COMPOSITOR_HOST,
            RUNPOD_COMPOSITOR_SSH_PORT,
            audio_path,
            remote_audio,
        )
        if not up_ok:
            raise RuntimeError(f"RunPod audio upload failed: {up_err}")

        remote_sub = ""
        if subtitle_path and Path(subtitle_path).exists():
            remote_sub = f"{remote_dir}/captions.ass"
            up_ok, up_err = await asyncio.to_thread(
                _upload_file_to_runpod_blocking,
                RUNPOD_COMPOSITOR_HOST,
                RUNPOD_COMPOSITOR_SSH_PORT,
                subtitle_path,
                remote_sub,
            )
            if not up_ok:
                raise RuntimeError(f"RunPod subtitle upload failed: {up_err}")

        remote_sfx = ""
        if sfx_track and Path(sfx_track).exists():
            remote_sfx = f"{remote_dir}/sfx.mp3"
            up_ok, up_err = await asyncio.to_thread(
                _upload_file_to_runpod_blocking,
                RUNPOD_COMPOSITOR_HOST,
                RUNPOD_COMPOSITOR_SSH_PORT,
                sfx_track,
                remote_sfx,
            )
            if not up_ok:
                raise RuntimeError(f"RunPod sfx upload failed: {up_err}")

        concat_lines = "".join([f"file '{p}'\n" for p in uploaded_remote_files])
        local_concat.write_text(concat_lines, encoding="utf-8")
        remote_concat = f"{remote_dir}/concat.txt"
        up_ok, up_err = await asyncio.to_thread(
            _upload_file_to_runpod_blocking,
            RUNPOD_COMPOSITOR_HOST,
            RUNPOD_COMPOSITOR_SSH_PORT,
            str(local_concat),
            remote_concat,
        )
        if not up_ok:
            raise RuntimeError(f"RunPod concat upload failed: {up_err}")

        style = _normalize_transition_style(transition_style)
        xfade_type = TRANSITION_STYLE_MAP.get(style, "fade")
        transition_dur = _transition_duration_for_style(style)
        use_xfade = (style != "no_motion" and style != "none" and len(uploaded_remote_files) > 1)
        if use_xfade:
            durations = list(clip_durations or [])
            while len(durations) < len(uploaded_remote_files):
                durations.append(5.0)
            durations = [max(0.4, float(d or 5.0)) for d in durations[:len(uploaded_remote_files)]]
            ff_inputs = " ".join([f"-i '{p}'" for p in uploaded_remote_files])
            parts = []
            cumulative = durations[0]
            for idx in range(1, len(uploaded_remote_files)):
                left = "0:v" if idx == 1 else f"v{idx-1}"
                out = f"v{idx}"
                offset = max(0.0, cumulative - transition_dur * idx)
                parts.append(f"[{left}][{idx}:v]xfade=transition={xfade_type}:duration={transition_dur:.3f}:offset={offset:.3f}[{out}]")
                cumulative += durations[idx]
            final_label = f"v{len(uploaded_remote_files) - 1}"
            concat_cmd = (
                f"ffmpeg -y {ff_inputs} -filter_complex \"{';'.join(parts)}\" "
                f"-map '[{final_label}]' -c:v libx264 -preset fast -pix_fmt yuv420p '{remote_merged}'"
            )
        else:
            concat_cmd = (
                f"ffmpeg -y -f concat -safe 0 -i '{remote_concat}' "
                f"-c:v libx264 -preset fast -pix_fmt yuv420p '{remote_merged}'"
            )
        ok, err = await asyncio.to_thread(
            _run_remote_cmd_blocking,
            RUNPOD_COMPOSITOR_HOST,
            RUNPOD_COMPOSITOR_SSH_PORT,
            concat_cmd,
        )
        if not ok:
            raise RuntimeError(f"RunPod concat failed: {err}")

        if remote_sub and remote_sfx:
            merge_cmd = (
                f"ffmpeg -y -i '{remote_merged}' -i '{remote_audio}' -i '{remote_sfx}' "
                f"-vf \"ass={remote_sub}\" "
                f"-filter_complex \"[1:a]volume=1.0[voice];[2:a]volume=0.18[sfx];"
                f"[voice][sfx]amix=inputs=2:duration=first:dropout_transition=2,apad=pad_dur=0.8[aout]\" "
                f"-map 0:v -map \"[aout]\" -c:v libx264 -preset fast -pix_fmt yuv420p "
                f"-c:a aac -b:a 192k -shortest '{remote_output}'"
            )
        elif remote_sub:
            merge_cmd = (
                f"ffmpeg -y -i '{remote_merged}' -i '{remote_audio}' "
                f"-vf \"ass={remote_sub}\" -af apad=pad_dur=0.8 "
                f"-c:v libx264 -preset fast -pix_fmt yuv420p -c:a aac -b:a 192k -shortest '{remote_output}'"
            )
        elif remote_sfx:
            merge_cmd = (
                f"ffmpeg -y -i '{remote_merged}' -i '{remote_audio}' -i '{remote_sfx}' "
                f"-filter_complex \"[1:a]volume=1.0[voice];[2:a]volume=0.18[sfx];"
                f"[voice][sfx]amix=inputs=2:duration=first:dropout_transition=2,apad=pad_dur=0.8[aout]\" "
                f"-map 0:v -map \"[aout]\" -c:v libx264 -preset fast -pix_fmt yuv420p "
                f"-c:a aac -b:a 192k -shortest '{remote_output}'"
            )
        else:
            merge_cmd = (
                f"ffmpeg -y -i '{remote_merged}' -i '{remote_audio}' "
                f"-af apad=pad_dur=0.8 -c:v libx264 -preset fast -pix_fmt yuv420p "
                f"-c:a aac -b:a 192k -shortest '{remote_output}'"
            )

        ok, err = await asyncio.to_thread(
            _run_remote_cmd_blocking,
            RUNPOD_COMPOSITOR_HOST,
            RUNPOD_COMPOSITOR_SSH_PORT,
            merge_cmd,
        )
        if not ok:
            raise RuntimeError(f"RunPod final merge failed: {err}")

        dl_ok, dl_err = await asyncio.to_thread(
            _download_file_from_runpod_blocking,
            RUNPOD_COMPOSITOR_HOST,
            RUNPOD_COMPOSITOR_SSH_PORT,
            remote_output,
            output_path,
        )
        if not dl_ok:
            raise RuntimeError(f"RunPod output download failed: {dl_err}")

        if not Path(output_path).exists() or Path(output_path).stat().st_size == 0:
            raise RuntimeError("RunPod output file missing after download")
        return output_path
    finally:
        local_concat.unlink(missing_ok=True)
        await asyncio.to_thread(
            _run_remote_cmd_blocking,
            RUNPOD_COMPOSITOR_HOST,
            RUNPOD_COMPOSITOR_SSH_PORT,
            f"rm -rf '{remote_dir}'",
        )


async def composite_video(
    scenes: list,
    scene_assets: list,
    audio_path: str,
    output_path: str,
    resolution: str = "720p",
    use_svd: bool = False,
    subtitle_path: str = None,
    sfx_paths: list[str] = None,
    bgm_track: str = "",
    transition_style: str = "smooth",
    micro_escalation_mode: bool = False,
) -> str:
    """Composite scene clips into final MP4 with optional burned-in captions and SFX.
    scene_assets: list of dicts with keys: image, frames, kling_clip
    subtitle_path: optional ASS subtitle file for word-synced captions
    sfx_paths: optional per-scene SFX audio files to mix under voiceover
    """
    config = RESOLUTION_CONFIGS[resolution]
    out_w = config["output_width"]
    out_h = config["output_height"]

    job_ts = str(int(time.time() * 1000))
    concat_file = TEMP_DIR / ("concat_" + job_ts + ".txt")
    scene_clips = []
    clip_durations = []

    num_scenes = len(scenes)
    for i, (scene, asset) in enumerate(zip(scenes, scene_assets)):
        duration = scene.get("duration_sec", 4)
        if i == num_scenes - 1:
            duration += 1.0
        text_overlay = "" if subtitle_path else scene.get("text_overlay", "")
        clip_name = "scene_" + str(i) + "_" + job_ts + ".mp4"
        clip_path = str(TEMP_DIR / clip_name)

        if asset.get("kling_clip"):
            try:
                await kling_clip_to_scene(
                    asset["kling_clip"], duration, clip_path,
                    out_w, out_h, text_overlay, resolution,
                )
                scene_clips.append(Path(clip_path))
                clip_durations.append(float(duration))
                continue
            except Exception as e:
                log.warning(f"Kling clip processing failed for scene {i}: {e}")

        if use_svd and asset.get("frames"):
            try:
                await frames_to_clip(
                    asset["frames"], duration, clip_path,
                    out_w, out_h, text_overlay, resolution,
                )
                scene_clips.append(Path(clip_path))
                clip_durations.append(float(duration))
                continue
            except Exception as e:
                log.warning(f"SVD clip failed for scene {i}, falling back to static: {e}")

        await static_image_to_clip(
            asset["image"], duration, clip_path,
            out_w, out_h, text_overlay, resolution,
        )
        scene_clips.append(Path(clip_path))
        clip_durations.append(float(duration))

    existing_clips = [c for c in scene_clips if c.exists() and c.stat().st_size > 0]
    if not existing_clips:
        raise RuntimeError("No scene clips were created -- nothing to composite")
    working_clips = list(existing_clips)
    working_durations = [clip_durations[i] if i < len(clip_durations) else _probe_video_duration_seconds(str(c)) for i, c in enumerate(existing_clips)]
    if micro_escalation_mode:
        try:
            working_clips, working_durations = await _build_micro_escalation_clips(existing_clips, working_durations, job_ts)
            log.info(f"Micro-escalation enabled: {len(existing_clips)} base clips -> {len(working_clips)} beat clips")
        except Exception as e:
            log.warning(f"Micro-escalation build failed, falling back to base clips: {e}")
            working_clips = list(existing_clips)
            working_durations = list(working_durations)
    log.info(f"Compositing {len(working_clips)} scene clips into video")

    sfx_track = ""
    if sfx_paths and any(sfx_paths):
        sfx_track_path = str(TEMP_DIR / ("sfx_full_" + job_ts + ".mp3"))
        sfx_track = await _merge_sfx_track(sfx_paths, scenes, sfx_track_path)
    ambience_track = ""
    if sfx_track or _audio_track_exists(bgm_track):
        ambience_path = str(TEMP_DIR / ("ambience_full_" + job_ts + ".mp3"))
        ambience_track = await _mix_ambience_tracks(sfx_track, bgm_track, ambience_path)

    if RUNPOD_COMPOSITOR_ENABLED:
        try:
            await _composite_video_on_runpod(
                working_clips,
                audio_path,
                output_path,
                subtitle_path=subtitle_path,
                sfx_track=ambience_track,
                transition_style=transition_style,
                clip_durations=working_durations,
            )
            for clip in set(scene_clips + working_clips):
                clip.unlink(missing_ok=True)
            if ambience_track:
                Path(ambience_track).unlink(missing_ok=True)
            if sfx_track:
                Path(sfx_track).unlink(missing_ok=True)
            log.info("Final compositing offloaded to RunPod")
            return str(output_path)
        except Exception as e:
            if not RUNPOD_COMPOSITOR_FALLBACK_LOCAL:
                raise
            log.warning(f"RunPod compositing failed, falling back local: {e}")

    with open(concat_file, "w") as f:
        for clip in working_clips:
            f.write("file '" + str(clip.resolve()) + "'\n")

    merged_video = TEMP_DIR / ("merged_" + job_ts + ".mp4")
    style = _normalize_transition_style(transition_style)
    xfade_type = TRANSITION_STYLE_MAP.get(style, "fade")
    transition_dur = _transition_duration_for_style(style)
    use_xfade = (style != "no_motion" and style != "none" and len(working_clips) > 1)
    if use_xfade:
        durations = []
        for i, clip in enumerate(working_clips):
            probed = _probe_video_duration_seconds(str(clip))
            fallback = working_durations[i] if i < len(working_durations) else 5.0
            durations.append(max(0.4, probed if probed > 0.1 else float(fallback)))
        cmd = ["ffmpeg", "-y"]
        for clip in working_clips:
            cmd.extend(["-i", str(clip)])
        parts = []
        cumulative = durations[0]
        for idx in range(1, len(working_clips)):
            left = "0:v" if idx == 1 else f"v{idx-1}"
            out = f"v{idx}"
            offset = max(0.0, cumulative - transition_dur * idx)
            parts.append(f"[{left}][{idx}:v]xfade=transition={xfade_type}:duration={transition_dur:.3f}:offset={offset:.3f}[{out}]")
            cumulative += durations[idx]
        final_label = f"v{len(working_clips)-1}"
        cmd.extend([
            "-filter_complex", ";".join(parts),
            "-map", f"[{final_label}]",
            "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
            str(merged_video),
        ])
    else:
        cmd = [
            "ffmpeg", "-y", "-f", "concat", "-safe", "0",
            "-i", str(concat_file),
            "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
            str(merged_video),
        ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr_concat = await proc.communicate()
    if proc.returncode != 0:
        err_msg = stderr_concat.decode()[-500:]
        log.error(f"FFmpeg concat error: {err_msg}")
        raise RuntimeError("FFmpeg failed to concat scene clips: " + err_msg[-200:])

    if not merged_video.exists() or merged_video.stat().st_size == 0:
        raise RuntimeError("FFmpeg concat produced no output file")

    has_sfx = ambience_track and Path(ambience_track).exists()

    if subtitle_path and Path(subtitle_path).exists():
        sub_abs = str(Path(subtitle_path).resolve()).replace("\\", "/").replace(":", "\\:")
        if has_sfx:
            cmd = [
                "ffmpeg", "-y",
                "-i", str(merged_video),
                "-i", audio_path,
                "-i", ambience_track,
                "-vf", f"ass={sub_abs}",
                "-filter_complex", "[1:a]volume=1.0[voice];[2:a]volume=0.18[sfx];[voice][sfx]amix=inputs=2:duration=first:dropout_transition=2,apad=pad_dur=0.8[aout]",
                "-map", "0:v", "-map", "[aout]",
                "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest",
                str(output_path),
            ]
        else:
            cmd = [
                "ffmpeg", "-y",
                "-i", str(merged_video),
                "-i", audio_path,
                "-vf", f"ass={sub_abs}",
                "-af", "apad=pad_dur=0.8",
                "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest",
                str(output_path),
            ]
        log.info(f"Burning captions from {subtitle_path}" + (" + SFX" if has_sfx else ""))
    else:
        if has_sfx:
            cmd = [
                "ffmpeg", "-y",
                "-i", str(merged_video),
                "-i", audio_path,
                "-i", ambience_track,
                "-filter_complex", "[1:a]volume=1.0[voice];[2:a]volume=0.18[sfx];[voice][sfx]amix=inputs=2:duration=first:dropout_transition=2,apad=pad_dur=0.8[aout]",
                "-map", "0:v", "-map", "[aout]",
                "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest",
                str(output_path),
            ]
        else:
            cmd = [
                "ffmpeg", "-y",
                "-i", str(merged_video),
                "-i", audio_path,
                "-af", "apad=pad_dur=0.8",
                "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest",
                str(output_path),
            ]

    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr_merge = await proc.communicate()
    if proc.returncode != 0:
        if subtitle_path:
            log.warning(f"Subtitle burn-in failed, retrying without: {stderr_merge.decode()[-300:]}")
            cmd_fallback = [
                "ffmpeg", "-y",
                "-i", str(merged_video),
                "-i", audio_path,
                "-af", "apad=pad_dur=0.8",
                "-c:v", "libx264", "-preset", "fast", "-pix_fmt", "yuv420p",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest",
                str(output_path),
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd_fallback, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            _, stderr_merge = await proc.communicate()
        if proc.returncode != 0:
            err_msg = stderr_merge.decode()[-500:]
            log.error(f"FFmpeg final merge error: {err_msg}")
            raise RuntimeError("FFmpeg failed to merge video + audio: " + err_msg[-200:])

    if not Path(output_path).exists() or Path(output_path).stat().st_size == 0:
        raise RuntimeError("FFmpeg produced no final output file")

    for clip in set(scene_clips + working_clips):
        clip.unlink(missing_ok=True)
    concat_file.unlink(missing_ok=True)
    merged_video.unlink(missing_ok=True)
    if ambience_track:
        Path(ambience_track).unlink(missing_ok=True)
    if sfx_track:
        Path(sfx_track).unlink(missing_ok=True)

    log.info(f"Video composited successfully: {Path(output_path).stat().st_size / 1024 / 1024:.1f} MB")
    return str(output_path)


# ─── Full Generation Pipeline ─────────────────────────────────────────────────


def _normalize_scenes_for_render(scenes: list) -> list:
    normalized = []
    for idx, raw_scene in enumerate(scenes or []):
        scene = dict(raw_scene or {})
        narration = str(scene.get("narration", "") or "").strip()
        visual_description = str(scene.get("visual_description", "") or "").strip()
        negative_prompt = str(scene.get("negative_prompt", "") or "").strip()
        if not visual_description:
            visual_description = narration or f"Scene {idx + 1} visual"
        try:
            duration = float(scene.get("duration_sec", 5))
        except Exception:
            duration = 5.0
        duration = max(3.5, min(duration, 10.0))
        scene["narration"] = narration
        scene["visual_description"] = visual_description
        scene["negative_prompt"] = negative_prompt
        scene["duration_sec"] = round(duration, 2)
        normalized.append(scene)
    return normalized


SKELETON_QUALITY_MODES = {"standard", "cinematic"}
SKELETON_CINEMATIC_PROMPT_ADDON = (
    "CINEMATIC UPGRADE RULES: Open each scene with immediate tension or high-stakes action. "
    "Use dynamic 24-35mm framing, low-angle or over-shoulder composition, strong foreground/background depth, "
    "volumetric rim lighting, and high local contrast. Keep a premium viral short aesthetic with clean silhouette separation. "
    "Avoid static centered idle poses, flat lighting, washed contrast, cartoon style, and text overlays."
)
STORY_MASTER_CONSISTENCY_PROMPT = (
    "MASTER STORY CONSISTENCY RULES (apply to every scene): "
    "Keep one continuous visual universe across all scenes. Keep recurring people, places, era details, "
    "color grade, and camera language continuity scene-to-scene when the script indicates recurrence. "
    "Do not switch art style or turn into cartoon/anime unless explicitly requested. "
    "Preserve realistic skin texture, grounded proportions, and emotionally readable expressions. "
    "When multiple subjects appear, prioritize whichever subject the current script beat is about. "
    "Maintain bright readable mids, clean separation, and premium cinematic detail in every frame."
)
STORY_CINEMATIC_PROMPT_ADDON = (
    "CINEMATIC STORY UPGRADE RULES: Open each scene with visible emotional stakes or conflict. "
    "Use dynamic 24-35mm framing, motivated camera motion, foreground/background depth, and volumetric edge lighting. "
    "Keep every composition readable in vertical format and avoid static idle center-framing. "
    "Avoid washed contrast, muddy shadows, low-detail backgrounds, and stock-photo framing."
)
STORY_EXPLAINER_CONSISTENCY_PROMPT = (
    "MASTER STORY CONSISTENCY RULES (apply to every scene): "
    "Keep one continuous visual universe across all scenes, but do not force a recurring protagonist or human presenter. "
    "For concept-heavy beats, preserve continuity through recurring anatomy, objects, mechanisms, environments, color grade, and camera language. "
    "Prioritize the core concept, process, body system, object, or environment the beat is explaining. "
    "Use readable cinematic layouts that make the mechanism obvious at phone-screen size."
)
STORY_EXPLAINER_CINEMATIC_PROMPT_ADDON = (
    "CINEMATIC STORY UPGRADE RULES: visualize the mechanism directly with premium explainer cinematography. "
    "Use readable cutaway views, macro detail, layered depth, motivated lighting, and vertical-friendly composition. "
    "Prefer anatomy, systems, heat, cells, objects, environments, diagrams, or process visuals when they explain the beat more clearly than a person on screen. "
    "Avoid defaulting to a single human character unless the beat explicitly requires one."
)
STORY_REALISM_REFINEMENT = (
    "Realism lock: photoreal human skin texture, natural pores, physically plausible hands and fingers, "
    "natural eye reflections, grounded facial proportions, realistic fabric micro-detail, and non-plastic materials. "
    "No CGI waxiness, no uncanny face, no extra fingers, no malformed hands."
)
STORY_EXPLAINER_REALISM_REFINEMENT = (
    "Realism lock: premium photoreal explainer visual with medically and physically plausible anatomy, "
    "clear body-system visualization, readable objects and environments, grounded materials, and crisp cinematic lighting. "
    "No stock-photo spokesperson, no random presenter, no irrelevant repeated character, no plastic CGI anatomy, and no abstract blob shapes replacing the core concept."
)


def _story_scene_prefers_explainer_visuals(text: str) -> bool:
    source = str(text or "").strip().lower()
    if not source:
        return False
    human_hits = len(re.findall(r"\b(man|woman|person|people|character|protagonist|hero|lead|worker|student|doctor|patient|he|she|they|someone|guy|girl)\b", source))
    explainer_hits = len(re.findall(
        r"\b(fever|immune|immune system|pyrogen|pyrogens|hypothalamus|thermostat|temperature|degrees|thermometer|infection|bacteria|virus|viruses|body|blood|blood vessels|cells|cell|chemical|signals|sweat|sweating|muscles|brain|anatomy|organ|organs|disease|symptom|mechanism|process|science|medical|biology|physiology|system)\b",
        source,
    ))
    concept_hits = len(re.findall(
        r"\b(explains?|how it works|why it happens|what happens|step by step|cause|effect|reaction|response|cool down|heat|cooling|reproduce|reset)\b",
        source,
    ))
    return explainer_hits >= 2 or (explainer_hits >= 1 and concept_hits >= 1) or (explainer_hits >= 1 and human_hits == 0)


def _configured_image_provider_order() -> list[str]:
    provider_order = [
        str(p or "").strip().lower()
        for p in str(IMAGE_PROVIDER_ORDER or "").split(",")
        if str(p or "").strip()
    ]
    if provider_order:
        return provider_order
    if XAI_IMAGE_FALLBACK_ENABLED and bool(FAL_AI_KEY or XAI_API_KEY):
        return ["fal"]
    return ["hidream", "wan22", "sdxl"] if HIDREAM_ENABLED else ["wan22", "sdxl"]


def _configured_local_image_provider_order() -> list[str]:
    local_providers: list[str] = []
    for provider in _configured_image_provider_order():
        key = _normalize_image_provider_key(provider)
        if key in {"hidream", "wan22", "sdxl"} and key not in local_providers:
            local_providers.append(key)
    return local_providers


def _story_prompt_prefix_for_scene(visual_description: str) -> str:
    if _story_scene_prefers_explainer_visuals(visual_description):
        return (
            "Cinematic photoreal explainer scene, Unreal Engine 5 grade realism with filmic cinematography, "
            "emotionally resonant composition with depth of field and grounded concept detail, "
            "dramatic volumetric lighting with motivated light sources, ray traced global illumination, "
            "atmospheric particles, lens behavior, film grain, and richly detailed cinematic environment, 8k ultra HD, "
        )
    return TEMPLATE_PROMPT_PREFIXES.get("story", "")


def _story_consistency_prompt_for_scene(visual_description: str) -> str:
    return STORY_EXPLAINER_CONSISTENCY_PROMPT if _story_scene_prefers_explainer_visuals(visual_description) else STORY_MASTER_CONSISTENCY_PROMPT


def _story_cinematic_addon_for_scene(visual_description: str) -> str:
    return STORY_EXPLAINER_CINEMATIC_PROMPT_ADDON if _story_scene_prefers_explainer_visuals(visual_description) else STORY_CINEMATIC_PROMPT_ADDON


def _story_salvage_refinement_for_scene(prompt: str) -> str:
    return STORY_EXPLAINER_REALISM_REFINEMENT if _story_scene_prefers_explainer_visuals(prompt) else STORY_REALISM_REFINEMENT


def _normalize_skeleton_quality_mode(value: str | None, template: str = "skeleton") -> str:
    if template not in {"skeleton", "story"}:
        return "standard"
    raw = str(value or "").strip().lower()
    if raw in {"pro", "high", "cinematic_plus"}:
        raw = "cinematic"
    if raw in SKELETON_QUALITY_MODES:
        return raw
    # Default to cinematic for skeleton/story upgrades while keeping generation memory profile unchanged.
    return "cinematic"


def _normalize_mint_mode(value, template: str) -> bool:
    t = str(template or "").strip().lower()
    # Skeleton prompts already pass through strong template constraints; mint rewrite
    # can over-truncate scene-specific objects/actions in creative control.
    if t == "skeleton":
        return False
    default_on = t in {"story"}
    return _bool_from_any(value, default=default_on)


def _creative_prompt_passthrough_enabled(session: dict | None) -> bool:
    """When enabled, Creative Control uses the user's scene prompt verbatim."""
    if not isinstance(session, dict):
        return True
    return _bool_from_any(session.get("prompt_passthrough"), True)


def _apply_mint_scene_compiler(scenes: list, template: str, mint_mode: bool = True) -> list:
    """Deterministic scene rewrite pass for stronger first-render reliability."""
    if not _normalize_mint_mode(mint_mode, template):
        return scenes
    if template not in {"skeleton", "story"}:
        return scenes

    def _sentences(text: str) -> list[str]:
        parts = re.split(r"(?<=[.!?])\s+", text.strip())
        return [p.strip() for p in parts if p and p.strip()]

    compiled = []
    for raw_scene in scenes or []:
        scene = dict(raw_scene or {})
        visual = re.sub(r"\s+", " ", str(scene.get("visual_description", "") or "")).strip()
        chunks = _sentences(visual)
        if not chunks:
            if template == "skeleton":
                chunks = [
                    "A canonical ivory-white anatomical skeleton with large realistic eyeballs and a clearly visible translucent body silhouette appears in a detailed cinematic environment.",
                    "The skeleton takes a clear action pose with role-specific props inside a richly detailed environment that clearly matches the specific topic, with readable background depth.",
                    "Motion begins instantly with smooth human-like momentum and directional continuity into the next shot.",
                ]
            else:
                chunks = [
                    "The primary subject(s) for this script beat appear in a specific cinematic environment.",
                    "The subject(s) perform a clear action with visible stakes and readable composition.",
                    "Camera movement and subject motion continue smoothly to preserve scene-to-scene continuity.",
                ]
        if len(chunks) > 3:
            chunks = chunks[:3]
        while len(chunks) < 3:
            chunks.append("")

        if template == "skeleton":
            skeleton_lock = "A canonical ivory-white anatomical skeleton with large realistic eyeballs and a clearly visible translucent body silhouette appears in a topic-matched cinematic environment."
            first_chunk = chunks[0].strip()
            explicit_outfit_request = bool(re.search(r"\b(outfit|uniform|suit|coat|armor|jersey|scrubs|clothes|clothing|costume|dress|robe|hoodie|jacket)\b", " ".join(chunks), re.IGNORECASE))
            if not re.search(r"\b(canonical|anatomical)\b", first_chunk, re.IGNORECASE) or not re.search(r"\b(translucent|glass[- ]?like|body silhouette)\b", first_chunk, re.IGNORECASE):
                first_chunk = f"{skeleton_lock} {first_chunk}".strip()
            if explicit_outfit_request:
                first_chunk = first_chunk.rstrip(".!?") + ". Keep any explicitly requested outfit visible while preserving the same canonical skeleton anatomy."
            elif not re.search(r"\b(no clothing|no costume|no uniforms?|no armor)\b", first_chunk, re.IGNORECASE):
                first_chunk = first_chunk.rstrip(".!?") + ". No clothing, uniforms, armor, or costumes."
            chunks[0] = first_chunk
            if not explicit_outfit_request:
                chunks[1] = re.sub(r"\b(outfit|uniform|suit|coat|armor|jersey|scrubs|clothes|clothing|costume)\b", "props", chunks[1], flags=re.IGNORECASE)
            combined = " ".join(chunks[:3])
            internal_focus = _skeleton_scene_prefers_internal_cutaway(combined)
            minimal_background = _skeleton_scene_requests_minimal_background(combined)
            if not _skeleton_scene_has_environment_cue(chunks[0]):
                if minimal_background:
                    chunks[0] = chunks[0].rstrip(".!?") + " With an intentional premium minimal backdrop."
                elif internal_focus:
                    chunks[0] = chunks[0].rstrip(".!?") + " In a readable internal or microscopic cutaway environment."
                else:
                    chunks[0] = chunks[0].rstrip(".!?") + " In a richly detailed environment matching the scene request."
            if not re.search(r"\b(holding|aiming|running|facing|fighting|action|pose|gestur|turning|pointing|sit|sits|seated|sitting)\b", chunks[1], re.IGNORECASE):
                chunks[1] = (chunks[1].rstrip(".!?") + " " if chunks[1] else "") + \
                    "The skeleton takes a clear action pose with a role-specific prop and readable composition."
            has_table = bool(re.search(r"\b(table|desk|countertop)\b", combined, re.IGNORECASE))
            has_sit = bool(re.search(r"\b(sit|sits|seated|sitting)\b", combined, re.IGNORECASE))
            has_brain = bool(re.search(r"\bbrain\b", combined, re.IGNORECASE))
            has_money = bool(re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", combined, re.IGNORECASE))
            if internal_focus and not re.search(r"\b(cutaway|micro|microscopic|internal|bloodstream|blood vessels?|organ|cell|tissue|anatomical|artery|vein)\b", chunks[1], re.IGNORECASE):
                chunks[1] = chunks[1].rstrip(".!?") + ". Use a readable internal or microscopic cutaway environment with clear contextual structures and layered depth."
            if has_table and not has_sit:
                chunks[1] = chunks[1].rstrip(".!?") + ". Skeleton is clearly seated at the table."
            if has_brain and has_money and not re.search(r"\b(two|both)\b", combined, re.IGNORECASE):
                chunks[2] = chunks[2].rstrip(".!?") + ". Keep both required props visible: one brain and one pile of money."
            if not _skeleton_scene_has_environment_cue(combined):
                if minimal_background:
                    chunks[1] = chunks[1].rstrip(".!?") + " Keep the requested minimal backdrop intentional and clearly lit."
                elif internal_focus:
                    chunks[1] = chunks[1].rstrip(".!?") + " Use a readable internal or microscopic cutaway environment with layered depth."
                else:
                    chunks[1] = chunks[1].rstrip(".!?") + " Place the skeleton in a richly detailed environment with readable background context."
            if not re.search(r"\b(prop|object|item|scene request|requested)\b", " ".join(chunks[:3]), re.IGNORECASE):
                chunks[1] = chunks[1].rstrip(".!?") + ". Keep all explicitly requested props/objects visible and readable."
            if not re.search(r"\b(motion|camera|moves|moving|drift|continuity|momentum)\b", chunks[2], re.IGNORECASE):
                chunks[2] = (chunks[2].rstrip(".!?") + " " if chunks[2] else "") + \
                    "Motion begins instantly with smooth momentum and directional continuity."
            if not _skeleton_scene_has_camera_cue(combined):
                chunks[2] = chunks[2].rstrip(".!?") + " Choose framing that fits the beat instead of repeating the same centered medium hero shot."
        else:
            explainer_visuals = template == "story" and _story_scene_prefers_explainer_visuals(" ".join(chunks[:3]))
            if explainer_visuals:
                if not re.search(r"\b(anatomy|mechanism|process|system|cells?|organs?|brain|blood|temperature|infection|bacteria|virus|heat|sweat|diagram|cutaway|thermometer|molecule)\b", chunks[0], re.IGNORECASE):
                    chunks[0] = (chunks[0].rstrip(".!?") + " " if chunks[0] else "") + \
                        "The core mechanism or body-system concept for this script beat is clearly visible."
                if not re.search(r"\b(environment|background|setting|location|interior|exterior|cutaway|macro|inside|within)\b", chunks[0], re.IGNORECASE):
                    chunks[0] = chunks[0].rstrip(".!?") + " In a specific cinematic environment or readable cutaway view."
                if not re.search(r"\b(action|moving|running|walking|turning|interacting|holding|flow|circulat|heat|signal|response|reaction|change)\b", chunks[1], re.IGNORECASE):
                    chunks[1] = (chunks[1].rstrip(".!?") + " " if chunks[1] else "") + \
                        "Show the mechanism actively happening with visible cause-and-effect and readable stakes."
                if not re.search(r"\b(camera|motion|continuity|transition|cutaway|macro|push)\b", chunks[2], re.IGNORECASE):
                    chunks[2] = (chunks[2].rstrip(".!?") + " " if chunks[2] else "") + \
                        "Camera movement and visual continuity should track the process clearly into the next scene."
            else:
                if not re.search(r"\b(character|protagonist|person|figure|subject|crowd|group|family|worker)\b", chunks[0], re.IGNORECASE):
                    chunks[0] = (chunks[0].rstrip(".!?") + " " if chunks[0] else "") + \
                        "The primary subject(s) for this script beat are clearly visible."
                if not re.search(r"\b(environment|background|setting|location|interior|exterior)\b", chunks[0], re.IGNORECASE):
                    chunks[0] = chunks[0].rstrip(".!?") + " In a specific cinematic environment."
                if not re.search(r"\b(action|moving|running|walking|turning|interacting|holding)\b", chunks[1], re.IGNORECASE):
                    chunks[1] = (chunks[1].rstrip(".!?") + " " if chunks[1] else "") + \
                        "The subject(s) perform a clear action with visible stakes."
                if not re.search(r"\b(camera|motion|continuity|transition)\b", chunks[2], re.IGNORECASE):
                    chunks[2] = (chunks[2].rstrip(".!?") + " " if chunks[2] else "") + \
                        "Camera and subject motion preserve smooth continuity into the next scene."

        scene["visual_description"] = " ".join([
            c.strip().rstrip(".!?") + "."
            for c in chunks[:3]
            if c and c.strip()
        ]).strip()
        compiled.append(scene)
    return compiled


def _build_skeleton_image_prompt(
    visual_description: str,
    skeleton_anchor: str = "",
    quality_mode: str = "cinematic",
    immutable_context: str = "",
) -> str:
    def _scene_object_lock(scene_text: str) -> str:
        text = str(scene_text or "").strip().lower()
        if not text:
            return ""
        rules: list[str] = []
        has_brain = bool(re.search(r"\bbrain\b", text))
        has_money = bool(re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", text))
        has_table = bool(re.search(r"\btable|desk|countertop\b", text))
        has_glow = bool(re.search(r"\b(glow|glowing|emissive|light[- ]?emitting|luminous)\b", text))
        if has_table:
            rules.append(
                "TABLE LOCK: skeleton is seated at a table/desk and the tabletop is clearly visible in frame."
            )
            rules.append(
                "PROP PLACEMENT LOCK: required props are on/above the tabletop, not random floating orbs unless explicitly requested."
            )
        if has_brain:
            rules.append(
                "BRAIN PROP LOCK: show one clearly identifiable human brain with realistic gyri/sulci folds (not a smooth sphere, fruit, or generic ball)."
            )
        if has_money:
            rules.append(
                "MONEY PROP LOCK: show one clearly identifiable pile/stack of paper cash banknotes (not coins-only, not blank paper scraps)."
            )
        if has_brain and has_money:
            rules.append("OBJECT COUNT LOCK: include both required props (brain and money) in the same shot and keep both readable.")
        if has_glow and (has_brain or has_money):
            rules.append(
                "GLOW LOCK: each requested glowing prop must visibly emit soft bloom/light so the glow is obvious at phone-screen size."
            )
        return " ".join(rules).strip()

    mode = _normalize_skeleton_quality_mode(quality_mode, template="skeleton")
    addon = (SKELETON_CINEMATIC_PROMPT_ADDON + " ") if mode == "cinematic" else ""
    immutable = (str(immutable_context or "").strip() + " ") if immutable_context else ""
    delta = _sanitize_skeleton_scene_delta(str(visual_description or "").strip())
    explicit_outfit_request = bool(re.search(r"\b(suit|tuxedo|armor|uniform|costume|hoodie|jacket|dress|shirt|pants|coat|robe|scrubs|jersey)\b", delta, re.IGNORECASE))
    object_lock = _scene_object_lock(delta)
    glow_lock = ""
    context_lock = _skeleton_scene_context_lock(delta)
    framing_lock = _skeleton_scene_framing_lock(delta)
    if re.search(r"\b(glow|glowing|emissive|neon|luminescen|light[- ]?emitting)\b", delta, re.IGNORECASE):
        glow_lock = (
            "PROP LIGHTING LOCK: any requested glowing object must visibly emit light/bloom and be clearly identifiable on-screen. "
        )
    scene_mandate = (
        "SCENE CONTENT LOCK (must be visible and preserved): "
        + delta
        + ". Keep every explicitly requested prop/object/action visible and readable in frame."
    ).strip()
    return (
        SKELETON_IMAGE_STYLE_PREFIX + " "
        + scene_mandate + " "
        + (
            "SCENE PRIORITY RULE: preserve requested setting, pose, action, props, object count, and any explicitly requested outfit from the scene content lock while keeping the same canonical skeleton anatomy. "
            if explicit_outfit_request
            else "SCENE PRIORITY RULE: preserve requested setting, pose, action, props, and object count from the scene content lock; ignore any clothing/costume directives to preserve canonical skeleton identity. "
        )
        + (object_lock + " " if object_lock else "")
        + glow_lock
        + context_lock + " "
        + framing_lock + " "
        + SKELETON_MASTER_CONSISTENCY_PROMPT + " "
        + "NON-NEGOTIABLE CHARACTER RULE: use the exact same canonical anatomical skeleton in every scene: ivory-white skull and bones, large realistic eyeballs with visible iris and wet reflective highlights, clearly visible translucent soft-tissue silhouette around torso/limbs, identical skull proportions and eye spacing. "
        + "COLOR/RENDER RULE: realistic photographic rendering with natural ivory bone tones; never x-ray, radiograph, CT, fluoroscopy, or neon-blue scan aesthetics. "
        + "EYE RULE: eyes must be realistic and non-glowing; no emissive, neon, laser, or light-emitting eyes. Any glow in scene is only from props/environment, never from eye sockets. "
        + "CANONICAL LOOK LOCK: this is NEVER a bare-bones model. A clear glass-like translucent human shell must visibly wrap all shown body regions (head, torso, arms, hands, pelvis, legs when visible) with bones clearly seen through it. "
        + "VISIBILITY LOCK: the translucent shell must be clearly noticeable at phone-screen size, with medium-opacity glass edges and subtle interior translucency around the skeleton form. "
        + "ANATOMY LOCK: skull-only face geometry, never human skin/flesh face features. "
        + "HAIR LOCK: no human hair, scalp, beard, eyebrows, eyelashes, wig, or hairstyle elements. "
        + (
            "NON-NEGOTIABLE STYLE RULE: if the scene explicitly requests clothing, keep that wardrobe visible while preserving the same skeleton identity, translucent shell, and anatomy. "
            if explicit_outfit_request
            else "NON-NEGOTIABLE STYLE RULE: no clothing, uniforms, armor, or costumes on the skeleton body. "
        )
        + "COMPOSITION RULE: keep the skeleton prominent and instantly readable in vertical 9:16, but allow off-center placement and visible environment depth whenever that makes the beat clearer. The frame should feel designed, not generic. "
        + addon + immutable + skeleton_anchor + delta + " "
        + SKELETON_IMAGE_SUFFIX
    )


def _build_story_image_prompt(
    visual_description: str,
    quality_mode: str = "cinematic",
    immutable_context: str = "",
) -> str:
    mode = _normalize_skeleton_quality_mode(quality_mode, template="story")
    addon = (_story_cinematic_addon_for_scene(visual_description) + " ") if mode == "cinematic" else ""
    immutable = (str(immutable_context or "").strip() + " ") if immutable_context else ""
    return (
        _story_prompt_prefix_for_scene(visual_description) + " "
        + _story_consistency_prompt_for_scene(visual_description) + " "
        + addon + immutable + str(visual_description or "").strip()
    ).strip()


def _apply_template_scene_constraints(scenes: list, template: str, quality_mode: str = "standard") -> list:
    """Harden model output so template-critical constraints cannot drift."""
    if template not in {"skeleton", "story", "motivation"}:
        return scenes
    quality_mode = _normalize_skeleton_quality_mode(quality_mode, template=template)

    def _sentences(text: str) -> list[str]:
        parts = re.split(r"(?<=[.!?])\s+", text.strip())
        return [p.strip() for p in parts if p and p.strip()]

    constrained = []
    for raw_scene in scenes or []:
        scene = dict(raw_scene or {})
        visual = re.sub(r"\s+", " ", str(scene.get("visual_description", "") or "")).strip()
        chunks = _sentences(visual)
        if not chunks:
            if template == "skeleton":
                chunks = [
                    "A canonical ivory-white anatomical skeleton with large realistic eyeballs and a clearly visible translucent body silhouette appears in a topic-matched cinematic environment.",
                    "The skeleton holds role-specific props inside a richly detailed environment that clearly matches the specific topic, with readable background elements and layered depth.",
                    "Motion cue: smooth natural arm and head movement with realistic momentum.",
                ]
            else:
                chunks = [
                    "The key subject(s) from this story beat appear in a specific cinematic environment.",
                    "The subject(s) perform a clear action with visible emotional stakes and readable composition.",
                    "Motion cue: camera and subject movement preserve smooth continuity into the next scene.",
                ]
        if len(chunks) > 3:
            chunks = chunks[:3]

        if template == "skeleton":
            skeleton_lock = "A canonical ivory-white anatomical skeleton with large realistic eyeballs and a clearly visible translucent body silhouette appears in a topic-matched cinematic environment."
            first_chunk = chunks[0].strip()
            explicit_outfit_request = bool(re.search(r"\b(outfit|uniform|suit|coat|armor|jersey|scrubs|clothes|clothing|costume|dress|robe|hoodie|jacket)\b", " ".join(chunks), re.IGNORECASE))
            if not re.search(r"\b(canonical|anatomical)\b", first_chunk, re.IGNORECASE) or not re.search(r"\b(translucent|glass[- ]?like|body silhouette)\b", first_chunk, re.IGNORECASE):
                first_chunk = f"{skeleton_lock} {first_chunk}".strip()
            if explicit_outfit_request:
                first_chunk = first_chunk.rstrip(".!?") + ". Keep any explicitly requested outfit visible while preserving the same canonical skeleton anatomy."
            elif not re.search(r"\b(no clothing|no costume|no uniforms?|no armor)\b", first_chunk, re.IGNORECASE):
                first_chunk = first_chunk.rstrip(".!?") + ". No clothing, uniforms, armor, or costumes."
            chunks[0] = first_chunk

            if len(chunks) < 2:
                chunks.append("The skeleton holds role-specific props inside a richly detailed environment that clearly matches the specific topic, with readable background elements and layered depth.")
            if len(chunks) < 3:
                chunks.append("Motion cue: smooth natural arm and head movement with realistic momentum.")
            # Some scene prompts come through as a single sentence. Ensure secondary chunk exists before mutating it.
            if not explicit_outfit_request:
                chunks[1] = re.sub(
                    r"\b(outfit|uniform|suit|coat|armor|jersey|scrubs|clothes|clothing|costume)\b",
                    "props",
                    chunks[1],
                    flags=re.IGNORECASE,
                )
            combined = " ".join(chunks[:3])
            internal_focus = _skeleton_scene_prefers_internal_cutaway(combined)
            minimal_background = _skeleton_scene_requests_minimal_background(combined)
            has_table = bool(re.search(r"\b(table|desk|countertop)\b", combined, re.IGNORECASE))
            has_sit = bool(re.search(r"\b(sit|sits|seated|sitting)\b", combined, re.IGNORECASE))
            has_brain = bool(re.search(r"\bbrain\b", combined, re.IGNORECASE))
            has_money = bool(re.search(r"\b(money|cash|banknotes?|dollars?|currency)\b", combined, re.IGNORECASE))
            if internal_focus and not re.search(r"\b(cutaway|micro|microscopic|internal|bloodstream|blood vessels?|organ|cell|tissue|anatomical|artery|vein)\b", chunks[1], re.IGNORECASE):
                chunks[1] = chunks[1].rstrip(".!?") + ". Use a readable internal or microscopic cutaway environment with clear contextual structures and layered depth."
            if has_table and not has_sit:
                chunks[1] = chunks[1].rstrip(".!?") + ". Skeleton is clearly seated at the table."
            if has_brain and has_money and not re.search(r"\b(two|both)\b", combined, re.IGNORECASE):
                chunks[2] = chunks[2].rstrip(".!?") + ". Keep both required props visible: one brain and one pile of money."
            if not _skeleton_scene_has_environment_cue(combined):
                if minimal_background:
                    chunks[1] = chunks[1].rstrip(".!?") + ". Keep the requested minimal backdrop intentional, premium, and clearly lit."
                elif internal_focus:
                    chunks[1] = chunks[1].rstrip(".!?") + ". Use a readable internal or microscopic cutaway environment with clear contextual structures and layered depth."
                else:
                    chunks[1] = chunks[1].rstrip(".!?") + ". Place the skeleton in a richly detailed environment that clearly matches the topic, with readable background elements and layered depth."
            if not re.search(r"\b(prop|object|item|scene request|requested)\b", " ".join(chunks[:3]), re.IGNORECASE):
                chunks[1] = chunks[1].rstrip(".!?") + ". Keep all explicitly requested props/objects visible and readable."
            if not _skeleton_scene_has_camera_cue(combined):
                chunks[2] = chunks[2].rstrip(".!?") + ". Choose framing that fits the beat: wide environmental for scale, medium action for gestures, low-angle or over-shoulder for drama, and close cutaway or prop detail when needed. Avoid repeating the same centered medium hero shot."

            # Hard default for Skeleton AI: eyeballs must always be present.
            eyes_ok = any(
                re.search(r"\b(eye|eyes|eyeball|eyeballs|iris|pupil|sockets?)\b", c, re.IGNORECASE)
                for c in chunks[:3]
            )
            if not eyes_ok:
                chunks[0] = chunks[0].rstrip(".!?") + " Keep realistic visible eyeballs in both eye sockets."
        else:
            explainer_visuals = template == "story" and _story_scene_prefers_explainer_visuals(" ".join(chunks[:3]))
            first = chunks[0]
            if explainer_visuals:
                if not re.search(r"\b(anatomy|mechanism|process|system|cells?|organs?|brain|blood|temperature|infection|bacteria|virus|heat|sweat|diagram|cutaway|thermometer|molecule)\b", first, re.IGNORECASE):
                    if first:
                        first = f"The key concept or mechanism for this beat {first[0].lower() + first[1:]}"
                    else:
                        first = "The key concept or mechanism for this beat is clearly visible."
                if not re.search(r"\b(environment|setting|location|interior|exterior|city|street|room|forest|desert|studio|cutaway|macro|inside|within)\b", first, re.IGNORECASE):
                    first = first.rstrip(".!?") + " in a specific cinematic environment or readable cutaway view."
                chunks[0] = first

                if len(chunks) < 2:
                    chunks.append("Show the process actively happening with readable cause-and-effect and visible stakes.")
                if len(chunks) < 3:
                    chunks.append("Motion cue: camera movement and transitions preserve visual continuity while explaining the mechanism.")
            else:
                if not re.search(r"\b(main character|protagonist|hero|lead|character|person|subject|crowd|group|family|worker)\b", first, re.IGNORECASE):
                    if first:
                        first = f"The key subject(s) for this beat {first[0].lower() + first[1:]}"
                    else:
                        first = "The key subject(s) for this beat are clearly visible."
                if not re.search(r"\b(environment|setting|location|interior|exterior|city|street|room|forest|desert|studio)\b", first, re.IGNORECASE):
                    first = first.rstrip(".!?") + " in a specific cinematic environment."
                chunks[0] = first

                if len(chunks) < 2:
                    chunks.append("The subject(s) perform a clear action with visible emotional stakes and readable composition.")
                if len(chunks) < 3:
                    chunks.append("Motion cue: camera and subject movement preserve scene-to-scene continuity.")

        if quality_mode == "cinematic":
            if not re.search(r"\b(high[- ]stakes|danger|conflict|urgent|impact|tension|emotional stakes)\b", chunks[0], re.IGNORECASE):
                chunks[0] = chunks[0].rstrip(".!?") + " in an immediate high-stakes moment."
            if len(chunks) < 2:
                chunks.append("Dynamic 24-35mm low-angle cinematic framing with volumetric rim lighting and deep subject separation.")
            elif not re.search(r"\b(lens|camera|low-angle|over-shoulder|dolly|framing|rim light|volumetric)\b", chunks[1], re.IGNORECASE):
                chunks[1] = chunks[1].rstrip(".!?") + ". Dynamic 24-35mm cinematic framing with low-angle depth and volumetric rim lighting."
            if len(chunks) < 3:
                chunks.append("Motion starts instantly with smooth human-like momentum and carries continuity into the next shot.")
            elif not re.search(r"\b(motion|moves|moving|turns|gestures|drift|camera|continuity)\b", chunks[2], re.IGNORECASE):
                chunks[2] = chunks[2].rstrip(".!?") + ". Motion starts instantly with smooth momentum and clear directional continuity."

        scene["visual_description"] = " ".join([
            c.strip().rstrip(".!?") + "."
            for c in chunks[:3]
            if c and c.strip()
        ]).strip()
        constrained.append(scene)
    return constrained


def _force_template_scene_duration(scenes: list, template: str) -> list:
    """Lock selected templates to fixed 5s scenes for stable Kling generation."""
    if template not in {"skeleton", "story", "motivation"}:
        return scenes
    forced = []
    for s in scenes or []:
        scene = dict(s or {})
        scene["duration_sec"] = 5.0
        forced.append(scene)
    return forced


def _job_diag_init(job_id: str, mode: str):
    _prune_in_memory_jobs()
    job = jobs.get(job_id)
    if not job:
        return
    now = time.time()
    job["diagnostics"] = {
        "mode": mode,
        "started_at": now,
        "last_updated_at": now,
        "current_stage": "queued",
        "stage_started_at": now,
        "stage_durations_sec": {},
        "scene_events": [],
    }
    try:
        asyncio.create_task(persist_job_state(job_id, job))
    except Exception:
        pass


def _job_set_stage(job_id: str, status: str, progress: int | None = None):
    job = jobs.get(job_id)
    if not job:
        return
    now = time.time()
    diag = job.setdefault("diagnostics", {
        "mode": "unknown",
        "started_at": now,
        "last_updated_at": now,
        "current_stage": "queued",
        "stage_started_at": now,
        "stage_durations_sec": {},
        "scene_events": [],
    })
    prev_stage = diag.get("current_stage")
    prev_started = float(diag.get("stage_started_at") or now)
    if prev_stage:
        elapsed = max(0.0, now - prev_started)
        durations = diag.setdefault("stage_durations_sec", {})
        durations[prev_stage] = round(float(durations.get(prev_stage, 0.0)) + elapsed, 2)
    diag["current_stage"] = status
    diag["stage_started_at"] = now
    diag["last_updated_at"] = now
    job["status"] = status
    if progress is not None:
        job["progress"] = progress
    try:
        asyncio.create_task(persist_job_state(job_id, job))
    except Exception:
        pass


def _job_record_scene_event(job_id: str, scene_idx: int, total_scenes: int, event: str, detail: str = ""):
    job = jobs.get(job_id)
    if not job:
        return
    now = time.time()
    diag = job.setdefault("diagnostics", {"scene_events": [], "last_updated_at": now})
    events = diag.setdefault("scene_events", [])
    events.append({
        "ts": round(now, 3),
        "scene": scene_idx + 1,
        "total_scenes": total_scenes,
        "event": event,
        "detail": detail[:240],
    })
    if len(events) > 30:
        del events[:-30]
    diag["last_updated_at"] = now
    try:
        asyncio.create_task(persist_job_state(job_id, job))
    except Exception:
        pass


def _job_diag_finalize(job_id: str):
    job = jobs.get(job_id)
    if not job:
        return
    now = time.time()
    diag = job.get("diagnostics")
    if not isinstance(diag, dict):
        return
    current_stage = diag.get("current_stage")
    stage_started = float(diag.get("stage_started_at") or now)
    if current_stage:
        elapsed = max(0.0, now - stage_started)
        durations = diag.setdefault("stage_durations_sec", {})
        durations[current_stage] = round(float(durations.get(current_stage, 0.0)) + elapsed, 2)
    diag["stage_started_at"] = now
    diag["last_updated_at"] = now
    status = str(job.get("status", "") or "")
    if status in {"complete", "error"} and not job.get("completed_at"):
        job["completed_at"] = now
    if status == "error" and job.get("credit_charged") and (not job.get("credit_refunded")):
        user_id = str(job.get("user_id", "") or "")
        source = str(job.get("credit_source", "") or "")
        month_key = str(job.get("credit_month_key", "") or "")
        if user_id and source in {"monthly", "topup"}:
            try:
                asyncio.create_task(_refund_generation_credit(
                    user_id,
                    source,
                    month_key=month_key,
                    credits=int(job.get("credit_amount", 1) or 1),
                ))
                job["credit_refunded"] = True
            except Exception:
                pass
    if status in {"complete", "error"} and job.get("credit_charged"):
        _append_usage_ledger({
            "type": "generation_terminal",
            "job_id": job_id,
            "user_id": str(job.get("user_id", "") or ""),
            "status": status,
            "template": str(job.get("template", "") or ""),
            "plan": str(job.get("plan", "") or ""),
            "quality_mode": str(job.get("quality_mode", "") or ""),
            "mint_mode": bool(job.get("mint_mode", False)),
            "credit_source": str(job.get("credit_source", "") or ""),
            "credit_refunded": bool(job.get("credit_refunded", False)),
            "estimated_cost_usd": float(_estimate_job_cost_usd(job)),
            "ts": now,
        })
    _record_kpi_for_job(job_id, job)
    try:
        asyncio.create_task(persist_job_state(job_id, job))
    except Exception:
        pass

async def run_generation_pipeline(
    job_id: str,
    template: str,
    topic: str,
    resolution: str = "720p",
    language: str = "en",
    quality_mode: str = "standard",
    mint_mode: bool = True,
    transition_style: str = "smooth",
    micro_escalation_mode: bool = True,
):
    try:
        job_state = jobs.get(job_id, {})
        voice_id = str(job_state.get("voice_id", "") or "").strip()
        voice_speed = _normalize_voice_speed(job_state.get("voice_speed", 1.0), default=1.0)
        pacing_mode = _normalize_pacing_mode(job_state.get("pacing_mode", "standard"))
        art_style = _normalize_art_style(job_state.get("art_style", "auto"), template=template)
        reference_image_url = _normalize_reference_with_default(template, str(job_state.get("reference_image_url", "") or "").strip())
        reference_lock_mode = _normalize_reference_lock_mode(job_state.get("reference_lock_mode"), default="strict")
        reference_dna = job_state.get("reference_dna", {}) if isinstance(job_state.get("reference_dna"), dict) else {}
        _job_set_stage(job_id, "generating_script", 5)
        lang_name = SUPPORTED_LANGUAGES.get(language, {}).get("name", "English")
        log.info(f"[{job_id}] Generating script for '{topic}' ({template}, {resolution}, {lang_name})")

        lang_instruction = ""
        if language != "en":
            lang_instruction = f"\n\nIMPORTANT: Write ALL narration text in {lang_name}. The visual_description fields should remain in English (for image generation), but ALL narration/voiceover text MUST be in {lang_name}."
        script_data = await generate_script(template, topic, extra_instructions=lang_instruction)
        scenes = _normalize_scenes_for_render(script_data.get("scenes", []))
        quality_mode = _normalize_skeleton_quality_mode(quality_mode, template=template)
        mint_mode = _normalize_mint_mode(mint_mode, template=template)
        micro_escalation_mode = _normalize_micro_escalation_mode(micro_escalation_mode, template=template)
        scenes = _apply_template_scene_constraints(scenes, template, quality_mode=quality_mode)
        scenes = _apply_mint_scene_compiler(scenes, template, mint_mode=mint_mode)
        if not (template == "story" and pacing_mode != "standard"):
            scenes = _force_template_scene_duration(scenes, template)
        scenes = _apply_story_pacing(scenes, template, pacing_mode=pacing_mode)
        if not scenes:
            raise ValueError("Script generation returned no scenes")

        fal_video_enabled = bool(FAL_AI_KEY)
        runway_video_enabled = bool(RUNWAY_API_KEY)
        use_video_engine = fal_video_enabled or runway_video_enabled
        animation_enabled = _bool_from_any(job_state.get("animation_enabled"), _bool_from_any(job_state.get("story_animation_enabled"), True))
        use_video = bool(use_video_engine and animation_enabled)
        if template == "reddit":
            use_video = False
        if template != "reddit" and animation_enabled and not use_video_engine:
            raise RuntimeError("Video is required but no engine is configured (set RUNWAY_API_KEY or FAL_AI_KEY)")
        if not animation_enabled or template == "reddit":
            mode_label = "static image"
        elif runway_video_enabled:
            mode_label = "Runway (primary)"
        elif fal_video_enabled:
            mode_label = "FalAI Kling 2.1"
        else:
            mode_label = "static image"
        jobs[job_id]["generation_mode"] = "video" if use_video else "image"

        _job_set_stage(job_id, "generating_images", 10)
        jobs[job_id]["total_scenes"] = len(scenes)
        log.info(f"[{job_id}] Script ready: {len(scenes)} scenes. Mode: {mode_label}, {resolution}")

        neg_prompt = TEMPLATE_NEGATIVE_PROMPTS.get(template, NEGATIVE_PROMPT)
        scene_assets = []
        scene_prompts = [str(s.get("visual_description", "") or "") for s in scenes]
        scene_images = []
        total_steps = len(scenes) * (2 if use_video else 1)
        gen_ts = str(int(time.time() * 1000))

        skeleton_anchor = ""
        skeleton_reference_image_url = ""
        if template == "skeleton":
            skeleton_anchor = _canonical_skeleton_anchor()
            skeleton_reference_image_url = reference_image_url or SKELETON_GLOBAL_REFERENCE_IMAGE_URL

        for i, scene in enumerate(scenes):
            jobs[job_id]["current_scene"] = i + 1
            step_base = i * (2 if use_video else 1)
            _job_set_stage(job_id, "generating_images", 10 + int((step_base / total_steps) * 55))
            _job_record_scene_event(job_id, i, len(scenes), "image_start")

            full_prompt = _build_scene_prompt_with_reference(
                template=template,
                visual_description=scene.get("visual_description", ""),
                quality_mode=quality_mode,
                skeleton_anchor=skeleton_anchor,
                reference_dna=reference_dna,
                reference_lock_mode=reference_lock_mode,
                art_style=art_style,
            )
            img_path = str(TEMP_DIR / (job_id + "_scene_" + str(i) + ".png"))
            scene_reference_url = _resolve_reference_for_scene(job_state, template, i) or (
                skeleton_reference_image_url if template == "skeleton" else reference_image_url
            )
            img_result = await generate_scene_image(
                full_prompt,
                img_path,
                resolution=resolution,
                negative_prompt=neg_prompt,
                template=template,
                reference_image_url=scene_reference_url,
                reference_lock_mode=reference_lock_mode,
            )
            if template == "skeleton" and not skeleton_reference_image_url and i == 0:
                skeleton_reference_image_url = _file_to_data_image_url(img_path)
            cdn_url = img_result.get("cdn_url")
            engine_name = "Skeleton LoRA" if (template == "skeleton" and not cdn_url) else ("Grok Imagine" if cdn_url else "SDXL")
            log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} image generated ({engine_name})")
            _job_record_scene_event(job_id, i, len(scenes), "image_ready", engine_name)
            if resolution == "720p":
                persisted_scene = _persist_auto_scene_image(
                    job_id,
                    i,
                    img_path,
                    scene,
                    template,
                    img_result,
                    source="initial_auto",
                )
                if persisted_scene:
                    scene_images.append(persisted_scene)

            asset = {"image": img_path, "frames": None, "kling_clip": None}

            if use_video:
                _job_set_stage(job_id, "animating_scenes", 10 + int(((step_base + 1) / total_steps) * 55))
                _job_record_scene_event(job_id, i, len(scenes), "animation_start")
                kling_motion = TEMPLATE_KLING_MOTION.get(template, "Cinematic motion, smooth camera movement, subtle animation.")
                anim_prompt = scene.get("visual_description", "") + " " + kling_motion
                try:
                    anim_result = await animate_scene(
                        img_path, anim_prompt,
                        str(TEMP_DIR), i, gen_ts,
                        duration_sec=scene.get("duration_sec", 5),
                        image_cdn_url=cdn_url,
                        prefer_wan=(template == "skeleton"),
                    )
                except Exception as anim_err:
                    jobs[job_id]["animation_warnings"] = int(jobs[job_id].get("animation_warnings", 0)) + 1
                    log.warning(f"[{job_id}] Scene {i+1}/{len(scenes)} animation failed, using static image: {anim_err}")
                    _job_record_scene_event(job_id, i, len(scenes), "animation_failed", str(anim_err))
                    anim_result = {"type": "static"}
                if anim_result["type"] in ("kling_clip", "wan_clip", "grok_clip", "runway_clip"):
                    asset["kling_clip"] = anim_result["path"]
                    if anim_result["type"] == "runway_clip":
                        engine = "Runway"
                    elif anim_result["type"] == "grok_clip":
                        engine = "Grok Imagine Video"
                    elif anim_result["type"] == "kling_clip":
                        engine = "Kling 2.1"
                    else:
                        engine = "Wan 2.2"
                    log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} animated by {engine}")
                    _job_record_scene_event(job_id, i, len(scenes), "animation_ready", engine)
                else:
                    log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} using static image")
                    _job_record_scene_event(job_id, i, len(scenes), "animation_static")

            scene_assets.append(asset)

        jobs[job_id]["scene_prompts"] = scene_prompts
        jobs[job_id]["scene_images"] = scene_images

        _job_set_stage(job_id, "generating_voice", 70)
        log.info(f"[{job_id}] Generating voiceover...")

        full_narration = " ".join(s.get("narration", "") for s in scenes)
        audio_path = str(TEMP_DIR / (job_id + "_voice.mp3"))
        vo_result = await generate_voiceover(
            full_narration,
            audio_path,
            template=template,
            language=language,
            override_voice_id=voice_id,
            override_speed=voice_speed,
        )
        audio_path = vo_result["audio_path"]
        word_timings = vo_result.get("word_timings", [])

        subtitle_path = None
        if word_timings:
            subtitle_path = str(TEMP_DIR / (job_id + "_captions.ass"))
            generate_ass_subtitles(word_timings, subtitle_path, resolution=resolution, template=template)
            log.info(f"[{job_id}] Word-synced captions generated: {len(word_timings)} words ({lang_name})")

        sfx_paths = []
        if _sfx_enabled():
            _job_set_stage(job_id, "generating_sfx", 78)
            for i, scene in enumerate(scenes):
                sfx_out = str(TEMP_DIR / (job_id + "_sfx_" + str(i) + ".mp3"))
                desc = scene.get("visual_description", "")
                dur = scene.get("duration_sec", 5)
                sfx_file = await generate_scene_sfx(desc, dur, sfx_out, template=template, scene_index=i, total_scenes=len(scenes))
                sfx_paths.append(sfx_file)
            sfx_paths = await _quintuple_check_scene_sfx(scenes, sfx_paths, template, job_id=job_id)
            log.info(f"[{job_id}] SFX generated: {sum(1 for s in sfx_paths if s)}/{len(scenes)} scenes")
        else:
            log.info(f"[{job_id}] SFX disabled globally; skipping generation/mix")

        _job_set_stage(job_id, "compositing", 82)
        log.info(f"[{job_id}] Compositing final video at {resolution}...")

        output_filename = template + "_" + job_id + ".mp4"
        output_path = str(OUTPUT_DIR / output_filename)
        await composite_video(
            scenes,
            scene_assets,
            audio_path,
            output_path,
            resolution=resolution,
            use_svd=use_video,
            subtitle_path=subtitle_path,
            sfx_paths=sfx_paths,
            transition_style=_normalize_transition_style(transition_style),
            micro_escalation_mode=micro_escalation_mode,
        )

        for sfx in sfx_paths:
            if sfx:
                Path(sfx).unlink(missing_ok=True)
        for asset in scene_assets:
            Path(asset["image"]).unlink(missing_ok=True)
            if asset.get("kling_clip"):
                Path(asset["kling_clip"]).unlink(missing_ok=True)
            if asset.get("frames"):
                for fp in asset["frames"]:
                    Path(fp).unlink(missing_ok=True)
                frame_dir = Path(asset["frames"][0]).parent
                if frame_dir.exists():
                    shutil.rmtree(frame_dir, ignore_errors=True)
        Path(audio_path).unlink(missing_ok=True)
        if subtitle_path:
            Path(subtitle_path).unlink(missing_ok=True)

        _job_set_stage(job_id, "complete", 100)
        jobs[job_id]["output_file"] = output_filename
        jobs[job_id]["resolution"] = resolution
        jobs[job_id]["metadata"] = {
            "title": script_data.get("title", topic),
            "description": script_data.get("description", ""),
            "tags": script_data.get("tags", []),
        }
        await _update_project_by_job(job_id, {
            "status": "rendered",
            "output_file": output_filename,
            "title": script_data.get("title", topic),
        })
        _job_diag_finalize(job_id)
        log.info(f"[{job_id}] COMPLETE: {output_filename} ({resolution}, {mode_label})")

    except Exception as e:
        log.error(f"[{job_id}] Pipeline failed: {e}", exc_info=True)
        _job_set_stage(job_id, "error")
        jobs[job_id]["error"] = str(e)
        _job_diag_finalize(job_id)
        await _update_project_by_job(job_id, {"status": "error", "error": str(e)})


# ─── API Endpoints ────────────────────────────────────────────────────────────

async def _create_or_update_project(project_id: str, data: dict):
    async with _projects_lock:
        existing = _projects.get(project_id, {})
        merged = {**existing, **data}
        merged["project_id"] = project_id
        merged["updated_at"] = time.time()
        if "created_at" not in merged:
            merged["created_at"] = time.time()
        _projects[project_id] = merged
        _save_projects_store()


async def _update_project_by_job(job_id: str, fields: dict):
    async with _projects_lock:
        target_id = None
        for pid, p in _projects.items():
            if p.get("job_id") == job_id:
                target_id = pid
                break
        if not target_id:
            return
        _projects[target_id] = {**_projects[target_id], **fields, "updated_at": time.time()}
        _save_projects_store()


async def _update_project_by_session(user_id: str, session_id: str, fields: dict):
    async with _projects_lock:
        target_id = None
        for pid, p in _projects.items():
            if p.get("user_id") == user_id and p.get("session_id") == session_id:
                target_id = pid
                break
        if not target_id:
            return
        _projects[target_id] = {**_projects[target_id], **fields, "updated_at": time.time()}
        _save_projects_store()


class LongFormPauseError(RuntimeError):
    def __init__(self, message: str, details: dict):
        super().__init__(message)
        self.details = dict(details or {})


def _longform_review_state(session: dict) -> dict:
    chapters = list((session or {}).get("chapters") or [])
    total = len(chapters)
    approved = sum(1 for c in chapters if str((c or {}).get("status", "")).strip().lower() == "approved")
    pending = total - approved
    avg_score = 0.0
    if total > 0:
        avg_score = sum(float((c or {}).get("viral_score", 0) or 0) for c in chapters) / total
    return {
        "total_chapters": total,
        "approved_chapters": approved,
        "pending_chapters": max(0, pending),
        "all_approved": (total > 0 and approved == total),
        "viral_score_total": round(avg_score, 2),
    }


def _longform_public_session(session: dict) -> dict:
    s = dict(session or {})
    chapters = []
    for ch in list(s.get("chapters") or []):
        chapter = dict(ch or {})
        chapter_scenes = []
        for scene_idx, raw_scene in enumerate(list(chapter.get("scenes") or [])):
            scene = dict(raw_scene or {})
            chapter_scenes.append({
                "scene_num": int(scene.get("scene_num", scene_idx + 1) or (scene_idx + 1)),
                "duration_sec": float(scene.get("duration_sec", 5.0) or 5.0),
                "narration": str(scene.get("narration", "") or ""),
                "visual_description": str(scene.get("visual_description", "") or ""),
                "text_overlay": str(scene.get("text_overlay", "") or ""),
                "image_url": str(scene.get("image_url", "") or ""),
                "image_status": str(scene.get("image_status", "missing") or "missing"),
                "image_error": str(scene.get("image_error", "") or ""),
            })
        chapters.append({
            "index": int(chapter.get("index", 0) or 0),
            "title": str(chapter.get("title", "") or ""),
            "summary": str(chapter.get("summary", "") or ""),
            "target_sec": float(chapter.get("target_sec", 0) or 0),
            "status": str(chapter.get("status", "pending_review") or "pending_review"),
            "retry_count": int(chapter.get("retry_count", 0) or 0),
            "viral_score": int(chapter.get("viral_score", 0) or 0),
            "brand_slot": str(chapter.get("brand_slot", "") or ""),
            "scene_count": len(chapter_scenes),
            "scenes": chapter_scenes,
            "last_error": str(chapter.get("last_error", "") or ""),
        })
    return {
        "session_id": str(s.get("session_id", "") or ""),
        "template": str(s.get("template", "") or ""),
        "format_preset": str(s.get("format_preset", "explainer") or "explainer"),
        "topic": str(s.get("topic", "") or ""),
        "input_title": str(s.get("input_title", "") or ""),
        "input_description": str(s.get("input_description", "") or ""),
        "source_url": str(s.get("source_url", "") or ""),
        "analytics_notes": str(s.get("analytics_notes", "") or ""),
        "strategy_notes": str(s.get("strategy_notes", "") or ""),
        "target_minutes": float(s.get("target_minutes", 0) or 0),
        "language": str(s.get("language", "en") or "en"),
        "resolution": str(s.get("resolution", "720p_landscape") or "720p_landscape"),
        "animation_enabled": bool(s.get("animation_enabled", True)),
        "sfx_enabled": bool(s.get("sfx_enabled", True)),
        "whisper_mode": str(s.get("whisper_mode", "subtle") or "subtle"),
        "status": str(s.get("status", "draft_review") or "draft_review"),
        "job_id": str(s.get("job_id", "") or ""),
        "paused_error": s.get("paused_error", None),
        "metadata_pack": dict(s.get("metadata_pack") or {}),
        "chapters": chapters,
        "review_state": _longform_review_state(s),
        "draft_progress": dict(s.get("draft_progress") or {}),
        "created_at": float(s.get("created_at", 0) or 0),
        "updated_at": float(s.get("updated_at", 0) or 0),
        "package": dict(s.get("package") or {}),
    }


def _longform_session_summary(session: dict) -> dict:
    s = dict(session or {})
    chapters = list(s.get("chapters") or [])
    preview_image_url = ""
    for chapter in reversed(chapters):
        chapter_scenes = list((chapter or {}).get("scenes") or [])
        for scene in chapter_scenes:
            candidate = str((scene or {}).get("image_url", "") or "").strip()
            if candidate:
                preview_image_url = candidate
                break
        if preview_image_url:
            break
    package = dict(s.get("package") or {})
    output_file = str(package.get("output_file", "") or "")
    return {
        "session_id": str(s.get("session_id", "") or ""),
        "template": str(s.get("template", "") or ""),
        "format_preset": str(s.get("format_preset", "explainer") or "explainer"),
        "topic": str(s.get("topic", "") or ""),
        "input_title": str(s.get("input_title", "") or ""),
        "source_url": str(s.get("source_url", "") or ""),
        "target_minutes": float(s.get("target_minutes", 0) or 0),
        "language": str(s.get("language", "en") or "en"),
        "resolution": str(s.get("resolution", "720p_landscape") or "720p_landscape"),
        "status": str(s.get("status", "draft_review") or "draft_review"),
        "job_id": str(s.get("job_id", "") or ""),
        "review_state": _longform_review_state(s),
        "draft_progress": dict(s.get("draft_progress") or {}),
        "paused_error": s.get("paused_error", None),
        "preview_image_url": preview_image_url,
        "output_file": output_file,
        "created_at": float(s.get("created_at", 0) or 0),
        "updated_at": float(s.get("updated_at", 0) or 0),
    }


def _longform_chapter_scene_targets(target_minutes: float) -> tuple[int, float]:
    chapter_count = _longform_chapter_count_for_minutes(target_minutes)
    chapter_target_sec = max(35.0, float(target_minutes) * 60.0 / max(chapter_count, 1))
    return chapter_count, chapter_target_sec


def _longform_placeholder_chapter(
    chapter_index: int,
    chapter_target_sec: float,
    brand_slot: str = "",
    status: str = "awaiting_previous_approval",
) -> dict:
    return {
        "index": int(chapter_index),
        "title": f"Chapter {int(chapter_index) + 1}",
        "summary": "Generating chapter draft...",
        "target_sec": round(float(chapter_target_sec), 2),
        "scenes": [],
        "status": str(status or "awaiting_previous_approval"),
        "retry_count": 0,
        "brand_slot": str(brand_slot or ""),
        "viral_score": 0,
        "last_error": "",
    }


def _longform_fallback_chapter(
    topic: str,
    input_title: str,
    chapter_index: int,
    chapter_target_sec: float,
    chapter_count: int,
    brand_slot: str = "",
    tone: str = "neutral",
    template: str = "story",
) -> dict:
    scene_goal = max(6, min(24, int(round(float(chapter_target_sec) / 5.0))))
    scenes: list[dict] = []
    for i in range(scene_goal):
        beat = i + 1
        scenes.append({
            "scene_num": beat,
            "duration_sec": 5.0,
            "narration": (
                f"{input_title}: chapter {chapter_index + 1}, beat {beat}. "
                f"This section deepens the core idea of {topic} with clear progression and stakes."
            ),
            "visual_description": (
                f"Cinematic documentary frame for {topic}, chapter {chapter_index + 1}, beat {beat}; "
                "clear subject, dynamic camera motion, realistic lighting, high detail."
            ),
            "text_overlay": f"Chapter {chapter_index + 1} - Beat {beat}",
        })
    normalized = _normalize_longform_scenes_for_render(scenes)
    normalized = _scale_scene_durations_to_target(normalized, chapter_target_sec)
    normalized = _longform_enforce_tone_on_scenes(normalized, tone=tone, template=template)
    out = {
        "index": int(chapter_index),
        "title": f"Chapter {chapter_index + 1} - {topic[:64].strip() or 'Main Segment'}",
        "summary": (
            f"Fallback draft for chapter {chapter_index + 1} of {chapter_count}. "
            "Review and regenerate if needed."
        ),
        "tone": str(tone or "neutral"),
        "target_sec": round(float(len(normalized) * 5.0), 2),
        "scenes": normalized,
        "status": "pending_review",
        "retry_count": 0,
        "brand_slot": str(brand_slot or ""),
        "viral_score": 0,
        "last_error": "",
    }
    out = _longform_apply_brand_slot(out, brand_slot, input_title=input_title)
    out["viral_score"] = _longform_chapter_retention_score(out)
    return out


def _longform_preview_filename(session_id: str, chapter_index: int, scene_index: int) -> str:
    safe_sid = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(session_id or "").strip())[:64] or "lf"
    return f"{safe_sid}_c{int(chapter_index) + 1:02d}_s{int(scene_index) + 1:03d}.png"


def _longform_preview_url(filename: str) -> str:
    return f"/api/longform/preview/{filename}"


async def _longform_attach_scene_previews(session_id: str, template: str, chapter: dict, resolution: str = "720p_landscape") -> dict:
    out = dict(chapter or {})
    chapter_index = int(out.get("index", 0) or 0)
    scenes = _normalize_longform_scenes_for_render(list(out.get("scenes") or []))
    scenes = _scale_scene_durations_to_target(scenes, out.get("target_sec", 0))
    if not scenes:
        out["scenes"] = []
        out["target_sec"] = 0.0
        return out

    neg_prompt = TEMPLATE_NEGATIVE_PROMPTS.get(template, NEGATIVE_PROMPT)
    skeleton_anchor = _canonical_skeleton_anchor() if template == "skeleton" else ""
    # Previews should be cheap and fast. Avoid strict conditioning that can force costly retries.
    reference_image_url = ""
    preview_resolution = "720p_landscape" if str(resolution or "").endswith("_landscape") else "720p"
    LONGFORM_PREVIEW_DIR.mkdir(parents=True, exist_ok=True)
    now_cache_bust = int(time.time())
    chapter_tone = str(out.get("tone", "") or "").strip().lower()
    if chapter_tone not in {"horror", "neutral"}:
        derived_narration = " ".join(str((s or {}).get("narration", "") or "") for s in scenes)
        chapter_tone = _longform_detect_tone(
            template,
            str(out.get("title", "") or ""),
            str(out.get("summary", "") or ""),
            derived_narration,
        )
    out["tone"] = chapter_tone
    if _longform_is_horror_tone(chapter_tone):
        neg_prompt = (
            neg_prompt
            + ", bright cheerful lighting, warm happy tone, colorful festival mood, comedy framing, daylight beach party aesthetics"
        )

    scripted_scenes: list[dict] = []
    for scene_idx, raw_scene in enumerate(scenes):
        scene = dict(raw_scene or {})
        scene["scene_num"] = int(scene.get("scene_num", scene_idx + 1) or (scene_idx + 1))
        scene["duration_sec"] = 5.0
        scene["visual_description"] = _longform_tone_locked_visual_description(
            str(scene.get("visual_description", "") or ""),
            tone=chapter_tone,
            template=template,
        )
        scene["image_url"] = ""
        scene["image_status"] = "pending_script_lock"
        scene["image_error"] = ""
        scripted_scenes.append(scene)

    out["scenes"] = scripted_scenes
    out["target_sec"] = round(float(len(scripted_scenes) * 5.0), 2)

    # Persist all scripted scenes first so owner can review story flow before images finish.
    try:
        async with _longform_sessions_lock:
            live = _longform_sessions.get(session_id)
            if isinstance(live, dict):
                chapters_live = list(live.get("chapters") or [])
                if 0 <= chapter_index < len(chapters_live):
                    chapter_live = dict(chapters_live[chapter_index] or {})
                    chapter_live["scenes"] = [dict(s) for s in scripted_scenes]
                    chapter_live["target_sec"] = out["target_sec"]
                    chapter_live["status"] = "draft_generating_images"
                    chapters_live[chapter_index] = chapter_live
                    live["chapters"] = chapters_live
                    progress = dict(live.get("draft_progress") or {})
                    progress["preview_scene_total"] = int(len(scripted_scenes))
                    progress["preview_scene_generated"] = 0
                    live["draft_progress"] = progress
                    live["updated_at"] = time.time()
                    _save_longform_sessions()
    except Exception:
        pass

    for scene_idx, raw_scene in enumerate(scripted_scenes):
        scene = dict(raw_scene or {})
        scene["image_status"] = "generating"
        visual_desc = _longform_tone_locked_visual_description(
            str(scene.get("visual_description", "") or ""),
            tone=chapter_tone,
            template=template,
        )
        scene["visual_description"] = visual_desc

        prompt = _build_scene_prompt_with_reference(
            template=template,
            visual_description=visual_desc,
            quality_mode="cinematic",
            skeleton_anchor=skeleton_anchor,
            reference_dna={},
            reference_lock_mode="strict",
            art_style="auto",
        )
        filename = _longform_preview_filename(session_id, chapter_index, scene_idx)
        output_path = str(LONGFORM_PREVIEW_DIR / filename)
        try:
            img_result = await generate_scene_image(
                prompt,
                output_path,
                resolution=preview_resolution,
                negative_prompt=neg_prompt,
                template=template,
                reference_image_url=reference_image_url,
                reference_lock_mode="inspired",
                best_of_enabled=False,
                salvage_enabled=False,
            )
            if Path(output_path).exists():
                scene["image_url"] = f"{_longform_preview_url(filename)}?v={now_cache_bust}"
                scene["image_status"] = "ready"
                scene["image_error"] = ""
            else:
                cdn_url = str((img_result or {}).get("cdn_url", "") or "")
                if cdn_url:
                    scene["image_url"] = cdn_url
                    scene["image_status"] = "ready"
                    scene["image_error"] = ""
                else:
                    scene["image_status"] = "error"
                    scene["image_error"] = "Preview image was not generated"
        except Exception as e:
            scene["image_status"] = "error"
            scene["image_error"] = str(e)[:220]

        scripted_scenes[scene_idx] = scene
        # Stream image progress while preserving full scripted scene list.
        try:
            async with _longform_sessions_lock:
                live = _longform_sessions.get(session_id)
                if isinstance(live, dict):
                    chapters_live = list(live.get("chapters") or [])
                    if 0 <= chapter_index < len(chapters_live):
                        chapter_live = dict(chapters_live[chapter_index] or {})
                        chapter_live["scenes"] = [dict(s) for s in scripted_scenes]
                        chapter_live["target_sec"] = out["target_sec"]
                        chapter_live["status"] = "draft_generating_images"
                        chapters_live[chapter_index] = chapter_live
                        live["chapters"] = chapters_live
                        progress = dict(live.get("draft_progress") or {})
                        progress["preview_scene_total"] = int(len(scripted_scenes))
                        progress["preview_scene_generated"] = int(scene_idx + 1)
                        live["draft_progress"] = progress
                        live["updated_at"] = time.time()
                        _save_longform_sessions()
        except Exception:
            pass

    out["scenes"] = scripted_scenes
    out["target_sec"] = round(float(len(scripted_scenes) * 5.0), 2)
    return out


def _longform_generated_chapter_count(chapters: list[dict]) -> int:
    total = 0
    for chapter in list(chapters or []):
        if list((chapter or {}).get("scenes") or []):
            total += 1
    return total


def _longform_approved_chapter_count(chapters: list[dict]) -> int:
    return sum(1 for chapter in list(chapters or []) if str((chapter or {}).get("status", "") or "") == "approved")


async def _generate_longform_chapter_for_session(session_id: str, chapter_index: int) -> None:
    chapter_count = 0
    fallback_used = 0
    try:
        async with _longform_sessions_lock:
            session = dict(_longform_sessions.get(session_id) or {})
        if not session:
            return

        template = _normalize_longform_template(session.get("template", "story"))
        format_preset = str(session.get("format_preset", "explainer") or "explainer").strip().lower()
        topic = str(session.get("topic", "") or "").strip()
        input_title = str(session.get("input_title", "") or "").strip()
        input_description = str(session.get("input_description", "") or "").strip()
        metadata_pack = dict(session.get("metadata_pack") or {})
        source_context = str(metadata_pack.get("source_context", "") or "").strip()
        strategy_notes = _marketing_doctrine_text(str(session.get("strategy_notes", "") or "").strip())
        language = _normalize_longform_language(session.get("language", "en"))
        resolution = str(session.get("resolution", "720p_landscape") or "720p_landscape")
        chapter_tone = _longform_detect_tone(template, topic, input_title, input_description)
        target_minutes = _normalize_longform_target_minutes(session.get("target_minutes", LONGFORM_DEFAULT_TARGET_MINUTES))
        chapter_count, chapter_target_sec = _longform_chapter_scene_targets(target_minutes)

        chapter_list = list(session.get("chapters") or [])
        if chapter_index < 0 or chapter_index >= len(chapter_list):
            return
        chapter_seed = dict(chapter_list[chapter_index] or {})
        slot = str(chapter_seed.get("brand_slot", "") or _longform_brand_slot(chapter_index, chapter_count))
        prior_retry = int(chapter_seed.get("retry_count", 0) or 0)

        try:
            chapter = await _generate_longform_chapter(
                template=template,
                topic=topic,
                input_title=input_title,
                input_description=input_description,
                format_preset=format_preset,
                chapter_index=chapter_index,
                chapter_count=chapter_count,
                chapter_target_sec=chapter_target_sec,
                language=language,
                brand_slot=slot,
                source_context=source_context,
                strategy_notes=strategy_notes,
            )
            chapter["last_error"] = ""
        except Exception as e:
            fallback_used = 1
            chapter = _longform_fallback_chapter(
                topic=topic,
                input_title=input_title or topic or "Untitled",
                chapter_index=chapter_index,
                chapter_target_sec=chapter_target_sec,
                chapter_count=chapter_count,
                brand_slot=slot,
                tone=chapter_tone,
                template=template,
            )
            chapter["last_error"] = f"AI draft failed once ({type(e).__name__}); fallback draft generated."
            log.warning(f"[longform:{session_id}] chapter {chapter_index + 1}/{chapter_count} fallback used: {e}")

        chapter = await _longform_attach_scene_previews(
            session_id=session_id,
            template=template,
            chapter=chapter,
            resolution=resolution,
        )
        chapter["status"] = "pending_review"
        chapter["retry_count"] = max(prior_retry, int(chapter.get("retry_count", 0) or 0))

        async with _longform_sessions_lock:
            live = _longform_sessions.get(session_id)
            if not isinstance(live, dict):
                return
            chapters_live = list(live.get("chapters") or [])
            if chapter_index >= len(chapters_live):
                return
            chapters_live[chapter_index] = chapter
            live["chapters"] = chapters_live
            progress = dict(live.get("draft_progress") or {})
            live["status"] = "draft_review"
            live["draft_progress"] = {
                "total_chapters": int(len(chapters_live)),
                "generated_chapters": int(_longform_generated_chapter_count(chapters_live)),
                "approved_chapters": int(_longform_approved_chapter_count(chapters_live)),
                "failed_chapters": int(progress.get("failed_chapters", 0) or 0) + int(fallback_used),
                "stage": "awaiting_owner_approval",
            }
            live["updated_at"] = time.time()
            _save_longform_sessions()
    except Exception as e:
        log.error(f"[longform:{session_id}] chapter generation failed: {e}", exc_info=True)
        async with _longform_sessions_lock:
            live = _longform_sessions.get(session_id)
            if isinstance(live, dict):
                live["status"] = "error"
                live["paused_error"] = {
                    "stage": "draft_generation",
                    "chapter_index": int(chapter_index),
                    "error": str(e),
                }
                chapters_live = list(live.get("chapters") or [])
                progress = dict(live.get("draft_progress") or {})
                live["draft_progress"] = {
                    "total_chapters": int(chapter_count or len(chapters_live)),
                    "generated_chapters": int(_longform_generated_chapter_count(chapters_live)),
                    "approved_chapters": int(_longform_approved_chapter_count(chapters_live)),
                    "failed_chapters": int(progress.get("failed_chapters", 0) or 0),
                    "stage": "error",
                }
                live["updated_at"] = time.time()
                _save_longform_sessions()


async def _queue_next_longform_chapter_if_ready(session_id: str) -> None:
    next_idx: int | None = None
    async with _longform_sessions_lock:
        live = _longform_sessions.get(session_id)
        if not isinstance(live, dict):
            return
        chapters = list(live.get("chapters") or [])
        if not chapters:
            return
        if any(str((c or {}).get("status", "") or "") == "draft_generating" for c in chapters):
            return

        for i, chapter in enumerate(chapters):
            status = str((chapter or {}).get("status", "") or "")
            if status == "awaiting_previous_approval":
                next_idx = i
                break

        generated = _longform_generated_chapter_count(chapters)
        approved = _longform_approved_chapter_count(chapters)
        progress = dict(live.get("draft_progress") or {})

        if next_idx is None:
            live["status"] = "draft_review"
            live["draft_progress"] = {
                "total_chapters": int(len(chapters)),
                "generated_chapters": int(generated),
                "approved_chapters": int(approved),
                "failed_chapters": int(progress.get("failed_chapters", 0) or 0),
                "stage": "ready_for_finalize" if approved == len(chapters) else "awaiting_owner_approval",
            }
            live["updated_at"] = time.time()
            _save_longform_sessions()
            return

        if any(str((chapters[j] or {}).get("status", "") or "") != "approved" for j in range(next_idx)):
            live["status"] = "draft_review"
            live["draft_progress"] = {
                "total_chapters": int(len(chapters)),
                "generated_chapters": int(generated),
                "approved_chapters": int(approved),
                "failed_chapters": int(progress.get("failed_chapters", 0) or 0),
                "stage": "awaiting_owner_approval",
            }
            live["updated_at"] = time.time()
            _save_longform_sessions()
            return

        chapter = dict(chapters[next_idx] or {})
        chapter["status"] = "draft_generating"
        chapter["summary"] = chapter.get("summary") or "Generating chapter draft..."
        chapter["last_error"] = ""
        chapter["scenes"] = []
        chapters[next_idx] = chapter
        live["chapters"] = chapters
        live["status"] = "draft_generating"
        live["draft_progress"] = {
            "total_chapters": int(len(chapters)),
            "generated_chapters": int(generated),
            "approved_chapters": int(approved),
            "failed_chapters": int(progress.get("failed_chapters", 0) or 0),
            "stage": f"generating_chapter_{next_idx + 1}",
        }
        live["updated_at"] = time.time()
        _save_longform_sessions()

    asyncio.create_task(_generate_longform_chapter_for_session(session_id, next_idx))


async def _run_longform_pipeline(job_id: str, session_id: str):
    pause_details: dict = {}
    session_snapshot: dict = {}
    try:
        async with _longform_sessions_lock:
            # Worker runs in a separate process from the API; reload disk state
            # so newly-created sessions are visible before render starts.
            if session_id not in _longform_sessions:
                _load_longform_sessions()
            if session_id not in _longform_sessions:
                raise RuntimeError("Long-form session not found")
            session_snapshot = dict(_longform_sessions[session_id])
        template = str(session_snapshot.get("template", "story") or "story")
        resolution = str(session_snapshot.get("resolution", "720p_landscape") or "720p_landscape")
        language = _normalize_longform_language(session_snapshot.get("language", "en"))
        animation_enabled = _bool_from_any(session_snapshot.get("animation_enabled"), True)
        sfx_enabled = _bool_from_any(session_snapshot.get("sfx_enabled"), True)
        whisper_mode = _normalize_longform_whisper_mode(session_snapshot.get("whisper_mode", "subtle"))
        chapters = list(session_snapshot.get("chapters") or [])
        topic = str(session_snapshot.get("topic", "") or "")
        input_title = str(session_snapshot.get("input_title", "") or topic or "Untitled")
        input_description = str(session_snapshot.get("input_description", "") or "")
        session_tone = _longform_detect_tone(template, topic, input_title, input_description)
        chapter_tones = {
            int((chapter or {}).get("index", idx) or idx): str((chapter or {}).get("tone", session_tone) or session_tone)
            for idx, chapter in enumerate(chapters)
        }
        render_horror_audio = _longform_is_horror_tone(session_tone) or any(
            _longform_is_horror_tone(tone) for tone in chapter_tones.values()
        )

        scenes: list[dict] = []
        chapter_markers: list[dict] = []
        cursor_sec = 0.0
        for chapter in chapters:
            chapter_idx = int((chapter or {}).get("index", len(chapter_markers)) or len(chapter_markers))
            chapter_title = str((chapter or {}).get("title", f"Chapter {chapter_idx + 1}") or f"Chapter {chapter_idx + 1}")
            chapter_scenes = _normalize_longform_scenes_for_render((chapter or {}).get("scenes", []))
            chapter_scenes = _scale_scene_durations_to_target(chapter_scenes, float((chapter or {}).get("target_sec", 70) or 70))
            chapter_scenes = list(
                (
                    _longform_apply_brand_slot(
                        {"scenes": chapter_scenes},
                        str((chapter or {}).get("brand_slot", "") or ""),
                        input_title=input_title,
                    ).get("scenes")
                    or chapter_scenes
                )
            )
            chapter_scenes = _normalize_longform_scenes_for_render(chapter_scenes)
            chapter_tone = str((chapter or {}).get("tone", chapter_tones.get(chapter_idx, session_tone)) or chapter_tones.get(chapter_idx, session_tone))
            chapter_scenes = _longform_enforce_tone_on_scenes(chapter_scenes, tone=chapter_tone, template=template)
            if not chapter_scenes:
                raise LongFormPauseError(
                    f"Chapter {chapter_idx + 1} has no scenes",
                    {"chapter_index": chapter_idx, "stage": "planning", "error": "empty_chapter_scenes"},
                )
            start_sec = cursor_sec
            for scene in chapter_scenes:
                scene_copy = dict(scene or {})
                scene_copy["_chapter_index"] = chapter_idx
                scenes.append(scene_copy)
                cursor_sec += float(scene_copy.get("duration_sec", 6) or 6)
            chapter_markers.append({
                "index": chapter_idx,
                "title": chapter_title,
                "start_sec": round(start_sec, 2),
                "end_sec": round(cursor_sec, 2),
            })

        if not scenes:
            raise RuntimeError("Long-form render has no scenes")

        _job_set_stage(job_id, "generating_images", 8)
        jobs[job_id]["total_scenes"] = len(scenes)
        jobs[job_id]["generation_mode"] = "video" if animation_enabled else "image"
        neg_prompt = TEMPLATE_NEGATIVE_PROMPTS.get(template, NEGATIVE_PROMPT)
        if render_horror_audio:
            neg_prompt = (
                neg_prompt
                + ", bright cheerful lighting, warm happy tone, colorful festival mood, comedy framing, daylight beach party aesthetics"
            )
        scene_assets: list[dict] = []
        scene_prompts: list[str] = []
        skeleton_anchor = _canonical_skeleton_anchor() if template == "skeleton" else ""
        reference_image_url = _normalize_reference_with_default(template, "")
        total_steps = len(scenes) * (2 if animation_enabled else 1)
        gen_ts = str(int(time.time() * 1000))

        for i, scene in enumerate(scenes):
            jobs[job_id]["current_scene"] = i + 1
            step_base = i * (2 if animation_enabled else 1)
            _job_set_stage(job_id, "generating_images", 8 + int((step_base / max(total_steps, 1)) * 58))
            chapter_index = int(scene.get("_chapter_index", 0) or 0)
            chapter_tone = str(chapter_tones.get(chapter_index, session_tone) or session_tone)
            locked_visual = _longform_tone_locked_visual_description(
                str(scene.get("visual_description", "") or ""),
                tone=chapter_tone,
                template=template,
            )
            scene["visual_description"] = locked_visual
            full_prompt = _build_scene_prompt_with_reference(
                template=template,
                visual_description=locked_visual,
                quality_mode="cinematic",
                skeleton_anchor=skeleton_anchor,
                reference_dna={},
                reference_lock_mode="strict",
                art_style="auto",
            )
            scene_prompts.append(locked_visual)

            img_path = str(TEMP_DIR / f"{job_id}_lf_scene_{i}.png")
            img_result = None
            image_ok = False
            image_error = ""
            for attempt in range(1, LONGFORM_MAX_SCENE_RETRIES + 1):
                try:
                    img_result = await generate_scene_image(
                        full_prompt,
                        img_path,
                        resolution=resolution,
                        negative_prompt=neg_prompt,
                        template=template,
                        reference_image_url=reference_image_url,
                        reference_lock_mode="strict",
                        best_of_enabled=False,
                        salvage_enabled=False,
                    )
                    quality = _score_generated_image_quality(img_path, full_prompt, template=template)
                    score = float(quality.get("score", 0.0) or 0.0)
                    min_score = _image_quality_min_score(
                        template=template,
                        lock_mode="strict",
                        has_reference=bool(reference_image_url),
                    )
                    if score < min_score:
                        log.warning(
                            f"[{job_id}] Long-form image quality below gate for scene {i + 1}: "
                            f"{score:.1f} < {min_score:.1f}. Accepting for throughput."
                        )
                    image_ok = True
                    break
                except Exception as e:
                    image_error = str(e)
            if not image_ok:
                pause_details = {
                    "chapter_index": chapter_index,
                    "scene_index": i,
                    "stage": "image_generation",
                    "error": image_error or "image_generation_failed_after_retries",
                }
                raise LongFormPauseError("Image generation failed repeatedly", pause_details)

            asset = {"image": img_path, "frames": None, "kling_clip": None}
            cdn_url = (img_result or {}).get("cdn_url")
            if animation_enabled:
                _job_set_stage(job_id, "animating_scenes", 8 + int(((step_base + 1) / max(total_steps, 1)) * 58))
                anim_error = ""
                clip_ok = False
                for _attempt in range(1, LONGFORM_MAX_SCENE_RETRIES + 1):
                    try:
                        clip_path = str(TEMP_DIR / f"{job_id}_lf_clip_{i}.mp4")
                        motion_prompt = locked_visual + " " + TEMPLATE_KLING_MOTION.get(template, "Cinematic motion.")
                        out_clip = await animate_image_kling(
                            image_path=img_path,
                            prompt=motion_prompt,
                            output_clip_path=clip_path,
                            duration=str(max(4, min(12, int(round(float(scene.get("duration_sec", 6) or 6)))))),
                            aspect_ratio="16:9",
                            image_cdn_url=cdn_url,
                        )
                        if out_clip and Path(out_clip).exists():
                            asset["kling_clip"] = out_clip
                            clip_ok = True
                            break
                    except Exception as e:
                        anim_error = str(e)
                if not clip_ok:
                    pause_details = {
                        "chapter_index": chapter_index,
                        "scene_index": i,
                        "stage": "animation",
                        "error": anim_error or "animation_failed_after_retries",
                    }
                    raise LongFormPauseError("Animation failed repeatedly", pause_details)
            scene_assets.append(asset)

        _job_set_stage(job_id, "generating_voice", 70)
        full_narration = " ".join(str((s or {}).get("narration", "") or "") for s in scenes)
        audio_path = str(TEMP_DIR / f"{job_id}_lf_voice.mp3")
        vo_result = await generate_voiceover(full_narration, audio_path, template=template, language=language)
        audio_path = vo_result["audio_path"]
        word_timings = vo_result.get("word_timings", [])

        subtitle_path = None
        if word_timings:
            subtitle_path = str(TEMP_DIR / f"{job_id}_lf_captions.ass")
            generate_ass_subtitles(word_timings, subtitle_path, resolution=resolution, template=template)

        sfx_paths: list[str] = []
        if sfx_enabled:
            _job_set_stage(job_id, "generating_sfx", 78)
            whisper_hint = ""
            if whisper_mode == "subtle":
                whisper_hint = " with subtle whispered ambience under bed"
            elif whisper_mode == "cinematic":
                whisper_hint = " with cinematic whisper textures and breathy tension"
            for i, scene in enumerate(scenes):
                sfx_out = str(TEMP_DIR / f"{job_id}_lf_sfx_{i}.mp3")
                chapter_tone = str(chapter_tones.get(int(scene.get("_chapter_index", 0) or 0), session_tone) or session_tone)
                desc = _longform_tone_locked_visual_description(
                    str(scene.get("visual_description", "") or ""),
                    tone=chapter_tone,
                    template=template,
                ) + whisper_hint
                dur = float(scene.get("duration_sec", 6) or 6)
                sfx_file = await generate_scene_sfx(desc, dur, sfx_out, template=template, scene_index=i, total_scenes=len(scenes))
                sfx_paths.append(sfx_file)
            sfx_paths = await _quintuple_check_scene_sfx(scenes, sfx_paths, template, job_id=job_id)

        bgm_track = ""
        if render_horror_audio:
            bgm_path = str(TEMP_DIR / f"{job_id}_lf_horror_bgm.mp3")
            total_duration = sum(float((s or {}).get("duration_sec", 5.0) or 5.0) for s in scenes) + 1.0
            bgm_track = await _generate_spooky_bgm_track(total_duration, bgm_path, whisper_mode=whisper_mode)

        _job_set_stage(job_id, "compositing", 84)
        output_filename = f"longform_{template}_{job_id}.mp4"
        output_path = str(OUTPUT_DIR / output_filename)
        await composite_video(
            scenes,
            scene_assets,
            audio_path,
            output_path,
            resolution=resolution,
            use_svd=animation_enabled,
            subtitle_path=subtitle_path,
            sfx_paths=sfx_paths,
            bgm_track=bgm_track,
            transition_style="smooth",
            micro_escalation_mode=False,
        )

        for sfx in sfx_paths:
            if sfx:
                Path(sfx).unlink(missing_ok=True)
        if bgm_track:
            Path(bgm_track).unlink(missing_ok=True)
        for asset in scene_assets:
            Path(str(asset.get("image", ""))).unlink(missing_ok=True)
            if asset.get("kling_clip"):
                Path(str(asset["kling_clip"])).unlink(missing_ok=True)
            if asset.get("frames"):
                for fp in asset["frames"]:
                    Path(fp).unlink(missing_ok=True)
        Path(audio_path).unlink(missing_ok=True)
        if subtitle_path:
            Path(subtitle_path).unlink(missing_ok=True)

        _job_set_stage(job_id, "complete", 100)
        jobs[job_id]["output_file"] = output_filename
        jobs[job_id]["resolution"] = resolution
        jobs[job_id]["metadata"] = {
            "title": input_title,
            "description": str(session_snapshot.get("input_description", "") or ""),
            "tags": list(((session_snapshot.get("metadata_pack") or {}).get("tags") or [])),
            "chapters": chapter_markers,
        }
        _job_diag_finalize(job_id)
        async with _longform_sessions_lock:
            session_live = _longform_sessions.get(session_id)
            if isinstance(session_live, dict):
                session_live["status"] = "complete"
                session_live["paused_error"] = None
                session_live["package"] = {
                    "output_file": output_filename,
                    "chapters": chapter_markers,
                    "title_variants": list(((session_live.get("metadata_pack") or {}).get("title_variants") or [])),
                    "description_variants": list(((session_live.get("metadata_pack") or {}).get("description_variants") or [])),
                    "thumbnail_prompts": list(((session_live.get("metadata_pack") or {}).get("thumbnail_prompts") or [])),
                }
                session_live["updated_at"] = time.time()
                _save_longform_sessions()
    except LongFormPauseError as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)
        _job_diag_finalize(job_id)
        async with _longform_sessions_lock:
            session_live = _longform_sessions.get(session_id)
            if isinstance(session_live, dict):
                session_live["status"] = "paused_needs_fix"
                session_live["paused_error"] = dict(e.details or pause_details or {})
                session_live["updated_at"] = time.time()
                _save_longform_sessions()
    except Exception as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)
        _job_diag_finalize(job_id)
        async with _longform_sessions_lock:
            session_live = _longform_sessions.get(session_id)
            if isinstance(session_live, dict):
                session_live["status"] = "error"
                if pause_details:
                    session_live["paused_error"] = dict(pause_details)
                session_live["updated_at"] = time.time()
                _save_longform_sessions()


@app.post("/api/longform/session")
async def create_longform_session(req: LongFormSessionCreateRequest, request: Request = None):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _longform_owner_beta_enabled(user):
        raise HTTPException(403, "Long-form owner beta is restricted")
    if not XAI_API_KEY:
        raise HTTPException(500, "XAI_API_KEY not configured")
    if not ELEVENLABS_API_KEY:
        raise HTTPException(500, "ELEVENLABS_API_KEY not configured")

    template = _normalize_longform_template(req.template)
    format_preset = str(getattr(req, "format_preset", "explainer") or "explainer").strip().lower()
    if format_preset not in {"recap", "explainer", "documentary", "story_channel"}:
        format_preset = "explainer"
    topic = str(req.topic or "").strip()
    input_title = str(req.input_title or "").strip()
    input_description = str(req.input_description or "").strip()
    source_url = _normalize_external_source_url(getattr(req, "source_url", ""))
    if str(getattr(req, "source_url", "") or "").strip() and not source_url:
        raise HTTPException(400, "Source URL is invalid")
    analytics_notes = str(getattr(req, "analytics_notes", "") or "").strip()
    strategy_notes = str(getattr(req, "strategy_notes", "") or "").strip()
    if not topic:
        raise HTTPException(400, "Topic is required")
    if not input_title:
        raise HTTPException(400, "Video title is required")
    if not input_description:
        raise HTTPException(400, "Video description is required")
    target_minutes = _normalize_longform_target_minutes(req.target_minutes)
    language = _normalize_longform_language(req.language)
    whisper_mode = _normalize_longform_whisper_mode(req.whisper_mode)
    source_bundle = await _fetch_source_video_bundle(source_url, language=language) if source_url else {}
    source_analysis = await _build_source_performance_analysis(
        source_bundle=source_bundle,
        analytics_notes=analytics_notes,
        topic=topic,
        input_title=input_title,
        input_description=input_description,
        strategy_notes=strategy_notes,
    )
    source_context = _render_source_context(source_bundle, source_analysis, analytics_notes)

    chapter_count, chapter_target_sec = _longform_chapter_scene_targets(target_minutes)
    chapters = [
        _longform_placeholder_chapter(
            i,
            chapter_target_sec,
            _longform_brand_slot(i, chapter_count),
            status="awaiting_previous_approval",
        )
        for i in range(chapter_count)
    ]

    title_variants = []
    for t in [
        _longform_title_variant(input_title, topic),
        f"{input_title} | Full Breakdown",
        f"{topic} - Complete Breakdown",
        *list(source_analysis.get("title_angles") or []),
    ]:
        tt = str(t or "").strip()
        if tt and tt not in title_variants:
            title_variants.append(tt)

    description_variants = []
    for d in [
        input_description,
        (input_description + " Built with NYPTID Studio."),
        ("Built in NYPTID Studio: " + input_description),
        *list(source_analysis.get("description_angles") or []),
    ]:
        dd = str(d or "").strip()
        if dd and dd not in description_variants:
            description_variants.append(dd)

    thumbnail_prompts: list[str] = []
    for prompt in [
        *list(source_analysis.get("thumbnail_angles") or []),
        f"{input_title} cinematic hero frame, high contrast, readable face/subject, YouTube thumbnail style",
        f"{topic} dramatic split-moment scene, premium documentary look, 16:9 thumbnail",
        f"{topic} faceless YouTube thumbnail, strong contrast, one clear emotional focal point, 16:9",
    ]:
        candidate = str(prompt or "").strip()
        if candidate and candidate not in thumbnail_prompts:
            thumbnail_prompts.append(candidate)

    metadata_pack = {
        "title_variants": title_variants[:3],
        "description_variants": description_variants[:3],
        "thumbnail_prompts": thumbnail_prompts[:3],
        "tags": [
            template,
            format_preset,
            "nyptid",
            "longform",
            topic[:32].replace(" ", "_").lower(),
            *[str(tag).strip().replace(" ", "_").lower() for tag in list(source_bundle.get("tags") or [])[:6] if str(tag).strip()],
        ],
        "source_video": source_bundle,
        "source_analysis": source_analysis,
        "source_context": source_context,
        "strategy_notes": strategy_notes,
        "marketing_doctrine": list(CATALYST_MARKETING_DOCTRINE),
    }
    session_id = f"lf_{int(time.time())}_{random.randint(1000, 9999)}"
    now = time.time()
    session_data = {
        "session_id": session_id,
        "user_id": str(user.get("id", "") or ""),
        "template": template,
        "format_preset": format_preset,
        "topic": topic,
        "input_title": input_title,
        "input_description": input_description,
        "source_url": source_url,
        "analytics_notes": analytics_notes,
        "strategy_notes": strategy_notes,
        "target_minutes": float(target_minutes),
        "language": language,
        "resolution": "720p_landscape",
        "animation_enabled": _bool_from_any(req.animation_enabled, True),
        "sfx_enabled": _bool_from_any(req.sfx_enabled, True),
        "whisper_mode": whisper_mode,
        "chapters": chapters,
        "status": "draft_review",
        "paused_error": None,
        "job_id": "",
        "metadata_pack": metadata_pack,
        "draft_progress": {
            "total_chapters": int(chapter_count),
            "generated_chapters": 0,
            "approved_chapters": 0,
            "failed_chapters": 0,
            "stage": "queued_first_chapter",
        },
        "package": {},
        "created_at": now,
        "updated_at": now,
    }
    async with _longform_sessions_lock:
        _longform_sessions[session_id] = session_data
        _save_longform_sessions()
    await _queue_next_longform_chapter_if_ready(session_id)
    async with _longform_sessions_lock:
        session_live = _longform_sessions.get(session_id, session_data)
    return {"session": _longform_public_session(session_live)}


@app.get("/api/longform/session/{session_id}/status")
async def longform_session_status(session_id: str, request: Request = None):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _longform_owner_beta_enabled(user):
        raise HTTPException(403, "Long-form owner beta is restricted")
    should_resume = False
    now = time.time()
    async with _longform_sessions_lock:
        _load_longform_sessions()
        session = _longform_sessions.get(session_id)
        if isinstance(session, dict):
            chapters = list(session.get("chapters") or [])
            in_progress_idx = -1
            in_progress_status = ""
            for idx, chapter in enumerate(chapters):
                status = str((chapter or {}).get("status", "") or "")
                if status in {"draft_generating", "draft_generating_images"}:
                    in_progress_idx = idx
                    in_progress_status = status
                    break

            stalled_sec = max(0.0, now - float(session.get("updated_at", 0) or 0))
            if in_progress_idx >= 0:
                chapter_live = dict(chapters[in_progress_idx] or {})
                chapter_scenes = list(chapter_live.get("scenes") or [])
                scene_count = len(chapter_scenes)
                # Recover from crash/restart if a generation task died before scripts/scenes were persisted.
                if in_progress_status == "draft_generating" and scene_count <= 0 and stalled_sec > 180.0:
                    chapter_live["status"] = "awaiting_previous_approval"
                    chapter_live["last_error"] = "Recovered from stalled generation task; chapter resumed."
                    chapters[in_progress_idx] = chapter_live
                    session["chapters"] = chapters
                    session["status"] = "draft_review"
                    progress = dict(session.get("draft_progress") or {})
                    progress["stage"] = "auto_resume_after_stall"
                    progress["preview_scene_generated"] = 0
                    session["draft_progress"] = progress
                    session["updated_at"] = now
                    _save_longform_sessions()
                    should_resume = True
                # Recover from stalled preview-image generation so owner can approve/regenerate and continue.
                elif in_progress_status == "draft_generating_images" and scene_count > 0 and stalled_sec > 180.0:
                    ready_count = 0
                    for idx, raw_scene in enumerate(chapter_scenes):
                        scene = dict(raw_scene or {})
                        has_img = bool(str(scene.get("image_url", "") or "").strip())
                        if has_img:
                            ready_count += 1
                        else:
                            status = str(scene.get("image_status", "") or "")
                            if status != "error":
                                scene["image_status"] = "error"
                                scene["image_error"] = "Preview generation stalled; regenerate chapter to refresh this scene."
                            chapter_scenes[idx] = scene

                    missing_count = max(0, scene_count - ready_count)
                    chapter_live["scenes"] = chapter_scenes
                    chapter_live["status"] = "pending_review"
                    chapter_live["last_error"] = (
                        f"Recovered from stalled preview generation ({ready_count}/{scene_count} ready). "
                        f"{missing_count} previews missing; regenerate chapter if needed."
                        if missing_count > 0
                        else "Recovered from stalled preview generation; chapter moved to review."
                    )
                    chapters[in_progress_idx] = chapter_live
                    session["chapters"] = chapters
                    session["status"] = "draft_review"
                    progress = dict(session.get("draft_progress") or {})
                    progress["stage"] = "auto_resume_after_preview_stall"
                    progress["preview_scene_total"] = int(scene_count)
                    progress["preview_scene_generated"] = int(ready_count)
                    progress["generated_chapters"] = int(_longform_generated_chapter_count(chapters))
                    progress["approved_chapters"] = int(_longform_approved_chapter_count(chapters))
                    session["draft_progress"] = progress
                    session["updated_at"] = now
                    _save_longform_sessions()
            elif any(str((c or {}).get("status", "") or "") == "awaiting_previous_approval" for c in chapters):
                should_resume = True
    if not session:
        raise HTTPException(404, "Long-form session not found")
    if str(session.get("user_id", "") or "") != str(user.get("id", "") or ""):
        raise HTTPException(403, "Forbidden")
    if should_resume:
        await _queue_next_longform_chapter_if_ready(session_id)
        async with _longform_sessions_lock:
            _load_longform_sessions()
            session = _longform_sessions.get(session_id) or session
    job_id = str(session.get("job_id", "") or "")
    job = jobs.get(job_id, {}) if job_id else {}
    if job_id:
        persisted = await get_persisted_job_state(job_id)
        if isinstance(persisted, dict) and persisted:
            jobs[job_id] = persisted
            job = persisted
        # If worker failed before it could write session state, prevent a
        # permanently stuck "rendering" session in the UI.
        if str((job or {}).get("status", "") or "") == "error":
            async with _longform_sessions_lock:
                _load_longform_sessions()
                session_live = _longform_sessions.get(session_id)
                if isinstance(session_live, dict) and str(session_live.get("status", "") or "") == "rendering":
                    session_live["status"] = "error"
                    session_live["updated_at"] = time.time()
                    _save_longform_sessions()
                    session = session_live
    return {"session": _longform_public_session(session), "job": job}


@app.get("/api/longform/sessions")
async def list_longform_sessions(request: Request = None, limit: int = 25):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _longform_owner_beta_enabled(user):
        raise HTTPException(403, "Long-form owner beta is restricted")

    limit = max(1, min(100, int(limit or 25)))
    user_id = str(user.get("id", "") or "")
    async with _longform_sessions_lock:
        _load_longform_sessions()
        sessions = [
            _longform_session_summary(s)
            for s in list(_longform_sessions.values())
            if str((s or {}).get("user_id", "") or "") == user_id
        ]
    sessions.sort(key=lambda s: float(s.get("updated_at", 0) or 0), reverse=True)
    return {"sessions": sessions[:limit]}


@app.get("/api/longform/preview/{filename}")
async def longform_preview_file(filename: str):
    safe = os.path.basename(filename)
    if not safe or safe != filename:
        raise HTTPException(400, "Invalid filename")
    path = LONGFORM_PREVIEW_DIR / safe
    if not path.exists():
        raise HTTPException(404, "Preview not found")
    suffix = path.suffix.lower()
    media_map = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
    }
    return FileResponse(str(path), media_type=media_map.get(suffix, "application/octet-stream"), filename=safe)


@app.post("/api/longform/session/{session_id}/chapter-action")
async def longform_chapter_action(session_id: str, req: LongFormChapterActionRequest, request: Request = None):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _longform_owner_beta_enabled(user):
        raise HTTPException(403, "Long-form owner beta is restricted")
    action = str(req.action or "").strip().lower()
    if action not in {"approve", "regenerate"}:
        raise HTTPException(400, "action must be approve or regenerate")

    should_queue_next = False
    async with _longform_sessions_lock:
        _load_longform_sessions()
        session = _longform_sessions.get(session_id)
        if not session:
            raise HTTPException(404, "Long-form session not found")
        if str(session.get("user_id", "") or "") != str(user.get("id", "") or ""):
            raise HTTPException(403, "Forbidden")
        chapters = list(session.get("chapters") or [])
        chapter_index = int(req.chapter_index)
        if chapter_index < 0 or chapter_index >= len(chapters):
            raise HTTPException(400, "Invalid chapter index")
        chapter = dict(chapters[chapter_index] or {})
        chapter_status = str(chapter.get("status", "") or "")
        if action == "approve":
            if chapter_status in {"awaiting_previous_approval", "draft_generating"}:
                raise HTTPException(400, "This chapter is not ready for approval yet")
            chapter["status"] = "approved"
            chapter["last_error"] = ""
            chapters[chapter_index] = chapter
            session["chapters"] = chapters
            session["status"] = "draft_review"
            progress = dict(session.get("draft_progress") or {})
            session["draft_progress"] = {
                "total_chapters": int(len(chapters)),
                "generated_chapters": int(_longform_generated_chapter_count(chapters)),
                "approved_chapters": int(_longform_approved_chapter_count(chapters)),
                "failed_chapters": int(progress.get("failed_chapters", 0) or 0),
                "stage": "awaiting_owner_approval",
            }
            session["updated_at"] = time.time()
            _save_longform_sessions()
            should_queue_next = True
        else:
            if chapter_status in {"awaiting_previous_approval", "draft_generating"}:
                raise HTTPException(400, "This chapter is not ready for regeneration yet")

            # Mark in-progress before releasing lock for generation.
            chapter["status"] = "regenerating"
            chapter["retry_count"] = int(chapter.get("retry_count", 0) or 0) + 1
            chapters[chapter_index] = chapter
            session["chapters"] = chapters
            session["updated_at"] = time.time()
            _save_longform_sessions()
            session_copy = dict(session)

    if should_queue_next:
        await _queue_next_longform_chapter_if_ready(session_id)
        async with _longform_sessions_lock:
            session_live = _longform_sessions.get(session_id)
        if not session_live:
            raise HTTPException(404, "Long-form session not found")
        chapter_live = dict((list(session_live.get("chapters") or [])[chapter_index]) or {})
        return {"session": _longform_public_session(session_live), "chapter": chapter_live}

    regenerated = await _generate_longform_chapter(
        template=str(session_copy.get("template", "story") or "story"),
        topic=str(session_copy.get("topic", "") or ""),
        input_title=str(session_copy.get("input_title", "") or ""),
        input_description=str(session_copy.get("input_description", "") or ""),
        format_preset=str(session_copy.get("format_preset", "explainer") or "explainer"),
        chapter_index=chapter_index,
        chapter_count=len(list(session_copy.get("chapters") or [])),
        chapter_target_sec=float(chapter.get("target_sec", 70) or 70),
        language=_normalize_longform_language(session_copy.get("language", "en")),
        brand_slot=str(chapter.get("brand_slot", "") or ""),
        fix_note=str(req.reason or "").strip(),
        source_context=str((dict(session_copy.get("metadata_pack") or {})).get("source_context", "") or ""),
        strategy_notes=_marketing_doctrine_text(str(session_copy.get("strategy_notes", "") or "").strip()),
    )
    regenerated = await _longform_attach_scene_previews(
        session_id=session_id,
        template=str(session_copy.get("template", "story") or "story"),
        chapter=regenerated,
        resolution=str(session_copy.get("resolution", "720p_landscape") or "720p_landscape"),
    )
    regenerated["retry_count"] = int(chapter.get("retry_count", 1) or 1)
    async with _longform_sessions_lock:
        session_live = _longform_sessions.get(session_id)
        if not session_live:
            raise HTTPException(404, "Long-form session not found")
        chapters_live = list(session_live.get("chapters") or [])
        chapters_live[chapter_index] = regenerated
        session_live["chapters"] = chapters_live
        session_live["status"] = "draft_review"
        progress = dict(session_live.get("draft_progress") or {})
        session_live["draft_progress"] = {
            "total_chapters": int(len(chapters_live)),
            "generated_chapters": int(_longform_generated_chapter_count(chapters_live)),
            "approved_chapters": int(_longform_approved_chapter_count(chapters_live)),
            "failed_chapters": int(progress.get("failed_chapters", 0) or 0),
            "stage": "awaiting_owner_approval",
        }
        session_live["updated_at"] = time.time()
        _save_longform_sessions()
        return {"session": _longform_public_session(session_live), "chapter": regenerated}


@app.post("/api/longform/session/{session_id}/resolve-error")
async def longform_resolve_error(session_id: str, req: LongFormResolveErrorRequest, request: Request = None):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _longform_owner_beta_enabled(user):
        raise HTTPException(403, "Long-form owner beta is restricted")
    async with _longform_sessions_lock:
        _load_longform_sessions()
        session = _longform_sessions.get(session_id)
        if not session:
            raise HTTPException(404, "Long-form session not found")
        if str(session.get("user_id", "") or "") != str(user.get("id", "") or ""):
            raise HTTPException(403, "Forbidden")
        paused = dict(session.get("paused_error") or {})
        if not paused:
            raise HTTPException(400, "Session is not paused for manual fix")
        chapter_index = int(req.chapter_index)
        chapters = list(session.get("chapters") or [])
        if chapter_index < 0 or chapter_index >= len(chapters):
            raise HTTPException(400, "Invalid chapter index")
        chapter = dict(chapters[chapter_index] or {})
        session_copy = dict(session)

    regenerated = await _generate_longform_chapter(
        template=str(session_copy.get("template", "story") or "story"),
        topic=str(session_copy.get("topic", "") or ""),
        input_title=str(session_copy.get("input_title", "") or ""),
        input_description=str(session_copy.get("input_description", "") or ""),
        format_preset=str(session_copy.get("format_preset", "explainer") or "explainer"),
        chapter_index=chapter_index,
        chapter_count=len(list(session_copy.get("chapters") or [])),
        chapter_target_sec=float(chapter.get("target_sec", 70) or 70),
        language=_normalize_longform_language(session_copy.get("language", "en")),
        brand_slot=str(chapter.get("brand_slot", "") or ""),
        fix_note=str(req.fix_note or "").strip(),
        source_context=str((dict(session_copy.get("metadata_pack") or {})).get("source_context", "") or ""),
        strategy_notes=_marketing_doctrine_text(str(session_copy.get("strategy_notes", "") or "").strip()),
    )
    regenerated = await _longform_attach_scene_previews(
        session_id=session_id,
        template=str(session_copy.get("template", "story") or "story"),
        chapter=regenerated,
        resolution=str(session_copy.get("resolution", "720p_landscape") or "720p_landscape"),
    )
    regenerated["status"] = "approved" if _bool_from_any(req.force_accept, False) else "pending_review"
    regenerated["retry_count"] = int(chapter.get("retry_count", 0) or 0) + 1
    force_accept = _bool_from_any(req.force_accept, False)
    async with _longform_sessions_lock:
        session_live = _longform_sessions.get(session_id)
        if not session_live:
            raise HTTPException(404, "Long-form session not found")
        chapters_live = list(session_live.get("chapters") or [])
        chapters_live[chapter_index] = regenerated
        session_live["chapters"] = chapters_live
        session_live["status"] = "draft_review"
        session_live["paused_error"] = None
        progress = dict(session_live.get("draft_progress") or {})
        session_live["draft_progress"] = {
            "total_chapters": int(len(chapters_live)),
            "generated_chapters": int(_longform_generated_chapter_count(chapters_live)),
            "approved_chapters": int(_longform_approved_chapter_count(chapters_live)),
            "failed_chapters": int(progress.get("failed_chapters", 0) or 0),
            "stage": "awaiting_owner_approval",
        }
        session_live["updated_at"] = time.time()
        _save_longform_sessions()
    if force_accept:
        await _queue_next_longform_chapter_if_ready(session_id)
    async with _longform_sessions_lock:
        session_latest = _longform_sessions.get(session_id)
    if not session_latest:
        raise HTTPException(404, "Long-form session not found")
    return {"session": _longform_public_session(session_latest), "chapter": regenerated}


@app.post("/api/longform/session/{session_id}/finalize")
async def longform_finalize(session_id: str, background_tasks: BackgroundTasks, request: Request = None):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _longform_owner_beta_enabled(user):
        raise HTTPException(403, "Long-form owner beta is restricted")
    async with _longform_sessions_lock:
        _load_longform_sessions()
        session = _longform_sessions.get(session_id)
        if not session:
            raise HTTPException(404, "Long-form session not found")
        if str(session.get("user_id", "") or "") != str(user.get("id", "") or ""):
            raise HTTPException(403, "Forbidden")
        if session.get("paused_error"):
            raise HTTPException(400, "Session is paused due to an unresolved chapter error")
        review = _longform_review_state(session)
        if not review.get("all_approved", False):
            raise HTTPException(
                400,
                f"All chapters must be approved before finalize ({review.get('approved_chapters', 0)}/{review.get('total_chapters', 0)} approved).",
            )
        job_id = f"{int(time.time())}_{random.randint(1000, 9999)}"
        jobs[job_id] = {
            "status": "queued",
            "progress": 0,
            "template": str(session.get("template", "story") or "story"),
            "topic": str(session.get("topic", "") or ""),
            "lane": "longform",
            "mode": "longform_finalize",
            "resolution": str(session.get("resolution", "720p_landscape") or "720p_landscape"),
            "plan": "pro",
            "user_id": str(user.get("id", "") or ""),
            "created_at": time.time(),
            "type": "longform",
            "target_minutes": float(session.get("target_minutes", 0) or 0),
            "animation_enabled": bool(session.get("animation_enabled", True)),
            "story_animation_enabled": bool(session.get("animation_enabled", True)),
            "voice_id": "",
            "voice_speed": 1.0,
            "pacing_mode": "standard",
            "subtitles_enabled": True,
            "reference_lock_mode": "strict",
            "transition_style": "smooth",
            "micro_escalation_mode": False,
            "credit_charged": False,
            "credit_source": "admin",
            "credit_cost": 0,
            "billing_source": "workspace_access" if not _is_admin_user(user) else "owner_override",
            "credit_month_key": _month_key(),
            "credit_refunded": False,
        }
        session["job_id"] = job_id
        session["status"] = "rendering"
        session["updated_at"] = time.time()
        _save_longform_sessions()

    _job_diag_init(job_id, "longform")
    try:
        await enqueue_generation_job(job_id, "pro", _run_longform_pipeline, (job_id, session_id))
    except QueueFullError as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)
        await persist_job_state(job_id, jobs[job_id])
        async with _longform_sessions_lock:
            session_live = _longform_sessions.get(session_id)
            if isinstance(session_live, dict):
                session_live["status"] = "draft_review"
                session_live["updated_at"] = time.time()
                _save_longform_sessions()
        raise HTTPException(429, str(e))
    return {"job_id": job_id}


@app.post("/api/creative/script")
async def creative_generate_script(req: GenerateRequest, request: Request = None):
    """Phase 1: Generate script + scenes for user review. Returns editable scene list."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _user_has_paid_access(user):
        raise HTTPException(402, "Active subscription required. Please choose a plan.")
    user_plan, _plan_limits = _resolve_user_plan_for_limits(user)
    is_admin = user.get("email", "") in ADMIN_EMAILS
    billing_active = _billing_active_for_user(user)
    can_run, _source, _state = await _reserve_generation_credit(
        user,
        user_plan if not is_admin else "pro",
        billing_active,
        is_admin=is_admin,
        usage_kind="non_animated",
    )
    if not can_run:
        raise HTTPException(402, "Non-animated meter exhausted for this month. Please wait for renewal or upgrade plan.")
    _ensure_template_allowed(req.template, user)
    quality_mode = _normalize_skeleton_quality_mode(req.quality_mode, template=req.template)
    mint_mode = _normalize_mint_mode(req.mint_mode, template=req.template)
    art_style = _normalize_art_style(req.art_style, template=req.template)
    cinematic_boost = _normalize_cinematic_boost(getattr(req, "cinematic_boost", True))
    reference_lock_mode = _normalize_reference_lock_mode(req.reference_lock_mode, default="strict")
    default_reference_url = _default_reference_for_template(req.template)
    reference_dna, reference_quality = await _extract_reference_profile(default_reference_url, req.template, reference_lock_mode)
    transition_style = _normalize_transition_style(req.transition_style)
    micro_escalation_mode = _normalize_micro_escalation_mode(req.micro_escalation_mode, template=req.template)
    if cinematic_boost:
        quality_mode = "cinematic"
        transition_style = "cinematic"
        micro_escalation_mode = True
        mint_mode = True
    if cinematic_boost:
        quality_mode = "cinematic"
        transition_style = "cinematic"
        micro_escalation_mode = True
        mint_mode = True
    voice_id = str(req.voice_id or "").strip()
    voice_speed = _normalize_voice_speed(req.voice_speed, default=1.0)
    pacing_mode = _normalize_pacing_mode(req.pacing_mode)
    animation_enabled = _bool_from_any(req.animation_enabled, _bool_from_any(req.story_animation_enabled, True))
    if req.template != "story" or not STORY_ADVANCED_CONTROLS_ENABLED:
        voice_id = ""
        voice_speed = 1.0
        pacing_mode = "standard"
    lang_name = SUPPORTED_LANGUAGES.get(req.language, {}).get("name", "English")
    lang_instruction = ""
    if req.language != "en":
        lang_instruction = f"\n\nIMPORTANT: Write ALL narration text in {lang_name}. The visual_description fields should remain in English (for image generation), but ALL narration/voiceover text MUST be in {lang_name}."
    script_to_short_mode = str(req.mode or "").strip().lower() == "script_to_short"
    if script_to_short_mode and not SCRIPT_TO_SHORT_ENABLED:
        raise HTTPException(503, "Script to Short is temporarily disabled")
    script_to_short_instruction = ""
    if script_to_short_mode:
        scene_range = "12-15" if req.template == "story" else "10-14"
        script_to_short_instruction = (
            "\n\nSCRIPT-TO-SHORT MODE: The user input is already a full script. "
            "Do not invent a different story. Keep the original intent and wording as much as possible while adapting for timing. "
            f"Split into {scene_range} scenes with clear visual progression and strong retention pacing. "
            "Process the script in strict chronological order from the first sentence to the final payoff. "
            "Do not skip late beats, collapse major transitions into generic filler, or rewrite the source into a different premise. "
            "Each scene must map to one consecutive beat from the exact source script and preserve the same subject, setting, action, and emotional turn implied by that beat. "
            "If the script names a person, object, outfit, location, mechanism, number, or timeframe, keep it in the matching scene instead of generalizing it away. "
            "Do not force a single unchanged main character in every scene; match subjects to each script beat. "
            "Each scene must have concise narration and a cinematic visual_description that can be directly rendered. "
            "Make the visual_description fields self-contained, specific, and editable so users can regenerate each scene from the prompt alone. "
            "Every visual_description should read like a production-ready image prompt with the exact subject, setting, action, framing, lighting, and key props required by that script beat."
        )
    resolution = _normalize_output_resolution(req.resolution, priority_allowed=False)
    try:
        script_data = await generate_script(
            req.template,
            req.prompt,
            extra_instructions=(lang_instruction + script_to_short_instruction),
        )
    except httpx.HTTPStatusError as e:
        status_code = getattr(getattr(e, "response", None), "status_code", None)
        if status_code == 429:
            raise HTTPException(
                503,
                "Script generation is temporarily rate-limited upstream. Retry in a minute.",
            ) from e
        raise HTTPException(
            502,
            "Script generation failed upstream. Retry shortly.",
        ) from e
    scenes = _normalize_scenes_for_render(script_data.get("scenes", []))
    scenes = _apply_template_scene_constraints(scenes, req.template, quality_mode=quality_mode)
    scenes = _apply_mint_scene_compiler(scenes, req.template, mint_mode=mint_mode)
    if not scenes:
        raise HTTPException(500, "Script generation returned no scenes")
    session_id = f"cs_{int(time.time())}_{random.randint(1000, 9999)}"
    async with _creative_sessions_lock:
        _creative_sessions[session_id] = {
            "session_id": session_id,
            "user_id": user["id"],
            "template": req.template,
            "topic": req.prompt,
            "resolution": resolution,
            "language": req.language,
            "script_data": script_data,
            "scenes": scenes,
            "scene_images": {},
            "quality_mode": quality_mode,
            "mint_mode": mint_mode,
            "art_style": art_style,
            "cinematic_boost": cinematic_boost,
            "transition_style": transition_style,
            "micro_escalation_mode": micro_escalation_mode,
            "voice_id": voice_id,
            "voice_speed": voice_speed,
            "pacing_mode": pacing_mode,
            "animation_enabled": animation_enabled,
            "story_animation_enabled": animation_enabled if req.template == "story" else True,
            "reference_image_url": default_reference_url,
            "reference_lock_mode": reference_lock_mode,
            "reference_dna": reference_dna,
            "reference_quality": reference_quality,
            "reference_image_uploaded": False,
            "rolling_reference_image_url": default_reference_url,
            "prompt_passthrough": True,
            "created_at": time.time(),
        }
        _save_creative_sessions_to_disk()
    return {
        "session_id": session_id,
        "title": script_data.get("title", req.prompt),
        "scenes": [
            {
                "index": i,
                "narration": s.get("narration", ""),
                "visual_description": s.get("visual_description", ""),
                "duration_sec": s.get("duration_sec", 5),
            }
            for i, s in enumerate(scenes)
        ],
        "quality_mode": quality_mode,
        "mint_mode": mint_mode,
        "art_style": art_style,
        "cinematic_boost": cinematic_boost,
        "transition_style": transition_style,
        "micro_escalation_mode": micro_escalation_mode,
        "voice_id": voice_id,
        "voice_speed": voice_speed,
        "pacing_mode": pacing_mode,
        "animation_enabled": animation_enabled,
        "reference_lock_mode": reference_lock_mode,
        "prompt_passthrough": True,
        "mode": "script_to_short" if script_to_short_mode else "creative",
    }


@app.post("/api/creative/session")
async def creative_create_session(body: dict, request: Request = None):
    """Create an empty creative session (no script generation). User builds scenes manually."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _user_has_paid_access(user):
        raise HTTPException(402, "Active subscription required. Please choose a plan.")
    template = body.get("template", "skeleton")
    quality_mode = _normalize_skeleton_quality_mode(body.get("quality_mode"), template=template)
    mint_mode = _normalize_mint_mode(body.get("mint_mode"), template=template)
    art_style = _normalize_art_style(body.get("art_style"), template=template)
    transition_style = _normalize_transition_style(body.get("transition_style", "smooth"))
    micro_escalation_mode = _normalize_micro_escalation_mode(body.get("micro_escalation_mode"), template=template)
    cinematic_boost = _normalize_cinematic_boost(body.get("cinematic_boost", True))
    if cinematic_boost:
        quality_mode = "cinematic"
        transition_style = "cinematic"
        micro_escalation_mode = True
        mint_mode = True
    voice_id = str(body.get("voice_id", "") or "").strip()
    voice_speed = _normalize_voice_speed(body.get("voice_speed", 1.0), default=1.0)
    pacing_mode = _normalize_pacing_mode(body.get("pacing_mode", "standard"))
    if template != "story" or not STORY_ADVANCED_CONTROLS_ENABLED:
        voice_id = ""
        voice_speed = 1.0
        pacing_mode = "standard"
    reference_lock_mode = _normalize_reference_lock_mode(body.get("reference_lock_mode"), default="strict")
    default_reference_url = _default_reference_for_template(template)
    reference_dna, reference_quality = await _extract_reference_profile(default_reference_url, template, reference_lock_mode)
    _ensure_template_allowed(template, user)
    _user_plan, plan_limits = _resolve_user_plan_for_limits(user)
    requested_resolution = body.get("resolution", "720p")
    resolution = _normalize_output_resolution(requested_resolution, priority_allowed=bool(plan_limits.get("priority", False)))
    animation_enabled = _bool_from_any(body.get("animation_enabled"), _bool_from_any(body.get("story_animation_enabled"), True))
    story_animation_enabled = animation_enabled if template == "story" else True
    session_id = f"cs_{int(time.time())}_{random.randint(1000, 9999)}"
    async with _creative_sessions_lock:
        _creative_sessions[session_id] = {
            "session_id": session_id,
            "user_id": user["id"],
            "template": template,
            "topic": body.get("topic", "Untitled"),
            "resolution": resolution,
            "language": body.get("language", "en"),
            "script_data": {"title": body.get("topic", "Untitled"), "tags": []},
            "scenes": [],
            "scene_images": {},
            "quality_mode": quality_mode,
            "mint_mode": mint_mode,
            "art_style": art_style,
            "cinematic_boost": cinematic_boost,
            "transition_style": transition_style,
            "micro_escalation_mode": micro_escalation_mode,
            "voice_id": voice_id,
            "voice_speed": voice_speed,
            "pacing_mode": pacing_mode,
            "animation_enabled": animation_enabled,
            "story_animation_enabled": story_animation_enabled,
            "reference_image_url": default_reference_url,
            "reference_lock_mode": reference_lock_mode,
            "reference_dna": reference_dna,
            "reference_quality": reference_quality,
            "reference_image_uploaded": False,
            "rolling_reference_image_url": default_reference_url,
            "prompt_passthrough": True,
            "created_at": time.time(),
        }
        _save_creative_sessions_to_disk()
    project_id = _new_project_id()
    await _create_or_update_project(project_id, {
        "user_id": user["id"],
        "template": template,
        "topic": body.get("topic", "Untitled"),
        "mode": "creative",
        "status": "draft",
        "resolution": resolution,
        "language": body.get("language", "en"),
        "art_style": art_style,
        "cinematic_boost": cinematic_boost,
        "voice_id": voice_id,
        "voice_speed": voice_speed,
        "pacing_mode": pacing_mode,
        "animation_enabled": animation_enabled,
        "session_id": session_id,
        "story_animation_enabled": story_animation_enabled,
        "scene_count": 0,
    })
    log.info(f"Creative session created: {session_id} for user {user['id']}")
    return {
        "session_id": session_id,
        "project_id": project_id,
        "story_animation_enabled": story_animation_enabled,
        "quality_mode": quality_mode,
        "mint_mode": mint_mode,
        "art_style": art_style,
        "cinematic_boost": cinematic_boost,
        "transition_style": transition_style,
        "micro_escalation_mode": micro_escalation_mode,
        "voice_id": voice_id,
        "voice_speed": voice_speed,
        "pacing_mode": pacing_mode,
        "animation_enabled": animation_enabled,
        "reference_lock_mode": reference_lock_mode,
        "prompt_passthrough": True,
    }


@app.post("/api/creative/reference-image")
async def creative_reference_image(
    session_id: str = Form(...),
    reference_image: UploadFile = File(...),
    reference_lock_mode: str = Form("strict"),
    request: Request = None,
):
    """Upload optional creative reference style image and persist it for all scene generations."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    session = _get_creative_session(session_id)
    if not session:
        raise HTTPException(404, "Creative session not found")
    if session["user_id"] != user["id"]:
        raise HTTPException(403, "Not your session")
    if not reference_image.content_type or not reference_image.content_type.startswith("image/"):
        raise HTTPException(400, "Reference file must be an image")

    raw = await reference_image.read()
    if not raw:
        raise HTTPException(400, "Reference image is empty")
    if len(raw) > 8 * 1024 * 1024:
        raise HTTPException(400, "Reference image must be <= 8MB")

    lock_mode = _normalize_reference_lock_mode(reference_lock_mode, default=_normalize_reference_lock_mode(session.get("reference_lock_mode"), "strict"))
    quality = _analyze_reference_quality(raw, lock_mode=lock_mode)
    if not quality.get("accepted", True) and lock_mode == "strict":
        raise HTTPException(400, "Reference image quality too low for Strict Reference Lock. Upload a higher-resolution image or switch to Style Inspired mode.")

    mime = reference_image.content_type or "image/png"
    data_url = f"data:{mime};base64,{base64.b64encode(raw).decode()}"
    ext = ".png"
    if "jpeg" in mime or "jpg" in mime:
        ext = ".jpg"
    elif "webp" in mime:
        ext = ".webp"
    ref_dir = TEMP_DIR / "creative_references"
    ref_dir.mkdir(parents=True, exist_ok=True)
    ref_name = f"{session_id}_reference{ext}"
    ref_path = ref_dir / ref_name
    ref_path.write_bytes(raw)
    public_url = f"{SITE_URL.rstrip('/')}/api/creative/reference-file/{ref_name}"
    session["reference_image_url"] = data_url
    session["reference_image_path"] = str(ref_path)
    session["reference_image_public_url"] = public_url
    session["reference_lock_mode"] = lock_mode
    session["reference_quality"] = quality
    session["reference_dna"] = _extract_reference_dna(raw, template=str(session.get("template", "skeleton") or "skeleton"))
    session["reference_image_uploaded"] = True
    session["rolling_reference_image_url"] = public_url
    if session.get("template") == "skeleton":
        # Keep legacy skeleton key in sync for downstream compatibility.
        session["skeleton_reference_image"] = public_url
    async with _creative_sessions_lock:
        _save_creative_sessions_to_disk()
    return {
        "ok": True,
        "reference_lock_mode": lock_mode,
        "quality": quality,
        "reference_image_public_url": public_url,
        "reference_dna": session.get("reference_dna", {}),
    }


@app.get("/api/creative/reference-file/{filename}")
async def creative_reference_file(filename: str):
    """Serve uploaded creative reference images over HTTPS for provider conditioning."""
    safe = os.path.basename(filename)
    if not safe or safe != filename:
        raise HTTPException(400, "Invalid filename")
    path = TEMP_DIR / "creative_references" / safe
    if not path.exists():
        raise HTTPException(404, "Reference image not found")
    media_type = "image/png"
    if path.suffix.lower() in {".jpg", ".jpeg"}:
        media_type = "image/jpeg"
    elif path.suffix.lower() == ".webp":
        media_type = "image/webp"
    return FileResponse(str(path), media_type=media_type, filename=safe)


@app.get("/api/creative/session/{session_id}/status")
async def creative_session_status(session_id: str, request: Request = None):
    """Return lightweight status for restoring Creative Control UI state after refresh."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    session = _get_creative_session(session_id)
    if not session:
        raise HTTPException(404, "Creative session not found")
    if session["user_id"] != user["id"]:
        raise HTTPException(403, "Not your session")
    return {
        "session_id": session_id,
        "has_reference_image": bool(
            _skeleton_session_has_explicit_reference(session)
            if str(session.get("template", "") or "").strip().lower() == "skeleton"
            else (session.get("reference_image_url") or session.get("skeleton_reference_image"))
        ),
        "reference_lock_mode": _normalize_reference_lock_mode(session.get("reference_lock_mode"), "strict"),
        "reference_quality": session.get("reference_quality", {}),
        "template": session.get("template", ""),
        "quality_mode": _normalize_skeleton_quality_mode(session.get("quality_mode"), template=session.get("template", "")),
        "mint_mode": _normalize_mint_mode(session.get("mint_mode"), template=session.get("template", "")),
        "art_style": _normalize_art_style(session.get("art_style", "auto"), template=session.get("template", "")),
        "cinematic_boost": _normalize_cinematic_boost(session.get("cinematic_boost", False)),
        "transition_style": _normalize_transition_style(session.get("transition_style", "smooth")),
        "micro_escalation_mode": _normalize_micro_escalation_mode(session.get("micro_escalation_mode"), template=session.get("template", "")),
        "topic": session.get("topic", ""),
        "scene_count": len(session.get("scenes", [])),
        "animation_enabled": _bool_from_any(session.get("animation_enabled"), _bool_from_any(session.get("story_animation_enabled"), True)),
        "story_animation_enabled": _bool_from_any(session.get("story_animation_enabled"), True),
        "prompt_passthrough": _creative_prompt_passthrough_enabled(session),
    }


@app.get("/api/creative/session/{session_id}/scene-images")
async def creative_session_scene_images(session_id: str, request: Request = None):
    """Return persisted creative scene images from backend storage (RunPod), not browser cache."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    session = _get_creative_session(session_id)
    if not session:
        raise HTTPException(404, "Creative session not found")
    if session["user_id"] != user["id"]:
        raise HTTPException(403, "Not your session")

    out = []
    scene_images = session.get("scene_images", {}) or {}
    for key, info in scene_images.items():
        if not isinstance(info, dict):
            continue
        path = str(info.get("path", "") or "")
        if not path:
            continue
        p = Path(path)
        if not p.exists():
            continue
        try:
            idx = int(key)
        except Exception:
            continue
        mime = "image/png" if p.suffix.lower() == ".png" else "image/jpeg"
        payload = base64.b64encode(p.read_bytes()).decode("ascii")
        out.append({
            "scene_index": idx,
            "image_data": f"data:{mime};base64,{payload}",
            "generation_id": str(info.get("generation_id", "") or ""),
            "cdn_url": str(info.get("cdn_url", "") or ""),
        })
    out.sort(key=lambda x: int(x.get("scene_index", 0)))
    return {"session_id": session_id, "scene_images": out}


@app.post("/api/creative/scene-image")
async def creative_scene_image(req: SceneImageRequest, request: Request = None):
    """Generate (or regenerate) an image for a specific scene. Unlimited regenerations."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _user_has_paid_access(user):
        raise HTTPException(402, "Active subscription required. Please choose a plan.")
    user_plan, _plan_limits = _resolve_user_plan_for_limits(user)
    is_admin = user.get("email", "") in ADMIN_EMAILS
    billing_active = _billing_active_for_user(user)
    can_run, _source, _state = await _reserve_generation_credit(
        user,
        user_plan if not is_admin else "pro",
        billing_active,
        is_admin=is_admin,
        usage_kind="non_animated",
    )
    if not can_run:
        raise HTTPException(402, "Non-animated meter exhausted for this month. Please wait for renewal or upgrade plan.")
    session = _get_creative_session(req.session_id)
    if not session:
        raise HTTPException(404, "Creative session not found")
    if session["user_id"] != user["id"]:
        raise HTTPException(403, "Not your session")

    if "scene_images" not in session:
        session["scene_images"] = {}

    while len(session["scenes"]) <= req.scene_index:
        session["scenes"].append({"visual_description": "", "narration": "", "duration_sec": 5})

    prev_img = session["scene_images"].get(req.scene_index, {})
    prev_gen_id = prev_img.get("generation_id")
    if prev_gen_id:
        asyncio.create_task(_mark_training_feedback(prev_gen_id, accepted=False, user_id=user.get("id", ""), event="regenerate"))

    template = session.get("template", req.template)
    quality_mode = _normalize_skeleton_quality_mode(req.quality_mode or session.get("quality_mode"), template=template)
    mint_mode = _normalize_mint_mode(req.mint_mode if req.mint_mode is not None else session.get("mint_mode"), template=template)
    art_style = _normalize_art_style(req.art_style or session.get("art_style", "auto"), template=template)
    cinematic_boost = _normalize_cinematic_boost(getattr(req, "cinematic_boost", session.get("cinematic_boost", False)))
    if cinematic_boost:
        quality_mode = "cinematic"
        mint_mode = True
    reference_lock_mode = _normalize_reference_lock_mode(req.reference_lock_mode or session.get("reference_lock_mode"), "strict")
    session["reference_lock_mode"] = reference_lock_mode
    session["art_style"] = art_style
    _ensure_reference_public_url(req.session_id, session)
    user_plan, plan_limits = _resolve_user_plan_for_limits(user)
    resolution = _normalize_output_resolution(session.get("resolution", req.resolution), priority_allowed=bool(plan_limits.get("priority", False)))
    neg_prompt = str(getattr(req, "negative_prompt", "") or "").strip()
    raw_prompt = str(req.prompt or "").strip()
    if not raw_prompt:
        raise HTTPException(400, "Scene prompt is required")
    prompt_passthrough = _creative_prompt_passthrough_enabled(session)
    scene_reference = _resolve_reference_for_scene(session, template, req.scene_index)
    prompt_requests_damage_or_fatigue = bool(
        re.search(
            r"\b(crack|cracks|fracture|fractured|chip|chipped|bruise|bruises|damaged|damage|tired|fatigued|weary|slouch|slouched|hunch|hunched|droop|drooping|exhausted)\b",
            raw_prompt,
            flags=re.IGNORECASE,
        )
    )
    effective_reference_lock_mode = reference_lock_mode
    if (
        template == "skeleton"
        and prompt_passthrough
        and reference_lock_mode == "strict"
        and prompt_requests_damage_or_fatigue
        and not bool(scene_reference)
    ):
        # Strict reference lock can suppress requested damage/posture deltas.
        # In passthrough mode, soften to inspired so prompt details can render.
        effective_reference_lock_mode = "inspired"
    if template == "skeleton":
        log.info(
            f"Creative scene prompt mode: passthrough={prompt_passthrough} "
            f"lock={reference_lock_mode}->{effective_reference_lock_mode} "
            f"prompt='{raw_prompt[:220]}'"
        )
    if prompt_passthrough:
        constrained_prompt = raw_prompt
        full_prompt = _build_creative_passthrough_scene_prompt(
            template=template,
            visual_description=raw_prompt,
            quality_mode=quality_mode,
            reference_dna=session.get("reference_dna", {}),
            reference_lock_mode=reference_lock_mode,
            art_style=art_style,
        )
    else:
        constrained_prompt = raw_prompt
        if template == "skeleton":
            constrained_prompt = _apply_template_scene_constraints(
                [{"visual_description": raw_prompt}],
                template,
                quality_mode=quality_mode,
            )[0]["visual_description"]
        constrained_prompt = _apply_mint_scene_compiler(
            [{"visual_description": constrained_prompt}],
            template,
            mint_mode=mint_mode,
        )[0]["visual_description"]
        full_prompt = _build_scene_prompt_with_reference(
            template=template,
            visual_description=constrained_prompt,
            quality_mode=quality_mode,
            reference_dna=session.get("reference_dna", {}),
            reference_lock_mode=reference_lock_mode,
            art_style=art_style,
        )

    img_path = str(TEMP_DIR / f"{req.session_id}_scene_{req.scene_index}.png")
    try:
        img_result = await generate_scene_image(
            full_prompt,
            img_path,
            resolution=resolution,
            negative_prompt=neg_prompt,
            template=template,
            reference_image_url=scene_reference,
            reference_lock_mode=effective_reference_lock_mode,
            best_of_enabled=False,
            salvage_enabled=(template == "story"),
            interactive_fast=True,
            prompt_passthrough=prompt_passthrough,
        )
    except Exception as e:
        err_text = str(e or "").strip()
        err_l = err_text.lower()
        if "hidream is the only configured image provider" in err_l:
            raise HTTPException(503, err_text) from e
        if template == "skeleton" and "hidream" in err_l and ("timed out" in err_l or "timeout" in err_l):
            raise HTTPException(
                504,
                "Skeleton image generation timed out on the NYPTID Studio image engine. Click Regenerate to retry.",
            ) from e
        if template == "skeleton" and (
            "Skeleton generation blocked" in err_text
            or "WAN2.2 text-to-image is unavailable" in err_text
        ):
            raise HTTPException(503, err_text) from e
        if template == "skeleton" and (
            "timed out" in err_l
            or "timeout" in err_l
        ):
            raise HTTPException(
                504,
                "Skeleton image generation timed out on the NYPTID Studio image engine. Click Regenerate to retry.",
            ) from e
        if template == "skeleton" and "exhausted wan2.2 attempt budget" in err_l:
            raise HTTPException(
                504,
                "WAN2.2 exhausted interactive budget before finishing. Install the real WAN2.2 TI2V/T2I model (wan2.2_ti2v_5B_fp16.safetensors) to avoid slow fallback lanes.",
            ) from e
        if template == "skeleton" and (
            "qa gate failed" in err_l
            or "failed qa gate" in err_l
            or ("qa gate" in err_l and "skeleton" in err_l)
            or "prompt mismatch" in err_l
            or "prop_missing_or_wrong" in err_l
        ):
            raise HTTPException(
                422,
                "Scene prompt not satisfied strongly enough (skeleton/props mismatch). Click Regenerate to retry with stricter composition.",
            ) from e
        should_try_moderation_rewrite = (
            template == "story"
            and (
                _looks_like_provider_moderation_error(str(e or ""))
                or _prompt_likely_moderated(constrained_prompt)
            )
        )
        if should_try_moderation_rewrite:
            retry_candidates = [
                _soften_story_prompt_for_moderation(constrained_prompt, aggressive=False),
                _soften_story_prompt_for_moderation(constrained_prompt, aggressive=True),
            ]
            seen = {constrained_prompt}
            retried = False
            last_retry_error: Exception | None = None
            for candidate in retry_candidates:
                if not candidate or candidate in seen:
                    continue
                seen.add(candidate)
                retried = True
                constrained_prompt = candidate
                full_prompt = _build_scene_prompt_with_reference(
                    template=template,
                    visual_description=constrained_prompt,
                    quality_mode=quality_mode,
                    reference_dna=session.get("reference_dna", {}),
                    reference_lock_mode=reference_lock_mode,
                    art_style=art_style,
                )
                try:
                    img_result = await generate_scene_image(
                        full_prompt,
                        img_path,
                        resolution=resolution,
                        negative_prompt=neg_prompt,
                        template=template,
                        reference_image_url=scene_reference,
                        reference_lock_mode=effective_reference_lock_mode,
                        best_of_enabled=False,
                        salvage_enabled=False,
                        interactive_fast=True,
                        prompt_passthrough=prompt_passthrough,
                    )
                    last_retry_error = None
                    break
                except Exception as retry_err:
                    last_retry_error = retry_err
            if last_retry_error is not None or not retried:
                raise HTTPException(
                    400,
                    "Image provider rejected this scene prompt due to safety policy. Try regenerate, or simplify violent wording while keeping the same emotion.",
                ) from (last_retry_error or e)
        else:
            if template == "skeleton":
                detail = err_text if err_text else "unknown skeleton image generation failure"
                if len(detail) > 260:
                    detail = detail[:260].rstrip() + "..."
                raise HTTPException(500, f"Skeleton image generation failed: {detail}") from e
            raise HTTPException(
                500,
                "Scene image generation failed. Please regenerate this scene.",
            ) from e
    if template == "skeleton":
        qa_notes = list(img_result.get("qa_notes", []) or [])
        qa_ok = bool(img_result.get("qa_ok", False))
        severe_qa = (not qa_ok) and _skeleton_notes_are_severe(qa_notes)
        if severe_qa:
            raise HTTPException(
                422,
                "Scene prompt not satisfied strongly enough (skeleton/props mismatch). Click Regenerate to retry with stricter composition.",
            )
        # Creative prompt-passthrough mode should not nudge extra regenerations for
        # non-severe soft-accept outcomes; treat those as acceptable matches.
        if prompt_passthrough and not qa_ok:
            img_result["qa_ok"] = True
            img_result["qa_notes"] = [n for n in qa_notes if str(n or "").strip().lower() != "interactive_soft_accept"]
    if template == "skeleton" and not _skeleton_default_identity_locked(session) and req.scene_index == 0 and not (session.get("skeleton_reference_image") or session.get("reference_image_url")):
        session["skeleton_reference_image"] = _file_to_data_image_url(img_path)
    if template == "skeleton" and _skeleton_default_identity_locked(session):
        session["rolling_reference_image_url"] = _default_reference_for_template("skeleton")
    elif reference_lock_mode == "strict" or not session.get("rolling_reference_image_url"):
        rolled = str(img_result.get("cdn_url", "") or "").strip() or _file_to_data_image_url(img_path)
        if rolled:
            session["rolling_reference_image_url"] = rolled

    gen_id = img_result.get("generation_id", "")

    import base64 as b64mod
    img_bytes = Path(img_path).read_bytes()
    img_b64 = b64mod.b64encode(img_bytes).decode()

    session["scene_images"][req.scene_index] = {
        "path": img_path,
        "cdn_url": img_result.get("cdn_url"),
        "prompt": constrained_prompt,
        "generation_id": gen_id,
        "qa_score": float(img_result.get("qa_score", 0.0) or 0.0),
        "qa_ok": bool(img_result.get("qa_ok", False)),
        "qa_notes": img_result.get("qa_notes", []),
    }

    while len(session["scenes"]) <= req.scene_index:
        session["scenes"].append({"narration": "", "visual_description": "", "negative_prompt": "", "duration_sec": 5})
    session["scenes"][req.scene_index]["visual_description"] = constrained_prompt
    session["scenes"][req.scene_index]["negative_prompt"] = neg_prompt
    session["quality_mode"] = quality_mode
    session["mint_mode"] = mint_mode
    session["art_style"] = art_style
    session["cinematic_boost"] = cinematic_boost
    async with _creative_sessions_lock:
        _save_creative_sessions_to_disk()
    await _update_project_by_session(user.get("id", ""), req.session_id, {
        "status": "draft",
        "scene_count": len(session["scenes"]),
        "scenes": session.get("scenes", []),
        "narration": session.get("narration", ""),
    })

    return {
        "scene_index": req.scene_index,
        "image_data": f"data:image/png;base64,{img_b64}",
        "prompt_used": constrained_prompt,
        "generation_id": gen_id,
        "quality_mode": quality_mode,
        "mint_mode": mint_mode,
        "qa_score": float(img_result.get("qa_score", 0.0) or 0.0),
        "qa_ok": bool(img_result.get("qa_ok", False)),
        "qa_notes": img_result.get("qa_notes", []),
    }


@app.post("/api/creative/scene-feedback")
async def creative_scene_feedback(body: dict, request: Request = None):
    """Mark a generated image as accepted (user moved on) or rejected (user regenerated)."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    gen_id = body.get("generation_id", "")
    accepted = body.get("accepted", True)
    if gen_id:
        await _mark_training_feedback(gen_id, accepted=accepted, user_id=user.get("id", ""), event="scene_feedback")
    return {"ok": True, "generation_id": gen_id, "status": "accepted" if accepted else "rejected"}


@app.put("/api/creative/scene/{session_id}/{scene_index}")
async def creative_update_scene(session_id: str, scene_index: int, body: dict, request: Request = None):
    """Update a scene's narration or visual description."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    session = _get_creative_session(session_id)
    if not session or session["user_id"] != user["id"]:
        raise HTTPException(404, "Session not found")
    if scene_index >= len(session["scenes"]):
        raise HTTPException(400, "Invalid scene index")
    scene = session["scenes"][scene_index]
    if "narration" in body:
        scene["narration"] = body["narration"]
    if "visual_description" in body:
        scene["visual_description"] = body["visual_description"]
    if "negative_prompt" in body:
        scene["negative_prompt"] = str(body.get("negative_prompt", "") or "")
    if "duration_sec" in body:
        scene["duration_sec"] = body["duration_sec"]
    async with _creative_sessions_lock:
        _save_creative_sessions_to_disk()
    await _update_project_by_session(user.get("id", ""), session_id, {
        "status": "draft",
        "scene_count": len(session["scenes"]),
        "scenes": session.get("scenes", []),
        "narration": session.get("narration", ""),
    })
    return {"ok": True, "scene": scene}


@app.post("/api/creative/finalize")
async def creative_finalize(req: FinalizeRequest, background_tasks: BackgroundTasks, request: Request = None):
    """Phase 3: Run the full pipeline using the user's scenes + images."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _user_has_paid_access(user):
        raise HTTPException(402, "Active subscription required. Please choose a plan.")
    session = _get_creative_session(req.session_id)
    if not session or session["user_id"] != user["id"]:
        raise HTTPException(404, "Session not found")
    _ensure_template_allowed(session.get("template", req.template), user)
    _ensure_reference_public_url(req.session_id, session)

    if req.narration:
        session["narration"] = req.narration
    if req.scenes:
        session["scenes"] = req.scenes
    quality_mode = _normalize_skeleton_quality_mode(req.quality_mode or session.get("quality_mode"), template=session.get("template", req.template))
    mint_mode = _normalize_mint_mode(req.mint_mode if req.mint_mode is not None else session.get("mint_mode"), template=session.get("template", req.template))
    art_style = _normalize_art_style(req.art_style or session.get("art_style", "auto"), template=session.get("template", req.template))
    cinematic_boost = _normalize_cinematic_boost(getattr(req, "cinematic_boost", session.get("cinematic_boost", False)))
    transition_style = _normalize_transition_style(req.transition_style or session.get("transition_style"))
    micro_escalation_mode = _normalize_micro_escalation_mode(
        req.micro_escalation_mode if req.micro_escalation_mode is not None else session.get("micro_escalation_mode"),
        template=session.get("template", req.template),
    )
    if cinematic_boost:
        quality_mode = "cinematic"
        mint_mode = True
        transition_style = "cinematic"
        micro_escalation_mode = True
    voice_id = str(req.voice_id or session.get("voice_id", "") or "").strip()
    voice_speed = _normalize_voice_speed(req.voice_speed if req.voice_speed is not None else session.get("voice_speed", 1.0), default=1.0)
    pacing_mode = _normalize_pacing_mode(req.pacing_mode or session.get("pacing_mode", "standard"))
    subtitles_enabled = _bool_from_any(req.subtitles_enabled, _bool_from_any(session.get("subtitles_enabled"), True))
    if session.get("template", req.template) != "story" or not STORY_ADVANCED_CONTROLS_ENABLED:
        voice_id = ""
        voice_speed = 1.0
        pacing_mode = "standard"
    reference_lock_mode = _normalize_reference_lock_mode(req.reference_lock_mode or session.get("reference_lock_mode"), "strict")
    animation_enabled = _bool_from_any(
        req.animation_enabled,
        _bool_from_any(session.get("animation_enabled"), _bool_from_any(req.story_animation_enabled, _bool_from_any(session.get("story_animation_enabled"), True))),
    )
    story_animation_enabled = animation_enabled if session.get("template") == "story" else True
    async with _creative_sessions_lock:
        session["quality_mode"] = quality_mode
        session["mint_mode"] = mint_mode
        session["art_style"] = art_style
        session["cinematic_boost"] = cinematic_boost
        session["transition_style"] = transition_style
        session["micro_escalation_mode"] = micro_escalation_mode
        session["voice_id"] = voice_id
        session["voice_speed"] = voice_speed
        session["pacing_mode"] = pacing_mode
        session["subtitles_enabled"] = subtitles_enabled
        session["reference_lock_mode"] = reference_lock_mode
        session["animation_enabled"] = animation_enabled
        session["story_animation_enabled"] = story_animation_enabled
        _save_creative_sessions_to_disk()
    if not session["scenes"]:
        raise HTTPException(400, "No scenes provided")

    user_plan, plan_limits = _resolve_user_plan_for_limits(user)
    is_admin = user.get("email", "") in ADMIN_EMAILS
    billing_active = _billing_active_for_user(user)
    usage_kind = "animated" if animation_enabled else "non_animated"
    credits_required = max(1, len(session.get("scenes", []))) if animation_enabled else 1
    can_render, credit_source, credit_state = await _reserve_generation_credit(
        user,
        user_plan if not is_admin else "pro",
        billing_active,
        is_admin=is_admin,
        usage_kind=usage_kind,
        credits_needed=credits_required,
    )
    if not can_render:
        if usage_kind == "non_animated":
            raise HTTPException(402, "Non-animated meter exhausted for this month. Please wait for renewal or upgrade plan.")
        available_credits = int(credit_state.get("credits_total_remaining", 0) or 0)
        required_credits = int(credit_state.get("credits_needed", credits_required) or credits_required)
        raise HTTPException(
            402,
            f"This render needs {required_credits} animation credits, but only {available_credits} are available. Buy more credits or switch to slideshow.",
        )
    resolution = _normalize_output_resolution(session.get("resolution", req.resolution), priority_allowed=bool(plan_limits.get("priority", False)))
    total_duration = sum(float(s.get("duration_sec", 5) or 5) for s in session.get("scenes", []))
    if total_duration > float(plan_limits.get("max_duration_sec", 60)):
        raise HTTPException(400, f"Creative project exceeds plan duration limit ({int(plan_limits.get('max_duration_sec', 60))}s).")
    # The 12-scene cap was for legacy WAN-heavy skeleton renders.
    # With Runway as primary, allow longer skeleton projects.
    if (not RUNWAY_API_KEY) and session.get("template") == "skeleton" and len(session.get("scenes", [])) > 12:
        raise HTTPException(400, "Skeleton Creative projects are limited to 12 scenes when Runway is unavailable.")

    job_id = f"{int(time.time())}_{random.randint(1000, 9999)}"
    jobs[job_id] = {
        "status": "queued",
        "progress": 0,
        "template": session["template"],
        "topic": session["topic"],
        "lane": "create",
        "mode": "creative_finalize" if animation_enabled else "creative_slideshow",
        "resolution": resolution,
        "plan": user_plan,
        "user_id": user.get("id"),
        "created_at": time.time(),
        "animation_enabled": animation_enabled,
        "story_animation_enabled": story_animation_enabled,
        "quality_mode": quality_mode,
        "mint_mode": mint_mode,
            "art_style": art_style,
        "transition_style": transition_style,
        "micro_escalation_mode": micro_escalation_mode,
        "voice_id": voice_id,
        "voice_speed": voice_speed,
        "pacing_mode": pacing_mode,
        "subtitles_enabled": subtitles_enabled,
        "reference_lock_mode": reference_lock_mode,
        "credit_charged": True,
        "credit_source": credit_source,
        "credit_amount": credits_required,
        "credit_cost": credits_required if animation_enabled else 0,
        "billing_source": "owner_override" if is_admin else credit_source,
        "credit_month_key": credit_state.get("month_key", _month_key()),
        "credit_refunded": False,
    }
    _job_diag_init(job_id, "creative")
    await _update_project_by_session(user.get("id", ""), req.session_id, {
        "status": "rendering",
        "job_id": job_id,
        "scene_count": len(session.get("scenes", [])),
        "scenes": session.get("scenes", []),
        "narration": session.get("narration", ""),
        "voice_id": voice_id,
        "voice_speed": voice_speed,
        "pacing_mode": pacing_mode,
        "animation_enabled": animation_enabled,
        "story_animation_enabled": story_animation_enabled,
    })

    for si_data in session.get("scene_images", {}).values():
        gen_id = si_data.get("generation_id")
        if gen_id:
            asyncio.create_task(_mark_training_feedback(gen_id, accepted=True, user_id=user.get("id", ""), event="finalize"))

    try:
        # Queue runtime compatibility:
        # pass only the original positional args and read render modes from session.
        await enqueue_generation_job(job_id, user_plan, _run_creative_pipeline, (job_id, session, resolution))
    except QueueFullError as e:
        if jobs[job_id].get("credit_charged") and str(jobs[job_id].get("credit_source", "")) in {"monthly", "topup"}:
            await _refund_generation_credit(
                str(jobs[job_id].get("user_id", "") or ""),
                str(jobs[job_id].get("credit_source", "") or ""),
                month_key=str(jobs[job_id].get("credit_month_key", "") or ""),
                credits=int(jobs[job_id].get("credit_amount", 1) or 1),
            )
            jobs[job_id]["credit_refunded"] = True
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)
        await persist_job_state(job_id, jobs[job_id])
        raise HTTPException(429, str(e))
    return {"job_id": job_id}


async def _run_creative_pipeline(
    job_id: str,
    session: dict,
    resolution: str,
    quality_mode: str = "standard",
    mint_mode: bool = True,
    transition_style: str = "smooth",
    micro_escalation_mode: bool = True,
):
    """Run generation using pre-approved creative session scenes."""
    try:
        template = session["template"]
        quality_mode = _normalize_skeleton_quality_mode(session.get("quality_mode") or quality_mode, template=template)
        mint_mode = _normalize_mint_mode(session.get("mint_mode") if session.get("mint_mode") is not None else mint_mode, template=template)
        transition_style = _normalize_transition_style(session.get("transition_style") or transition_style)
        micro_escalation_mode = _normalize_micro_escalation_mode(session.get("micro_escalation_mode") if session.get("micro_escalation_mode") is not None else micro_escalation_mode, template=template)
        cinematic_boost = _normalize_cinematic_boost(session.get("cinematic_boost", False))
        if cinematic_boost:
            quality_mode = "cinematic"
            mint_mode = True
            transition_style = "cinematic"
            micro_escalation_mode = True
        voice_id = str(session.get("voice_id", "") or "").strip()
        voice_speed = _normalize_voice_speed(session.get("voice_speed", 1.0), default=1.0)
        pacing_mode = _normalize_pacing_mode(session.get("pacing_mode", "standard"))
        subtitles_enabled = _bool_from_any(session.get("subtitles_enabled"), True)
        scenes = _normalize_scenes_for_render(session["scenes"])
        prompt_passthrough = _creative_prompt_passthrough_enabled(session)
        if not prompt_passthrough:
            scenes = _apply_template_scene_constraints(scenes, template, quality_mode=quality_mode)
            scenes = _apply_mint_scene_compiler(scenes, template, mint_mode=mint_mode)
        if not (template == "story" and pacing_mode != "standard"):
            scenes = _force_template_scene_duration(scenes, template)
        scenes = _apply_story_pacing(scenes, template, pacing_mode=pacing_mode)
        language = session.get("language", "en")
        scene_images = session.get("scene_images", {})
        script_data = session.get("script_data", {})
        reference_lock_mode = _normalize_reference_lock_mode(session.get("reference_lock_mode"), "strict")
        reference_dna = session.get("reference_dna", {}) if isinstance(session.get("reference_dna"), dict) else {}
        art_style = _normalize_art_style(session.get("art_style", "auto"), template=template)

        fal_video_enabled = bool(FAL_AI_KEY)
        runway_video_enabled = bool(RUNWAY_API_KEY)
        use_video_engine = fal_video_enabled or runway_video_enabled
        animation_enabled = _bool_from_any(session.get("animation_enabled"), _bool_from_any(session.get("story_animation_enabled"), True))
        story_animation_enabled = _bool_from_any(session.get("story_animation_enabled"), animation_enabled)
        use_video = bool(use_video_engine and animation_enabled)
        if use_video and not use_video_engine:
            raise RuntimeError("Video is required but no engine is configured (set RUNWAY_API_KEY or FAL_AI_KEY)")
        jobs[job_id]["animation_enabled"] = bool(animation_enabled)
        jobs[job_id]["story_animation_enabled"] = bool(story_animation_enabled)
        gen_ts = str(int(time.time() * 1000))

        _job_set_stage(job_id, "generating_images", 10)
        jobs[job_id]["total_scenes"] = len(scenes)

        scene_assets = []
        total_steps = len(scenes) * (2 if use_video else 1)
        skeleton_anchor = ""
        if template == "skeleton" and scenes:
            skeleton_anchor = _canonical_skeleton_anchor()

        for i, scene in enumerate(scenes):
            jobs[job_id]["current_scene"] = i + 1
            step_base = i * (2 if use_video else 1)
            _job_set_stage(job_id, "generating_images", 10 + int((step_base / total_steps) * 55))
            _job_record_scene_event(job_id, i, len(scenes), "image_start")

            pre_gen = scene_images.get(i) or scene_images.get(str(i))
            if pre_gen and Path(pre_gen["path"]).exists():
                img_path = pre_gen["path"]
                cdn_url = pre_gen.get("cdn_url")
                log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} using pre-approved image")
                _job_record_scene_event(job_id, i, len(scenes), "image_ready", "pre_approved")
            else:
                if prompt_passthrough:
                    full_prompt = _build_creative_passthrough_scene_prompt(
                        template=template,
                        visual_description=scene.get("visual_description", ""),
                        quality_mode=quality_mode,
                        skeleton_anchor=skeleton_anchor,
                        reference_dna=reference_dna,
                        reference_lock_mode=reference_lock_mode,
                        art_style=art_style,
                    )
                else:
                    full_prompt = _build_scene_prompt_with_reference(
                        template=template,
                        visual_description=scene.get("visual_description", ""),
                        quality_mode=quality_mode,
                        skeleton_anchor=skeleton_anchor,
                        reference_dna=reference_dna,
                        reference_lock_mode=reference_lock_mode,
                        art_style=art_style,
                    )
                img_path = str(TEMP_DIR / (job_id + "_scene_" + str(i) + ".png"))
                scene_negative_prompt = str(scene.get("negative_prompt", "") or "").strip()
                img_result = await generate_scene_image(
                    full_prompt,
                    img_path,
                    resolution=resolution,
                    negative_prompt=scene_negative_prompt,
                    template=template,
                    reference_image_url=_resolve_reference_for_scene(session, template, i),
                    reference_lock_mode=reference_lock_mode,
                    prompt_passthrough=prompt_passthrough,
                )
                if template == "skeleton" and not _skeleton_default_identity_locked(session) and not (session.get("reference_image_url") or session.get("skeleton_reference_image")) and i == 0:
                    skeleton_seed = _file_to_data_image_url(img_path)
                    if skeleton_seed:
                        session["skeleton_reference_image"] = skeleton_seed
                if template == "skeleton" and _skeleton_default_identity_locked(session):
                    session["rolling_reference_image_url"] = _default_reference_for_template("skeleton")
                elif reference_lock_mode == "strict" or not session.get("rolling_reference_image_url"):
                    rolled = str(img_result.get("cdn_url", "") or "").strip() or _file_to_data_image_url(img_path)
                    if rolled:
                        session["rolling_reference_image_url"] = rolled
                cdn_url = img_result.get("cdn_url")
                log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} image generated fresh")
                _job_record_scene_event(job_id, i, len(scenes), "image_ready", "generated")

            asset = {"image": img_path, "frames": None, "kling_clip": None}

            if use_video:
                _job_set_stage(job_id, "animating_scenes", 10 + int(((step_base + 1) / total_steps) * 55))
                _job_record_scene_event(job_id, i, len(scenes), "animation_start")
                kling_motion = TEMPLATE_KLING_MOTION.get(template, "Cinematic motion, smooth camera movement, subtle animation.")
                anim_prompt = scene.get("visual_description", "") + " " + kling_motion
                try:
                    anim_result = await animate_scene(
                        img_path, anim_prompt, str(TEMP_DIR), i, gen_ts,
                        duration_sec=scene.get("duration_sec", 5), image_cdn_url=cdn_url,
                        prefer_wan=(template == "skeleton"),
                    )
                except Exception as anim_err:
                    jobs[job_id]["animation_warnings"] = int(jobs[job_id].get("animation_warnings", 0)) + 1
                    log.warning(f"[{job_id}] Scene {i+1}/{len(scenes)} animation failed, using static image: {anim_err}")
                    _job_record_scene_event(job_id, i, len(scenes), "animation_failed", str(anim_err))
                    anim_result = {"type": "static"}
                if anim_result["type"] in ("kling_clip", "wan_clip", "grok_clip", "runway_clip"):
                    asset["kling_clip"] = anim_result["path"]
                    if anim_result["type"] == "runway_clip":
                        engine = "Runway"
                    elif anim_result["type"] == "grok_clip":
                        engine = "Grok Imagine Video"
                    elif anim_result["type"] == "kling_clip":
                        engine = "Kling 2.1"
                    else:
                        engine = "Wan 2.2"
                    log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} animated by {engine}")
                    _job_record_scene_event(job_id, i, len(scenes), "animation_ready", engine)
                else:
                    log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} using static image")
                    _job_record_scene_event(job_id, i, len(scenes), "animation_static")

            scene_assets.append(asset)

        _job_set_stage(job_id, "generating_voice", 70)
        full_narration = session.get("narration", "") or " ".join(s.get("narration", "") for s in scenes)
        audio_path = str(TEMP_DIR / (job_id + "_voice.mp3"))
        vo_result = await generate_voiceover(
            full_narration,
            audio_path,
            template=template,
            language=language,
            override_voice_id=voice_id,
            override_speed=voice_speed,
        )
        audio_path = vo_result["audio_path"]
        word_timings = vo_result.get("word_timings", [])

        subtitle_path = None
        if subtitles_enabled and word_timings:
            subtitle_path = str(TEMP_DIR / (job_id + "_captions.ass"))
            generate_ass_subtitles(word_timings, subtitle_path, resolution=resolution, template=template)

        sfx_paths = []
        if _sfx_enabled():
            _job_set_stage(job_id, "generating_sfx", 78)
            for i, scene in enumerate(scenes):
                sfx_out = str(TEMP_DIR / (job_id + "_sfx_" + str(i) + ".mp3"))
                desc = scene.get("visual_description", "")
                dur = scene.get("duration_sec", 5)
                sfx_file = await generate_scene_sfx(desc, dur, sfx_out, template=template, scene_index=i, total_scenes=len(scenes))
                sfx_paths.append(sfx_file)
            sfx_paths = await _quintuple_check_scene_sfx(scenes, sfx_paths, template, job_id=job_id)
            log.info(f"[{job_id}] SFX generated: {sum(1 for s in sfx_paths if s)}/{len(scenes)} scenes")
        else:
            log.info(f"[{job_id}] SFX disabled globally; skipping generation/mix")

        _job_set_stage(job_id, "compositing", 82)
        output_filename = template + "_" + job_id + ".mp4"
        output_path = str(OUTPUT_DIR / output_filename)
        await composite_video(
            scenes,
            scene_assets,
            audio_path,
            output_path,
            resolution=resolution,
            use_svd=use_video,
            subtitle_path=subtitle_path,
            sfx_paths=sfx_paths,
            transition_style=_normalize_transition_style(transition_style),
            micro_escalation_mode=micro_escalation_mode,
        )

        for sfx in sfx_paths:
            if sfx:
                Path(sfx).unlink(missing_ok=True)
        for asset in scene_assets:
            Path(asset["image"]).unlink(missing_ok=True)
            if asset.get("kling_clip"):
                Path(asset["kling_clip"]).unlink(missing_ok=True)
        Path(audio_path).unlink(missing_ok=True)
        if subtitle_path:
            Path(subtitle_path).unlink(missing_ok=True)

        _job_set_stage(job_id, "complete", 100)
        jobs[job_id]["output_file"] = output_filename
        jobs[job_id]["resolution"] = resolution
        jobs[job_id]["metadata"] = {
            "title": script_data.get("title", session.get("topic", "")),
            "description": script_data.get("description", ""),
            "tags": script_data.get("tags", []),
        }
        await _update_project_by_job(job_id, {
            "status": "rendered",
            "output_file": output_filename,
            "title": script_data.get("title", session.get("topic", "")),
        })
        _job_diag_finalize(job_id)
        log.info(f"[{job_id}] CREATIVE PIPELINE COMPLETE: {output_filename}")

        sid = session.get("session_id")
        if sid:
            async with _creative_sessions_lock:
                _creative_sessions.pop(sid, None)
                _save_creative_sessions_to_disk()

    except Exception as e:
        log.error(f"[{job_id}] Creative pipeline failed: {e}", exc_info=True)
        _job_set_stage(job_id, "error")
        jobs[job_id]["error"] = str(e)
        _job_diag_finalize(job_id)
        await _update_project_by_job(job_id, {"status": "error", "error": str(e)})


@app.get("/api/languages")
async def list_languages():
    return {"languages": [{"code": k, "name": v["name"]} for k, v in SUPPORTED_LANGUAGES.items()]}


@app.get("/api/health")
async def health():
    skeleton_lora = await check_skeleton_lora_available()
    provider_order = _configured_image_provider_order()
    hidream_configured = any(_normalize_image_provider_key(p) == "hidream" for p in provider_order)
    wan_configured = any(_normalize_image_provider_key(p) == "wan22" for p in provider_order)
    hidream_ready = await check_hidream_available() if hidream_configured else False
    hidream_edit_ready = await check_hidream_edit_available() if hidream_configured else False
    wan_ready = await check_wan22_available() if wan_configured else False
    wan_t2i_ready = await check_wan22_t2i_available() if wan_configured else False
    now_ts = time.time()
    hidream_checked_ts = float(_hidream_availability_cache.get("checked_ts", 0.0) or 0.0)
    hidream_last_ok_ts = float(_hidream_availability_cache.get("last_ok_ts", 0.0) or 0.0)
    hidream_last_error = str(_hidream_availability_cache.get("last_error", "") or "")
    hidream_model = str(_hidream_availability_cache.get("model_name", "") or "")
    hidream_edit_checked_ts = float(_hidream_edit_availability_cache.get("checked_ts", 0.0) or 0.0)
    hidream_edit_last_ok_ts = float(_hidream_edit_availability_cache.get("last_ok_ts", 0.0) or 0.0)
    hidream_edit_last_error = str(_hidream_edit_availability_cache.get("last_error", "") or "")
    hidream_edit_model = str(_hidream_edit_availability_cache.get("model_name", "") or "")
    wan_t2i_checked_ts = float(_wan22_t2i_availability_cache.get("checked_ts", 0.0) or 0.0)
    wan_t2i_last_ok_ts = float(_wan22_t2i_availability_cache.get("last_ok_ts", 0.0) or 0.0)
    wan_t2i_last_error = str(_wan22_t2i_availability_cache.get("last_error", "") or "")
    wan_t2i_mode = str(_wan22_t2i_availability_cache.get("mode", "") or "")
    wan_t2i_checkpoint = str(_wan22_t2i_availability_cache.get("ckpt_name", "") or "")
    wan_t2i_unet = str(_wan22_t2i_availability_cache.get("unet_name", "") or "")
    provider_label = " > ".join(provider_order)
    backend_commit, frontend_bundle = _read_deploy_meta()
    fal_video_enabled = bool(FAL_AI_KEY)
    runway_video_enabled = bool(RUNWAY_API_KEY)
    grok_video_enabled = USE_XAI_VIDEO and bool(XAI_API_KEY)
    if runway_video_enabled and fal_video_enabled:
        video_engine = "Runway (primary) + FalAI Kling fallback"
    elif runway_video_enabled and grok_video_enabled:
        video_engine = "Runway (primary) + Grok fallback"
    elif fal_video_enabled:
        video_engine = "FalAI Kling 2.1"
    elif runway_video_enabled:
        video_engine = "Runway Image-to-Video"
    elif grok_video_enabled:
        video_engine = "Grok Imagine Video"
    elif wan_ready:
        video_engine = "Wan 2.2 (RunPod)"
    elif FAL_AI_KEY:
        video_engine = "Kling 2.1 Standard"
    else:
        video_engine = "Static"
    return {
        "status": "online",
        "engine": "NYPTID Studio Engine v3.0",
        "ffmpeg_available": _ffmpeg_available(),
        "kling_enabled": bool(FAL_AI_KEY),
        "hidream_ready": hidream_ready,
        "hidream_model": hidream_model,
        "hidream_checked_ago_sec": int(max(0.0, now_ts - hidream_checked_ts)) if hidream_checked_ts else -1,
        "hidream_last_ok_ago_sec": int(max(0.0, now_ts - hidream_last_ok_ts)) if hidream_last_ok_ts else -1,
        "hidream_last_error": hidream_last_error,
        "hidream_edit_ready": hidream_edit_ready,
        "hidream_edit_model": hidream_edit_model,
        "hidream_edit_checked_ago_sec": int(max(0.0, now_ts - hidream_edit_checked_ts)) if hidream_edit_checked_ts else -1,
        "hidream_edit_last_ok_ago_sec": int(max(0.0, now_ts - hidream_edit_last_ok_ts)) if hidream_edit_last_ok_ts else -1,
        "hidream_edit_last_error": hidream_edit_last_error,
        "wan22_ready": wan_ready,
        "wan22_t2i_ready": wan_t2i_ready,
        "wan22_t2i_mode": wan_t2i_mode,
        "wan22_t2i_checkpoint": wan_t2i_checkpoint,
        "wan22_t2i_unet": wan_t2i_unet,
        "wan22_t2i_checked_ago_sec": int(max(0.0, now_ts - wan_t2i_checked_ts)) if wan_t2i_checked_ts else -1,
        "wan22_t2i_last_ok_ago_sec": int(max(0.0, now_ts - wan_t2i_last_ok_ts)) if wan_t2i_last_ok_ts else -1,
        "wan22_t2i_last_error": wan_t2i_last_error,
        "video_engine": video_engine,
        "runway_key_configured": runway_video_enabled,
        "runway_key_source": RUNWAY_API_KEY_SOURCE if runway_video_enabled else "",
        "runway_video_model": RUNWAY_VIDEO_MODEL if runway_video_enabled else "",
        "comfyui_url": COMFYUI_URL[:50],
        "skeleton_lora": skeleton_lora,
        "image_engine_skeleton": (
            ("Skeleton LoRA (local) > " + provider_label)
            if skeleton_lora
            else provider_label
        ),
        "image_provider_order": provider_order,
        "xai_image_fallback_enabled": bool(XAI_IMAGE_FALLBACK_ENABLED),
        "fal_image_backup_model": _normalize_fal_image_backup_model(FAL_IMAGE_BACKUP_MODEL) if FAL_AI_KEY else "",
        "image_local_provider_retries": int(IMAGE_LOCAL_PROVIDER_RETRIES),
        "image_provider_failure_cooldown_sec": int(IMAGE_PROVIDER_FAILURE_COOLDOWN_SEC),
        "image_provider_wan_skip_if_unavailable": bool(IMAGE_PROVIDER_WAN_SKIP_IF_UNAVAILABLE),
        "skeleton_require_wan22": bool(SKELETON_REQUIRE_WAN22),
        "image_provider_fail_counts": dict(_image_provider_fail_counts),
        "image_provider_success_counts": dict(_image_provider_success_counts),
        "image_provider_cooldowns_sec": _provider_cooldown_snapshot(),
        "image_fallback_events_total": int(_image_provider_fallback_total),
        "image_fallback_events_pairs": dict(_image_provider_fallback_pairs),
        "template_adapter_routing_enabled": bool(TEMPLATE_ADAPTER_ROUTING_ENABLED),
        "template_adapter_routes": sorted(k for k in (TEMPLATE_ADAPTER_ROUTING or {}).keys()),
        "backend_commit": backend_commit,
        "frontend_bundle": frontend_bundle,
        "queue_mode": "redis" if (REDIS_QUEUE_ENABLED and bool(REDIS_URL)) else "inprocess",
        "force_720p_only": FORCE_720P_ONLY,
    }


@app.head("/api/health")
async def health_head():
    return Response(status_code=200)


@app.post("/api/admin/comfyui-url")
async def set_comfyui_url(body: dict, user: dict = Depends(require_auth)):
    """Admin-only: update the ComfyUI URL at runtime (e.g. after cloudflared restart)."""
    email = user.get("email", "")
    if email not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin only")
    global COMFYUI_URL
    new_url = body.get("url", "").strip().rstrip("/")
    if not new_url:
        raise HTTPException(400, "url required")
    COMFYUI_URL = new_url
    wan_ready = await check_wan22_available()
    return {"ok": True, "comfyui_url": COMFYUI_URL, "wan22_ready": wan_ready}


@app.get("/api/admin/training-stats")
async def training_stats(user: dict = Depends(require_auth)):
    """Admin: get training data collection stats."""
    email = user.get("email", "")
    if email not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin only")
    pairs = list(TRAINING_DATA_DIR.glob("*.png"))
    accepted = sum(1 for e in _pending_training.values() if e["status"] == "accepted")
    rejected = sum(1 for e in _pending_training.values() if e["status"] == "rejected")
    pending = sum(1 for e in _pending_training.values() if e["status"] == "pending")
    return {
        "total_on_disk": len(pairs),
        "accepted": accepted,
        "rejected": rejected,
        "pending_review": pending,
        "disk_mb": round(sum(p.stat().st_size for p in pairs) / (1024 * 1024), 1),
    }


@app.get("/api/admin/analytics")
async def admin_analytics(user: dict = Depends(require_auth)):
    """Admin dashboard analytics: active usage + paid tier totals + monthly revenue estimate."""
    email = user.get("email", "")
    if email not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin only")

    active_job_statuses = {"queued", "generating_script", "generating_images", "animating_scenes", "generating_voice", "generating_sfx", "compositing", "analyzing"}
    active_jobs = []
    for j in jobs.values():
        if isinstance(j, dict) and j.get("status") in active_job_statuses:
            active_jobs.append(j)
    active_generations = len(active_jobs)
    active_generating_users = len({j.get("user_id") for j in active_jobs if j.get("user_id")})

    tier_counts = {"starter": 0, "creator": 0, "pro": 0, "elite": 0, "demo_pro": 0}
    monthly_revenue_usd = 0.0
    revenue_source = "none"

    if STRIPE_SECRET_KEY:
        try:
            subs = stripe_lib.Subscription.list(status="all", limit=100, expand=["data.items.data.price"])
            for sub in subs.auto_paging_iter():
                sub_status = sub.get("status")
                if sub_status not in ("active", "trialing", "past_due", "unpaid"):
                    continue
                for item in sub.get("items", {}).get("data", []):
                    price = item.get("price", {}) or {}
                    price_id = price.get("id", "")
                    plan = STRIPE_PRICE_TO_PLAN.get(price_id)
                    if plan in tier_counts:
                        qty = int(item.get("quantity", 1) or 1)
                        tier_counts[plan] += qty
                        monthly_revenue_usd += ((price.get("unit_amount") or 0) / 100.0) * qty
            revenue_source = "stripe"
        except Exception as e:
            log.warning(f"Admin analytics stripe read failed: {e}")

    if revenue_source != "stripe" and SUPABASE_URL and (SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY):
        # Fallback: infer subscriber counts from profile plans if Stripe is unavailable.
        try:
            svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.get(
                    f"{SUPABASE_URL}/rest/v1/profiles?select=plan&limit=5000",
                    headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
                )
                if resp.status_code == 200:
                    for row in resp.json():
                        p = row.get("plan")
                        if p in tier_counts:
                            tier_counts[p] += 1
                            monthly_revenue_usd += float(PLAN_PRICE_USD.get(p, 0.0) or 0.0)
                    revenue_source = "profiles"
        except Exception as e:
            log.warning(f"Admin analytics profiles fallback failed: {e}")

    active_users_signins_15m = 0
    if SUPABASE_URL and (SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY):
        try:
            svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
            now_utc = datetime.now(timezone.utc)
            async with httpx.AsyncClient(timeout=20) as client:
                users_resp = await client.get(
                    f"{SUPABASE_URL}/auth/v1/admin/users?per_page=500",
                    headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
                )
                if users_resp.status_code == 200:
                    users_data = users_resp.json()
                    user_list = users_data.get("users", users_data) if isinstance(users_data, dict) else users_data
                    for u in user_list:
                        ts = u.get("last_sign_in_at")
                        if not ts:
                            continue
                        try:
                            signed_in_at = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                            if (now_utc - signed_in_at).total_seconds() <= 15 * 60:
                                active_users_signins_15m += 1
                        except Exception:
                            continue
        except Exception as e:
            log.warning(f"Admin analytics active-users fetch failed: {e}")

    queue_depth = 0
    queue_workers = 1
    queue_max_depth = 1
    try:
        queue_depth = max(0, int(await get_queue_depth()))
    except Exception as e:
        log.warning(f"Admin analytics queue depth read failed: {e}")
    try:
        queue_workers = max(1, int(get_queue_workers()))
    except Exception as e:
        log.warning(f"Admin analytics queue workers read failed: {e}")
    try:
        queue_max_depth = max(1, int(get_queue_max_depth()))
    except Exception as e:
        log.warning(f"Admin analytics queue max-depth read failed: {e}")
    active_users_estimate = max(active_generating_users, active_users_signins_15m)
    queue_utilization_pct = round((queue_depth / queue_max_depth) * 100, 1)
    active_generations_per_worker = round(active_generations / queue_workers, 2)
    high_load_detected = queue_utilization_pct >= 70.0 or active_generations_per_worker >= 1.0
    voice_diag = await _voice_provider_snapshot(force_refresh=False)
    return {
        "active_generations": active_generations,
        "queue_depth": queue_depth,
        "queue_workers": queue_workers,
        "queue_max_depth": queue_max_depth,
        "queue_utilization_pct": queue_utilization_pct,
        "active_generations_per_worker": active_generations_per_worker,
        "high_load_detected": high_load_detected,
        "active_users_generating": active_generating_users,
        "active_users_signins_15m": active_users_signins_15m,
        "active_users_estimate": active_users_estimate,
        "maintenance_banner_enabled": _maintenance_banner_enabled,
        "maintenance_banner_message": _maintenance_banner_message,
        "subscribers_by_tier": tier_counts,
        "total_paid_subscribers": sum(tier_counts.values()),
        "monthly_revenue_usd": round(monthly_revenue_usd, 2),
        # We currently expose gross subscription revenue as a profit proxy.
        "monthly_profit_usd": round(monthly_revenue_usd, 2),
        "revenue_source": revenue_source,
        "voice_provider_ok": voice_diag["provider_ok"],
        "voice_catalog_source": voice_diag["source"],
        "voice_catalog_count": voice_diag["count"],
        "voice_catalog_warning": voice_diag["warning"],
    }


@app.get("/api/admin/waiting-list")
async def admin_waiting_list(user: dict = Depends(require_auth)):
    if user.get("email", "") not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin only")
    rows = await _supabase_get_waitlist_rows(limit=3000)
    total = len(rows)
    by_plan = {"starter": 0, "creator": 0, "pro": 0, "elite": 0}
    paid_revenue_monthly = 0.0
    for row in rows:
        plan = str((row or {}).get("plan", "") or "").strip().lower()
        if plan in by_plan:
            by_plan[plan] += 1
        if bool((row or {}).get("paid")):
            paid_revenue_monthly += float((row or {}).get("price_usd", 0.0) or 0.0)
    return {
        "rows": rows,
        "summary": {
            "total": total,
            "by_plan": by_plan,
            "paid_revenue_monthly_usd": round(paid_revenue_monthly, 2),
        },
    }


@app.get("/api/admin/billing-audit")
async def admin_billing_audit(user: dict = Depends(require_auth)):
    email = str(user.get("email", "") or "")
    if email not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin only")
    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not SUPABASE_URL or not svc_key:
        raise HTTPException(500, "Supabase not configured")

    rows: list[dict] = []
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                f"{SUPABASE_URL}/auth/v1/admin/users?per_page=500",
                headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
            )
            if resp.status_code != 200:
                raise HTTPException(500, "Failed to read auth users for billing audit")
            users_data = resp.json()
            user_list = users_data.get("users", users_data) if isinstance(users_data, dict) else users_data
            by_email = {
                str(u.get("email", "") or "").strip().lower(): str(u.get("id", "") or "")
                for u in (user_list or [])
                if u and str(u.get("email", "") or "").strip()
            }

            prof = await client.get(
                f"{SUPABASE_URL}/rest/v1/profiles?select=id,plan",
                headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
            )
            if prof.status_code != 200:
                raise HTTPException(500, "Failed to read profile plans for billing audit")
            profiles = prof.json()
            profiles = profiles if isinstance(profiles, list) else []
            for p in profiles:
                plan = str((p or {}).get("plan", "none") or "none").strip().lower()
                if not _profile_plan_is_paid(plan):
                    continue
                uid = str((p or {}).get("id", "") or "")
                acct_email = ""
                for e, eid in by_email.items():
                    if eid == uid:
                        acct_email = e
                        break
                if not acct_email:
                    continue
                stripe_diag = _stripe_subscription_snapshot(acct_email)
                stripe_status = str(stripe_diag.get("status", "") or "")
                stripe_ok = bool(stripe_diag.get("ok")) and stripe_status in {"active", "trialing", "past_due"}
                cancel_at_period_end = bool(stripe_diag.get("cancel_at_period_end", False))
                status_source = "stripe" if stripe_ok else "profile_fallback"
                next_renewal_unix = int(stripe_diag.get("next_renewal_unix", 0) or 0)
                next_renewal_source = str(stripe_diag.get("next_renewal_source", "") or "")
                paid_at_unix = int(stripe_diag.get("paid_at_unix", 0) or 0)
                interval_months = max(1, int(stripe_diag.get("interval_months", 1) or 1))
                if next_renewal_unix <= 0 and paid_at_unix > 0 and not cancel_at_period_end:
                    rolled = _next_renewal_from_anchor(paid_at_unix, interval_months)
                    if rolled > 0:
                        next_renewal_unix = int(rolled)
                        next_renewal_source = next_renewal_source or "paid_at_rollforward_fallback"
                rows.append(
                    {
                        "email": acct_email,
                        "user_id": uid,
                        "plan": plan,
                        "status_source": status_source,
                        "stripe_status": stripe_status or "unknown",
                        "cancel_at_period_end": cancel_at_period_end,
                        "billing_active": bool(stripe_ok or _profile_plan_is_paid(plan)),
                        "next_renewal_unix": next_renewal_unix,
                        "next_renewal_source": next_renewal_source,
                        "paid_at_unix": paid_at_unix,
                    }
                )
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Admin billing audit failed: {e}")
        raise HTTPException(500, "Billing audit failed")

    rows.sort(key=lambda r: (r.get("plan", ""), r.get("email", "")))
    return {"rows": rows, "total_paid_profiles": len(rows)}


@app.post("/api/admin/maintenance-banner")
async def admin_set_maintenance_banner(body: dict, user: dict = Depends(require_auth)):
    global _maintenance_banner_enabled, _maintenance_banner_message
    email = user.get("email", "")
    if email not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin only")

    enabled = _bool_from_any(body.get("enabled"), _maintenance_banner_enabled)
    message = str(body.get("message", _maintenance_banner_message)).strip()
    if not message:
        message = "Studio is under high load. Queue times may be longer than usual while we scale capacity."

    _maintenance_banner_enabled = enabled
    _maintenance_banner_message = message

    try:
        escaped_message = '"' + message.replace('"', '\\"') + '"'
        _persist_env_overrides(
            {
                "MAINTENANCE_BANNER_ENABLED": "1" if enabled else "0",
                "MAINTENANCE_BANNER_MESSAGE": escaped_message,
            }
        )
    except Exception as e:
        log.warning(f"Failed to persist maintenance banner settings to .env: {e}")

    return {
        "ok": True,
        "maintenance_banner_enabled": _maintenance_banner_enabled,
        "maintenance_banner_message": _maintenance_banner_message,
    }


@app.get("/api/config")
async def public_config():
    return {
        "supabase_url": SUPABASE_URL,
        "supabase_anon_key": SUPABASE_ANON_KEY,
        "stripe_enabled": bool(STRIPE_SECRET_KEY and STRIPE_TOPUP_PUBLIC_ENABLED),
        "waitlist_only_mode": False,
        "waitlist_requires_stripe_payment": False,
        "maintenance_banner_enabled": _maintenance_banner_enabled,
        "maintenance_banner_message": _maintenance_banner_message,
        "plans": {
            name: {k: v for k, v in limits.items()}
            for name, limits in PLAN_LIMITS.items()
        },
        "plan_features": {
            name: list(features)
            for name, features in PLAN_FEATURES.items()
        },
        "plan_prices_usd": {k: float(v) for k, v in PLAN_PRICE_USD.items()},
        "prices": ({v: k for k, v in STRIPE_PRICE_TO_PLAN.items()} if STRIPE_TOPUP_PUBLIC_ENABLED else {}),
        "topup_packs": [
            {"price_id": price_id, **meta}
            for price_id, meta in TOPUP_PACKS.items()
        ],
        "transition_styles": list(TRANSITION_STYLE_MAP.keys()),
        "story_art_style_count": len(ART_STYLE_PRESETS),
        "render_capabilities": {
            "animated_max_resolution": ("720p" if FORCE_720P_ONLY else "1080p"),
            "micro_escalation_supported": True,
            "micro_escalation_max_source_scenes": MICRO_ESCALATION_MAX_SOURCE_SCENES,
            "micro_escalation_max_output_clips": MICRO_ESCALATION_MAX_OUTPUT_CLIPS,
        },
        "billing_model": {
            "hybrid_enabled": True,
            "model": "membership_plus_credits",
            "default_membership_plan_id": _default_membership_plan_id(),
            "membership_label": "Catalyst Membership",
            "paypal_primary": True,
            "slideshows_free": True,
            "animated_credit_label": "Catalyst credits",
            "non_animated_credit_label": "script/image/slideshow operations",
            "overage_label": "credit wallet top-ups",
            "hard_stop_on_animated_exhaustion": True,
            "waitlist_only_mode": False,
            "waitlist_requires_stripe_payment": False,
            "kling21_standard_i2v_5s_usd": KLING21_STANDARD_I2V_5S_USD,
            "animation_markup_multiplier": ANIMATION_MARKUP_MULTIPLIER,
            "animation_credit_unit_usd": ANIMATION_CREDIT_UNIT_USD,
        },
        "feature_flags": {
            "script_to_short_enabled": SCRIPT_TO_SHORT_ENABLED,
            "story_advanced_controls_enabled": STORY_ADVANCED_CONTROLS_ENABLED,
            "story_retention_tuning_enabled": STORY_RETENTION_TUNING_ENABLED,
            "disable_all_sfx": DISABLE_ALL_SFX,
            "longform_beta_enabled": bool(LONGFORM_BETA_ENABLED),
        },
    }


@app.get("/api/landing/notifications")
async def landing_notifications_feed():
    cutoff = time.time() - (7 * 24 * 3600)
    async with _landing_notifications_lock:
        events = [
            e for e in _landing_notifications
            if isinstance(e, dict) and float(e.get("ts") or 0.0) >= cutoff
        ]
        events = events[-LANDING_NOTIFICATIONS_PUBLIC_LIMIT:]
    return {"events": events}


@app.get("/api/me")
async def get_me(user: dict = Depends(require_auth)):
    email = user.get("email", "")
    is_admin = email in ADMIN_EMAILS
    access_snapshot = _paid_access_snapshot_for_user(user)
    billing_active = bool(access_snapshot.get("billing_active"))
    plan = str(access_snapshot.get("plan", user.get("plan", "none")) or "none").strip().lower()
    if plan == "free":
        plan = "none"
    next_renewal_unix = int(access_snapshot.get("next_renewal_unix", 0) or 0)
    next_renewal_source = str(access_snapshot.get("next_renewal_source", "") or "")
    billing_anchor_unix = int(access_snapshot.get("billing_anchor_unix", 0) or 0)
    if (
        billing_active
        and str(access_snapshot.get("source", "") or "") == "stripe"
        and next_renewal_unix <= 0
        and billing_anchor_unix > 0
    ):
        stripe_diag = _stripe_subscription_snapshot(email)
        interval_months = max(1, int((stripe_diag or {}).get("interval_months", 1) or 1))
        rolled = _next_renewal_from_anchor(billing_anchor_unix, interval_months)
        if rolled > 0:
            next_renewal_unix = int(rolled)
            next_renewal_source = next_renewal_source or "paid_at_rollforward_fallback"
    if is_admin:
        limits = PLAN_LIMITS["pro"]
        limits = {**limits, "videos_per_month": 9999}
    else:
        limits = PLAN_LIMITS.get(plan, PLAN_LIMITS["starter"])
    credit_state = _credit_state_for_user(user, plan if not is_admin else "pro", billing_active, is_admin=is_admin)
    has_demo = is_admin or (PRODUCT_DEMO_PUBLIC_ENABLED and plan == "demo_pro")
    effective_plan = "pro" if is_admin else plan
    membership_plan_id = _membership_plan_for_user(user, access_snapshot)
    features = _plan_features_for(effective_plan, is_admin=is_admin)
    lane_access = _public_lane_access_for_user(user, access_snapshot)
    return {
        "id": user["id"],
        "email": email,
        "plan": effective_plan,
        "role": "admin" if is_admin else "user",
        "owner_override": is_admin,
        "billing_active": billing_active,
        "membership_active": billing_active,
        "membership_plan_id": membership_plan_id,
        "membership_source": str(access_snapshot.get("source", "") or ""),
        "membership_label": "Catalyst Membership" if billing_active or is_admin else "",
        "next_renewal_unix": next_renewal_unix,
        "next_renewal_source": next_renewal_source,
        "billing_anchor_unix": billing_anchor_unix,
        "limits": limits,
        "features": features,
        "lane_access": lane_access,
        "animated_credits_remaining": credit_state["animated_monthly_remaining"],
        "animated_credits_used": credit_state["animated_monthly_used"],
        "animated_credits_limit": credit_state["animated_monthly_limit"],
        "animated_topup_credits_remaining": credit_state["animated_topup_credits"],
        "animated_credits_total_remaining": credit_state["animated_total_remaining"],
        "non_animated_ops_remaining": credit_state["non_animated_monthly_remaining"],
        "non_animated_ops_used": credit_state["non_animated_monthly_used"],
        "non_animated_ops_limit": credit_state["non_animated_monthly_limit"],
        "monthly_credits_remaining": credit_state["monthly_remaining"],
        "monthly_credits_used": credit_state["monthly_used"],
        "monthly_credits_limit": credit_state["monthly_limit"],
        "topup_credits_remaining": credit_state["topup_credits"],
        "credits_total_remaining": credit_state["credits_total_remaining"],
        "included_credits_remaining": credit_state["monthly_remaining"],
        "included_credits_used": credit_state["monthly_used"],
        "included_credits_limit": credit_state["monthly_limit"],
        "credit_wallet_balance": credit_state["topup_credits"],
        "billing_source_precedence": ["monthly", "topup"],
        "requires_topup": credit_state["requires_topup"],
        "credit_month": credit_state["month_key"],
        "demo_access": has_demo,
        "demo_price_id": DEMO_PRO_PRICE_ID,
        "demo_coming_soon": (not PRODUCT_DEMO_PUBLIC_ENABLED),
        "longform_owner_beta": bool(lane_access.get("longform")),
    }


def _user_has_paid_access(user: dict | None) -> bool:
    return bool(user)


@app.post("/api/generate")
async def generate_short(req: GenerateRequest, background_tasks: BackgroundTasks, request: Request = None):
    if not XAI_API_KEY:
        raise HTTPException(500, "XAI_API_KEY not configured")
    if not ELEVENLABS_API_KEY:
        raise HTTPException(500, "ELEVENLABS_API_KEY not configured")

    user = await get_current_user_from_request(request) if request else None
    if user and not _user_has_paid_access(user):
        raise HTTPException(402, "Active subscription required. Please choose a plan.")
    _ensure_template_allowed(req.template, user)
    quality_mode = _normalize_skeleton_quality_mode(req.quality_mode, template=req.template)
    mint_mode = _normalize_mint_mode(req.mint_mode, template=req.template)
    art_style = _normalize_art_style(req.art_style, template=req.template)
    voice_id = str(req.voice_id or "").strip()
    voice_speed = _normalize_voice_speed(req.voice_speed, default=1.0)
    pacing_mode = _normalize_pacing_mode(req.pacing_mode)
    animation_enabled = _bool_from_any(req.animation_enabled, _bool_from_any(req.story_animation_enabled, True))
    if req.template != "story" or not STORY_ADVANCED_CONTROLS_ENABLED:
        voice_id = ""
        voice_speed = 1.0
        pacing_mode = "standard"
    reference_lock_mode = _normalize_reference_lock_mode(req.reference_lock_mode, default="strict")
    reference_image_url = _normalize_reference_with_default(req.template, str(req.reference_image_url or "").strip())
    reference_dna, reference_quality = await _extract_reference_profile(reference_image_url, req.template, reference_lock_mode)
    if (
        reference_quality
        and not reference_quality.get("accepted", True)
        and reference_lock_mode == "strict"
        and not _is_template_default_reference(req.template, reference_image_url)
    ):
        raise HTTPException(400, "Reference image quality too low for Strict lock. Upload a higher-resolution image or use Inspired lock.")
    user_plan = "starter"
    plan_limits = PLAN_LIMITS["starter"]
    is_admin = False
    if user:
        user_plan, plan_limits = _resolve_user_plan_for_limits(user)
        is_admin = user.get("email", "") in ADMIN_EMAILS
        billing_active = _billing_active_for_user(user)
        usage_kind = "animated" if animation_enabled else "non_animated"
        can_render, credit_source, credit_state = await _reserve_generation_credit(
            user,
            user_plan if not is_admin else "pro",
            billing_active,
            is_admin=is_admin,
            usage_kind=usage_kind,
        )
        if not can_render:
            if usage_kind == "non_animated":
                raise HTTPException(402, "Non-animated meter exhausted for this month. Please wait for renewal or upgrade plan.")
            raise HTTPException(
                402,
                "No animated render credits left this month. Buy an animated top-up pack to continue.",
            )
    else:
        credit_source = ""
        credit_state = {"month_key": _month_key()}
    transition_style = _normalize_transition_style(req.transition_style)
    micro_escalation_mode = _normalize_micro_escalation_mode(req.micro_escalation_mode, template=req.template)
    cinematic_boost = _normalize_cinematic_boost(getattr(req, "cinematic_boost", False))

    resolution = _normalize_output_resolution(req.resolution, priority_allowed=bool(plan_limits.get("priority", False)))

    job_id = f"{int(time.time())}_{random.randint(1000, 9999)}"
    jobs[job_id] = {
        "status": "queued",
        "progress": 0,
        "template": req.template,
        "topic": req.prompt,
        "lane": "create",
        "mode": "auto_generate",
        "resolution": resolution,
        "plan": user_plan,
        "user_id": user.get("id") if user else None,
        "created_at": time.time(),
        "quality_mode": quality_mode,
        "mint_mode": mint_mode,
        "art_style": art_style,
        "cinematic_boost": cinematic_boost,
        "transition_style": transition_style,
        "micro_escalation_mode": micro_escalation_mode,
        "voice_id": voice_id,
        "voice_speed": voice_speed,
        "pacing_mode": pacing_mode,
        "animation_enabled": animation_enabled,
        "story_animation_enabled": animation_enabled if req.template == "story" else True,
        "reference_image_url": reference_image_url,
        "reference_lock_mode": reference_lock_mode,
        "reference_dna": reference_dna,
        "reference_quality": reference_quality,
        "credit_charged": bool(user),
        "credit_source": credit_source,
        "credit_amount": 1,
        "credit_cost": 1 if animation_enabled else 0,
        "billing_source": "owner_override" if is_admin else (credit_source or "workspace_access"),
        "credit_month_key": credit_state.get("month_key", _month_key()),
        "credit_refunded": False,
    }
    _job_diag_init(job_id, "auto")
    language = req.language if req.language in SUPPORTED_LANGUAGES else "en"
    if user:
        project_id = _new_project_id()
        await _create_or_update_project(project_id, {
            "user_id": user.get("id"),
            "template": req.template,
            "topic": req.prompt,
            "mode": "auto",
            "status": "rendering",
            "resolution": resolution,
            "language": language,
            "art_style": art_style,
            "cinematic_boost": cinematic_boost,
            "voice_id": voice_id,
            "voice_speed": voice_speed,
            "pacing_mode": pacing_mode,
            "animation_enabled": animation_enabled,
            "job_id": job_id,
        })
        jobs[job_id]["project_id"] = project_id

    try:
        await enqueue_generation_job(
            job_id,
            user_plan,
            run_generation_pipeline,
            (job_id, req.template, req.prompt, resolution, language, quality_mode, mint_mode, transition_style, micro_escalation_mode),
        )
    except QueueFullError as e:
        if jobs[job_id].get("credit_charged") and str(jobs[job_id].get("credit_source", "")) in {"monthly", "topup"}:
            await _refund_generation_credit(
                str(jobs[job_id].get("user_id", "") or ""),
                str(jobs[job_id].get("credit_source", "") or ""),
                month_key=str(jobs[job_id].get("credit_month_key", "") or ""),
                credits=int(jobs[job_id].get("credit_amount", 1) or 1),
            )
            jobs[job_id]["credit_refunded"] = True
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)
        await persist_job_state(job_id, jobs[job_id])
        raise HTTPException(429, str(e))

    return {"status": "accepted", "job_id": job_id}


@app.get("/api/auto/scene-image/{job_id}/{filename}")
async def auto_scene_image(job_id: str, filename: str):
    safe = Path(filename).name
    if safe != filename:
        raise HTTPException(400, "Invalid filename")
    path = _auto_scene_dir(job_id) / safe
    if not path.exists():
        raise HTTPException(404, "Image not found")
    media_type = "image/png" if path.suffix.lower() == ".png" else "image/jpeg"
    return FileResponse(str(path), media_type=media_type, filename=safe)


@app.post("/api/auto/regenerate-scene-image")
async def auto_regenerate_scene_image(body: dict, request: Request = None):
    job_id = str((body or {}).get("job_id", "") or "").strip()
    if not job_id:
        raise HTTPException(400, "job_id is required")
    try:
        scene_index = int((body or {}).get("scene_index", 0))
    except Exception:
        raise HTTPException(400, "scene_index must be an integer")

    state = jobs.get(job_id)
    in_memory_job = True
    if not isinstance(state, dict):
        persisted = await get_persisted_job_state(job_id)
        if not isinstance(persisted, dict):
            raise HTTPException(404, "Job not found")
        state = dict(persisted)
        in_memory_job = False

    # If the job is tied to a user, require matching auth for regeneration.
    owner_user_id = str(state.get("user_id", "") or "")
    if owner_user_id:
        user = await get_current_user_from_request(request) if request else None
        if not user or str(user.get("id", "")) != owner_user_id:
            raise HTTPException(403, "Not authorized for this job")

    resolution = str(state.get("resolution", "720p") or "720p")
    if resolution != "720p":
        raise HTTPException(400, "Image regeneration is available only for 720p auto mode")

    scene_images = state.get("scene_images", [])
    if not isinstance(scene_images, list) or not scene_images:
        raise HTTPException(400, "No persisted scene images for this job")
    if scene_index < 0 or scene_index >= len(scene_images):
        raise HTTPException(404, "Scene index out of range")

    template = str(state.get("template", "skeleton") or "skeleton")
    quality_mode = _normalize_skeleton_quality_mode((body or {}).get("quality_mode") or state.get("quality_mode"), template=template)
    mint_mode = _normalize_mint_mode((body or {}).get("mint_mode") if isinstance(body, dict) else None, template=template)
    art_style = _normalize_art_style(state.get("art_style", "auto"), template=template)
    reference_lock_mode = _normalize_reference_lock_mode(state.get("reference_lock_mode"), default="strict")
    reference_dna = state.get("reference_dna", {}) if isinstance(state.get("reference_dna"), dict) else {}
    reference_image_url = _normalize_reference_with_default(template, str(state.get("reference_image_url", "") or "").strip())
    scene_entry = scene_images[scene_index] if isinstance(scene_images[scene_index], dict) else {}
    scene_prompt = str(scene_entry.get("visual_description", "") or "")
    if not scene_prompt:
        prompts = state.get("scene_prompts", [])
        if isinstance(prompts, list) and 0 <= scene_index < len(prompts):
            scene_prompt = str(prompts[scene_index] or "")
    if not scene_prompt:
        raise HTTPException(400, "Scene prompt unavailable for regeneration")
    scene_prompt = _apply_mint_scene_compiler(
        [{"visual_description": scene_prompt}],
        template,
        mint_mode=mint_mode,
    )[0]["visual_description"]

    neg_prompt = TEMPLATE_NEGATIVE_PROMPTS.get(template, NEGATIVE_PROMPT)
    skeleton_anchor = ""
    if template == "skeleton" and isinstance(scene_images, list) and scene_images:
        skeleton_anchor = _canonical_skeleton_anchor()
    if template == "skeleton":
        scene_prompt = _apply_template_scene_constraints(
            [{"visual_description": scene_prompt}],
            template,
            quality_mode=quality_mode,
        )[0]["visual_description"]
    full_prompt = _build_scene_prompt_with_reference(
        template=template,
        visual_description=scene_prompt,
        quality_mode=quality_mode,
        skeleton_anchor=skeleton_anchor,
        reference_dna=reference_dna,
        reference_lock_mode=reference_lock_mode,
        art_style=art_style,
    )

    ts = int(time.time() * 1000)
    out_name = f"scene_{scene_index + 1:02d}_regen_{ts}.png"
    out_path = str(_auto_scene_dir(job_id) / out_name)
    scene_reference_url = _resolve_reference_for_scene(state, template, scene_index) or reference_image_url
    img_result = await generate_scene_image(
        full_prompt,
        out_path,
        resolution="720p",
        negative_prompt=neg_prompt,
        template=template,
        reference_image_url=scene_reference_url,
        reference_lock_mode=reference_lock_mode,
    )

    old_gen_id = str(scene_entry.get("generation_id", "") or "")
    new_gen_id = str(img_result.get("generation_id", "") or "")
    if old_gen_id and new_gen_id and old_gen_id != new_gen_id:
        try:
            asyncio.create_task(_mark_training_feedback(old_gen_id, accepted=False, user_id=owner_user_id, event="auto_regenerate"))
        except Exception:
            pass

    updated = {
        "scene_index": scene_index,
        "filename": out_name,
        "image_url": _auto_scene_url(job_id, out_name),
        "local_path": str(Path(out_path)),
        "visual_description": scene_prompt,
        "template": template,
        "quality_mode": quality_mode,
        "generation_id": new_gen_id,
        "cdn_url": str(img_result.get("cdn_url", "") or ""),
        "source": "regenerated_auto",
        "updated_at": time.time(),
    }
    scene_images[scene_index] = updated
    state["scene_images"] = scene_images
    state["regenerate_count"] = int(state.get("regenerate_count", 0) or 0) + 1
    if in_memory_job:
        jobs[job_id]["scene_images"] = scene_images
        jobs[job_id]["regenerate_count"] = int(jobs[job_id].get("regenerate_count", 0) or 0) + 1
    await persist_job_state(job_id, state)
    return {"ok": True, "job_id": job_id, "scene_index": scene_index, "image": updated}


@app.get("/api/status/{job_id}")
async def job_status(job_id: str):
    _prune_in_memory_jobs()
    persisted = await get_persisted_job_state(job_id)
    if isinstance(persisted, dict):
        _record_kpi_for_job(job_id, persisted)
        if persisted.get("kpi_recorded"):
            await persist_job_state(job_id, persisted)
        return persisted
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    _record_kpi_for_job(job_id, jobs[job_id])
    return jobs[job_id]


@app.get("/api/download/{filename}")
async def download_video(filename: str):
    path = OUTPUT_DIR / filename
    if not path.exists():
        raise HTTPException(404, "Video not found")
    return FileResponse(str(path), media_type="video/mp4", filename=filename)


@app.post("/api/chatstory/render")
async def render_chat_story(
    request: Request,
    payload: str = Form(...),
    avatar: Optional[UploadFile] = File(None),
    background_video: Optional[UploadFile] = File(None),
):
    user = await get_current_user_from_request(request)
    if not user:
        raise HTTPException(401, "Authentication required")
    if not _chat_story_access_for_user(user):
        raise HTTPException(403, "Chat Story requires an active Starter, Creator, or Pro monthly plan.")

    try:
        parsed_payload = json.loads(payload or "{}")
    except Exception:
        raise HTTPException(400, "Invalid chat story payload")

    messages = parsed_payload.get("messages") or []
    if not isinstance(messages, list) or not any(str(item.get("text", "") or "").strip() for item in messages if isinstance(item, dict)):
        raise HTTPException(400, "Add at least one chat message before rendering.")

    render_id = f"chatstory_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
    work_dir = TEMP_DIR / render_id
    work_dir.mkdir(parents=True, exist_ok=True)
    avatar_path = ""
    bg_video_path = ""

    try:
        if avatar and avatar.filename:
            avatar_ext = Path(avatar.filename).suffix or ".png"
            avatar_path = str(work_dir / f"avatar{avatar_ext}")
            with open(avatar_path, "wb") as handle:
                while chunk := await avatar.read(1024 * 1024):
                    handle.write(chunk)

        if background_video and background_video.filename:
            bg_ext = Path(background_video.filename).suffix or ".mp4"
            bg_video_path = str(work_dir / f"background{bg_ext}")
            with open(bg_video_path, "wb") as handle:
                while chunk := await background_video.read(1024 * 1024):
                    handle.write(chunk)

        payload_path = work_dir / "payload.json"
        payload_path.write_text(json.dumps(parsed_payload, ensure_ascii=False, indent=2), encoding="utf-8")

        output_name = f"{render_id}.mp4"
        output_path = OUTPUT_DIR / output_name
        script_path = Path(__file__).resolve().parent / "ops" / "render_chat_story.py"

        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            str(script_path),
            "--payload",
            str(payload_path),
            "--output",
            str(output_path),
            "--avatar",
            avatar_path,
            "--background-video",
            bg_video_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            log.error("Chat Story render failed: %s", (stderr or b"").decode("utf-8", errors="replace"))
            raise HTTPException(500, "Chat Story render failed.")

        meta = {}
        try:
            meta = json.loads((stdout or b"{}").decode("utf-8", errors="replace").strip() or "{}")
        except Exception:
            meta = {}

        if not output_path.exists():
            raise HTTPException(500, "Chat Story render did not produce an output video.")

        return {
            "ok": True,
            "output_file": output_name,
            "download_url": f"/api/download/{quote(output_name)}",
            "lane": "chatstory",
            "mode": "chatstory_render",
            "credit_cost": 0,
            "billing_source": "workspace_access" if not _is_admin_user(user) else "owner_override",
            "duration_sec": meta.get("duration_sec"),
            "message_count": meta.get("message_count"),
            "voice": meta.get("voice"),
            "theme": meta.get("theme"),
            "background": meta.get("background"),
            "used_background_video": bool(meta.get("used_background_video")),
        }
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


CLONE_ANALYSIS_PROMPT = """You are a viral video reverse-engineering expert. Analyze the source video and extract its EXACT winning formula so it can be replicated on a new topic.

You will receive:
1. Context about a viral short (uploaded video metadata, audio timing, or description)
2. A new topic to apply the viral formula to

Your job: figure out the EXACT structure, pacing, and style of the source video and replicate it beat-for-beat on the new topic.

Analyze these elements:
- HOOK: What made someone stop scrolling in the first 1-3 seconds? (question? claim? shock?)
- PACING: How fast are cuts? What is the average scene duration?
- VISUAL STYLE: 3D skeletons? Cinematic? Text-heavy? What background color?
- NARRATION STYLE: Fast? Punchy? How many sentences per scene?
- TEXT OVERLAYS: One word at a time? Full sentences? Bold impact font?
- STRUCTURE: VS comparison? Countdown? Story arc? How is info revealed?
- RETENTION TRICKS: Money flying, size comparisons, face-offs, shocking numbers?

TEMPLATE DEFINITIONS (pick the one that matches BEST):
- "skeleton" = Canonical 3D anatomical skeleton identity (same skull/eyes/bone proportions every scene, no clothing/costumes), topic-relevant environments allowed, VS comparisons/career breakdowns, one-word bold captions. Example: "NASCAR vs F1 Driver Who Makes More Money"
- "history" = Epic cinematic historical scenes. Battles, empires, ancient events. Dramatic narrator, god rays, film grain. 2-4 word caption phrases. Example: "What Happened to the Roman Legion That Vanished"
- "story" = Cinematic AI visual stories with emotional arc. Pixar/UE5 quality. Continuity of recurring subjects/locations across scenes (do not force one unchanged protagonist). Poetic narration. Minimal captions. Example: "The Last Lighthouse Keeper"
- "reddit" = Reddit story narration. First-person dramatic stories (AITA, TIFU). Photorealistic modern-day scenes illustrating the story. Dialogue/reaction captions. Example: "AITA for Kicking Out My Sister"
- "top5" = Ranked countdown lists (#5 to #1). Each item dramatically different. Documentary quality visuals. Numbered captions. Example: "Top 5 Most Expensive Things Ever Sold"
- "random" = Chaotic, fast-paced, unpredictable content. Every scene wildly different. Surreal visuals. 1-3 word reaction captions. Example: "Things That Should Not Exist"

Output MUST be valid JSON:
{
  "detected_template": "skeleton|history|story|reddit|top5|random",
  "viral_analysis": {
    "hook_type": "What kind of hook (shock/question/claim/visual)",
    "pacing": "fast|medium|slow",
    "avg_scene_duration": 3.5,
    "scene_count": 10,
    "tone": "Description of voice/narration tone",
    "retention_tricks": ["trick1", "trick2"],
    "what_made_it_viral": "1-2 sentence summary"
  },
  "optimized_prompt": "An enhanced prompt that combines the viral formula with the new topic. This should be detailed enough to pass directly to the script generator."
}"""


async def extract_audio_from_video(video_path: str) -> str | None:
    """Extract audio track from a video file for transcription."""
    audio_path = video_path.rsplit(".", 1)[0] + "_audio.mp3"
    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-vn", "-acodec", "libmp3lame", "-q:a", "4",
        audio_path,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    await proc.communicate()
    if proc.returncode == 0 and Path(audio_path).exists():
        return audio_path
    return None


async def transcribe_audio_with_grok(audio_path: str) -> str:
    """Use ffmpeg to get audio duration then estimate narration from file size for context."""
    cmd = ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", str(audio_path)]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, _ = await proc.communicate()
    duration = 0
    if proc.returncode == 0:
        try:
            data = json.loads(stdout.decode())
            duration = float(data.get("format", {}).get("duration", 0))
        except Exception:
            pass
    return f"Audio duration: {duration:.1f}s"


async def analyze_viral_video(topic: str, video_description: str, transcript_hint: str = "", source_notes: str = "") -> dict:
    user_parts = []
    user_parts.append("Source viral video context: " + video_description)
    if transcript_hint:
        user_parts.append("Audio/timing info from source: " + transcript_hint)
    if source_notes:
        user_parts.append("Operator notes / analytics hints: " + source_notes)
    user_parts.append("New topic to apply the viral formula to: " + topic)
    user_msg = "\n\n".join(user_parts)

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {XAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "grok-3-mini-fast",
                "messages": [
                    {"role": "system", "content": CLONE_ANALYSIS_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                "temperature": 0.6,
            },
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        start = content.find("{")
        end = content.rfind("}") + 1
        if start == -1 or end == 0:
            raise ValueError("No JSON in clone analysis response")
        return json.loads(content[start:end])


async def extract_video_metadata(file_path: str) -> dict:
    """Extract basic metadata from uploaded video using ffprobe."""
    try:
        cmd = [
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_format", "-show_streams", str(file_path),
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        if proc.returncode == 0:
            data = json.loads(stdout.decode())
            duration = float(data.get("format", {}).get("duration", 0))
            streams = data.get("streams", [])
            video_stream = next((s for s in streams if s.get("codec_type") == "video"), {})
            return {
                "duration_sec": round(duration, 1),
                "width": int(video_stream.get("width", 0)),
                "height": int(video_stream.get("height", 0)),
                "fps": video_stream.get("r_frame_rate", "30/1"),
            }
    except Exception as e:
        log.warning(f"ffprobe metadata extraction failed: {e}")
    return {}


async def generate_clone_script(template: str, topic: str, viral_analysis: dict) -> dict:
    """Generate a script that replicates the source video's exact formula on a new topic."""
    base_prompt = TEMPLATE_SYSTEM_PROMPTS.get(template, TEMPLATE_SYSTEM_PROMPTS["random"])

    hook_type = viral_analysis.get("hook_type", "claim")
    pacing = viral_analysis.get("pacing", "fast")
    avg_dur = viral_analysis.get("avg_scene_duration", 3.5)
    scene_count = viral_analysis.get("scene_count", 10)
    tone = viral_analysis.get("tone", "energetic and punchy")
    tricks = viral_analysis.get("retention_tricks", [])
    what_viral = viral_analysis.get("what_made_it_viral", "")

    clone_override = (
        "\n\nCRITICAL CLONE INSTRUCTIONS -- you MUST follow these EXACTLY:\n"
        "You are cloning a proven viral video. Replicate its formula precisely.\n"
        "- Hook type: " + str(hook_type) + " -- your opening MUST use this exact hook style\n"
        "- Pacing: " + str(pacing) + " -- match this exact energy level\n"
        "- Target scene count: " + str(scene_count) + " scenes\n"
        "- Average scene duration: " + str(avg_dur) + " seconds\n"
        "- Narration tone: " + str(tone) + "\n"
        "- Retention tricks to replicate: " + ", ".join(tricks) + "\n"
        "- Why the original went viral: " + str(what_viral) + "\n"
        "\nDo NOT write generic content. Do NOT say things like 'dive into the world of' or "
        "'buckle up'. Write EXACTLY like the source video's style -- punchy, direct, zero filler. "
        "Every single word must earn its place. If the source was a skeleton comparing things, "
        "YOU compare things the same way. Match the structure beat-for-beat.\n"
        "\nNarration must be SHORT and PUNCHY -- 1-2 sentences max per scene. "
        "No yapping. No fluff. Every sentence is a hook or a fact bomb."
    )

    full_prompt = base_prompt + clone_override

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {XAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "grok-3-mini-fast",
                "messages": [
                    {"role": "system", "content": full_prompt},
                    {"role": "user", "content": "Clone this viral formula onto new topic: " + topic},
                ],
                "temperature": 0.7,
            },
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        start = content.find("{")
        end = content.rfind("}") + 1
        if start == -1 or end == 0:
            raise ValueError("No JSON in clone script response")
        return json.loads(content[start:end])


async def run_clone_pipeline(
    job_id: str,
    topic: str,
    video_path: str | None,
    source_url: str = "",
    analytics_notes: str = "",
    resolution: str = "720p",
):
    try:
        _job_set_stage(job_id, "analyzing", 5)
        log.info(f"[{job_id}] Clone: analyzing viral video for topic '{topic}'")

        video_context = topic
        transcript_hint = ""
        meta = {}
        source_bundle = {}
        normalized_source_url = _normalize_external_source_url(source_url)
        if normalized_source_url:
            source_bundle = await _fetch_source_video_bundle(normalized_source_url)
            if source_bundle:
                jobs[job_id]["source_video"] = {
                    "source_url": str(source_bundle.get("source_url", "") or normalized_source_url),
                    "title": str(source_bundle.get("title", "") or ""),
                    "channel": str(source_bundle.get("channel", "") or ""),
                    "duration_sec": int(source_bundle.get("duration_sec", 0) or 0),
                    "thumbnail_url": str(source_bundle.get("thumbnail_url", "") or ""),
                    "public_summary": str(source_bundle.get("public_summary", "") or ""),
                }
                video_context = str(source_bundle.get("public_summary", "") or topic)
                transcript_hint = str(source_bundle.get("transcript_excerpt", "") or "")
        if video_path:
            meta = await extract_video_metadata(video_path) or {}
            if meta:
                upload_context = (
                    "Source video file uploaded: "
                    + str(meta.get("duration_sec", "?")) + "s long, "
                    + str(meta.get("width", "?")) + "x" + str(meta.get("height", "?")) + " resolution"
                )
                video_context = f"{video_context}\n{upload_context}".strip()
            audio_path = await extract_audio_from_video(video_path)
            if audio_path:
                transcript_hint = await transcribe_audio_with_grok(audio_path) or transcript_hint
                Path(audio_path).unlink(missing_ok=True)

        analysis = await analyze_viral_video(topic, video_context, transcript_hint, analytics_notes)
        detected_template = analysis.get("detected_template", "random")
        viral_info = analysis.get("viral_analysis", {})

        jobs[job_id]["template"] = detected_template
        jobs[job_id]["viral_analysis"] = viral_info
        jobs[job_id]["progress"] = 12
        log.info(f"[{job_id}] Clone analysis: template={detected_template}, hook={viral_info.get('hook_type', '?')}, scenes={viral_info.get('scene_count', '?')}")

        if video_path:
            Path(video_path).unlink(missing_ok=True)

        _job_set_stage(job_id, "generating_script", 15)
        script_data = await generate_clone_script(detected_template, topic, viral_info)
        scenes = _normalize_scenes_for_render(script_data.get("scenes", []))
        clone_quality_mode = _normalize_skeleton_quality_mode("cinematic", template=detected_template)
        clone_mint_mode = _normalize_mint_mode(True, template=detected_template)
        scenes = _apply_template_scene_constraints(scenes, detected_template, quality_mode=clone_quality_mode)
        scenes = _apply_mint_scene_compiler(scenes, detected_template, mint_mode=clone_mint_mode)
        scenes = _force_template_scene_duration(scenes, detected_template)
        if not scenes:
            raise ValueError("Clone script generation returned no scenes")

        fal_video_enabled = bool(FAL_AI_KEY)
        runway_video_enabled = bool(RUNWAY_API_KEY)
        use_video_engine = fal_video_enabled or runway_video_enabled
        use_video = use_video_engine
        if detected_template == "reddit":
            use_video = False
        if detected_template != "reddit" and not use_video_engine:
            raise RuntimeError("Video is required but no engine is configured (set RUNWAY_API_KEY or FAL_AI_KEY)")
        if runway_video_enabled:
            mode_label = "Runway (primary)"
        elif fal_video_enabled:
            mode_label = "FalAI Kling 2.1"
        else:
            mode_label = "static image"
        jobs[job_id]["generation_mode"] = "video" if use_video_engine else "image"

        _job_set_stage(job_id, "generating_images", 20)
        jobs[job_id]["total_scenes"] = len(scenes)
        log.info(f"[{job_id}] Clone script ready: {len(scenes)} scenes. Mode: {mode_label}, {resolution}")

        prompt_prefix = TEMPLATE_PROMPT_PREFIXES.get(detected_template, "")
        neg_prompt = TEMPLATE_NEGATIVE_PROMPTS.get(detected_template, NEGATIVE_PROMPT)
        scene_assets = []
        total_steps = len(scenes) * (2 if use_video else 1)
        gen_ts = str(int(time.time() * 1000))

        clone_skeleton_anchor = ""
        clone_skeleton_reference_image_url = SKELETON_GLOBAL_REFERENCE_IMAGE_URL if detected_template == "skeleton" else ""
        if detected_template == "skeleton" and scenes:
            clone_skeleton_anchor = _canonical_skeleton_anchor()

        for i, scene in enumerate(scenes):
            jobs[job_id]["current_scene"] = i + 1
            step_base = i * (2 if use_video else 1)
            _job_set_stage(job_id, "generating_images", 20 + int((step_base / total_steps) * 50))
            _job_record_scene_event(job_id, i, len(scenes), "image_start")

            if detected_template == "skeleton":
                vis_desc = scene.get("visual_description", "")
                full_prompt = _build_skeleton_image_prompt(
                    vis_desc,
                    skeleton_anchor=clone_skeleton_anchor,
                    quality_mode=clone_quality_mode,
                )
            else:
                full_prompt = prompt_prefix + scene.get("visual_description", "")
            img_path = str(TEMP_DIR / (job_id + "_scene_" + str(i) + ".png"))
            img_result = await generate_scene_image(
                full_prompt,
                img_path,
                resolution=resolution,
                negative_prompt=neg_prompt,
                template=detected_template,
                reference_image_url=clone_skeleton_reference_image_url if detected_template == "skeleton" else "",
                reference_lock_mode="strict",
            )
            if detected_template == "skeleton" and not clone_skeleton_reference_image_url and i == 0:
                clone_skeleton_reference_image_url = _file_to_data_image_url(img_path)
            cdn_url = img_result.get("cdn_url")
            engine_name = "Skeleton LoRA" if (detected_template == "skeleton" and not cdn_url) else ("Grok Imagine" if cdn_url else "SDXL")
            log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} image generated ({engine_name})")
            _job_record_scene_event(job_id, i, len(scenes), "image_ready", engine_name)

            asset = {"image": img_path, "frames": None, "kling_clip": None}

            if use_video:
                _job_set_stage(job_id, "animating_scenes", 20 + int(((step_base + 1) / total_steps) * 50))
                _job_record_scene_event(job_id, i, len(scenes), "animation_start")
                kling_motion = TEMPLATE_KLING_MOTION.get(detected_template, "Cinematic motion, smooth camera movement, subtle animation.")
                anim_prompt = scene.get("visual_description", "") + " " + kling_motion
                try:
                    anim_result = await animate_scene(
                        img_path, anim_prompt,
                        str(TEMP_DIR), i, gen_ts,
                        duration_sec=scene.get("duration_sec", 5),
                        image_cdn_url=cdn_url,
                        prefer_wan=(detected_template == "skeleton"),
                    )
                except Exception as anim_err:
                    jobs[job_id]["animation_warnings"] = int(jobs[job_id].get("animation_warnings", 0)) + 1
                    log.warning(f"[{job_id}] Scene {i+1}/{len(scenes)} animation failed, using static image: {anim_err}")
                    _job_record_scene_event(job_id, i, len(scenes), "animation_failed", str(anim_err))
                    anim_result = {"type": "static"}
                if anim_result["type"] in ("kling_clip", "wan_clip", "grok_clip", "runway_clip"):
                    asset["kling_clip"] = anim_result["path"]
                    if anim_result["type"] == "runway_clip":
                        engine = "Runway"
                    elif anim_result["type"] == "grok_clip":
                        engine = "Grok Imagine Video"
                    elif anim_result["type"] == "kling_clip":
                        engine = "Kling 2.1"
                    else:
                        engine = "Wan 2.2"
                    log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} animated by {engine}")
                    _job_record_scene_event(job_id, i, len(scenes), "animation_ready", engine)
                else:
                    log.info(f"[{job_id}] Scene {i+1}/{len(scenes)} using static image")
                    _job_record_scene_event(job_id, i, len(scenes), "animation_static")

            scene_assets.append(asset)

        _job_set_stage(job_id, "generating_voice", 75)
        log.info(f"[{job_id}] Generating voiceover...")

        full_narration = " ".join(s.get("narration", "") for s in scenes)
        audio_path = str(TEMP_DIR / (job_id + "_voice.mp3"))
        vo_result = await generate_voiceover(full_narration, audio_path, template=detected_template)
        audio_path = vo_result["audio_path"]
        word_timings = vo_result.get("word_timings", [])

        subtitle_path = None
        if word_timings:
            subtitle_path = str(TEMP_DIR / (job_id + "_captions.ass"))
            generate_ass_subtitles(word_timings, subtitle_path, resolution=resolution, template=detected_template)
            log.info(f"[{job_id}] Word-synced captions generated: {len(word_timings)} words")

        _job_set_stage(job_id, "compositing", 85)
        log.info(f"[{job_id}] Compositing final video at {resolution}...")

        output_filename = detected_template + "_" + job_id + ".mp4"
        output_path = str(OUTPUT_DIR / output_filename)
        await composite_video(scenes, scene_assets, audio_path, output_path, resolution=resolution, use_svd=use_video, subtitle_path=subtitle_path)

        for asset in scene_assets:
            Path(asset["image"]).unlink(missing_ok=True)
            if asset.get("kling_clip"):
                Path(asset["kling_clip"]).unlink(missing_ok=True)
            if asset.get("frames"):
                for fp in asset["frames"]:
                    Path(fp).unlink(missing_ok=True)
                frame_dir = Path(asset["frames"][0]).parent
                if frame_dir.exists():
                    shutil.rmtree(frame_dir, ignore_errors=True)
        Path(audio_path).unlink(missing_ok=True)
        if subtitle_path:
            Path(subtitle_path).unlink(missing_ok=True)

        _job_set_stage(job_id, "complete", 100)
        jobs[job_id]["output_file"] = output_filename
        jobs[job_id]["resolution"] = resolution
        jobs[job_id]["metadata"] = {
            "title": script_data.get("title", topic),
            "description": script_data.get("description", ""),
            "tags": script_data.get("tags", []),
        }
        _job_diag_finalize(job_id)
        log.info(f"[{job_id}] COMPLETE: {output_filename} ({resolution}, {mode_label})")

    except Exception as e:
        log.error(f"[{job_id}] Clone pipeline failed: {e}", exc_info=True)
        _job_set_stage(job_id, "error")
        jobs[job_id]["error"] = str(e)
        _job_diag_finalize(job_id)
        if video_path:
            Path(video_path).unlink(missing_ok=True)


@app.post("/api/clone")
async def clone_video(
    topic: str = Form(...),
    resolution: str = Form("720p"),
    source_url: str = Form(""),
    analytics_notes: str = Form(""),
    file: UploadFile = File(None),
    background_tasks: BackgroundTasks = None,
    request: Request = None,
):
    if not XAI_API_KEY or not ELEVENLABS_API_KEY:
        raise HTTPException(500, "API keys not configured")

    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _user_has_paid_access(user):
        raise HTTPException(402, "Active subscription required. Please choose a plan.")

    res = _normalize_output_resolution(resolution, priority_allowed=False)

    video_path = None
    if file and file.filename:
        video_path = str(TEMP_DIR / f"clone_upload_{int(time.time())}.mp4")
        with open(video_path, "wb") as f:
            while chunk := await file.read(1024 * 1024):
                f.write(chunk)

    job_id = f"clone_{int(time.time())}_{random.randint(1000, 9999)}"
    jobs[job_id] = {
        "status": "queued",
        "progress": 0,
        "template": "analyzing...",
        "topic": topic,
        "source_url": _normalize_external_source_url(source_url),
        "lane": "clone",
        "mode": "clone_rebuild",
        "resolution": res,
        "credit_cost": 0,
        "billing_source": "workspace_access",
        "user_id": str(user.get("id", "") or ""),
        "created_at": time.time(),
    }

    try:
        await enqueue_generation_job(
            job_id,
            "starter",
            run_clone_pipeline,
            (job_id, topic, video_path, source_url, analytics_notes, res),
        )
    except QueueFullError as e:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)
        await persist_job_state(job_id, jobs[job_id])
        raise HTTPException(429, str(e))
    return {"status": "accepted", "job_id": job_id}


@app.get("/api/jobs")
async def list_jobs():
    return {jid: {k: v for k, v in j.items() if k != "output_file"} for jid, j in jobs.items()}


@app.get("/api/projects")
async def list_projects(request: Request = None):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    uid = user.get("id", "")
    rows = [p for p in _projects.values() if p.get("user_id") == uid]
    rows.sort(key=lambda p: p.get("updated_at", 0), reverse=True)
    drafts = [p for p in rows if p.get("status") in ("draft", "rendering")]
    renders = [p for p in rows if p.get("status") in ("rendered", "error")]
    return {"drafts": drafts, "renders": renders, "total": len(rows)}


@app.get("/api/projects/{project_id}")
async def get_project(project_id: str, request: Request = None):
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    proj = _projects.get(project_id)
    if not proj or proj.get("user_id") != user.get("id"):
        raise HTTPException(404, "Project not found")
    return {"project": proj}


# ─── Stripe Payments ──────────────────────────────────────────────────────────

def _stripe_find_customer_id_by_email(email: str) -> str:
    """Best-effort Stripe customer lookup for billing portal/checkout continuity."""
    if not email:
        return ""
    try:
        customers = stripe_lib.Customer.list(email=email, limit=10)
        data = list(getattr(customers, "data", []) or [])
        if not data:
            return ""
        data.sort(key=lambda c: int(getattr(c, "created", 0) or 0), reverse=True)
        return str(data[0].id)
    except Exception as e:
        log.warning(f"Stripe customer lookup failed for {email}: {e}")
        return ""


def _stripe_value(obj, key: str, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _to_unix(value) -> int:
    try:
        if value is None:
            return 0
        return int(float(value))
    except Exception:
        return 0


def _add_months_utc(anchor_unix: int, months: int) -> int:
    anchor = int(anchor_unix or 0)
    m = max(1, int(months or 1))
    if anchor <= 0:
        return 0
    dt = datetime.fromtimestamp(anchor, tz=timezone.utc)
    month_index = (dt.month - 1) + m
    year = dt.year + (month_index // 12)
    month = (month_index % 12) + 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    shifted = dt.replace(year=year, month=month, day=day)
    return int(shifted.timestamp())


def _next_renewal_from_anchor(anchor_unix: int, months: int, now_unix: int | None = None) -> int:
    """Roll an anchor forward by billing intervals until it is in the future."""
    anchor = int(anchor_unix or 0)
    step_months = max(1, int(months or 1))
    if anchor <= 0:
        return 0
    now_ts = int(now_unix or time.time())
    candidate = _add_months_utc(anchor, step_months)
    if candidate <= 0:
        return 0
    # 20 years of monthly roll-forward is a safe upper bound and prevents bad loops.
    for _ in range(240):
        if candidate > now_ts:
            return int(candidate)
        nxt = _add_months_utc(candidate, step_months)
        if nxt <= candidate:
            break
        candidate = nxt
    return int(candidate if candidate > now_ts else 0)


def _subscription_interval_months(sub) -> int:
    try:
        items = _stripe_value(sub, "items", {}) or {}
        item_data = _stripe_value(items, "data", []) or []
        first_item = item_data[0] if item_data else {}
        price = _stripe_value(first_item, "price", {}) or {}
        recurring = _stripe_value(price, "recurring", {}) or {}
        interval = str(_stripe_value(recurring, "interval", "month") or "month").strip().lower()
        interval_count = max(1, _to_unix(_stripe_value(recurring, "interval_count", 1) or 1))
        if interval == "year":
            return interval_count * 12
        if interval == "month":
            return interval_count
        # Fallback for non-monthly intervals (all Studio plans are monthly today).
        return 1
    except Exception:
        return 1


def _invoice_period_end_unix(invoice_obj) -> int:
    lines = _stripe_value(invoice_obj, "lines", {}) or {}
    line_data = _stripe_value(lines, "data", []) or []
    if not line_data:
        return 0
    period = _stripe_value(line_data[0], "period", {}) or {}
    return _to_unix(_stripe_value(period, "end", 0) or 0)


def _invoice_paid_at_unix(invoice_obj) -> int:
    transitions = _stripe_value(invoice_obj, "status_transitions", {}) or {}
    paid_at = _to_unix(_stripe_value(transitions, "paid_at", 0) or 0)
    if paid_at > 0:
        return paid_at
    return _to_unix(_stripe_value(invoice_obj, "created", 0) or 0)


def _stripe_has_active_subscription(email: str) -> bool:
    """Return True when user has active access through Stripe."""
    if not STRIPE_SECRET_KEY:
        # Paywall is strict: no Stripe key means no paid verification.
        return False
    customer_id = _stripe_find_customer_id_by_email(email)
    if not customer_id:
        return False
    try:
        subs = stripe_lib.Subscription.list(customer=customer_id, status="all", limit=20)
        active_statuses = {"active", "trialing", "past_due"}
        for sub in list(getattr(subs, "data", []) or []):
            status = str(getattr(sub, "status", "") or "")
            if status in active_statuses:
                return True
        return False
    except Exception as e:
        # Strict paywall: fail closed on Stripe lookup errors.
        log.warning(f"Stripe subscription lookup failed for {email}: {e}")
        return False


def _stripe_subscription_snapshot(email: str) -> dict:
    """Best-effort Stripe snapshot used by admin billing audit."""
    out = {
        "ok": False,
        "plan": "",
        "status": "",
        "next_renewal_unix": 0,
        "next_renewal_source": "",
        "cancel_at_period_end": False,
        "paid_at_unix": 0,
        "interval_months": 1,
    }
    if not STRIPE_SECRET_KEY or not email:
        return out
    customer_id = _stripe_find_customer_id_by_email(email)
    if not customer_id:
        return out
    try:
        subs = stripe_lib.Subscription.list(
            customer=customer_id,
            status="all",
            limit=20,
            expand=["data.latest_invoice", "data.items.data.price"],
        )
        ranked = list(_stripe_value(subs, "data", []) or [])
        if not ranked:
            return out
        status_rank = {"active": 0, "trialing": 1, "past_due": 2, "incomplete": 3, "canceled": 4}
        ranked.sort(
            key=lambda s: (
                status_rank.get(str(_stripe_value(s, "status", "") or "").strip().lower(), 99),
                -_to_unix(_stripe_value(s, "created", 0) or 0),
            )
        )
        chosen = ranked[0]
        items = _stripe_value(chosen, "items", {}) or {}
        item_data = _stripe_value(items, "data", []) or []
        first_item = item_data[0] if item_data else {}
        first_price = _stripe_value(first_item, "price", {}) or {}
        active_price_id = str(_stripe_value(first_price, "id", "") or "")
        interval_months = _subscription_interval_months(chosen)
        status = str(_stripe_value(chosen, "status", "") or "")
        cancel_at_period_end = bool(_stripe_value(chosen, "cancel_at_period_end", False))
        current_period_end = _to_unix(_stripe_value(chosen, "current_period_end", 0) or 0)
        current_period_start = _to_unix(_stripe_value(chosen, "current_period_start", 0) or 0)
        billing_cycle_anchor = _to_unix(_stripe_value(chosen, "billing_cycle_anchor", 0) or 0)
        start_date = _to_unix(_stripe_value(chosen, "start_date", 0) or 0)
        trial_end = _to_unix(_stripe_value(chosen, "trial_end", 0) or 0)
        created_unix = _to_unix(_stripe_value(chosen, "created", 0) or 0)
        paid_at_unix = current_period_start or billing_cycle_anchor or start_date or created_unix
        next_renewal_unix = current_period_end
        next_renewal_source = "current_period_end" if next_renewal_unix > 0 else ""

        latest_invoice = _stripe_value(chosen, "latest_invoice", None)
        invoice_obj = None
        if latest_invoice:
            if isinstance(latest_invoice, str):
                try:
                    invoice_obj = stripe_lib.Invoice.retrieve(latest_invoice)
                except Exception:
                    invoice_obj = None
            else:
                invoice_obj = latest_invoice
        if invoice_obj:
            invoice_paid_at = _invoice_paid_at_unix(invoice_obj)
            if invoice_paid_at > 0:
                paid_at_unix = invoice_paid_at
            if next_renewal_unix <= 0:
                invoice_period_end = _invoice_period_end_unix(invoice_obj)
                if invoice_period_end > 0:
                    next_renewal_unix = invoice_period_end
                    next_renewal_source = "invoice_period_end"
                elif invoice_paid_at > 0:
                    rolled = _next_renewal_from_anchor(invoice_paid_at, interval_months)
                    if rolled > 0:
                        next_renewal_unix = rolled
                        next_renewal_source = "invoice_paid_at_rollforward"
        if next_renewal_unix <= 0 and trial_end > 0 and status == "trialing":
            next_renewal_unix = trial_end
            next_renewal_source = "trial_end"
        if next_renewal_unix <= 0 and not cancel_at_period_end:
            anchor = billing_cycle_anchor or current_period_start or paid_at_unix or start_date or created_unix
            if anchor > 0:
                rolled = _next_renewal_from_anchor(anchor, interval_months)
                if rolled > 0:
                    next_renewal_unix = rolled
                    next_renewal_source = "anchor_rollforward"

        out["ok"] = True
        out["plan"] = str(STRIPE_PRICE_TO_PLAN.get(active_price_id, "") or "").strip().lower()
        out["status"] = status
        out["cancel_at_period_end"] = cancel_at_period_end
        out["paid_at_unix"] = int(paid_at_unix or 0)
        out["next_renewal_unix"] = int(next_renewal_unix or 0)
        out["next_renewal_source"] = next_renewal_source
        out["interval_months"] = max(1, int(interval_months or 1))
        return out
    except Exception as e:
        log.warning(f"Stripe subscription snapshot failed for {email}: {e}")
        return out


async def _supabase_find_user_id_by_email(email: str) -> str:
    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not svc_key or not SUPABASE_URL or not email:
        return ""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            users_resp = await client.get(
                f"{SUPABASE_URL}/auth/v1/admin/users?per_page=500",
                headers={
                    "apikey": svc_key,
                    "Authorization": f"Bearer {svc_key}",
                },
            )
            if users_resp.status_code != 200:
                return ""
            users_data = users_resp.json()
            user_list = users_data.get("users", users_data) if isinstance(users_data, dict) else users_data
            for u in user_list or []:
                if str(u.get("email", "")).strip().lower() == email.strip().lower():
                    return str(u.get("id", ""))
    except Exception as e:
        log.warning(f"Supabase user lookup failed for {email}: {e}")
    return ""


async def _supabase_set_user_plan(user_id: str, plan: str):
    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not svc_key or not SUPABASE_URL or not user_id:
        return
    async with httpx.AsyncClient(timeout=15) as client:
        await client.post(
            f"{SUPABASE_URL}/rest/v1/profiles",
            headers={
                "apikey": svc_key,
                "Authorization": f"Bearer {svc_key}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates",
            },
            json={"id": user_id, "plan": plan},
        )


def _stripe_price_id_for_plan(plan: str) -> str:
    normalized = str(plan or "").strip().lower()
    for price_id, mapped_plan in STRIPE_PRICE_TO_PLAN.items():
        if str(mapped_plan or "").strip().lower() == normalized:
            return str(price_id)
    return ""


async def _supabase_get_waitlist_rows(limit: int = 2000) -> list[dict]:
    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not svc_key or not SUPABASE_URL:
        return []
    fallback_table = "app_settings"
    fallback_prefix = "studio_waitlist_reservation:"

    def _waitlist_missing(resp: httpx.Response) -> bool:
        if resp.status_code == 404:
            return True
        try:
            body = resp.json()
        except Exception:
            body = {}
        text = json.dumps(body).lower() if isinstance(body, (dict, list)) else str(body).lower()
        return "could not find the table" in text and "waiting_list" in text

    def _parse_fallback_value(raw: object) -> dict:
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                return {}
        return {}

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/waiting_list?select=*&order=created_at.desc&limit={int(max(1, limit))}",
                headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
            )
            if resp.status_code == 200:
                rows = resp.json()
                return rows if isinstance(rows, list) else []

            if not _waitlist_missing(resp):
                return []

            fallback_resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/{fallback_table}?select=id,key,value,updated_at&key=like.{quote(fallback_prefix + '%')}&order=updated_at.desc&limit={int(max(1, limit))}",
                headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
            )
            if fallback_resp.status_code != 200:
                return []
            raw_rows = fallback_resp.json()
            raw_rows = raw_rows if isinstance(raw_rows, list) else []
            rows: list[dict] = []
            for item in raw_rows:
                if not isinstance(item, dict):
                    continue
                parsed = _parse_fallback_value(item.get("value"))
                key = str(item.get("key", "") or "")
                email = str(parsed.get("email", "") or "").strip().lower()
                if not email and key.startswith(fallback_prefix):
                    email = key[len(fallback_prefix):].strip().lower()
                if not email:
                    continue
                rows.append(
                    {
                        "id": str(item.get("id", "") or ""),
                        "email": email,
                        "plan": str(parsed.get("plan", "starter") or "starter").strip().lower(),
                        "price_usd": float(parsed.get("price_usd", 0.0) or 0.0),
                        "paid": bool(parsed.get("paid", False)),
                        "stripe_session_id": parsed.get("stripe_session_id"),
                        "created_at": str(parsed.get("created_at", item.get("updated_at", "")) or ""),
                    }
                )
            rows.sort(
                key=lambda r: str(r.get("created_at", "") or ""),
                reverse=True,
            )
            return rows
    except Exception as e:
        log.warning(f"Supabase waiting_list read failed: {e}")
        return []


async def _supabase_upsert_waitlist_entry(
    *,
    email: str,
    plan: str,
    price_usd: float,
    paid: bool,
) -> bool:
    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not svc_key or not SUPABASE_URL or not email:
        return False
    payload = {
        "email": str(email or "").strip().lower(),
        "plan": str(plan or "").strip().lower(),
        "price_usd": float(price_usd or 0.0),
        "paid": bool(paid),
    }
    fallback_table = "app_settings"
    fallback_prefix = "studio_waitlist_reservation:"

    def _waitlist_missing(resp: httpx.Response) -> bool:
        if resp.status_code == 404:
            return True
        try:
            body = resp.json()
        except Exception:
            body = {}
        text = json.dumps(body).lower() if isinstance(body, (dict, list)) else str(body).lower()
        return "could not find the table" in text and "waiting_list" in text

    def _parse_fallback_value(raw: object) -> dict:
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                return {}
        return {}

    async def _fallback_upsert(client: httpx.AsyncClient) -> bool:
        key = f"{fallback_prefix}{payload['email']}"
        existing = await client.get(
            f"{SUPABASE_URL}/rest/v1/{fallback_table}?key=eq.{quote(key)}&select=id,value&limit=1",
            headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
        )
        existing_rows = existing.json() if existing.status_code == 200 else []
        existing_rows = existing_rows if isinstance(existing_rows, list) else []
        value = {
            "email": payload["email"],
            "plan": payload["plan"],
            "price_usd": payload["price_usd"],
            "paid": payload["paid"],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        if existing_rows:
            prev = _parse_fallback_value(existing_rows[0].get("value"))
            value["created_at"] = str(prev.get("created_at", value["created_at"]) or value["created_at"])
            value["paid"] = bool(prev.get("paid")) or value["paid"]
            update = await client.patch(
                f"{SUPABASE_URL}/rest/v1/{fallback_table}?id=eq.{quote(str(existing_rows[0].get('id', '') or ''))}",
                headers={
                    "apikey": svc_key,
                    "Authorization": f"Bearer {svc_key}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal",
                },
                json={"value": value},
            )
            return update.status_code in {200, 204}
        insert = await client.post(
            f"{SUPABASE_URL}/rest/v1/{fallback_table}",
            headers={
                "apikey": svc_key,
                "Authorization": f"Bearer {svc_key}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal",
            },
            json={"key": key, "value": value},
        )
        return insert.status_code in {200, 201, 204}

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            existing_resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/waiting_list?email=eq.{quote(payload['email'])}&select=id,paid&limit=1",
                headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
            )
            if _waitlist_missing(existing_resp):
                return await _fallback_upsert(client)
            existing_rows = existing_resp.json() if existing_resp.status_code == 200 else []
            existing_rows = existing_rows if isinstance(existing_rows, list) else []
            if existing_rows:
                existing_paid = bool(existing_rows[0].get("paid"))
                update_payload = dict(payload)
                if existing_paid:
                    update_payload["paid"] = True
                update_resp = await client.patch(
                    f"{SUPABASE_URL}/rest/v1/waiting_list?email=eq.{quote(payload['email'])}",
                    headers={
                        "apikey": svc_key,
                        "Authorization": f"Bearer {svc_key}",
                        "Content-Type": "application/json",
                        "Prefer": "return=minimal",
                    },
                    json=update_payload,
                )
                if _waitlist_missing(update_resp):
                    return await _fallback_upsert(client)
                return update_resp.status_code in {200, 204}
            insert_resp = await client.post(
                f"{SUPABASE_URL}/rest/v1/waiting_list",
                headers={
                    "apikey": svc_key,
                    "Authorization": f"Bearer {svc_key}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal",
                },
                json=payload,
            )
            if _waitlist_missing(insert_resp):
                return await _fallback_upsert(client)
            return insert_resp.status_code in {200, 201, 204}
    except Exception as e:
        log.warning(f"Supabase waiting_list upsert failed for {email}: {e}")
        return False


def _price_id_for_plan_id(plan_id: str) -> str:
    normalized = str(plan_id or "").strip().lower()
    for price_id, mapped_plan in STRIPE_PRICE_TO_PLAN.items():
        if str(mapped_plan or "").strip().lower() == normalized:
            return str(price_id or "").strip()
    return ""


@app.post("/api/checkout")
async def create_checkout(req: CheckoutRequest, user: dict = Depends(require_auth)):
    requested_product = str(getattr(req, "product", "") or "").strip().lower()
    requested_plan = str(getattr(req, "plan", "") or "").strip().lower()
    price_id = str(req.price_id or "").strip()
    if requested_product == "membership" and not requested_plan and not price_id:
        requested_plan = _default_membership_plan_id()
    if requested_plan and not price_id:
        price_id = _price_id_for_plan_id(requested_plan)
    if not price_id:
        raise HTTPException(400, "Missing price id")
    plan = str(STRIPE_PRICE_TO_PLAN.get(price_id, requested_plan) or "").strip().lower()
    if plan not in CHAT_STORY_ALLOWED_PLANS:
        raise HTTPException(400, "This membership plan is not available for PayPal checkout.")
    price_usd = float(PLAN_PRICE_USD.get(plan, 0.0) or 0.0)
    if price_usd <= 0:
        raise HTTPException(400, f"Membership pricing is not configured for {plan}.")
    checkout_url = await _create_paypal_subscription_order(user, price_id, plan, price_usd)
    return {"checkout_url": checkout_url}


async def _capture_paypal_order_api(order_id: str) -> tuple[dict, str]:
    capture = await _paypal_request("POST", f"/v2/checkout/orders/{order_id}/capture", json_body={})
    status = str(capture.get("status", "") or "").upper()
    capture_id = ""
    for purchase_unit in list(capture.get("purchase_units", []) or []):
        payments = purchase_unit.get("payments", {}) or {}
        captures = list(payments.get("captures", []) or [])
        if captures:
            capture_id = str(captures[0].get("id", "") or "")
            break
    if status not in {"COMPLETED", "APPROVED"} and not capture_id:
        raise HTTPException(400, "PayPal payment was not completed")
    return capture, capture_id


async def _create_paypal_subscription_order(user: dict, price_id: str, plan: str, price_usd: float) -> str:
    if not _paypal_enabled():
        raise HTTPException(400, "PayPal is not configured on this billing account yet.")
    normalized_plan = str(plan or "").strip().lower()
    if normalized_plan not in CHAT_STORY_ALLOWED_PLANS:
        raise HTTPException(400, "Only Starter, Creator, and Pro legacy plan IDs can be sold through membership checkout.")
    order_payload = {
        "intent": "CAPTURE",
        "purchase_units": [
            {
                "reference_id": str(price_id or ""),
                "custom_id": str(user.get("id", "") or ""),
                "description": f"NYPTID Studio Catalyst Membership ({normalized_plan.title()})",
                "amount": {
                    "currency_code": "USD",
                    "value": f"{float(price_usd or 0.0):.2f}",
                },
            }
        ],
        "application_context": {
            "brand_name": "NYPTID Studio",
            "landing_page": "LOGIN",
            "user_action": "PAY_NOW",
            "shipping_preference": "NO_SHIPPING",
            "return_url": f"{_api_public_url()}/api/paypal/return",
            "cancel_url": f"{_billing_site_url()}?page=subscription&subscription=cancelled&provider=paypal",
        },
    }
    data = await _paypal_request("POST", "/v2/checkout/orders", json_body=order_payload)
    order_id = str(data.get("id", "") or "")
    approve_url = ""
    for link in list(data.get("links", []) or []):
        if str(link.get("rel", "") or "").lower() == "approve":
            approve_url = str(link.get("href", "") or "")
            break
    if not order_id or not approve_url:
        raise HTTPException(500, "PayPal approval URL missing")
    async with _paypal_orders_lock:
        _paypal_orders[order_id] = {
            "kind": "subscription",
            "user_id": str(user.get("id", "") or ""),
            "email": str(user.get("email", "") or "").strip().lower(),
            "price_id": str(price_id or ""),
            "plan": normalized_plan,
            "price_usd": float(price_usd or 0.0),
            "activated": False,
            "capture_id": "",
            "created_at": time.time(),
        }
        _save_paypal_orders()
    return approve_url


async def _create_paypal_topup_order(user: dict, price_id: str, pack: dict) -> str:
    if not _paypal_enabled():
        raise HTTPException(400, "PayPal is not configured on this billing account yet.")
    order_payload = {
        "intent": "CAPTURE",
        "purchase_units": [
            {
                "reference_id": str(price_id or ""),
                "custom_id": str(user.get("id", "") or ""),
                "description": f"NYPTID Studio {str(pack.get('pack', '') or '').title()} AC Credits",
                "amount": {
                    "currency_code": "USD",
                    "value": f"{float(pack.get('price_usd', 0.0) or 0.0):.2f}",
                },
            }
        ],
        "application_context": {
            "brand_name": "NYPTID Studio",
            "landing_page": "LOGIN",
            "user_action": "PAY_NOW",
            "shipping_preference": "NO_SHIPPING",
            "return_url": f"{_api_public_url()}/api/paypal/return",
            "cancel_url": f"{_billing_site_url()}?topup=cancelled&provider=paypal",
        },
    }
    data = await _paypal_request("POST", "/v2/checkout/orders", json_body=order_payload)
    order_id = str(data.get("id", "") or "")
    approve_url = ""
    for link in list(data.get("links", []) or []):
        if str(link.get("rel", "") or "").lower() == "approve":
            approve_url = str(link.get("href", "") or "")
            break
    if not order_id or not approve_url:
        raise HTTPException(500, "PayPal approval URL missing")
    async with _paypal_orders_lock:
        _paypal_orders[order_id] = {
            "kind": "topup",
            "user_id": str(user.get("id", "") or ""),
            "email": str(user.get("email", "") or ""),
            "price_id": str(price_id or ""),
            "pack": str(pack.get("pack", "") or ""),
            "credits": int(pack.get("credits", 0) or 0),
            "price_usd": float(pack.get("price_usd", 0.0) or 0.0),
            "credited": False,
            "capture_id": "",
            "created_at": time.time(),
        }
        _save_paypal_orders()
    return approve_url


async def _capture_paypal_topup_order(order_id: str) -> dict:
    order_id = str(order_id or "").strip()
    if not order_id:
        raise HTTPException(400, "Missing PayPal order id")
    async with _paypal_orders_lock:
        order_meta = dict(_paypal_orders.get(order_id, {}) or {})
    if not order_meta:
        raise HTTPException(404, "PayPal order was not found")
    if order_meta.get("credited"):
        return order_meta
    _, capture_id = await _capture_paypal_order_api(order_id)
    async with _paypal_orders_lock:
        latest = dict(_paypal_orders.get(order_id, {}) or {})
        if latest.get("credited"):
            return latest
        await _credit_topup_wallet(
            user_id=str(latest.get("user_id", "") or ""),
            credits=int(latest.get("credits", 0) or 0),
            source=str(latest.get("pack", "paypal") or "paypal"),
            stripe_session_id=order_id,
        )
        latest["credited"] = True
        latest["capture_id"] = capture_id
        latest["captured_at"] = time.time()
        _paypal_orders[order_id] = latest
        _save_paypal_orders()
    await _append_landing_notification(
        event_type="topup",
            credits=int(latest.get("credits", 0) or 0),
            customer_email=str(latest.get("email", "") or ""),
    )
    return latest


async def _capture_paypal_subscription_order(order_id: str) -> dict:
    order_id = str(order_id or "").strip()
    if not order_id:
        raise HTTPException(400, "Missing PayPal order id")
    async with _paypal_orders_lock:
        order_meta = dict(_paypal_orders.get(order_id, {}) or {})
    if not order_meta:
        raise HTTPException(404, "PayPal order was not found")
    if str(order_meta.get("kind", "") or "").strip().lower() not in {"subscription", "monthly"}:
        raise HTTPException(400, "PayPal order is not a monthly subscription checkout")
    if order_meta.get("activated"):
        return order_meta
    _, capture_id = await _capture_paypal_order_api(order_id)
    user_id = str(order_meta.get("user_id", "") or "").strip()
    email = str(order_meta.get("email", "") or "").strip().lower()
    plan = str(order_meta.get("plan", "none") or "none").strip().lower()
    if plan not in CHAT_STORY_ALLOWED_PLANS:
        raise HTTPException(400, "PayPal subscription order is missing a valid plan")
    now_unix = int(time.time())
    current_user = {"id": user_id, "email": email, "plan": plan}
    current_record = _paypal_subscription_record_for_user(current_user)
    current_snapshot = _paypal_subscription_snapshot_for_user(current_user)
    period_start_unix = now_unix
    if current_snapshot.get("billing_active") and str(current_snapshot.get("record_plan", "none") or "none") == plan:
        active_end = int((current_record or {}).get("period_end_unix", 0) or 0)
        if active_end > now_unix:
            period_start_unix = active_end
    period_end_unix = _add_months_utc(period_start_unix, 1)
    if period_end_unix <= period_start_unix:
        period_end_unix = period_start_unix + (31 * 24 * 3600)
    subscription_key_candidates = _paypal_subscription_lookup_keys(user_id, email)
    subscription_key = subscription_key_candidates[0] if subscription_key_candidates else order_id
    subscription_record = {
        "provider": "paypal_manual",
        "user_id": user_id,
        "email": email,
        "plan": plan,
        "price_id": str(order_meta.get("price_id", "") or ""),
        "price_usd": float(order_meta.get("price_usd", 0.0) or 0.0),
        "order_id": order_id,
        "capture_id": capture_id,
        "status": "active",
        "period_start_unix": int(period_start_unix),
        "period_end_unix": int(period_end_unix),
        "created_at": float((current_record or {}).get("created_at", 0) or order_meta.get("created_at", time.time()) or time.time()),
        "updated_at": time.time(),
        "renewal_mode": "manual_paypal",
    }
    async with _paypal_subscriptions_lock:
        _paypal_subscriptions[subscription_key] = subscription_record
        _save_paypal_subscriptions()
    async with _paypal_orders_lock:
        latest = dict(_paypal_orders.get(order_id, {}) or {})
        if latest.get("activated"):
            return latest
        latest["activated"] = True
        latest["capture_id"] = capture_id
        latest["captured_at"] = time.time()
        latest["period_start_unix"] = int(period_start_unix)
        latest["period_end_unix"] = int(period_end_unix)
        latest["paypal_subscription_key"] = subscription_key
        _paypal_orders[order_id] = latest
        _save_paypal_orders()
    if user_id:
        await _supabase_set_user_plan(user_id, plan)
    await _append_landing_notification(
        event_type="subscription",
        plan=plan,
        customer_email=email,
    )
    return latest


@app.post("/api/checkout/topup")
async def create_topup_checkout(req: TopupCheckoutRequest, user: dict = Depends(require_auth)):
    pack = TOPUP_PACKS.get(req.price_id)
    if not pack:
        raise HTTPException(400, "Invalid top-up pack")
    if user.get("email", "") in ADMIN_EMAILS:
        raise HTTPException(400, "Admin account does not require top-up packs")
    preferred_method = str(getattr(req, "preferred_method", "paypal") or "paypal").strip().lower()
    if preferred_method not in {"card", "paypal"}:
        preferred_method = "paypal"
    if preferred_method == "card" and not STRIPE_TOPUP_PUBLIC_ENABLED:
        raise HTTPException(400, "Stripe checkout is coming soon. Use PayPal for now.")
    if preferred_method == "paypal":
        checkout_url = await _create_paypal_topup_order(user, req.price_id, pack)
        return {"checkout_url": checkout_url}
    if not STRIPE_SECRET_KEY:
        raise HTTPException(500, "Stripe not configured")
    try:
        checkout_price_id = str(pack.get("stripe_price_id", "") or "").strip()
        line_item = (
            {"price": checkout_price_id, "quantity": 1}
            if checkout_price_id
            else {
                "price_data": {
                    "currency": "usd",
                    "product_data": {
                        "name": f"NYPTID Studio {str(pack.get('pack', '') or '').title()} AC Credits",
                        "description": f"{int(pack.get('credits', 0) or 0)} animation credits",
                    },
                    "unit_amount": int(round(float(pack.get("price_usd", 0.0) or 0.0) * 100)),
                },
                "quantity": 1,
            }
        )
        checkout_payload = {
            "mode": "payment",
            "line_items": [line_item],
            "success_url": f"{_billing_site_url()}?topup=success",
            "cancel_url": f"{_billing_site_url()}?topup=cancelled",
            "client_reference_id": user["id"],
            "metadata": {
                "user_id": user["id"],
                "topup_price_id": req.price_id,
                "topup_pack": str(pack.get("pack", "")),
                "topup_credits": str(int(pack.get("credits", 0) or 0)),
            },
        }
        customer_id = _stripe_find_customer_id_by_email(user["email"])
        if customer_id:
            checkout_payload["customer"] = customer_id
        else:
            checkout_payload["customer_email"] = user["email"]
        session = stripe_lib.checkout.Session.create(
            **checkout_payload,
            payment_method_types=["card"],
        )
        return {"checkout_url": session.url}
    except Exception as e:
        log.error(f"Stripe top-up checkout error: {e}")
        raise HTTPException(500, f"Payment error: {str(e)}")


@app.get("/api/paypal/return")
async def paypal_return(token: str = "", PayerID: str = ""):
    order_id = str(token or "").strip()
    async with _paypal_orders_lock:
        order_meta = dict(_paypal_orders.get(order_id, {}) or {})
    order_kind = str(order_meta.get("kind", "topup") or "topup").strip().lower()
    success_url = f"{_billing_site_url()}?topup=success&provider=paypal"
    error_url = f"{_billing_site_url()}?topup=cancelled&provider=paypal"
    if order_kind in {"subscription", "monthly"}:
        plan = str(order_meta.get("plan", "") or "").strip().lower()
        plan_suffix = f"&plan={quote(plan)}" if plan else ""
        success_url = f"{_billing_site_url()}?page=subscription&subscription=success&provider=paypal{plan_suffix}"
        error_url = f"{_billing_site_url()}?page=subscription&subscription=cancelled&provider=paypal{plan_suffix}"
    try:
        if order_kind in {"subscription", "monthly"}:
            await _capture_paypal_subscription_order(order_id)
        else:
            await _capture_paypal_topup_order(order_id)
        return RedirectResponse(url=success_url, status_code=302)
    except Exception as e:
        log.error(f"PayPal return/capture failed for {order_id} payer={PayerID}: {e}")
        message = quote(str(getattr(e, "detail", str(e)) or "PayPal capture failed")[:180])
        separator = "&" if "?" in error_url else "?"
        return RedirectResponse(url=f"{error_url}{separator}error={message}", status_code=302)


@app.post("/api/billing-portal")
async def create_billing_portal_session(user: dict = Depends(require_auth)):
    """Create a Stripe customer portal session so users can cancel/manage plans."""
    access_snapshot = _paid_access_snapshot_for_user(user)
    billing_active = bool(access_snapshot.get("billing_active"))
    access_source = str(access_snapshot.get("source", "") or "").strip().lower()
    if billing_active and access_source != "stripe":
        plan = str(access_snapshot.get("plan", "") or "").strip().lower()
        plan_suffix = f"&plan={quote(plan)}" if plan else ""
        provider_suffix = f"&provider={quote(access_source)}" if access_source else ""
        subscription_state = "manual" if access_source == "paypal_manual" else "details"
        return {
            "portal_url": (
                f"{_billing_site_url()}?page=subscription&subscription={subscription_state}"
                f"{provider_suffix}{plan_suffix}"
            )
        }
    if not STRIPE_SECRET_KEY:
        raise HTTPException(500, "Stripe not configured")
    customer_id = _stripe_find_customer_id_by_email(user.get("email", ""))
    if not customer_id:
        raise HTTPException(404, "No billing profile found yet. Complete a credit-pack purchase first.")
    try:
        portal = stripe_lib.billing_portal.Session.create(
            customer=customer_id,
            return_url=f"{_billing_site_url()}?billing=updated",
        )
        return {"portal_url": portal.url}
    except Exception as e:
        log.error(f"Stripe billing portal error: {e}")
        raise HTTPException(500, "Could not open billing portal")


@app.post("/api/waitlist/join")
async def join_waitlist(req: WaitlistJoinRequest, user: dict = Depends(require_auth)):
    raise HTTPException(410, "Waiting list has been removed from Studio.")


@app.post("/api/stripe-webhook")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    try:
        if STRIPE_WEBHOOK_SECRET:
            event = stripe_lib.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
        else:
            event = json.loads(payload)
    except Exception as e:
        log.error(f"Webhook signature verification failed: {e}")
        raise HTTPException(400, "Invalid signature")

    if event.get("type") == "checkout.session.completed":
        session_data = event["data"]["object"]
        metadata = session_data.get("metadata", {}) or {}
        mode = str(session_data.get("mode", "") or "")
        user_id = str(session_data.get("client_reference_id") or metadata.get("user_id") or "")
        customer_email = str(
            (session_data.get("customer_details", {}) or {}).get("email")
            or session_data.get("customer_email")
            or ""
        )
        if mode == "subscription":
            plan = metadata.get("plan", "starter")
            if user_id and SUPABASE_URL:
                svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
                try:
                    async with httpx.AsyncClient(timeout=15) as client:
                        await client.post(
                            f"{SUPABASE_URL}/rest/v1/profiles",
                            headers={
                                "apikey": svc_key,
                                "Authorization": f"Bearer {svc_key}",
                                "Content-Type": "application/json",
                                "Prefer": "resolution=merge-duplicates",
                            },
                            json={"id": user_id, "plan": plan},
                        )
                    log.info(f"Stripe webhook: user {user_id} upgraded to {plan}")
                except Exception as e:
                    log.error(f"Failed to update plan for {user_id}: {e}")
            await _append_landing_notification(
                event_type="subscription",
                plan=str(plan or "starter"),
                customer_email=customer_email,
            )
        elif mode == "payment":
            checkout_kind = str(metadata.get("checkout_kind", "") or "")
            if checkout_kind == "waitlist_reservation":
                plan = str(metadata.get("plan", "") or "").strip().lower()
                plan_price_usd = float(metadata.get("plan_price_usd", PLAN_PRICE_USD.get(plan, 0.0)) or 0.0)
                if customer_email and plan in {"starter", "creator", "pro", "elite"}:
                    await _supabase_upsert_waitlist_entry(
                        email=customer_email,
                        plan=plan,
                        price_usd=plan_price_usd,
                        paid=True,
                    )
                    await _append_landing_notification(
                        event_type="subscription",
                        plan=plan,
                        customer_email=customer_email,
                    )
                    log.info(f"Stripe webhook: confirmed waitlist reservation {customer_email} -> {plan}")
                return {"status": "ok"}
            topup_credits = int(str(metadata.get("topup_credits", "0") or "0"))
            if user_id and topup_credits > 0:
                await _credit_topup_wallet(
                    user_id=user_id,
                    credits=topup_credits,
                    source=str(metadata.get("topup_pack", "topup") or "topup"),
                    stripe_session_id=str(session_data.get("id", "") or ""),
                )
                log.info(f"Stripe webhook: credited {topup_credits} top-up credits to {user_id}")
                await _append_landing_notification(
                    event_type="topup",
                    credits=topup_credits,
                    customer_email=customer_email,
                )

    elif event.get("type") in {"customer.subscription.created", "customer.subscription.updated", "customer.subscription.deleted"}:
        sub = event["data"]["object"]
        customer_email = str(sub.get("customer_email", "") or "")
        customer_id = str(sub.get("customer", "") or "")
        if not customer_email and customer_id and STRIPE_SECRET_KEY:
            try:
                customer = stripe_lib.Customer.retrieve(customer_id)
                customer_email = str(getattr(customer, "email", "") or "")
            except Exception as e:
                log.warning(f"Failed to resolve Stripe customer email from {customer_id}: {e}")
        if not customer_email:
            log.warning("Subscription lifecycle event received without resolvable customer email")
            return {"status": "ok"}

        event_type = str(event.get("type", "") or "")
        status = str(sub.get("status", "") or "")
        items = (((sub.get("items", {}) or {}).get("data", []) or []))
        active_price_id = ""
        if items:
            active_price_id = str((((items[0] or {}).get("price", {}) or {}).get("id", "") or ""))
        mapped_plan = STRIPE_PRICE_TO_PLAN.get(active_price_id, "")

        try:
            target_id = await _supabase_find_user_id_by_email(customer_email)
            if not target_id:
                log.warning(f"Subscription lifecycle event but no Supabase user found for {customer_email}")
                return {"status": "ok"}
            if event_type == "customer.subscription.deleted":
                await _supabase_set_user_plan(target_id, "none")
                log.info(f"Subscription deleted; downgraded {customer_email} -> none")
            elif status in {"active", "trialing", "past_due"} and mapped_plan:
                await _supabase_set_user_plan(target_id, mapped_plan)
                log.info(f"Subscription lifecycle sync: {customer_email} -> {mapped_plan} ({status})")
            elif status in {"canceled", "unpaid", "incomplete_expired"}:
                await _supabase_set_user_plan(target_id, "none")
                log.info(f"Subscription became inactive; downgraded {customer_email} -> none")
        except Exception as e:
            log.error(f"Failed subscription lifecycle sync for {customer_email}: {e}")

    elif event.get("type") in {"invoice.payment_failed", "invoice.payment_succeeded"}:
        inv = event["data"]["object"]
        customer_email = str(inv.get("customer_email", "") or "")
        customer_id = str(inv.get("customer", "") or "")
        if not customer_email and customer_id and STRIPE_SECRET_KEY:
            try:
                customer = stripe_lib.Customer.retrieve(customer_id)
                customer_email = str(getattr(customer, "email", "") or "")
            except Exception as e:
                log.warning(f"Failed to resolve invoice customer email from {customer_id}: {e}")
        if not customer_email:
            return {"status": "ok"}
        target_id = await _supabase_find_user_id_by_email(customer_email)
        if not target_id:
            return {"status": "ok"}
        if event.get("type") == "invoice.payment_failed":
            await _supabase_set_user_plan(target_id, "none")
            log.info(f"Invoice payment failed; downgraded {customer_email} -> none")
        else:
            # Keep profile + Stripe lifecycle in sync on successful recurring renewals.
            lines = (((inv.get("lines", {}) or {}).get("data", []) or []))
            price_id = ""
            if lines:
                price_id = str((((lines[0] or {}).get("price", {}) or {}).get("id", "") or ""))
            plan = STRIPE_PRICE_TO_PLAN.get(price_id, "")
            if plan:
                await _supabase_set_user_plan(target_id, plan)
                log.info(f"Invoice payment succeeded; ensured {customer_email} -> {plan}")

    return {"status": "ok"}


# ─── Admin: set plan for a user (admin-only) ─────────────────────────────────

@app.post("/api/admin/set-plan")
async def admin_set_plan(req: SetPlanRequest, user: dict = Depends(require_auth)):
    if user.get("email") not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin access required")
    if req.plan not in list(PLAN_LIMITS.keys()) + ["admin", "none"]:
        raise HTTPException(400, f"Invalid plan. Options: {list(PLAN_LIMITS.keys())}")
    if req.plan == "admin" and req.email not in ADMIN_EMAILS:
        raise HTTPException(400, "Only whitelisted admin emails can be admin.")

    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not svc_key or not SUPABASE_URL:
        raise HTTPException(500, "Supabase not configured")

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            users_resp = await client.get(
                f"{SUPABASE_URL}/auth/v1/admin/users?per_page=500",
                headers={
                    "apikey": svc_key,
                    "Authorization": f"Bearer {svc_key}",
                },
            )
            target_id = None
            if users_resp.status_code == 200:
                users_data = users_resp.json()
                user_list = users_data.get("users", users_data) if isinstance(users_data, dict) else users_data
                for u in user_list:
                    if u.get("email") == req.email:
                        target_id = u["id"]
                        break

            if not target_id:
                return {"error": f"User {req.email} not found in Supabase auth"}

            await client.post(
                f"{SUPABASE_URL}/rest/v1/profiles",
                headers={
                    "apikey": svc_key,
                    "Authorization": f"Bearer {svc_key}",
                    "Content-Type": "application/json",
                    "Prefer": "resolution=merge-duplicates",
                },
                json={"id": target_id, "plan": req.plan, "role": "admin" if req.email in ADMIN_EMAILS else "user"},
            )
        return {"status": "ok", "email": req.email, "plan": req.plan}
    except Exception as e:
        log.error(f"Admin set-plan error: {e}")
        raise HTTPException(500, str(e))


@app.post("/api/admin/cancel-subscription")
async def admin_cancel_subscription(body: dict, user: dict = Depends(require_auth)):
    if user.get("email") not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin access required")
    if not STRIPE_SECRET_KEY:
        raise HTTPException(500, "Stripe not configured")

    target_email = str((body or {}).get("email", "") or "").strip().lower()
    if not target_email:
        raise HTTPException(400, "Missing email")
    cancel_now = _bool_from_any((body or {}).get("cancel_now"), False)

    try:
        customer_id = _stripe_find_customer_id_by_email(target_email)
        if not customer_id:
            raise HTTPException(404, f"No Stripe customer found for {target_email}")

        subs = stripe_lib.Subscription.list(customer=customer_id, status="all", limit=50)
        data = list(getattr(subs, "data", []) or [])
        canceled: list[dict] = []
        for sub in data:
            sub_id = str(getattr(sub, "id", "") or "")
            sub_status = str(getattr(sub, "status", "") or "")
            if not sub_id:
                continue
            if sub_status not in {"active", "trialing", "past_due", "incomplete"}:
                continue
            if cancel_now:
                stripe_lib.Subscription.delete(sub_id)
                action = "canceled_now"
            else:
                stripe_lib.Subscription.modify(sub_id, cancel_at_period_end=True)
                action = "cancel_at_period_end"
            canceled.append({"id": sub_id, "status": sub_status, "action": action})

        return {
            "ok": True,
            "email": target_email,
            "cancel_now": cancel_now,
            "canceled_count": len(canceled),
            "subscriptions": canceled,
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Admin cancel-subscription error for {target_email}: {e}")
        raise HTTPException(500, f"Failed to cancel subscription(s): {e}")


# ─── User Feedback Collection ─────────────────────────────────────────────────

@app.post("/api/feedback")
async def submit_feedback(req: FeedbackRequest, user: dict = Depends(require_auth)):
    if req.rating < 1 or req.rating > 5:
        raise HTTPException(400, "Rating must be 1-5")

    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not SUPABASE_URL or not svc_key:
        raise HTTPException(500, "Feedback storage not configured")

    feedback_data = {
        "user_id": user["id"],
        "email": user.get("email", ""),
        "job_id": req.job_id,
        "rating": req.rating,
        "comment": req.comment[:2000] if req.comment else "",
        "template": req.template,
        "language": req.language,
        "feature": req.feature or "general",
        "plan": user.get("plan", "starter"),
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                f"{SUPABASE_URL}/rest/v1/feedback",
                headers={
                    "apikey": svc_key,
                    "Authorization": f"Bearer {svc_key}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal",
                },
                json=feedback_data,
            )
            if resp.status_code not in (200, 201, 204):
                log.warning(f"Feedback insert failed ({resp.status_code}): {resp.text[:200]}")
                raise HTTPException(500, "Failed to save feedback")
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Feedback submit error: {e}")
        raise HTTPException(500, "Failed to save feedback")

    log.info(f"Feedback received: {req.rating}/5 from {user.get('email', '?')} for {req.feature} (job: {req.job_id[:20]})")
    return {"status": "ok"}


@app.get("/api/admin/feedback")
async def get_all_feedback(user: dict = Depends(require_auth)):
    if user.get("email") not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin access required")

    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not SUPABASE_URL or not svc_key:
        return {"feedback": []}

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/feedback?order=created_at.desc&limit=500",
                headers={
                    "apikey": svc_key,
                    "Authorization": f"Bearer {svc_key}",
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                avg_rating = sum(f.get("rating", 0) for f in data) / len(data) if data else 0
                return {
                    "feedback": data,
                    "total": len(data),
                    "avg_rating": round(avg_rating, 2),
                }
    except Exception as e:
        log.error(f"Feedback fetch error: {e}")

    return {"feedback": [], "total": 0, "avg_rating": 0}


@app.get("/api/admin/kpi")
async def get_admin_kpi(user: dict = Depends(require_auth)):
    if user.get("email") not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin access required")
    total_jobs = int(_kpi_metrics.get("total_jobs", 0))
    completed_jobs = int(_kpi_metrics.get("completed_jobs", 0))
    first_pass = int(_kpi_metrics.get("first_render_pass_jobs", 0))
    total_time = float(_kpi_metrics.get("total_publishable_time_sec", 0.0))
    total_cost = float(_kpi_metrics.get("total_estimated_cost_usd", 0.0))
    success_rate = (completed_jobs / total_jobs) if total_jobs else 0.0
    first_render_success_rate = (first_pass / total_jobs) if total_jobs else 0.0
    avg_time_to_publishable_sec = (total_time / completed_jobs) if completed_jobs else 0.0
    avg_estimated_cost_per_short = (total_cost / max(total_jobs, 1)) if total_jobs else 0.0
    return {
        "targets": KPI_TARGETS,
        "metrics": {
            **_kpi_metrics,
            "success_rate": round(success_rate, 4),
            "first_render_success_rate": round(first_render_success_rate, 4),
            "avg_time_to_publishable_sec": round(avg_time_to_publishable_sec, 2),
            "avg_estimated_cost_per_short_usd": round(avg_estimated_cost_per_short, 3),
        },
        "on_track": {
            "first_render_success_rate": first_render_success_rate >= float(KPI_TARGETS["first_render_success_rate"]),
            "time_to_publishable_sec": avg_time_to_publishable_sec <= float(KPI_TARGETS["time_to_publishable_sec"]),
            "estimated_cost_per_short_usd": avg_estimated_cost_per_short <= float(KPI_TARGETS["estimated_cost_per_short_usd"]),
        },
    }


# ─── Startup: seed accounts ──────────────────────────────────────────────────

SEED_ACCOUNTS = {
    "omatic657@gmail.com": {"plan": "admin", "role": "admin"},
}


@app.on_event("startup")
async def seed_profiles():
    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not SUPABASE_URL or not svc_key:
        log.warning("Supabase not configured, skipping profile seeding")
        return

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            users_resp = await client.get(
                f"{SUPABASE_URL}/auth/v1/admin/users?per_page=500",
                headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
            )
            if users_resp.status_code != 200:
                log.warning(f"Could not list users for seeding (status {users_resp.status_code})")
                return

            users_data = users_resp.json()
            user_list = users_data.get("users", users_data) if isinstance(users_data, dict) else users_data

            for email, profile in SEED_ACCOUNTS.items():
                user_id = None
                for u in user_list:
                    if u.get("email") == email:
                        user_id = u["id"]
                        break
                if not user_id:
                    log.info(f"Seed user {email} not found in auth yet (will be seeded on first login)")
                    continue

                resp = await client.post(
                    f"{SUPABASE_URL}/rest/v1/profiles",
                    headers={
                        "apikey": svc_key,
                        "Authorization": f"Bearer {svc_key}",
                        "Content-Type": "application/json",
                        "Prefer": "resolution=merge-duplicates",
                    },
                    json={"id": user_id, "plan": profile["plan"], "role": profile["role"]},
                )
                log.info(f"Seeded {email} -> {profile['plan']} (status {resp.status_code})")
    except Exception as e:
        log.warning(f"Profile seeding failed: {e}")


# ─── Thumbnail System ─────────────────────────────────────────────────────────

THUMBNAIL_DIR.mkdir(parents=True, exist_ok=True)
THUMBNAIL_UPLOAD_DIR = THUMBNAIL_DIR / "library"
THUMBNAIL_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
THUMBNAIL_OUTPUT_DIR = THUMBNAIL_DIR / "generated"
THUMBNAIL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
THUMBNAIL_SHARE_DIR = THUMBNAIL_DIR / "shared"
THUMBNAIL_SHARE_DIR.mkdir(parents=True, exist_ok=True)
THUMBNAIL_SHARE_STATE_FILE = THUMBNAIL_DIR / "share_links.json"
THUMBNAIL_STYLE_STATE_FILE = THUMBNAIL_DIR / "pikzels_style_profiles.json"
_thumbnail_share_links: dict[str, dict] = {}
_thumbnail_share_lock = asyncio.Lock()
_thumbnail_style_profiles: dict[str, dict] = {}
_thumbnail_style_lock = asyncio.Lock()

THUMBNAIL_RUNPOD_HOST = os.getenv("THUMBNAIL_RUNPOD_HOST", "root@69.30.85.41")
THUMBNAIL_RUNPOD_SSH_PORT = os.getenv("THUMBNAIL_RUNPOD_SSH_PORT", "22118")
RUNPOD_SSH = f"ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p {THUMBNAIL_RUNPOD_SSH_PORT} {THUMBNAIL_RUNPOD_HOST}"
RUNPOD_TRAINING_DIR = "/workspace/thumbnail_training/images"
LORA_NAME = "nyptid_thumbnails.safetensors"


def _load_thumbnail_share_links() -> None:
    global _thumbnail_share_links
    try:
        if THUMBNAIL_SHARE_STATE_FILE.exists():
            data = json.loads(THUMBNAIL_SHARE_STATE_FILE.read_text(encoding="utf-8"))
            _thumbnail_share_links = data if isinstance(data, dict) else {}
        else:
            _thumbnail_share_links = {}
    except Exception:
        _thumbnail_share_links = {}


def _save_thumbnail_share_links() -> None:
    THUMBNAIL_SHARE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    THUMBNAIL_SHARE_STATE_FILE.write_text(json.dumps(_thumbnail_share_links, indent=2), encoding="utf-8")


def _load_thumbnail_style_profiles() -> None:
    global _thumbnail_style_profiles
    try:
        if THUMBNAIL_STYLE_STATE_FILE.exists():
            data = json.loads(THUMBNAIL_STYLE_STATE_FILE.read_text(encoding="utf-8"))
            _thumbnail_style_profiles = data if isinstance(data, dict) else {}
        else:
            _thumbnail_style_profiles = {}
    except Exception:
        _thumbnail_style_profiles = {}


def _save_thumbnail_style_profiles() -> None:
    THUMBNAIL_STYLE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    THUMBNAIL_STYLE_STATE_FILE.write_text(json.dumps(_thumbnail_style_profiles, indent=2), encoding="utf-8")


def _thumbnail_style_profile_key(user: Optional[dict]) -> str:
    return _storage_safe_user_segment(user)


def _thumbnail_share_url(token: str) -> str:
    return f"{_api_public_url().rstrip('/')}/api/public/thumbnail-share/{quote(str(token or '').strip())}"


async def _register_thumbnail_share(path: Path, user: Optional[dict], ttl_sec: int = 7200) -> str:
    if not path.exists():
        raise FileNotFoundError(str(path))
    token = secrets.token_urlsafe(24)
    async with _thumbnail_share_lock:
        _load_thumbnail_share_links()
        _thumbnail_share_links[token] = {
            "path": str(path),
            "user_id": str((user or {}).get("id", "") or ""),
            "expires_at": int(time.time() + max(300, int(ttl_sec or 7200))),
        }
        _save_thumbnail_share_links()
    return token


def _thumbnail_library_images(user: Optional[dict]) -> list[Path]:
    library_dir = _thumbnail_library_dir_for_user(user) if user else THUMBNAIL_UPLOAD_DIR
    files = [
        p for p in library_dir.iterdir()
        if p.is_file() and p.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}
    ] if library_dir.exists() else []
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files


async def _build_thumbnail_style_zip(user: Optional[dict]) -> tuple[Path, list[str], str]:
    files = _thumbnail_library_images(user)[:3]
    if len(files) < 1:
        raise RuntimeError("Upload at least 1 thumbnail before building a style pack.")
    safe_user = _storage_safe_user_segment(user)
    signature = "|".join(
        f"{p.name}:{int(p.stat().st_size)}:{int(p.stat().st_mtime)}"
        for p in files
    )
    zip_path = THUMBNAIL_SHARE_DIR / f"{safe_user}_style_pack.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for idx, file_path in enumerate(files, start=1):
            ext = ".jpg" if file_path.suffix.lower() not in {".png", ".jpg", ".jpeg"} else file_path.suffix.lower()
            zf.write(file_path, arcname=f"reference_{idx}{ext}")
    return zip_path, [p.name for p in files], signature


def _pikzels_error_message(payload: dict, fallback: str) -> str:
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            message = str(error.get("message", "") or "").strip()
            details = str(error.get("details", "") or "").strip()
            if message and details:
                return f"{message}: {details}"
            if message:
                return message
    return fallback


async def _pikzels_request(method: str, path: str, json_body: Optional[dict] = None, params: Optional[dict] = None) -> dict:
    if not PIKZELS_API_KEY:
        raise RuntimeError("PIKZELS_API_KEY not configured")
    async with httpx.AsyncClient(timeout=90, follow_redirects=True) as client:
        resp = await client.request(
            method.upper(),
            f"https://api.pikzels.com{path}",
            headers={
                "X-Api-Key": PIKZELS_API_KEY,
                "Content-Type": "application/json",
            },
            json=json_body,
            params=params,
        )
    try:
        payload = resp.json()
    except Exception:
        payload = {}
    if resp.status_code not in (200, 201):
        raise RuntimeError(_pikzels_error_message(payload, f"Pikzels request failed ({resp.status_code})"))
    return payload if isinstance(payload, dict) else {}


async def _refresh_thumbnail_style_profile(user: Optional[dict]) -> dict:
    key = _thumbnail_style_profile_key(user)
    async with _thumbnail_style_lock:
        _load_thumbnail_style_profiles()
        profile = dict(_thumbnail_style_profiles.get(key, {}) or {})
    style_id = str(profile.get("style_id", "") or "").strip()
    if not style_id or not PIKZELS_API_KEY:
        return profile
    try:
        data = await _pikzels_request("GET", "/v1/style", params={"id": style_id})
        normalized_status = str(data.get("status", "") or "").strip().lower()
        profile.update({
            "style_id": style_id,
            "status": normalized_status,
            "progress": int(float(data.get("progress", 0) or 0)),
            "name": str(data.get("name", profile.get("name", "")) or ""),
            "preview_url": str(data.get("temp_portrait_url", profile.get("preview_url", "")) or ""),
            "last_checked_at": int(time.time()),
        })
        async with _thumbnail_style_lock:
            _load_thumbnail_style_profiles()
            _thumbnail_style_profiles[key] = profile
            _save_thumbnail_style_profiles()
    except Exception as e:
        profile["last_error"] = str(e)
    return profile


async def _train_thumbnail_style_profile(user: Optional[dict]) -> dict:
    if not PIKZELS_API_KEY:
        raise RuntimeError("PIKZELS_API_KEY not configured")
    zip_path, selected_files, signature = await _build_thumbnail_style_zip(user)
    key = _thumbnail_style_profile_key(user)
    safe_user = re.sub(r"[^a-zA-Z0-9_-]+", "", key)[:20] or "studio"
    async with _thumbnail_style_lock:
        _load_thumbnail_style_profiles()
        existing = dict(_thumbnail_style_profiles.get(key, {}) or {})
    if str(existing.get("library_signature", "") or "") == signature and str(existing.get("style_id", "") or "").strip():
        return await _refresh_thumbnail_style_profile(user)
    share_token = await _register_thumbnail_share(zip_path, user, ttl_sec=10800)
    style_name = f"{safe_user[:20]}-{int(time.time())}"[:25]
    result = await _pikzels_request(
        "POST",
        "/v1/style",
        json_body={
            "name": style_name,
            "training_data": _thumbnail_share_url(share_token),
        },
    )
    profile = {
        "style_id": str(result.get("id", "") or ""),
        "status": str(result.get("status", "queued") or "queued").strip().lower(),
        "progress": 0,
        "name": style_name,
        "preview_url": "",
        "library_signature": signature,
        "reference_files": selected_files,
        "last_synced_at": int(time.time()),
    }
    async with _thumbnail_style_lock:
        _load_thumbnail_style_profiles()
        _thumbnail_style_profiles[key] = profile
        _save_thumbnail_style_profiles()
    return profile


def _storage_safe_user_segment(user: Optional[dict]) -> str:
    raw = str((user or {}).get("id", "") or "").strip()
    if not raw:
        raw = str((user or {}).get("email", "") or "").strip().lower()
    safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", raw).strip("_")
    return safe or "anonymous"


def _thumbnail_library_dir_for_user(user: Optional[dict]) -> Path:
    path = THUMBNAIL_UPLOAD_DIR / _storage_safe_user_segment(user)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _thumbnail_output_dir_for_user(user: Optional[dict]) -> Path:
    path = THUMBNAIL_OUTPUT_DIR / _storage_safe_user_segment(user)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _thumbnail_scp_cmd(local_path: str, remote_path: str) -> str:
    return (
        f"scp -o StrictHostKeyChecking=no -P {THUMBNAIL_RUNPOD_SSH_PORT} "
        f"\"{local_path}\" {THUMBNAIL_RUNPOD_HOST}:{remote_path}"
    )


def _thumbnail_target_user_host() -> tuple[str, str]:
    if "@" in THUMBNAIL_RUNPOD_HOST:
        user, host = THUMBNAIL_RUNPOD_HOST.split("@", 1)
        return user, host
    return "root", THUMBNAIL_RUNPOD_HOST


def _sftp_mkdir_p(sftp, remote_dir: str):
    parts = [p for p in remote_dir.strip("/").split("/") if p]
    cur = "/"
    for part in parts:
        cur = f"{cur}{part}/"
        try:
            sftp.stat(cur)
        except Exception:
            sftp.mkdir(cur)


def _sync_file_to_runpod_blocking(local_path: str, remote_path: str) -> tuple[bool, str]:
    if not Path(local_path).exists():
        return False, "local file missing"

    if paramiko is not None:
        user, host = _thumbnail_target_user_host()
        client = None
        sftp = None
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=host,
                port=int(THUMBNAIL_RUNPOD_SSH_PORT),
                username=user,
                timeout=20,
                allow_agent=True,
                look_for_keys=True,
            )
            sftp = client.open_sftp()
            remote_parent = str(Path(remote_path).parent).replace("\\", "/")
            _sftp_mkdir_p(sftp, remote_parent)
            sftp.put(local_path, remote_path)
            return True, ""
        except Exception as e:
            log.warning(f"Paramiko thumbnail sync failed, falling back to scp: {e}")
        finally:
            try:
                if sftp:
                    sftp.close()
            except Exception:
                pass
            try:
                if client:
                    client.close()
            except Exception:
                pass

    cmd = _thumbnail_scp_cmd(local_path, remote_path)
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if proc.returncode == 0:
        return True, ""
    return False, (proc.stderr or proc.stdout or "scp failed")[:300]


def _runpod_target_user_host(host_value: str) -> tuple[str, str]:
    if "@" in host_value:
        user, host = host_value.split("@", 1)
        return user, host
    return "root", host_value


def _run_remote_cmd_blocking(host_value: str, port_value: str, remote_cmd: str) -> tuple[bool, str]:
    user, host = _runpod_target_user_host(host_value)
    if paramiko is not None:
        client = None
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=host,
                port=int(port_value),
                username=user,
                timeout=30,
                allow_agent=True,
                look_for_keys=True,
            )
            _, stdout, stderr = client.exec_command(remote_cmd, timeout=1800)
            out = stdout.read().decode(errors="ignore")
            err = stderr.read().decode(errors="ignore")
            code = stdout.channel.recv_exit_status()
            if code == 0:
                return True, out
            return False, (err or out or f"remote exit {code}")[:500]
        except Exception as e:
            return False, str(e)[:500]
        finally:
            try:
                if client:
                    client.close()
            except Exception:
                pass

    ssh_cmd = (
        f"ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p {port_value} "
        f"{host_value} \"{remote_cmd}\""
    )
    proc = subprocess.run(ssh_cmd, shell=True, capture_output=True, text=True)
    if proc.returncode == 0:
        return True, proc.stdout[:500]
    return False, (proc.stderr or proc.stdout or "ssh failed")[:500]


def _download_file_from_runpod_blocking(host_value: str, port_value: str, remote_path: str, local_path: str) -> tuple[bool, str]:
    local_parent = Path(local_path).parent
    local_parent.mkdir(parents=True, exist_ok=True)
    user, host = _runpod_target_user_host(host_value)

    if paramiko is not None:
        client = None
        sftp = None
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=host,
                port=int(port_value),
                username=user,
                timeout=30,
                allow_agent=True,
                look_for_keys=True,
            )
            sftp = client.open_sftp()
            sftp.get(remote_path, local_path)
            return True, ""
        except Exception as e:
            return False, str(e)[:500]
        finally:
            try:
                if sftp:
                    sftp.close()
            except Exception:
                pass
            try:
                if client:
                    client.close()
            except Exception:
                pass

    scp_cmd = (
        f"scp -o StrictHostKeyChecking=no -P {port_value} "
        f"{host_value}:{remote_path} \"{local_path}\""
    )
    proc = subprocess.run(scp_cmd, shell=True, capture_output=True, text=True)
    if proc.returncode == 0:
        return True, ""
    return False, (proc.stderr or proc.stdout or "scp download failed")[:500]


def _upload_file_to_runpod_blocking(host_value: str, port_value: str, local_path: str, remote_path: str) -> tuple[bool, str]:
    if not Path(local_path).exists():
        return False, "local file missing"
    user, host = _runpod_target_user_host(host_value)

    if paramiko is not None:
        client = None
        sftp = None
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=host,
                port=int(port_value),
                username=user,
                timeout=30,
                allow_agent=True,
                look_for_keys=True,
            )
            sftp = client.open_sftp()
            remote_parent = str(Path(remote_path).parent).replace("\\", "/")
            _sftp_mkdir_p(sftp, remote_parent)
            sftp.put(local_path, remote_path)
            return True, ""
        except Exception as e:
            return False, str(e)[:500]
        finally:
            try:
                if sftp:
                    sftp.close()
            except Exception:
                pass
            try:
                if client:
                    client.close()
            except Exception:
                pass

    scp_cmd = (
        f"scp -o StrictHostKeyChecking=no -P {port_value} "
        f"\"{local_path}\" {host_value}:{remote_path}"
    )
    proc = subprocess.run(scp_cmd, shell=True, capture_output=True, text=True)
    if proc.returncode == 0:
        return True, ""
    return False, (proc.stderr or proc.stdout or "scp upload failed")[:500]


async def sync_thumbnail_to_runpod(local_path: str) -> tuple[bool, str]:
    """SCP a thumbnail to RunPod's training images directory."""
    try:
        remote_path = f"{RUNPOD_TRAINING_DIR.rstrip('/')}/{Path(local_path).name}"
        ok, err = await asyncio.to_thread(_sync_file_to_runpod_blocking, local_path, remote_path)
        if ok:
            log.info(f"Synced {Path(local_path).name} to RunPod training dir")
            return True, ""
        log.warning(f"RunPod sync failed: {err}")
        return False, err
    except Exception as e:
        err = str(e)
        log.warning(f"RunPod thumbnail sync error: {err}")
        return False, err


async def check_lora_status(user: Optional[dict] = None) -> dict:
    """Expose thumbnail style-training status using Pikzels-backed profiles."""
    library_dir = _thumbnail_library_dir_for_user(user) if user else THUMBNAIL_UPLOAD_DIR
    local_count = len([p for p in library_dir.iterdir() if p.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}]) if library_dir.exists() else 0
    profile = await _refresh_thumbnail_style_profile(user)
    status = str(profile.get("status", "") or "").strip().lower()
    ready = status == "ready"
    is_training = status in {"queued", "analyzing", "training"}
    return {
        "lora_available": ready,
        "is_training": is_training,
        "total_images": len(list(profile.get("reference_files") or [])),
        "local_library_images": local_count,
        "trained_images": len(list(profile.get("reference_files") or [])),
        "version": 1 if ready else 0,
        "last_train": int(profile.get("last_synced_at", 0) or 0),
        "training_available": bool(PIKZELS_API_KEY) and local_count >= 1,
        "provider": "pikzels",
        "style_id": str(profile.get("style_id", "") or ""),
        "style_status": status,
        "portrait_url": str(profile.get("preview_url", "") or ""),
    }


@app.get("/api/thumbnails/training-status")
async def training_status(user: dict = Depends(require_auth)):
    return await check_lora_status(user)


@app.post("/api/thumbnails/sync-library")
async def sync_thumbnail_library(user: dict = Depends(require_auth)):
    files = _thumbnail_library_images(user)
    if not files:
        return {"status": "no_files", "queued": 0, "synced": 0, "failed": 0}
    try:
        profile = await _train_thumbnail_style_profile(user)
    except Exception as e:
        raise HTTPException(400, str(e))
    return {
        "status": str(profile.get("status", "queued") or "queued"),
        "queued": min(3, len(files)),
        "synced": min(3, len(list(profile.get("reference_files") or []))),
        "failed": 0,
        "style_id": str(profile.get("style_id", "") or ""),
        "style_status": str(profile.get("status", "") or ""),
    }


@app.post("/api/thumbnails/upload")
async def upload_thumbnails(
    files: list[UploadFile] = File(...),
    background_tasks: BackgroundTasks = None,
    user: dict = Depends(require_auth),
):
    library_dir = _thumbnail_library_dir_for_user(user)
    saved = []
    for file in files:
        if not file.filename:
            continue
        ext = Path(file.filename).suffix.lower()
        if ext not in {".png", ".jpg", ".jpeg", ".webp"}:
            continue
        ts = int(time.time() * 1000)
        safe_name = str(ts) + "_" + str(random.randint(1000, 9999)) + ext
        dest = library_dir / safe_name
        with open(dest, "wb") as f:
            while chunk := await file.read(1024 * 1024):
                f.write(chunk)
        saved.append({
            "id": safe_name,
            "name": file.filename,
            "size": dest.stat().st_size,
            "url": "/api/thumbnails/library/" + safe_name,
        })
        if background_tasks and _is_admin_user(user):
            background_tasks.add_task(sync_thumbnail_to_runpod, str(dest))
    return {"uploaded": len(saved), "files": saved}


@app.get("/api/thumbnails/library")
async def list_thumbnails(user: dict = Depends(require_auth)):
    library_dir = _thumbnail_library_dir_for_user(user)
    files = []
    for f in sorted(library_dir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        if f.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}:
            files.append({
                "id": f.name,
                "name": f.name,
                "size": f.stat().st_size,
                "url": f"/api/thumbnails/library/{f.name}",
                "created_at": f.stat().st_mtime,
            })
    return {"files": files, "total": len(files)}


@app.post("/api/thumbnails/feedback")
async def thumbnail_feedback(req: ThumbnailFeedbackRequest, user: dict = Depends(require_auth)):
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    await _mark_training_feedback(
        req.generation_id,
        accepted=req.accepted,
        user_id=user.get("id", ""),
        event="thumbnail_feedback",
    )
    return {"ok": True, "generation_id": req.generation_id, "status": "accepted" if req.accepted else "rejected"}


@app.get("/api/thumbnails/library/{filename}")
async def serve_thumbnail(filename: str, request: Request):
    user = await get_current_user_from_request(request)
    if not user:
        raise HTTPException(401, "Authentication required")
    path = _thumbnail_library_dir_for_user(user) / filename
    if not path.exists():
        raise HTTPException(404, "Thumbnail not found")
    mime = "image/png" if path.suffix == ".png" else "image/jpeg" if path.suffix in (".jpg", ".jpeg") else "image/webp"
    return FileResponse(str(path), media_type=mime)


@app.delete("/api/thumbnails/library/{filename}")
async def delete_thumbnail(filename: str, user: dict = Depends(require_auth)):
    path = _thumbnail_library_dir_for_user(user) / filename
    if path.exists():
        path.unlink()
    return {"status": "deleted"}


@app.get("/api/public/thumbnail-share/{token}")
async def serve_public_thumbnail_share(token: str):
    async with _thumbnail_share_lock:
        _load_thumbnail_share_links()
        entry = dict(_thumbnail_share_links.get(token, {}) or {})
        if not entry:
            raise HTTPException(404, "Shared asset not found")
        expires_at = int(entry.get("expires_at", 0) or 0)
        if expires_at > 0 and expires_at < int(time.time()):
            _thumbnail_share_links.pop(token, None)
            _save_thumbnail_share_links()
            raise HTTPException(410, "Shared asset expired")
    path = Path(str(entry.get("path", "") or ""))
    if not path.exists():
        raise HTTPException(404, "Shared asset missing")
    suffix = path.suffix.lower()
    media_type = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".zip": "application/zip",
    }.get(suffix, "application/octet-stream")
    return FileResponse(str(path), media_type=media_type, filename=path.name)


@app.get("/api/thumbnails/generated/{filename}")
async def serve_generated_thumbnail(filename: str, request: Request):
    user = await get_current_user_from_request(request)
    if not user:
        raise HTTPException(401, "Authentication required")
    path = _thumbnail_output_dir_for_user(user) / filename
    if not path.exists():
        raise HTTPException(404, "Generated thumbnail not found")
    mime = "image/png"
    return FileResponse(str(path), media_type=mime)


THUMBNAIL_ANALYSIS_PROMPT = """You are an elite YouTube thumbnail design strategist and art director. You analyze what makes thumbnails get clicks and generate precise production prompts that create thumbnails that outperform human designers.

You understand:
- Color psychology (red/yellow = urgency, blue = trust, contrast = attention)
- Face/emotion science (shocked expressions get 2-3x CTR)
- Text placement rules (big bold text, 3-5 words max, high contrast)
- Composition (rule of thirds, leading lines, depth)
- Platform-specific optimization (1920x1080 output, mobile-first readability)

When given a video description, style reference, or sketch, you produce a detailed SDXL image generation prompt that will create a click-worthy thumbnail.

Output MUST be valid JSON:
{
  "prompt": "Detailed thumbnail prompt. Include: subject, composition, lighting, colors, text elements, style, camera angle. Be extremely specific about every visual element. High-end professional YouTube thumbnail.",
  "negative_prompt": "Elements to avoid in the generation",
  "title_text": "The 3-5 word overlay text for the thumbnail (if applicable, empty string if none)",
  "style_notes": "Brief description of the design strategy being used"
}"""


THUMBNAIL_STYLE_TRANSFER_PROMPT = """You are an elite YouTube thumbnail design strategist. You will receive:
1. A description of the STYLE to emulate (from a reference thumbnail the user likes)
2. A description of what the NEW thumbnail should show

Your job: merge the visual style of the reference with the new content to create a prompt that produces a thumbnail in that exact style but with new content.

Analyze the style reference for: color palette, composition style, lighting mood, text treatment, overall aesthetic.
Then apply that exact style to the new content.

Output MUST be valid JSON:
{
  "prompt": "Detailed thumbnail prompt combining the reference style with new content. Professional YouTube thumbnail, 1920x1080 composition.",
  "negative_prompt": "Elements to avoid",
  "title_text": "The overlay text for this thumbnail (3-5 words, empty string if none)",
  "style_notes": "How the reference style was adapted"
}"""


THUMBNAIL_SCREENSHOT_PROMPT = """You are an elite YouTube thumbnail analyst and designer. The user has provided a description of their YouTube channel's existing thumbnails (or a description of what has worked for them before).

Your job: identify the patterns that made those thumbnails successful, then generate a NEW thumbnail prompt that follows the same winning formula but feels fresh and evolved.

Analyze for: recurring color schemes, text styles, composition patterns, emotional triggers, branding elements.

Output MUST be valid JSON:
{
  "prompt": "Detailed thumbnail prompt for a new thumbnail following the user's proven style. Professional YouTube thumbnail, 1920x1080.",
  "negative_prompt": "Elements to avoid",
  "title_text": "Overlay text (3-5 words, empty if none)",
  "style_notes": "Pattern analysis of what works for this channel",
  "patterns_detected": ["pattern1", "pattern2", "pattern3"]
}"""


async def _generate_thumbnail_prompt(req: ThumbnailGenerateRequest) -> dict:
    if req.mode == "style_transfer":
        system_prompt = THUMBNAIL_STYLE_TRANSFER_PROMPT
        user_msg = f"STYLE REFERENCE: {req.description}\n\nNEW THUMBNAIL CONTENT: {req.screenshot_description or req.description}"
    elif req.mode == "screenshot_analysis":
        system_prompt = THUMBNAIL_SCREENSHOT_PROMPT
        user_msg = f"CHANNEL THUMBNAIL ANALYSIS: {req.screenshot_description or req.description}\n\nNEW VIDEO TO MAKE THUMBNAIL FOR: {req.description}"
    else:
        system_prompt = THUMBNAIL_ANALYSIS_PROMPT
        user_msg = f"Create a viral YouTube thumbnail for: {req.description}"

    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {XAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": "grok-3-mini-fast",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_msg},
                ],
                "temperature": 0.7,
            },
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        start = content.find("{")
        end = content.rfind("}") + 1
        if start == -1 or end == 0:
            raise ValueError("No JSON in thumbnail AI response")
        return json.loads(content[start:end])


async def _check_lora_exists() -> bool:
    """Quick check if the trained LoRA file exists in ComfyUI."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(COMFYUI_URL + "/object_info/LoraLoader")
            if resp.status_code == 200:
                data = resp.json()
                lora_info = data.get("LoraLoader", {}).get("input", {}).get("required", {})
                lora_names = lora_info.get("lora_name", [[]])[0]
                return LORA_NAME in lora_names
    except Exception:
        pass
    return False


async def _enforce_thumbnail_1080(output_path: str) -> str:
    """Force final thumbnail image to exactly 1920x1080."""
    fixed_out = str(Path(output_path).with_name(Path(output_path).stem + "_1920x1080.png"))
    cmd = [
        "ffmpeg", "-y",
        "-i", output_path,
        "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,setsar=1",
        fixed_out,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError("Failed to enforce 1920x1080 thumbnail: " + stderr.decode()[-200:])
    fixed = Path(fixed_out)
    if not fixed.exists() or fixed.stat().st_size == 0:
        raise RuntimeError("1920x1080 thumbnail output missing")
    Path(output_path).unlink(missing_ok=True)
    fixed.rename(output_path)
    return output_path


async def _generate_thumbnail_image(
    prompt: str,
    negative_prompt: str,
    output_path: str,
    user: Optional[dict],
    mode: str = "describe",
    style_ref_path: str = "",
) -> dict:
    """Generate a thumbnail via Pikzels and save the image locally."""
    if not PIKZELS_API_KEY:
        raise RuntimeError("PIKZELS_API_KEY not configured")

    mode_normalized = str(mode or "describe").strip().lower()
    profile = await _refresh_thumbnail_style_profile(user)
    style_id = ""
    if str(profile.get("status", "") or "").strip().lower() == "ready":
        style_id = str(profile.get("style_id", "") or "").strip()

    composed_prompt = str(prompt or "").strip()
    if negative_prompt:
        composed_prompt = f"{composed_prompt}\nAvoid: {negative_prompt.strip()}"

    if style_ref_path and Path(style_ref_path).exists():
        ref_token = await _register_thumbnail_share(Path(style_ref_path), user, ttl_sec=7200)
        body = {
            "prompt": composed_prompt,
            "image_url": _thumbnail_share_url(ref_token),
            "model": PIKZELS_RECREATE_MODEL,
            "format": "16:9",
        }
        if style_id:
            body["style"] = style_id
        result = await _pikzels_request("POST", "/v1/recreate", json_body=body)
        provider_mode = "recreate"
    else:
        body = {
            "prompt": composed_prompt,
            "model": PIKZELS_THUMBNAIL_MODEL,
            "format": "16:9",
        }
        if style_id and mode_normalized in {"describe", "screenshot_analysis", "style_transfer"}:
            body["style"] = style_id
        result = await _pikzels_request("POST", "/v1/thumbnail", json_body=body)
        provider_mode = "thumbnail"

    output_url = str(result.get("output", "") or "").strip()
    if not output_url:
        raise RuntimeError("Pikzels did not return an output URL")
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        download = await client.get(output_url)
        if download.status_code != 200:
            raise RuntimeError("Failed to download Pikzels thumbnail output")
    with open(output_path, "wb") as f:
        f.write(download.content)
    return {
        "path": output_path,
        "output_url": output_url,
        "request_id": str(result.get("request_id", "") or ""),
        "style_id": style_id,
        "provider_mode": provider_mode,
    }


@app.post("/api/thumbnails/generate")
async def generate_thumbnail(req: ThumbnailGenerateRequest, background_tasks: BackgroundTasks, user: dict = Depends(require_auth)):
    if not XAI_API_KEY:
        raise HTTPException(500, "XAI_API_KEY not configured")
    if not PIKZELS_API_KEY:
        raise HTTPException(500, "PIKZELS_API_KEY not configured")
    library_dir = _thumbnail_library_dir_for_user(user)
    output_dir = _thumbnail_output_dir_for_user(user)

    job_id = f"thumb_{int(time.time())}_{random.randint(1000, 9999)}"
    jobs[job_id] = {
        "status": "queued",
        "progress": 0,
        "type": "thumbnail",
        "lane": "thumbnails",
        "mode": req.mode,
        "credit_cost": 0,
        "billing_source": "workspace_access",
        "user_id": str(user.get("id", "") or ""),
        "created_at": time.time(),
    }

    async def _run_thumbnail_pipeline():
        try:
            jobs[job_id]["status"] = "analyzing"
            jobs[job_id]["progress"] = 10
            log.info(f"[{job_id}] Thumbnail gen: mode={req.mode}")

            ai_result = await _generate_thumbnail_prompt(req)
            thumb_prompt = ai_result.get("prompt", req.description)
            thumb_negative = ai_result.get("negative_prompt", NEGATIVE_PROMPT)
            title_text = ai_result.get("title_text", "")
            style_notes = ai_result.get("style_notes", "")
            if title_text:
                thumb_prompt = f"{thumb_prompt} Prominent thumbnail text overlay: {title_text}."

            jobs[job_id]["status"] = "generating"
            jobs[job_id]["progress"] = 30
            jobs[job_id]["ai_analysis"] = {
                "title_text": title_text,
                "style_notes": style_notes,
                "patterns": ai_result.get("patterns_detected", []),
            }

            style_ref_path = ""
            if req.style_reference_id:
                ref_path = library_dir / req.style_reference_id
                if ref_path.exists():
                    style_ref_path = str(ref_path)

            output_name = f"{job_id}.png"
            output_path = str(output_dir / output_name)
            render_result = await _generate_thumbnail_image(
                thumb_prompt,
                thumb_negative,
                output_path,
                user=user,
                mode=req.mode,
                style_ref_path=style_ref_path,
            )

            await _enforce_thumbnail_1080(output_path)
            thumb_gen_id = await _save_training_candidate(
                thumb_prompt,
                output_path,
                template="thumbnail",
                source="pikzels",
                metadata={
                    "mode": req.mode,
                    "title_text": title_text,
                    "user_id": user.get("id", ""),
                    "provider_mode": str(render_result.get("provider_mode", "") or ""),
                    "provider_request_id": str(render_result.get("request_id", "") or ""),
                    "output_url": str(render_result.get("output_url", "") or ""),
                    "style_id": str(render_result.get("style_id", "") or ""),
                },
            )

            jobs[job_id]["status"] = "complete"
            jobs[job_id]["progress"] = 100
            jobs[job_id]["output_file"] = output_name
            jobs[job_id]["output_url"] = f"/api/thumbnails/generated/{output_name}"
            jobs[job_id]["generation_id"] = thumb_gen_id
            jobs[job_id]["provider"] = "pikzels"
            jobs[job_id]["provider_request_id"] = str(render_result.get("request_id", "") or "")
            log.info(f"[{job_id}] Thumbnail COMPLETE: {output_name}")

        except Exception as e:
            log.error(f"[{job_id}] Thumbnail pipeline failed: {e}", exc_info=True)
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = str(e)

    background_tasks.add_task(_run_thumbnail_pipeline)
    return {"status": "accepted", "job_id": job_id}


COMPRESS_THRESHOLD_MB = 50


async def compress_video_if_needed(video_path: str, job_id: str, label: str = "demo") -> str:
    """Compress video to 720p if file exceeds COMPRESS_THRESHOLD_MB. Returns path (possibly new)."""
    file_size_mb = Path(video_path).stat().st_size / (1024 * 1024)
    if file_size_mb <= COMPRESS_THRESHOLD_MB:
        return video_path

    log.info(f"[{job_id}] {label} video is {file_size_mb:.0f}MB (>{COMPRESS_THRESHOLD_MB}MB), compressing to 720p...")
    jobs[job_id]["status"] = f"compressing_{label}"
    jobs[job_id]["progress"] = jobs[job_id].get("progress", 0)
    jobs[job_id]["compress_info"] = {
        "label": label,
        "original_size_mb": round(file_size_mb, 1),
        "target": "720p",
    }

    compressed_path = video_path.rsplit(".", 1)[0] + "_compressed.mp4"
    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-vf", "scale=-2:720",
        "-c:v", "libx264", "-preset", "fast", "-crf", "28",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        compressed_path
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    _, stderr_data = await proc.communicate()

    if proc.returncode == 0 and Path(compressed_path).exists():
        new_size_mb = Path(compressed_path).stat().st_size / (1024 * 1024)
        log.info(f"[{job_id}] {label} compressed: {file_size_mb:.0f}MB -> {new_size_mb:.0f}MB")
        jobs[job_id]["compress_info"]["compressed_size_mb"] = round(new_size_mb, 1)
        Path(video_path).unlink(missing_ok=True)
        return compressed_path
    else:
        log.warning(f"[{job_id}] {label} compression failed, using original. stderr: {stderr_data.decode()[-300:]}")
        Path(compressed_path).unlink(missing_ok=True)
        return video_path


async def run_demo_pipeline(job_id: str, demo_path: str, ref_path: str, face_path: str,
                            product_name: str, reference_notes: str,
                            pip_position: str = "bottom-right", voice_id: str = ""):
    """Full product demo video generation pipeline."""
    try:
        ref_style = ""
        if ref_path and Path(ref_path).exists():
            jobs[job_id]["status"] = "analyzing_reference"
            jobs[job_id]["progress"] = 3
            log.info(f"[{job_id}] Analyzing reference video for style...")
            ref_analysis = await analyze_screen_recording(ref_path)
            ref_style = (
                f"Match the style, pacing, and energy of this reference video: "
                f"{ref_analysis.get('description', '')}. "
                f"Duration: {ref_analysis.get('duration', 0):.0f}s, "
                f"{ref_analysis.get('frame_count', 0)} key frames analyzed."
            )
            Path(ref_path).unlink(missing_ok=True)

        jobs[job_id]["status"] = "analyzing"
        jobs[job_id]["progress"] = 5
        log.info(f"[{job_id}] Demo pipeline: analyzing demo video...")

        analysis = await analyze_screen_recording(demo_path)
        jobs[job_id]["analysis"] = {
            "duration": analysis["duration"],
            "frame_count": analysis["frame_count"]
        }
        jobs[job_id]["progress"] = 20
        log.info(f"[{job_id}] Screen analyzed: {analysis['frame_count']} frames, {analysis['duration']:.1f}s")

        jobs[job_id]["status"] = "scripting"
        jobs[job_id]["progress"] = 25
        log.info(f"[{job_id}] Generating demo script...")

        full_ref_notes = reference_notes
        if ref_style:
            full_ref_notes = ref_style + ("\n" + reference_notes if reference_notes else "")
        script_data = await generate_demo_script(analysis, product_name, full_ref_notes)
        jobs[job_id]["script"] = script_data
        jobs[job_id]["progress"] = 40
        log.info(f"[{job_id}] Script ready: {len(script_data.get('segments', []))} segments")

        jobs[job_id]["status"] = "generating_voice"
        jobs[job_id]["progress"] = 45
        log.info(f"[{job_id}] Generating voiceover...")

        full_narration = " ".join(seg.get("text", seg.get("narration", "")) for seg in script_data.get("segments", []))
        audio_path = str(TEMP_DIR / (job_id + "_demo_voice.mp3"))
        vo_result = await generate_voiceover(full_narration, audio_path, template="motivation", override_voice_id=voice_id)
        audio_path = vo_result["audio_path"]
        word_timings = vo_result.get("word_timings", [])
        jobs[job_id]["progress"] = 60

        # Keep demo narration aligned to the uploaded recording duration without rushed speech.
        target_demo_dur = float(analysis.get("duration", 0.0) or 0.0)
        source_audio_dur = _probe_audio_duration_seconds(audio_path)
        if target_demo_dur > 0.2 and source_audio_dur > 0.2:
            speed_ratio = source_audio_dur / target_demo_dur
            if source_audio_dur < (target_demo_dur - 0.08):
                fit_audio_path = str(TEMP_DIR / (job_id + "_demo_voice_fit.mp3"))
                # Only slow down short narration; never speed up long narration (sounds rushed).
                gentle_ratio = max(speed_ratio, 0.88)
                atempo_chain = _build_atempo_filter_chain(gentle_ratio)
                fit_cmd = [
                    "ffmpeg", "-y", "-i", audio_path,
                    "-af", f"{atempo_chain},apad=pad_dur={target_demo_dur + 0.25:.3f}",
                    "-t", f"{target_demo_dur:.3f}",
                    "-c:a", "libmp3lame", "-b:a", "192k",
                    fit_audio_path,
                ]
                fit_proc = await asyncio.create_subprocess_exec(
                    *fit_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                _, fit_err = await fit_proc.communicate()
                if fit_proc.returncode == 0 and Path(fit_audio_path).exists():
                    Path(audio_path).unlink(missing_ok=True)
                    audio_path = fit_audio_path
                    if word_timings:
                        remapped = []
                        for wt in word_timings:
                            start = max(0.0, float(wt.get("start", 0.0)) / gentle_ratio)
                            end = max(start + 0.01, float(wt.get("end", start)) / gentle_ratio)
                            if start <= target_demo_dur + 0.15:
                                remapped.append({
                                    "word": wt.get("word", ""),
                                    "start": start,
                                    "end": min(end, target_demo_dur + 0.15),
                                })
                        word_timings = remapped
                    jobs[job_id]["audio_sync"] = {
                        "source_duration": round(source_audio_dur, 3),
                        "target_duration": round(target_demo_dur, 3),
                        "speed_ratio": round(gentle_ratio, 5),
                        "mode": "slowdown_and_pad",
                    }
                    log.info(f"[{job_id}] Demo voiceover fit to video duration ({source_audio_dur:.2f}s -> {target_demo_dur:.2f}s)")
                else:
                    log.warning(f"[{job_id}] Demo voiceover duration-fit failed, using original audio: {(fit_err.decode(errors='ignore')[-200:])}")
            else:
                jobs[job_id]["audio_sync"] = {
                    "source_duration": round(source_audio_dur, 3),
                    "target_duration": round(target_demo_dur, 3),
                    "speed_ratio": 1.0,
                    "mode": "no_speedup",
                }

        subtitle_path = None
        if word_timings:
            demo_w, demo_h = 1920, 1080
            try:
                probe_cmd = [
                    "ffprobe", "-v", "error", "-show_entries", "stream=width,height",
                    "-of", "csv=p=0:s=x", demo_path
                ]
                p = await asyncio.create_subprocess_exec(
                    *probe_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                out, _ = await p.communicate()
                dims = out.decode().strip().split("\n")[0]
                if "x" in dims:
                    demo_w, demo_h = int(dims.split("x")[0]), int(dims.split("x")[1])
            except Exception:
                pass
            subtitle_path = str(TEMP_DIR / (job_id + "_demo_captions.ass"))
            generate_ass_subtitles(word_timings, subtitle_path, video_width=demo_w, video_height=demo_h)

        jobs[job_id]["status"] = "generating_face"
        jobs[job_id]["progress"] = 65

        talking_head_path = None
        has_face = False

        try:
            if face_path and Path(face_path).exists():
                log.info(f"[{job_id}] Generating talking head animation...")
                talking_head_path = str(TEMP_DIR / (job_id + "_talking_head.mp4"))
                await generate_talking_head(face_path, audio_path, talking_head_path)
                has_face = True
                log.info(f"[{job_id}] Talking head generated")
            else:
                log.info(f"[{job_id}] Face PiP disabled (no face image uploaded)")
                talking_head_path = None
                has_face = False
            jobs[job_id]["progress"] = 80
        except Exception as face_err:
            log.warning(f"[{job_id}] Talking head failed ({face_err}), continuing without face PiP")
            talking_head_path = None
            has_face = False
            jobs[job_id]["progress"] = 80

        jobs[job_id]["status"] = "compositing"
        jobs[job_id]["progress"] = 85
        log.info(f"[{job_id}] Compositing demo video...")

        output_filename = f"demo_{job_id}.mp4"
        output_path = str(OUTPUT_DIR / output_filename)

        if has_face and talking_head_path and Path(talking_head_path).exists():
            await composite_demo_video(
                demo_path, talking_head_path, audio_path,
                output_path, subtitle_path=subtitle_path,
                pip_position=pip_position
            )
        else:
            sub_filter = ""
            if subtitle_path and Path(subtitle_path).exists():
                sub_abs = str(Path(subtitle_path).resolve()).replace("\\", "/").replace(":", "\\:")
                sub_filter = f"-vf ass={sub_abs}"
            cmd = ["ffmpeg", "-y", "-i", demo_path, "-i", audio_path]
            if sub_filter:
                cmd += ["-vf", f"ass={sub_abs}"]
            cmd += [
                "-map", "0:v:0",
                "-map", "1:a:0",
                "-c:v", "libx264", "-preset", "fast", "-crf", "20",
                "-c:a", "aac", "-b:a", "192k",
                "-shortest", "-movflags", "+faststart",
                output_path
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            await proc.communicate()
            if not Path(output_path).exists():
                cmd_simple = ["ffmpeg", "-y", "-i", demo_path, "-i", audio_path,
                              "-c:v", "libx264", "-preset", "fast", "-crf", "20",
                              "-c:a", "aac", "-b:a", "192k", "-shortest", output_path]
                proc = await asyncio.create_subprocess_exec(
                    *cmd_simple, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                await proc.communicate()

        jobs[job_id]["progress"] = 95

        if talking_head_path:
            Path(talking_head_path).unlink(missing_ok=True)
        Path(audio_path).unlink(missing_ok=True)
        if subtitle_path:
            Path(subtitle_path).unlink(missing_ok=True)

        jobs[job_id]["status"] = "complete"
        jobs[job_id]["progress"] = 100
        jobs[job_id]["output_file"] = output_filename
        jobs[job_id]["output_url"] = f"/api/download/{output_filename}"
        log.info(f"[{job_id}] Demo COMPLETE: {output_filename}")

    except Exception as e:
        log.error(f"[{job_id}] Demo pipeline failed: {e}", exc_info=True)
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(e)

    finally:
        Path(demo_path).unlink(missing_ok=True)
        if ref_path and Path(ref_path).exists():
            Path(ref_path).unlink(missing_ok=True)
        if face_path and Path(face_path).exists():
            Path(face_path).unlink(missing_ok=True)


@app.get("/api/voices")
async def list_voices():
    """List available ElevenLabs voices for the user to choose from."""
    voices, source, provider_ok, warning = await _fetch_voice_catalog()
    _cache_voice_catalog(source, provider_ok, len(voices), warning)
    out = {"voices": voices, "source": source, "provider_ok": provider_ok}
    if warning:
        out["warning"] = warning
    return out


@app.post("/api/voices/preview")
async def preview_voice(request: Request):
    """Generate a short voice preview with a given voice_id."""
    if not ELEVENLABS_API_KEY:
        raise HTTPException(500, "ElevenLabs API key not configured")
    body = await request.json()
    voice_id = body.get("voice_id", "")
    if not voice_id:
        raise HTTPException(400, "voice_id required")
    preview_text = body.get("text", "Hey there! This is a quick preview of what I sound like. Pretty cool, right?")
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}",
            headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
            json={
                "text": preview_text,
                "model_id": "eleven_turbo_v2_5",
                "voice_settings": {"stability": 0.5, "similarity_boost": 0.75, "style": 0.3},
            },
        )
        if resp.status_code != 200:
            raise HTTPException(resp.status_code, f"ElevenLabs error: {resp.text[:200]}")
    from fastapi.responses import Response
    return Response(content=resp.content, media_type="audio/mpeg",
                    headers={"Content-Disposition": f"inline; filename=preview_{voice_id}.mp3"})


@app.post("/api/demo")
async def create_demo_video(
    background_tasks: BackgroundTasks,
    demo_video: UploadFile = File(...),
    reference_video: Optional[UploadFile] = File(None),
    face_image: Optional[UploadFile] = File(None),
    product_name: str = Form(""),
    reference_notes: str = Form(""),
    pip_position: str = Form("bottom-right"),
    voice_id: str = Form(""),
    request: Request = None,
):
    user = await get_current_user_from_request(request)
    if not user:
        raise HTTPException(401, "Authentication required")

    user_email = user.get("email", "")
    user_plan = user.get("plan", "starter")
    is_admin = user_email in ADMIN_EMAILS
    if (not PRODUCT_DEMO_PUBLIC_ENABLED) and (not is_admin):
        raise HTTPException(403, "Product Demo is coming soon.")
    has_demo = is_admin or (user_plan == "demo_pro")
    if not has_demo:
        raise HTTPException(403, "Product Demo requires the Demo Pro plan ($150/mo). Upgrade to access this feature.")

    job_id = f"demo_{int(time.time()*1000)}_{random.randint(1000,9999)}"

    demo_ext = Path(demo_video.filename or "video.mp4").suffix or ".mp4"
    demo_path = str(DEMO_DIR / (job_id + "_demo" + demo_ext))
    with open(demo_path, "wb") as f:
        while chunk := await demo_video.read(1024 * 1024):
            f.write(chunk)

    ref_path = ""
    if reference_video and reference_video.filename:
        ref_ext = Path(reference_video.filename).suffix or ".mp4"
        ref_path = str(DEMO_DIR / (job_id + "_reference" + ref_ext))
        with open(ref_path, "wb") as f:
            while chunk := await reference_video.read(1024 * 1024):
                f.write(chunk)

    face_path = ""
    if face_image and face_image.filename:
        face_ext = Path(face_image.filename).suffix or ".png"
        face_path = str(DEMO_DIR / (job_id + "_face" + face_ext))
        with open(face_path, "wb") as f:
            while chunk := await face_image.read(1024 * 1024):
                f.write(chunk)

    jobs[job_id] = {
        "status": "queued", "progress": 0, "type": "demo",
        "product_name": product_name,
        "created_at": time.time()
    }

    background_tasks.add_task(
        run_demo_pipeline, job_id, demo_path, ref_path, face_path,
        product_name, reference_notes, pip_position, voice_id
    )

    return {"status": "accepted", "job_id": job_id}


# ─── Static Files ─────────────────────────────────────────────────────────────

_default_dist_dir = (Path(__file__).resolve().parent / "ViralShorts-App" / "dist").resolve()
dist_dir = Path(os.getenv("FRONTEND_DIST_DIR", str(_default_dist_dir))).resolve()
if dist_dir.exists():
    log.info(f"Serving frontend dist from: {dist_dir}")
    assets_dir = dist_dir / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir), html=False), name="assets")

    @app.get("/", include_in_schema=False)
    async def _serve_landing_page():
        index_path = dist_dir / "index.html"
        if not index_path.exists():
            raise HTTPException(404, "Landing page not found")
        return FileResponse(str(index_path), media_type="text/html")

    @app.get("/{page_name}.html", include_in_schema=False)
    async def _serve_dist_html(page_name: str):
        if not re.fullmatch(r"[A-Za-z0-9._-]+", page_name or ""):
            raise HTTPException(404, "Not Found")
        html_path = dist_dir / f"{page_name}.html"
        if not html_path.exists():
            raise HTTPException(404, "Not Found")
        return FileResponse(str(html_path), media_type="text/html")

    @app.get("/{asset_name}", include_in_schema=False)
    async def _serve_root_asset(asset_name: str):
        if "/" in asset_name or "\\" in asset_name:
            raise HTTPException(404, "Not Found")
        asset_path = dist_dir / asset_name
        if not asset_path.exists() or not asset_path.is_file():
            raise HTTPException(404, "Not Found")
        suffix = asset_path.suffix.lower()
        media_map = {
            ".txt": "text/plain; charset=utf-8",
            ".xml": "application/xml",
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".webp": "image/webp",
            ".svg": "image/svg+xml",
            ".ico": "image/x-icon",
            ".json": "application/json",
        }
        return FileResponse(str(asset_path), media_type=media_map.get(suffix))
else:
    log.warning(f"Frontend dist directory not found: {dist_dir}")


if __name__ == "__main__":
    for f in OUTPUT_DIR.iterdir():
        if f.suffix == ".mp4" and f.stat().st_mtime < time.time() - 86400:
            f.unlink(missing_ok=True)
    uvicorn.run("backend:app", host="0.0.0.0", port=int(os.getenv("PORT", "8091")), reload=True)
