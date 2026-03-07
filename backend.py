import os
import re
import base64
import shutil
import random
import asyncio
import json
import time
import subprocess
import tempfile
import logging
import io
import httpx
import jwt
from datetime import datetime, timezone
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, Form, BackgroundTasks, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional
import stripe as stripe_lib
import uvicorn
from backend_settings import (
    XAI_API_KEY,
    ELEVENLABS_API_KEY,
    COMFYUI_URL,
    SUPABASE_URL,
    SUPABASE_ANON_KEY,
    SUPABASE_JWT_SECRET,
    STRIPE_SECRET_KEY,
    STRIPE_WEBHOOK_SECRET,
    SITE_URL,
    FAL_AI_KEY,
    XAI_IMAGE_MODEL,
    XAI_VIDEO_MODEL,
    RUNWAY_API_KEY,
    RUNWAY_API_KEY_SOURCE,
    RUNWAY_VIDEO_MODEL,
    RUNWAY_API_VERSION,
    PLAN_PRICE_USD,
    XAI_IMAGE_ASPECT_RATIO,
    XAI_IMAGE_RESOLUTION,
    USE_XAI_VIDEO,
    PRODUCT_DEMO_PUBLIC_ENABLED,
    SKELETON_GLOBAL_REFERENCE_IMAGE_URL,
    USE_FAL_GROK_IMAGE,
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
)
from backend_models import (
    GenerateRequest,
    SceneImageRequest,
    FinalizeRequest,
    CheckoutRequest,
    TopupCheckoutRequest,
    SetPlanRequest,
    FeedbackRequest,
    ThumbnailFeedbackRequest,
    ThumbnailGenerateRequest,
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
    from PIL import Image, ImageFilter, ImageStat
except Exception:
    Image = None
    ImageFilter = None
    ImageStat = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("nyptid-studio")

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
USAGE_LEDGER_PATH = TEMP_DIR / "usage_ledger.jsonl"
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


def _month_key(ts: float | None = None) -> str:
    now = datetime.fromtimestamp(ts or time.time(), tz=timezone.utc)
    return f"{now.year:04d}-{now.month:02d}"


def _wallet_for_user(user_id: str) -> dict:
    if not user_id:
        return {"topup_credits": 0, "monthly_usage": {}, "updated_at": time.time()}
    wallet = _topup_wallets.get(user_id)
    if not isinstance(wallet, dict):
        wallet = {"topup_credits": 0, "monthly_usage": {}, "updated_at": time.time()}
        _topup_wallets[user_id] = wallet
    wallet.setdefault("topup_credits", 0)
    wallet.setdefault("monthly_usage", {})
    wallet.setdefault("updated_at", time.time())
    return wallet


def _plan_monthly_credit_limit(plan: str) -> int:
    limits = PLAN_LIMITS.get(plan, PLAN_LIMITS.get("starter", {}))
    return int(limits.get("videos_per_month", 0) or 0)


def _credit_state_for_user(user: dict, effective_plan: str, billing_active: bool, is_admin: bool = False) -> dict:
    if is_admin:
        return {
            "monthly_limit": 9999,
            "monthly_used": 0,
            "monthly_remaining": 9999,
            "topup_credits": 9999,
            "credits_total_remaining": 9999,
            "requires_topup": False,
            "month_key": _month_key(),
        }
    user_id = str(user.get("id", "") or "")
    wallet = _wallet_for_user(user_id)
    mk = _month_key()
    monthly_used = int((wallet.get("monthly_usage", {}) or {}).get(mk, 0) or 0)
    monthly_limit = _plan_monthly_credit_limit(effective_plan) if billing_active else 0
    monthly_remaining = max(0, monthly_limit - monthly_used)
    topup = int(wallet.get("topup_credits", 0) or 0)
    total_remaining = monthly_remaining + topup
    return {
        "monthly_limit": monthly_limit,
        "monthly_used": monthly_used,
        "monthly_remaining": monthly_remaining,
        "topup_credits": topup,
        "credits_total_remaining": total_remaining,
        "requires_topup": bool(billing_active and total_remaining <= 0),
        "month_key": mk,
    }


async def _reserve_generation_credit(user: dict, effective_plan: str, billing_active: bool, is_admin: bool = False) -> tuple[bool, str, dict]:
    if is_admin:
        return True, "admin", _credit_state_for_user(user, effective_plan, billing_active, is_admin=True)
    if not billing_active:
        return False, "subscription_required", _credit_state_for_user(user, effective_plan, billing_active, is_admin=False)
    user_id = str(user.get("id", "") or "")
    async with _topup_wallet_lock:
        wallet = _wallet_for_user(user_id)
        state = _credit_state_for_user(user, effective_plan, billing_active, is_admin=False)
        mk = state["month_key"]
        if state["monthly_remaining"] > 0:
            usage = dict(wallet.get("monthly_usage", {}) or {})
            usage[mk] = int(usage.get(mk, 0) or 0) + 1
            wallet["monthly_usage"] = usage
            wallet["updated_at"] = time.time()
            _save_topup_wallets()
            return True, "monthly", _credit_state_for_user(user, effective_plan, billing_active, is_admin=False)
        topup = int(wallet.get("topup_credits", 0) or 0)
        if topup > 0:
            wallet["topup_credits"] = topup - 1
            wallet["updated_at"] = time.time()
            _save_topup_wallets()
            return True, "topup", _credit_state_for_user(user, effective_plan, billing_active, is_admin=False)
        return False, "topup_required", state


async def _refund_generation_credit(user_id: str, source: str, month_key: str = "") -> None:
    if not user_id or source not in {"monthly", "topup"}:
        return
    async with _topup_wallet_lock:
        wallet = _wallet_for_user(user_id)
        if source == "topup":
            wallet["topup_credits"] = int(wallet.get("topup_credits", 0) or 0) + 1
        else:
            mk = month_key or _month_key()
            usage = dict(wallet.get("monthly_usage", {}) or {})
            usage[mk] = max(0, int(usage.get(mk, 0) or 0) - 1)
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
        wallet["topup_credits"] = int(wallet.get("topup_credits", 0) or 0) + int(credits)
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
        return "720p"
    if not priority_allowed and resolution == "1080p":
        return "720p"
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


REFERENCE_LOCK_MODES = {"strict", "inspired"}
ART_STYLE_PRESETS = {
    "auto": "",
    "cinematic_realism": "Photoreal cinematic realism with natural skin detail, physically-plausible lighting, clean lens behavior, and premium film color grade.",
    "commercial_polish": "High-end commercial look: crisp product-grade detail, controlled highlights, clean background separation, and premium ad-level finish.",
    "moody_noir": "Moody low-key cinematic style with rich shadow contrast, tasteful grain-free clarity, and dramatic yet realistic lighting.",
    "bright_lifestyle": "Bright modern lifestyle aesthetic with soft natural light, inviting color balance, realistic textures, and clean premium framing.",
}


def _normalize_reference_lock_mode(value, default: str = "strict") -> str:
    text = str(value or default).strip().lower()
    return text if text in REFERENCE_LOCK_MODES else default


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
        + ("Maintain exact identity/outfit continuity scene-to-scene. " if lock_mode == "strict" else "Preserve style while allowing pose/composition variety. ")
        + (
            "For skeleton keep skull geometry, outfit, and logo family unchanged."
            if template == "skeleton"
            else (
                "For story keep recurring subjects, key locations, and grade continuity consistent when the script indicates recurrence."
                if template == "story"
                else "Keep subject styling and grading consistent across scenes."
            )
        )
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
                },
                "notes": notes,
            }
    except Exception as e:
        return {"score": 0.0, "ok": False, "reason": f"scoring_error:{e}"}


def _resolve_reference_for_scene(session: dict, template: str, scene_index: int) -> str:
    base_ref_public = str(session.get("reference_image_public_url", "") or "")
    base_ref_inline = str(session.get("reference_image_url", "") or "")
    base_ref = base_ref_public or base_ref_inline
    skeleton_ref = str(session.get("skeleton_reference_image", "") or "")
    rolling_ref = str(session.get("rolling_reference_image_url", "") or "")
    lock_mode = _normalize_reference_lock_mode(session.get("reference_lock_mode"), default="strict")
    selected = skeleton_ref if template == "skeleton" and skeleton_ref else base_ref
    if template == "skeleton" and not selected:
        selected = SKELETON_GLOBAL_REFERENCE_IMAGE_URL
    # External providers are more reliable with public HTTPS URLs than data URLs.
    if lock_mode == "strict":
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


def _billing_active_for_user(user: Optional[dict]) -> bool:
    """Resolve paid access with a resilient profile-plan fallback."""
    if not user:
        return False
    email = str(user.get("email", "") or "").strip()
    if email in ADMIN_EMAILS:
        return True
    if _stripe_has_active_subscription(email):
        return True
    stored_plan = str(user.get("plan", "none") or "none")
    if _profile_plan_is_paid(stored_plan):
        log.warning("Stripe check failed/inactive; allowing paid access via profile plan for %s", email)
        return True
    return False


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
    "skeleton": """You are an elite viral short-form video scriptwriter for the "Skeleton" format. These are photorealistic 3D animated shorts where skeleton characters in detailed outfits deliver rapid-fire comparisons. The reference channel is CrypticScience.

CRITICAL: Each visual_description will be used to GENERATE AN IMAGE and then ANIMATE IT INTO A VIDEO CLIP. Keep each visual_description SIMPLE but DETAILED, with a HARD MAX of 3 sentences:
- Sentence 1: exact outfit + identity lock details first (full body outfit, colors, logos/brand family, accessories).
- Sentence 2: pose + prop + camera framing.
- Sentence 3: motion/action cues only (what moves and how).
Never exceed 3 sentences. Prefer 2-3 concise sentences over long paragraphs.

THE SKELETON CHARACTER RULES (STRICT):
- Think of it as a REAL PERSON wearing a full outfit, but with a SKULL for a head and SKELETON HANDS. The body under the clothes is NOT visible -- clothes cover everything from neck to feet.
- The skull is glossy ivory-white with detailed bone texture. Realistic human eyeballs in the sockets with colored iris and wet shine.
- CLOTHING (CRITICAL -- THIS IS THE MOST IMPORTANT RULE): The skeleton MUST be wearing a COMPLETE outfit that COVERS THE ENTIRE BODY from neck to feet:
  * If it's a pilot: full navy pilot uniform with epaulettes, tie, pants, shoes -- NO bare ribcage showing
  * If it's a doctor: full white lab coat BUTTONED UP over scrubs, stethoscope, dress pants, shoes -- NO bare spine showing
  * If it's a race driver (NASCAR/F1/etc): full racing suit zipped to the collar, gloves, boots -- NO bare bones showing
  * ONLY the skull face and bony hands should be visible. The rest of the body is HIDDEN by opaque clothes.
  * Clothes fit like on a real person with proper draping, wrinkles, and fabric weight.
  * OUTFIT LOCK: For each character identity, keep one exact outfit design across all scenes (same base suit, same main colors, same sponsor/logo style language, same accessories). Do not drift outfit style scene-to-scene unless explicitly requested.
- ONE skeleton per scene unless it's a VS/comparison shot (max 2)
- Always FULL BODY visible from head to toe, centered in frame
- EVERY scene the skeleton must be DOING something with ultra-smooth human-like natural motion -- fluid arm gestures, natural head turns, realistic weight and momentum. Zach D Films quality movement. NEVER stiff, robotic, or jerky motion.

BACKGROUND: Solid clean teal/mint green (#5AC8B8) studio backdrop. Smooth gradient lighting. No environments, rooms, or outdoor scenes.

CAMERA AND LIGHTING:
- Professional studio photography lighting: key light from upper-left, fill light from right, rim light on edges
- Slight depth of field blur on background
- Camera is at chest height, slight upward angle (heroic framing)
- Vary camera angle per scene: medium shot, slight close-up, wide establishing, over-shoulder

PROPS AND VISUAL STORYTELLING:
- Money/dollar bills physically floating in the air when discussing earnings (not CGI overlays)
- The skeleton HOLDS relevant props: steering wheel, trophy, briefcase, gold bars, tools of the trade
- In VS scenes: two skeletons face each other with dramatic lighting split between them
- Relevant objects in frame: race cars in miniature, stacks of cash, equipment

MOTION DIRECTION (for animation -- include this in visual_description):
- Describe what MOVES: "skeleton gestures with right hand," "money bills drift slowly downward," "skeleton turns head to face camera"
- Describe the ENERGY: "confident stance, skeleton leans forward assertively" or "skeleton shrugs with palms up"
- ALL motion must be ultra-smooth and human-like with natural weight and follow-through, like a real person moving. Fluid transitions, no snapping between poses.
- Clothing must sway and fold realistically with the body movement showing proper fabric physics
- Eyes must track and shift naturally with subtle micro-movements
- Keep motion SUBTLE and realistic -- no wild jumping or dancing. Zach D Films quality smooth cinematic motion.

STRUCTURE (10 scenes, 45-50 seconds):
1. HOOK: "[A] vs [B] -- who makes more?" plus an immediate numeric stake in the first line (example: "$250M vs $500M over 10 years"). Skeleton looking directly at camera, arms crossed
2. SETUP: Context scene. Both skeletons in their outfits facing each other
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
      "visual_description": "A skeleton character (skull head, skeleton hands, but body fully covered by clothes) wearing [EXACT DETAILED OUTFIT that covers the ENTIRE body: e.g. a navy blue pilot uniform with gold epaulettes, navy tie, pressed navy pants, black dress shoes -- no bare bones visible except skull and hands]. The skeleton is [EXACT POSE: e.g. standing confidently with arms crossed] and holding [SPECIFIC PROP: e.g. a pilot helmet in right hand]. [Camera angle: e.g. medium shot, slight low angle]. Background: solid clean teal-blue studio. [Motion cue: e.g. skeleton gestures with right hand].",
      "text_overlay": "ONE_WORD"
    }
  ],
  "description": "YouTube description with hashtags",
  "tags": ["tag1", "tag2"]
}

Generate exactly 10 scenes. CRITICAL: EVERY visual_description MUST start with the outfit description FIRST (e.g. "A skeleton character wearing a full navy surgeon's scrubs with stethoscope..."). The outfit is the MOST IMPORTANT part -- it defines WHO the skeleton represents. Keep outfit consistency locked for each character across all 10 scenes unless explicitly instructed otherwise. Never write a bare skeleton without clothing. Each visual_description must be 1-3 sentences (hard max 3), covering outfit lock, pose/props/camera, and motion.""",

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
2. SETUP (Scenes 2-3): Establish the character, their world, and what they want
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
    if extra_instructions:
        system_prompt += extra_instructions
    story_script_to_short_mode = (
        template == "story"
        and "SCRIPT-TO-SHORT MODE (PRE-ALPHA)" in str(extra_instructions or "")
    )
    async def _call_script_gen(prompt_text: str, temp: float = 0.8) -> dict:
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
                        {"role": "user", "content": f"Create a viral short about: {topic}"},
                    ],
                    "temperature": temp,
                },
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            start = content.find("{")
            end = content.rfind("}") + 1
            if start == -1 or end == 0:
                raise ValueError("No JSON found in Grok response")
            return json.loads(content[start:end])

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

    first = await _call_script_gen(system_prompt, temp=0.8)
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
        + "and include camera/motion continuity language in each scene."
        + retention_tuning
    )
    second = await _call_script_gen(hardened_prompt, temp=0.65)
    second_score, _ = _score_story_script_quality(second)
    return second if second_score >= first_score else first


# ─── ElevenLabs TTS ───────────────────────────────────────────────────────────


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
    "dramatic": "fadeblack",
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
    if style in {"dramatic", "blur"}:
        return 0.22
    if style in {"slide", "zoom"}:
        return 0.18
    return 0.16


def _normalize_micro_escalation_mode(value, template: str = "") -> bool:
    if template not in {"skeleton", "story", "motivation"}:
        return False
    if value is None:
        return True
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
        res_w = 1080 if resolution == "1080p" else 720
        res_h = 1920 if resolution == "1080p" else 1280
        is_landscape = False

    skeleton_pro_style = (template == "skeleton" and not is_landscape)

    if skeleton_pro_style:
        font_size = 84 if resolution == "1080p" else 60
        outline = 3 if resolution == "1080p" else 2
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




async def _run_comfyui_workflow(workflow: dict, output_node: str, output_type: str = "images") -> dict:
    """Submit a workflow to ComfyUI and wait for the specified output node to complete."""
    async with httpx.AsyncClient(timeout=900) as client:
        resp = await client.post(f"{COMFYUI_URL}/prompt", json={"prompt": workflow})
        if resp.status_code != 200:
            log.error(f"ComfyUI rejected workflow ({resp.status_code}): {resp.text[:1000]}")
        resp.raise_for_status()
        prompt_id = resp.json()["prompt_id"]

        for poll_i in range(450):
            await asyncio.sleep(2)
            history = await client.get(f"{COMFYUI_URL}/history/{prompt_id}")
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
                if poll_i % 30 == 0 and poll_i > 0:
                    log.info(f"ComfyUI workflow still running... {poll_i * 2}s elapsed")
        raise TimeoutError("ComfyUI workflow timed out after 900s")


async def _download_comfyui_file(file_info: dict, output_path: str):
    """Download a generated file (image or video frame) from ComfyUI."""
    async with httpx.AsyncClient(timeout=120) as client:
        filename = file_info["filename"]
        subfolder = file_info.get("subfolder", "")
        ftype = file_info.get("type", "output")
        url = f"{COMFYUI_URL}/view?filename={filename}&subfolder={subfolder}&type={ftype}"
        resp = await client.get(url)
        with open(output_path, "wb") as f:
            f.write(resp.content)


GROK_IMAGINE_URL = "https://fal.run/xai/grok-imagine-image"


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


async def _generate_image_xai_direct(prompt: str, output_path: str, reference_image_url: str = "") -> dict:
    """Generate image directly via xAI API. No fal.ai needed.
    Returns {"local_path": str, "cdn_url": str}.
    """
    if not XAI_API_KEY:
        raise RuntimeError("XAI_API_KEY not configured")

    headers = {"Authorization": f"Bearer {XAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": XAI_IMAGE_MODEL,
        "prompt": prompt,
        "n": 1,
        "response_format": "url",
        "aspect_ratio": XAI_IMAGE_ASPECT_RATIO,
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
                # If reference-conditioning payload is rejected by provider, retry once without reference.
                if reference_image_url and resp.status_code in (400, 404, 422):
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


async def generate_image_grok(prompt: str, output_path: str, resolution: str = "720p", reference_image_url: str = "") -> dict:
    """Generate an image using Grok Imagine. Tries fal.ai first, falls back to direct xAI API.
    Returns {"local_path": str, "cdn_url": str} so Kling can use the URL directly.
    """
    if USE_FAL_GROK_IMAGE and FAL_AI_KEY:
        try:
            aspect = "9:16"
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
                log.info(f"Fal Grok image conditioning enabled: {'https_url' if reference_image_url.startswith('http') else 'inline_data_url'}")

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

            log.info(f"Grok Imagine (fal.ai) saved: {output_path} ({Path(output_path).stat().st_size / 1024:.0f} KB)")
            gen_id = await _save_training_candidate(prompt, output_path, source="grok_imagine")
            return {"local_path": output_path, "cdn_url": cdn_url, "generation_id": gen_id}
        except Exception as e:
            log.warning(f"Fal.ai Grok Imagine failed, falling back to direct xAI: {e}")

    log.info(f"Using direct xAI API image generation model={XAI_IMAGE_MODEL}")
    return await _generate_image_xai_direct(prompt, output_path, reference_image_url=reference_image_url)


SKELETON_LORA_NAME = "nyptid_skeleton_v1.safetensors"
SKELETON_LORA_STRENGTH = 0.85
SKELETON_TRIGGER_TOKEN = "nyptid_skeleton"
SKELETON_LORA_NEGATIVE = "blurry, low quality, text, watermark, deformed, ugly, bad anatomy, non-skeleton, human skin, flesh, muscles, realistic human, cartoon, anime, painting, 2D, illustration, transparent clothes, see-through clothes, x-ray clothes, invisible fabric, naked skeleton, broken bones, dislocated joints, extra limbs, missing limbs, empty eye sockets, no eyes, hollow eyes, robotic motion, stiff pose, jerky movement"


async def check_skeleton_lora_available() -> bool:
    """Check if the skeleton LoRA exists on the ComfyUI server."""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"{COMFYUI_URL}/object_info/LoraLoader")
            if resp.status_code == 200:
                data = resp.json()
                lora_list = data.get("LoraLoader", {}).get("input", {}).get("required", {}).get("lora_name", [[]])[0]
                return SKELETON_LORA_NAME in lora_list
    except Exception:
        pass
    return False


async def generate_image_skeleton_lora(prompt: str, output_path: str, resolution: str = "720p") -> str:
    """Generate skeleton image using fine-tuned LoRA on ComfyUI SDXL."""
    config = RESOLUTION_CONFIGS[resolution]
    lora_prompt = f"{SKELETON_TRIGGER_TOKEN}, {prompt}"

    workflow = {
        "4": {
            "class_type": "CheckpointLoaderSimple",
            "inputs": {"ckpt_name": "sd_xl_base_1.0.safetensors"},
        },
        "10": {
            "class_type": "LoraLoader",
            "inputs": {
                "lora_name": SKELETON_LORA_NAME,
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
            "inputs": {"text": SKELETON_LORA_NEGATIVE, "clip": ["10", 1]},
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
    log.info(f"Skeleton LoRA image generated: {output_path} ({Path(output_path).stat().st_size / 1024:.0f} KB)")
    return output_path


async def generate_scene_image(
    prompt: str,
    output_path: str,
    resolution: str = "720p",
    negative_prompt: str = "",
    template: str = "",
    reference_image_url: str = "",
) -> dict:
    """Generate a scene image. Priority for skeleton template: LoRA > Grok Imagine > SDXL.
    For other templates: Grok Imagine > SDXL.
    Returns {"local_path": str, "cdn_url": str | None}.
    """
    async def _enforce_1080_image(path: str) -> None:
        if resolution != "1080p":
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

    if template == "skeleton":
        try:
            lora_available = await check_skeleton_lora_available()
            if lora_available:
                await generate_image_skeleton_lora(prompt, output_path, resolution=resolution)
                await _enforce_1080_image(output_path)
                log.info("Skeleton image generated via LoRA (zero API cost)")
                return {"local_path": output_path, "cdn_url": None}
        except Exception as e:
            log.warning(f"Skeleton LoRA generation failed, falling back to Grok Imagine: {e}")

    if FAL_AI_KEY or XAI_API_KEY:
        try:
            if IMAGE_QUALITY_BESTOF_ENABLED and IMAGE_QUALITY_BESTOF_COUNT > 1:
                best_count = max(2, int(IMAGE_QUALITY_BESTOF_COUNT))
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
                        )
                        await _enforce_1080_image(cand_path)
                        qa = _score_generated_image_quality(cand_path, prompt=prompt, template=template)
                        candidates.append({"path": cand_path, "result": cand_result, "qa": qa, "idx": idx})
                        log.info(
                            f"Best-of candidate {idx+1}/{best_count} score={qa.get('score', 0.0)} "
                            f"ok={qa.get('ok', False)} notes={','.join(qa.get('notes', []))}"
                        )
                    except Exception as cand_err:
                        log.warning(f"Best-of candidate {idx+1}/{best_count} failed: {cand_err}")
                if not candidates:
                    raise RuntimeError("all best-of image candidates failed")

                winner = max(candidates, key=lambda c: float(c.get("qa", {}).get("score", 0.0)))
                winner_path = str(winner["path"])
                winner_result = dict(winner["result"])
                winner_qa = dict(winner.get("qa", {}))
                if Path(winner_path).resolve() != Path(output_path).resolve():
                    shutil.copyfile(winner_path, output_path)
                winner_result["local_path"] = output_path
                winner_result["qa_score"] = winner_qa.get("score", 0.0)
                winner_result["qa_ok"] = bool(winner_qa.get("ok", False))
                winner_result["qa_notes"] = winner_qa.get("notes", [])
                # Story salvage pass: if winner is still below threshold, do one extra high-realism attempt.
                if template == "story" and not winner_result["qa_ok"]:
                    try:
                        salvage_prompt = f"{prompt} {STORY_REALISM_REFINEMENT}"
                        salvage_path = str(cand_root.with_name(f"{cand_root.stem}_salvage{cand_root.suffix or '.png'}"))
                        salvage_result = await generate_image_grok(
                            salvage_prompt,
                            salvage_path,
                            resolution=resolution,
                            reference_image_url=reference_image_url,
                        )
                        await _enforce_1080_image(salvage_path)
                        salvage_qa = _score_generated_image_quality(salvage_path, prompt=salvage_prompt, template=template)
                        if float(salvage_qa.get("score", 0.0)) > float(winner_result.get("qa_score", 0.0)):
                            shutil.copyfile(salvage_path, output_path)
                            winner_result = dict(salvage_result)
                            winner_result["local_path"] = output_path
                            winner_result["qa_score"] = salvage_qa.get("score", 0.0)
                            winner_result["qa_ok"] = bool(salvage_qa.get("ok", False))
                            winner_result["qa_notes"] = salvage_qa.get("notes", [])
                            log.info(f"Story salvage image replaced winner, score={winner_result['qa_score']}")
                        Path(salvage_path).unlink(missing_ok=True)
                    except Exception as salvage_err:
                        log.warning(f"Story salvage pass failed: {salvage_err}")
                if not winner_result["qa_ok"]:
                    log.warning(
                        f"Best-of winner below threshold {IMAGE_QUALITY_MIN_SCORE}: "
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
            )
            await _enforce_1080_image(output_path)
            qa = _score_generated_image_quality(output_path, prompt=prompt, template=template)
            result["qa_score"] = qa.get("score", 0.0)
            result["qa_ok"] = bool(qa.get("ok", False))
            result["qa_notes"] = qa.get("notes", [])
            if template == "story" and not result["qa_ok"]:
                try:
                    salvage_prompt = f"{prompt} {STORY_REALISM_REFINEMENT}"
                    salvage_path = str(Path(output_path).with_name(Path(output_path).stem + "_salvage" + Path(output_path).suffix))
                    salvage_result = await generate_image_grok(
                        salvage_prompt,
                        salvage_path,
                        resolution=resolution,
                        reference_image_url=reference_image_url,
                    )
                    await _enforce_1080_image(salvage_path)
                    salvage_qa = _score_generated_image_quality(salvage_path, prompt=salvage_prompt, template=template)
                    if float(salvage_qa.get("score", 0.0)) > float(result.get("qa_score", 0.0)):
                        shutil.copyfile(salvage_path, output_path)
                        result = dict(salvage_result)
                        result["local_path"] = output_path
                        result["qa_score"] = salvage_qa.get("score", 0.0)
                        result["qa_ok"] = bool(salvage_qa.get("ok", False))
                        result["qa_notes"] = salvage_qa.get("notes", [])
                        log.info(f"Story single-pass salvage improved image score={result['qa_score']}")
                    Path(salvage_path).unlink(missing_ok=True)
                except Exception as salvage_err:
                    log.warning(f"Story single-pass salvage failed: {salvage_err}")
            return result
        except Exception as e:
            log.warning(f"Grok image generation failed (fal.ai + xAI direct), falling back to SDXL: {e}")

    await generate_image_comfyui(prompt, output_path, resolution=resolution, negative_prompt=negative_prompt)
    await _enforce_1080_image(output_path)
    qa = _score_generated_image_quality(output_path, prompt=prompt, template=template)
    return {"local_path": output_path, "cdn_url": None, "qa_score": qa.get("score", 0.0), "qa_ok": bool(qa.get("ok", False)), "qa_notes": qa.get("notes", [])}


async def generate_image_comfyui(prompt: str, output_path: str, resolution: str = "720p", negative_prompt: str = "") -> str:
    """Fallback: generate image via ComfyUI SDXL on RunPod."""
    config = RESOLUTION_CONFIGS[resolution]
    neg = negative_prompt or NEGATIVE_PROMPT

    workflow = {
        "3": {
            "class_type": "KSampler",
            "inputs": {
                "seed": random.randint(0, 2**32),
                "steps": 30,
                "cfg": 7.5,
                "sampler_name": "dpmpp_2m",
                "scheduler": "karras",
                "denoise": 1.0,
                "model": ["4", 0],
                "positive": ["6", 0],
                "negative": ["7", 0],
                "latent_image": ["5", 0],
            },
        },
        "4": {
            "class_type": "CheckpointLoaderSimple",
            "inputs": {"ckpt_name": "sd_xl_base_1.0.safetensors"},
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
            "inputs": {"filename_prefix": "nyptid_gen", "images": ["8", 0]},
        },
    }

    if config.get("upscale"):
        workflow["10"] = {
            "class_type": "LatentUpscaleBy",
            "inputs": {
                "samples": ["3", 0],
                "scale_by": config["upscale_factor"],
                "upscale_method": "bislerp",
            },
        }
        workflow["11"] = {
            "class_type": "KSampler",
            "inputs": {
                "seed": random.randint(0, 2**32),
                "steps": 15,
                "cfg": 7.0,
                "sampler_name": "dpmpp_2m",
                "scheduler": "karras",
                "denoise": 0.4,
                "model": ["4", 0],
                "positive": ["6", 0],
                "negative": ["7", 0],
                "latent_image": ["10", 0],
            },
        }
        workflow["8"]["inputs"]["samples"] = ["11", 0]

    result = await _run_comfyui_workflow(workflow, "9", "images")
    await _download_comfyui_file(result["images"][0], output_path)
    return output_path


async def check_wan22_available() -> bool:
    """Check if the Wan 2.2 I2V models exist on the ComfyUI server."""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"{COMFYUI_URL}/object_info")
            if resp.status_code == 200:
                content = resp.text
                return WAN22_I2V_HIGH.split(".")[0] in content or "wan2.2" in content.lower()
    except Exception as e:
        log.warning(f"Wan 2.2 availability check failed: {e}")
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
    """Animate a scene image using Runway first, then FalAI Kling fallback."""
    provider_errors = []
    try:
        requested_duration = float(duration_sec)
    except Exception:
        requested_duration = 5.0
    # FalAI Kling I2V accepts only 5s or 10s durations.
    kling_duration = 10 if requested_duration >= 7.5 else 5

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
    filename = "nyptid_scene_" + str(int(time.time() * 1000)) + ".png"
    img_bytes = Path(image_path).read_bytes()
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            f"{COMFYUI_URL}/upload/image",
            files={"image": (filename, img_bytes, "image/png")},
            data={"overwrite": "true"},
        )
        if resp.status_code != 200:
            raise RuntimeError(f"ComfyUI image upload failed ({resp.status_code}): {resp.text[:200]}")
        result = resp.json()
        uploaded_name = result.get("name", filename)
    log.info(f"Image uploaded to ComfyUI via HTTP: {uploaded_name}")
    return uploaded_name


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
                "negative": ["8", 2],
                "latent_image": ["8", 1],
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

    if RUNPOD_COMPOSITOR_ENABLED:
        try:
            await _composite_video_on_runpod(
                working_clips,
                audio_path,
                output_path,
                subtitle_path=subtitle_path,
                sfx_track=sfx_track,
                transition_style=transition_style,
                clip_durations=working_durations,
            )
            for clip in set(scene_clips + working_clips):
                clip.unlink(missing_ok=True)
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

    has_sfx = sfx_track and Path(sfx_track).exists()

    if subtitle_path and Path(subtitle_path).exists():
        sub_abs = str(Path(subtitle_path).resolve()).replace("\\", "/").replace(":", "\\:")
        if has_sfx:
            cmd = [
                "ffmpeg", "-y",
                "-i", str(merged_video),
                "-i", audio_path,
                "-i", sfx_track,
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
                "-i", sfx_track,
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
        if not visual_description:
            visual_description = narration or f"Scene {idx + 1} visual"
        try:
            duration = float(scene.get("duration_sec", 5))
        except Exception:
            duration = 5.0
        duration = max(3.5, min(duration, 10.0))
        scene["narration"] = narration
        scene["visual_description"] = visual_description
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
STORY_REALISM_REFINEMENT = (
    "Realism lock: photoreal human skin texture, natural pores, physically plausible hands and fingers, "
    "natural eye reflections, grounded facial proportions, realistic fabric micro-detail, and non-plastic materials. "
    "No CGI waxiness, no uncanny face, no extra fingers, no malformed hands."
)


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
    default_on = template in {"skeleton", "story"}
    return _bool_from_any(value, default=default_on)


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
                    "A fully clothed skeleton character wearing a role-accurate outfit from neck to feet appears in a detailed cinematic environment.",
                    "The skeleton takes a clear action pose with a role-specific prop while camera framing emphasizes conflict and readability.",
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
            if "skeleton" not in chunks[0].lower():
                chunks[0] = f"A skeleton character {chunks[0][0].lower() + chunks[0][1:]}" if chunks[0] else \
                    "A skeleton character appears in a cinematic environment."
            if "wearing" not in chunks[0].lower():
                chunks[0] = chunks[0].rstrip(".!?") + " wearing a fully opaque role-accurate outfit from neck to feet."
            if not re.search(r"\b(environment|background|street|city|desert|forest|interior|exterior|battlefield|temple|rome|studio)\b", chunks[0], re.IGNORECASE):
                chunks[0] = chunks[0].rstrip(".!?") + " In a detailed environment matching the scene request."
            if not re.search(r"\b(holding|aiming|running|facing|fighting|action|pose|gestur|turning|pointing)\b", chunks[1], re.IGNORECASE):
                chunks[1] = (chunks[1].rstrip(".!?") + " " if chunks[1] else "") + \
                    "The skeleton takes a clear action pose with a role-specific prop and readable composition."
            if not re.search(r"\b(motion|camera|moves|moving|drift|continuity|momentum)\b", chunks[2], re.IGNORECASE):
                chunks[2] = (chunks[2].rstrip(".!?") + " " if chunks[2] else "") + \
                    "Motion begins instantly with smooth momentum and directional continuity."
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

        scene["visual_description"] = " ".join([c.strip() for c in chunks[:3] if c and c.strip()]).strip()
        compiled.append(scene)
    return compiled


def _build_skeleton_image_prompt(
    visual_description: str,
    skeleton_anchor: str = "",
    quality_mode: str = "cinematic",
    immutable_context: str = "",
) -> str:
    mode = _normalize_skeleton_quality_mode(quality_mode, template="skeleton")
    addon = (SKELETON_CINEMATIC_PROMPT_ADDON + " ") if mode == "cinematic" else ""
    immutable = (str(immutable_context or "").strip() + " ") if immutable_context else ""
    return (
        SKELETON_IMAGE_STYLE_PREFIX + " "
        + SKELETON_MASTER_CONSISTENCY_PROMPT + " "
        + "NON-NEGOTIABLE CHARACTER RULE: both eye sockets contain realistic human-like eyeballs with visible iris and wet reflective highlights in every scene. "
        + addon + immutable + skeleton_anchor + str(visual_description or "").strip() + " "
        + SKELETON_IMAGE_SUFFIX
    )


def _build_story_image_prompt(
    visual_description: str,
    quality_mode: str = "cinematic",
    immutable_context: str = "",
) -> str:
    mode = _normalize_skeleton_quality_mode(quality_mode, template="story")
    addon = (STORY_CINEMATIC_PROMPT_ADDON + " ") if mode == "cinematic" else ""
    immutable = (str(immutable_context or "").strip() + " ") if immutable_context else ""
    return (
        TEMPLATE_PROMPT_PREFIXES.get("story", "") + " "
        + STORY_MASTER_CONSISTENCY_PROMPT + " "
        + addon + immutable + str(visual_description or "").strip()
    ).strip()


def _apply_template_scene_constraints(scenes: list, template: str, quality_mode: str = "standard") -> list:
    """Harden model output so template-critical constraints cannot drift."""
    if template not in {"skeleton", "story"}:
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
                    "A skeleton character wearing a fully opaque role-accurate outfit from neck to feet stands centered in frame.",
                    "The skeleton holds a role-specific prop in a cinematic environment matching the scene request.",
                    "Motion cue: smooth natural arm and head movement with realistic fabric sway.",
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
            first = chunks[0]
            if "skeleton" not in first.lower():
                if first:
                    first = f"A skeleton character {first[0].lower() + first[1:]}"
                else:
                    first = "A skeleton character is centered in frame."
            if "wearing" not in first.lower():
                first = first.rstrip(".!?") + " wearing a full opaque outfit from neck to feet."
            chunks[0] = first

            if len(chunks) < 2:
                chunks.append("The skeleton holds a role-specific prop in a cinematic environment that matches the scene request.")
            if len(chunks) < 3:
                chunks.append("Motion cue: smooth natural arm and head movement with realistic fabric sway.")

            # Hard default for Skeleton AI: eyeballs must always be present.
            eyes_ok = any(
                re.search(r"\b(eye|eyes|eyeball|eyeballs|iris|pupil|sockets?)\b", c, re.IGNORECASE)
                for c in chunks[:3]
            )
            if not eyes_ok:
                chunks[0] = chunks[0].rstrip(".!?") + " with realistic visible eyeballs in both eye sockets."
            elif not re.search(r"\beyeball|eyeballs|iris|pupil\b", chunks[0], re.IGNORECASE):
                chunks[0] = chunks[0].rstrip(".!?") + " Keep realistic eyeballs with visible iris detail."
        else:
            first = chunks[0]
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
                chunks[1] = chunks[1].rstrip(".!?") + " Dynamic 24-35mm cinematic framing with low-angle depth and volumetric rim lighting."
            if len(chunks) < 3:
                chunks.append("Motion starts instantly with smooth human-like momentum and carries continuity into the next shot.")
            elif not re.search(r"\b(motion|moves|moving|turns|gestures|drift|camera|continuity)\b", chunks[2], re.IGNORECASE):
                chunks[2] = chunks[2].rstrip(".!?") + " Motion starts instantly with smooth momentum and clear directional continuity."

        scene["visual_description"] = " ".join(chunks[:3]).strip()
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
                asyncio.create_task(_refund_generation_credit(user_id, source, month_key=month_key))
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
        reference_image_url = str(job_state.get("reference_image_url", "") or "").strip()
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
        use_video = use_video_engine
        if template == "reddit":
            use_video = False
        if template != "reddit" and not use_video_engine:
            raise RuntimeError("Video is required but no engine is configured (set RUNWAY_API_KEY or FAL_AI_KEY)")
        if runway_video_enabled:
            mode_label = "Runway (primary)"
        elif fal_video_enabled:
            mode_label = "FalAI Kling 2.1"
        else:
            mode_label = "static image"
        jobs[job_id]["generation_mode"] = "video" if use_video_engine else "image"

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
            skeleton_reference_image_url = reference_image_url or SKELETON_GLOBAL_REFERENCE_IMAGE_URL
        if template == "skeleton" and scenes:
            s1_desc = scenes[0].get("visual_description", "")
            outfit_match = re.search(r'[Ww]earing\s+(.{20,200}?)(?:\.|,\s*(?:standing|holding|facing|looking|posed))', s1_desc)
            if outfit_match:
                skeleton_anchor = f"CONSISTENCY ANCHOR -- every skeleton in this video wears: {outfit_match.group(1).strip()}. "

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
            img_result = await generate_scene_image(
                full_prompt,
                img_path,
                resolution=resolution,
                negative_prompt=neg_prompt,
                template=template,
                reference_image_url=skeleton_reference_image_url if template == "skeleton" else reference_image_url,
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


@app.post("/api/creative/script")
async def creative_generate_script(req: GenerateRequest, request: Request = None):
    """Phase 1: Generate script + scenes for user review. Returns editable scene list."""
    user = await get_current_user_from_request(request) if request else None
    if not user:
        raise HTTPException(401, "Auth required")
    if not _user_has_paid_access(user):
        raise HTTPException(402, "Active subscription required. Please choose a plan.")
    _ensure_template_allowed(req.template, user)
    quality_mode = _normalize_skeleton_quality_mode(req.quality_mode, template=req.template)
    mint_mode = _normalize_mint_mode(req.mint_mode, template=req.template)
    art_style = _normalize_art_style(req.art_style, template=req.template)
    reference_lock_mode = _normalize_reference_lock_mode(req.reference_lock_mode, default="strict")
    transition_style = _normalize_transition_style(req.transition_style)
    micro_escalation_mode = _normalize_micro_escalation_mode(req.micro_escalation_mode, template=req.template)
    voice_id = str(req.voice_id or "").strip()
    voice_speed = _normalize_voice_speed(req.voice_speed, default=1.0)
    pacing_mode = _normalize_pacing_mode(req.pacing_mode)
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
            "\n\nSCRIPT-TO-SHORT MODE (PRE-ALPHA): The user input is already a full script. "
            "Do not invent a different story. Keep the original intent and wording as much as possible while adapting for timing. "
            f"Split into {scene_range} scenes with clear visual progression and strong retention pacing. "
            "Do not force a single unchanged main character in every scene; match subjects to each script beat. "
            "Each scene must have concise narration and a cinematic visual_description that can be directly rendered."
        )
    resolution = _normalize_output_resolution(req.resolution, priority_allowed=False)
    script_data = await generate_script(req.template, req.prompt, extra_instructions=(lang_instruction + script_to_short_instruction))
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
            "transition_style": transition_style,
            "micro_escalation_mode": micro_escalation_mode,
            "voice_id": voice_id,
            "voice_speed": voice_speed,
            "pacing_mode": pacing_mode,
            "reference_image_url": "",
            "reference_lock_mode": reference_lock_mode,
            "reference_dna": {},
            "reference_quality": {},
            "rolling_reference_image_url": "",
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
        "transition_style": transition_style,
        "micro_escalation_mode": micro_escalation_mode,
        "voice_id": voice_id,
        "voice_speed": voice_speed,
        "pacing_mode": pacing_mode,
        "reference_lock_mode": reference_lock_mode,
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
    voice_id = str(body.get("voice_id", "") or "").strip()
    voice_speed = _normalize_voice_speed(body.get("voice_speed", 1.0), default=1.0)
    pacing_mode = _normalize_pacing_mode(body.get("pacing_mode", "standard"))
    if template != "story" or not STORY_ADVANCED_CONTROLS_ENABLED:
        voice_id = ""
        voice_speed = 1.0
        pacing_mode = "standard"
    reference_lock_mode = _normalize_reference_lock_mode(body.get("reference_lock_mode"), default="strict")
    _ensure_template_allowed(template, user)
    _user_plan, plan_limits = _resolve_user_plan_for_limits(user)
    requested_resolution = body.get("resolution", "720p")
    resolution = _normalize_output_resolution(requested_resolution, priority_allowed=bool(plan_limits.get("priority", False)))
    story_animation_enabled = _bool_from_any(body.get("story_animation_enabled"), True)
    if template != "story" or resolution != "720p":
        story_animation_enabled = True
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
            "transition_style": transition_style,
            "micro_escalation_mode": micro_escalation_mode,
            "voice_id": voice_id,
            "voice_speed": voice_speed,
            "pacing_mode": pacing_mode,
            "story_animation_enabled": story_animation_enabled,
            "reference_image_url": "",
            "reference_lock_mode": reference_lock_mode,
            "reference_dna": {},
            "reference_quality": {},
            "rolling_reference_image_url": "",
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
        "voice_id": voice_id,
        "voice_speed": voice_speed,
        "pacing_mode": pacing_mode,
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
        "transition_style": transition_style,
        "micro_escalation_mode": micro_escalation_mode,
        "voice_id": voice_id,
        "voice_speed": voice_speed,
        "pacing_mode": pacing_mode,
        "reference_lock_mode": reference_lock_mode,
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
        "has_reference_image": bool(session.get("reference_image_url") or session.get("skeleton_reference_image")),
        "reference_lock_mode": _normalize_reference_lock_mode(session.get("reference_lock_mode"), "strict"),
        "reference_quality": session.get("reference_quality", {}),
        "template": session.get("template", ""),
        "quality_mode": _normalize_skeleton_quality_mode(session.get("quality_mode"), template=session.get("template", "")),
        "mint_mode": _normalize_mint_mode(session.get("mint_mode"), template=session.get("template", "")),
        "art_style": _normalize_art_style(session.get("art_style", "auto"), template=session.get("template", "")),
        "transition_style": _normalize_transition_style(session.get("transition_style", "smooth")),
        "micro_escalation_mode": _normalize_micro_escalation_mode(session.get("micro_escalation_mode"), template=session.get("template", "")),
        "topic": session.get("topic", ""),
        "scene_count": len(session.get("scenes", [])),
        "story_animation_enabled": _bool_from_any(session.get("story_animation_enabled"), True),
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
    reference_lock_mode = _normalize_reference_lock_mode(req.reference_lock_mode or session.get("reference_lock_mode"), "strict")
    session["reference_lock_mode"] = reference_lock_mode
    session["art_style"] = art_style
    _ensure_reference_public_url(req.session_id, session)
    user_plan, plan_limits = _resolve_user_plan_for_limits(user)
    resolution = _normalize_output_resolution(session.get("resolution", req.resolution), priority_allowed=bool(plan_limits.get("priority", False)))
    neg_prompt = TEMPLATE_NEGATIVE_PROMPTS.get(template, NEGATIVE_PROMPT)
    constrained_prompt = req.prompt
    if template == "skeleton":
        constrained_prompt = _apply_template_scene_constraints(
            [{"visual_description": req.prompt}],
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
    scene_reference = _resolve_reference_for_scene(session, template, req.scene_index)

    img_path = str(TEMP_DIR / f"{req.session_id}_scene_{req.scene_index}.png")
    try:
        img_result = await generate_scene_image(
            full_prompt,
            img_path,
            resolution=resolution,
            negative_prompt=neg_prompt,
            template=template,
            reference_image_url=scene_reference,
        )
    except Exception as e:
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
            raise HTTPException(
                500,
                "Scene image generation failed. Please regenerate this scene.",
            ) from e
    if template == "skeleton" and req.scene_index == 0 and not (session.get("skeleton_reference_image") or session.get("reference_image_url")):
        session["skeleton_reference_image"] = _file_to_data_image_url(img_path)
    if reference_lock_mode == "strict" or not session.get("rolling_reference_image_url"):
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
        session["scenes"].append({"narration": "", "visual_description": "", "duration_sec": 5})
    session["scenes"][req.scene_index]["visual_description"] = constrained_prompt
    session["quality_mode"] = quality_mode
    session["mint_mode"] = mint_mode
    session["art_style"] = art_style
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
    transition_style = _normalize_transition_style(req.transition_style or session.get("transition_style"))
    micro_escalation_mode = _normalize_micro_escalation_mode(
        req.micro_escalation_mode if req.micro_escalation_mode is not None else session.get("micro_escalation_mode"),
        template=session.get("template", req.template),
    )
    voice_id = str(req.voice_id or session.get("voice_id", "") or "").strip()
    voice_speed = _normalize_voice_speed(req.voice_speed if req.voice_speed is not None else session.get("voice_speed", 1.0), default=1.0)
    pacing_mode = _normalize_pacing_mode(req.pacing_mode or session.get("pacing_mode", "standard"))
    subtitles_enabled = _bool_from_any(req.subtitles_enabled, _bool_from_any(session.get("subtitles_enabled"), True))
    if session.get("template", req.template) != "story" or not STORY_ADVANCED_CONTROLS_ENABLED:
        voice_id = ""
        voice_speed = 1.0
        pacing_mode = "standard"
    reference_lock_mode = _normalize_reference_lock_mode(req.reference_lock_mode or session.get("reference_lock_mode"), "strict")
    story_animation_enabled = _bool_from_any(req.story_animation_enabled, _bool_from_any(session.get("story_animation_enabled"), True))
    if session.get("template") != "story":
        story_animation_enabled = True
    async with _creative_sessions_lock:
        session["quality_mode"] = quality_mode
        session["mint_mode"] = mint_mode
        session["art_style"] = art_style
        session["transition_style"] = transition_style
        session["micro_escalation_mode"] = micro_escalation_mode
        session["voice_id"] = voice_id
        session["voice_speed"] = voice_speed
        session["pacing_mode"] = pacing_mode
        session["subtitles_enabled"] = subtitles_enabled
        session["reference_lock_mode"] = reference_lock_mode
        session["story_animation_enabled"] = story_animation_enabled
        _save_creative_sessions_to_disk()
    if not session["scenes"]:
        raise HTTPException(400, "No scenes provided")

    user_plan, plan_limits = _resolve_user_plan_for_limits(user)
    is_admin = user.get("email", "") in ADMIN_EMAILS
    billing_active = _billing_active_for_user(user)
    can_render, credit_source, credit_state = await _reserve_generation_credit(
        user,
        user_plan if not is_admin else "pro",
        billing_active,
        is_admin=is_admin,
    )
    if not can_render:
        raise HTTPException(
            402,
            "No generation credits left this month. Buy a top-up pack to continue.",
        )
    resolution = _normalize_output_resolution(session.get("resolution", req.resolution), priority_allowed=bool(plan_limits.get("priority", False)))
    if resolution != "720p":
        story_animation_enabled = True
        session["story_animation_enabled"] = True
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
        "resolution": resolution,
        "plan": user_plan,
        "user_id": user.get("id"),
        "created_at": time.time(),
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
        voice_id = str(session.get("voice_id", "") or "").strip()
        voice_speed = _normalize_voice_speed(session.get("voice_speed", 1.0), default=1.0)
        pacing_mode = _normalize_pacing_mode(session.get("pacing_mode", "standard"))
        subtitles_enabled = _bool_from_any(session.get("subtitles_enabled"), True)
        scenes = _normalize_scenes_for_render(session["scenes"])
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
        story_animation_enabled = _bool_from_any(session.get("story_animation_enabled"), True)
        if template == "story" and resolution == "720p" and not story_animation_enabled:
            use_video = False
        else:
            use_video = use_video_engine
        if use_video and not use_video_engine:
            raise RuntimeError("Video is required but no engine is configured (set RUNWAY_API_KEY or FAL_AI_KEY)")
        jobs[job_id]["story_animation_enabled"] = bool(story_animation_enabled)
        gen_ts = str(int(time.time() * 1000))

        _job_set_stage(job_id, "generating_images", 10)
        jobs[job_id]["total_scenes"] = len(scenes)

        neg_prompt = TEMPLATE_NEGATIVE_PROMPTS.get(template, NEGATIVE_PROMPT)
        scene_assets = []
        total_steps = len(scenes) * (2 if use_video else 1)
        skeleton_anchor = ""
        if template == "skeleton" and scenes:
            s1_desc = str(scenes[0].get("visual_description", "") or "")
            outfit_match = re.search(r'[Ww]earing\s+(.{20,200}?)(?:\.|,\s*(?:standing|holding|facing|looking|posed))', s1_desc)
            if outfit_match:
                skeleton_anchor = f"CONSISTENCY ANCHOR -- every skeleton in this video wears: {outfit_match.group(1).strip()}. "

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
                img_result = await generate_scene_image(
                    full_prompt,
                    img_path,
                    resolution=resolution,
                    negative_prompt=neg_prompt,
                    template=template,
                    reference_image_url=_resolve_reference_for_scene(session, template, i),
                )
                if template == "skeleton" and not (session.get("reference_image_url") or session.get("skeleton_reference_image")) and i == 0:
                    skeleton_seed = _file_to_data_image_url(img_path)
                    if skeleton_seed:
                        session["skeleton_reference_image"] = skeleton_seed
                if reference_lock_mode == "strict" or not session.get("rolling_reference_image_url"):
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
    wan_ready = await check_wan22_available()
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
        "kling_enabled": bool(FAL_AI_KEY),
        "wan22_ready": wan_ready,
        "video_engine": video_engine,
        "runway_key_configured": runway_video_enabled,
        "runway_key_source": RUNWAY_API_KEY_SOURCE if runway_video_enabled else "",
        "runway_video_model": RUNWAY_VIDEO_MODEL if runway_video_enabled else "",
        "comfyui_url": COMFYUI_URL[:50],
        "skeleton_lora": skeleton_lora,
        "image_engine_skeleton": (
            "Skeleton LoRA (local)"
            if skeleton_lora
            else (f"xAI {XAI_IMAGE_MODEL}" if XAI_API_KEY else "SDXL")
        ),
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
        "stripe_enabled": bool(STRIPE_SECRET_KEY),
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
        "prices": {v: k for k, v in STRIPE_PRICE_TO_PLAN.items()},
        "topup_packs": [
            {"price_id": price_id, **meta}
            for price_id, meta in TOPUP_PACKS.items()
        ],
        "transition_styles": list(TRANSITION_STYLE_MAP.keys()),
        "render_capabilities": {
            "animated_max_resolution": ("720p" if FORCE_720P_ONLY else "1080p"),
            "micro_escalation_supported": True,
            "micro_escalation_max_source_scenes": MICRO_ESCALATION_MAX_SOURCE_SCENES,
            "micro_escalation_max_output_clips": MICRO_ESCALATION_MAX_OUTPUT_CLIPS,
        },
        "feature_flags": {
            "script_to_short_enabled": SCRIPT_TO_SHORT_ENABLED,
            "story_advanced_controls_enabled": STORY_ADVANCED_CONTROLS_ENABLED,
            "story_retention_tuning_enabled": STORY_RETENTION_TUNING_ENABLED,
            "disable_all_sfx": DISABLE_ALL_SFX,
        },
    }


@app.get("/api/me")
async def get_me(user: dict = Depends(require_auth)):
    plan = str(user.get("plan", "none") or "none")
    if plan == "free":
        plan = "none"
    email = user.get("email", "")
    is_admin = email in ADMIN_EMAILS
    billing_active = _billing_active_for_user(user)
    if (not is_admin) and (not billing_active):
        # Unpaid accounts should present as no active plan.
        plan = "none"
    elif (not is_admin) and plan not in PLAN_LIMITS:
        # Paid account with missing/unknown stored plan defaults to paid starter tier.
        plan = "starter"
    if is_admin:
        limits = PLAN_LIMITS["pro"]
        limits = {**limits, "videos_per_month": 9999}
    else:
        limits = PLAN_LIMITS.get(plan, PLAN_LIMITS["starter"])
    credit_state = _credit_state_for_user(user, plan if not is_admin else "pro", billing_active, is_admin=is_admin)
    has_demo = is_admin or (PRODUCT_DEMO_PUBLIC_ENABLED and plan == "demo_pro")
    effective_plan = "pro" if is_admin else plan
    features = _plan_features_for(effective_plan, is_admin=is_admin)
    return {
        "id": user["id"],
        "email": email,
        "plan": effective_plan,
        "role": "admin" if is_admin else "user",
        "billing_active": billing_active,
        "limits": limits,
        "features": features,
        "monthly_credits_remaining": credit_state["monthly_remaining"],
        "monthly_credits_used": credit_state["monthly_used"],
        "monthly_credits_limit": credit_state["monthly_limit"],
        "topup_credits_remaining": credit_state["topup_credits"],
        "credits_total_remaining": credit_state["credits_total_remaining"],
        "requires_topup": credit_state["requires_topup"],
        "credit_month": credit_state["month_key"],
        "demo_access": has_demo,
        "demo_price_id": DEMO_PRO_PRICE_ID,
        "demo_coming_soon": (not PRODUCT_DEMO_PUBLIC_ENABLED),
    }


def _user_has_paid_access(user: dict | None) -> bool:
    if not user:
        return False
    email = str(user.get("email", "") or "")
    if email in ADMIN_EMAILS:
        return True
    return _billing_active_for_user(user)


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
    if req.template != "story" or not STORY_ADVANCED_CONTROLS_ENABLED:
        voice_id = ""
        voice_speed = 1.0
        pacing_mode = "standard"
    reference_lock_mode = _normalize_reference_lock_mode(req.reference_lock_mode, default="strict")
    reference_image_url = str(req.reference_image_url or "").strip()
    reference_dna = {}
    if reference_image_url.startswith("data:image/"):
        raw_ref, _mime = _decode_data_image_url(reference_image_url)
        if raw_ref:
            quality = _analyze_reference_quality(raw_ref, lock_mode=reference_lock_mode)
            if not quality.get("accepted", True) and reference_lock_mode == "strict":
                raise HTTPException(400, "Reference image quality too low for Strict lock. Upload a higher-resolution image or use Inspired lock.")
            reference_dna = _extract_reference_dna(raw_ref, template=req.template)
    user_plan = "starter"
    plan_limits = PLAN_LIMITS["starter"]
    is_admin = False
    if user:
        user_plan, plan_limits = _resolve_user_plan_for_limits(user)
        is_admin = user.get("email", "") in ADMIN_EMAILS
        billing_active = _billing_active_for_user(user)
        can_render, credit_source, credit_state = await _reserve_generation_credit(
            user,
            user_plan if not is_admin else "pro",
            billing_active,
            is_admin=is_admin,
        )
        if not can_render:
            raise HTTPException(
                402,
                "No generation credits left this month. Buy a top-up pack to continue.",
            )
    else:
        credit_source = ""
        credit_state = {"month_key": _month_key()}
    transition_style = _normalize_transition_style(req.transition_style)
    micro_escalation_mode = _normalize_micro_escalation_mode(req.micro_escalation_mode, template=req.template)

    resolution = _normalize_output_resolution(req.resolution, priority_allowed=bool(plan_limits.get("priority", False)))

    job_id = f"{int(time.time())}_{random.randint(1000, 9999)}"
    jobs[job_id] = {
        "status": "queued",
        "progress": 0,
        "template": req.template,
        "topic": req.prompt,
        "resolution": resolution,
        "plan": user_plan,
        "user_id": user.get("id") if user else None,
        "created_at": time.time(),
        "quality_mode": quality_mode,
        "mint_mode": mint_mode,
        "art_style": art_style,
        "transition_style": transition_style,
        "micro_escalation_mode": micro_escalation_mode,
        "voice_id": voice_id,
        "voice_speed": voice_speed,
        "pacing_mode": pacing_mode,
        "reference_image_url": reference_image_url,
        "reference_lock_mode": reference_lock_mode,
        "reference_dna": reference_dna,
        "credit_charged": bool(user),
        "credit_source": credit_source,
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
            "voice_id": voice_id,
            "voice_speed": voice_speed,
            "pacing_mode": pacing_mode,
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
    reference_image_url = str(state.get("reference_image_url", "") or "").strip()
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
        first_scene = scene_images[0] if isinstance(scene_images[0], dict) else {}
        s1_desc = str(first_scene.get("visual_description", "") or "")
        outfit_match = re.search(r'[Ww]earing\s+(.{20,200}?)(?:\.|,\s*(?:standing|holding|facing|looking|posed))', s1_desc)
        if outfit_match:
            skeleton_anchor = f"CONSISTENCY ANCHOR -- every skeleton in this video wears: {outfit_match.group(1).strip()}. "
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
    img_result = await generate_scene_image(
        full_prompt,
        out_path,
        resolution="720p",
        negative_prompt=neg_prompt,
        template=template,
        reference_image_url=reference_image_url,
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
- "skeleton" = 3D skeleton characters wearing topic-relevant outfits on teal/green studio background. VS comparisons, career/earnings breakdowns. One-word bold captions. Example: "NASCAR vs F1 Driver Who Makes More Money"
- "history" = Epic cinematic historical scenes. Battles, empires, ancient events. Dramatic narrator, god rays, film grain. 2-4 word caption phrases. Example: "What Happened to the Roman Legion That Vanished"
- "story" = Cinematic AI visual stories with emotional arc. Pixar/UE5 quality. Consistent character across scenes. Poetic narration. Minimal captions. Example: "The Last Lighthouse Keeper"
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


async def analyze_viral_video(topic: str, video_description: str, transcript_hint: str = "") -> dict:
    user_parts = []
    user_parts.append("Source viral video context: " + video_description)
    if transcript_hint:
        user_parts.append("Audio/timing info from source: " + transcript_hint)
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


async def run_clone_pipeline(job_id: str, topic: str, video_path: str | None, resolution: str = "720p"):
    try:
        _job_set_stage(job_id, "analyzing", 5)
        log.info(f"[{job_id}] Clone: analyzing viral video for topic '{topic}'")

        video_context = topic
        transcript_hint = ""
        meta = {}
        if video_path:
            meta = await extract_video_metadata(video_path) or {}
            if meta:
                video_context = (
                    "Source video file uploaded: "
                    + str(meta.get("duration_sec", "?")) + "s long, "
                    + str(meta.get("width", "?")) + "x" + str(meta.get("height", "?")) + " resolution"
                )
            audio_path = await extract_audio_from_video(video_path)
            if audio_path:
                transcript_hint = await transcribe_audio_with_grok(audio_path)
                Path(audio_path).unlink(missing_ok=True)

        analysis = await analyze_viral_video(topic, video_context, transcript_hint)
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
            s1_desc = scenes[0].get("visual_description", "")
            outfit_match = re.search(r'[Ww]earing\s+(.{20,200}?)(?:\.|,\s*(?:standing|holding|facing|looking|posed))', s1_desc)
            if outfit_match:
                clone_skeleton_anchor = f"CONSISTENCY ANCHOR -- every skeleton in this video wears: {outfit_match.group(1).strip()}. "

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
        "resolution": res,
        "created_at": time.time(),
    }

    try:
        await enqueue_generation_job(job_id, "starter", run_clone_pipeline, (job_id, topic, video_path, res))
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


@app.post("/api/checkout")
async def create_checkout(req: CheckoutRequest, user: dict = Depends(require_auth)):
    if not STRIPE_SECRET_KEY:
        raise HTTPException(500, "Stripe not configured")
    if req.price_id not in STRIPE_PRICE_TO_PLAN:
        raise HTTPException(400, "Invalid price ID")
    target_plan = STRIPE_PRICE_TO_PLAN.get(req.price_id, "")
    user_email = user.get("email", "")
    user_plan = user.get("plan", "starter")
    is_admin = user_email in ADMIN_EMAILS
    if target_plan == "demo_pro" and (not PRODUCT_DEMO_PUBLIC_ENABLED) and (not is_admin):
        raise HTTPException(403, "Demo Pro is coming soon.")

    try:
        checkout_payload = {
            "mode": "subscription",
            "payment_method_types": ["card"],
            "line_items": [{"price": req.price_id, "quantity": 1}],
            "success_url": f"{SITE_URL}?payment=success",
            "cancel_url": f"{SITE_URL}?payment=cancelled",
            "client_reference_id": user["id"],
            "metadata": {"user_id": user["id"], "plan": STRIPE_PRICE_TO_PLAN[req.price_id]},
        }
        customer_id = _stripe_find_customer_id_by_email(user["email"])
        if customer_id:
            checkout_payload["customer"] = customer_id
        else:
            checkout_payload["customer_email"] = user["email"]
        session = stripe_lib.checkout.Session.create(**checkout_payload)
        return {"checkout_url": session.url}
    except Exception as e:
        log.error(f"Stripe checkout error: {e}")
        raise HTTPException(500, f"Payment error: {str(e)}")


@app.post("/api/checkout/topup")
async def create_topup_checkout(req: TopupCheckoutRequest, user: dict = Depends(require_auth)):
    if not STRIPE_SECRET_KEY:
        raise HTTPException(500, "Stripe not configured")
    pack = TOPUP_PACKS.get(req.price_id)
    if not pack:
        raise HTTPException(400, "Invalid top-up pack")
    if user.get("email", "") in ADMIN_EMAILS:
        raise HTTPException(400, "Admin account does not require top-up packs")
    try:
        checkout_payload = {
            "mode": "payment",
            "payment_method_types": ["card"],
            "line_items": [{"price": req.price_id, "quantity": 1}],
            "success_url": f"{SITE_URL}?topup=success",
            "cancel_url": f"{SITE_URL}?topup=cancelled",
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
        session = stripe_lib.checkout.Session.create(**checkout_payload)
        return {"checkout_url": session.url}
    except Exception as e:
        log.error(f"Stripe top-up checkout error: {e}")
        raise HTTPException(500, f"Payment error: {str(e)}")


@app.post("/api/billing-portal")
async def create_billing_portal_session(user: dict = Depends(require_auth)):
    """Create a Stripe customer portal session so users can cancel/manage plans."""
    if not STRIPE_SECRET_KEY:
        raise HTTPException(500, "Stripe not configured")
    customer_id = _stripe_find_customer_id_by_email(user.get("email", ""))
    if not customer_id:
        raise HTTPException(404, "No billing profile found yet. Start a subscription first.")
    try:
        portal = stripe_lib.billing_portal.Session.create(
            customer=customer_id,
            return_url=f"{SITE_URL}?billing=updated",
        )
        return {"portal_url": portal.url}
    except Exception as e:
        log.error(f"Stripe billing portal error: {e}")
        raise HTTPException(500, "Could not open billing portal")


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
        elif mode == "payment":
            topup_credits = int(str(metadata.get("topup_credits", "0") or "0"))
            if user_id and topup_credits > 0:
                await _credit_topup_wallet(
                    user_id=user_id,
                    credits=topup_credits,
                    source=str(metadata.get("topup_pack", "topup") or "topup"),
                    stripe_session_id=str(session_data.get("id", "") or ""),
                )
                log.info(f"Stripe webhook: credited {topup_credits} top-up credits to {user_id}")

    elif event.get("type") == "customer.subscription.deleted":
        sub = event["data"]["object"]
        customer_email = str(sub.get("customer_email", "") or "")
        customer_id = str(sub.get("customer", "") or "")
        if not customer_email and customer_id and STRIPE_SECRET_KEY:
            try:
                customer = stripe_lib.Customer.retrieve(customer_id)
                customer_email = str(getattr(customer, "email", "") or "")
            except Exception as e:
                log.warning(f"Failed to resolve Stripe customer email from {customer_id}: {e}")
        if customer_email:
            try:
                target_id = await _supabase_find_user_id_by_email(customer_email)
                if target_id:
                    await _supabase_set_user_plan(target_id, "none")
                    log.info(f"Subscription cancelled; downgraded {customer_email} -> none")
                else:
                    log.warning(f"Subscription cancelled but no Supabase user found for {customer_email}")
            except Exception as e:
                log.error(f"Failed to downgrade cancelled subscription for {customer_email}: {e}")
        else:
            log.warning("Subscription cancelled event received without resolvable customer email")

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

THUMBNAIL_DIR = Path("thumbnails")
THUMBNAIL_DIR.mkdir(exist_ok=True)
THUMBNAIL_UPLOAD_DIR = THUMBNAIL_DIR / "library"
THUMBNAIL_UPLOAD_DIR.mkdir(exist_ok=True)
THUMBNAIL_OUTPUT_DIR = THUMBNAIL_DIR / "generated"
THUMBNAIL_OUTPUT_DIR.mkdir(exist_ok=True)

THUMBNAIL_RUNPOD_HOST = os.getenv("THUMBNAIL_RUNPOD_HOST", "root@69.30.85.41")
THUMBNAIL_RUNPOD_SSH_PORT = os.getenv("THUMBNAIL_RUNPOD_SSH_PORT", "22118")
RUNPOD_SSH = f"ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -p {THUMBNAIL_RUNPOD_SSH_PORT} {THUMBNAIL_RUNPOD_HOST}"
RUNPOD_TRAINING_DIR = "/workspace/thumbnail_training/images"
LORA_NAME = "nyptid_thumbnails.safetensors"


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


async def check_lora_status() -> dict:
    """Check if the LoRA trainer is running and if a trained LoRA exists on RunPod."""
    local_count = len([p for p in THUMBNAIL_UPLOAD_DIR.iterdir() if p.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}]) if THUMBNAIL_UPLOAD_DIR.exists() else 0
    try:
        cmd = (
            f"{RUNPOD_SSH} '"
            "ls -la /workspace/ComfyUI/models/loras/nyptid_thumbnails.safetensors 2>/dev/null; "
            "cat /workspace/thumbnail_training/output/training_state.json 2>/dev/null; "
            "test -f /workspace/thumbnail_training/output/training.lock && echo TRAINING_ACTIVE || echo TRAINING_IDLE; "
            "ls /workspace/thumbnail_training/images/ 2>/dev/null | wc -l"
            "'"
        )
        proc = await asyncio.create_subprocess_shell(
            cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        output = stdout.decode()

        has_lora = "nyptid_thumbnails.safetensors" in output and "No such file" not in output
        is_training = "TRAINING_ACTIVE" in output
        lines = output.strip().split("\n")
        image_count = 0
        for line in lines:
            if line.strip().isdigit():
                image_count = int(line.strip())

        state = {}
        try:
            for line in lines:
                if line.strip().startswith("{"):
                    state = json.loads(line.strip())
                    break
        except Exception:
            pass

        return {
            "lora_available": has_lora,
            "is_training": is_training,
            "total_images": image_count,
            "local_library_images": local_count,
            "trained_images": state.get("image_count", 0),
            "version": state.get("version", 0),
            "last_train": state.get("last_train", 0),
        }
    except Exception as e:
        log.warning(f"LoRA status check failed: {e}")
        return {
            "lora_available": False,
            "is_training": False,
            "total_images": 0,
            "local_library_images": local_count,
            "trained_images": 0,
            "version": 0,
            "last_train": 0,
        }


@app.get("/api/thumbnails/training-status")
async def training_status(user: dict = Depends(require_auth)):
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    return await check_lora_status()


@app.post("/api/thumbnails/sync-library")
async def sync_thumbnail_library(user: dict = Depends(require_auth)):
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    files = [p for p in THUMBNAIL_UPLOAD_DIR.iterdir() if p.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}]
    if not files:
        return {"status": "no_files", "queued": 0, "synced": 0, "failed": 0}

    synced = 0
    failed = 0
    failed_files = []
    for p in files:
        ok, err = await sync_thumbnail_to_runpod(str(p))
        if ok:
            synced += 1
        else:
            failed += 1
            failed_files.append({"file": p.name, "error": err[:180]})

    status = "complete" if failed == 0 else ("partial" if synced > 0 else "failed")
    log.info(f"Thumbnail library sync finished: status={status}, synced={synced}, failed={failed}, total={len(files)}")
    return {
        "status": status,
        "queued": len(files),
        "synced": synced,
        "failed": failed,
        "failed_files": failed_files[:10],
    }


@app.post("/api/thumbnails/upload")
async def upload_thumbnails(
    files: list[UploadFile] = File(...),
    background_tasks: BackgroundTasks = None,
    user: dict = Depends(require_auth),
):
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    saved = []
    for file in files:
        if not file.filename:
            continue
        ext = Path(file.filename).suffix.lower()
        if ext not in {".png", ".jpg", ".jpeg", ".webp"}:
            continue
        ts = int(time.time() * 1000)
        safe_name = str(ts) + "_" + str(random.randint(1000, 9999)) + ext
        dest = THUMBNAIL_UPLOAD_DIR / safe_name
        with open(dest, "wb") as f:
            while chunk := await file.read(1024 * 1024):
                f.write(chunk)
        saved.append({
            "id": safe_name,
            "name": file.filename,
            "size": dest.stat().st_size,
            "url": "/api/thumbnails/library/" + safe_name,
        })
        if background_tasks:
            background_tasks.add_task(sync_thumbnail_to_runpod, str(dest))
    return {"uploaded": len(saved), "files": saved}


@app.get("/api/thumbnails/library")
async def list_thumbnails(user: dict = Depends(require_auth)):
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    files = []
    for f in sorted(THUMBNAIL_UPLOAD_DIR.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
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
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    path = THUMBNAIL_UPLOAD_DIR / filename
    if not path.exists():
        raise HTTPException(404, "Thumbnail not found")
    mime = "image/png" if path.suffix == ".png" else "image/jpeg" if path.suffix in (".jpg", ".jpeg") else "image/webp"
    return FileResponse(str(path), media_type=mime)


@app.delete("/api/thumbnails/library/{filename}")
async def delete_thumbnail(filename: str, user: dict = Depends(require_auth)):
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    path = THUMBNAIL_UPLOAD_DIR / filename
    if path.exists():
        path.unlink()
    return {"status": "deleted"}


@app.get("/api/thumbnails/generated/{filename}")
async def serve_generated_thumbnail(filename: str, request: Request):
    user = await get_current_user_from_request(request)
    if not user:
        raise HTTPException(401, "Authentication required")
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    path = THUMBNAIL_OUTPUT_DIR / filename
    if not path.exists():
        raise HTTPException(404, "Generated thumbnail not found")
    mime = "image/png"
    return FileResponse(str(path), media_type=mime)


THUMBNAIL_ANALYSIS_PROMPT = """You are an elite YouTube thumbnail design strategist and art director. You analyze what makes thumbnails get clicks and generate precise SDXL image prompts to create thumbnails that outperform human designers.

You understand:
- Color psychology (red/yellow = urgency, blue = trust, contrast = attention)
- Face/emotion science (shocked expressions get 2-3x CTR)
- Text placement rules (big bold text, 3-5 words max, high contrast)
- Composition (rule of thirds, leading lines, depth)
- Platform-specific optimization (1920x1080 output, mobile-first readability)

When given a video description, style reference, or sketch, you produce a detailed SDXL image generation prompt that will create a click-worthy thumbnail.

Output MUST be valid JSON:
{
  "prompt": "Detailed SDXL prompt for the thumbnail image. Include: subject, composition, lighting, colors, text elements, style, camera angle. Be extremely specific about every visual element. 8k quality, professional YouTube thumbnail.",
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
  "prompt": "Detailed SDXL prompt combining the reference style with new content. 8k quality, professional YouTube thumbnail, 1920x1080 composition.",
  "negative_prompt": "Elements to avoid",
  "title_text": "The overlay text for this thumbnail (3-5 words, empty string if none)",
  "style_notes": "How the reference style was adapted"
}"""


THUMBNAIL_SCREENSHOT_PROMPT = """You are an elite YouTube thumbnail analyst and designer. The user has provided a description of their YouTube channel's existing thumbnails (or a description of what has worked for them before).

Your job: identify the patterns that made those thumbnails successful, then generate a NEW thumbnail prompt that follows the same winning formula but feels fresh and evolved.

Analyze for: recurring color schemes, text styles, composition patterns, emotional triggers, branding elements.

Output MUST be valid JSON:
{
  "prompt": "Detailed SDXL prompt for a new thumbnail following the user's proven style. 8k quality, professional YouTube thumbnail, 1920x1080.",
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


async def _generate_thumbnail_image(prompt: str, negative_prompt: str, output_path: str, style_ref_path: str = "") -> str:
    """Generate a thumbnail using SDXL via ComfyUI, with trained LoRA if available."""
    use_lora = await _check_lora_exists()
    if use_lora:
        log.info("Using trained thumbnail LoRA for generation")

    model_source = ["1", 0]
    clip_source = ["1", 1]
    if use_lora:
        model_source = ["99", 0]
        clip_source = ["99", 1]

    lora_node = {}
    if use_lora:
        lora_node = {
            "99": {
                "class_type": "LoraLoader",
                "inputs": {
                    "lora_name": LORA_NAME,
                    "strength_model": 0.75,
                    "strength_clip": 0.75,
                    "model": ["1", 0],
                    "clip": ["1", 1],
                },
            },
        }

    if style_ref_path and Path(style_ref_path).exists():
        uploaded_name = await _upload_image_to_comfyui(style_ref_path)
        workflow = {
            "1": {
                "class_type": "CheckpointLoaderSimple",
                "inputs": {"ckpt_name": "sd_xl_base_1.0.safetensors"},
            },
            **lora_node,
            "2": {
                "class_type": "LoadImage",
                "inputs": {"image": uploaded_name},
            },
            "3": {
                "class_type": "VAEEncode",
                "inputs": {"pixels": ["2", 0], "vae": ["1", 2]},
            },
            "4": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": prompt, "clip": clip_source},
            },
            "5": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": negative_prompt, "clip": clip_source},
            },
            "6": {
                "class_type": "KSampler",
                "inputs": {
                    "seed": random.randint(0, 2**32),
                    "steps": 30,
                    "cfg": 7.0,
                    "sampler_name": "dpmpp_2m",
                    "scheduler": "karras",
                    "denoise": 0.65,
                    "model": model_source,
                    "positive": ["4", 0],
                    "negative": ["5", 0],
                    "latent_image": ["3", 0],
                },
            },
            "7": {
                "class_type": "VAEDecode",
                "inputs": {"samples": ["6", 0], "vae": ["1", 2]},
            },
            "8": {
                "class_type": "SaveImage",
                "inputs": {"filename_prefix": "nyptid_thumb", "images": ["7", 0]},
            },
        }
    else:
        workflow = {
            "1": {
                "class_type": "CheckpointLoaderSimple",
                "inputs": {"ckpt_name": "sd_xl_base_1.0.safetensors"},
            },
            **lora_node,
            "2": {
                "class_type": "EmptyLatentImage",
                "inputs": {"width": 1920, "height": 1080, "batch_size": 1},
            },
            "3": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": prompt, "clip": clip_source},
            },
            "4": {
                "class_type": "CLIPTextEncode",
                "inputs": {"text": negative_prompt, "clip": clip_source},
            },
            "5": {
                "class_type": "KSampler",
                "inputs": {
                    "seed": random.randint(0, 2**32),
                    "steps": 30,
                    "cfg": 7.5,
                    "sampler_name": "dpmpp_2m",
                    "scheduler": "karras",
                    "denoise": 1.0,
                    "model": model_source,
                    "positive": ["3", 0],
                    "negative": ["4", 0],
                    "latent_image": ["2", 0],
                },
            },
            "6": {
                "class_type": "VAEDecode",
                "inputs": {"samples": ["5", 0], "vae": ["1", 2]},
            },
            "7": {
                "class_type": "SaveImage",
                "inputs": {"filename_prefix": "nyptid_thumb", "images": ["6", 0]},
            },
        }

    result = await _run_comfyui_workflow(workflow, "8" if style_ref_path else "7", "images")
    await _download_comfyui_file(result["images"][0], output_path)
    return output_path


@app.post("/api/thumbnails/generate")
async def generate_thumbnail(req: ThumbnailGenerateRequest, background_tasks: BackgroundTasks, user: dict = Depends(require_auth)):
    if not _is_admin_user(user):
        raise HTTPException(403, "Admin only")
    if not XAI_API_KEY:
        raise HTTPException(500, "XAI_API_KEY not configured")

    job_id = f"thumb_{int(time.time())}_{random.randint(1000, 9999)}"
    jobs[job_id] = {
        "status": "queued",
        "progress": 0,
        "type": "thumbnail",
        "mode": req.mode,
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

            jobs[job_id]["status"] = "generating"
            jobs[job_id]["progress"] = 30
            jobs[job_id]["ai_analysis"] = {
                "title_text": title_text,
                "style_notes": style_notes,
                "patterns": ai_result.get("patterns_detected", []),
            }

            style_ref_path = ""
            if req.style_reference_id:
                ref_path = THUMBNAIL_UPLOAD_DIR / req.style_reference_id
                if ref_path.exists():
                    style_ref_path = str(ref_path)

            output_name = f"{job_id}.png"
            output_path = str(THUMBNAIL_OUTPUT_DIR / output_name)
            await _generate_thumbnail_image(thumb_prompt, thumb_negative, output_path, style_ref_path)

            if title_text:
                try:
                    safe_title = title_text.replace("'", "'\\''").replace(":", "\\:")
                    titled_out = str(THUMBNAIL_OUTPUT_DIR / (job_id + "_titled.png"))
                    vf_filter = (
                        "drawtext=text='" + safe_title + "':"
                        "fontsize=72:fontcolor=white:borderw=5:bordercolor=black:"
                        "x=(w-text_w)/2:y=h*0.78:"
                        "fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
                    )
                    cmd = [
                        "ffmpeg", "-y",
                        "-i", output_path,
                        "-vf", vf_filter,
                        titled_out,
                    ]
                    proc = await asyncio.create_subprocess_exec(
                        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                    )
                    _, stderr = await proc.communicate()
                    if proc.returncode == 0:
                        titled_path = Path(titled_out)
                        if titled_path.exists():
                            Path(output_path).unlink(missing_ok=True)
                            titled_path.rename(output_path)
                except Exception as e:
                    log.warning(f"[{job_id}] Title overlay failed, using without text: {e}")

            await _enforce_thumbnail_1080(output_path)
            thumb_gen_id = await _save_training_candidate(
                thumb_prompt,
                output_path,
                template="thumbnail",
                source="thumbnail_ai",
                metadata={
                    "mode": req.mode,
                    "title_text": title_text,
                    "user_id": user.get("id", ""),
                },
            )

            jobs[job_id]["status"] = "complete"
            jobs[job_id]["progress"] = 100
            jobs[job_id]["output_file"] = output_name
            jobs[job_id]["output_url"] = f"/api/thumbnails/generated/{output_name}"
            jobs[job_id]["generation_id"] = thumb_gen_id
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
    app.mount("/", StaticFiles(directory=str(dist_dir), html=True), name="static")
else:
    log.warning(f"Frontend dist directory not found: {dist_dir}")


if __name__ == "__main__":
    for f in OUTPUT_DIR.iterdir():
        if f.suffix == ".mp4" and f.stat().st_mtime < time.time() - 86400:
            f.unlink(missing_ok=True)
    uvicorn.run("backend:app", host="0.0.0.0", port=int(os.getenv("PORT", "8091")), reload=True)
