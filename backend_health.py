"""Health payload builder for the Studio API."""

from __future__ import annotations

import time
from typing import Callable

from backend_settings import (
    FAL_AI_KEY,
    FAL_IMAGE_BACKUP_MODEL,
    FORCE_720P_ONLY,
    IMAGE_LOCAL_PROVIDER_RETRIES,
    IMAGE_PROVIDER_FAILURE_COOLDOWN_SEC,
    IMAGE_PROVIDER_WAN_SKIP_IF_UNAVAILABLE,
    REDIS_QUEUE_ENABLED,
    REDIS_URL,
)


_HEALTH_PROBE_CACHE: dict[str, tuple[float, bool]] = {}
_HEALTH_PROBE_TTL_SEC = 30.0


def _public_error_code(value: object) -> str:
    """Collapse provider/runtime errors to a non-sensitive operational code."""
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if "acknowledg" in text:
        return "acknowledgement_pending"
    if any(marker in text for marker in ("busy", "queue", "capacity", "concurrent", "rate limit", "429", "resource")):
        return "provider_busy"
    if any(marker in text for marker in ("timeout", "timed out", "deadline")):
        return "provider_timeout"
    if any(marker in text for marker in ("unauthorized", "forbidden", "authentication", "401", "403", "api key")):
        return "provider_authentication_failed"
    if any(marker in text for marker in ("not configured", "missing configuration", "disabled")):
        return "provider_not_configured"
    if any(marker in text for marker in ("unavailable", "connection", "connect", "dns")):
        return "provider_unavailable"
    return "provider_error"


def _public_queue_consumer_health(value: dict | None) -> dict:
    public = dict(value or {})
    public["last_error"] = _public_error_code(public.get("last_error"))
    return public


def build_health_payload(
    *,
    log,
    ffmpeg_available: Callable[[], bool],
    comfyui_url: Callable[[], str],
    read_deploy_meta: Callable[[], tuple[str, str]],
    check_skeleton_lora_available: Callable,
    check_hidream_available: Callable,
    check_hidream_edit_available: Callable,
    check_wan22_available: Callable,
    check_wan22_t2i_available: Callable,
    configured_image_provider_order: Callable[[], list[str]],
    normalize_image_provider_key: Callable[[str], str],
    normalize_fal_image_backup_model: Callable[[str | None], str],
    provider_cooldown_snapshot: Callable[[], dict],
    hidream_availability_cache: dict,
    hidream_edit_availability_cache: dict,
    wan22_t2i_availability_cache: dict,
    image_provider_fail_counts: dict,
    image_provider_success_counts: dict,
    image_provider_fallback_total: Callable[[], int],
    image_provider_fallback_pairs: dict,
    queue_runtime_health: Callable[[], dict] | None = None,
):
    async def cached_probe(name: str, fn) -> bool:
        entry = _HEALTH_PROBE_CACHE.get(name)
        now = time.time()
        if entry and (now - entry[0]) < _HEALTH_PROBE_TTL_SEC:
            return entry[1]
        try:
            value = bool(await fn())
        except Exception as exc:
            log.warning("Health probe %s raised: %s", name, str(exc)[:200])
            value = False
        _HEALTH_PROBE_CACHE[name] = (now, value)
        return value

    async def health_payload():
        # Health reports the effective Studio provider policy, not dormant
        # local adapters or secrets that happen to remain in the environment.
        # Avoid probing retired media planes from this public endpoint.
        provider_order = [
            str(provider).strip().lower()
            for provider in configured_image_provider_order()
            if str(provider).strip().lower() == "fal"
        ]
        skeleton_lora = False
        hidream_ready = False
        hidream_edit_ready = False
        wan_ready = False
        wan_t2i_ready = False
        now_ts = time.time()
        hidream_checked_ts = float(hidream_availability_cache.get("checked_ts", 0.0) or 0.0)
        hidream_last_ok_ts = float(hidream_availability_cache.get("last_ok_ts", 0.0) or 0.0)
        hidream_last_error = _public_error_code(hidream_availability_cache.get("last_error", ""))
        hidream_model = str(hidream_availability_cache.get("model_name", "") or "")
        hidream_edit_checked_ts = float(hidream_edit_availability_cache.get("checked_ts", 0.0) or 0.0)
        hidream_edit_last_ok_ts = float(hidream_edit_availability_cache.get("last_ok_ts", 0.0) or 0.0)
        hidream_edit_last_error = _public_error_code(hidream_edit_availability_cache.get("last_error", ""))
        hidream_edit_model = str(hidream_edit_availability_cache.get("model_name", "") or "")
        wan_t2i_checked_ts = float(wan22_t2i_availability_cache.get("checked_ts", 0.0) or 0.0)
        wan_t2i_last_ok_ts = float(wan22_t2i_availability_cache.get("last_ok_ts", 0.0) or 0.0)
        wan_t2i_last_error = _public_error_code(wan22_t2i_availability_cache.get("last_error", ""))
        wan_t2i_mode = str(wan22_t2i_availability_cache.get("mode", "") or "")
        wan_t2i_checkpoint = str(wan22_t2i_availability_cache.get("ckpt_name", "") or "")
        wan_t2i_unet = str(wan22_t2i_availability_cache.get("unet_name", "") or "")
        provider_label = " > ".join(provider_order)
        backend_commit, frontend_bundle = read_deploy_meta()
        queue_consumer = _public_queue_consumer_health(queue_runtime_health()) if queue_runtime_health else {
            "required": False,
            "running": False,
            "ready": True,
        }
        queue_consumer_ready = bool(queue_consumer.get("ready", True))
        runpod_production = False
        runpod_longform = False
        runpod_control_ready = False
        runpod_storage_ready = False
        try:
            from studio_agent.runpod_bridge import runpod_configured
            from studio_agent.runpod_contract import (
                runpod_longform_enabled,
                runpod_production_enabled,
            )
            from studio_agent.runpod_storage import configured as runpod_storage_configured

            runpod_production = bool(runpod_production_enabled())
            runpod_longform = bool(runpod_longform_enabled())
            runpod_control_ready = bool(runpod_configured())
            runpod_storage_ready = bool(runpod_storage_configured())
        except Exception:
            # Health must remain available even when the optional RunPod plane
            # is intentionally disabled or only partially configured.
            pass
        fal_video_enabled = bool(FAL_AI_KEY)
        if fal_video_enabled:
            video_engine = "FalAI Kling 2.1"
        else:
            video_engine = "Unavailable (FAL required)"
        return {
            "status": "online" if queue_consumer_ready else "degraded",
            "engine": "NYPTID Studio Engine v3.0",
            "ffmpeg_available": ffmpeg_available(),
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
            "runway_key_configured": False,
            "runway_key_source": "",
            "runway_video_model": "",
            "comfyui_configured": False,
            "skeleton_lora": skeleton_lora,
            "image_engine_skeleton": provider_label,
            "image_provider_order": provider_order,
            "xai_image_fallback_enabled": False,
            "fal_image_backup_model": normalize_fal_image_backup_model(FAL_IMAGE_BACKUP_MODEL) if FAL_AI_KEY else "",
            "image_local_provider_retries": int(IMAGE_LOCAL_PROVIDER_RETRIES),
            "image_provider_failure_cooldown_sec": int(IMAGE_PROVIDER_FAILURE_COOLDOWN_SEC),
            "image_provider_wan_skip_if_unavailable": bool(IMAGE_PROVIDER_WAN_SKIP_IF_UNAVAILABLE),
            "skeleton_require_wan22": False,
            "image_provider_fail_counts": dict(image_provider_fail_counts),
            "image_provider_success_counts": dict(image_provider_success_counts),
            "image_provider_cooldowns_sec": provider_cooldown_snapshot(),
            "image_fallback_events_total": int(image_provider_fallback_total()),
            "image_fallback_events_pairs": dict(image_provider_fallback_pairs),
            "template_adapter_routing_enabled": False,
            "template_adapter_routes": [],
            "backend_commit": backend_commit,
            "frontend_bundle": frontend_bundle,
            "queue_mode": "redis" if (REDIS_QUEUE_ENABLED and bool(REDIS_URL)) else "inprocess",
            "queue_consumer": queue_consumer,
            "queue_consumer_running": bool(queue_consumer.get("running", False)),
            "queue_consumer_ready": queue_consumer_ready,
            "force_720p_only": FORCE_720P_ONLY,
            "runpod_production_enabled": runpod_production,
            "runpod_longform_enabled": runpod_longform,
            "runpod_control_configured": runpod_control_ready,
            "runpod_storage_configured": runpod_storage_ready,
            "runpod_configured": bool(runpod_control_ready and runpod_storage_ready),
        }

    return health_payload
