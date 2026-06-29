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
    RUNWAY_API_KEY,
    RUNWAY_API_KEY_SOURCE,
    RUNWAY_VIDEO_MODEL,
    SKELETON_REQUIRE_WAN22,
    TEMPLATE_ADAPTER_ROUTING,
    TEMPLATE_ADAPTER_ROUTING_ENABLED,
    XAI_IMAGE_FALLBACK_ENABLED,
)


_HEALTH_PROBE_CACHE: dict[str, tuple[float, bool]] = {}
_HEALTH_PROBE_TTL_SEC = 30.0


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
        skeleton_lora = await cached_probe("skeleton_lora", check_skeleton_lora_available)
        provider_order = configured_image_provider_order()
        hidream_configured = any(normalize_image_provider_key(p) == "hidream" for p in provider_order)
        wan_configured = any(normalize_image_provider_key(p) == "wan22" for p in provider_order)
        hidream_ready = await cached_probe("hidream", check_hidream_available) if hidream_configured else False
        hidream_edit_ready = await cached_probe("hidream_edit", check_hidream_edit_available) if hidream_configured else False
        wan_ready = await cached_probe("wan22", check_wan22_available) if wan_configured else False
        wan_t2i_ready = await cached_probe("wan22_t2i", check_wan22_t2i_available) if wan_configured else False
        now_ts = time.time()
        hidream_checked_ts = float(hidream_availability_cache.get("checked_ts", 0.0) or 0.0)
        hidream_last_ok_ts = float(hidream_availability_cache.get("last_ok_ts", 0.0) or 0.0)
        hidream_last_error = str(hidream_availability_cache.get("last_error", "") or "")
        hidream_model = str(hidream_availability_cache.get("model_name", "") or "")
        hidream_edit_checked_ts = float(hidream_edit_availability_cache.get("checked_ts", 0.0) or 0.0)
        hidream_edit_last_ok_ts = float(hidream_edit_availability_cache.get("last_ok_ts", 0.0) or 0.0)
        hidream_edit_last_error = str(hidream_edit_availability_cache.get("last_error", "") or "")
        hidream_edit_model = str(hidream_edit_availability_cache.get("model_name", "") or "")
        wan_t2i_checked_ts = float(wan22_t2i_availability_cache.get("checked_ts", 0.0) or 0.0)
        wan_t2i_last_ok_ts = float(wan22_t2i_availability_cache.get("last_ok_ts", 0.0) or 0.0)
        wan_t2i_last_error = str(wan22_t2i_availability_cache.get("last_error", "") or "")
        wan_t2i_mode = str(wan22_t2i_availability_cache.get("mode", "") or "")
        wan_t2i_checkpoint = str(wan22_t2i_availability_cache.get("ckpt_name", "") or "")
        wan_t2i_unet = str(wan22_t2i_availability_cache.get("unet_name", "") or "")
        provider_label = " > ".join(provider_order)
        backend_commit, frontend_bundle = read_deploy_meta()
        fal_video_enabled = bool(FAL_AI_KEY)
        runway_video_enabled = bool(RUNWAY_API_KEY)
        if runway_video_enabled and fal_video_enabled:
            video_engine = "Runway (primary) + FalAI Kling fallback"
        elif fal_video_enabled:
            video_engine = "FalAI Kling 2.1"
        elif runway_video_enabled:
            video_engine = "Runway Image-to-Video"
        elif wan_ready:
            video_engine = "Wan 2.2 (RunPod)"
        else:
            video_engine = "Static"
        return {
            "status": "online",
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
            "runway_key_configured": runway_video_enabled,
            "runway_key_source": RUNWAY_API_KEY_SOURCE if runway_video_enabled else "",
            "runway_video_model": RUNWAY_VIDEO_MODEL if runway_video_enabled else "",
            "comfyui_url": str(comfyui_url())[:50],
            "skeleton_lora": skeleton_lora,
            "image_engine_skeleton": ("Skeleton LoRA (local) > " + provider_label) if skeleton_lora else provider_label,
            "image_provider_order": provider_order,
            "xai_image_fallback_enabled": bool(XAI_IMAGE_FALLBACK_ENABLED),
            "fal_image_backup_model": normalize_fal_image_backup_model(FAL_IMAGE_BACKUP_MODEL) if FAL_AI_KEY else "",
            "image_local_provider_retries": int(IMAGE_LOCAL_PROVIDER_RETRIES),
            "image_provider_failure_cooldown_sec": int(IMAGE_PROVIDER_FAILURE_COOLDOWN_SEC),
            "image_provider_wan_skip_if_unavailable": bool(IMAGE_PROVIDER_WAN_SKIP_IF_UNAVAILABLE),
            "skeleton_require_wan22": bool(SKELETON_REQUIRE_WAN22),
            "image_provider_fail_counts": dict(image_provider_fail_counts),
            "image_provider_success_counts": dict(image_provider_success_counts),
            "image_provider_cooldowns_sec": provider_cooldown_snapshot(),
            "image_fallback_events_total": int(image_provider_fallback_total()),
            "image_fallback_events_pairs": dict(image_provider_fallback_pairs),
            "template_adapter_routing_enabled": bool(TEMPLATE_ADAPTER_ROUTING_ENABLED),
            "template_adapter_routes": sorted(k for k in (TEMPLATE_ADAPTER_ROUTING or {}).keys()),
            "backend_commit": backend_commit,
            "frontend_bundle": frontend_bundle,
            "queue_mode": "redis" if (REDIS_QUEUE_ENABLED and bool(REDIS_URL)) else "inprocess",
            "force_720p_only": FORCE_720P_ONLY,
        }

    return health_payload
