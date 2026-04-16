from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
from pathlib import Path

import httpx

from audio import _probe_video_duration_seconds
from backend_catalog import PLAN_FEATURES, RESOLUTION_CONFIGS, SUPPORTED_LANGUAGES
from backend_catalyst_profiles import _catalyst_scene_execution_profile
from backend_script_prompts import TEMPLATE_SYSTEM_PROMPTS
from backend_settings import (
    FAL_AI_KEY,
    FORCE_720P_ONLY,
    LONGFORM_DEFAULT_TARGET_MINUTES,
    LONGFORM_MAX_TARGET_MINUTES,
    LONGFORM_MIN_TARGET_MINUTES,
    STORY_ADVANCED_CONTROLS_ENABLED,
    STORY_RETENTION_TUNING_ENABLED,
    TEMP_DIR,
    XAI_API_KEY,
)

log = logging.getLogger("nyptid-studio")


_named_human_subject_likeness_lock = lambda *texts, skeleton_mode=False: ""
_longform_named_human_priority_lock = lambda *texts, template="", archetype_key="": ""
_build_scene_prompt_with_reference = (
    lambda template, visual_description, quality_mode="cinematic", skeleton_anchor="", reference_dna=None, reference_lock_mode="strict", art_style="auto": str(visual_description or "")
)
_longform_subject_lock = lambda topic, input_title, input_description="": ""
_xai_json_completion = None
_fal_openrouter_json_completion = None


def configure_video_pipeline_runtime_hooks(
    *,
    named_human_subject_likeness_lock=None,
    longform_named_human_priority_lock=None,
    build_scene_prompt_with_reference=None,
    longform_subject_lock=None,
    xai_json_completion=None,
    fal_openrouter_json_completion=None,
):
    global _named_human_subject_likeness_lock
    global _longform_named_human_priority_lock
    global _build_scene_prompt_with_reference
    global _longform_subject_lock
    global _xai_json_completion
    global _fal_openrouter_json_completion
    if named_human_subject_likeness_lock is not None:
        _named_human_subject_likeness_lock = named_human_subject_likeness_lock
    if longform_named_human_priority_lock is not None:
        _longform_named_human_priority_lock = longform_named_human_priority_lock
    if build_scene_prompt_with_reference is not None:
        _build_scene_prompt_with_reference = build_scene_prompt_with_reference
    if longform_subject_lock is not None:
        _longform_subject_lock = longform_subject_lock
    if xai_json_completion is not None:
        _xai_json_completion = xai_json_completion
    if fal_openrouter_json_completion is not None:
        _fal_openrouter_json_completion = fal_openrouter_json_completion


DEFAULT_CREATIVE_IMAGE_MODEL_ID = "studio_default"
DEFAULT_CREATIVE_VIDEO_MODEL_ID = "kling21_standard"
LONGFORM_ALLOWED_TEMPLATES = {"story", "skeleton"}
LONGFORM_WHISPER_MODES = {"off", "subtle", "cinematic"}
LONGFORM_HORROR_VISUAL_DIRECTIVE = (
    "Horror tone lock: psychological dread, ominous atmosphere, eerie shadows, moody low-key lighting, drifting fog/mist, "
    "and unsettling cinematic realism. Keep it grounded and tense. No gore, no comedy, no bright cheerful styling."
)
LONGFORM_3D_DOC_VISUAL_DIRECTIVE = (
    "3D documentary style lock: premium stylized 3D explainer/documentary render, not live-action photography. "
    "Use designed environments, polished CGI materials, clean focal hierarchy, motion-design readability, engineered composition, "
    "and camera setups that feel like a premium YouTube business/documentary video. Bias toward dossier-table reveals, boardroom power dynamics, "
    "archive proof frames, surveillance staging, ownership webs, human consequence tableaux, and crisp evidence-first storytelling rather than product-shot machinery. "
    "This should feel like elite faceless YouTube documentary 3D: more designed, more intentional, more motion-graphics-aware, and more obviously CG. "
    "Avoid gritty street-photo realism, random warehouses, candid live-action film stills, cluttered lab repetition, medical-textbook rendering, floating machine hero objects, or generic moody humans unless the beat absolutely requires a person."
)
LONGFORM_3D_CRIME_DOC_VISUAL_DIRECTIVE = (
    "3D crime-case documentary style lock: premium stylized 3D true-crime/case-breakdown render, not live-action photography. "
    "Use designed evidence environments, source-capture framing, court and studio consequence scenes, case-board composition, phone and records logic, "
    "timeline-map staging, surveillance monitors, photoreal named-human pressure scenes, and one dominant contradiction per frame. "
    "This should feel like an expensive faceless YouTube case breakdown: editorial, evidence-led, human-scale, and obviously CG without looking like a random boardroom slideshow. "
    "Bias toward real-life human faces, hands, posture, and evidence handling whenever the beat involves a public figure or named participant. "
    "Avoid empty archive rooms, untouched dossier tables, generic corporate meetings, skeleton crowds, x-ray humans, sterile labs, floating machine props, and readable wall text or UI text."
)
LONGFORM_3D_PSYCHOLOGY_DOC_VISUAL_DIRECTIVE = (
    "3D psychology documentary style lock: premium stylized 3D faceless documentary render about hidden behavior, not live-action photography. "
    "Use designed psychological environments, clean focal hierarchy, controlled dark-stage contrast, elegant surveillance and dossier framing, mirror or split-self reversals, "
    "social power dynamics, influence webs, symbolic mind-worlds, and human consequence tableaux that feel invasive, premium, and obviously CG. "
    "Bias toward emotional symbols, hidden triggers, observation rooms, interrogation/archive staging, and consequence-first human scenes rather than abstract machine assemblies. "
    "Avoid literal exposed brains, textbook anatomy, sterile labs, floating gears, generic machine filler, cluttered boardrooms, or random sci-fi widgets unless the beat explicitly demands them."
)


def _clip_text(value: str, max_chars: int = 240) -> str:
    compact = re.sub(r"\s+", " ", str(value or "").strip())
    if len(compact) <= max_chars:
        return compact
    return compact[: max(0, max_chars - 1)].rstrip() + "..."


def _dedupe_preserve_order(values: list[str], max_items: int = 250, max_chars: int = 128) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in list(values or []):
        value = str(raw or "").strip()
        if not value:
            continue
        value = value[:max_chars]
        lowered = value.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        out.append(value)
        if len(out) >= max_items:
            break
    return out

CREATIVE_IMAGE_MODEL_PROFILES = [
    {
        "id": "studio_default",
        "label": "Studio Default",
        "provider": "nyptid_hybrid",
        "tier": "basic",
        "summary": "Best continuity for NYPTID templates. Uses Studio's tuned hybrid lane automatically.",
        "speed": "Balanced",
        "credit_cost_per_image": 0,
        "estimated_unit_usd": 0.02,
        "billing_unit": "image",
        "fal_endpoint_id": "",
        "enabled": True,
        "supports_reference_conditioning": True,
    },
    {
        "id": "grok_imagine",
        "label": "Grok Imagine",
        "provider": "fal",
        "tier": "basic",
        "summary": "Fast default image lane through fal.ai.",
        "speed": "Fast",
        "credit_cost_per_image": 0,
        "estimated_unit_usd": 0.02,
        "billing_unit": "image",
        "fal_endpoint_id": "xai/grok-imagine-image",
        "enabled": bool(FAL_AI_KEY or XAI_API_KEY),
        "supports_reference_conditioning": True,
    },
    {
        "id": "imagen4_fast",
        "label": "Imagen 4 Fast",
        "provider": "fal",
        "tier": "basic",
        "summary": "Google's faster image model. Good for quick scene passes.",
        "speed": "Very Fast",
        "credit_cost_per_image": 0,
        "estimated_unit_usd": 0.02,
        "billing_unit": "image",
        "fal_endpoint_id": "fal-ai/imagen4/preview/fast",
        "enabled": bool(FAL_AI_KEY),
        "supports_reference_conditioning": False,
    },
    {
        "id": "imagen4_preview",
        "label": "Imagen 4 Preview",
        "provider": "fal",
        "tier": "basic",
        "summary": "Google's standard Imagen 4 model. Best quality-to-cost ratio.",
        "speed": "Fast",
        "credit_cost_per_image": 0,
        "estimated_unit_usd": 0.03,
        "billing_unit": "image",
        "fal_endpoint_id": "fal-ai/imagen4/preview",
        "enabled": bool(FAL_AI_KEY),
        "supports_reference_conditioning": False,
    },
    {
        "id": "imagen4_ultra",
        "label": "Imagen 4 Ultra",
        "provider": "fal",
        "tier": "premium",
        "summary": "Google's highest-quality text-to-image lane.",
        "speed": "Medium",
        "credit_cost_per_image": 4,
        "estimated_unit_usd": 0.06,
        "billing_unit": "image",
        "fal_endpoint_id": "fal-ai/imagen4/preview/ultra",
        "enabled": bool(FAL_AI_KEY),
        "supports_reference_conditioning": False,
    },
    {
        "id": "recraft_v4",
        "label": "Recraft V4",
        "provider": "fal",
        "tier": "premium",
        "summary": "Design-first image generation with cleaner composition and ad-style polish.",
        "speed": "Medium",
        "credit_cost_per_image": 4,
        "estimated_unit_usd": 0.04,
        "billing_unit": "image",
        "fal_endpoint_id": "fal-ai/recraft/v4/text-to-image",
        "enabled": bool(FAL_AI_KEY),
        "supports_reference_conditioning": False,
    },
    {
        "id": "seedream45",
        "label": "Seedream 4.5",
        "provider": "fal",
        "tier": "premium",
        "summary": "High-end prompt adherence with polished commercial image quality.",
        "speed": "Medium",
        "credit_cost_per_image": 4,
        "estimated_unit_usd": 0.04,
        "billing_unit": "image",
        "fal_endpoint_id": "fal-ai/bytedance/seedream/v4.5/text-to-image",
        "enabled": bool(FAL_AI_KEY),
        "supports_reference_conditioning": False,
    },
    {
        "id": "ernie_image",
        "label": "ERNIE-Image",
        "provider": "fal",
        "tier": "basic",
        "summary": "Baidu's cinematic image model. Best thumbnail + scene quality at lowest cost.",
        "speed": "Fast",
        "credit_cost_per_image": 0,
        "estimated_unit_usd": 0.01,
        "billing_unit": "image",
        "fal_endpoint_id": "fal-ai/ernie-image",
        "enabled": bool(FAL_AI_KEY),
        "supports_reference_conditioning": False,
    },
    {
        "id": "flux_2_pro",
        "label": "FLUX 2 Pro",
        "provider": "fal",
        "tier": "premium",
        "summary": "High-fidelity prompt rendering with strong cinematic framing.",
        "speed": "Medium",
        "credit_cost_per_image": 4,
        "estimated_unit_usd": 0.03,
        "billing_unit": "processed_megapixels",
        "fal_endpoint_id": "fal-ai/flux-2-pro",
        "enabled": bool(FAL_AI_KEY),
        "supports_reference_conditioning": False,
    },
    {
        "id": "nano_banana_pro",
        "label": "Nano Banana Pro",
        "provider": "fal",
        "tier": "elite",
        "summary": "Premium reasoning-based image generation with strong text and product composition.",
        "speed": "Medium",
        "credit_cost_per_image": 5,
        "estimated_unit_usd": 0.15,
        "billing_unit": "image",
        "fal_endpoint_id": "fal-ai/nano-banana-pro",
        "enabled": bool(FAL_AI_KEY),
        "supports_reference_conditioning": False,
    },
    {
        "id": "recraft_v4_pro",
        "label": "Recraft V4 Pro",
        "provider": "fal",
        "tier": "elite",
        "summary": "Designer-grade image generation for top-end ad and thumbnail style work.",
        "speed": "Slow",
        "credit_cost_per_image": 5,
        "estimated_unit_usd": 0.25,
        "billing_unit": "image",
        "fal_endpoint_id": "fal-ai/recraft/v4/pro/text-to-image",
        "enabled": bool(FAL_AI_KEY),
        "supports_reference_conditioning": False,
    },
]
CREATIVE_IMAGE_MODEL_MAP = {str(profile["id"]): profile for profile in CREATIVE_IMAGE_MODEL_PROFILES}

CREATIVE_VIDEO_MODEL_PROFILES = [
    {
        "id": "kling21_standard",
        "label": "Kling 2.1 Standard",
        "provider": "fal",
        "tier": "basic",
        "summary": "Default animation lane for Studio renders.",
        "speed": "Balanced",
        "credit_multiplier": 1,
        "estimated_unit_usd": 0.056,
        "billing_unit": "second",
        "fal_endpoint_id": "fal-ai/kling-video/v2.1/standard/image-to-video",
        "enabled": bool(FAL_AI_KEY),
    },
    {
        "id": "kling21_pro",
        "label": "Kling 2.1 Pro",
        "provider": "fal",
        "tier": "premium",
        "summary": "Sharper motion and stronger camera handling than the standard lane.",
        "speed": "Balanced",
        "credit_multiplier": 4,
        "estimated_unit_usd": 0.098,
        "billing_unit": "second",
        "fal_endpoint_id": "fal-ai/kling-video/v2.1/pro/image-to-video",
        "enabled": bool(FAL_AI_KEY),
    },
    {
        "id": "veo3_fast",
        "label": "Veo 3 Fast",
        "provider": "fal",
        "tier": "premium",
        "summary": "Premium cinematic motion with heavier wallet burn.",
        "speed": "Slow",
        "credit_multiplier": 4,
        "estimated_unit_usd": 0.10,
        "billing_unit": "second",
        "fal_endpoint_id": "fal-ai/veo3/fast/image-to-video",
        "enabled": bool(FAL_AI_KEY),
    },
    {
        "id": "kling21_master",
        "label": "Kling 2.1 Master",
        "provider": "fal",
        "tier": "elite",
        "summary": "Highest-cost Kling lane for top-end shot quality.",
        "speed": "Slow",
        "credit_multiplier": 5,
        "estimated_unit_usd": 0.28,
        "billing_unit": "second",
        "fal_endpoint_id": "fal-ai/kling-video/v2.1/master/image-to-video",
        "enabled": bool(FAL_AI_KEY),
    },
    {
        "id": "pixverse_c1",
        "label": "PixVerse C1 (Film Grade)",
        "provider": "fal",
        "tier": "elite",
        "summary": "Film-grade hyper-realistic video. Use for hero scenes (opening, climax, chapter intros).",
        "speed": "Slow",
        "credit_multiplier": 6,
        "estimated_unit_usd": 0.09,
        "billing_unit": "second",
        "fal_endpoint_id": "fal-ai/pixverse/c1/image-to-video",
        "enabled": bool(FAL_AI_KEY),
    },
    {
        "id": "pixverse_v6",
        "label": "PixVerse V6",
        "provider": "fal",
        "tier": "premium",
        "summary": "Latest PixVerse model. Strong motion, good quality-to-cost ratio.",
        "speed": "Balanced",
        "credit_multiplier": 3,
        "estimated_unit_usd": 0.07,
        "billing_unit": "second",
        "fal_endpoint_id": "fal-ai/pixverse/v6/image-to-video",
        "enabled": bool(FAL_AI_KEY),
    },
]
CREATIVE_VIDEO_MODEL_MAP = {str(profile["id"]): profile for profile in CREATIVE_VIDEO_MODEL_PROFILES}


def _creative_model_catalog_copy(entries: list[dict]) -> list[dict]:
    return [dict(entry) for entry in entries]


def _normalize_creative_image_model_id(value: str | None, template: str = "") -> str:
    requested = str(value or "").strip().lower()
    if requested in CREATIVE_IMAGE_MODEL_MAP and bool(CREATIVE_IMAGE_MODEL_MAP[requested].get("enabled", False)):
        return requested
    for candidate in (
        DEFAULT_CREATIVE_IMAGE_MODEL_ID,
        "imagen4_fast",
        "grok_imagine",
    ):
        profile = CREATIVE_IMAGE_MODEL_MAP.get(candidate)
        if profile and bool(profile.get("enabled", False)):
            return candidate
    return DEFAULT_CREATIVE_IMAGE_MODEL_ID


def _normalize_scene_image_model_id(value: str | None, template: str = "") -> str:
    normalized = _normalize_creative_image_model_id(value, template=template)
    if str(template or "").strip().lower() == "skeleton":
        return "imagen4_preview"
    return normalized


def _normalize_creative_video_model_id(value: str | None) -> str:
    requested = str(value or "").strip().lower()
    if requested in CREATIVE_VIDEO_MODEL_MAP and bool(CREATIVE_VIDEO_MODEL_MAP[requested].get("enabled", False)):
        return requested
    for candidate in (
        DEFAULT_CREATIVE_VIDEO_MODEL_ID,
        "kling21_standard",
    ):
        profile = CREATIVE_VIDEO_MODEL_MAP.get(candidate)
        if profile and bool(profile.get("enabled", False)):
            return candidate
    return DEFAULT_CREATIVE_VIDEO_MODEL_ID


def _creative_image_model_profile(value: str | None, template: str = "") -> dict:
    model_id = _normalize_creative_image_model_id(value, template=template)
    return dict(CREATIVE_IMAGE_MODEL_MAP.get(model_id) or CREATIVE_IMAGE_MODEL_MAP[DEFAULT_CREATIVE_IMAGE_MODEL_ID])


def _creative_video_model_profile(value: str | None) -> dict:
    model_id = _normalize_creative_video_model_id(value)
    return dict(CREATIVE_VIDEO_MODEL_MAP.get(model_id) or CREATIVE_VIDEO_MODEL_MAP[DEFAULT_CREATIVE_VIDEO_MODEL_ID])


def _creative_image_credit_cost(value: str | None, template: str = "") -> int:
    profile = _creative_image_model_profile(value, template=template)
    return max(0, int(profile.get("credit_cost_per_image", 0) or 0))


def _creative_video_credit_multiplier(value: str | None) -> int:
    profile = _creative_video_model_profile(value)
    return max(1, int(profile.get("credit_multiplier", 1) or 1))


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


def _normalize_longform_template(value: str) -> str:
    template = str(value or "").strip().lower()
    return template if template in LONGFORM_ALLOWED_TEMPLATES else "story"


def _normalize_longform_target_minutes(value) -> float:
    try:
        minutes = float(value)
    except Exception:
        minutes = float(LONGFORM_DEFAULT_TARGET_MINUTES)
    effective_max = max(30.0, float(LONGFORM_MAX_TARGET_MINUTES))
    return max(float(LONGFORM_MIN_TARGET_MINUTES), min(effective_max, minutes))


def _normalize_longform_whisper_mode(value: str) -> str:
    mode = str(value or "subtle").strip().lower()
    return mode if mode in LONGFORM_WHISPER_MODES else "subtle"


def _normalize_longform_language(value: str) -> str:
    lang = str(value or "en").strip().lower()
    return lang if lang in SUPPORTED_LANGUAGES else "en"


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
    if value is None:
        return template in {"skeleton", "story", "motivation", "daytrading"}
    return _bool_from_any(value, template in {"skeleton", "story", "motivation", "daytrading"})


CREATIVE_SHORT_BGM_PROFILES = {
    "story": "story_pulse_bed",
    "motivation": "story_pulse_bed",
    "skeleton": "cinematic_dark_tension",
    "daytrading": "documentary_tension",
    "chatstory": "story_pulse_bed",
    "reddit": "precision_explainer_bed",
}


CREATIVE_SHORT_SOUND_MIX = {
    "default": {
        "bgm_required": True,
        "music_profile": "story_pulse_bed",
        "voice_gain": 1.02,
        "ambience_gain": 0.24,
        "sfx_gain": 1.05,
        "bgm_gain": 0.06,
        "whisper_mode": "off",
    },
    "story": {
        "music_profile": "story_pulse_bed",
        "voice_gain": 1.03,
        "ambience_gain": 0.26,
        "sfx_gain": 1.08,
        "bgm_gain": 0.05,
    },
    "motivation": {
        "music_profile": "story_pulse_bed",
        "voice_gain": 1.04,
        "ambience_gain": 0.2,
        "sfx_gain": 0.95,
        "bgm_gain": 0.05,
    },
    "skeleton": {
        "music_profile": "cinematic_dark_tension",
        "voice_gain": 1.05,
        "ambience_gain": 0.34,
        "sfx_gain": 1.25,
        "bgm_gain": 0.06,
    },
    "daytrading": {
        "music_profile": "documentary_tension",
        "voice_gain": 1.03,
        "ambience_gain": 0.22,
        "sfx_gain": 1.08,
        "bgm_gain": 0.06,
    },
    "chatstory": {
        "music_profile": "story_pulse_bed",
        "voice_gain": 1.02,
        "ambience_gain": 0.18,
        "sfx_gain": 0.92,
        "bgm_gain": 0.04,
    },
    "reddit": {
        "bgm_required": False,
        "music_profile": "precision_explainer_bed",
        "voice_gain": 1.0,
        "ambience_gain": 0.12,
        "sfx_gain": 0.9,
        "bgm_gain": 0.04,
    },
}


def _creative_template_sound_mix_profile(template: str) -> dict:
    normalized = str(template or "").strip().lower()
    profile = dict(CREATIVE_SHORT_SOUND_MIX.get("default", {}))
    profile.update(CREATIVE_SHORT_SOUND_MIX.get(normalized, {}))
    if not profile.get("music_profile"):
        profile["music_profile"] = CREATIVE_SHORT_BGM_PROFILES.get(normalized, "story_pulse_bed")
    return profile


def _creative_template_force_sfx(template: str) -> bool:
    normalized = str(template or "").strip().lower()
    return normalized in {"story", "motivation", "skeleton", "daytrading", "chatstory"}


def _creative_template_supports_voice_controls(template: str) -> bool:
    if not STORY_ADVANCED_CONTROLS_ENABLED:
        return False
    return str(template or "").strip().lower() in {"story", "daytrading"}


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


def _apply_story_pacing(scenes: list, template: str, pacing_mode: str = "standard") -> list:
    if template != "story":
        return scenes
    mode = _normalize_pacing_mode(pacing_mode)
    mult = {"standard": 1.0, "fast": 0.9, "very_fast": 0.8}.get(mode, 1.0)
    paced = []
    for scene_item in scenes or []:
        scene = dict(scene_item or {})
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
    scene_payloads: list[dict] | None = None,
    pattern_interrupt_interval_sec: int = 12,
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
        scene_payload = dict(scene_payloads[i] or {}) if scene_payloads and i < len(scene_payloads) else {}
        purpose = str(scene_payload.get("engagement_purpose", "") or "").lower()
        motion_direction = str(scene_payload.get("motion_direction", "") or "").lower()
        execution_intensity = str(scene_payload.get("_execution_intensity", "") or "").strip().lower()
        interrupt_strength = str(scene_payload.get("_interrupt_strength", "") or "").strip().lower()
        cut_profile = str(scene_payload.get("_cut_profile", "") or "").strip().lower()
        payoff_hold_sec = max(0.7, min(1.8, float(scene_payload.get("_payoff_hold_sec", 1.1) or 1.1)))
        interrupt_every = max(2, int(round(float(pattern_interrupt_interval_sec or 12) / 5.0)))
        is_opening = i < 2
        is_interrupt = i > 0 and (i + 1) % interrupt_every == 0
        is_payoff = i >= max(0, len(source_clips) - 2)
        strong_interrupt = any(token in purpose or token in motion_direction for token in ["interrupt", "contrast", "reveal", "payoff", "hook"])
        if base_dur < 3.2:
            micro_clips.append(clip)
            micro_durations.append(base_dur)
            continue

        boundaries = [0.0, round(base_dur * 0.34, 3), round(base_dur * 0.68, 3), base_dur]
        if base_dur < 4.8:
            boundaries = [0.0, round(base_dur * 0.52, 3), base_dur]
        if is_opening or is_interrupt or strong_interrupt:
            boundaries = [0.0, round(base_dur * 0.26, 3), round(base_dur * 0.58, 3), base_dur]
        elif is_payoff:
            boundaries = [0.0, round(base_dur * 0.44, 3), base_dur]
        if execution_intensity in {"high", "attack"} and len(boundaries) >= 3:
            boundaries = [0.0, round(base_dur * 0.22, 3), round(base_dur * 0.52, 3), base_dur]
        if interrupt_strength == "high" and (is_interrupt or strong_interrupt) and len(boundaries) >= 3:
            boundaries = [0.0, round(base_dur * 0.21, 3), round(base_dur * 0.49, 3), base_dur]
        if is_payoff and payoff_hold_sec >= 1.2:
            hold_start = max(0.0, round(base_dur - min(payoff_hold_sec, max(0.9, base_dur * 0.45)), 3))
            boundaries = [0.0, hold_start, base_dur]

        for b in range(len(boundaries) - 1):
            if len(micro_clips) >= MICRO_ESCALATION_MAX_OUTPUT_CLIPS:
                return list(source_clips), list(source_durations)
            start = max(0.0, boundaries[b])
            seg_dur = max(0.42, boundaries[b + 1] - boundaries[b])
            out = TEMP_DIR / f"micro_{job_ts}_{i}_{b}.mp4"
            vf = "eq=contrast=1.02:saturation=1.03"
            if is_opening and b == 0:
                vf = "setpts=0.88*PTS,eq=contrast=1.10:saturation=1.08"
            elif (is_interrupt or strong_interrupt) and b == 1:
                vf = "setpts=0.90*PTS,eq=contrast=1.12:saturation=1.10"
            elif is_payoff and b == len(boundaries) - 2:
                vf = "setpts=0.98*PTS,eq=contrast=1.06:saturation=1.06"
            elif b == 1:
                vf = "setpts=0.92*PTS,eq=contrast=1.08:saturation=1.06"
            elif b >= 2:
                vf = "setpts=0.97*PTS,eq=contrast=1.05:saturation=1.1"
            if execution_intensity == "attack" and b == 0:
                vf = "setpts=0.84*PTS,eq=contrast=1.14:saturation=1.10"
            elif cut_profile == "contrast-cut" and (is_interrupt or strong_interrupt):
                vf = "setpts=0.89*PTS,eq=contrast=1.15:saturation=1.12"
            elif cut_profile == "punch-cut" and b <= 1:
                vf = "setpts=0.87*PTS,eq=contrast=1.12:saturation=1.09"
            cmd = [
                "ffmpeg",
                "-y",
                "-ss",
                f"{start:.3f}",
                "-t",
                f"{seg_dur:.3f}",
                "-i",
                clip_path,
                "-an",
                "-vf",
                vf,
                "-threads",
                "1",
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-pix_fmt",
                "yuv420p",
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
                micro_clips.append(clip)
                micro_durations.append(base_dur)
                break

    return micro_clips, micro_durations


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


def _longform_is_horror_tone(tone: str) -> bool:
    return str(tone or "").strip().lower() == "horror"


def _longform_prefers_3d_documentary_visuals(template: str, format_preset: str = "") -> bool:
    template_key = str(template or "").strip().lower()
    preset_key = str(format_preset or "").strip().lower()
    if preset_key not in {"explainer", "documentary", "recap"}:
        return False
    if template_key == "story":
        return True
    if template_key == "skeleton":
        return preset_key in {"explainer", "documentary"}
    return False


def _longform_default_art_style(template: str, format_preset: str = "") -> str:
    if _longform_prefers_3d_documentary_visuals(template, format_preset):
        return "documentary_3d"
    if str(template or "").strip().lower() == "story" and str(format_preset or "").strip().lower() == "story_channel":
        return "unreal_cinematic"
    return "auto"


def _longform_documentary_archetype(
    *,
    edit_blueprint: dict | None = None,
    chapter_blueprint: dict | None = None,
    format_preset: str = "",
    topic: str = "",
    input_title: str = "",
    narration: str = "",
    visual_description: str = "",
) -> str:
    if str(format_preset or "").strip().lower() != "documentary":
        return "general"
    haystack = " ".join([
        str((edit_blueprint or {}).get("archetype_key", "") or "").strip().lower(),
        str((edit_blueprint or {}).get("archetype_label", "") or "").strip().lower(),
        str((chapter_blueprint or {}).get("focus", "") or "").strip().lower(),
        str((chapter_blueprint or {}).get("visual_motif", "") or "").strip().lower(),
        str(topic or "").strip().lower(),
        str(input_title or "").strip().lower(),
        str(narration or "").strip().lower(),
        str(visual_description or "").strip().lower(),
    ])
    if re.search(
        r"\b(power_history|geopolitics_history|history / geopolitics|ancient greece|ancient greek|greek world|athens|sparta|city[- ]state|peloponnesian|hellenic|classical world|classical greece|history rewind|paradox)\b",
        haystack,
        flags=re.IGNORECASE,
    ):
        return "systems_documentary"
    if re.search(
        r"\b(true crime|crime|criminal|case|court|courtroom|sentenc|sentence|trial|charges?|charged|complaint|indict|kidnapp|robbery|victim|defendant|prosecutor|federal|evidence|surveillance|forensic|dna|detective|arrest|warrant|plea|home confinement|monitoring|cellphone|call log|license plate|records|timeline|case file|studio ambush)\b",
        haystack,
        flags=re.IGNORECASE,
    ):
        return "crime_documentary"
    if re.search(
        r"\b(psychology|psychological|brain|mind|behavior|behaviour|manipulat|bias|blind spot|subconscious|attention|dopamine|emotion|thought|memory|control|choice|choices|decision|decisions|influence|hidden behavior)\b",
        haystack,
        flags=re.IGNORECASE,
    ):
        return "psychology_documentary"
    return "systems_documentary"


def _longform_tone_locked_visual_description(
    visual_description: str,
    tone: str,
    template: str,
    format_preset: str = "",
) -> str:
    base = str(visual_description or "").strip()
    lower = base.lower()
    if _longform_prefers_3d_documentary_visuals(template, format_preset) and "3d documentary style lock:" not in lower:
        documentary_archetype = _longform_documentary_archetype(
            format_preset=format_preset,
            visual_description=base,
        )
        doc_visual_directive = (
            LONGFORM_3D_PSYCHOLOGY_DOC_VISUAL_DIRECTIVE
            if documentary_archetype == "psychology_documentary"
            else LONGFORM_3D_CRIME_DOC_VISUAL_DIRECTIVE
            if documentary_archetype == "crime_documentary"
            else LONGFORM_3D_DOC_VISUAL_DIRECTIVE
        )
        base = (base + " " + doc_visual_directive).strip()
        lower = base.lower()
    if not re.search(r"\b(no on-screen text|no typography|no captions|no labels|no logos|no watermarks)\b", lower):
        base = (
            base
            + " No on-screen text, no typography, no captions, no labels, no logos, no watermarks, and no chapter cards inside the image itself."
        ).strip()
        lower = base.lower()
    if not _longform_is_horror_tone(tone):
        return base
    if "horror tone lock:" in lower:
        return base
    skeleton_horror_hint = ""
    if str(template or "").strip().lower() == "skeleton":
        skeleton_horror_hint = (
            " Environment must fit a horror mystery beat (abandoned roads, dark forests, empty corridors, foggy night exteriors) "
            "while preserving the same canonical skeleton identity."
        )
    return (base + " " + LONGFORM_HORROR_VISUAL_DIRECTIVE + skeleton_horror_hint).strip()


def _longform_enforce_tone_on_scenes(
    scenes: list[dict],
    tone: str,
    template: str,
    format_preset: str = "",
) -> list[dict]:
    out: list[dict] = []
    for raw_scene in list(scenes or []):
        scene = dict(raw_scene or {})
        scene["visual_description"] = _longform_tone_locked_visual_description(
            str(scene.get("visual_description", "") or ""),
            tone=tone,
            template=template,
            format_preset=format_preset,
        )
        out.append(scene)
    return out


def _longform_chapter_count_for_minutes(target_minutes: float) -> int:
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


def _longform_prefers_psychology_documentary_visuals(
    visual_description: str,
    tone: str,
    template: str,
    format_preset: str = "",
) -> bool:
    if not _longform_prefers_3d_documentary_visuals(template, format_preset):
        return False
    haystack = " ".join([
        str(visual_description or "").strip().lower(),
        str(tone or "").strip().lower(),
    ])
    return bool(re.search(
        r"\b(psychology|psychological|brain|mind|mental|behavior|behaviour|manipulat|bias|blind spot|subconscious|attention|dopamine|emotion|thought|memory|control|choice|choices|decision|decisions|influence|gaslight|fear|shame|envy|desire|hidden behavior|trigger)\b",
        haystack,
    ))


def _longform_fallback_visual_focus(topic: str, input_title: str) -> str:
    raw = str(topic or input_title or "").strip()
    if not raw:
        return "the central mechanism, object, or environment being explained"
    cleaned = re.sub(r"[^a-zA-Z0-9\s]", " ", raw)
    cleaned = re.sub(r"\btop\s+\d+\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(
        r"\b(disturbing|shocking|crazy|insane|mind[-\s]?blowing|secrets?|facts?|truth|ultimate|complete|full)\b",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"\b(explained|breakdown|hidden from you|hides from you|you need to know|that no one tells you)\b",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\byour\b", "the subject's", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -,:")
    if not cleaned:
        return "the central mechanism, object, or environment being explained"
    if len(cleaned.split()) > 7 or re.search(r"\b(top|best|worst|secrets?|facts?|reasons?|ways?)\b", cleaned, flags=re.IGNORECASE):
        return "the central mechanism, object, or environment being explained"
    return cleaned.lower()


_LONGFORM_PROMPT_CONTROL_PATTERNS = [
    r"\bstart directly on\b",
    r"\bopen on\b",
    r"\bsubject focus\b",
    r"\bscene role\b",
    r"\bedit intensity\b",
    r"\bcaption rhythm\b",
    r"\bmotion execution\b",
    r"\bengagement beat\b",
    r"\bretention objective\b",
    r"\bcurrent catalyst archetype\b",
    r"\bnext run should\b",
    r"\bmake elite\b",
    r"\buse premium\b",
    r"\bavoid literal\b",
    r"\bpackaging iteration depth\b",
    r"\bhook proof density\b",
    r"\bopening variation\b",
    r"\bmake the first \d+\s*(to|-)\s*\d+ seconds\b",
    r"\bshorten setup brutally\b",
    r"\bpromise, proof, and reversal\b",
    r"\bkeep captions\b",
    r"\bbias every chapter\b",
    r"\bchange staging aggressively\b",
    r"\bkeep the first scene of each chapter\b",
    r"\bopen on a different\b",
    r"\bhero[-\s]?object\b",
    r"\baggressive spotlight\b",
    r"\bbuilt around\b",
    r"\bvisual motif\b",
    r"\bvariation rule\b",
    r"\bavoid repeating the same\b",
    r"\bproof[-\s]?first\b",
    r"\barchitectural dolly\b",
]


def _longform_scene_sentences(text: str) -> list[str]:
    return [
        part.strip()
        for part in re.split(r"(?<=[.!?])\s+", str(text or "").strip())
        if part and part.strip()
    ]


def _longform_sentence_is_prompt_control(sentence: str) -> bool:
    lowered = str(sentence or "").strip().lower()
    if not lowered:
        return False
    return any(re.search(pattern, lowered, flags=re.IGNORECASE) for pattern in _LONGFORM_PROMPT_CONTROL_PATTERNS)


def _clean_longform_scene_text(text: str) -> str:
    kept = [s for s in _longform_scene_sentences(text) if not _longform_sentence_is_prompt_control(s)]
    cleaned = " ".join(kept).strip()
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -,:")
    return cleaned


def _longform_text_is_strategy_garbage(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return True
    if _longform_sentence_is_prompt_control(lowered):
        return True
    return bool(re.search(
        r"\b(scene-setting|proof mode|opening beat|captions?|chapter|seconds?|setup|packaging|thumbnail|visual lock|hook proof|variation|retention|framing|spotlight|built around|dolly|proof[-\s]?first)\b",
        lowered,
        flags=re.IGNORECASE,
    ))


def _longform_psychology_focus_phrase(candidate: str, scene_role: str) -> str:
    cleaned = _clip_text(_clean_longform_scene_text(candidate), 140)
    lowered = cleaned.lower()
    if (
        not cleaned
        or _longform_text_is_strategy_garbage(cleaned)
        or len(cleaned.split()) > 9
        or bool(re.search(r"\b(hero[-\s]?object|spotlight|built around|visual motif|framing|proof[-\s]?first)\b", lowered))
        or "central mechanism, object, or environment being explained" in lowered
        or "your decisions" in lowered
        or "quietly rewriting" in lowered
    ):
        fallbacks = {
            "opening": "a private decision being quietly steered",
            "middle": "the hidden trigger behind a confident choice",
            "closer": "the personal cost of a manipulated decision",
        }
        return fallbacks.get(scene_role, "a manipulated choice unfolding in real time")
    return cleaned


def _longform_psychology_topic_subject(topic: str, input_title: str, scene_role: str) -> str:
    haystack = " ".join([str(topic or "").strip(), str(input_title or "").strip()]).lower()
    if re.search(r"\b(decision|decisions|choice|choices)\b", haystack):
        variants = {
            "opening": "a confident decision being quietly rewritten by status pressure and social cues",
            "middle": "the instant a private choice stops being private and starts being steered",
            "closer": "the personal cost after someone realizes the decision was never fully their own",
        }
        return variants.get(scene_role, "a confident decision being quietly rewritten by status pressure and social cues")
    if re.search(r"\b(attention|focus|distraction|notice|noticed)\b", haystack):
        variants = {
            "opening": "attention being hijacked before the subject notices",
            "middle": "the exact cue that redirects attention away from the truth",
            "closer": "the cost of realizing attention was manipulated too late",
        }
        return variants.get(scene_role, "attention being hijacked before the subject notices")
    if re.search(r"\b(memory|remember|recall|false memory)\b", haystack):
        variants = {
            "opening": "memory being quietly reshaped in a conversation that feels harmless",
            "middle": "the subtle cue that changes what the subject thinks they remember",
            "closer": "the fallout after a rewritten memory gets treated as truth",
        }
        return variants.get(scene_role, "memory being quietly reshaped in a conversation that feels harmless")
    if re.search(r"\b(bias|blind spot|subconscious|belief|beliefs)\b", haystack):
        variants = {
            "opening": "a blind spot being exploited before the subject can defend against it",
            "middle": "the invisible cue that makes a bad belief feel self-generated",
            "closer": "the consequence of discovering a bias only after it made the decision",
        }
        return variants.get(scene_role, "a blind spot being exploited before the subject can defend against it")
    if re.search(r"\b(manipulat|influence|control|obey|compliance|status|pressure)\b", haystack):
        variants = {
            "opening": "a subtle manipulation landing in a room where nobody looks openly dangerous",
            "middle": "the exact social cue that flips confidence into compliance",
            "closer": "the moment the manipulation becomes visible only after the damage is done",
        }
        return variants.get(scene_role, "a subtle manipulation landing in a room where nobody looks openly dangerous")
    if re.search(r"\b(brain|mind|psychology|behavior|behaviour)\b", haystack):
        variants = {
            "opening": "the hidden cue that makes the mind accept a bad decision as its own idea",
            "middle": "the moment a normal-looking environment quietly rewires what feels like free choice",
            "closer": "the human consequence after the mind follows a script it never noticed",
        }
        return variants.get(scene_role, "the hidden cue that makes the mind accept a bad decision as its own idea")
    return {
        "opening": "a private choice being invisibly steered in a real room",
        "middle": "the trigger that quietly redirects a human decision",
        "closer": "the cost of realizing the decision was manipulated",
    }.get(scene_role, "a private choice being invisibly steered in a real room")


def _longform_scene_focus_phrase(
    *,
    narration: str,
    chapter_blueprint: dict | None = None,
    topic: str = "",
    input_title: str = "",
) -> str:
    cleaned_narration = _clean_longform_scene_text(narration)
    if cleaned_narration and len(cleaned_narration.split()) <= 18 and not _longform_text_is_strategy_garbage(cleaned_narration):
        return _clip_text(cleaned_narration, 140)
    for candidate in [
        str((chapter_blueprint or {}).get("focus", "") or "").strip(),
        str((chapter_blueprint or {}).get("improvement_focus", "") or "").strip(),
        str((chapter_blueprint or {}).get("visual_motif", "") or "").strip(),
        _longform_fallback_visual_focus(topic, input_title),
    ]:
        cleaned = _clean_longform_scene_text(candidate)
        if cleaned and not _longform_text_is_strategy_garbage(cleaned):
            return _clip_text(cleaned, 140)
    return "the hidden mechanism behind the behavior"


def _longform_scene_looks_like_machine_filler(text: str, archetype_key: str = "") -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return True
    generic_machine = bool(re.search(r"\b(gear|gears|cog|cogs|reactor|capsule|widget|widgets|module|modules|machine core|machine assembly|console|device housing)\b", lowered))
    generic_stage = bool(re.search(r"\b(isolated hero-object|hero object stage|hero object under aggressive spotlight|macro mechanism cutaway|process-diagram environment|clean cinematic lighting|readable subject hierarchy|strong depth|designed in 3d|explainer frame focused on)\b", lowered))
    literal_brain = bool(re.search(r"\bbrain\b", lowered))
    human_context = bool(re.search(r"\b(person|people|face|faces|eyes|hand|hands|body|mirror|surveillance|dossier|interrogation|boardroom|room|corridor|office|crowd|couple|conversation|choice|decision|consequence)\b", lowered))
    if archetype_key == "psychology_documentary":
        if generic_machine or generic_stage:
            return True
        if literal_brain and not human_context:
            return True
    if archetype_key == "crime_documentary":
        if generic_machine or generic_stage:
            return True
        if literal_brain and not human_context:
            return True
    if archetype_key == "systems_documentary" and generic_stage:
        return True
    return False


def _longform_psychology_scene_too_generic(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return True
    generic_room = bool(re.search(r"\b(dossier room|surveillance[-\s]?grade|archive room|evidence room|boardroom|office|meeting room|interrogation room|archive|backroom)\b", lowered))
    human_action = bool(re.search(r"\b(person|people|executive|subject|man|woman|faces?|hands?|body language|conversation|meeting|dinner|interview|pressured|steered|manipulator|gaze|posture|choice|decision|consequence|compliance)\b", lowered))
    abstract_trigger = bool(re.search(r"\b(hidden trigger|social cue|pressure shaping|influence|leverage point)\b", lowered))
    static_prop_bias = bool(re.search(r"\b(table|desk|room|archive|dossier|surveillance)\b", lowered)) and not human_action
    if generic_room and not human_action:
        return True
    if abstract_trigger and not human_action:
        return True
    if static_prop_bias:
        return True
    return False


def _longform_crime_scene_too_generic(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return True
    generic_room = bool(re.search(r"\b(boardroom|archive room|dossier room|meeting room|office|conference room|backroom|control room)\b", lowered))
    human_action = bool(re.search(r"\b(person|people|face|faces|hands?|body|victim|defendant|witness|lawyer|agent|suspect|conversation|arguing|restrained|dragged|signing|pointing|interview|walking|running|surveillance)\b", lowered))
    case_context = bool(re.search(r"\b(case|court|complaint|evidence|records|timeline|map|route|studio|phone|call log|license plate|monitor|surveillance|filing|docket|contract|ankle monitor|home confinement|cash|watch|jewelry|robbery|kidnapp)\b", lowered))
    static_prop_bias = bool(re.search(r"\b(table|desk|room|archive|boardroom|dossier|office)\b", lowered)) and not human_action and not case_context
    generic_systems = bool(re.search(r"\b(boardroom power-map|ownership|money-flow|institutional pressure|hidden system|infrastructure)\b", lowered))
    skeleton_crowd = bool(re.search(r"\b(skeletons|multiple skeleton|committee of skeleton|skeleton meeting|x-ray humans?)\b", lowered))
    if generic_room and not case_context:
        return True
    if static_prop_bias or generic_systems or skeleton_crowd:
        return True
    return False


def _build_documentary_scene_repair(
    *,
    narration: str,
    chapter_blueprint: dict | None = None,
    scene_index: int = 0,
    total_scenes: int = 0,
    topic: str = "",
    input_title: str = "",
    archetype_key: str = "systems_documentary",
    template: str = "",
) -> tuple[str, str]:
    scene_role = "opening" if scene_index == 0 else ("closer" if total_scenes > 0 and scene_index >= max(total_scenes - 2, 0) else "middle")
    focus_phrase = _longform_scene_focus_phrase(
        narration=narration,
        chapter_blueprint=chapter_blueprint,
        topic=topic,
        input_title=input_title,
    )
    if archetype_key == "psychology_documentary":
        focus_phrase = _longform_psychology_focus_phrase(focus_phrase, scene_role)
        topic_subject = _longform_psychology_topic_subject(topic, input_title, scene_role)
        if focus_phrase in {
            "a private decision being quietly steered",
            "the hidden trigger behind a confident choice",
            "the personal cost of a manipulated decision",
            "a manipulated choice unfolding in real time",
        }:
            focus_phrase = topic_subject
        proof_modes = [
            "a high-rise office or boardroom at dusk where two or three people are mid-conversation and one person is being quietly pushed toward the wrong choice by timing, status, and body language",
            "a surveillance-led evidence room where screens, photos, and posture reveal the instant a planted suggestion takes hold in a real person",
            "a mirror-and-shadow consequence setup where a subject faces the split between private instinct and outside pressure inside a believable room",
            "an upscale dinner, interview, or executive meeting where one calm manipulator quietly steers another person's decision without raising their voice",
            "an archive, backroom, or intelligence-style office where fragments of evidence expose how a trigger became a consequence for a real person",
            "a symbolic but human-scale psychology environment built into architecture using reflections, masks, strings, split selves, and attention funnels with people still present in frame",
        ]
        action_line = (
            "Show one human consequence, one hidden trigger, and one visible power imbalance. At least one person must be clearly visible; no empty rooms, no untouched desks, and no static prop-only staging."
            if scene_role == "opening"
            else "Show the trigger, the leverage point, and the personal consequence in the same frame. Keep readable faces, hands, posture, and eye lines so the influence feels invasive and real."
            if scene_role == "middle"
            else "Land on a premium consequence or reversal frame with a real person carrying the cost of the hidden behavior. No empty aftermath room unless the narration is explicitly about absence."
        )
    elif archetype_key == "crime_documentary":
        proof_modes = [
            "a photoreal human close-up or medium shot where the named subject is under visible pressure and one decisive evidence cue is present in the same frame",
            "a premium 3D case-board room where one dominant evidence thread ties together complaint pages, phones, photos, and a studio floor plan",
            "a map-and-timeline evidence frame showing travel, arrival, and one decisive contradiction through routes, timestamps, and surveillance stills",
            "a believable recording studio, hallway, SUV, or curbside pressure scene where the human stakes and one key evidence cue are visible together",
            "a court hallway, interview room, or custody-adjacent consequence frame where one real human subject, one legal cue, and one emotional contradiction carry the image",
            "a documentary control-room frame built from monitors, call logs, plate-reader hits, rental records, and redacted filing fragments",
            "a court, custody, interview, or aftermath consequence frame where legal exposure and human cost are obvious at a glance",
            "a public-image versus case-evidence split frame using phones, social posts, records, and one dominant contradiction",
        ]
        action_line = (
            "Make the contradiction visible immediately. Use at least one readable human stake, one evidence object or screen, and one designed environment. Never stage a generic meeting room or untouched dossier table."
            if scene_role == "opening"
            else "Show the proof trail and the consequence in the same frame: phone, filing, route, monitor, contract, surveillance still, or witness pressure cue. Keep readable faces, hands, and posture so the human stakes lead the image."
            if scene_role == "middle"
            else "Land on the clearest consequence frame: court exposure, home-confinement irony, public-post contradiction, or the moment the evidence trail closes in."
        )
    else:
        proof_modes = [
            "a premium dossier-table proof frame inside a real room with one decisive evidence trail",
            "a boardroom power-map composition exposing winners, losers, and hidden leverage",
            "a system proof frame that shows incentives, pressure, and the visible outcome in one environment",
            "an archive or ledger proof frame that makes control, money flow, or institutional pressure obvious",
            "an infrastructure or network view showing where the system actually routes power",
            "a before-versus-after consequence frame that makes the hidden system legible at a glance",
        ]
        action_line = (
            "Make the contradiction or proof obvious before any explanation so the viewer instantly understands the stakes."
            if scene_role == "opening"
            else "Show the hidden system and the visible consequence in the same frame so the mechanism feels expensive and readable."
            if scene_role == "middle"
            else "Land on the payoff frame that proves who benefits, who loses, and why the system matters."
        )
    proof_mode = proof_modes[scene_index % len(proof_modes)]
    if scene_role == "opening":
        if archetype_key == "psychology_documentary":
            narration_line = "The decision looks personal, but the pressure shaping it is already in the room."
        elif archetype_key == "crime_documentary":
            narration_line = f"The case around {focus_phrase} stops looking like a headline once the first human consequence and evidence cue appear together."
        else:
            narration_line = f"What looks natural around {focus_phrase} is actually being steered by a hidden system."
    elif scene_role == "middle":
        if archetype_key == "psychology_documentary":
            narration_line = f"The trigger behind {focus_phrase} only makes sense once the leverage and consequence are visible together."
        elif archetype_key == "crime_documentary":
            narration_line = f"The contradiction around {focus_phrase} only lands once the proof trail and the human pressure are visible together."
        else:
            narration_line = f"The mechanism behind {focus_phrase} becomes obvious once the trigger and consequence are shown together."
    else:
        if archetype_key == "psychology_documentary":
            narration_line = "By the time the subject notices the pattern, the consequence is already locked in."
        elif archetype_key == "crime_documentary":
            narration_line = f"The case around {focus_phrase} is already legible once the evidence trail is shown end-to-end."
        else:
            narration_line = f"The consequence around {focus_phrase} is already visible by the time the system is noticed."
    visual_line = (
        (
            f"Premium cinematic psychology documentary scene set in {proof_mode}. Center the frame on {focus_phrase}. "
            f"{action_line} Use controlled cinematic CG, premium human-scale interiors, restrained symbolism, and real social pressure instead of abstract props. "
            f"No empty room, no untouched table, no static dossier still life, no isolated object pedestal, no literal anatomy, and no floating machine filler."
            if archetype_key == "psychology_documentary"
            else f"Premium cinematic crime-case documentary scene set in {proof_mode}. Center the frame on {focus_phrase}. "
            f"{action_line} Use designed evidence layouts, source-capture logic, readable human consequence, and premium editorial CG. "
            f"No generic boardroom filler, no untouched archive room, no committee of skeletons, no x-ray humans, no floating machine props, no anonymous stock-person face, and no legible article text."
            if archetype_key == "crime_documentary"
            else f"Premium 3D systems documentary frame set in {proof_mode}, visualizing {focus_phrase}. "
            f"{action_line} Use a real environment, clean premium CG staging, and no isolated object pedestal, sterile lab filler, or floating machine props."
        )
    )
    if str(template or "").strip().lower() == "skeleton" and archetype_key == "crime_documentary":
        visual_line = _clip_text(
            visual_line
            + " Use the recurring Cryptic Science skeleton only as an editorial host, witness, or silhouette when useful. Keep case participants human whenever the beat is about evidence, pressure, or consequence. Never turn the whole room into skeleton characters.",
            460,
        )
    return (_clip_text(narration_line, 180), _clip_text(visual_line, 420))


def _repair_longform_generated_scenes(
    scenes: list,
    *,
    template: str,
    format_preset: str,
    topic: str,
    input_title: str,
    edit_blueprint: dict | None = None,
    chapter_blueprint: dict | None = None,
    allow_narration_rewrite: bool = True,
) -> list[dict]:
    if str(template or "").strip().lower() not in {"story", "skeleton"} or str(format_preset or "").strip().lower() != "documentary":
        return [dict(scene or {}) for scene in list(scenes or [])]
    repaired: list[dict] = []
    total_scenes = len(list(scenes or []))
    for scene_index, raw_scene in enumerate(list(scenes or [])):
        scene = dict(raw_scene or {})
        narration = str(scene.get("narration", "") or "").strip()
        visual_description = str(scene.get("visual_description", "") or "").strip()
        cleaned_narration = _clean_longform_scene_text(narration)
        cleaned_visual = _clean_longform_scene_text(visual_description)
        archetype_key = _longform_documentary_archetype(
            edit_blueprint=edit_blueprint,
            chapter_blueprint=chapter_blueprint,
            format_preset=format_preset,
            topic=topic,
            input_title=input_title,
            narration=cleaned_narration or narration,
            visual_description=cleaned_visual or visual_description,
        )
        narration_contaminated = bool(narration) and (not cleaned_narration or cleaned_narration != narration)
        visual_contaminated = bool(visual_description) and (not cleaned_visual or cleaned_visual != visual_description)
        visual_filler = _longform_scene_looks_like_machine_filler(cleaned_visual or visual_description, archetype_key=archetype_key)
        visual_generic = (
            (archetype_key == "psychology_documentary" and _longform_psychology_scene_too_generic(cleaned_visual or visual_description))
            or (archetype_key == "crime_documentary" and _longform_crime_scene_too_generic(cleaned_visual or visual_description))
        )
        if allow_narration_rewrite and (not cleaned_narration or narration_contaminated):
            repaired_narration, repaired_visual = _build_documentary_scene_repair(
                narration=cleaned_narration or narration or visual_description,
                chapter_blueprint=chapter_blueprint,
                scene_index=scene_index,
                total_scenes=total_scenes,
                topic=topic,
                input_title=input_title,
                archetype_key=archetype_key,
                template=template,
            )
            cleaned_narration = repaired_narration
            if visual_contaminated or visual_filler or visual_generic or not cleaned_visual:
                cleaned_visual = repaired_visual
        elif visual_contaminated or visual_filler or visual_generic or not cleaned_visual:
            _, repaired_visual = _build_documentary_scene_repair(
                narration=cleaned_narration or narration or visual_description,
                chapter_blueprint=chapter_blueprint,
                scene_index=scene_index,
                total_scenes=total_scenes,
                topic=topic,
                input_title=input_title,
                archetype_key=archetype_key,
                template=template,
            )
            cleaned_visual = repaired_visual
        scene["narration"] = cleaned_narration or narration or f"Chapter beat {scene_index + 1}."
        scene["visual_description"] = cleaned_visual or visual_description or scene["narration"]
        repaired.append(scene)
    return repaired


def _render_longform_blueprint_prompt_context(
    edit_blueprint: dict | None = None,
    chapter_blueprint: dict | None = None,
) -> str:
    edit_blueprint = dict(edit_blueprint or {})
    chapter_blueprint = dict(chapter_blueprint or {})
    hook_strategy = dict(edit_blueprint.get("hook_strategy") or {})
    motion_strategy = dict(edit_blueprint.get("motion_strategy") or {})
    sound_strategy = dict(edit_blueprint.get("sound_strategy") or {})
    execution_strategy = dict(edit_blueprint.get("execution_strategy") or {})
    lines = [
        ("Blueprint archetype: " + str(edit_blueprint.get("archetype_label", "") or "").strip()) if str(edit_blueprint.get("archetype_label", "") or "").strip() else "",
        ("Hook mission: " + str(hook_strategy.get("promise", "") or "").strip()) if str(hook_strategy.get("promise", "") or "").strip() else "",
        ("First 30 seconds: " + str(hook_strategy.get("first_30s_mission", "") or "").strip()) if str(hook_strategy.get("first_30s_mission", "") or "").strip() else "",
        ("Chapter focus: " + str(chapter_blueprint.get("focus", "") or "").strip()) if str(chapter_blueprint.get("focus", "") or "").strip() else "",
        ("Chapter hook job: " + str(chapter_blueprint.get("hook_job", "") or "").strip()) if str(chapter_blueprint.get("hook_job", "") or "").strip() else "",
        ("Chapter visual motif: " + str(chapter_blueprint.get("visual_motif", "") or "").strip()) if str(chapter_blueprint.get("visual_motif", "") or "").strip() else "",
        ("Motion rules: " + "; ".join(list(motion_strategy.get("visual_rules") or [])[:4])) if list(motion_strategy.get("visual_rules") or []) else "",
        ("Sound rules: " + "; ".join(list(sound_strategy.get("mix_notes") or [])[:3])) if list(sound_strategy.get("mix_notes") or []) else "",
        ("Voice rules: " + "; ".join(list(sound_strategy.get("voice_direction") or [])[:2])) if list(sound_strategy.get("voice_direction") or []) else "",
        ("Execution: " + "; ".join([
            ("opening=" + str(execution_strategy.get("opening_intensity", "") or "").strip()) if str(execution_strategy.get("opening_intensity", "") or "").strip() else "",
            ("interrupts=" + str(execution_strategy.get("interrupt_strength", "") or "").strip()) if str(execution_strategy.get("interrupt_strength", "") or "").strip() else "",
            ("cuts=" + str(execution_strategy.get("cut_profile", "") or "").strip()) if str(execution_strategy.get("cut_profile", "") or "").strip() else "",
            ("visual variation=" + str(execution_strategy.get("visual_variation_rule", "") or "").strip()) if str(execution_strategy.get("visual_variation_rule", "") or "").strip() else "",
        ])) if execution_strategy else "",
    ]
    lines = [line for line in lines if line]
    if not lines:
        return ""
    return _clip_text(
        "--- INTERNAL PLANNING NOTES (DO NOT COPY ANY OF THIS TEXT INTO NARRATION OR VISUAL FIELDS — these are direction notes for YOU the writer, NOT script content) ---\n"
        + "\n".join(lines),
        2200,
    )


def _normalize_longform_scenes_for_render(scenes: list) -> list:
    normalized = []
    for idx, raw_scene in enumerate(scenes or []):
        scene = dict(raw_scene or {})
        narration = str(scene.get("narration", "") or "").strip()
        visual_description = str(scene.get("visual_description", "") or "").strip()
        motion_direction = str(scene.get("motion_direction", "") or "").strip()
        sfx_direction = str(scene.get("sfx_direction", "") or "").strip()
        engagement_purpose = str(scene.get("engagement_purpose", "") or "").strip()
        if not narration:
            narration = f"Chapter beat {idx + 1}."
        if not visual_description:
            visual_description = narration or f"Scene {idx + 1} visual"
        # Long-form chapter previews and final render are hard-locked to 5s/scene.
        duration = 5.0
        scene["narration"] = narration
        scene["visual_description"] = visual_description
        scene["motion_direction"] = motion_direction or "controlled push or cutaway that reinforces the narration beat"
        scene["sfx_direction"] = sfx_direction or "clean cinematic accent that sells the reveal without overpowering narration"
        scene["engagement_purpose"] = engagement_purpose or "move the viewer to the next beat immediately"
        scene["scene_num"] = int(scene.get("scene_num", idx + 1) or (idx + 1))
        scene["duration_sec"] = round(duration, 2)
        normalized.append(scene)
    return normalized


def _build_longform_scene_execution_prompt(
    *,
    scene: dict,
    template: str,
    format_preset: str,
    topic: str = "",
    input_title: str = "",
    edit_blueprint: dict | None = None,
    chapter_blueprint: dict | None = None,
    scene_index: int = 0,
    total_scenes: int = 0,
    skeleton_anchor: str = "",
    art_style: str = "auto",
    has_subject_reference: bool = False,
    subject_reference_name: str = "",
) -> str:
    scene = dict(scene or {})
    visual_description_raw = str(scene.get("visual_description", "") or "").strip()
    visual_description = _clip_text(_clean_longform_scene_text(visual_description_raw) or visual_description_raw, 420)
    motion_direction = str(scene.get("motion_direction", "") or "").strip()
    named_human_lock = _named_human_subject_likeness_lock(
        visual_description_raw,
        visual_description,
        str(scene.get("narration", "") or ""),
        topic,
        input_title,
        subject_reference_name,
    )
    execution = _catalyst_scene_execution_profile(
        edit_blueprint=edit_blueprint,
        chapter_blueprint=chapter_blueprint,
        scene_index=scene_index,
        total_scenes=total_scenes,
    )
    chapter_blueprint = dict(chapter_blueprint or {})
    visual_motif = _clip_text(str(chapter_blueprint.get("visual_motif", "") or ""), 180)
    visual_variation_rule = _clip_text(str(execution.get("visual_variation_rule", "") or ""), 180)
    if _longform_prefers_3d_documentary_visuals(template, format_preset):
        documentary_archetype = _longform_documentary_archetype(
            edit_blueprint=edit_blueprint,
            chapter_blueprint=chapter_blueprint,
            format_preset=format_preset,
            narration=str(scene.get("narration", "") or ""),
            visual_description=visual_description,
        )
        documentary_visual_description = visual_description
        lowered_visual = documentary_visual_description.lower()
        if (
            not documentary_visual_description
            or _longform_text_is_strategy_garbage(documentary_visual_description)
            or _longform_scene_looks_like_machine_filler(documentary_visual_description, archetype_key=documentary_archetype)
            or (documentary_archetype == "psychology_documentary" and _longform_psychology_scene_too_generic(documentary_visual_description))
            or "central mechanism, object, or environment being explained" in lowered_visual
            or "visual motif" in lowered_visual
            or "variation rule" in lowered_visual
            or "hero object" in lowered_visual
            or "aggressive spotlight" in lowered_visual
            or "built around" in lowered_visual
        ):
            _, documentary_visual_description = _build_documentary_scene_repair(
                narration=str(scene.get("narration", "") or documentary_visual_description or ""),
                chapter_blueprint=chapter_blueprint,
                scene_index=scene_index,
                total_scenes=total_scenes,
                topic=topic,
                input_title=input_title,
                archetype_key=documentary_archetype,
                template=template,
            )
        documentary_visual_description = re.sub(
            r"^(?:Fern-grade|Premium cinematic) (?:premium 3D )?(psychology|systems|crime-case) documentary scene,\s*obviously CG,[^.]+\.\s*",
            "",
            str(documentary_visual_description or "").strip(),
            flags=re.IGNORECASE,
        ).strip()
        documentary_prefix = (
            "Premium cinematic psychology documentary scene, obviously CG, human-scale, emotionally invasive, and set inside a real designed environment. No live-action photography, no isolated object pedestal, no literal anatomy."
            if documentary_archetype == "psychology_documentary"
            else "Premium cinematic crime-case documentary scene, obviously CG, evidence-first, human-scale, and built around real case material. No live-action photography, no generic boardroom filler, no untouched dossier table, and no multi-skeleton crowd."
            if documentary_archetype == "crime_documentary"
            else "Premium cinematic systems documentary scene, obviously CG, proof-first, human-scale, and built around an expensive documentary environment. No live-action photography, no isolated object pedestal, no glossy machine hero shot."
        )
        if str(documentary_visual_description or "").strip().lower().startswith(("fern-grade premium 3d", "premium cinematic")):
            documentary_prefix = ""
        documentary_environment_guidance = (
            "Prefer active human pressure scenes, mirrored conversations, surveillance moments, executive meetings, interviews, and consequence frames over empty rooms, untouched desks, or static dossier tables."
            if documentary_archetype == "psychology_documentary"
            else "Prefer named-human close-ups, studio interiors, court corridors, phone and records evidence, source-capture monitors, route and timeline boards, surveillance-led consequence frames, and one dominant contradiction over boardrooms, static dossier tables, or empty archive rooms."
            if documentary_archetype == "crime_documentary"
            else "Prefer ancient temples, marble columns, agoras, amphitheaters, classical architecture, ancient battlefields, Mediterranean landscapes, torchlit interiors, scroll-filled libraries, and historical human consequence over modern offices or boardrooms."
            if re.search(r"\b(ancient|greek|greece|roman|rome|medieval|renaissance|classical|hellenic|sparta|athens|egypt|persian|byzantine|ottoman|viking|celtic|feudal)\b", str(topic or "").lower() + " " + str(input_title or "").lower())
            else "Prefer designed rooms, dossier tables, surveillance setups, boardrooms, archives, maps, human consequence, and grounded symbolic environments over isolated floating objects."
        )
        human_priority_lock = _longform_named_human_priority_lock(
            visual_description_raw,
            visual_description,
            str(scene.get("narration", "") or ""),
            topic,
            input_title,
            subject_reference_name,
            template=template,
            archetype_key=documentary_archetype,
        )
        subject_reference_phrase = (
            f"Use the attached subject reference image for {subject_reference_name} only: preserve the same face, hair, skin tone, build, age cues, and signature styling whenever that person appears. Keep the scene Magnates-grade in framing, lighting, set design, and CG discipline."
            if has_subject_reference and str(subject_reference_name or "").strip()
            else "Use the attached subject reference image for identity only: preserve the same face, hair, skin tone, build, age cues, and signature styling whenever that person appears. Keep the scene Magnates-grade in framing, lighting, set design, and CG discipline."
            if has_subject_reference
            else "Use the attached Cryptic Science case reference sheet for framing, lighting, set design, source-capture composition, and evidence-board discipline only; never reproduce timestamps, site names, captions, or layout text from the reference."
            if documentary_archetype == "crime_documentary"
            else "Use the attached Magnates reference sheet for cinematic framing, lighting, set design, and CG discipline only; never copy text, logos, or layouts literally from the reference."
        )
        documentary_output_guardrail = (
            "No legible text overlays, no chapter cards, no branded logos, no watermarks, no readable article text, and no giant wall text or monitor text in the scene."
            if documentary_archetype == "crime_documentary"
            else "No text overlays, no chapter cards, no labels, no UI panels, no watermarks, no pseudo-text in the scene."
        )
        visual_parts = _dedupe_preserve_order([
            documentary_visual_description,
            f"Series anchor: {execution.get('series_anchor', '')}." if execution.get("series_anchor") and str(format_preset or "").strip().lower() == "recap" else "",
            "Open on the payoff image immediately before adding explanation." if execution.get("is_opening") else "",
            "Close with a clean consequence frame or controlled reveal that tees up the next beat." if execution.get("is_closer") else "",
            subject_reference_phrase,
            named_human_lock,
            human_priority_lock,
            "Use the recurring skeleton as an editorial host only when useful; keep case participants human and never fill a room with multiple skeleton people." if documentary_archetype == "crime_documentary" and str(template or "").strip().lower() == "skeleton" else "",
            documentary_environment_guidance,
            documentary_output_guardrail,
        ], max_items=8, max_chars=240)
        visual_delta = " ".join(part for part in visual_parts if part).strip()
        full_documentary_prompt = f"{documentary_prefix} {visual_delta}".strip()
        # For skeleton template in documentary mode: inject condensed skeleton identity
        # (the full _build_skeleton_image_prompt creates prompts too long for image models)
        if str(template or "").strip().lower() == "skeleton":
            skeleton_identity = (
                "Photorealistic 3D cinematic render. The main character is a translucent glass-skinned humanoid skeleton figure "
                "with ivory-white bones visible through a smooth transparent glass body shell, realistic natural human eyes "
                "(visible iris, pupil, wet reflections, natural eye color, NOT glowing). "
                "The glass skin refracts light with subtle caustic highlights. "
            )
            return f"{skeleton_identity}{full_documentary_prompt}".strip()
        return _build_scene_prompt_with_reference(
            template=template,
            visual_description=full_documentary_prompt,
            quality_mode="cinematic",
            skeleton_anchor=skeleton_anchor,
            reference_dna={},
            reference_lock_mode="strict",
            art_style=art_style,
        )
    visual_parts = _dedupe_preserve_order([
        visual_description,
        f"Scene role: {execution['scene_role'].replace('_', ' ')}." if execution.get("scene_role") else "",
        f"Series anchor: {execution.get('series_anchor', '')}." if execution.get("series_anchor") and str(format_preset or "").strip().lower() == "recap" else "",
        (
            f"Preserve the exact same subject identity for {subject_reference_name} from the attached reference image whenever that person appears."
            if has_subject_reference and str(subject_reference_name or "").strip()
            else "Preserve the exact same subject identity from the attached reference image whenever that person appears."
            if has_subject_reference
            else ""
        ),
        named_human_lock,
        f"Visual motif: {visual_motif}." if visual_motif else "",
        f"Variation rule: {visual_variation_rule}." if visual_variation_rule else "",
        "Pattern interrupt required in composition, scale, or contrast." if execution.get("is_interrupt") else "",
        "Open on the payoff image immediately before adding explanation." if execution.get("is_opening") else "",
        "Close with a clean consequence frame or controlled reveal that tees up the next beat." if execution.get("is_closer") else "",
        "No text overlays, no chapter cards, no labels, no UI panels, no watermarks.",
    ], max_items=8, max_chars=220)
    visual_delta = " ".join(part for part in visual_parts if part).strip()
    return _build_scene_prompt_with_reference(
        template=template,
        visual_description=visual_delta,
        quality_mode="cinematic",
        skeleton_anchor=skeleton_anchor,
        reference_dna={},
        reference_lock_mode="strict",
        art_style=art_style,
    )


def _build_longform_scene_motion_prompt(
    *,
    scene: dict,
    edit_blueprint: dict | None = None,
    chapter_blueprint: dict | None = None,
    scene_index: int = 0,
    total_scenes: int = 0,
) -> str:
    scene = dict(scene or {})
    execution = _catalyst_scene_execution_profile(
        edit_blueprint=edit_blueprint,
        chapter_blueprint=chapter_blueprint,
        scene_index=scene_index,
        total_scenes=total_scenes,
    )
    parts = _dedupe_preserve_order([
        str(scene.get("visual_description", "") or ""),
        str(scene.get("motion_direction", "") or ""),
        f"Series anchor: {execution.get('series_anchor', '')}" if execution.get("series_anchor") else "",
        f"Edit intensity: {execution.get('execution_intensity', '')}" if execution.get("execution_intensity") else "",
        f"Cut profile: {execution.get('cut_profile', '')}" if execution.get("cut_profile") else "",
        *list(execution.get("niche_execution_notes") or [])[:2],
        *list(execution.get("motion_cues") or [])[:4],
        "Start with a stronger push-in and immediate reveal." if execution.get("is_opening") else "",
        "Use a visible pattern interrupt and contrast reset mid-shot." if execution.get("is_interrupt") else "",
        "Let the final move linger for payoff before the next cut." if execution.get("payoff_hold_sec", 0) >= 1.2 else "",
        "Finish with a more deliberate pull-back or consequence hold." if execution.get("is_closer") else "",
    ], max_items=8, max_chars=180)
    return " ".join(parts).strip()


def _build_longform_scene_sfx_brief(
    *,
    scene: dict,
    edit_blueprint: dict | None = None,
    chapter_blueprint: dict | None = None,
    scene_index: int = 0,
    total_scenes: int = 0,
) -> str:
    scene = dict(scene or {})
    execution = _catalyst_scene_execution_profile(
        edit_blueprint=edit_blueprint,
        chapter_blueprint=chapter_blueprint,
        scene_index=scene_index,
        total_scenes=total_scenes,
    )
    return " ".join(part for part in [
        str(scene.get("visual_description", "") or "").strip(),
        f"SFX direction: {str(scene.get('sfx_direction', '') or '').strip()}." if str(scene.get("sfx_direction", "") or "").strip() else "",
        f"Engagement purpose: {str(scene.get('engagement_purpose', '') or '').strip()}." if str(scene.get("engagement_purpose", "") or "").strip() else "",
        f"Series anchor: {execution.get('series_anchor', '')}." if execution.get("series_anchor") else "",
        f"Sound density: {execution.get('sound_density', '')}." if execution.get("sound_density") else "",
        f"Caption rhythm: {execution.get('caption_rhythm', '')}." if execution.get("caption_rhythm") else "",
        f"Niche execution: {'; '.join(execution.get('niche_execution_notes') or [])}." if execution.get("niche_execution_notes") else "",
        f"Sound execution: {'; '.join(execution.get('sound_cues') or [])}." if execution.get("sound_cues") else "",
    ] if part).strip()


def _scale_scene_durations_to_target(scenes: list, target_sec: float) -> list:
    target = float(target_sec or 0.0)
    count = len(list(scenes or []))
    per_scene = 5.0
    if count > 0 and target > 0.0:
        per_scene = max(5.0, min(12.0, round(target / count, 2)))
    scaled = []
    for idx, raw in enumerate(scenes or []):
        scene = dict(raw or {})
        scene["scene_num"] = int(scene.get("scene_num", idx + 1) or (idx + 1))
        scene["duration_sec"] = per_scene
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
    purposes = " ".join(str((s or {}).get("engagement_purpose", "") or "") for s in scenes).lower()
    if not re.search(r"\b(hook|reveal|contrast|consequence|payoff|interrupt)\b", purposes):
        score -= 8
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
    format_preset: str = "explainer",
    edit_blueprint: dict | None = None,
    chapter_blueprint: dict | None = None,
) -> dict:
    documentary_mode = _longform_prefers_3d_documentary_visuals(template, format_preset)
    scene_goal = (
        max(4, min(10, int(round(float(chapter_target_sec) / 11.0))))
        if documentary_mode
        else max(6, min(24, int(round(float(chapter_target_sec) / 5.0))))
    )
    topic_focus = _longform_fallback_visual_focus(topic, input_title)
    edit_blueprint = dict(edit_blueprint or {})
    chapter_blueprint = dict(chapter_blueprint or {})
    motion_strategy = dict(edit_blueprint.get("motion_strategy") or {})
    sound_strategy = dict(edit_blueprint.get("sound_strategy") or {})
    blueprint_focus = _clip_text(str(chapter_blueprint.get("focus", "") or ""), 180)
    blueprint_hook = _clip_text(str(chapter_blueprint.get("hook_job", "") or ""), 160)
    blueprint_shock = _clip_text(str(chapter_blueprint.get("shock_device", "") or ""), 80)
    blueprint_visual_motif = _clip_text(str(chapter_blueprint.get("visual_motif", "") or ""), 180)
    blueprint_motion = _clip_text(str(chapter_blueprint.get("motion_note", "") or ""), 140)
    blueprint_sound = _clip_text(str(chapter_blueprint.get("sound_note", "") or ""), 140)
    blueprint_retention = _clip_text(str(chapter_blueprint.get("retention_goal", "") or ""), 140)
    blueprint_improvement = _clip_text(str(chapter_blueprint.get("improvement_focus", "") or ""), 140)
    documentary_archetype = _longform_documentary_archetype(
        edit_blueprint=edit_blueprint,
        chapter_blueprint=chapter_blueprint,
        format_preset=format_preset,
        topic=topic,
        input_title=input_title,
    )
    if str(format_preset or "").strip().lower() == "documentary":
        if documentary_archetype == "psychology_documentary":
            visual_modes = [
                "surveillance-grade dossier room where a hidden trigger lands on a real person making a real decision",
                "mirror or split-self consequence scene inside a designed room showing private perception versus hidden control",
                "social manipulation tableau with one figure subtly steering another in a premium office, hallway, or meeting space",
                "archive or interrogation-room proof scene exposing the behavior pattern behind the choice",
                "human-scale influence network around one relationship, conversation, or decision point",
                "controlled symbolic mind-world built into a real architectural environment using reflections, masks, strings, or attention funnels rather than literal anatomy",
            ]
            narration_modes = [
                "Open on the invasive consequence first so the audience instantly feels the hidden behavior.",
                "Show the trigger and the human reaction in the same beat.",
                "Translate the hidden influence into a personal consequence the viewer can picture immediately.",
                "Raise the stakes by showing how the behavior quietly reshapes a decision or relationship.",
                "Introduce a reversal between what the person believes and what is actually controlling the outcome.",
                "Land the beat on a consequence or proof image that makes the next reveal feel more personal.",
            ]
        elif documentary_archetype == "crime_documentary":
            visual_modes = [
                "premium case-board evidence frame with complaint pages, photos, phones, and one decisive contradiction in a designed room",
                "timeline and route board showing travel, studio arrival, and the evidence path in one readable composition",
                "recording-studio, hallway, parking-lot, or custody-pressure scene where one human stake and one proof cue collide",
                "documentary control-room frame with surveillance monitors, call logs, rental records, and redacted filing fragments",
                "court or consequence frame showing legal exposure, public contradiction, or home-confinement irony",
                "source-capture split frame contrasting public image against the case evidence trail",
            ]
            narration_modes = [
                "Open with the contradiction or allegation that becomes much worse once the evidence is visible.",
                "Show the proof trail and the human consequence in the same beat.",
                "Translate the legal or factual detail into a pressure scene the viewer can picture instantly.",
                "Raise the stakes by connecting one more record, route, phone, witness, or surveillance clue.",
                "Introduce a reversal between the public story and what the case file suggests.",
                "Land the beat on the clearest consequence or unresolved question so the next reveal feels necessary.",
            ]
        else:
            visual_modes = [
                "premium dossier-table proof scene with one dominant evidence thread in a real designed room",
                "boardroom or institutional power-map scene showing leverage and consequence around real people",
                "ownership or money-flow network built into a table, wall, or room tied directly to the subject",
                "archive, ledger, or infrastructure proof scene exposing the hidden system in a cinematic environment",
                "before-versus-after consequence scene with one sharply legible change in the same designed space",
                "human consequence scene showing who benefits, who loses, and why the system matters",
            ]
            narration_modes = [
                "Open with the clearest contradiction or proof first so the audience immediately understands why this beat matters.",
                "Show the hidden system and the visible consequence in the same beat.",
                "Translate the explanation into a downstream effect the viewer can picture instantly.",
                "Raise the stakes by showing how the system expands, routes power, or shifts incentives.",
                "Introduce a contrast or reversal that sharpens the point instead of repeating it.",
                "Land the beat with a concrete payoff that makes the next reveal feel bigger.",
            ]
    else:
        visual_modes = [
            "isolated hero-object stage composition with one dominant subject",
            "macro mechanism cutaway with one clearly visible internal process",
            "stylized human interaction shot with one person reacting to the core subject",
            "tabletop map or system-view composition with one zone clearly emphasized",
            "before-versus-after comparison frame with strong contrast between two states",
            "clean process-diagram environment with one visible cause and one visible effect",
        ]
        narration_modes = [
            "Open with the clearest reveal first so the audience immediately understands why this beat matters.",
            "Show the mechanism or hidden driver that pushes the idea forward.",
            "Translate the explanation into a consequence the viewer can feel or picture instantly.",
            "Raise the stakes by showing how the system expands, spreads, or takes control.",
            "Introduce a contrast, reversal, or clash that sharpens the point instead of repeating it.",
            "Land the beat with a concrete payoff that makes the next reveal feel bigger.",
        ]
    scenes: list[dict] = []
    for i in range(scene_goal):
        beat = i + 1
        visual_mode = visual_modes[i % len(visual_modes)]
        narration_mode = narration_modes[i % len(narration_modes)]
        scenes.append({
            "scene_num": beat,
            "duration_sec": 5.0,
            "narration": " ".join(part for part in [blueprint_hook or narration_mode, blueprint_focus or blueprint_improvement] if part).strip(),
            "visual_description": (
                f"Premium cinematic documentary scene focused on {topic_focus}. "
                f"Use {blueprint_visual_motif or visual_mode}, cinematic environmental storytelling, readable focal hierarchy, strong depth, and at least one grounded human, room, or proof context when relevant, "
                f"and one unmistakable visual change from the previous beat. Shock device: {blueprint_shock or 'clear escalation or contrast'}. "
                "Keep the named subject exact; do not substitute a different organ, device, or symbol. "
                "Make it feel unmistakably designed in 3D, not photographic. Avoid isolated floating objects, pedestal product shots, literal brains, sterile labs, and pseudo-text. No written words, no interface overlays, no chapter cards."
            ),
            "text_overlay": "",
            "motion_direction": blueprint_motion or str(motion_strategy.get("transition_style", "") or "controlled push-in and clean contrast cut"),
            "sfx_direction": blueprint_sound or str(sound_strategy.get("music_profile", "") or "premium documentary tension accent"),
            "engagement_purpose": blueprint_retention or "advance the explanation with a stronger hook, escalation, or contrast",
        })
    scenes = _repair_longform_generated_scenes(
        scenes,
        template=template,
        format_preset=format_preset,
        topic=topic,
        input_title=input_title,
        edit_blueprint=edit_blueprint,
        chapter_blueprint=chapter_blueprint,
        allow_narration_rewrite=True,
    )
    normalized = _normalize_longform_scenes_for_render(scenes)
    normalized = _scale_scene_durations_to_target(normalized, chapter_target_sec)
    normalized = _longform_enforce_tone_on_scenes(normalized, tone=tone, template=template, format_preset=format_preset)
    out = {
        "index": int(chapter_index),
        "title": f"Chapter {chapter_index + 1}",
        "summary": blueprint_focus or "Auto-built chapter draft ready for review.",
        "tone": str(tone or "neutral"),
        "target_sec": round(sum(float((scene or {}).get("duration_sec", 5.0) or 5.0) for scene in normalized), 2),
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
    edit_blueprint: dict | None = None,
    chapter_blueprint: dict | None = None,
) -> dict:
    if _xai_json_completion is None and _fal_openrouter_json_completion is None:
        raise RuntimeError("video_pipeline runtime hook 'xai_json_completion' or 'fal_openrouter_json_completion' must be configured")

    tone = _longform_detect_tone(template, topic, input_title, input_description)
    lang_name = SUPPORTED_LANGUAGES.get(language, {}).get("name", "English")
    documentary_mode = _longform_prefers_3d_documentary_visuals(template, format_preset)
    scene_goal = (
        max(4, min(10, int(round(float(chapter_target_sec) / 11.0))))
        if documentary_mode
        else max(6, min(24, int(round(float(chapter_target_sec) / 5.0))))
    )
    subject_lock = _longform_subject_lock(topic, input_title, input_description)
    edit_blueprint = dict(edit_blueprint or {})
    chapter_blueprint = dict(chapter_blueprint or {})
    system_prompt = (
        "You are writing one chapter of a long-form YouTube video package for NYPTID Studio. "
        "Output strict JSON with keys: chapter_title, chapter_summary, scenes, chapter_description. "
        f"Generate {scene_goal}-{scene_goal + 2} scenes. Each scene must include scene_num, duration_sec, narration, visual_description, text_overlay, motion_direction, sfx_direction, engagement_purpose. "
        "narration must be concise and engaging; visual_description must be render-ready and specific. "
        "motion_direction must describe the camera or motion-graphics beat; sfx_direction must describe the sound-design accent; engagement_purpose must explain why the scene keeps attention. "
        + ("Most scenes should land between 8 and 12 seconds so the chapter can breathe without exploding render count. Each scene narration MUST be 30-60 words (2-4 full sentences) to fill the duration. One-sentence narrations are too short. " if documentary_mode else "Every scene duration_sec must be exactly 5. ")
        + "Narration-first rule: each visual_description must directly visualize that same scene's narration beat. "
        "Optimize for retention, clean structure, and YouTube packaging strength instead of generic filler. "
        "CRITICAL RULE: narration must be ORIGINAL DOCUMENTARY SCRIPT written for a viewer, NOT planning notes or instructions. "
        "Never copy planning language, Catalyst instructions, blueprint text, improvement targets, or focus directives into narration or visual_description fields. "
        "Do not write meta phrases like 'open on', 'start directly on', 'subject focus', 'the trigger behind', 'the case around', 'the contradiction around', "
        "'current Catalyst archetype', 'next run should', 'keep the promise', 'deliver a clean reversal', 'ground the theory', or 'avoid' inside scene fields. "
        "Every narration line must sound like a real documentary narrator speaking to a YouTube audience — conversational, specific, fact-driven, and emotionally engaging. "
        "Each scene narration should be 2-4 sentences long (25-50 words) with concrete facts, names, studies, or examples — NOT abstract planning language."
    )
    system_prompt += " " + subject_lock
    if _longform_prefers_3d_documentary_visuals(template, format_preset):
        documentary_archetype = str(edit_blueprint.get("archetype_key", "") or "").strip().lower()
        system_prompt += (
            " Visual default for this format: premium stylized 3D documentary imagery. "
            "Every visual_description should bias toward designed 3D sets, readable human-scale proof framing, clean motion-design composition, "
            "polished CGI materials, bold focal hierarchy, and premium YouTube documentary energy. "
            "Do not default to gritty live-action stills, random empty warehouses, street-photo realism, or moody candid humans unless the narration beat truly requires that. "
            "Scene variation rule: rotate between proof frames, consequence scenes, archive or source setups, map or timeline views, human pressure scenes, before-versus-after contrasts, and consequence-driven environments. "
            "Do not repeat the same isolated object, sterile room, or floating machine in the same chapter."
        )
        if documentary_archetype == "psychology_documentary":
            system_prompt += (
                " Psychology-documentary lock: visualize hidden behavior through dossiers, surveillance, social manipulation tableaux, mirror/reversal frames, attention funnels, hidden triggers, observed choice moments, and human consequence scenes. "
                "Favor Fern-style dark systems framing over generic explainer widgets. Do not default to literal exposed brains, textbook anatomy, sterile labs, floating machines, sci-fi device showcases, or abstract gear sculptures unless the narration explicitly requires them."
            )
        elif documentary_archetype == "crime_documentary":
            system_prompt += (
                " Crime-documentary lock: visualize the case through complaint-file fragments, phone and record trails, route maps, surveillance stills, studio or custody pressure scenes, source-capture layouts, and court or consequence frames. "
                "Use the recurring skeleton only as an editorial host or connective witness when useful; keep named humans human whenever the beat is about evidence, pressure, or consequence. "
                "Do not default to generic boardrooms, untouched dossier tables, empty archive rooms, multi-skeleton meetings, x-ray humans, anatomy filler, or floating machine stages."
            )
        elif documentary_archetype == "systems_documentary":
            system_prompt += (
                " Systems-documentary lock: visualize control through dossiers, boardrooms, ledgers, networks, maps, infrastructure, and consequence frames. "
                "Favor premium cinematic systems staging and Magnates-style expensive consequence framing over generic floating-object stages, random lab props, anatomy filler, and glossy machine hero shots unless the narration explicitly requires them."
            )
    if format_preset == "recap" or str(edit_blueprint.get("niche_key", "") or "").strip().lower() == "manga_recap":
        recap_anchor = _clip_text(str(edit_blueprint.get("series_anchor", "") or topic or input_title or "the series"), 120)
        system_prompt += (
            f" Recap execution lock: this is a premium manga/manhwa recap follow-up inside {recap_anchor}, not a generic documentary explainer. "
            "Keep scenes inside the same fictional universe, chapter-turn logic, power system, betrayal stakes, and rivalry hierarchy. "
            "Prefer protagonist/rival/system/monster/weapon environments, kinetic panel-like composition, and addiction-grade reveal pacing. "
            "Do not drift into generic business, lab, medical, or empty-object documentary visuals unless narration literally requires it."
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
    blueprint_prompt_context = _render_longform_blueprint_prompt_context(edit_blueprint, chapter_blueprint)
    if blueprint_prompt_context:
        user_prompt += blueprint_prompt_context + "\n"
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

    # Try FAL OpenRouter (Claude Sonnet 4.6) first, fall back to Grok
    chapter_data = None
    if _fal_openrouter_json_completion is not None:
        try:
            chapter_data = await _fal_openrouter_json_completion(system_prompt, user_prompt, temperature=0.65, timeout_sec=120)
            log.info("Longform chapter gen succeeded via FAL OpenRouter (Claude Sonnet 4.6)")
        except Exception as fal_exc:
            log.warning("FAL OpenRouter chapter gen failed, falling back to Grok: %s", str(fal_exc)[:200])
    if chapter_data is None:
        chapter_data = await _xai_json_completion(system_prompt, user_prompt, temperature=0.65, timeout_sec=90)
    raw_scenes = chapter_data.get("scenes", [])
    scenes = _normalize_longform_scenes_for_render(raw_scenes)
    scenes = _repair_longform_generated_scenes(
        scenes,
        template=template,
        format_preset=format_preset,
        topic=topic,
        input_title=input_title,
        edit_blueprint=edit_blueprint,
        chapter_blueprint=chapter_blueprint,
        allow_narration_rewrite=True,
    )
    scenes = _scale_scene_durations_to_target(scenes, chapter_target_sec)
    scenes = _longform_enforce_tone_on_scenes(
        scenes,
        tone=tone,
        template=template,
        format_preset=format_preset,
    )
    chapter_total_sec = round(float(len(scenes) * 5.0), 2)
    out = {
        "index": int(chapter_index),
        "title": str(chapter_data.get("chapter_title", f"Chapter {chapter_index + 1}") or f"Chapter {chapter_index + 1}"),
        "summary": str(chapter_data.get("chapter_summary", "") or ""),
        "tone": str(tone),
        "format_preset": format_preset,
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

def _story_scene_prefers_explainer_visuals(text: str) -> bool:
    source = str(text or "").strip().lower()
    if not source:
        return False
    if "3d documentary style lock:" in source or "3d psychology documentary style lock:" in source:
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

async def generate_script(template: str, topic: str, extra_instructions: str = "") -> dict:
    system_prompt = TEMPLATE_SYSTEM_PROMPTS.get(template, TEMPLATE_SYSTEM_PROMPTS["random"])
    topic_text = str(topic or "").strip()
    script_to_short_mode = "SCRIPT-TO-SHORT MODE" in str(extra_instructions or "")
    comparison_topic = bool(re.search(r"\b(vs\.?|versus)\b", topic_text, re.IGNORECASE))
    if template in {"skeleton", "story", "motivation", "daytrading"}:
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

    def _topic_title_fallback() -> str:
        title_words = re.findall(r"[A-Za-z0-9']+", topic_text)[:8]
        if title_words:
            return " ".join(title_words).strip()
        template_titles = {
            "story": "AI Story Short",
            "motivation": "Motivation Short",
            "skeleton": "Skeleton AI Short",
            "daytrading": "Day Trading Short",
        }
        return template_titles.get(template, "Catalyst Short")

    def _topic_fallback_beats() -> list[str]:
        compact_topic = re.sub(r"\s+", " ", topic_text).strip(" .")
        if not compact_topic:
            compact_topic = _topic_title_fallback()
        if template == "daytrading":
            return [
                f"Open with the trading pain point behind {compact_topic}.",
                f"Show the hidden mistake beginners make around {compact_topic}.",
                f"Explain the market condition or setup that matters most for {compact_topic}.",
                f"Reveal the emotional trap that ruins execution during {compact_topic}.",
                f"Show the smarter risk rule that protects traders during {compact_topic}.",
                f"Break down the cleaner entry or confirmation for {compact_topic}.",
                f"Show how patience changes the result with {compact_topic}.",
                f"End with the hard lesson or rule traders should remember from {compact_topic}.",
            ]
        if template == "motivation":
            return [
                f"Open with the painful truth behind {compact_topic}.",
                f"Show the excuse or fear that keeps people stuck around {compact_topic}.",
                f"Explain the cost of doing nothing about {compact_topic}.",
                f"Show the mindset shift that changes the direction of {compact_topic}.",
                f"Reveal the disciplined action that turns {compact_topic} into progress.",
                f"Show how consistency compounds when someone commits to {compact_topic}.",
                f"Escalate the stakes by showing who wins and who loses around {compact_topic}.",
                f"End with a decisive challenge connected to {compact_topic}.",
            ]
        if template == "skeleton":
            return [
                f"Open with the core contrast or reveal behind {compact_topic}.",
                f"Show the first side of {compact_topic} with clear props and setting.",
                f"Show the opposite side of {compact_topic} and make the contrast obvious.",
                f"Highlight the hidden cost, struggle, or tradeoff inside {compact_topic}.",
                f"Raise the stakes with money, time, status, or lifestyle consequences in {compact_topic}.",
                f"Show the turning point that makes the comparison behind {compact_topic} undeniable.",
                f"Present the most visual proof point for {compact_topic}.",
                f"End with the sharpest payoff or verdict for {compact_topic}.",
            ]
        return [
            f"Open with the strongest hook inside {compact_topic}.",
            f"Introduce the main subject and immediate problem behind {compact_topic}.",
            f"Show the first escalation or discovery connected to {compact_topic}.",
            f"Reveal the hidden truth, risk, or twist behind {compact_topic}.",
            f"Raise the emotional or practical stakes around {compact_topic}.",
            f"Show the turning point that changes the direction of {compact_topic}.",
            f"Push toward the clearest payoff or consequence of {compact_topic}.",
            f"End with the strongest final line for {compact_topic}.",
        ]

    def _fallback_visual_description_for_template(scene_text: str) -> str:
        beat = re.sub(r"\s+", " ", str(scene_text or "").strip().rstrip("."))
        if template == "daytrading":
            return (
                f"Photoreal premium trading scene illustrating: {beat}. "
                "Realistic trading desk, market screens, believable charts, human hands or trader presence when useful, grounded finance lighting, and vertical short-video framing."
            )
        if template == "skeleton":
            return (
                f"Canonical cinematic skeleton scene illustrating: {beat}. "
                "Same ivory-white skeleton with realistic eyes and translucent shell, topic-matched props, grounded environment, and readable vertical framing."
            )
        return _fallback_visual_description(scene_text)

    def _build_topic_fallback() -> dict:
        beats = _topic_fallback_beats()
        scenes: list[dict] = []
        for idx, beat in enumerate(beats, start=1):
            narration = beat.strip()
            scenes.append(
                {
                    "scene_num": idx,
                    "duration_sec": 5.0,
                    "narration": narration,
                    "visual_description": _fallback_visual_description_for_template(narration),
                    "text_overlay": _fallback_scene_overlay(narration),
                }
            )
        title = _topic_title_fallback()
        tags = [template, "shorts", "nyptid"]
        if template == "daytrading":
            tags.extend(["trading", "investing", "finance"])
        elif template == "motivation":
            tags.extend(["motivation", "mindset", "discipline"])
        elif template == "skeleton":
            tags.extend(["comparison", "viral", "3d"])
        elif template == "story":
            tags.extend(["story", "cinematic", "viral"])
        return {
            "title": title,
            "scenes": scenes,
            "description": f"{title} generated by Catalyst.",
            "tags": tags[:8],
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
        # --- Try FAL OpenRouter (Claude Sonnet 4.6) first ---
        if _fal_openrouter_json_completion is not None:
            try:
                result = await _fal_openrouter_json_completion(prompt_text, user_prompt, temperature=temp, timeout_sec=90)
                log.info("Script generation succeeded via FAL OpenRouter (Claude Sonnet 4.6)")
                return result
            except Exception as fal_exc:
                log.warning("FAL OpenRouter script gen failed, falling back to Grok: %s", str(fal_exc)[:200])
        # --- Fallback to Grok (xAI) ---
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
        if status_code in {429, 500, 502, 503, 504}:
            if script_to_short_mode:
                log.warning(
                    f"Script-to-short upstream unavailable ({status_code}); using local scene-plan fallback for template={template}"
                )
                return _build_script_to_short_fallback()
            log.warning(
                f"Short-form script upstream unavailable ({status_code}); using local topic fallback for template={template}"
            )
            return _build_topic_fallback()
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
    try:
        second = await _call_script_gen(hardened_prompt, temp=0.65)
        second_score, _ = _score_story_script_quality(second)
        best = second if second_score >= first_score else first
        best_score = max(first_score, second_score)
    except Exception as retry_exc:
        log.warning(f"Story script hardening retry failed; keeping first draft: {retry_exc}")
        best = first
        best_score = first_score
    if story_script_to_short_mode and best_score < 80:
        ultra_hardened = (
            hardened_prompt
            + "\n\nFAILSAFE OVERRIDE (MUST FOLLOW): "
            + "Do NOT truncate. Complete all script beats in order. "
            + "Output exactly 12-15 scenes with no premature ending, no skipped late-script events, and no generic replacement prompts."
        )
        try:
            third = await _call_script_gen(ultra_hardened, temp=0.55)
            third_score, _ = _score_story_script_quality(third)
            if third_score >= best_score:
                best = third
                best_score = third_score
        except Exception as retry_exc:
            log.warning(f"Story script ultra-hardening retry failed; keeping best available draft: {retry_exc}")
    return best

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

def _split_text_into_fallback_beats(source_text: str, min_count: int, max_count: int) -> list[str]:
    text = re.sub(r"\s+", " ", str(source_text or "").strip())
    if not text:
        return []
    base_parts = [part.strip(" ,;:-") for part in re.split(r"(?<=[.!?])\s+", text) if part.strip(" ,;:-")]
    beats = list(base_parts or [text])

    def _split_fragment_once(fragment: str) -> tuple[str, str] | None:
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
            return None
        left = source[:split_at].strip(" ,;:-")
        right = source[split_at:].strip(" ,;:-")
        if len(left) < 24 or len(right) < 24:
            return None
        return (left, right)

    while len(beats) < min_count:
        longest_index = max(range(len(beats)), key=lambda idx: len(beats[idx]))
        split_pair = _split_fragment_once(beats[longest_index])
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

def _build_skeleton_shorts_local_fallback(
    source_text: str,
    channel_context: dict | None = None,
    trend_titles: list[str] | None = None,
    public_shorts_playbook: dict | None = None,
    script_to_short_mode: bool = False,
    trend_hunt_enabled: bool = False,
) -> dict:
    channel_context = dict(channel_context or {})
    trend_titles = [str(v).strip() for v in list(trend_titles or []) if str(v).strip()]
    public_shorts_playbook = dict(public_shorts_playbook or {})
    source = re.sub(r"\s+", " ", str(source_text or "").strip())
    if not source:
        source = "viral skeleton comparison"
    angle_candidates = [dict(v or {}) for v in list(public_shorts_playbook.get("angle_candidates") or []) if isinstance(v, dict)]
    comparison_match = re.split(r"\bvs\.?\b|\bversus\b", source, maxsplit=1, flags=re.IGNORECASE)
    comparison_mode = len(comparison_match) == 2
    compact_topic = _clip_text(source, 180)
    if angle_candidates and (trend_hunt_enabled or not str(source_text or "").strip()):
        best_angle = str((angle_candidates[0] or {}).get("angle", "") or "").strip()
        if best_angle:
            compact_topic = _clip_text(best_angle, 180)
    trend_hint = trend_titles[0] if trend_titles else ""
    public_hook_hint = str((list(public_shorts_playbook.get("hook_moves") or [""])[:1] or [""])[0] or "").strip()
    public_visual_hint = str((list(public_shorts_playbook.get("visual_moves") or [""])[:1] or [""])[0] or "").strip()
    keyword_moves = [str(v).strip() for v in list(public_shorts_playbook.get("keyword_moves") or []) if str(v).strip()]

    if script_to_short_mode:
        beats = _split_text_into_fallback_beats(source, min_count=10, max_count=14)
    elif comparison_mode:
        left = re.sub(r"\s+", " ", comparison_match[0]).strip(" -")
        right = re.sub(r"\s+", " ", comparison_match[1]).strip(" -")
        beats = [
            f"Open with the most shocking truth about {left} versus {right}.",
            f"Show {left} first with the clearest visual advantage or status cue.",
            f"Show {right} with a stronger counterpoint so the contrast is obvious instantly.",
            f"Reveal the hidden tradeoff most people miss between {left} and {right}.",
            f"Raise the stakes with money, time, status, pain, or effort consequences.",
            f"Use the clearest visual proof point that makes the comparison undeniable.",
            f"Show the side that looks stronger at first but hides a real weakness.",
            f"Show the side that looks weaker at first but wins on the metric that matters.",
            f"Escalate to the most dramatic consequence of choosing the wrong side.",
            f"End with the sharpest verdict on {left} versus {right}.",
        ]
    else:
        beats = [
            f"Open with the strongest hidden truth inside {compact_topic}.",
            f"Show the first visual proof point behind {compact_topic}.",
            f"Reveal the cost, pressure, or sacrifice most people ignore in {compact_topic}.",
            f"Raise the stakes with money, time, status, or survival consequences around {compact_topic}.",
            f"Show the hidden system or mechanism driving {compact_topic}.",
            f"Contrast what people think about {compact_topic} with what actually happens.",
            f"Show the most dramatic payoff or damage caused by {compact_topic}.",
            f"End with the clearest verdict that makes {compact_topic} unforgettable.",
        ]
    if trend_hunt_enabled and trend_hint:
        beats[0] = f"Open with a fresher breakout angle on {compact_topic}, not a recycled take. Trend cue: {trend_hint}."
        beats[min(2, len(beats) - 1)] = f"Introduce a surprising, shareable angle that could become a new comparison trend around {compact_topic}."
    if public_hook_hint:
        beats[0] = _clip_text((beats[0].rstrip(". ") + f". Public benchmark move: {public_hook_hint}"), 220)
    if public_visual_hint and len(beats) > 1:
        beats[1] = _clip_text((beats[1].rstrip(". ") + f". Visual benchmark: {public_visual_hint}"), 220)

    scenes: list[dict] = []
    base_duration = 50.0
    scene_duration = max(4.0, min(5.5, round(base_duration / max(len(beats), 1), 2)))
    for idx, beat in enumerate(beats[:14], start=1):
        narration = beat.strip()
        visual_description = (
            f"Canonical cinematic skeleton scene for this beat: {narration} "
            "Same ivory-white anatomical skeleton with realistic eyes and translucent body shell, "
            "hyper-real 3D materials, topic-matched props, premium vertical framing, and instantly readable contrast."
        )
        if public_visual_hint:
            visual_description = visual_description.rstrip(". ") + f". Benchmark visual cue: {public_visual_hint}"
        scenes.append(
            {
                "scene_num": idx,
                "duration_sec": scene_duration,
                "narration": narration,
                "visual_description": visual_description,
            }
        )

    if comparison_mode:
        left = re.sub(r"\s+", " ", comparison_match[0]).strip(" -")
        right = re.sub(r"\s+", " ", comparison_match[1]).strip(" -")
        title = _clip_text(f"{left} vs {right}: The Truth Nobody Tells You", 80)
        tags = [left.lower(), right.lower(), "comparison", "skeleton ai", "viral shorts", "3d"]
    else:
        title = _clip_text(compact_topic if len(compact_topic) <= 70 else f"The Hidden Truth About {compact_topic[:48].rstrip()}", 80)
        tags = [compact_topic.lower(), "skeleton ai", "viral shorts", "3d comparison", "shorts"]
    if keyword_moves and not comparison_mode:
        title = _clip_text(f"{keyword_moves[0].title()} Truth: {compact_topic}", 80)
        tags = [*tags, *keyword_moves[:3]]
    if trend_hunt_enabled:
        tags.append("trend hunt")

    return {
        "title": title,
        "scenes": scenes,
        "description": f"{title} generated by Catalyst Skeleton fallback.",
        "tags": _dedupe_preserve_order([t for t in tags if t], max_items=8, max_chars=40),
    }
