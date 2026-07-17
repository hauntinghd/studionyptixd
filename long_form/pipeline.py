"""
Long-form Studio render pipeline.

Background-job pattern: kick a job, run async, poll status. Per-channel
sub-pipelines registered in SUB_PIPELINES dict. PR #120 ships the
sleep_doc sub-pipeline (HR — Ken-Burns slideshow + fal MiniMax narration +
mmaudio ambient bed); PR #122 will register v5_episode (EM — LTX i2v +
Whisper callouts + silence-kill + 2-pass loudnorm).

Storage: ``/var/data/long_form/<job_id>/`` on Fly (or ``long_form/output/<job_id>/``
fallback locally — auto-detected). Layout:

    state.json                       — job manifest (channel_key, outline,
                                       phase progress, timestamps)
    chapters.json                    — Grok-expanded chapters with full
                                       narration + scene_prompts
    stills/scene_NNNN.png            — ernie-image scenes
    audio/chapter_NN.mp3             — per-chapter MiniMax narration
    audio/narration.mp3              — concat'd full narration
    audio/ambient.mp3                — mmaudio loop (30s max, stream-looped at compose)
    audio/mix.mp3                    — narration + ambient mixdown
    thumbnails/thumb_N.png           — seedream candidate thumbnails
    LongForm_<job_id>.mp4            — final 1080p60 output

Cost budget for HR 9-hour sleep doc (per channel registry $73 estimate):
    - 18 chapters × any-llm Sonnet 4.5 ≈ $9.00
    - 540 ernie-image scenes × $0.03 ≈ $16.20
    - 65k-word fal MiniMax speech-02-hd ≈ $39.00 (390k chars × $0.10/1k)
    - 3 seedream v4.5 thumbnails × $0.04 ≈ $0.12
    - 1 mmaudio-v2 30s ambient bed ≈ $0.03
    - subtotal ≈ $64.40, with retry overhead → ~$73 envelope

Casey's HR feedback rule (feedback_hr_premium_fal_tts.md): TTS MUST be fal
MiniMax — NOT Edge — Egypt 9H Edge-TTS shipped and Casey called it bad.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import re
import shutil
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Awaitable, Callable

import httpx

from studio_agent.image_model_catalog import (
    is_seedream_model,
    normalize_seedream_model_id,
    seedream_endpoint,
    seedream_model_spec,
    seedream_provider,
)


# ─────────────────────────────────────────────────────────────────────────────
# Globals — paths, fal endpoints, status registry, task strong-refs
# ─────────────────────────────────────────────────────────────────────────────

# Fly persistent volume detection — must be Linux + the mount must exist.
# (On Windows local dev, Path("/var/data").exists() resolves to D:\var\data
# which can pass true after a single run because mkdir creates it. Restrict
# to posix to keep local dev under long_form/output/.)
def _resolve_lf_output_root() -> Path:
    override = (os.environ.get("LF_OUTPUT_ROOT") or "").strip()
    if override:
        return Path(override)
    if os.name == "posix":
        var_data = Path("/var/data")
        if var_data.is_dir():
            return var_data / "long_form"
    return Path("long_form/output")


LF_OUTPUT_ROOT = _resolve_lf_output_root()
LF_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# fal endpoints used by this pipeline.
SEEDREAM_URL = "https://fal.run/fal-ai/bytedance/seedream/v4.5/text-to-image"
SEEDREAM_EDIT_URL = "https://fal.run/fal-ai/bytedance/seedream/v4.5/edit"
ERNIE_URL = "https://fal.run/fal-ai/ernie-image"
MINIMAX_TTS_URL = "https://fal.run/fal-ai/minimax/speech-02-hd"
# mmaudio-v2 has TWO variants; we want the text-to-audio one for ambient
# beds + per-scene SFX synthesis from prompt + duration. The plain
# /mmaudio-v2 endpoint is video-to-audio (requires a video_url) and was
# 422'ing every call from PR #120/#127 with "video_url: Field required".
MMAUDIO_URL = "https://fal.run/fal-ai/mmaudio-v2/text-to-audio"

# Per-job in-memory progress snapshot. Survives across HTTP requests but not
# process restarts — that's fine because state.json on disk is the source of
# truth; this is a lightweight cache for /jobs/{id}/status polling.
_lf_jobs_status: dict[str, dict[str, Any]] = {}

# Strong-ref retention for asyncio.create_task background tasks. Without this
# the GC sometimes cancels them mid-render (we hit this exact bug on ZT short
# Phase 4.5b — see lesson #2 in the 2026-05-08 handoff).
_lf_running_tasks: set[asyncio.Task] = set()


def _spawn_lf_background_coro(coro, job_id: str) -> asyncio.Task | None:
    """Run inline on RunPod; otherwise schedule on the active API loop."""
    worker_mode = str(os.getenv("STUDIO_RUNPOD_WORKER_MODE") or "").strip().lower()
    if worker_mode in {"1", "true", "yes", "on"}:
        # A RunPod Serverless handler is synchronous and must own the complete
        # production stage before returning.  Running inline also avoids
        # creating an orphaned task that dies when the job process is recycled.
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(coro)
        else:
            # The current RunPod SDK invokes sync handlers from its own asyncio
            # loop. asyncio.run cannot nest there, so use a joined thread with
            # its own loop while keeping the handler synchronous to completion.
            errors: list[BaseException] = []

            def _run() -> None:
                try:
                    asyncio.run(coro)
                except BaseException as exc:  # propagate stage failure verbatim
                    errors.append(exc)

            thread = threading.Thread(
                target=_run,
                name=f"studio-runpod-longform-{job_id}",
            )
            thread.start()
            thread.join()
            if errors:
                raise errors[0]
        return None
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError as exc:
        raise LFRenderError("no running event loop") from exc
    task = loop.create_task(coro)
    _lf_running_tasks.add(task)
    task.add_done_callback(_lf_running_tasks.discard)
    _lf_jobs_status.setdefault(job_id, {})["_task"] = task
    return task


class LFRenderError(RuntimeError):
    """Long-form render failure — caught by _run_render to mark the job failed."""


class LFMediaRouteChanged(LFRenderError):
    """A provider result became stale before it could be committed safely."""

    def __init__(self, message: str, *, receipts: list[dict[str, Any]] | None = None):
        super().__init__(message)
        self.receipts = list(receipts or [])


# ─────────────────────────────────────────────────────────────────────────────
# fal.ai pricing — live via Platform API (long_form.fal_pricing), with static
# fallbacks when FAL_KEY is missing or API is unreachable.
# ─────────────────────────────────────────────────────────────────────────────

from long_form.fal_pricing import (  # noqa: E402
    FALLBACK_USD as _FAL_FALLBACK,
    get_pricing_snapshot,
    i2v_cost_per_clip as _i2v_cost_per_clip,
    unit_cost as _fal_unit_cost,
)

# Back-compat alias for scripts that import FAL_PRICING_USD directly.
FAL_PRICING_USD: dict[str, float] = dict(_FAL_FALLBACK)
FAL_PRICING_USD.update({
    "seedream_v45_per_image": _FAL_FALLBACK["seedream_v45_per_image"],
    "ltx_13b_distilled_per_second": _FAL_FALLBACK["ltx_13b_distilled_per_second"],
    "ltx_13b_per_second": _FAL_FALLBACK["ltx_13b_distilled_per_second"],
    "mmaudio_v2_per_call": _FAL_FALLBACK["mmaudio_v2_per_second"] * 8,
    "grok_chapter_expand": 0.0,
    "grok_outline": 0.0,
})

MMAUDIO_DEFAULT_SEC = float(os.environ.get("LF_MMAUDIO_SEC", "8"))


def resolve_motion_ratio(outline: dict | None) -> tuple[str, float]:
    """Resolve the durable long-form motion policy used by cost and render."""
    data = outline or {}
    policy = str(data.get("motion_policy") or "balanced").strip().lower()
    defaults = {"full": 1.0, "balanced": 0.35, "economy": 0.15, "stills": 0.0}
    if policy not in defaults:
        policy = "balanced"
    raw_ratio = data.get("hero_motion_ratio")
    try:
        ratio = float(raw_ratio) if raw_ratio is not None else defaults[policy]
    except (TypeError, ValueError):
        ratio = defaults[policy]
    return policy, max(0.0, min(1.0, ratio))


def compute_render_cost(
    channel: dict,
    outline: dict | None = None,
    *,
    scenes_per_chapter_override: int | None = None,
    force_refresh_pricing: bool = False,
) -> dict:
    """Estimate render cost using live fal Platform API pricing when available.

    Returns a dict shaped for the frontend kickoff/gate display:
        {
            "stage_1_usd": float,        # stills + thumbnails (fal wallet)
            "stage_2_usd": float,        # i2v + fal VO + SFX
            "total_usd": float,          # fal wallet only, incl. cushion
            "breakdown": {step: usd},
            "non_fal_breakdown": {...},   # ElevenLabs / xAI script (not fal)
            "pricing_source": str,
            "pricing_fetched_at": float,
            ...
        }

    Falls back to channel.cost_estimate_usd if pipeline_kind is unknown.
    """
    pipeline_kind = (channel.get("pipeline_kind") or "").strip()
    chapters_list = list((outline or {}).get("chapters") or [])
    n_chapters = max(1, len(chapters_list))

    pricing = get_pricing_snapshot(force_refresh=force_refresh_pricing)
    cushion_pct = float(FAL_PRICING_USD.get("cushion_pct", 0.15))

    def with_cushion(x: float) -> float:
        return round(x * (1 + cushion_pct), 2)

    def _cost_meta(extra: dict) -> dict:
        return {
            **extra,
            "pricing_source": pricing.get("source"),
            "pricing_fetched_at": pricing.get("fetched_at"),
            "pricing_error": pricing.get("error"),
        }

    # ── v5_episode (EM, Lacuna, PB Live, Hidden Cortex) ──────────────────
    if pipeline_kind == "v5_episode":
        scenes_per_chapter = scenes_per_chapter_override or 12
        n_scenes = n_chapters * scenes_per_chapter
        motion_policy, motion_ratio = resolve_motion_ratio(outline)
        animated_scenes = min(n_scenes, max(0, round(n_scenes * motion_ratio)))
        total_vo_chars = n_scenes * 200
        vo_k = total_vo_chars / 1000.0

        clip_sec = float(os.environ.get("EM_LTX_CLIP_SEC", "12"))
        # v5_pipeline.py always renders LTX 13B distilled today.
        i2v_model = "ltx_13b"
        per_clip, i2v_note = _i2v_cost_per_clip(
            pricing, i2v_model=i2v_model, clip_sec=clip_sec
        )

        still_per, _ = _fal_unit_cost(
            pricing, "seedream_v45", fallback_key="seedream_v45_per_image", quantity=1.0
        )
        sfx_per, _ = _fal_unit_cost(
            pricing,
            "mmaudio_v2",
            fallback_key="mmaudio_v2_per_second",
            quantity=MMAUDIO_DEFAULT_SEC,
        )

        voice_provider = (channel.get("voice_provider_default") or "fal_minimax").strip()
        non_fal: dict[str, float] = {}
        fal_vo = 0.0
        if voice_provider == "fal_minimax":
            fal_vo, _ = _fal_unit_cost(
                pricing,
                "minimax_speech",
                fallback_key="fal_minimax_per_1k_chars",
                quantity=vo_k,
            )
        elif voice_provider == "elevenlabs":
            non_fal["elevenlabs_vo"] = round(
                vo_k * FAL_PRICING_USD["elevenlabs_per_1k_chars"], 2
            )

        is_em = (channel.get("key") or "") == "empire_magnates"
        thumb_count = 0 if is_em else 3

        breakdown = {
            "stills_seedream": round(n_scenes * still_per, 2),
            "thumbnails_seedream": round(thumb_count * still_per, 2),
            "ltx_i2v_clips": round(animated_scenes * per_clip, 2),
            "mmaudio_sfx_per_scene": round(n_scenes * sfx_per, 2),
            "fal_minimax_vo": round(fal_vo, 2),
        }
        stage_1 = breakdown["stills_seedream"] + breakdown["thumbnails_seedream"]
        stage_2 = (
            breakdown["ltx_i2v_clips"]
            + breakdown["mmaudio_sfx_per_scene"]
            + breakdown["fal_minimax_vo"]
        )
        fal_sub = stage_1 + stage_2
        return _cost_meta({
            "stage_1_usd": with_cushion(stage_1),
            "stage_2_usd": with_cushion(stage_2),
            "total_usd": with_cushion(fal_sub),
            "fal_subtotal_usd": round(fal_sub, 2),
            "non_fal_usd": round(sum(non_fal.values()), 2),
            "all_in_usd": round(fal_sub + sum(non_fal.values()), 2),
            "breakdown": breakdown,
            "non_fal_breakdown": non_fal,
            "n_scenes": n_scenes,
            "animated_scenes": animated_scenes,
            "still_motion_scenes": n_scenes - animated_scenes,
            "motion_policy": motion_policy,
            "hero_motion_ratio": motion_ratio,
            "n_chapters": n_chapters,
            "pipeline_kind": pipeline_kind,
            "i2v_model_billed": i2v_model,
            "i2v_billing_note": i2v_note,
            "mmaudio_sec_per_scene": MMAUDIO_DEFAULT_SEC,
        })

    # ── sleep_doc (HR 9hr) ────────────────────────────────────────────────
    if pipeline_kind == "sleep_doc":
        # Match the render path's precedence (outline > channel > default) so
        # the kickoff estimate reflects what will actually be billed.
        scenes_per_chapter = int(
            scenes_per_chapter_override
            or (outline or {}).get("scenes_per_chapter")
            or channel.get("scenes_per_chapter")
            or 30
        )
        n_scenes = n_chapters * scenes_per_chapter
        target_sec = float((outline or {}).get("target_duration_sec", 0) or 0)
        if target_sec <= 0:
            target_sec = float(channel.get("default_minutes", 540) or 540) * 60
        total_words = (target_sec / 60.0) * 120
        total_vo_chars = total_words * 5
        vo_k = total_vo_chars / 1000.0

        image_model = str(channel.get("image_model_default") or "ernie_image").strip().lower()
        if image_model in {"grok_imagine", "grok_imagine_quality", "grok-imagine-image-quality"}:
            still_per = 0.05
            still_key = "stills_grok_imagine_quality"
        elif image_model in {"grok_imagine_standard", "grok-imagine-image"}:
            still_per = 0.02
            still_key = "stills_grok_imagine"
        elif is_seedream_model(image_model):
            normalized_seedream = normalize_seedream_model_id(image_model)
            seedream_stem = {
                "seedream_v4": "seedream_v4",
                "seedream_v5_lite": "seedream_v5_lite",
            }.get(normalized_seedream, "seedream_v45")
            still_per, _ = _fal_unit_cost(
                pricing,
                seedream_stem,
                fallback_key=f"{seedream_stem}_per_image",
                quantity=1.0,
            )
            still_key = "stills_seedream"
        else:
            # ERNIE is billed per megapixel; the sleep-doc renderer requests 1920x1080.
            still_per = FAL_PRICING_USD["ernie_per_image"] * (1920 * 1080 / 1_000_000)
            still_key = "stills_ernie"
        seed_per, _ = _fal_unit_cost(
            pricing, "seedream_v45", fallback_key="seedream_v45_per_image", quantity=1.0
        )
        sfx_ambient, _ = _fal_unit_cost(
            pricing,
            "mmaudio_v2",
            fallback_key="mmaudio_v2_per_second",
            quantity=30.0,
        )
        voice_provider = str(channel.get("voice_provider_default") or "xai").strip().lower()
        if voice_provider == "xai":
            narration_key = "xai_narration"
            narration_cost = round(vo_k * 0.015, 2)  # official xAI: $15 / 1M input chars
        else:
            narration_key = "fal_minimax_narration"
            narration_cost, _ = _fal_unit_cost(
                pricing,
                "minimax_speech",
                fallback_key="fal_minimax_per_1k_chars",
                quantity=vo_k,
            )

        breakdown = {
            still_key: round(n_scenes * still_per, 2),
            "thumbnails_seedream": round(3 * seed_per, 2),
            narration_key: round(narration_cost, 2),
            "mmaudio_ambient_bed": round(sfx_ambient, 2),
        }
        stage_1 = breakdown[still_key] + breakdown["thumbnails_seedream"]
        stage_2 = breakdown[narration_key] + breakdown["mmaudio_ambient_bed"]
        fal_sub = stage_1 + breakdown["mmaudio_ambient_bed"]
        non_fal = {narration_key: breakdown[narration_key]} if voice_provider == "xai" else {}
        if voice_provider != "xai":
            fal_sub += breakdown[narration_key]
        return _cost_meta({
            "stage_1_usd": with_cushion(stage_1),
            "stage_2_usd": with_cushion(stage_2),
            "total_usd": with_cushion(fal_sub),
            "fal_subtotal_usd": round(fal_sub, 2),
            "non_fal_usd": round(sum(non_fal.values()), 2),
            "all_in_usd": round(fal_sub + sum(non_fal.values()), 2),
            "breakdown": breakdown,
            "non_fal_breakdown": non_fal,
            "n_scenes": n_scenes,
            "n_chapters": n_chapters,
            "pipeline_kind": pipeline_kind,
            "image_model": image_model,
            "voice_provider": voice_provider,
            "still_usd_per_image": round(still_per, 5),
            "paid_i2v_usd": 0.0,
            "motion_policy": "stills",
        })

    # Unknown pipeline_kind — preserve channel registry fallback.
    fallback = float(channel.get("cost_estimate_usd", 0) or 0)
    return {
        "stage_1_usd": round(fallback * 0.7, 2),
        "stage_2_usd": round(fallback * 0.3, 2),
        "total_usd": round(fallback, 2),
        "breakdown": {"channel_registry_fallback": fallback},
        "n_scenes": 0,
        "n_chapters": n_chapters,
        "pipeline_kind": pipeline_kind or "unknown",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Job ID + path helpers
# ─────────────────────────────────────────────────────────────────────────────

def _new_job_id() -> str:
    """12-hex job id — same shape as ZT private (also acts as media-fetch
    capability token; gating by job_id replaces Authorization headers for
    <img>/<video> tags)."""
    return uuid.uuid4().hex[:12]


def _job_dir(job_id: str) -> Path:
    return LF_OUTPUT_ROOT / job_id


def _ensure_job_dir(job_id: str) -> Path:
    d = _job_dir(job_id)
    (d / "stills").mkdir(parents=True, exist_ok=True)
    (d / "audio").mkdir(parents=True, exist_ok=True)
    (d / "thumbnails").mkdir(parents=True, exist_ok=True)
    return d


def _slugify(s: str, max_len: int = 24) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", str(s or "").lower()).strip("_")
    return (s[:max_len].rstrip("_")) or "longform"


def _state_path(job_id: str) -> Path:
    return _job_dir(job_id) / "state.json"


def _chapters_path(job_id: str) -> Path:
    return _job_dir(job_id) / "chapters.json"


def _final_mp4_path(job_id: str, title_slug: str) -> Path:
    return _job_dir(job_id) / f"LongForm_{title_slug}_{job_id}.mp4"


def save_state(job_id: str, state: dict[str, Any]) -> None:
    """Persist state.json atomically (tmp + rename)."""
    p = _state_path(job_id)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, ensure_ascii=True), encoding="utf-8")
    tmp.replace(p)


def load_state(job_id: str) -> dict[str, Any] | None:
    p = _state_path(job_id)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _longform_media_route_snapshot(
    job_id: str,
    *,
    fallback_image_model: str = "",
    fallback_video_model: str = "",
) -> dict[str, Any]:
    """Resolve the binding picker route immediately before a media dispatch.

    Studio Agent persists ``_session_id`` plus the initial route token in the
    outline.  When a session is available we re-read it, so a picker change is
    visible to an already-running long-form job.  Router-only jobs retain the
    immutable outline route instead of silently guessing a different model.
    """

    state = load_state(job_id) or {}
    outline = state.get("outline") if isinstance(state.get("outline"), dict) else {}
    session_id = str(
        outline.get("_session_id")
        or outline.get("session_id")
        or state.get("session_id")
        or ""
    ).strip()
    if session_id:
        try:
            from studio_agent import store

            try:
                session = store.get_session(
                    session_id,
                    reconcile_jobs=False,
                    _prune_active_jobs=False,
                ) or {}
            except TypeError:
                session = store.get_session(session_id) or {}
            route = store.media_route_snapshot(session)
            return {
                "session_id": session_id,
                "revision": max(1, int(route.get("revision") or 1)),
                "image_model_id": str(
                    route.get("image_model_id") or fallback_image_model or ""
                ).strip(),
                "video_model": str(
                    route.get("video_model") or fallback_video_model or ""
                ).strip(),
                "updated_at": float(route.get("updated_at") or 0.0),
            }
        except Exception:
            # A transient session read must not replace the route token already
            # captured on the owned job with global defaults.
            pass

    try:
        revision = max(
            1,
            int(
                outline.get("media_route_revision")
                or outline.get("_media_route_revision")
                or state.get("media_route_revision")
                or 1
            ),
        )
    except (TypeError, ValueError):
        revision = 1
    return {
        "session_id": session_id,
        "revision": revision,
        "image_model_id": str(
            outline.get("image_model_id")
            or state.get("image_model_id")
            or fallback_image_model
            or ""
        ).strip(),
        "video_model": str(
            outline.get("video_model")
            or state.get("video_model")
            or fallback_video_model
            or ""
        ).strip(),
        "updated_at": float(
            outline.get("media_route_updated_at")
            or state.get("media_route_updated_at")
            or 0.0
        ),
    }


def _same_longform_media_route(
    expected: dict[str, Any],
    current: dict[str, Any],
    *,
    stage: str,
) -> bool:
    key = "image_model_id" if stage == "image" else "video_model"
    return (
        int(expected.get("revision") or 1) == int(current.get("revision") or 1)
        and str(expected.get(key) or "").strip() == str(current.get(key) or "").strip()
    )


def _longform_media_sidecars(path: Path) -> list[Path]:
    return [
        path.with_suffix(path.suffix + ".fal.json"),
        path.with_suffix(path.suffix + ".visualqa.json"),
    ]


def _write_longform_route_receipt(
    job_id: str,
    receipt: dict[str, Any],
    *,
    canonical_asset: Path | None = None,
) -> None:
    receipts_dir = _job_dir(job_id) / "media_route_receipts"
    receipts_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.time_ns()
    event_path = receipts_dir / (
        f"{stamp}-{str(receipt.get('stage') or 'media')}-"
        f"{int(receipt.get('scene_index') or 0):04d}-{uuid.uuid4().hex[:8]}.json"
    )
    payload = dict(receipt)
    payload["receipt_path"] = str(event_path.relative_to(_job_dir(job_id))).replace("\\", "/")
    tmp = event_path.with_suffix(event_path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    tmp.replace(event_path)
    if canonical_asset is not None and str(receipt.get("status") or "") == "committed":
        sidecar = canonical_asset.with_suffix(canonical_asset.suffix + ".media-route.json")
        side_tmp = sidecar.with_suffix(sidecar.suffix + ".tmp")
        side_tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
        side_tmp.replace(sidecar)


def _quarantine_longform_candidate(
    job_id: str,
    candidate: Path,
    *,
    stage: str,
    scene_index: int,
    reason: str,
) -> str:
    if not candidate.is_file():
        for sidecar in _longform_media_sidecars(candidate):
            sidecar.unlink(missing_ok=True)
        return ""
    quarantine_dir = _job_dir(job_id) / "quarantine" / stage
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    safe_reason = re.sub(r"[^a-z0-9_-]+", "-", str(reason).lower()).strip("-")[:32]
    target = quarantine_dir / (
        f"scene-{int(scene_index):04d}-{safe_reason or 'stale'}-{uuid.uuid4().hex[:8]}"
        f"{candidate.suffix}"
    )
    candidate.replace(target)
    for source in _longform_media_sidecars(candidate):
        if source.is_file():
            source.replace(target.with_suffix(target.suffix + source.name[len(candidate.name):]))
    return str(target.relative_to(_job_dir(job_id))).replace("\\", "/")


def _dispatch_longform_media_revision_aware(
    job_id: str,
    *,
    stage: str,
    scene_index: int,
    destination: Path,
    dispatch: Callable[[str, Path, Callable[[], bool]], Path],
    fallback_model: str,
    route_resolver: Callable[[], dict[str, Any]] | None = None,
    max_route_restarts: int = 4,
) -> tuple[Path, dict[str, Any]]:
    """Dispatch, validate and atomically commit one routed media artifact.

    Remote calls only write hidden candidates. A picker change after dispatch,
    during a provider fallback, or immediately before/after commit quarantines
    that candidate and restarts with the latest route. The previous canonical
    asset is retained until a current-revision candidate is safely committed.
    """

    normalized_stage = str(stage or "").strip().lower()
    if normalized_stage not in {"image", "video"}:
        raise ValueError(f"unsupported long-form media stage: {stage!r}")
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    model_key = "image_model_id" if normalized_stage == "image" else "video_model"

    def _resolve() -> dict[str, Any]:
        if route_resolver is not None:
            return dict(route_resolver() or {})
        return _longform_media_route_snapshot(
            job_id,
            fallback_image_model=fallback_model if normalized_stage == "image" else "",
            fallback_video_model=fallback_model if normalized_stage == "video" else "",
        )

    receipts: list[dict[str, Any]] = []
    for route_attempt in range(1, max(1, int(max_route_restarts)) + 1):
        route = _resolve()
        model = str(route.get(model_key) or fallback_model or "").strip()
        route[model_key] = model
        revision = max(1, int(route.get("revision") or 1))
        candidate = destination.with_name(
            f".{destination.stem}.{normalized_stage}.route-{revision}-"
            f"{uuid.uuid4().hex[:10]}{destination.suffix}"
        )
        started_at = time.time()

        def _fallback_route_is_current() -> bool:
            return _same_longform_media_route(route, _resolve(), stage=normalized_stage)

        try:
            rendered = dispatch(model, candidate, _fallback_route_is_current)
            candidate = Path(rendered or candidate)
            if not candidate.is_file() or candidate.stat().st_size <= 0:
                raise LFRenderError(
                    f"{normalized_stage} provider returned no candidate for scene {scene_index}"
                )
        except Exception as exc:
            after_error = _resolve()
            stale = not _same_longform_media_route(route, after_error, stage=normalized_stage)
            quarantined = _quarantine_longform_candidate(
                job_id,
                candidate,
                stage=normalized_stage,
                scene_index=scene_index,
                reason="stale-provider-error" if stale else "provider-failed",
            )
            receipt = {
                "job_id": job_id,
                "stage": normalized_stage,
                "scene_index": int(scene_index),
                "status": "stale_after_provider_error" if stale else "provider_failed",
                "route_attempt": route_attempt,
                "media_route_revision": revision,
                "provider_model": model,
                "route": route,
                "current_route": after_error,
                "quarantined_candidate": quarantined,
                "prior_asset_retained": destination.is_file(),
                "error": f"{type(exc).__name__}: {exc}"[:500],
                "started_at": started_at,
                "finished_at": time.time(),
            }
            receipts.append(receipt)
            _write_longform_route_receipt(job_id, receipt)
            if stale:
                continue
            raise

        after_provider = _resolve()
        if not _same_longform_media_route(route, after_provider, stage=normalized_stage):
            quarantined = _quarantine_longform_candidate(
                job_id,
                candidate,
                stage=normalized_stage,
                scene_index=scene_index,
                reason="stale-after-provider",
            )
            receipt = {
                "job_id": job_id,
                "stage": normalized_stage,
                "scene_index": int(scene_index),
                "status": "stale_after_provider",
                "route_attempt": route_attempt,
                "media_route_revision": revision,
                "provider_model": model,
                "route": route,
                "current_route": after_provider,
                "quarantined_candidate": quarantined,
                "prior_asset_retained": destination.is_file(),
                "started_at": started_at,
                "finished_at": time.time(),
            }
            receipts.append(receipt)
            _write_longform_route_receipt(job_id, receipt)
            continue

        before_commit = _resolve()
        if not _same_longform_media_route(route, before_commit, stage=normalized_stage):
            quarantined = _quarantine_longform_candidate(
                job_id,
                candidate,
                stage=normalized_stage,
                scene_index=scene_index,
                reason="stale-before-commit",
            )
            receipt = {
                "job_id": job_id,
                "stage": normalized_stage,
                "scene_index": int(scene_index),
                "status": "stale_before_commit",
                "route_attempt": route_attempt,
                "media_route_revision": revision,
                "provider_model": model,
                "route": route,
                "current_route": before_commit,
                "quarantined_candidate": quarantined,
                "prior_asset_retained": destination.is_file(),
                "started_at": started_at,
                "finished_at": time.time(),
            }
            receipts.append(receipt)
            _write_longform_route_receipt(job_id, receipt)
            continue

        prior = destination.with_name(f".{destination.name}.{uuid.uuid4().hex[:8]}.prior")
        had_prior = destination.is_file()
        if had_prior:
            shutil.copy2(destination, prior)
        candidate.replace(destination)
        after_commit = _resolve()
        if not _same_longform_media_route(route, after_commit, stage=normalized_stage):
            destination.replace(candidate)
            if prior.is_file():
                prior.replace(destination)
            quarantined = _quarantine_longform_candidate(
                job_id,
                candidate,
                stage=normalized_stage,
                scene_index=scene_index,
                reason="stale-after-commit",
            )
            receipt = {
                "job_id": job_id,
                "stage": normalized_stage,
                "scene_index": int(scene_index),
                "status": "stale_after_commit",
                "route_attempt": route_attempt,
                "media_route_revision": revision,
                "provider_model": model,
                "route": route,
                "current_route": after_commit,
                "quarantined_candidate": quarantined,
                "prior_asset_retained": had_prior,
                "started_at": started_at,
                "finished_at": time.time(),
            }
            receipts.append(receipt)
            _write_longform_route_receipt(job_id, receipt)
            continue

        prior.unlink(missing_ok=True)
        for source in _longform_media_sidecars(candidate):
            suffix = source.name[len(candidate.name):]
            target = destination.with_suffix(destination.suffix + suffix)
            target.unlink(missing_ok=True)
            if source.is_file():
                source.replace(target)
        receipt = {
            "job_id": job_id,
            "stage": normalized_stage,
            "scene_index": int(scene_index),
            "status": "committed",
            "route_attempt": route_attempt,
            "media_route_revision": revision,
            "provider_model": model,
            "route": route,
            "asset": str(destination.relative_to(_job_dir(job_id))).replace("\\", "/"),
            "prior_asset_replaced": had_prior,
            "started_at": started_at,
            "finished_at": time.time(),
        }
        receipts.append(receipt)
        _write_longform_route_receipt(job_id, receipt, canonical_asset=destination)
        return destination, receipt

    raise LFMediaRouteChanged(
        f"{normalized_stage} route changed repeatedly for long-form scene {scene_index}; "
        "no stale provider result was committed",
        receipts=receipts,
    )


def update_status(job_id: str, **fields: Any) -> None:
    """Merge fields into the in-memory status registry. Caller is responsible
    for also persisting whichever fields belong on disk via save_state."""
    entry = _lf_jobs_status.setdefault(job_id, {})
    entry.update(fields)
    entry["updated_at"] = time.time()


def get_status(job_id: str) -> dict[str, Any] | None:
    """Return a defensive copy of the status snapshot or None if no such job."""
    entry = _lf_jobs_status.get(job_id)
    if entry is None:
        # Try to recover from disk — useful after a process restart.
        st = load_state(job_id)
        if not st:
            return None
        entry = {
            "phase": st.get("phase", "unknown"),
            "percent": st.get("percent", 0),
            "error": st.get("error", ""),
            "started_at": st.get("started_at", 0),
            "updated_at": st.get("updated_at", 0),
            "narration_done": st.get("narration_done", 0),
            "narration_total": st.get("narration_total", 0),
            "scene_done": st.get("scenes_generated", 0),
            "scene_total": st.get("scenes_generated", 0),
            "detail": st.get("stage_detail", ""),
        }
        _lf_jobs_status[job_id] = entry
    out = dict(entry)
    out.pop("_task", None)  # don't leak the asyncio.Task object
    return out


def list_recent_jobs(limit: int = 20) -> list[dict[str, Any]]:
    """Return up to N most recent jobs, newest first. Sources state.json on
    disk so survives process restart (memory registry alone wouldn't)."""
    if not LF_OUTPUT_ROOT.exists():
        return []
    rows: list[tuple[float, dict[str, Any]]] = []
    for sub in LF_OUTPUT_ROOT.iterdir():
        if not sub.is_dir():
            continue
        sp = sub / "state.json"
        if not sp.exists():
            continue
        try:
            st = json.loads(sp.read_text(encoding="utf-8"))
        except Exception:
            continue
        ts = float(
            st.get("created_at")
            or st.get("started_at")
            or sp.stat().st_mtime
        )
        st["job_id"] = sub.name
        rows.append((ts, st))
    rows.sort(key=lambda r: r[0], reverse=True)
    return [r[1] for r in rows[:limit]]


_GALLERY_RESUME_PHASES = frozenset({"scenes"})
_FINALIZE_RESUME_PHASES = frozenset({
    "narration", "ambient", "thumbnails", "compose",
    "scene_assembly", "i2v", "vo", "sfx", "finalizing",
})


def resume_stalled_jobs(*, limit: int = 30) -> list[str]:
    """Re-spawn background work killed by Fly redeploy or machine restart."""
    resumed: list[str] = []
    for st in list_recent_jobs(limit=limit):
        job_id = str(st.get("job_id") or "").strip()
        if not job_id:
            continue
        phase = str(st.get("phase") or "")
        try:
            if phase in _GALLERY_RESUME_PHASES and st.get("proof_scene_approved"):
                expand_visual_proof(job_id)
                resumed.append(job_id)
            elif phase in _FINALIZE_RESUME_PHASES:
                start_finalize(job_id)
                resumed.append(job_id)
            elif phase == "failed":
                job_dir = _ensure_job_dir(job_id)
                narration = job_dir / "audio" / "narration.mp3"
                if narration.is_file() and narration.stat().st_size > 8192:
                    start_finalize(job_id)
                    resumed.append(job_id)
        except Exception:
            continue
    return resumed


# ─────────────────────────────────────────────────────────────────────────────
# fal HTTP helpers — share the round-robin key pool pattern from
# zerotier_private/pipeline.py + mongol_project/generate_pipeline.py
# ─────────────────────────────────────────────────────────────────────────────

def _fal_keys() -> list[str]:
    keys: list[str] = []
    for name in ("FAL_AI_KEY", "FAL_AI_KEY_2", "FAL_AI_KEY_3",
                 "FAL_AI_KEY_4", "FAL_AI_KEY_5", "FAL_AI_KEY_6"):
        v = (os.environ.get(name) or "").strip()
        if v:
            keys.append(v)
    if not keys:
        raise LFRenderError("no FAL_AI_KEY* in env")
    return keys


_key_cursor = [0]


def _next_fal_key() -> str:
    keys = _fal_keys()
    k = keys[_key_cursor[0] % len(keys)]
    _key_cursor[0] += 1
    return k


def _fal_post(url: str, payload: dict, *, timeout_s: int = 600, attempts: int = 3) -> dict:
    """POST to fal with retry on 429/5xx + key rotation. Same pattern as
    zerotier_private/pipeline.py — tested on ZT renders."""
    last_err: str = ""
    for attempt in range(attempts):
        key = _next_fal_key()
        try:
            with httpx.Client(timeout=timeout_s) as c:
                r = c.post(
                    url,
                    headers={
                        "Authorization": f"Key {key}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = f"HTTP {r.status_code}: {r.text[:200]}"
                time.sleep(2 ** attempt)
                continue
            raise LFRenderError(f"fal {url.rsplit('/', 2)[-1]} HTTP {r.status_code}: {r.text[:300]}")
        except httpx.HTTPError as exc:
            last_err = f"{type(exc).__name__}: {exc}"
            time.sleep(2 ** attempt)
    raise LFRenderError(f"fal {url} failed after {attempts} attempts: {last_err}")


def _download(url: str, out_path: Path, timeout_s: int = 120) -> None:
    """Stream fal's signed URL to disk. We always download immediately because
    fal signed URLs expire quickly."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as c:
        r = c.get(url)
        r.raise_for_status()
        out_path.write_bytes(r.content)


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1 — chapter expansion (Grok per-chapter narration + scene prompts)
# ─────────────────────────────────────────────────────────────────────────────

CHAPTER_PROMPT_TEMPLATE = """You are writing a single chapter of a long-form documentary.

Channel context (locked grammar):
{channel_system_prompt}

Visual style for this channel:
{visual_style}

Documentary title: {outline_title}
Hook: {outline_hook}
Chapter index: {chapter_index} of {chapter_count}
Chapter title: {chapter_title}
Chapter synopsis: {chapter_synopsis}
Target chapter duration: {chapter_minutes} minutes
Target word count: ~{target_words} words (at {wpm} wpm)
Number of scene-images for this chapter: {scenes_per_chapter}

RETENTION STRUCTURE (required even for calm/sleep delivery): hook an unresolved historical question, build rising
action, make the central conflict concrete, show the comeback/reversal or consequence, then land a final rising
action and payoff. Keep the voice gentle; conflict means narrative causality and stakes, not loud editing.
Every scene prompt must depict the exact narration beat with period-accurate people, architecture, clothing, props,
geography, season, and time of day. Never use a generic history tableau when a specific visual can be shown.

Return strict JSON, NO markdown fences, with this exact shape:
{{
  "chapter_index": {chapter_index},
  "title": "{chapter_title}",
  "narration": "<flowing prose ~{target_words} words. Multiple paragraphs.>",
  "word_count": <int actual>,
  "scene_prompts": [
    "<scene 1 image prompt — 20-40 words. Concrete visual: subject + environment + lighting + framing. Apply the channel visual style. NO text, NO watermarks, NO logos.>",
    ... {scenes_per_chapter} total ...
  ]
}}
"""

CHAPTER_NARRATION_PROMPT_TEMPLATE = """You are writing a single chapter of a long-form documentary.

Channel context (locked grammar):
{channel_system_prompt}

Visual style for this channel:
{visual_style}

Documentary title: {outline_title}
Hook: {outline_hook}
Chapter index: {chapter_index} of {chapter_count}
Chapter title: {chapter_title}
Chapter synopsis: {chapter_synopsis}
Target chapter duration: {chapter_minutes} minutes
Target word count: ~{target_words} words (at {wpm} wpm)

RETENTION STRUCTURE (required even for calm/sleep delivery): hook an unresolved historical question, build rising
action, make the central conflict concrete, show the comeback/reversal or consequence, then land a final rising
action and payoff. Keep the voice gentle; conflict means narrative causality and stakes, not loud editing.

Return ONLY the narration prose (~{target_words} words, multiple paragraphs).
No JSON, no markdown fences, no commentary, no scene prompts.
"""

CHAPTER_SCENES_PROMPT_TEMPLATE = """You are planning still-image prompts for one chapter of a long-form documentary.

Channel context (locked grammar):
{channel_system_prompt}

Visual style for this channel:
{visual_style}

Documentary title: {outline_title}
Chapter index: {chapter_index} of {chapter_count}
Chapter title: {chapter_title}
Number of scene-images required: {scenes_per_chapter}

Every scene prompt must depict the exact narration beat with period-accurate people, architecture, clothing, props,
geography, season, and time of day. Never use a generic history tableau when a specific visual can be shown.

Narration for this chapter (map prompts in chronological order):
{narration_excerpt}

Return strict JSON only, NO markdown fences:
{{
  "scene_prompts": [
    "<scene 1 image prompt — 20-40 words. Concrete visual: subject + environment + lighting + framing. Apply the channel visual style. NO text, NO watermarks, NO logos.>",
    ... exactly {scenes_per_chapter} total ...
  ]
}}
"""


def _strip_json_fences(s: str) -> str:
    s = s.strip().strip("`").strip()
    if s.lower().startswith("json"):
        s = s[4:].strip()
    return s


def _chapter_narration_token_budget(target_words: int) -> int:
    return max(1200, min(32768, int(target_words * 1.45) + 512))


def _chapter_scenes_token_budget(scenes_per_chapter: int) -> int:
    return max(800, min(8192, scenes_per_chapter * 90 + 512))


def _parse_chapter_json(raw: str, *, chapter_index: int) -> dict:
    cleaned = _strip_json_fences(raw)
    try:
        data = json.loads(cleaned)
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        pass
    try:
        from json_repair import repair_json

        repaired = repair_json(cleaned, return_objects=True)
        if isinstance(repaired, dict):
            return repaired
        if isinstance(repaired, str):
            data = json.loads(repaired)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    raise LFRenderError(
        f"chapter {chapter_index} JSON parse failed; raw: {cleaned[:300]}"
    )


def _gen_chapter_narration(
    grok,
    *,
    channel: dict,
    outline: dict,
    chapter_index: int,
    chapter_count: int,
    chapter_title: str,
    chapter_synopsis: str,
    chapter_minutes: int,
    target_words: int,
    wpm: int,
) -> str:
    sys = (
        f"{channel.get('system_prompt', '')}\n\n"
        f"Visual style: {channel.get('visual_style', '')}\n\n"
        "Return narration prose only. No JSON, no markdown fences, no commentary."
    )
    user = CHAPTER_NARRATION_PROMPT_TEMPLATE.format(
        channel_system_prompt=channel.get("system_prompt", ""),
        visual_style=channel.get("visual_style", ""),
        outline_title=outline.get("title", ""),
        outline_hook=outline.get("hook", ""),
        chapter_index=chapter_index,
        chapter_count=chapter_count,
        chapter_title=chapter_title,
        chapter_synopsis=chapter_synopsis,
        chapter_minutes=chapter_minutes,
        target_words=target_words,
        wpm=wpm,
    )
    narration_budget = _chapter_narration_token_budget(target_words)
    raw = grok.complete(sys, user, max_tokens=narration_budget, temperature=0.65)
    narration = _strip_json_fences(raw).strip()
    if not narration:
        raise LFRenderError(f"chapter {chapter_index} narration empty")
    return narration


def _gen_chapter_scene_prompts(
    grok,
    *,
    channel: dict,
    outline: dict,
    chapter_index: int,
    chapter_count: int,
    chapter_title: str,
    narration: str,
    scenes_per_chapter: int,
) -> list[str]:
    excerpt = narration if len(narration) <= 12000 else (
        narration[:6000] + "\n\n[... middle omitted for brevity ...]\n\n" + narration[-4000:]
    )
    sys = (
        f"{channel.get('system_prompt', '')}\n\n"
        f"Visual style: {channel.get('visual_style', '')}\n\n"
        "Output strict JSON only. No markdown fences, no commentary."
    )
    user = CHAPTER_SCENES_PROMPT_TEMPLATE.format(
        channel_system_prompt=channel.get("system_prompt", ""),
        visual_style=channel.get("visual_style", ""),
        outline_title=outline.get("title", ""),
        chapter_index=chapter_index,
        chapter_count=chapter_count,
        chapter_title=chapter_title,
        scenes_per_chapter=scenes_per_chapter,
        narration_excerpt=excerpt,
    )
    scenes_budget = _chapter_scenes_token_budget(scenes_per_chapter)
    raw = grok.complete(sys, user, max_tokens=scenes_budget, temperature=0.55)
    data = _parse_chapter_json(raw, chapter_index=chapter_index)
    prompts = [str(p) for p in (data.get("scene_prompts") or []) if str(p).strip()]
    if not prompts:
        raise LFRenderError(f"chapter {chapter_index} scene_prompts empty")
    return prompts


def _gen_chapter(
    grok,
    *,
    channel: dict,
    outline: dict,
    chapter_index: int,
    chapter_count: int,
    scenes_per_chapter: int,
    wpm: int,
) -> dict:
    """Run one chapter expansion via the existing GrokClient. Caller passes
    the same client instance across all chapters so we get session reuse."""
    chapters = outline.get("chapters") or []
    if chapter_index >= len(chapters):
        raise LFRenderError(f"chapter_index {chapter_index} out of range (have {len(chapters)})")
    ch = chapters[chapter_index]
    chapter_minutes = max(1, int(ch.get("minutes", 1)))
    target_words = chapter_minutes * wpm
    chapter_title = str(ch.get("title", f"Chapter {chapter_index + 1}"))

    # Always split narration + scene prompts — combined JSON blobs truncate on sleep-doc prose.
    narration = _gen_chapter_narration(
        grok,
        channel=channel,
        outline=outline,
        chapter_index=chapter_index,
        chapter_count=chapter_count,
        chapter_title=chapter_title,
        chapter_synopsis=str(ch.get("synopsis", "")),
        chapter_minutes=chapter_minutes,
        target_words=target_words,
        wpm=wpm,
    )
    scene_prompts = _gen_chapter_scene_prompts(
        grok,
        channel=channel,
        outline=outline,
        chapter_index=chapter_index,
        chapter_count=chapter_count,
        chapter_title=chapter_title,
        narration=narration,
        scenes_per_chapter=scenes_per_chapter,
    )
    return {
        "chapter_index": chapter_index,
        "title": chapter_title,
        "narration": narration,
        "word_count": len(narration.split()),
        "scene_prompts": scene_prompts,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 — scene image gen (ernie-image per scene; thread pool for throughput)
# ─────────────────────────────────────────────────────────────────────────────

def _gen_scene_image(prompt: str, out_path: Path, *, image_model: str = "ernie") -> Path:
    """Render a single scene still. ernie-image is the canonical sleep-doc
    model (cheap $0.03/image, 1920×1080). seedream falls back via param if
    Casey wants higher fidelity at 4× cost."""
    if out_path.exists() and out_path.stat().st_size > 1024:
        return out_path
    normalized = str(image_model or "ernie_image").strip().lower()
    if normalized in {
        "grok_imagine", "grok_imagine_quality", "grok-imagine-image-quality",
        "grok_imagine_standard", "grok-imagine-image",
    }:
        api_key = str(os.environ.get("XAI_API_KEY") or "").strip()
        if not api_key:
            raise LFRenderError("xAI image generation requires XAI_API_KEY")
        xai_model = (
            "grok-imagine-image-quality"
            if normalized in {"grok_imagine", "grok_imagine_quality", "grok-imagine-image-quality"}
            else "grok-imagine-image"
        )
        with httpx.Client(timeout=240, follow_redirects=True) as client:
            response = client.post(
                "https://api.x.ai/v1/images/generations",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={
                    "model": xai_model,
                    "prompt": str(prompt or "")[:759],
                    "n": 1,
                    "response_format": "b64_json",
                    "aspect_ratio": "16:9",
                    "resolution": "1k",
                },
            )
        if response.status_code not in (200, 201):
            raise LFRenderError(f"{xai_model} {response.status_code}: {response.text[:300]}")
        item = ((response.json() or {}).get("data") or [{}])[0] or {}
        encoded = str(item.get("b64_json") or "").strip()
        remote_url = str(item.get("url") or "").strip()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if encoded:
            out_path.write_bytes(base64.b64decode(encoded))
        elif remote_url:
            _download(remote_url, out_path, timeout_s=120)
        else:
            raise LFRenderError(f"{xai_model} returned no image data")
        return out_path
    if is_seedream_model(normalized):
        normalized = normalize_seedream_model_id(normalized)
        from skeleton_ai.styled_stills import (
            _modal_seedream_t2i_result,
            _seedream_t2i_payload,
        )
        from skeleton_ai.canonical_edit import _first_result_image_url

        payload = _seedream_t2i_payload(
            normalized,
            prompt=prompt,
            negative_prompt="text, watermark, distorted anatomy, low quality",
            seed=420042,
        )
        payload["image_size"] = "landscape_16_9"
        provider = seedream_provider(normalized)
        endpoint = seedream_endpoint(normalized, edit=False)
        if provider == "modal":
            spec = seedream_model_spec(normalized)
            data = _modal_seedream_t2i_result(
                endpoint,
                payload,
                remote_model_id=str(spec.get("remote_model_id") or "bytedance/seedream/v5/lite"),
            )
            img_url = _first_result_image_url(data)
            if not img_url:
                raise LFRenderError(f"{normalized} returned no image URL")
            _download(img_url, out_path, timeout_s=120)
            return out_path
        url = f"https://fal.run/{endpoint}"
    else:
        url = ERNIE_URL
        payload = {"prompt": prompt, "image_size": {"width": 1920, "height": 1080}}
    data = _fal_post(url, payload, timeout_s=240)
    images = data.get("images") or []
    if not images:
        raise LFRenderError(f"image gen returned no images: {data}")
    img_url = images[0].get("url", "")
    if not img_url:
        raise LFRenderError(f"image gen response missing url: {data}")
    _download(img_url, out_path, timeout_s=120)
    return out_path


def _outline_uses_skeleton(outline: dict[str, Any] | None) -> bool:
    """True only for Studio's explicit canonical-skeleton render style."""
    data = outline or {}
    blob = " ".join(
        str(data.get(key) or "")
        for key in ("render_style", "render_style_lock", "visual_style")
    ).lower()
    return "skeleton" in blob or "mrskelewelly" in blob or "mr skelewelly" in blob


def _gen_skeleton_longform_scene(
    prompt: str,
    out_path: Path,
    *,
    topic: str = "",
    locked_outfit: str = "",
    image_model_id: str = "seedream_edit",
    route_guard: Callable[[], bool] | None = None,
) -> Path:
    """Reference-edit + QA one 16:9 skeleton long-form still.

    A long-form job may contain hundreds of scenes, so a single artifact cannot
    be allowed to silently enter its review gallery. Every still is compared to
    the canonical master; one failed frame gets one fresh canonical retry and a
    second failure is excluded from the render rather than being normalized as
    acceptable output.
    """
    from skeleton_ai.canonical_edit import (
        build_scene_edit_prompt,
        generate_still_edit,
        resolve_master_reference_local,
    )
    from studio_agent.visual_qa import audit_skeleton_still

    out_path = Path(out_path)
    outfit = locked_outfit or (
        "no clothing; full clear glass shell and ivory skeleton visible; empty hands; no jewelry"
    )
    reference = resolve_master_reference_local()

    def _audit(force: bool = False) -> dict[str, Any]:
        return audit_skeleton_still(
            out_path,
            reference=reference,
            locked_outfit=outfit,
            force=force,
        )

    if out_path.exists() and out_path.stat().st_size > 1024:
        existing = _audit()
        if existing.get("status") == "pass" and existing.get("pass") is True:
            return out_path
        rejected = out_path.parent / "rejected_stills" / f"{out_path.stem}_cached_rejected.png"
        rejected.parent.mkdir(parents=True, exist_ok=True)
        rejected.unlink(missing_ok=True)
        out_path.replace(rejected)

    compiled = build_scene_edit_prompt(
        topic=topic,
        visual_description=prompt,
        outfit=outfit,
        visual_brief="Long-form cinematic scene using the canonical skeleton host",
        aspect_ratio="16:9",
        cast_count=1,
    )
    # Honor the universal ≤300-char visual fix contract.
    from studio_agent.visual_fix_contract import PROMPT_CHAR_BUDGET

    compiled = compiled[:PROMPT_CHAR_BUDGET]
    rejected_reports: list[str] = []
    for attempt in (1, 2):
        if route_guard is not None and not route_guard():
            raise LFMediaRouteChanged(
                "image route changed before skeleton still provider retry"
            )
        generate_still_edit(
            compiled,
            out_path,
            seed=420042 + attempt,
            image_model_id=image_model_id or "seedream_edit",
        )
        qa = _audit(force=True)
        if qa.get("status") == "pass" and qa.get("pass") is True:
            return out_path
        rejected = out_path.parent / "rejected_stills" / f"{out_path.stem}_attempt{attempt}.png"
        rejected.parent.mkdir(parents=True, exist_ok=True)
        rejected.unlink(missing_ok=True)
        if out_path.is_file():
            out_path.replace(rejected)
        rejected_reports.append(str(qa.get("summary") or "semantic still QA failed")[:300])

    raise LFRenderError(
        f"skeleton long-form still {out_path.name} blocked by semantic QA after two attempts: "
        + " | ".join(rejected_reports)
    )


def _gen_scenes_batch(
    chapters: list[dict],
    stills_dir: Path,
    image_model: str,
    *,
    job_id: str | None = None,
    route_resolver: Callable[[], dict[str, Any]] | None = None,
    on_progress: Callable[[int, int], None] | None = None,
    concurrency: int = 4,
    skeleton_style: bool = False,
    topic: str = "",
    locked_outfit: str = "",
) -> list[Path]:
    """Generate every scene image in chapters[*].scene_prompts in parallel.

    Indexing: global_idx = chapter_index * scenes_per_chapter + local_idx
    Filename: scene_NNNN.png (zero-padded global)
    """
    tasks: list[tuple[int, str, Path]] = []
    scenes_per_chapter = len(chapters[0].get("scene_prompts") or []) if chapters else 0
    max_scenes = max(1, int(os.environ.get("STUDIO_LONGFORM_MAX_SCENES", "144") or 144))
    for ch in chapters:
        ch_idx = int(ch.get("chapter_index", 0))
        for local_idx, prompt in enumerate(ch.get("scene_prompts") or []):
            global_idx = ch_idx * scenes_per_chapter + local_idx
            if global_idx >= max_scenes:
                break
            out = stills_dir / f"scene_{global_idx:04d}.png"
            tasks.append((global_idx, prompt, out))

    total = len(tasks)
    out_paths: list[Path] = []
    done = 0
    pending: list[tuple[int, str, Path]] = []
    for gi, prompt, out in tasks:
        try:
            if out.is_file() and out.stat().st_size > 4096:
                out_paths.append(out)
                done += 1
                continue
        except Exception:
            pass
        pending.append((gi, prompt, out))
    if on_progress and done > 0:
        on_progress(done, total)
    if not pending:
        out_paths.sort(key=lambda p: int(re.search(r"scene_(\d+)", p.name).group(1)))
        return out_paths

    def _generate_one(gi: int, prompt: str, out: Path) -> Path:
        if not job_id:
            if skeleton_style:
                return _gen_skeleton_longform_scene(
                    prompt,
                    out,
                    topic=topic,
                    locked_outfit=locked_outfit,
                    image_model_id=image_model,
                )
            return _gen_scene_image(prompt, out, image_model=image_model)

        def _dispatch(model: str, candidate: Path, route_guard: Callable[[], bool]) -> Path:
            if skeleton_style:
                return _gen_skeleton_longform_scene(
                    prompt,
                    candidate,
                    topic=topic,
                    locked_outfit=locked_outfit,
                    image_model_id=model,
                    route_guard=route_guard,
                )
            return _gen_scene_image(prompt, candidate, image_model=model)

        committed, _receipt = _dispatch_longform_media_revision_aware(
            job_id,
            stage="image",
            scene_index=gi,
            destination=out,
            dispatch=_dispatch,
            fallback_model=image_model,
            route_resolver=route_resolver,
        )
        return committed

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        future_to_task = {
            ex.submit(_generate_one, gi, prompt, out): (gi, out)
            for gi, prompt, out in pending
        }
        for fut in as_completed(future_to_task):
            gi, out = future_to_task[fut]
            try:
                fut.result()
                out_paths.append(out)
            except Exception as e:
                # Keep going — single scene failure shouldn't kill 540-scene render.
                # We log it on state.json so the operator can re-run after.
                print(f"[scenes] scene {gi} failed: {e}")
            done += 1
            if on_progress and (done % 5 == 0 or done == total):
                on_progress(done, total)
    out_paths.sort(key=lambda p: int(re.search(r"scene_(\d+)", p.name).group(1)))
    return out_paths


# ─────────────────────────────────────────────────────────────────────────────
# Phase 3 — fal MiniMax narration (per-chapter, then concat)
# ─────────────────────────────────────────────────────────────────────────────

def _gen_minimax_chapter(
    text: str,
    out_path: Path,
    *,
    voice_id: str = "English_Trustworthy_Man",
) -> Path:
    """Render one chapter's narration via fal MiniMax speech-02-hd.

    speech-02-hd is the premium-tier voice mandated by the HR feedback memory
    (feedback_hr_premium_fal_tts.md): Egypt 9H Edge-TTS shipped and Casey
    called it bad. fal MiniMax is the only acceptable HR voice tier.
    """
    if out_path.exists() and out_path.stat().st_size > 4096:
        return out_path
    payload = {
        "text": text,
        "voice_setting": {
            "voice_id": voice_id,
            "speed": 0.92,                # slower than default for sleep pacing
            "vol": 1.0,
            "pitch": 0,
        },
        "output_format": "url",     # fal expects 'url' or 'hex' here — NOT 'mp3'.
                                     # 'mp3' is the audio container which already
                                     # goes in audio_setting.format below.
        "audio_setting": {
            "sample_rate": 32000,
            "bitrate": 128000,
            "format": "mp3",
            "channel": 1,
        },
    }
    # MiniMax has a per-call char limit (~5000 chars). Chunk if needed.
    text = text.strip()
    if len(text) <= 5000:
        data = _fal_post(MINIMAX_TTS_URL, payload, timeout_s=300)
        url = (data.get("audio") or {}).get("url") or data.get("audio_url")
        if not url:
            raise LFRenderError(f"MiniMax response missing audio url: {data}")
        _download(url, out_path, timeout_s=120)
        return out_path

    # Long chapter — chunk into <=4500 char paragraph batches, render each,
    # then ffmpeg concat into the chapter MP3.
    parts = _chunk_text(text, max_chars=4500)
    part_paths: list[Path] = []
    for i, part in enumerate(parts):
        part_payload = dict(payload, text=part)
        data = _fal_post(MINIMAX_TTS_URL, part_payload, timeout_s=300)
        url = (data.get("audio") or {}).get("url") or data.get("audio_url")
        if not url:
            raise LFRenderError(f"MiniMax part {i} missing url: {data}")
        pp = out_path.with_name(f"{out_path.stem}_p{i:02d}.mp3")
        _download(url, pp, timeout_s=120)
        part_paths.append(pp)
    _ffmpeg_concat_audio(part_paths, out_path)
    for pp in part_paths:
        try:
            pp.unlink()
        except Exception:
            pass
    return out_path


def _gen_xai_chapter(
    text: str,
    out_path: Path,
    *,
    voice_id: str = "rex",
) -> Path:
    """Render a chapter with the owner's xAI API key, chunked and resumable."""
    if out_path.exists() and out_path.stat().st_size > 4096:
        return out_path
    from skeleton_ai.voice_xai import synthesize

    clean = str(text or "").strip()
    if not clean:
        raise LFRenderError("xAI narration text is empty")
    parts = _chunk_text(clean, max_chars=4000)
    part_paths: list[Path] = []
    for i, part in enumerate(parts):
        pp = out_path.with_name(f"{out_path.stem}_xai_p{i:03d}.mp3")
        if not (pp.exists() and pp.stat().st_size > 2048):
            synthesize(text=part, out_path=pp, voice_id=voice_id, speed=0.92)
        part_paths.append(pp)
    if len(part_paths) == 1:
        part_paths[0].replace(out_path)
    else:
        _ffmpeg_concat_audio(part_paths, out_path)
        for pp in part_paths:
            pp.unlink(missing_ok=True)
    return out_path


def _chunk_text(text: str, *, max_chars: int = 4500) -> list[str]:
    """Split text on paragraph boundaries so each chunk <= max_chars."""
    paras = re.split(r"\n\n+", text.strip())
    out: list[str] = []
    buf = ""
    for p in paras:
        candidate = (buf + "\n\n" + p) if buf else p
        if len(candidate) <= max_chars:
            buf = candidate
        else:
            if buf:
                out.append(buf)
            # If the single paragraph exceeds max_chars, split on sentence breaks.
            if len(p) > max_chars:
                sents = re.split(r"(?<=[\.!?])\s+", p)
                cur = ""
                for s in sents:
                    cand = (cur + " " + s) if cur else s
                    if len(cand) <= max_chars:
                        cur = cand
                    else:
                        if cur:
                            out.append(cur)
                        cur = s
                if cur:
                    out.append(cur)
                buf = ""
            else:
                buf = p
    if buf:
        out.append(buf)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Phase 4 — mmaudio ambient bed
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_AMBIENT_PROMPT = (
    "calm orchestral ambient pad, slow strings and low brass, gentle, "
    "cinematic, no drums, no percussion, continuous drone, bedtime atmosphere"
)

_BGM_OFF = {"off", "none", "no", "no background music"}


def resolve_background_music(outline: dict, channel: dict) -> str:
    """Pick BGM mode for finalize. Channel default upgrades legacy 'off' jobs."""
    bgm = str(outline.get("background_music") or "auto").strip() or "auto"
    if bgm.lower() in _BGM_OFF:
        channel_default = str(channel.get("default_background_music") or "").strip()
        if channel_default and channel_default.lower() not in _BGM_OFF:
            bgm = channel_default
    return bgm


def resolve_ambient_prompt(*, bgm_choice: str, channel: dict, outline: dict) -> str:
    """Build the mmaudio prompt for the ambient bed under narration."""
    outline_brief = str(outline.get("sound_design_brief") or "").strip()
    channel_brief = str(channel.get("sound_design") or "").strip()
    bed_suffix = (
        "Long-form cinematic ambient bed, instrumental only, no vocals, "
        "no lyrics, low-volume under narration."
    )
    if bgm_choice.lower() in _BGM_OFF:
        brief = outline_brief or channel_brief
        return (
            (f"{brief}. " if brief else "")
            + "Very subtle natural room tone and sparse sound effects only, "
            "no music, no melody, no vocals, no lyrics."
        )
    if bgm_choice.lower() not in {"", "auto"}:
        brief = outline_brief or channel_brief
        parts = [p for p in (brief, f"Background music direction: {bgm_choice}.", bed_suffix) if p]
        return " ".join(parts)
    base = str(channel.get("ambient_bed_prompt") or DEFAULT_AMBIENT_PROMPT).strip() or DEFAULT_AMBIENT_PROMPT
    if outline_brief and outline_brief != channel_brief:
        return f"{outline_brief}. {base}"
    return base


def resolve_ken_burns(outline: dict, channel: dict) -> tuple[bool, bool]:
    """Sleep-history channels always Ken-Burns stills even if outline flags are stale."""
    channel_key = str(channel.get("key") or "").strip().lower()
    motion_policy = str(outline.get("motion_policy") or "").strip().lower()
    if channel_key == "history_rewind" or motion_policy == "stills":
        return True, bool(outline.get("light_shake_enabled", True))
    return (
        bool(outline.get("ken_burns_enabled", True)),
        bool(outline.get("light_shake_enabled", False)),
    )


MMAUDIO_MAX_DURATION_SEC = 30


def _gen_ambient(out_path: Path, *, prompt: str = DEFAULT_AMBIENT_PROMPT, duration_sec: int = 30) -> Path:
    """Short ambient loop (fal mmaudio caps duration at 30s). ffmpeg stream-loops
    it under the full narration during compose."""
    if out_path.exists() and out_path.stat().st_size > 1024:
        return out_path
    duration = max(1, min(int(duration_sec or 30), MMAUDIO_MAX_DURATION_SEC))
    data = _fal_post(MMAUDIO_URL, {"prompt": prompt, "duration": duration}, timeout_s=120)
    url = (data.get("audio") or {}).get("url") or data.get("audio_url")
    if not url:
        raise LFRenderError(f"mmaudio response missing audio url: {data}")
    _download(url, out_path, timeout_s=120)
    return out_path


# ─────────────────────────────────────────────────────────────────────────────
# Phase 5 — seedream thumbnails (3 candidates from channel thumbnail_style)
# ─────────────────────────────────────────────────────────────────────────────

# Cache of channel_id -> (fetched_at, [thumbnail urls]). The channel's real
# published covers change rarely; refreshing every 6h keeps the style current
# without hammering the RSS feed on every candidate render.
_channel_thumb_ref_cache: dict[str, tuple[float, list[str]]] = {}
_CHANNEL_THUMB_REF_TTL_S = 6 * 3600


def channel_reference_thumbnails(channel: dict, max_refs: int = 3) -> list[str]:
    """Return public CDN URLs of the channel's real published thumbnails.

    This is how thumbnail generation stays in the creator's actual style
    instead of a hand-written prompt: the latest published covers are pulled
    (no auth — YouTube RSS feed + i.ytimg.com) and fed to Seedream *edit* as
    style references. Self-updating: as the channel's look evolves, so do the
    references. Fails soft to [] so the T2I fallback still renders."""
    channel_id = str(channel.get("channel_id") or "").strip()
    if not channel_id.startswith("UC"):
        return []
    cached = _channel_thumb_ref_cache.get(channel_id)
    if cached and (time.time() - cached[0]) < _CHANNEL_THUMB_REF_TTL_S:
        return list(cached[1][:max_refs])
    refs: list[str] = []
    try:
        with httpx.Client(timeout=15, follow_redirects=True) as c:
            feed = c.get(
                f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
            )
            feed.raise_for_status()
            video_ids = re.findall(r"<yt:videoId>([\w-]{6,20})</yt:videoId>", feed.text)
            for vid in video_ids:
                if len(refs) >= max_refs:
                    break
                # maxresdefault is 1280x720; hqdefault (480x360, letterboxed)
                # would skew the edit toward 4:3, so it is not used.
                url = f"https://i.ytimg.com/vi/{vid}/maxresdefault.jpg"
                try:
                    head = c.head(url)
                    if head.status_code == 200:
                        refs.append(url)
                except Exception:
                    continue
    except Exception as exc:
        print(f"[thumbnails] channel reference fetch failed for {channel_id}: {exc}")
        return []
    _channel_thumb_ref_cache[channel_id] = (time.time(), refs)
    return list(refs[:max_refs])


def thumbnail_display_title(title: str) -> str:
    """Short on-image title in the channel's real cover style.

    The SEO title ("The Rise and Fall of the Mongol Empire | Full Documentary |
    9 Hours | History for Sleep") is what YouTube shows under the video; the
    cover itself only says "THE MONGOL EMPIRE". Rendering the full SEO title on
    the image is what produced the banner-strip / badge clutter Casey rejected."""
    core = str(title or "").split("|", 1)[0].strip().rstrip(".")
    m = re.match(r"^the\s+(?:rise\s+and\s+fall|history|story)\s+of\s+(the\s+)?(.+)$", core, re.I)
    if m:
        core = ("The " if m.group(1) else "") + m.group(2).strip()
    return core or str(title or "").strip()


def _thumbnail_via_channel_references(
    prompt: str,
    refs: list[str],
    out: Path,
    *,
    timeout_s: int = 180,
) -> bool:
    """Render one thumbnail with the channel's real covers as style anchors.

    Returns True on success; False lets the caller fall back to plain T2I."""
    styled_prompt = (
        "The attached images are this channel's real published YouTube thumbnails — "
        "they are the ONLY style authority. Create ONE NEW 16:9 thumbnail for the new "
        "video below that belongs unmistakably to the same set: same art style, "
        "rendering technique, color grade, lighting, and composition language. "
        "Title text must copy the references' treatment exactly — same typeface "
        "feel, size, placement, and restraint; keep it as short as the references keep "
        "theirs. STRICT: the ONLY text on the image is the short title; add NOTHING "
        "the references do not use (no corner badges, no banner strips, no subtitle "
        "rows, no taglines, no duration text, no logos). New subject and scene for "
        "the new title; identical channel identity.\n\n"
        f"{prompt}"
    )
    try:
        data = _fal_post(
            SEEDREAM_EDIT_URL,
            {
                "prompt": styled_prompt,
                "image_urls": list(refs),
                "image_size": {"width": 1920, "height": 1080},
                "num_images": 1,
                "negative_prompt": (
                    "collage, grid of images, split frame, watermark, "
                    "garbled text, misspelled words, extra badges"
                ),
            },
            timeout_s=timeout_s,
        )
        images = data.get("images") or []
        img_url = (images[0] or {}).get("url", "") if images else ""
        if not img_url:
            return False
        _download(img_url, out, timeout_s=120)
        return True
    except Exception as exc:
        print(f"[thumbnails] reference-styled render failed, falling back to T2I: {exc}")
        return False


def _gen_thumbnails(channel: dict, outline: dict, thumbs_dir: Path, count: int = 3) -> list[Path]:
    """Generate N thumbnail candidates. PR #132: bumped resolution to
    1920x1080 (was 1280x720) for higher-fidelity covers + dropped the
    'Wide establishing shot' variant (which produced thumbnails with
    too-small subjects on Casey's Peter Thiel render — the #1 tile he
    rejected). Variants are now all subject-prominent compositions."""
    base_prompt = (channel.get("thumbnail_style_prompt") or channel.get("visual_style") or "").strip()
    title = (outline.get("title") or "").strip()
    out_paths: list[Path] = []
    # The channel's own published covers are the ground-truth style. When they
    # are reachable, every candidate is rendered as an edit against them; the
    # hand-written style prompt only carries subject/variant direction.
    refs = channel_reference_thumbnails(channel)
    # Subject-prominent variants only (no wide-establishing shots).
    variant_hints = [
        "Medium portrait composition, subject filling 40-50% of frame, dramatic key light.",
        "Low-angle dramatic composition, subject silhouetted against backlight, heroic stance.",
        "Tight chest-up close-up, subject filling 60% of frame, shallow depth of field.",
    ]
    for i in range(count):
        out = thumbs_dir / f"thumb_{i + 1}.png"
        if out.exists() and out.stat().st_size > 1024:
            out_paths.append(out)
            continue
        if refs and _thumbnail_via_channel_references(
            (
                f"Short on-image title (render exactly this text): {thumbnail_display_title(title)}.\n"
                f"Full video topic for the scene: {title}.\n"
                f"Composition variant: {variant_hints[i % len(variant_hints)]}"
            ),
            refs,
            out,
        ):
            out_paths.append(out)
            continue
        full_prompt = (
            f"{base_prompt}\n\nDocumentary title context: {title}.\n\n"
            f"Composition variant: {variant_hints[i % len(variant_hints)]}"
        )
        data = _fal_post(
            SEEDREAM_URL,
            {
                "prompt": full_prompt,
                "image_size": {"width": 1920, "height": 1080},
                "negative_prompt": (
                    "real human face, photographic skin, photorealistic person, "
                    "real eyes, real mouth, ordinary human, model, actor, "
                    "wide establishing shot, tiny subject, distant subject"
                ),
            },
            timeout_s=180,
        )
        images = data.get("images") or []
        if not images:
            print(f"[thumbnails] thumb {i + 1} returned no images")
            continue
        img_url = images[0].get("url", "")
        if not img_url:
            continue
        _download(img_url, out, timeout_s=120)
        out_paths.append(out)
    return out_paths


# ─────────────────────────────────────────────────────────────────────────────
# Phase 6 — ffmpeg compose (slideshow + audio mix + 2-pass loudnorm + mux)
#
# Mirrors mongol_project/compose_1080p60.sh which is the validated sleep-doc
# slideshow recipe. Per-scene duration = narration_total / scene_count, then
# concat-demuxer + amix ambient under narration + libx264 1080p output.
# ─────────────────────────────────────────────────────────────────────────────

def _ffprobe_dur(path: Path) -> float:
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise LFRenderError(f"ffprobe failed on {path}: {r.stderr[:200]}")
    try:
        return float((r.stdout or "0").strip())
    except ValueError:
        return 0.0


def _ffmpeg_concat_audio(parts: list[Path], out_path: Path) -> Path:
    """Bit-stream concat MP3 parts (no re-encode) via concat demuxer."""
    if not parts:
        raise LFRenderError("no audio parts to concat")
    list_file = out_path.with_suffix(".list.txt")
    list_file.write_text(
        "\n".join(f"file '{str(p.resolve()).replace(chr(92), '/')}'" for p in parts),
        encoding="utf-8",
    )
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
        "-f", "concat", "-safe", "0", "-i", str(list_file),
        "-c", "copy", str(out_path),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    list_file.unlink(missing_ok=True)
    if r.returncode != 0:
        raise LFRenderError(f"ffmpeg concat-audio failed: {r.stderr[-400:]}")
    return out_path


def _two_pass_loudnorm(in_path: Path, out_path: Path) -> Path:
    """Broadcast-grade 2-pass loudnorm to -14 LUFS. Same target ZT private
    Phase 4.6 uses for shorts."""
    # Pass 1: measure
    cmd1 = [
        "ffmpeg", "-hide_banner", "-loglevel", "info", "-i", str(in_path),
        "-af", "loudnorm=I=-14:TP=-1.5:LRA=11:print_format=json",
        "-f", "null", "-",
    ]
    r1 = subprocess.run(cmd1, capture_output=True, text=True)
    out_text = (r1.stderr or "") + (r1.stdout or "")
    m = re.search(r"\{[^{}]*\"input_i\"[^{}]*\}", out_text, re.DOTALL)
    if not m:
        # If measurement fails, skip second pass and just bit-copy.
        out_path.write_bytes(in_path.read_bytes())
        return out_path
    measured = json.loads(m.group(0))
    cmd2 = [
        "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
        "-i", str(in_path),
        "-af", (
            "loudnorm=I=-14:TP=-1.5:LRA=11:"
            f"measured_I={measured.get('input_i', '-23')}:"
            f"measured_TP={measured.get('input_tp', '-2')}:"
            f"measured_LRA={measured.get('input_lra', '7')}:"
            f"measured_thresh={measured.get('input_thresh', '-34')}:"
            f"offset={measured.get('target_offset', '0')}:"
            "linear=true:print_format=summary"
        ),
        "-c:a", "libmp3lame", "-b:a", "192k",
        str(out_path),
    ]
    r2 = subprocess.run(cmd2, capture_output=True, text=True)
    if r2.returncode != 0:
        raise LFRenderError(f"loudnorm pass 2 failed: {r2.stderr[-400:]}")
    return out_path


def _list_scenes_sorted(stills_dir: Path) -> list[Path]:
    """Sort by integer scene index (scene_NNNN.png) — the dir-listing order
    is alphabetical by default which would put scene_10 before scene_2."""
    matches: list[tuple[int, Path]] = []
    for p in stills_dir.iterdir():
        if not p.is_file():
            continue
        m = re.match(r"scene_(\d+)", p.stem)
        if not m:
            continue
        if p.suffix.lower() not in (".png", ".jpg", ".jpeg", ".webp"):
            continue
        matches.append((int(m.group(1)), p))
    matches.sort(key=lambda r: r[0])
    return [p for _, p in matches]


def _sum_chapter_audio_durations(audio_dir: Path) -> float:
    total = 0.0
    for part in sorted(audio_dir.glob("chapter_*.mp3")):
        if part.is_file() and part.stat().st_size > 512:
            total += _ffprobe_dur(part)
    return total


def _resolve_narration_duration_sec(narration: Path, *, job_id: str | None = None) -> float:
    """Use the longest trustworthy narration duration (fixes truncated concat probes)."""
    probe = _ffprobe_dur(narration)
    candidates = [probe] if probe > 0 else []
    if job_id:
        st = load_state(job_id) or {}
        stored = float(st.get("narration_duration_sec") or 0)
        if stored > 0:
            candidates.append(stored)
        audio_dir = _ensure_job_dir(job_id) / "audio"
        chapter_sum = _sum_chapter_audio_durations(audio_dir)
        if chapter_sum > 0:
            candidates.append(chapter_sum)
    if not candidates:
        return 0.0
    best = max(candidates)
    if probe > 0 and best > probe * 1.25:
        return best
    return best


def _compose_slideshow(
    stills: list[Path],
    narration: Path,
    ambient: Path,
    out_path: Path,
    *,
    fps: int = 60,
    ken_burns_enabled: bool = True,
    light_shake_enabled: bool = False,
    job_id: str | None = None,
) -> Path:
    """Full slideshow compose: scenes held for narration_total/scene_count
    seconds each, ambient mixed under narration at -16dB."""
    narr_sec = _resolve_narration_duration_sec(narration, job_id=job_id)
    if narr_sec <= 0:
        raise LFRenderError("narration has zero duration")
    scene_count = len(stills)
    if scene_count == 0:
        raise LFRenderError("no scene stills to compose")
    per_scene = narr_sec / scene_count
    if per_scene < 0.25:
        raise LFRenderError(
            f"narration duration {narr_sec:.1f}s too short for {scene_count} scenes "
            f"(probe={_ffprobe_dur(narration):.1f}s) — check audio/narration.mp3 concat"
        )

    # Mix audio: ambient stream-looped + narration, longest=narration
    mix_path = out_path.with_name(out_path.stem + "_mix.mp3")
    cmd_mix = [
        "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
        "-stream_loop", "-1", "-i", str(ambient),
        "-i", str(narration),
        "-filter_complex",
        "[0:a]volume=0.15[a0];[1:a]volume=1.0[a1];"
        "[a0][a1]amix=inputs=2:duration=longest:dropout_transition=3",
        "-t", f"{narr_sec:.3f}",
        "-c:a", "libmp3lame", "-b:a", "192k",
        str(mix_path),
    ]
    r = subprocess.run(cmd_mix, capture_output=True, text=True)
    if r.returncode != 0:
        raise LFRenderError(f"ffmpeg amix failed: {r.stderr[-400:]}")

    # 2-pass loudnorm on the mix.
    final_audio = mix_path.with_name(mix_path.stem + "_lk.mp3")
    _two_pass_loudnorm(mix_path, final_audio)

    # Build concat-demuxer list.
    concat_file = out_path.with_suffix(".concat.txt")
    lines = []
    for s in stills:
        p = str(s.resolve()).replace("\\", "/")
        lines.append(f"file '{p}'")
        lines.append(f"duration {per_scene:.4f}")
    # Repeat final still once (concat demuxer requirement) with explicit output -t cap below.
    last = str(stills[-1].resolve()).replace("\\", "/")
    lines.append(f"file '{last}'")
    concat_file.write_text("\n".join(lines), encoding="utf-8")

    base_filter = (
        "scale=2200:1238:force_original_aspect_ratio=increase,"
        "crop=2200:1238,"
    )
    if ken_burns_enabled:
        # Time-based expressions reset naturally at each still's hold interval.
        # The optional emphasis is only two pixels and occurs briefly every three minutes.
        shake = "+if(lt(mod(it,180),0.7),sin(it*20)*2,0)" if light_shake_enabled else ""
        motion_filter = (
            f"zoompan=z='1.02+0.055*(0.5-0.5*cos(2*PI*mod(it,{per_scene:.4f})/{per_scene:.4f}))':"
            f"x='(iw-iw/zoom)/2+sin(it/7)*8{shake}':"
            f"y='(ih-ih/zoom)/2+cos(it/9)*5{shake}':d=1:s=1920x1080:fps={fps},"
        )
    else:
        motion_filter = "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,"
        base_filter = ""
    cmd_v = [
        "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
        "-f", "concat", "-safe", "0", "-i", str(concat_file),
        "-i", str(final_audio),
        "-vf",
        base_filter + motion_filter + "format=yuv420p,fps=" + str(fps),
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "26", "-g", "300",
        "-c:a", "aac", "-b:a", "192k",
        "-movflags", "+faststart",
        "-t", f"{narr_sec:.3f}",
        str(out_path),
    ]
    r2 = subprocess.run(cmd_v, capture_output=True, text=True)
    concat_file.unlink(missing_ok=True)
    mix_path.unlink(missing_ok=True)
    final_audio.unlink(missing_ok=True)
    if r2.returncode != 0:
        raise LFRenderError(f"ffmpeg compose failed: {r2.stderr[-500:]}")
    return out_path


# ─────────────────────────────────────────────────────────────────────────────
# Sleep-doc orchestrator (HR pipeline_kind="sleep_doc")
# ─────────────────────────────────────────────────────────────────────────────

async def run_sleep_doc_pipeline(
    job_id: str,
    channel: dict,
    outline: dict,
    *,
    scenes_per_chapter: int = 30,
    wpm: int = 120,
) -> None:
    """End-to-end HR sleep-doc render. Each phase updates state.json + the
    in-memory status registry so the frontend's poll loop can report
    progress accurately.

    All per-fal-call work runs in a thread-pool executor (sync httpx); we
    only touch asyncio at the phase boundaries so we don't block the
    FastAPI event loop.
    """
    job_dir = _ensure_job_dir(job_id)
    state = load_state(job_id) or {}
    state.update({
        "job_id": job_id,
        "channel_key": channel.get("key"),
        "channel_label": channel.get("label"),
        "pipeline_kind": "sleep_doc",
        "outline": outline,
        "phase": "starting",
        "percent": 0,
        "started_at": time.time(),
        "scenes_per_chapter": scenes_per_chapter,
        "wpm": wpm,
    })
    save_state(job_id, state)
    update_status(job_id, phase="starting", percent=0)

    loop = asyncio.get_running_loop()

    # ── Phase 1 — chapters (Grok per-chapter expansion) ────────────────────
    update_status(job_id, phase="chapters", percent=2)
    state["phase"] = "chapters"
    save_state(job_id, state)

    # Lazy import — keeps pipeline.py importable on machines without GrokClient.
    from skeleton_ai.scripting_grok import GrokClient
    grok = GrokClient()

    chapters_path = _chapters_path(job_id)
    if chapters_path.exists():
        try:
            chapters_data = json.loads(chapters_path.read_text(encoding="utf-8"))
            chapters_done = list(chapters_data.get("chapters") or [])
        except Exception:
            chapters_done = []
    else:
        chapters_done = []

    chapter_count = len(outline.get("chapters") or [])
    done_indices = {int(c.get("chapter_index", -1)) for c in chapters_done}

    for ch_idx in range(chapter_count):
        if ch_idx in done_indices:
            continue
        result = await loop.run_in_executor(
            None,
            lambda i=ch_idx: _gen_chapter(
                grok,
                channel=channel,
                outline=outline,
                chapter_index=i,
                chapter_count=chapter_count,
                scenes_per_chapter=scenes_per_chapter,
                wpm=wpm,
            ),
        )
        chapters_done.append(result)
        # Persist after each chapter so resume works after crash.
        chapters_data = {"outline_title": outline.get("title", ""), "chapters": sorted(chapters_done, key=lambda c: int(c.get("chapter_index", 0)))}
        chapters_path.write_text(json.dumps(chapters_data, indent=2, ensure_ascii=True), encoding="utf-8")
        # 0-15% range for chapter phase (lots of subsequent work).
        pct = 2 + int(13 * (ch_idx + 1) / max(1, chapter_count))
        update_status(
            job_id,
            phase="chapters",
            percent=pct,
            chapter_done=ch_idx + 1,
            chapter_total=chapter_count,
        )
        state["percent"] = pct
        save_state(job_id, state)

    chapters_data = json.loads(chapters_path.read_text(encoding="utf-8"))
    chapters = chapters_data["chapters"]

    # ── Phase 2 — scene image gen ──────────────────────────────────────────
    update_status(job_id, phase="scenes", percent=15)
    state["phase"] = "scenes"
    save_state(job_id, state)

    stills_dir = job_dir / "stills"
    image_model = channel.get("image_model_default", "ernie")

    def _on_scene_progress(done: int, total: int) -> None:
        # Scenes phase = 15-45% (the longest phase by wall time).
        pct = 15 + int(30 * done / max(1, total))
        update_status(
            job_id, phase="scenes", percent=pct,
            scene_done=done, scene_total=total,
        )

    # A long-form job begins with one proof still by default. Planning all
    # chapters is free of image spend; rendering the gallery is not. Keep the
    # complete chapter plan on disk so approval can expand from the exact same
    # scene-zero reference rather than starting a second, drifting job.
    proof_only = bool(outline.get("visual_proof_only"))
    scene_chapters = chapters
    if proof_only:
        first = dict(chapters[0])
        first["scene_prompts"] = list(first.get("scene_prompts") or [])[:1]
        scene_chapters = [first]
    stills = await loop.run_in_executor(
        None,
        lambda: _gen_scenes_batch(
            scene_chapters, stills_dir, image_model,
            job_id=job_id,
            on_progress=_on_scene_progress,
            concurrency=4,
            skeleton_style=_outline_uses_skeleton(outline),
            topic=str(outline.get("title") or outline.get("topic") or ""),
            locked_outfit=str(
                outline.get("locked_outfit")
                or "simple dark turtleneck and dark trousers"
            ),
        ),
    )
    if not stills:
        raise LFRenderError("scene gen produced no stills")
    state["scenes_generated"] = len(stills)
    state["visual_proof_only"] = proof_only
    state["proof_scene_approved"] = False
    state["percent"] = 45
    save_state(job_id, state)

    # PR #127 PER-SCENE GATE: pause here. Phase 3+ run via finalize_sleep_doc()
    # called from POST /api/long-form/jobs/{id}/finalize after Casey reviews +
    # approves the still gallery. The next ~$48-58 of fal spend (narration +
    # ambient + compose) only fires once he OKs.
    state["phase"] = "awaiting_approval"
    state["percent"] = 45
    save_state(job_id, state)
    update_status(job_id, phase="awaiting_approval", percent=45)
    return


async def finalize_sleep_doc_pipeline(job_id: str) -> None:
    """Continue a paused sleep_doc render through narration → ambient →
    thumbnails → compose. Loads channel + outline + chapters from disk so
    the pipeline survives process restart between approval and finalize."""
    state = load_state(job_id)
    if not state:
        raise LFRenderError(f"no state for job {job_id}")
    # Allowed: awaiting_approval (normal), failed (rerun), AND any of the
    # finalize phases themselves so a stalled / restarted finalize can pick
    # up where it left off (PR #128 — fal mmaudio-v2 422'd ambient calls
    # which left jobs stuck mid-finalize).
    if state.get("phase") not in (
        "awaiting_approval", "narration", "ambient", "thumbnails",
        "compose", "failed", "cancelled",
    ):
        raise LFRenderError(
            f"job {job_id} is in phase {state.get('phase')!r}; "
            "finalize requires awaiting_approval (or resume from a stalled "
            "finalize / failure)"
        )

    # Re-hydrate channel from registry by key.
    from long_form.prompts.channels import get_channel
    channel = dict(get_channel(state["channel_key"]))
    outline = state.get("outline") or {}
    style_lock = str(outline.get("render_style_lock") or "").strip()
    if style_lock:
        channel["visual_style"] = f"{style_lock} {channel.get('visual_style') or ''}".strip()
        channel["thumbnail_style_prompt"] = (
            f"{style_lock} {channel.get('thumbnail_style_prompt') or ''}"
        ).strip()

    job_dir = _ensure_job_dir(job_id)
    chapters_path = _chapters_path(job_id)
    if not chapters_path.exists():
        raise LFRenderError(f"chapters.json missing for job {job_id} — cannot finalize")
    chapters_data = json.loads(chapters_path.read_text(encoding="utf-8"))
    chapters = chapters_data["chapters"]

    loop = asyncio.get_running_loop()
    audio_dir = job_dir / "audio"
    audio_dir.mkdir(parents=True, exist_ok=True)

    # Load the (possibly regenerated) stills list. Sorted by scene index so
    # the slideshow concat preserves narrative order.
    stills_dir = job_dir / "stills"
    stills = _list_scenes_sorted(stills_dir)
    if not stills:
        raise LFRenderError("no stills present at finalize time — re-run the stills phase")

    # ── Phase 3 — narration (fal MiniMax per chapter, then concat) ────────
    update_status(job_id, phase="narration", percent=46)
    state["phase"] = "narration"
    save_state(job_id, state)

    chapter_mp3s: list[Path] = []
    voice_provider = str(channel.get("voice_provider_default") or "xai").strip().lower()
    voice_id = channel.get("voice_id_default") or ("rex" if voice_provider == "xai" else "English_Trustworthy_Man")

    for i, ch in enumerate(chapters):
        out = audio_dir / f"chapter_{int(ch['chapter_index']):02d}.mp3"
        if not (out.exists() and out.stat().st_size > 4096):
            generator = _gen_xai_chapter if voice_provider == "xai" else _gen_minimax_chapter
            await loop.run_in_executor(
                None,
                lambda c=ch, o=out, g=generator: g(c["narration"], o, voice_id=voice_id),
            )
        chapter_mp3s.append(out)
        # Narration phase = 46-78%
        pct = 46 + int(32 * (i + 1) / max(1, len(chapters)))
        state["phase"] = "narration"
        state["narration_done"] = i + 1
        state["narration_total"] = len(chapters)
        state["percent"] = pct
        save_state(job_id, state)
        update_status(
            job_id, phase="narration", percent=pct,
            narration_done=i + 1, narration_total=len(chapters),
            detail=f"Voiceover — chapter {i + 1}/{len(chapters)}",
        )

    narration_full = audio_dir / "narration.mp3"
    if not (narration_full.exists() and narration_full.stat().st_size > 8192):
        await loop.run_in_executor(None, lambda: _ffmpeg_concat_audio(chapter_mp3s, narration_full))
    state["narration_path"] = str(narration_full.relative_to(LF_OUTPUT_ROOT))
    state["narration_duration_sec"] = _ffprobe_dur(narration_full)
    state["percent"] = 78
    save_state(job_id, state)

    # ── Phase 4 — ambient bed (1 mmaudio call, ffmpeg tiles at compose) ───
    update_status(job_id, phase="ambient", percent=80)
    state["phase"] = "ambient"
    save_state(job_id, state)
    ambient = audio_dir / "ambient.mp3"
    bgm_choice = resolve_background_music(outline, channel)
    outline["background_music"] = bgm_choice
    state["outline"] = outline
    save_state(job_id, state)
    ambient_prompt = resolve_ambient_prompt(
        bgm_choice=bgm_choice, channel=channel, outline=outline
    )
    sound_brief = str(outline.get("sound_design_brief") or channel.get("sound_design") or "").strip()
    await loop.run_in_executor(None, lambda: _gen_ambient(ambient, prompt=ambient_prompt))
    state["sound_design"] = {
        "sfx_enabled": bool(outline.get("sfx_enabled", True)),
        "background_music": bgm_choice,
        "sound_design_brief": sound_brief,
        "ambient_prompt": ambient_prompt,
        "ambient_path": str(ambient.relative_to(LF_OUTPUT_ROOT)),
    }
    state["percent"] = 82
    save_state(job_id, state)

    # ── Phase 5 — thumbnails ──────────────────────────────────────────────
    update_status(job_id, phase="thumbnails", percent=83)
    state["phase"] = "thumbnails"
    save_state(job_id, state)
    thumbs_dir = job_dir / "thumbnails"
    thumbs = await loop.run_in_executor(
        None, lambda: _gen_thumbnails(channel, outline, thumbs_dir, count=3)
    )
    state["thumbnails_generated"] = len(thumbs)
    state["percent"] = 86
    save_state(job_id, state)

    # ── Phase 6 — compose ─────────────────────────────────────────────────
    update_status(job_id, phase="compose", percent=87)
    state["phase"] = "compose"
    save_state(job_id, state)

    title_slug = _slugify(outline.get("title", "longform"))
    out_mp4 = _final_mp4_path(job_id, title_slug)
    fps = int(channel.get("fps") or outline.get("fps") or 30)
    ken_burns, light_shake = resolve_ken_burns(outline, channel)
    await loop.run_in_executor(
        None,
        lambda: _compose_slideshow(
            stills,
            narration_full,
            ambient,
            out_mp4,
            fps=fps,
            ken_burns_enabled=ken_burns,
            light_shake_enabled=light_shake,
            job_id=job_id,
        ),
    )

    state["mp4_path"] = str(out_mp4.relative_to(LF_OUTPUT_ROOT))
    state["mp4_duration_sec"] = _ffprobe_dur(out_mp4)
    state["mp4_size_bytes"] = out_mp4.stat().st_size
    state["phase"] = "done"
    state["percent"] = 100
    state["finished_at"] = time.time()
    save_state(job_id, state)
    update_status(job_id, phase="done", percent=100)
    try:
        from studio_agent import jobs as agent_jobs

        agent_jobs._build_longform_package(job_id)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Sub-pipeline registry + outer task wrapper
# ─────────────────────────────────────────────────────────────────────────────

# Mapping from channel.pipeline_kind → async runner. Registered:
#   'sleep_doc' (HR — PR #120) → run_sleep_doc_pipeline (stills phase only post-PR #127)
#   'v5_episode' (EM/Lacuna/PB Live/Hidden Cortex — PR #123)
# After PR #127, runners stop after the stills phase and set
# state.phase='awaiting_approval'. The finalize phase runs via
# FINALIZE_PIPELINES[kind] kicked from POST /jobs/{id}/finalize.
SUB_PIPELINES: dict[str, Callable[..., Awaitable[None]]] = {
    "sleep_doc": run_sleep_doc_pipeline,
}

FINALIZE_PIPELINES: dict[str, Callable[..., Awaitable[None]]] = {
    "sleep_doc": finalize_sleep_doc_pipeline,
}

# Per-pipeline still-regeneration functions for the per-scene approval gate.
# Each takes (job_id, scene_idx, new_prompt: str | None) and synchronously
# regenerates that one still (typically 5-15s).
REGENERATE_FUNCTIONS: dict[str, Callable[..., Path]] = {
    "sleep_doc": None,   # set below
    "v5_episode": None,  # set below
}


def regenerate_sleep_doc_still(job_id: str, scene_idx: int,
                               new_prompt: str | None = None) -> Path:
    """Regenerate one sleep_doc still. If new_prompt is provided, persist it
    back to chapters.json so future re-renders use it. Returns the new still
    path."""
    state = load_state(job_id)
    if not state:
        raise LFRenderError(f"no state for job {job_id}")
    chapters_path = _chapters_path(job_id)
    if not chapters_path.exists():
        raise LFRenderError("chapters.json missing — cannot regenerate")
    chapters_data = json.loads(chapters_path.read_text(encoding="utf-8"))
    chapters = chapters_data["chapters"]
    scenes_per_chapter = int(state.get("scenes_per_chapter", 30))
    ch_idx = scene_idx // scenes_per_chapter
    local_idx = scene_idx % scenes_per_chapter
    if ch_idx >= len(chapters):
        raise LFRenderError(f"scene_idx {scene_idx} out of range (ch {ch_idx})")
    ch = chapters[ch_idx]
    prompts = ch.get("scene_prompts") or []
    if local_idx >= len(prompts):
        raise LFRenderError(f"scene_idx {scene_idx} out of range (local {local_idx})")
    prompt = (new_prompt or prompts[local_idx]).strip()
    if not prompt:
        raise LFRenderError("prompt cannot be empty")
    job_dir = _job_dir(job_id)
    out = job_dir / "stills" / f"scene_{scene_idx:04d}.png"
    from long_form.prompts.channels import get_channel
    channel = get_channel(state["channel_key"])
    outline = state.get("outline") or {}
    image_model = str(
        outline.get("image_model_id") or channel.get("image_model_default") or "ernie"
    )

    def _dispatch(model: str, candidate: Path, route_guard: Callable[[], bool]) -> Path:
        if _outline_uses_skeleton(outline):
            return _gen_skeleton_longform_scene(
                prompt,
                candidate,
                topic=str(outline.get("title") or outline.get("topic") or ""),
                locked_outfit=str(
                    outline.get("locked_outfit")
                    or "simple dark turtleneck and dark trousers"
                ),
                image_model_id=model,
                route_guard=route_guard,
            )
        return _gen_scene_image(prompt, candidate, image_model=model)

    committed, _receipt = _dispatch_longform_media_revision_aware(
        job_id,
        stage="image",
        scene_index=scene_idx,
        destination=out,
        dispatch=_dispatch,
        fallback_model=image_model,
    )
    if new_prompt and new_prompt != prompts[local_idx]:
        chapters[ch_idx]["scene_prompts"][local_idx] = new_prompt
        chapters_data["chapters"] = chapters
        tmp = chapters_path.with_suffix(chapters_path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(chapters_data, indent=2, ensure_ascii=True), encoding="utf-8"
        )
        tmp.replace(chapters_path)
    return committed


REGENERATE_FUNCTIONS["sleep_doc"] = regenerate_sleep_doc_still


def _register_v5_episode() -> None:
    """Lazy-register v5_episode so importing pipeline.py doesn't pull in
    the EL/i2v dependency chain at module load time. v5_pipeline.py imports
    helpers from pipeline.py, so this back-edge has to be deferred."""
    try:
        from long_form.v5_pipeline import (
            run_v5_episode_pipeline,
            finalize_v5_episode_pipeline,
            regenerate_v5_still,
        )
        SUB_PIPELINES["v5_episode"] = run_v5_episode_pipeline
        FINALIZE_PIPELINES["v5_episode"] = finalize_v5_episode_pipeline
        REGENERATE_FUNCTIONS["v5_episode"] = regenerate_v5_still
    except Exception as exc:  # noqa: BLE001
        # Don't crash the API on import failure — sleep_doc still works
        # standalone, and EM renders will surface a clear error via
        # _run_render's "pipeline_kind not registered" path.
        import logging
        logging.getLogger(__name__).warning(
            "v5_episode pipeline registration deferred: %s", exc
        )


_register_v5_episode()


async def _run_render(job_id: str, channel: dict, outline: dict) -> None:
    """Outer wrapper that catches errors + marks the job failed on disk +
    in memory. Sub-pipelines never raise to here in normal flow — they
    update state along the way."""
    pipeline_kind = (channel.get("pipeline_kind") or "sleep_doc").strip()
    runner = SUB_PIPELINES.get(pipeline_kind)
    if runner is None:
        st = load_state(job_id) or {}
        st.update({"phase": "failed", "error": f"pipeline_kind={pipeline_kind!r} not registered"})
        save_state(job_id, st)
        update_status(job_id, phase="failed", error=st["error"])
        return
    try:
        if pipeline_kind == "sleep_doc":
            await runner(
                job_id,
                channel,
                outline,
                scenes_per_chapter=int(
                    outline.get("scenes_per_chapter")
                    or channel.get("scenes_per_chapter")
                    or 20
                ),
                wpm=int(outline.get("wpm") or channel.get("wpm") or 120),
            )
        else:
            await runner(job_id, channel, outline)
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        st = load_state(job_id) or {}
        st.update({"phase": "failed", "error": msg, "failed_at": time.time()})
        save_state(job_id, st)
        update_status(job_id, phase="failed", error=msg)


def start_render(
    channel: dict,
    outline: dict,
    *,
    requested_job_id: str | None = None,
) -> str:
    """Public API — kick the stills phase of a render. Returns job_id
    immediately; pipeline runs in the asyncio background and pauses at
    state.phase='awaiting_approval' after generating all stills. Casey
    reviews + regenerates as needed, then POST /finalize resumes the rest.

    Caller polls /jobs/{id}/status — phase=='awaiting_approval' is the gate."""
    if not isinstance(outline, dict) or not outline.get("chapters"):
        raise LFRenderError("outline must include a non-empty 'chapters' list")
    requested = str(requested_job_id or "").strip()
    if requested and len(requested) <= 48 and requested.replace("_", "").isalnum():
        job_id = requested
    else:
        job_id = _new_job_id()
    _ensure_job_dir(job_id)
    state = {
        "job_id": job_id,
        "user_id": str(outline.get("user_id") or "").strip(),
        "channel_key": channel.get("key"),
        "channel_label": channel.get("label"),
        "pipeline_kind": channel.get("pipeline_kind") or "sleep_doc",
        "outline": outline,
        "phase": "queued",
        "percent": 0,
        "created_at": time.time(),
    }
    save_state(job_id, state)
    update_status(job_id, phase="queued", percent=0, started_at=time.time())

    _spawn_lf_background_coro(_run_render(job_id, channel, outline), job_id)
    return job_id


async def _expand_visual_proof(job_id: str) -> None:
    """Render the remaining gallery only after the user accepts scene zero."""
    state = load_state(job_id) or {}
    outline = state.get("outline") if isinstance(state.get("outline"), dict) else {}
    if not outline:
        raise LFRenderError(f"no outline for job {job_id}")
    from long_form.prompts.channels import get_channel
    channel = dict(get_channel(str(state.get("channel_key") or "")))
    if str(outline.get("image_model_id") or "").strip():
        channel["image_model_default"] = str(outline["image_model_id"]).strip()
    style_lock = str(outline.get("render_style_lock") or "").strip()
    if style_lock:
        channel["visual_style"] = f"{style_lock} {channel.get('visual_style') or ''}".strip()
    chapters_path = _chapters_path(job_id)
    if not chapters_path.is_file():
        raise LFRenderError("chapters.json missing; cannot expand proof")
    chapters = list((json.loads(chapters_path.read_text(encoding="utf-8")) or {}).get("chapters") or [])
    if not chapters:
        raise LFRenderError("no chapters available to expand proof")
    stills_dir = _ensure_job_dir(job_id) / "stills"
    loop = asyncio.get_running_loop()
    existing = len(_list_scenes_sorted(stills_dir))
    resume_pct = 15
    if existing > 1:
        # Keep UI honest when resuming after a redeploy mid-gallery.
        resume_pct = min(44, 15 + int(30 * existing / max(existing + 50, 1)))
    state.update({
        "phase": "scenes",
        "proof_scene_approved": True,
        "percent": resume_pct,
        "visual_proof_only": False,
        "scenes_generated": existing,
    })
    outline["visual_proof_only"] = False
    state["outline"] = outline
    save_state(job_id, state)
    update_status(
        job_id, phase="scenes", percent=resume_pct,
        scene_done=existing,
        detail=f"Resuming gallery — {existing} scenes on disk",
    )

    def _on_scene_progress(done: int, total: int) -> None:
        pct = 15 + int(30 * done / max(1, total))
        update_status(
            job_id, phase="scenes", percent=pct,
            scene_done=done, scene_total=total,
            detail=f"Building gallery — {done}/{total} scenes",
        )

    stills = await loop.run_in_executor(
        None,
        lambda: _gen_scenes_batch(
            chapters, stills_dir, channel.get("image_model_default", "ernie"), job_id=job_id, concurrency=4,
            on_progress=_on_scene_progress,
            skeleton_style=_outline_uses_skeleton(outline),
            topic=str(outline.get("title") or outline.get("topic") or ""),
            locked_outfit=str(outline.get("locked_outfit") or "simple dark turtleneck and dark trousers"),
        ),
    )
    if not stills:
        raise LFRenderError("proof expansion produced no stills")
    state.update({"phase": "awaiting_approval", "percent": 45, "scenes_generated": len(stills), "visual_proof_only": False})
    save_state(job_id, state)
    update_status(job_id, phase="awaiting_approval", percent=45)


def expand_visual_proof(job_id: str) -> None:
    state = load_state(job_id) or {}
    phase = str(state.get("phase") or "")
    if phase == "scenes" and state.get("proof_scene_approved"):
        # Gallery expansion already accepted — keep polling progress.
        entry = _lf_jobs_status.get(job_id) or {}
        task = entry.get("_task")
        if task is None or getattr(task, "done", lambda: True)():
            _spawn_lf_background_coro(_expand_visual_proof(job_id), job_id)
        return
    if phase == "awaiting_approval" and bool(state.get("visual_proof_only")):
        _spawn_lf_background_coro(_expand_visual_proof(job_id), job_id)
        return
    raise LFRenderError("job is not awaiting approval of a one-scene long-form proof")


async def _run_finalize(job_id: str) -> None:
    """Outer wrapper for the finalize phase. Loads state to determine the
    pipeline_kind, then calls the right finalize runner."""
    state = load_state(job_id)
    if not state:
        update_status(job_id, phase="failed", error="no state for job")
        return
    pipeline_kind = (state.get("pipeline_kind") or "sleep_doc").strip()
    runner = FINALIZE_PIPELINES.get(pipeline_kind)
    if runner is None:
        st = state
        st.update({"phase": "failed", "error": f"finalize for pipeline_kind={pipeline_kind!r} not registered"})
        save_state(job_id, st)
        update_status(job_id, phase="failed", error=st["error"])
        return
    try:
        await runner(job_id)
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        st = load_state(job_id) or {}
        st.update({"phase": "failed", "error": msg, "failed_at": time.time()})
        save_state(job_id, st)
        update_status(job_id, phase="failed", error=msg)


def start_finalize(job_id: str) -> None:
    """Public API — resume a job paused at awaiting_approval (normal flow)
    OR stalled mid-finalize (scene_assembly / narration / ambient / etc)
    OR cancelled / failed. The inner finalize runners handle each phase
    idempotently; start_finalize just kicks the asyncio task again so a
    previously-killed task gets re-run.

    Phase allowlist mirrors finalize_v5_episode_pipeline +
    finalize_sleep_doc_pipeline (PR #128 expansion). Was previously only
    accepting awaiting_approval/failed and rejecting Resume on a stalled
    scene_assembly with HTTP 400 Bad Request."""
    state = load_state(job_id)
    if not state:
        raise LFRenderError(f"no such job {job_id}")
    allowed = (
        "awaiting_approval", "failed", "cancelled",
        # Mid-finalize phases — task may have died (Fly redeploy, machine
        # restart, OOM); re-kicking is safe because every per-helper is
        # idempotent (file-exists checks reuse already-rendered output).
        "scene_assembly", "narration", "ambient", "thumbnails", "compose",
        "i2v", "vo", "sfx", "finalizing",
    )
    if state.get("phase") not in allowed:
        raise LFRenderError(
            f"job {job_id} is in phase {state.get('phase')!r}; "
            f"finalize requires one of {sorted(allowed)}"
        )
    update_status(job_id, phase="finalizing", percent=int(state.get("percent") or 73))
    _spawn_lf_background_coro(_run_finalize(job_id), job_id)


def cancel_render(job_id: str) -> dict:
    """Cancel an in-flight render. Sets state.phase='cancelled' and tries
    to cancel the asyncio task. Cooperative — the per-scene loops check
    fal calls one at a time, so cancellation lands at the next scene
    boundary rather than mid-scene. Already-completed scenes keep their
    artifacts on disk so a future Resume / re-finalize can pick them up.

    Returns {phase, was_cancelled} so the caller knows whether the task
    was running or already terminated."""
    state = load_state(job_id)
    if not state:
        raise LFRenderError(f"no such job {job_id}")
    entry = _lf_jobs_status.get(job_id) or {}
    task = entry.get("_task")
    was_running = isinstance(task, asyncio.Task) and not task.done()
    if was_running:
        task.cancel()
    state["phase"] = "cancelled"
    state["cancelled_at"] = time.time()
    save_state(job_id, state)
    update_status(job_id, phase="cancelled", error="cancelled by user")
    return {"phase": "cancelled", "was_running": was_running}


def regenerate_thumbnail(
    job_id: str,
    idx: int,
    custom_prompt: str | None = None,
) -> Path:
    """Regenerate a single thumbnail (idx 1-based) for the panel's
    'Regenerate' button. Re-runs seedream with either a fresh variant
    hint (when custom_prompt is None) or with the user-supplied prompt
    verbatim (overrides the channel thumbnail_style + variant hint).

    Returns the new file path. Caller should serve with cache-bust
    ?v=<mtime> so the <img> reloads."""
    import random as _random

    state = load_state(job_id)
    if not state:
        raise LFRenderError(f"no such job {job_id}")
    if idx < 1 or idx > 12:
        raise LFRenderError(f"thumbnail idx {idx} out of range (1..12)")

    from long_form.prompts.channels import get_channel
    channel = get_channel(state["channel_key"])
    outline = state.get("outline") or {}

    thumbs_dir = _job_dir(job_id) / "thumbnails"
    thumbs_dir.mkdir(parents=True, exist_ok=True)
    out = thumbs_dir / f"thumb_{idx}.png"
    if out.exists():
        out.unlink()

    base_prompt = (
        channel.get("thumbnail_style_prompt")
        or channel.get("visual_style")
        or ""
    ).strip()
    title = (outline.get("title") or "").strip()

    # A user revision ("make it darker", "less text") must stay on-brand, so
    # the channel's real covers anchor the style even for custom prompts.
    refs = channel_reference_thumbnails(channel)

    if custom_prompt and custom_prompt.strip():
        full_prompt = custom_prompt.strip()
        ref_prompt = full_prompt
    else:
        # Wider variant pool than the original 3 in _gen_thumbnails so
        # regenerating a bad tile actually gives Casey something different.
        variants = [
            "Medium portrait composition, subject center, mid-shot, dramatic lighting.",
            "Low-angle dramatic composition, subject silhouetted against backlight.",
            "Tight close-up, subject filling frame, shallow depth of field.",
            "Over-the-shoulder composition, subject's back to camera, environmental context.",
            "Rule-of-thirds, subject left-third, large title text right.",
            "Subject center-frame heroic pose, cinematic 35mm composition.",
            "Profile shot, subject side-on, dramatic chiaroscuro lighting.",
        ]
        variant = _random.choice(variants)
        full_prompt = (
            f"{base_prompt}\n\nDocumentary title context: {title}.\n\n"
            f"Composition variant: {variant}"
        )
        ref_prompt = (
            f"Short on-image title (render exactly this text): {thumbnail_display_title(title)}.\n"
            f"Full video topic for the scene: {title}.\n"
            f"Composition variant: {variant}"
        )

    if not (refs and _thumbnail_via_channel_references(ref_prompt, refs, out)):
        data = _fal_post(
            SEEDREAM_URL,
            {"prompt": full_prompt, "image_size": {"width": 1280, "height": 720}},
            timeout_s=180,
        )
        images = data.get("images") or []
        if not images:
            raise LFRenderError(f"thumbnail gen returned no images: {data}")
        img_url = images[0].get("url", "")
        if not img_url:
            raise LFRenderError(f"thumbnail gen response missing url: {data}")
        _download(img_url, out, timeout_s=120)

    # Update state.thumbnails_generated so /jobs/{id} reports the right count
    # if this regeneration adds a new index beyond what was originally generated.
    existing = int(state.get("thumbnails_generated", 0) or 0)
    if idx > existing:
        state["thumbnails_generated"] = idx
        save_state(job_id, state)

    return out


def regenerate_still(job_id: str, scene_idx: int,
                     new_prompt: str | None = None) -> Path:
    """Public API — regenerate one still. Synchronous (typical call ~5-15s
    of fal seedream/ernie time). Returns the new still path so the caller
    can serve it back via the capability-token /still/{idx} endpoint with
    a cache-bust hint."""
    state = load_state(job_id)
    if not state:
        raise LFRenderError(f"no such job {job_id}")
    pipeline_kind = (state.get("pipeline_kind") or "sleep_doc").strip()
    fn = REGENERATE_FUNCTIONS.get(pipeline_kind)
    if fn is None:
        raise LFRenderError(f"regenerate for pipeline_kind={pipeline_kind!r} not registered")
    return fn(job_id, scene_idx, new_prompt)


# ─────────────────────────────────────────────────────────────────────────────
# Media path helpers (used by the router for capability-token serving)
# ─────────────────────────────────────────────────────────────────────────────

def job_mp4_path(job_id: str) -> Path | None:
    """Return the canonical Final_<slug>_<jobid>.mp4 path, or None if absent.
    Excludes any *_mix.mp3 / *_lk.mp3 intermediates (lesson #8 from
    SESSION_2026-05-08 — ZT MP4 endpoint served the silent intermediate)."""
    d = _job_dir(job_id)
    if not d.exists():
        return None
    state = load_state(job_id) or {}
    rel = state.get("mp4_path") or ""
    if rel:
        candidate = LF_OUTPUT_ROOT / rel
        if candidate.exists() and candidate.suffix.lower() == ".mp4":
            return candidate
    # Fallback: scan for LongForm_*<job_id>.mp4 (excludes silent intermediates).
    for f in d.glob(f"LongForm_*_{job_id}.mp4"):
        if f.is_file():
            return f
    return None


def job_thumbnail_path(job_id: str, idx: int) -> Path | None:
    p = _job_dir(job_id) / "thumbnails" / f"thumb_{int(idx)}.png"
    return p if p.exists() else None


def job_still_path(job_id: str, scene_idx: int) -> Path | None:
    p = _job_dir(job_id) / "stills" / f"scene_{int(scene_idx):04d}.png"
    return p if p.exists() else None
