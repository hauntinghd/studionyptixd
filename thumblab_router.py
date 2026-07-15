"""Authenticated, owner-scoped ThumbLab production routes.

This module restores the API contract consumed by ``ThumbnailPanel.tsx``
without reviving the deleted RunPod/SSH thumbnail-training surface.  Provider
work is preceded by a durable idempotency claim and a unified-credit hold;
failed jobs release that hold and successful jobs commit it exactly once.
"""
from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import re
import time
import uuid
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse
from PIL import Image, ImageDraw, ImageFont, ImageOps, UnidentifiedImageError

from backend_models import ThumbnailGenerateRequest
from backend_settings import (
    FAL_AI_KEY,
    PIKZELS_API_KEY,
    PIKZELS_THUMBNAIL_MODEL,
    THUMBNAIL_DIR,
)
from studio_agent.command_execution import ExecutionReceipt, FileExecutionLedger
from studio_agent.product_reference import ProductReferenceError, _fetch_public_resource
from upload_limits import MAX_REFERENCE_IMAGE_BYTES, MAX_THUMBNAIL_VIDEO_BYTES, UploadTooLargeError, write_upload_limited
from video_pipeline import CREATIVE_IMAGE_MODEL_MAP


log = logging.getLogger("nyptid-studio")

THUMBNAIL_MODEL_CREDITS: dict[str, int] = {
    "ernie_image": 3,
    "seedream45": 5,
    "imagen4_fast": 4,
    "recraft_v4": 5,
    "grok_imagine": 6,
}
THUMBNAIL_DEFAULT_CREDITS = 5
THUMBNAIL_MAX_REFERENCE_URLS = 4
THUMBNAIL_MAX_IMAGE_PIXELS = 40_000_000
THUMBNAIL_ALLOWED_VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm", ".m4v", ".avi"}
THUMBNAIL_MODEL_ORDER = ("ernie_image", "seedream45", "imagen4_fast", "recraft_v4", "grok_imagine")
SEEDREAM_THUMB_EDIT_URL = "https://fal.run/fal-ai/bytedance/seedream/v4.5/edit"
_UPLOAD_ID_RE = re.compile(r"^vid_[0-9a-f]{32}$")
_THUMBLAB_OUTPUT_FILE_RE = re.compile(r"^thumb_[0-9a-f]{32}\.png$")
_SAFE_OUTPUT_FILE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}\.png$")

THUMBNAIL_ANALYSIS_PROMPT = """You are an elite YouTube thumbnail art director.
Return one valid JSON object with keys prompt, negative_prompt, title_text,
style_notes, and patterns_detected. Design for a 1920x1080, mobile-readable
thumbnail with one clear subject, strong contrast, and no copied branding."""

THUMBNAIL_VISION_PROMPT = """Analyze this reference thumbnail for reusable
packaging principles, never copied branding. Return valid JSON with composition,
color_palette, text_style, emotional_hook, patterns, do_not_copy,
generation_directive, ctr_score, and score_reason."""


@dataclass
class ThumbLabRuntime:
    require_auth: Callable[..., Any]
    jobs: dict[str, dict]
    persist_job_state: Callable[..., Any]
    fal_json_completion: Callable[..., Any]
    fal_vision_json_completion: Callable[..., Any]
    generate_image_fal_selected_model: Callable[..., Any]
    youtube_fetch_public_channel_page_videos: Callable[..., Any]
    list_connected_youtube_channels_for_user: Callable[..., Any]
    save_training_candidate: Optional[Callable[..., Any]]
    reserve_credits: Callable[..., Any]
    release_reservation: Callable[..., Any]
    commit_reservation: Callable[..., Any]
    ledger: FileExecutionLedger
    storage_root: Path
    fal_ai_key: str
    pikzels_api_key: str
    pikzels_thumbnail_model: str
    max_video_bytes: int
    probe_video_duration: Callable[..., Any]
    extract_frame_image: Callable[..., Any]


_DEFAULT_RUNTIME: ThumbLabRuntime | None = None


async def _maybe_await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


def _user_id(user: dict | None) -> str:
    return str((user or {}).get("id") or "").strip()


def _require_user_id(user: dict | None) -> str:
    user_id = _user_id(user)
    if not user_id:
        raise HTTPException(401, "Authentication required")
    return user_id


def _owner_segment(user: dict | None) -> str:
    """Return a collision-resistant path segment without exposing account IDs."""

    user_id = _require_user_id(user)
    return hashlib.sha256(("thumblab-owner-v1:" + user_id).encode("utf-8")).hexdigest()[:40]


def _owner_root(runtime: ThumbLabRuntime, user: dict, *, create: bool = False) -> Path:
    path = runtime.storage_root / "users" / _owner_segment(user)
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path


def _thumbnail_output_dir(runtime: ThumbLabRuntime, user: dict, *, create: bool = False) -> Path:
    path = _owner_root(runtime, user, create=create) / "generated"
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path


def _thumbnail_video_dir(runtime: ThumbLabRuntime, user: dict, *, create: bool = False) -> Path:
    path = _owner_root(runtime, user, create=create) / "video_uploads"
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path


def _thumbnail_frame_dir(runtime: ThumbLabRuntime, user: dict, *, create: bool = False) -> Path:
    path = _owner_root(runtime, user, create=create) / "frames"
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path


def _thumbnail_cache_dir(runtime: ThumbLabRuntime, user: dict, *, create: bool = False) -> Path:
    path = _owner_root(runtime, user, create=create) / "reference_cache"
    if create:
        path.mkdir(parents=True, exist_ok=True)
    return path


def _valid_upload_id(upload_id: str) -> str:
    normalized = str(upload_id or "").strip().lower()
    if not _UPLOAD_ID_RE.fullmatch(normalized):
        raise HTTPException(400, "Invalid thumbnail upload id")
    return normalized


def _thumbnail_upload_path(runtime: ThumbLabRuntime, user: dict, upload_id: str) -> Path | None:
    normalized = _valid_upload_id(upload_id)
    base = _thumbnail_video_dir(runtime, user, create=False)
    if not base.is_dir():
        return None
    matches = [p for p in base.glob(normalized + ".*") if p.is_file() and p.suffix.lower() in THUMBNAIL_ALLOWED_VIDEO_EXTENSIONS]
    return matches[0] if len(matches) == 1 else None


def _thumbnail_frame_path(runtime: ThumbLabRuntime, user: dict, upload_id: str, *, create: bool = False) -> Path:
    normalized = _valid_upload_id(upload_id)
    return _thumbnail_frame_dir(runtime, user, create=create) / f"{normalized}.jpg"


def _thumbnail_credit_cost(image_model: str = "") -> int:
    normalized = str(image_model or "").strip().lower().replace("seedream_45", "seedream45")
    return int(THUMBNAIL_MODEL_CREDITS.get(normalized, THUMBNAIL_DEFAULT_CREDITS))


def _thumbnail_model_catalog(runtime: ThumbLabRuntime) -> list[dict[str, Any]]:
    models: list[dict[str, Any]] = []
    if runtime.fal_ai_key:
        for model_id in THUMBNAIL_MODEL_ORDER:
            profile = dict(CREATIVE_IMAGE_MODEL_MAP.get(model_id) or {})
            if not str(profile.get("fal_endpoint_id") or "").strip():
                continue
            models.append(
                {
                    "id": model_id,
                    "label": str(profile.get("label") or model_id),
                    "credits": _thumbnail_credit_cost(model_id),
                }
            )
    elif runtime.pikzels_api_key:
        models.append({"id": "seedream45", "label": "Pikzels Thumbnail", "credits": 5})
    return models or [{"id": "seedream45", "label": "Seedream 4.5", "credits": 5}]


def _strict_youtube_channel_ref(raw: str) -> tuple[str, str]:
    """Normalize only YouTube channel IDs and handles; never pass arbitrary URLs."""

    value = str(raw or "").strip()
    if not value:
        return "", ""
    if re.fullmatch(r"UC[A-Za-z0-9_-]{10,}", value):
        return "", value
    if value.startswith("@") and re.fullmatch(r"@[A-Za-z0-9._-]{2,80}", value):
        return f"https://www.youtube.com/{value}", ""
    if re.fullmatch(r"[A-Za-z0-9._-]{2,80}", value):
        return f"https://www.youtube.com/@{value}", ""

    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or parsed.username or parsed.password:
        return "", ""
    host = str(parsed.hostname or "").lower().rstrip(".")
    if host not in {"youtube.com", "www.youtube.com", "m.youtube.com"}:
        return "", ""
    channel_match = re.fullmatch(r"/channel/(UC[A-Za-z0-9_-]{10,})/?", parsed.path)
    if channel_match:
        return "", channel_match.group(1)
    handle_match = re.fullmatch(r"/@([A-Za-z0-9._-]{2,80})/?", parsed.path)
    if handle_match:
        return f"https://www.youtube.com/@{handle_match.group(1)}", ""
    return "", ""


def _normalize_image_bytes(payload: bytes, destination: Path) -> None:
    if not payload:
        raise ValueError("Remote image was empty")
    try:
        with Image.open(BytesIO(payload)) as source:
            source.verify()
        with Image.open(BytesIO(payload)) as source:
            width, height = source.size
            if width <= 0 or height <= 0 or width * height > THUMBNAIL_MAX_IMAGE_PIXELS:
                raise ValueError("Remote image dimensions are not allowed")
            normalized = ImageOps.exif_transpose(source).convert("RGB")
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.suffix.lower() in {".jpg", ".jpeg"}:
                normalized.save(destination, "JPEG", quality=92, optimize=True)
            else:
                normalized.save(destination, "PNG", optimize=True)
    except (UnidentifiedImageError, Image.DecompressionBombError, OSError) as exc:
        raise ValueError("Remote resource is not a safe image") from exc


async def _download_public_image(url: str, destination: Path) -> Path:
    try:
        result = await asyncio.to_thread(
            _fetch_public_resource,
            str(url or "").strip(),
            max_bytes=MAX_REFERENCE_IMAGE_BYTES,
            accept="image/avif,image/webp,image/png,image/jpeg",
        )
    except ProductReferenceError as exc:
        raise ValueError(f"Reference thumbnail URL is not allowed: {exc}") from exc
    content_type = str((result.get("headers") or {}).get("content-type") or "").split(";", 1)[0].strip().lower()
    if not content_type.startswith("image/"):
        raise ValueError("Reference thumbnail URL did not return an image")
    _normalize_image_bytes(bytes(result.get("body") or b""), destination)
    return destination


async def _probe_video_duration_default(path: Path) -> float:
    proc = await asyncio.create_subprocess_exec(
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _stderr = await proc.communicate()
    if proc.returncode != 0:
        return 0.0
    try:
        return max(0.0, float(stdout.decode("utf-8", errors="ignore").strip() or 0))
    except ValueError:
        return 0.0


async def _extract_frame_image_default(source: Path, destination: Path, *, seek_seconds: float) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-y",
        "-ss",
        str(max(0.0, float(seek_seconds or 0.0))),
        "-i",
        str(source),
        "-frames:v",
        "1",
        "-q:v",
        "2",
        "-vf",
        "scale=1280:-2",
        str(destination),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _stdout, stderr = await proc.communicate()
    if proc.returncode != 0 or not destination.is_file() or destination.stat().st_size <= 0:
        destination.unlink(missing_ok=True)
        raise RuntimeError(f"Frame extraction failed: {stderr.decode('utf-8', errors='ignore')[-200:]}")
    try:
        with Image.open(destination) as image:
            image.verify()
    except Exception as exc:
        destination.unlink(missing_ok=True)
        raise RuntimeError("Frame extraction returned an invalid image") from exc


async def _extract_frame(runtime: ThumbLabRuntime, user: dict, upload_id: str, pct: float) -> dict[str, Any]:
    source = _thumbnail_upload_path(runtime, user, upload_id)
    if source is None or not source.is_file():
        raise HTTPException(404, "Video upload not found")
    duration = float(await _maybe_await(runtime.probe_video_duration(source)) or 0.0)
    if duration <= 0:
        raise HTTPException(422, "Uploaded video could not be decoded")
    normalized_pct = max(0.01, min(float(pct or 0.12), 0.95))
    seek = max(0.0, duration * normalized_pct)
    destination = _thumbnail_frame_path(runtime, user, upload_id, create=True)
    await _maybe_await(runtime.extract_frame_image(source, destination, seek_seconds=seek))
    return {
        "upload_id": _valid_upload_id(upload_id),
        "preview_url": f"/api/thumbnails/frame/{_valid_upload_id(upload_id)}",
        "seek_sec": round(seek, 2),
        "duration_sec": round(duration, 1),
    }


def _catalyst_packaging(channel_id: str) -> dict[str, Any]:
    target = str(channel_id or "").strip()
    if not target:
        return {}
    try:
        from long_form.catalyst_bridge import fetch_channel_snapshot, shape_catalyst_insights

        record = fetch_channel_snapshot(target) or {}
        insights = shape_catalyst_insights(record) if record else {}
        analytics = dict(record.get("analytics_snapshot") or {})
        return {
            "channel_id": target,
            "channel_title": str(record.get("title") or record.get("channel_handle") or ""),
            "packaging_learnings": list(analytics.get("packaging_learnings") or insights.get("hook_patterns") or [])[:8],
            "title_pattern_hints": list(analytics.get("title_pattern_hints") or insights.get("hook_patterns") or [])[:6],
            "top_titles": [
                str(row.get("title") or "")
                for row in list(insights.get("top_titles") or [])[:5]
                if isinstance(row, dict) and str(row.get("title") or "").strip()
            ],
        }
    except Exception:
        return {}


async def _connected_channels(runtime: ThumbLabRuntime, user: dict) -> list[dict[str, Any]]:
    payload = await _maybe_await(runtime.list_connected_youtube_channels_for_user(user, sync=False))
    return [dict(row) for row in list((payload or {}).get("channels") or []) if isinstance(row, dict)]


def _channel_id(row: dict[str, Any]) -> str:
    return str(row.get("channel_id") or row.get("id") or "").strip()


async def _authorized_catalyst_context(runtime: ThumbLabRuntime, user: dict, requested_channel_id: str) -> dict[str, Any]:
    requested = str(requested_channel_id or "").strip()
    if not requested:
        return {}
    channels = await _connected_channels(runtime, user)
    if requested not in {_channel_id(row) for row in channels}:
        raise HTTPException(403, "That YouTube channel is not connected to this account")
    return _catalyst_packaging(requested)


def _score_prompt_variant(variant: dict[str, Any], vision: dict[str, Any]) -> float:
    prompt = str(variant.get("prompt") or "").strip()
    title_words = str(variant.get("title_text") or "").split()
    score = min(30.0, len(prompt) / 8.0)
    if 2 <= len(title_words) <= 6:
        score += 18.0
    if str(variant.get("negative_prompt") or "").strip():
        score += 5.0
    if str(variant.get("style_notes") or "").strip():
        score += 7.0
    try:
        score += max(0.0, min(100.0, float(vision.get("ctr_score") or 0.0))) * 0.2
    except (TypeError, ValueError):
        pass
    return round(score, 2)


def _fallback_prompt(req: ThumbnailGenerateRequest, catalyst: dict[str, Any], vision: dict[str, Any]) -> dict[str, Any]:
    subject = str(req.description or req.video_title or "the video's central conflict").strip()
    directive = str(vision.get("generation_directive") or "").strip()
    packaging = "; ".join(str(v) for v in list(catalyst.get("packaging_learnings") or [])[:3] if str(v).strip())
    prompt = (
        f"Premium 16:9 YouTube thumbnail for {subject}. One dominant subject, one clear contradiction, "
        "high contrast, cinematic lighting, intentional negative space, mobile-readable composition."
    )
    if directive:
        prompt += f" Reference-derived art direction: {directive}."
    if packaging:
        prompt += f" Channel packaging guidance: {packaging}."
    return {
        "prompt": prompt,
        "negative_prompt": "clutter, tiny text, illegible typography, copied logos, collage, duplicate subjects, watermark",
        "title_text": "",
        "style_notes": "Deterministic ThumbLab fallback direction",
        "patterns_detected": list(vision.get("patterns") or [])[:6],
    }


async def _analyze_reference_images(
    runtime: ThumbLabRuntime,
    paths: list[Path],
    *,
    context: str,
) -> dict[str, Any]:
    if not paths:
        return {}
    ranked: list[dict[str, Any]] = []
    for index, path in enumerate(paths[:THUMBNAIL_MAX_REFERENCE_URLS]):
        try:
            row = await _maybe_await(
                runtime.fal_vision_json_completion(
                    THUMBNAIL_VISION_PROMPT,
                    f"New video context: {context}",
                    image_paths=[str(path)],
                    temperature=0.3,
                    timeout_sec=75,
                )
            )
            row = dict(row or {})
        except Exception as exc:
            log.warning("ThumbLab reference vision failed: %s", str(exc)[:180])
            row = {"error": str(exc)[:180]}
        row["source_index"] = index
        try:
            row["vision_score"] = max(0.0, min(100.0, float(row.get("ctr_score") or 0.0)))
        except (TypeError, ValueError):
            row["vision_score"] = 0.0
        ranked.append(row)
    usable = [row for row in ranked if not row.get("error")]
    if not usable:
        return {}
    usable.sort(key=lambda row: -float(row.get("vision_score") or 0.0))
    top = usable[0]
    patterns: list[str] = []
    for row in usable:
        for value in list(row.get("patterns") or []):
            normalized = str(value or "").strip()
            if normalized and normalized not in patterns:
                patterns.append(normalized)
    return {
        "composition": str(top.get("composition") or ""),
        "color_palette": str(top.get("color_palette") or ""),
        "text_style": str(top.get("text_style") or ""),
        "emotional_hook": str(top.get("emotional_hook") or ""),
        "generation_directive": str(top.get("generation_directive") or ""),
        "do_not_copy": str(top.get("do_not_copy") or ""),
        "ctr_score": top.get("ctr_score", 0),
        "score_reason": str(top.get("score_reason") or ""),
        "vision_score": float(top.get("vision_score") or 0.0),
        "patterns": patterns[:8],
        "reference_scores": [
            {
                "index": int(row.get("source_index") or 0),
                "vision_score": float(row.get("vision_score") or 0.0),
                "ctr_score": row.get("ctr_score", 0),
                "score_reason": str(row.get("score_reason") or "")[:180],
            }
            for row in ranked
        ],
    }


async def _prompt_variant(
    runtime: ThumbLabRuntime,
    req: ThumbnailGenerateRequest,
    *,
    vision: dict[str, Any],
    catalyst: dict[str, Any],
    packaging_first: bool,
) -> dict[str, Any]:
    references = "\n".join(
        f"- {str(url)[:500]}" for url in list(req.reference_thumbnail_urls or [])[:THUMBNAIL_MAX_REFERENCE_URLS]
    )
    channel_guidance = "\n".join(
        f"- {str(value)[:300]}" for value in list(catalyst.get("packaging_learnings") or [])[:6]
    )
    user_prompt = (
        f"Video title: {str(req.video_title or '')[:500]}\n"
        f"Topic and hook: {str(req.description or '')[:1600]}\n"
        f"Reference notes: {str(req.screenshot_description or '')[:1600]}\n"
        f"Reference creator: {str(req.reference_creator or '')[:300]}\n"
        f"Reference URLs (context only):\n{references}\n"
        f"Vision direction: {json.dumps(vision, ensure_ascii=False)[:3500]}\n"
        f"This account's Catalyst packaging guidance:\n{channel_guidance}\n"
        f"Packaging-first variant: {'yes' if packaging_first else 'no'}"
    )
    result = await _maybe_await(
        runtime.fal_json_completion(
            THUMBNAIL_ANALYSIS_PROMPT,
            user_prompt,
            temperature=0.82 if packaging_first else 0.62,
            timeout_sec=90,
        )
    )
    return dict(result or {})


async def _generate_prompt_ab(
    runtime: ThumbLabRuntime,
    req: ThumbnailGenerateRequest,
    *,
    vision: dict[str, Any],
    catalyst: dict[str, Any],
) -> dict[str, Any]:
    variants: list[dict[str, Any]] = []
    for packaging_first in (False, True):
        try:
            variant = await _prompt_variant(
                runtime,
                req,
                vision=vision,
                catalyst=catalyst,
                packaging_first=packaging_first,
            )
            if str(variant.get("prompt") or "").strip():
                variants.append(variant)
        except Exception as exc:
            log.warning("ThumbLab prompt variant failed: %s", str(exc)[:180])
    if not variants:
        return _fallback_prompt(req, catalyst, vision)
    if len(variants) == 1:
        winner = dict(variants[0])
        winner["ab_scoring"] = {
            "picked": "a",
            "variant_a_score": _score_prompt_variant(winner, vision),
            "variant_b_score": 0,
            "vision_score": float(vision.get("vision_score") or 0.0),
        }
        return winner
    score_a = _score_prompt_variant(variants[0], vision)
    score_b = _score_prompt_variant(variants[1], vision)
    picked = "a" if score_a >= score_b else "b"
    winner = dict(variants[0] if picked == "a" else variants[1])
    winner["ab_scoring"] = {
        "picked": picked,
        "variant_a_score": score_a,
        "variant_b_score": score_b,
        "vision_score": float(vision.get("vision_score") or 0.0),
    }
    return winner


async def _collect_reference_paths(
    runtime: ThumbLabRuntime,
    user: dict,
    *,
    job_id: str,
    reference_urls: list[str],
    frame_path: Path | None,
) -> list[Path]:
    paths: list[Path] = []
    if frame_path is not None and frame_path.is_file():
        paths.append(frame_path)
    cache_dir = _thumbnail_cache_dir(runtime, user, create=True) / job_id
    cache_dir.mkdir(parents=True, exist_ok=True)
    for index, raw_url in enumerate(list(reference_urls or [])[:THUMBNAIL_MAX_REFERENCE_URLS]):
        url = str(raw_url or "").strip()
        if not url:
            continue
        destination = cache_dir / f"reference_{index}.jpg"
        paths.append(await _download_public_image(url, destination))
    return paths[: THUMBNAIL_MAX_REFERENCE_URLS + 1]


async def _write_seedream_edit(
    runtime: ThumbLabRuntime,
    *,
    prompt: str,
    negative_prompt: str,
    reference_paths: list[Path],
    output_path: Path,
) -> dict[str, Any]:
    if not runtime.fal_ai_key:
        raise RuntimeError("fal.ai is not configured")
    import fal_client
    import fal_gate

    uploaded_urls = [
        await asyncio.to_thread(fal_client.upload_file, str(path))
        for path in reference_paths[:THUMBNAIL_MAX_REFERENCE_URLS]
    ]
    body: dict[str, Any] = {
        "prompt": str(prompt or "")[:4000],
        "image_urls": uploaded_urls,
        "image_size": "auto_2K",
        "num_images": 1,
    }
    if negative_prompt:
        body["negative_prompt"] = str(negative_prompt)[:500]
    data = await fal_gate.post_with_retry(
        SEEDREAM_THUMB_EDIT_URL,
        api_key=runtime.fal_ai_key,
        json_body=body,
        timeout_sec=180,
        max_attempts=3,
        source="thumblab_seedream_edit",
    )
    images = list((data or {}).get("images") or [])
    image_url = str((images[0] or {}).get("url") or "").strip() if images else ""
    if not image_url:
        raise RuntimeError("Seedream edit returned no image")
    await _download_public_image(image_url, output_path)
    return {
        "path": str(output_path),
        "output_url": image_url,
        "request_id": str((data or {}).get("request_id") or ""),
        "provider": "seedream45",
        "provider_label": "Seedream 4.5 Edit",
        "provider_mode": "fal_seedream_edit",
    }


async def _write_pikzels_thumbnail(
    runtime: ThumbLabRuntime,
    *,
    prompt: str,
    output_path: Path,
) -> dict[str, Any]:
    if not runtime.pikzels_api_key:
        raise RuntimeError("Pikzels is not configured")
    async with httpx.AsyncClient(timeout=120, follow_redirects=False) as client:
        response = await client.post(
            "https://api.pikzels.com/v1/thumbnail",
            headers={"X-Api-Key": runtime.pikzels_api_key, "Content-Type": "application/json"},
            json={"prompt": str(prompt or "")[:4000], "model": runtime.pikzels_thumbnail_model, "format": "16:9"},
        )
    try:
        payload = response.json()
    except Exception:
        payload = {}
    if response.status_code not in {200, 201}:
        raise RuntimeError(f"Pikzels thumbnail request failed ({response.status_code})")
    output_url = str((payload or {}).get("output") or "").strip()
    if not output_url:
        raise RuntimeError("Pikzels returned no output image")
    await _download_public_image(output_url, output_path)
    return {
        "path": str(output_path),
        "output_url": output_url,
        "request_id": str((payload or {}).get("request_id") or ""),
        "provider": "pikzels",
        "provider_label": "Pikzels",
        "provider_mode": "pikzels_thumbnail",
    }


async def _render_thumbnail_image(
    runtime: ThumbLabRuntime,
    prompt: str,
    negative_prompt: str,
    output_path: str,
    *,
    user: Optional[dict],
    mode: str = "describe",
    style_ref_path: str = "",
    image_model: str = "",
    reference_image_paths: list[str] | None = None,
) -> dict[str, Any]:
    del mode, style_ref_path
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    preferred = str(image_model or "seedream45").strip().lower().replace("seedream_45", "seedream45")
    catalog_ids = {row["id"] for row in _thumbnail_model_catalog(runtime)}
    if preferred not in catalog_ids:
        raise RuntimeError("Unsupported thumbnail model")
    reference_paths = [Path(path) for path in list(reference_image_paths or []) if Path(path).is_file()]
    errors: list[str] = []
    if reference_paths and preferred == "seedream45":
        try:
            return await _write_seedream_edit(
                runtime,
                prompt=prompt,
                negative_prompt=negative_prompt,
                reference_paths=reference_paths,
                output_path=destination,
            )
        except Exception as exc:
            errors.append(f"seedream edit: {str(exc)[:180]}")

    # Never silently upgrade a user to a more expensive fallback model. The
    # credit hold is derived from this exact selected model.
    candidates = [preferred]
    for model_id in candidates:
        if model_id not in catalog_ids:
            continue
        try:
            result = await _maybe_await(
                runtime.generate_image_fal_selected_model(
                    model_id,
                    prompt,
                    str(destination),
                    resolution="1080p_landscape",
                    negative_prompt=negative_prompt,
                )
            )
            if not destination.is_file() or destination.stat().st_size <= 0:
                raise RuntimeError("provider returned no local thumbnail")
            return {
                "path": str(destination),
                "output_url": str((result or {}).get("cdn_url") or ""),
                "request_id": "",
                "provider": str((result or {}).get("provider") or model_id),
                "provider_label": str((result or {}).get("provider_label") or model_id),
                "provider_mode": f"fal_{model_id}",
            }
        except Exception as exc:
            errors.append(f"{model_id}: {str(exc)[:180]}")
            destination.unlink(missing_ok=True)
    if runtime.pikzels_api_key and preferred == "seedream45":
        try:
            return await _write_pikzels_thumbnail(runtime, prompt=prompt, output_path=destination)
        except Exception as exc:
            errors.append(f"pikzels: {str(exc)[:180]}")
    raise RuntimeError("Thumbnail generation failed: " + " | ".join(errors[-3:]))


async def _enforce_thumbnail_1080(output_path: str) -> str:
    """Normalize any provider image to a safe, exact 1920x1080 PNG."""

    path = Path(output_path)
    if not path.is_file() or path.stat().st_size <= 0:
        raise RuntimeError("Thumbnail output is missing")
    try:
        with Image.open(path) as source:
            width, height = source.size
            if width <= 0 or height <= 0 or width * height > THUMBNAIL_MAX_IMAGE_PIXELS:
                raise RuntimeError("Thumbnail dimensions are not allowed")
            image = ImageOps.exif_transpose(source).convert("RGB")
            image.thumbnail((1920, 1080), Image.Resampling.LANCZOS)
            canvas = Image.new("RGB", (1920, 1080), (0, 0, 0))
            canvas.paste(image, ((1920 - image.width) // 2, (1080 - image.height) // 2))
            temporary = path.with_name(f".{path.stem}.{uuid.uuid4().hex}.png")
            canvas.save(temporary, "PNG", optimize=True)
        temporary.replace(path)
    except (UnidentifiedImageError, Image.DecompressionBombError, OSError) as exc:
        raise RuntimeError("Thumbnail provider returned an invalid image") from exc
    return str(path)


def _apply_thumbnail_text_overlay(image_path: str, text: str, position: str = "bottom_left") -> str:
    normalized_text = " ".join(str(text or "").strip().upper().split())[:80]
    if not normalized_text:
        return image_path
    path = Path(image_path)
    try:
        with Image.open(path) as source:
            image = source.convert("RGB")
        draw = ImageDraw.Draw(image)
        font_size = max(60, int(image.height * 0.08))
        font = None
        for font_path in (
            "C:/Windows/Fonts/arialbd.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ):
            try:
                font = ImageFont.truetype(font_path, font_size)
                break
            except OSError:
                continue
        font = font or ImageFont.load_default()
        bounds = draw.textbbox((0, 0), normalized_text, font=font, stroke_width=max(3, font_size // 20))
        text_width = bounds[2] - bounds[0]
        text_height = bounds[3] - bounds[1]
        padding = int(image.width * 0.03)
        if position == "center":
            x, y = (image.width - text_width) // 2, int(image.height * 0.7)
        else:
            x, y = padding, image.height - text_height - padding - 20
        draw.text(
            (x, y),
            normalized_text,
            font=font,
            fill="white",
            stroke_width=max(3, font_size // 20),
            stroke_fill="black",
        )
        image.save(path, "PNG", optimize=True)
    except Exception as exc:
        log.warning("ThumbLab text overlay skipped: %s", str(exc)[:180])
    return str(path)


def _thumbnail_output_dir_for_user(user: Optional[dict]) -> Path:
    runtime = _DEFAULT_RUNTIME
    if runtime is None:
        raise RuntimeError("ThumbLab runtime has not been configured")
    return _thumbnail_output_dir(runtime, dict(user or {}), create=True)


async def _generate_thumbnail_image(
    prompt: str,
    negative_prompt: str,
    output_path: str,
    user: Optional[dict],
    mode: str = "describe",
    style_ref_path: str = "",
    image_model: str = "",
    reference_image_paths: list[str] | None = None,
) -> dict[str, Any]:
    runtime = _DEFAULT_RUNTIME
    if runtime is None:
        raise RuntimeError("ThumbLab runtime has not been configured")
    return await _render_thumbnail_image(
        runtime,
        prompt,
        negative_prompt,
        output_path,
        user=user,
        mode=mode,
        style_ref_path=style_ref_path,
        image_model=image_model,
        reference_image_paths=reference_image_paths,
    )


def _idempotency_key(user_id: str, command_id: str) -> str:
    canonical = json.dumps(
        {"user_id": str(user_id or ""), "command_id": str(command_id or "")},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(("thumblab-generate-v1:" + canonical).encode("utf-8")).hexdigest()


def _request_contract(req: ThumbnailGenerateRequest) -> dict[str, Any]:
    return req.model_dump(mode="json")


def _same_contract(receipt: ExecutionReceipt, contract: dict[str, Any]) -> bool:
    left = json.dumps(receipt.arguments, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    right = json.dumps(contract, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return receipt.tool_name == "generate_thumbnail" and left == right


def _idempotent_replay(receipt: ExecutionReceipt, contract: dict[str, Any]) -> dict[str, Any]:
    if not _same_contract(receipt, contract):
        raise HTTPException(409, "X-Idempotency-Key was already used for a different thumbnail request")
    if receipt.status in {"failed", "rejected"}:
        status = int((receipt.result or {}).get("http_status") or 409)
        raise HTTPException(status, receipt.error or "The original thumbnail request failed")
    payload = dict(receipt.result or {})
    payload["idempotent_replay"] = True
    payload["duplicate_of"] = receipt.execution_id
    return payload


def _save_execution_receipt(
    runtime: ThumbLabRuntime,
    *,
    key: str,
    command_id: str,
    status: str,
    contract: dict[str, Any],
    result: dict[str, Any],
    error: str = "",
    started_at: float,
) -> ExecutionReceipt:
    receipt = ExecutionReceipt(
        execution_id=f"exec_{uuid.uuid4().hex[:20]}",
        idempotency_key=key,
        command_id=command_id,
        status=status,  # type: ignore[arg-type]
        tool_name="generate_thumbnail",
        target_job_id=str(result.get("job_id") or ""),
        arguments=contract,
        result=dict(result),
        error=str(error or "")[:1000],
        started_at=started_at,
        finished_at=time.time(),
    )
    runtime.ledger.save(receipt)
    return receipt


async def _persist_job(runtime: ThumbLabRuntime, job_id: str) -> None:
    try:
        await _maybe_await(runtime.persist_job_state(job_id, runtime.jobs[job_id]))
    except Exception as exc:
        log.error("ThumbLab job persistence failed for %s: %s", job_id, str(exc)[:180])


async def _thumbnail_pipeline(
    runtime: ThumbLabRuntime,
    *,
    job_id: str,
    req: ThumbnailGenerateRequest,
    user: dict,
    reservation_id: str,
    credit_cost: int,
    catalyst: dict[str, Any],
) -> None:
    user_id = _user_id(user)
    delivered = False
    output_path: Path | None = None
    try:
        runtime.jobs[job_id].update({"status": "analyzing", "progress": 8})
        await _persist_job(runtime, job_id)

        frame_path: Path | None = None
        upload_id = str(req.video_upload_id or "").strip()
        if upload_id:
            source = _thumbnail_upload_path(runtime, user, upload_id)
            if source is None:
                raise RuntimeError("The selected video upload is missing or belongs to another account")
            candidate = _thumbnail_frame_path(runtime, user, upload_id, create=False)
            if not candidate.is_file():
                await _extract_frame(runtime, user, upload_id, req.frame_at_pct or 0.12)
            if candidate.is_file():
                frame_path = candidate

        reference_urls = [
            str(value or "").strip()
            for value in list(req.reference_thumbnail_urls or [])[:THUMBNAIL_MAX_REFERENCE_URLS]
            if str(value or "").strip()
        ]
        reference_paths = await _collect_reference_paths(
            runtime,
            user,
            job_id=job_id,
            reference_urls=reference_urls,
            frame_path=frame_path,
        )
        context = " ".join(
            value
            for value in (
                str(req.video_title or "").strip(),
                str(req.description or "").strip(),
                str(req.reference_creator or "").strip(),
            )
            if value
        )[:3000]
        vision = await _analyze_reference_images(runtime, reference_paths, context=context)
        runtime.jobs[job_id]["progress"] = 22
        await _persist_job(runtime, job_id)

        ai_result = await _generate_prompt_ab(runtime, req, vision=vision, catalyst=catalyst)
        prompt = str(ai_result.get("prompt") or req.description or "").strip()
        negative_prompt = str(ai_result.get("negative_prompt") or "").strip()
        title_text = " ".join(str(ai_result.get("title_text") or "").split())[:80]
        if title_text:
            prompt += f" Prominent, legible thumbnail text: {title_text}."
        runtime.jobs[job_id].update(
            {
                "status": "generating",
                "progress": 36,
                "ai_analysis": {
                    "title_text": title_text,
                    "style_notes": str(ai_result.get("style_notes") or "")[:1000],
                    "patterns": list(ai_result.get("patterns_detected") or vision.get("patterns") or [])[:8],
                    "vision": vision,
                    "catalyst_channel": str(catalyst.get("channel_title") or ""),
                    "ab_scoring": dict(ai_result.get("ab_scoring") or {}),
                },
            }
        )
        await _persist_job(runtime, job_id)

        output_name = f"{job_id}.png"
        output_path = _thumbnail_output_dir(runtime, user, create=True) / output_name
        render = await _render_thumbnail_image(
            runtime,
            prompt,
            negative_prompt,
            str(output_path),
            user=user,
            mode=req.mode,
            image_model=str(req.image_model or "seedream45"),
            reference_image_paths=[str(path) for path in reference_paths],
        )
        await _enforce_thumbnail_1080(str(output_path))

        await _maybe_await(
            runtime.commit_reservation(
                user_id,
                reservation_id,
                actual_credits=credit_cost,
                reason="thumbnail_complete",
                metadata={"job_id": job_id, "model": str(req.image_model or "seedream45")},
            )
        )
        # The file does not become a deliverable until the credit hold has
        # been committed. If commit raises, the failure path deletes it and
        # releases any still-live reservation.
        delivered = True

        generation_id = ""
        if runtime.save_training_candidate is not None:
            try:
                generation_id = str(
                    await _maybe_await(
                        runtime.save_training_candidate(
                            prompt,
                            str(output_path),
                            template="thumbnail",
                            source=str(render.get("provider") or "fal"),
                            metadata={
                                "mode": req.mode,
                                "title_text": title_text,
                                "user_id": user_id,
                                "job_id": job_id,
                                "provider_mode": str(render.get("provider_mode") or ""),
                            },
                        )
                    )
                    or ""
                )
            except Exception as exc:
                log.warning("ThumbLab training-candidate save skipped: %s", str(exc)[:180])

        runtime.jobs[job_id].update(
            {
                "status": "complete",
                "progress": 100,
                "output_file": output_name,
                "output_url": f"/api/thumbnails/generated/{output_name}",
                "generation_id": generation_id,
                "provider": str(render.get("provider") or "fal"),
                "provider_label": str(render.get("provider_label") or render.get("provider") or "fal"),
                "provider_mode": str(render.get("provider_mode") or ""),
                "provider_request_id": str(render.get("request_id") or ""),
                "credit_reserved": False,
                "credit_charged": True,
                "completed_at": time.time(),
            }
        )
        await _persist_job(runtime, job_id)
    except Exception as exc:
        log.error("ThumbLab pipeline failed for %s: %s", job_id, str(exc)[:300], exc_info=True)
        refunded = False
        if not delivered:
            if output_path is not None:
                output_path.unlink(missing_ok=True)
            try:
                await _maybe_await(
                    runtime.release_reservation(user_id, reservation_id, reason="thumbnail_failed")
                )
                refunded = True
            except Exception as refund_exc:
                log.error("ThumbLab credit release failed for %s: %s", job_id, str(refund_exc)[:180])
        runtime.jobs[job_id].update(
            {
                "status": "error",
                "error": str(exc)[:500],
                "credit_reserved": bool(delivered),
                "credit_refunded": refunded,
                "failed_at": time.time(),
            }
        )
        await _persist_job(runtime, job_id)


def _default_credit_functions() -> tuple[Callable[..., Any], Callable[..., Any], Callable[..., Any]]:
    import unified_credits as credits

    return credits.reserve_credits, credits.release_reservation, credits.commit_reservation


def build_thumblab_router(
    *,
    require_auth: Callable[..., Any],
    jobs: dict[str, dict],
    persist_job_state: Callable[..., Any],
    fal_json_completion: Callable[..., Any],
    fal_vision_json_completion: Callable[..., Any],
    generate_image_fal_selected_model: Callable[..., Any],
    youtube_fetch_public_channel_page_videos: Callable[..., Any],
    list_connected_youtube_channels_for_user: Callable[..., Any],
    save_training_candidate: Optional[Callable[..., Any]] = None,
    reserve_credits: Optional[Callable[..., Any]] = None,
    release_reservation: Optional[Callable[..., Any]] = None,
    commit_reservation: Optional[Callable[..., Any]] = None,
    storage_root: str | Path = THUMBNAIL_DIR,
    idempotency_ledger: FileExecutionLedger | None = None,
    fal_ai_key: str = FAL_AI_KEY,
    pikzels_api_key: str = PIKZELS_API_KEY,
    pikzels_thumbnail_model: str = PIKZELS_THUMBNAIL_MODEL,
    max_video_bytes: int = MAX_THUMBNAIL_VIDEO_BYTES,
    probe_video_duration: Callable[..., Any] = _probe_video_duration_default,
    extract_frame_image: Callable[..., Any] = _extract_frame_image_default,
) -> APIRouter:
    """Build only the secure endpoints consumed by the active ThumbLab panel."""

    global _DEFAULT_RUNTIME
    root = Path(storage_root)
    root.mkdir(parents=True, exist_ok=True)
    defaults = _default_credit_functions()
    runtime = ThumbLabRuntime(
        require_auth=require_auth,
        jobs=jobs,
        persist_job_state=persist_job_state,
        fal_json_completion=fal_json_completion,
        fal_vision_json_completion=fal_vision_json_completion,
        generate_image_fal_selected_model=generate_image_fal_selected_model,
        youtube_fetch_public_channel_page_videos=youtube_fetch_public_channel_page_videos,
        list_connected_youtube_channels_for_user=list_connected_youtube_channels_for_user,
        save_training_candidate=save_training_candidate,
        reserve_credits=reserve_credits or defaults[0],
        release_reservation=release_reservation or defaults[1],
        commit_reservation=commit_reservation or defaults[2],
        ledger=idempotency_ledger or FileExecutionLedger(root / "idempotency"),
        storage_root=root,
        fal_ai_key=str(fal_ai_key or "").strip(),
        pikzels_api_key=str(pikzels_api_key or "").strip(),
        pikzels_thumbnail_model=str(pikzels_thumbnail_model or "pkz-3").strip(),
        max_video_bytes=max(1, int(max_video_bytes or MAX_THUMBNAIL_VIDEO_BYTES)),
        probe_video_duration=probe_video_duration,
        extract_frame_image=extract_frame_image,
    )
    _DEFAULT_RUNTIME = runtime
    router = APIRouter()

    @router.get("/api/thumbnails/models")
    async def thumbnail_models(user: dict = Depends(require_auth)):
        _require_user_id(user)
        return {
            "models": _thumbnail_model_catalog(runtime),
            "default_credits": THUMBNAIL_DEFAULT_CREDITS,
            "max_reference_urls": THUMBNAIL_MAX_REFERENCE_URLS,
        }

    @router.get("/api/thumbnails/my-channels")
    async def thumbnail_my_channels(user: dict = Depends(require_auth)):
        _require_user_id(user)
        rows = await _connected_channels(runtime, user)
        channels: list[dict[str, Any]] = []
        for row in rows:
            channel_id = _channel_id(row)
            if not channel_id:
                continue
            catalyst = _catalyst_packaging(channel_id)
            channels.append(
                {
                    "channel_id": channel_id,
                    "title": str(row.get("title") or row.get("channel_title") or catalyst.get("channel_title") or channel_id),
                    "packaging_learnings": list(catalyst.get("packaging_learnings") or [])[:8],
                    "title_pattern_hints": list(catalyst.get("title_pattern_hints") or [])[:6],
                }
            )
        return {"channels": channels, "total": len(channels)}

    @router.get("/api/thumbnails/creator-gallery")
    async def thumbnail_creator_gallery(
        url: str = Query("", max_length=500),
        max_results: int = Query(36, ge=6, le=60),
        user: dict = Depends(require_auth),
    ):
        _require_user_id(user)
        channel_url, channel_id = _strict_youtube_channel_ref(url)
        if not channel_url and not channel_id:
            raise HTTPException(400, "Paste a valid YouTube channel URL, channel ID, or @handle")
        rows = await _maybe_await(
            runtime.youtube_fetch_public_channel_page_videos(
                "",
                channel_url=channel_url,
                channel_id=channel_id,
                max_results=max_results,
            )
        )
        if not rows:
            raise HTTPException(404, "Could not load public videos for that YouTube channel")
        videos: list[dict[str, Any]] = []
        channel_title = ""
        for row in list(rows or []):
            if not isinstance(row, dict):
                continue
            video_id = str(row.get("video_id") or "").strip()
            if not re.fullmatch(r"[A-Za-z0-9_-]{6,20}", video_id):
                continue
            channel_title = channel_title or str(row.get("channel_title") or row.get("channel") or "").strip()
            thumbnail_url = str(row.get("thumbnail_url") or "").strip() or f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"
            videos.append(
                {
                    "video_id": video_id,
                    "title": str(row.get("title") or "")[:500],
                    "views": max(0, int(float(row.get("views") or row.get("view_count") or 0) or 0)),
                    "thumbnail_url": thumbnail_url,
                    "watch_url": f"https://www.youtube.com/watch?v={video_id}",
                }
            )
        videos.sort(key=lambda row: -int(row.get("views") or 0))
        return {
            "channel_url": channel_url or f"https://www.youtube.com/channel/{channel_id}",
            "channel_id": channel_id,
            "channel_title": channel_title or str(url)[:200],
            "videos": videos,
            "total": len(videos),
        }

    @router.post("/api/thumbnails/upload-video")
    async def thumbnail_upload_video(
        file: UploadFile = File(...),
        user: dict = Depends(require_auth),
    ):
        _require_user_id(user)
        original_name = Path(str(file.filename or "")).name[:180]
        extension = Path(original_name).suffix.lower()
        if extension not in THUMBNAIL_ALLOWED_VIDEO_EXTENSIONS:
            raise HTTPException(400, "Unsupported video format; use MP4, MOV, MKV, WebM, M4V, or AVI")
        upload_id = f"vid_{uuid.uuid4().hex}"
        destination = _thumbnail_video_dir(runtime, user, create=True) / f"{upload_id}{extension}"
        try:
            size = await write_upload_limited(
                file,
                destination,
                max_bytes=runtime.max_video_bytes,
                label="ThumbLab video",
            )
        except UploadTooLargeError as exc:
            raise HTTPException(413, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        finally:
            await file.close()
        duration = float(await _maybe_await(runtime.probe_video_duration(destination)) or 0.0)
        if duration <= 0:
            destination.unlink(missing_ok=True)
            raise HTTPException(422, "Uploaded file is not a decodable video")
        if duration >= 3600:
            duration_label = f"{int(duration // 3600)}h {int((duration % 3600) // 60)}m"
        else:
            duration_label = f"{int(duration // 60)}m {int(duration % 60)}s"
        return {
            "upload_id": upload_id,
            "filename": original_name,
            "size_mb": round(size / (1024 * 1024), 2),
            "duration_sec": round(duration, 1),
            "duration_label": duration_label,
        }

    @router.post("/api/thumbnails/extract-frame")
    async def thumbnail_extract_frame(
        upload_id: str = Query("", max_length=64),
        pct: float = Query(0.12, ge=0.01, le=0.95),
        user: dict = Depends(require_auth),
    ):
        _require_user_id(user)
        return await _extract_frame(runtime, user, upload_id, pct)

    @router.get("/api/thumbnails/frame/{upload_id}")
    async def thumbnail_serve_frame(upload_id: str, user: dict = Depends(require_auth)):
        _require_user_id(user)
        path = _thumbnail_frame_path(runtime, user, upload_id, create=False)
        if not path.is_file():
            raise HTTPException(404, "Frame not found")
        return FileResponse(
            str(path),
            media_type="image/jpeg",
            headers={"Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff"},
        )

    @router.get("/api/thumbnails/generated/{filename}")
    async def thumbnail_serve_generated(filename: str, user: dict = Depends(require_auth)):
        user_id = _require_user_id(user)
        normalized = str(filename or "").strip()
        if not _SAFE_OUTPUT_FILE_RE.fullmatch(normalized):
            raise HTTPException(404, "Generated thumbnail not found")
        if _THUMBLAB_OUTPUT_FILE_RE.fullmatch(normalized):
            # A provider may have already written the PNG while the unified
            # credit commit is still pending. Never expose that file until the
            # job is both owner-matched and complete.
            job_id = normalized[:-4]
            job = dict(runtime.jobs.get(job_id) or {})
            if (
                str(job.get("status") or "").lower() != "complete"
                or str(job.get("user_id") or job.get("owner_user_id") or "") != user_id
            ):
                raise HTTPException(404, "Generated thumbnail not found")
        path = _thumbnail_output_dir(runtime, user, create=False) / normalized
        if not path.is_file():
            raise HTTPException(404, "Generated thumbnail not found")
        return FileResponse(
            str(path),
            media_type="image/png",
            headers={"Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff"},
        )

    @router.post("/api/thumbnails/generate", status_code=202)
    async def thumbnail_generate(
        req: ThumbnailGenerateRequest,
        background_tasks: BackgroundTasks,
        request: Request,
        user: dict = Depends(require_auth),
    ):
        user_id = _require_user_id(user)
        if not runtime.fal_ai_key and not runtime.pikzels_api_key:
            raise HTTPException(503, "Thumbnail image provider is not configured")
        command_id = str(request.headers.get("x-idempotency-key") or "").strip()
        if not command_id:
            raise HTTPException(400, "X-Idempotency-Key is required for thumbnail generation")
        if len(command_id) > 256:
            raise HTTPException(400, "X-Idempotency-Key is too long")
        description = str(req.description or "").strip()
        if not description or len(description) > 4000:
            raise HTTPException(400, "Thumbnail description must be between 1 and 4000 characters")
        raw_refs = [str(value or "").strip() for value in list(req.reference_thumbnail_urls or []) if str(value or "").strip()]
        if len(raw_refs) > THUMBNAIL_MAX_REFERENCE_URLS:
            raise HTTPException(400, f"At most {THUMBNAIL_MAX_REFERENCE_URLS} reference thumbnails are allowed")
        model_id = str(req.image_model or "seedream45").strip().lower().replace("seedream_45", "seedream45")
        if model_id not in {row["id"] for row in _thumbnail_model_catalog(runtime)}:
            raise HTTPException(400, "Unsupported thumbnail model")
        req = req.model_copy(update={"image_model": model_id, "reference_thumbnail_urls": raw_refs})
        if str(req.video_upload_id or "").strip() and _thumbnail_upload_path(runtime, user, req.video_upload_id) is None:
            raise HTTPException(404, "Video upload not found")

        catalyst = await _authorized_catalyst_context(runtime, user, str(req.channel_id or ""))
        contract = _request_contract(req)
        key = _idempotency_key(user_id, command_id)
        previous = runtime.ledger.get(key)
        if previous is not None:
            return _idempotent_replay(previous, contract)
        if not runtime.ledger.claim(key, command_id):
            previous = runtime.ledger.get(key)
            if previous is not None:
                return _idempotent_replay(previous, contract)
            raise HTTPException(409, "The original thumbnail request is still being accepted")

        started_at = time.time()
        credit_cost = _thumbnail_credit_cost(model_id)
        try:
            reservation = await _maybe_await(
                runtime.reserve_credits(
                    user_id,
                    credit_cost,
                    reason="thumbnail_generate",
                    metadata={"model": model_id, "mode": req.mode, "command_id": command_id},
                )
            )
            reservation_id = str((reservation or {}).get("reservation_id") or "")
            if not reservation_id:
                raise RuntimeError("Credit reservation was not created")
        except Exception as exc:
            status = int(exc.status_code) if isinstance(exc, HTTPException) else 402 if exc.__class__.__name__ == "InsufficientCreditsError" else 503
            message = str(getattr(exc, "detail", "") or str(exc) or "Credit reservation failed")[:500]
            _save_execution_receipt(
                runtime,
                key=key,
                command_id=command_id,
                status="failed",
                contract=contract,
                result={"ok": False, "http_status": status},
                error=message,
                started_at=started_at,
            )
            raise HTTPException(status, message) from exc

        job_id = f"thumb_{uuid.uuid4().hex}"
        runtime.jobs[job_id] = {
            "status": "queued",
            "progress": 0,
            "type": "thumbnail",
            "lane": "thumbnails",
            "mode": req.mode,
            "credit_cost": credit_cost,
            "billing_source": "unified_credits",
            "credit_reserved": True,
            "user_id": user_id,
            "owner_user_id": user_id,
            "created_at": time.time(),
        }
        await _persist_job(runtime, job_id)
        response = {"status": "accepted", "job_id": job_id, "credit_cost": credit_cost}
        try:
            _save_execution_receipt(
                runtime,
                key=key,
                command_id=command_id,
                status="accepted",
                contract=contract,
                result=response,
                started_at=started_at,
            )
        except Exception as exc:
            # Never leave a paid hold or an untracked queued job behind when
            # the durable replay receipt could not be written.
            released = False
            try:
                await _maybe_await(
                    runtime.release_reservation(user_id, reservation_id, reason="thumbnail_accept_receipt_failed")
                )
                released = True
            except Exception as release_exc:
                log.error("ThumbLab receipt-failure credit release failed: %s", str(release_exc)[:180])
            runtime.jobs[job_id].update(
                {
                    "status": "error",
                    "error": "Thumbnail request could not be durably accepted",
                    "credit_reserved": not released,
                    "credit_refunded": released,
                    "failed_at": time.time(),
                }
            )
            await _persist_job(runtime, job_id)
            raise HTTPException(503, "Thumbnail request could not be durably accepted; no provider work was started") from exc
        background_tasks.add_task(
            _thumbnail_pipeline,
            runtime,
            job_id=job_id,
            req=req,
            user=dict(user),
            reservation_id=reservation_id,
            credit_cost=credit_cost,
            catalyst=catalyst,
        )
        return response

    return router


__all__ = [
    "build_thumblab_router",
    "_apply_thumbnail_text_overlay",
    "_enforce_thumbnail_1080",
    "_generate_thumbnail_image",
    "_thumbnail_output_dir_for_user",
]
