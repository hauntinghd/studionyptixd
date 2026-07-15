"""
Multi-model stills engine for Skeleton AI.

Frontend's image-model picker exposes 7 keys; this module maps each one to
its canonical fal.ai endpoint and dispatches the call. Switching models on
the frontend results in the next /scenes request hitting a different fal
endpoint immediately — no client-side caching.

Endpoints (canonical, sourced from video_pipeline.CREATIVE_IMAGE_MODEL_PROFILES):
  seedream_45         → fal-ai/bytedance/seedream/v4.5/text-to-image
  flux_2_pro          → fal-ai/flux-2-pro
  imagen4             → fal-ai/imagen4/preview
  recraft_v4_pro      → fal-ai/recraft/v4/pro/text-to-image
  nano_banana_pro     → fal-ai/nano-banana-pro
  ernie_image         → fal-ai/ernie-image           (free)
  nano_banana_free    → fal-ai/nano-banana           (free)

All run through fal_client.subscribe so we don't have to manage queue
polling manually. The output URL is then downloaded to the workspace.
"""
from __future__ import annotations
import os
import time
from pathlib import Path
from typing import Any

import fal_client

from .fal_auth import require_fal_key
import httpx


class StillsError(RuntimeError):
    pass


# Map frontend model_key → fal endpoint slug.
# Keep value names verbatim across frontend ImageModel union and backend.
MODEL_ENDPOINTS: dict[str, str] = {
    "seedream_45":      "fal-ai/bytedance/seedream/v4.5/text-to-image",
    "flux_2_pro":       "fal-ai/flux-2-pro",
    "imagen4":          "fal-ai/imagen4/preview",
    "recraft_v4_pro":   "fal-ai/recraft/v4/pro/text-to-image",
    "nano_banana_pro":  "fal-ai/nano-banana-pro",
    "ernie_image":      "fal-ai/ernie-image",
    "nano_banana_free": "fal-ai/nano-banana",
}


def list_models() -> list[dict[str, Any]]:
    """Used by /api/skeleton-ai/pricing or similar info endpoints."""
    return [
        {"key": k, "endpoint": v} for k, v in MODEL_ENDPOINTS.items()
    ]


def _ensure_fal() -> None:
    try:
        require_fal_key("still generation")
    except RuntimeError as exc:
        raise StillsError(str(exc)) from exc


def _build_args(model_key: str, prompt: str, negative_prompt: str,
                width: int, height: int) -> dict[str, Any]:
    """Per-model argument shaping. Most fal endpoints accept different params.

    Right now we keep this minimal — some endpoints ignore extra fields
    silently, some 422 if you send the wrong key. We tune as we hit issues.
    """
    base = {"prompt": prompt[:759]}
    neg = (negative_prompt or "")[:1500]

    if model_key == "seedream_45":
        return {
            **base,
            "negative_prompt": neg,
            "image_size": {"width": width, "height": height},
            "num_images": 1,
            "guidance_scale": 7.0,
            "num_inference_steps": 80,
            "enable_safety_checker": False,
        }
    if model_key in ("flux_2_pro",):
        return {
            **base,
            "negative_prompt": neg,
            "image_size": {"width": width, "height": height},
            "num_images": 1,
            "enable_safety_checker": False,
        }
    if model_key in ("imagen4",):
        # imagen4 expects aspect_ratio rather than size dict.
        return {
            **base,
            "aspect_ratio": "9:16",
            "num_images": 1,
        }
    if model_key in ("recraft_v4_pro",):
        return {
            **base,
            "image_size": {"width": width, "height": height},
            "style": "realistic_image",
            "num_images": 1,
        }
    if model_key in ("nano_banana_pro", "nano_banana_free"):
        return {
            **base,
            "aspect_ratio": "9:16",
            "num_images": 1,
        }
    if model_key in ("ernie_image",):
        return {
            **base,
            "image_size": {"width": width, "height": height},
            "num_images": 1,
        }
    # Unknown model: best-effort minimal payload.
    return {**base, "num_images": 1}


def _extract_first_url(result: dict[str, Any]) -> str:
    """Most fal image endpoints return either {images:[{url}]} or {image:{url}}."""
    if not isinstance(result, dict):
        raise StillsError(f"unexpected result shape: {type(result).__name__}")
    images = result.get("images")
    if isinstance(images, list) and images:
        url = (images[0] or {}).get("url") if isinstance(images[0], dict) else None
        if url:
            return url
    img = result.get("image")
    if isinstance(img, dict) and img.get("url"):
        return img["url"]
    if result.get("image_url"):
        return result["image_url"]
    raise StillsError(f"no image URL in result: keys={list(result.keys())}")


def generate(
    model_key: str,
    prompt: str,
    out_path: Path,
    *,
    negative_prompt: str = "",
    width: int = 1440,
    height: int = 2560,
) -> Path:
    """Generate a still using the specified model. Idempotent — skips if output exists."""
    out_path = Path(out_path)
    if out_path.exists() and out_path.stat().st_size > 1024:
        return out_path

    endpoint = MODEL_ENDPOINTS.get(model_key)
    if not endpoint:
        raise StillsError(
            f"unsupported model_key {model_key!r}. "
            f"valid: {sorted(MODEL_ENDPOINTS.keys())}"
        )

    _ensure_fal()
    args = _build_args(model_key, prompt, negative_prompt, width, height)

    try:
        result = fal_client.subscribe(endpoint, arguments=args)
    except Exception as e:  # broad — fal_client wraps various transport errors
        raise StillsError(f"{model_key} ({endpoint}) failed: {e}") from e

    url = _extract_first_url(result)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _download(url, out_path)
    return out_path


def _download(url: str, dest: Path, retries: int = 3) -> None:
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            with httpx.stream("GET", url, timeout=180) as r:
                if r.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"server {r.status_code}", request=r.request, response=r
                    )
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r.iter_bytes(chunk_size=1024 * 256):
                        f.write(chunk)
            return
        except (httpx.HTTPError, httpx.RequestError) as e:
            last_exc = e
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
    if last_exc:
        raise last_exc
