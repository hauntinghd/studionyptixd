"""
i2v engine — Seedance 2.0 default, Kling 2.1 Pro premium upgrade.

Per the i2v bake-off (2026-05-04):
  - LTX 13B distilled  → BROKEN (glow drift on character eyes)
  - Kling 2.1 Pro      → BEST quality but $0.40-0.50/clip (premium tier only)
  - Seedance 2.0       → strong character preservation, $0.10-0.15/clip (default)
  - Pixverse V6        → solid, similar cost to Seedance
  - Wan 2.2            → black eye sockets glitch

Skeleton AI defaults:
  Standard tier (5 AC):  Seedance 2.0
  Premium upgrade (+2 AC): Kling 2.1 Pro

Both via fal_client.subscribe.
"""
from __future__ import annotations
import os
import time
from pathlib import Path

import fal_client
import httpx


class I2VError(RuntimeError):
    pass


SEEDANCE_ENDPOINT = "bytedance/seedance-2.0/image-to-video"
KLING_PRO_ENDPOINT = "fal-ai/kling-video/v2.1/pro/image-to-video"


def _ensure_fal():
    key = os.getenv("FAL_AI_KEY", "").strip()
    if not key:
        raise I2VError("FAL_AI_KEY not set in env")
    os.environ["FAL_KEY"] = key  # fal_client reads this
    return key


def generate(
    still_path: Path,
    motion_prompt: str,
    out_path: Path,
    *,
    tier: str = "standard",
    duration_sec: int = 5,
    aspect_ratio: str = "9:16",
) -> Path:
    """
    Animate a still into a short clip.

    tier: "standard" (Seedance 2.0) or "premium" (Kling 2.1 Pro).
    """
    out_path = Path(out_path)
    if out_path.exists() and out_path.stat().st_size > 1024:
        return out_path

    _ensure_fal()
    image_url = fal_client.upload_file(str(still_path))

    if tier == "premium":
        endpoint = KLING_PRO_ENDPOINT
        args = {
            "prompt": motion_prompt,
            "image_url": image_url,
            "duration": str(duration_sec),
            "aspect_ratio": aspect_ratio,
            "negative_prompt": _NEG_VIDEO,
        }
    else:
        endpoint = SEEDANCE_ENDPOINT
        args = {
            "prompt": motion_prompt,
            "image_url": image_url,
            "duration": str(duration_sec),
            "aspect_ratio": aspect_ratio,
            # Seedance 2.0 defaults to ON; auto-generated audio trips
            # partner_validation_failed ("Output audio has sensitive content")
            # because the silent skeleton clips can produce odd ambient SFX.
            # Narration is added separately via ElevenLabs in the mux step.
            "generate_audio": False,
        }

    result = fal_client.subscribe(endpoint, arguments=args)
    video_url = (result.get("video") or {}).get("url") or result.get("video_url")
    if not video_url:
        raise I2VError(f"{endpoint} returned no video URL: {result}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    _download(video_url, out_path)
    return out_path


_NEG_VIDEO = (
    "blur, low quality, jitter, warping, deformation, identity drift, "
    "glowing eyes, supernatural eyes, white glowing eye, asymmetric eyes, "
    "character morphing, body warping, frozen pose, text overlays"
)


def _download(url: str, dest: Path, retries: int = 3) -> None:
    last_exc = None
    for attempt in range(retries):
        try:
            with httpx.stream("GET", url, timeout=300) as r:
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


# AC pricing (per Casey's pricing tiers)
AC_COST_STANDARD = 5  # Seedance 2.0 default
AC_COST_PREMIUM = 7   # +2 AC for Kling Pro upgrade
