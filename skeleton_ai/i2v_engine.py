"""
i2v engine — multi-model fallback for skeleton + character imagery.

Per the i2v bake-off (2026-05-04) + Marvel-vs-DC content-policy run (2026-05-06):
  - Seedance 2.0   → strong char preservation, $0.10-0.15/clip BUT trips
                     Bytedance's content policy on skeleton+weapon imagery
                     (the canonical Skeleton AI signature combination).
                     'Output video has sensitive content' / partner_validation_failed.
                     Kept as PRIMARY for non-character beats (intros, transitions);
                     auto-falls back to Pixverse V6 on content_policy_violation.
  - Pixverse V6    → $0.045/s (~$0.225/clip), permissive moderation, the
                     fallback that actually animates skeleton-Iron-Man +
                     skeleton-Strange without flagging.
  - Kling 2.1 Pro  → $0.40-0.50/clip (premium tier only — best quality, mid
                     moderation strictness).
  - LTX 13B        → retired for skeleton (glow drift on character eyes).
  - Wan 2.2        → black eye sockets glitch.

Pipeline:
  Standard tier (5 AC):  Seedance 2.0  → Pixverse V6 (auto-fallback)
  Premium upgrade (7 AC): Kling 2.1 Pro

FAL queue polling is throttled for full multi-scene renders.
"""
from __future__ import annotations
import os
import time
from pathlib import Path

import fal_client
import httpx

FAL_SUBSCRIBE_TIMEOUT_SEC = int(os.getenv("FAL_I2V_SUBSCRIBE_TIMEOUT_SEC", "600"))
FAL_I2V_POLL_INTERVAL_SEC = float(os.getenv("FAL_I2V_POLL_INTERVAL_SEC", "5"))
FAL_I2V_POLL_MAX_INTERVAL_SEC = float(os.getenv("FAL_I2V_POLL_MAX_INTERVAL_SEC", "15"))


class I2VError(RuntimeError):
    pass


SEEDANCE_ENDPOINT = "bytedance/seedance-2.0/image-to-video"
PIXVERSE_V6_ENDPOINT = "fal-ai/pixverse/v6/image-to-video"
KLING_PRO_ENDPOINT = "fal-ai/kling-video/v2.1/pro/image-to-video"

# Standard tier fallback chain — first model that doesn't 422 wins.
STANDARD_FALLBACK_CHAIN = [SEEDANCE_ENDPOINT, PIXVERSE_V6_ENDPOINT]

AC_COST_STANDARD = 5
AC_COST_PREMIUM = 7

VIDEO_MODELS: dict[str, dict[str, object]] = {
    "seedance": {
        "label": "Seedance 2.0",
        "description": "Default. Strong motion; auto-falls back to Pixverse on content-policy flags.",
        "endpoints": list(STANDARD_FALLBACK_CHAIN),
        "ac_cost": AC_COST_STANDARD,
    },
    "pixverse": {
        "label": "Pixverse V6",
        "description": "Permissive moderation; use when Seedance blocks skeleton imagery.",
        "endpoints": [PIXVERSE_V6_ENDPOINT],
        "ac_cost": AC_COST_STANDARD,
    },
    "kling_pro": {
        "label": "Kling 2.1 Pro",
        "description": "Highest quality motion; premium cost per short.",
        "endpoints": [KLING_PRO_ENDPOINT],
        "ac_cost": AC_COST_PREMIUM,
    },
}


def _ensure_fal():
    key = os.getenv("FAL_AI_KEY", "").strip()
    if not key:
        raise I2VError("FAL_AI_KEY not set in env")
    os.environ["FAL_KEY"] = key  # fal_client reads this
    return key


def _is_content_policy_error(exc: Exception) -> bool:
    """Match Bytedance's partner_validation_failed + Pixverse moderation rejects."""
    msg = str(exc).lower()
    return any(s in msg for s in (
        "content_policy_violation",
        "partner_validation_failed",
        "sensitive content",
        "content policy",
    ))


def _build_args(endpoint: str, motion_prompt: str, image_url: str,
                duration_sec: int, aspect_ratio: str) -> dict:
    """Per-endpoint arg shape. Seedance wants generate_audio=False to dodge
    its audio classifier; Pixverse + Kling have a leaner schema."""
    if endpoint == SEEDANCE_ENDPOINT:
        return {
            "prompt": motion_prompt,
            "image_url": image_url,
            "duration": str(duration_sec),
            "aspect_ratio": aspect_ratio,
            "generate_audio": False,
        }
    if endpoint == PIXVERSE_V6_ENDPOINT:
        return {
            "prompt": motion_prompt,
            "image_url": image_url,
            "duration": str(duration_sec),
            "aspect_ratio": aspect_ratio,
            "negative_prompt": _NEG_VIDEO,
        }
    if endpoint == KLING_PRO_ENDPOINT:
        return {
            "prompt": motion_prompt,
            "image_url": image_url,
            "duration": str(duration_sec),
            "aspect_ratio": aspect_ratio,
            "negative_prompt": _NEG_VIDEO,
        }
    return {
        "prompt": motion_prompt,
        "image_url": image_url,
        "duration": str(duration_sec),
        "aspect_ratio": aspect_ratio,
    }


def _queue_result(endpoint: str, args: dict, *, timeout_sec: int) -> dict:
    """Submit a FAL queue job and poll slowly enough for multi-scene renders."""
    handle = fal_client.submit(endpoint, arguments=args)
    request_id = getattr(handle, "request_id", None)
    if not request_id:
        raise I2VError(f"{endpoint} returned no request_id")

    deadline = time.monotonic() + max(30, timeout_sec)
    interval = max(1.0, FAL_I2V_POLL_INTERVAL_SEC)
    max_interval = max(interval, FAL_I2V_POLL_MAX_INTERVAL_SEC)
    last_status = None

    while time.monotonic() < deadline:
        status = fal_client.status(endpoint, request_id, with_logs=False)
        last_status = status
        status_name = status.__class__.__name__.lower()
        if status_name == "completed":
            response_url = getattr(handle, "response_url", "")
            if response_url:
                response = handle.client.get(response_url, timeout=120)
                response.raise_for_status()
                payload = response.json()
            else:
                payload = fal_client.result(endpoint, request_id)
            if not isinstance(payload, dict):
                raise I2VError(f"{endpoint} returned non-object result: {payload!r}")
            return payload
        if status_name in {"failed", "canceled", "cancelled"}:
            raise I2VError(f"{endpoint} {status_name} on {request_id}: {status}")

        time.sleep(interval)
        interval = min(max_interval, interval + 2)

    raise I2VError(
        f"{endpoint} timed out after {timeout_sec}s on request {request_id}; "
        f"last_status={last_status}"
    )


def list_video_models() -> list[dict[str, object]]:
    """Selectable i2v models for Skeleton AI (image stills are always canonical edit)."""
    return [
        {
            "key": "seedance",
            "label": "Seedance 2.0",
            "description": "Default. Auto-falls back to Pixverse on content-policy flags.",
            "ac_cost": AC_COST_STANDARD,
            "image_model": "seedream_v45_edit (locked — not selectable)",
        },
        {
            "key": "pixverse",
            "label": "Pixverse V6",
            "description": "Permissive moderation when Seedance blocks skeleton scenes.",
            "ac_cost": AC_COST_STANDARD,
            "image_model": "seedream_v45_edit (locked — not selectable)",
        },
        {
            "key": "kling_pro",
            "label": "Kling 2.1 Pro",
            "description": "Best motion quality; higher AC per short.",
            "ac_cost": AC_COST_PREMIUM,
            "image_model": "seedream_v45_edit (locked — not selectable)",
        },
    ]


def resolve_video_model_chain(
    *,
    video_model: str | None = None,
    tier: str = "standard",
) -> tuple[list[str], str]:
    """Return (endpoint chain, resolved video_model key)."""
    vm = (video_model or "").strip().lower()
    if not vm:
        vm = "kling_pro" if tier == "premium" else "seedance"
    if vm in ("premium", "standard"):
        vm = "kling_pro" if vm == "premium" else "seedance"
    spec = VIDEO_MODELS.get(vm)
    if not spec:
        raise I2VError(
            f"unknown video_model {video_model!r}. "
            f"valid: {sorted(VIDEO_MODELS.keys())}"
        )
    return list(spec["endpoints"]), vm  # type: ignore[arg-type]


def ac_cost_for_video_model(video_model: str | None = None, *, tier: str = "standard") -> int:
    _, vm = resolve_video_model_chain(video_model=video_model, tier=tier)
    return int(VIDEO_MODELS[vm]["ac_cost"])  # type: ignore[arg-type]


def generate(
    still_path: Path,
    motion_prompt: str,
    out_path: Path,
    *,
    tier: str = "standard",
    video_model: str | None = None,
    duration_sec: int = 5,
    aspect_ratio: str = "9:16",
) -> Path:
    """
    Animate a still into a short clip.

    video_model: seedance | pixverse | kling_pro (preferred). tier is legacy fallback.
    """
    out_path = Path(out_path)
    if out_path.exists() and out_path.stat().st_size > 1024:
        return out_path

    _ensure_fal()
    image_url = fal_client.upload_file(str(still_path))

    chain, _vm = resolve_video_model_chain(video_model=video_model, tier=tier)

    last_exc: Exception | None = None
    used_endpoint: str | None = None
    result: dict | None = None
    for endpoint in chain:
        args = _build_args(endpoint, motion_prompt, image_url, duration_sec, aspect_ratio)
        try:
            result = _queue_result(endpoint, args, timeout_sec=FAL_SUBSCRIBE_TIMEOUT_SEC)
            used_endpoint = endpoint
            break
        except Exception as e:
            last_exc = e
            if _is_content_policy_error(e) and endpoint != chain[-1]:
                # Try next model in the chain.
                print(f"  [i2v] {endpoint} flagged content_policy on {still_path.name}; falling back...")
                continue
            raise I2VError(f"{endpoint} failed on {still_path.name}: {e}") from e

    if result is None:
        raise I2VError(f"all i2v endpoints failed on {still_path.name}: {last_exc}")

    video_url = (result.get("video") or {}).get("url") or result.get("video_url")
    if not video_url:
        raise I2VError(f"{used_endpoint} returned no video URL: {result}")

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
