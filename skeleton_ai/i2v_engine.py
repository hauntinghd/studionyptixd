"""
i2v engine — multi-model fallback for skeleton + character imagery.

Per the i2v bake-off (2026-05-04) + Marvel-vs-DC content-policy run (2026-05-06):
  - Seedance 2.0   → strong character preservation; premium token-priced lane
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
import base64
import json
import mimetypes
import os
import time
from pathlib import Path
from typing import Callable

import httpx
try:
    import fal_client
except Exception:  # pragma: no cover - optional when simulation mode is active
    fal_client = None  # type: ignore[assignment]

from .fal_auth import require_fal_key
from . import render_simulation

FAL_SUBSCRIBE_TIMEOUT_SEC = int(os.getenv("FAL_I2V_SUBSCRIBE_TIMEOUT_SEC", "600"))
FAL_I2V_POLL_INTERVAL_SEC = float(os.getenv("FAL_I2V_POLL_INTERVAL_SEC", "5"))
FAL_I2V_POLL_MAX_INTERVAL_SEC = float(os.getenv("FAL_I2V_POLL_MAX_INTERVAL_SEC", "15"))


class I2VError(RuntimeError):
    pass


class I2VRouteChanged(RuntimeError):
    """The creator changed the selected media route before fallback dispatch."""


def _require_current_fallback_route(
    fallback_guard: Callable[[], bool] | None,
) -> None:
    """Fail closed before spending on a lower-priority provider request."""

    if fallback_guard is None:
        return
    try:
        route_is_current = fallback_guard()
    except I2VRouteChanged:
        raise
    except Exception as exc:
        raise I2VRouteChanged(
            "Could not verify the current media route before video fallback"
        ) from exc
    if route_is_current is not True:
        raise I2VRouteChanged(
            "Media route changed before video fallback dispatch"
        )


SEEDANCE_ENDPOINT = "bytedance/seedance-2.0/image-to-video"
PIXVERSE_V6_ENDPOINT = "fal-ai/pixverse/v6/image-to-video"
KLING_PRO_ENDPOINT = "fal-ai/kling-video/v2.1/pro/image-to-video"
LTX_098_ENDPOINT = "fal-ai/ltxv-13b-098-distilled/image-to-video"
XAI_VIDEO_ENDPOINT = "https://api.x.ai/v1/videos/generations"
XAI_VIDEO_STATUS_ENDPOINT = "https://api.x.ai/v1/videos/{request_id}"
XAI_GROK_VIDEO_ENDPOINT = "xai:grok-imagine-video"
XAI_GROK_VIDEO_15_ENDPOINT = "xai:grok-imagine-video-1.5"
XAI_GROK_VIDEO_15_1080P_ENDPOINT = "xai:grok-imagine-video-1.5:1080p"
XAI_USD_TICKS_PER_DOLLAR = 10_000_000_000

# Standard tier fallback chain — first model that doesn't 422 wins.
STANDARD_FALLBACK_CHAIN = [SEEDANCE_ENDPOINT, PIXVERSE_V6_ENDPOINT]

AC_COST_STANDARD = 5
AC_COST_PREMIUM = 7


def _is_provider_credit_limit_error(exc: Exception) -> bool:
    """Recognize an exhausted provider balance without masking auth defects."""

    text = str(exc or "").strip().lower()
    credit_signal = any(
        phrase in text
        for phrase in (
            "used all available credits",
            "reached its monthly spending limit",
            "monthly spending limit",
            "credit balance",
            "insufficient credits",
            "insufficient balance",
            "billing limit",
            "spending limit",
        )
    )
    return credit_signal and any(signal in text for signal in ("403", "permission-denied", "credit", "billing", "spending"))

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
    "ltx_budget": {
        "label": "LTX 0.9.8 Budget",
        "description": "Lowest-cost full animation lane. Use when budget matters more than premium motion.",
        "endpoints": [LTX_098_ENDPOINT],
        "ac_cost": 3,
    },
    "grok_imagine_video": {
        "label": "Grok Imagine Video",
        "description": "xAI i2v first; auto-falls back to Seedance→Pixverse on moderation blocks.",
        # xAI often rejects psychology/skeleton motion; always keep a FAL rescue path.
        "endpoints": [XAI_GROK_VIDEO_ENDPOINT, SEEDANCE_ENDPOINT, PIXVERSE_V6_ENDPOINT],
        "ac_cost": AC_COST_STANDARD,
    },
    "grok_imagine_video_15": {
        "label": "Grok Imagine Video 1.5",
        "description": "Higher-quality xAI i2v at 720p; falls back to Seedance→Pixverse.",
        "endpoints": [XAI_GROK_VIDEO_15_ENDPOINT, SEEDANCE_ENDPOINT, PIXVERSE_V6_ENDPOINT],
        "ac_cost": AC_COST_PREMIUM,
    },
    "grok_imagine_video_15_1080p": {
        "label": "Grok Imagine Video 1.5 1080p",
        "description": "1080p xAI i2v for final tests; falls back to Seedance→Pixverse.",
        "endpoints": [XAI_GROK_VIDEO_15_1080P_ENDPOINT, SEEDANCE_ENDPOINT, PIXVERSE_V6_ENDPOINT],
        "ac_cost": 10,
    },
}


def _ensure_fal():
    try:
        return require_fal_key("image-to-video")
    except RuntimeError as exc:
        raise I2VError(str(exc)) from exc


def _ensure_xai() -> str:
    api_key = str(os.getenv("XAI_API_KEY") or "").strip()
    if not api_key:
        raise I2VError("xAI image-to-video requires XAI_API_KEY")
    return api_key


def _is_content_policy_error(exc: Exception) -> bool:
    """Match Seedance/Pixverse/xAI moderation rejects so we can fall back."""
    msg = str(exc).lower()
    return any(s in msg for s in (
        "content_policy_violation",
        "partner_validation_failed",
        "sensitive content",
        "content policy",
        "content moderation",
        "rejected by content",
        "moderation",
        "unsafe",
        "violat",
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
            "resolution": "720p",
            "generate_audio": False,
        }
    if endpoint == PIXVERSE_V6_ENDPOINT:
        return {
            "prompt": motion_prompt,
            "image_url": image_url,
            "duration": str(duration_sec),
            "aspect_ratio": aspect_ratio,
            "resolution": "720p",
            "generate_audio_switch": False,
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
    if endpoint == LTX_098_ENDPOINT:
        fps = 24
        return {
            "prompt": motion_prompt,
            "image_url": image_url,
            "negative_prompt": _NEG_VIDEO,
            "resolution": "720p",
            "aspect_ratio": aspect_ratio,
            "num_frames": max(121, int(duration_sec) * fps),
            "frame_rate": fps,
            "expand_prompt": False,
            "enable_detail_pass": False,
        }
    return {
        "prompt": motion_prompt,
        "image_url": image_url,
        "duration": str(duration_sec),
        "aspect_ratio": aspect_ratio,
    }


def _image_data_uri(path: Path) -> str:
    mime = mimetypes.guess_type(str(path))[0] or "image/png"
    data = base64.b64encode(Path(path).read_bytes()).decode("ascii")
    return f"data:{mime};base64,{data}"


def _xai_cost_usd(payload: dict) -> float | None:
    try:
        usage = payload.get("usage") if isinstance(payload, dict) else None
        ticks = usage.get("cost_in_usd_ticks") if isinstance(usage, dict) else None
        if ticks is None:
            return None
        return max(0.0, float(ticks) / XAI_USD_TICKS_PER_DOLLAR)
    except Exception:
        return None


def _xai_endpoint_config(endpoint: str) -> tuple[str, str]:
    if endpoint == XAI_GROK_VIDEO_15_1080P_ENDPOINT:
        return "grok-imagine-video-1.5", "1080p"
    if endpoint == XAI_GROK_VIDEO_15_ENDPOINT:
        return "grok-imagine-video-1.5", "720p"
    if endpoint == XAI_GROK_VIDEO_ENDPOINT:
        return "grok-imagine-video", "720p"
    raise I2VError(f"unknown xAI video endpoint {endpoint!r}")


def _xai_i2v_result(endpoint: str, motion_prompt: str, still_path: Path, *, duration_sec: int, aspect_ratio: str) -> dict:
    api_key = _ensure_xai()
    model, resolution = _xai_endpoint_config(endpoint)
    duration = max(1, min(15, int(duration_sec or 5)))
    guarded_motion = str(motion_prompt or "Subtle idle micro-motion with a stable camera.").strip()
    if "silent" not in guarded_motion.lower() and "no talking" not in guarded_motion.lower():
        guarded_motion = (
            "SILENT visual-only — no talking, no jaw/mouth motion, no dialogue, no music. "
            + guarded_motion
        )
    guarded_motion = guarded_motion[:300]
    payload = {
        "model": model,
        "prompt": guarded_motion,
        "image": {"url": _image_data_uri(still_path)},
        "duration": duration,
        "aspect_ratio": aspect_ratio,
        "resolution": resolution,
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    with httpx.Client(timeout=120) as client:
        response = client.post(XAI_VIDEO_ENDPOINT, headers=headers, json=payload)
        if response.status_code >= 400:
            detail = response.text.replace(api_key, "[redacted]")[:500]
            raise I2VError(f"{model} {response.status_code}: {detail}")
        body = response.json()
        request_id = str(body.get("request_id") or "").strip()
        if not request_id:
            raise I2VError(f"{model} returned no request_id: {body}")

        deadline = time.monotonic() + max(60, FAL_SUBSCRIBE_TIMEOUT_SEC)
        while time.monotonic() < deadline:
            status_response = client.get(
                XAI_VIDEO_STATUS_ENDPOINT.format(request_id=request_id),
                headers={"Authorization": headers["Authorization"]},
            )
            if status_response.status_code >= 400:
                detail = status_response.text.replace(api_key, "[redacted]")[:500]
                raise I2VError(f"{model} status {status_response.status_code}: {detail}")
            status_body = status_response.json()
            status = str(status_body.get("status") or "").lower()
            if status in {"done", "completed", "succeeded", "success"}:
                status_body["_xai_model"] = model
                status_body["_xai_request_id"] = request_id
                status_body["_xai_resolution"] = resolution
                status_body["_xai_cost_usd"] = _xai_cost_usd(status_body)
                return status_body
            if status in {"failed", "expired", "cancelled", "canceled", "error"}:
                raise I2VError(f"{model} {status} on {request_id}: {status_body}")
            time.sleep(max(1.0, FAL_I2V_POLL_INTERVAL_SEC))
    raise I2VError(f"{model} timed out after {FAL_SUBSCRIBE_TIMEOUT_SEC}s on request {request_id}")


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
            payload["_fal_endpoint"] = endpoint
            payload["_fal_request_id"] = request_id
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
            "key": "ltx_budget",
            "label": "LTX 0.9.8 Budget",
            "description": "Cheapest full-animation lane; test before using on premium client work.",
            "ac_cost": 3,
            "image_model": "seedream_v45_edit (locked - not selectable)",
        },
        {
            "key": "kling_pro",
            "label": "Kling 2.1 Pro",
            "description": "Best motion quality; higher AC per short.",
            "ac_cost": AC_COST_PREMIUM,
            "image_model": "seedream_v45_edit (locked — not selectable)",
        },
        {
            "key": "grok_imagine_video",
            "label": "Grok Imagine Video",
            "description": "Low-cost xAI image-to-video lane for cheaper motion tests.",
            "ac_cost": AC_COST_STANDARD,
            "image_model": "user-selected image model",
        },
        {
            "key": "grok_imagine_video_15",
            "label": "Grok Imagine Video 1.5",
            "description": "Higher-quality xAI image-to-video at 720p.",
            "ac_cost": AC_COST_PREMIUM,
            "image_model": "user-selected image model",
        },
        {
            "key": "grok_imagine_video_15_1080p",
            "label": "Grok Imagine Video 1.5 1080p",
            "description": "1080p xAI image-to-video for final tests only.",
            "ac_cost": 10,
            "image_model": "user-selected image model",
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
    fallback_guard: Callable[[], bool] | None = None,
) -> Path:
    """
    Animate a still into a short clip.

    video_model: seedance | pixverse | kling_pro (preferred). tier is legacy fallback.
    """
    # One hard prompt limit for xAI and every FAL fallback lane.
    motion_prompt = str(motion_prompt or "").strip()[:300]
    out_path = Path(out_path)
    if out_path.exists() and out_path.stat().st_size > 1024:
        return out_path

    if render_simulation.enabled():
        render_simulation.write_video(
            out_path,
            still_path=still_path,
            duration_sec=float(duration_sec or 5),
        )
        try:
            (out_path.with_suffix(out_path.suffix + ".fal.json")).write_text(
                json.dumps(
                    {
                        "endpoint": "simulation/i2v",
                        "request_id": "",
                        "duration_sec": int(duration_sec),
                        "video_model": video_model or tier,
                        "simulated": True,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
        except Exception:
            pass
        return out_path

    chain, _vm = resolve_video_model_chain(video_model=video_model, tier=tier)
    image_url = ""

    last_exc: Exception | None = None
    used_endpoint: str | None = None
    result: dict | None = None
    for endpoint_index, endpoint in enumerate(chain):
        try:
            if str(endpoint).startswith("xai:"):
                if endpoint_index:
                    _require_current_fallback_route(fallback_guard)
                result = _xai_i2v_result(
                    endpoint,
                    motion_prompt,
                    still_path,
                    duration_sec=duration_sec,
                    aspect_ratio=aspect_ratio,
                )
            else:
                # Lazy FAL upload so xAI-first chains still work without FAL when xAI succeeds.
                if not image_url:
                    if endpoint_index:
                        _require_current_fallback_route(fallback_guard)
                    _ensure_fal()
                    image_url = fal_client.upload_file(str(still_path))
                # Soften motion prompt on fallback after a moderation reject.
                fb_motion = motion_prompt
                if last_exc is not None and _is_content_policy_error(last_exc):
                    fb_motion = (
                        "Subtle idle motion only: gentle breathing, slight weight shift, "
                        "stable camera. Safe PG-13. No violence, no intimacy, no text. "
                        + str(motion_prompt or "")
                    )[:300]
                args = _build_args(endpoint, fb_motion, image_url, duration_sec, aspect_ratio)
                if endpoint_index:
                    # Recheck after a potentially slow upload and immediately
                    # before the billable fallback generation request.
                    _require_current_fallback_route(fallback_guard)
                result = _queue_result(endpoint, args, timeout_sec=FAL_SUBSCRIBE_TIMEOUT_SEC)
            used_endpoint = endpoint
            break
        except I2VRouteChanged:
            raise
        except Exception as e:
            last_exc = e
            if endpoint != chain[-1] and (
                _is_content_policy_error(e)
                or _is_provider_credit_limit_error(e)
                or "400" in str(e)
                or "invalid argument" in str(e).lower()
            ):
                # Try next model in the chain (xAI moderation / Seedance policy → Pixverse).
                print(f"  [i2v] {endpoint} failed on {still_path.name}; falling back... ({e})")
                continue
            raise I2VError(f"{endpoint} failed on {still_path.name}: {e}") from e

    if result is None:
        raise I2VError(f"all i2v endpoints failed on {still_path.name}: {last_exc}")

    video_url = (result.get("video") or {}).get("url") or result.get("video_url") or result.get("url")
    if not video_url:
        raise I2VError(f"{used_endpoint} returned no video URL: {result}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    _download(video_url, out_path)
    # Grok Imagine Video bakes native talk/music; Studio VO is a later FAL step.
    try:
        from skeleton_ai.compose import strip_clip_audio

        strip_clip_audio(out_path)
    except Exception:
        pass
    try:
        (out_path.with_suffix(out_path.suffix + ".fal.json")).write_text(
            json.dumps(
                {
                    "endpoint": used_endpoint,
                    "request_id": result.get("_fal_request_id") or result.get("_xai_request_id"),
                    "duration_sec": int(duration_sec),
                    "video_model": _vm,
                    "video_url": video_url,
                    "xai_model": result.get("_xai_model"),
                    "xai_resolution": result.get("_xai_resolution"),
                    "xai_cost_usd": result.get("_xai_cost_usd"),
                    "audio_stripped": True,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
    except Exception:
        pass
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
