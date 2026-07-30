"""FAL-only image-to-video engine for Studio and Skeleton AI."""
from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Callable

import httpx

try:
    import fal_client
except Exception:  # pragma: no cover - optional when simulation mode is active
    fal_client = None  # type: ignore[assignment]

from studio_agent import production_budget, production_costs

from . import render_simulation
from .fal_auth import require_fal_key

FAL_SUBSCRIBE_TIMEOUT_SEC = int(os.getenv("FAL_I2V_SUBSCRIBE_TIMEOUT_SEC", "600"))
FAL_I2V_POLL_INTERVAL_SEC = float(os.getenv("FAL_I2V_POLL_INTERVAL_SEC", "5"))
FAL_I2V_POLL_MAX_INTERVAL_SEC = float(os.getenv("FAL_I2V_POLL_MAX_INTERVAL_SEC", "15"))

SEEDANCE_ENDPOINT = "bytedance/seedance-2.0/image-to-video"
PIXVERSE_V6_ENDPOINT = "fal-ai/pixverse/v6/image-to-video"
KLING_PRO_ENDPOINT = "fal-ai/kling-video/v2.1/pro/image-to-video"
LTX_098_ENDPOINT = "fal-ai/ltxv-13b-098-distilled/image-to-video"

# Premium positioning: Kling 2.1 Pro is the default motion lane — it is both the
# highest-quality FAL i2v model and ~3x cheaper per second than Seedance
# ($0.098/s vs $0.3024/s), so it is strictly better for the no-artifacting bar.
DEFAULT_FAL_VIDEO_MODEL = "kling_pro"
LEGACY_NON_FAL_VIDEO_MODELS = {
    "grok_imagine_video": DEFAULT_FAL_VIDEO_MODEL,
    "grok-imagine-video": DEFAULT_FAL_VIDEO_MODEL,
    "xai:grok-imagine-video": DEFAULT_FAL_VIDEO_MODEL,
    "grok_imagine_video_15": DEFAULT_FAL_VIDEO_MODEL,
    "grok-imagine-video-1.5": DEFAULT_FAL_VIDEO_MODEL,
    "xai:grok-imagine-video-1.5": DEFAULT_FAL_VIDEO_MODEL,
    "grok_imagine_video_15_1080p": DEFAULT_FAL_VIDEO_MODEL,
    "grok-imagine-video-1.5:1080p": DEFAULT_FAL_VIDEO_MODEL,
    "xai:grok-imagine-video-1.5:1080p": DEFAULT_FAL_VIDEO_MODEL,
}

STANDARD_FALLBACK_CHAIN = [SEEDANCE_ENDPOINT, PIXVERSE_V6_ENDPOINT]
# The default lane must keep a permissive second hop. Content-policy rejections
# are routine on FAL, and a single-endpoint default would turn a recoverable
# moderation bounce into a failed scene. Pixverse is the permissive lane, and
# clip QA still gates whatever comes back, so a fallback can never smuggle
# artifacted motion into a delivered video.
KLING_PRO_FALLBACK_CHAIN = [KLING_PRO_ENDPOINT, PIXVERSE_V6_ENDPOINT]
AC_COST_STANDARD = 5
AC_COST_PREMIUM = 7


class I2VError(RuntimeError):
    pass


class I2VRouteChanged(RuntimeError):
    """The creator changed the selected media route before fallback dispatch."""


VIDEO_MODELS: dict[str, dict[str, object]] = {
    "seedance": {
        "label": "Seedance 2.0",
        "description": "Legacy FAL lane; falls back to Pixverse on policy rejection.",
        "endpoints": list(STANDARD_FALLBACK_CHAIN),
        "ac_cost": AC_COST_STANDARD,
    },
    "pixverse": {
        "label": "Pixverse V6",
        "description": "Permissive FAL moderation lane for difficult scenes.",
        "endpoints": [PIXVERSE_V6_ENDPOINT],
        "ac_cost": AC_COST_STANDARD,
    },
    "kling_pro": {
        "label": "Kling 2.1 Pro",
        "description": "Default FAL lane: highest quality, falls back to Pixverse on policy rejection.",
        "endpoints": list(KLING_PRO_FALLBACK_CHAIN),
        # Deliberately AC_COST_STANDARD, not AC_COST_PREMIUM. This is now the lane
        # every standard-tier job resolves to, and skeleton_ai_router reserves
        # AC_COST_STANDARD for those jobs. Leaving it at AC_COST_PREMIUM made the
        # settled cost (7) exceed the reservation (5) on every short. Kling Pro is
        # also genuinely cheaper than the model it replaced ($0.098/s vs $0.3024/s),
        # so charging more AC for it was backwards.
        "ac_cost": AC_COST_STANDARD,
    },
    "ltx_budget": {
        "label": "LTX 0.9.8 Budget",
        "description": "Lowest-cost full-animation FAL lane.",
        "endpoints": [LTX_098_ENDPOINT],
        "ac_cost": 3,
    },
}


def normalize_fal_video_model_id(value: str | None, *, tier: str = "standard") -> str:
    """Resolve current and legacy selections to an explicit FAL model key."""
    raw = str(value or "").strip().lower().replace(" ", "_")
    if not raw:
        return "kling_pro" if str(tier or "").lower() == "premium" else DEFAULT_FAL_VIDEO_MODEL
    if raw in {"premium", "standard"}:
        return "kling_pro" if raw == "premium" else DEFAULT_FAL_VIDEO_MODEL
    return LEGACY_NON_FAL_VIDEO_MODELS.get(raw, raw)


def _ensure_fal() -> str:
    try:
        key = require_fal_key("image-to-video")
    except RuntimeError as exc:
        raise I2VError(str(exc)) from exc
    if fal_client is None:
        raise I2VError("FAL client is unavailable for image-to-video")
    return key


def _require_current_fallback_route(
    fallback_guard: Callable[[], bool] | None,
) -> None:
    """Fail closed before spending on a lower-priority FAL request."""
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
        raise I2VRouteChanged("Media route changed before video fallback dispatch")


def _is_content_policy_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(
        marker in message
        for marker in (
            "content_policy_violation",
            "partner_validation_failed",
            "sensitive content",
            "content policy",
            "content moderation",
            "rejected by content",
            "moderation",
            "unsafe",
            "violat",
        )
    )


def _silent_motion_prompt(motion_prompt: str) -> str:
    prompt = str(motion_prompt or "Subtle idle micro-motion with a stable camera.").strip()
    guard = "SILENT visual-only. No dialogue, speech, singing, music, or generated audio. "
    return (guard + prompt)[:300]


def _verify_silent_output(path: Path) -> dict[str, object]:
    """Fail closed unless ffprobe proves the generated clip has no audio stream."""
    try:
        completed = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "stream=index,codec_type",
                "-of",
                "json",
                str(Path(path)),
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise I2VError(f"Could not verify generated clip silence: {exc}") from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "ffprobe failed").strip()[:300]
        raise I2VError(f"Could not verify generated clip silence: {detail}")
    try:
        payload = json.loads(completed.stdout or "{}")
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise I2VError("Could not parse generated clip stream metadata") from exc
    streams = payload.get("streams") if isinstance(payload, dict) else None
    if not isinstance(streams, list):
        raise I2VError("Generated clip stream metadata is missing")
    video_streams = [row for row in streams if isinstance(row, dict) and row.get("codec_type") == "video"]
    audio_streams = [row for row in streams if isinstance(row, dict) and row.get("codec_type") == "audio"]
    if not video_streams:
        raise I2VError("Generated clip has no verified video stream")
    if audio_streams:
        raise I2VError("Generated clip still contains an audio stream after stripping")
    return {
        "status": "pass",
        "pass": True,
        "summary": "Verified video stream with no audio stream",
        "video_streams": len(video_streams),
        "audio_streams": 0,
    }


#: The clip lengths the FAL video lanes actually accept. These endpoints take
#: `duration` as a string and bill per whole tier, so an arbitrary number is
#: both a rejection risk and, when rounded up, double the price.
SUPPORTED_CLIP_SECONDS = (5, 10)


def normalize_clip_seconds(duration_sec: float) -> int:
    """Snap a beat length to the shortest supported clip that covers it.

    Callers derive beat durations from narration timing, so they ask for things
    like 5.5s or 6.0s. Truncating is wrong (the clip comes up short) and always
    rounding up is expensive (a 5.1s beat buys a 10s clip). Snapping to the
    shortest covering tier keeps the request valid and the price minimal.
    """
    requested = float(duration_sec or 0)
    for supported in SUPPORTED_CLIP_SECONDS:
        if requested <= supported + 1e-6:
            return supported
    return SUPPORTED_CLIP_SECONDS[-1]


def _build_args(
    endpoint: str,
    motion_prompt: str,
    image_url: str,
    duration_sec: int,
    aspect_ratio: str,
) -> dict:
    """Build endpoint-specific FAL arguments while disabling generated audio."""
    prompt = _silent_motion_prompt(motion_prompt)
    duration_sec = normalize_clip_seconds(duration_sec)
    if endpoint == SEEDANCE_ENDPOINT:
        return {
            "prompt": prompt,
            "image_url": image_url,
            "duration": str(duration_sec),
            "aspect_ratio": aspect_ratio,
            "resolution": "720p",
            "generate_audio": False,
        }
    if endpoint == PIXVERSE_V6_ENDPOINT:
        return {
            "prompt": prompt,
            "image_url": image_url,
            "duration": str(duration_sec),
            "aspect_ratio": aspect_ratio,
            "resolution": "720p",
            "generate_audio_switch": False,
            "negative_prompt": _NEG_VIDEO,
        }
    if endpoint == KLING_PRO_ENDPOINT:
        return {
            "prompt": prompt,
            "image_url": image_url,
            "duration": str(duration_sec),
            "aspect_ratio": aspect_ratio,
            "negative_prompt": _NEG_VIDEO,
        }
    if endpoint == LTX_098_ENDPOINT:
        fps = 24
        return {
            "prompt": prompt,
            "image_url": image_url,
            "negative_prompt": _NEG_VIDEO,
            "resolution": "720p",
            "aspect_ratio": aspect_ratio,
            "num_frames": max(121, int(duration_sec) * fps),
            "frame_rate": fps,
            "expand_prompt": False,
            "enable_detail_pass": False,
        }
    raise I2VError(f"unsupported FAL image-to-video endpoint {endpoint!r}")


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
    """Return only runnable FAL image-to-video selections."""
    return [
        {
            "key": key,
            "label": spec["label"],
            "description": spec["description"],
            "ac_cost": spec["ac_cost"],
            "provider": "fal",
            "image_model": "FAL Seedream (selected separately)",
        }
        for key, spec in VIDEO_MODELS.items()
    ]


def resolve_video_model_chain(
    *,
    video_model: str | None = None,
    tier: str = "standard",
) -> tuple[list[str], str]:
    """Return an all-FAL endpoint chain and its normalized model key."""
    model_id = normalize_fal_video_model_id(video_model, tier=tier)
    spec = VIDEO_MODELS.get(model_id)
    if not spec:
        raise I2VError(
            f"unknown video_model {video_model!r}. valid: {sorted(VIDEO_MODELS.keys())}"
        )
    return list(spec["endpoints"]), model_id  # type: ignore[arg-type]


def ac_cost_for_video_model(video_model: str | None = None, *, tier: str = "standard") -> int:
    _, model_id = resolve_video_model_chain(video_model=video_model, tier=tier)
    return int(VIDEO_MODELS[model_id]["ac_cost"])  # type: ignore[arg-type]


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
    budget_workspace: Path | None = None,
    budget_attempt_recorder: Callable[[str, float, int], None] | None = None,
    disabled_providers: set[str] | None = None,
    capacity_reporter: Callable[[str, Exception, str], None] | None = None,
) -> Path:
    """Animate a still through FAL and strip any returned audio track."""
    requested_video_model = str(video_model or tier or "").strip().lower()
    chain, resolved_video_model = resolve_video_model_chain(video_model=video_model, tier=tier)
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
            out_path.with_suffix(out_path.suffix + ".fal.json").write_text(
                json.dumps(
                    {
                        "provider": "fal",
                        "endpoint": "simulation/i2v",
                        "request_id": "",
                        "duration_sec": int(duration_sec),
                        "video_model": resolved_video_model,
                        "requested_video_model": requested_video_model,
                        "simulated": True,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
        except Exception:
            pass
        return out_path

    disabled = {str(value or "").strip().lower() for value in (disabled_providers or set())}
    if "fal" in disabled:
        raise I2VError("FAL image-to-video is disabled; no media provider remains")

    _ensure_fal()
    image_url = str(fal_client.upload_file(str(still_path)) or "").strip()
    if not image_url:
        raise I2VError("FAL still upload returned no image URL")

    last_error: Exception | None = None
    used_endpoint: str | None = None
    result: dict | None = None
    failed_endpoints: list[dict[str, str]] = []
    in_flight_estimated_usd = 0.0
    for endpoint_index, endpoint in enumerate(chain):
        attempt_cost_value = 0.0
        try:
            if endpoint_index:
                _require_current_fallback_route(fallback_guard)

            attempt_cost, _attempt_note, _attempt_key = production_costs.price_fal_video(
                endpoint,
                seconds=float(duration_sec),
            )
            attempt_cost_value = float(attempt_cost)
            if budget_attempt_recorder is not None:
                budget_attempt_recorder(
                    str(endpoint),
                    attempt_cost_value,
                    endpoint_index + 1,
                )
            elif budget_workspace is not None:
                production_budget.enforce_incremental_spend(
                    Path(budget_workspace),
                    attempt_cost,
                    operation="image_to_video",
                    provider="fal",
                    model=str(endpoint),
                    in_flight_usd=in_flight_estimated_usd,
                )

            fallback_motion = motion_prompt
            if last_error is not None and _is_content_policy_error(last_error):
                fallback_motion = (
                    "Subtle idle motion only, gentle breathing, slight weight shift, stable camera. "
                    "Safe PG-13, no violence, no intimacy, no text. "
                    + str(motion_prompt or "")
                )
            args = _build_args(
                endpoint,
                fallback_motion,
                image_url,
                duration_sec,
                aspect_ratio,
            )
            result = _queue_result(endpoint, args, timeout_sec=FAL_SUBSCRIBE_TIMEOUT_SEC)
            used_endpoint = endpoint
            break
        except I2VRouteChanged:
            raise
        except production_budget.BudgetExceededError:
            raise
        except Exception as exc:
            last_error = exc
            if capacity_reporter is not None:
                try:
                    capacity_reporter("fal", exc, str(endpoint))
                except Exception:
                    pass
            failed_endpoints.append(
                {
                    "endpoint": str(endpoint),
                    "error_class": (
                        "content_policy" if _is_content_policy_error(exc) else exc.__class__.__name__
                    ),
                    "detail": str(exc)[:300],
                }
            )
            in_flight_estimated_usd += attempt_cost_value
            retryable = (
                _is_content_policy_error(exc)
                or "400" in str(exc)
                or "invalid argument" in str(exc).lower()
            )
            if endpoint != chain[-1] and retryable:
                continue
            raise I2VError(f"{endpoint} failed on {Path(still_path).name}: {exc}") from exc

    if result is None:
        raise I2VError(f"all i2v endpoints failed on {Path(still_path).name}: {last_error}")

    video = result.get("video") if isinstance(result.get("video"), dict) else {}
    video_url = video.get("url") or result.get("video_url") or result.get("url")
    if not video_url:
        raise I2VError(f"{used_endpoint} returned no video URL: {result}")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _download(str(video_url), out_path)
    try:
        from skeleton_ai.compose import strip_clip_audio

        strip_clip_audio(out_path)
        audio_silence = _verify_silent_output(out_path)
    except Exception as exc:
        out_path.unlink(missing_ok=True)
        if isinstance(exc, I2VError):
            raise
        raise I2VError(f"Could not produce a verified-silent i2v clip: {exc}") from exc
    try:
        out_path.with_suffix(out_path.suffix + ".fal.json").write_text(
            json.dumps(
                {
                    "provider": "fal",
                    "endpoint": used_endpoint,
                    "request_id": result.get("_fal_request_id"),
                    "duration_sec": int(duration_sec),
                    "video_model": resolved_video_model,
                    "video_url": video_url,
                    "requested_video_model": requested_video_model,
                    "model_migrated_from": (
                        requested_video_model
                        if requested_video_model != resolved_video_model
                        else None
                    ),
                    "fallback_from": failed_endpoints[0]["endpoint"] if failed_endpoints else None,
                    "provider_failures": failed_endpoints,
                    "audio_stripped": True,
                    "audio_silence": audio_silence,
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
    "character morphing, body warping, frozen pose, text overlays, dialogue, music, audio"
)


def _download(url: str, dest: Path, retries: int = 3) -> None:
    last_error = None
    for attempt in range(retries):
        try:
            with httpx.stream("GET", url, timeout=300) as response:
                if response.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"server {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                with open(dest, "wb") as output:
                    for chunk in response.iter_bytes(chunk_size=1024 * 256):
                        output.write(chunk)
            return
        except (httpx.HTTPError, httpx.RequestError) as exc:
            last_error = exc
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
    if last_error:
        raise last_error


__all__ = [
    "DEFAULT_FAL_VIDEO_MODEL",
    "I2VError",
    "I2VRouteChanged",
    "LEGACY_NON_FAL_VIDEO_MODELS",
    "VIDEO_MODELS",
    "ac_cost_for_video_model",
    "generate",
    "list_video_models",
    "normalize_fal_video_model_id",
    "resolve_video_model_chain",
]
