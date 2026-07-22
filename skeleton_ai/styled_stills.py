"""FAL Seedream still generation and reference-aware editing."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
try:
    import fal_client
except Exception:  # pragma: no cover - optional when simulation mode is active
    fal_client = None  # type: ignore[assignment]

from .fal_auth import require_fal_key
from .canonical_edit import _first_result_image_url, _queue_result
from . import render_simulation
from studio_agent.image_model_catalog import (
    normalize_seedream_model_id,
    seedream_endpoint,
    seedream_model_spec,
)

SEEDREAM_T2I_URL = "https://fal.run/fal-ai/bytedance/seedream/v4.5/text-to-image"
DEFAULT_FAL_IMAGE_MODEL = "seedream_edit"
LEGACY_NON_FAL_IMAGE_MODEL_IDS = frozenset(
    {
        "grok_imagine",
        "grok_imagine_standard",
        "grok-imagine-image",
        "grok-imagine-image-quality",
        "seedream_v5_lite_modal",
        "seedream5_lite_modal",
    }
)


class StyledStillError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        cost_usd: float | None = None,
        provider: str = "",
        operation: str = "",
    ) -> None:
        super().__init__(message)
        self.cost_usd = cost_usd
        self.provider = provider
        self.operation = operation


def normalize_fal_image_model_id(value: Any) -> str:
    """Map saved non-FAL selections to an explicit runnable FAL model."""
    raw = str(value or "").strip().lower().replace(" ", "_")
    if raw in {"seedream_v5_lite_modal", "seedream5_lite_modal"}:
        return "seedream_v5_lite"
    normalized = normalize_seedream_model_id(raw)
    spec = seedream_model_spec(normalized)
    if str(spec.get("provider") or "").lower() == "fal":
        return normalized
    return DEFAULT_FAL_IMAGE_MODEL


def _ensure_fal() -> None:
    try:
        require_fal_key("styled still generation")
    except RuntimeError as exc:
        raise StyledStillError(str(exc)) from exc
    if fal_client is None:
        raise StyledStillError("FAL client is unavailable for styled still generation")


def _download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with httpx.stream("GET", url, timeout=180) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=1024 * 256):
                f.write(chunk)


def _seedream_t2i_payload(
    model_id: str,
    *,
    prompt: str,
    negative_prompt: str,
    seed: int,
) -> dict[str, Any]:
    """Build only fields accepted by the selected Seedream generation API."""
    normalized = normalize_seedream_model_id(model_id) or "seedream_edit"
    payload: dict[str, Any] = {
        "prompt": str(prompt or "")[:759],
        "image_size": "portrait_16_9",
        "num_images": 1,
    }
    if normalized in {"seedream_v5_lite", "seedream_v5_lite_modal"}:
        payload["max_images"] = 1
        payload["enable_safety_checker"] = True
    elif normalized == "seedream_v4":
        # Seedream v4's published schema does not accept negative_prompt.
        payload["seed"] = int(seed)
        payload["enable_safety_checker"] = True
    else:
        payload["negative_prompt"] = str(negative_prompt or "")[:1500]
        payload["seed"] = int(seed)
        payload["enable_safety_checker"] = True
    return payload


def build_styled_scene_prompt(
    *,
    style_prefix: str,
    scene_action: str,
    outfit: str = "",
    topic: str = "",
    visual_brief: str = "",
) -> str:
    parts = [style_prefix.strip()]
    if visual_brief:
        parts.append(f"CREATIVE LOCK: {visual_brief.strip()}")
    if outfit:
        parts.append(f"WARDROBE: {outfit.strip()}")
    if topic:
        parts.append(f"TOPIC: {topic.strip()}")
    parts.append(f"SCENE: {scene_action.strip()}")
    parts.append("Vertical 9:16 frame, single clear focal subject, premium production quality.")
    return " ".join(p for p in parts if p)[:759]


def generate_still_t2i(
    prompt: str,
    out_path: Path,
    *,
    negative_prompt: str,
    seed: int = 420042,
    image_model_id: str = "",
) -> dict[str, Any]:
    out_path = Path(out_path)
    requested_model = str(image_model_id or "").strip().lower()
    normalized_seedream = normalize_fal_image_model_id(requested_model)
    if out_path.exists() and out_path.stat().st_size > 1024:
        return {
            "local_path": str(out_path),
            "provider": normalized_seedream,
            "provider_transport": "fal",
            "cached": True,
        }

    if render_simulation.enabled():
        render_simulation.write_still(out_path, label="Seedream T2I simulation")
        return {
            "local_path": str(out_path),
            "provider": "simulation_seedream_t2i",
            "selected_model": normalized_seedream,
            "seed": seed,
            "bytes": out_path.stat().st_size,
            "simulated": True,
        }

    model_spec = seedream_model_spec(normalized_seedream)
    if not model_spec:
        raise StyledStillError(f"FAL image model is unavailable: {normalized_seedream}")
    endpoint = seedream_endpoint(normalized_seedream, edit=False)
    if not endpoint:
        raise StyledStillError(f"Seedream generation model is unavailable: {normalized_seedream}")
    payload = _seedream_t2i_payload(
        normalized_seedream,
        prompt=prompt,
        negative_prompt=negative_prompt,
        seed=seed,
    )
    try:
        _ensure_fal()
        result = _queue_result(endpoint, payload, timeout_sec=300)
    except Exception as exc:
        if isinstance(exc, StyledStillError):
            raise
        raise StyledStillError(f"{normalized_seedream} generation failed: {exc}") from exc
    url = _first_result_image_url(result)
    if not url:
        raise StyledStillError(f"{normalized_seedream} generation returned no image URL")
    _download(url, out_path)
    return {
        "local_path": str(out_path),
        "cdn_url": url,
        "provider": normalized_seedream,
        "provider_transport": "fal",
        "seed": seed,
        "bytes": out_path.stat().st_size,
        "requested_model": requested_model or None,
        "model_migrated_from": (
            requested_model if requested_model and requested_model != normalized_seedream else None
        ),
    }


def generate_still_xai_edit(
    prompt: str,
    out_path: Path,
    *,
    reference_path: str | Path,
    image_model_id: str = DEFAULT_FAL_IMAGE_MODEL,
) -> dict[str, Any]:
    """Compatibility shim that migrates legacy xAI edits to FAL Seedream."""
    out_path = Path(out_path)
    ref = Path(reference_path)
    if not ref.is_file() or ref.stat().st_size <= 1024:
        raise StyledStillError("FAL image edit requires a usable source image")
    requested_model = str(image_model_id or "").strip().lower()
    normalized_model = normalize_fal_image_model_id(requested_model)
    if render_simulation.enabled():
        render_simulation.write_still(out_path, label="FAL image edit simulation")
        return {
            "local_path": str(out_path),
            "provider": "simulation_seedream_image_edit",
            "selected_model": normalized_model,
            "simulated": True,
            "cost_usd": 0.0,
        }
    _ensure_fal()
    endpoint = seedream_endpoint(normalized_model, edit=True)
    if not endpoint:
        raise StyledStillError(f"FAL image edit model is unavailable: {normalized_model}")
    try:
        reference_url = fal_client.upload_file(str(ref))
    except Exception as exc:
        raise StyledStillError(f"FAL reference upload failed: {exc}") from exc
    if not str(reference_url or "").strip():
        raise StyledStillError("FAL reference upload returned no image URL")
    payload = {
        "prompt": str(prompt or "")[:759],
        "image_urls": [str(reference_url)],
        "image_size": "auto_2K",
        "num_images": 1,
        "enable_safety_checker": True,
    }
    try:
        result = _queue_result(endpoint, payload, timeout_sec=300)
    except Exception as exc:
        raise StyledStillError(f"{normalized_model} edit failed: {exc}") from exc
    url = _first_result_image_url(result)
    if not url:
        raise StyledStillError(f"{normalized_model} edit returned no image URL")
    _download(url, out_path)
    return {
        "local_path": str(out_path),
        "cdn_url": url,
        "provider": normalized_model,
        "provider_transport": "fal",
        "bytes": out_path.stat().st_size,
        "requested_model": requested_model or None,
        "model_migrated_from": (
            requested_model if requested_model and requested_model != normalized_model else None
        ),
    }


def generate_still_reference_edit(
    prompt: str,
    out_path: Path,
    *,
    reference_paths: list[str],
    negative_prompt: str,
    seed: int = 420042,
) -> dict[str, Any]:
    """Create an ad still while locking product identity to supplied images."""
    out_path = Path(out_path)
    if out_path.exists() and out_path.stat().st_size > 1024:
        return {"local_path": str(out_path), "provider": "seedream_v45_product_edit", "cached": True}
    if render_simulation.enabled():
        render_simulation.write_still(out_path, label="Product edit simulation")
        return {
            "local_path": str(out_path),
            "provider": "simulation_seedream_product_edit",
            "seed": seed,
            "bytes": out_path.stat().st_size,
            "simulated": True,
        }
    _ensure_fal()
    urls: list[str] = []
    for raw in list(reference_paths or [])[:3]:
        path = Path(str(raw or ""))
        if path.is_file() and path.stat().st_size > 1024:
            urls.append(fal_client.upload_file(str(path)))
        elif str(raw).startswith(("http://", "https://")):
            urls.append(str(raw))
    if not urls:
        raise StyledStillError("product reference edit requires at least one usable product image")
    result = _queue_result(
        "fal-ai/bytedance/seedream/v4.5/edit",
        {
            "prompt": (
                "PRODUCT IDENTITY LOCK: preserve the exact product design, logo placement, colors, "
                "materials, proportions, screen UI, and packaging visible in the reference images. "
                "Do not invent a replacement product. " + str(prompt or "")
            )[:759],
            "image_urls": urls,
            "negative_prompt": str(negative_prompt or "")[:1500],
            "image_size": "auto_2K",
            "num_images": 1,
            "seed": int(seed),
        },
        timeout_sec=300,
    )
    images = list((result or {}).get("images") or [])
    url = str((images[0] or {}).get("url") or "").strip() if images else ""
    if not url:
        raise StyledStillError(f"product reference edit returned no image: {result!r}")
    _download(url, out_path)
    return {
        "local_path": str(out_path),
        "cdn_url": url,
        "provider": "seedream_v45_product_edit",
        "seed": seed,
        "bytes": out_path.stat().st_size,
    }
