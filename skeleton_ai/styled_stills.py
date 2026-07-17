"""Seedream v4.5 text-to-image stills for styled shortform (non-skeleton)."""
from __future__ import annotations

import os
import base64
import json
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
    modal_seedream_request_headers,
    normalize_seedream_model_id,
    seedream_endpoint,
    seedream_model_spec,
    seedream_provider,
)

SEEDREAM_T2I_URL = "https://fal.run/fal-ai/bytedance/seedream/v4.5/text-to-image"
XAI_IMAGE_URL = "https://api.x.ai/v1/images/generations"
XAI_IMAGE_EDIT_URL = "https://api.x.ai/v1/images/edits"
XAI_USD_TICKS_PER_DOLLAR = 10_000_000_000


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


def _xai_error_cost_usd(text: str) -> float | None:
    try:
        payload = json.loads(str(text or ""))
        usage = payload.get("usage") if isinstance(payload, dict) else None
        ticks = usage.get("cost_in_usd_ticks") if isinstance(usage, dict) else None
        if ticks is None:
            return None
        return max(0.0, float(ticks) / XAI_USD_TICKS_PER_DOLLAR)
    except Exception:
        return None


def _xai_payload_cost_usd(payload: dict[str, Any]) -> float | None:
    try:
        usage = payload.get("usage") if isinstance(payload, dict) else None
        ticks = usage.get("cost_in_usd_ticks") if isinstance(usage, dict) else None
        if ticks is None:
            return None
        return max(0.0, float(ticks) / XAI_USD_TICKS_PER_DOLLAR)
    except Exception:
        return None


def _data_uri(path: Path) -> str:
    suffix = path.suffix.lower()
    mime = "image/jpeg" if suffix in {".jpg", ".jpeg"} else "image/png"
    return f"data:{mime};base64,{base64.b64encode(path.read_bytes()).decode('ascii')}"


def _write_xai_image_response(response_json: dict[str, Any], out_path: Path) -> dict[str, Any]:
    data = (response_json or {}).get("data") or []
    item = (data[0] or {}) if data else {}
    b64 = str(item.get("b64_json") or "").strip()
    url = str(item.get("url") or "").strip()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if b64:
        out_path.write_bytes(base64.b64decode(b64))
    elif url:
        _download(url, out_path)
    else:
        raise StyledStillError(f"xAI returned no image data: {str(response_json)[:200]}")
    return {
        "local_path": str(out_path),
        "cdn_url": url or None,
        "cost_usd": _xai_payload_cost_usd(response_json),
        "bytes": out_path.stat().st_size,
    }


def _ensure_fal() -> None:
    try:
        require_fal_key("styled still generation")
    except RuntimeError as exc:
        raise StyledStillError(str(exc)) from exc


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
        payload["seed"] = int(seed)
        payload["enable_safety_checker"] = True
    else:
        payload["negative_prompt"] = str(negative_prompt or "")[:1500]
        payload["seed"] = int(seed)
        payload["enable_safety_checker"] = True
    return payload


def _modal_seedream_t2i_result(
    endpoint_url: str,
    payload: dict[str, Any],
    *,
    remote_model_id: str,
) -> dict[str, Any]:
    """Call the optional operator-supplied Modal Seedream HTTP contract."""
    if not endpoint_url:
        raise StyledStillError("Modal Seedream is not configured")
    headers = modal_seedream_request_headers()
    timeout = max(30, int(os.getenv("MODAL_SEEDREAM_TIMEOUT_SEC", "300") or "300"))
    with httpx.Client(timeout=timeout, follow_redirects=False) as client:
        response = client.post(
            endpoint_url,
            headers=headers,
            json={"task": "text_to_image", "model": remote_model_id, "input": payload},
        )
    if response.status_code not in (200, 201):
        raise StyledStillError(
            f"Modal Seedream generation failed ({response.status_code}): {response.text[:300]}"
        )
    result = response.json()
    if not isinstance(result, dict):
        raise StyledStillError("Modal Seedream generation returned a non-object response")
    for key in ("output", "data"):
        nested = result.get(key)
        if isinstance(nested, dict) and (
            nested.get("images") or nested.get("image") or nested.get("image_url")
        ):
            return nested
    return result


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
    if out_path.exists() and out_path.stat().st_size > 1024:
        return {
            "local_path": str(out_path),
            "provider": "seedream_v45_t2i",
            "cached": True,
        }

    if render_simulation.enabled():
        render_simulation.write_still(out_path, label="Seedream T2I simulation")
        return {
            "local_path": str(out_path),
            "provider": "simulation_seedream_t2i",
            "seed": seed,
            "bytes": out_path.stat().st_size,
            "simulated": True,
        }

    normalized_model = str(image_model_id or "").strip().lower()
    if normalized_model in {"grok_imagine", "grok_imagine_standard"}:
        api_key = str(os.environ.get("XAI_API_KEY") or "").strip()
        if not api_key:
            raise StyledStillError("xAI image generation requires XAI_API_KEY")
        xai_model = "grok-imagine-image-quality" if normalized_model == "grok_imagine" else "grok-imagine-image"
        payload = {
            "model": xai_model,
            "prompt": str(prompt or "")[:759],
            "n": 1,
            "response_format": "b64_json",
            "aspect_ratio": "9:16",
            "resolution": "2k",
        }
        with httpx.Client(timeout=240, follow_redirects=True) as client:
            response = client.post(
                XAI_IMAGE_URL,
                json=payload,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
            )
        if response.status_code not in (200, 201):
            raise StyledStillError(
                f"{xai_model} {response.status_code}: {response.text[:300]}",
                cost_usd=_xai_error_cost_usd(response.text),
                provider="xai",
                operation=normalized_model,
            )
        response_json = response.json() or {}
        written = _write_xai_image_response(response_json, out_path)
        return {
            **written,
            "provider": normalized_model,
            "xai_model": xai_model,
            "seed": seed,
        }

    normalized_seedream = normalize_seedream_model_id(normalized_model)
    model_spec = seedream_model_spec(normalized_seedream)
    if not model_spec:
        normalized_seedream = "seedream_edit"
        model_spec = seedream_model_spec(normalized_seedream)
    provider = seedream_provider(normalized_seedream)
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
        if provider == "modal":
            result = _modal_seedream_t2i_result(
                endpoint,
                payload,
                remote_model_id=str(model_spec.get("remote_model_id") or "bytedance/seedream/v5/lite"),
            )
        else:
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
        "provider_transport": provider,
        "seed": seed,
        "bytes": out_path.stat().st_size,
    }


def generate_still_xai_edit(
    prompt: str,
    out_path: Path,
    *,
    reference_path: str | Path,
    image_model_id: str = "grok_imagine",
) -> dict[str, Any]:
    out_path = Path(out_path)
    ref = Path(reference_path)
    if not ref.is_file() or ref.stat().st_size <= 1024:
        raise StyledStillError("xAI image edit requires a usable source image")
    if render_simulation.enabled():
        render_simulation.write_still(out_path, label="xAI image edit simulation")
        return {
            "local_path": str(out_path),
            "provider": "simulation_xai_image_edit",
            "simulated": True,
            "cost_usd": 0.0,
        }
    api_key = str(os.environ.get("XAI_API_KEY") or "").strip()
    if not api_key:
        raise StyledStillError("xAI image edit requires XAI_API_KEY")
    normalized_model = str(image_model_id or "grok_imagine").strip().lower()
    xai_model = "grok-imagine-image" if normalized_model == "grok_imagine_standard" else "grok-imagine-image-quality"
    payload = {
        "model": xai_model,
        "prompt": str(prompt or "")[:759],
        "image": {"url": _data_uri(ref), "type": "image_url"},
        "response_format": "b64_json",
        "resolution": "2k",
    }
    with httpx.Client(timeout=240, follow_redirects=True) as client:
        response = client.post(
            XAI_IMAGE_EDIT_URL,
            json=payload,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
    if response.status_code not in (200, 201):
        raise StyledStillError(
            f"{xai_model} edit {response.status_code}: {response.text[:300]}",
            cost_usd=_xai_error_cost_usd(response.text),
            provider="xai",
            operation=f"{normalized_model}_edit",
        )
    response_json = response.json() or {}
    written = _write_xai_image_response(response_json, out_path)
    return {
        **written,
        "provider": f"{normalized_model}_edit",
        "xai_model": xai_model,
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
