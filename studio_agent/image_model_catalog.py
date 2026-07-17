"""Lightweight Studio image-model catalog and Seedream routing metadata.

This module deliberately has no provider SDK imports so it can be shared by
the local API, Studio Agent session store, and render workers.  A Seedream
family exposes both text-to-image and edit endpoints; renderers choose the
edit endpoint only when they actually have reference images.

Modal is an optional operator-supplied HTTP adapter.  It is never advertised
unless ``MODAL_SEEDREAM_ENDPOINT_URL`` is configured.
"""
from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse


SEEDREAM_MODEL_SPECS: dict[str, dict[str, Any]] = {
    "seedream_edit": {
        "id": "seedream_edit",
        "label": "Seedream 4.5",
        "provider": "fal",
        "tier": "premium",
        "summary": "Seedream 4.5 generation plus reference-aware editing for consistent scene stills.",
        "speed": "Medium",
        "estimated_unit_usd": 0.04,
        "billing_unit": "image",
        "text_to_image_endpoint": "fal-ai/bytedance/seedream/v4.5/text-to-image",
        "edit_endpoint": "fal-ai/bytedance/seedream/v4.5/edit",
        "supports_reference_conditioning": True,
    },
    "seedream_v4": {
        "id": "seedream_v4",
        "label": "Seedream 4.0",
        "provider": "fal",
        "tier": "basic",
        "summary": "Lower-cost Seedream generation and editing with strong bilingual prompt adherence.",
        "speed": "Fast",
        "estimated_unit_usd": 0.03,
        "billing_unit": "image",
        "text_to_image_endpoint": "fal-ai/bytedance/seedream/v4/text-to-image",
        "edit_endpoint": "fal-ai/bytedance/seedream/v4/edit",
        "supports_reference_conditioning": True,
    },
    "seedream_v5_lite": {
        "id": "seedream_v5_lite",
        "label": "Seedream 5.0 Lite",
        "provider": "fal",
        "tier": "premium",
        "summary": "Latest fast Seedream lane with high-resolution generation and multi-reference editing.",
        "speed": "Fast",
        "estimated_unit_usd": 0.035,
        "billing_unit": "image",
        "text_to_image_endpoint": "bytedance/seedream/v5/lite/text-to-image",
        "edit_endpoint": "bytedance/seedream/v5/lite/edit",
        "supports_reference_conditioning": True,
    },
}

SEEDREAM_MODEL_ALIASES = {
    "seedream45": "seedream_edit",
    "seedream_45": "seedream_edit",
    "seedream_v45": "seedream_edit",
    "seedream_v45_edit": "seedream_edit",
    "seedream4": "seedream_v4",
    "seedream_v4_edit": "seedream_v4",
    "seedream5_lite": "seedream_v5_lite",
    "seedream_v5_lite_edit": "seedream_v5_lite",
    "seedream5_lite_modal": "seedream_v5_lite_modal",
}

MODAL_SEEDREAM_MODEL_ID = "seedream_v5_lite_modal"


def _https_endpoint(value: str) -> str:
    raw = str(value or "").strip().rstrip("/")
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
    except Exception:
        return ""
    # Modal web endpoints are HTTPS.  Refuse an arbitrary scheme so a typo can
    # never turn the server into a local-file or internal-network client.
    return raw if parsed.scheme == "https" and bool(parsed.netloc) else ""


def modal_seedream_endpoint_url() -> str:
    return _https_endpoint(os.getenv("MODAL_SEEDREAM_ENDPOINT_URL", ""))


def modal_seedream_auth_token() -> str:
    return str(os.getenv("MODAL_SEEDREAM_AUTH_TOKEN", "") or "").strip()


def modal_seedream_request_headers() -> dict[str, str]:
    """Auth headers for a protected Modal web endpoint or custom proxy.

    Modal proxy auth uses ``Modal-Key``/``Modal-Secret``.  Bearer remains an
    optional custom-endpoint convention and is never synthesized.
    """
    headers = {"Content-Type": "application/json"}
    token_id = str(os.getenv("MODAL_PROXY_TOKEN_ID", "") or "").strip()
    token_secret = str(os.getenv("MODAL_PROXY_TOKEN_SECRET", "") or "").strip()
    if token_id and token_secret:
        headers["Modal-Key"] = token_id
        headers["Modal-Secret"] = token_secret
    bearer = modal_seedream_auth_token()
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    return headers


def modal_seedream_configured() -> bool:
    return bool(modal_seedream_endpoint_url())


def normalize_seedream_model_id(value: Any) -> str:
    raw = str(value or "").strip().lower().replace(" ", "_")
    return SEEDREAM_MODEL_ALIASES.get(raw, raw)


def is_seedream_model(value: Any) -> bool:
    model_id = normalize_seedream_model_id(value)
    return model_id in SEEDREAM_MODEL_SPECS or model_id == MODAL_SEEDREAM_MODEL_ID


def seedream_model_spec(value: Any) -> dict[str, Any]:
    model_id = normalize_seedream_model_id(value)
    if model_id == MODAL_SEEDREAM_MODEL_ID:
        return {
            "id": MODAL_SEEDREAM_MODEL_ID,
            "label": "Seedream 5.0 Lite (Modal)",
            "provider": "modal",
            "tier": "operator",
            "summary": "Operator-hosted Seedream HTTP lane; shown only while its Modal endpoint is configured.",
            "speed": "Fast",
            "estimated_unit_usd": None,
            "billing_unit": "image",
            "modal_endpoint_url": modal_seedream_endpoint_url(),
            "remote_model_id": str(
                os.getenv("MODAL_SEEDREAM_MODEL_ID", "bytedance/seedream/v5/lite")
                or "bytedance/seedream/v5/lite"
            ).strip(),
            "supports_reference_conditioning": True,
        }
    return dict(SEEDREAM_MODEL_SPECS.get(model_id) or {})


def seedream_model_profiles(*, fal_enabled: bool = True) -> list[dict[str, Any]]:
    profiles: list[dict[str, Any]] = []
    for spec in SEEDREAM_MODEL_SPECS.values():
        profile = dict(spec)
        profile["enabled"] = bool(fal_enabled)
        profiles.append(profile)
    if modal_seedream_configured():
        profile = seedream_model_spec(MODAL_SEEDREAM_MODEL_ID)
        profile["enabled"] = True
        profiles.append(profile)
    return profiles


def seedream_endpoint(value: Any, *, edit: bool) -> str:
    spec = seedream_model_spec(value)
    if not spec:
        return ""
    if str(spec.get("provider") or "").lower() == "modal":
        return str(spec.get("modal_endpoint_url") or "")
    key = "edit_endpoint" if edit else "text_to_image_endpoint"
    return str(spec.get(key) or "").strip()


def seedream_provider(value: Any) -> str:
    return str(seedream_model_spec(value).get("provider") or "").strip().lower()


def seedream_estimated_unit_usd(value: Any) -> float | None:
    raw = seedream_model_spec(value).get("estimated_unit_usd")
    try:
        return float(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None
