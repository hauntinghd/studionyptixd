"""Studio image-to-video catalog with provider-backed pricing.

FAL exposes endpoint prices through its Platform pricing API. The catalog uses
the same cached snapshot as production preflight so the picker and final quote
cannot silently disagree. xAI model-list responses do not include rates, so
their published per-second and input-image rates remain explicit catalog data.
"""
from __future__ import annotations

from typing import Any

from long_form import fal_pricing


VIDEO_MODEL_SPECS: tuple[dict[str, Any], ...] = (
    {
        "id": "grok_imagine_video",
        "label": "Grok Imagine Video",
        "provider": "xai",
        "tier": "standard",
        "summary": "Cheapest Grok image-to-video lane at 720p.",
        "speed": "Fast",
        "estimated_unit_usd": 0.07,
        "billing_unit": "second",
        "input_image_usd": 0.002,
        "pricing_source": "xai_published",
        "pricing_assumptions": "720p",
    },
    {
        "id": "grok_imagine_video_15",
        "label": "Grok Imagine Video 1.5",
        "provider": "xai",
        "tier": "premium",
        "summary": "Higher-quality Grok image-to-video at 720p.",
        "speed": "Medium",
        "estimated_unit_usd": 0.14,
        "billing_unit": "second",
        "input_image_usd": 0.01,
        "pricing_source": "xai_published",
        "pricing_assumptions": "720p",
    },
    {
        "id": "grok_imagine_video_15_1080p",
        "label": "Grok Imagine Video 1.5 1080p",
        "provider": "xai",
        "tier": "premium",
        "summary": "Full-resolution Grok image-to-video for final renders.",
        "speed": "Slow",
        "estimated_unit_usd": 0.25,
        "billing_unit": "second",
        "input_image_usd": 0.01,
        "pricing_source": "xai_published",
        "pricing_assumptions": "1080p",
    },
    {
        "id": "seedance",
        "label": "Seedance 2.0",
        "provider": "fal",
        "tier": "premium",
        "summary": "Premium cinematic motion through FAL.",
        "speed": "Medium",
        "fal_pricing_key": "seedance_20_i2v",
        "fallback_pricing_key": "seedance_20_i2v_per_second",
        "pricing_assumptions": "720p, standard, no audio",
    },
    {
        "id": "kling_pro",
        "label": "Kling 2.1 Pro",
        "provider": "fal",
        "tier": "premium",
        "summary": "Premium hero-scene motion with a model-specific prompt adapter.",
        "speed": "Slow",
        "fal_pricing_key": "kling_v21_pro",
        "fallback_pricing_key": "kling_v21_pro_per_second",
        "pricing_assumptions": "Pro tier",
    },
    {
        "id": "pixverse",
        "label": "PixVerse V6",
        "provider": "fal",
        "tier": "standard",
        "summary": "Strong-value motion and moderation fallback through FAL.",
        "speed": "Medium",
        "fal_pricing_key": "pixverse_v6",
        "fallback_pricing_key": "pixverse_v6_per_second",
        "pricing_assumptions": "720p, no audio",
    },
    {
        "id": "ltx_budget",
        "label": "LTX 13B Budget",
        "provider": "fal",
        "tier": "basic",
        "summary": "Lowest-cost full-motion FAL lane.",
        "speed": "Fast",
        "fal_pricing_key": "ltx_098_distilled",
        "fallback_pricing_key": "ltx_098_distilled_per_second",
        "pricing_assumptions": "24fps, detail pass off",
    },
)


def _fal_price_profile(spec: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    pricing_key = str(spec.get("fal_pricing_key") or "")
    endpoint = str(fal_pricing.ENDPOINTS.get(pricing_key) or "")
    row = (snapshot.get("prices") or {}).get(endpoint) if endpoint else None
    fetched_at = float(snapshot.get("fetched_at") or 0.0)
    source = str(snapshot.get("source") or "fallback")
    if isinstance(row, dict) and row.get("unit_price") is not None:
        effective, pricing_note = fal_pricing.unit_cost(
            snapshot,
            pricing_key,
            fallback_key=str(spec.get("fallback_pricing_key") or ""),
            quantity=1.0,
        )
        return {
            "estimated_unit_usd": float(effective),
            "billing_unit": "second",
            "pricing_source": f"{source}_derived",
            "pricing_fetched_at": fetched_at,
            "pricing_live": source == "fal_api",
            "pricing_endpoint": endpoint,
            "pricing_note": pricing_note,
            "pricing_assumptions": str(spec.get("pricing_assumptions") or ""),
        }

    fallback_key = str(spec.get("fallback_pricing_key") or "")
    return {
        "estimated_unit_usd": float(fal_pricing.FALLBACK_USD.get(fallback_key, 0.0) or 0.0),
        "billing_unit": "second",
        "pricing_source": "fallback",
        "pricing_fetched_at": fetched_at,
        "pricing_live": False,
        "pricing_endpoint": endpoint,
        "pricing_note": f"fallback:{fallback_key}",
        "pricing_assumptions": str(spec.get("pricing_assumptions") or ""),
    }


def video_model_profiles(
    *,
    fal_enabled: bool = True,
    pricing_snapshot: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Return every runnable I2V model with a concrete unit rate."""

    snapshot = pricing_snapshot if pricing_snapshot is not None else fal_pricing.get_pricing_snapshot()
    profiles: list[dict[str, Any]] = []
    for item in VIDEO_MODEL_SPECS:
        profile = dict(item)
        profile.pop("fal_pricing_key", None)
        profile.pop("fallback_pricing_key", None)
        if str(item.get("provider") or "").lower() == "fal":
            profile.update(_fal_price_profile(item, snapshot))
            profile["enabled"] = bool(fal_enabled)
        else:
            profile["enabled"] = True
            profile["pricing_live"] = False
            profile["pricing_fetched_at"] = 0.0
        profiles.append(profile)
    return profiles


__all__ = ["VIDEO_MODEL_SPECS", "video_model_profiles"]
