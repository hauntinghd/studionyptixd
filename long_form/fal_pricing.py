"""Live fal.ai Platform API pricing for render cost estimates.

Uses GET https://api.fal.ai/v1/models/pricing (requires FAL_KEY / FAL_AI_KEY).
Results are cached in memory + optional disk file so Studio does not hammer
the API on every outline poll.

Log drains / webhooks on fal are for *billing alerts* after you spend — not
needed for upfront estimates. This module is the estimate path.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

FAL_PLATFORM_BASE = "https://api.fal.ai/v1"

# Endpoints our pipelines actually call (v5_episode, sleep_doc, ZT Ken Burns).
ENDPOINTS: dict[str, str] = {
    "ltx_13b_distilled": "fal-ai/ltx-video-13b-distilled/image-to-video",
    "seedream_v45": "fal-ai/bytedance/seedream/v4.5/text-to-image",
    "seedream_v45_edit": "fal-ai/bytedance/seedream/v4.5/edit",
    "mmaudio_v2": "fal-ai/mmaudio-v2/text-to-audio",
    "minimax_speech": "fal-ai/minimax/speech-02-hd",
    "kling_v21_standard": "fal-ai/kling-video/v2.1/standard/image-to-video",
    "kling_v21_pro": "fal-ai/kling-video/v2.1/pro/image-to-video",
    "pixverse_v6": "fal-ai/pixverse/v6/image-to-video",
    "minimax_speech_zt": "fal-ai/minimax/speech-02-hd",
}

# Used when API is down or key missing — last verified 2026-05-28 via Platform API.
FALLBACK_USD: dict[str, float] = {
    "ltx_13b_distilled_per_clip": 0.04,
    "seedream_v45_per_image": 0.04,
    "mmaudio_v2_per_second": 0.001,
    "fal_minimax_per_1k_chars": 0.10,
    "elevenlabs_per_1k_chars": 0.10,
    "kling_v21_standard_per_second": 0.056,
    "kling_v21_pro_per_second": 0.098,
    "pixverse_v6_per_second": 0.045,
    "ernie_per_image": 0.03,
    "cushion_pct": 0.15,
    # Script expansion via xAI Grok — not billed to fal wallet.
    "grok_chapter_expand": 0.0,
    "grok_outline": 0.0,
}

DEFAULT_CACHE_PATH = Path(
    os.environ.get(
        "FAL_PRICING_CACHE_PATH",
        str(Path(__file__).resolve().parents[1] / "analysis" / "fal_pricing_cache.json"),
    )
)
DEFAULT_TTL_SEC = int(os.environ.get("FAL_PRICING_TTL_SEC", "3600"))

_mem_cache: dict[str, Any] | None = None
_mem_fetched_at: float = 0.0


class FalPricingError(RuntimeError):
    pass


def _fal_api_key() -> str:
    return (os.environ.get("FAL_AI_KEY") or os.environ.get("FAL_KEY") or "").strip()


def _read_disk_cache() -> dict[str, Any] | None:
    try:
        p = DEFAULT_CACHE_PATH
        if not p.exists():
            return None
        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or "prices" not in data:
            return None
        return data
    except (OSError, json.JSONDecodeError):
        return None


def _write_disk_cache(payload: dict[str, Any]) -> None:
    try:
        DEFAULT_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        DEFAULT_CACHE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except OSError:
        pass


def fetch_live_prices(
    endpoint_ids: list[str] | None = None,
    *,
    timeout_sec: float = 30.0,
) -> dict[str, dict[str, Any]]:
    """Fetch unit prices from fal Platform API. Requires FAL_KEY."""
    key = _fal_api_key()
    if not key:
        raise FalPricingError("FAL_AI_KEY / FAL_KEY not set")

    ids = endpoint_ids or list(ENDPOINTS.values())
    # API accepts repeated endpoint_id query params.
    q = "&".join(f"endpoint_id={urllib.parse.quote(eid, safe='')}" for eid in ids)
    url = f"{FAL_PLATFORM_BASE}/models/pricing?{q}"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Key {key}", "Accept": "application/json"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:400]
        raise FalPricingError(f"fal pricing HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise FalPricingError(f"fal pricing network error: {exc}") from exc

    by_id: dict[str, dict[str, Any]] = {}
    for row in data.get("prices") or []:
        eid = str(row.get("endpoint_id") or "").strip()
        if eid:
            by_id[eid] = {
                "endpoint_id": eid,
                "unit_price": float(row.get("unit_price") or 0),
                "unit": str(row.get("unit") or "").strip().lower(),
                "currency": str(row.get("currency") or "USD"),
            }
    return by_id


def get_pricing_snapshot(*, force_refresh: bool = False) -> dict[str, Any]:
    """Return cached or live pricing snapshot for pipeline cost math."""
    global _mem_cache, _mem_fetched_at

    now = time.time()
    if (
        not force_refresh
        and _mem_cache is not None
        and (now - _mem_fetched_at) < DEFAULT_TTL_SEC
    ):
        return _mem_cache

    if not force_refresh:
        disk = _read_disk_cache()
        if disk and (now - float(disk.get("fetched_at", 0) or 0)) < DEFAULT_TTL_SEC:
            _mem_cache = disk
            _mem_fetched_at = float(disk.get("fetched_at", 0) or 0)
            return disk

    source = "fallback"
    by_id: dict[str, dict[str, Any]] = {}
    error: str | None = None
    try:
        by_id = fetch_live_prices()
        source = "fal_api"
    except FalPricingError as exc:
        error = str(exc)
        disk = _read_disk_cache()
        if disk and disk.get("prices"):
            by_id = disk.get("prices") or {}
            source = "disk_cache_stale"
        else:
            by_id = {}

    snapshot = {
        "fetched_at": now,
        "source": source,
        "error": error,
        "prices": by_id,
        "endpoints": ENDPOINTS,
        "fallback_usd": FALLBACK_USD,
    }
    _mem_cache = snapshot
    _mem_fetched_at = now
    if source == "fal_api":
        _write_disk_cache(snapshot)
    return snapshot


def _price_row(snapshot: dict[str, Any], key: str) -> dict[str, Any] | None:
    eid = ENDPOINTS.get(key)
    if not eid:
        return None
    return (snapshot.get("prices") or {}).get(eid)


def unit_cost(
    snapshot: dict[str, Any],
    key: str,
    *,
    fallback_key: str,
    quantity: float = 1.0,
) -> tuple[float, str]:
    """Return (usd, billing_note) for quantity units of billing measure."""
    return _unit_cost(snapshot, key, fallback_key=fallback_key, quantity=quantity)


def _unit_cost(
    snapshot: dict[str, Any],
    key: str,
    *,
    fallback_key: str,
    quantity: float = 1.0,
) -> tuple[float, str]:
    """Return (usd, billing_note) for quantity units of billing measure."""
    row = _price_row(snapshot, key)
    fb = FALLBACK_USD.get(fallback_key, 0.0)

    if not row or row.get("unit_price") is None:
        return round(fb * quantity, 4), f"fallback:{fallback_key}"

    unit = str(row.get("unit") or "").lower()
    price = float(row["unit_price"])

    if unit in ("images", "image", "units", "unit", "videos", "video"):
        return round(price * quantity, 4), f"live:{key}@{price}/{unit}×{quantity}"

    if unit == "seconds":
        return round(price * quantity, 4), f"live:{key}@${price}/s×{quantity}s"

    if "1000" in unit and "character" in unit:
        return round(price * quantity, 4), f"live:{key}@${price}/1k_chars×{quantity}k"

    # Unknown unit — treat unit_price as flat per call.
    return round(price * quantity, 4), f"live:{key}@{price}×{quantity}(unknown_unit={unit})"


def i2v_cost_per_clip(
    snapshot: dict[str, Any],
    *,
    i2v_model: str = "ltx_13b",
    clip_sec: float = 12.0,
) -> tuple[float, str]:
    """Cost for one i2v clip based on channel model default."""
    model = (i2v_model or "ltx_13b").strip().lower()
    if model in ("ltx_13b", "ltx", "ltx_13b_distilled"):
        return _unit_cost(
            snapshot, "ltx_13b_distilled", fallback_key="ltx_13b_distilled_per_clip", quantity=1.0
        )
    if "kling" in model and "pro" in model:
        return _unit_cost(
            snapshot,
            "kling_v21_pro",
            fallback_key="kling_v21_pro_per_second",
            quantity=max(5.0, clip_sec),
        )
    if "kling" in model:
        return _unit_cost(
            snapshot,
            "kling_v21_standard",
            fallback_key="kling_v21_standard_per_second",
            quantity=max(5.0, clip_sec),
        )
    if "pixverse" in model:
        return _unit_cost(
            snapshot,
            "pixverse_v6",
            fallback_key="pixverse_v6_per_second",
            quantity=max(5.0, clip_sec),
        )
    return _unit_cost(
        snapshot, "ltx_13b_distilled", fallback_key="ltx_13b_distilled_per_clip", quantity=1.0
    )


def refresh_pricing_cache() -> dict[str, Any]:
    """Force-refresh from fal API (call from admin route or pre-render)."""
    return get_pricing_snapshot(force_refresh=True)


def pricing_status() -> dict[str, Any]:
    """Lightweight status for API/UI."""
    snap = get_pricing_snapshot()
    age = time.time() - float(snap.get("fetched_at", 0) or 0)
    return {
        "source": snap.get("source"),
        "fetched_at": snap.get("fetched_at"),
        "age_sec": round(age, 1),
        "ttl_sec": DEFAULT_TTL_SEC,
        "error": snap.get("error"),
        "endpoint_count": len(snap.get("prices") or {}),
        "cache_path": str(DEFAULT_CACHE_PATH),
    }
