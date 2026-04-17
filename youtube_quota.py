"""
YouTube Data API v3 quota budget tracker.

Google caps each project at 10,000 units/day. Catalyst's scraping + trend polling
+ user lookups all draw from the same pool. This module tracks daily spend,
enforces a configurable cap, and logs per-method breakdown so we can see where
the budget actually goes.

Persisted to disk so a RunPod cold boot mid-day doesn't reset the counter and
let us blow the cap twice.

Cost table comes from Google's published values:
    https://developers.google.com/youtube/v3/determine_quota_cost

Usage:
    from youtube_quota import cost_for, reserve

    cost = cost_for("search.list")           # 100
    if not await reserve(cost, "search.list", note="trending niche=finance"):
        return []                             # caller degrades gracefully (cached or empty)
    # ...then make the actual API call

Admin view:
    GET /api/admin/youtube-quota  ->  calls youtube_quota.breakdown()
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger("nyptid-studio.youtube_quota")

# ─── Config ────────────────────────────────────────────────────────────────

# Google's free-tier daily cap; override via env for multi-project setups.
YOUTUBE_DAILY_QUOTA_CAP = int(os.getenv("YOUTUBE_DAILY_QUOTA_CAP", "10000"))

# Warn when we cross this fraction of the cap (logs a WARN but still allows the call).
YOUTUBE_QUOTA_WARN_PCT = float(os.getenv("YOUTUBE_QUOTA_WARN_PCT", "0.85"))

# Reserve this fraction for user-initiated lookups; background jobs should not exceed
# (1 - this) of the cap. Enforced only if the caller passes kind="background".
YOUTUBE_QUOTA_INTERACTIVE_RESERVE_PCT = float(os.getenv("YOUTUBE_QUOTA_INTERACTIVE_RESERVE_PCT", "0.20"))

# Keep this many days of history in the log file (for trend analysis / debugging).
YOUTUBE_QUOTA_HISTORY_DAYS = int(os.getenv("YOUTUBE_QUOTA_HISTORY_DAYS", "14"))

# Persist under TEMP_DIR so the file stays with the rest of the worker's state.
# Falls back to repo-root tmp if TEMP_DIR isn't set (e.g. during local dev).
_TEMP_DIR = Path(os.getenv("TEMP_DIR") or (Path(__file__).resolve().parent / "temp_assets"))
_QUOTA_FILE = _TEMP_DIR / "youtube_quota_log.json"


# ─── Cost table ────────────────────────────────────────────────────────────
# Matches Google's published values as of 2026-04. Keyed by both full
# "resource.method" and bare "resource" forms so callers can pass either.

_METHOD_COST = {
    # Expensive
    "search.list": 100,
    "search": 100,
    "captions.insert": 400,
    "captions.download": 200,
    "captions.update": 450,
    "videos.insert": 1600,
    "videos.update": 50,
    "thumbnails.set": 50,
    # Cheap reads (1 unit)
    "videos.list": 1,
    "videos": 1,
    "channels.list": 1,
    "channels": 1,
    "channels.update": 50,
    "playlists.list": 1,
    "playlists": 1,
    "playlistItems.list": 1,
    "playlistItems": 1,
    "commentThreads.list": 1,
    "commentThreads": 1,
    "comments.list": 1,
    "comments": 1,
    "activities.list": 1,
    "activities": 1,
    "subscriptions.list": 1,
    "channelSections.list": 1,
    "captions.list": 50,
    "i18nRegions.list": 1,
    "i18nLanguages.list": 1,
    "videoCategories.list": 1,
    # Default when method is unknown — assume cheap read; caller should pass the real method.
    "default": 1,
}


def cost_for(method: str) -> int:
    """Return quota units cost for the given API method name.

    Accepts either full dotted form ("search.list") or bare resource ("search").
    Unknown methods fall back to 1 unit; callers should always pass the real name.
    """
    key = str(method or "").strip().lower()
    return _METHOD_COST.get(key, _METHOD_COST["default"])


# ─── State ─────────────────────────────────────────────────────────────────

_lock = asyncio.Lock()
_cache: dict = {}
_cache_loaded = False


def _today_utc_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _parse_date_key(key: str) -> datetime | None:
    try:
        return datetime.strptime(key, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _load_if_needed() -> None:
    global _cache, _cache_loaded
    if _cache_loaded:
        return
    try:
        if _QUOTA_FILE.exists():
            raw = _QUOTA_FILE.read_text(encoding="utf-8")
            loaded = json.loads(raw) if raw else {}
            if isinstance(loaded, dict):
                _cache = loaded
            else:
                log.warning("YouTube quota log was not a dict; resetting")
                _cache = {}
        else:
            _cache = {}
    except Exception as e:
        log.warning("Failed to load YouTube quota log (%s); starting fresh", e)
        _cache = {}
    _cache_loaded = True


def _save() -> None:
    try:
        _QUOTA_FILE.parent.mkdir(parents=True, exist_ok=True)
        _QUOTA_FILE.write_text(json.dumps(_cache, indent=2, sort_keys=True), encoding="utf-8")
    except Exception as e:
        log.warning("Failed to persist YouTube quota log: %s", e)


def _prune_old(keep_days: int) -> None:
    """Drop buckets older than `keep_days` to keep the file small."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=keep_days)
    stale = []
    for k in list(_cache.keys()):
        d = _parse_date_key(k)
        if d is None or d < cutoff:
            stale.append(k)
    for k in stale:
        _cache.pop(k, None)


def _today_bucket() -> dict:
    """Return (and lazily create) today's bucket."""
    _load_if_needed()
    key = _today_utc_key()
    if key not in _cache:
        _cache[key] = {
            "total": 0,
            "by_method": {},
            "by_kind": {},
            "first_call_at_unix": time.time(),
            "last_call_at_unix": time.time(),
        }
        _prune_old(YOUTUBE_QUOTA_HISTORY_DAYS)
    return _cache[key]


# ─── Public API ────────────────────────────────────────────────────────────


async def reserve(units: int, method: str = "", *, kind: str = "interactive", note: str = "") -> bool:
    """Attempt to reserve quota units before making a YouTube API call.

    Returns True if the reservation succeeded (caller may proceed). Returns False
    if it would push today's spend past the daily cap OR if kind="background"
    and the request would dip into the interactive-reserve pool. Callers MUST
    check the return value and degrade gracefully (serve cached data, skip the
    optional feature, etc.).

    Args:
        units: Cost of the call. Use cost_for(method) to look up.
        method: Dotted method name for the breakdown ("search.list", "videos.list").
        kind: "interactive" (user-initiated) or "background" (scheduled scraping).
              Background calls respect YOUTUBE_QUOTA_INTERACTIVE_RESERVE_PCT.
        note: Optional short free-text context for the log (e.g. "trending niche=finance").
    """
    units = max(1, int(units or 0))
    method_key = str(method or "unknown").strip().lower()
    kind_key = "background" if str(kind or "").strip().lower().startswith("back") else "interactive"

    async with _lock:
        bucket = _today_bucket()
        new_total = bucket["total"] + units

        # Hard cap applies to everyone.
        if new_total > YOUTUBE_DAILY_QUOTA_CAP:
            log.error(
                "YouTube quota REFUSED: +%d (method=%s kind=%s) would push %d -> %d past daily cap %d. note=%s",
                units, method_key, kind_key, bucket["total"], new_total, YOUTUBE_DAILY_QUOTA_CAP, note,
            )
            return False

        # Background jobs must leave headroom for interactive users.
        if kind_key == "background":
            background_cap = int(YOUTUBE_DAILY_QUOTA_CAP * (1.0 - YOUTUBE_QUOTA_INTERACTIVE_RESERVE_PCT))
            if new_total > background_cap:
                log.warning(
                    "YouTube quota REFUSED (background reserve): +%d (method=%s) would push %d -> %d past background cap %d (interactive reserve %.0f%%). note=%s",
                    units, method_key, bucket["total"], new_total, background_cap,
                    YOUTUBE_QUOTA_INTERACTIVE_RESERVE_PCT * 100, note,
                )
                return False

        # Commit.
        bucket["total"] = new_total
        bucket["by_method"][method_key] = bucket["by_method"].get(method_key, 0) + units
        bucket["by_kind"][kind_key] = bucket["by_kind"].get(kind_key, 0) + units
        bucket["last_call_at_unix"] = time.time()
        _save()

        pct = new_total / YOUTUBE_DAILY_QUOTA_CAP
        if pct >= YOUTUBE_QUOTA_WARN_PCT:
            log.warning(
                "YouTube quota approaching cap: %d/%d (%.0f%%) after method=%s kind=%s note=%s",
                new_total, YOUTUBE_DAILY_QUOTA_CAP, pct * 100, method_key, kind_key, note,
            )
        else:
            log.info(
                "YouTube quota +%d (method=%s kind=%s) -> %d/%d (%.0f%%)",
                units, method_key, kind_key, new_total, YOUTUBE_DAILY_QUOTA_CAP, pct * 100,
            )
        return True


async def refund(units: int, method: str = "", *, reason: str = "") -> None:
    """Return previously-reserved units when the downstream call failed before hitting the API.

    Use only when you reserved units then the call never actually went out (e.g. a
    validation error thrown before the httpx call). Do NOT refund on API errors —
    Google counts failed calls against the quota.
    """
    units = max(0, int(units or 0))
    if units == 0:
        return
    method_key = str(method or "unknown").strip().lower()
    async with _lock:
        bucket = _today_bucket()
        bucket["total"] = max(0, bucket["total"] - units)
        current = bucket["by_method"].get(method_key, 0)
        bucket["by_method"][method_key] = max(0, current - units)
        _save()
        log.info("YouTube quota refund: -%d (method=%s reason=%s) -> %d/%d",
                 units, method_key, reason, bucket["total"], YOUTUBE_DAILY_QUOTA_CAP)


async def spent_today() -> int:
    async with _lock:
        return _today_bucket()["total"]


async def remaining_today() -> int:
    async with _lock:
        return max(0, YOUTUBE_DAILY_QUOTA_CAP - _today_bucket()["total"])


async def breakdown() -> dict:
    """Return today's quota-spend breakdown for admin UI / monitoring."""
    async with _lock:
        bucket = _today_bucket()
        total = int(bucket["total"])
        remaining = max(0, YOUTUBE_DAILY_QUOTA_CAP - total)
        pct_used = (total / YOUTUBE_DAILY_QUOTA_CAP) if YOUTUBE_DAILY_QUOTA_CAP else 0.0
        return {
            "date_utc": _today_utc_key(),
            "daily_cap": YOUTUBE_DAILY_QUOTA_CAP,
            "total_spent": total,
            "total_remaining": remaining,
            "pct_used": round(pct_used, 3),
            "warn_threshold_pct": YOUTUBE_QUOTA_WARN_PCT,
            "interactive_reserve_pct": YOUTUBE_QUOTA_INTERACTIVE_RESERVE_PCT,
            "background_cap": int(YOUTUBE_DAILY_QUOTA_CAP * (1.0 - YOUTUBE_QUOTA_INTERACTIVE_RESERVE_PCT)),
            "by_method": dict(bucket.get("by_method", {})),
            "by_kind": dict(bucket.get("by_kind", {})),
            "first_call_at_unix": bucket.get("first_call_at_unix"),
            "last_call_at_unix": bucket.get("last_call_at_unix"),
        }


async def history(days: int = 7) -> list[dict]:
    """Return per-day spend history (most-recent first) for trend analysis."""
    async with _lock:
        _load_if_needed()
        rows = []
        for key in sorted(_cache.keys(), reverse=True)[:max(1, int(days or 7))]:
            entry = _cache.get(key) or {}
            rows.append({
                "date_utc": key,
                "total_spent": int(entry.get("total", 0)),
                "by_method": dict(entry.get("by_method", {})),
                "by_kind": dict(entry.get("by_kind", {})),
            })
        return rows
