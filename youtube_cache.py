"""
Persistent YouTube API response cache.

Studio runs on RunPod Serverless with workers that die after `idleTimeout` seconds.
Every cold boot resets an in-memory cache, so the same Catalyst question (e.g.
"what's trending in finance?") re-burns 100 quota units on every fresh worker.
This module persists responses to disk so the cache survives restarts and the
10k/day quota actually buys us 10k/day of NEW data, not repeated refetches.

Design:
- Single JSON file at `TEMP_DIR/youtube_cache.json`. Small footprint (~few MB
  max), fast to load, no schema migrations to worry about.
- Per-entry TTL. Different data types expire at different rates:
    - Search results:    6 h  (trending-ish, changes but not minute-by-minute)
    - Video metadata:   24 h  (title/description/duration effectively static)
    - Video stats:       1 h  (views/likes change — but not fast enough to hit every time)
    - Channel metadata:  6 h  (sub-count, title, thumbnail)
    - Channel uploads:   1 h  (new uploads land; 1h catches them fast enough for Catalyst)
    - Captions:         24 h  (static once published)
- Prune on every write (drop expired + anything older than the longest TTL).
- Opportunistic promote: serve stale cache on quota-exceeded errors so the UI
  degrades gracefully instead of throwing.

Usage:
    from youtube_cache import get, set, make_key, CacheKind

    key = make_key("videos.list", id="abc123,def456", part="snippet,statistics")
    cached = await get(key)
    if cached is not None:
        return cached

    # ...reserve quota, call API...
    await set(key, response, kind=CacheKind.VIDEO_METADATA)
    return response
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger("nyptid-studio.youtube_cache")

_TEMP_DIR = Path(os.getenv("TEMP_DIR") or (Path(__file__).resolve().parent / "temp_assets"))
_CACHE_FILE = _TEMP_DIR / "youtube_cache.json"

# Maximum entries before aggressive pruning kicks in (keeps file + RAM bounded).
_CACHE_MAX_ENTRIES = int(os.getenv("YOUTUBE_CACHE_MAX_ENTRIES", "5000"))


@dataclass(frozen=True)
class CacheKind:
    """Canonical cache-kind names with their TTLs (seconds)."""
    SEARCH = "search"                      # 6h — trending results
    VIDEO_METADATA = "video_metadata"      # 24h — title/desc/duration (static)
    VIDEO_STATS = "video_stats"            # 1h — views/likes (dynamic)
    CHANNEL_METADATA = "channel_metadata"  # 6h — sub-count, title
    CHANNEL_UPLOADS = "channel_uploads"    # 1h — latest-video lookups
    CAPTIONS = "captions"                  # 24h — caption text (static once published)
    GENERIC = "generic"                    # 5m — default for uncategorized calls


_KIND_TTL_SEC: dict[str, int] = {
    CacheKind.SEARCH:           6 * 3600,
    CacheKind.VIDEO_METADATA:  24 * 3600,
    CacheKind.VIDEO_STATS:          3600,
    CacheKind.CHANNEL_METADATA: 6 * 3600,
    CacheKind.CHANNEL_UPLOADS:      3600,
    CacheKind.CAPTIONS:        24 * 3600,
    CacheKind.GENERIC:               300,
}

# The longest TTL across all kinds — used for eviction cutoff.
_MAX_TTL_SEC = max(_KIND_TTL_SEC.values())


def ttl_for(kind: str) -> int:
    return _KIND_TTL_SEC.get(str(kind or "").strip().lower(), _KIND_TTL_SEC[CacheKind.GENERIC])


def kind_for_method(method: str) -> str:
    """Map a Google API method to a cache kind. Callers can override when they know better."""
    m = str(method or "").strip().lower()
    if m in ("search.list", "search"):
        return CacheKind.SEARCH
    if m in ("videos.list", "videos"):
        # Caller should override to VIDEO_STATS when part=statistics is the primary payload.
        return CacheKind.VIDEO_METADATA
    if m in ("channels.list", "channels"):
        return CacheKind.CHANNEL_METADATA
    if m in ("playlistItems.list", "playlistItems"):
        return CacheKind.CHANNEL_UPLOADS
    if m in ("captions.list", "captions.download"):
        return CacheKind.CAPTIONS
    return CacheKind.GENERIC


# ─── Key construction ──────────────────────────────────────────────────────


def make_key(method: str, **params: Any) -> str:
    """Build a stable cache key from a method name + sorted param dict.

    Leaves the human-readable prefix intact so you can eyeball the cache file for
    debugging, then hashes the param blob to keep keys short + collision-free.
    """
    norm = json.dumps(
        {str(k): ("" if v is None else str(v)) for k, v in (params or {}).items()},
        sort_keys=True,
    )
    digest = hashlib.sha1(norm.encode("utf-8")).hexdigest()[:16]
    return f"{str(method or 'unknown').strip().lower()}::{digest}"


# ─── Persistent store ──────────────────────────────────────────────────────

_lock = asyncio.Lock()
_cache: dict[str, dict] = {}
_cache_loaded = False


def _load_if_needed() -> None:
    global _cache, _cache_loaded
    if _cache_loaded:
        return
    try:
        if _CACHE_FILE.exists():
            raw = _CACHE_FILE.read_text(encoding="utf-8")
            loaded = json.loads(raw) if raw else {}
            if isinstance(loaded, dict):
                _cache = loaded
            else:
                log.warning("YouTube cache file was not a dict; resetting")
                _cache = {}
        else:
            _cache = {}
    except Exception as e:
        log.warning("Failed to load YouTube cache (%s); starting fresh", e)
        _cache = {}
    _cache_loaded = True


def _save() -> None:
    try:
        _CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _CACHE_FILE.write_text(json.dumps(_cache, separators=(",", ":")), encoding="utf-8")
    except Exception as e:
        log.warning("Failed to persist YouTube cache: %s", e)


def _prune_expired() -> int:
    """Drop expired entries. Also enforce the max-size cap by evicting oldest."""
    now = time.time()
    removed = 0
    for key in list(_cache.keys()):
        entry = _cache[key]
        if not isinstance(entry, dict):
            del _cache[key]
            removed += 1
            continue
        ts = float(entry.get("ts_unix", 0) or 0)
        ttl = int(entry.get("ttl_sec", 0) or 0)
        if ts <= 0 or (now - ts) >= ttl:
            del _cache[key]
            removed += 1
    # Hard cap: if still over limit, evict oldest-first.
    if len(_cache) > _CACHE_MAX_ENTRIES:
        sorted_keys = sorted(_cache.keys(), key=lambda k: float(_cache[k].get("ts_unix", 0) or 0))
        overflow = len(_cache) - _CACHE_MAX_ENTRIES
        for k in sorted_keys[:overflow]:
            del _cache[k]
            removed += 1
    return removed


# ─── Public API ────────────────────────────────────────────────────────────


async def get(key: str, *, allow_stale: bool = False) -> Any | None:
    """Return the cached value if present and fresh. If `allow_stale=True`, returns
    stale values too (useful when quota is exhausted and we'd rather serve old data
    than throw)."""
    async with _lock:
        _load_if_needed()
        entry = _cache.get(key)
        if not isinstance(entry, dict):
            return None
        ts = float(entry.get("ts_unix", 0) or 0)
        ttl = int(entry.get("ttl_sec", 0) or 0)
        if ts <= 0:
            return None
        if allow_stale or (time.time() - ts) < ttl:
            return entry.get("value")
        return None


async def set(key: str, value: Any, *, kind: str = CacheKind.GENERIC, ttl_sec: int | None = None) -> None:
    """Store a value with the kind's default TTL (or an override)."""
    effective_ttl = int(ttl_sec) if ttl_sec is not None else ttl_for(kind)
    async with _lock:
        _load_if_needed()
        _cache[key] = {
            "value": value,
            "ts_unix": time.time(),
            "ttl_sec": effective_ttl,
            "kind": str(kind or CacheKind.GENERIC),
        }
        _prune_expired()
        _save()


async def invalidate(key: str) -> bool:
    async with _lock:
        _load_if_needed()
        if key in _cache:
            del _cache[key]
            _save()
            return True
        return False


async def stats() -> dict:
    """Return cache stats for admin observability."""
    async with _lock:
        _load_if_needed()
        now = time.time()
        total = len(_cache)
        fresh = 0
        stale = 0
        by_kind: dict[str, int] = {}
        for entry in _cache.values():
            if not isinstance(entry, dict):
                continue
            ts = float(entry.get("ts_unix", 0) or 0)
            ttl = int(entry.get("ttl_sec", 0) or 0)
            k = str(entry.get("kind", "unknown"))
            by_kind[k] = by_kind.get(k, 0) + 1
            if (now - ts) < ttl:
                fresh += 1
            else:
                stale += 1
        try:
            size_bytes = _CACHE_FILE.stat().st_size if _CACHE_FILE.exists() else 0
        except Exception:
            size_bytes = 0
        return {
            "total_entries": total,
            "fresh_entries": fresh,
            "stale_entries": stale,
            "by_kind": by_kind,
            "max_entries": _CACHE_MAX_ENTRIES,
            "file_size_bytes": size_bytes,
            "file_path": str(_CACHE_FILE),
        }
