"""
Catalyst reference-corpus backfill.

Phase 2 target: Catalyst holds "~50% of the data it needs to guarantee 100K+
long-form / 10M+ short-form views, any channel, any niche." This module is the
ingestion half of that — it scrapes trending videos from YouTube into a
persistent per-niche corpus that the rest of Catalyst learns from.

Design choice — batched /videos.list instead of per-niche /search.list:
    GET /videos?chart=mostPopular&part=snippet,statistics&maxResults=50
    is a SINGLE 1-unit call that returns 50 fully-hydrated trending videos.

    Doing the equivalent with per-niche search:
    GET /search?q={niche}&type=video + GET /videos?id=...
    is 100 + 1 = 101 units per niche × 8 niches = 808 units/poll.

    Same data volume for 808x less quota. This is the cornerstone of
    staying under the 10k/day cap while broadening the ingestion surface.

Classification: reuses `_catalyst_infer_niche` from backend_catalyst_core so
niches stay aligned with the rest of Catalyst's scoring + archetype routing.

Storage: TEMP_DIR/catalyst_reference_corpus.json. One JSON file per deployment.
Survives worker lifetime; needs a RunPod network volume to survive cold boots.

Usage:
    from catalyst_backfill import tick, corpus_stats

    # Called periodically (admin endpoint, cron, startup-tick, etc.)
    summary = await tick(budget_units=500, regions=["US"])
    # -> {"fetched": 50, "classified": 37, "skipped_unclassified": 13,
    #     "quota_spent": 1, "by_niche": {"business_documentary": 8, ...}}

    snapshot = await corpus_stats()
    # -> {"total_videos": 412, "by_niche": {"finance": {"count": 98, "newest_unix": ...}}}
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger("nyptid-studio.catalyst_backfill")

# ─── Config ────────────────────────────────────────────────────────────────

# Keep the top-N videos per niche (by view count). Older/lower-view entries are
# pruned on ingest to keep the corpus sharp + the file bounded.
CORPUS_MAX_PER_NICHE = int(os.getenv("CATALYST_CORPUS_MAX_PER_NICHE", "200"))

# Skip videos with < N views — they're not useful "what works" signals.
CORPUS_MIN_VIEWS = int(os.getenv("CATALYST_CORPUS_MIN_VIEWS", "5000"))

# Default regions to poll. YouTube trending is region-scoped; hit multiple
# regions for broader coverage at ~1 unit per region.
DEFAULT_REGIONS = [r.strip().upper() for r in os.getenv("CATALYST_BACKFILL_REGIONS", "US").split(",") if r.strip()]

# Per-tick budget cap. Even if the tick could do more, it won't exceed this
# many Data API units in one pass. Prevents a runaway admin click from
# draining the daily cap.
DEFAULT_TICK_BUDGET_UNITS = int(os.getenv("CATALYST_BACKFILL_TICK_BUDGET", "500"))

_TEMP_DIR = Path(os.getenv("TEMP_DIR") or (Path(__file__).resolve().parent / "temp_assets"))
_CORPUS_FILE = _TEMP_DIR / "catalyst_reference_corpus.json"


# ─── Persistent corpus ─────────────────────────────────────────────────────

_lock = asyncio.Lock()
_corpus: dict = {}
_corpus_loaded = False


def _empty_corpus() -> dict:
    return {
        "by_niche": {},
        "last_tick_unix": 0.0,
        "stats": {"total_ticks": 0, "total_videos_ingested": 0},
    }


def _load_if_needed() -> None:
    global _corpus, _corpus_loaded
    if _corpus_loaded:
        return
    try:
        if _CORPUS_FILE.exists():
            raw = _CORPUS_FILE.read_text(encoding="utf-8")
            loaded = json.loads(raw) if raw else _empty_corpus()
            if isinstance(loaded, dict) and "by_niche" in loaded:
                _corpus = loaded
            else:
                _corpus = _empty_corpus()
        else:
            _corpus = _empty_corpus()
    except Exception as e:
        log.warning("Failed to load Catalyst corpus (%s); starting fresh", e)
        _corpus = _empty_corpus()
    _corpus_loaded = True


def _save() -> None:
    try:
        _CORPUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        _CORPUS_FILE.write_text(json.dumps(_corpus, separators=(",", ":")), encoding="utf-8")
    except Exception as e:
        log.warning("Failed to persist Catalyst corpus: %s", e)


# ─── Classification ────────────────────────────────────────────────────────


def _infer_niche_for_video(video: dict) -> dict:
    """Thin wrapper around _catalyst_infer_niche that builds the text bundle."""
    snippet = dict(video.get("snippet") or {})
    title = str(snippet.get("title", "") or "")
    description = str(snippet.get("description", "") or "")
    tags = list(snippet.get("tags") or [])
    tags_text = " ".join(str(t or "").strip() for t in tags)
    # Deferred import so this module doesn't hard-depend on backend_catalyst_core
    # at load time (avoids a circular import if catalyst_backfill is loaded early).
    try:
        from backend_catalyst_core import _catalyst_infer_niche
    except Exception as e:
        log.error("Could not import _catalyst_infer_niche: %s", e)
        return {"key": "", "label": "", "confidence": 0.0}
    return _catalyst_infer_niche(title, description, tags_text)


# ─── Fetch ─────────────────────────────────────────────────────────────────


async def fetch_trending_batch(region: str = "US", max_results: int = 50, *, quota_kind: str = "background") -> list[dict]:
    """Fetch up to 50 trending videos for a region with a single 1-unit API call.

    Returns a list of raw YouTube video resources (snippet + statistics attached).
    The cache + quota instrumentation in `_youtube_public_api_get` handles
    reservation + storage — we just issue the call.
    """
    from youtube import _youtube_public_api_get
    import youtube_cache

    params = {
        "part": "snippet,statistics,contentDetails",
        "chart": "mostPopular",
        "regionCode": str(region or "US").upper(),
        "maxResults": str(max(1, min(int(max_results or 50), 50))),
    }
    try:
        payload, _key = await _youtube_public_api_get(
            "/videos",
            params=params,
            quota_kind=quota_kind,
            quota_note=f"trending_batch:region={region}",
            # Trending turns over fast — 1h cache is enough to avoid burning units on rapid re-polls
            # but fresh enough to catch new hits as they climb.
            cache_kind=youtube_cache.CacheKind.VIDEO_METADATA,
        )
    except Exception as e:
        log.warning("Trending fetch failed for region=%s: %s", region, str(e)[:200])
        return []
    items = payload.get("items") or []
    return [item for item in items if isinstance(item, dict)]


# ─── Ingest ────────────────────────────────────────────────────────────────


def _reference_from_video(video: dict, niche_info: dict) -> dict:
    vid = str(video.get("id", "") or "")
    snippet = dict(video.get("snippet") or {})
    stats = dict(video.get("statistics") or {})
    content_details = dict(video.get("contentDetails") or {})
    return {
        "video_id": vid,
        "title": str(snippet.get("title", "") or ""),
        "description": str(snippet.get("description", "") or "")[:400],
        "channel_id": str(snippet.get("channelId", "") or ""),
        "channel_title": str(snippet.get("channelTitle", "") or ""),
        "published_at": str(snippet.get("publishedAt", "") or ""),
        "tags": list(snippet.get("tags") or [])[:12],
        "category_id": str(snippet.get("categoryId", "") or ""),
        "views": int(float(stats.get("viewCount", 0) or 0)),
        "likes": int(float(stats.get("likeCount", 0) or 0)),
        "comments": int(float(stats.get("commentCount", 0) or 0)),
        "duration_iso": str(content_details.get("duration", "") or ""),
        "niche_key": str(niche_info.get("key", "") or ""),
        "niche_label": str(niche_info.get("label", "") or ""),
        "niche_confidence": float(niche_info.get("confidence", 0.0) or 0.0),
        "ingested_at_unix": time.time(),
        "source_region": "",  # filled in by tick()
    }


def _prune_niche_bucket(bucket: dict) -> None:
    """Cap per-niche list to CORPUS_MAX_PER_NICHE entries, keeping highest-view first."""
    videos = list(bucket.get("videos") or [])
    # Dedup by video_id (latest entry wins — newer stats beat old ones).
    by_id: dict[str, dict] = {}
    for v in videos:
        if not isinstance(v, dict):
            continue
        vid = str(v.get("video_id", "") or "")
        if not vid:
            continue
        existing = by_id.get(vid)
        if existing is None or float(v.get("ingested_at_unix", 0) or 0) > float(existing.get("ingested_at_unix", 0) or 0):
            by_id[vid] = v
    deduped = list(by_id.values())
    deduped.sort(key=lambda v: int(v.get("views", 0) or 0), reverse=True)
    bucket["videos"] = deduped[:CORPUS_MAX_PER_NICHE]


# ─── Public tick + stats ───────────────────────────────────────────────────


async def tick(budget_units: int | None = None, regions: list[str] | None = None) -> dict:
    """Run one backfill iteration.

    Args:
        budget_units: Max Data API units to spend this tick. Defaults to
                      CATALYST_BACKFILL_TICK_BUDGET (500). Hard-capped —
                      the tick stops as soon as spent+next_call > budget.
        regions: List of region codes to poll. Defaults to DEFAULT_REGIONS.

    Returns a summary dict for the admin endpoint / logs.
    """
    import youtube_quota

    budget = int(budget_units) if budget_units is not None else DEFAULT_TICK_BUDGET_UNITS
    target_regions = [r.strip().upper() for r in (regions or DEFAULT_REGIONS) if r.strip()] or ["US"]

    summary: dict[str, Any] = {
        "tick_started_at_unix": time.time(),
        "regions_polled": [],
        "fetched": 0,
        "classified": 0,
        "skipped_unclassified": 0,
        "skipped_low_views": 0,
        "quota_spent": 0,
        "by_niche": {},
        "errors": [],
    }

    spent_at_start = await youtube_quota.spent_today()

    for region in target_regions:
        # Every /videos.list call is 1 unit. Stop if we're about to exceed the tick budget.
        if (await youtube_quota.spent_today()) - spent_at_start + 1 > budget:
            summary["errors"].append(f"tick budget {budget} reached before region={region}")
            break

        videos = await fetch_trending_batch(region=region, max_results=50, quota_kind="background")
        summary["regions_polled"].append(region)
        summary["fetched"] += len(videos)

        async with _lock:
            _load_if_needed()
            for vid in videos:
                views = int(float((vid.get("statistics") or {}).get("viewCount", 0) or 0))
                if views < CORPUS_MIN_VIEWS:
                    summary["skipped_low_views"] += 1
                    continue

                niche_info = _infer_niche_for_video(vid)
                niche_key = str(niche_info.get("key", "") or "")
                if not niche_key:
                    summary["skipped_unclassified"] += 1
                    continue

                reference = _reference_from_video(vid, niche_info)
                reference["source_region"] = region

                bucket = _corpus["by_niche"].setdefault(niche_key, {"videos": [], "label": niche_info.get("label", ""), "last_ingested_unix": 0.0})
                bucket["label"] = niche_info.get("label", bucket.get("label", ""))
                bucket.setdefault("videos", []).append(reference)
                bucket["last_ingested_unix"] = time.time()
                _prune_niche_bucket(bucket)

                summary["classified"] += 1
                summary["by_niche"][niche_key] = summary["by_niche"].get(niche_key, 0) + 1

            _corpus["last_tick_unix"] = time.time()
            _corpus.setdefault("stats", {"total_ticks": 0, "total_videos_ingested": 0})
            _corpus["stats"]["total_ticks"] = int(_corpus["stats"].get("total_ticks", 0)) + 1
            _corpus["stats"]["total_videos_ingested"] = int(_corpus["stats"].get("total_videos_ingested", 0)) + summary["classified"]
            _save()

    summary["quota_spent"] = (await youtube_quota.spent_today()) - spent_at_start
    summary["tick_completed_at_unix"] = time.time()
    summary["tick_duration_sec"] = round(summary["tick_completed_at_unix"] - summary["tick_started_at_unix"], 2)
    log.info(
        "Catalyst backfill tick: fetched=%d classified=%d unclassified=%d low_views=%d quota_spent=%d regions=%s",
        summary["fetched"], summary["classified"], summary["skipped_unclassified"],
        summary["skipped_low_views"], summary["quota_spent"], summary["regions_polled"],
    )
    return summary


async def corpus_stats() -> dict:
    """Return a snapshot of the reference corpus for admin UI / monitoring."""
    async with _lock:
        _load_if_needed()
        by_niche_summary: dict[str, dict] = {}
        total_videos = 0
        for niche_key, bucket in (_corpus.get("by_niche") or {}).items():
            videos = list(bucket.get("videos") or [])
            if not videos:
                continue
            total_videos += len(videos)
            views_sorted = sorted((int(v.get("views", 0) or 0) for v in videos), reverse=True)
            ingested = [float(v.get("ingested_at_unix", 0) or 0) for v in videos]
            by_niche_summary[niche_key] = {
                "label": str(bucket.get("label", "") or ""),
                "count": len(videos),
                "max_views": views_sorted[0] if views_sorted else 0,
                "median_views": views_sorted[len(views_sorted) // 2] if views_sorted else 0,
                "last_ingested_unix": float(bucket.get("last_ingested_unix", 0) or 0),
                "oldest_ingested_unix": min(ingested) if ingested else 0.0,
                "newest_ingested_unix": max(ingested) if ingested else 0.0,
            }
        try:
            size_bytes = _CORPUS_FILE.stat().st_size if _CORPUS_FILE.exists() else 0
        except Exception:
            size_bytes = 0
        return {
            "total_videos": total_videos,
            "by_niche": by_niche_summary,
            "last_tick_unix": float(_corpus.get("last_tick_unix", 0) or 0),
            "stats": dict(_corpus.get("stats") or {}),
            "corpus_file_size_bytes": size_bytes,
            "max_per_niche": CORPUS_MAX_PER_NICHE,
            "min_views_threshold": CORPUS_MIN_VIEWS,
        }


async def top_videos_for_niche(niche_key: str, limit: int = 20) -> list[dict]:
    """Return the top-N videos (by views) currently in the corpus for a niche.

    Used by Catalyst learning + script generation to know what works per niche.
    """
    async with _lock:
        _load_if_needed()
        bucket = (_corpus.get("by_niche") or {}).get(str(niche_key or "").strip().lower()) or {}
        videos = list(bucket.get("videos") or [])
        videos.sort(key=lambda v: int(v.get("views", 0) or 0), reverse=True)
        return videos[:max(1, int(limit or 20))]
