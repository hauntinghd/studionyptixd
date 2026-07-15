"""Always-on Catalyst runtime for Studio Agent (channel sync + public niche warm cache)."""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

log = logging.getLogger("nyptid-studio.catalyst_runtime")

def _auto_enabled() -> bool:
    """Read env at call time so dotenv load order cannot freeze this as False."""
    return str(os.getenv("STUDIO_CATALYST_AUTO_ENABLED", "1")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


# Prefer _auto_enabled() at runtime; this snapshot is for callers that only read the attr.
AUTO_ENABLED = _auto_enabled()
# Default 10 minutes — continuous enough for Live Demand niches without burning YouTube quota.
TICK_INTERVAL_SEC = int(os.getenv("STUDIO_CATALYST_TICK_INTERVAL_SEC", str(10 * 60)))
CHANNEL_SYNC_MAX = int(os.getenv("STUDIO_CATALYST_CHANNEL_SYNC_MAX", "8"))
NICHE_WARM_MAX = int(os.getenv("STUDIO_CATALYST_NICHE_WARM_MAX", "6"))

_niche_queries: dict[str, float] = {}
_learning_events: list[dict[str, Any]] = []
_loop_task: asyncio.Task | None = None
_lock = asyncio.Lock()
_last_tick_at: float = 0.0


def register_niche_query(query: str) -> None:
    """Remember a niche query Studio Agent searched so background ticks can warm it."""
    clean = " ".join(str(query or "").split()).strip()
    if not clean:
        return
    _niche_queries[clean] = time.time()
    if len(_niche_queries) > 80:
        oldest = sorted(_niche_queries.items(), key=lambda item: item[1])[:20]
        for key, _ts in oldest:
            _niche_queries.pop(key, None)


def record_runtime_learning_event(
    *,
    kind: str,
    query: str = "",
    detail: str = "",
    metadata: dict[str, Any] | None = None,
) -> None:
    """In-memory continuous learning signal (also flushed into channel memory on ticks)."""
    global _learning_events
    _learning_events.append(
        {
            "kind": str(kind or "").strip()[:80],
            "query": str(query or "").strip()[:220],
            "detail": str(detail or "").strip()[:300],
            "metadata": dict(metadata or {}),
            "at": time.time(),
        }
    )
    if len(_learning_events) > 200:
        _learning_events = _learning_events[-120:]


def catalyst_runtime_status() -> dict[str, Any]:
    """Health snapshot for diagnostics / admin."""
    return {
        "auto_enabled": _auto_enabled(),
        "tick_interval_sec": TICK_INTERVAL_SEC,
        "loop_running": bool(_loop_task is not None and not _loop_task.done()),
        "tracked_niches": len(_niche_queries),
        "pending_learning_events": len(_learning_events),
        "last_tick_at": _last_tick_at,
    }


def schedule_niche_warm(query: str) -> None:
    """Fire-and-forget warm for one query."""
    register_niche_query(query)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    loop.create_task(warm_public_niche_query(query), name="studio-catalyst-warm")


def schedule_session_catalyst_warm(
    *,
    user_id: str = "",
    channel_id: str = "",
    search_query: str = "",
) -> None:
    """Warm public niche cache and lightly refresh a connected channel after agent work."""
    if search_query:
        schedule_niche_warm(search_query)
    if not user_id or not channel_id:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    loop.create_task(
        refresh_connected_channel(user_id, channel_id),
        name="studio-catalyst-channel-refresh",
    )


async def warm_public_niche_query(query: str) -> dict[str, Any]:
    clean = " ".join(str(query or "").split()).strip()
    if not clean:
        return {"ok": False, "reason": "empty_query"}
    try:
        import youtube_quota
        from studio_agent.turn_plan import (
            refine_public_search_query,
            sanitize_public_search_query,
        )
        from studio_agent.catalyst_prediction import filter_public_rows_for_query
        from studio_analytics_router import _merge_public_search_orders

        if youtube_quota.background_should_pause_sync():
            snap = youtube_quota.snapshot_sync()
            return {
                "ok": False,
                "reason": "youtube_quota_background_paused",
                "query": clean,
                "quota": snap,
            }
        if not youtube_quota.can_afford_sync(100):
            return {
                "ok": False,
                "reason": "youtube_quota_exhausted",
                "query": clean,
                "quota": youtube_quota.snapshot_sync(),
            }

        sanitized = sanitize_public_search_query(clean) or clean
        sanitized = refine_public_search_query(sanitized) or sanitized
        if not sanitized:
            return {"ok": False, "reason": "meta_query"}
        # Cache-first single search.list (100 units). Never fresh=true on background warm.
        rows = await _merge_public_search_orders(
            sanitized,
            days=7,
            max_results=8,
            fresh=False,
            prefer_recent=True,
        )
        rows = filter_public_rows_for_query(rows, search_query=sanitized, user_text=sanitized)
        hydrated = [
            row for row in list(rows or [])
            if isinstance(row, dict) and str(row.get("evidence_level") or "") == "hydrated_video_stats"
        ]
        register_niche_query(sanitized)
        return {
            "ok": True,
            "query": sanitized,
            "hydrated_rows": len(hydrated),
            "total_rows": len(list(rows or [])),
            "prefer_recent": True,
        }
    except Exception as exc:
        log.warning("Studio Catalyst niche warm failed for %s: %s", clean[:80], str(exc)[:200])
        return {"ok": False, "query": clean, "error": str(exc)[:200]}


async def refresh_connected_channel(user_id: str, channel_id: str) -> dict[str, Any]:
    uid = str(user_id or "").strip()
    ch_id = str(channel_id or "").strip()
    if not uid or not ch_id:
        return {"ok": False, "reason": "missing_ids"}
    try:
        from youtube import _youtube_sync_and_persist_for_user

        record = await _youtube_sync_and_persist_for_user(uid, ch_id)
        title = str((record or {}).get("title") or "").strip()
        return {"ok": True, "channel_id": ch_id, "title": title}
    except Exception as exc:
        log.warning("Studio Catalyst channel refresh failed (%s): %s", ch_id[:24], str(exc)[:200])
        return {"ok": False, "channel_id": ch_id, "error": str(exc)[:200]}


async def _sync_stale_connected_channels(max_channels: int = CHANNEL_SYNC_MAX) -> dict[str, Any]:
    synced = 0
    errors = 0
    try:
        from youtube_connections_store import hydrate

        hyd = hydrate() or {}
    except Exception as exc:
        return {"synced": 0, "errors": 1, "note": str(exc)[:160]}
    targets: list[tuple[str, str]] = []
    now = time.time()
    for uid, bucket in list(hyd.items()):
        if not isinstance(bucket, dict):
            continue
        for ch_id, rec in list((bucket.get("channels") or {}).items()):
            if not isinstance(rec, dict):
                continue
            last_sync = float(rec.get("last_synced_at") or rec.get("updated_at") or 0)
            if now - last_sync < 3600:
                continue
            targets.append((str(uid), str(ch_id)))
    for uid, ch_id in targets[: max(1, int(max_channels or 1))]:
        result = await refresh_connected_channel(uid, ch_id)
        if result.get("ok"):
            synced += 1
        else:
            errors += 1
    return {"synced": synced, "errors": errors, "candidates": len(targets)}


async def studio_catalyst_tick() -> dict[str, Any]:
    """One background pass: warm recent niche queries + refresh stale connected channels."""
    global _last_tick_at, _learning_events
    async with _lock:
        try:
            import youtube_quota

            if youtube_quota.background_should_pause_sync():
                snap = youtube_quota.snapshot_sync()
                _last_tick_at = time.time()
                log.info(
                    "Studio Catalyst tick SKIPPED (quota pause %.0f%% used, threshold %.0f%%)",
                    float(snap.get("pct_used") or 0) * 100,
                    float(snap.get("background_pause_pct") or 0.7) * 100,
                )
                return {
                    "ok": True,
                    "skipped": True,
                    "reason": "youtube_quota_background_paused",
                    "warmed_queries": 0,
                    "hydrated_total": 0,
                    "channels": {"synced": 0, "errors": 0},
                    "quota": snap,
                    "tick_at": _last_tick_at,
                }
        except Exception:
            pass
        warm_results: list[dict[str, Any]] = []
        recent_queries = sorted(_niche_queries.items(), key=lambda item: item[1], reverse=True)
        for query, _ts in recent_queries[: max(1, NICHE_WARM_MAX)]:
            result = await warm_public_niche_query(query)
            warm_results.append(result)
            if str(result.get("reason") or "") in {
                "youtube_quota_background_paused",
                "youtube_quota_exhausted",
            }:
                break  # stop the warm loop for this tick
        channel_summary = await _sync_stale_connected_channels()
        # Flush recent learning events into durable Catalyst learning records.
        flushed = 0
        try:
            from studio_agent import catalyst_learning

            events = list(_learning_events)
            _learning_events = []
            for event in events[-40:]:
                try:
                    catalyst_learning.record_turn_outcome(
                        "studio_catalyst_runtime",
                        {
                            "registry_key": "studio_agent_global",
                            "channel_title": "Studio Catalyst Runtime",
                        },
                        turn_kind="public_research",
                        search_query=str(event.get("query") or ""),
                        tool_fires=None,
                        predicted_topics=[],
                    )
                    flushed += 1
                except Exception:
                    continue
        except Exception:
            flushed = 0
        _last_tick_at = time.time()
        return {
            "ok": True,
            "warmed_queries": len(warm_results),
            "hydrated_total": sum(int(r.get("hydrated_rows") or 0) for r in warm_results),
            "channels": channel_summary,
            "learning_events_flushed": flushed,
            "tick_at": _last_tick_at,
        }


async def _auto_loop_runner() -> None:
    log.info("Studio Catalyst auto-loop starting (interval=%ds)", TICK_INTERVAL_SEC)
    try:
        await asyncio.sleep(90)
    except asyncio.CancelledError:
        return
    while True:
        try:
            summary = await studio_catalyst_tick()
            log.info(
                "Studio Catalyst tick: warmed=%s hydrated=%s channels_synced=%s",
                summary.get("warmed_queries"),
                summary.get("hydrated_total"),
                (summary.get("channels") or {}).get("synced"),
            )
        except Exception as exc:
            log.warning("Studio Catalyst tick failed: %s", str(exc)[:300])
        try:
            await asyncio.sleep(max(120, TICK_INTERVAL_SEC))
        except asyncio.CancelledError:
            return


def start_studio_catalyst_loop() -> None:
    global _loop_task, AUTO_ENABLED
    AUTO_ENABLED = _auto_enabled()
    if not AUTO_ENABLED:
        log.info("Studio Catalyst auto-loop disabled (STUDIO_CATALYST_AUTO_ENABLED=0)")
        return
    if _loop_task is not None and not _loop_task.done():
        return
    try:
        _loop_task = asyncio.create_task(_auto_loop_runner())
        log.info("Studio Catalyst auto-loop started (interval=%ss)", TICK_INTERVAL_SEC)
    except RuntimeError:
        log.warning("start_studio_catalyst_loop() called outside running event loop")


def stop_studio_catalyst_loop() -> None:
    global _loop_task
    if _loop_task is not None and not _loop_task.done():
        _loop_task.cancel()
    _loop_task = None