"""
Studio Analytics & Insights — channel Catalyst data + public YouTube search trends.

  GET  /api/studio/analytics/channels
  GET  /api/studio/analytics/channel?channel_id=&registry_key=
  GET  /api/studio/analytics/search-trends?query=&days=30&registry_key=
  GET  /api/studio/analytics/product          (admin product metrics passthrough)
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException, Query

from long_form.catalyst_bridge import (
    CHANNEL_KEY_TO_ID,
    fetch_channel_snapshot,
    shape_catalyst_insights,
)
from studio_agent.access import is_owner


def _user_id(user: dict) -> str:
    return str((user or {}).get("id") or (user or {}).get("user_id") or "")


async def _fetch_public_search_videos(
    query: str,
    *,
    days: int = 30,
    max_results: int = 15,
    order: str = "viewCount",
    fresh: bool = False,
) -> list[dict[str, Any]]:
    """Recent public YouTube search results (Data API key, no OAuth).

    Search results alone are not evidence for view-count or "trend" claims.
    This helper always tries to hydrate returned IDs through videos.list and
    labels the evidence quality so Catalyst/Studio Agent cannot quietly treat
    snippet-only rows as verified performance data.
    """
    from youtube import _youtube_fetch_public_videos_api_key, _youtube_public_api_get

    q = " ".join(str(query or "").split()).strip()
    if not q:
        return []
    published_after = datetime.now(timezone.utc) - timedelta(days=max(1, min(int(days or 30), 90)))
    search_key = ""
    try:
        payload, search_key = await _youtube_public_api_get(
            "/search",
            params={
                "part": "snippet",
                "type": "video",
                "q": q,
                "order": order,
                "maxResults": max(1, min(int(max_results or 15), 25)),
                "publishedAfter": published_after.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "relevanceLanguage": "en",
            },
            timeout_sec=25,
            quota_kind="user",
            quota_note=f"studio_analytics_search:{q[:48]}",
            cache_bypass=bool(fresh),
        )
    except Exception as exc:
        return [{"error": str(exc)[:200]}]

    raw_rows: list[dict[str, Any]] = []
    video_ids: list[str] = []
    for item in list(payload.get("items") or []):
        sn = (item or {}).get("snippet") or {}
        vid = ((item or {}).get("id") or {}).get("videoId") or ""
        if vid:
            video_ids.append(str(vid))
        raw_rows.append({
            "video_id": vid,
            "title": str(sn.get("title") or ""),
            "channel": str(sn.get("channelTitle") or ""),
            "channel_title": str(sn.get("channelTitle") or ""),
            "channel_id": str(sn.get("channelId") or ""),
            "published_at": str(sn.get("publishedAt") or ""),
            "description": str(sn.get("description") or "")[:240],
            "watch_url": f"https://www.youtube.com/watch?v={vid}" if vid else "",
        })

    hydrate_error = ""
    hydrated_by_id: dict[str, dict[str, Any]] = {}
    if video_ids:
        try:
            hydrated = await _youtube_fetch_public_videos_api_key(video_ids)
            hydrated_by_id = {str(row.get("video_id") or ""): row for row in hydrated if isinstance(row, dict)}
        except Exception as exc:
            hydrate_error = str(exc)[:200]

    def _cache_status(active_key: str) -> str:
        if active_key == "(cache)":
            return "cache"
        if active_key == "(stale-cache)":
            return "stale-cache"
        return "fresh"

    out: list[dict[str, Any]] = []
    for row in raw_rows:
        vid = str(row.get("video_id") or "")
        stats = hydrated_by_id.get(vid) or {}
        if stats:
            row.update({
                "title": stats.get("title") or row.get("title") or "",
                "channel": stats.get("channel_title") or row.get("channel") or "",
                "channel_title": stats.get("channel_title") or row.get("channel_title") or "",
                "published_at": stats.get("published_at") or row.get("published_at") or "",
                "thumbnail_url": stats.get("thumbnail_url") or "",
                "views": int(stats.get("views") or 0),
                "likes": int(stats.get("likes") or 0),
                "comments": int(stats.get("comments") or 0),
                "duration_sec": int(stats.get("duration_sec") or 0),
                "tags": stats.get("tags") or [],
                "evidence_level": "hydrated_video_stats",
                "support_label": _trend_support_label(
                    views=int(stats.get("views") or 0),
                    published_at=str(stats.get("published_at") or row.get("published_at") or ""),
                    days=days,
                ),
            })
        else:
            row.update({
                "views": None,
                "likes": None,
                "comments": None,
                "duration_sec": None,
                "tags": [],
                "evidence_level": "snippet_only",
                "support_label": "unsupported_no_hydrated_stats",
                **({"evidence_error": hydrate_error} if hydrate_error else {}),
            })
        row.update({
            "search_order": order,
            "query_window_days": days,
            "cache_status": _cache_status(search_key),
            "private_analytics": False,
        })
        out.append(row)
    return out


def _trend_support_label(*, views: int, published_at: str, days: int) -> str:
    """Conservative public-search support labels for agent/UI wording."""
    try:
        published = datetime.fromisoformat(str(published_at or "").replace("Z", "+00:00"))
        age_days = max(0.0, (datetime.now(timezone.utc) - published).total_seconds() / 86400)
    except Exception:
        age_days = float(days or 30)
    views = int(views or 0)
    window = max(1, min(int(days or 30), 90))
    if views >= 1_000_000 and age_days <= window:
        return "strong_public_precedent"
    if views >= 100_000 and age_days <= window:
        return "supported_public_precedent"
    if views >= 10_000:
        return "weak_public_precedent"
    return "unsupported_or_low_signal"


def _default_queries_for_registry(registry_key: str) -> list[str]:
    """Niche search queries derived from channel registry label + key."""
    try:
        from long_form.prompts.channels import get_channel

        ch = get_channel(registry_key)
        label = str(ch.get("label") or registry_key).replace("_", " ")
        fmt = str(ch.get("format") or "long_form")
        if fmt == "shorts":
            return [f"{label} shorts", f"{label} viral"]
        return [label, f"{label} documentary", f"{label} explained"]
    except Exception:
        return [registry_key.replace("_", " ")]


def _predict_topics(
    *,
    trending_titles: list[str],
    channel_titles: list[str],
    niche_keywords: list[str],
    limit: int = 8,
) -> list[dict[str, Any]]:
    from youtube import score_topic_opportunity

    candidates: list[str] = []
    seen: set[str] = set()
    for t in trending_titles:
        t = str(t or "").strip()
        if len(t) < 12 or t.lower() in seen:
            continue
        seen.add(t.lower())
        candidates.append(t)
    for kw in niche_keywords:
        kw = str(kw or "").strip()
        if kw and kw.lower() not in seen:
            seen.add(kw.lower())
            candidates.append(kw)

    scored: list[dict[str, Any]] = []
    for c in candidates[:20]:
        scored.append(
            score_topic_opportunity(
                c,
                channel_titles,
                trending_titles,
                niche_keywords,
            )
        )
    scored.sort(key=lambda x: float(x.get("composite_score") or 0), reverse=True)
    return scored[:limit]


def _public_search_evidence_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    hydrated = [r for r in list(rows or []) if r.get("evidence_level") == "hydrated_video_stats"]
    supported = [
        r for r in hydrated
        if str(r.get("support_label") or "") in {"strong_public_precedent", "supported_public_precedent"}
    ]
    stale = [r for r in list(rows or []) if str(r.get("cache_status") or "") == "stale-cache"]
    errors = [r for r in list(rows or []) if r.get("error") or r.get("evidence_error")]
    return {
        "total_rows": len(list(rows or [])),
        "hydrated_rows": len(hydrated),
        "supported_rows": len(supported),
        "stale_rows": len(stale),
        "error_rows": len(errors),
        "strongest_public_evidence": [
            {
                "title": r.get("title"),
                "channel": r.get("channel") or r.get("channel_title"),
                "views": r.get("views"),
                "likes": r.get("likes"),
                "duration_sec": r.get("duration_sec"),
                "published_at": r.get("published_at"),
                "watch_url": r.get("watch_url"),
                "support_label": r.get("support_label"),
                "cache_status": r.get("cache_status"),
            }
            for r in sorted(supported, key=lambda item: int(item.get("views") or 0), reverse=True)[:5]
        ],
    }


def build_studio_analytics_router(
    *,
    require_auth: Callable,
    is_admin_check: Callable[[dict], bool] | None = None,
    admin_analytics_fn: Callable | None = None,
) -> APIRouter:
    router = APIRouter(prefix="/api/studio/analytics", tags=["studio-analytics"])

    def _auth(user: dict = Depends(require_auth)) -> dict:
        return user

    def _admin_only(user: dict = Depends(require_auth)) -> dict:
        if _full_analytics_access(user):
            return user
        raise HTTPException(403, "Admin access required")

    def _full_analytics_access(user: dict) -> bool:
        if is_owner(user, is_admin_check):
            return True
        return bool(is_admin_check and is_admin_check(user))

    def _channels_payload(hyd: dict[str, Any], *, scope_user_id: str | None) -> list[dict[str, Any]]:
        from long_form.prompts.channels import CHANNELS

        id_to_key = {v["channel_id"]: k for k, v in CHANNELS.items() if v.get("channel_id")}
        out: list[dict[str, Any]] = []
        user_buckets = hyd.values() if scope_user_id is None else [hyd.get(scope_user_id) or {}]
        for u in user_buckets:
            if not isinstance(u, dict):
                continue
            for ch_id, rec in (u.get("channels") or {}).items():
                if not isinstance(rec, dict):
                    continue
                snap = rec.get("analytics_snapshot") or {}
                key = id_to_key.get(ch_id, "")
                ch_meta = CHANNELS.get(key, {}) if key else {}
                out.append({
                    "channel_id": ch_id,
                    "channel_title": rec.get("title") or rec.get("channel_handle") or ch_id,
                    "subscriber_count": int(rec.get("subscriber_count", 0) or 0),
                    "view_count": int(rec.get("view_count", 0) or 0),
                    "video_count": int(rec.get("video_count", 0) or 0),
                    "harvest_present": bool(snap),
                    "registry_key": key,
                    "registry_label": ch_meta.get("label", ""),
                    "registry_format": ch_meta.get("format", ""),
                })
        out.sort(key=lambda c: (-int(c.get("subscriber_count") or 0), c.get("channel_title") or ""))
        return out

    @router.get("/channels")
    async def list_channels(
        user: dict = Depends(_auth),
        sync: bool = Query(True),
    ):
        if sync:
            try:
                from youtube import _list_connected_youtube_channels_for_user

                await _list_connected_youtube_channels_for_user(user, sync=True)
            except Exception:
                # Fall back to the most recent stored snapshot; this endpoint
                # should keep the dashboard usable even if YouTube is throttled.
                pass
        try:
            from youtube_connections_store import hydrate

            hyd = hydrate() or {}
        except Exception as exc:
            raise HTTPException(503, f"connection store unavailable: {exc}") from exc

        uid = _user_id(user)
        scope = None if _full_analytics_access(user) else uid
        if scope is not None and not scope:
            raise HTTPException(401, "sign in required")
        out = _channels_payload(hyd, scope_user_id=scope)
        return {"channels": out, "total": len(out), "scope": "all" if scope is None else "user"}

    @router.get("/channel")
    async def channel_analytics(
        user: dict = Depends(_auth),
        channel_id: str = Query(""),
        registry_key: str = Query(""),
    ):
        ch_id = str(channel_id or "").strip()
        reg_key = str(registry_key or "").strip()
        if not ch_id and reg_key:
            ch_id = CHANNEL_KEY_TO_ID.get(reg_key, "")
        if not ch_id:
            raise HTTPException(400, "channel_id or registry_key required")

        if not _full_analytics_access(user):
            uid = _user_id(user)
            try:
                from youtube_connections_store import hydrate

                bucket = (hydrate() or {}).get(uid) or {}
                allowed = set((bucket.get("channels") or {}).keys())
            except Exception:
                allowed = set()
            if ch_id not in allowed:
                raise HTTPException(403, "channel not linked to your account")

        record = fetch_channel_snapshot(ch_id)
        insights = shape_catalyst_insights(record)
        snap = (record or {}).get("analytics_snapshot") or {}

        velocity: dict[str, Any] = {}
        try:
            from youtube import _youtube_connected_channel_access_token, youtube_get_latest_video_velocity

            access_token, _rec = await _youtube_connected_channel_access_token(user, ch_id)
            if access_token:
                velocity = await youtube_get_latest_video_velocity(
                    access_token=access_token,
                    channel_id=ch_id,
                )
        except Exception:
            velocity = {}

        return {
            "channel_id": ch_id,
            "registry_key": reg_key or next(
                (k for k, v in CHANNEL_KEY_TO_ID.items() if v == ch_id),
                "",
            ),
            "channel_title": (record or {}).get("title") or (record or {}).get("channel_handle") or "",
            "insights": insights,
            "analytics_snapshot": {
                "channel_summary": snap.get("channel_summary"),
                "packaging_learnings": snap.get("packaging_learnings") or [],
                "retention_learnings": snap.get("retention_learnings") or [],
                "title_pattern_hints": snap.get("title_pattern_hints") or [],
                "historical_compare": snap.get("historical_compare"),
            },
            "velocity": velocity,
        }

    @router.get("/search-trends")
    async def search_trends(
        user: dict = Depends(_auth),
        query: str = Query(""),
        days: int = Query(30, ge=7, le=90),
        registry_key: str = Query(""),
        max_results: int = Query(12, ge=5, le=25),
    ):
        reg_key = str(registry_key or "").strip()
        queries = [str(query).strip()] if str(query or "").strip() else []
        if not queries and reg_key:
            queries = _default_queries_for_registry(reg_key)
        if not queries:
            queries = ["YouTube documentary", "viral shorts 2026"]

        channel_titles: list[str] = []
        niche_keywords: list[str] = []
        if reg_key:
            try:
                from long_form.prompts.channels import get_channel

                ch = get_channel(reg_key)
                niche_keywords = [str(ch.get("label") or reg_key)]
                ch_id = CHANNEL_KEY_TO_ID.get(reg_key, "")
                if ch_id:
                    rec = fetch_channel_snapshot(ch_id)
                    ins = shape_catalyst_insights(rec)
                    channel_titles = [t.get("title", "") for t in ins.get("top_titles") or []]
            except Exception:
                pass

        by_query: dict[str, list[dict[str, Any]]] = {}
        all_titles: list[str] = []
        for q in queries[:3]:
            recent = await _fetch_public_search_videos(q, days=days, max_results=max_results, order="date")
            popular = await _fetch_public_search_videos(q, days=days, max_results=max_results, order="viewCount")
            merged: list[dict[str, Any]] = []
            seen_ids: set[str] = set()
            for row in recent + popular:
                vid = str(row.get("video_id") or "")
                if vid and vid in seen_ids:
                    continue
                if vid:
                    seen_ids.add(vid)
                merged.append({**row, "query": q})
                if row.get("title"):
                    all_titles.append(str(row["title"]))
            by_query[q] = merged

        predictions = _predict_topics(
            trending_titles=all_titles,
            channel_titles=channel_titles,
            niche_keywords=niche_keywords or queries,
        )

        return {
            "window_days": days,
            "queries": queries,
            "results_by_query": by_query,
            "evidence_summary": _public_search_evidence_summary(
                [row for rows in by_query.values() for row in list(rows or [])]
            ),
            "predicted_topics": predictions,
            "evidence_contract": (
                "Public search rows must be hydrated before they support view-count or trend claims. "
                "Predicted topics are candidate ideas unless tied to strongest_public_evidence."
            ),
            "note": "Public search uses YouTube Data API. Predictions score niche fit, channel gap, and public-search title momentum; they are not private YouTube Analytics.",
        }

    @router.get("/product")
    async def product_metrics(user: dict = Depends(_admin_only)):
        if not admin_analytics_fn:
            raise HTTPException(501, "Product analytics not wired")
        return await admin_analytics_fn(user)

    return router
