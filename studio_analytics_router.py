"""
Studio Analytics & Insights — channel Catalyst data + public YouTube search trends.

  GET  /api/studio/analytics/channels
  GET  /api/studio/analytics/channel?channel_id=&registry_key=
  GET  /api/studio/analytics/search-trends?query=&days=30&registry_key=
  GET  /api/studio/analytics/product          (admin product metrics passthrough)
"""
from __future__ import annotations

import asyncio
import re
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


def _public_video_momentum_fields(
    *,
    views: int,
    likes: int,
    comments: int,
    published_at: str,
) -> dict[str, Any]:
    """Derive lightweight public engagement signals from hydrated YouTube stats."""
    views = max(0, int(views or 0))
    likes = max(0, int(likes or 0))
    comments = max(0, int(comments or 0))
    age_days = 0.0
    try:
        published = datetime.fromisoformat(str(published_at or "").replace("Z", "+00:00"))
        age_days = max(0.25, (datetime.now(timezone.utc) - published).total_seconds() / 86400)
    except Exception:
        age_days = 30.0
    engagement_rate = round((likes + comments) / views, 4) if views else None
    like_ratio = round(likes / views, 4) if views else None
    views_per_day = round(views / age_days, 1) if views else None
    return {
        "engagement_rate": engagement_rate,
        "like_ratio": like_ratio,
        "views_per_day": views_per_day,
        "age_days": round(age_days, 2),
    }


TOP_PERFORMER_WINDOW_DAYS = 365
HISTORICAL_TOP_PERFORMER = True  # viewCount pass omits publishedAfter for all-time niche winners


def _published_after_cutoff(*, days: int, historical: bool = False) -> datetime | None:
    """Return publishedAfter cutoff, or None when historical all-time search is requested."""
    if historical:
        return None
    # Live Demand supports 1-day windows (markets / "last 24h"); floor used to be 7.
    window = max(1, min(int(days or 30), 90))
    return datetime.now(timezone.utc) - timedelta(days=window)


def _row_usable_for_topic_prediction(row: dict[str, Any], *, search_query: str = "") -> bool:
    if str(row.get("evidence_level") or "") != "hydrated_video_stats":
        return False
    title = str(row.get("title") or "").strip()
    if search_query:
        try:
            from studio_agent.catalyst_prediction import (
                _is_day_trading_false_positive,
                _query_is_day_trading_niche,
                title_matches_day_trading_intent,
            )

            if _query_is_day_trading_niche([search_query]):
                if _is_day_trading_false_positive(title) or not title_matches_day_trading_intent(title):
                    return False
        except Exception:
            pass
    support = str(row.get("support_label") or "").strip()
    if support in {
        "strong_public_precedent",
        "supported_public_precedent",
        "weak_public_precedent",
        "exploratory_public_signal",
    }:
        return True
    window = int(row.get("query_window_days") or 0)
    views = int(row.get("views") or 0)
    if window and window <= 7:
        return views >= 500
    return views >= 25_000


async def _merge_public_search_orders(
    query: str,
    *,
    days: int = 30,
    max_results: int = 10,
    fresh: bool = False,
    top_performer_days: int | None = None,
    prefer_recent: bool | None = None,
) -> list[dict[str, Any]]:
    """Merge recent + top-viewed public search rows for richer demand context.

    Quota strategy:
    - fresh=true (user asked live/right-now): dual search.list (date + viewCount) = 200 units/query
    - default/cache-first: single viewCount search.list (100 units) and derive recent
      momentum from hydrated published_at + views_per_day (prefer_recent only filters age)

    Window strategy:
    - recent_momentum uses the caller's short ``days`` window (default 30)
    - top_performers uses a longer viewCount window UNLESS prefer_recent (Live Demand
      12–24h / 1–7d windows) — then top performers stay inside a short age cap so
      2022 fidget-toy virals cannot dominate a day-trading demand brief.
    """
    from studio_agent.turn_plan import refine_public_search_query, simplify_public_search_query
    from studio_agent.catalyst_prediction import filter_public_rows_for_query

    search_query = " ".join(str(query or "").split()).strip()
    search_query = refine_public_search_query(search_query) or search_query
    momentum_days = max(1, min(int(days or 30), 90))
    if prefer_recent is None:
        # Prefer-recent age filtering does NOT require a second search.list — only fresh does.
        prefer_recent = bool(momentum_days <= 7)
    # Live/fresh short windows: never use all-time historical top performers.
    use_historical = bool(HISTORICAL_TOP_PERFORMER and not prefer_recent and momentum_days > 7)
    if prefer_recent:
        performer_days = max(momentum_days, min(int(top_performer_days or 30), 30))
    else:
        performer_days = max(
            momentum_days,
            min(int(top_performer_days or TOP_PERFORMER_WINDOW_DAYS), 365),
        )
    popular = await _fetch_public_search_videos(
        search_query,
        days=performer_days,
        max_results=max_results,
        order="viewCount",
        fresh=fresh,
        historical=use_historical,
        search_profile="top_performers",
    )
    if not popular and search_query:
        simplified = simplify_public_search_query(search_query)
        if simplified and simplified.lower() != search_query.lower():
            search_query = refine_public_search_query(simplified) or simplified
            popular = await _fetch_public_search_videos(
                search_query,
                days=performer_days,
                max_results=max_results,
                order="viewCount",
                fresh=fresh,
                historical=use_historical,
                search_profile="top_performers",
            )
    query = search_query
    merged: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    def _row_on_query_niche(row: dict[str, Any]) -> bool:
        """Drop obvious off-niche false positives (fidget trading vs day trading)."""
        try:
            from studio_agent.catalyst_prediction import (
                _is_day_trading_false_positive,
                _query_is_day_trading_niche,
                title_matches_day_trading_intent,
            )
        except Exception:
            # Fail closed for day-trading-looking queries; fail open otherwise.
            title = str(row.get("title") or "")
            low_q = str(query or "").lower()
            if "day trad" in low_q or ("trading" in low_q and any(
                w in low_q for w in ("stock", "forex", "futures", "market", "scalp")
            )):
                return False
            return True
        title = str(row.get("title") or "")
        if _query_is_day_trading_niche([query]):
            if _is_day_trading_false_positive(title):
                return False
            return title_matches_day_trading_intent(title)
        return True

    def _append(rows: list[dict[str, Any]], profile: str, *, window_days: int) -> None:
        for row in rows:
            if not isinstance(row, dict):
                continue
            if not _row_on_query_niche(row):
                continue
            if prefer_recent and profile == "top_performers":
                age = float(row.get("age_days") or 999)
                # Keep evergreen only if still inside a tight recent band
                if age > float(max(momentum_days * 4, 14)):
                    continue
            vid = str(row.get("video_id") or "")
            if vid and vid in seen_ids:
                continue
            if vid:
                seen_ids.add(vid)
            merged.append({
                **row,
                "search_profile": profile,
                "query": query,
                "query_window_days": window_days,
            })

    # Dual search.list (date order) ONLY when fresh=true. prefer_recent alone must not
    # double the cost — that was a major quota burner for Catalyst + Live Demand.
    if fresh:
        recent = await _fetch_public_search_videos(
            query,
            days=momentum_days,
            max_results=max(max_results, 12),
            order="date",
            fresh=True,
            historical=False,
            search_profile="recent_momentum",
        )
        _append(recent, "recent_momentum", window_days=momentum_days)
    else:
        age_cap = float(max(1, momentum_days if prefer_recent else max(momentum_days, 30)))
        recent_candidates = sorted(
            [
                row for row in popular
                if isinstance(row, dict)
                and str(row.get("evidence_level") or "") == "hydrated_video_stats"
                and float(row.get("age_days") or 999) <= age_cap
            ],
            key=lambda row: (
                float(row.get("views_per_day") or 0),
                int(row.get("views") or 0),
            ),
            reverse=True,
        )[:max_results]
        _append(recent_candidates, "recent_momentum_derived", window_days=momentum_days)

    def _max_views(rows: list[dict[str, Any]]) -> int:
        return max((int(row.get("views") or 0) for row in rows if isinstance(row, dict)), default=0)

    query_low = str(query or "").lower()
    shorts_scoped_query = "shorts" in query_low or re.search(r"\bshort\b", query_low)
    if use_historical and _max_views(popular) < 100_000 and not shorts_scoped_query:
        long_form_query = f"{query} documentary -shorts"
        alt_popular = await _fetch_public_search_videos(
            long_form_query,
            days=performer_days,
            max_results=max_results,
            order="viewCount",
            fresh=fresh,
            historical=True,
            search_profile="top_performers",
        )
        if _max_views(alt_popular) > _max_views(popular):
            popular = alt_popular
            query = long_form_query

    # Prefer-recent: only backfill top performers when recent rows are thin — and never
    # with all-time historical age (window_days=0) for Live Demand short windows.
    if prefer_recent:
        if len(merged) < max(4, max_results // 2):
            _append(popular, "top_performers", window_days=performer_days)
    else:
        _append(
            popular,
            "top_performers",
            window_days=0 if use_historical else performer_days,
        )
    # Final hard filter at the search foundation (every consumer sees clean rows only).
    return filter_public_rows_for_query(merged, search_query=query, user_text=query)


async def _fetch_public_search_videos(
    query: str,
    *,
    days: int = 30,
    max_results: int = 15,
    order: str = "viewCount",
    fresh: bool = False,
    historical: bool = False,
    search_profile: str = "",
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
    published_after = _published_after_cutoff(days=days, historical=historical)
    search_params: dict[str, Any] = {
        "part": "snippet",
        "type": "video",
        "q": q,
        "order": order,
        "maxResults": max(1, min(int(max_results or 15), 25)),
        "relevanceLanguage": "en",
    }
    if published_after is not None:
        search_params["publishedAfter"] = published_after.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    search_key = ""
    try:
        payload, search_key = await _youtube_public_api_get(
            "/search",
            params=search_params,
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
            views = int(stats.get("views") or 0)
            likes = int(stats.get("likes") or 0)
            comments = int(stats.get("comments") or 0)
            published_at = str(stats.get("published_at") or row.get("published_at") or "")
            row.update({
                "title": stats.get("title") or row.get("title") or "",
                "channel": stats.get("channel_title") or row.get("channel") or "",
                "channel_title": stats.get("channel_title") or row.get("channel_title") or "",
                "published_at": published_at,
                "thumbnail_url": stats.get("thumbnail_url") or "",
                "views": views,
                "likes": likes,
                "comments": comments,
                "duration_sec": int(stats.get("duration_sec") or 0),
                "tags": stats.get("tags") or [],
                "evidence_level": "hydrated_video_stats",
                "support_label": _trend_support_label(
                    views=views,
                    published_at=published_at,
                    days=days,
                    search_profile=search_profile or ("top_performers" if historical else ""),
                ),
                **_public_video_momentum_fields(
                    views=views,
                    likes=likes,
                    comments=comments,
                    published_at=published_at,
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


def _trend_support_label(
    *,
    views: int,
    published_at: str,
    days: int,
    search_profile: str = "",
) -> str:
    """Conservative public-search support labels for agent/UI wording.

    Short Live Demand windows (1–7d) use velocity-aware thresholds so brand-new
    niche Shorts are not all marked unsupported just because they have not hit
    10k–25k lifetime views yet.
    """
    try:
        published = datetime.fromisoformat(str(published_at or "").replace("Z", "+00:00"))
        age_days = max(0.25, (datetime.now(timezone.utc) - published).total_seconds() / 86400)
    except Exception:
        age_days = max(0.25, float(days or 30))
    views = int(views or 0)
    profile = str(search_profile or "").strip().lower()
    window = max(1, min(int(days or 30), 90))
    views_per_day = views / max(age_days, 0.25)

    if profile == "top_performers":
        # Prefer-recent top_performers still use a short age cap; do not demand
        # all-time 100k winners for a 2-day Live Demand pass.
        if window <= 7:
            if views >= 50_000 or views_per_day >= 15_000:
                return "strong_public_precedent"
            if views >= 15_000 or views_per_day >= 5_000:
                return "supported_public_precedent"
            if views >= 3_000 or views_per_day >= 1_000:
                return "weak_public_precedent"
            if views >= 500:
                return "exploratory_public_signal"
            return "unsupported_or_low_signal"
        if views >= 500_000:
            return "strong_public_precedent"
        if views >= 100_000:
            return "supported_public_precedent"
        if views >= 25_000:
            return "weak_public_precedent"
        return "unsupported_or_low_signal"

    # recent_momentum (and default)
    if window <= 3:
        if views >= 25_000 or views_per_day >= 8_000:
            return "strong_public_precedent"
        if views >= 5_000 or views_per_day >= 2_000:
            return "supported_public_precedent"
        if views >= 1_000 or views_per_day >= 400:
            return "weak_public_precedent"
        if views >= 200:
            return "exploratory_public_signal"
        return "unsupported_or_low_signal"
    if window <= 7:
        if views >= 50_000 or views_per_day >= 7_000:
            return "strong_public_precedent"
        if views >= 10_000 or views_per_day >= 1_500:
            return "supported_public_precedent"
        if views >= 2_500 or views_per_day >= 400:
            return "weak_public_precedent"
        if views >= 400:
            return "exploratory_public_signal"
        return "unsupported_or_low_signal"

    if views >= 1_000_000 and age_days <= window:
        return "strong_public_precedent"
    if views >= 100_000 and age_days <= window:
        return "supported_public_precedent"
    if views >= 10_000 and age_days <= window:
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
    from studio_agent.turn_plan import _compact_search_keywords, is_meta_research_query

    for kw in niche_keywords:
        kw = str(kw or "").strip()
        if not kw:
            continue
        if is_meta_research_query(kw):
            compact_terms = _compact_search_keywords([kw], max_terms=6)
            kw = " ".join(compact_terms).strip()
            if not kw or is_meta_research_query(kw):
                continue
        if kw.lower() in seen:
            continue
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
            merged = await _merge_public_search_orders(
                q, days=days, max_results=max_results, fresh=False,
            )
            by_query[q] = merged
            for row in merged:
                if row.get("title"):
                    all_titles.append(str(row["title"]))

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
