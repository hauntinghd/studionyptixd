"""
Bridge layer: pulls Catalyst Hub channel-performance data and shapes it
into a context block the long-form generator can use to bias topic + hook.

Studio's Catalyst module already harvests:
  - Top-performing video titles (by views, by views-per-subscriber)
  - Hook formulas decoded from the channel's outliers
  - Recent thumbnail patterns
  - Breakout videos (high views vs channel baseline)
  - Reference-video style guides

We surface the highest-signal subset to:
  (a) The frontend Channel/Outline tab — so user sees suggested topics seeded
      by their channel's actual analytics.
  (b) The Grok outline call — so the system prompt knows what works on
      THIS channel, not generic-doc framing.

This file deliberately keeps the Catalyst-data shape thin / forgiving — if
Catalyst returns nothing for a channel (newly connected, no harvest yet),
we degrade gracefully to empty insights rather than blocking long-form gen.
"""
from __future__ import annotations
from typing import Any


# Channel key (long_form.prompts.channels) → YouTube channel ID
# (the OAuth lookup key in `youtube_channel_connections`).
# Pulled directly from each channel's "channel_id" field for single source of truth.
def _build_channel_id_map() -> dict[str, str]:
    from long_form.prompts.channels import CHANNELS
    return {k: v.get("channel_id", "") for k, v in CHANNELS.items() if v.get("channel_id")}


CHANNEL_KEY_TO_ID = _build_channel_id_map()


# Legacy alias preserved so older callers don't break — same map.
CHANNEL_KEY_TO_CATALYST_HANDLE = CHANNEL_KEY_TO_ID


def fetch_channel_snapshot(channel_id: str) -> dict | None:
    """
    Read the latest analytics_snapshot directly from the OAuth connection
    store for the given channel ID. The connection store mirrors what
    Catalyst Hub harvests during auto-tick — pulling here avoids a second
    HTTP hop through the Hub endpoints.
    """
    if not channel_id:
        return None
    try:
        from youtube_connections_store import hydrate
        hyd = hydrate()
    except Exception:
        return None
    for u in (hyd or {}).values():
        if not isinstance(u, dict):
            continue
        ch_map = u.get("channels", {})
        if not isinstance(ch_map, dict):
            continue
        rec = ch_map.get(channel_id)
        if isinstance(rec, dict):
            return rec
    return None


def shape_catalyst_insights(connection_record: dict | None) -> dict[str, Any]:
    """
    Trim a Catalyst connection-store record down to the long-form-relevant signals.

    Connection store shape (verified 2026-05-06):
      record["analytics_snapshot"] = {
          "top_videos": [{video_id, title, views, likes, comments, ...}],
          "top_video_titles": [str, ...],
          "channel_audit": {...},
          "channel_summary": {...},
          "packaging_learnings": [str, ...],
          "retention_learnings": [str, ...],
          "title_pattern_hints": [str, ...],
          "historical_compare": {...},
      }
    Plus channel-level fields directly on the record:
      record["subscriber_count"], record["view_count"], record["video_count"], etc.

    Empty/absent fields → empty arrays (frontend hides empty sections).
    """
    empty = {
        "top_titles": [], "breakout_titles": [], "hook_patterns": [],
        "thumbnail_signals": [], "subscribers": 0, "videos": 0,
        "channel_views": 0, "harvest_present": False,
    }
    if not isinstance(connection_record, dict):
        return empty

    snap = connection_record.get("analytics_snapshot") or {}
    if not isinstance(snap, dict):
        snap = {}

    # Channel-level metadata sits on the record itself, not in the snapshot.
    subs = int(connection_record.get("subscriber_count", 0) or 0)
    videos = int(connection_record.get("video_count", 0) or 0)
    ch_views = int(connection_record.get("view_count", 0) or 0)

    top_titles: list[dict[str, Any]] = []
    for v in (snap.get("top_videos") or [])[:10]:
        if not isinstance(v, dict):
            continue
        top_titles.append({
            "title": str(v.get("title", "")),
            "views": int(v.get("views", v.get("view_count", 0)) or 0),
            "video_id": str(v.get("video_id", "")),
            "likes": int(v.get("likes", v.get("like_count", 0)) or 0),
            "vps": (
                float(v.get("views", v.get("view_count", 0)) or 0) / subs
                if subs > 0 else 0.0
            ),
        })
    # Sort by views desc.
    top_titles.sort(key=lambda x: x["views"], reverse=True)

    # Breakouts = top titles where views > median × 2 (rough heuristic since the
    # connection store doesn't store an explicit "breakout_videos" list).
    breakouts: list[dict[str, Any]] = []
    if top_titles:
        view_list = sorted([t["views"] for t in top_titles])
        median = view_list[len(view_list) // 2] if view_list else 0
        for t in top_titles:
            if median > 0 and t["views"] >= median * 2:
                breakouts.append({
                    "title": t["title"],
                    "views": t["views"],
                    "video_id": t["video_id"],
                    "lift_vs_baseline": (t["views"] / median) if median else 0.0,
                })

    # Decoded hooks / packaging learnings — Catalyst stores these as plain string lists.
    hook_patterns: list[str] = []
    for src in (snap.get("title_pattern_hints"), snap.get("packaging_learnings")):
        if isinstance(src, list):
            for h in src[:6]:
                if isinstance(h, str) and h.strip() and h.strip() not in hook_patterns:
                    hook_patterns.append(h.strip())

    thumb_signals: list[str] = []
    for src in (snap.get("retention_learnings"),):
        if isinstance(src, list):
            for t in src[:4]:
                if isinstance(t, str) and t.strip():
                    thumb_signals.append(t.strip())

    return {
        "top_titles": top_titles,
        "breakout_titles": breakouts[:6],
        "hook_patterns": hook_patterns[:8],
        "thumbnail_signals": thumb_signals[:4],
        "subscribers": subs,
        "videos": videos,
        "channel_views": ch_views,
        "harvest_present": bool(snap),
    }


def insights_to_grok_context(insights: dict[str, Any]) -> str:
    """
    Format shaped insights as a plain-text block to inject into Grok's
    system prompt. Caps the total length so we don't blow the token budget.
    """
    if not insights:
        return ""
    lines: list[str] = []
    if insights.get("subscribers"):
        lines.append(f"- Channel subscriber count: {insights['subscribers']:,}")
    if insights.get("median_vps"):
        lines.append(f"- Median views-per-subscriber on the channel: {insights['median_vps']:.2f}")

    if insights.get("top_titles"):
        lines.append("- Top-performing titles on this channel (by views):")
        for t in insights["top_titles"][:6]:
            lines.append(f"    • [{t['views']:,} views] {t['title']}")

    if insights.get("breakout_titles"):
        lines.append("- Breakout titles (high lift vs channel baseline):")
        for t in insights["breakout_titles"][:4]:
            lift = t.get("lift_vs_baseline", 0)
            lines.append(f"    • [{lift:.1f}x lift] {t['title']}")

    if insights.get("hook_patterns"):
        lines.append("- Decoded hook formulas that work on this channel:")
        for h in insights["hook_patterns"][:4]:
            lines.append(f"    • {h}")

    if insights.get("thumbnail_signals"):
        lines.append("- Thumbnail patterns that get clicks:")
        for t in insights["thumbnail_signals"][:3]:
            lines.append(f"    • {t}")

    return "\n".join(lines)


def assess_channel_growth(insights: dict[str, Any] | None, *, harvest_present: bool) -> dict[str, Any]:
    """
    Turn Catalyst-shaped insights into a Studio Agent playbook:
    new vs established channel, what's working, what isn't, next posts.
    """
    ins = insights or {}
    subs = int(ins.get("subscribers") or 0)
    videos = int(ins.get("videos") or 0)
    ch_views = int(ins.get("channel_views") or 0)
    top = list(ins.get("top_titles") or [])
    breakouts = list(ins.get("breakout_titles") or [])
    hooks = list(ins.get("hook_patterns") or [])

    if subs < 100 and videos <= 8:
        stage = "brand_new"
        stage_label = "Brand-new channel"
    elif subs < 1_000 or (not harvest_present and subs < 5_000):
        stage = "early"
        stage_label = "Early channel (building baseline)"
    elif subs < 50_000:
        stage = "growing"
        stage_label = "Growing channel"
    else:
        stage = "established"
        stage_label = "Established channel"

    working: list[str] = []
    not_working: list[str] = []
    next_actions: list[str] = []

    if not harvest_present:
        not_working.append(
            "No Catalyst harvest yet — analytics snapshot missing. Connect YouTube in "
            "Studio → Settings → Channels and allow harvest to complete."
        )
        next_actions.append(
            "Call youtube_oauth_status, guide user to connect, then re-run get_channel_analytics."
        )
    elif not top and videos > 0:
        not_working.append(
            "Videos exist but no top-performer list in harvest — may need more uploads or a re-sync."
        )
    elif videos == 0:
        not_working.append("No published videos on record — cannot infer packaging or retention winners yet.")

    if top:
        best = top[0]
        working.append(
            f"Strongest title on record: «{best.get('title', '')}» ({int(best.get('views', 0)):,} views)."
        )
        if len(top) >= 2:
            weak = top[-1]
            if int(weak.get("views", 0)) < int(best.get("views", 0)) * 0.15:
                not_working.append(
                    f"Weak tail vs winner: «{weak.get('title', '')}» ({int(weak.get('views', 0)):,} views) — "
                    "study packaging gap vs top video."
                )
    if breakouts:
        b = breakouts[0]
        working.append(
            f"Breakout lift: «{b.get('title', '')}» (~{float(b.get('lift_vs_baseline', 0)):.1f}x vs median)."
        )
    if hooks:
        working.append(f"Hook patterns that repeat on-channel: {hooks[0][:120]}")

    if stage == "brand_new":
        next_actions.extend([
            "Treat as positioning sprint: 3–5 tightly themed uploads, one packaging style, no format hopping.",
            "Use get_public_search_trends + competitor analyze_competitor_video on 2 niche winners.",
            "Propose one Skeleton short OR one 8–12 min doc outline — not both until identity is clear.",
        ])
    elif stage == "early":
        next_actions.extend([
            "Double down on the best-performing title pattern; iterate thumbnails before new topics.",
            "Post 1–2x/week; each video should answer why someone would subscribe after one watch.",
        ])
    elif stage == "growing":
        next_actions.extend([
            "Clone breakout packaging (title cadence + thumb grammar) on adjacent topics.",
            "Alternate proven winners with one experimental topic per month (20% portfolio).",
        ])
    else:
        next_actions.extend([
            "Protect baseline CTR packaging; test only one variable per upload (title OR thumb OR hook).",
            "Use velocity data (if present) to time follow-ups within 48h of a spike.",
        ])

    if subs == 0 and ch_views == 0 and videos == 0:
        summary = (
            "This looks like a new or empty channel profile. Focus on niche positioning, "
            "competitor homework, and a repeatable first series — not diagnosing 'bad' videos yet."
        )
    elif not working and harvest_present:
        summary = (
            "Harvest is present but no clear winners — prioritize consistent uploads and "
            "tighter packaging before scaling spend on long renders."
        )
    elif working and not_working:
        summary = "Mixed signals: double down on winners; fix packaging on underperformers."
    elif working:
        summary = "Clear winners on-channel — clone hooks, pacing, and packaging on the next topic."
    else:
        summary = "Connect YouTube and harvest analytics before recommending a posting strategy."

    return {
        "stage": stage,
        "stage_label": stage_label,
        "subscribers": subs,
        "videos": videos,
        "channel_views": ch_views,
        "harvest_present": harvest_present,
        "summary": summary,
        "whats_working": working[:6],
        "whats_not_working": not_working[:6],
        "recommended_next_actions": next_actions[:8],
    }
