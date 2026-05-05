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


# Channel key (long_form.prompts.channels) → Catalyst channel slug.
# Keep in sync with whatever names Casey's channels are stored under in
# Supabase `youtube_channel_connections`. The frontend uses the long-form
# channel keys; the Catalyst-Hub APIs use the YouTube handle / channel-id.
CHANNEL_KEY_TO_CATALYST_HANDLE: dict[str, str] = {
    "lacuna": "WeAreLacuna",
    "hidden_cortex": "HiddenCortex",
    "pb_live": "PBLies",                # Catalyst was set up under PB Lies
    "lofi_radio": "LoFiRadio",
    "empire_magnates": "EmpireMagnates",
    "history_rewind": "HistoryRewind",
}


def shape_catalyst_insights(catalyst_payload: dict | None) -> dict[str, Any]:
    """
    Trim a Catalyst Hub snapshot down to the long-form-relevant signals.

    Catalyst Hub returns a deeply-nested object full of analytics, profile
    metadata, and reference-video output. We pull only what helps long-form:
    top titles, breakout titles, and hook patterns.

    Empty/absent fields → empty arrays (frontend hides empty sections).
    """
    if not isinstance(catalyst_payload, dict):
        return {
            "top_titles": [],
            "breakout_titles": [],
            "hook_patterns": [],
            "thumbnail_signals": [],
        }

    profile = catalyst_payload.get("profile") or {}
    outliers = catalyst_payload.get("outliers") or {}
    reference = catalyst_payload.get("reference_video_analysis") or {}
    velocity = catalyst_payload.get("velocity") or {}

    top_titles = []
    for v in (outliers.get("top_videos") or [])[:10]:
        if not isinstance(v, dict):
            continue
        top_titles.append({
            "title": str(v.get("title", "")),
            "views": int(v.get("view_count", 0) or 0),
            "vps": float(v.get("views_per_subscriber", 0) or 0),
            "video_id": str(v.get("video_id", "")),
        })

    breakouts = []
    for v in (outliers.get("breakout_videos") or [])[:10]:
        if not isinstance(v, dict):
            continue
        breakouts.append({
            "title": str(v.get("title", "")),
            "views": int(v.get("view_count", 0) or 0),
            "lift_vs_baseline": float(v.get("lift_vs_baseline", 0) or 0),
            "video_id": str(v.get("video_id", "")),
        })

    hook_patterns = []
    decoded = profile.get("decoded_hook_formulas") or reference.get("hook_formulas") or []
    if isinstance(decoded, list):
        for h in decoded[:6]:
            if isinstance(h, str) and h.strip():
                hook_patterns.append(h.strip())
            elif isinstance(h, dict) and h.get("formula"):
                hook_patterns.append(str(h["formula"]))

    thumb_signals = []
    decoded_thumb = profile.get("thumbnail_signals") or reference.get("thumbnail_signals") or []
    if isinstance(decoded_thumb, list):
        for t in decoded_thumb[:6]:
            if isinstance(t, str) and t.strip():
                thumb_signals.append(t.strip())
            elif isinstance(t, dict) and t.get("description"):
                thumb_signals.append(str(t["description"]))

    return {
        "top_titles": top_titles,
        "breakout_titles": breakouts,
        "hook_patterns": hook_patterns,
        "thumbnail_signals": thumb_signals,
        "subscribers": int(profile.get("subscriber_count", 0) or 0),
        "median_vps": float(velocity.get("median_views_per_subscriber", 0) or 0),
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
