"""Catalyst performance learning: why content does and does not do well.

Studio already had a great deal of Catalyst machinery, but all of its learning
was about *how the render should look* - "avoid duplicate hands", "never use
split-screen". None of it was about what performs. The live channel memory on
the box was 2.4 KB of visual watchouts, last written 2026-07-06, and the
learning-records file it was supposed to write alongside did not exist at all.

So when the agent proposed a topic it was pattern-matching a demand cluster
with no idea what the channel had already published or how any of it did. It
said so itself, in the session that prompted this module: "Catalyst is
pattern-matching on the topic cluster without checking against what you've
already put out."

ClipLab solves this in one small file (``server/catalyst.py``) and the shape is
worth copying exactly:

* learn from the channel's **own** published numbers, never generic trend data
* compare **winners against losers** - what not to repeat is half the signal
* persist the lessons and feed them back, so picks compound
* fall back to deterministic rules when the model is unavailable, so the loop
  degrades instead of blocking
* carry a cold-start playbook for a channel with no history yet

The one thing deliberately not copied is ClipLab's YouTube-shaped record.
Catalyst is meant to learn from every platform Studio connects to, so posts are
normalised into a platform-neutral shape *first* and every rule below reads
only the normalised fields. Today YouTube is the only live adapter - it is the
only platform Studio holds real analytics for; TikTok, Instagram and X appear
in the codebase solely as URL sources for transcripts. Adding one of them later
is writing an adapter, not revisiting any of this.
"""
from __future__ import annotations

import json
import logging
import math
import re
from datetime import datetime, timezone
from typing import Any, Iterable

log = logging.getLogger("nyptid-studio.catalyst_performance")

PLATFORM_YOUTUBE = "youtube"
PLATFORM_TIKTOK = "tiktok"
PLATFORM_INSTAGRAM = "instagram"
PLATFORM_X = "x"

#: Every platform the normaliser understands. Only the ones Studio actually
#: holds analytics for produce rows; the rest are declared so that a future
#: integration has a defined landing place rather than a new schema.
SUPPORTED_PLATFORMS: tuple[str, ...] = (
    PLATFORM_YOUTUBE,
    PLATFORM_TIKTOK,
    PLATFORM_INSTAGRAM,
    PLATFORM_X,
)

LIVE_PLATFORMS: tuple[str, ...] = (PLATFORM_YOUTUBE,)

SHORT_MAX_SECONDS = 185.0
LONGFORM_MIN_SECONDS = 300.0

#: Per-platform source field names for each normalised metric, best first.
#: Watch-through is the metric that differs most: YouTube reports a percentage,
#: TikTok a completion rate, Instagram plays against reach. Normalising to a
#: 0..1 ratio is what lets one set of rules read all of them.
_FIELD_MAP: dict[str, dict[str, tuple[str, ...]]] = {
    PLATFORM_YOUTUBE: {
        "post_id": ("video_id", "id", "post_id"),
        "title": ("title", "name"),
        "published_at": ("published_at", "publishedAt", "published"),
        "duration_sec": ("duration", "duration_sec", "lengthSeconds"),
        "views": ("views", "viewCount", "view_count"),
        # "average_view_percentage" is the key Studio's own connected-channel
        # retention rows use; the camelCase form comes straight off the
        # Reporting API. Both have to land here or the strongest signal the
        # channel has is silently read as zero.
        "watch_ratio": (
            "avg_view_pct",
            "averageViewPercentage",
            "average_view_percentage",
            "watch_ratio",
        ),
        "likes": ("likes", "likeCount"),
        "comments": ("comments", "commentCount"),
        "shares": ("shares", "shareCount"),
        "follows": ("subs_gained", "subscribersGained", "follows"),
    },
    PLATFORM_TIKTOK: {
        "post_id": ("id", "item_id", "post_id"),
        "title": ("title", "caption", "desc"),
        "published_at": ("create_time", "published_at"),
        "duration_sec": ("duration", "video_duration"),
        "views": ("video_views", "play_count", "views"),
        "watch_ratio": ("completion_rate", "watch_ratio", "avg_watch_pct"),
        "likes": ("like_count", "likes", "digg_count"),
        "comments": ("comment_count", "comments"),
        "shares": ("share_count", "shares"),
        "follows": ("follows", "new_followers"),
    },
    PLATFORM_INSTAGRAM: {
        "post_id": ("id", "media_id", "post_id"),
        "title": ("caption", "title"),
        "published_at": ("timestamp", "published_at"),
        "duration_sec": ("video_duration", "duration"),
        "views": ("plays", "video_views", "impressions", "views"),
        "watch_ratio": ("watch_ratio", "avg_watch_pct"),
        "likes": ("like_count", "likes"),
        "comments": ("comments_count", "comments"),
        "shares": ("shares", "share_count"),
        "follows": ("follows", "follower_count"),
    },
    PLATFORM_X: {
        "post_id": ("id", "tweet_id", "post_id"),
        "title": ("text", "title"),
        "published_at": ("created_at", "published_at"),
        "duration_sec": ("duration", "video_duration"),
        "views": ("impression_count", "views"),
        "watch_ratio": ("watch_ratio", "video_completion_rate"),
        "likes": ("like_count", "likes"),
        "comments": ("reply_count", "comments"),
        "shares": ("retweet_count", "shares"),
        "follows": ("follows", "new_followers"),
    },
}


def _first(row: dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _number(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return default if math.isnan(out) or math.isinf(out) else out


def _ratio(value: Any) -> float:
    """Accept a percentage or a fraction and always return 0..1.

    Platforms disagree: YouTube's averageViewPercentage is 86.46, TikTok's
    completion rate is 0.32. Guessing wrong turns a strong video into a weak
    one and inverts every lesson drawn from it.
    """
    raw = _number(value, 0.0)
    if raw <= 0:
        return 0.0
    if raw > 1.0:
        raw = raw / 100.0
    return min(1.0, raw)


def normalize_post(platform: str, row: dict[str, Any]) -> dict[str, Any]:
    """One published post, in the shape every rule below reads."""
    key = str(platform or "").strip().lower()
    fields = _FIELD_MAP.get(key)
    if fields is None:
        raise ValueError(f"unsupported platform: {platform!r}")
    row = dict(row or {})
    duration = _number(_first(row, fields["duration_sec"]), 0.0)
    # Whether this platform reported engagement at all. Missing is not zero:
    # a source that does not expose likes would otherwise read as a channel
    # nobody likes, and fire a conversion alarm on a healthy account.
    has_engagement = any(
        _first(row, fields[metric]) is not None
        for metric in ("likes", "comments", "shares", "follows")
    )
    return {
        "has_engagement": has_engagement,
        "platform": key,
        "post_id": str(_first(row, fields["post_id"]) or "").strip(),
        "title": re.sub(r"\s+", " ", str(_first(row, fields["title"]) or "")).strip(),
        "published_at": str(_first(row, fields["published_at"]) or "").strip(),
        "duration_sec": duration,
        "is_short": bool(duration and duration <= SHORT_MAX_SECONDS),
        "views": _number(_first(row, fields["views"]), 0.0),
        "watch_ratio": _ratio(_first(row, fields["watch_ratio"])),
        "likes": _number(_first(row, fields["likes"]), 0.0),
        "comments": _number(_first(row, fields["comments"]), 0.0),
        "shares": _number(_first(row, fields["shares"]), 0.0),
        "follows": _number(_first(row, fields["follows"]), 0.0),
    }


def normalize_posts(platform: str, rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows or []:
        try:
            post = normalize_post(platform, row)
        except ValueError:
            raise
        except Exception:
            continue
        if post["title"] or post["post_id"]:
            out.append(post)
    return out


def _age_days(published_at: str) -> float:
    try:
        parsed = datetime.fromisoformat(str(published_at or "").replace("Z", "+00:00"))
    except ValueError:
        return 365.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0.5, (datetime.now(timezone.utc) - parsed).total_seconds() / 86400.0)


def performance_score(post: dict[str, Any]) -> float:
    """Rank posts against each other on this channel.

    Watch-through leads because it is the only metric that survives a small
    channel: 581 views at 86% watched is a better signal than 805 views at
    52%, and ranking on raw views alone would teach the opposite lesson.
    Views enter through a square root so a single outlier cannot dominate.
    """
    watch = _ratio(post.get("watch_ratio"))
    views = max(0.0, _number(post.get("views")))
    return round(watch * 200.0 + math.sqrt(views), 4)


def detect_channel_profile(posts: list[dict[str, Any]]) -> dict[str, Any]:
    """What kind of channel this is, from its own uploads."""
    shorts = [p for p in posts if p.get("duration_sec") and p["duration_sec"] <= SHORT_MAX_SECONDS]
    longs = [p for p in posts if p.get("duration_sec", 0.0) >= LONGFORM_MIN_SECONDS]
    total = len(shorts) + len(longs)
    if total == 0:
        return {
            "type": "unknown",
            "n_shorts": 0,
            "n_longform": 0,
            "short_share": 0.0,
            "confidence": "none",
            "label": "New or empty channel",
        }
    short_share = len(shorts) / total
    if short_share >= 0.7:
        ctype, label = "shorts_native", "Shorts-first channel"
    elif short_share <= 0.3:
        ctype, label = "longform_repurposer", "Long-form channel repurposed into shorts"
    else:
        ctype, label = "hybrid", "Mixed channel (long-form + shorts)"
    return {
        "type": ctype,
        "n_shorts": len(shorts),
        "n_longform": len(longs),
        "short_share": round(short_share, 3),
        "confidence": "high" if total >= 12 else "medium" if total >= 5 else "low",
        "label": label,
    }


#: Rates below which a channel is leaving conversion on the table. Deliberately
#: conservative - a directive that fires on a healthy channel trains the
#: planner to ignore directives.
WEAK_LIKE_RATE = 0.012
WEAK_COMMENT_RATE = 0.0015
WEAK_FOLLOW_RATE = 0.002
MIN_VIEWS_FOR_RATES = 200.0


def engagement_directives(posts: list[dict[str, Any]]) -> list[str]:
    """The high-retention / low-conversion trap, named deterministically.

    No model call: this is arithmetic, and it has to keep working when the
    model is down. Returns nothing when conversion is already healthy.
    """
    scored = [
        p
        for p in posts
        if _number(p.get("views")) >= MIN_VIEWS_FOR_RATES and p.get("has_engagement")
    ]
    total_views = sum(_number(p.get("views")) for p in scored)
    if not scored or total_views <= 0:
        return []
    like_rate = sum(_number(p.get("likes")) for p in scored) / total_views
    comment_rate = sum(_number(p.get("comments")) for p in scored) / total_views
    follow_rate = sum(_number(p.get("follows")) for p in scored) / total_views
    avg_watch = sum(_ratio(p.get("watch_ratio")) for p in scored) / len(scored)

    weak: list[str] = []
    if like_rate < WEAK_LIKE_RATE:
        weak.append(f"likes {like_rate * 100:.2f}%")
    if comment_rate < WEAK_COMMENT_RATE:
        weak.append(f"comments {comment_rate * 100:.2f}%")
    if follow_rate < WEAK_FOLLOW_RATE:
        weak.append(f"follows {follow_rate * 100:.2f}%")
    if not weak:
        return []

    directives = [
        f"CATALYST: retention is {avg_watch * 100:.0f}% but conversion is weak "
        f"({', '.join(weak)} per view across {int(total_views)} views). "
        "The bottleneck is conversion, not the hook."
    ]
    if comment_rate < WEAK_COMMENT_RATE:
        directives.append(
            "End on a debatable claim or an open question so viewers argue instead of swiping; "
            "a smooth self-contained video gets watched but not discussed."
        )
    if like_rate < WEAK_LIKE_RATE:
        directives.append(
            "Land an emotional peak or a payoff line at the end - that beat is what earns the like."
        )
    if follow_rate < WEAK_FOLLOW_RATE:
        directives.append(
            "Signal a repeatable promise so the viewer knows there is more like this here."
        )
    return directives


def growth_playbook(niche: str = "") -> list[str]:
    """Cold start. Real lessons supersede these the moment they exist."""
    rules = [
        "Open on the most arresting sentence; the first 1.5 seconds decides whether the viewer stays.",
        "Make it self-contained - most viewers arrive cold with no context.",
        "Give it a tension then payoff arc; a flat interesting statement is watched once and forgotten.",
        "End on a beat that provokes a reaction so viewers comment rather than swipe.",
        "Cut dead time. Density is retention on short-form.",
    ]
    low = str(niche or "").lower()
    if any(word in low for word in ("psychology", "relationship", "dating", "attachment")):
        rules.append(
            "Name a specific mechanism rather than restating the pain - explanation outperforms description."
        )
    return rules


def published_topic_index(posts: list[dict[str, Any]], *, limit: int = 25) -> list[dict[str, Any]]:
    """What this channel has already put out, best first.

    The exact thing the agent was missing when it proposed a topic.
    """
    ranked = sorted(posts, key=performance_score, reverse=True)
    out: list[dict[str, Any]] = []
    for post in ranked[:limit]:
        if not post.get("title"):
            continue
        out.append(
            {
                "title": post["title"],
                "views": int(_number(post.get("views"))),
                "watch_pct": round(_ratio(post.get("watch_ratio")) * 100, 2),
                "age_days": round(_age_days(post.get("published_at", "")), 1),
                "platform": post.get("platform", ""),
            }
        )
    return out


MIN_POSTS_FOR_LESSONS = 3


def _lesson_prompt(winners: list[dict[str, Any]], losers: list[dict[str, Any]]) -> str:
    def line(post: dict[str, Any]) -> str:
        return (
            f"  [{_ratio(post.get('watch_ratio')) * 100:.0f}% watched, "
            f"{int(_number(post.get('views')))} views, "
            f"+{int(_number(post.get('follows')))} follows] {post.get('title', '')[:140]}"
        )

    return (
        "You are Catalyst, a channel-growth learning engine. From this channel's own published "
        "performance, write concrete lessons the planner should follow when choosing and framing "
        "the next post. Compare the winners and losers and find the real pattern - hook style, "
        "topic, emotion, specificity, framing - that separates them.\n\n"
        "TOP PERFORMERS:\n" + "\n".join(line(p) for p in winners) + "\n\n"
        "UNDERPERFORMERS:\n" + "\n".join(line(p) for p in losers) + "\n\n"
        "Each lesson must be specific to what this data shows, not generic advice; one sentence; "
        "and actionable when choosing the next topic.\n"
        'Return JSON only: {"lessons": ["...", "..."]}'
    )


def _parse_lessons(raw: str, *, max_lessons: int) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    match = re.search(r"\{.*\}", text, re.S)
    if not match:
        return []
    try:
        payload = json.loads(match.group(0))
    except json.JSONDecodeError:
        return []
    lessons = payload.get("lessons") if isinstance(payload, dict) else None
    if not isinstance(lessons, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in lessons:
        lesson = re.sub(r"\s+", " ", str(item or "")).strip()[:240]
        key = lesson.lower()
        if lesson and key not in seen:
            seen.add(key)
            out.append(lesson)
        if len(out) >= max_lessons:
            break
    return out


def distill_lessons(
    posts: list[dict[str, Any]],
    *,
    max_lessons: int = 8,
    completion: Any = None,
) -> list[str]:
    """Durable channel lessons from winners versus losers.

    Returns an empty list rather than raising when the model is unavailable -
    the deterministic directives above still carry the turn.
    """
    scored = [p for p in posts if _number(p.get("views")) > 0 or _ratio(p.get("watch_ratio")) > 0]
    if len(scored) < MIN_POSTS_FOR_LESSONS:
        return []
    scored.sort(key=performance_score, reverse=True)
    half = max(1, min(5, len(scored) // 2))
    winners, losers = scored[:half], scored[-half:]

    if completion is None:
        try:
            from studio_agent.reference_providers import (
                anthropic_messages_completion,
                reference_analysis_model,
            )
        except Exception:
            return []

        def completion(prompt: str) -> dict[str, Any]:  # type: ignore[misc]
            return anthropic_messages_completion(
                prompt=prompt,
                model=reference_analysis_model(),
                max_tokens=1200,
                temperature=0.2,
            )

    try:
        result = completion(_lesson_prompt(winners, losers)) or {}
    except Exception as exc:
        log.warning("catalyst distill_lessons failed: %s", exc)
        return []
    return _parse_lessons(str(result.get("text") or ""), max_lessons=max_lessons)


def build_performance_brief(
    posts: list[dict[str, Any]],
    *,
    niche: str = "",
    lessons: list[str] | None = None,
    max_titles: int = 8,
) -> str:
    """The block the planner reads before proposing a topic.

    Advisory by design. It states what the channel already published and what
    its own numbers say, and lets the planner decide - the same way ClipLab
    feeds lessons into clip selection rather than vetoing picks.
    """
    if not posts:
        return "\n".join(
            ["CATALYST - no published performance yet, cold-start rules apply:"]
            + [f"- {rule}" for rule in growth_playbook(niche)]
        )

    profile = detect_channel_profile(posts)
    index = published_topic_index(posts, limit=max_titles)
    parts = [
        f"CATALYST - this channel's own published performance ({profile['label']}, "
        f"{profile['confidence']} confidence, {len(posts)} posts):",
        "Already published (do not repeat an angle without a clearly new mechanism):",
    ]
    parts += [
        f"- \"{row['title']}\" - {row['watch_pct']:.0f}% watched, {row['views']} views, {row['age_days']:.0f}d old"
        for row in index
    ]
    directives = engagement_directives(posts)
    if directives:
        parts.append("Conversion directives:")
        parts += [f"- {line}" for line in directives]
    if lessons:
        parts.append("Learned from this channel's winners vs losers:")
        parts += [f"- {lesson}" for lesson in lessons]
    return "\n".join(parts)
