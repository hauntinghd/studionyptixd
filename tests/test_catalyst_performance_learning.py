"""Catalyst has to learn what performs, not only what renders cleanly.

The live channel memory was 2.4 KB of visual watchouts ("avoid duplicate
hands"), last written 2026-07-06, and the learning-records file beside it did
not exist. So the agent proposed topics with no idea what the channel had
already published - which it admitted in the session that prompted this
module: "Catalyst is pattern-matching on the topic cluster without checking
against what you've already put out."

The fixture below is the real MrSkeleWelly data from that session.
"""
from __future__ import annotations

import pytest

from studio_agent.catalyst_performance import (
    PLATFORM_TIKTOK,
    PLATFORM_YOUTUBE,
    build_performance_brief,
    detect_channel_profile,
    distill_lessons,
    engagement_directives,
    growth_playbook,
    normalize_post,
    normalize_posts,
    performance_score,
    published_topic_index,
)

# Real rows, as the YouTube analytics path reports them. Only views and
# retention were observed in that session, so no engagement counts appear here -
# inventing them would let a test assert a conclusion the data never supported.
MRSKELEWELLY = [
    {
        "video_id": "a1",
        "title": "The Real Reason Men Build Emotional Walls",
        "duration": 58,
        "views": 581,
        "avg_view_pct": 86.46,
        "published_at": "2026-07-10T12:00:00Z",
    },
    {
        "video_id": "a2",
        "title": "Why Men Self-Sabotage When They Fall in Love",
        "duration": 61,
        "views": 805,
        "avg_view_pct": 51.94,
        "published_at": "2026-07-04T12:00:00Z",
    },
    {
        "video_id": "a3",
        "title": "Why Men Suddenly Pull Away When You Show You Care",
        "duration": 55,
        "views": 318,
        "avg_view_pct": 55.0,
        "published_at": "2026-06-28T12:00:00Z",
    },
    {
        "video_id": "a4",
        "title": "Why Men Pull Away Right When Things Get Serious",
        "duration": 57,
        "views": 131,
        "avg_view_pct": 49.41,
        "published_at": "2026-06-20T12:00:00Z",
    },
]


def _posts():
    return normalize_posts(PLATFORM_YOUTUBE, MRSKELEWELLY)


# --- Platform neutrality ------------------------------------------------------

def test_a_youtube_percentage_and_a_tiktok_fraction_mean_the_same_thing() -> None:
    """The metric platforms disagree on most.

    YouTube reports averageViewPercentage as 86.46; TikTok reports a
    completion rate as 0.8646. Reading either literally would invert every
    lesson drawn from that channel.
    """
    yt = normalize_post(PLATFORM_YOUTUBE, {"id": "x", "title": "t", "avg_view_pct": 86.46})
    tt = normalize_post(PLATFORM_TIKTOK, {"id": "x", "title": "t", "completion_rate": 0.8646})
    assert yt["watch_ratio"] == pytest.approx(tt["watch_ratio"], abs=1e-4)
    assert 0.0 <= yt["watch_ratio"] <= 1.0


def test_every_declared_platform_normalizes() -> None:
    """A future integration must land in the existing shape, not a new one."""
    from studio_agent.catalyst_performance import SUPPORTED_PLATFORMS

    for platform in SUPPORTED_PLATFORMS:
        post = normalize_post(platform, {"id": "1", "title": "hello", "views": 10})
        assert post["platform"] == platform
        assert set(post) >= {"views", "watch_ratio", "likes", "comments", "shares", "follows"}


def test_an_unknown_platform_is_refused_not_guessed() -> None:
    with pytest.raises(ValueError):
        normalize_post("myspace", {"id": "1"})


# --- Ranking ------------------------------------------------------------------

def test_watch_through_outranks_raw_views() -> None:
    """581 views at 86% is the better video than 805 views at 52%.

    Ranking on views alone would teach this channel the opposite lesson and
    send it back to the angle that underperformed.
    """
    posts = _posts()
    best = max(posts, key=performance_score)
    assert best["title"] == "The Real Reason Men Build Emotional Walls"


def test_the_index_reports_what_was_already_published() -> None:
    index = published_topic_index(_posts())
    titles = [row["title"] for row in index]
    assert titles[0] == "The Real Reason Men Build Emotional Walls"
    assert len(titles) == 4
    assert index[0]["watch_pct"] == pytest.approx(86.46, abs=0.01)


def test_the_channel_is_recognised_as_shorts_first() -> None:
    profile = detect_channel_profile(_posts())
    assert profile["type"] == "shorts_native"
    assert profile["n_shorts"] == 4


# --- Deterministic directives -------------------------------------------------

WEAK_CONVERSION = [
    {
        "id": f"w{i}",
        "title": f"watched but forgettable {i}",
        "duration": 55,
        "views": 1200,
        "avg_view_pct": 68,
        "likes": 6,
        "comments": 0,
        "subs_gained": 1,
    }
    for i in range(4)
]


def test_weak_conversion_fires_directives_without_a_model() -> None:
    """This must keep working when the model is down."""
    directives = engagement_directives(normalize_posts(PLATFORM_YOUTUBE, WEAK_CONVERSION))
    assert directives
    joined = " ".join(directives).lower()
    assert "conversion" in joined
    assert any("comment" in d.lower() for d in directives)


def test_healthy_conversion_stays_quiet() -> None:
    """A directive that fires on a healthy channel trains the planner to ignore directives."""
    healthy = normalize_posts(
        PLATFORM_YOUTUBE,
        [
            {
                "id": f"h{i}",
                "title": f"strong {i}",
                "duration": 55,
                "views": 1000,
                "avg_view_pct": 70,
                "likes": 60,
                "comments": 12,
                "subs_gained": 9,
            }
            for i in range(4)
        ],
    )
    assert engagement_directives(healthy) == []


def test_a_channel_below_the_view_floor_is_not_judged() -> None:
    tiny = normalize_posts(
        PLATFORM_YOUTUBE, [{"id": "t1", "title": "new", "duration": 40, "views": 12}]
    )
    assert engagement_directives(tiny) == []


# --- Lessons ------------------------------------------------------------------

def test_lessons_need_enough_history() -> None:
    two = normalize_posts(PLATFORM_YOUTUBE, MRSKELEWELLY[:2])
    assert distill_lessons(two) == []


def test_lessons_parse_from_the_model() -> None:
    captured = {}

    def fake(prompt: str):
        captured["prompt"] = prompt
        return {"text": '{"lessons": ["Name the mechanism, do not restate the pain.", "Name the mechanism, do not restate the pain."]}'}

    lessons = distill_lessons(_posts(), completion=fake)
    assert lessons == ["Name the mechanism, do not restate the pain."]
    # Winners and losers must both reach the model, or there is no contrast to learn from.
    assert "TOP PERFORMERS" in captured["prompt"]
    assert "UNDERPERFORMERS" in captured["prompt"]
    assert "The Real Reason Men Build Emotional Walls" in captured["prompt"]
    assert "Why Men Pull Away Right When Things Get Serious" in captured["prompt"]


def test_a_model_outage_degrades_instead_of_raising() -> None:
    def broken(prompt: str):
        raise RuntimeError("anthropic 429")

    assert distill_lessons(_posts(), completion=broken) == []


def test_unparseable_output_is_dropped() -> None:
    assert distill_lessons(_posts(), completion=lambda p: {"text": "sorry, no"}) == []


# --- The brief the planner reads ---------------------------------------------

def test_the_brief_names_what_was_already_published() -> None:
    """The exact gap: the planner could not see the channel's own back catalogue."""
    brief = build_performance_brief(_posts(), niche="psychology")
    assert "The Real Reason Men Build Emotional Walls" in brief
    assert "do not repeat" in brief.lower()
    assert "86% watched" in brief


def test_the_brief_carries_learned_lessons_when_present() -> None:
    brief = build_performance_brief(_posts(), lessons=["Name the mechanism."])
    assert "Name the mechanism." in brief


def test_a_channel_with_no_history_gets_the_cold_start_playbook() -> None:
    brief = build_performance_brief([], niche="psychology")
    assert "cold-start" in brief.lower()
    assert any(rule[:20] in brief for rule in growth_playbook("psychology"))


def test_absent_engagement_data_is_not_reported_as_weak_conversion() -> None:
    """Missing is not zero.

    The MrSkeleWelly rows carry views and retention but no like/comment counts.
    Treating those as zeros would tell the planner the channel converts badly
    when nothing is known about its conversion at all.
    """
    posts = _posts()
    assert not any(p["has_engagement"] for p in posts)
    assert engagement_directives(posts) == []


# --- Wiring: the brief has to reach the planner --------------------------------

def test_retention_rows_normalize_from_studios_own_key() -> None:
    """Studio's connected-channel rows use average_view_percentage.

    If that key is not in the field map the strongest signal the channel has
    reads as zero and every lesson drawn from it is wrong.
    """
    post = normalize_post(
        PLATFORM_YOUTUBE,
        {"video_id": "v1", "title": "t", "average_view_percentage": 86.46, "view_count": 581},
    )
    assert post["watch_ratio"] == pytest.approx(0.8646, abs=1e-4)
    assert post["views"] == 581


def test_the_analytics_tool_records_the_performance_brief() -> None:
    from studio_agent.memory import _channel_performance_notes

    notes = _channel_performance_notes({"channel_video_rows": MRSKELEWELLY}, {})
    assert notes
    assert "The Real Reason Men Build Emotional Walls" in notes[0]


def test_the_brief_falls_back_to_top_titles_without_retention() -> None:
    from studio_agent.memory import _channel_performance_notes

    notes = _channel_performance_notes(
        {}, {"top_titles": [{"video_id": "a", "title": "A published short", "views": 400}]}
    )
    assert notes and "A published short" in notes[0]


def test_a_broken_learning_layer_never_breaks_the_analytics_tool() -> None:
    from studio_agent.memory import _channel_performance_notes

    assert _channel_performance_notes({"channel_video_rows": ["not a dict"]}, {}) == []
    assert _channel_performance_notes({}, {}) == []
