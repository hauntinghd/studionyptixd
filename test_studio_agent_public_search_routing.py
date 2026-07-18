import os
import sys
import types
import asyncio
import json
from unittest.mock import patch

os.environ["REDIS_QUEUE_ENABLED"] = "0"
os.environ["REDIS_URL"] = ""
try:
    import stripe  # noqa: F401
except ModuleNotFoundError:
    sys.modules.setdefault("stripe", types.SimpleNamespace())

from studio_agent.runner import (
    ToolFire,
    _build_requested_topic_production,
    _fire_verification_step,
    _grounded_research_summary_from_tools,
    _is_channel_status_only_answer,
    _wants_production_execution,
    _needs_fresh_public_search,
    _needs_public_search_preflight,
)
from studio_agent import tools as agent_tools


def test_updated_data_followup_requires_public_search_preflight():
    text = "same thing again, but lets get more updated data since its been about 3 days now"

    assert _needs_public_search_preflight(text)
    assert _needs_fresh_public_search(text)


def test_plain_followup_without_data_does_not_force_public_search():
    assert not _needs_public_search_preflight("same thing again")


def test_verify_what_people_want_to_watch_requires_public_search():
    text = "id like you to verify all of your information regarding what people want to watch, we cannot take any chances"

    assert _needs_public_search_preflight(text)


def test_lets_do_chosen_topic_stays_in_planning_until_explicit_commit():
    text = "lets do The Real Reason Men Pull Away After getting close"

    assert not _wants_production_execution(text)


def test_lets_do_longform_topic_routes_to_longform_render():
    recovered = _build_requested_topic_production(
        {"render_style": "cinematic", "registry_key": "empire_magnates"},
        "lets do a long-form documentary about The Wirecard Collapse",
        content_format="long",
        active_registry="empire_magnates",
    )

    assert recovered is not None
    name, args = recovered
    assert name == "start_longform_render"
    assert args["channel_key"] == "empire_magnates"
    assert "Wirecard Collapse" in args["topic"]
    assert args["motion_policy"] == "balanced"


def test_verification_step_event_payload_is_structured():
    events = []

    async def emit(payload):
        events.append(payload)

    asyncio.run(_fire_verification_step(
        emit,
        "tool_evidence",
        "running",
        label="Run required data tools",
        detail="Pulling public search data.",
    ))

    assert events == [
        {
            "event": "verification_step",
            "step": "tool_evidence",
            "status": "running",
            "label": "Run required data tools",
            "detail": "Pulling public search data.",
            "required": True,
        }
    ]


def test_recommend_video_topics_returns_serialized_tool_result():
    def fake_run_async(coro):
        coro.close()
        return {
            "recommended_topics": [{"topic": "Why People Fall Under Your Influence"}],
            "trending_sample": [],
        }

    with (
        patch.object(agent_tools, "_run_async", side_effect=fake_run_async),
        patch.object(agent_tools.telemetry, "record_event"),
        patch.object(agent_tools.telemetry, "record_tool_call"),
    ):
        result = agent_tools.execute_tool_logged(
            "recommend_video_topics",
            {"niche_query": "psychology"},
            user_id="test-user",
            content_format="short",
            session_id="test-session",
        )

    assert isinstance(result, str)
    payload = json.loads(result)
    assert payload["recommended_topics"][0]["topic"] == "Why People Fall Under Your Influence"


def test_channel_status_only_answer_is_detected_for_public_demand_replacement():
    text = """I can see data for MrSkelewelly.

- Source: youtube_analytics_live
- OAuth connected for private analytics: yes
- Video rows available: 4
- Retention rows available: 4
"""

    assert _is_channel_status_only_answer(text)


def test_grounded_research_summary_combines_channel_and_public_evidence():
    channel = {
        "channel_title": "MrSkelewelly",
        "analytics_data_quality": {
            "effective_source": "youtube_analytics_live",
            "oauth_connected": True,
            "video_rows_available": 1,
            "retention_rows_available": 1,
        },
        "video_metrics": {
            "video_rows_available": 1,
            "retention_rows_available": 1,
            "top_shorts_by_retention": [
                {
                    "video_id": "vid-1",
                    "title": "The Real Reason Men Build Emotional Walls",
                    "views": 570,
                    "average_view_percentage": 86.94,
                    "average_view_duration_sec": 27,
                    "published_at": "2026-06-25T00:00:00Z",
                }
            ],
        },
    }
    public = {
        "fresh": True,
        "videos": [
            {
                "video_id": "pub-1",
                "title": "Why Men Go Silent After Getting Close",
                "channel_title": "Psychology Channel",
                "views": 1200000,
                "likes": 50000,
                "published_at": "2026-06-20T00:00:00Z",
                "support_label": "high public demand",
            }
        ],
        "recommended_topics": [
            {
                "topic": "Why Men Go Silent After Getting Close",
                "reason": "Matches selected-channel relationship psychology retention and public search demand.",
            }
        ],
    }
    text = _grounded_research_summary_from_tools(
        [
            ToolFire("get_channel_analytics", {"registry_key": "mrskelewelly"}, json.dumps(channel)),
            ToolFire("get_public_search_trends", {"query": "psychology shorts", "fresh": True}, json.dumps(public)),
        ],
        active_label="MrSkelewelly",
        user_text="verify what people want to watch",
    )

    assert "I verified public YouTube demand" in text
    assert "cross-checked selected-channel data" in text
    assert "The Real Reason Men Build Emotional Walls" in text
    assert "Why Men Go Silent After Getting Close" in text
    assert "Blocked: do not reuse old viral view-count claims" in text


if __name__ == "__main__":
    test_updated_data_followup_requires_public_search_preflight()
    test_plain_followup_without_data_does_not_force_public_search()
    print("studio agent public search routing tests passed")
