import sys
import types
import json

sys.modules.setdefault("stripe", types.SimpleNamespace())

from studio_agent import runner
from studio_agent.anti_hallucination import AuditReport, ToolFire, guard_text
from studio_agent.tools import _normalize_shortform_category_args


def test_current_text_zerotier_overrides_stale_empire_context():
    session = {"registry_key": ""}
    active = runner._active_registry_key(session, "WRONG channel, ZeroTier does comic books")
    assert active == "zerotier"


def test_current_text_mrskelewelly_can_select_channel():
    session = {"registry_key": ""}
    active = runner._active_registry_key(session, "pull all data from Mr. SkeleWelly")
    assert active == "mrskelewelly"


def test_mrskelewelly_channel_key_is_not_used_as_skeleton_category():
    args = {
        "category_key": "mrskelewelly",
        "topic": "The Real Reason You Overthink Everything",
        "video_model": "seedance",
        "render_style": "skeleton_host",
    }

    normalized = _normalize_shortform_category_args(args)

    assert normalized["category_key"] == "human_limits"
    assert normalized["_selected_channel_key"] == "mrskelewelly"
    assert args["category_key"] == "mrskelewelly"


def test_channel_guard_rewrites_wrong_registry_for_analytics():
    args = runner._channel_guard_tool_args(
        "get_channel_analytics",
        {"registry_key": "empire_magnates"},
        "zerotier",
    )
    assert args["registry_key"] == "zerotier"
    assert args["_corrected_registry_key"]["requested"] == "empire_magnates"


def test_channel_guard_does_not_touch_unscoped_tools():
    args = runner._channel_guard_tool_args(
        "list_render_styles",
        {"registry_key": "empire_magnates"},
        "zerotier",
    )
    assert args["registry_key"] == "empire_magnates"


def test_short_followup_after_analytics_promise_requires_preflight():
    messages = [
        {"role": "assistant", "content": "Let me pull the live channel data first before we plan anything."},
        {"role": "user", "content": "so?"},
    ]
    assert runner._is_channel_data_followup("so?")
    assert runner._recent_assistant_promised_channel_data(messages)


def test_stalled_channel_data_text_is_detected():
    text = "Still waiting on the analytics pull -- let me call that now directly."
    assert runner._assistant_stalled_on_channel_data(text)


def test_pull_all_data_requires_channel_preflight():
    assert runner._needs_channel_data_preflight(
        "go ahead and pull all of the data from Mr. SkeleWelly"
    )


def test_fake_channel_analytics_tool_text_is_detected():
    text = 'Let me pull that data right now.\n\nTool: get_channel_analytics\n\n{"registry_key": "mrskelewelly"}'
    assert runner._assistant_stalled_on_channel_data(text)


def test_retention_guard_does_not_dead_end_on_screenshot_export():
    report = AuditReport(
        blocked_claims=[
            "claimed or implied specific video-level AVD/retention while analytics tool lacked video-level retention rows"
        ]
    )
    text = guard_text("unsupported claim", report)
    assert "continue from the available selected-channel snapshot/public data" in text
    assert "YouTube Studio screenshot/export" not in text


def test_short_plan_fallback_uses_available_channel_data_without_inventing_winner():
    result = {
        "channel_title": "MrSkeleWelly",
        "analytics_data_quality": {
            "effective_source": "catalyst_harvest_snapshot",
            "oauth_connected": False,
            "video_rows_available": 1,
            "retention_rows_available": 0,
            "limitation": "YouTube Analytics OAuth did not return per-video retention rows.",
            "channel_resolution": {"matched_by": "exact_registry_key"},
        },
        "video_metrics": {"video_rows_available": 1, "retention_rows_available": 0},
    }
    text = runner._grounded_channel_plan_from_tools(
        [ToolFire("get_channel_analytics", {"registry_key": "mrskelewelly"}, json.dumps(result))],
        active_label="MrSkeleWelly",
        user_text="pull all data from Mr. SkeleWelly and plan a new short form video that doesn't flop",
    )
    assert "No usable row-level retention videos were returned" in text
    assert "cannot name an exact high-AVD winner" in text
    assert "Do not animate until approved scenes are selected." in text
    assert "one synced caption per word" in text
    assert "The Real Reason You Overthink Everything" not in text


def test_short_plan_fallback_uses_live_retention_rows_when_present():
    result = {
        "channel_title": "MrSkeleWelly",
        "analytics_data_quality": {
            "effective_source": "youtube_analytics_live",
            "oauth_connected": True,
            "video_rows_available": 3,
            "retention_rows_available": 3,
            "channel_resolution": {"matched_by": "exact_channel_id"},
        },
        "video_metrics": {"video_rows_available": 3, "retention_rows_available": 3},
    }
    text = runner._grounded_channel_plan_from_tools(
        [ToolFire("get_channel_analytics", {"registry_key": "mrskelewelly"}, json.dumps(result))],
        active_label="MrSkeleWelly",
        user_text="pull all data from Mr. SkeleWelly and plan a new short form video that doesn't flop",
    )
    assert "Source: youtube_analytics_live" in text
    assert "OAuth connected for private analytics: yes" in text
    assert "Retention rows available: 3" in text
    assert "Retention row count was reported, but no actual video-title/metric rows were returned" in text
    assert "The analytics tool reported retention rows, but the payload did not include actual video titles/metrics" in text
    assert "cannot rank a winner" in text
    assert "Retention rows with actual video titles/metrics are present" not in text
    assert "The Real Reason You Overthink Everything" not in text
    assert "screenshot/export" not in text


def test_short_plan_uses_actual_live_video_rows_when_returned():
    result = {
        "channel_title": "MrSkeleWelly",
        "analytics_data_quality": {
            "effective_source": "youtube_analytics_live",
            "oauth_connected": True,
            "video_rows_available": 3,
            "retention_rows_available": 3,
            "channel_resolution": {"matched_by": "exact_channel_id"},
        },
        "video_metrics": {
            "video_rows_available": 3,
            "retention_rows_available": 3,
            "top_shorts_by_retention": [
                {
                    "video_id": "skelly-1",
                    "title": "The Reason You Never stay Consistant",
                    "views": 306,
                    "average_view_percentage": 58.2,
                    "average_view_duration_sec": 31,
                    "duration_sec": 55,
                    "impression_click_through_rate": 2.9,
                    "is_short": True,
                },
                {
                    "video_id": "skelly-2",
                    "title": "The Lower One",
                    "views": 35,
                    "average_view_percentage": 21.0,
                    "average_view_duration_sec": 12,
                    "duration_sec": 55,
                    "is_short": True,
                },
            ],
            "top_by_views": [
                {
                    "video_id": "skelly-3",
                    "title": "Views Only",
                    "views": 400,
                    "average_view_percentage": 0.0,
                    "average_view_duration_sec": 0,
                    "duration_sec": 60,
                    "is_short": True,
                }
            ],
        },
    }
    text = runner._grounded_channel_plan_from_tools(
        [ToolFire("get_channel_analytics", {"registry_key": "mrskelewelly"}, json.dumps(result))],
        active_label="MrSkeleWelly",
        user_text="pull all data from Mr. SkeleWelly and plan a new short form video that doesn't flop",
    )
    assert "Actual selected-channel videos returned" in text
    assert "The Reason You Never stay Consistant" in text
    assert "58.20% avg view" in text
    assert "Best returned reference: The Reason You Never stay Consistant" in text
    assert "follow the strongest returned pattern" in text
    assert "The Real Reason You Overthink Everything" not in text


def test_public_search_request_requires_public_search_preflight():
    assert runner._needs_public_search_preflight(
        "why don't we figure out what people are actually looking for on YouTube Shorts and plan the next short"
    )


def test_fake_public_search_progress_is_detected():
    text = "Good. I now have four data pulls running simultaneously. Give me a moment to read the returned data."
    assert runner._assistant_stalled_on_channel_data(text)


if __name__ == "__main__":
    test_current_text_zerotier_overrides_stale_empire_context()
    test_current_text_mrskelewelly_can_select_channel()
    test_channel_guard_rewrites_wrong_registry_for_analytics()
    test_channel_guard_does_not_touch_unscoped_tools()
    test_short_followup_after_analytics_promise_requires_preflight()
    test_stalled_channel_data_text_is_detected()
    test_pull_all_data_requires_channel_preflight()
    test_fake_channel_analytics_tool_text_is_detected()
    test_retention_guard_does_not_dead_end_on_screenshot_export()
    test_short_plan_fallback_uses_available_channel_data_without_inventing_winner()
    test_short_plan_fallback_uses_live_retention_rows_when_present()
    test_short_plan_uses_actual_live_video_rows_when_returned()
    test_public_search_request_requires_public_search_preflight()
    test_fake_public_search_progress_is_detected()
    test_mrskelewelly_channel_key_is_not_used_as_skeleton_category()
    print("channel guard tests passed")
