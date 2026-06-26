import sys
import types
import json

sys.modules.setdefault("stripe", types.SimpleNamespace())

from studio_agent import runner
from studio_agent.anti_hallucination import AuditReport, ToolFire, audit_turn, guard_text
from studio_agent.tone import sanitize_assistant_text
from studio_agent.tools import _normalize_shortform_category_args, _resolve_user_channel_connection, _video_metric_summary


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


def test_registry_channel_id_overrides_stale_session_channel_id():
    resolved = _resolve_user_channel_connection(
        "",
        "UCu_2fPA-ZmRZsfSeb5VIpPA",
        "zerotier",
    )
    assert resolved["lookup_channel_id"] == "UC9Gth_4MVet6rdPH7MHJf-g"
    assert resolved["registry_channel_id"] == "UC9Gth_4MVet6rdPH7MHJf-g"


def test_action_request_requires_real_tool_selection():
    assert runner._requires_tool_execution("Regenerate scene 3 and show me the new still.")
    assert runner._requires_tool_execution("Pull the ZeroTier channel analytics.")
    assert not runner._requires_tool_execution("What model are you using?")


def test_anti_hallucination_correction_is_not_rewritten_as_fake_progress():
    text = (
        "I should not narrate work without executing it. "
        "I need to run the matching Studio tool now or name the exact blocker."
    )
    cleaned = sanitize_assistant_text(text)
    assert "handling that now" not in cleaned.lower()
    assert "should not narrate work" in cleaned.lower()


def test_failed_render_cannot_be_described_as_resubmitting_now():
    failed = ToolFire(
        "start_shortform_generate",
        {"category_key": "mrskelewelly"},
        json.dumps({"error": '"mrskelewelly" is not a valid lane key'}),
    )

    report = audit_turn(
        assistant_text=(
            "I will resubmit now with education as the category. "
            "Resubmitting now — one moment."
        ),
        user_text="Let's get it started.",
        tool_fires=[failed],
    )

    assert report.has_blockers
    assert any("promised execution" in claim for claim in report.blocked_claims)


def test_call_and_submitting_now_without_tool_are_blocked():
    report = audit_turn(
        assistant_text=(
            "Let me call the render tool directly now with the correct category. "
            "Here is exactly what I am submitting. Submitting now."
        ),
        user_text="Let's get it started.",
        tool_fires=[],
    )

    assert report.has_blockers
    assert any("promised execution" in claim for claim in report.blocked_claims)


def test_successful_render_can_be_reported_without_future_promise_block():
    started = ToolFire(
        "start_shortform_generate",
        {"category_key": "human_limits"},
        json.dumps({"status": "awaiting_scene_review", "job_id": "short_123"}),
    )

    report = audit_turn(
        assistant_text="I started the production and it is awaiting scene review.",
        user_text="Let's get it started.",
        tool_fires=[started],
    )

    assert not report.has_blockers


def test_explicit_start_language_recovers_last_production():
    session = {
        "session_id": "sa_test_recovery",
        "last_production": {
            "tool": "analyze_reference_video",
            "arguments": {"url": "https://youtube.com/shorts/reference"},
        },
        "runs": [{
            "events": [{
                "event": "pending_actions",
                "data": {
                    "actions": [{
                        "tool": "start_shortform_generate",
                        "arguments": {
                            "category_key": "mrskelewelly",
                            "topic": "The real reason men build emotional walls",
                            "video_model": "seedance",
                            "render_style": "comic_book",
                        },
                    }],
                },
            }],
        }],
    }

    recovered = runner._recover_requested_production(
        session,
        "Let's get it started in here. Let's do it.",
    )

    assert recovered is not None
    name, args = recovered
    assert name == "start_shortform_generate"
    assert args["category_key"] == "mrskelewelly"


def test_non_action_chat_does_not_recover_or_repeat_production():
    session = {
        "session_id": "sa_test_no_recovery",
        "last_production": {
            "tool": "start_shortform_generate",
            "arguments": {"category_key": "human_limits", "topic": "Existing job"},
        },
    }

    assert runner._recover_requested_production(session, "What title did we choose?") is None


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


def test_current_posted_video_requires_latest_upload_channel_preflight():
    text = "I want you to look at the current video we posted on the channel and get all its data."
    assert runner._needs_channel_data_preflight(text)
    assert runner._needs_latest_upload_focus(text)


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


def test_grounded_channel_status_names_latest_upload_separately_from_best_retention():
    result = {
        "channel_title": "MrSkeleWelly",
        "analytics_data_quality": {
            "effective_source": "youtube_analytics_live",
            "oauth_connected": True,
            "video_rows_available": 2,
            "retention_rows_available": 2,
            "focus": "latest_upload",
            "channel_resolution": {"matched_by": "exact_channel_id"},
        },
        "latest_upload": {
            "video_id": "latest",
            "title": "The Real Reason Men Build Emotional Walls",
            "views": 565,
            "average_view_percentage": 88.2,
            "average_view_duration_sec": 49,
            "published_at": "2026-06-26T01:00:00Z",
            "duration_sec": 55,
            "is_short": True,
        },
        "video_metrics": {
            "video_rows_available": 2,
            "retention_rows_available": 2,
            "top_shorts_by_retention": [
                {
                    "video_id": "older",
                    "title": "The Reason You Never stay Consistant",
                    "views": 302,
                    "average_view_percentage": 51.31,
                    "average_view_duration_sec": 28,
                    "published_at": "2026-06-20T01:00:00Z",
                    "duration_sec": 55,
                    "is_short": True,
                }
            ],
        },
    }
    text = runner._grounded_channel_status_from_tools(
        [ToolFire("get_channel_analytics", {"registry_key": "mrskelewelly", "focus": "latest_upload"}, json.dumps(result))],
        active_label="MrSkeleWelly",
    )
    assert "Latest upload: The Real Reason Men Build Emotional Walls" in text
    assert "565 views" in text
    assert "88.20% avg view" in text
    assert "The Reason You Never stay Consistant" not in text


def test_video_metric_summary_tracks_latest_upload_by_publish_date():
    summary = _video_metric_summary({
        "uploaded_videos": [
            {
                "video_id": "old",
                "title": "Older Short",
                "published_at": "2026-06-20T00:00:00Z",
                "views": 302,
                "average_view_percentage": 51.31,
                "duration_sec": 55,
            },
            {
                "video_id": "new",
                "title": "The Real Reason Men Build Emotional Walls",
                "published_at": "2026-06-26T00:00:00Z",
                "views": 565,
                "average_view_percentage": 88.2,
                "duration_sec": 55,
            },
        ]
    })
    assert summary["latest_upload"]["title"] == "The Real Reason Men Build Emotional Walls"
    assert summary["latest_upload"]["average_view_percentage"] == 88.2


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
    test_failed_render_cannot_be_described_as_resubmitting_now()
    test_call_and_submitting_now_without_tool_are_blocked()
    test_successful_render_can_be_reported_without_future_promise_block()
    test_explicit_start_language_recovers_last_production()
    test_non_action_chat_does_not_recover_or_repeat_production()
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
