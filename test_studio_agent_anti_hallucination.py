from studio_agent.anti_hallucination import ToolFire, audit_turn, five_layer_audit, guard_text


def test_blocks_channel_data_claim_without_tool():
    report = audit_turn(
        assistant_text="Based on your channel data, Mango Markets is crushing your baseline.",
        user_text="What should Empire Magnates make next?",
        tool_fires=[],
    )
    assert report.has_blockers
    assert "channel" in guard_text("bad", report).lower()


def test_allows_channel_data_claim_with_analytics_tool():
    report = audit_turn(
        assistant_text="Based on your channel data, the financial-crime uploads are the useful reference set.",
        user_text="What is working on Empire Magnates?",
        tool_fires=[ToolFire("get_channel_analytics", {"registry_key": "empire_magnates"}, "{}")],
    )
    assert not report.has_blockers


def test_blocks_printed_tool_call_without_matching_backend_tool():
    report = audit_turn(
        assistant_text=(
            "Tool: get_public_search_trends\n\n"
            '{"niche_query":"psychology behavior self improvement YouTube Shorts","platform":"youtube"}\n\n'
            "Good. I now have four data pulls running simultaneously."
        ),
        user_text="Pull live search trend data before recommending a topic.",
        tool_fires=[],
    )
    assert report.has_blockers
    assert "printed tool-call text" in " ".join(report.blocked_claims)


def test_blocks_fake_tool_progress_without_tool_fire():
    report = audit_turn(
        assistant_text=(
            "Good. I now have four data pulls running simultaneously. "
            "Give me a moment to read the returned data."
        ),
        user_text=(
            "Figure out what people are actually looking for on YouTube Shorts "
            "before recommending a topic."
        ),
        tool_fires=[],
    )
    assert report.has_blockers
    assert "claimed backend tool/search work was running" in " ".join(report.blocked_claims)


def test_allows_printed_tool_call_when_matching_backend_tool_exists():
    report = audit_turn(
        assistant_text=(
            "Tool: get_public_search_trends\n\n"
            '{"niche_query":"psychology behavior self improvement YouTube Shorts","platform":"youtube"}'
        ),
        user_text="Pull live search trend data before recommending a topic.",
        tool_fires=[
            ToolFire(
                "get_public_search_trends",
                {"niche_query": "psychology behavior self improvement YouTube Shorts", "platform": "youtube"},
                '{"ok":true,"results":[]}',
            )
        ],
    )
    assert not any("printed tool-call text" in claim for claim in report.blocked_claims)


def test_blocks_production_complete_without_completed_result():
    report = audit_turn(
        assistant_text="Re-edit complete. Download the MP4.",
        user_text="Fix the captions.",
        tool_fires=[ToolFire("re_edit_production", {"job_id": "abc"}, '{"status":"reedit_finalize_started"}')],
    )
    assert report.has_blockers
    assert "complete" in " ".join(report.blocked_claims)


def test_blocks_complete_status_without_deliverable():
    report = audit_turn(
        assistant_text="Re-edit complete. Download the MP4.",
        user_text="Fix the captions.",
        tool_fires=[ToolFire("re_edit_production", {"job_id": "abc"}, '{"status":"complete"}')],
    )
    assert report.has_blockers


def test_allows_production_complete_with_completed_deliverable():
    report = audit_turn(
        assistant_text="Re-edit complete. Download the MP4.",
        user_text="Fix the captions.",
        tool_fires=[
            ToolFire(
                "re_edit_production",
                {"job_id": "abc"},
                '{"status":"complete","video_path":"skeleton_ai/output/abc/styled_short.mp4","mp4_url":"/api/studio-agent/jobs/abc/media?kind=shortform"}',
            )
        ],
    )
    assert not report.has_blockers


def test_blocks_already_posted_empire_topic_as_next_video():
    report = audit_turn(
        assistant_text="Your next video should be The Trader Who Legally Stole $114 Million from Mango Markets.",
        user_text="Find my next Empire Magnates video.",
        tool_fires=[ToolFire("recommend_video_topics", {"registry_key": "empire_magnates"}, "{}")],
    )
    assert report.has_blockers
    assert "already-posted" in " ".join(report.blocked_claims)


def test_five_layer_names_router_failure():
    audit = five_layer_audit(
        assistant_text="I just pulled live data and the latest data shows this niche is moving.",
        user_text="Give me the latest signal.",
        tool_fires=[],
    )
    layer = audit.layer("router")
    assert layer is not None
    assert not layer.passed
    assert audit.report.has_blockers


def test_five_layer_aliases_still_work():
    audit = five_layer_audit(
        assistant_text="Your analytics show CTR and retention are strongest on financial crime.",
        user_text="What is current on my channel?",
        tool_fires=[],
    )
    layer = audit.layer("claim_type")
    assert layer is not None
    assert not layer.passed


def test_five_layer_names_engineer_failure():
    audit = five_layer_audit(
        assistant_text="The render is complete. Download the MP4.",
        user_text="Re-edit this.",
        tool_fires=[ToolFire("poll_render_job", {"job_id": "abc"}, '{"status":"running"}')],
    )
    layer = audit.layer("engineer")
    assert layer is not None
    assert not layer.passed


def test_five_layer_names_auditor_failure():
    audit = five_layer_audit(
        assistant_text="Start building the next Empire Magnates video on Wirecard.",
        user_text="Find a new video.",
        tool_fires=[ToolFire("recommend_video_topics", {"registry_key": "empire_magnates"}, "{}")],
    )
    layer = audit.layer("auditor")
    assert layer is not None
    assert not layer.passed


def test_five_layer_final_correction_triggers_on_blockers():
    audit = five_layer_audit(
        assistant_text="Re-edit complete. Download the MP4.",
        user_text="Fix it.",
        tool_fires=[],
    )
    layer = audit.layer("verifier")
    assert layer is not None
    assert not layer.passed
    assert "complete yet" in guard_text("bad", audit.report).lower()


def test_blocks_current_answer_without_current_tool():
    report = audit_turn(
        assistant_text="Empire Magnates currently has 10 subscribers and the latest signal is financial crime.",
        user_text="Pull the most recent data from Empire Magnates.",
        tool_fires=[],
    )
    assert report.has_blockers
    assert "current" in " ".join(report.blocked_claims).lower()


def test_allows_current_answer_after_channel_tool():
    report = audit_turn(
        assistant_text="Empire Magnates currently has 10 subscribers and the latest channel pull says financial crime is the reference set.",
        user_text="Pull the most recent data from Empire Magnates.",
        tool_fires=[ToolFire("get_channel_analytics", {"registry_key": "empire_magnates"}, '{"subscribers":10}')],
    )
    assert not report.has_blockers


def test_blocks_specific_retention_claim_when_video_rows_missing():
    report = audit_turn(
        assistant_text="You're right, there is definitely a video with 50-60% AVD on ZeroTier.",
        user_text="Find the ZeroTier short-form video with 50-60% AVD.",
        tool_fires=[
            ToolFire(
                "get_channel_analytics",
                {"registry_key": "zerotier"},
                '{"analytics_data_quality":{"video_level_retention_available":false,"limitation":"No per-video retention rows."}}',
            )
        ],
    )
    assert report.has_blockers
    assert "video-level" in " ".join(report.blocked_claims).lower()
    assert "youtube analytics oauth" in guard_text("bad", report).lower()


def test_allows_specific_retention_claim_when_video_rows_exist():
    report = audit_turn(
        assistant_text="ZeroTier has a specific high-retention short at 63.6% AVD in the returned top_shorts_by_retention rows.",
        user_text="Find the ZeroTier short-form video with 50-60% AVD.",
        tool_fires=[
            ToolFire(
                "get_channel_analytics",
                {"registry_key": "zerotier"},
                '{"analytics_data_quality":{"video_level_retention_available":true},"video_metrics":{"top_shorts_by_retention":[{"title":"Test","average_view_percentage":63.6}]}}',
            )
        ],
    )
    assert not report.has_blockers


def test_blocks_cannot_browse_youtube_when_public_search_exists():
    report = audit_turn(
        assistant_text="I can't browse YouTube directly. Share a Lume URL and I can analyze it.",
        user_text="Go on YouTube and look up Lume.",
        tool_fires=[],
    )
    assert report.has_blockers
    guarded = guard_text("bad", report).lower()
    assert "public youtube search" in guarded
    assert "quota" in guarded


def test_allows_public_youtube_search_result_before_reference_choice():
    report = audit_turn(
        assistant_text="I found public Lume reference candidates. Pick one, or I can analyze the top result.",
        user_text="Go on YouTube and look up Lume.",
        tool_fires=[
            ToolFire(
                "search_youtube_public",
                {"query": "Lume documentary YouTube"},
                '{"videos":[{"title":"Example","watch_url":"https://www.youtube.com/watch?v=abc"}]}',
            )
        ],
    )
    assert not report.has_blockers


def test_blocks_execution_promise_without_tool():
    report = audit_turn(
        assistant_text="Let me check the status and verify the render.",
        user_text="Try again.",
        tool_fires=[],
    )
    assert report.has_blockers
    assert "promised execution" in " ".join(report.blocked_claims)


def test_allows_execution_promise_with_tool():
    report = audit_turn(
        assistant_text="Let me check the status and verify the render.",
        user_text="Try again.",
        tool_fires=[ToolFire("poll_render_job", {"job_id": "abc"}, '{"status":"running"}')],
    )
    assert not report.has_blockers


def test_blocks_generated_count_mismatch():
    report = audit_turn(
        assistant_text="I generated three shorts for the channel.",
        user_text="Make three shorts.",
        tool_fires=[ToolFire("start_shortform_generate", {"prompt": "one"}, '{"status":"complete","video_path":"a.mp4"}')],
    )
    assert report.has_blockers
    assert "claimed generated/rendered production count" in " ".join(report.blocked_claims)


def test_blocks_placeholder_phrase():
    audit = five_layer_audit(
        assistant_text="The rest of the code remains the same.",
        user_text="Fix Studio Agent.",
        tool_fires=[],
    )
    layer = audit.layer("corrector")
    assert layer is not None
    assert not layer.passed
