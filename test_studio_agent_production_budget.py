import json

from studio_agent import production_budget


def test_shortform_budget_passes_when_under_cap():
    estimate = production_budget.enforce_budget(
        "start_shortform_generate",
        {
            "render_style": "comic_book",
            "category_key": "outcast",
            "topic": "test",
            "video_model": "seedance",
            "animate": False,
            "max_budget_usd": 5.0,
        },
    )
    assert estimate is not None
    assert estimate.estimated_usd <= estimate.max_budget_usd


def test_shortform_budget_blocks_when_over_cap():
    try:
        production_budget.enforce_budget(
            "start_shortform_generate",
            {
                "render_style": "cinematic",
                "category_key": "outcast",
                "topic": "test",
                "video_model": "kling_pro",
                "animate": True,
                "max_budget_usd": 0.01,
            },
        )
    except production_budget.BudgetExceededError as exc:
        payload = json.loads(str(exc))
        assert payload["error"] == "budget_exceeded"
        assert payload["budget"]["estimated_usd"] > payload["budget"]["max_budget_usd"]
    else:
        raise AssertionError("BudgetExceededError was not raised")


def test_budget_metadata_is_attached_to_json_results():
    estimate = production_budget.estimate_tool_cost(
        "generate_longform_thumbnails",
        {"count": 2, "max_budget_usd": 1.0},
    )
    result = production_budget.with_budget_metadata('{"status":"started"}', estimate)
    payload = json.loads(result)
    assert payload["status"] == "started"
    assert payload["budget"]["tool"] == "generate_longform_thumbnails"
    assert payload["budget"]["estimated_usd"] <= payload["budget"]["max_budget_usd"]


def test_shortform_full_auto_budget_uses_duration_and_model_rate():
    estimate = production_budget.estimate_tool_cost(
        "start_shortform_generate",
        {
            "scene_count": 12,
            "duration_seconds": 72,
            "video_model": "kling_pro",
            "animate": True,
            "_full_auto": True,
            "script": "x" * 2400,
            "max_budget_usd": 25.0,
        },
    )

    breakdown = estimate.breakdown
    assert breakdown["video_seconds"] == 72
    assert breakdown["video_model"] == "kling_pro"
    assert breakdown["video_usd_per_second"] == production_budget._fallback("kling_v21_pro_per_second")
    assert breakdown["tts_chars"] == 2400
    assert breakdown["cushion_pct"] >= 0.25
    assert estimate.estimated_usd > breakdown["stills_usd"] + breakdown["video_usd"]


def test_animate_scene_budget_uses_selected_scene_count_and_duration():
    estimate = production_budget.estimate_tool_cost(
        "animate_production_scenes",
        {
            "scene_indices": [0, 2, 4],
            "scene_durations": [4.5, 6.0, 7.5],
            "video_model": "pixverse",
            "max_budget_usd": 3.0,
        },
    )

    assert estimate.breakdown["scene_count"] == 3
    assert estimate.breakdown["video_seconds"] == 18.0
    assert estimate.breakdown["video_model"] == "pixverse"
    assert estimate.estimated_usd == round(18.0 * production_budget._fallback("pixverse_v6_per_second"), 4)


if __name__ == "__main__":
    test_shortform_budget_passes_when_under_cap()
    test_shortform_budget_blocks_when_over_cap()
    test_budget_metadata_is_attached_to_json_results()
    test_shortform_full_auto_budget_uses_duration_and_model_rate()
    test_animate_scene_budget_uses_selected_scene_count_and_duration()
    print("production budget tests passed")
