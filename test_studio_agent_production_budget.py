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


if __name__ == "__main__":
    test_shortform_budget_passes_when_under_cap()
    test_shortform_budget_blocks_when_over_cap()
    test_budget_metadata_is_attached_to_json_results()
    print("production budget tests passed")
