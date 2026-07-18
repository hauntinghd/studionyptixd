from __future__ import annotations

import json

import pytest

import unified_credits
from studio_agent import idempotent_mutations, production_budget, tools
from studio_agent.command_execution import FileExecutionLedger


def _priced_text_args(command_id: str = "outline-command-1") -> dict:
    return {
        "channel_key": "history_rewind",
        "topic": "A metered outline",
        "model": "claude-sonnet-4-6",
        "_billing_prompt_price_per_m": 3.0,
        "_billing_completion_price_per_m": 15.0,
        "_billing_input_chars": 4000,
        "_runpod_command_id": command_id,
    }


def test_unpriced_longform_text_fails_before_execution() -> None:
    with pytest.raises(production_budget.BudgetExceededError, match="model_pricing_unavailable"):
        production_budget.enforce_budget(
            "generate_longform_outline",
            {"model": "unpriced/model", "_billing_input_chars": 1000},
        )


def test_logged_outline_settles_actual_tokens_once_and_replays_without_spend(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        idempotent_mutations,
        "_LEDGER",
        FileExecutionLedger(tmp_path / "mutations"),
    )
    monkeypatch.setattr(tools, "_require_longform_entitlement", lambda _user_id: None)
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: False)

    executed: list[tuple[str, dict]] = []
    reserved: list[float] = []
    settled: list[dict] = []

    def execute(name, arguments, **_context):
        executed.append((name, dict(arguments)))
        return json.dumps(
            {
                "outline": {"title": "Metered", "chapters": []},
                "billing": {
                    "provider": "anthropic_direct",
                    "model": "claude-sonnet-4-6",
                    "prompt_tokens": 1000,
                    "completion_tokens": 500,
                    "provider_usd": 0.0105,
                    "usage_reported": True,
                },
            }
        )

    def reserve(_user_id, provider_usd, **_kwargs):
        reserved.append(float(provider_usd))
        return {"reservation_id": "res-outline", "credits": 25, "unlimited": False}

    def settle(_user_id, reservation_id, **kwargs):
        settled.append({"reservation_id": reservation_id, **dict(kwargs)})
        return {"credits_charged": 11, "balance": 89, "refunded_credits": 14}

    monkeypatch.setattr(tools, "execute_tool", execute)
    monkeypatch.setattr(unified_credits, "reserve_usd", reserve)
    monkeypatch.setattr(unified_credits, "usd_to_credits", lambda usd: 11 if float(usd) else 0)
    monkeypatch.setattr(unified_credits, "settle_reservation", settle)

    first = json.loads(
        tools.execute_tool_logged(
            "generate_longform_outline",
            _priced_text_args(),
            user_id="creator-1",
            content_format="long",
        )
    )
    replay = json.loads(
        tools.execute_tool_logged(
            "generate_longform_outline",
            _priced_text_args(),
            user_id="creator-1",
            content_format="long",
        )
    )

    assert len(executed) == 1
    assert len(reserved) == 1
    assert len(settled) == 1
    assert settled[0]["reservation_id"] == "res-outline"
    assert settled[0]["actual_credits"] == 11
    assert settled[0]["allow_negative"] is True
    assert first["billing"]["metering_mode"] == "provider_usage"
    assert first["credits"] == {
        "charged": 11,
        "balance_after": 89,
        "refunded_credits": 14,
        "metering_mode": "provider_usage",
    }
    assert replay["idempotent_replay"] is True
    assert replay["outline"]["title"] == "Metered"


def test_thumbnail_regeneration_is_budgeted_and_idempotent() -> None:
    assert "regenerate_longform_thumbnail" in production_budget.EXPENSIVE_TOOLS
    assert "regenerate_longform_thumbnail" in idempotent_mutations.LOCAL_IDEMPOTENT_TOOLS
    estimate = production_budget.estimate_tool_cost(
        "regenerate_longform_thumbnail",
        {"image_model_id": "seedream_edit"},
    )
    assert estimate.estimated_usd > 0
    assert estimate.max_budget_usd == 0.25
