from __future__ import annotations

import json

import pytest

import unified_credits
from studio_agent import idempotent_mutations, openrouter, production_budget, tools
from studio_agent.command_execution import FileExecutionLedger
from studio_agent.execution_context import production_command_scope


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

    with production_command_scope(
        "outline-command-1",
        user_id="creator-1",
        source="test",
    ):
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


def test_text_catalog_pricing_happens_only_after_idempotency_claim(
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
    order: list[str] = []
    real_begin = idempotent_mutations.begin

    def traced_begin(**kwargs):
        order.append("claim")
        return real_begin(**kwargs)

    async def pricing(_model):
        order.append("pricing_catalog")
        return 3.0, 15.0

    monkeypatch.setattr(idempotent_mutations, "begin", traced_begin)
    monkeypatch.setattr(openrouter, "model_pricing", pricing)
    monkeypatch.setattr(
        tools,
        "execute_tool",
        lambda *_args, **_kwargs: json.dumps(
            {
                "outline": {"title": "Claimed", "chapters": []},
                "billing": {
                    "provider": "anthropic_direct",
                    "model": "claude-sonnet-4-6",
                    "prompt_tokens": 1,
                    "completion_tokens": 1,
                    "provider_usd": 0.000018,
                    "usage_reported": True,
                },
            }
        ),
    )
    monkeypatch.setattr(
        unified_credits,
        "reserve_usd",
        lambda *_args, **_kwargs: {
            "reservation_id": "res-order",
            "credits": 1,
            "unlimited": False,
        },
    )
    monkeypatch.setattr(
        unified_credits,
        "settle_reservation",
        lambda *_args, **_kwargs: {
            "credits_charged": 1,
            "balance": 99,
            "refunded_credits": 0,
        },
    )
    monkeypatch.setattr(unified_credits, "usd_to_credits", lambda _usd: 1)
    arguments = _priced_text_args("outline-command-order")
    arguments.pop("_billing_prompt_price_per_m")
    arguments.pop("_billing_completion_price_per_m")

    with production_command_scope(
        "outline-command-order",
        user_id="creator-1",
        source="test",
    ):
        result = json.loads(
            tools.execute_tool_logged(
                "generate_longform_outline",
                arguments,
                user_id="creator-1",
                content_format="long",
            )
        )

    assert result["outline"]["title"] == "Claimed"
    assert order == ["claim", "pricing_catalog"]


def test_unpriced_catalog_result_fails_after_claim_but_before_spend_or_inference(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ledger = FileExecutionLedger(tmp_path / "mutations")
    monkeypatch.setattr(idempotent_mutations, "_LEDGER", ledger)
    monkeypatch.setattr(tools, "_require_longform_entitlement", lambda _user_id: None)
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: False)
    calls: list[str] = []

    async def pricing(_model):
        calls.append("pricing_catalog")
        return None, None

    monkeypatch.setattr(openrouter, "model_pricing", pricing)
    monkeypatch.setattr(
        tools,
        "execute_tool",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("unpriced command reached inference")
        ),
    )
    monkeypatch.setattr(
        unified_credits,
        "reserve_usd",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("unpriced command reached credit reservation")
        ),
    )
    arguments = _priced_text_args("outline-command-unpriced")
    arguments.pop("_billing_prompt_price_per_m")
    arguments.pop("_billing_completion_price_per_m")

    with production_command_scope(
        "outline-command-unpriced",
        user_id="creator-1",
        source="test",
    ):
        with pytest.raises(RuntimeError, match="pricing is unavailable"):
            tools.execute_tool_logged(
                "generate_longform_outline",
                arguments,
                user_id="creator-1",
                content_format="long",
            )

    assert calls == ["pricing_catalog"]


def test_thumbnail_regeneration_is_budgeted_and_idempotent() -> None:
    assert "regenerate_longform_thumbnail" in production_budget.EXPENSIVE_TOOLS
    assert "regenerate_longform_thumbnail" in idempotent_mutations.LOCAL_IDEMPOTENT_TOOLS
    estimate = production_budget.estimate_tool_cost(
        "regenerate_longform_thumbnail",
        {"image_model_id": "seedream_edit"},
    )
    assert estimate.estimated_usd > 0
    assert estimate.max_budget_usd == 0.25
