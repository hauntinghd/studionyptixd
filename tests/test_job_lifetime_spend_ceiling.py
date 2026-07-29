"""Whole-job spend ceiling (STUDIO_MAX_JOB_USD).

The per-tool approved cap in credit_reservation.json is measured from a baseline
captured when that tool starts, so a repair loop can make N tool calls that each
stay inside their own cap and still spend N x cap across one job. That is how a
~$4 short became $18.71 in production. These tests pin the ceiling that closes
it, because the failure mode is somebody's real money.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from studio_agent import production_budget, production_costs


def _spend(workspace: Path, usd: float, *, stage: str = "stills") -> None:
    """Record real provider spend the way the pipeline does."""
    production_costs.record_event(
        workspace,
        stage=stage,
        provider="fal",
        operation="test_op",
        usd=usd,
        quantity=1,
        unit="image",
        endpoint="fal-ai/test",
        scene_index=0,
        metadata={"pricing_note": "test"},
    )


def _approve_tool_cap(workspace: Path, *, cap_usd: float) -> None:
    """Write the per-tool approved cap, baselined at current spend.

    This mirrors what a fresh tool call does: its cap only governs spend *after*
    the baseline, which is precisely why per-tool caps stack.
    """
    summary = production_costs.load_summary(workspace)
    baseline = float(summary.get("total_usd_decimal", summary.get("total_usd", 0.0)) or 0.0)
    (workspace / "credit_reservation.json").write_text(
        json.dumps({
            "tool": "regenerate_production_scene_still",
            "cost_baseline_usd": baseline,
            "budget": {"max_budget_usd": cap_usd},
        }),
        encoding="utf-8",
    )


def test_unset_ceiling_leaves_behaviour_unchanged(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("STUDIO_MAX_JOB_USD", raising=False)
    state = production_budget.enforce_incremental_spend(
        tmp_path, 5.0, operation="regenerate_production_scene_still"
    )
    assert state.get("enforced") is False
    assert "job_lifetime_cap_usd" not in state


def test_ceiling_blocks_a_dispatch_that_would_exceed_it(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("STUDIO_MAX_JOB_USD", "10")
    _spend(tmp_path, 9.60)
    with pytest.raises(production_budget.BudgetExceededError) as excinfo:
        production_budget.enforce_incremental_spend(
            tmp_path, 1.00, operation="regenerate_production_scene_still"
        )
    payload = json.loads(str(excinfo.value))
    assert payload["error"] == "budget_exceeded_job_lifetime"
    assert payload["budget"]["job_lifetime_cap_usd"] == 10.0
    assert payload["budget"]["job_lifetime_spent_usd"] == pytest.approx(9.60, abs=1e-6)


def test_ceiling_allows_a_dispatch_that_fits(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("STUDIO_MAX_JOB_USD", "10")
    _spend(tmp_path, 4.00)
    state = production_budget.enforce_incremental_spend(
        tmp_path, 0.50, operation="regenerate_production_scene_still"
    )
    assert state["job_lifetime_remaining_usd"] == pytest.approx(6.00, abs=1e-6)


def test_ceiling_counts_in_flight_spend(tmp_path, monkeypatch) -> None:
    """Concurrent dispatches must not each see the same headroom."""
    monkeypatch.setenv("STUDIO_MAX_JOB_USD", "10")
    _spend(tmp_path, 8.00)
    with pytest.raises(production_budget.BudgetExceededError):
        production_budget.enforce_incremental_spend(
            tmp_path,
            1.50,
            operation="regenerate_production_scene_still",
            in_flight_usd=1.00,
        )


def test_ceiling_holds_across_stacked_per_tool_caps(tmp_path, monkeypatch) -> None:
    """The regression this exists for.

    Each iteration re-approves a $5 per-tool cap baselined at current spend, so
    the per-tool check alone would happily permit unbounded total spend. The
    lifetime ceiling must stop the run once cumulative spend reaches it.
    """
    monkeypatch.setenv("STUDIO_MAX_JOB_USD", "10")
    dispatched = 0.0
    blocked = False
    for _ in range(40):
        _approve_tool_cap(tmp_path, cap_usd=5.0)
        try:
            production_budget.enforce_incremental_spend(
                tmp_path, 1.00, operation="regenerate_production_scene_still"
            )
        except production_budget.BudgetExceededError as exc:
            assert json.loads(str(exc))["error"] == "budget_exceeded_job_lifetime"
            blocked = True
            break
        _spend(tmp_path, 1.00)
        dispatched += 1.00

    assert blocked, "40 stacked $5 tool caps were never stopped by the lifetime ceiling"
    assert dispatched <= 10.0 + 1e-9
    summary = production_costs.load_summary(tmp_path)
    total = float(summary.get("total_usd_decimal", summary.get("total_usd", 0.0)) or 0.0)
    assert total <= 10.0 + 1e-9


def test_per_tool_cap_still_enforced_under_the_ceiling(tmp_path, monkeypatch) -> None:
    """The new ceiling must not weaken the existing per-tool cap."""
    monkeypatch.setenv("STUDIO_MAX_JOB_USD", "100")
    _approve_tool_cap(tmp_path, cap_usd=0.50)
    with pytest.raises(production_budget.BudgetExceededError) as excinfo:
        production_budget.enforce_incremental_spend(
            tmp_path, 2.00, operation="regenerate_production_scene_still"
        )
    assert json.loads(str(excinfo.value))["error"] == "budget_exceeded_mid_job"
