from __future__ import annotations

import json

import pytest

import unified_credits


@pytest.fixture(autouse=True)
def _isolated_wallet(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    prior_wallets = dict(unified_credits._wallets)
    prior_loaded = unified_credits._loaded
    monkeypatch.setattr(unified_credits, "WALLETS_PATH", tmp_path / "wallets.json")
    monkeypatch.setattr(unified_credits, "LEDGER_PATH", tmp_path / "ledger.jsonl")
    monkeypatch.delenv("STUDIO_RUNPOD_WORKER_MODE", raising=False)
    unified_credits._wallets.clear()
    unified_credits._loaded = False
    yield
    unified_credits._wallets.clear()
    unified_credits._wallets.update(prior_wallets)
    unified_credits._loaded = prior_loaded


def test_settlement_charges_verified_overage_in_one_wallet_operation() -> None:
    unified_credits.add_credits("user-1", 100, reason="test")
    reservation = unified_credits.reserve_credits("user-1", 40, reason="render")

    result = unified_credits.settle_reservation(
        "user-1",
        reservation["reservation_id"],
        actual_credits=70,
        reason="runpod_actuals",
    )

    assert result["credits_charged"] == 70
    assert result["charged_from_hold"] == 40
    assert result["overage_credits"] == 30
    assert result["shortfall"] == 0
    assert result["balance"] == 30
    assert result["lifetime_spent"] == 70
    rows = [json.loads(line) for line in unified_credits.LEDGER_PATH.read_text().splitlines()]
    assert [row["type"] for row in rows].count("settle") == 1


def test_settlement_refunds_unused_hold_atomically() -> None:
    unified_credits.add_credits("user-1", 100, reason="test")
    reservation = unified_credits.reserve_credits("user-1", 40, reason="render")

    result = unified_credits.settle_reservation(
        "user-1",
        reservation["reservation_id"],
        actual_credits=20,
    )

    assert result["credits_charged"] == 20
    assert result["refunded_credits"] == 20
    assert result["overage_credits"] == 0
    assert result["balance"] == 80
    assert result["lifetime_spent"] == 20
