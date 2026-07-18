from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import backend
import billing
import paypal_webhook_store
import unified_credits


class DummyRequest:
    def __init__(self, payload: dict, *, stripe: bool = False) -> None:
        self._body = json.dumps(payload).encode("utf-8")
        self.headers = {"stripe-signature": "test-signature"} if stripe else {}

    async def body(self) -> bytes:
        return self._body


def test_stripe_topup_returns_to_the_web_billing_surface(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(backend, "_billing_site_url", lambda: "https://studio.nyptidindustries.com")

    assert backend._stripe_topup_return_url("success") == (
        "https://studio.nyptidindustries.com?page=billing&topup=success&provider=stripe"
    )
    assert backend._stripe_topup_return_url("cancelled") == (
        "https://studio.nyptidindustries.com?page=billing&topup=cancelled&provider=stripe"
    )
    with pytest.raises(ValueError, match="Unsupported Stripe top-up return status"):
        backend._stripe_topup_return_url("other")


@pytest.fixture()
def isolated_billing_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    wallet_snapshot = dict(billing._topup_wallets)
    event_snapshot = dict(billing._paypal_webhook_events)
    order_snapshot = dict(billing._paypal_orders)
    subscription_snapshot = dict(billing._paypal_subscriptions)
    unified_snapshot = dict(unified_credits._wallets)
    version_snapshot = dict(billing._billing_effect_versions)
    unified_loaded = unified_credits._loaded

    monkeypatch.setattr(billing, "TOPUP_WALLET_PATH", tmp_path / "topup_wallets.json")
    monkeypatch.setattr(billing, "PAYPAL_WEBHOOK_EVENTS_PATH", tmp_path / "webhook_events.json")
    monkeypatch.setattr(billing, "PAYPAL_ORDERS_PATH", tmp_path / "paypal_orders.json")
    monkeypatch.setattr(billing, "PAYPAL_SUBSCRIPTIONS_PATH", tmp_path / "paypal_subscriptions.json")
    monkeypatch.setattr(billing, "BILLING_EFFECT_VERSIONS_PATH", tmp_path / "billing_effect_versions.json")
    monkeypatch.setattr(billing, "USAGE_LEDGER_PATH", tmp_path / "usage.jsonl")
    monkeypatch.setattr(billing, "LANDING_NOTIFICATIONS_PATH", tmp_path / "landing.json")
    monkeypatch.setattr(unified_credits, "WALLETS_PATH", tmp_path / "unified_wallets.json")
    monkeypatch.setattr(unified_credits, "LEDGER_PATH", tmp_path / "unified_ledger.jsonl")

    billing._topup_wallets.clear()
    billing._paypal_webhook_events.clear()
    billing._paypal_orders.clear()
    billing._paypal_subscriptions.clear()
    billing._billing_effect_versions.clear()
    unified_credits._wallets.clear()
    unified_credits._loaded = True
    monkeypatch.setattr(paypal_webhook_store, "configured", lambda: False)

    async def no_notification(*_args, **_kwargs) -> None:
        return None

    async def no_profile_write(*_args, **_kwargs) -> None:
        return None

    async def no_profile_read(*_args, **_kwargs) -> dict:
        return {}

    monkeypatch.setattr(backend, "_append_landing_notification", no_notification)
    monkeypatch.setattr(backend, "_supabase_set_user_plan", no_profile_write)
    monkeypatch.setattr(backend, "_supabase_set_stripe_identity", no_profile_write)
    monkeypatch.setattr(backend, "_supabase_get_billing_profile", no_profile_read)
    yield tmp_path

    billing._topup_wallets.clear()
    billing._topup_wallets.update(wallet_snapshot)
    billing._paypal_webhook_events.clear()
    billing._paypal_webhook_events.update(event_snapshot)
    billing._paypal_orders.clear()
    billing._paypal_orders.update(order_snapshot)
    billing._paypal_subscriptions.clear()
    billing._paypal_subscriptions.update(subscription_snapshot)
    billing._billing_effect_versions.clear()
    billing._billing_effect_versions.update(version_snapshot)
    unified_credits._wallets.clear()
    unified_credits._wallets.update(unified_snapshot)
    unified_credits._loaded = unified_loaded


def test_unified_wallet_failed_snapshot_rolls_back_to_durable_state(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unified_credits.add_credits("user-a", 40, idempotency_key="first")
    durable_before = unified_credits.WALLETS_PATH.read_bytes()

    def fail_write(_path: Path, _payload: object) -> None:
        raise OSError("simulated replace failure")

    monkeypatch.setattr(unified_credits, "_atomic_write_json", fail_write)
    with pytest.raises(OSError):
        unified_credits.add_credits("user-a", 25, idempotency_key="second")

    assert unified_credits.WALLETS_PATH.read_bytes() == durable_before
    assert unified_credits.get_state("user-a")["topup_balance"] == 40
    assert "second" not in unified_credits._wallets["user-a"]["processed_events"]


def test_legacy_wallet_mutations_are_idempotent_and_atomic(isolated_billing_state: Path) -> None:
    first = asyncio.run(billing._credit_topup_wallet("user-a", 50, "pack", "session-a"))
    duplicate = asyncio.run(billing._credit_topup_wallet("user-a", 50, "pack", "session-a"))
    debit = asyncio.run(billing._debit_topup_wallet("user-a", 20, "refund", "order-a"))
    debit_retry = asyncio.run(billing._debit_topup_wallet("user-a", 20, "refund", "order-a"))

    assert first is True
    assert duplicate is False
    assert debit == 20
    assert debit_retry == 20
    assert billing._topup_wallets["user-a"]["animated_topup_credits"] == 30
    persisted = json.loads(billing.TOPUP_WALLET_PATH.read_text(encoding="utf-8"))
    assert persisted["user-a"]["animated_topup_credits"] == 30


def test_local_webhook_claim_supports_failure_retry_and_completion(isolated_billing_state: Path) -> None:
    first = asyncio.run(billing._claim_webhook_event("paypal", "evt-a", "PAYMENT.CAPTURE.COMPLETED"))
    busy = asyncio.run(billing._claim_webhook_event("paypal", "evt-a", "PAYMENT.CAPTURE.COMPLETED"))
    assert first["status"] == "claimed"
    assert busy["status"] == "busy"

    assert asyncio.run(
        billing._fail_webhook_event("paypal", "evt-a", first["claim_id"], error_code="test_failure")
    )
    retry = asyncio.run(billing._claim_webhook_event("paypal", "evt-a", "PAYMENT.CAPTURE.COMPLETED"))
    assert retry["status"] == "claimed"
    assert retry["attempts"] == 2
    assert asyncio.run(
        billing._complete_webhook_event("paypal", "evt-a", retry["claim_id"], action="credited")
    )
    duplicate = asyncio.run(billing._claim_webhook_event("paypal", "evt-a", "PAYMENT.CAPTURE.COMPLETED"))
    assert duplicate["status"] == "duplicate"
    persisted = json.loads(billing.PAYPAL_WEBHOOK_EVENTS_PATH.read_text(encoding="utf-8"))
    assert persisted["paypal:evt-a"]["status"] == "completed"


def test_concurrent_local_webhook_claim_has_exactly_one_owner(isolated_billing_state: Path) -> None:
    async def claim_many() -> list[dict]:
        return await asyncio.gather(*(
            billing._claim_webhook_event("stripe", "evt-concurrent", "checkout.session.completed")
            for _ in range(12)
        ))

    results = asyncio.run(claim_many())
    assert [row["status"] for row in results].count("claimed") == 1
    assert [row["status"] for row in results].count("busy") == 11


def test_ordered_billing_effect_rejects_older_and_persists_version(isolated_billing_state: Path) -> None:
    applied: list[str] = []

    async def newer() -> str:
        applied.append("newer")
        return "newer"

    async def older() -> str:
        applied.append("older")
        return "older"

    first = asyncio.run(billing._run_ordered_billing_effect(
        "stripe", ["subscription:sub-a", "customer:cus-a"], "evt-new", 200.0, newer,
    ))
    stale = asyncio.run(billing._run_ordered_billing_effect(
        "stripe", ["subscription:sub-a", "customer:cus-a"], "evt-old", 100.0, older,
    ))

    assert first["status"] == "applied"
    assert stale["status"] == "stale"
    assert applied == ["newer"]
    persisted = json.loads(billing.BILLING_EFFECT_VERSIONS_PATH.read_text(encoding="utf-8"))
    assert all(row["event_id"] == "evt-new" for row in persisted.values())


def test_unversioned_billing_events_are_serialized_but_never_discarded(isolated_billing_state: Path) -> None:
    applied: list[str] = []

    async def run(label: str) -> str:
        applied.append(label)
        return label

    asyncio.run(billing._run_ordered_billing_effect(
        "paypal", ["user:user-a"], "evt-no-time-a", 0.0, lambda: run("a"),
    ))
    asyncio.run(billing._run_ordered_billing_effect(
        "paypal", ["user:user-a"], "evt-no-time-b", 0.0, lambda: run("b"),
    ))
    assert applied == ["a", "b"]


def test_verified_stripe_created_prevents_stale_lifecycle_effect(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    applied: list[str] = []

    async def fake_apply(event: dict) -> dict:
        applied.append(str(event["id"]))
        return {"status": "ok", "action": str(event["type"])}

    monkeypatch.setattr(backend, "_stripe_apply_webhook_event_unordered", fake_apply)
    newer = {
        "id": "evt-new", "type": "customer.subscription.updated", "created": 200,
        "data": {"object": {"id": "sub-a", "customer": "cus-a", "status": "active"}},
    }
    older = {
        "id": "evt-old", "type": "customer.subscription.deleted", "created": 100,
        "data": {"object": {"id": "sub-a", "customer": "cus-a", "status": "canceled"}},
    }

    assert asyncio.run(backend._stripe_apply_webhook_event(newer))["action"] == "customer.subscription.updated"
    assert asyncio.run(backend._stripe_apply_webhook_event(older))["action"] == "stale_ignored"
    assert applied == ["evt-new"]


def test_verified_paypal_create_time_prevents_stale_user_plan_effect(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    billing._paypal_orders["order-new"] = {
        "kind": "subscription", "user_id": "user-a", "capture_id": "capture-new",
    }
    applied: list[str] = []

    async def fake_apply(event: dict) -> dict:
        applied.append(str(event["id"]))
        return {"event_type": event["event_type"], "action": "applied"}

    monkeypatch.setattr(backend, "_paypal_apply_webhook_event_unordered", fake_apply)
    newer = {
        "id": "paypal-new", "event_type": "PAYMENT.CAPTURE.COMPLETED",
        "create_time": "2026-07-15T12:00:00Z", "resource": {"id": "capture-new"},
    }
    older = {
        "id": "paypal-old", "event_type": "BILLING.SUBSCRIPTION.CANCELLED",
        "create_time": "2026-07-14T12:00:00Z",
        "resource": {"id": "sub-old", "custom_id": "user-a"},
    }

    assert asyncio.run(backend._paypal_apply_webhook_event(newer))["action"] == "applied"
    assert asyncio.run(backend._paypal_apply_webhook_event(older))["action"] == "stale_ignored"
    assert applied == ["paypal-new"]


def test_remote_store_requires_service_role_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(paypal_webhook_store, "_SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setattr(paypal_webhook_store, "_SUPABASE_SERVICE_KEY", "")
    assert paypal_webhook_store.configured() is False
    monkeypatch.setattr(paypal_webhook_store, "_SUPABASE_SERVICE_KEY", "service-role-test")
    assert paypal_webhook_store.configured() is True
    assert paypal_webhook_store._headers()["Authorization"] == "Bearer service-role-test"


def test_remote_claim_schema_has_unique_identity_and_service_only_rls() -> None:
    migration = (
        Path(__file__).resolve().parents[1]
        / "migrations"
        / "2026-04-18_paypal_webhook_events.sql"
    ).read_text(encoding="utf-8").lower()
    normalized = " ".join(migration.split())
    assert "event_id text primary key" in normalized
    assert "auth.role() = 'service_role'" in normalized


def test_remote_takeover_condition_includes_observed_status(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        def __init__(self, status_code: int, payload: object) -> None:
            self.status_code = status_code
            self._payload = payload
            self.content = json.dumps(payload).encode("utf-8")

        def json(self):
            return self._payload

    patch_params: dict = {}

    class FakeClient:
        def __init__(self, **_kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args) -> None:
            return None

        async def post(self, *_args, **_kwargs):
            return FakeResponse(201, [])  # unique-key conflict ignored

        async def get(self, *_args, **_kwargs):
            return FakeResponse(200, [{
                "event_id": "evt-stale",
                "payload_excerpt": {
                    "status": "processing",
                    "claim_id": "old-claim",
                    "claimed_at": 1.0,
                    "attempts": 1,
                    "first_seen_at": 1.0,
                },
            }])

        async def patch(self, *_args, **kwargs):
            patch_params.update(kwargs.get("params") or {})
            return FakeResponse(200, [{"event_id": "evt-stale"}])

    monkeypatch.setattr(paypal_webhook_store, "_SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setattr(paypal_webhook_store, "_SUPABASE_SERVICE_KEY", "service-role-test")
    monkeypatch.setattr(paypal_webhook_store.httpx, "AsyncClient", FakeClient)
    result = asyncio.run(paypal_webhook_store.claim_event("evt-stale", lease_seconds=1))

    assert result["status"] == "claimed"
    assert patch_params["payload_excerpt->>claim_id"] == "eq.old-claim"
    assert patch_params["payload_excerpt->>status"] == "eq.processing"


def test_webhook_completion_store_failure_is_not_acknowledged(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def verified(_headers: dict, _body: bytes) -> bool:
        return True

    async def apply(_event: dict) -> dict:
        nonlocal calls
        calls += 1
        return {"action": "applied"}

    real_write = billing._atomic_write_json

    def fail_completed_snapshot(path: Path, payload: object) -> None:
        if path == billing.PAYPAL_WEBHOOK_EVENTS_PATH and isinstance(payload, dict):
            if any(isinstance(row, dict) and row.get("status") == "completed" for row in payload.values()):
                raise OSError("completion store unavailable")
        real_write(path, payload)

    monkeypatch.setattr(backend, "_paypal_verify_webhook_signature", verified)
    monkeypatch.setattr(backend, "_paypal_apply_webhook_event", apply)
    monkeypatch.setattr(billing, "_atomic_write_json", fail_completed_snapshot)
    event = {"id": "paypal-completion-fails", "event_type": "PAYMENT.CAPTURE.COMPLETED", "resource": {}}

    with pytest.raises(HTTPException) as first:
        asyncio.run(backend._paypal_webhook(DummyRequest(event)))
    assert first.value.status_code == 503
    assert billing._paypal_webhook_events["paypal:paypal-completion-fails"]["status"] == "processing"

    with pytest.raises(HTTPException) as retry:
        asyncio.run(backend._paypal_webhook(DummyRequest(event)))
    assert retry.value.status_code == 503
    assert calls == 1


def test_remote_completion_failure_returns_503_and_never_acknowledges(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def verified(_headers: dict, _body: bytes) -> bool:
        return True

    async def apply(_event: dict) -> dict:
        nonlocal calls
        calls += 1
        return {"action": "applied"}

    async def claimed(*_args, **_kwargs) -> dict:
        return {"status": "claimed", "claim_id": "remote-claim"}

    async def completion_failed(*_args, **_kwargs) -> bool:
        return False

    async def failed(*_args, **_kwargs) -> bool:
        return True

    monkeypatch.setattr(backend, "_paypal_verify_webhook_signature", verified)
    monkeypatch.setattr(backend, "_paypal_apply_webhook_event", apply)
    monkeypatch.setattr(paypal_webhook_store, "configured", lambda: True)
    monkeypatch.setattr(paypal_webhook_store, "claim_event", claimed)
    monkeypatch.setattr(paypal_webhook_store, "complete_event", completion_failed)
    monkeypatch.setattr(paypal_webhook_store, "fail_event", failed)
    event = {"id": "paypal-remote-completion-fails", "event_type": "PAYMENT.CAPTURE.COMPLETED", "resource": {}}

    with pytest.raises(HTTPException) as caught:
        asyncio.run(backend._paypal_webhook(DummyRequest(event)))
    assert caught.value.status_code == 503
    assert calls == 1
    assert billing._paypal_webhook_events["paypal:paypal-remote-completion-fails"]["status"] == "failed"


def test_paypal_claim_failure_is_fail_closed_before_effect(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def verified(_headers: dict, _body: bytes) -> bool:
        return True

    async def apply(_event: dict) -> dict:
        nonlocal calls
        calls += 1
        return {"action": "should_not_run"}

    def fail_write(_path: Path, _payload: object) -> None:
        raise OSError("claim store unavailable")

    monkeypatch.setattr(backend, "_paypal_verify_webhook_signature", verified)
    monkeypatch.setattr(backend, "_paypal_apply_webhook_event", apply)
    monkeypatch.setattr(billing, "_atomic_write_json", fail_write)
    event = {"id": "paypal-fail-closed", "event_type": "PAYMENT.CAPTURE.COMPLETED", "resource": {}}

    with pytest.raises(HTTPException) as caught:
        asyncio.run(backend._paypal_webhook(DummyRequest(event)))
    assert caught.value.status_code == 503
    assert calls == 0


def test_paypal_failed_effect_retries_then_deduplicates(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def verified(_headers: dict, _body: bytes) -> bool:
        return True

    async def apply(_event: dict) -> dict:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("transient")
        return {"action": "applied"}

    monkeypatch.setattr(backend, "_paypal_verify_webhook_signature", verified)
    monkeypatch.setattr(backend, "_paypal_apply_webhook_event", apply)
    event = {"id": "paypal-retry", "event_type": "PAYMENT.CAPTURE.COMPLETED", "resource": {}}

    with pytest.raises(HTTPException) as first:
        asyncio.run(backend._paypal_webhook(DummyRequest(event)))
    assert first.value.status_code == 500
    assert asyncio.run(backend._paypal_webhook(DummyRequest(event)))["status"] == "ok"
    assert asyncio.run(backend._paypal_webhook(DummyRequest(event)))["action"] == "duplicate"
    assert calls == 2


def test_configured_remote_claim_failure_is_fail_closed(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def verified(_headers: dict, _body: bytes) -> bool:
        return True

    async def apply(_event: dict) -> dict:
        nonlocal calls
        calls += 1
        return {"action": "should_not_run"}

    async def unavailable_claim(*_args, **_kwargs) -> dict:
        return {"status": "unavailable", "claim_id": ""}

    monkeypatch.setattr(backend, "_paypal_verify_webhook_signature", verified)
    monkeypatch.setattr(backend, "_paypal_apply_webhook_event", apply)
    monkeypatch.setattr(paypal_webhook_store, "configured", lambda: True)
    monkeypatch.setattr(paypal_webhook_store, "claim_event", unavailable_claim)
    event = {"id": "paypal-remote-down", "event_type": "PAYMENT.CAPTURE.COMPLETED", "resource": {}}

    with pytest.raises(HTTPException) as caught:
        asyncio.run(backend._paypal_webhook(DummyRequest(event)))
    assert caught.value.status_code == 503
    assert calls == 0
    assert billing._paypal_webhook_events["paypal:paypal-remote-down"]["status"] == "failed"


def test_paypal_topup_retry_after_wallet_mirror_failure_does_not_double_grant(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    billing._paypal_orders["order-retry"] = {
        "kind": "topup",
        "user_id": "user-a",
        "email": "creator@example.com",
        "price_id": "legacy-small",
        "pack": "small",
        "credits": 30,
        "credited": False,
    }

    capture_calls = 0

    async def captured(_order_id: str) -> tuple[dict, str]:
        nonlocal capture_calls
        capture_calls += 1
        return {}, "capture-retry"

    monkeypatch.setattr(backend, "_capture_paypal_order_api", captured)
    real_add = unified_credits.add_credits
    add_calls = 0

    def flaky_add(*args, **kwargs):
        nonlocal add_calls
        add_calls += 1
        if add_calls == 1:
            raise RuntimeError("wallet temporarily unavailable")
        return real_add(*args, **kwargs)

    monkeypatch.setattr(unified_credits, "add_credits", flaky_add)
    with pytest.raises(RuntimeError):
        asyncio.run(backend._capture_paypal_topup_order("order-retry"))
    assert billing._topup_wallets["user-a"]["animated_topup_credits"] == 30
    assert not billing._paypal_orders["order-retry"].get("credited")

    result = asyncio.run(backend._capture_paypal_topup_order("order-retry"))
    assert result["credited"] is True
    assert asyncio.run(backend._capture_paypal_topup_order("order-retry"))["credited"] is True
    assert billing._topup_wallets["user-a"]["animated_topup_credits"] == 30
    assert unified_credits.get_state("user-a")["topup_balance"] == 30
    assert add_calls == 2
    assert capture_calls == 1


def test_concurrent_paypal_return_and_webhook_capture_once(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    billing._paypal_orders["order-concurrent"] = {
        "kind": "topup",
        "user_id": "user-a",
        "email": "creator@example.com",
        "price_id": "legacy-small",
        "pack": "small",
        "credits": 30,
        "credited": False,
    }
    capture_calls = 0

    async def captured(_order_id: str) -> tuple[dict, str]:
        nonlocal capture_calls
        capture_calls += 1
        await asyncio.sleep(0)
        return {}, "capture-concurrent"

    monkeypatch.setattr(backend, "_capture_paypal_order_api", captured)

    async def run_both() -> list[dict]:
        return await asyncio.gather(
            backend._capture_paypal_topup_order("order-concurrent"),
            backend._capture_paypal_topup_order("order-concurrent"),
        )

    results = asyncio.run(run_both())
    assert capture_calls == 1
    assert all(row["credited"] is True for row in results)
    assert billing._topup_wallets["user-a"]["animated_topup_credits"] == 30
    assert unified_credits.get_state("user-a")["topup_balance"] == 30


def test_paypal_subscription_retry_reuses_period_and_finishes_effects_once(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    billing._paypal_orders["subscription-retry"] = {
        "kind": "subscription",
        "user_id": "user-a",
        "email": "creator@example.com",
        "plan": "creator",
        "price_id": "creator-monthly",
        "price_usd": 29.0,
        "activated": False,
        "created_at": 1_700_000_000.0,
    }

    capture_calls = 0

    async def captured(_order_id: str) -> tuple[dict, str]:
        nonlocal capture_calls
        capture_calls += 1
        return {}, "subscription-capture"

    async def set_profile(_user_id: str, _plan: str) -> None:
        return None

    monkeypatch.setattr(backend, "_capture_paypal_order_api", captured)
    monkeypatch.setattr(backend, "_supabase_set_user_plan", set_profile)
    real_grant_cycle = unified_credits.grant_plan_cycle
    grant_calls = 0

    def flaky_grant_cycle(*args, **kwargs):
        nonlocal grant_calls
        grant_calls += 1
        if grant_calls == 1:
            raise RuntimeError("wallet temporarily unavailable")
        return real_grant_cycle(*args, **kwargs)

    monkeypatch.setattr(unified_credits, "grant_plan_cycle", flaky_grant_cycle)
    with pytest.raises(RuntimeError):
        asyncio.run(backend._capture_paypal_subscription_order("subscription-retry"))
    assert not billing._paypal_orders["subscription-retry"].get("activated")
    first_record = next(iter(billing._paypal_subscriptions.values()))
    first_period = (first_record["period_start_unix"], first_record["period_end_unix"])

    result = asyncio.run(backend._capture_paypal_subscription_order("subscription-retry"))
    second_record = next(iter(billing._paypal_subscriptions.values()))
    assert result["activated"] is True
    assert (second_record["period_start_unix"], second_record["period_end_unix"]) == first_period
    assert grant_calls == 2
    processed = billing._topup_wallets["user-a"]["processed_mutations"]
    assert "activation_reset:paypal:subscription-retry" in processed
    assert capture_calls == 1


def test_paypal_topup_reversal_debits_both_wallets_exactly_once(
    isolated_billing_state: Path,
) -> None:
    billing._paypal_orders["order-refund"] = {
        "kind": "topup",
        "user_id": "user-a",
        "credits": 30,
        "capture_id": "capture-refund",
        "credited": True,
    }
    asyncio.run(billing._credit_topup_wallet("user-a", 30, "paypal", "order-refund"))
    unified_credits.add_credits(
        "user-a",
        30,
        idempotency_key="paypal_order:order-refund",
    )

    assert asyncio.run(backend._paypal_revoke_topup_for_order("order-refund", "refund_first")) == 30
    assert asyncio.run(backend._paypal_revoke_topup_for_order("order-refund", "reversal_retry")) == 30
    assert billing._topup_wallets["user-a"]["animated_topup_credits"] == 0
    assert unified_credits.get_state("user-a")["topup_balance"] == 0
    assert billing._paypal_orders["order-refund"]["unified_credits_debited"] == 30


def test_stripe_subscription_checkout_syncs_plan_without_granting_credits(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = "studio_pro_5k"
    monkeypatch.setattr(backend, "STRIPE_SECRET_KEY", "")
    event = {
        "id": "evt-stripe-subscription",
        "type": "checkout.session.completed",
        "data": {
            "object": {
                "id": "cs-subscription",
                "mode": "subscription",
                "payment_status": "paid",
                "subscription": {
                    "id": "sub-studio-pro-5k",
                    "status": "active",
                    "metadata": {"user_id": "user-a", "plan": plan},
                },
                "client_reference_id": "user-a",
                "customer_email": "creator@example.com",
                "metadata": {"user_id": "user-a", "plan": plan},
            }
        },
    }
    monkeypatch.setattr(backend, "STRIPE_WEBHOOK_SECRET", "test-secret")
    monkeypatch.setattr(backend, "SUPABASE_URL", "")
    monkeypatch.setattr(backend.stripe_lib.Webhook, "construct_event", lambda *_args: event)
    request = DummyRequest({}, stripe=True)

    assert asyncio.run(backend._stripe_webhook(request))["status"] == "ok"
    first = unified_credits.get_state("user-a")
    assert first["plan"] == plan
    assert first["monthly_balance"] == 0

    assert asyncio.run(backend._stripe_webhook(request))["action"] == "duplicate"
    duplicate = unified_credits.get_state("user-a")
    assert duplicate["monthly_balance"] == first["monthly_balance"]
    assert duplicate["balance"] == first["balance"]


def test_stripe_dynamic_subscription_metadata_syncs_without_email_or_grant(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_updates: list[tuple[str, str]] = []

    async def set_profile(user_id: str, plan: str) -> None:
        profile_updates.append((user_id, plan))

    async def unexpected_email_lookup(_email: str) -> str:
        raise AssertionError("metadata-owned lifecycle events must not require an email lookup")

    monkeypatch.setattr(backend, "_supabase_set_user_plan", set_profile)
    monkeypatch.setattr(backend, "_supabase_find_user_id_by_email", unexpected_email_lookup)
    event = {
        "id": "evt-dynamic-subscription",
        "type": "customer.subscription.created",
        "data": {
            "object": {
                "id": "sub-dynamic",
                "status": "active",
                "customer": "cus-dynamic",
                "metadata": {"user_id": "user-a", "plan": "studio_pro_5k"},
                "items": {"data": [{"price": {"id": "price_dynamic_checkout"}}]},
            }
        },
    }

    result = asyncio.run(backend._stripe_apply_webhook_event_unordered(event))

    assert result["action"] == "customer.subscription.created"
    assert profile_updates == [("user-a", "studio_pro_5k")]
    state = unified_credits.get_state("user-a")
    assert state["plan"] == "studio_pro_5k"
    assert state["monthly_balance"] == 0


def test_stripe_lifecycle_plan_validates_metadata_and_uses_static_price_fallback() -> None:
    static_price = "price_1T4eTUBL8lRmwao2EK3JDOpy"

    assert backend._stripe_lifecycle_plan(
        {"plan": "studio_pro_5k"},
        static_price,
    ) == "creator"
    assert backend._stripe_lifecycle_plan(
        {"plan": "forged_unknown_plan"},
        static_price,
    ) == "creator"
    assert backend._stripe_lifecycle_plan(
        {"plan": "forged_unknown_plan"},
        "price_unmapped",
    ) == ""


def test_stripe_past_due_snapshot_is_not_paid_access(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        backend,
        "_paypal_subscription_snapshot_for_user",
        lambda _user: {"known": False, "billing_active": False},
    )
    monkeypatch.setattr(
        backend,
        "_stripe_subscription_snapshot",
        lambda _email: {"ok": True, "status": "past_due", "plan": "studio_pro_1k"},
    )

    snapshot = backend._paid_access_snapshot_for_user(
        {"id": "user-a", "email": "past-due@example.com", "plan": "studio_pro_1k"}
    )

    assert snapshot["billing_active"] is False
    assert snapshot["source"] == ""


def test_stripe_failed_invoice_then_past_due_stays_revoked_and_success_restores_once(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(backend, "STRIPE_SECRET_KEY", "")
    profile_updates: list[tuple[str, str]] = []
    set_plan_calls: list[tuple[str, str, bool]] = []
    current_event: dict[str, dict] = {}

    async def set_profile(user_id: str, plan: str) -> None:
        profile_updates.append((user_id, plan))

    real_set_plan = unified_credits.set_plan

    def tracked_set_plan(user_id: str, plan: str, *, grant_now: bool = True):
        set_plan_calls.append((user_id, plan, grant_now))
        return real_set_plan(user_id, plan, grant_now=grant_now)

    def invoice_event(event_id: str, event_type: str, created: int) -> dict:
        return {
            "id": event_id,
            "type": event_type,
            "created": created,
            "data": {
                "object": {
                    "id": f"in_{event_id}",
                    "customer": "cus-dynamic",
                    "subscription": {
                        "id": "sub-dynamic",
                        "status": "active" if event_type != "invoice.payment_failed" else "past_due",
                        "metadata": {"user_id": "user-a", "plan": "studio_pro_1k"},
                    },
                    "lines": {"data": [{
                        "price": {"id": "price_dynamic_checkout"},
                        "period": {"start": 1000, "end": 2000},
                    }]},
                }
            },
        }

    monkeypatch.setattr(backend, "STRIPE_WEBHOOK_SECRET", "test-secret")
    monkeypatch.setattr(backend, "_supabase_set_user_plan", set_profile)
    monkeypatch.setattr(unified_credits, "set_plan", tracked_set_plan)
    monkeypatch.setattr(
        backend.stripe_lib.Webhook,
        "construct_event",
        lambda *_args: current_event["value"],
    )
    request = DummyRequest({}, stripe=True)

    current_event["value"] = invoice_event("evt-invoice-failed", "invoice.payment_failed", 100)
    assert asyncio.run(backend._stripe_webhook(request))["status"] == "ok"
    assert unified_credits.get_state("user-a")["plan"] == ""

    current_event["value"] = {
        "id": "evt-sub-past-due",
        "type": "customer.subscription.updated",
        "created": 101,
        "data": {
            "object": {
                "id": "sub-dynamic",
                "status": "past_due",
                "customer": "cus-dynamic",
                "metadata": {"user_id": "user-a", "plan": "studio_pro_1k"},
                "items": {"data": [{"price": {"id": "price_dynamic_checkout"}}]},
            }
        },
    }
    assert asyncio.run(backend._stripe_webhook(request))["status"] == "ok"
    assert unified_credits.get_state("user-a")["plan"] == ""

    current_event["value"] = invoice_event("evt-invoice-succeeded", "invoice.payment_succeeded", 102)
    assert asyncio.run(backend._stripe_webhook(request))["status"] == "ok"
    assert unified_credits.get_state("user-a")["plan"] == "studio_pro_1k"
    assert asyncio.run(backend._stripe_webhook(request))["action"] == "duplicate"

    assert profile_updates == [
        ("user-a", "none"),
        ("user-a", "none"),
        ("user-a", "studio_pro_1k"),
    ]
    assert set_plan_calls == [
        ("user-a", "", False),
        ("user-a", "", False),
        ("user-a", "studio_pro_1k", False),
    ]


def test_stripe_invoice_retrieves_subscription_metadata_for_dynamic_price(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_updates: list[tuple[str, str]] = []
    retrieve_calls: list[str] = []

    async def set_profile(user_id: str, plan: str) -> None:
        profile_updates.append((user_id, plan))

    def retrieve(subscription_id: str) -> dict:
        retrieve_calls.append(subscription_id)
        return {
            "id": subscription_id,
            "status": "active",
            "customer": "cus-retrieve",
            "metadata": {"user_id": "user-a", "plan": "studio_pro_2500"},
            "items": {"data": [{"price": {"id": "price_dynamic_checkout"}}]},
        }

    monkeypatch.setattr(backend, "STRIPE_SECRET_KEY", "sk_test_lifecycle")
    monkeypatch.setattr(backend, "_supabase_set_user_plan", set_profile)
    monkeypatch.setattr(backend.stripe_lib.Subscription, "retrieve", retrieve)
    event = {
        "id": "evt-invoice-retrieve",
        "type": "invoice.payment_succeeded",
        "data": {
            "object": {
                "id": "in-retrieve",
                "subscription": "sub-retrieve",
                "customer": "cus-retrieve",
                "lines": {"data": [{
                    "price": {"id": "price_dynamic_checkout"},
                    "period": {"start": 1000, "end": 2000},
                }]},
            }
        },
    }

    result = asyncio.run(backend._stripe_apply_webhook_event_unordered(event))

    assert result["action"] == "invoice.payment_succeeded"
    assert retrieve_calls == ["sub-retrieve"]
    assert profile_updates == [("user-a", "studio_pro_2500")]
    assert unified_credits.get_state("user-a")["plan"] == "studio_pro_2500"


def test_stripe_unknown_successful_lifecycle_mapping_fails_closed(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = {
        "id": "evt-invoice-unknown-plan",
        "type": "invoice.payment_succeeded",
        "data": {
            "object": {
                "id": "in-unknown-plan",
                "metadata": {"user_id": "user-a", "plan": "forged_unknown_plan"},
                "lines": {"data": [{"price": {"id": "price_unmapped"}}]},
            }
        },
    }
    monkeypatch.setattr(backend, "STRIPE_WEBHOOK_SECRET", "test-secret")
    monkeypatch.setattr(backend.stripe_lib.Webhook, "construct_event", lambda *_args: event)

    with pytest.raises(HTTPException) as error:
        asyncio.run(backend._stripe_webhook(DummyRequest({}, stripe=True)))

    assert error.value.status_code == 500
    assert billing._paypal_webhook_events["stripe:evt-invoice-unknown-plan"]["status"] == "failed"


def test_stripe_subscription_snapshot_uses_validated_metadata_plan_for_dynamic_price(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    subscription = {
        "id": "sub-snapshot-dynamic",
        "created": 1_700_000_000,
        "status": "active",
        "metadata": {"plan": "studio_pro_11k"},
        "items": {
            "data": [
                {
                    "price": {
                        "id": "price_dynamic_checkout",
                        "recurring": {"interval": "month", "interval_count": 1},
                    }
                }
            ]
        },
    }
    monkeypatch.setattr(billing, "STRIPE_SECRET_KEY", "sk_test_snapshot")
    monkeypatch.setattr(billing, "_stripe_find_customer_id_by_email", lambda _email: "cus-snapshot")
    monkeypatch.setattr(
        billing.stripe_lib.Subscription,
        "list",
        lambda **_kwargs: {"data": [subscription]},
    )

    snapshot = billing._stripe_subscription_snapshot("creator@example.com")

    assert snapshot["ok"] is True
    assert snapshot["status"] == "active"
    assert snapshot["plan"] == "studio_pro_11k"


def test_stripe_paid_invoice_event_variants_grant_one_cycle_once(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(backend, "STRIPE_SECRET_KEY", "")

    def paid_event(event_id: str, event_type: str) -> dict:
        return {
            "id": event_id,
            "type": event_type,
            "created": 100,
            "data": {
                "object": {
                    "id": "in-shared-cycle",
                    "customer": "cus-a",
                    "subscription": {
                        "id": "sub-a",
                        "status": "active",
                        "metadata": {"user_id": "user-a", "plan": "studio_pro_1k"},
                    },
                    "lines": {"data": [{
                        "price": {"id": "price_dynamic_checkout"},
                        "period": {"start": 1000, "end": 2000},
                    }]},
                }
            },
        }

    asyncio.run(backend._stripe_apply_webhook_event(paid_event("evt-paid", "invoice.paid")))
    first = unified_credits.get_state("user-a")
    asyncio.run(backend._stripe_apply_webhook_event(
        paid_event("evt-payment-succeeded", "invoice.payment_succeeded")
    ))
    second = unified_credits.get_state("user-a")

    assert first["monthly_balance"] == int(backend.UNIFIED_PLANS["studio_pro_1k"]["monthly_credits"])
    assert second["balance"] == first["balance"]
    assert len(unified_credits._wallets["user-a"]["plan_grant_cycles"]) == 1


def test_stripe_unpaid_subscription_checkout_cannot_sync_or_grant(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def unexpected_sync(*_args, **_kwargs) -> None:
        raise AssertionError("unpaid checkout must not synchronize entitlement")

    monkeypatch.setattr(backend, "_stripe_sync_plan", unexpected_sync)
    event = {
        "id": "evt-unpaid-sub",
        "type": "checkout.session.completed",
        "data": {"object": {
            "id": "cs-unpaid-sub",
            "mode": "subscription",
            "payment_status": "unpaid",
            "client_reference_id": "user-a",
            "metadata": {"user_id": "user-a", "plan": "studio_pro_1k"},
        }},
    }

    result = asyncio.run(backend._stripe_apply_webhook_event_unordered(event))
    assert result["action"] == "subscription_payment_pending"
    assert unified_credits.get_state("user-a")["balance"] == 0


def test_stripe_unknown_active_snapshot_cannot_use_stored_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        backend,
        "_paypal_subscription_snapshot_for_user",
        lambda _user: {"known": False, "billing_active": False},
    )
    monkeypatch.setattr(
        backend,
        "_stripe_subscription_snapshot",
        lambda _email: {"ok": True, "status": "active", "plan": ""},
    )

    snapshot = backend._paid_access_snapshot_for_user(
        {"id": "user-a", "email": "unknown@example.com", "plan": "studio_pro_11k"}
    )
    assert snapshot["billing_active"] is False
    assert snapshot["plan"] == "none"


def test_stripe_unknown_subscription_status_revokes_fail_closed(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile_updates: list[tuple[str, str]] = []

    async def set_profile(user_id: str, plan: str) -> None:
        profile_updates.append((user_id, plan))

    monkeypatch.setattr(backend, "STRIPE_SECRET_KEY", "")
    monkeypatch.setattr(backend, "_supabase_set_user_plan", set_profile)
    monkeypatch.setattr(backend, "_stripe_active_subscription_candidate", lambda *_args, **_kwargs: {})
    unified_credits.set_plan("user-a", "studio_pro_1k")
    event = {
        "id": "evt-unknown-status",
        "type": "customer.subscription.updated",
        "data": {"object": {
            "id": "sub-a",
            "customer": "cus-a",
            "status": "paused_by_provider",
            "metadata": {"user_id": "user-a", "plan": "studio_pro_1k"},
        }},
    }

    asyncio.run(backend._stripe_apply_webhook_event_unordered(event))
    assert profile_updates[-1] == ("user-a", "none")
    assert unified_credits.get_state("user-a")["plan"] == ""


def test_stripe_snapshot_known_price_overrides_stale_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    subscription = {
        "id": "sub-static",
        "created": 1_700_000_000,
        "status": "active",
        "metadata": {"plan": "studio_pro_11k"},
        "items": {"data": [{"price": {
            "id": "price_1T4eTUBL8lRmwao2EK3JDOpy",
            "recurring": {"interval": "month", "interval_count": 1},
        }}]},
    }
    monkeypatch.setattr(billing, "STRIPE_SECRET_KEY", "sk_test_snapshot")
    monkeypatch.setattr(billing, "_stripe_find_customer_id_by_email", lambda _email: "cus-static")
    monkeypatch.setattr(billing.stripe_lib.Subscription, "list", lambda **_kwargs: {"data": [subscription]})

    assert billing._stripe_subscription_snapshot("creator@example.com")["plan"] == "creator"


def test_stripe_checkout_reuses_customer_and_blocks_duplicate_live_subscription(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def profile(_user_id: str) -> dict:
        return {"stripe_customer_id": "cus-persisted", "stripe_subscription_id": "sub-live"}

    monkeypatch.setattr(backend, "STRIPE_SECRET_KEY", "sk_test")
    monkeypatch.setattr(backend, "_supabase_get_billing_profile", profile)
    monkeypatch.setattr(
        backend,
        "_stripe_active_subscription_candidate",
        lambda *_args, **_kwargs: {
            "id": "sub-live", "plan": "studio_pro_1k", "status": "active", "customer_id": "cus-persisted",
        },
    )
    monkeypatch.setattr(
        backend.stripe_lib.billing_portal.Session,
        "create",
        lambda **_kwargs: SimpleNamespace(url="https://billing.example/portal"),
    )
    monkeypatch.setattr(
        backend.stripe_lib.checkout.Session,
        "create",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("duplicate checkout must not be created")),
    )

    with pytest.raises(HTTPException) as error:
        asyncio.run(backend._create_stripe_membership_checkout(
            {"id": "user-a", "email": "creator@example.com"}, "studio_pro_1k", 19.0,
        ))
    assert error.value.status_code == 409
    assert error.value.detail["portal_url"] == "https://billing.example/portal"


def test_stripe_checkout_uses_explicit_persisted_customer(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def profile(_user_id: str) -> dict:
        return {"stripe_customer_id": "cus-persisted", "stripe_subscription_id": ""}

    created_payload: dict = {}

    def create_session(**kwargs):
        created_payload.update(kwargs)
        return SimpleNamespace(url="https://checkout.example/session")

    monkeypatch.setattr(backend, "STRIPE_SECRET_KEY", "sk_test")
    monkeypatch.setattr(backend, "_supabase_get_billing_profile", profile)
    monkeypatch.setattr(backend, "_stripe_active_subscription_candidate", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(backend.stripe_lib.checkout.Session, "create", create_session)

    url = asyncio.run(backend._create_stripe_membership_checkout(
        {"id": "user-a", "email": "creator@example.com"}, "studio_pro_1k", 19.0,
    ))
    assert url == "https://checkout.example/session"
    assert created_payload["customer"] == "cus-persisted"
    assert "customer_email" not in created_payload


@pytest.mark.parametrize("event_type", ["customer.subscription.deleted", "invoice.payment_failed"])
def test_stripe_duplicate_subscription_failure_preserves_other_active_subscription(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
    event_type: str,
) -> None:
    monkeypatch.setattr(backend, "STRIPE_SECRET_KEY", "")
    profile_updates: list[tuple[str, str]] = []
    identity_updates: list[dict] = []

    async def set_profile(user_id: str, plan: str) -> None:
        profile_updates.append((user_id, plan))

    async def set_identity(user_id: str, **kwargs) -> None:
        identity_updates.append({"user_id": user_id, **kwargs})

    monkeypatch.setattr(backend, "_supabase_set_user_plan", set_profile)
    monkeypatch.setattr(backend, "_supabase_set_stripe_identity", set_identity)
    monkeypatch.setattr(
        backend,
        "_stripe_active_subscription_candidate",
        lambda *_args, **_kwargs: {
            "id": "sub-good", "plan": "studio_pro_5k", "status": "active", "customer_id": "cus-a",
        },
    )
    unified_credits.set_plan("user-a", "studio_pro_5k")
    if event_type.startswith("customer."):
        obj = {
            "id": "sub-bad", "customer": "cus-a", "status": "canceled",
            "metadata": {"user_id": "user-a", "plan": "studio_pro_1k"},
        }
    else:
        obj = {
            "id": "in-failed", "customer": "cus-a",
            "subscription": {
                "id": "sub-bad", "status": "past_due",
                "metadata": {"user_id": "user-a", "plan": "studio_pro_1k"},
            },
        }
    event = {"id": f"evt-{event_type}", "type": event_type, "data": {"object": obj}}

    asyncio.run(backend._stripe_apply_webhook_event_unordered(event))
    assert unified_credits.get_state("user-a")["plan"] == "studio_pro_5k"
    assert profile_updates[-1] == ("user-a", "studio_pro_5k")
    assert identity_updates[-1]["subscription_id"] == "sub-good"


def test_supabase_plan_write_raises_on_non_2xx(monkeypatch: pytest.MonkeyPatch) -> None:
    class FailedResponse:
        status_code = 500

    class FailedClient:
        def __init__(self, **_kwargs) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def post(self, *_args, **_kwargs):
            return FailedResponse()

    monkeypatch.setattr(billing, "SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setattr(billing, "SUPABASE_SERVICE_KEY", "service-role")
    monkeypatch.setattr(billing.httpx, "AsyncClient", FailedClient)

    with pytest.raises(RuntimeError, match="Supabase plan write failed"):
        asyncio.run(billing._supabase_set_user_plan("user-a", "studio_pro_1k"))


def test_stripe_retry_after_partial_effect_does_not_double_grant(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = {
        "id": "evt-stripe-retry",
        "type": "checkout.session.completed",
        "data": {
            "object": {
                "id": "cs-retry",
                "mode": "payment",
                "payment_status": "paid",
                "client_reference_id": "user-a",
                "customer_email": "creator@example.com",
                "metadata": {
                    "user_id": "user-a",
                    "topup_credits": "25",
                    "topup_pack": "small",
                    "topup_price_id": "legacy-small",
                },
            }
        },
    }
    monkeypatch.setattr(backend, "STRIPE_WEBHOOK_SECRET", "test-secret")
    monkeypatch.setattr(backend.stripe_lib.Webhook, "construct_event", lambda *_args: event)
    real_add = unified_credits.add_credits
    add_calls = 0

    def flaky_add(*args, **kwargs):
        nonlocal add_calls
        add_calls += 1
        if add_calls == 1:
            raise RuntimeError("wallet temporarily unavailable")
        return real_add(*args, **kwargs)

    monkeypatch.setattr(unified_credits, "add_credits", flaky_add)
    request = DummyRequest({}, stripe=True)

    with pytest.raises(HTTPException) as first:
        asyncio.run(backend._stripe_webhook(request))
    assert first.value.status_code == 500
    assert billing._topup_wallets["user-a"]["animated_topup_credits"] == 25

    assert asyncio.run(backend._stripe_webhook(request))["status"] == "ok"
    assert asyncio.run(backend._stripe_webhook(request))["action"] == "duplicate"
    assert billing._topup_wallets["user-a"]["animated_topup_credits"] == 25
    assert unified_credits.get_state("user-a")["topup_balance"] == 25
    assert add_calls == 2


def test_stripe_topup_waits_for_async_payment_and_grants_session_once(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = {
        "id": "cs-delayed-topup",
        "mode": "payment",
        "payment_status": "unpaid",
        "client_reference_id": "user-a",
        "customer_email": "creator@example.com",
        "metadata": {
            "user_id": "user-a",
            "topup_credits": "40",
            "topup_pack": "delayed",
            "topup_price_id": "legacy-delayed",
        },
    }
    current_event = {
        "value": {
            "id": "evt-delayed-completed",
            "type": "checkout.session.completed",
            "data": {"object": session},
        }
    }
    monkeypatch.setattr(backend, "STRIPE_WEBHOOK_SECRET", "test-secret")
    monkeypatch.setattr(
        backend.stripe_lib.Webhook,
        "construct_event",
        lambda *_args: current_event["value"],
    )
    request = DummyRequest({}, stripe=True)

    completed = asyncio.run(backend._stripe_webhook(request))
    assert completed["outcome"]["action"] == "topup_payment_pending"
    assert billing._topup_wallets.get("user-a") is None
    assert unified_credits.get_state("user-a")["topup_balance"] == 0

    paid_session = {**session, "payment_status": "paid"}
    current_event["value"] = {
        "id": "evt-delayed-succeeded",
        "type": "checkout.session.async_payment_succeeded",
        "data": {"object": paid_session},
    }
    succeeded = asyncio.run(backend._stripe_webhook(request))
    assert succeeded["outcome"]["action"] == "checkout.session.async_payment_succeeded"
    assert billing._topup_wallets["user-a"]["animated_topup_credits"] == 40
    assert unified_credits.get_state("user-a")["topup_balance"] == 40

    # A second delivered success event for the same Checkout Session must use
    # the existing session-scoped idempotency keys instead of granting twice.
    current_event["value"] = {
        "id": "evt-delayed-succeeded-redelivered",
        "type": "checkout.session.async_payment_succeeded",
        "data": {"object": paid_session},
    }
    assert asyncio.run(backend._stripe_webhook(request))["status"] == "ok"
    assert billing._topup_wallets["user-a"]["animated_topup_credits"] == 40
    assert unified_credits.get_state("user-a")["topup_balance"] == 40


def test_stripe_webhook_normalizes_stripe_object_event(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = {
        "id": "evt-stripe-object",
        "type": "checkout.session.completed",
        "data": {
            "object": {
                "id": "cs-stripe-object",
                "mode": "payment",
                "payment_status": "paid",
                "client_reference_id": "user-a",
                "metadata": {
                    "user_id": "user-a",
                    "topup_credits": "15",
                    "topup_pack": "object-pack",
                    "topup_price_id": "legacy-object-pack",
                },
            }
        },
    }

    class StripeObjectLike:
        def to_dict(self) -> dict:
            return event

    monkeypatch.setattr(backend, "STRIPE_WEBHOOK_SECRET", "test-secret")
    monkeypatch.setattr(
        backend.stripe_lib.Webhook,
        "construct_event",
        lambda *_args: StripeObjectLike(),
    )

    result = asyncio.run(backend._stripe_webhook(DummyRequest({}, stripe=True)))
    assert result["status"] == "ok"
    assert billing._topup_wallets["user-a"]["animated_topup_credits"] == 15
    assert unified_credits.get_state("user-a")["topup_balance"] == 15


def test_stripe_webhook_rejects_non_dict_normalized_event(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class MalformedStripeObject:
        def to_dict(self) -> list[dict]:
            return []

    monkeypatch.setattr(backend, "STRIPE_WEBHOOK_SECRET", "test-secret")
    monkeypatch.setattr(
        backend.stripe_lib.Webhook,
        "construct_event",
        lambda *_args: MalformedStripeObject(),
    )

    with pytest.raises(HTTPException) as error:
        asyncio.run(backend._stripe_webhook(DummyRequest({}, stripe=True)))
    assert error.value.status_code == 400
