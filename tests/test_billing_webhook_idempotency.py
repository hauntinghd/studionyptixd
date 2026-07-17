from __future__ import annotations

import asyncio
import json
from pathlib import Path

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

    monkeypatch.setattr(backend, "_append_landing_notification", no_notification)
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
    real_set_plan = unified_credits.set_plan
    set_plan_calls = 0

    def flaky_set_plan(*args, **kwargs):
        nonlocal set_plan_calls
        set_plan_calls += 1
        if set_plan_calls == 1:
            raise RuntimeError("wallet temporarily unavailable")
        return real_set_plan(*args, **kwargs)

    monkeypatch.setattr(unified_credits, "set_plan", flaky_set_plan)
    with pytest.raises(RuntimeError):
        asyncio.run(backend._capture_paypal_subscription_order("subscription-retry"))
    assert not billing._paypal_orders["subscription-retry"].get("activated")
    first_record = next(iter(billing._paypal_subscriptions.values()))
    first_period = (first_record["period_start_unix"], first_record["period_end_unix"])

    result = asyncio.run(backend._capture_paypal_subscription_order("subscription-retry"))
    second_record = next(iter(billing._paypal_subscriptions.values()))
    assert result["activated"] is True
    assert (second_record["period_start_unix"], second_record["period_end_unix"]) == first_period
    assert set_plan_calls == 2
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


def test_stripe_subscription_checkout_grants_selected_plan_once(
    isolated_billing_state: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = "studio_pro_5k"
    event = {
        "id": "evt-stripe-subscription",
        "type": "checkout.session.completed",
        "data": {
            "object": {
                "id": "cs-subscription",
                "mode": "subscription",
                "subscription": "sub-studio-pro-5k",
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
    assert first["monthly_balance"] == int(backend.UNIFIED_PLANS[plan]["monthly_credits"])

    assert asyncio.run(backend._stripe_webhook(request))["action"] == "duplicate"
    duplicate = unified_credits.get_state("user-a")
    assert duplicate["monthly_balance"] == first["monthly_balance"]
    assert duplicate["balance"] == first["balance"]


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
