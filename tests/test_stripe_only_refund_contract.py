from __future__ import annotations

import asyncio
import inspect
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import backend
import billing
import backend_queue
import backend_settings
from backend_billing_handlers import build_create_checkout_handler
from backend_models import TopupCheckoutRequest, WaitlistJoinRequest


ROOT = Path(__file__).resolve().parents[1]


class _CheckoutRequest:
    product = "membership"
    plan = "creator"
    price_id = ""


def test_membership_checkout_has_one_stripe_provider() -> None:
    calls: list[tuple[str, float]] = []

    async def create_stripe(_user: dict, plan: str, price_usd: float) -> str:
        calls.append((plan, price_usd))
        return "https://checkout.stripe.test/session"

    handler = build_create_checkout_handler(
        checkout_request_model=_CheckoutRequest,
        require_auth=lambda: None,
        default_membership_plan_id=lambda: "creator",
        price_id_for_plan_id=lambda plan: f"price_{plan}",
        stripe_price_to_plan={"price_creator": "creator"},
        unified_plans={"creator": {"price_usd": 49}},
        plan_price_usd={},
        chat_story_allowed_plans={"creator"},
        stripe_secret_key="sk_test",
        create_stripe_membership_checkout=create_stripe,
    )

    result = asyncio.run(handler(_CheckoutRequest(), user={"id": "user-a"}))

    assert result == {
        "checkout_url": "https://checkout.stripe.test/session",
        "provider": "stripe",
    }
    assert calls == [("creator", 49.0)]


def test_membership_checkout_fails_closed_when_stripe_is_unavailable() -> None:
    async def unexpected(*_args, **_kwargs):
        raise AssertionError("Stripe checkout must not be called without a key")

    handler = build_create_checkout_handler(
        checkout_request_model=_CheckoutRequest,
        require_auth=lambda: None,
        default_membership_plan_id=lambda: "creator",
        price_id_for_plan_id=lambda plan: f"price_{plan}",
        stripe_price_to_plan={"price_creator": "creator"},
        unified_plans={"creator": {"price_usd": 49}},
        plan_price_usd={},
        chat_story_allowed_plans={"creator"},
        stripe_secret_key="",
        create_stripe_membership_checkout=unexpected,
    )

    with pytest.raises(HTTPException) as caught:
        asyncio.run(handler(_CheckoutRequest(), user={"id": "user-a"}))

    assert caught.value.status_code == 503
    assert "Stripe" in str(caught.value.detail)


def test_all_public_paypal_routes_are_gone_and_preserve_history() -> None:
    routes = []
    for mounted_route in backend.app.routes:
        original_router = getattr(mounted_route, "original_router", None)
        routes.extend(getattr(original_router, "routes", []) if original_router else [mounted_route])
    mounted = {
        (route.path, method): route.endpoint
        for route in routes
        for method in getattr(route, "methods", set())
    }
    assert mounted[("/api/paypal/return", "GET")] is backend._paypal_return_retired
    assert mounted[("/api/paypal/webhook", "POST")] is backend._paypal_webhook_retired
    assert mounted[("/api/paypal/verify/{order_id}", "GET")] is backend._paypal_verify_order_retired

    historical_key = "test-historical-paypal-order"
    previous = billing._paypal_orders.get(historical_key)
    billing._paypal_orders[historical_key] = {"status": "historical"}
    try:
        calls = (
            backend._paypal_return_retired(token=historical_key),
            backend._paypal_webhook_retired(request=object()),
            backend._paypal_verify_order_retired(historical_key),
        )
        for call in calls:
            with pytest.raises(HTTPException) as caught:
                asyncio.run(call)
            assert caught.value.status_code == 410
        assert billing._paypal_orders[historical_key] == {"status": "historical"}
    finally:
        if previous is None:
            billing._paypal_orders.pop(historical_key, None)
        else:
            billing._paypal_orders[historical_key] = previous


def test_paypal_cannot_be_selected_for_topups_or_waitlist() -> None:
    price_id = next(iter(backend.TOPUP_PACKS))
    with pytest.raises(HTTPException) as topup_error:
        asyncio.run(
            backend._create_topup_checkout(
                TopupCheckoutRequest(price_id=price_id, preferred_method="paypal"),
                user={"id": "user-a", "email": "creator@example.com"},
            )
        )
    assert topup_error.value.status_code == 410

    with pytest.raises(HTTPException) as waitlist_error:
        asyncio.run(
            backend._join_waitlist(
                WaitlistJoinRequest(
                    plan="starter",
                    provider="paypal",
                    email="creator@example.com",
                ),
                request=None,
            )
        )
    assert waitlist_error.value.status_code == 410


def test_topup_without_preferred_method_uses_stripe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    price_id = next(iter(backend.TOPUP_PACKS))
    monkeypatch.setattr(backend, "STRIPE_TOPUP_PUBLIC_ENABLED", True)
    monkeypatch.setattr(backend, "STRIPE_SECRET_KEY", "sk_test")
    monkeypatch.setattr(
        backend.stripe_lib.checkout.Session,
        "create",
        lambda **_kwargs: SimpleNamespace(url="https://checkout.stripe.test/topup"),
    )

    result = asyncio.run(
        backend._create_topup_checkout(
            TopupCheckoutRequest(price_id=price_id),
            user={"id": "user-a", "email": "creator@example.com"},
        )
    )

    assert result == {
        "checkout_url": "https://checkout.stripe.test/topup",
        "provider": "stripe",
    }


def test_historical_paypal_subscription_never_confers_paid_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        backend,
        "_paypal_subscription_snapshot_for_user",
        lambda _user: {
            "known": True,
            "billing_active": True,
            "plan": "studio_pro_1k",
            "provider": "paypal_manual",
        },
    )
    monkeypatch.setattr(
        backend,
        "_stripe_subscription_snapshot",
        lambda _email: {"ok": False, "status": "", "plan": ""},
    )
    monkeypatch.setattr(backend, "ADMIN_EMAILS", set())

    snapshot = backend._paid_access_snapshot_for_user(
        {"id": "user-a", "email": "creator@example.com", "plan": "studio_pro_1k"}
    )

    assert snapshot["manual_record_present"] is True
    assert snapshot["billing_active"] is False
    assert snapshot["plan"] == "none"
    assert snapshot["source"] == ""


def test_public_product_surface_contains_no_paypal_checkout_or_copy() -> None:
    public_roots = (
        ROOT / "ViralShorts-App" / "src",
        ROOT / "ViralShorts-App" / "public",
        ROOT / "ViralShorts-App" / "src-tauri" / "src",
    )
    checked: list[Path] = []
    for public_root in public_roots:
        for path in public_root.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in {".ts", ".tsx", ".html", ".rs"}:
                continue
            checked.append(path)
            text = path.read_text(encoding="utf-8").lower()
            assert "paypal" not in text, f"retired payment provider remains public in {path}"
    assert checked
    privacy = (
        ROOT / "ViralShorts-App" / "src" / "studio" / "pages" / "PrivacyPage.tsx"
    ).read_text(encoding="utf-8")
    terms = (
        ROOT / "ViralShorts-App" / "src" / "studio" / "pages" / "TermsPage.tsx"
    ).read_text(encoding="utf-8")
    for provider in ("Stripe", "Anthropic", "Contabo", "Fal.ai", "Vercel"):
        assert provider in privacy
        assert provider in terms


def test_retired_paypal_credentials_cannot_reactivate_from_environment() -> None:
    settings_source = (ROOT / "backend_settings.py").read_text(encoding="utf-8")
    refund_source = (ROOT / "backend_refunds.py").read_text(encoding="utf-8")

    assert 'os.getenv("PAYPAL_' not in settings_source
    assert backend_settings.PAYPAL_CLIENT_ID == ""
    assert backend_settings.PAYPAL_CLIENT_SECRET == ""
    assert backend_settings.PAYPAL_WEBHOOK_ID == ""
    assert "PayPal order / invoice id" not in refund_source
    assert "Stripe charge, payment-intent, or invoice id" in refund_source


@pytest.fixture()
def isolated_credit_wallet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    wallet_snapshot = dict(billing._topup_wallets)
    original_loaded = billing._topup_wallets.copy()
    monkeypatch.setattr(billing, "TOPUP_WALLET_PATH", tmp_path / "wallets.json")
    monkeypatch.setattr(billing, "USAGE_LEDGER_PATH", tmp_path / "usage.jsonl")
    billing._topup_wallets.clear()
    yield tmp_path
    billing._topup_wallets.clear()
    billing._topup_wallets.update(wallet_snapshot or original_loaded)


def test_generation_refund_is_durable_and_idempotent_across_retry(
    isolated_credit_wallet: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    billing._topup_wallets["user-a"] = {
        "topup_credits": 7,
        "animated_topup_credits": 7,
        "monthly_usage": {},
        "monthly_usage_non_animated": {},
        "processed_mutations": {},
    }
    billing._save_topup_wallets()
    real_atomic_write = billing._atomic_write_json

    def fail_atomic_write(_path: Path, _payload: object) -> None:
        raise OSError("disk unavailable")

    monkeypatch.setattr(billing, "_atomic_write_json", fail_atomic_write)
    with pytest.raises(OSError):
        asyncio.run(
            billing._refund_generation_credit(
                "user-a",
                "topup",
                credits=5,
                idempotency_key="generation_refund:job-a",
            )
        )

    failed_wallet = billing._topup_wallets["user-a"]
    assert failed_wallet["animated_topup_credits"] == 7
    assert "generation_refund:job-a" not in failed_wallet["processed_mutations"]

    monkeypatch.setattr(billing, "_atomic_write_json", real_atomic_write)
    assert asyncio.run(
        billing._refund_generation_credit(
            "user-a",
            "topup",
            credits=5,
            idempotency_key="generation_refund:job-a",
        )
    ) is True
    assert asyncio.run(
        billing._refund_generation_credit(
            "user-a",
            "topup",
            credits=5,
            idempotency_key="generation_refund:job-a",
        )
    ) is False

    assert billing._topup_wallets["user-a"]["animated_topup_credits"] == 12
    persisted = json.loads(billing.TOPUP_WALLET_PATH.read_text(encoding="utf-8"))
    assert persisted["user-a"]["animated_topup_credits"] == 12
    assert "generation_refund:job-a" in persisted["user-a"]["processed_mutations"]


def test_terminal_job_never_marks_refunded_before_durable_restore(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = "refund_contract_job"
    previous = backend.jobs.get(job_id)
    backend.jobs[job_id] = {
        "status": "error",
        "credit_charged": True,
        "credit_refunded": False,
        "credit_source": "topup",
        "credit_month_key": "2026-07",
        "credit_amount": 3,
        "user_id": "user-a",
        "diagnostics": {
            "current_stage": "error",
            "stage_started_at": 1,
            "stage_durations_sec": {},
        },
    }
    calls: list[dict] = []

    async def failed_refund(*_args, **kwargs):
        calls.append(dict(kwargs))
        raise OSError("wallet snapshot unavailable")

    monkeypatch.setattr(backend, "_refund_generation_credit", failed_refund)
    monkeypatch.setattr(backend, "schedule_persist_job_state", lambda *_args: None)
    monkeypatch.setattr(backend, "_append_usage_ledger", lambda *_args: None)
    monkeypatch.setattr(backend, "_record_kpi_for_job", lambda *_args: None)
    try:
        asyncio.run(backend._job_diag_finalize(job_id))
        assert backend.jobs[job_id]["credit_refunded"] is False
        assert backend.jobs[job_id]["credit_refund_pending"] is True
        assert calls == [{"month_key": "2026-07", "credits": 3, "idempotency_key": f"generation_refund:{job_id}"}]

        async def restored_refund(*_args, **_kwargs):
            return True

        monkeypatch.setattr(backend, "_refund_generation_credit", restored_refund)
        asyncio.run(backend._job_diag_finalize(job_id))
        assert backend.jobs[job_id]["credit_refunded"] is True
        assert backend.jobs[job_id]["credit_refund_pending"] is False
    finally:
        if previous is None:
            backend.jobs.pop(job_id, None)
        else:
            backend.jobs[job_id] = previous


def test_terminal_refund_does_not_depend_on_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = "refund_contract_without_diagnostics"
    previous = backend.jobs.get(job_id)
    backend.jobs[job_id] = {
        "status": "error",
        "credit_charged": True,
        "credit_refunded": False,
        "credit_source": "monthly",
        "credit_month_key": "2026-07",
        "credit_amount": 2,
        "user_id": "user-a",
    }
    calls: list[str] = []

    async def restored_refund(*_args, **kwargs):
        calls.append(str(kwargs.get("idempotency_key") or ""))
        return True

    monkeypatch.setattr(backend, "_refund_generation_credit", restored_refund)
    monkeypatch.setattr(backend, "schedule_persist_job_state", lambda *_args: None)
    monkeypatch.setattr(backend, "_append_usage_ledger", lambda *_args: None)
    monkeypatch.setattr(backend, "_record_kpi_for_job", lambda *_args: None)
    try:
        asyncio.run(backend._job_diag_finalize(job_id))
        assert calls == [f"generation_refund:{job_id}"]
        assert backend.jobs[job_id]["credit_refunded"] is True
        assert isinstance(backend.jobs[job_id]["diagnostics"], dict)
    finally:
        if previous is None:
            backend.jobs.pop(job_id, None)
        else:
            backend.jobs[job_id] = previous


def test_terminal_queue_receipt_waits_for_refund_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = {
        "status": "error",
        "credit_charged": True,
        "credit_refunded": False,
        "credit_refund_pending": True,
    }
    reconciliations: list[int] = []
    persisted: list[bool] = []
    acknowledgements: list[str] = []

    async def reconcile(_job_id: str, job_state: dict) -> dict:
        reconciliations.append(len(reconciliations) + 1)
        if len(reconciliations) >= 2:
            job_state["credit_refunded"] = True
            job_state["credit_refund_pending"] = False
        return job_state

    async def persist(_job_id: str, job_state: dict) -> bool:
        persisted.append(bool(job_state.get("credit_refunded")))
        return True

    async def acknowledge(payload: dict) -> bool:
        acknowledgements.append(str(payload.get("job_id") or ""))
        return True

    async def no_delay(_seconds: float) -> None:
        return None

    monkeypatch.setattr(backend_queue, "_terminal_job_reconciler", reconcile)
    monkeypatch.setattr(backend_queue, "persist_terminal_job_state", persist)
    monkeypatch.setattr(backend_queue, "acknowledge_generation_job", acknowledge)
    monkeypatch.setattr(backend_queue.asyncio, "sleep", no_delay)

    result = asyncio.run(
        backend_queue._persist_and_ack_terminal_claim(
            {"job_id": "job-refund-recovery"},
            "job-refund-recovery",
            state,
            stop_event=None,
        )
    )

    assert result is True
    assert reconciliations == [1, 2]
    assert persisted == [True]
    assert acknowledgements == ["job-refund-recovery"]


def test_periodic_refund_outbox_replays_persisted_pending_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = "persisted_refund_outbox"
    previous = backend.jobs.get(job_id)
    state = {
        "status": "error",
        "credit_charged": True,
        "credit_refunded": False,
        "credit_refund_pending": True,
        "credit_source": "topup",
        "credit_month_key": "2026-07",
        "credit_amount": 4,
        "user_id": "user-a",
    }
    refund_keys: list[str] = []
    persisted: list[dict] = []

    async def list_pending():
        return [(job_id, dict(state))]

    async def refund(*_args, **kwargs):
        refund_keys.append(str(kwargs.get("idempotency_key") or ""))
        return True

    async def persist(_job_id: str, job_state: dict) -> bool:
        persisted.append(dict(job_state))
        return True

    monkeypatch.setattr(backend, "list_pending_refund_job_states", list_pending)
    monkeypatch.setattr(backend, "_refund_generation_credit", refund)
    monkeypatch.setattr(backend, "persist_job_state_awaited", persist)
    try:
        completed = asyncio.run(backend._reconcile_pending_generation_refunds_once())
        assert completed == 1
        assert refund_keys == [f"generation_refund:{job_id}"]
        assert persisted[-1]["credit_refunded"] is True
        assert persisted[-1]["credit_refund_pending"] is False
    finally:
        if previous is None:
            backend.jobs.pop(job_id, None)
        else:
            backend.jobs[job_id] = previous


def test_queue_admission_failure_persists_pending_before_refund_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = "queue_admission_refund_outbox"
    previous = backend.jobs.get(job_id)
    backend.jobs[job_id] = {
        "status": "queued",
        "credit_charged": True,
        "credit_refunded": False,
        "credit_source": "monthly",
        "credit_month_key": "2026-07",
        "credit_amount": 2,
        "user_id": "user-a",
    }
    persisted: list[dict] = []

    async def persist(_job_id: str, job_state: dict) -> bool:
        persisted.append(dict(job_state))
        return True

    async def fail_refund(*_args, **_kwargs):
        raise OSError("wallet unavailable")

    monkeypatch.setattr(backend, "persist_job_state_awaited", persist)
    monkeypatch.setattr(backend, "_refund_generation_credit", fail_refund)
    monkeypatch.setattr(backend, "schedule_persist_job_state", lambda *_args: None)
    monkeypatch.setattr(backend, "_append_usage_ledger", lambda *_args: None)
    monkeypatch.setattr(backend, "_record_kpi_for_job", lambda *_args: None)
    try:
        result = asyncio.run(
            backend._mark_job_failed_and_reconcile_refund(job_id, "queue full")
        )
        assert persisted[0]["status"] == "error"
        assert persisted[0]["credit_refund_pending"] is True
        assert persisted[0]["credit_refunded"] is False
        assert result["credit_refund_pending"] is True
        assert result["credit_refunded"] is False
        assert len(persisted) == 2
    finally:
        if previous is None:
            backend.jobs.pop(job_id, None)
        else:
            backend.jobs[job_id] = previous


def test_interactive_scene_failure_refunds_wallet_journal_and_reports_truth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settlement_calls: list[dict] = []

    async def fail_after_charge(_req, _request=None, *, refund_context=None):
        refund_context.update(
            {
                "charged": True,
                "operation_id": str(refund_context.get("operation_id") or ""),
                "user_id": "user-a",
                "source": "topup",
                "month_key": "2026-07",
                "credits": 3,
                "session_id": "session-a",
                "scene_index": 2,
            }
        )
        raise HTTPException(503, "provider unavailable")

    async def settle(user_id: str, operation_id: str, **kwargs):
        settlement_calls.append(
            {
                "user_id": user_id,
                "operation_id": operation_id,
                **dict(kwargs),
            }
        )
        return False

    monkeypatch.setattr(backend, "_creative_scene_image_impl", fail_after_charge)
    monkeypatch.setattr(backend, "_settle_generation_credit_operation", settle)

    with pytest.raises(HTTPException) as caught:
        asyncio.run(
            backend._creative_scene_image(
                SimpleNamespace(session_id="session-a", scene_index=2),
                request=None,
            )
        )

    assert caught.value.status_code == 503
    assert "restoration is pending" in str(caught.value.detail)
    assert "refunded" not in str(caught.value.detail).lower()
    assert len(settlement_calls) == 1
    assert settlement_calls[0]["user_id"] == "user-a"
    assert settlement_calls[0]["operation_id"].startswith("interactive_scene:")
    assert settlement_calls[0]["outcome"] == "refund"
    assert settlement_calls[0]["reason"] == (
        "interactive scene session-a:2 failed: HTTPException"
    )


def test_non_animated_failure_refund_is_durable_and_idempotent(
    isolated_credit_wallet: Path,
) -> None:
    billing._topup_wallets["user-a"] = {
        "topup_credits": 0,
        "animated_topup_credits": 0,
        "monthly_usage": {},
        "monthly_usage_non_animated": {"2026-07": 3},
        "processed_mutations": {},
    }
    billing._save_topup_wallets()

    assert asyncio.run(
        billing._refund_generation_credit(
            "user-a",
            "non_animated_free",
            month_key="2026-07",
            credits=1,
            idempotency_key="generation_refund:non-animated-a",
        )
    ) is True
    assert asyncio.run(
        billing._refund_generation_credit(
            "user-a",
            "non_animated_free",
            month_key="2026-07",
            credits=1,
            idempotency_key="generation_refund:non-animated-a",
        )
    ) is False
    assert (
        billing._topup_wallets["user-a"]["monthly_usage_non_animated"]["2026-07"]
        == 2
    )


def test_non_animated_meter_enforces_boundary_atomically(
    isolated_credit_wallet: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(billing, "_month_key", lambda: "2026-07")
    monkeypatch.setattr(billing, "_plan_monthly_non_animated_limit", lambda _plan: 1)
    user = {"id": "user-meter"}

    async def reserve_concurrently() -> list[tuple[bool, str, dict]]:
        return await asyncio.gather(
            *[
                billing._reserve_generation_credit(
                    user,
                    "creator",
                    True,
                    usage_kind="non_animated",
                )
                for _ in range(8)
            ]
        )

    results = asyncio.run(reserve_concurrently())

    assert sum(1 for allowed, _source, _state in results if allowed) == 1
    assert sum(1 for allowed, _source, _state in results if not allowed) == 7
    wallet = billing._topup_wallets["user-meter"]
    assert wallet["monthly_usage_non_animated"]["2026-07"] == 1
    denied_state = next(state for allowed, _source, state in results if not allowed)
    assert denied_state["non_animated_monthly_remaining"] == 0
    assert denied_state["credits_needed"] == 1


def test_non_animated_multi_credit_reservation_and_refund_are_symmetric(
    isolated_credit_wallet: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(billing, "_month_key", lambda: "2026-07")
    monkeypatch.setattr(billing, "_plan_monthly_non_animated_limit", lambda _plan: 3)
    user = {"id": "user-multi-meter"}

    allowed, source, state = asyncio.run(
        billing._reserve_generation_credit(
            user,
            "creator",
            True,
            usage_kind="non_animated",
            credits_needed=3,
        )
    )
    assert allowed is True
    assert source == "non_animated_free"
    assert state["non_animated_monthly_remaining"] == 0

    denied, denied_source, denied_state = asyncio.run(
        billing._reserve_generation_credit(
            user,
            "creator",
            True,
            usage_kind="non_animated",
            credits_needed=1,
        )
    )
    assert denied is False
    assert denied_source == "non_animated_limit"
    assert denied_state["credits_needed"] == 1

    assert asyncio.run(
        billing._refund_generation_credit(
            "user-multi-meter",
            "non_animated_free",
            month_key="2026-07",
            credits=3,
            idempotency_key="generation_refund:multi-meter",
        )
    ) is True
    assert (
        billing._topup_wallets["user-multi-meter"]["monthly_usage_non_animated"][
            "2026-07"
        ]
        == 0
    )


def test_interactive_charge_and_recovery_obligation_share_wallet_snapshot(
    isolated_credit_wallet: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(billing, "_month_key", lambda: "2026-07")
    monkeypatch.setattr(billing, "_plan_monthly_animated_limit", lambda _plan: 0)
    billing._topup_wallets["user-crash"] = {
        "topup_credits": 5,
        "animated_topup_credits": 5,
        "monthly_usage": {},
        "monthly_usage_non_animated": {},
        "processed_mutations": {},
        "pending_generation_operations": {},
    }
    billing._save_topup_wallets()

    allowed, source, _state = asyncio.run(
        billing._reserve_generation_credit(
            {"id": "user-crash"},
            "creator",
            True,
            usage_kind="animated",
            credits_needed=3,
            operation_id="interactive_scene:crash-safe",
            operation_context={
                "kind": "interactive_scene_image",
                "session_id": "session-a",
                "scene_index": 1,
            },
        )
    )
    assert allowed is True
    assert source == "topup"

    persisted = json.loads(billing.TOPUP_WALLET_PATH.read_text(encoding="utf-8"))
    persisted_wallet = persisted["user-crash"]
    assert persisted_wallet["animated_topup_credits"] == 2
    assert (
        persisted_wallet["pending_generation_operations"][
            "interactive_scene:crash-safe"
        ]["credits"]
        == 3
    )

    # Simulate a fresh process loading only the durable wallet snapshot.
    billing._topup_wallets.clear()
    billing._load_topup_wallets()
    stale = asyncio.run(
        billing._list_stale_generation_credit_operations(
            older_than=9999999999.0
        )
    )
    assert [
        (row["user_id"], row["operation_id"])
        for row in stale
    ] == [("user-crash", "interactive_scene:crash-safe")]

    assert asyncio.run(
        billing._settle_generation_credit_operation(
            "user-crash",
            "interactive_scene:crash-safe",
            outcome="refund",
            reason="simulated process crash",
        )
    ) is True
    recovered = billing._topup_wallets["user-crash"]
    assert recovered["animated_topup_credits"] == 5
    assert recovered["pending_generation_operations"] == {}
    assert asyncio.run(
        billing._settle_generation_credit_operation(
            "user-crash",
            "interactive_scene:crash-safe",
            outcome="refund",
            reason="duplicate recovery",
        )
    ) is True


def test_pending_operation_capacity_rejects_before_any_debit(
    isolated_credit_wallet: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(billing, "_month_key", lambda: "2026-07")
    monkeypatch.setattr(billing, "_plan_monthly_animated_limit", lambda _plan: 0)
    pending = {
        f"interactive_scene:existing:{index}": {
            "source": "topup",
            "month_key": "2026-07",
            "credits": 1,
            "created_at": 1.0,
        }
        for index in range(256)
    }
    billing._topup_wallets["user-cap"] = {
        "topup_credits": 5,
        "animated_topup_credits": 5,
        "monthly_usage": {},
        "monthly_usage_non_animated": {},
        "processed_mutations": {},
        "pending_generation_operations": pending,
    }
    billing._save_topup_wallets()

    with pytest.raises(RuntimeError, match="awaiting settlement"):
        asyncio.run(
            billing._reserve_generation_credit(
                {"id": "user-cap"},
                "creator",
                True,
                usage_kind="animated",
                credits_needed=3,
                operation_id="interactive_scene:new-operation",
            )
        )

    wallet = billing._topup_wallets["user-cap"]
    assert wallet["animated_topup_credits"] == 5
    persisted = json.loads(billing.TOPUP_WALLET_PATH.read_text(encoding="utf-8"))
    assert persisted["user-cap"]["animated_topup_credits"] == 5
    assert len(persisted["user-cap"]["pending_generation_operations"]) == 256


@pytest.mark.parametrize(
    "mode",
    ("creative_finalize", "auto_short_generate"),
)
def test_queued_charge_survives_process_reload_and_refunds_on_failure(
    mode: str,
    isolated_credit_wallet: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(billing, "_month_key", lambda: "2026-07")
    monkeypatch.setattr(billing, "_plan_monthly_animated_limit", lambda _plan: 0)
    user_id = f"user-{mode}"
    job_id = f"job_{mode}"
    operation_id = f"queued_job:{job_id}"
    billing._topup_wallets[user_id] = {
        "topup_credits": 10,
        "animated_topup_credits": 10,
        "monthly_usage": {},
        "monthly_usage_non_animated": {},
        "processed_mutations": {},
        "pending_generation_operations": {},
    }
    billing._save_topup_wallets()

    first = asyncio.run(
        billing._reserve_generation_credit(
            {"id": user_id},
            "creator",
            True,
            usage_kind="animated",
            credits_needed=4,
            operation_id=operation_id,
            operation_context={
                "kind": "queued_generation_job",
                "job_id": job_id,
                "mode": mode,
            },
        )
    )
    duplicate = asyncio.run(
        billing._reserve_generation_credit(
            {"id": user_id},
            "creator",
            True,
            usage_kind="animated",
            credits_needed=4,
            operation_id=operation_id,
            operation_context={
                "kind": "queued_generation_job",
                "job_id": job_id,
                "mode": mode,
            },
        )
    )
    assert first[0] is duplicate[0] is True
    assert duplicate[2]["reservation_replayed"] is True
    assert billing._topup_wallets[user_id]["animated_topup_credits"] == 6

    billing._topup_wallets.clear()
    billing._load_topup_wallets()
    terminal = {
        "status": "error",
        "user_id": user_id,
        "credit_charged": True,
        "credit_source": "topup",
        "credit_amount": 4,
        "credit_month_key": "2026-07",
        "credit_operation_id": operation_id,
        "credit_refunded": False,
    }
    reconciled = asyncio.run(
        backend._reconcile_terminal_generation_job(job_id, terminal)
    )

    assert reconciled["credit_refunded"] is True
    assert reconciled["credit_refund_pending"] is False
    assert billing._topup_wallets[user_id]["animated_topup_credits"] == 10
    assert billing._topup_wallets[user_id]["pending_generation_operations"] == {}


def test_queued_success_commits_charge_instead_of_refunding(
    isolated_credit_wallet: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(billing, "_month_key", lambda: "2026-07")
    monkeypatch.setattr(billing, "_plan_monthly_animated_limit", lambda _plan: 0)
    billing._topup_wallets["user-success"] = {
        "topup_credits": 8,
        "animated_topup_credits": 8,
        "monthly_usage": {},
        "monthly_usage_non_animated": {},
        "processed_mutations": {},
        "pending_generation_operations": {},
    }
    billing._save_topup_wallets()
    operation_id = "queued_job:job_success"
    asyncio.run(
        billing._reserve_generation_credit(
            {"id": "user-success"},
            "creator",
            True,
            usage_kind="animated",
            credits_needed=3,
            operation_id=operation_id,
            operation_context={
                "kind": "queued_generation_job",
                "job_id": "job_success",
            },
        )
    )

    reconciled = asyncio.run(
        backend._reconcile_terminal_generation_job(
            "job_success",
            {
                "status": "complete",
                "user_id": "user-success",
                "credit_charged": True,
                "credit_source": "topup",
                "credit_amount": 3,
                "credit_operation_id": operation_id,
                "credit_refunded": False,
            },
        )
    )

    assert reconciled["credit_operation_committed"] is True
    assert reconciled["credit_refunded"] is False
    assert reconciled["credit_refund_pending"] is False
    assert billing._topup_wallets["user-success"]["animated_topup_credits"] == 5
    assert (
        billing._topup_wallets["user-success"]["pending_generation_operations"]
        == {}
    )


def test_public_queued_routes_create_stable_job_before_reservation() -> None:
    request = SimpleNamespace(
        headers={"x-idempotency-key": "stable-public-command"}
    )
    first = backend._backend_owned_job_id(
        request,
        user_id="user-a",
        operation="creative_finalize",
    )
    second = backend._backend_owned_job_id(
        request,
        user_id="user-a",
        operation="creative_finalize",
    )
    assert first == second
    assert first != backend._backend_owned_job_id(
        request,
        user_id="user-b",
        operation="creative_finalize",
    )

    for endpoint in (backend._creative_finalize, backend._generate_short):
        source = inspect.getsource(endpoint)
        assert source.index("_backend_owned_job_id(") < source.index(
            "_reserve_generation_credit("
        )
        assert "operation_id=credit_operation_id" in source
        assert '"kind": "queued_generation_job"' in source


def test_interactive_script_failure_refunds_the_backend_owned_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation_ids: list[str] = []
    settlements: list[tuple[str, str, str]] = []

    async def fail_after_charge(_req, _request, *, refund_context):
        operation_ids.append(str(refund_context["operation_id"]))
        refund_context.update(
            {
                "charged": True,
                "user_id": "user-script",
                "source": "topup",
                "month_key": "2026-07",
                "credits": 1,
            }
        )
        raise HTTPException(502, "Script provider failed")

    async def settle(user_id, operation_id, *, outcome, reason):
        settlements.append((user_id, operation_id, outcome))
        return True

    monkeypatch.setattr(
        backend,
        "_creative_generate_script_impl",
        fail_after_charge,
    )
    monkeypatch.setattr(
        backend,
        "_settle_generation_credit_operation",
        settle,
    )
    request = SimpleNamespace(
        headers={"x-idempotency-key": "script-command-1"}
    )

    for _ in range(2):
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(
                backend._creative_generate_script(
                    SimpleNamespace(),
                    request,
                )
            )
        assert exc_info.value.status_code == 502
        assert "credit charge was refunded" in str(exc_info.value.detail).lower()

    assert operation_ids[0] == operation_ids[1]
    assert operation_ids[0].startswith("interactive_script:job_")
    assert settlements == [
        ("user-script", operation_ids[0], "refund"),
        ("user-script", operation_ids[0], "refund"),
    ]
