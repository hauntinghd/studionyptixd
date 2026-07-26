from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

import backend
from backend_settings import UNIFIED_PLANS
from studio_agent.access import STUDIO_AGENT_PLANS
from studio_agent import provider_policy, store
import studio_agent_router


ROOT = Path(__file__).resolve().parents[1]


def test_every_unified_paid_plan_can_use_studio_agent() -> None:
    assert STUDIO_AGENT_PLANS == frozenset(UNIFIED_PLANS)
    assert "studio_pro_1k" in STUDIO_AGENT_PLANS
    assert "studio_pro_32k" in STUDIO_AGENT_PLANS


def test_agent_router_uses_the_authenticated_non_billing_lane_check() -> None:
    source = (ROOT / "backend.py").read_text(encoding="utf-8")
    assert "lane_access_check=_authenticated_studio_lane_access_for_user" in source
    assert "export_access_check=_authenticated_studio_lane_access_for_user" in source
    assert '(_public_lane_access_for_user(user) or {}).get("agent")' not in source
    assert '(_public_lane_access_for_user(user) or {}).get("export_final")' not in source
    assert '.get("studio_agent")' not in source


def test_public_release_allows_authenticated_job_owners_to_export() -> None:
    source = (ROOT / "backend.py").read_text(encoding="utf-8")
    assert '"export_final": authenticated,' in source
    assert 'not beta_access' not in source


def test_agent_bootstrap_chat_and_owned_export_checks_make_zero_stripe_calls(
    tmp_path,
    monkeypatch,
) -> None:
    stripe_calls: list[str] = []

    def unexpected_stripe_call(*_args, **_kwargs):
        stripe_calls.append("stripe")
        raise AssertionError("authenticated Studio lane access must not discover Stripe")

    monkeypatch.setattr(backend, "_stripe_subscription_snapshot", unexpected_stripe_call)
    monkeypatch.setattr(backend, "_paid_access_snapshot_for_user", unexpected_stripe_call)
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path / "sessions")
    monkeypatch.setattr(studio_agent_router.openrouter, "api_key", lambda: "test-key")
    monkeypatch.setattr(
        studio_agent_router.model_registry,
        "assert_model_selectable",
        lambda _model: None,
    )
    media_path = tmp_path / "owned-final.mp4"
    media_path.write_bytes(b"owned-final")
    monkeypatch.setattr(
        studio_agent_router.agent_jobs,
        "job_access_metadata",
        lambda _job_id, _kind: {
            "exists": True,
            "owner_id": "lane-user",
            "kind": "shortform",
        },
    )
    monkeypatch.setattr(
        studio_agent_router.agent_jobs,
        "resolve_media_path",
        lambda _job_id, _kind: media_path,
    )

    app = FastAPI()
    app.include_router(
        studio_agent_router.build_studio_agent_router(
            require_auth=lambda: {
                "id": "lane-user",
                "email": "lane-user@example.com",
                "plan": "free",
            },
            is_admin_check=lambda _user: False,
            lane_access_check=backend._authenticated_studio_lane_access_for_user,
            export_access_check=backend._authenticated_studio_lane_access_for_user,
        )
    )
    client = TestClient(app)

    assert client.get("/api/studio-agent/sessions").status_code == 200
    bootstrap = client.post(
        "/api/studio-agent/sessions/bootstrap",
        json={
            "bootstrap_key": "stripe-free-bootstrap",
            "model": provider_policy.DEFAULT_RUNNER_MODEL,
        },
    )
    assert bootstrap.status_code == 200, bootstrap.text
    # A 404 proves chat passed the shared authenticated lane dependency and
    # stopped at session ownership, without entering model execution.
    assert client.post(
        "/api/studio-agent/sessions/sa_missing/chat",
        json={"message": "hello"},
    ).status_code == 404
    assert client.get(
        "/api/studio-agent/jobs/owned-job/media?kind=shortform"
    ).status_code == 200
    assert stripe_calls == []


def test_paid_public_lanes_remain_fail_closed_when_stripe_is_unavailable(
    monkeypatch,
) -> None:
    stripe_calls: list[str] = []

    def unavailable_stripe(_email: str) -> dict:
        stripe_calls.append("stripe")
        return {"ok": False, "status": "", "plan": ""}

    monkeypatch.setattr(backend, "_stripe_subscription_snapshot", unavailable_stripe)
    lanes = backend._public_lane_access_for_user(
        {
            "id": "unavailable-paid-lane-user",
            "email": "unavailable@example.com",
            # A stale profile plan alone must not grant a paid lane.
            "plan": "studio_pro_1k",
        }
    )

    assert lanes["agent"] is True
    assert lanes["export_final"] is True
    assert lanes["chatstory"] is False
    assert lanes["membership"] is False
    assert stripe_calls == ["stripe"]


def test_public_app_preserves_backend_emergency_access_controls() -> None:
    source = (ROOT / "ViralShorts-App" / "src" / "studio" / "shared.tsx").read_text(
        encoding="utf-8"
    )
    assert "setWaitlistOnlyMode(Boolean(cfg.waitlist_only_mode));" in source
    assert (
        "setWaitlistRequiresStripePayment(Boolean(cfg.waitlist_requires_stripe_payment));"
        in source
    )


def test_legacy_unmetered_longform_workspace_remains_owner_only() -> None:
    source = (ROOT / "backend.py").read_text(encoding="utf-8")
    helper = source.split("def _longform_owner_beta_enabled", 1)[1].split(
        "def _longform_deep_analysis_enabled", 1
    )[0]
    assert "return _is_admin_user(user)" in helper
    assert '_public_lane_access_for_user(user)' not in helper


def test_visual_proof_duration_is_not_misrouted_as_existing_short_expansion() -> None:
    message = (
        "yes make it -- render that plan for 1 scene first so i can see what it looks like, "
        "and keep the skeleton-anatomy visual style, only 30 seconds"
    )

    assert store.is_expand_short_request(message) is False
    assert store.is_hard_production_commit(message) is True


def test_real_scene_one_expansion_language_remains_an_expansion() -> None:
    assert store.is_expand_short_request(
        "I like Scene 1. Keep it and make the remaining five scenes."
    ) is True
