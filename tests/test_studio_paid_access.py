from pathlib import Path

from backend_settings import UNIFIED_PLANS
from studio_agent.access import STUDIO_AGENT_PLANS
from studio_agent import store


ROOT = Path(__file__).resolve().parents[1]


def test_every_unified_paid_plan_can_use_studio_agent() -> None:
    assert STUDIO_AGENT_PLANS == frozenset(UNIFIED_PLANS)
    assert "studio_pro_1k" in STUDIO_AGENT_PLANS
    assert "studio_pro_32k" in STUDIO_AGENT_PLANS


def test_agent_router_uses_the_public_agent_lane_key() -> None:
    source = (ROOT / "backend.py").read_text(encoding="utf-8")
    expected = (
        'lane_access_check=lambda user: bool('
        '(_public_lane_access_for_user(user) or {}).get("agent"))'
    )
    assert expected in source
    assert '.get("studio_agent")' not in source


def test_public_release_allows_authenticated_job_owners_to_export() -> None:
    source = (ROOT / "backend.py").read_text(encoding="utf-8")
    assert '"export_final": authenticated,' in source
    assert 'not beta_access' not in source


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
