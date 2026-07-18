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
