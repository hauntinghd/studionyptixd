from pathlib import Path

from backend_settings import UNIFIED_PLANS
from studio_agent.access import STUDIO_AGENT_PLANS


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
