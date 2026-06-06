"""Who may use Studio Agent and how billing applies."""
from __future__ import annotations

from typing import Any, Callable

STUDIO_AGENT_PLANS = frozenset({"creator", "studio"})


def is_owner(user: dict | None, is_admin_check: Callable[[dict], bool] | None) -> bool:
    if not user or not is_admin_check:
        return False
    try:
        return bool(is_admin_check(user))
    except Exception:
        return False


def unified_plan(user_id: str) -> str:
    uid = str(user_id or "").strip()
    if not uid:
        return ""
    try:
        import unified_credits as uc

        uc.ensure_monthly_grant(uid)
        return str((uc.get_state(uid) or {}).get("plan") or "").strip().lower()
    except Exception:
        return ""


def account_profile(
    user: dict | None,
    *,
    is_admin_check: Callable[[dict], bool] | None = None,
) -> dict[str, Any]:
    """Owner (admin) = unmetered. Subscriber = unified wallet. Everyone else = no agent."""
    uid = str((user or {}).get("id") or (user or {}).get("user_id") or "").strip()
    owner = is_owner(user, is_admin_check)
    plan = unified_plan(uid) if uid else ""
    spec_name = ""
    balance = 0
    monthly = 0
    if uid and not owner:
        try:
            import unified_credits as uc

            st = uc.get_state(uid)
            balance = int(st.get("balance") or 0)
            monthly = int(st.get("monthly_credits") or 0)
            spec_name = str(st.get("plan_name") or "")
        except Exception:
            pass
    if owner:
        return {
            "tier": "owner",
            "label": "Owner account",
            "unlimited": True,
            "plan": "owner",
            "plan_name": "Owner",
            "balance": balance,
            "monthly_credits": 0,
            "metering": "none",
        }
    if plan in STUDIO_AGENT_PLANS:
        return {
            "tier": "subscriber",
            "label": spec_name or plan.title(),
            "unlimited": False,
            "plan": plan,
            "plan_name": spec_name,
            "balance": balance,
            "monthly_credits": monthly,
            "metering": "unified_credits",
        }
    return {
        "tier": "none",
        "label": "",
        "unlimited": False,
        "plan": plan or "",
        "plan_name": "",
        "balance": 0,
        "monthly_credits": 0,
        "metering": "blocked",
    }


def can_use_studio_agent(
    user: dict | None,
    *,
    is_admin_check: Callable[[dict], bool] | None = None,
    lane_access_check: Callable[[dict], bool] | None = None,
) -> bool:
    if is_owner(user, is_admin_check):
        return True
    if lane_access_check and user:
        try:
            return bool(lane_access_check(user))
        except Exception:
            pass
    uid = str((user or {}).get("id") or "").strip()
    return unified_plan(uid) in STUDIO_AGENT_PLANS
