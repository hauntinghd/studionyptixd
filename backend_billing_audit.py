"""Admin billing audit payload builder for the Studio API."""

from __future__ import annotations

from typing import Callable

import httpx
from fastapi import HTTPException


def build_admin_billing_audit_payload(
    *,
    admin_emails: set[str],
    supabase_url: str,
    supabase_service_key: str,
    supabase_anon_key: str,
    profile_plan_is_paid: Callable[[str], bool],
    stripe_subscription_snapshot: Callable[[str], dict],
    next_renewal_from_anchor: Callable[[int, int], int],
    log,
):
    async def admin_billing_audit_payload(user: dict):
        email = str(user.get("email", "") or "")
        if email not in admin_emails:
            raise HTTPException(403, "Admin only")
        svc_key = supabase_service_key or supabase_anon_key
        if not supabase_url or not svc_key:
            raise HTTPException(500, "Supabase not configured")

        rows: list[dict] = []
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.get(
                    f"{supabase_url}/auth/v1/admin/users?per_page=500",
                    headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
                )
                if resp.status_code != 200:
                    raise HTTPException(500, "Failed to read auth users for billing audit")
                users_data = resp.json()
                user_list = users_data.get("users", users_data) if isinstance(users_data, dict) else users_data
                by_email = {
                    str(supabase_user.get("email", "") or "").strip().lower(): str(
                        supabase_user.get("id", "") or ""
                    )
                    for supabase_user in (user_list or [])
                    if supabase_user and str(supabase_user.get("email", "") or "").strip()
                }

                prof = await client.get(
                    f"{supabase_url}/rest/v1/profiles?select=id,plan",
                    headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
                )
                if prof.status_code != 200:
                    raise HTTPException(500, "Failed to read profile plans for billing audit")
                profiles = prof.json()
                profiles = profiles if isinstance(profiles, list) else []
                for profile in profiles:
                    plan = str((profile or {}).get("plan", "none") or "none").strip().lower()
                    if not profile_plan_is_paid(plan):
                        continue
                    uid = str((profile or {}).get("id", "") or "")
                    acct_email = ""
                    for profile_email, eid in by_email.items():
                        if eid == uid:
                            acct_email = profile_email
                            break
                    if not acct_email:
                        continue
                    stripe_diag = stripe_subscription_snapshot(acct_email)
                    stripe_status = str(stripe_diag.get("status", "") or "")
                    stripe_ok = bool(stripe_diag.get("ok")) and stripe_status in {"active", "trialing", "past_due"}
                    cancel_at_period_end = bool(stripe_diag.get("cancel_at_period_end", False))
                    status_source = "stripe" if stripe_ok else "profile_fallback"
                    next_renewal_unix = int(stripe_diag.get("next_renewal_unix", 0) or 0)
                    next_renewal_source = str(stripe_diag.get("next_renewal_source", "") or "")
                    paid_at_unix = int(stripe_diag.get("paid_at_unix", 0) or 0)
                    interval_months = max(1, int(stripe_diag.get("interval_months", 1) or 1))
                    if next_renewal_unix <= 0 and paid_at_unix > 0 and not cancel_at_period_end:
                        rolled = next_renewal_from_anchor(paid_at_unix, interval_months)
                        if rolled > 0:
                            next_renewal_unix = int(rolled)
                            next_renewal_source = next_renewal_source or "paid_at_rollforward_fallback"
                    rows.append(
                        {
                            "email": acct_email,
                            "user_id": uid,
                            "plan": plan,
                            "status_source": status_source,
                            "stripe_status": stripe_status or "unknown",
                            "cancel_at_period_end": cancel_at_period_end,
                            "billing_active": bool(stripe_ok or profile_plan_is_paid(plan)),
                            "next_renewal_unix": next_renewal_unix,
                            "next_renewal_source": next_renewal_source,
                            "paid_at_unix": paid_at_unix,
                        }
                    )
        except HTTPException:
            raise
        except Exception as exc:
            log.error(f"Admin billing audit failed: {exc}")
            raise HTTPException(500, "Billing audit failed")

        rows.sort(key=lambda row: (row.get("plan", ""), row.get("email", "")))
        return {"rows": rows, "total_paid_profiles": len(rows)}

    return admin_billing_audit_payload
