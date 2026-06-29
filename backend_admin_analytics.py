"""Admin analytics payload builder for the Studio API."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

import httpx
from fastapi import HTTPException


def build_admin_analytics_payload(
    *,
    admin_emails: set[str],
    jobs_ref: dict,
    stripe_secret_key: str,
    stripe_lib,
    stripe_price_to_plan: dict,
    supabase_url: str,
    supabase_service_key: str,
    supabase_anon_key: str,
    plan_price_usd: dict,
    get_queue_depth: Callable,
    get_queue_workers: Callable[[], int],
    get_queue_max_depth: Callable[[], int],
    voice_provider_snapshot: Callable,
    maintenance_snapshot: Callable[[], tuple[bool, str]],
    log,
):
    async def admin_analytics_payload(user: dict):
        """Admin dashboard analytics: active usage + paid tier totals + monthly revenue estimate."""
        email = user.get("email", "")
        if email not in admin_emails:
            raise HTTPException(403, "Admin only")

        active_job_statuses = {
            "queued",
            "generating_script",
            "generating_images",
            "animating_scenes",
            "generating_voice",
            "generating_sfx",
            "compositing",
            "analyzing",
        }
        active_jobs = []
        for job in jobs_ref.values():
            if isinstance(job, dict) and job.get("status") in active_job_statuses:
                active_jobs.append(job)
        active_generations = len(active_jobs)
        active_generating_users = len({job.get("user_id") for job in active_jobs if job.get("user_id")})

        tier_counts = {"starter": 0, "creator": 0, "pro": 0, "elite": 0, "demo_pro": 0}
        monthly_revenue_usd = 0.0
        revenue_source = "none"

        if stripe_secret_key:
            try:
                subs = stripe_lib.Subscription.list(status="all", limit=100, expand=["data.items.data.price"])
                for sub in subs.auto_paging_iter():
                    sub_status = sub.get("status")
                    if sub_status not in ("active", "trialing", "past_due", "unpaid"):
                        continue
                    for item in sub.get("items", {}).get("data", []):
                        price = item.get("price", {}) or {}
                        price_id = price.get("id", "")
                        plan = stripe_price_to_plan.get(price_id)
                        if plan in tier_counts:
                            qty = int(item.get("quantity", 1) or 1)
                            tier_counts[plan] += qty
                            monthly_revenue_usd += ((price.get("unit_amount") or 0) / 100.0) * qty
                revenue_source = "stripe"
            except Exception as exc:
                log.warning(f"Admin analytics stripe read failed: {exc}")

        if revenue_source != "stripe" and supabase_url and (supabase_service_key or supabase_anon_key):
            try:
                svc_key = supabase_service_key or supabase_anon_key
                async with httpx.AsyncClient(timeout=20) as client:
                    resp = await client.get(
                        f"{supabase_url}/rest/v1/profiles?select=plan&limit=5000",
                        headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
                    )
                    if resp.status_code == 200:
                        for row in resp.json():
                            plan = row.get("plan")
                            if plan in tier_counts:
                                tier_counts[plan] += 1
                                monthly_revenue_usd += float(plan_price_usd.get(plan, 0.0) or 0.0)
                        revenue_source = "profiles"
            except Exception as exc:
                log.warning(f"Admin analytics profiles fallback failed: {exc}")

        active_users_signins_15m = 0
        if supabase_url and (supabase_service_key or supabase_anon_key):
            try:
                svc_key = supabase_service_key or supabase_anon_key
                now_utc = datetime.now(timezone.utc)
                async with httpx.AsyncClient(timeout=20) as client:
                    users_resp = await client.get(
                        f"{supabase_url}/auth/v1/admin/users?per_page=500",
                        headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
                    )
                    if users_resp.status_code == 200:
                        users_data = users_resp.json()
                        user_list = users_data.get("users", users_data) if isinstance(users_data, dict) else users_data
                        for supabase_user in user_list:
                            ts = supabase_user.get("last_sign_in_at")
                            if not ts:
                                continue
                            try:
                                signed_in_at = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                                if (now_utc - signed_in_at).total_seconds() <= 15 * 60:
                                    active_users_signins_15m += 1
                            except Exception:
                                continue
            except Exception as exc:
                log.warning(f"Admin analytics active-users fetch failed: {exc}")

        queue_depth = 0
        queue_workers = 1
        queue_max_depth = 1
        try:
            queue_depth = max(0, int(await get_queue_depth()))
        except Exception as exc:
            log.warning(f"Admin analytics queue depth read failed: {exc}")
        try:
            queue_workers = max(1, int(get_queue_workers()))
        except Exception as exc:
            log.warning(f"Admin analytics queue workers read failed: {exc}")
        try:
            queue_max_depth = max(1, int(get_queue_max_depth()))
        except Exception as exc:
            log.warning(f"Admin analytics queue max-depth read failed: {exc}")
        active_users_estimate = max(active_generating_users, active_users_signins_15m)
        queue_utilization_pct = round((queue_depth / queue_max_depth) * 100, 1)
        active_generations_per_worker = round(active_generations / queue_workers, 2)
        high_load_detected = queue_utilization_pct >= 70.0 or active_generations_per_worker >= 1.0
        maintenance_banner_enabled, maintenance_banner_message = maintenance_snapshot()
        voice_diag = await voice_provider_snapshot(force_refresh=False)
        return {
            "active_generations": active_generations,
            "queue_depth": queue_depth,
            "queue_workers": queue_workers,
            "queue_max_depth": queue_max_depth,
            "queue_utilization_pct": queue_utilization_pct,
            "active_generations_per_worker": active_generations_per_worker,
            "high_load_detected": high_load_detected,
            "active_users_generating": active_generating_users,
            "active_users_signins_15m": active_users_signins_15m,
            "active_users_estimate": active_users_estimate,
            "maintenance_banner_enabled": maintenance_banner_enabled,
            "maintenance_banner_message": maintenance_banner_message,
            "subscribers_by_tier": tier_counts,
            "total_paid_subscribers": sum(tier_counts.values()),
            "monthly_revenue_usd": round(monthly_revenue_usd, 2),
            "monthly_profit_usd": round(monthly_revenue_usd, 2),
            "revenue_source": revenue_source,
            "voice_provider_ok": voice_diag["provider_ok"],
            "voice_catalog_source": voice_diag["source"],
            "voice_catalog_count": voice_diag["count"],
            "voice_catalog_warning": voice_diag["warning"],
        }

    return admin_analytics_payload
