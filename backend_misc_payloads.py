"""Miscellaneous payload builders for the Studio API."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Callable

from fastapi import HTTPException


def build_training_stats_payload(
    *,
    admin_emails: set[str],
    training_data_dir: Path,
    pending_training_ref: dict,
):
    async def training_stats_payload(user: dict):
        """Admin: get training data collection stats."""
        email = user.get("email", "")
        if email not in admin_emails:
            raise HTTPException(403, "Admin only")
        pairs = list(training_data_dir.glob("*.png"))
        accepted = sum(1 for entry in pending_training_ref.values() if entry["status"] == "accepted")
        rejected = sum(1 for entry in pending_training_ref.values() if entry["status"] == "rejected")
        pending = sum(1 for entry in pending_training_ref.values() if entry["status"] == "pending")
        return {
            "total_on_disk": len(pairs),
            "accepted": accepted,
            "rejected": rejected,
            "pending_review": pending,
            "disk_mb": round(sum(path.stat().st_size for path in pairs) / (1024 * 1024), 1),
        }

    return training_stats_payload


def build_admin_waiting_list_payload(
    *,
    admin_emails: set[str],
    supabase_get_waitlist_rows: Callable,
):
    async def admin_waiting_list_payload(user: dict):
        if user.get("email", "") not in admin_emails:
            raise HTTPException(403, "Admin only")
        rows = await supabase_get_waitlist_rows(limit=3000)
        total = len(rows)
        by_plan = {"starter": 0, "creator": 0, "pro": 0, "elite": 0}
        paid_revenue_monthly = 0.0
        for row in rows:
            plan = str((row or {}).get("plan", "") or "").strip().lower()
            if plan in by_plan:
                by_plan[plan] += 1
            if bool((row or {}).get("paid")):
                paid_revenue_monthly += float((row or {}).get("price_usd", 0.0) or 0.0)
        return {
            "rows": rows,
            "summary": {
                "total": total,
                "by_plan": by_plan,
                "paid_revenue_monthly_usd": round(paid_revenue_monthly, 2),
            },
        }

    return admin_waiting_list_payload


def build_maintenance_banner_payload(
    *,
    admin_emails: set[str],
    maintenance_snapshot: Callable[[], tuple[bool, str]],
    set_maintenance_snapshot: Callable[[bool, str], tuple[bool, str]],
    bool_from_any: Callable[[object, bool], bool],
    persist_env_overrides: Callable[[dict], None],
    log,
):
    async def set_maintenance_banner_payload(body: dict, user: dict):
        email = user.get("email", "")
        if email not in admin_emails:
            raise HTTPException(403, "Admin only")

        current_enabled, current_message = maintenance_snapshot()
        enabled = bool_from_any(body.get("enabled"), current_enabled)
        message = str(body.get("message", current_message)).strip()
        if not message:
            message = "Studio is under high load. Queue times may be longer than usual while we scale capacity."

        updated_enabled, updated_message = set_maintenance_snapshot(enabled, message)

        try:
            escaped_message = '"' + message.replace('"', '\\"') + '"'
            persist_env_overrides(
                {
                    "MAINTENANCE_BANNER_ENABLED": "1" if enabled else "0",
                    "MAINTENANCE_BANNER_MESSAGE": escaped_message,
                }
            )
        except Exception as exc:
            log.warning(f"Failed to persist maintenance banner settings to .env: {exc}")

        return {
            "ok": True,
            "maintenance_banner_enabled": updated_enabled,
            "maintenance_banner_message": updated_message,
        }

    return set_maintenance_banner_payload


def build_landing_notifications_payload(
    *,
    landing_notifications_ref: list,
    landing_notifications_lock,
    public_limit: int,
):
    async def landing_notifications_payload():
        cutoff = time.time() - (7 * 24 * 3600)
        async with landing_notifications_lock:
            events = [
                event
                for event in landing_notifications_ref
                if isinstance(event, dict) and float(event.get("ts") or 0.0) >= cutoff
            ]
            events = events[-public_limit:]
        return {"events": events}

    return landing_notifications_payload
