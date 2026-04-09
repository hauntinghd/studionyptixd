from __future__ import annotations

import asyncio
import calendar
import json
import logging
import time
from datetime import datetime, timezone
from urllib.parse import quote

import httpx
import stripe as stripe_lib

from backend_catalog import PLAN_LIMITS
from backend_settings import SUPABASE_ANON_KEY, SUPABASE_SERVICE_KEY, SUPABASE_URL, STRIPE_PRICE_TO_PLAN, STRIPE_SECRET_KEY, TEMP_DIR

log = logging.getLogger("nyptid-studio")

KPI_TARGETS = {
    "first_render_success_rate": 0.95,
    "time_to_publishable_sec": 8 * 60,
    "estimated_cost_per_short_usd": 2.00,
}
KPI_METRICS_PATH = TEMP_DIR / "kpi_metrics.json"
TOPUP_WALLET_PATH = TEMP_DIR / "topup_wallets.json"
PAYPAL_ORDERS_PATH = TEMP_DIR / "paypal_orders.json"
PAYPAL_SUBSCRIPTIONS_PATH = TEMP_DIR / "paypal_subscriptions.json"
USAGE_LEDGER_PATH = TEMP_DIR / "usage_ledger.jsonl"
LANDING_NOTIFICATIONS_PATH = TEMP_DIR / "landing_notifications.json"
LANDING_NOTIFICATIONS_LIMIT = 120
LANDING_NOTIFICATIONS_PUBLIC_LIMIT = 25

_kpi_metrics = {
    "total_jobs": 0,
    "completed_jobs": 0,
    "error_jobs": 0,
    "first_render_pass_jobs": 0,
    "total_publishable_time_sec": 0.0,
    "total_estimated_cost_usd": 0.0,
    "template_breakdown": {},
    "updated_at": 0.0,
}
_topup_wallets: dict[str, dict] = {}
_topup_wallet_lock = asyncio.Lock()
_paypal_orders: dict[str, dict] = {}
_paypal_orders_lock = asyncio.Lock()
_paypal_subscriptions: dict[str, dict] = {}
_paypal_subscriptions_lock = asyncio.Lock()
_landing_notifications: list[dict] = []
_landing_notifications_lock = asyncio.Lock()


def _load_kpi_metrics() -> None:
    try:
        if KPI_METRICS_PATH.exists():
            loaded = json.loads(KPI_METRICS_PATH.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                _kpi_metrics.update(loaded)
    except Exception:
        pass


def _save_kpi_metrics() -> None:
    try:
        KPI_METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
        KPI_METRICS_PATH.write_text(json.dumps(_kpi_metrics, indent=2), encoding="utf-8")
    except Exception:
        pass


def _load_topup_wallets() -> None:
    try:
        if TOPUP_WALLET_PATH.exists():
            data = json.loads(TOPUP_WALLET_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                _topup_wallets.clear()
                _topup_wallets.update(data)
                return
    except Exception:
        pass
    _topup_wallets.clear()


def _save_topup_wallets() -> None:
    try:
        TOPUP_WALLET_PATH.parent.mkdir(parents=True, exist_ok=True)
        TOPUP_WALLET_PATH.write_text(json.dumps(_topup_wallets, indent=2), encoding="utf-8")
    except Exception:
        pass


def _load_paypal_orders() -> None:
    try:
        if PAYPAL_ORDERS_PATH.exists():
            data = json.loads(PAYPAL_ORDERS_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                _paypal_orders.clear()
                _paypal_orders.update(data)
                return
    except Exception:
        pass
    _paypal_orders.clear()


def _save_paypal_orders() -> None:
    try:
        PAYPAL_ORDERS_PATH.parent.mkdir(parents=True, exist_ok=True)
        PAYPAL_ORDERS_PATH.write_text(json.dumps(_paypal_orders, indent=2), encoding="utf-8")
    except Exception:
        pass


def _load_paypal_subscriptions() -> None:
    try:
        if PAYPAL_SUBSCRIPTIONS_PATH.exists():
            data = json.loads(PAYPAL_SUBSCRIPTIONS_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                _paypal_subscriptions.clear()
                _paypal_subscriptions.update(data)
                return
    except Exception:
        pass
    _paypal_subscriptions.clear()


def _save_paypal_subscriptions() -> None:
    try:
        PAYPAL_SUBSCRIPTIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
        PAYPAL_SUBSCRIPTIONS_PATH.write_text(json.dumps(_paypal_subscriptions, indent=2), encoding="utf-8")
    except Exception:
        pass


def _mask_email_for_public(email: str) -> str:
    raw = str(email or "").strip().lower()
    if "@" not in raw:
        return "a creator"
    local, domain = raw.split("@", 1)
    local = local.strip()
    domain = domain.strip()
    if not local or not domain:
        return "a creator"
    if len(local) <= 2:
        safe_local = local[0] + "*"
    else:
        safe_local = local[:2] + ("*" * min(4, max(1, len(local) - 2)))
    return f"{safe_local}@{domain}"


def _load_landing_notifications() -> None:
    try:
        if LANDING_NOTIFICATIONS_PATH.exists():
            data = json.loads(LANDING_NOTIFICATIONS_PATH.read_text(encoding="utf-8"))
            if isinstance(data, list):
                cleaned: list[dict] = []
                for item in data[-LANDING_NOTIFICATIONS_LIMIT:]:
                    if isinstance(item, dict):
                        cleaned.append(item)
                _landing_notifications.clear()
                _landing_notifications.extend(cleaned)
                return
    except Exception:
        pass
    _landing_notifications.clear()


def _save_landing_notifications() -> None:
    try:
        LANDING_NOTIFICATIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
        LANDING_NOTIFICATIONS_PATH.write_text(
            json.dumps(_landing_notifications[-LANDING_NOTIFICATIONS_LIMIT:], ensure_ascii=True, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


async def _append_landing_notification(event_type: str, plan: str = "", credits: int = 0, customer_email: str = "") -> None:
    evt_type = str(event_type or "").strip().lower()
    if evt_type not in {"subscription", "topup"}:
        return
    now = time.time()
    event = {
        "type": evt_type,
        "plan": str(plan or "").strip().lower(),
        "credits": int(max(0, credits)),
        "email_masked": _mask_email_for_public(customer_email),
        "ts": now,
    }
    async with _landing_notifications_lock:
        _landing_notifications.append(event)
        if len(_landing_notifications) > LANDING_NOTIFICATIONS_LIMIT:
            _landing_notifications[:] = _landing_notifications[-LANDING_NOTIFICATIONS_LIMIT:]
        _save_landing_notifications()


def _month_key(ts: float | None = None) -> str:
    now = datetime.fromtimestamp(ts or time.time(), tz=timezone.utc)
    return f"{now.year:04d}-{now.month:02d}"


def _wallet_for_user(user_id: str) -> dict:
    if not user_id:
        return {
            "topup_credits": 0,
            "animated_topup_credits": 0,
            "monthly_usage": {},
            "monthly_usage_non_animated": {},
            "updated_at": time.time(),
        }
    wallet = _topup_wallets.get(user_id)
    if not isinstance(wallet, dict):
        wallet = {
            "topup_credits": 0,
            "animated_topup_credits": 0,
            "monthly_usage": {},
            "monthly_usage_non_animated": {},
            "updated_at": time.time(),
        }
        _topup_wallets[user_id] = wallet
    wallet.setdefault("topup_credits", 0)
    wallet.setdefault("animated_topup_credits", int(wallet.get("topup_credits", 0) or 0))
    wallet.setdefault("monthly_usage", {})
    wallet.setdefault("monthly_usage_non_animated", {})
    wallet.setdefault("updated_at", time.time())
    return wallet


def _plan_monthly_animated_limit(plan: str) -> int:
    limits = PLAN_LIMITS.get(plan, PLAN_LIMITS.get("free", PLAN_LIMITS.get("starter", {})))
    return int(limits.get("animated_renders_per_month", limits.get("videos_per_month", 0)) or 0)


def _plan_monthly_non_animated_limit(plan: str) -> int:
    limits = PLAN_LIMITS.get(plan, PLAN_LIMITS.get("free", PLAN_LIMITS.get("starter", {})))
    fallback = int(limits.get("animated_renders_per_month", limits.get("videos_per_month", 0)) or 0) * 10
    return int(limits.get("non_animated_ops_per_month", fallback) or 0)


def _credit_state_for_user(user: dict, effective_plan: str, billing_active: bool, is_admin: bool = False) -> dict:
    if is_admin:
        return {
            "animated_monthly_limit": 9999,
            "animated_monthly_used": 0,
            "animated_monthly_remaining": 9999,
            "animated_topup_credits": 9999,
            "animated_total_remaining": 9999,
            "non_animated_monthly_limit": 9999,
            "non_animated_monthly_used": 0,
            "non_animated_monthly_remaining": 9999,
            "requires_topup": False,
            "month_key": _month_key(),
            "monthly_limit": 9999,
            "monthly_used": 0,
            "monthly_remaining": 9999,
            "topup_credits": 9999,
            "credits_total_remaining": 9999,
        }
    user_id = str(user.get("id", "") or "")
    wallet = _wallet_for_user(user_id)
    mk = _month_key()
    animated_used = int((wallet.get("monthly_usage", {}) or {}).get(mk, 0) or 0)
    non_animated_used = int((wallet.get("monthly_usage_non_animated", {}) or {}).get(mk, 0) or 0)
    plan_with_included_credits = billing_active or str(effective_plan or "").strip().lower() == "free"
    animated_limit = _plan_monthly_animated_limit(effective_plan) if plan_with_included_credits else 0
    non_animated_limit = _plan_monthly_non_animated_limit(effective_plan)
    animated_remaining = max(0, animated_limit - animated_used)
    non_animated_remaining = max(0, non_animated_limit - non_animated_used)
    topup = int(wallet.get("animated_topup_credits", wallet.get("topup_credits", 0)) or 0)
    total_remaining = animated_remaining + topup
    return {
        "animated_monthly_limit": animated_limit,
        "animated_monthly_used": animated_used,
        "animated_monthly_remaining": animated_remaining,
        "animated_topup_credits": topup,
        "animated_total_remaining": total_remaining,
        "non_animated_monthly_limit": non_animated_limit,
        "non_animated_monthly_used": non_animated_used,
        "non_animated_monthly_remaining": non_animated_remaining,
        "requires_topup": bool(total_remaining <= 0),
        "month_key": mk,
        "monthly_limit": animated_limit,
        "monthly_used": animated_used,
        "monthly_remaining": animated_remaining,
        "topup_credits": topup,
        "credits_total_remaining": total_remaining,
    }


async def _reserve_generation_credit(
    user: dict,
    effective_plan: str,
    billing_active: bool,
    is_admin: bool = False,
    usage_kind: str = "animated",
    credits_needed: int = 1,
) -> tuple[bool, str, dict]:
    if is_admin:
        return True, "admin", _credit_state_for_user(user, effective_plan, billing_active, is_admin=True)
    user_id = str(user.get("id", "") or "")
    required_credits = max(1, int(credits_needed or 1))
    async with _topup_wallet_lock:
        wallet = _wallet_for_user(user_id)
        state = _credit_state_for_user(user, effective_plan, billing_active, is_admin=False)
        mk = state["month_key"]
        if usage_kind == "non_animated":
            usage = dict(wallet.get("monthly_usage_non_animated", {}) or {})
            usage[mk] = int(usage.get(mk, 0) or 0) + 1
            wallet["monthly_usage_non_animated"] = usage
            wallet["updated_at"] = time.time()
            _save_topup_wallets()
            return True, "non_animated_free", _credit_state_for_user(user, effective_plan, billing_active, is_admin=False)
        monthly_remaining = int(state.get("animated_monthly_remaining", 0) or 0)
        if monthly_remaining >= required_credits:
            usage = dict(wallet.get("monthly_usage", {}) or {})
            usage[mk] = int(usage.get(mk, 0) or 0) + required_credits
            wallet["monthly_usage"] = usage
            wallet["updated_at"] = time.time()
            _save_topup_wallets()
            refreshed = _credit_state_for_user(user, effective_plan, billing_active, is_admin=False)
            refreshed["credits_needed"] = required_credits
            return True, "monthly", refreshed
        topup = int(wallet.get("animated_topup_credits", wallet.get("topup_credits", 0)) or 0)
        if topup >= required_credits:
            wallet["animated_topup_credits"] = topup - required_credits
            wallet["topup_credits"] = wallet["animated_topup_credits"]
            wallet["updated_at"] = time.time()
            _save_topup_wallets()
            refreshed = _credit_state_for_user(user, effective_plan, billing_active, is_admin=False)
            refreshed["credits_needed"] = required_credits
            return True, "topup", refreshed
        state["credits_needed"] = required_credits
        return False, "topup_required", state


async def _refund_generation_credit(user_id: str, source: str, month_key: str = "", credits: int = 1) -> None:
    if not user_id or source not in {"monthly", "topup"}:
        return
    credit_amount = max(1, int(credits or 1))
    async with _topup_wallet_lock:
        wallet = _wallet_for_user(user_id)
        if source == "topup":
            wallet["animated_topup_credits"] = int(wallet.get("animated_topup_credits", wallet.get("topup_credits", 0)) or 0) + credit_amount
            wallet["topup_credits"] = wallet["animated_topup_credits"]
        else:
            mk = month_key or _month_key()
            usage = dict(wallet.get("monthly_usage", {}) or {})
            usage[mk] = max(0, int(usage.get(mk, 0) or 0) - credit_amount)
            wallet["monthly_usage"] = usage
        wallet["updated_at"] = time.time()
        _save_topup_wallets()


def _append_usage_ledger(event: dict) -> None:
    try:
        USAGE_LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
        with USAGE_LEDGER_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=True) + "\n")
    except Exception:
        pass


async def _credit_topup_wallet(user_id: str, credits: int, source: str, stripe_session_id: str = "") -> None:
    if not user_id or credits <= 0:
        return
    async with _topup_wallet_lock:
        wallet = _wallet_for_user(user_id)
        wallet["animated_topup_credits"] = int(wallet.get("animated_topup_credits", wallet.get("topup_credits", 0)) or 0) + int(credits)
        wallet["topup_credits"] = wallet["animated_topup_credits"]
        wallet["updated_at"] = time.time()
        _save_topup_wallets()
    _append_usage_ledger({
        "type": "topup_credit",
        "user_id": user_id,
        "credits": int(credits),
        "source": source,
        "stripe_session_id": stripe_session_id,
        "ts": time.time(),
    })


def _estimate_job_cost_usd(job_state: dict) -> float:
    template = str(job_state.get("template", "") or "")
    mode = str(job_state.get("generation_mode", "video") or "video")
    resolution = str(job_state.get("resolution", "720p") or "720p")
    total_scenes = int(job_state.get("total_scenes", 0) or 0)
    if total_scenes <= 0 and isinstance(job_state.get("scene_assets"), list):
        total_scenes = len(job_state.get("scene_assets", []))
    total_scenes = max(total_scenes, 1)
    image_scene_cost = 0.03 if template == "skeleton" else 0.04
    video_scene_cost = 0.16 if resolution == "720p" else 0.25
    per_scene = image_scene_cost + (video_scene_cost if mode == "video" else 0.0)
    return round(total_scenes * per_scene, 3)


def _stripe_find_customer_id_by_email(email: str) -> str:
    """Best-effort Stripe customer lookup for billing portal/checkout continuity."""
    if not email:
        return ""
    try:
        customers = stripe_lib.Customer.list(email=email, limit=10)
        data = list(getattr(customers, "data", []) or [])
        if not data:
            return ""
        data.sort(key=lambda c: int(getattr(c, "created", 0) or 0), reverse=True)
        return str(data[0].id)
    except Exception as e:
        log.warning(f"Stripe customer lookup failed for {email}: {e}")
        return ""


def _stripe_value(obj, key: str, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _to_unix(value) -> int:
    try:
        if value is None:
            return 0
        return int(float(value))
    except Exception:
        return 0


def _add_months_utc(anchor_unix: int, months: int) -> int:
    anchor = int(anchor_unix or 0)
    m = max(1, int(months or 1))
    if anchor <= 0:
        return 0
    dt = datetime.fromtimestamp(anchor, tz=timezone.utc)
    month_index = (dt.month - 1) + m
    year = dt.year + (month_index // 12)
    month = (month_index % 12) + 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    shifted = dt.replace(year=year, month=month, day=day)
    return int(shifted.timestamp())


def _next_renewal_from_anchor(anchor_unix: int, months: int, now_unix: int | None = None) -> int:
    """Roll an anchor forward by billing intervals until it is in the future."""
    anchor = int(anchor_unix or 0)
    step_months = max(1, int(months or 1))
    if anchor <= 0:
        return 0
    now_ts = int(now_unix or time.time())
    candidate = _add_months_utc(anchor, step_months)
    if candidate <= 0:
        return 0
    for _ in range(240):
        if candidate > now_ts:
            return int(candidate)
        nxt = _add_months_utc(candidate, step_months)
        if nxt <= candidate:
            break
        candidate = nxt
    return int(candidate if candidate > now_ts else 0)


def _subscription_interval_months(sub) -> int:
    try:
        items = _stripe_value(sub, "items", {}) or {}
        item_data = _stripe_value(items, "data", []) or []
        first_item = item_data[0] if item_data else {}
        price = _stripe_value(first_item, "price", {}) or {}
        recurring = _stripe_value(price, "recurring", {}) or {}
        interval = str(_stripe_value(recurring, "interval", "month") or "month").strip().lower()
        interval_count = max(1, _to_unix(_stripe_value(recurring, "interval_count", 1) or 1))
        if interval == "year":
            return interval_count * 12
        if interval == "month":
            return interval_count
        return 1
    except Exception:
        return 1


def _invoice_period_end_unix(invoice_obj) -> int:
    lines = _stripe_value(invoice_obj, "lines", {}) or {}
    line_data = _stripe_value(lines, "data", []) or []
    if not line_data:
        return 0
    period = _stripe_value(line_data[0], "period", {}) or {}
    return _to_unix(_stripe_value(period, "end", 0) or 0)


def _invoice_paid_at_unix(invoice_obj) -> int:
    transitions = _stripe_value(invoice_obj, "status_transitions", {}) or {}
    paid_at = _to_unix(_stripe_value(transitions, "paid_at", 0) or 0)
    if paid_at > 0:
        return paid_at
    return _to_unix(_stripe_value(invoice_obj, "created", 0) or 0)


def _stripe_subscription_snapshot(email: str) -> dict:
    """Best-effort Stripe snapshot used by admin billing audit."""
    out = {
        "ok": False,
        "plan": "",
        "status": "",
        "next_renewal_unix": 0,
        "next_renewal_source": "",
        "cancel_at_period_end": False,
        "paid_at_unix": 0,
        "interval_months": 1,
    }
    if not STRIPE_SECRET_KEY or not email:
        return out
    customer_id = _stripe_find_customer_id_by_email(email)
    if not customer_id:
        return out
    try:
        subs = stripe_lib.Subscription.list(
            customer=customer_id,
            status="all",
            limit=20,
            expand=["data.latest_invoice", "data.items.data.price"],
        )
        ranked = list(_stripe_value(subs, "data", []) or [])
        if not ranked:
            return out
        status_rank = {"active": 0, "trialing": 1, "past_due": 2, "incomplete": 3, "canceled": 4}
        ranked.sort(
            key=lambda s: (
                status_rank.get(str(_stripe_value(s, "status", "") or "").strip().lower(), 99),
                -_to_unix(_stripe_value(s, "created", 0) or 0),
            )
        )
        chosen = ranked[0]
        items = _stripe_value(chosen, "items", {}) or {}
        item_data = _stripe_value(items, "data", []) or []
        first_item = item_data[0] if item_data else {}
        first_price = _stripe_value(first_item, "price", {}) or {}
        active_price_id = str(_stripe_value(first_price, "id", "") or "")
        interval_months = _subscription_interval_months(chosen)
        status = str(_stripe_value(chosen, "status", "") or "")
        cancel_at_period_end = bool(_stripe_value(chosen, "cancel_at_period_end", False))
        current_period_end = _to_unix(_stripe_value(chosen, "current_period_end", 0) or 0)
        current_period_start = _to_unix(_stripe_value(chosen, "current_period_start", 0) or 0)
        billing_cycle_anchor = _to_unix(_stripe_value(chosen, "billing_cycle_anchor", 0) or 0)
        start_date = _to_unix(_stripe_value(chosen, "start_date", 0) or 0)
        trial_end = _to_unix(_stripe_value(chosen, "trial_end", 0) or 0)
        created_unix = _to_unix(_stripe_value(chosen, "created", 0) or 0)
        paid_at_unix = current_period_start or billing_cycle_anchor or start_date or created_unix
        next_renewal_unix = current_period_end
        next_renewal_source = "current_period_end" if next_renewal_unix > 0 else ""

        latest_invoice = _stripe_value(chosen, "latest_invoice", None)
        invoice_obj = None
        if latest_invoice:
            if isinstance(latest_invoice, str):
                try:
                    invoice_obj = stripe_lib.Invoice.retrieve(latest_invoice)
                except Exception:
                    invoice_obj = None
            else:
                invoice_obj = latest_invoice
        if invoice_obj:
            invoice_paid_at = _invoice_paid_at_unix(invoice_obj)
            if invoice_paid_at > 0:
                paid_at_unix = invoice_paid_at
            if next_renewal_unix <= 0:
                invoice_period_end = _invoice_period_end_unix(invoice_obj)
                if invoice_period_end > 0:
                    next_renewal_unix = invoice_period_end
                    next_renewal_source = "invoice_period_end"
                elif invoice_paid_at > 0:
                    rolled = _next_renewal_from_anchor(invoice_paid_at, interval_months)
                    if rolled > 0:
                        next_renewal_unix = rolled
                        next_renewal_source = "invoice_paid_at_rollforward"
        if next_renewal_unix <= 0 and trial_end > 0 and status == "trialing":
            next_renewal_unix = trial_end
            next_renewal_source = "trial_end"
        if next_renewal_unix <= 0 and not cancel_at_period_end:
            anchor = billing_cycle_anchor or current_period_start or paid_at_unix or start_date or created_unix
            if anchor > 0:
                rolled = _next_renewal_from_anchor(anchor, interval_months)
                if rolled > 0:
                    next_renewal_unix = rolled
                    next_renewal_source = "anchor_rollforward"

        out["ok"] = True
        out["plan"] = str(STRIPE_PRICE_TO_PLAN.get(active_price_id, "") or "").strip().lower()
        out["status"] = status
        out["cancel_at_period_end"] = cancel_at_period_end
        out["paid_at_unix"] = int(paid_at_unix or 0)
        out["next_renewal_unix"] = int(next_renewal_unix or 0)
        out["next_renewal_source"] = next_renewal_source
        out["interval_months"] = max(1, int(interval_months or 1))
        return out
    except Exception as e:
        log.warning(f"Stripe subscription snapshot failed for {email}: {e}")
        return out


async def _supabase_find_user_id_by_email(email: str) -> str:
    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not svc_key or not SUPABASE_URL or not email:
        return ""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            users_resp = await client.get(
                f"{SUPABASE_URL}/auth/v1/admin/users?per_page=500",
                headers={
                    "apikey": svc_key,
                    "Authorization": f"Bearer {svc_key}",
                },
            )
            if users_resp.status_code != 200:
                return ""
            users_data = users_resp.json()
            user_list = users_data.get("users", users_data) if isinstance(users_data, dict) else users_data
            for u in user_list or []:
                if str(u.get("email", "")).strip().lower() == email.strip().lower():
                    return str(u.get("id", ""))
    except Exception as e:
        log.warning(f"Supabase user lookup failed for {email}: {e}")
    return ""


async def _supabase_set_user_plan(user_id: str, plan: str):
    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not svc_key or not SUPABASE_URL or not user_id:
        return
    async with httpx.AsyncClient(timeout=15) as client:
        await client.post(
            f"{SUPABASE_URL}/rest/v1/profiles",
            headers={
                "apikey": svc_key,
                "Authorization": f"Bearer {svc_key}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates",
            },
            json={"id": user_id, "plan": plan},
        )


async def _supabase_get_waitlist_rows(limit: int = 2000) -> list[dict]:
    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not svc_key or not SUPABASE_URL:
        return []
    fallback_table = "app_settings"
    fallback_prefix = "studio_waitlist_reservation:"

    def _waitlist_missing(resp: httpx.Response) -> bool:
        if resp.status_code == 404:
            return True
        try:
            body = resp.json()
        except Exception:
            body = {}
        text = json.dumps(body).lower() if isinstance(body, (dict, list)) else str(body).lower()
        return "could not find the table" in text and "waiting_list" in text

    def _parse_fallback_value(raw: object) -> dict:
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                return {}
        return {}

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/waiting_list?select=*&order=created_at.desc&limit={int(max(1, limit))}",
                headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
            )
            if resp.status_code == 200:
                rows = resp.json()
                return rows if isinstance(rows, list) else []

            if not _waitlist_missing(resp):
                return []

            fallback_resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/{fallback_table}?select=id,key,value,updated_at&key=like.{quote(fallback_prefix + '%')}&order=updated_at.desc&limit={int(max(1, limit))}",
                headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
            )
            if fallback_resp.status_code != 200:
                return []
            raw_rows = fallback_resp.json()
            raw_rows = raw_rows if isinstance(raw_rows, list) else []
            rows: list[dict] = []
            for item in raw_rows:
                if not isinstance(item, dict):
                    continue
                parsed = _parse_fallback_value(item.get("value"))
                key = str(item.get("key", "") or "")
                email = str(parsed.get("email", "") or "").strip().lower()
                if not email and key.startswith(fallback_prefix):
                    email = key[len(fallback_prefix):].strip().lower()
                if not email:
                    continue
                rows.append(
                    {
                        "id": str(item.get("id", "") or ""),
                        "email": email,
                        "plan": str(parsed.get("plan", "starter") or "starter").strip().lower(),
                        "price_usd": float(parsed.get("price_usd", 0.0) or 0.0),
                        "paid": bool(parsed.get("paid", False)),
                        "stripe_session_id": parsed.get("stripe_session_id"),
                        "created_at": str(parsed.get("created_at", item.get("updated_at", "")) or ""),
                    }
                )
            rows.sort(key=lambda r: str(r.get("created_at", "") or ""), reverse=True)
            return rows
    except Exception as e:
        log.warning(f"Supabase waiting_list read failed: {e}")
        return []


async def _supabase_upsert_waitlist_entry(
    *,
    email: str,
    plan: str,
    price_usd: float,
    paid: bool,
) -> bool:
    svc_key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not svc_key or not SUPABASE_URL or not email:
        return False
    payload = {
        "email": str(email or "").strip().lower(),
        "plan": str(plan or "").strip().lower(),
        "price_usd": float(price_usd or 0.0),
        "paid": bool(paid),
    }
    fallback_table = "app_settings"
    fallback_prefix = "studio_waitlist_reservation:"

    def _waitlist_missing(resp: httpx.Response) -> bool:
        if resp.status_code == 404:
            return True
        try:
            body = resp.json()
        except Exception:
            body = {}
        text = json.dumps(body).lower() if isinstance(body, (dict, list)) else str(body).lower()
        return "could not find the table" in text and "waiting_list" in text

    def _parse_fallback_value(raw: object) -> dict:
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                return {}
        return {}

    async def _fallback_upsert(client: httpx.AsyncClient) -> bool:
        key = f"{fallback_prefix}{payload['email']}"
        existing = await client.get(
            f"{SUPABASE_URL}/rest/v1/{fallback_table}?key=eq.{quote(key)}&select=id,value&limit=1",
            headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
        )
        existing_rows = existing.json() if existing.status_code == 200 else []
        existing_rows = existing_rows if isinstance(existing_rows, list) else []
        value = {
            "email": payload["email"],
            "plan": payload["plan"],
            "price_usd": payload["price_usd"],
            "paid": payload["paid"],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        if existing_rows:
            prev = _parse_fallback_value(existing_rows[0].get("value"))
            value["created_at"] = str(prev.get("created_at", value["created_at"]) or value["created_at"])
            value["paid"] = bool(prev.get("paid")) or value["paid"]
            update = await client.patch(
                f"{SUPABASE_URL}/rest/v1/{fallback_table}?id=eq.{quote(str(existing_rows[0].get('id', '') or ''))}",
                headers={
                    "apikey": svc_key,
                    "Authorization": f"Bearer {svc_key}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal",
                },
                json={"value": value},
            )
            return update.status_code in {200, 204}
        insert = await client.post(
            f"{SUPABASE_URL}/rest/v1/{fallback_table}",
            headers={
                "apikey": svc_key,
                "Authorization": f"Bearer {svc_key}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal",
            },
            json={"key": key, "value": value},
        )
        return insert.status_code in {200, 201, 204}

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            existing_resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/waiting_list?email=eq.{quote(payload['email'])}&select=id,paid&limit=1",
                headers={"apikey": svc_key, "Authorization": f"Bearer {svc_key}"},
            )
            if _waitlist_missing(existing_resp):
                return await _fallback_upsert(client)
            existing_rows = existing_resp.json() if existing_resp.status_code == 200 else []
            existing_rows = existing_rows if isinstance(existing_rows, list) else []
            if existing_rows:
                existing_paid = bool(existing_rows[0].get("paid"))
                update_payload = dict(payload)
                if existing_paid:
                    update_payload["paid"] = True
                update_resp = await client.patch(
                    f"{SUPABASE_URL}/rest/v1/waiting_list?email=eq.{quote(payload['email'])}",
                    headers={
                        "apikey": svc_key,
                        "Authorization": f"Bearer {svc_key}",
                        "Content-Type": "application/json",
                        "Prefer": "return=minimal",
                    },
                    json=update_payload,
                )
                if _waitlist_missing(update_resp):
                    return await _fallback_upsert(client)
                return update_resp.status_code in {200, 204}
            insert_resp = await client.post(
                f"{SUPABASE_URL}/rest/v1/waiting_list",
                headers={
                    "apikey": svc_key,
                    "Authorization": f"Bearer {svc_key}",
                    "Content-Type": "application/json",
                    "Prefer": "return=minimal",
                },
                json=payload,
            )
            if _waitlist_missing(insert_resp):
                return await _fallback_upsert(client)
            return insert_resp.status_code in {200, 201, 204}
    except Exception as e:
        log.warning(f"Supabase waiting_list upsert failed for {email}: {e}")
        return False


def _record_kpi_for_job(job_id: str, job_state: dict) -> None:
    if not isinstance(job_state, dict):
        return
    if bool(job_state.get("kpi_recorded")):
        return
    status = str(job_state.get("status", "") or "").strip().lower()
    if status not in {"complete", "rendered", "error"}:
        return
    template = str(job_state.get("template", "") or "unknown").strip().lower() or "unknown"
    estimated_cost = _estimate_job_cost_usd(job_state)
    _kpi_metrics["total_jobs"] = int(_kpi_metrics.get("total_jobs", 0) or 0) + 1
    if status in {"complete", "rendered"}:
        _kpi_metrics["completed_jobs"] = int(_kpi_metrics.get("completed_jobs", 0) or 0) + 1
    else:
        _kpi_metrics["error_jobs"] = int(_kpi_metrics.get("error_jobs", 0) or 0) + 1
    if not bool(job_state.get("regenerate_count")) and status in {"complete", "rendered"}:
        _kpi_metrics["first_render_pass_jobs"] = int(_kpi_metrics.get("first_render_pass_jobs", 0) or 0) + 1
    _kpi_metrics["total_estimated_cost_usd"] = round(
        float(_kpi_metrics.get("total_estimated_cost_usd", 0.0) or 0.0) + estimated_cost,
        3,
    )
    publishable_seconds = 0.0
    try:
        publishable_seconds = float(job_state.get("completed_at", 0) or 0) - float(job_state.get("created_at", 0) or 0)
    except Exception:
        publishable_seconds = 0.0
    if publishable_seconds > 0 and status in {"complete", "rendered"}:
        _kpi_metrics["total_publishable_time_sec"] = round(
            float(_kpi_metrics.get("total_publishable_time_sec", 0.0) or 0.0) + publishable_seconds,
            3,
        )
    template_breakdown = dict(_kpi_metrics.get("template_breakdown", {}) or {})
    template_stats = dict(template_breakdown.get(template, {}) or {})
    template_stats["total"] = int(template_stats.get("total", 0) or 0) + 1
    if status in {"complete", "rendered"}:
        template_stats["completed"] = int(template_stats.get("completed", 0) or 0) + 1
    else:
        template_stats["error"] = int(template_stats.get("error", 0) or 0) + 1
    template_breakdown[template] = template_stats
    _kpi_metrics["template_breakdown"] = template_breakdown
    _kpi_metrics["updated_at"] = time.time()
    job_state["estimated_cost_usd"] = estimated_cost
    job_state["kpi_recorded"] = True
    _save_kpi_metrics()
