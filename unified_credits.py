"""Unified credit system — one wallet across OpenRouter + fal + ElevenLabs.

Replaces the fragmented animation / render-fuel / shorts wallets with a single
balance that is *debited from real provider spend*:

    credits_charged = ceil(provider_usd * (1 + CREDIT_MARGIN) / CREDIT_USD_VALUE)

Two plans (see backend_settings.UNIFIED_PLANS):
    creator : $60/mo  ->  5,000 credits  (~83 cr/$)
    studio  : $200/mo -> 20,000 credits  (100 cr/$  <- best value)

This module is intentionally self-contained (own lock + JSON persistence on the
Fly volume) so it can be adopted incrementally without touching the legacy
billing.py wallets. New surfaces (Studio Agent, unified-plan users) debit here;
legacy plans keep working until fully migrated.
"""
from __future__ import annotations

import json
import math
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend_settings import (
    CREDIT_MARGIN,
    CREDIT_USD_VALUE,
    TEMP_DIR,
    UNIFIED_PLANS,
)

WALLETS_PATH = Path(TEMP_DIR) / "unified_credit_wallets.json"
LEDGER_PATH = Path(TEMP_DIR) / "unified_credit_ledger.jsonl"

_lock = threading.RLock()
_wallets: dict[str, dict[str, Any]] = {}
_loaded = False


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
def _load() -> None:
    global _loaded
    if _loaded:
        return
    try:
        if WALLETS_PATH.exists():
            data = json.loads(WALLETS_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                _wallets.update(data)
    except Exception:
        pass
    _loaded = True


def _save() -> None:
    try:
        WALLETS_PATH.parent.mkdir(parents=True, exist_ok=True)
        WALLETS_PATH.write_text(json.dumps(_wallets, indent=2), encoding="utf-8")
    except Exception:
        pass


def _append_ledger(event: dict[str, Any]) -> None:
    try:
        LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LEDGER_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, ensure_ascii=True) + "\n")
    except Exception:
        pass


def _month_key(ts: float | None = None) -> str:
    now = datetime.fromtimestamp(ts or time.time(), tz=timezone.utc)
    return f"{now.year:04d}-{now.month:02d}"


def _wallet(user_id: str) -> dict[str, Any]:
    _load()
    w = _wallets.get(user_id)
    if not isinstance(w, dict):
        w = {
            "balance": 0,            # spendable credits (monthly grant + topups, unified)
            "plan": "",              # creator | studio | ""
            "granted_month": "",     # last month a monthly grant was applied
            "lifetime_spent": 0,
            "updated_at": time.time(),
        }
        _wallets[user_id] = w
    w.setdefault("balance", 0)
    w.setdefault("plan", "")
    w.setdefault("granted_month", "")
    w.setdefault("lifetime_spent", 0)
    return w


# ---------------------------------------------------------------------------
# Cost -> credits conversion
# ---------------------------------------------------------------------------
def usd_to_credits(provider_usd: float) -> int:
    """Convert a raw provider USD cost into billable credits (margin applied)."""
    usd = max(0.0, float(provider_usd or 0.0))
    if usd <= 0:
        return 0
    raw = usd * (1.0 + float(CREDIT_MARGIN)) / max(1e-9, float(CREDIT_USD_VALUE))
    return max(1, math.ceil(raw))


def openrouter_usd(usage: dict[str, Any], prompt_ppm: float | None, completion_ppm: float | None) -> float:
    """USD cost of an OpenRouter completion from token usage + per-million pricing."""
    pt = float((usage or {}).get("prompt_tokens", 0) or 0)
    ct = float((usage or {}).get("completion_tokens", 0) or 0)
    p = float(prompt_ppm or 0.0) / 1_000_000.0
    c = float(completion_ppm or 0.0) / 1_000_000.0
    return pt * p + ct * c


def elevenlabs_usd(characters: int, per_1k_usd: float = 0.10) -> float:
    return max(0, int(characters or 0)) / 1000.0 * float(per_1k_usd)


def fal_render_usd(
    *,
    images: int = 0,
    video_seconds: float = 0.0,
    tts_chars: int = 0,
    image_key: str = "seedream_v45",
    image_fallback: str = "seedream_v45_per_image",
    video_key: str = "kling_v21_standard",
    video_fallback: str = "kling_v21_standard_per_second",
) -> dict[str, Any]:
    """Compute fal USD from actual asset counts using live fal Platform pricing.

    Returns {usd, breakdown} so the ledger records exactly what was charged.
    Falls back to fal_pricing.FALLBACK_USD when the live API/key is unavailable.
    """
    breakdown: dict[str, float] = {}
    total = 0.0
    try:
        from long_form import fal_pricing as fp

        snap = fp.get_pricing_snapshot()
        if images > 0:
            usd, _note = fp.unit_cost(snap, image_key, fallback_key=image_fallback, quantity=float(images))
            breakdown["images"] = round(usd, 6)
            total += usd
        if video_seconds > 0:
            usd, _note = fp.unit_cost(snap, video_key, fallback_key=video_fallback, quantity=float(video_seconds))
            breakdown["video"] = round(usd, 6)
            total += usd
        if tts_chars > 0:
            rate = float(fp.FALLBACK_USD.get("elevenlabs_per_1k_chars", 0.10))
            usd = tts_chars / 1000.0 * rate
            breakdown["tts"] = round(usd, 6)
            total += usd
    except Exception as exc:
        breakdown["error"] = str(exc)[:200]
    return {"usd": round(total, 6), "breakdown": breakdown}


def debit_fal_render(
    user_id: str,
    *,
    images: int = 0,
    video_seconds: float = 0.0,
    tts_chars: int = 0,
    reason: str = "fal_render",
    metadata: dict | None = None,
) -> tuple[int, int]:
    cost = fal_render_usd(images=images, video_seconds=video_seconds, tts_chars=tts_chars)
    md = dict(metadata or {})
    md.update({"fal_breakdown": cost["breakdown"], "images": images, "video_seconds": video_seconds, "tts_chars": tts_chars})
    return debit_usd(user_id, cost["usd"], reason=reason, metadata=md, allow_negative=True)


def debit_elevenlabs(
    user_id: str,
    characters: int,
    *,
    per_1k_usd: float = 0.10,
    reason: str = "elevenlabs_tts",
    metadata: dict | None = None,
) -> tuple[int, int]:
    cost = elevenlabs_usd(characters, per_1k_usd=per_1k_usd)
    md = dict(metadata or {})
    md.update({"characters": int(characters or 0), "per_1k_usd": per_1k_usd})
    return debit_usd(user_id, cost, reason=reason, metadata=md, allow_negative=True)


# ---------------------------------------------------------------------------
# Plan grants
# ---------------------------------------------------------------------------
def plan_monthly_credits(plan: str) -> int:
    spec = UNIFIED_PLANS.get(str(plan or "").strip().lower())
    return int((spec or {}).get("monthly_credits", 0) or 0)


def set_plan(user_id: str, plan: str, *, grant_now: bool = True) -> dict[str, Any]:
    """Assign a unified plan. Optionally apply this month's credit grant."""
    plan = str(plan or "").strip().lower()
    with _lock:
        w = _wallet(user_id)
        w["plan"] = plan
        if grant_now:
            _grant_monthly_locked(user_id, plan)
        w["updated_at"] = time.time()
        _save()
        return dict(w)


def _grant_monthly_locked(user_id: str, plan: str) -> bool:
    w = _wallet(user_id)
    mk = _month_key()
    if w.get("granted_month") == mk:
        return False
    credits = plan_monthly_credits(plan)
    if credits <= 0:
        return False
    w["balance"] = int(w.get("balance", 0) or 0) + credits
    w["granted_month"] = mk
    w["updated_at"] = time.time()
    _append_ledger({
        "type": "monthly_grant",
        "user_id": user_id,
        "plan": plan,
        "credits": credits,
        "month": mk,
        "balance_after": w["balance"],
        "ts": time.time(),
    })
    return True


def ensure_monthly_grant(user_id: str) -> dict[str, Any]:
    """Idempotently apply the current month's grant for the user's plan."""
    with _lock:
        w = _wallet(user_id)
        if w.get("plan"):
            if _grant_monthly_locked(user_id, w["plan"]):
                _save()
        return dict(w)


def add_credits(user_id: str, credits: int, *, reason: str = "topup", metadata: dict | None = None) -> dict[str, Any]:
    credits = int(credits or 0)
    with _lock:
        w = _wallet(user_id)
        w["balance"] = int(w.get("balance", 0) or 0) + max(0, credits)
        w["updated_at"] = time.time()
        _save()
        _append_ledger({
            "type": "credit",
            "user_id": user_id,
            "credits": credits,
            "reason": reason,
            "metadata": metadata or {},
            "balance_after": w["balance"],
            "ts": time.time(),
        })
        return dict(w)


# ---------------------------------------------------------------------------
# Balance + debit
# ---------------------------------------------------------------------------
def get_balance(user_id: str) -> int:
    with _lock:
        return int(_wallet(user_id).get("balance", 0) or 0)


def get_state(user_id: str) -> dict[str, Any]:
    with _lock:
        w = _wallet(user_id)
        plan = str(w.get("plan") or "")
        spec = UNIFIED_PLANS.get(plan, {})
        return {
            "balance": int(w.get("balance", 0) or 0),
            "plan": plan,
            "plan_name": spec.get("name") or "",
            "monthly_credits": int(spec.get("monthly_credits", 0) or 0),
            "lifetime_spent": int(w.get("lifetime_spent", 0) or 0),
            "month": _month_key(),
        }


def can_afford(user_id: str, credits: int) -> bool:
    return get_balance(user_id) >= max(0, int(credits or 0))


def debit_credits(
    user_id: str,
    credits: int,
    *,
    reason: str,
    metadata: dict | None = None,
    allow_negative: bool = False,
) -> tuple[bool, int]:
    """Debit credits. Returns (ok, balance_after).

    If insufficient and not allow_negative, no debit occurs and ok=False.
    `allow_negative` is for metered post-paid usage where we never want to drop a
    completed render — it clamps the wallet at 0 and records the shortfall.
    """
    credits = max(0, int(credits or 0))
    if credits == 0:
        return True, get_balance(user_id)
    with _lock:
        w = _wallet(user_id)
        bal = int(w.get("balance", 0) or 0)
        if bal < credits and not allow_negative:
            return False, bal
        new_bal = max(0, bal - credits)
        w["balance"] = new_bal
        w["lifetime_spent"] = int(w.get("lifetime_spent", 0) or 0) + credits
        w["updated_at"] = time.time()
        _save()
        _append_ledger({
            "type": "debit",
            "user_id": user_id,
            "credits": -credits,
            "reason": reason,
            "metadata": metadata or {},
            "balance_after": new_bal,
            "shortfall": max(0, credits - bal),
            "ts": time.time(),
        })
        return True, new_bal


def debit_usd(
    user_id: str,
    provider_usd: float,
    *,
    reason: str,
    metadata: dict | None = None,
    allow_negative: bool = True,
) -> tuple[int, int]:
    """Convert provider USD -> credits and debit. Returns (credits_charged, balance_after)."""
    credits = usd_to_credits(provider_usd)
    md = dict(metadata or {})
    md["provider_usd"] = round(float(provider_usd or 0.0), 6)
    md["credit_usd_value"] = float(CREDIT_USD_VALUE)
    md["credit_margin"] = float(CREDIT_MARGIN)
    _ok, bal = debit_credits(user_id, credits, reason=reason, metadata=md, allow_negative=allow_negative)
    return credits, bal


def recent_ledger(user_id: str = "", limit: int = 50) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        if not LEDGER_PATH.exists():
            return []
        with LEDGER_PATH.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    evt = json.loads(line)
                except Exception:
                    continue
                if user_id and evt.get("user_id") != user_id:
                    continue
                rows.append(evt)
    except Exception:
        return rows[-limit:]
    return rows[-limit:]
