"""Unified credit system — one wallet across OpenRouter + fal + ElevenLabs.

Replaces the fragmented animation / render-fuel / shorts wallets with a single
balance that is *debited from real provider spend*:

    credits_charged = ceil(provider_usd * (1 + CREDIT_MARGIN) / CREDIT_USD_VALUE)

Plans are defined in backend_settings.UNIFIED_PLANS. The public Studio Pro
ladder is one plan family with selectable monthly credit grants; legacy plan IDs
remain supported for existing subscribers and webhook compatibility.

This module is intentionally self-contained (own lock + JSON persistence on the
Fly volume) so it can be adopted incrementally without touching the legacy
billing.py wallets. New surfaces (Studio Agent, unified-plan users) debit here;
legacy plans keep working until fully migrated.
"""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_HALF_UP, InvalidOperation
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
PENDING_GRANTS_PATH = Path(TEMP_DIR) / "pending_credit_grants.json"

_lock = threading.RLock()
_wallets: dict[str, dict[str, Any]] = {}
_loaded = False
USD_QUANT = Decimal("0.000001")


def _fsync_parent(path: Path) -> None:
    """Best-effort directory sync after replace (supported on POSIX)."""
    if os.name == "nt":
        return
    try:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        fd = os.open(str(path.parent), flags)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def _atomic_write_json(path: Path, payload: Any) -> None:
    """Write a complete JSON snapshot without exposing a partial file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        _fsync_parent(path)
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass


def _runpod_worker_mode() -> bool:
    return str(os.getenv("STUDIO_RUNPOD_WORKER_MODE") or "").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _runpod_dispatch_id() -> str:
    raw = str(os.getenv("STUDIO_RUNPOD_DISPATCH_ID") or "").strip()
    return raw if raw.startswith("rpd_") and raw[4:].isalnum() else "unattributed"


def _worker_cost_fact_path(dispatch_id: str | None = None) -> Path:
    root = Path(str(os.getenv("APP_DATA_DIR") or TEMP_DIR)).expanduser()
    safe_id = str(dispatch_id or _runpod_dispatch_id()).strip()
    if not (safe_id.startswith("rpd_") and safe_id[4:].isalnum()):
        safe_id = "unattributed"
    return root / "runpod_worker_cost_facts" / f"{safe_id}.jsonl"


def _record_worker_cost_fact(kind: str, **payload: Any) -> dict[str, Any]:
    """Persist provider-cost facts, never a wallet mutation, on RunPod workers."""

    fact = {
        "kind": str(kind or "provider_cost"),
        "dispatch_id": _runpod_dispatch_id(),
        "ts": time.time(),
        **payload,
    }
    path = _worker_cost_fact_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with _lock:
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(fact, ensure_ascii=False, default=str) + "\n")
                fh.flush()
    except OSError:
        pass
    return fact


def get_runpod_worker_cost_facts(dispatch_id: str = "") -> list[dict[str, Any]]:
    """Read deferred billing facts for control-plane reconciliation."""

    path = _worker_cost_fact_path(dispatch_id or None)
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    row = json.loads(line)
                except (TypeError, ValueError):
                    continue
                if isinstance(row, dict):
                    rows.append(row)
    except OSError:
        pass
    return rows


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
def _load() -> None:
    global _loaded
    if _runpod_worker_mode():
        _loaded = True
        return
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
    if _runpod_worker_mode():
        return
    try:
        _atomic_write_json(WALLETS_PATH, _wallets)
    except Exception:
        # A failed replace must not leave an in-memory mutation that was never
        # made durable. Roll back to the last complete snapshot before raising.
        _wallets.clear()
        try:
            if WALLETS_PATH.is_file():
                data = json.loads(WALLETS_PATH.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    _wallets.update(data)
        finally:
            raise


def _append_ledger(event: dict[str, Any]) -> None:
    if _runpod_worker_mode():
        return
    try:
        LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LEDGER_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, ensure_ascii=True) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
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
            "balance": 0,            # derived compatibility field
            "monthly_balance": 0,
            "rollover_balance": 0,
            "topup_balance": 0,
            "plan": "",              # creator | studio | ""
            "granted_month": "",     # last month a monthly grant was applied
            "granted_credits": 0,
            "lifetime_spent": 0,
            "unlimited": False,
            "beta_access": False,
            "reservations": {},
            "processed_events": [],
            "processed_adjustments": {},
            "updated_at": time.time(),
        }
        _wallets[user_id] = w
    # Preserve pre-launch balances as non-expiring purchased credit. This avoids
    # taking value away from early users when migrating from the flat wallet.
    if not any(k in w for k in ("monthly_balance", "rollover_balance", "topup_balance")):
        w["topup_balance"] = max(0, int(w.get("balance", 0) or 0))
    w.setdefault("balance", 0)
    w.setdefault("monthly_balance", 0)
    w.setdefault("rollover_balance", 0)
    w.setdefault("topup_balance", 0)
    w.setdefault("plan", "")
    w.setdefault("granted_month", "")
    w.setdefault("granted_credits", 0)
    w.setdefault("lifetime_spent", 0)
    w.setdefault("unlimited", False)
    w.setdefault("beta_access", False)
    w.setdefault("reservations", {})
    w.setdefault("processed_events", [])
    w.setdefault("processed_adjustments", {})
    _sync_balance(w)
    return w


def _sync_balance(w: dict[str, Any]) -> int:
    total = (
        max(0, int(w.get("rollover_balance", 0) or 0))
        + max(0, int(w.get("monthly_balance", 0) or 0))
        + max(0, int(w.get("topup_balance", 0) or 0))
    )
    w["balance"] = total
    return total


def _consume_locked(w: dict[str, Any], credits: int) -> dict[str, int] | None:
    required = max(0, int(credits or 0))
    if required <= 0:
        return {"rollover_balance": 0, "monthly_balance": 0, "topup_balance": 0}
    if _sync_balance(w) < required:
        return None
    consumed = {"rollover_balance": 0, "monthly_balance": 0, "topup_balance": 0}
    # Spend expiring credits first, then the current grant, then purchased reloads.
    for bucket in ("rollover_balance", "monthly_balance", "topup_balance"):
        available = max(0, int(w.get(bucket, 0) or 0))
        take = min(required, available)
        if take:
            w[bucket] = available - take
            consumed[bucket] = take
            required -= take
        if required <= 0:
            break
    _sync_balance(w)
    return consumed


def _restore_locked(w: dict[str, Any], buckets: dict[str, Any]) -> None:
    for bucket in ("rollover_balance", "monthly_balance", "topup_balance"):
        w[bucket] = max(0, int(w.get(bucket, 0) or 0)) + max(0, int(buckets.get(bucket, 0) or 0))
    _sync_balance(w)


# ---------------------------------------------------------------------------
# Cost -> credits conversion
# ---------------------------------------------------------------------------
def _decimal(value: Any, default: str = "0") -> Decimal:
    try:
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value if value is not None else default))
    except (InvalidOperation, ValueError):
        return Decimal(default)


def _usd_decimal(value: Any) -> Decimal:
    usd = _decimal(value)
    if usd <= 0:
        return Decimal("0")
    return usd.quantize(USD_QUANT, rounding=ROUND_HALF_UP)


def _usd_float(value: Any) -> float:
    return float(_usd_decimal(value))


def usd_to_credits(provider_usd: float | Decimal | str) -> int:
    """Convert a raw provider USD cost into billable credits (margin applied)."""
    usd = _usd_decimal(provider_usd)
    if usd <= 0:
        return 0
    credit_value = max(_decimal(CREDIT_USD_VALUE), Decimal("0.000000001"))
    margin_multiplier = Decimal("1") + max(_decimal(CREDIT_MARGIN), Decimal("0"))
    raw = (usd * margin_multiplier) / credit_value
    return max(1, int(raw.to_integral_value(rounding=ROUND_CEILING)))


def openrouter_usd(usage: dict[str, Any], prompt_ppm: float | None, completion_ppm: float | None) -> Decimal:
    """USD cost of an OpenRouter completion from token usage + per-million pricing."""
    pt = max(Decimal("0"), _decimal((usage or {}).get("prompt_tokens", 0)))
    ct = max(Decimal("0"), _decimal((usage or {}).get("completion_tokens", 0)))
    p = max(Decimal("0"), _decimal(prompt_ppm)) / Decimal("1000000")
    c = max(Decimal("0"), _decimal(completion_ppm)) / Decimal("1000000")
    return _usd_decimal((pt * p) + (ct * c))


def elevenlabs_usd(characters: int, per_1k_usd: float = 0.10) -> Decimal:
    chars = Decimal(max(0, int(characters or 0)))
    return _usd_decimal((chars / Decimal("1000")) * max(Decimal("0"), _decimal(per_1k_usd)))


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
    breakdown: dict[str, float | str] = {}
    breakdown_exact: dict[str, str] = {}
    total = Decimal("0")
    try:
        from long_form import fal_pricing as fp

        snap = fp.get_pricing_snapshot()
        if images > 0:
            usd, _note = fp.unit_cost(snap, image_key, fallback_key=image_fallback, quantity=float(images))
            amount = _usd_decimal(usd)
            breakdown["images"] = _usd_float(amount)
            breakdown_exact["images"] = str(amount)
            total += amount
        if video_seconds > 0:
            usd, _note = fp.unit_cost(snap, video_key, fallback_key=video_fallback, quantity=float(video_seconds))
            amount = _usd_decimal(usd)
            breakdown["video"] = _usd_float(amount)
            breakdown_exact["video"] = str(amount)
            total += amount
        if tts_chars > 0:
            rate = _decimal(fp.FALLBACK_USD.get("elevenlabs_per_1k_chars", 0.10))
            amount = _usd_decimal((Decimal(max(0, int(tts_chars))) / Decimal("1000")) * rate)
            breakdown["tts"] = _usd_float(amount)
            breakdown_exact["tts"] = str(amount)
            total += amount
    except Exception as exc:
        breakdown["error"] = str(exc)[:200]
    total = _usd_decimal(total)
    return {"usd": _usd_float(total), "usd_decimal": str(total), "breakdown": breakdown, "breakdown_decimal": breakdown_exact}


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
    md.update({
        "fal_breakdown": cost["breakdown"],
        "fal_breakdown_decimal": cost.get("breakdown_decimal", {}),
        "images": images,
        "video_seconds": video_seconds,
        "tts_chars": tts_chars,
    })
    return debit_usd(user_id, cost.get("usd_decimal", cost["usd"]), reason=reason, metadata=md, allow_negative=False)


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
    return debit_usd(user_id, cost, reason=reason, metadata=md, allow_negative=False)


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
    credits = plan_monthly_credits(plan)
    if credits <= 0:
        return False
    if w.get("granted_month") == mk:
        prior_grant = max(0, int(w.get("granted_credits", 0) or 0))
        upgrade_delta = max(0, credits - prior_grant)
        if upgrade_delta <= 0:
            return False
        w["monthly_balance"] = int(w.get("monthly_balance", 0) or 0) + upgrade_delta
        w["granted_credits"] = credits
        w["updated_at"] = time.time()
        _sync_balance(w)
        _append_ledger({
            "type": "plan_upgrade_grant",
            "user_id": user_id,
            "plan": plan,
            "credits": upgrade_delta,
            "month": mk,
            "balance_after": w["balance"],
            "ts": time.time(),
        })
        return True
    # Only the immediately previous month's unused grant can roll forward.
    # Any older rollover expires here. Purchased reloads never expire.
    w["rollover_balance"] = min(
        max(0, int(w.get("monthly_balance", 0) or 0)),
        credits,
    )
    w["monthly_balance"] = credits
    w["granted_month"] = mk
    w["granted_credits"] = credits
    w["updated_at"] = time.time()
    _sync_balance(w)
    _append_ledger({
        "type": "monthly_grant",
        "user_id": user_id,
        "plan": plan,
        "credits": credits,
        "rollover_credits": int(w.get("rollover_balance", 0) or 0),
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


def add_credits(
    user_id: str,
    credits: int,
    *,
    reason: str = "topup",
    metadata: dict | None = None,
    idempotency_key: str = "",
) -> dict[str, Any]:
    credits = int(credits or 0)
    with _lock:
        w = _wallet(user_id)
        key = str(idempotency_key or "").strip()
        processed = list(w.get("processed_events") or [])
        if key and key in processed:
            return dict(w)
        w["topup_balance"] = int(w.get("topup_balance", 0) or 0) + max(0, credits)
        _sync_balance(w)
        if key:
            processed.append(key)
            w["processed_events"] = processed[-5000:]
        w["updated_at"] = time.time()
        _save()
        _append_ledger({
            "type": "credit",
            "user_id": user_id,
            "credits": credits,
            "reason": reason,
            "metadata": metadata or {},
            "idempotency_key": key,
            "balance_after": w["balance"],
            "ts": time.time(),
        })
        return dict(w)


def register_pending_grant(
    email: str,
    target_balance: int,
    *,
    reason: str,
    idempotency_key: str,
    beta_access: bool = False,
) -> dict[str, Any]:
    """Persist a first-login credit grant without creating an auth account."""
    normalized = str(email or "").strip().lower()
    target = max(0, int(target_balance or 0))
    key = str(idempotency_key or "").strip()
    if not normalized or "@" not in normalized or target <= 0 or not key:
        raise ValueError("valid email, target balance, and idempotency key are required")
    with _lock:
        try:
            payload = json.loads(PENDING_GRANTS_PATH.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        row = {
            "email": normalized,
            "target_balance": target,
            "reason": str(reason or "pending_grant")[:120],
            "idempotency_key": key,
            "beta_access": bool(beta_access),
            "created_at": time.time(),
        }
        payload[normalized] = row
        _atomic_write_json(PENDING_GRANTS_PATH, payload)
        return row


def claim_pending_grant(user_id: str, email: str) -> dict[str, Any] | None:
    """Apply and remove an email grant exactly once after verified authentication."""
    uid = str(user_id or "").strip()
    normalized = str(email or "").strip().lower()
    if not uid or not normalized:
        return None
    with _lock:
        try:
            payload = json.loads(PENDING_GRANTS_PATH.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return None
        if not isinstance(payload, dict) or not isinstance(payload.get(normalized), dict):
            return None
        row = dict(payload[normalized])
        target = max(0, int(row.get("target_balance", 0) or 0))
        current = get_balance(uid)
        amount = max(0, target - current)
        if amount > 0:
            wallet = add_credits(
                uid,
                amount,
                reason=str(row.get("reason") or "pending_grant"),
                metadata={"email": normalized, "source": "first_login_pending_grant"},
                idempotency_key=str(row.get("idempotency_key") or ""),
            )
        else:
            wallet = get_state(uid)
        if bool(row.get("beta_access")):
            wallet = set_beta_access(uid, True, reason="pending_beta_grant_claimed")
        payload.pop(normalized, None)
        _atomic_write_json(PENDING_GRANTS_PATH, payload)
        return {"credits_added": amount, "target_balance": target, "balance": wallet.get("balance", current)}


def set_beta_access(user_id: str, enabled: bool = True, *, reason: str = "controlled_beta") -> dict[str, Any]:
    """Grant product-lane access independently from paid membership state."""
    uid = str(user_id or "").strip()
    if not uid:
        return {}
    with _lock:
        w = _wallet(uid)
        if bool(w.get("beta_access")) == bool(enabled):
            return dict(w)
        w["beta_access"] = bool(enabled)
        w["updated_at"] = time.time()
        _save()
        _append_ledger({
            "type": "beta_access_changed",
            "user_id": uid,
            "enabled": bool(enabled),
            "reason": str(reason or "controlled_beta")[:120],
            "balance_after": w["balance"],
            "ts": time.time(),
        })
        return dict(w)


def remove_topup_credits(
    user_id: str,
    credits: int,
    *,
    reason: str = "topup_reversal",
    metadata: dict | None = None,
    idempotency_key: str,
) -> int:
    """Idempotently reverse purchased credits, clamped to that bucket.

    Refunds must not consume monthly or rollover entitlements when the
    purchased balance has already been spent. The persisted adjustment record
    preserves the original amount removed so crash retries remain observable
    and cannot debit the same purchase twice.
    """
    uid = str(user_id or "").strip()
    requested = max(0, int(credits or 0))
    key = str(idempotency_key or "").strip()
    if not uid or requested <= 0 or not key:
        return 0
    with _lock:
        w = _wallet(uid)
        adjustments = w.get("processed_adjustments")
        if not isinstance(adjustments, dict):
            adjustments = {}
        prior = adjustments.get(key)
        if isinstance(prior, dict):
            return max(0, int(prior.get("credits_removed", 0) or 0))
        current = max(0, int(w.get("topup_balance", 0) or 0))
        removed = min(current, requested)
        w["topup_balance"] = current - removed
        _sync_balance(w)
        adjustments[key] = {
            "credits_removed": removed,
            "credits_requested": requested,
            "reason": str(reason or "topup_reversal")[:120],
            "recorded_at": time.time(),
        }
        w["processed_adjustments"] = dict(list(adjustments.items())[-5000:])
        w["updated_at"] = time.time()
        _save()
        _append_ledger({
            "type": "topup_reversal",
            "user_id": uid,
            "credits": -removed,
            "credits_requested": requested,
            "reason": reason,
            "metadata": metadata or {},
            "idempotency_key": key,
            "balance_after": w["balance"],
            "ts": time.time(),
        })
        return removed


def set_unlimited(user_id: str, enabled: bool = True, *, reason: str = "owner_admin") -> dict[str, Any]:
    """Persist a server-side owner/admin bypass for every credit enforcement path."""
    uid = str(user_id or "").strip()
    if not uid:
        return {}
    with _lock:
        w = _wallet(uid)
        if bool(w.get("unlimited")) == bool(enabled):
            return dict(w)
        w["unlimited"] = bool(enabled)
        w["updated_at"] = time.time()
        _save()
        _append_ledger({
            "type": "unlimited_changed",
            "user_id": uid,
            "enabled": bool(enabled),
            "reason": reason,
            "balance_after": w["balance"],
            "ts": time.time(),
        })
        return dict(w)


def is_unlimited(user_id: str) -> bool:
    uid = str(user_id or "").strip()
    if not uid:
        return False
    with _lock:
        return bool(_wallet(uid).get("unlimited"))


# ---------------------------------------------------------------------------
# Balance + debit
# ---------------------------------------------------------------------------
def get_balance(user_id: str) -> int:
    with _lock:
        w = _wallet(user_id)
        if w.get("unlimited"):
            return 999_999_999
        return _sync_balance(w)


def get_state(user_id: str) -> dict[str, Any]:
    with _lock:
        w = _wallet(user_id)
        plan = str(w.get("plan") or "")
        spec = UNIFIED_PLANS.get(plan, {})
        return {
            "balance": 999_999_999 if w.get("unlimited") else _sync_balance(w),
            "unlimited": bool(w.get("unlimited")),
            "beta_access": bool(w.get("beta_access")),
            "plan": plan,
            "plan_name": spec.get("name") or "",
            "monthly_credits": int(spec.get("monthly_credits", 0) or 0),
            "monthly_balance": int(w.get("monthly_balance", 0) or 0),
            "rollover_balance": int(w.get("rollover_balance", 0) or 0),
            "topup_balance": int(w.get("topup_balance", 0) or 0),
            "reserved_credits": sum(
                int((row or {}).get("credits", 0) or 0)
                for row in (w.get("reservations") or {}).values()
            ),
            "lifetime_spent": int(w.get("lifetime_spent", 0) or 0),
            "month": _month_key(),
        }


def can_afford(user_id: str, credits: int) -> bool:
    return is_unlimited(user_id) or get_balance(user_id) >= max(0, int(credits or 0))


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
    if _runpod_worker_mode():
        _record_worker_cost_fact(
            "debit_credits",
            user_id=str(user_id or ""),
            credits=credits,
            reason=str(reason or ""),
            metadata=dict(metadata or {}),
            allow_negative=bool(allow_negative),
        )
        return True, 0
    if credits == 0:
        return True, get_balance(user_id)
    with _lock:
        w = _wallet(user_id)
        if w.get("unlimited"):
            return True, 999_999_999
        bal = _sync_balance(w)
        if bal < credits and not allow_negative:
            return False, bal
        consumed = _consume_locked(w, min(credits, bal)) or {}
        new_bal = _sync_balance(w)
        w["lifetime_spent"] = int(w.get("lifetime_spent", 0) or 0) + credits
        w["updated_at"] = time.time()
        _save()
        _append_ledger({
            "type": "debit",
            "user_id": user_id,
            "credits": -credits,
            "reason": reason,
            "metadata": metadata or {},
            "buckets": consumed,
            "balance_after": new_bal,
            "shortfall": max(0, credits - bal),
            "ts": time.time(),
        })
        return True, new_bal


def debit_usd(
    user_id: str,
    provider_usd: float | Decimal | str,
    *,
    reason: str,
    metadata: dict | None = None,
    allow_negative: bool = False,
) -> tuple[int, int]:
    """Convert provider USD -> credits and debit. Returns (credits_charged, balance_after)."""
    provider_amount = _usd_decimal(provider_usd)
    credits = usd_to_credits(provider_usd)
    md = dict(metadata or {})
    md["provider_usd"] = _usd_float(provider_amount)
    md["provider_usd_decimal"] = str(provider_amount)
    md["credit_usd_value"] = float(CREDIT_USD_VALUE)
    md["credit_usd_value_decimal"] = str(_decimal(CREDIT_USD_VALUE))
    md["credit_margin"] = float(CREDIT_MARGIN)
    if _runpod_worker_mode():
        _record_worker_cost_fact(
            "provider_usd",
            user_id=str(user_id or ""),
            provider_usd=_usd_float(provider_amount),
            provider_usd_decimal=str(provider_amount),
            credits=credits,
            reason=str(reason or ""),
            metadata=md,
            allow_negative=bool(allow_negative),
        )
        return credits, 0
    _ok, bal = debit_credits(user_id, credits, reason=reason, metadata=md, allow_negative=allow_negative)
    return credits, bal


class InsufficientCreditsError(RuntimeError):
    pass


def reserve_credits(
    user_id: str,
    credits: int,
    *,
    reason: str,
    metadata: dict | None = None,
) -> dict[str, Any]:
    """Atomically hold credits before a paid operation begins."""
    uid = str(user_id or "").strip()
    amount = max(0, int(credits or 0))
    if _runpod_worker_mode():
        return {
            "reservation_id": f"worker_{_runpod_dispatch_id()}_{uuid.uuid4().hex[:12]}",
            "credits": amount,
            "unlimited": False,
            "worker_deferred": True,
            "balance_after": 0,
            "reason": str(reason or "production"),
            "metadata": dict(metadata or {}),
        }
    if not uid or amount <= 0:
        return {"reservation_id": "", "credits": 0, "unlimited": False, "balance_after": get_balance(uid)}
    with _lock:
        w = _wallet(uid)
        if w.get("unlimited"):
            return {
                "reservation_id": f"owner_{uuid.uuid4().hex[:16]}",
                "credits": 0,
                "unlimited": True,
                "balance_after": 999_999_999,
            }
        consumed = _consume_locked(w, amount)
        if consumed is None:
            raise InsufficientCreditsError(
                f"This production needs {amount:,} credits, but only {_sync_balance(w):,} are available."
            )
        reservation_id = f"res_{uuid.uuid4().hex}"
        reservation = {
            "reservation_id": reservation_id,
            "credits": amount,
            "reason": str(reason or "production"),
            "metadata": dict(metadata or {}),
            "buckets": consumed,
            "created_at": time.time(),
        }
        reservations = dict(w.get("reservations") or {})
        reservations[reservation_id] = reservation
        w["reservations"] = reservations
        w["updated_at"] = time.time()
        _save()
        _append_ledger({
            "type": "reserve",
            "user_id": uid,
            **reservation,
            "balance_after": w["balance"],
            "ts": time.time(),
        })
        return {**reservation, "unlimited": False, "balance_after": w["balance"]}


def reserve_usd(
    user_id: str,
    provider_usd: float | Decimal | str,
    *,
    reason: str,
    metadata: dict | None = None,
    repair_reserve_pct: float = 0.25,
) -> dict[str, Any]:
    provider_amount = _usd_decimal(provider_usd)
    base = usd_to_credits(provider_usd)
    repair_multiplier = Decimal("1") + max(Decimal("0"), _decimal(repair_reserve_pct))
    held = int((Decimal(base) * repair_multiplier).to_integral_value(rounding=ROUND_CEILING))
    md = dict(metadata or {})
    md.update({
        "estimated_provider_usd": _usd_float(provider_amount),
        "estimated_provider_usd_decimal": str(provider_amount),
        "base_estimated_credits": base,
        "repair_reserve_pct": float(repair_reserve_pct or 0.0),
    })
    return reserve_credits(user_id, held, reason=reason, metadata=md)


def release_reservation(user_id: str, reservation_id: str, *, reason: str = "operation_failed") -> dict[str, Any]:
    uid = str(user_id or "").strip()
    rid = str(reservation_id or "").strip()
    if _runpod_worker_mode():
        _record_worker_cost_fact(
            "reservation_release",
            user_id=uid,
            reservation_id=rid,
            reason=str(reason or ""),
            credits=0,
        )
        return {"balance": 0, "worker_deferred": True}
    if not uid or not rid or rid.startswith("owner_"):
        return get_state(uid) if uid else {}
    with _lock:
        w = _wallet(uid)
        reservations = dict(w.get("reservations") or {})
        reservation = reservations.pop(rid, None)
        if not reservation:
            return get_state(uid)
        _restore_locked(w, dict(reservation.get("buckets") or {}))
        w["reservations"] = reservations
        w["updated_at"] = time.time()
        _save()
        _append_ledger({
            "type": "release",
            "user_id": uid,
            "reservation_id": rid,
            "credits": int(reservation.get("credits", 0) or 0),
            "reason": reason,
            "balance_after": w["balance"],
            "ts": time.time(),
        })
        return get_state(uid)


def commit_reservation(
    user_id: str,
    reservation_id: str,
    *,
    actual_credits: int | None = None,
    reason: str = "production_started",
    metadata: dict | None = None,
) -> dict[str, Any]:
    """Finalize a hold and refund any unused repair reserve."""
    uid = str(user_id or "").strip()
    rid = str(reservation_id or "").strip()
    if _runpod_worker_mode():
        _record_worker_cost_fact(
            "reservation_commit",
            user_id=uid,
            reservation_id=rid,
            actual_credits=max(0, int(actual_credits or 0)) if actual_credits is not None else None,
            reason=str(reason or ""),
            metadata=dict(metadata or {}),
        )
        return {"balance": 0, "worker_deferred": True}
    if not uid or not rid or rid.startswith("owner_"):
        return get_state(uid) if uid else {}
    with _lock:
        w = _wallet(uid)
        reservations = dict(w.get("reservations") or {})
        reservation = reservations.pop(rid, None)
        if not reservation:
            return get_state(uid)
        held = max(0, int(reservation.get("credits", 0) or 0))
        charged = held if actual_credits is None else max(0, min(held, int(actual_credits or 0)))
        refund = held - charged
        if refund:
            original = dict(reservation.get("buckets") or {})
            restored: dict[str, int] = {}
            remaining = refund
            for bucket in ("topup_balance", "monthly_balance", "rollover_balance"):
                take = min(remaining, int(original.get(bucket, 0) or 0))
                restored[bucket] = take
                remaining -= take
            _restore_locked(w, restored)
        w["reservations"] = reservations
        w["lifetime_spent"] = int(w.get("lifetime_spent", 0) or 0) + charged
        w["updated_at"] = time.time()
        _save()
        _append_ledger({
            "type": "commit",
            "user_id": uid,
            "reservation_id": rid,
            "credits": -charged,
            "held_credits": held,
            "refunded_credits": refund,
            "reason": reason,
            "metadata": metadata or {},
            "balance_after": w["balance"],
            "ts": time.time(),
        })
        return get_state(uid)


def settle_reservation(
    user_id: str,
    reservation_id: str,
    *,
    actual_credits: int,
    reason: str = "production_actuals",
    metadata: dict | None = None,
    allow_negative: bool = True,
) -> dict[str, Any]:
    """Atomically settle a hold and any verified overage in one wallet write.

    ``commit_reservation`` intentionally clamps to the held amount. Deferred
    RunPod reconciliation can learn a larger actual only after production has
    finished, so splitting commit + debit would create a crash window between
    two mutations. This operation consumes the hold, refunds any unused part,
    and consumes (or records) the overage under the same lock and ledger row.
    """

    uid = str(user_id or "").strip()
    rid = str(reservation_id or "").strip()
    target = max(0, int(actual_credits or 0))
    if _runpod_worker_mode():
        _record_worker_cost_fact(
            "reservation_settle",
            user_id=uid,
            reservation_id=rid,
            actual_credits=target,
            reason=str(reason or "production_actuals"),
            metadata=dict(metadata or {}),
            allow_negative=bool(allow_negative),
        )
        return {"balance": 0, "worker_deferred": True, "credits_charged": target}
    if not uid or not rid or rid.startswith("owner_"):
        return {**(get_state(uid) if uid else {}), "credits_charged": 0}
    with _lock:
        w = _wallet(uid)
        if w.get("unlimited"):
            return {**get_state(uid), "credits_charged": 0, "unlimited": True}
        reservations = dict(w.get("reservations") or {})
        reservation = reservations.pop(rid, None)
        if not reservation:
            return {**get_state(uid), "credits_charged": 0, "reservation_missing": True}

        held = max(0, int(reservation.get("credits", 0) or 0))
        from_hold = min(held, target)
        refund = held - from_hold
        overage = max(0, target - from_hold)
        available_after_refund = _sync_balance(w) + refund
        if overage > available_after_refund and not allow_negative:
            # Leave the reservation untouched when the caller asked for a
            # strict balance gate. Deferred provider actuals normally use the
            # post-paid policy because the render has already happened.
            reservations[rid] = reservation
            w["reservations"] = reservations
            return {
                **get_state(uid),
                "credits_charged": 0,
                "insufficient_for_overage": True,
                "required_overage": overage,
            }
        if refund:
            original = dict(reservation.get("buckets") or {})
            restored: dict[str, int] = {}
            remaining_refund = refund
            for bucket in ("topup_balance", "monthly_balance", "rollover_balance"):
                take = min(remaining_refund, int(original.get(bucket, 0) or 0))
                restored[bucket] = take
                remaining_refund -= take
            _restore_locked(w, restored)
        available_after_refund = _sync_balance(w)
        consumed_overage = _consume_locked(w, min(overage, available_after_refund)) or {}
        shortfall = max(0, overage - available_after_refund)
        w["reservations"] = reservations
        w["lifetime_spent"] = int(w.get("lifetime_spent", 0) or 0) + target
        w["updated_at"] = time.time()
        _sync_balance(w)
        _save()
        _append_ledger({
            "type": "settle",
            "user_id": uid,
            "reservation_id": rid,
            "credits": -target,
            "held_credits": held,
            "charged_from_hold": from_hold,
            "overage_credits": overage,
            "overage_buckets": consumed_overage,
            "shortfall": shortfall,
            "refunded_credits": refund,
            "reason": reason,
            "metadata": metadata or {},
            "balance_after": w["balance"],
            "ts": time.time(),
        })
        return {
            **get_state(uid),
            "credits_charged": target,
            "charged_from_hold": from_hold,
            "overage_credits": overage,
            "shortfall": shortfall,
            "refunded_credits": refund,
        }


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
