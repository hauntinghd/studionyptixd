"""Supabase-backed idempotency store for PayPal webhook events.

Problem: the in-memory + local-JSON dedup (`_paypal_webhook_events`) lives in
each RunPod worker's ephemeral filesystem. When a worker cycles (template
update, idle timeout, crash), the dedup state is wiped. PayPal retries a
webhook up to 25 times over 3 days; a lost worker + a retry = duplicate
application of the same event. For topups that's a double-credit. For
subscription cancellations, a double-cancellation. For refunds, a double-
refund.

Fix: Supabase is the authoritative idempotency store. `has_processed()`
consults Supabase on every webhook; `mark_processed()` upserts there.

Falls back gracefully if Supabase is not configured — in that case the
existing in-memory file-cache is the only dedup. That's fine for dev.

See also: migrations/2026-04-18_paypal_webhook_events.sql
"""
from __future__ import annotations

import logging
import os
from typing import Any

import httpx

log = logging.getLogger(__name__)

_SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
_SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "").strip() or os.getenv("SUPABASE_ANON_KEY", "").strip()
_TABLE = "paypal_webhook_events"
_REQUEST_TIMEOUT = 5.0  # webhook path is latency-sensitive; PayPal retries if we're slow


def configured() -> bool:
    return bool(_SUPABASE_URL and _SUPABASE_SERVICE_KEY)


def _headers() -> dict[str, str]:
    return {
        "apikey": _SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {_SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }


async def has_processed(event_id: str) -> bool:
    """True if this event_id was already applied (don't re-credit).

    Returns False on any Supabase error — caller should fall through to
    local dedup. This is safe-by-default: if Supabase is down, we may
    double-apply; but the local file-cache still catches repeats within
    a single worker lifetime.
    """
    event_id = str(event_id or "").strip()
    if not event_id or not configured():
        return False
    try:
        async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT) as client:
            resp = await client.get(
                f"{_SUPABASE_URL}/rest/v1/{_TABLE}",
                headers=_headers(),
                params={"select": "event_id", "event_id": f"eq.{event_id}", "limit": "1"},
            )
            if resp.status_code != 200:
                return False
            data = resp.json()
            return bool(data and len(data) > 0)
    except Exception as exc:
        log.warning("paypal_webhook_store has_processed failed: %s", str(exc)[:200])
        return False


async def mark_processed(event_id: str, event_type: str = "", payload_excerpt: dict[str, Any] | None = None) -> bool:
    """Upsert the event into Supabase. Returns True on success.

    Safe to call repeatedly with the same event_id — the table has a PK
    on event_id so duplicate inserts collapse to a no-op.
    """
    event_id = str(event_id or "").strip()
    if not event_id or not configured():
        return False
    try:
        async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT) as client:
            resp = await client.post(
                f"{_SUPABASE_URL}/rest/v1/{_TABLE}",
                headers={**_headers(), "Prefer": "resolution=merge-duplicates"},
                json={
                    "event_id": event_id,
                    "event_type": str(event_type or "")[:80],
                    "payload_excerpt": payload_excerpt or {},
                },
            )
            if resp.status_code not in (200, 201, 204):
                log.warning(
                    "paypal_webhook_store mark_processed status=%d body=%s",
                    resp.status_code, resp.text[:200],
                )
                return False
    except Exception as exc:
        log.warning("paypal_webhook_store mark_processed failed: %s", str(exc)[:200])
        return False
    return True
