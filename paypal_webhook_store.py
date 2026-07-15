"""Supabase-backed idempotency store for PayPal webhook events.

Supabase is the cross-instance idempotency layer. Events are inserted in a
``processing`` state before effects, then conditionally transitioned to
``completed`` or ``failed`` by claim id. Failed and stale claims can be
reclaimed; a live claim cannot. Callers must fail closed on ``unavailable``.

See also: migrations/2026-04-18_paypal_webhook_events.sql
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx

log = logging.getLogger(__name__)

_SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
_SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "").strip()
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
    """Compatibility read: only completed (or legacy) rows are processed."""
    event_id = str(event_id or "").strip()
    if not event_id or not configured():
        return False
    try:
        async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT) as client:
            resp = await client.get(
                f"{_SUPABASE_URL}/rest/v1/{_TABLE}",
                headers=_headers(),
                params={
                    "select": "event_id,payload_excerpt",
                    "event_id": f"eq.{event_id}",
                    "limit": "1",
                },
            )
            if resp.status_code != 200:
                return False
            data = resp.json()
            if not isinstance(data, list) or not data:
                return False
            payload = data[0].get("payload_excerpt") if isinstance(data[0], dict) else None
            if not isinstance(payload, dict) or not payload.get("status"):
                return True  # rows from the pre-state-machine schema are completed
            return str(payload.get("status") or "").lower() == "completed"
    except Exception as exc:
        log.warning("paypal_webhook_store has_processed failed: %s", type(exc).__name__)
        return False


def _claim_payload(
    *,
    claim_id: str,
    provider: str,
    attempts: int,
    first_seen_at: float,
) -> dict[str, Any]:
    now = time.time()
    return {
        "status": "processing",
        "claim_id": claim_id,
        "provider": str(provider or "paypal")[:24],
        "attempts": max(1, int(attempts or 1)),
        "first_seen_at": float(first_seen_at or now),
        "claimed_at": now,
        "updated_at": now,
    }


async def claim_event(
    event_id: str,
    *,
    event_type: str = "",
    provider: str = "paypal",
    lease_seconds: float = 300.0,
) -> dict[str, Any]:
    """Atomically claim an event in Supabase.

    Returns status ``claimed``, ``duplicate``, ``busy``, or ``unavailable``.
    ``unavailable`` is never permission to run effects.
    """
    normalized_id = str(event_id or "").strip()
    if not normalized_id or not configured():
        return {"status": "unavailable", "claim_id": ""}
    claim_id = f"rwh_{uuid.uuid4().hex}"
    payload = _claim_payload(
        claim_id=claim_id,
        provider=provider,
        attempts=1,
        first_seen_at=time.time(),
    )
    row = {
        "event_id": normalized_id,
        "event_type": str(event_type or "")[:80],
        "payload_excerpt": payload,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT) as client:
            insert = await client.post(
                f"{_SUPABASE_URL}/rest/v1/{_TABLE}",
                headers={**_headers(), "Prefer": "resolution=ignore-duplicates,return=representation"},
                json=row,
            )
            if insert.status_code not in (200, 201):
                log.warning("webhook claim insert failed status=%d", insert.status_code)
                return {"status": "unavailable", "claim_id": ""}
            inserted = insert.json() if insert.content else []
            if isinstance(inserted, list) and inserted:
                return {"status": "claimed", "claim_id": claim_id}

            lookup = await client.get(
                f"{_SUPABASE_URL}/rest/v1/{_TABLE}",
                headers=_headers(),
                params={"select": "event_id,event_type,payload_excerpt,processed_at", "event_id": f"eq.{normalized_id}", "limit": "1"},
            )
            if lookup.status_code != 200:
                log.warning("webhook claim lookup failed status=%d", lookup.status_code)
                return {"status": "unavailable", "claim_id": ""}
            rows = lookup.json()
            if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
                return {"status": "unavailable", "claim_id": ""}
            prior = rows[0]
            prior_payload = prior.get("payload_excerpt")
            if not isinstance(prior_payload, dict) or not prior_payload.get("status"):
                return {"status": "duplicate", "claim_id": ""}
            prior_status = str(prior_payload.get("status") or "").strip().lower()
            if prior_status == "completed":
                return {"status": "duplicate", "claim_id": ""}
            if prior_status == "processing":
                if str(prior_payload.get("claim_id") or "") == claim_id:
                    return {"status": "claimed", "claim_id": claim_id}
                claimed_at = float(prior_payload.get("claimed_at", 0) or 0)
                if claimed_at > 0 and (time.time() - claimed_at) < max(1.0, float(lease_seconds or 0)):
                    return {"status": "busy", "claim_id": ""}
            elif prior_status != "failed":
                return {"status": "unavailable", "claim_id": ""}

            old_claim_id = str(prior_payload.get("claim_id") or "").strip()
            if not old_claim_id:
                return {"status": "unavailable", "claim_id": ""}
            replacement = _claim_payload(
                claim_id=claim_id,
                provider=provider,
                attempts=int(prior_payload.get("attempts", 0) or 0) + 1,
                first_seen_at=float(prior_payload.get("first_seen_at", 0) or 0),
            )
            takeover = await client.patch(
                f"{_SUPABASE_URL}/rest/v1/{_TABLE}",
                headers={**_headers(), "Prefer": "return=representation"},
                params={
                    "event_id": f"eq.{normalized_id}",
                    "payload_excerpt->>claim_id": f"eq.{old_claim_id}",
                    # The claim id alone is insufficient: the original owner can
                    # complete while this requester is waiting on the row lock.
                    # Including the observed state lets PostgreSQL re-check both
                    # predicates after the concurrent update and prevents a
                    # completed row from being resurrected as processing.
                    "payload_excerpt->>status": f"eq.{prior_status}",
                },
                json={
                    "event_type": str(event_type or "")[:80],
                    "payload_excerpt": replacement,
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            if takeover.status_code not in (200, 204):
                return {"status": "unavailable", "claim_id": ""}
            taken = takeover.json() if takeover.content else []
            if isinstance(taken, list) and taken:
                return {"status": "claimed", "claim_id": claim_id}
            return {"status": "busy", "claim_id": ""}
    except Exception as exc:
        log.warning("webhook claim failed: %s", type(exc).__name__)
        return {"status": "unavailable", "claim_id": ""}


async def _transition_event(
    event_id: str,
    claim_id: str,
    *,
    status: str,
    event_type: str = "",
    provider: str = "paypal",
    action: str = "",
    error_code: str = "",
) -> bool:
    normalized_id = str(event_id or "").strip()
    normalized_claim = str(claim_id or "").strip()
    if not normalized_id or not normalized_claim or not configured():
        return False
    now = time.time()
    payload = {
        "status": status,
        "claim_id": normalized_claim,
        "provider": str(provider or "paypal")[:24],
        "action": str(action or "")[:160],
        "error_code": str(error_code or "")[:80],
        "updated_at": now,
    }
    payload[f"{status}_at"] = now
    try:
        async with httpx.AsyncClient(timeout=_REQUEST_TIMEOUT) as client:
            response = await client.patch(
                f"{_SUPABASE_URL}/rest/v1/{_TABLE}",
                headers={**_headers(), "Prefer": "return=representation"},
                params={
                    "event_id": f"eq.{normalized_id}",
                    "payload_excerpt->>claim_id": f"eq.{normalized_claim}",
                    "payload_excerpt->>status": "eq.processing",
                },
                json={
                    "event_type": str(event_type or "")[:80],
                    "payload_excerpt": payload,
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            if response.status_code not in (200, 204):
                log.warning("webhook transition failed status=%d", response.status_code)
                return False
            rows = response.json() if response.content else []
            return isinstance(rows, list) and bool(rows)
    except Exception as exc:
        log.warning("webhook transition failed: %s", type(exc).__name__)
        return False


async def complete_event(
    event_id: str,
    claim_id: str,
    *,
    event_type: str = "",
    provider: str = "paypal",
    action: str = "",
) -> bool:
    return await _transition_event(
        event_id,
        claim_id,
        status="completed",
        event_type=event_type,
        provider=provider,
        action=action,
    )


async def fail_event(
    event_id: str,
    claim_id: str,
    *,
    event_type: str = "",
    provider: str = "paypal",
    error_code: str = "effect_failed",
) -> bool:
    return await _transition_event(
        event_id,
        claim_id,
        status="failed",
        event_type=event_type,
        provider=provider,
        error_code=error_code,
    )


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
                    "payload_excerpt": {
                        "status": "completed",
                        "action": str((payload_excerpt or {}).get("action", "") or "")[:160],
                        "completed_at": time.time(),
                    },
                },
            )
            if resp.status_code not in (200, 201, 204):
                log.warning("paypal_webhook_store mark_processed status=%d", resp.status_code)
                return False
    except Exception as exc:
        log.warning("paypal_webhook_store mark_processed failed: %s", type(exc).__name__)
        return False
    return True
