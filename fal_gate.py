"""Concurrency gate + retry wrapper for fal.run calls.

Problem: fal.ai enforces a ~20-concurrent-request ceiling PER API KEY. Without
a gate, bursty traffic (a single longform render fires 200+ parallel image
calls, or 10 users do 10 things each) immediately hits the ceiling and every
in-flight call after the 20th returns:

    {"detail": "Reached concurrent requests limit of 20",
     "type": "concurrent_requests_limit",
     "data": {"limit": 20, ...}}

Studio needs to never show that error to a paying user. This module provides:

1. A server-wide asyncio.Semaphore that caps our own in-flight count below the
   fal limit (default 16 — leaves 4 slots for admin/debug/parallel work).
2. `post_with_retry()` — the single path our code should use to hit fal.run.
   Transparently:
   - Acquires a gate slot (FIFO waiting if full)
   - Runs the httpx POST with timeout
   - Detects `concurrent_requests_limit` in the response body (fal returns 200
     with the error body, not a 429 HTTP status — don't try to short-circuit
     on status code alone)
   - Detects 429 / 5xx and backs off with jitter
   - Raises `FalBusy` (retryable) or `FalFailed` (terminal) with a human-
     friendly message the frontend can show verbatim
   - Fires a Studio alert if queue wait exceeds 10s or if 3+ retries needed

Migration path: wrap call sites one by one. Safe to coexist with un-gated
httpx.post calls during transition, but un-gated calls still risk bleeding
into fal's limit and forcing real-world retries on gated callers.
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from typing import Any

import httpx

try:
    import studio_alerts as _alerts
except Exception:  # pragma: no cover
    _alerts = None  # type: ignore

log = logging.getLogger(__name__)

# Account-wide fal concurrent limit is 20. Cap ourselves at 16 so admin calls
# and retries still have headroom.
_FAL_CONCURRENT_SOFT_CAP = int(os.getenv("FAL_CONCURRENT_SOFT_CAP", "16") or "16")
_FAL_DEFAULT_TIMEOUT = float(os.getenv("FAL_DEFAULT_TIMEOUT", "120") or "120")

_gate: asyncio.Semaphore | None = None


class FalBusy(RuntimeError):
    """All retries exhausted because fal was at/over capacity. Caller can suggest client retry."""


class FalFailed(RuntimeError):
    """fal returned a terminal, non-retryable error (content policy, 400, auth, etc.)."""


def _get_gate() -> asyncio.Semaphore:
    global _gate
    if _gate is None:
        _gate = asyncio.Semaphore(_FAL_CONCURRENT_SOFT_CAP)
    return _gate


def _is_concurrent_limit_error(body_text: str) -> bool:
    if not body_text:
        return False
    # fal returns 200 (or sometimes 429) with this error type in the body:
    return "concurrent_requests_limit" in body_text or "Reached concurrent requests limit" in body_text


def _is_content_policy_error(body_text: str) -> bool:
    if not body_text:
        return False
    return "content_policy_violation" in body_text or "content checker" in body_text


async def post_with_retry(
    url: str,
    *,
    api_key: str,
    json_body: dict[str, Any] | None = None,
    data_binary: bytes | None = None,
    timeout_sec: float | None = None,
    max_attempts: int = 5,
    source: str = "",
) -> dict[str, Any] | bytes:
    """POST to a fal.run endpoint with concurrency gate + retry.

    Returns parsed JSON (dict) by default. If response content-type is not JSON,
    returns raw bytes.

    Raises FalBusy on exhausted retries (retryable upstream).
    Raises FalFailed on terminal errors (content policy, auth, 400-class).
    """
    if not api_key:
        raise FalFailed("FAL_AI_KEY not configured")

    timeout = timeout_sec if timeout_sec is not None else _FAL_DEFAULT_TIMEOUT
    gate = _get_gate()

    # Measure wait time to alert on prolonged queue pressure.
    wait_start = time.time()
    async with gate:
        wait_elapsed = time.time() - wait_start
        if wait_elapsed > 10.0 and _alerts and _alerts.is_configured():
            _alerts.send_alert(
                "warn",
                "fal_gate: slot wait > 10s",
                f"Waited {wait_elapsed:.1f}s for a fal slot. Consider raising FAL_CONCURRENT_SOFT_CAP or adding a second fal key.",
                context={"url": url, "source": source, "cap": _FAL_CONCURRENT_SOFT_CAP},
            )

        last_error: str = ""
        for attempt in range(max_attempts):
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    headers = {
                        "Authorization": f"Key {api_key}",
                    }
                    if json_body is not None:
                        headers["Content-Type"] = "application/json"
                        resp = await client.post(url, headers=headers, json=json_body)
                    elif data_binary is not None:
                        headers["Content-Type"] = "application/json"
                        resp = await client.post(url, headers=headers, content=data_binary)
                    else:
                        raise FalFailed("post_with_retry called with no body")

                body_text = resp.text
                content_type = resp.headers.get("content-type", "").lower()

                # concurrent_requests_limit → backoff + retry. fal returns this as
                # a 200 sometimes, a 429 other times. Body-text match is authoritative.
                if _is_concurrent_limit_error(body_text):
                    last_error = "fal concurrent_requests_limit"
                    backoff = min(15.0, 1.5 * (attempt + 1) + random.uniform(0, 2.0))
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(backoff)
                        continue
                    break

                # Terminal content-policy rejection.
                if _is_content_policy_error(body_text):
                    raise FalFailed(f"fal content policy violation: {body_text[:400]}")

                # Retryable HTTP statuses.
                if resp.status_code in {429, 500, 502, 503, 504}:
                    last_error = f"fal HTTP {resp.status_code}"
                    backoff = min(12.0, 1.5 * (attempt + 1) + random.uniform(0, 1.5))
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(backoff)
                        continue
                    break

                # Other non-2xx → terminal.
                if resp.status_code >= 400:
                    raise FalFailed(f"fal HTTP {resp.status_code}: {body_text[:400]}")

                # Success.
                if "application/json" in content_type:
                    return resp.json()
                return resp.content
            except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError) as net_exc:
                last_error = f"network: {net_exc.__class__.__name__}"
                backoff = min(10.0, 1.5 * (attempt + 1) + random.uniform(0, 1.5))
                if attempt < max_attempts - 1:
                    await asyncio.sleep(backoff)
                    continue
                break

    # All attempts exhausted.
    if _alerts and _alerts.is_configured() and max_attempts >= 3:
        _alerts.send_alert(
            "warn",
            "fal_gate: retries exhausted",
            f"After {max_attempts} attempts, fal still unavailable. Last error: {last_error}",
            context={"url": url, "source": source},
        )
    raise FalBusy(
        f"fal upstream is busy. Retried {max_attempts} times; last error: {last_error}. "
        f"Try again in 30–60 seconds."
    )


def queue_depth() -> int:
    """Best-effort estimate of how many callers are currently waiting for a slot.

    Returns 0 if the gate hasn't been initialized yet.
    """
    g = _gate
    if g is None:
        return 0
    # Python's Semaphore exposes an internal counter; this is informational only.
    try:
        # ._value is the remaining slots; waiters count is len(._waiters).
        return len(getattr(g, "_waiters", []) or [])
    except Exception:
        return 0


def available_slots() -> int:
    g = _gate
    if g is None:
        return _FAL_CONCURRENT_SOFT_CAP
    try:
        return int(getattr(g, "_value", _FAL_CONCURRENT_SOFT_CAP))
    except Exception:
        return 0
