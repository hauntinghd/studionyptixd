"""Studio error + event alerts to a Discord webhook.

Design notes:
- Rate-limited in-memory (no Redis) with a 60s dedup window keyed on (kind, signature).
  One bug triggered by 1000 users produces ~1 Discord alert, not 1000.
- Fire-and-forget: alert sends never block the caller and never raise.
- Payload is Discord embed with a color-coded level; always includes deploy SHA + host.
- Turned off entirely if STUDIO_ERROR_WEBHOOK_URL is unset → zero overhead in dev.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import platform
import time
import traceback
from typing import Any

import httpx

log = logging.getLogger(__name__)

_WEBHOOK_URL = os.getenv("STUDIO_ERROR_WEBHOOK_URL", "").strip()
_HOST = platform.node() or "studio"
_BACKEND_COMMIT = os.getenv("BACKEND_COMMIT", "")[:10]

# Rate-limit state: { (kind, sig): last_fire_unix }
_rate_state: dict[tuple[str, str], float] = {}
_RATE_WINDOW_SEC = 60.0

_COLORS = {
    "error":   0xE74C3C,  # red
    "warn":    0xF39C12,  # orange
    "info":    0x3498DB,  # blue
    "success": 0x2ECC71,  # green
}

_ICONS = {
    "error":   "🔴",
    "warn":    "🟡",
    "info":    "🔵",
    "success": "🟢",
}


def _signature(title: str, description: str) -> str:
    """Hash that collapses "same error at same line" into one dedup key."""
    # Use first 200 chars; that's enough to pin down the call site but not every
    # per-request variable.
    raw = (title + "\n" + description)[:200]
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()[:12]


def _should_fire(kind: str, title: str, description: str) -> bool:
    if not _WEBHOOK_URL:
        return False
    sig = _signature(title, description)
    key = (kind, sig)
    now = time.time()
    last = _rate_state.get(key, 0.0)
    if now - last < _RATE_WINDOW_SEC:
        return False
    _rate_state[key] = now
    # Light GC — drop entries older than 10 minutes
    if len(_rate_state) > 500:
        cutoff = now - 600.0
        for k in list(_rate_state.keys()):
            if _rate_state[k] < cutoff:
                del _rate_state[k]
    return True


async def _post(payload: dict[str, Any]) -> None:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(_WEBHOOK_URL, json=payload)
    except Exception:  # never raise from an alert path
        log.debug("studio_alert webhook post failed", exc_info=True)


def send_alert(
    kind: str,
    title: str,
    description: str = "",
    *,
    context: dict[str, Any] | None = None,
    fields: list[dict[str, str]] | None = None,
) -> None:
    """Fire-and-forget alert to the Studio error webhook.

    kind: error | warn | info | success
    title: short headline (under 256 chars)
    description: body (under 2000 chars; truncated)
    context: small k/v map appended as embed fields (endpoint, user, session_id, etc.)
    fields: explicit list of Discord embed field dicts (overrides context formatting)
    """
    kind = (kind or "info").lower()
    if kind not in _COLORS:
        kind = "info"
    if not _should_fire(kind, title, description):
        return

    icon = _ICONS.get(kind, "")
    ttl = f"{icon} {title}"[:256]
    desc = (description or "")[:2000]

    embed_fields: list[dict[str, Any]] = []
    if fields:
        embed_fields = [{"name": f.get("name", "")[:256], "value": str(f.get("value", ""))[:1024], "inline": bool(f.get("inline", True))} for f in fields][:25]
    elif context:
        for k, v in list(context.items())[:10]:
            embed_fields.append({"name": str(k)[:256], "value": str(v)[:1024], "inline": True})

    footer_parts = [_HOST]
    if _BACKEND_COMMIT:
        footer_parts.append(_BACKEND_COMMIT)
    footer_parts.append(time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()))
    footer = " • ".join(footer_parts)

    payload = {
        "username": "Studio Alerts",
        "embeds": [{
            "title": ttl,
            "description": desc,
            "color": _COLORS[kind],
            "fields": embed_fields,
            "footer": {"text": footer[:2048]},
        }],
    }

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_post(payload))
    except RuntimeError:
        # Not in an event loop — run ad-hoc. Only happens from sync startup code.
        try:
            asyncio.run(_post(payload))
        except Exception:
            log.debug("studio_alert sync post failed", exc_info=True)


def send_exception(
    exc: BaseException,
    *,
    source: str,
    context: dict[str, Any] | None = None,
) -> None:
    """Shorthand for sending an exception as an error alert with traceback."""
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    # Keep last 1500 chars of traceback (Discord embed limit is 2048, leave room)
    tb_tail = tb[-1500:] if len(tb) > 1500 else tb
    send_alert(
        "error",
        f"{type(exc).__name__} in {source}",
        f"```\n{tb_tail}\n```",
        context=context,
    )


def is_configured() -> bool:
    return bool(_WEBHOOK_URL)
