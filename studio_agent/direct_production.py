"""Safety boundary for legacy/direct Studio production HTTP routes.

Planning, reads, uploads, thumbnails, and chat never enter this module.  A
covered production mutation uses the same logged tool contract as Studio Agent;
an uncovered mutation fails closed while RunPod routing is enabled.
"""
from __future__ import annotations

import json
import os
from typing import Any

from fastapi import HTTPException, Request
from fastapi.concurrency import run_in_threadpool


LONGFORM_RUNPOD_ENABLED_ENV = "STUDIO_RUNPOD_LONGFORM_ENABLED"


def runpod_production_enabled() -> bool:
    # Resolve dynamically so tests and runtime flag changes use the exact same
    # switch as execute_tool_logged.
    from studio_agent import tools

    return bool(tools._runpod_production_enabled())


def longform_runpod_enabled() -> bool:
    return str(os.getenv(LONGFORM_RUNPOD_ENABLED_ENV, "0") or "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def require_longform_runpod_if_global_enabled() -> bool:
    """Return routing state, independently gating long-form's costly lane."""

    if not runpod_production_enabled():
        return False
    if not longform_runpod_enabled():
        raise HTTPException(
            503,
            "Long-form RunPod production is safety-disabled. Set "
            f"{LONGFORM_RUNPOD_ENABLED_ENV}=1 only after timeout and metering parity is verified.",
        )
    return True


def require_idempotency_key(request: Request) -> str:
    command_id = str(request.headers.get("x-idempotency-key") or "").strip()
    if not command_id:
        raise HTTPException(400, "X-Idempotency-Key is required for RunPod production mutations.")
    if len(command_id) > 512:
        raise HTTPException(400, "X-Idempotency-Key is too long.")
    return command_id


async def execute_logged_production(
    name: str,
    arguments: dict[str, Any],
    *,
    request: Request,
    user_id: str,
    content_format: str,
) -> dict[str, Any]:
    """Execute one covered mutation through Studio's logged RunPod boundary."""

    command_id = require_idempotency_key(request)
    dispatched_arguments = dict(arguments or {})
    dispatched_arguments["_runpod_command_id"] = command_id
    from studio_agent import tools

    try:
        raw = await run_in_threadpool(
            tools.execute_tool_logged,
            name,
            dispatched_arguments,
            user_id=str(user_id or ""),
            content_format=str(content_format or ""),
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(503, f"RunPod production dispatch failed: {str(exc)[:400]}") from exc
    try:
        payload = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        raise HTTPException(502, "Studio production dispatch returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise HTTPException(502, "Studio production dispatch returned an invalid response")
    return payload


def fail_closed_uncovered(request: Request, route: str) -> None:
    """Reject an incompatible legacy mutation before any local/provider work."""

    require_idempotency_key(request)
    raise HTTPException(
        503,
        f"{route} has no idempotent RunPod parity and is disabled while RunPod production is enabled.",
    )


__all__ = [
    "LONGFORM_RUNPOD_ENABLED_ENV",
    "execute_logged_production",
    "fail_closed_uncovered",
    "longform_runpod_enabled",
    "require_idempotency_key",
    "require_longform_runpod_if_global_enabled",
    "runpod_production_enabled",
]
