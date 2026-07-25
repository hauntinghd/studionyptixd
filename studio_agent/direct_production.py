"""Safety boundary for direct paid Studio HTTP routes.

Read-only calls, uploads, and ordinary chat never enter this module. A paid
planning or production mutation uses the same idempotent, logged credit
contract as Studio Agent; an uncovered production mutation fails closed while
RunPod routing is enabled.
"""
from __future__ import annotations

import hashlib
import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator
from typing import Any

from fastapi import HTTPException, Request, UploadFile
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
        raise HTTPException(400, "X-Idempotency-Key is required for Studio production mutations.")
    if len(command_id) > 512:
        raise HTTPException(400, "X-Idempotency-Key is too long.")
    return command_id


async def upload_content_contract(
    upload: UploadFile | None,
    *,
    chunk_bytes: int = 1024 * 1024,
) -> dict[str, Any]:
    """Fingerprint an upload without consuming it for the downstream handler.

    Filename, MIME type, and declared size are not an identity: two different
    files can share all three. Direct multipart production routes therefore
    bind the idempotency receipt to a streamed SHA-256 of the actual bytes.
    ``UploadFile`` transparently moves disk-backed reads to Starlette's thread
    pool, and the original cursor is restored even when hashing fails.
    """

    if upload is None:
        return {}
    handle = getattr(upload, "file", None)
    if handle is None:
        raise HTTPException(400, "Production upload is not readable.")
    try:
        original_position = int(handle.tell())
    except Exception:
        original_position = 0
    digest = hashlib.sha256()
    content_bytes = 0
    read_size = max(64 * 1024, min(int(chunk_bytes or 0), 8 * 1024 * 1024))
    try:
        await upload.seek(0)
        while True:
            chunk = await upload.read(read_size)
            if not chunk:
                break
            digest.update(chunk)
            content_bytes += len(chunk)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(400, "Production upload could not be fingerprinted.") from exc
    finally:
        try:
            await upload.seek(original_position)
        except Exception:
            pass
    return {
        "filename": str(getattr(upload, "filename", "") or ""),
        "content_type": str(getattr(upload, "content_type", "") or ""),
        "size": (
            int(getattr(upload, "size", 0))
            if isinstance(getattr(upload, "size", None), int)
            else None
        ),
        "content_bytes": content_bytes,
        "content_sha256": digest.hexdigest(),
    }


@dataclass
class DirectProductionClaim:
    """One backend-authorized HTTP production mutation.

    Direct Studio panels do not get to mint tool arguments or provider
    identities.  They supply an opaque retry key; the backend derives the
    exact mutation identity, claims it once, and stores the accepted result.
    """

    arguments: dict[str, Any]
    replay: dict[str, Any] | None
    _claim: Any = None
    _completed: bool = False

    def complete(self, result: dict[str, Any]) -> dict[str, Any]:
        if self.replay is not None:
            return dict(self.replay)
        from studio_agent import idempotent_mutations

        payload = dict(result or {})
        idempotent_mutations.complete(self._claim, payload)
        self._completed = True
        return payload


@contextmanager
def claim_direct_production(
    name: str,
    arguments: dict[str, Any],
    *,
    request: Request,
    user_id: str,
    content_format: str,
) -> Iterator[DirectProductionClaim]:
    """Claim a whole legacy-panel mutation before provider, spend, or spawn.

    This adapter lets upload-oriented routes keep their established response
    shape while using the same backend authority and cross-process receipt
    store as Agent tool execution.
    """

    command_id = require_idempotency_key(request)
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        raise HTTPException(401, "auth_required")
    from studio_agent import idempotent_mutations
    from studio_agent.execution_context import (
        authorize_production_mutation,
        production_command_scope,
    )

    claim = None
    with production_command_scope(
        command_id,
        user_id=normalized_user_id,
        session_id="",
        source="server_workflow",
        user_text=f"direct:{name}",
    ):
        try:
            authorized_arguments, mutation = authorize_production_mutation(
                name,
                dict(arguments or {}),
                user_id=normalized_user_id,
                session_id="",
            )
            if mutation is None:
                raise RuntimeError(
                    f"{name} rejected: backend command authorization was not created"
                )
            claim, replay = idempotent_mutations.begin(
                tool_name=name,
                arguments=authorized_arguments,
                command_id=command_id,
                user_id=normalized_user_id,
            )
            if claim is None and replay is None:
                raise RuntimeError(
                    f"{name} rejected: production mutation is not registered "
                    "with the backend idempotency ledger"
                )
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(409, str(exc)[:400]) from exc
        execution = DirectProductionClaim(
            arguments=authorized_arguments,
            replay=dict(replay) if isinstance(replay, dict) else None,
            _claim=claim,
        )
        try:
            yield execution
            if claim is not None and not execution._completed:
                raise RuntimeError(
                    f"{name} exited without committing its production receipt"
                )
        except Exception as exc:
            if claim is not None and not execution._completed:
                idempotent_mutations.fail(claim, exc)
            raise


async def execute_logged_production(
    name: str,
    arguments: dict[str, Any],
    *,
    request: Request,
    user_id: str,
    content_format: str,
) -> dict[str, Any]:
    """Execute one covered mutation through Studio's logged billing boundary."""

    command_id = require_idempotency_key(request)
    dispatched_arguments = dict(arguments or {})
    from studio_agent import tools
    from studio_agent.execution_context import production_command_scope

    try:
        # Direct production panels use the same backend-issued authority as
        # Studio chat. The browser contributes only an idempotency key; it
        # cannot inject provider identity, target ownership, or mutation ids.
        with production_command_scope(
            command_id,
            user_id=str(user_id or ""),
            session_id="",
            source="server_workflow",
            user_text=f"direct:{name}",
        ):
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
        raise HTTPException(503, f"Studio production execution failed: {str(exc)[:400]}") from exc
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
    "DirectProductionClaim",
    "LONGFORM_RUNPOD_ENABLED_ENV",
    "claim_direct_production",
    "execute_logged_production",
    "fail_closed_uncovered",
    "longform_runpod_enabled",
    "require_idempotency_key",
    "require_longform_runpod_if_global_enabled",
    "runpod_production_enabled",
    "upload_content_contract",
]
