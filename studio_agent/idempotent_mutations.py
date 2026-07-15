"""Cross-process idempotency for direct/local billable Studio mutations."""
from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any

from studio_agent.command_execution import ExecutionReceipt, FileExecutionLedger, _default_ledger_root


LOCAL_IDEMPOTENT_TOOLS = {
    "expand_visual_proof_shortform",
    "animate_production_scenes",
    "finalize_production",
    "regenerate_production_scene",
    "regenerate_production_scene_still",
    "edit_production_scene_still",
    "edit_production_scenes_still",
    "repair_production_scene_animation",
    "re_edit_production",
    "expand_longform_visual_proof",
    "regenerate_longform_still",
    "finalize_longform_render",
}

_LEDGER = FileExecutionLedger(_default_ledger_root() / "local_mutations")


@dataclass(frozen=True)
class MutationClaim:
    key: str
    command_id: str
    tool_name: str
    target_job_id: str
    arguments: dict[str, Any]
    started_at: float


def _public_arguments(arguments: dict[str, Any] | None) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in dict(arguments or {}).items()
        if not str(key).startswith("_credit_") and str(key) != "_runpod_command_id"
    }


def _key(*, user_id: str, command_id: str) -> str:
    canonical = json.dumps(
        {"user_id": str(user_id or ""), "command_id": str(command_id or "")},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(("local-mutation-v1:" + canonical).encode("utf-8")).hexdigest()


def _same_contract(previous: ExecutionReceipt, tool_name: str, arguments: dict[str, Any]) -> bool:
    left = json.dumps(previous.arguments, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    right = json.dumps(arguments, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return previous.tool_name == tool_name and left == right


def begin(
    *,
    tool_name: str,
    arguments: dict[str, Any],
    command_id: str,
    user_id: str,
) -> tuple[MutationClaim | None, dict[str, Any] | None]:
    """Claim once or return a prior/pending response without running providers."""

    normalized_command = str(command_id or "").strip()
    if not normalized_command or tool_name not in LOCAL_IDEMPOTENT_TOOLS:
        return None, None
    public_arguments = _public_arguments(arguments)
    key = _key(user_id=user_id, command_id=normalized_command)
    previous = _LEDGER.get(key)
    if previous is not None:
        if not _same_contract(previous, tool_name, public_arguments):
            raise RuntimeError("X-Idempotency-Key was already used for a different production mutation.")
        if previous.status in {"failed", "rejected"}:
            raise RuntimeError(previous.error or "The original idempotent production mutation failed.")
        replay = dict(previous.result or {})
        replay["idempotent_replay"] = True
        replay["duplicate_of"] = previous.execution_id
        return None, replay
    if not _LEDGER.claim(key, normalized_command):
        previous = _LEDGER.get(key)
        if previous is not None:
            if not _same_contract(previous, tool_name, public_arguments):
                raise RuntimeError("X-Idempotency-Key was already used for a different production mutation.")
            replay = dict(previous.result or {})
            replay["idempotent_replay"] = True
            replay["duplicate_of"] = previous.execution_id
            return None, replay
        return None, {
            "ok": True,
            "status": "claim_pending",
            "idempotent_replay": True,
            "claim_pending": True,
            "note": "The original production mutation still owns this idempotency key.",
        }
    return MutationClaim(
        key=key,
        command_id=normalized_command,
        tool_name=tool_name,
        target_job_id=str(public_arguments.get("job_id") or ""),
        arguments=public_arguments,
        started_at=time.time(),
    ), None


def complete(claim: MutationClaim | None, result: dict[str, Any]) -> None:
    if claim is None:
        return
    status_text = str((result or {}).get("status") or "").strip().lower()
    status = "accepted" if status_text in {"accepted", "queued", "running", "claim_pending"} else "completed"
    _LEDGER.save(
        ExecutionReceipt(
            execution_id=f"exec_{uuid.uuid4().hex[:20]}",
            idempotency_key=claim.key,
            command_id=claim.command_id,
            status=status,
            tool_name=claim.tool_name,
            target_job_id=claim.target_job_id,
            arguments=claim.arguments,
            result=dict(result or {}),
            started_at=claim.started_at,
            finished_at=time.time(),
        )
    )


def fail(claim: MutationClaim | None, error: Exception) -> None:
    if claim is None:
        return
    message = str(error or "production mutation failed")[:1000]
    _LEDGER.save(
        ExecutionReceipt(
            execution_id=f"exec_{uuid.uuid4().hex[:20]}",
            idempotency_key=claim.key,
            command_id=claim.command_id,
            status="failed",
            tool_name=claim.tool_name,
            target_job_id=claim.target_job_id,
            arguments=claim.arguments,
            result={"ok": False, "status": "failed", "error": message},
            error=message,
            started_at=claim.started_at,
            finished_at=time.time(),
        )
    )


__all__ = ["LOCAL_IDEMPOTENT_TOOLS", "MutationClaim", "begin", "complete", "fail"]
