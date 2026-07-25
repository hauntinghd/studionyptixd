"""Cross-process idempotency for direct/local billable Studio mutations."""
from __future__ import annotations

import hashlib
import json
import os
import socket
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any

from studio_agent.command_execution import ExecutionReceipt, FileExecutionLedger, _default_ledger_root


LOCAL_IDEMPOTENT_TOOLS = {
    "skeleton_generate_script",
    "skeleton_plan_script",
    "skeleton_generate_scenes",
    "skeleton_regenerate_scene",
    "skeleton_generate",
    "analyze_reference_video",
    "analyze_competitor_video",
    "retry_reference_analysis",
    "generate_longform_outline",
    "expand_longform_chapter",
    "start_shortform_generate",
    "start_longform_render",
    "expand_visual_proof_shortform",
    "generate_longform_thumbnails",
    "animate_production_scenes",
    "finalize_production",
    "regenerate_production_scene",
    "regenerate_production_scenes",
    "regenerate_production_scene_still",
    "edit_production_scene_still",
    "edit_production_scenes_still",
    "set_production_scenes_animate",
    "set_production_scene_duration",
    "set_production_scene_prompt",
    "repair_production_scene_animation",
    "audit_and_repair_production_scenes",
    "re_edit_production",
    "expand_longform_visual_proof",
    "regenerate_longform_still",
    "regenerate_longform_thumbnail",
    "finalize_longform_render",
    "cancel_longform_render",
    "ingest_cliplab_attachment",
    "analyze_cliplab_video",
    "render_cliplab_segments",
    "remix_cliplab_short",
    "catalyst_refresh_hub",
    "catalyst_analyze_reference_video",
    "catalyst_analyze_reference_video_manual",
    "catalyst_clear_reference_video",
    "catalyst_save_instructions",
    "catalyst_launch_longform",
    "catalyst_generate_longform_suggestions",
    "catalyst_auto_tick",
    "catalyst_set_auto_pilot",
    "catalyst_upload_longform",
    "catalyst_upload_short",
    "catalyst_add_reference",
    "catalyst_update_reference",
    "catalyst_delete_reference",
    "catalyst_sync_channels",
    "catalyst_sync_channel",
    "catalyst_sync_channel_outcomes",
}

_LEDGER = FileExecutionLedger(_default_ledger_root() / "local_mutations")
_CLAIM_LEASE_SECONDS = max(
    15.0,
    float(os.getenv("STUDIO_LOCAL_MUTATION_CLAIM_LEASE_SECONDS", "90") or 90),
)
_CLAIM_HEARTBEATS_LOCK = threading.RLock()
_CLAIM_HEARTBEATS: dict[str, tuple[threading.Event, threading.Thread]] = {}


@dataclass(frozen=True)
class MutationClaim:
    key: str
    command_id: str
    tool_name: str
    target_job_id: str
    arguments: dict[str, Any]
    started_at: float
    owner_token: str = ""
    lease_seconds: float = 0.0


def _start_claim_heartbeat(
    ledger: FileExecutionLedger,
    claim: MutationClaim,
) -> None:
    if not claim.owner_token or claim.lease_seconds <= 0:
        return
    stop = threading.Event()

    def heartbeat() -> None:
        interval = max(0.05, min(5.0, float(claim.lease_seconds) / 3.0))
        while not stop.wait(interval):
            if not ledger.renew_claim(
                claim.key,
                owner_token=claim.owner_token,
                lease_seconds=claim.lease_seconds,
            ):
                return

    thread = threading.Thread(
        target=heartbeat,
        daemon=True,
        name=f"studio-mutation-claim-{claim.key[:10]}",
    )
    with _CLAIM_HEARTBEATS_LOCK:
        _CLAIM_HEARTBEATS[claim.owner_token] = (stop, thread)
    thread.start()


def _stop_claim_heartbeat(claim: MutationClaim | None) -> None:
    if claim is None or not claim.owner_token:
        return
    with _CLAIM_HEARTBEATS_LOCK:
        heartbeat = _CLAIM_HEARTBEATS.pop(claim.owner_token, None)
    if heartbeat is None:
        return
    stop, thread = heartbeat
    stop.set()
    if thread.is_alive() and thread is not threading.current_thread():
        thread.join(timeout=1.0)


def _public_arguments(arguments: dict[str, Any] | None) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in dict(arguments or {}).items()
        if not str(key).startswith("_credit_")
        and not str(key).startswith("_billing_")
        and str(key) != "_runpod_command_id"
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
    owner_token = (
        f"{socket.gethostname()}:{os.getpid()}:{threading.get_ident()}:{uuid.uuid4().hex}"
    )
    leased_claim = getattr(_LEDGER, "claim_with_lease", None)
    claimed = (
        bool(
            leased_claim(
                key,
                normalized_command,
                owner_token=owner_token,
                lease_seconds=_CLAIM_LEASE_SECONDS,
            )
        )
        if callable(leased_claim)
        else bool(_LEDGER.claim(key, normalized_command))
    )
    if not claimed:
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
    claim = MutationClaim(
        key=key,
        command_id=normalized_command,
        tool_name=tool_name,
        target_job_id=str(public_arguments.get("job_id") or ""),
        arguments=public_arguments,
        started_at=time.time(),
        owner_token=owner_token if callable(leased_claim) else "",
        lease_seconds=_CLAIM_LEASE_SECONDS if callable(leased_claim) else 0.0,
    )
    if isinstance(_LEDGER, FileExecutionLedger):
        _start_claim_heartbeat(_LEDGER, claim)
    return claim, None


def complete(claim: MutationClaim | None, result: dict[str, Any]) -> None:
    if claim is None:
        return
    status_text = str((result or {}).get("status") or "").strip().lower()
    status = "accepted" if status_text in {"accepted", "queued", "running", "claim_pending"} else "completed"
    receipt = ExecutionReceipt(
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
    try:
        save_owned = getattr(_LEDGER, "save_if_claim_owner", None)
        if claim.owner_token and callable(save_owned):
            save_owned(
                receipt,
                owner_token=claim.owner_token,
                lease_seconds=claim.lease_seconds,
            )
        else:
            _LEDGER.save(receipt)
    finally:
        _stop_claim_heartbeat(claim)


def fail(claim: MutationClaim | None, error: Exception) -> None:
    if claim is None:
        return
    message = str(error or "production mutation failed")[:1000]
    receipt = ExecutionReceipt(
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
    try:
        save_owned = getattr(_LEDGER, "save_if_claim_owner", None)
        if claim.owner_token and callable(save_owned):
            save_owned(
                receipt,
                owner_token=claim.owner_token,
                lease_seconds=claim.lease_seconds,
            )
        else:
            _LEDGER.save(receipt)
    finally:
        _stop_claim_heartbeat(claim)


__all__ = ["LOCAL_IDEMPOTENT_TOOLS", "MutationClaim", "begin", "complete", "fail"]
