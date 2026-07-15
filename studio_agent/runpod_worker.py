"""Production-only RunPod Serverless worker.

The public ``handler(event)`` accepts only a signed Studio production envelope.
It is deliberately not an HTTP proxy and has no chat, planning, list, read,
poll, or status dispatch surface.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable

from studio_agent.runpod_contract import (
    RunPodContractError,
    normalize_tool_arguments,
    verify_signed_envelope,
)


_ASYNC_PRODUCTION_TOOLS = frozenset(
    {
        "start_shortform_generate",
        "expand_visual_proof_shortform",
        "start_longform_render",
        "expand_longform_visual_proof",
    }
)
_TERMINAL_OR_REVIEW_STATUSES = frozenset(
    {
        "complete",
        "completed",
        "failed",
        "error",
        "cancelled",
        "canceled",
        "awaiting_approval",
        "awaiting_scene_review",
        "awaiting_animation_review",
        "review_scenes",
        "scenes_approved",
    }
)


class WorkerConfigurationError(RuntimeError):
    pass


def _verify_runpod_network_volume(root: Path) -> None:
    """Require the Linux RunPod network-volume mount, not an ephemeral folder."""

    if os.name != "posix":
        raise WorkerConfigurationError("RunPod production workers require Linux")
    mount = Path("/runpod-volume")
    if not mount.is_dir() or mount.is_symlink() or not os.path.ismount(str(mount)):
        raise WorkerConfigurationError("/runpod-volume is not a verified mounted network volume")
    try:
        root.resolve(strict=False).relative_to(mount.resolve(strict=True))
    except (OSError, ValueError) as exc:
        raise WorkerConfigurationError("APP_DATA_DIR must be inside /runpod-volume") from exc


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
        default=str,
    ).encode("utf-8")


def _atomic_json_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        with temp.open("w", encoding="utf-8") as fh:
            json.dump(value, fh, indent=2, ensure_ascii=False, default=str)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temp, path)
    finally:
        try:
            temp.unlink(missing_ok=True)
        except OSError:
            pass


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def prepare_worker_environment() -> Path:
    """Enable worker mode and bind every production path to the network volume."""

    raw = str(os.getenv("APP_DATA_DIR") or "").strip()
    if not raw:
        raise WorkerConfigurationError("APP_DATA_DIR is required for durable RunPod execution")
    root = Path(raw).expanduser()
    if not root.is_absolute():
        raise WorkerConfigurationError("APP_DATA_DIR must be an absolute network-volume path")
    _verify_runpod_network_volume(root)
    root.mkdir(parents=True, exist_ok=True)
    os.environ["STUDIO_RUNPOD_WORKER_MODE"] = "1"
    os.environ["SKELETON_AI_OUTPUT_ROOT"] = str(root / "skeleton_ai" / "output")
    os.environ["LF_OUTPUT_ROOT"] = str(root / "long_form")
    return root


def _receipt_paths(data_root: Path, dispatch_id: str) -> tuple[Path, Path]:
    directory = data_root / "runpod_worker_dispatches"
    return directory / f"{dispatch_id}.claim.json", directory / f"{dispatch_id}.receipt.json"


def _semantic_payload_hash(envelope: dict[str, Any]) -> str:
    """Hash execution identity while ignoring signed retry-only metadata."""

    identity = {
        "schema": envelope["schema"],
        "dispatch_id": envelope["dispatch_id"],
        "command_id": envelope["command_id"],
        "tool": envelope["tool"],
        "arguments": normalize_tool_arguments(envelope["arguments"], semantic=True),
        "context": envelope["context"],
    }
    return hashlib.sha256(_json_bytes(identity)).hexdigest()


def _claim_once(
    claim_path: Path,
    receipt_path: Path,
    *,
    dispatch_id: str,
    payload_hash: str,
    tool: str,
) -> tuple[str, dict[str, Any] | None]:
    receipt = _read_json(receipt_path)
    if receipt is not None:
        if str(receipt.get("payload_hash") or "") != payload_hash:
            return "conflict", None
        return "receipt", receipt

    claim_path.parent.mkdir(parents=True, exist_ok=True)
    claim = {
        "dispatch_id": dispatch_id,
        "payload_hash": payload_hash,
        "tool": tool,
        "claimed_at": time.time(),
        "pid": os.getpid(),
    }
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    try:
        descriptor = os.open(str(claim_path), flags, 0o600)
    except FileExistsError:
        existing = _read_json(claim_path)
        if existing is None:
            return "unknown_claim", None
        if str(existing.get("payload_hash") or "") != payload_hash:
            return "conflict", None
        # Never auto-retry an existing claim. It may have spent provider money
        # before a process loss but not reached its receipt write.
        return "claimed", existing
    try:
        payload = json.dumps(claim, indent=2, ensure_ascii=False).encode("utf-8")
        os.write(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return "new", claim


def _load_tool_executor() -> Callable[..., str]:
    # Lazy import is required: tools.py resolves output roots at import time.
    # The control plane already budgeted and reserved this signed command.
    # Calling execute_tool_logged here would reserve a second time against a
    # worker-local wallet, so the worker invokes the underlying tool exactly
    # once and returns cost facts for control-plane reconciliation.
    from studio_agent.tools import execute_tool

    return execute_tool


def _parse_tool_result(raw: Any) -> Any:
    if not isinstance(raw, str):
        return raw
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return raw


def _job_id_from_result(result: Any, arguments: dict[str, Any]) -> str:
    if isinstance(result, dict):
        value = str(result.get("job_id") or "").strip()
        if value:
            return value
    return str(
        arguments.get("job_id")
        or arguments.get("studio_job_id")
        or arguments.get("_requested_job_id")
        or ""
    ).strip()


def _wait_timeout_seconds() -> float:
    try:
        value = float(os.getenv("RUNPOD_PRODUCTION_WAIT_TIMEOUT_SECONDS", "7200"))
    except (TypeError, ValueError):
        value = 7200.0
    return max(30.0, min(value, 86_400.0))


def _poll_interval_seconds() -> float:
    try:
        value = float(os.getenv("RUNPOD_PRODUCTION_POLL_SECONDS", "1"))
    except (TypeError, ValueError):
        value = 1.0
    return max(0.1, min(value, 10.0))


def _compact_progress(snapshot: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "job_id",
        "kind",
        "status",
        "stage",
        "phase",
        "progress",
        "percent",
        "running",
        "scene_done",
        "scene_total",
        "detail",
        "error",
    )
    return {key: snapshot[key] for key in keys if snapshot.get(key) is not None}


def _emit_progress_update(event: Any, snapshot: dict[str, Any]) -> None:
    """Publish optional RunPod status progress without exposing a read tool."""

    try:
        import runpod

        # RunPod documents progress as a string. Compact JSON preserves the
        # structured Studio snapshot for GET /status clients.
        progress = json.dumps(
            _compact_progress(snapshot),
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )
        runpod.serverless.progress_update(event, progress)
    except Exception:
        # Local tests and non-RunPod execution intentionally do not require the
        # SDK or a live worker transport.
        return


def _wait_for_production_gate(
    tool: str,
    job_id: str,
    event: Any = None,
) -> dict[str, Any] | None:
    """Keep a RunPod job alive until its background stage reaches a durable gate."""

    if tool not in _ASYNC_PRODUCTION_TOOLS or not job_id:
        return None
    from studio_agent.jobs import get_job_snapshot

    kind = "longform" if "longform" in tool else "shortform"
    deadline = time.monotonic() + _wait_timeout_seconds()
    last_snapshot: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        try:
            snapshot = get_job_snapshot(job_id, kind)
        except Exception as exc:
            snapshot = {"job_id": job_id, "kind": kind, "status": "running", "detail": str(exc)[:200]}
        if isinstance(snapshot, dict):
            last_snapshot = snapshot
            _emit_progress_update(event, snapshot)
            status = str(snapshot.get("status") or snapshot.get("stage") or "").strip().lower()
            running = snapshot.get("running")
            if status in _TERMINAL_OR_REVIEW_STATUSES or (running is False and status not in {"", "unknown"}):
                return snapshot
        time.sleep(_poll_interval_seconds())
    timed_out = {
        **(last_snapshot or {"job_id": job_id, "kind": kind}),
        "status": "worker_wait_timeout",
        "error": "Production did not reach a durable gate before the worker wait timeout",
    }
    _emit_progress_update(event, timed_out)
    return timed_out


def _shortform_cost_total(job_id: str) -> Decimal | None:
    """Read the durable provider ledger position for one short-form job."""

    normalized = str(job_id or "").strip()
    if not normalized:
        return None
    from studio_agent import production_costs
    from studio_agent.fs_paths import skeleton_output_root

    workspace = skeleton_output_root() / normalized
    if not (workspace / "cost_ledger.jsonl").is_file():
        return None
    summary = production_costs.summarize(workspace)
    try:
        amount = Decimal(str(summary.get("total_usd_decimal", summary.get("total_usd", "0"))))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not amount.is_finite() or amount < 0:
        return None
    return amount


def _operation_cost_delta(before: Decimal | None, after: Decimal | None) -> Decimal | None:
    if after is None:
        return None
    baseline = before if before is not None else Decimal("0")
    return max(Decimal("0"), after - baseline)


def _billing_facts(
    dispatch_id: str,
    snapshot: dict[str, Any] | None,
    *,
    tool: str,
    job_id: str,
    user_id: str,
    operation_usd: Decimal | None,
) -> dict[str, Any]:
    try:
        from unified_credits import get_runpod_worker_cost_facts

        facts = get_runpod_worker_cost_facts(dispatch_id)
    except Exception:
        facts = []
    # The job ledger delta is the operation-scoped source of truth. Remove any
    # cumulative/provider debit echoes so reconciliation can never double-count
    # the same provider spend.
    report_complete = "longform" not in str(tool or "") and operation_usd is not None
    if report_complete:
        facts = [fact for fact in facts if str(fact.get("kind") or "") != "provider_usd"]
        facts.append(
            {
                "kind": "provider_usd",
                "dispatch_id": dispatch_id,
                "user_id": str(user_id or ""),
                "tool": str(tool or ""),
                "job_id": str(job_id or ""),
                "provider_usd": float(operation_usd),
                "provider_usd_decimal": str(operation_usd),
                "operation_scoped": True,
                "authoritative": True,
            }
        )
    if isinstance(snapshot, dict) and isinstance(snapshot.get("cost"), dict):
        facts = [
            *facts,
            {
                "kind": "job_cost_snapshot",
                "dispatch_id": dispatch_id,
                "cost": snapshot["cost"],
            },
        ]
    return {
        "mode": "control_plane_reconciliation",
        "wallet_mutated": False,
        "report_complete": report_complete,
        "provider_cost_facts": facts,
        "facts_path": f"runpod_worker_cost_facts/{dispatch_id}.jsonl",
    }


def _rejection(code: str, message: str) -> dict[str, Any]:
    return {"ok": False, "status": "rejected", "error": str(code), "detail": str(message)[:300]}


def handler(event: Any) -> dict[str, Any]:
    """Run exactly one authenticated production tool call, at most once durably."""

    try:
        data_root = prepare_worker_environment()
    except WorkerConfigurationError as exc:
        return _rejection("worker_not_configured", str(exc))

    if not isinstance(event, dict) or "input" not in event:
        return _rejection("invalid_runpod_event", "RunPod event must contain an input envelope")
    try:
        envelope = verify_signed_envelope(event.get("input"))
    except RunPodContractError as exc:
        return _rejection(exc.code, str(exc))

    dispatch_id = str(envelope["dispatch_id"])
    tool = str(envelope["tool"])
    payload_hash = _semantic_payload_hash(envelope)
    claim_path, receipt_path = _receipt_paths(data_root, dispatch_id)
    claim_status, prior = _claim_once(
        claim_path,
        receipt_path,
        dispatch_id=dispatch_id,
        payload_hash=payload_hash,
        tool=tool,
    )
    if claim_status == "receipt" and prior is not None:
        replay = dict(prior.get("response") or {})
        replay["idempotent_replay"] = True
        return replay
    if claim_status == "conflict":
        return _rejection("dispatch_conflict", "dispatch_id was previously used for a different payload")
    if claim_status in {"claimed", "unknown_claim"}:
        return {
            "ok": False,
            "status": "execution_state_unknown" if claim_status == "unknown_claim" else "already_claimed",
            "error": "dispatch_already_claimed",
            "dispatch_id": dispatch_id,
            "detail": "The worker will not execute a claimed dispatch twice; inspect its durable receipt/facts.",
        }

    os.environ["STUDIO_RUNPOD_DISPATCH_ID"] = dispatch_id
    context = dict(envelope["context"])
    arguments = dict(envelope["arguments"])
    workspace_job_id = _job_id_from_result(None, arguments)
    job_id = workspace_job_id
    operation_before: Decimal | None = None
    workspace_reconciliation: dict[str, Any] | None = None
    started_at = time.time()
    try:
        if tool not in {"start_shortform_generate", "start_longform_render"}:
            if not workspace_job_id:
                raise WorkerConfigurationError("Existing-job production command is missing job_id")
            from studio_agent import runpod_storage

            workspace_reconciliation = runpod_storage.reconcile_staged_workspace(
                workspace_job_id,
                "longform" if "longform" in tool else "shortform",
            )
        if "longform" not in tool:
            operation_before = _shortform_cost_total(workspace_job_id)
        # This is the sole Studio mutation/execution call in the worker.
        raw_result = _load_tool_executor()(
            tool,
            arguments,
            user_id=str(context["user_id"]),
            content_format=str(context["content_format"]),
            session_id=str(context["session_id"] or "") or None,
        )
        result = _parse_tool_result(raw_result)
        job_id = _job_id_from_result(result, arguments)
        snapshot = _wait_for_production_gate(tool, job_id, event)
        wait_timed_out = isinstance(snapshot, dict) and snapshot.get("status") == "worker_wait_timeout"
        operation_usd = (
            None
            if wait_timed_out or "longform" in tool
            else _operation_cost_delta(operation_before, _shortform_cost_total(job_id))
        )
        response: dict[str, Any] = {
            "ok": not wait_timed_out,
            "status": "worker_wait_timeout" if wait_timed_out else "completed",
            "schema": envelope["schema"],
            "dispatch_id": dispatch_id,
            "command_id": envelope["command_id"],
            "tool": tool,
            "job_id": job_id or None,
            "result": result,
            "job_snapshot": snapshot,
            "billing": _billing_facts(
                dispatch_id,
                snapshot,
                tool=tool,
                job_id=job_id,
                user_id=str(context["user_id"]),
                operation_usd=operation_usd,
            ),
            "workspace_reconciliation": workspace_reconciliation,
            "worker_mode": True,
            "execution_count": 1,
            "started_at": started_at,
            "finished_at": time.time(),
            "idempotent_replay": False,
        }
    except Exception as exc:
        operation_usd = (
            None
            if "longform" in tool
            else _operation_cost_delta(operation_before, _shortform_cost_total(job_id))
        )
        response = {
            "ok": False,
            "status": "failed",
            "schema": envelope["schema"],
            "dispatch_id": dispatch_id,
            "command_id": envelope["command_id"],
            "tool": tool,
            "error": type(exc).__name__,
            "detail": str(exc)[:500],
            "billing": _billing_facts(
                dispatch_id,
                None,
                tool=tool,
                job_id=job_id,
                user_id=str(context["user_id"]),
                operation_usd=operation_usd,
            ),
            "workspace_reconciliation": workspace_reconciliation,
            "worker_mode": True,
            "execution_count": 1,
            "started_at": started_at,
            "finished_at": time.time(),
            "idempotent_replay": False,
        }

    receipt = {
        "dispatch_id": dispatch_id,
        "payload_hash": payload_hash,
        "tool": tool,
        "completed_at": time.time(),
        "response": response,
    }
    try:
        _atomic_json_write(receipt_path, receipt)
    except OSError:
        # The claim remains durable. A retry refuses to duplicate provider spend.
        response["receipt_error"] = "durable_receipt_write_failed"
    return response
