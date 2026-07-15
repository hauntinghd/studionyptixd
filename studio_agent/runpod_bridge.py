"""Strict RunPod dispatch for durable Studio production work.

This module is deliberately *not* a generic HTTP-to-RunPod proxy.  Planning,
chat, reads, job polling, and status requests must stay on the API service.
Only an allowlisted top-level production tool may create a RunPod queue item.

The public dispatch flow is intentionally small:

1. validate and sign one deterministic production envelope;
2. call RunPod's read-only ``/health`` endpoint;
3. submit that envelope exactly once to async ``/run``.

The Studio executor calls this bridge only when the explicit production flag
is enabled. Keeping this module isolated makes the enqueue policy independently
testable and prevents generic HTTP traffic from reaching RunPod.
"""
from __future__ import annotations

import json
import hashlib
import os
import re
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

from .runpod_contract import (
    RUNPOD_ENVELOPE_SCHEMA,
    RUNPOD_PRODUCTION_TOOL_ALLOWLIST,
    RunPodContractError,
    build_signed_envelope,
)


DEFAULT_MAX_QUEUE_DEPTH = 3
DEFAULT_CLAIM_WAIT_SEC = 30.0
DEFAULT_CLAIM_POLL_SEC = 0.05
_PRODUCTION_LEASE_FILENAME = "production.active.json"
_QUEUE_ADMISSION_FILENAME = "queue-admission.active.json"
_RUNPOD_JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")


class RunPodBridgeError(RuntimeError):
    """Base error for configuration, readiness, and dispatch failures."""

    code = "runpod_bridge_error"

    def __init__(self, message: str, *, status_code: int | None = None, detail: str = "") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.detail = str(detail or "")[:500]

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": self.code,
            "status_code": self.status_code,
            "detail": self.detail or str(self),
        }


class RunPodConfigurationError(RunPodBridgeError):
    code = "runpod_not_configured"


class RunPodDispatchPolicyError(RunPodBridgeError):
    code = "runpod_dispatch_policy_rejected"


class RunPodEndpointUnavailable(RunPodBridgeError):
    code = "runpod_endpoint_unavailable"


class RunPodPaymentRequired(RunPodEndpointUnavailable):
    code = "runpod_payment_required"


class RunPodEndpointBacklogged(RunPodEndpointUnavailable):
    code = "runpod_endpoint_backlogged"


class RunPodDispatchRejected(RunPodBridgeError):
    code = "runpod_dispatch_rejected"


def runpod_api_key() -> str:
    return str(os.getenv("RUNPOD_API_KEY") or "").strip()


def runpod_endpoint_id() -> str:
    return str(os.getenv("RUNPOD_ENDPOINT_ID") or "").strip()


def runpod_dispatch_secret() -> str:
    return str(os.getenv("RUNPOD_DISPATCH_SECRET") or "").strip()


def runpod_configured() -> bool:
    """Return whether strict signed production dispatch can run."""

    return bool(
        runpod_api_key()
        and runpod_endpoint_id()
        and len(runpod_dispatch_secret().encode("utf-8")) >= 32
    )


def dispatch_ledger_dir() -> Path:
    """Cross-process claim/receipt directory for control-plane dispatches."""

    configured = str(os.getenv("RUNPOD_DISPATCH_LEDGER_DIR") or "").strip()
    if configured:
        root = Path(configured).expanduser()
    else:
        app_data = str(os.getenv("APP_DATA_DIR") or "").strip()
        if app_data:
            root = Path(app_data).expanduser() / "runpod_dispatch_ledger" / "control_plane"
        else:
            from .fs_paths import data_root

            root = data_root() / "runpod_dispatch_ledger" / "control_plane"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _ledger_paths(dispatch_id: str) -> tuple[Path, Path]:
    normalized = str(dispatch_id or "").strip().lower()
    suffix = normalized[4:] if normalized.startswith("rpd_") else ""
    if len(suffix) != 40 or any(character not in "0123456789abcdef" for character in suffix):
        raise RunPodDispatchPolicyError("invalid dispatch_id for ledger")
    root = dispatch_ledger_dir()
    return root / f"{normalized}.claim.json", root / f"{normalized}.receipt.json"


def _validated_dispatch_id(dispatch_id: str) -> str:
    normalized = str(dispatch_id or "").strip().lower()
    _ledger_paths(normalized)
    return normalized


def acquire_production_lease(
    dispatch_id: str,
    *,
    studio_job_id: str = "",
    tool: str = "",
) -> dict[str, Any]:
    """Hold one global production command from staging through terminal sync.

    This first rollout deliberately serializes all production work.  A lease is
    never expired automatically: after a process loss, guessing that provider
    work stopped could let a second command overwrite the same mounted state or
    create duplicate spend.  Terminal workspace reconciliation releases it.
    """

    normalized = _validated_dispatch_id(dispatch_id)
    path = dispatch_ledger_dir() / _PRODUCTION_LEASE_FILENAME
    payload = {
        "dispatch_id": normalized,
        "studio_job_id": str(studio_job_id or "").strip()[:512],
        "tool": str(tool or "").strip()[:128],
        "pid": os.getpid(),
        "acquired_at": time.time(),
    }
    try:
        descriptor = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        existing = _read_json_file(path)
        if not existing:
            raise RunPodDispatchPolicyError(
                "RunPod production lease is unreadable; refusing another production command"
            )
        if str(existing.get("dispatch_id") or "") != normalized:
            raise RunPodEndpointBacklogged(
                "Another Studio production command is already active",
                status_code=409,
                detail="Wait for terminal workspace sync before starting another production command.",
            )
        for key in ("studio_job_id", "tool"):
            wanted = str(payload.get(key) or "")
            held = str(existing.get(key) or "")
            if wanted and held and wanted != held:
                raise RunPodDispatchPolicyError(
                    "Equivalent RunPod dispatch identity is bound to different production metadata"
                )
        return {**existing, "acquired": False, "idempotent_replay": True}
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    return {**payload, "acquired": True, "idempotent_replay": False}


def release_production_lease(dispatch_id: str) -> bool:
    """Release the global production lease only for its exact owner."""

    normalized = _validated_dispatch_id(dispatch_id)
    path = dispatch_ledger_dir() / _PRODUCTION_LEASE_FILENAME
    if not path.exists():
        return False
    existing = _read_json_file(path)
    if not existing:
        raise RunPodDispatchPolicyError(
            "RunPod production lease is unreadable and cannot be released safely"
        )
    if str(existing.get("dispatch_id") or "") != normalized:
        return False
    path.unlink()
    return True


def _acquire_queue_admission(dispatch_id: str) -> str:
    """Serialize the queue-depth observation and its corresponding POST."""

    normalized = _validated_dispatch_id(dispatch_id)
    path = dispatch_ledger_dir() / _QUEUE_ADMISSION_FILENAME
    token = uuid.uuid4().hex
    payload = {
        "dispatch_id": normalized,
        "token": token,
        "pid": os.getpid(),
        "acquired_at": time.time(),
    }
    try:
        descriptor = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as exc:
        raise RunPodEndpointBacklogged(
            "Another RunPod queue admission is in progress",
            status_code=429,
            detail="No production job was submitted by this command.",
        ) from exc
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    return token


def _release_queue_admission(dispatch_id: str, token: str) -> None:
    path = dispatch_ledger_dir() / _QUEUE_ADMISSION_FILENAME
    existing = _read_json_file(path)
    if (
        existing
        and str(existing.get("dispatch_id") or "") == str(dispatch_id or "")
        and str(existing.get("token") or "") == str(token or "")
    ):
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


def _read_json_file(path: Path) -> dict[str, Any] | None:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
        return parsed if isinstance(parsed, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _create_dispatch_claim(envelope: dict[str, Any]) -> str | None:
    dispatch_id = str(envelope.get("dispatch_id") or "")
    claim_path, _receipt_path = _ledger_paths(dispatch_id)
    claim_token = uuid.uuid4().hex
    payload = {
        "schema": RUNPOD_ENVELOPE_SCHEMA,
        "dispatch_id": dispatch_id,
        "command_id": str(envelope.get("command_id") or ""),
        "tool": str(envelope.get("tool") or ""),
        "claim_token": claim_token,
        "phase": "claimed",
        "pid": os.getpid(),
        "claimed_at": time.time(),
    }
    try:
        descriptor = os.open(str(claim_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return None
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        try:
            claim_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    return claim_token


def _update_dispatch_claim(dispatch_id: str, claim_token: str, **updates: Any) -> None:
    claim_path, _receipt_path = _ledger_paths(dispatch_id)
    current = _read_json_file(claim_path)
    if not current or str(current.get("claim_token") or "") != claim_token:
        raise RunPodDispatchRejected(
            "RunPod dispatch claim was lost before submission",
            detail="No production job was submitted.",
        )
    current.update(updates)
    current["updated_at"] = time.time()
    _atomic_write_json(claim_path, current)


def _release_dispatch_claim(dispatch_id: str, claim_token: str) -> None:
    claim_path, _receipt_path = _ledger_paths(dispatch_id)
    current = _read_json_file(claim_path)
    if current and str(current.get("claim_token") or "") == claim_token:
        try:
            claim_path.unlink(missing_ok=True)
        except OSError:
            pass


def _load_dispatch_receipt(dispatch_id: str) -> dict[str, Any] | None:
    _claim_path, receipt_path = _ledger_paths(dispatch_id)
    receipt = _read_json_file(receipt_path)
    if receipt and str(receipt.get("dispatch_id") or "") == dispatch_id:
        return receipt
    return None


def _save_dispatch_receipt(dispatch_id: str, receipt: dict[str, Any]) -> None:
    _claim_path, receipt_path = _ledger_paths(dispatch_id)
    _atomic_write_json(receipt_path, receipt)
    studio_job_id = str(receipt.get("studio_job_id") or receipt.get("job_id") or "").strip()
    if studio_job_id:
        index_dir = dispatch_ledger_dir() / "studio_job_index" / hashlib.sha256(
            studio_job_id.encode("utf-8")
        ).hexdigest()
        _atomic_write_json(index_dir / f"{dispatch_id}.json", receipt)


def get_dispatch_receipt(dispatch_id: str) -> dict[str, Any] | None:
    """Return a durable dispatch receipt without claiming or networking."""

    normalized = str(dispatch_id or "").strip().lower()
    # Reuse ledger path validation even when the receipt does not exist.
    _ledger_paths(normalized)
    receipt = _load_dispatch_receipt(normalized)
    return dict(receipt) if receipt is not None else None


def get_dispatch_receipt_by_studio_job_id(studio_job_id: str) -> dict[str, Any] | None:
    """Return the newest receipt associated with a Studio production job."""

    wanted = str(studio_job_id or "").strip()
    if not wanted or len(wanted) > 512:
        raise RunPodDispatchPolicyError("studio_job_id is required for receipt lookup")
    newest: tuple[float, float, str, dict[str, Any]] | None = None
    index_dir = dispatch_ledger_dir() / "studio_job_index" / hashlib.sha256(
        wanted.encode("utf-8")
    ).hexdigest()
    indexed_paths = list(index_dir.glob("rpd_*.json")) if index_dir.is_dir() else []
    # Fall back to the pre-index flat ledger so existing production receipts
    # remain discoverable after rollout. New polls inspect only this job's
    # small index directory instead of every historical dispatch receipt.
    receipt_paths = indexed_paths or list(dispatch_ledger_dir().glob("rpd_*.receipt.json"))
    for receipt_path in receipt_paths:
        receipt = _read_json_file(receipt_path)
        if not receipt:
            continue
        receipt_job_id = str(receipt.get("studio_job_id") or receipt.get("job_id") or "").strip()
        if receipt_job_id != wanted:
            continue
        try:
            submitted_at = float(receipt.get("submitted_at") or 0.0)
        except (TypeError, ValueError):
            submitted_at = 0.0
        try:
            modified_at = float(receipt_path.stat().st_mtime)
        except OSError:
            modified_at = 0.0
        candidate = (submitted_at, modified_at, receipt_path.name, receipt)
        if newest is None or candidate[:3] > newest[:3]:
            newest = candidate
    return dict(newest[3]) if newest is not None else None


def _studio_job_id_from_envelope(envelope: dict[str, Any]) -> str:
    arguments = envelope.get("arguments")
    if not isinstance(arguments, dict):
        return ""
    for key in ("studio_job_id", "job_id"):
        value = arguments.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()[:512]
    return ""


def _claim_wait_sec(value: float | None) -> float:
    if value is not None:
        return max(0.0, float(value))
    try:
        return max(0.0, float(os.getenv("RUNPOD_DISPATCH_CLAIM_WAIT_SEC", DEFAULT_CLAIM_WAIT_SEC)))
    except (TypeError, ValueError):
        return DEFAULT_CLAIM_WAIT_SEC


def _idempotent_replay(receipt: dict[str, Any]) -> dict[str, Any]:
    replay = dict(receipt)
    replay["idempotent_replay"] = True
    replay["duplicate_of_dispatch_id"] = str(receipt.get("dispatch_id") or "")
    return replay


def _acquire_dispatch_or_replay(
    envelope: dict[str, Any],
    *,
    wait_sec: float | None,
) -> tuple[str | None, dict[str, Any] | None]:
    dispatch_id = str(envelope.get("dispatch_id") or "")
    deadline = time.monotonic() + _claim_wait_sec(wait_sec)
    while True:
        receipt = _load_dispatch_receipt(dispatch_id)
        if receipt is not None:
            return None, _idempotent_replay(receipt)
        claim_token = _create_dispatch_claim(envelope)
        if claim_token:
            return claim_token, None
        if time.monotonic() >= deadline:
            claim_path, _receipt_path = _ledger_paths(dispatch_id)
            claim = _read_json_file(claim_path) or {}
            return None, {
                "ok": False,
                "status": "dispatch_claimed",
                "dispatch_id": dispatch_id,
                "command_id": str(envelope.get("command_id") or ""),
                "tool": str(envelope.get("tool") or ""),
                "idempotent_replay": True,
                "claim_pending": True,
                "claim_phase": str(claim.get("phase") or "unknown"),
                "note": "An equivalent production dispatch is already claimed; no duplicate RunPod job was submitted.",
            }
        time.sleep(DEFAULT_CLAIM_POLL_SEC)


def _resolved_credentials(endpoint_id: str | None = None) -> tuple[str, str]:
    api_key = runpod_api_key()
    resolved_endpoint = str(endpoint_id or runpod_endpoint_id()).strip()
    if not api_key:
        raise RunPodConfigurationError("RUNPOD_API_KEY is not set")
    if not resolved_endpoint:
        raise RunPodConfigurationError("RUNPOD_ENDPOINT_ID is not set")
    return api_key, resolved_endpoint


def _endpoint_base(endpoint_id: str) -> str:
    return f"https://api.runpod.ai/v2/{endpoint_id}"


def _response_detail(exc: urllib.error.HTTPError) -> str:
    try:
        return exc.read().decode("utf-8", errors="replace")[:500]
    except Exception:
        return ""


def _request_json(
    *,
    method: str,
    path: str,
    endpoint_id: str | None = None,
    body: dict[str, Any] | None = None,
    timeout_sec: float,
) -> tuple[dict[str, Any], str]:
    api_key, resolved_endpoint = _resolved_credentials(endpoint_id)
    encoded = None if body is None else json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        _endpoint_base(resolved_endpoint) + path,
        data=encoded,
        method=method.upper(),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            **({"Content-Type": "application/json"} if encoded is not None else {}),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        detail = _response_detail(exc)
        if int(exc.code or 0) == 402:
            raise RunPodPaymentRequired(
                "RunPod rejected the endpoint because the account requires payment",
                status_code=402,
                detail=detail,
            ) from exc
        raise RunPodEndpointUnavailable(
            f"RunPod endpoint request failed with HTTP {exc.code}",
            status_code=int(exc.code or 0) or None,
            detail=detail,
        ) from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise RunPodEndpointUnavailable(
            "RunPod endpoint could not be reached",
            detail=str(getattr(exc, "reason", exc))[:500],
        ) from exc

    try:
        parsed = json.loads(raw) if raw else {}
    except json.JSONDecodeError as exc:
        raise RunPodEndpointUnavailable(
            "RunPod returned a non-JSON response",
            detail=raw[:500],
        ) from exc
    if not isinstance(parsed, dict):
        raise RunPodEndpointUnavailable("RunPod returned an invalid response object")
    return parsed, resolved_endpoint


def _nonnegative_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _configured_max_queue_depth(value: int | None) -> int:
    raw: Any = value
    if raw is None:
        raw = os.getenv("RUNPOD_DISPATCH_MAX_QUEUE_DEPTH", str(DEFAULT_MAX_QUEUE_DEPTH))
    try:
        parsed = int(raw)
    except (TypeError, ValueError) as exc:
        raise RunPodConfigurationError(
            "RUNPOD_DISPATCH_MAX_QUEUE_DEPTH must be an integer of at least 1"
        ) from exc
    if parsed < 1:
        raise RunPodConfigurationError(
            "RUNPOD_DISPATCH_MAX_QUEUE_DEPTH must be at least 1; disabling the cap is forbidden"
        )
    return parsed


def preflight_runpod_endpoint(
    *,
    endpoint_id: str | None = None,
    timeout_sec: float = 15.0,
    max_queue_depth: int | None = None,
) -> dict[str, Any]:
    """Verify account/endpoint availability without creating a queued job.

    A serverless endpoint with zero ready workers can still be healthy: RunPod
    may cold-start it after ``/run``.  The preflight therefore rejects API,
    payment, and backlog failures, but does not require an idle worker.
    """

    queue_cap = _configured_max_queue_depth(max_queue_depth)
    health, resolved_endpoint = _request_json(
        method="GET",
        path="/health",
        endpoint_id=endpoint_id,
        timeout_sec=timeout_sec,
    )
    jobs = health.get("jobs") if isinstance(health.get("jobs"), dict) else {}
    workers = health.get("workers") if isinstance(health.get("workers"), dict) else {}
    queue_depth = _nonnegative_int(jobs.get("inQueue", jobs.get("in_queue", 0)))
    if queue_depth >= queue_cap:
        raise RunPodEndpointBacklogged(
            f"RunPod production queue is at its safety cap ({queue_depth}/{queue_cap})",
            status_code=429,
            detail="No production job was submitted.",
        )
    return {
        "ok": True,
        "endpoint_id": resolved_endpoint,
        "queue_depth": queue_depth,
        "max_queue_depth": queue_cap,
        "jobs": jobs,
        "workers": workers,
    }


def _reservation_receipt_metadata(
    arguments: dict[str, Any] | None,
    user_id: str,
) -> dict[str, Any]:
    reservation = (arguments or {}).get("_credit_reservation")
    if not isinstance(reservation, dict):
        return {}
    reservation_id = str(reservation.get("reservation_id") or "").strip()
    if not reservation_id:
        return {}
    metadata: dict[str, Any] = {
        "credit_reservation_id": reservation_id,
        "billing_user_id": str(user_id or "").strip(),
        "credit_reservation_unlimited": bool(reservation.get("unlimited")),
    }
    try:
        metadata["credit_reservation_credits"] = max(
            0, int(reservation.get("credits") or 0)
        )
    except (TypeError, ValueError):
        metadata["credit_reservation_credits"] = 0
    return metadata


def _decoded_progress(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text or text[:1] not in {"{", "["}:
        return value
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return value


def _normalize_runpod_job_status(
    response: dict[str, Any],
    *,
    requested_job_id: str,
    endpoint_id: str,
) -> dict[str, Any]:
    runpod_status = str(response.get("status") or "UNKNOWN").strip().upper()
    status_map = {
        "IN_QUEUE": ("queued", "runpod_queue", "Queued on RunPod"),
        "IN_PROGRESS": ("running", "runpod_execution", "Running on RunPod"),
        "RUNNING": ("running", "runpod_execution", "Running on RunPod"),
        "COMPLETED": ("complete", "complete", "RunPod production complete"),
        "FAILED": ("failed", "error", "RunPod production failed"),
        "CANCELLED": ("failed", "error", "RunPod production cancelled"),
        "TIMED_OUT": ("failed", "error", "RunPod production timed out"),
    }
    studio_status, stage, stage_label = status_map.get(
        runpod_status,
        ("unknown", "runpod_unknown", "Unknown RunPod status"),
    )
    output = response.get("output")
    output_dict = output if isinstance(output, dict) else {}
    if runpod_status == "COMPLETED" and output_dict.get("ok") is False:
        studio_status, stage, stage_label = "failed", "error", "RunPod worker failed"

    job_snapshot = output_dict.get("job_snapshot")
    if not isinstance(job_snapshot, dict):
        job_snapshot = None
    raw_progress = response.get("progress")
    if raw_progress is None:
        raw_progress = output_dict.get("progress")
    live_output = _decoded_progress(output)
    if runpod_status in {"IN_PROGRESS", "RUNNING"} and job_snapshot is None:
        if isinstance(live_output, dict) and (
            "job_id" in live_output or "status" in live_output or "stage" in live_output
        ):
            job_snapshot = dict(live_output)
        if raw_progress is None and output is not None:
            raw_progress = output
    if raw_progress is None and job_snapshot is not None:
        raw_progress = job_snapshot.get("progress")
    progress = _decoded_progress(raw_progress)
    # RunPod progress_update carries the worker's compact snapshot as a JSON
    # string. Surface that snapshot directly while preserving progress too.
    if job_snapshot is None and isinstance(progress, dict) and (
        "job_id" in progress or "status" in progress or "stage" in progress
    ):
        job_snapshot = dict(progress)
    if progress is None:
        progress = 100 if studio_status == "complete" else 0

    runpod_job_id = str(response.get("id") or requested_job_id).strip() or requested_job_id
    normalized: dict[str, Any] = {
        "ok": studio_status not in {"failed", "unknown"},
        "status": studio_status,
        "stage": stage,
        "stage_label": stage_label,
        "running": studio_status in {"queued", "running"},
        "terminal": studio_status in {"complete", "failed"},
        "progress": progress,
        "runpod_status": runpod_status,
        "runpod_job_id": runpod_job_id,
        "endpoint_id": endpoint_id,
    }
    if "output" in response:
        normalized["output"] = output
    if job_snapshot is not None:
        normalized["job_snapshot"] = job_snapshot
        studio_job_id = str(job_snapshot.get("job_id") or output_dict.get("job_id") or "").strip()
        if studio_job_id:
            normalized["studio_job_id"] = studio_job_id
    elif isinstance(output_dict.get("job_id"), str) and output_dict["job_id"].strip():
        normalized["studio_job_id"] = output_dict["job_id"].strip()
    error = response.get("error")
    if error is None:
        error = output_dict.get("error")
    if error is not None:
        normalized["error"] = error
    detail = response.get("detail")
    if detail is None:
        detail = output_dict.get("detail")
    if detail is not None:
        normalized["detail"] = detail
    return normalized


def get_runpod_job_status(
    runpod_job_id: str,
    *,
    endpoint_id: str | None = None,
    timeout_sec: float = 15.0,
) -> dict[str, Any]:
    """Read one RunPod job status; this function can never enqueue work."""

    normalized_job_id = str(runpod_job_id or "").strip()
    if not _RUNPOD_JOB_ID_RE.fullmatch(normalized_job_id):
        raise RunPodDispatchPolicyError("runpod_job_id is malformed")
    response, resolved_endpoint = _request_json(
        method="GET",
        path=f"/status/{normalized_job_id}",
        endpoint_id=endpoint_id,
        timeout_sec=timeout_sec,
    )
    return _normalize_runpod_job_status(
        response,
        requested_job_id=normalized_job_id,
        endpoint_id=resolved_endpoint,
    )


def dispatch_production_tool(
    tool: str,
    arguments: dict[str, Any] | None,
    *,
    command_id: str = "",
    user_id: str = "",
    session_id: str = "",
    content_format: str = "",
    endpoint_id: str | None = None,
    preflight_timeout_sec: float = 15.0,
    dispatch_timeout_sec: float = 30.0,
    max_queue_depth: int | None = None,
    claim_wait_sec: float | None = None,
) -> dict[str, Any]:
    """Submit exactly one async RunPod job for one production command.

    There is intentionally no ``async_mode`` switch and no generic payload
    argument.  Reads, chats, polling tools, and arbitrary HTTP routes cannot be
    smuggled through this function.
    """

    normalized_tool = str(tool or "").strip()
    if normalized_tool not in RUNPOD_PRODUCTION_TOOL_ALLOWLIST:
        raise RunPodDispatchPolicyError(
            f"{normalized_tool or '<empty>'} is not an allowlisted RunPod production tool"
        )
    if not isinstance(arguments or {}, dict):
        raise RunPodDispatchPolicyError("production tool arguments must be an object")

    try:
        envelope = build_signed_envelope(
            normalized_tool,
            dict(arguments or {}),
            command_id=str(command_id or ""),
            user_id=str(user_id or ""),
            session_id=str(session_id or ""),
            content_format=str(content_format or ""),
        )
    except RunPodContractError as exc:
        if getattr(exc, "code", "") == "missing_signing_secret":
            raise RunPodConfigurationError(str(exc)) from exc
        raise RunPodDispatchPolicyError(str(exc)) from exc

    dispatch_id = str(envelope.get("dispatch_id") or "")
    studio_job_id = _studio_job_id_from_envelope(envelope)
    lease = acquire_production_lease(
        dispatch_id,
        studio_job_id=studio_job_id,
        tool=normalized_tool,
    )
    lease_created = bool(lease.get("acquired"))
    try:
        claim_token, replay = _acquire_dispatch_or_replay(envelope, wait_sec=claim_wait_sec)
    except Exception:
        if lease_created:
            release_production_lease(dispatch_id)
        raise
    if replay is not None:
        # A completed receipt does not own a newly recreated lease. An
        # unresolved dispatch claim does: retain it until operator/terminal
        # reconciliation can prove that no production remains active.
        if lease_created and not bool(replay.get("claim_pending")):
            release_production_lease(dispatch_id)
        return replay
    if not claim_token:  # defensive: helper always returns a claim or a replay
        if lease_created:
            release_production_lease(dispatch_id)
        raise RunPodDispatchRejected("RunPod dispatch claim could not be established")

    admission_token = ""
    # Nothing has been submitted yet. Any health/configuration failure is safe
    # to retry, so remove our claims/lease before surfacing the error. The
    # queue admission lock deliberately spans GET /health through POST /run so
    # concurrent API processes cannot all observe the same spare queue slot.
    try:
        admission_token = _acquire_queue_admission(dispatch_id)
        preflight = preflight_runpod_endpoint(
            endpoint_id=endpoint_id,
            timeout_sec=preflight_timeout_sec,
            max_queue_depth=max_queue_depth,
        )
        _update_dispatch_claim(
            dispatch_id,
            claim_token,
            phase="preflight_passed",
            endpoint_id=str(preflight.get("endpoint_id") or ""),
        )
        _update_dispatch_claim(dispatch_id, claim_token, phase="submitting")
    except Exception:
        if admission_token:
            _release_queue_admission(dispatch_id, admission_token)
        _release_dispatch_claim(dispatch_id, claim_token)
        release_production_lease(dispatch_id)
        raise

    # Once POST /run begins, a timeout or lost response is ambiguous: RunPod may
    # already have accepted the work. Persist a fail-closed receipt and retain
    # the claim so no retry can create a second billable job.
    try:
        response, resolved_endpoint = _request_json(
            method="POST",
            path="/run",
            endpoint_id=endpoint_id,
            body={"input": envelope},
            timeout_sec=dispatch_timeout_sec,
        )
        runpod_job_id = str(response.get("id") or "").strip()
        runpod_status = str(response.get("status") or "IN_QUEUE").strip().upper()
        if not runpod_job_id:
            raise RunPodDispatchRejected(
                "RunPod accepted no identifiable production job",
                detail=json.dumps(response, ensure_ascii=False)[:500],
            )
        if runpod_status in {"FAILED", "CANCELLED", "TIMED_OUT"}:
            raise RunPodDispatchRejected(
                f"RunPod rejected production dispatch with status {runpod_status}",
                detail=str(response.get("error") or "")[:500],
            )
        receipt = {
            "ok": True,
            "status": "accepted",
            "dispatch_id": dispatch_id,
            "command_id": str(envelope.get("command_id") or ""),
            "tool": normalized_tool,
            "endpoint_id": resolved_endpoint,
            "runpod_job_id": runpod_job_id,
            "runpod_status": runpod_status,
            "preflight": preflight,
            "schema": RUNPOD_ENVELOPE_SCHEMA,
            "idempotent_replay": False,
            "submitted_at": time.time(),
            **_reservation_receipt_metadata(arguments, user_id),
        }
        if studio_job_id:
            receipt["studio_job_id"] = studio_job_id
        _save_dispatch_receipt(dispatch_id, receipt)
        # The durable receipt is authoritative once written.  A later failure
        # to annotate the claim must not replace a known accepted receipt with
        # an ambiguous-failure receipt.
        try:
            _update_dispatch_claim(
                dispatch_id,
                claim_token,
                phase="submitted",
                runpod_job_id=runpod_job_id,
                runpod_status=runpod_status,
            )
        except Exception:
            pass
        _release_queue_admission(dispatch_id, admission_token)
        return receipt
    except Exception as exc:
        failure = {
            "ok": False,
            "status": "dispatch_unknown",
            "dispatch_id": dispatch_id,
            "command_id": str(envelope.get("command_id") or ""),
            "tool": normalized_tool,
            "schema": RUNPOD_ENVELOPE_SCHEMA,
            "idempotent_replay": False,
            "fail_closed": True,
            "error": str(getattr(exc, "code", "") or exc.__class__.__name__),
            "detail": str(exc)[:500],
            "submitted_at": time.time(),
            **_reservation_receipt_metadata(arguments, user_id),
        }
        if studio_job_id:
            failure["studio_job_id"] = studio_job_id
        try:
            _save_dispatch_receipt(dispatch_id, failure)
            _update_dispatch_claim(
                dispatch_id,
                claim_token,
                phase="dispatch_unknown",
                error=failure["error"],
            )
        except Exception:
            # The O_EXCL claim itself remains the final duplicate-spend guard
            # even when the receipt filesystem becomes unwritable.
            pass
        _release_queue_admission(dispatch_id, admission_token)
        raise


__all__ = [
    "RUNPOD_PRODUCTION_TOOL_ALLOWLIST",
    "RunPodBridgeError",
    "RunPodConfigurationError",
    "RunPodDispatchPolicyError",
    "RunPodDispatchRejected",
    "RunPodEndpointBacklogged",
    "RunPodEndpointUnavailable",
    "RunPodPaymentRequired",
    "acquire_production_lease",
    "dispatch_ledger_dir",
    "dispatch_production_tool",
    "get_dispatch_receipt",
    "get_dispatch_receipt_by_studio_job_id",
    "get_runpod_job_status",
    "preflight_runpod_endpoint",
    "release_production_lease",
    "runpod_configured",
]
