"""Idempotent execution adapter for validated Studio commands."""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Literal, Protocol

from pydantic import Field

from studio_agent.command_contract import ContractModel
from studio_agent.command_validation import (
    CommandValidationResult,
    ExpandPostconditions,
    SceneRepairPostconditions,
)


ExecutionStatus = Literal["accepted", "completed", "duplicate", "rejected", "failed"]


class ExecutionReceipt(ContractModel):
    schema_version: Literal["studio-receipt-v1"] = "studio-receipt-v1"
    execution_id: str
    idempotency_key: str
    command_id: str
    status: ExecutionStatus
    tool_name: str
    target_job_id: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] = Field(default_factory=dict)
    error: str = ""
    started_at: float
    finished_at: float | None = None
    expected: ExpandPostconditions | SceneRepairPostconditions | None = None
    duplicate_of: str = ""

    def legacy_result_text(self) -> str:
        """Return the existing runner/tool-observation JSON shape."""

        payload = dict(self.result)
        if self.error and "error" not in payload:
            payload["error"] = self.error
        return json.dumps(payload, indent=2, ensure_ascii=False)


class ExecutionLedger(Protocol):
    def get(self, idempotency_key: str) -> ExecutionReceipt | None: ...

    def claim(self, idempotency_key: str, command_id: str) -> bool: ...

    def save(self, receipt: ExecutionReceipt) -> None: ...


class InMemoryExecutionLedger:
    """Thread-safe ledger suitable for tests and a single application process."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._claimed: dict[str, str] = {}
        self._receipts: dict[str, ExecutionReceipt] = {}

    def get(self, idempotency_key: str) -> ExecutionReceipt | None:
        with self._lock:
            return self._receipts.get(idempotency_key)

    def claim(self, idempotency_key: str, command_id: str) -> bool:
        with self._lock:
            if idempotency_key in self._claimed:
                return False
            self._claimed[idempotency_key] = command_id
            return True

    def save(self, receipt: ExecutionReceipt) -> None:
        with self._lock:
            self._claimed.setdefault(receipt.idempotency_key, receipt.command_id)
            self._receipts[receipt.idempotency_key] = receipt


class FileExecutionLedger:
    """Cross-process-safe claim files plus atomically replaced receipt JSON."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _receipt_path(self, key: str) -> Path:
        return self.root / f"{key}.json"

    def _claim_path(self, key: str) -> Path:
        return self.root / f"{key}.claim"

    def _claim_lock_path(self, key: str) -> Path:
        return self.root / f"{key}.claim.lock"

    @contextmanager
    def _claim_guard(self, key: str) -> Iterator[None]:
        """Serialize leased-claim comparisons across API worker processes."""

        with self._claim_lock_path(key).open("a+b") as handle:
            handle.seek(0, 2)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _read_claim_unlocked(
        self,
        key: str,
        *,
        legacy_lease_seconds: float,
    ) -> dict[str, Any] | None:
        path = self._claim_path(key)
        if not path.is_file():
            return None
        try:
            raw = path.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            return dict(parsed)
        # Claims written by pre-lease releases contained only the command id.
        # Their mtime becomes a bounded compatibility lease so an abandoned
        # file can no longer stall a durable workflow forever.
        try:
            claimed_at = float(path.stat().st_mtime)
        except OSError:
            claimed_at = 0.0
        return {
            "command_id": raw,
            "owner_token": "",
            "claimed_at": claimed_at,
            "expires_at": claimed_at + max(0.1, float(legacy_lease_seconds)),
        }

    def _write_claim_unlocked(self, key: str, payload: dict[str, Any]) -> None:
        destination = self._claim_path(key)
        temporary = self.root / f".{key}.{uuid.uuid4().hex}.claim.tmp"
        temporary.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(temporary, destination)

    def get(self, idempotency_key: str) -> ExecutionReceipt | None:
        path = self._receipt_path(idempotency_key)
        if not path.is_file():
            return None
        try:
            return ExecutionReceipt.model_validate_json(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def claim(self, idempotency_key: str, command_id: str) -> bool:
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            descriptor = os.open(self._claim_path(idempotency_key), flags)
        except FileExistsError:
            return False
        try:
            os.write(descriptor, str(command_id or "").encode("utf-8"))
        finally:
            os.close(descriptor)
        return True

    def claim_with_lease(
        self,
        idempotency_key: str,
        command_id: str,
        *,
        owner_token: str,
        lease_seconds: float,
    ) -> bool:
        """Acquire an expiring claim that can be recovered after process death."""

        key = str(idempotency_key or "").strip()
        owner = str(owner_token or "").strip()
        command = str(command_id or "").strip()
        if not key or not owner or not command:
            return False
        duration = max(0.1, float(lease_seconds))
        now = time.time()
        with self._claim_guard(key):
            if self._receipt_path(key).is_file():
                return False
            current = self._read_claim_unlocked(
                key,
                legacy_lease_seconds=duration,
            )
            current_owner = str((current or {}).get("owner_token") or "")
            current_expiry = float((current or {}).get("expires_at") or 0.0)
            if current is not None and current_owner != owner and current_expiry > now:
                return False
            self._write_claim_unlocked(
                key,
                {
                    "command_id": command,
                    "owner_token": owner,
                    "claimed_at": now,
                    "expires_at": now + duration,
                },
            )
            return True

    def renew_claim(
        self,
        idempotency_key: str,
        *,
        owner_token: str,
        lease_seconds: float,
    ) -> bool:
        """Renew only the caller's live claim."""

        key = str(idempotency_key or "").strip()
        owner = str(owner_token or "").strip()
        if not key or not owner:
            return False
        duration = max(0.1, float(lease_seconds))
        with self._claim_guard(key):
            if self._receipt_path(key).is_file():
                return False
            current = self._read_claim_unlocked(
                key,
                legacy_lease_seconds=duration,
            )
            if str((current or {}).get("owner_token") or "") != owner:
                return False
            now = time.time()
            current = dict(current or {})
            current["expires_at"] = now + duration
            current["renewed_at"] = now
            self._write_claim_unlocked(key, current)
            return True

    def save_if_claim_owner(
        self,
        receipt: ExecutionReceipt,
        *,
        owner_token: str,
        lease_seconds: float,
    ) -> bool:
        """Persist a leased result only if the caller still owns the claim."""

        key = str(receipt.idempotency_key or "").strip()
        owner = str(owner_token or "").strip()
        if not key or not owner:
            return False
        with self._claim_guard(key):
            current = self._read_claim_unlocked(
                key,
                legacy_lease_seconds=max(0.1, float(lease_seconds)),
            )
            if str((current or {}).get("owner_token") or "") != owner:
                return False
            self._save_unlocked(receipt)
            return True

    def _save_unlocked(self, receipt: ExecutionReceipt) -> None:
        destination = self._receipt_path(receipt.idempotency_key)
        temporary = self.root / f".{receipt.idempotency_key}.{uuid.uuid4().hex}.tmp"
        temporary.write_text(receipt.model_dump_json(indent=2), encoding="utf-8")
        os.replace(temporary, destination)

    def save(self, receipt: ExecutionReceipt) -> None:
        self._save_unlocked(receipt)


def _default_ledger_root() -> Path:
    configured = str(os.getenv("STUDIO_COMMAND_LEDGER_DIR") or "").strip()
    if configured:
        return Path(configured).expanduser()
    app_data = str(os.getenv("APP_DATA_DIR") or "").strip()
    if app_data:
        return Path(app_data).expanduser() / "studio_command_execution"
    from studio_agent.fs_paths import data_root

    return data_root() / "studio_command_execution"


_DEFAULT_LEDGER = FileExecutionLedger(_default_ledger_root())


def _idempotency_key(validation: CommandValidationResult) -> str:
    action = validation.resolved_action
    payload = {
        "command_id": validation.command_id,
        "tool": action.tool_name if action else "",
        "arguments": action.arguments.model_dump(mode="json", exclude_none=True) if action else {},
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _parse_result(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {"result": parsed}
    except Exception:
        try:
            from json_repair import repair_json

            parsed = repair_json(text, return_objects=True)
            return parsed if isinstance(parsed, dict) else {"raw_result": text}
        except Exception:
            return {"raw_result": text}


def execute_validated_command(
    validation: CommandValidationResult,
    *,
    user_id: str,
    session_id: str,
    content_format: str,
    tool_executor: Any,
    ledger: ExecutionLedger | None = None,
) -> ExecutionReceipt:
    """Call the legacy executor once and record async acceptance separately."""

    ledger = ledger or _DEFAULT_LEDGER
    now = time.time()
    key = _idempotency_key(validation)
    action = validation.resolved_action
    if not validation.can_execute or action is None:
        message = (
            validation.clarification.question
            if validation.clarification is not None
            else "Command did not pass validation."
        )
        return ExecutionReceipt(
            execution_id=f"exec_{uuid.uuid4().hex[:20]}",
            idempotency_key=key,
            command_id=validation.command_id,
            status="rejected",
            tool_name="",
            target_job_id="",
            error=message,
            started_at=now,
            finished_at=now,
        )
    previous = ledger.get(key)
    if previous is not None:
        replay_status: ExecutionStatus = (
            previous.status if previous.status in {"failed", "rejected"} else "duplicate"
        )
        return ExecutionReceipt(
            execution_id=f"exec_{uuid.uuid4().hex[:20]}",
            idempotency_key=key,
            command_id=validation.command_id,
            status=replay_status,
            tool_name=action.tool_name,
            target_job_id=action.arguments.job_id,
            arguments=action.arguments.as_legacy_dict(),
            result=previous.result,
            error=previous.error if replay_status in {"failed", "rejected"} else "",
            started_at=now,
            finished_at=now,
            expected=action.expected,
            duplicate_of=previous.execution_id,
        )
    if not ledger.claim(key, validation.command_id):
        previous = ledger.get(key)
        replay_status: ExecutionStatus = (
            previous.status
            if previous is not None and previous.status in {"failed", "rejected"}
            else "duplicate"
        )
        return ExecutionReceipt(
            execution_id=f"exec_{uuid.uuid4().hex[:20]}",
            idempotency_key=key,
            command_id=validation.command_id,
            status=replay_status,
            tool_name=action.tool_name,
            target_job_id=action.arguments.job_id,
            arguments=action.arguments.as_legacy_dict(),
            result=(
                previous.result
                if previous
                else {
                    "ok": True,
                    "status": "claim_pending",
                    "idempotency_claim_pending": True,
                    "note": "Another process holds this command claim; dispatch is not yet confirmed.",
                }
            ),
            error=(
                previous.error
                if previous is not None and replay_status in {"failed", "rejected"}
                else ""
            ),
            started_at=now,
            finished_at=now,
            expected=action.expected,
            duplicate_of=previous.execution_id if previous else "",
        )

    result: dict[str, Any] = {}
    error = ""
    status: ExecutionStatus = "accepted"
    try:
        raw = tool_executor(
            action.tool_name,
            action.arguments.as_legacy_dict(),
            user_id=user_id,
            content_format=content_format,
            session_id=session_id,
        )
        result = _parse_result(raw)
        result_status = str(result.get("status") or "").lower()
        if result.get("error") or result.get("ok") is False or result_status in {"failed", "error", "cancelled"}:
            status = "failed"
            error = str(result.get("error") or result.get("message") or result_status or "tool failed")
        elif result.get("idempotent_replay") is True:
            # The durable render tool may recognize an equivalent in-flight
            # command compiled by another HTTP turn/process. Surface that as a
            # duplicate receipt instead of pretending a new worker was started.
            status = "duplicate"
        elif action.tool_name == "audit_and_repair_production_scenes":
            # This tool performs its selected-scene audit and any required
            # repairs synchronously. Completion is still subject to observed
            # postconditions below; this status only describes the tool call.
            status = "completed"
        else:
            # expand_visual_proof_shortform starts a background thread. Even a
            # truthy return proves dispatch only; postcondition verification is
            # the sole authority allowed to mark the production complete.
            status = "accepted"
    except Exception as exc:
        status = "failed"
        error = str(exc)
        result = {"error": error}
    finished = time.time()
    receipt = ExecutionReceipt(
        execution_id=f"exec_{uuid.uuid4().hex[:20]}",
        idempotency_key=key,
        command_id=validation.command_id,
        status=status,
        tool_name=action.tool_name,
        target_job_id=action.arguments.job_id,
        arguments=action.arguments.as_legacy_dict(),
        result=result,
        error=error,
        started_at=now,
        finished_at=finished,
        expected=action.expected,
    )
    ledger.save(receipt)
    return receipt
