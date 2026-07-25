"""Read-only RunPod job projection and one-time control-plane billing.

The RunPod worker never owns the Studio wallet.  It emits provider-cost and
reservation intent facts; this module applies those facts on the control plane
after a terminal ``GET /status`` response.  The O_EXCL claim is deliberately
fail-closed: after a wallet mutation may have started, no second process is
allowed to guess whether it should repeat that mutation.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


_RECONCILIATION_STOP = threading.Event()
_RECONCILIATION_THREAD: threading.Thread | None = None
_RECONCILIATION_THREAD_LOCK = threading.RLock()
_RECONCILIATION_POLL_SECONDS = max(
    2.0,
    float(os.getenv("STUDIO_RUNPOD_RECONCILIATION_POLL_SECONDS", "10") or 10),
)


_STATE_KEYS = (
    "status",
    "stage",
    "phase",
    "stage_label",
    "progress",
    "percent",
    "running",
    "scene_done",
    "scene_total",
    "detail",
    "error",
)


def runpod_production_enabled() -> bool:
    from studio_agent.runpod_contract import runpod_production_enabled as enabled

    return enabled()


def reconciliation_ledger_dir() -> Path:
    configured = str(os.getenv("RUNPOD_RECONCILIATION_LEDGER_DIR") or "").strip()
    if configured:
        root = Path(configured).expanduser()
    else:
        app_data = str(os.getenv("APP_DATA_DIR") or "").strip()
        if app_data:
            root = Path(app_data).expanduser() / "runpod_billing_reconciliation" / "control_plane"
        else:
            from studio_agent.fs_paths import data_root

            root = data_root() / "runpod_billing_reconciliation" / "control_plane"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _safe_decimal(value: Any) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not parsed.is_finite() or parsed < 0:
        return None
    return parsed


def _worker_facts(output: Any) -> list[dict[str, Any]]:
    if not isinstance(output, dict):
        return []
    billing = output.get("billing")
    if not isinstance(billing, dict):
        return []
    facts = billing.get("provider_cost_facts")
    return [dict(fact) for fact in facts if isinstance(fact, dict)] if isinstance(facts, list) else []


def _authoritative_provider_facts(output: Any, facts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(output, dict):
        return []
    billing = output.get("billing")
    if not isinstance(billing, dict) or billing.get("report_complete") is not True:
        return []
    verified: list[dict[str, Any]] = []
    for fact in facts:
        if (
            str(fact.get("kind") or "") != "provider_usd"
            or fact.get("authoritative") is not True
            or fact.get("operation_scoped") is not True
        ):
            continue
        value = fact.get("provider_usd_decimal", fact.get("provider_usd"))
        if _safe_decimal(value) is not None:
            verified.append(fact)
    # One signed dispatch represents exactly one operation and therefore one
    # authoritative delta. Multiple facts are ambiguous rather than additive.
    return verified if len(verified) == 1 else []


def _provider_total_usd(output: Any, facts: list[dict[str, Any]]) -> Decimal:
    provider_facts = [fact for fact in facts if str(fact.get("kind") or "") == "provider_usd"]
    if provider_facts:
        total = Decimal("0")
        for fact in provider_facts:
            value = fact.get("provider_usd_decimal")
            if value is None:
                value = fact.get("provider_usd")
            parsed = _safe_decimal(value)
            if parsed is not None:
                total += parsed
        return total

    # A job snapshot is cumulative, so use one total as a fallback rather than
    # adding multiple copies of the same snapshot.
    candidates: list[Any] = []
    for fact in facts:
        if str(fact.get("kind") or "") != "job_cost_snapshot":
            continue
        cost = fact.get("cost")
        if isinstance(cost, dict):
            candidates.append(
                cost.get("actual_usd", cost.get("total_usd", cost.get("provider_usd")))
            )
    if isinstance(output, dict):
        snapshot = output.get("job_snapshot")
        if isinstance(snapshot, dict) and isinstance(snapshot.get("cost"), dict):
            cost = snapshot["cost"]
            candidates.append(
                cost.get("actual_usd", cost.get("total_usd", cost.get("provider_usd")))
            )
        result = output.get("result")
        if isinstance(result, dict) and isinstance(result.get("cost"), dict):
            cost = result["cost"]
            candidates.append(
                cost.get("actual_usd", cost.get("total_usd", cost.get("provider_usd")))
            )
    for value in candidates:
        parsed = _safe_decimal(value)
        if parsed is not None:
            return parsed
    return Decimal("0")


def _reservation_fact(
    facts: list[dict[str, Any]],
    receipt: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    candidates: list[tuple[float, int, dict[str, Any]]] = []
    for index, fact in enumerate(facts):
        kind = str(fact.get("kind") or "")
        if kind not in {"reservation_commit", "reservation_release"}:
            continue
        user_id = str(fact.get("user_id") or "").strip()
        reservation_id = str(fact.get("reservation_id") or "").strip()
        # Worker-created holds are local accounting placeholders, not original
        # control-plane reservations and must never be applied to the wallet.
        if not user_id or not reservation_id or reservation_id.startswith("worker_"):
            continue
        try:
            timestamp = float(fact.get("ts") or 0.0)
        except (TypeError, ValueError):
            timestamp = 0.0
        candidates.append((timestamp, index, fact))
    if candidates:
        return dict(max(candidates, key=lambda item: (item[0], item[1]))[2])
    receipt = receipt or {}
    user_id = str(receipt.get("billing_user_id") or "").strip()
    reservation_id = str(receipt.get("credit_reservation_id") or "").strip()
    if user_id and reservation_id and not bool(receipt.get("credit_reservation_unlimited")):
        return {
            "kind": "control_plane_reservation",
            "user_id": user_id,
            "reservation_id": reservation_id,
        }
    return None


def _ledger_paths(dispatch_id: str, user_id: str, reservation_id: str) -> tuple[Path, Path]:
    digest = hashlib.sha256(
        f"{dispatch_id}\0{user_id}\0{reservation_id}".encode("utf-8")
    ).hexdigest()
    root = reconciliation_ledger_dir()
    return root / f"{digest}.claim.json", root / f"{digest}.receipt.json"


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
        return parsed if isinstance(parsed, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _atomic_write(path: Path, payload: dict[str, Any]) -> None:
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


def _claim_once(claim_path: Path, payload: dict[str, Any]) -> bool:
    try:
        descriptor = os.open(str(claim_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return False
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        handle.flush()
        os.fsync(handle.fileno())
    return True


def reconcile_terminal_billing(
    status: dict[str, Any],
    receipt: dict[str, Any],
) -> dict[str, Any]:
    """Apply terminal worker billing facts at most once across processes."""

    if not bool(status.get("terminal")):
        return {"state": "pending_worker_completion"}
    output = status.get("output")
    receipt_dispatch_id = str(receipt.get("dispatch_id") or "").strip()
    output_dispatch_id = str(
        output.get("dispatch_id") if isinstance(output, dict) else ""
    ).strip()
    if receipt_dispatch_id and output_dispatch_id and receipt_dispatch_id != output_dispatch_id:
        return {
            "state": "dispatch_identity_mismatch",
            "wallet_mutated": False,
            "fail_closed": True,
        }
    dispatch_id = output_dispatch_id or receipt_dispatch_id
    if not dispatch_id:
        return {"state": "missing_dispatch_identity", "wallet_mutated": False}
    # Every wallet-relevant worker fact is written with its dispatch identity.
    # Ignore stale/unattributed facts instead of risking cross-job billing.
    facts = [
        fact
        for fact in _worker_facts(output)
        if str(fact.get("dispatch_id") or "").strip() == dispatch_id
    ]
    authoritative_facts = _authoritative_provider_facts(output, facts)
    if not authoritative_facts:
        return {
            "provider_usd": 0.0,
            "provider_usd_decimal": "0",
            "state": "worker_billing_report_incomplete",
            "wallet_mutated": False,
            "fail_closed": True,
        }
    total_usd = _provider_total_usd(output, authoritative_facts)
    reservation = _reservation_fact(facts, receipt)
    base = {
        "provider_usd": float(total_usd),
        "provider_usd_decimal": str(total_usd),
    }
    if reservation is None:
        return {**base, "state": "missing_reservation_fact", "wallet_mutated": False}
    user_id = str(reservation.get("user_id") or "").strip()
    reservation_id = str(reservation.get("reservation_id") or "").strip()
    claim_path, receipt_path = _ledger_paths(dispatch_id, user_id, reservation_id)
    prior = _read_json(receipt_path)
    if prior is not None:
        return {**prior, "idempotent_replay": True}

    claimed = _claim_once(
        claim_path,
        {
            "dispatch_id": dispatch_id,
            "user_id": user_id,
            "reservation_id": reservation_id,
            "claimed_at": time.time(),
            "pid": os.getpid(),
        },
    )
    if not claimed:
        # A receipt may have landed between our first read and O_EXCL failure.
        prior = _read_json(receipt_path)
        if prior is not None:
            return {**prior, "idempotent_replay": True}
        return {
            **base,
            "state": "reconciliation_in_progress_or_unknown",
            "wallet_mutated": False,
            "reservation_id": reservation_id,
            "fail_closed": True,
        }

    failed = str(status.get("status") or "").lower() == "failed"
    try:
        reported_actual_credits = max(0, int(reservation.get("actual_credits") or 0))
    except (TypeError, ValueError):
        reported_actual_credits = 0
    # A failed render can still have incurred real provider spend. Match the
    # local billing policy: charge actuals when present; release only when the
    # worker proves there was no billable spend.
    release = total_usd <= 0 and reported_actual_credits <= 0
    try:
        import unified_credits

        if release:
            unified_credits.release_reservation(
                user_id,
                reservation_id,
                reason="runpod_terminal_failed" if failed else "runpod_no_provider_spend",
            )
            result = {
                **base,
                "state": "released",
                "wallet_mutated": True,
                "credits_charged": 0,
                "reservation_id": reservation_id,
                "user_id": user_id,
                "dispatch_id": dispatch_id,
            }
        else:
            actual_credits = (
                unified_credits.usd_to_credits(total_usd)
                if total_usd > 0
                else reported_actual_credits
            )
            settlement = unified_credits.settle_reservation(
                user_id,
                reservation_id,
                actual_credits=actual_credits,
                reason="runpod_worker_actuals",
                metadata={
                    "dispatch_id": dispatch_id,
                    "runpod_job_id": str(status.get("runpod_job_id") or ""),
                    "provider_usd": float(total_usd),
                    "provider_usd_decimal": str(total_usd),
                },
            )
            if bool(settlement.get("reservation_missing")):
                return {
                    **base,
                    "state": "reservation_missing",
                    "wallet_mutated": False,
                    "reservation_id": reservation_id,
                    "fail_closed": True,
                }
            charged = max(0, int(settlement.get("credits_charged", actual_credits) or 0))
            result = {
                **base,
                "state": "settled",
                "wallet_mutated": True,
                "credits_charged": charged,
                "overage_credits": max(0, int(settlement.get("overage_credits", 0) or 0)),
                "reservation_id": reservation_id,
                "user_id": user_id,
                "dispatch_id": dispatch_id,
            }
        _atomic_write(receipt_path, result)
        return result
    except Exception as exc:
        # The wallet call may have succeeded before its caller saw an error.
        # Keep the O_EXCL claim and never risk a duplicate mutation on polling.
        return {
            **base,
            "state": "reconciliation_unknown",
            "wallet_mutated": False,
            "reservation_id": reservation_id,
            "fail_closed": True,
            "error": str(exc)[:240],
        }


def project_runpod_job_snapshot(
    job_id: str,
    kind: str,
    local_snapshot: dict[str, Any],
    *,
    reconcile: bool = False,
) -> dict[str, Any]:
    """Overlay RunPod progress; side effects require a backend worker."""

    result = dict(local_snapshot or {})
    result["job_id"] = str(job_id or "")
    result["kind"] = str(kind or "")
    if not runpod_production_enabled():
        return result

    try:
        from studio_agent import runpod_bridge

        receipt = runpod_bridge.get_dispatch_receipt_by_studio_job_id(job_id)
    except Exception as exc:
        result["runpod"] = {"status": "receipt_lookup_unavailable", "detail": str(exc)[:240]}
        return result
    if not receipt:
        return result
    runpod_job_id = str(receipt.get("runpod_job_id") or "").strip()
    if not runpod_job_id:
        result["runpod"] = {
            "status": str(receipt.get("status") or "dispatch_unknown"),
            "dispatch_id": str(receipt.get("dispatch_id") or ""),
            "billing_reconciliation": {"state": "awaiting_identifiable_runpod_job"},
        }
        return result

    try:
        status = runpod_bridge.get_runpod_job_status(
            runpod_job_id,
            endpoint_id=str(receipt.get("endpoint_id") or "") or None,
        )
    except Exception as exc:
        result["runpod"] = {
            "status": "status_unavailable",
            "runpod_job_id": runpod_job_id,
            "dispatch_id": str(receipt.get("dispatch_id") or ""),
            "detail": str(exc)[:240],
            "billing_reconciliation": {"state": "status_unavailable"},
        }
        return result

    dispatch_id = str(receipt.get("dispatch_id") or "")
    storage_sync: dict[str, Any] | None = None
    artifacts_local = False
    if (
        reconcile
        and bool(status.get("terminal"))
        and str(kind or "") in {"shortform", "longform"}
    ):
        try:
            from studio_agent import runpod_storage

            storage_sync = runpod_storage.sync_job_workspace(job_id, kind, dispatch_id)
            artifacts_local = bool(
                not storage_sync.get("pending")
                and int(storage_sync.get("files_downloaded") or 0) > 0
            )
        except Exception as exc:
            storage_sync = {
                "ok": False,
                "status": "sync_unavailable",
                "error": str(exc)[:240],
            }
        if (
            isinstance(storage_sync, dict)
            and bool(storage_sync.get("ok"))
            and not bool(storage_sync.get("pending"))
            and dispatch_id
        ):
            try:
                from studio_agent import runpod_bridge

                storage_sync["production_lease_released"] = bool(
                    runpod_bridge.release_production_lease(dispatch_id)
                )
            except Exception as exc:
                storage_sync["production_lease_release_error"] = str(exc)[:240]

    # Once the network-volume workspace has been transferred successfully, it
    # is safe to expose Fly/local URLs and rich scene metadata. Before that,
    # worker-local paths are deliberately kept nested and non-clickable.
    if artifacts_local:
        try:
            from studio_agent import jobs as studio_jobs

            local_after_sync = (
                studio_jobs._shortform_status(job_id)
                if str(kind or "") == "shortform"
                else studio_jobs._longform_status(job_id)
            )
            if isinstance(local_after_sync, dict):
                result.update(local_after_sync)
        except Exception as exc:
            if storage_sync is not None:
                storage_sync = dict(storage_sync)
                storage_sync["local_projection_error"] = str(exc)[:240]

    worker_snapshot = status.get("job_snapshot")
    if isinstance(worker_snapshot, dict):
        for key in _STATE_KEYS:
            if key in worker_snapshot:
                result[key] = worker_snapshot[key]
    else:
        for key in _STATE_KEYS:
            if key in status:
                result[key] = status[key]
    billing = (
        reconcile_terminal_billing(status, receipt)
        if reconcile
        else {
            "state": (
                "background_reconciliation_pending"
                if bool(status.get("terminal"))
                else "pending_worker_completion"
            ),
            "wallet_mutated": False,
        }
    )
    result["runpod_job_id"] = runpod_job_id
    result["execution_backend"] = "runpod_serverless"
    result["runpod_status"] = str(status.get("runpod_status") or "")
    result["runpod_terminal"] = bool(status.get("terminal"))
    result["runpod"] = {
        "status": str(status.get("status") or "unknown"),
        "stage": str(status.get("stage") or ""),
        "stage_label": str(status.get("stage_label") or ""),
        "running": bool(status.get("running")),
        "terminal": bool(status.get("terminal")),
        "progress": status.get("progress"),
        "runpod_status": str(status.get("runpod_status") or ""),
        "runpod_job_id": runpod_job_id,
        "dispatch_id": dispatch_id,
        "artifacts_local": artifacts_local,
        "billing_reconciliation": billing,
    }
    if storage_sync is not None:
        result["runpod"]["workspace_sync"] = storage_sync
    if isinstance(worker_snapshot, dict):
        # Preserve the worker's durable state for inspection, but do not copy
        # media URLs/paths to the local snapshot where the files do not exist.
        result["runpod"]["worker_job_snapshot"] = dict(worker_snapshot)
    result["job_id"] = str(job_id or "")
    result["kind"] = str(kind or "")
    return result


def _background_marker_path(dispatch_id: str) -> Path:
    digest = hashlib.sha256(str(dispatch_id or "").encode("utf-8")).hexdigest()
    return reconciliation_ledger_dir() / f"background-{digest}.json"


def reconcile_pending_runpod_jobs_once(*, limit: int = 500) -> int:
    """Reconcile terminal RunPod work outside browser/status requests."""

    if not runpod_production_enabled():
        return 0
    from studio_agent import runpod_bridge

    reconciled = 0
    for receipt in runpod_bridge.list_dispatch_receipts(limit=limit):
        dispatch_id = str(receipt.get("dispatch_id") or "").strip()
        job_id = str(receipt.get("studio_job_id") or receipt.get("job_id") or "").strip()
        runpod_job_id = str(receipt.get("runpod_job_id") or "").strip()
        if not dispatch_id or not job_id or not runpod_job_id:
            continue
        marker = _background_marker_path(dispatch_id)
        if marker.is_file():
            continue
        tool = str(receipt.get("tool") or "").strip().lower()
        kind = "longform" if "longform" in tool else "shortform"
        snapshot = project_runpod_job_snapshot(
            job_id,
            kind,
            {
                "job_id": job_id,
                "kind": kind,
                "status": "queued",
                "stage": "runpod_dispatch",
                "running": True,
            },
            reconcile=True,
        )
        if not bool(snapshot.get("runpod_terminal")):
            continue
        runpod = snapshot.get("runpod") if isinstance(snapshot.get("runpod"), dict) else {}
        storage = (
            runpod.get("workspace_sync")
            if isinstance(runpod.get("workspace_sync"), dict)
            else {}
        )
        if storage and storage.get("ok") is False:
            continue
        _atomic_write(
            marker,
            {
                "dispatch_id": dispatch_id,
                "job_id": job_id,
                "kind": kind,
                "reconciled_at": time.time(),
                "billing": dict(runpod.get("billing_reconciliation") or {}),
                "workspace_sync": dict(storage or {}),
            },
        )
        reconciled += 1
    return reconciled


def _reconciliation_monitor() -> None:
    while not _RECONCILIATION_STOP.is_set():
        try:
            reconcile_pending_runpod_jobs_once()
        except Exception:
            pass
        _RECONCILIATION_STOP.wait(_RECONCILIATION_POLL_SECONDS)


def start_runpod_reconciliation_service() -> None:
    global _RECONCILIATION_THREAD
    with _RECONCILIATION_THREAD_LOCK:
        if _RECONCILIATION_THREAD is not None and _RECONCILIATION_THREAD.is_alive():
            return
        _RECONCILIATION_STOP.clear()
        _RECONCILIATION_THREAD = threading.Thread(
            target=_reconciliation_monitor,
            daemon=True,
            name="runpod-control-plane-reconciliation",
        )
        _RECONCILIATION_THREAD.start()


def stop_runpod_reconciliation_service() -> None:
    _RECONCILIATION_STOP.set()


__all__ = [
    "project_runpod_job_snapshot",
    "reconcile_pending_runpod_jobs_once",
    "reconcile_terminal_billing",
    "reconciliation_ledger_dir",
    "runpod_production_enabled",
    "start_runpod_reconciliation_service",
    "stop_runpod_reconciliation_service",
]
