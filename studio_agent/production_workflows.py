"""Durable backend-owned production workflow execution.

The chat request only admits the command. This module owns every later
mutation and advances solely from durable postconditions, so a browser
disconnect or API process restart cannot reorder animation and finalization.
"""
from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
import uuid
from typing import Any

from studio_agent import store
from studio_agent.execution_context import production_command_scope


_LOG = logging.getLogger("nyptid-studio.production-workflows")
_POLL_SECONDS = max(
    0.25,
    float(os.getenv("STUDIO_PRODUCTION_WORKFLOW_POLL_SECONDS", "5") or 5),
)
_MONITOR_SECONDS = max(
    1.0,
    float(os.getenv("STUDIO_PRODUCTION_WORKFLOW_MONITOR_SECONDS", "10") or 10),
)
_LEASE_SECONDS = max(
    15.0,
    float(os.getenv("STUDIO_PRODUCTION_WORKFLOW_LEASE_SECONDS", "60") or 60),
)
_MAX_AGE_SECONDS = max(
    300.0,
    float(os.getenv("STUDIO_PRODUCTION_WORKFLOW_MAX_AGE_SECONDS", str(12 * 60 * 60)) or 0),
)
_PROCESS_OWNER = (
    f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:12]}"
)
_THREADS_LOCK = threading.RLock()
_ACTIVE_THREADS: dict[str, threading.Thread] = {}
_STOP = threading.Event()
_MONITOR_THREAD: threading.Thread | None = None


def _parse_result(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    try:
        parsed = json.loads(str(raw or "{}"))
    except Exception:
        return {"status": "unverifiable", "error": str(raw or "")[:500]}
    return dict(parsed) if isinstance(parsed, dict) else {"result": parsed}


def _job_snapshot(job_id: str) -> dict[str, Any]:
    from studio_agent.jobs import get_job_snapshot

    return dict(get_job_snapshot(job_id, "shortform") or {})


def _finalize_preflight(job_id: str) -> dict[str, Any]:
    from studio_agent.tools import shortform_finalize_preflight

    return dict(shortform_finalize_preflight(job_id) or {})


def _approval_is_durable(workflow: dict[str, Any]) -> bool:
    from skeleton_ai.styled_pipeline import load_scenes
    from studio_agent.tools import _shortform_workspace

    wanted = {int(value) for value in workflow.get("scene_indices") or []}
    animate = bool(workflow.get("animate"))
    rows = load_scenes(_shortform_workspace(str(workflow.get("job_id") or "")))
    observed: set[int] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            index = int(row.get("index"))
        except (TypeError, ValueError):
            continue
        if index not in wanted:
            continue
        if row.get("approved_for_video") is not True:
            return False
        if bool(row.get("approved_for_animation")) is not animate:
            return False
        observed.add(index)
    return bool(wanted) and observed == wanted


def _animation_is_durable(workflow: dict[str, Any]) -> bool:
    if not bool(workflow.get("animate")):
        return True
    return str(_finalize_preflight(str(workflow.get("job_id") or "")).get("status") or "") == "ready"


def _missing_animation_indices(workflow: dict[str, Any]) -> list[int]:
    """Resolve only selected clips that are still absent from durable storage."""

    from skeleton_ai.styled_pipeline import load_scenes
    from studio_agent.tools import _shortform_workspace

    workspace = _shortform_workspace(str(workflow.get("job_id") or ""))
    selected = {
        int(value)
        for value in (
            workflow.get("animation_scene_indices")
            or workflow.get("scene_indices")
            or []
        )
    }
    missing: list[int] = []
    for row in load_scenes(workspace):
        if not isinstance(row, dict):
            continue
        try:
            index = int(row.get("index"))
        except (TypeError, ValueError):
            continue
        if index not in selected:
            continue
        clip_rel = str(row.get("clip_rel") or "").strip()
        clip_path = workspace / clip_rel if clip_rel else None
        try:
            ready = bool(
                clip_path is not None
                and clip_path.is_file()
                and clip_path.stat().st_size > 0
            )
        except OSError:
            ready = False
        if not ready:
            missing.append(index)
    return sorted(set(missing))


def _final_is_durable(snapshot: dict[str, Any]) -> bool:
    return bool(
        str(snapshot.get("status") or "").strip().lower() == "complete"
        and snapshot.get("ready_to_post") is True
        and str(snapshot.get("download_url") or snapshot.get("mp4_url") or "").strip()
    )


def _definitive_failure(snapshot: dict[str, Any], *, finalizing: bool) -> str:
    status = str(snapshot.get("status") or "").strip().lower()
    stage = str(snapshot.get("stage") or snapshot.get("phase") or "").strip().lower()
    runpod = snapshot.get("runpod") if isinstance(snapshot.get("runpod"), dict) else {}
    running = bool(snapshot.get("running") or runpod.get("running"))
    runpod_terminal = bool(snapshot.get("runpod_terminal") or runpod.get("terminal"))
    failure_states = {
        "failed",
        "error",
        "cancelled",
        "canceled",
        "budget_exceeded",
        "visual_qa_failed",
        "render_qa_failed",
    }
    if finalizing:
        failure_states.update({"final_qa_blocked", "qa_blocked"})
    if (status in failure_states or stage in failure_states) and (not running or runpod_terminal):
        return str(
            snapshot.get("error")
            or snapshot.get("stage_detail")
            or runpod.get("detail")
            or f"production reached {status or stage}"
        )[:1000]
    return ""


def _root_transition(
    workflow: dict[str, Any],
    transition: str,
    *,
    result_status: str = "",
    error: str = "",
) -> None:
    store.record_production_command_transition(
        str(workflow.get("session_id") or ""),
        authority=dict(workflow.get("authority") or {}),
        mutation=dict(workflow.get("root_mutation") or {}),
        transition=transition,
        result_status=result_status,
        error=error,
    )


def _save(
    workflow: dict[str, Any],
    lease_owner: str,
    **fields: Any,
) -> dict[str, Any]:
    updated = store.update_production_workflow(
        str(workflow.get("session_id") or ""),
        str(workflow.get("workflow_id") or ""),
        lease_owner=lease_owner,
        fields=fields,
    )
    if not updated:
        raise RuntimeError("durable production workflow lease was lost")
    return updated


def _execute_tool_step(
    workflow: dict[str, Any],
    lease_owner: str,
    *,
    step: str,
    tool_name: str,
    arguments: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Execute one deterministic child mutation under the root command."""

    from studio_agent.tools import execute_tool_logged

    receipts = dict(workflow.get("step_receipts") or {})
    prior = receipts.get(step) if isinstance(receipts.get(step), dict) else {}
    attempts = max(0, int(prior.get("attempts") or 0)) + 1
    executing = {
        **dict(prior),
        "tool_name": tool_name,
        "arguments": {
            key: value
            for key, value in dict(arguments or {}).items()
            if not str(key).startswith("_")
        },
        "status": "executing",
        "attempts": attempts,
        "started_at": time.time(),
    }
    receipts[step] = executing
    workflow = _save(
        workflow,
        lease_owner,
        status="running",
        stage=step,
        step_receipts=receipts,
    )
    authority = dict(workflow.get("authority") or {})
    _root_transition(workflow, "executing", result_status=step)
    with production_command_scope(
        str(workflow.get("command_id") or ""),
        user_id=str(workflow.get("user_id") or ""),
        session_id=str(workflow.get("session_id") or ""),
        source=str(authority.get("source") or "server_workflow"),
        user_text=str(authority.get("execution_quote") or "ship existing short"),
        state_revision=int(authority.get("state_revision") or 0),
    ):
        raw = execute_tool_logged(
            tool_name,
            dict(arguments or {}),
            user_id=str(workflow.get("user_id") or ""),
            content_format="short",
            session_id=str(workflow.get("session_id") or ""),
        )
    result = _parse_result(raw)
    failure_statuses = {
        "failed",
        "error",
        "rejected",
        "cancelled",
        "canceled",
        "conflict",
        "qa_blocked",
        "visual_qa_failed",
        "render_qa_failed",
        "final_qa_blocked",
        "unverifiable",
    }
    status = str(result.get("status") or "").strip().lower()
    if result.get("ok") is False or status in failure_statuses:
        raise RuntimeError(
            str(result.get("error") or result.get("note") or f"{tool_name} failed")[:1000]
        )
    receipts = dict(workflow.get("step_receipts") or receipts)
    receipts[step] = {
        **executing,
        "status": (
            "accepted"
            if status in {"accepted", "queued", "running", "started", "claim_pending"}
            else "completed"
        ),
        "result_status": status,
        "result": {
            key: value
            for key, value in result.items()
            if key
            in {
                "ok",
                "status",
                "job_id",
                "stage",
                "accepted",
                "complete",
                "idempotent_replay",
                "postcondition_verified",
                "affected",
                "animated",
                "failed",
                "ready_to_post",
                "download_url",
                "mp4_url",
                "note",
            }
        },
        "finished_at": time.time(),
    }
    workflow = _save(workflow, lease_owner, step_receipts=receipts)
    _root_transition(workflow, "executing", result_status=f"{step}_accepted")
    return workflow, result


def _complete(
    workflow: dict[str, Any],
    lease_owner: str,
    snapshot: dict[str, Any],
) -> None:
    animate = bool(workflow.get("animate"))
    assistant_text = (
        "Studio finished the backend-owned production command: every selected scene was "
        + ("approved and animated, " if animate else "approved, ")
        + "the final QA passed, and the MP4 is ready on this production card."
    )
    finished = store.finish_production_workflow(
        str(workflow.get("session_id") or ""),
        str(workflow.get("workflow_id") or ""),
        lease_owner=lease_owner,
        succeeded=True,
        assistant_text=assistant_text,
        snapshot=snapshot,
    )
    if finished is None or str(finished.get("status") or "") != "completed":
        _LOG.info(
            "stale workflow owner skipped completion for %s",
            workflow.get("workflow_id"),
        )
        return


def _fail(
    workflow: dict[str, Any],
    lease_owner: str,
    error: str,
    snapshot: dict[str, Any] | None = None,
) -> None:
    detail = str(error or "production workflow failed")[:1000]
    visible = dict(snapshot or {})
    assistant_text = (
        "The backend-owned production command stopped before publishing because its "
        f"required postcondition failed: {detail}"
    )
    finished = store.finish_production_workflow(
        str(workflow.get("session_id") or ""),
        str(workflow.get("workflow_id") or ""),
        lease_owner=lease_owner,
        succeeded=False,
        assistant_text=assistant_text,
        snapshot=visible,
        error=detail,
    )
    if finished is None or str(finished.get("status") or "") != "failed":
        _LOG.info(
            "stale workflow owner skipped failure for %s",
            workflow.get("workflow_id"),
        )
        return


def advance_shortform_ship_workflow(
    workflow: dict[str, Any],
    lease_owner: str,
) -> tuple[dict[str, Any] | None, float]:
    """Advance one state-machine edge; return the next durable row and delay."""

    now = time.time()
    if now - float(workflow.get("created_at") or now) > _MAX_AGE_SECONDS:
        snapshot = _job_snapshot(str(workflow.get("job_id") or ""))
        _fail(workflow, lease_owner, "production workflow timed out", snapshot)
        return None, 0.0
    stage = str(workflow.get("stage") or "approve")
    job_id = str(workflow.get("job_id") or "")

    if stage == "approve":
        if not _approval_is_durable(workflow):
            workflow, _ = _execute_tool_step(
                workflow,
                lease_owner,
                step="approve",
                tool_name="set_production_scenes_animate",
                arguments={
                    "job_id": job_id,
                    "animate": bool(workflow.get("animate")),
                    "scene_indices": list(workflow.get("scene_indices") or []),
                },
            )
        if not _approval_is_durable(workflow):
            raise RuntimeError("scene approval postcondition was not durable")
        next_stage = "animate" if bool(workflow.get("animate")) else "finalize"
        workflow = _save(
            workflow,
            lease_owner,
            status="running",
            stage=next_stage,
            next_attempt_at=now,
        )
        return workflow, 0.0

    if stage == "animate":
        if _animation_is_durable(workflow):
            workflow = _save(workflow, lease_owner, stage="finalize", next_attempt_at=now)
            return workflow, 0.0
        receipts = dict(workflow.get("step_receipts") or {})
        animate_receipt = receipts.get("animate")
        if (
            not isinstance(animate_receipt, dict)
            or str(animate_receipt.get("status") or "") == "executing"
        ):
            retry_arguments = (
                dict(animate_receipt.get("arguments") or {})
                if isinstance(animate_receipt, dict)
                and str(animate_receipt.get("status") or "") == "executing"
                and isinstance(animate_receipt.get("arguments"), dict)
                else {}
            )
            if retry_arguments:
                if str(retry_arguments.get("job_id") or "") != job_id:
                    raise RuntimeError("persisted animation receipt target no longer matches workflow")
                animation_indices = sorted({
                    int(value)
                    for value in list(retry_arguments.get("scene_indices") or [])
                    if int(value) >= 0
                })
            else:
                animation_indices = _missing_animation_indices(workflow)
            animation_arguments = (
                retry_arguments
                if retry_arguments
                else {"job_id": job_id, "scene_indices": animation_indices}
            )
            if not animation_indices:
                preflight = _finalize_preflight(job_id)
                raise RuntimeError(
                    str(
                        preflight.get("error")
                        or preflight.get("note")
                        or "animation postcondition failed without a missing selected clip"
                    )
                )
            workflow, _ = _execute_tool_step(
                workflow,
                lease_owner,
                step="animate",
                tool_name="animate_production_scenes",
                arguments=animation_arguments,
            )
        workflow = _save(
            workflow,
            lease_owner,
            stage="wait_animation",
            next_attempt_at=now + _POLL_SECONDS,
        )
        return workflow, _POLL_SECONDS

    if stage == "wait_animation":
        if _animation_is_durable(workflow):
            workflow = _save(workflow, lease_owner, stage="finalize", next_attempt_at=now)
            return workflow, 0.0
        snapshot = _job_snapshot(job_id)
        failure = _definitive_failure(snapshot, finalizing=False)
        if failure:
            _fail(workflow, lease_owner, failure, snapshot)
            return None, 0.0
        workflow = _save(
            workflow,
            lease_owner,
            last_snapshot=snapshot,
            next_attempt_at=now + _POLL_SECONDS,
        )
        return workflow, _POLL_SECONDS

    if stage == "finalize":
        snapshot = _job_snapshot(job_id)
        if _final_is_durable(snapshot):
            _complete(workflow, lease_owner, snapshot)
            return None, 0.0
        preflight = _finalize_preflight(job_id)
        if str(preflight.get("status") or "") != "ready":
            # Finalize is never called speculatively. If animation was requested,
            # return to the durable animation wait; otherwise fail closed.
            if bool(workflow.get("animate")):
                workflow = _save(
                    workflow,
                    lease_owner,
                    stage="wait_animation",
                    last_snapshot=snapshot,
                    next_attempt_at=now + _POLL_SECONDS,
                )
                return workflow, _POLL_SECONDS
            raise RuntimeError(
                str(preflight.get("error") or preflight.get("note") or "finalize preflight failed")
            )
        receipts = dict(workflow.get("step_receipts") or {})
        finalize_receipt = receipts.get("finalize")
        if (
            not isinstance(finalize_receipt, dict)
            or str(finalize_receipt.get("status") or "") == "executing"
        ):
            workflow, _ = _execute_tool_step(
                workflow,
                lease_owner,
                step="finalize",
                tool_name="finalize_production",
                arguments={"job_id": job_id},
            )
        workflow = _save(
            workflow,
            lease_owner,
            stage="wait_finalize",
            next_attempt_at=now + _POLL_SECONDS,
        )
        return workflow, _POLL_SECONDS

    if stage == "wait_finalize":
        snapshot = _job_snapshot(job_id)
        if _final_is_durable(snapshot):
            _complete(workflow, lease_owner, snapshot)
            return None, 0.0
        failure = _definitive_failure(snapshot, finalizing=True)
        if failure:
            _fail(workflow, lease_owner, failure, snapshot)
            return None, 0.0
        workflow = _save(
            workflow,
            lease_owner,
            last_snapshot=snapshot,
            next_attempt_at=now + _POLL_SECONDS,
        )
        return workflow, _POLL_SECONDS

    raise RuntimeError(f"unknown production workflow stage: {stage}")


def _heartbeat(
    session_id: str,
    workflow_id: str,
    lease_owner: str,
    stop: threading.Event,
) -> None:
    interval = max(5.0, _LEASE_SECONDS / 3.0)
    while not stop.wait(interval):
        if store.claim_production_workflow(
            session_id,
            workflow_id,
            lease_owner=lease_owner,
            lease_seconds=_LEASE_SECONDS,
        ) is None:
            return


def _run_workflow(session_id: str, workflow_id: str) -> None:
    lease_owner = f"{_PROCESS_OWNER}:{workflow_id}"
    heartbeat_stop = threading.Event()
    heartbeat: threading.Thread | None = None
    try:
        workflow = store.claim_production_workflow(
            session_id,
            workflow_id,
            lease_owner=lease_owner,
            lease_seconds=_LEASE_SECONDS,
        )
        if workflow is None:
            return
        heartbeat = threading.Thread(
            target=_heartbeat,
            args=(session_id, workflow_id, lease_owner, heartbeat_stop),
            daemon=True,
            name=f"production-workflow-heartbeat-{workflow_id[-8:]}",
        )
        heartbeat.start()
        while not _STOP.is_set() and workflow is not None:
            try:
                next_at = float(workflow.get("next_attempt_at") or 0.0)
                if next_at > time.time() and _STOP.wait(min(next_at - time.time(), _POLL_SECONDS)):
                    return
                workflow, delay = advance_shortform_ship_workflow(workflow, lease_owner)
                if workflow is not None and delay > 0 and _STOP.wait(delay):
                    return
            except Exception as exc:
                snapshot: dict[str, Any] = {}
                try:
                    snapshot = _job_snapshot(str(workflow.get("job_id") or ""))
                except Exception:
                    pass
                _fail(workflow, lease_owner, str(exc), snapshot)
                return
    finally:
        heartbeat_stop.set()
        if heartbeat is not None and heartbeat.is_alive():
            heartbeat.join(timeout=1.0)
        with _THREADS_LOCK:
            _ACTIVE_THREADS.pop(workflow_id, None)


def schedule_production_workflow(workflow: dict[str, Any]) -> bool:
    workflow_id = str(workflow.get("workflow_id") or "").strip()
    session_id = str(workflow.get("session_id") or "").strip()
    if not workflow_id or not session_id:
        return False
    with _THREADS_LOCK:
        current = _ACTIVE_THREADS.get(workflow_id)
        if current is not None and current.is_alive():
            return False
        thread = threading.Thread(
            target=_run_workflow,
            args=(session_id, workflow_id),
            daemon=True,
            name=f"production-workflow-{workflow_id[-8:]}",
        )
        _ACTIVE_THREADS[workflow_id] = thread
        thread.start()
        return True


def enqueue_shortform_ship_workflow(
    *,
    session_id: str,
    authority: dict[str, Any],
    root_mutation: dict[str, Any],
    job_id: str,
    scene_indices: list[int],
    animation_scene_indices: list[int],
    animate: bool,
    schedule: bool = True,
) -> tuple[dict[str, Any], bool]:
    workflow, created = store.create_shortform_ship_workflow(
        session_id,
        authority=authority,
        root_mutation=root_mutation,
        job_id=job_id,
        scene_indices=scene_indices,
        animation_scene_indices=animation_scene_indices,
        animate=animate,
    )
    if schedule:
        schedule_production_workflow(workflow)
    return workflow, created


def resume_pending_production_workflows() -> int:
    scheduled = 0
    for workflow in store.list_pending_production_workflows():
        if schedule_production_workflow(workflow):
            scheduled += 1
    return scheduled


def _monitor() -> None:
    while not _STOP.is_set():
        try:
            resume_pending_production_workflows()
        except Exception:
            _LOG.exception("production workflow recovery scan failed")
        _STOP.wait(_MONITOR_SECONDS)


def start_production_workflow_service() -> None:
    global _MONITOR_THREAD
    with _THREADS_LOCK:
        if _MONITOR_THREAD is not None and _MONITOR_THREAD.is_alive():
            return
        _STOP.clear()
        _MONITOR_THREAD = threading.Thread(
            target=_monitor,
            daemon=True,
            name="production-workflow-recovery",
        )
        _MONITOR_THREAD.start()


def stop_production_workflow_service() -> None:
    _STOP.set()


__all__ = [
    "advance_shortform_ship_workflow",
    "enqueue_shortform_ship_workflow",
    "resume_pending_production_workflows",
    "schedule_production_workflow",
    "start_production_workflow_service",
    "stop_production_workflow_service",
]
