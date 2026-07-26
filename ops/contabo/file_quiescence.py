#!/usr/bin/env python3
"""Fail-closed inventory of resumable file-backed Studio production work."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


FORMAT = 1
WORKFLOW_TERMINAL = {"completed", "failed", "cancelled", "canceled"}
RUN_ACTIVE = {"queued", "running", "stream_disconnected"}
COMMAND_ACTIVE = {
    "authorized",
    "executing",
    "queued",
    "running",
    "started",
    "claim_pending",
    "stream_disconnected",
}
JOB_TERMINAL = {
    "complete",
    "completed",
    "success",
    "succeeded",
    "failed",
    "cancelled",
    "canceled",
    "ready",
    "finalized",
    "exported",
}
JOB_QUIESCENT = {
    "awaiting_scene_review",
    "awaiting_approval",
    "stills_done",
    "review_scenes",
    "scenes_approved",
    "awaiting_animation_review",
    "awaiting_thumbnail_review",
    "thumbnail_review",
    "final_qa_blocked",
    "ready_for_finalize",
}


class QuiescenceError(RuntimeError):
    pass


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise QuiescenceError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _entity_hash(kind: str, session_id: str, identity: str, status: str) -> str:
    raw = "\0".join((kind, session_id, identity, status)).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _rows(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


def _persisted_shortform_status(
    data_root: Path,
    *,
    job_id: str,
    kind: str,
) -> str:
    """Resolve a state-less short-form track from its durable result.

    Session ``active_jobs`` rows are tracking handles and older rows do not
    carry a status. Human-review states must survive a cutover, but an unknown
    or malformed durable state remains blocking.
    """

    safe_job_chars = (
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789_-"
    )
    if not job_id or any(char not in safe_job_chars for char in job_id):
        return ""
    if kind not in {"", "shortform"}:
        return ""
    output_root = (data_root / "skeleton_output").resolve(strict=False)
    result_path = output_root / job_id / "result.json"
    if not result_path.exists():
        return ""
    if result_path.is_symlink() or not result_path.is_file():
        raise QuiescenceError(
            f"short-form result is not a regular file: {job_id}/result.json"
        )
    resolved = result_path.resolve(strict=True)
    try:
        resolved.relative_to(output_root)
    except ValueError as exc:
        raise QuiescenceError(
            f"short-form result escapes the data root: {job_id}/result.json"
        ) from exc
    if resolved.stat().st_size > 16 * 1024 * 1024:
        raise QuiescenceError(
            f"short-form result is unexpectedly large: {job_id}/result.json"
        )
    try:
        payload = json.loads(
            resolved.read_text(encoding="utf-8"),
            object_pairs_hook=_strict_object,
        )
    except Exception as exc:
        raise QuiescenceError(
            f"short-form result is unreadable: {job_id}/result.json "
            f"({type(exc).__name__})"
        ) from exc
    if not isinstance(payload, dict):
        raise QuiescenceError(
            f"short-form result root is not an object: {job_id}/result.json"
        )
    return str(
        payload.get("status") or payload.get("stage") or payload.get("phase") or ""
    ).strip().lower()


def inspect_sessions(
    sessions_dir: Path,
    *,
    app: str = "",
    machine_id: str = "",
    captured_at_epoch: int | None = None,
) -> dict[str, Any]:
    sessions_dir = sessions_dir.resolve(strict=True)
    if not sessions_dir.is_dir() or sessions_dir.is_symlink():
        raise QuiescenceError("sessions directory must be a real directory")

    counts = {
        "production_workflows": 0,
        "active_runs": 0,
        "active_commands": 0,
        "active_jobs": 0,
    }
    blockers: list[str] = []
    file_records: list[str] = []
    quiescent_jobs = 0
    data_root = sessions_dir.parent
    paths = sorted(sessions_dir.glob("sa_*.json"), key=lambda item: item.name)
    for path in paths:
        mode = path.lstat().st_mode
        if path.is_symlink() or not stat.S_ISREG(mode):
            raise QuiescenceError(f"session path is not a regular file: {path.name}")
        try:
            payload = json.loads(
                path.read_text(encoding="utf-8"),
                object_pairs_hook=_strict_object,
            )
        except Exception as exc:
            raise QuiescenceError(
                f"session JSON is unreadable: {path.name} ({type(exc).__name__})"
            ) from exc
        if not isinstance(payload, dict):
            raise QuiescenceError(f"session JSON root is not an object: {path.name}")
        file_records.append(f"{path.name}\0{_file_sha256(path)}")
        if payload.get("deleted_at"):
            continue
        session_id = str(payload.get("session_id") or path.stem)

        for row in _rows(payload.get("production_workflows")):
            status = str(row.get("status") or "").strip().lower()
            if status not in WORKFLOW_TERMINAL:
                counts["production_workflows"] += 1
                blockers.append(
                    _entity_hash(
                        "workflow",
                        session_id,
                        str(row.get("workflow_id") or ""),
                        status,
                    )
                )

        for row in _rows(payload.get("runs")):
            status = str(row.get("status") or "").strip().lower()
            if status in RUN_ACTIVE:
                counts["active_runs"] += 1
                blockers.append(
                    _entity_hash(
                        "run",
                        session_id,
                        str(row.get("run_id") or row.get("request_id") or ""),
                        status,
                    )
                )

        for row in _rows(payload.get("production_commands")):
            status = str(row.get("status") or "").strip().lower()
            if status in COMMAND_ACTIVE:
                counts["active_commands"] += 1
                blockers.append(
                    _entity_hash(
                        "command",
                        session_id,
                        str(row.get("command_id") or ""),
                        status,
                    )
                )

        for row in _rows(payload.get("active_jobs")):
            status = str(
                row.get("status") or row.get("stage") or row.get("phase") or ""
            ).strip().lower()
            job_id = str(row.get("job_id") or row.get("id") or "").strip()
            if not status:
                status = _persisted_shortform_status(
                    data_root,
                    job_id=job_id,
                    kind=str(row.get("kind") or "").strip().lower(),
                )
            if status in JOB_QUIESCENT:
                quiescent_jobs += 1
                continue
            if status not in JOB_TERMINAL:
                counts["active_jobs"] += 1
                blockers.append(
                    _entity_hash(
                        "job",
                        session_id,
                        job_id,
                        status,
                    )
                )

    blockers.sort()
    file_records.sort()
    snapshot_material = json.dumps(
        {"files": file_records, "blockers": blockers, "counts": counts},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "format": FORMAT,
        "captured_at_epoch": int(captured_at_epoch or time.time()),
        "app": str(app or ""),
        "machine_id": str(machine_id or ""),
        "sessions_dir": str(sessions_dir),
        "session_file_count": len(paths),
        "quiescent_job_count": quiescent_jobs,
        "counts": counts,
        "total_blockers": sum(counts.values()),
        "drained": sum(counts.values()) == 0,
        "blocker_identity_sha256": hashlib.sha256(
            "\n".join(blockers).encode("ascii")
        ).hexdigest(),
        "snapshot_sha256": hashlib.sha256(snapshot_material).hexdigest(),
    }


def _write_atomic(path: Path, payload: dict[str, Any]) -> None:
    path = path.resolve(strict=False)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sessions-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--app", default="")
    parser.add_argument("--machine-id", default="")
    parser.add_argument("--require-drained", action="store_true")
    args = parser.parse_args()
    try:
        payload = inspect_sessions(
            args.sessions_dir,
            app=args.app,
            machine_id=args.machine_id,
        )
        if args.output:
            _write_atomic(args.output, payload)
        else:
            print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        if args.require_drained and not payload["drained"]:
            print(
                f"file-backed production is not quiescent: {payload['counts']}",
                file=sys.stderr,
            )
            return 1
        return 0
    except (OSError, QuiescenceError) as exc:
        print(f"file quiescence error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
