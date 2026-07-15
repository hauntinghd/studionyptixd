"""Observed-state verification for asynchronous Studio command execution."""
from __future__ import annotations

import hashlib
import inspect
import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

from pydantic import Field

from studio_agent.command_contract import ContractModel
from studio_agent.command_execution import ExecutionReceipt
from studio_agent.command_validation import SceneAssetFingerprint


CheckStatus = Literal["passed", "failed", "pending", "not_applicable"]
VerdictStatus = Literal["passed", "failed", "pending", "inconclusive"]
SafeClaim = Literal["none", "started", "completed"]


class PostconditionCheck(ContractModel):
    name: str
    status: CheckStatus
    expected: Any = None
    actual: Any = None
    message: str = ""
    required: bool = True


class PostconditionVerdict(ContractModel):
    receipt_id: str
    status: VerdictStatus
    safe_claim: SafeClaim
    checked_at: float
    checks: list[PostconditionCheck] = Field(default_factory=list)
    retry_after_seconds: float | None = None

    @property
    def can_report_completion(self) -> bool:
        return self.status == "passed" and self.safe_claim == "completed"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fingerprint_workspace_scenes(
    workspace: str | Path,
    scene_numbers: list[int],
) -> list[SceneAssetFingerprint]:
    """Hash persisted still/clip bytes for selected human-facing scenes."""

    root = Path(workspace)
    try:
        raw = json.loads((root / "scenes.json").read_text(encoding="utf-8"))
    except Exception:
        raw = []
    by_number: dict[int, dict[str, Any]] = {}
    for fallback, scene in enumerate(raw if isinstance(raw, list) else []):
        if not isinstance(scene, dict):
            continue
        try:
            number = int(scene.get("index", fallback)) + 1
        except (TypeError, ValueError):
            number = fallback + 1
        by_number[number] = scene
    out: list[SceneAssetFingerprint] = []
    for number in scene_numbers:
        scene = by_number.get(int(number), {})
        still = root / str(scene.get("still_rel") or f"stills/b{int(number) - 1:02d}.png")
        clip = root / str(scene.get("clip_rel") or f"clips/b{int(number) - 1:02d}.mp4")
        out.append(
            SceneAssetFingerprint(
                scene_number=int(number),
                still_sha256=_sha256_file(still) if still.is_file() else "",
                clip_sha256=_sha256_file(clip) if clip.is_file() else "",
            )
        )
    return out


def _call_snapshot_loader(loader: Callable[..., dict[str, Any]], job_id: str) -> dict[str, Any]:
    result = loader(job_id, "shortform")
    if inspect.isawaitable(result):
        raise TypeError("snapshot_loader must be synchronous during postcondition verification")
    return result if isinstance(result, dict) else {}


def _scene_rows(snapshot: dict[str, Any]) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for fallback, scene in enumerate(list(snapshot.get("scenes") or [])):
        if not isinstance(scene, dict):
            continue
        try:
            number = int(scene.get("index", fallback)) + 1
        except (TypeError, ValueError):
            number = fallback + 1
        out[number] = scene
    return out


def _actual_fingerprints(
    loader: Callable[[str, list[int]], list[SceneAssetFingerprint] | list[dict[str, Any]]] | None,
    job_id: str,
    scene_numbers: list[int],
) -> list[SceneAssetFingerprint]:
    if loader is None:
        return []
    try:
        raw = loader(job_id, scene_numbers)
    except Exception:
        return []
    out: list[SceneAssetFingerprint] = []
    for item in raw or []:
        try:
            out.append(item if isinstance(item, SceneAssetFingerprint) else SceneAssetFingerprint.model_validate(item))
        except Exception:
            continue
    return out


def verify_execution(
    receipt: ExecutionReceipt,
    *,
    snapshot_loader: Callable[..., dict[str, Any]],
    fingerprint_loader: Callable[[str, list[int]], list[SceneAssetFingerprint] | list[dict[str, Any]]] | None = None,
) -> PostconditionVerdict:
    """Verify the real job; a successful dispatch is never completion proof."""

    checked_at = time.time()
    if receipt.status in {"failed", "rejected"} or receipt.expected is None:
        return PostconditionVerdict(
            receipt_id=receipt.execution_id,
            status="failed",
            safe_claim="none",
            checked_at=checked_at,
            checks=[
                PostconditionCheck(
                    name="execution_accepted",
                    status="failed",
                    expected="accepted",
                    actual=receipt.status,
                    message=receipt.error or "Execution was not accepted.",
                )
            ],
        )
    expected = receipt.expected
    try:
        snapshot = _call_snapshot_loader(snapshot_loader, expected.job_id)
    except Exception as exc:
        return PostconditionVerdict(
            receipt_id=receipt.execution_id,
            status="inconclusive",
            safe_claim="started" if receipt.status in {"accepted", "duplicate"} else "none",
            checked_at=checked_at,
            retry_after_seconds=2.0,
            checks=[
                PostconditionCheck(
                    name="job_snapshot",
                    status="not_applicable",
                    expected=expected.job_id,
                    actual=None,
                    message=f"Snapshot unavailable: {exc}",
                )
            ],
        )

    checks: list[PostconditionCheck] = []
    actual_job_id = str(snapshot.get("job_id") or "")
    checks.append(
        PostconditionCheck(
            name="same_job",
            status="passed" if actual_job_id == expected.job_id else "failed",
            expected=expected.job_id,
            actual=actual_job_id,
            message="Expansion must remain on the approved proof job.",
        )
    )
    status = str(snapshot.get("status") or "").lower()
    stage = str(snapshot.get("stage") or "").lower()
    terminal_failure = status in {"failed", "cancelled", "error"} or stage in {"failed", "cancelled", "error"}
    running = bool(snapshot.get("running")) or status in {"running", "queued"} or stage in {
        "restarting",
        "expand_animate",
        "scene_plan",
        "stills",
        "animate",
        "running",
    }
    completed_stage = status == "complete" or stage in {
        "awaiting_scene_review",
        "awaiting_approval",
        "awaiting_animation_review",
        "complete",
    }
    checks.append(
        PostconditionCheck(
            name="lifecycle",
            status="failed" if terminal_failure else "passed" if completed_stage else "pending",
            expected="awaiting_scene_review or complete",
            actual={"status": status, "stage": stage},
        )
    )

    rows = _scene_rows(snapshot)
    try:
        current_scene_count = int(snapshot.get("current_scene") or len(rows) or 0)
    except (TypeError, ValueError):
        current_scene_count = len(rows)
    total_check_status: CheckStatus
    if current_scene_count == expected.expected_total_scene_count:
        total_check_status = "passed"
    elif running and current_scene_count < expected.expected_total_scene_count:
        total_check_status = "pending"
    else:
        total_check_status = "failed"
    checks.append(
        PostconditionCheck(
            name="exact_total_scene_count",
            status=total_check_status,
            expected=expected.expected_total_scene_count,
            actual=current_scene_count,
        )
    )
    expected_new = list(
        range(
            expected.expected_existing_scene_count + 1,
            expected.expected_total_scene_count + 1,
        )
    )
    actual_new = [number for number in expected_new if number in rows]
    new_status: CheckStatus = "passed" if actual_new == expected_new else "pending" if running else "failed"
    checks.append(
        PostconditionCheck(
            name="new_scene_set",
            status=new_status,
            expected=expected_new,
            actual=actual_new,
        )
    )

    before_by_scene = {item.scene_number: item for item in expected.preserved_assets}
    after = _actual_fingerprints(fingerprint_loader, expected.job_id, expected.preserved_scene_numbers)
    after_by_scene = {item.scene_number: item for item in after}
    if not before_by_scene or not after_by_scene:
        checks.append(
            PostconditionCheck(
                name="preserved_assets",
                status="not_applicable",
                expected=expected.preserved_scene_numbers,
                actual=list(after_by_scene),
                message="Byte fingerprints are required before completion can be claimed.",
                required=True,
            )
        )
    else:
        mismatched: list[int] = []
        missing: list[int] = []
        for number in expected.preserved_scene_numbers:
            before = before_by_scene.get(number)
            after_item = after_by_scene.get(number)
            if before is None or after_item is None:
                missing.append(number)
                continue
            comparable = False
            for field_name in ("still_sha256", "clip_sha256"):
                wanted = str(getattr(before, field_name) or "")
                if not wanted:
                    continue
                comparable = True
                if str(getattr(after_item, field_name) or "") != wanted:
                    mismatched.append(number)
                    break
            if not comparable:
                missing.append(number)
        fingerprint_status: CheckStatus = "failed" if mismatched else "not_applicable" if missing else "passed"
        checks.append(
            PostconditionCheck(
                name="preserved_assets",
                status=fingerprint_status,
                expected=expected.preserved_scene_numbers,
                actual={"mismatched": mismatched, "unverifiable": missing},
                message="Approved Scene 1 assets must remain byte-for-byte unchanged.",
            )
        )

    expected_animated = expected.expected_animated_scene_numbers
    if expected.animation_scope in {"new_scenes", "all_scenes"}:
        actual_animated = [number for number in expected_animated if bool(rows.get(number, {}).get("has_clip"))]
        animation_status: CheckStatus = (
            "passed"
            if actual_animated == expected_animated
            else "pending"
            if running
            else "failed"
        )
        checks.append(
            PostconditionCheck(
                name="animation_scope",
                status=animation_status,
                expected=expected_animated,
                actual=actual_animated,
            )
        )
    elif expected.animation_scope == "none":
        animated_new = [number for number in expected_new if bool(rows.get(number, {}).get("has_clip"))]
        checks.append(
            PostconditionCheck(
                name="animation_scope",
                status="passed" if not animated_new else "failed",
                expected=[],
                actual=animated_new,
            )
        )
    else:
        checks.append(
            PostconditionCheck(
                name="animation_scope",
                status="not_applicable",
                expected=expected.animation_scope,
                actual=None,
                required=False,
            )
        )

    required = [check for check in checks if check.required]
    if any(check.status == "failed" for check in required):
        verdict_status: VerdictStatus = "failed"
        safe_claim: SafeClaim = "none"
        retry_after = None
    elif any(check.status == "pending" for check in required):
        verdict_status = "pending"
        safe_claim = "started"
        retry_after = 2.0
    elif any(check.status == "not_applicable" for check in required):
        verdict_status = "inconclusive"
        safe_claim = "started"
        retry_after = None
    else:
        verdict_status = "passed"
        safe_claim = "completed"
        retry_after = None
    return PostconditionVerdict(
        receipt_id=receipt.execution_id,
        status=verdict_status,
        safe_claim=safe_claim,
        checked_at=checked_at,
        checks=checks,
        retry_after_seconds=retry_after,
    )

