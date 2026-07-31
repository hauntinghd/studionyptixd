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
from studio_agent.command_validation import SceneAssetFingerprint, SceneRepairPostconditions


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


def _finalize_verdict(
    receipt: ExecutionReceipt,
    checks: list[PostconditionCheck],
    *,
    checked_at: float,
) -> PostconditionVerdict:
    required = [check for check in checks if check.required]
    if any(check.status == "failed" for check in required):
        status: VerdictStatus = "failed"
        claim: SafeClaim = "none"
        retry_after = None
    elif any(check.status == "pending" for check in required):
        status = "pending"
        claim = "started"
        retry_after = 2.0
    elif any(check.status == "not_applicable" for check in required):
        status = "inconclusive"
        claim = "started"
        retry_after = None
    else:
        status = "passed"
        claim = "completed"
        retry_after = None
    return PostconditionVerdict(
        receipt_id=receipt.execution_id,
        status=status,
        safe_claim=claim,
        checked_at=checked_at,
        checks=checks,
        retry_after_seconds=retry_after,
    )


def _verify_scene_repair(
    receipt: ExecutionReceipt,
    expected: SceneRepairPostconditions,
    *,
    snapshot_loader: Callable[..., dict[str, Any]],
    fingerprint_loader: Callable[[str, list[int]], list[SceneAssetFingerprint] | list[dict[str, Any]]] | None,
    checked_at: float,
) -> PostconditionVerdict:
    try:
        snapshot = _call_snapshot_loader(snapshot_loader, expected.job_id)
    except Exception as exc:
        return PostconditionVerdict(
            receipt_id=receipt.execution_id,
            status="inconclusive",
            safe_claim="started" if receipt.status in {"accepted", "completed", "duplicate"} else "none",
            checked_at=checked_at,
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
            message="Scene repair must remain on the selected production job.",
        )
    )
    audited = sorted(
        int(number) + 1
        for number in receipt.result.get("audited") or []
        if str(number).lstrip("-").isdigit()
    )
    checks.append(
        PostconditionCheck(
            name="exact_selected_scene_set",
            status="passed" if audited == expected.selected_scene_numbers else "failed",
            expected=expected.selected_scene_numbers,
            actual=audited,
            message="The tool must audit exactly the creator-selected scenes and no others.",
        )
    )
    failed = sorted(
        int(number) + 1
        for number in receipt.result.get("failed") or []
        if str(number).lstrip("-").isdigit()
    )
    tool_ok = receipt.result.get("ok") is True and not failed
    checks.append(
        PostconditionCheck(
            name="selected_scene_repairs_succeeded",
            status="passed" if tool_ok else "failed",
            expected={"ok": True, "failed": []},
            actual={"ok": receipt.result.get("ok"), "failed": failed},
        )
    )
    status = str(snapshot.get("status") or "").lower()
    stage = str(snapshot.get("stage") or "").lower()
    terminal_failure = status in {"failed", "cancelled", "error"} or stage in {
        "failed",
        "cancelled",
        "error",
    }
    checks.append(
        PostconditionCheck(
            name="job_lifecycle",
            status="failed" if terminal_failure else "passed",
            expected="non-failed",
            actual={"status": status, "stage": stage},
        )
    )
    rows = _scene_rows(snapshot)
    actual_clips = [
        number
        for number in expected.expected_clip_scene_numbers
        if bool(rows.get(number, {}).get("has_clip"))
    ]
    checks.append(
        PostconditionCheck(
            name="selected_clip_continuity",
            status=(
                "passed"
                if actual_clips == expected.expected_clip_scene_numbers
                else "failed"
            ),
            expected=expected.expected_clip_scene_numbers,
            actual=actual_clips,
            message="Selected scenes that were animated before repair must remain clip-ready.",
        )
    )
    selected_qa = {
        number: {
            "qa_stale": bool(rows.get(number, {}).get("qa_stale", True)),
            "visual_qa_pass": (
                rows.get(number, {}).get("visual_qa", {}).get("pass")
                if isinstance(rows.get(number, {}).get("visual_qa"), dict)
                else None
            ),
        }
        for number in expected.selected_scene_numbers
    }
    # Postconditions verify that the tool did what it was told, not that a
    # probabilistic image generator reached perfection. These were previously
    # one all-or-nothing check: a single selected scene still failing visual QA
    # marked the whole command failed with safe_claim="none", so a repair of six
    # scenes where five came back clean was reported to the creator as though
    # nothing had happened. Every recorded production-stage command failure was
    # this tool, and the "error" was its own success note.
    #
    # It matters more now that structural defects deliberately do not retry:
    # a held scene never passes QA by design, which under the old check turned
    # every correct hold into a command failure.
    stale = [number for number, qa in selected_qa.items() if qa["qa_stale"]]
    checks.append(
        PostconditionCheck(
            name="selected_scene_qa_refreshed",
            status="failed" if stale else "passed",
            expected={"qa_stale": False},
            actual={"stale_scenes": stale},
            message=(
                "Every selected scene must carry fresh asset-bound QA - that is the "
                "proof the repair actually ran against it."
            ),
        )
    )
    still_failing = [
        number
        for number, qa in selected_qa.items()
        if not qa["qa_stale"] and qa["visual_qa_pass"] is not True
    ]
    checks.append(
        PostconditionCheck(
            name="selected_scene_quality",
            status="failed" if still_failing else "passed",
            expected={"visual_qa_pass": True},
            actual={"scenes": selected_qa, "still_failing": still_failing},
            message=(
                "Scenes whose quality the repair could not resolve. Reported as an "
                "outcome, not a command failure: the work was performed and the "
                "creator needs to see which scenes remain unresolved."
            ),
            required=False,
        )
    )

    if not expected.untouched_scene_numbers:
        checks.append(
            PostconditionCheck(
                name="untouched_scene_assets",
                status="passed",
                expected=[],
                actual=[],
                message="Every scene was explicitly selected, so there is no excluded asset set.",
            )
        )
    else:
        before_by_scene = {item.scene_number: item for item in expected.untouched_assets}
        after = _actual_fingerprints(
            fingerprint_loader,
            expected.job_id,
            expected.untouched_scene_numbers,
        )
        after_by_scene = {item.scene_number: item for item in after}
        if not before_by_scene or not after_by_scene:
            checks.append(
                PostconditionCheck(
                    name="untouched_scene_assets",
                    status="not_applicable",
                    expected=expected.untouched_scene_numbers,
                    actual=list(after_by_scene),
                    message="Byte fingerprints are required to prove excluded scenes remained untouched.",
                )
            )
        else:
            mismatched: list[int] = []
            unverifiable: list[int] = []
            for number in expected.untouched_scene_numbers:
                before = before_by_scene.get(number)
                after_item = after_by_scene.get(number)
                if before is None or after_item is None:
                    unverifiable.append(number)
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
                    unverifiable.append(number)
            checks.append(
                PostconditionCheck(
                    name="untouched_scene_assets",
                    status=(
                        "failed"
                        if mismatched
                        else "not_applicable"
                        if unverifiable
                        else "passed"
                    ),
                    expected=expected.untouched_scene_numbers,
                    actual={"mismatched": mismatched, "unverifiable": unverifiable},
                    message="Every unselected scene must remain byte-for-byte unchanged.",
                )
            )
    return _finalize_verdict(receipt, checks, checked_at=checked_at)


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
    if isinstance(expected, SceneRepairPostconditions) or getattr(expected, "kind", "") == "scene_repair":
        repair_expected = (
            expected
            if isinstance(expected, SceneRepairPostconditions)
            else SceneRepairPostconditions.model_validate(expected)
        )
        return _verify_scene_repair(
            receipt,
            repair_expected,
            snapshot_loader=snapshot_loader,
            fingerprint_loader=fingerprint_loader,
            checked_at=checked_at,
        )
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

    return _finalize_verdict(receipt, checks, checked_at=checked_at)
