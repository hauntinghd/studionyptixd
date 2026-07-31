"""A repair that did its job must not be reported as a total failure.

Every recorded production-stage command failure on this install was the same
tool - `audit_and_repair_production_scenes` - and in two of them the "error"
text was the tool's own success note:

    audit_and_repair_production_scenes did not reach a verified durable
    postcondition: Fresh artifact, identity, animation, and
    narrative-correspondence QA completed; only failed scenes were regenerated.

The command itself was compiled correctly from "all 6 scenes need to be fully
remade so please do that then animate them": right job, right scenes, right
scope. NLU and tool-calling worked. The verifier then required every selected
scene to pass visual QA, so one unresolved scene out of six failed the whole
command with safe_claim="none" - telling the creator nothing happened when five
scenes had in fact been remade.

That is the difference between verifying the tool did its work and demanding a
probabilistic generator be perfect. It matters more now that structural defects
deliberately do not retry: a held scene never passes QA by design.
"""
from __future__ import annotations

import pytest

from studio_agent.command_postconditions import (
    ExecutionReceipt,
    SceneRepairPostconditions,
    verify_execution,
)


def _snapshot(rows: dict[int, dict]) -> dict:
    return {
        "job_id": "116de45c8205",
        "status": "ready",
        "stage": "review",
        "scenes": [
            {
                "scene_number": number,
                "has_clip": row.get("has_clip", True),
                "qa_stale": row.get("qa_stale", False),
                "visual_qa": {"pass": row.get("qa_pass", True)},
            }
            for number, row in sorted(rows.items())
        ],
    }


def _receipt(**over) -> ExecutionReceipt:
    expected = SceneRepairPostconditions(
        kind="scene_repair",
        job_id="116de45c8205",
        selected_scene_numbers=[1, 2, 3, 4, 5, 6],
        untouched_scene_numbers=[],
        expected_clip_scene_numbers=[1, 2],
        repair_scope="full_quality",
        untouched_assets=[],
    )
    base = dict(
        execution_id="exec-1",
        idempotency_key="idem-1",
        command_id="cmd-1",
        status="accepted",
        tool_name="audit_and_repair_production_scenes",
        target_job_id="116de45c8205",
        expected=expected,
        started_at=0.0,
        finished_at=1.0,
        result={"ok": True, "audited": [0, 1, 2, 3, 4, 5], "failed": []},
    )
    base.update(over)
    return ExecutionReceipt(**base)


def _verify(rows: dict[int, dict]):
    return verify_execution(_receipt(), snapshot_loader=lambda *_a, **_k: _snapshot(rows))


def _check(verdict, name):
    match = [c for c in verdict.checks if c.name == name]
    assert match, f"{name} missing from {[c.name for c in verdict.checks]}"
    return match[0]


def test_a_fully_clean_repair_passes() -> None:
    verdict = _verify({n: {} for n in range(1, 7)})
    assert verdict.status == "passed"
    assert verdict.safe_claim == "completed"


def test_one_unresolved_scene_no_longer_fails_the_whole_command() -> None:
    """The exact recorded failure: six selected, one still imperfect."""
    rows = {n: {} for n in range(1, 7)}
    rows[4] = {"qa_pass": False}
    verdict = _verify(rows)

    assert verdict.status == "passed", [c.name for c in verdict.checks if c.status == "failed"]
    assert verdict.safe_claim == "completed"
    assert _check(verdict, "selected_scene_quality").status == "failed"


def test_the_unresolved_scene_is_still_reported() -> None:
    """Not failing the command must not mean hiding the problem."""
    rows = {n: {} for n in range(1, 7)}
    rows[4] = {"qa_pass": False}
    quality = _check(_verify(rows), "selected_scene_quality")
    assert quality.actual["still_failing"] == [4]
    assert quality.required is False


def test_a_structurally_held_scene_does_not_fail_the_command() -> None:
    """Structural defects deliberately do not retry, so they never pass QA.

    Under the old check every correct hold became a command failure.
    """
    rows = {n: {} for n in range(1, 7)}
    rows[2] = {"qa_pass": False}
    rows[5] = {"qa_pass": False}
    verdict = _verify(rows)
    assert verdict.status == "passed"
    assert _check(verdict, "selected_scene_quality").actual["still_failing"] == [2, 5]


def test_a_scene_the_repair_never_touched_still_fails() -> None:
    """The guarantee that must survive.

    Stale QA means the tool did not actually audit that scene, which is a real
    execution failure rather than a quality outcome.
    """
    rows = {n: {} for n in range(1, 7)}
    rows[3] = {"qa_stale": True}
    verdict = _verify(rows)

    assert verdict.status == "failed"
    assert verdict.safe_claim == "none"
    assert _check(verdict, "selected_scene_qa_refreshed").actual["stale_scenes"] == [3]


def test_every_scene_stale_fails() -> None:
    verdict = _verify({n: {"qa_stale": True} for n in range(1, 7)})
    assert verdict.status == "failed"


def test_a_scene_losing_its_clip_still_fails() -> None:
    """Clip continuity is a regression, not a quality opinion."""
    rows = {n: {} for n in range(1, 7)}
    rows[1] = {"has_clip": False}
    verdict = _verify(rows)
    assert verdict.status == "failed"
    assert _check(verdict, "selected_clip_continuity").status == "failed"


def test_quality_and_refresh_are_distinct_signals() -> None:
    """A stale scene must not also be counted as a quality failure.

    Otherwise one problem is reported twice and the creator cannot tell whether
    the repair ran at all.
    """
    rows = {n: {} for n in range(1, 7)}
    rows[3] = {"qa_stale": True, "qa_pass": False}
    verdict = _verify(rows)
    assert _check(verdict, "selected_scene_qa_refreshed").actual["stale_scenes"] == [3]
    assert 3 not in _check(verdict, "selected_scene_quality").actual["still_failing"]
