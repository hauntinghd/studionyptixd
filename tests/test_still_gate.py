"""No public route may render without visual QA, and no bad reference may animate.

Two findings drive this file.

First, `/api/skeleton-ai/generate` - the endpoint the Create panel calls - ran
`pipeline.run`, which had **zero** visual_qa call sites against 57 in the staged
path. Every short made through the form rendered completely ungated.

Second, the defects frame inspection found (thumbless four-fingered hands,
detached bones, stray scratch lines) were present in the reference stills
*before* animation ran. Animation is 90.7% of a short's cost and the reference
is 0.3%, so the reference is the cheapest possible place to stop a doomed render
- $0.04 against the $5.88 of clips it would seed.

The smooth cranium and large round eyes seen alongside them are not defects at
all: the channel's reference art defines both as the character.
"""
from __future__ import annotations

import pytest

from studio_agent import still_gate
from studio_agent.still_gate import (
    ReferenceStillRejected,
    evaluate_reference_still,
    require_animatable_reference,
)


def _qa(*issues: str, passed: bool = False) -> dict:
    return {"pass": passed, "status": "pass" if passed else "fail", "issues": list(issues)}


# --- The gate blocks what it must ---------------------------------------------

@pytest.mark.parametrize(
    "issue",
    ["hand_topology_failure", "glass_shell_failure", "material_artifact"],
)
def test_a_structurally_broken_reference_does_not_animate(issue: str) -> None:
    verdict = evaluate_reference_still(_qa(issue))
    assert verdict["animation_allowed"] is False
    assert issue in verdict["blocking_issues"]
    assert verdict["animation_spend_usd"] == 0.0


def test_the_observed_canary_reference_defects_all_block() -> None:
    """The exact defects found by extracting frames from a finished render."""
    verdict = evaluate_reference_still(
        _qa("hand_topology_failure", "glass_shell_failure")
    )
    assert verdict["animation_allowed"] is False
    assert len(verdict["blocking_issues"]) == 2


def test_missing_qa_is_not_permission_to_animate() -> None:
    """Unknown is not a pass - it would wave through the least examined stills."""
    assert evaluate_reference_still(None)["animation_allowed"] is False
    assert evaluate_reference_still({})["animation_allowed"] is False


def test_qa_that_could_not_run_blocks() -> None:
    verdict = evaluate_reference_still(_qa("qa_unavailable"))
    assert verdict["animation_allowed"] is False


# --- The gate permits what it must --------------------------------------------

def test_a_passing_reference_animates() -> None:
    verdict = evaluate_reference_still(_qa(passed=True))
    assert verdict["animation_allowed"] is True


def test_seed_dependent_issues_do_not_block_the_whole_render() -> None:
    """A merely-unlucky still is the repair path's job, not a hard stop.

    Blocking here would turn every recoverable defect into a dead render.
    """
    verdict = evaluate_reference_still(_qa("composition_failure", "layout_artifact"))
    assert verdict["animation_allowed"] is True
    assert verdict["blocking_issues"] == []
    assert "composition_failure" in verdict["seed_issues"]


# --- The raising form ---------------------------------------------------------

def test_require_raises_with_the_verdict_attached() -> None:
    with pytest.raises(ReferenceStillRejected) as excinfo:
        require_animatable_reference(_qa("hand_topology_failure"), still_path="/w/roster.png")
    assert excinfo.value.verdict["still"] == "/w/roster.png"
    assert excinfo.value.verdict["animation_spend_usd"] == 0.0


def test_require_returns_the_verdict_when_allowed() -> None:
    verdict = require_animatable_reference(_qa(passed=True), still_path="/w/roster.png")
    assert verdict["animation_allowed"] is True


# --- The one-shot path is no longer ungated -----------------------------------

def test_the_one_shot_pipeline_now_calls_visual_qa() -> None:
    """The finding this file exists for: pipeline.run had zero QA call sites."""
    from pathlib import Path

    source = Path(__file__).resolve().parents[1] / "skeleton_ai" / "pipeline.py"
    text = source.read_text(encoding="utf-8")
    assert "visual_qa" in text, "the public one-shot render path has no visual QA"
    assert "require_animatable_reference" in text, "reference stills are not gated"


def test_the_gate_runs_before_animation_in_the_one_shot_path() -> None:
    """Order matters: gating after animation would spend the money it saves."""
    from pathlib import Path

    source = Path(__file__).resolve().parents[1] / "skeleton_ai" / "pipeline.py"
    text = source.read_text(encoding="utf-8")
    gate_at = text.index("_gate_reference_still(\n")
    animate_at = text.index("clip_path = gen_clip(")
    assert gate_at < animate_at, "reference gate runs after animation spend"


def test_a_qa_exception_becomes_a_rejection_not_a_pass() -> None:
    """If the auditor itself breaks, the render must stop, not proceed blind."""
    verdict = evaluate_reference_still(
        {"status": "fail", "pass": False, "issues": ["qa_unavailable"],
         "summary": "Reference QA raised: boom"}
    )
    assert verdict["animation_allowed"] is False


def test_blocking_set_tracks_the_structural_classes() -> None:
    """The gate and the repair classifier must not drift apart.

    If a defect is structural enough to refuse a retry, it is structural enough
    to refuse animating twelve copies of it.
    """
    from studio_agent import defect_classes

    assert still_gate.BLOCKING_ISSUES == defect_classes.STRUCTURAL_ISSUES
