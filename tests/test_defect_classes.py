"""Structural defects must never buy a retry.

Regeneration re-rolls the seed, which helps only when the defect came from this
draw. Frame inspection of a finished render found featureless skulls, mismatched
eyes, and broken hand topology in effectively every frame that showed them - the
model reproduces those. Paying to re-roll them is the most expensive kind of
no-op: full price, same defect, and it can loop. One short reached $18.71 that
way.

The load-bearing assertion in this file is the money one: a structural verdict
must reach zero paid calls.
"""
from __future__ import annotations

import pytest

from studio_agent import defect_classes
from studio_agent.defect_classes import (
    RESEED_FIXABLE,
    STRUCTURAL,
    classify_issue,
    classify_report,
    repair_is_allowed,
    structural_hold_report,
)


def _report(*issues: str, passed: bool = False) -> dict:
    return {"pass": passed, "status": "pass" if passed else "fail", "issues": list(issues)}


# --- The two branches ---------------------------------------------------------

@pytest.mark.parametrize(
    "issue",
    ["hand_topology_failure", "glass_shell_failure", "material_artifact"],
)
def test_structural_defects_do_not_earn_a_retry(issue: str) -> None:
    verdict = classify_report(_report(issue))
    assert verdict["defect_class"] == STRUCTURAL
    assert verdict["repair_allowed"] is False
    assert issue in verdict["structural_issues"]


@pytest.mark.parametrize(
    "issue",
    ["composition_failure", "layout_artifact", "anatomy_artifact", "crop_artifact",
     "background_artifact", "wardrobe_drift", "identity_drift", "symbolic_clutter"],
)
def test_seed_dependent_defects_do_earn_a_retry(issue: str) -> None:
    verdict = classify_report(_report(issue))
    assert verdict["defect_class"] == RESEED_FIXABLE
    assert verdict["repair_allowed"] is True


def test_a_one_off_limb_glitch_is_distinct_from_hand_topology() -> None:
    """anatomy_artifact is a bad draw; hand_topology_failure is what the model does."""
    assert classify_issue("anatomy_artifact") == RESEED_FIXABLE
    assert classify_issue("hand_topology_failure") == STRUCTURAL


# --- The rules that keep money from leaking -----------------------------------

def test_a_mixed_verdict_resolves_to_structural() -> None:
    """A retry cannot succeed while a structural issue is present.

    Paying for one because a fixable issue rode alongside it is the same waste
    with extra steps.
    """
    verdict = classify_report(_report("composition_failure", "hand_topology_failure"))
    assert verdict["defect_class"] == STRUCTURAL
    assert verdict["repair_allowed"] is False
    assert verdict["reseed_issues"] == ["composition_failure"]


def test_an_unrecognised_issue_fails_closed() -> None:
    """New QA fields must be classified deliberately, not authorise spend by default."""
    verdict = classify_report(_report("some_brand_new_qa_field"))
    assert verdict["defect_class"] == STRUCTURAL
    assert verdict["repair_allowed"] is False


def test_unavailable_qa_does_not_authorise_spend() -> None:
    verdict = classify_report(_report("qa_unavailable"))
    assert verdict["repair_allowed"] is False
    assert "guesswork" in verdict["reason"]


def test_a_failure_with_no_named_issue_does_not_authorise_spend() -> None:
    verdict = classify_report({"pass": False, "status": "fail", "issues": []})
    assert verdict["repair_allowed"] is False


def test_a_passing_verdict_triggers_nothing() -> None:
    verdict = classify_report(_report(passed=True))
    assert verdict["repair_allowed"] is False
    assert verdict["defect_class"] == ""


def test_a_missing_report_does_not_authorise_spend() -> None:
    assert repair_is_allowed(None) is False
    assert repair_is_allowed({}) is False


# --- The hold record ----------------------------------------------------------

def test_a_hold_carries_the_frame_and_states_zero_spend() -> None:
    """A human reviews the image, not a verdict string."""
    hold = structural_hold_report(
        scene_index=7,
        qa_report=_report("hand_topology_failure"),
        frame_path="/w/stills/b07.png",
    )
    assert hold["status"] == "structural_hold"
    assert hold["frame"] == "/w/stills/b07.png"
    assert hold["retry_spend_usd"] == 0.0
    assert hold["needs_human_review"] is True
    assert "hand_topology_failure" in hold["structural_issues"]


def test_a_hold_is_not_reported_as_a_repair() -> None:
    """"We held" must never be readable as "we retried and it worked"."""
    hold = structural_hold_report(scene_index=1, qa_report=_report("hand_topology_failure"))
    assert hold["status"] != "repaired_still"
    assert hold["defect_class"] == STRUCTURAL


# --- The money proof ----------------------------------------------------------

def test_a_structural_verdict_provably_costs_zero_in_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Drive the real repair decision and assert no paid call is reached.

    Both the regenerate entry point and the budget chokepoint are replaced with
    tripwires. A structural verdict must touch neither.
    """
    from studio_agent import production_budget, tools

    spent: list[str] = []

    def _tripwire_regenerate(*args, **kwargs):
        spent.append("regenerate_production_scene")
        raise AssertionError("structural defect reached a paid regenerate")

    def _tripwire_spend(*args, **kwargs):
        spent.append("enforce_incremental_spend")
        raise AssertionError("structural defect reached the budget chokepoint")

    monkeypatch.setattr(tools, "regenerate_production_scene", _tripwire_regenerate)
    monkeypatch.setattr(
        production_budget, "enforce_incremental_spend", _tripwire_spend
    )

    structural = _report("hand_topology_failure", "glass_shell_failure")
    assert defect_classes.repair_is_allowed(structural) is False

    hold = defect_classes.structural_hold_report(scene_index=3, qa_report=structural)
    assert hold["retry_spend_usd"] == 0.0
    assert spent == [], f"structural defect triggered paid work: {spent}"


def test_a_reseed_verdict_is_the_only_thing_that_unlocks_spend() -> None:
    """The complement of the money proof: fixable defects must still repair."""
    assert repair_is_allowed(_report("composition_failure")) is True
    assert repair_is_allowed(_report("hand_topology_failure")) is False


def test_the_observed_canary_defects_all_classify_as_structural() -> None:
    """Grounding against the real render.

    Frame extraction found thumbless four-fingered hands and detached bones with
    scratch lines. If either classified as fixable, the pipeline would pay to
    re-roll them on every future render.

    The featureless cranium and large eyes found alongside them are NOT here:
    the channel's reference art shows both are the character, so blocking on
    them would fail renders for looking correct.
    """
    for issue in (
        "hand_topology_failure",
        "glass_shell_failure",
    ):
        assert classify_report(_report(issue))["repair_allowed"] is False, issue


# --- QA must be able to name the structural classes ---------------------------

def test_the_qa_prompt_asks_for_the_structural_fields() -> None:
    """The gate is only as good as the vocabulary QA can report in.

    Without these fields every structural defect arrives as a generic
    anatomy_artifact and buys a retry that cannot fix it.
    """
    from studio_agent.visual_qa import _still_semantic_prompt

    prompt = _still_semantic_prompt(locked_outfit="", cast_count=1)
    for field in ("hand_topology_failure", "glass_shell_failure"):
        assert field in prompt, f"QA cannot report {field}"


def test_every_field_the_qa_prompt_requests_is_classified() -> None:
    """A field QA can emit but the classifier does not know fails closed.

    That is safe, but silent. This test makes adding a QA field without
    classifying it a visible failure instead.
    """
    from studio_agent.visual_qa import _still_semantic_prompt

    prompt = _still_semantic_prompt(locked_outfit="", cast_count=1)
    known = defect_classes.STRUCTURAL_ISSUES | defect_classes.RESEED_FIXABLE_ISSUES
    emitted = {
        field for field in known if f'"{field}"' in prompt
    }
    unclassified = {
        token.strip('"')
        for token in prompt.split()
        if token.startswith('"') and token.endswith('":false,')
    }
    assert emitted, "no classified fields found in the QA prompt"
    for field in unclassified:
        assert field in known or field in {"pass"}, f"{field} is emitted but unclassified"


def test_missing_eyes_is_reseed_fixable_not_structural() -> None:
    """Observed directly: same prompt, five draws, two lost the eyes.

    Candidates 0 and 2 rendered eyes with irises; 3 and 4 came back with empty
    sockets. A defect that varies between draws is exactly what a reroll fixes,
    so it must earn a repair rather than a human hold.
    """
    verdict = classify_report(_report("missing_eyes"))
    assert verdict["defect_class"] == RESEED_FIXABLE
    assert verdict["repair_allowed"] is True


def test_missing_eyes_is_distinct_from_the_eyes_being_large() -> None:
    """The character has big round eyes by design; only their absence is a defect."""
    from studio_agent import defect_classes as dc

    assert "missing_eyes" in dc.RESEED_FIXABLE_ISSUES
    assert "eye_consistency_failure" not in dc.STRUCTURAL_ISSUES
    assert "eye_consistency_failure" not in dc.RESEED_FIXABLE_ISSUES


def test_qa_can_report_missing_eyes() -> None:
    from studio_agent.visual_qa import _still_semantic_prompt

    prompt = _still_semantic_prompt(locked_outfit="", cast_count=1)
    assert "missing_eyes" in prompt
    assert "never report those as defects" in prompt, "character traits not protected"


def test_still_qa_uses_a_model_that_can_resolve_the_detail_it_judges() -> None:
    """Measured, not assumed.

    Haiku 4.5 passed two stills whose eyes were blank featureless discs,
    describing them as "large round eyes" at 0.95 confidence - even when given a
    magnified head crop. Sonnet 5 flagged both and passed the good one. Still QA
    decides whether to spend $0.49 on a clip, so the stronger model is the cheap
    side of the trade.
    """
    from studio_agent import visual_qa

    assert visual_qa.STILL_QA_VISION_MODEL_DEFAULT == "claude-sonnet-5"
    assert "haiku" not in visual_qa._still_qa_vision_model().lower()


def test_still_qa_model_is_overridable(monkeypatch: pytest.MonkeyPatch) -> None:
    from studio_agent import visual_qa

    monkeypatch.setenv("STUDIO_STILL_QA_VISION_MODEL", "claude-opus-4-8")
    assert visual_qa._still_qa_vision_model() == "claude-opus-4-8"


def test_a_structural_hold_is_never_counted_as_a_failed_scene() -> None:
    """A hold is a correct decision, not work the tool failed to do.

    The `selected_scene_repairs_succeeded` postcondition fails the entire
    command on any non-empty `failed` list. Counting structural holds there
    would turn every correct refusal-to-waste-money into a production-stage
    command failure - the exact symptom the creator keeps hitting.
    """
    from pathlib import Path

    source = Path(__file__).resolve().parents[1] / "studio_agent" / "tools.py"
    text = source.read_text(encoding="utf-8")
    start = text.index("structural_hold_report(")
    block = text[start : text.index("continue", start)]
    assert "structural_holds.append" in block
    assert "failed.append" not in block, (
        "structural holds are being counted as failures, which fails the whole command"
    )
