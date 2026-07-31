"""The Approve card must survive the turn that created it.

Production could not be started through the agent at all. The backend prepared
a correct pending action, the assistant said "I've prepared production for your
short. Approve when you're ready", and the client then hid the card instantly.

`isStaleShortformPendingAction` treats a pending action as stale when
`visual_proof_only` is set and the creator's latest message did not ask for a
single still. But the permanent staged short-form contract stamps
`visual_proof_only` on *every* new short, and a creator answering "yes make it"
never mentions a proof still - so the test always failed and the card always
vanished.

The rule that should have saved it - "if the assistant just asked for approval,
never hide the card" - sat *after* that check and could never run.

There is no frontend test runner in this project, so the ordering is pinned
here. It is a real invariant: a client that hides the card it was just told to
show makes the product unusable, and nothing else in the suite would catch it.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

PANEL = (
    Path(__file__).resolve().parents[1]
    / "ViralShorts-App" / "src" / "studio" / "panels" / "AgentPanel.tsx"
).read_text(encoding="utf-8")

FUNCTION = PANEL[PANEL.index("function isStaleShortformPendingAction") :][:4000]

APPROVAL_RULE = "approve when you'?re ready|approval required|prepared production"
ONE_STILL_RULE = "args.visual_proof_only === true"


def test_both_rules_are_still_present() -> None:
    """Guard against the anchors silently disappearing and the test passing."""
    assert APPROVAL_RULE in FUNCTION
    assert ONE_STILL_RULE in FUNCTION


def test_a_just_requested_approval_is_checked_before_the_one_still_rule() -> None:
    """Order is the entire fix.

    If the one-still rule runs first it returns true and the function exits, so
    the approval rule below it is unreachable for every real production.
    """
    approval_at = FUNCTION.index(APPROVAL_RULE)
    one_still_at = FUNCTION.index(ONE_STILL_RULE)
    assert approval_at < one_still_at, (
        "the one-still staleness rule runs before the approval-request rule, "
        "so Approve cards are hidden the moment they are created"
    )


def test_the_approval_rule_keeps_the_card() -> None:
    """It must return false (keep), not true (hide)."""
    tail = FUNCTION[FUNCTION.index(APPROVAL_RULE) :][:200]
    assert "return false" in tail


def test_the_staged_contract_still_marks_new_shorts_as_visual_proof() -> None:
    """The backend half of the interaction this test describes.

    If this ever stops being true the ordering above stops mattering, and this
    test should be revisited rather than silently passing for the wrong reason.
    """
    store_source = (
        Path(__file__).resolve().parents[1] / "studio_agent" / "store.py"
    ).read_text(encoding="utf-8")
    assert 'aligned["visual_proof_only"] = True' in store_source


@pytest.mark.parametrize(
    "assistant_text",
    [
        "I've prepared production for your short. Approve when you're ready.",
        "Approval required before I spend anything.",
        "I have prepared production for the short.",
    ],
)
def test_the_real_assistant_wording_matches_the_approval_rule(assistant_text: str) -> None:
    """The rule is only useful if it matches what Studio actually says."""
    assert re.search(APPROVAL_RULE, assistant_text, re.I)
