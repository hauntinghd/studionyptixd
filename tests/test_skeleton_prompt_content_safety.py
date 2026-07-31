"""Skeleton still prompts must not read as nudity requests.

FAL's content checker rejected Studio's own roster prompt with
content_policy_violation / partner_validation_failed:

    "EDIT ref. SCENE: Plain neutral studio backdrop, full body front view.
     No clothes. 9:16; two hosts; no text. LOCK: each host thin glass skin on
     bones only; ... no human skin/text."

The skeleton legitimately wears nothing, but "No clothes." beside a full-body
framing instruction reads as a nudity request. It failed intermittently
(partner-side validation), so paid productions died at random beats - one run
lost $2.72 mid-job. The anatomy LOCK clause already carries the real meaning, so
the wardrobe clause only needs to say no garments exist.
"""
from __future__ import annotations

import re

import pytest

from skeleton_ai.canonical_edit import (
    is_content_policy_error,
    policy_safe_prompt,
)
from skeleton_ai.prompt_compose import compact_identity_locks, compose_skeleton_still_prompt


# Words a partner content checker is likely to score as a nudity request. These
# are about the *request* phrasing, not the rendered subject.
FORBIDDEN = (
    "no clothes",
    "nude",
    "naked",
    "unclothed",
    "undressed",
    "full body front view",
    "human skin",
    "glass skin",
)


def _prompts_without_outfit() -> list[str]:
    return [
        compose_skeleton_still_prompt(
            visual_description=visual,
            outfit=outfit,
            topic="why you test the people who actually love you",
            aspect_ratio="9:16",
            budget=300,
            cast_count=cast,
        )
        for visual in (
            "Plain neutral studio backdrop, full skeleton visible head to feet, front-facing.",
            "Skeleton host seated at a desk beside a glowing brain model.",
        )
        for outfit in ("", "no clothing", "default")
        for cast in (1, 2)
    ]


@pytest.mark.parametrize("term", FORBIDDEN)
def test_no_outfit_prompts_avoid_nudity_phrasing(term: str) -> None:
    for prompt in _prompts_without_outfit():
        assert term not in prompt.lower(), (
            f"prompt contains {term!r}, which FAL's content checker rejects:\n{prompt}"
        )


def test_absent_wardrobe_still_states_that_no_garments_exist() -> None:
    """Removing the risky phrasing must not lose the instruction entirely.

    Without some wardrobe statement the editor is free to dress the skeleton,
    which breaks canonical identity across scenes.
    """
    prompt = compose_skeleton_still_prompt(
        visual_description="Skeleton host at a desk.",
        outfit="",
        topic="psychology",
        aspect_ratio="9:16",
        budget=300,
        cast_count=1,
    )
    assert re.search(r"no garments", prompt, re.I), prompt


def test_explicit_wardrobe_is_still_honoured() -> None:
    prompt = compose_skeleton_still_prompt(
        visual_description="Skeleton host in a courtroom.",
        outfit="dark 1920s FBI suit and fedora",
        topic="true crime",
        aspect_ratio="9:16",
        budget=300,
        cast_count=1,
    )
    assert "WARDROBE: dark 1920s FBI suit and fedora." in prompt
    assert "no garments" not in prompt.lower()


@pytest.mark.parametrize("cast_count", (1, 2))
def test_identity_lock_avoids_skin_vocabulary(cast_count: int) -> None:
    """The LOCK clause said "skin" twice next to a garments negation.

    Run 3 of the MrSkelewelly canary died on exactly this: the roster prompt
    passed (the earlier fix held) but a *scene* prompt carrying the LOCK clause
    was rejected. "Shell"/"flesh" says the same thing to the editor.
    """
    lock = compact_identity_locks(cast_count=cast_count)
    assert "skin" not in lock.lower(), lock
    # The rendering intent must survive the rewording.
    assert "bones" in lock.lower(), lock


def test_policy_safe_prompt_strips_the_clauses_that_were_rejected() -> None:
    original = (
        "EDIT ref. SCENE: uneasy posture, head tilted. No garments. "
        "9:16; one host; no text. LOCK: thin glass skin on bones only "
        "(never dome/pod/capsule); no human skin/text. Eyes in skull sockets only."
    )
    safer = policy_safe_prompt(original)
    lowered = safer.lower()
    for term in ("no garments", "skin", "nude", "naked"):
        assert term not in lowered, f"{term!r} survived neutralisation:\n{safer}"
    # It must stay a usable edit instruction, not be gutted.
    assert "uneasy posture" in safer
    assert "9:16" in safer
    assert "eyes in skull sockets only" in lowered


def test_policy_safe_prompt_is_idempotent() -> None:
    """A second pass must not keep mutating, or the retry loop can never settle."""
    once = policy_safe_prompt(
        "SCENE: desk shot. No garments. LOCK: thin glass skin on bones only; no human skin/text."
    )
    assert policy_safe_prompt(once) == once


def test_policy_safe_prompt_leaves_clean_prompts_alone() -> None:
    """Equality with the original is the signal that suppresses a pointless retry."""
    clean = "EDIT ref. SCENE: skeleton host at a desk. 9:16; one host; no text."
    assert policy_safe_prompt(clean) == clean


def test_content_policy_errors_are_recognised_by_body_text() -> None:
    """raise_for_status() hides the body; the retry only fires if we match on it."""
    body = (
        'fal-ai/bytedance/seedream/v4.5/edit result fetch returned HTTP 422: '
        '{"detail":[{"msg":"The content could not be processed because it contained '
        'material flagged by a content checker.","type":"content_policy_violation",'
        '"ctx":{"extra_info":{"reason":"partner_validation_failed"}}}]}'
    )
    assert is_content_policy_error(RuntimeError(body))


@pytest.mark.parametrize(
    "message",
    ("connection reset by peer", "HTTP 500 internal server error", "timed out after 600s"),
)
def test_unrelated_failures_do_not_trigger_the_prompt_retry(message: str) -> None:
    """Retrying a transport failure with a reworded prompt would just burn money."""
    assert not is_content_policy_error(RuntimeError(message))


# --- Structural defect negatives (Task 3) -------------------------------------

@pytest.mark.parametrize(
    "term",
    ["missing fingers", "thumbless hand", "asymmetric eyes", "protruding eyes",
     "featureless cranium", "stray lines", "detached bones"],
)
def test_the_negative_prompt_names_the_structural_defects(term: str) -> None:
    """Frame inspection found all four structural classes in the reference.

    They were not in the negative prompt, so nothing was asking the model to
    avoid them.
    """
    from skeleton_ai.canonical_edit import NEG_EDIT

    assert term in NEG_EDIT.lower(), f"{term!r} is not suppressed"


def test_the_structural_negatives_did_not_reintroduce_nudity_phrasing() -> None:
    """The negatives must not undo the content-policy fix."""
    from skeleton_ai.canonical_edit import NEG_EDIT

    low = NEG_EDIT.lower()
    for banned in ("no clothes", "nude", "naked", "unclothed"):
        assert banned not in low, f"{banned!r} came back into the negative prompt"
