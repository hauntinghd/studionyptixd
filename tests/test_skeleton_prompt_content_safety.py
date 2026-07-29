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

from skeleton_ai.prompt_compose import compose_skeleton_still_prompt


# Words a partner content checker is likely to score as a nudity request. These
# are about the *request* phrasing, not the rendered subject.
FORBIDDEN = ("no clothes", "nude", "naked", "unclothed", "undressed", "full body front view")


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
