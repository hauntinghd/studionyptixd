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
    ["missing fingers", "thumbless hand", "fused fingers",
     "stray lines", "detached bones", "broken refraction"],
)
def test_the_negative_prompt_names_the_structural_defects(term: str) -> None:
    """Frame inspection found all four structural classes in the reference.

    They were not in the negative prompt, so nothing was asking the model to
    avoid them.

    Asserted against what the request actually carries, not against the source
    constant. The earlier version of this test checked ``NEG_EDIT`` while the
    dispatcher sent ``NEG_EDIT[:1500]`` - so it passed green while
    "stray lines" and "detached bones" were being cut off before dispatch, and
    the defects they name kept appearing in finished renders.
    """
    from skeleton_ai.canonical_edit import NEG_EDIT, fit_negative_prompt

    assert term in fit_negative_prompt(NEG_EDIT).lower(), f"{term!r} never reaches the model"


def test_the_structural_negatives_did_not_reintroduce_nudity_phrasing() -> None:
    """The negatives must not undo the content-policy fix."""
    from skeleton_ai.canonical_edit import NEG_EDIT

    low = NEG_EDIT.lower()
    for banned in ("no clothes", "nude", "naked", "unclothed"):
        assert banned not in low, f"{banned!r} came back into the negative prompt"


# --- Self-emitted light: the planner contradicting the identity lock ----------

def test_the_planners_circuit_glow_is_stripped_from_the_outfit() -> None:
    """The exact art direction that failed five of six scenes in production.

    The planner wrote this for a technology-flavoured topic. It asks the body
    to emit light while the identity lock says "No chest lights/orbs" - the
    positive prompt wins, the model draws the orb, and QA then rejects the
    scene Studio itself asked for. No number of repairs can pass that render.
    """
    from skeleton_ai.canonical_edit import sanitize_skeleton_outfit

    outfit = (
        "No clothing; bare ivory-white bones and translucent glass torso exposed, "
        "faint circuit-like light lines pulsing subtly along limbs to suggest live processing"
    )
    cleaned = sanitize_skeleton_outfit(outfit, topic="psychology").lower()

    for banned in ("circuit", "light lines", "pulsing"):
        assert banned not in cleaned, f"{banned!r} still asks the body to emit light"
    assert "glass torso" in cleaned, "the wardrobe intent must survive the scrub"


@pytest.mark.parametrize(
    "phrase",
    [
        "glowing chest cavity",
        "lit from within",
        "neon light lines along the ribs",
        "an energy core in the sternum",
        "self-illuminated bones",
    ],
)
def test_every_emissive_phrasing_is_stripped(phrase: str) -> None:
    from skeleton_ai.canonical_edit import sanitize_emissive_language

    assert not sanitize_emissive_language(phrase).strip()


@pytest.mark.parametrize(
    "phrase",
    [
        "sharp commercial lighting",
        "rim light on the shoulders",
        "soft studio lighting",
        "backlit background panel",
    ],
)
def test_environment_lighting_survives(phrase: str) -> None:
    """The style is lit. Only the body emitting its own light is the defect."""
    from skeleton_ai.canonical_edit import sanitize_emissive_language

    assert sanitize_emissive_language(phrase) == phrase


# --- The request cap must not eat defect classes ------------------------------

def test_the_negative_prompt_is_never_cut_mid_term() -> None:
    """A plain slice sent "...elon" and dropped the rest of the hand defects."""
    from skeleton_ai.canonical_edit import NEG_EDIT, fit_negative_prompt

    sent = fit_negative_prompt(NEG_EDIT)
    source_terms = {t.strip().lower() for t in NEG_EDIT.split(",") if t.strip()}
    for term in (t.strip().lower() for t in sent.split(",") if t.strip()):
        assert term in source_terms, f"{term!r} is a fragment, not a whole term"


def test_the_negative_prompt_fits_the_request_cap() -> None:
    from skeleton_ai.canonical_edit import (
        NEGATIVE_PROMPT_MAX_CHARS,
        NEG_EDIT,
        NEG_EDIT_DUAL,
        fit_negative_prompt,
    )

    for source in (NEG_EDIT, NEG_EDIT_DUAL):
        assert len(fit_negative_prompt(source)) <= NEGATIVE_PROMPT_MAX_CHARS


@pytest.mark.parametrize(
    "term",
    [
        "chest orb",
        "internal glow",
        "detached bones",
        "stray lines",
        "missing fingers",
        "thumbless hand",
        "missing teeth",
        "cropped feet",
        "eyes in chest",
    ],
)
def test_the_defect_classes_survive_the_cap(term: str) -> None:
    """Ordering is load-bearing: these must outrank generic filler.

    Every one of these names a defect found by extracting frames from a paid
    render. A dropped "detached bones" ships a defective video; a dropped
    "watermark" ships nothing.
    """
    from skeleton_ai.canonical_edit import NEG_EDIT, fit_negative_prompt

    assert term in fit_negative_prompt(NEG_EDIT).lower()


def test_a_repeated_term_does_not_pay_twice() -> None:
    from skeleton_ai.canonical_edit import fit_negative_prompt

    assert fit_negative_prompt("glowing eyes, blurry, glowing eyes") == "glowing eyes, blurry"


# --- Prompts must not carry fragments the model cannot act on -----------------

def test_the_scene_prompt_never_ends_mid_word() -> None:
    """Live prompts carried "...environment matching th…" and "hands;…".

    The budget was applied as a raw slice, so both the scene direction and the
    tail of the identity lock reached the editor as fragments, with a trailing
    ellipsis the model reads as content rather than as omission.
    """
    from skeleton_ai.prompt_compose import compose_skeleton_still_prompt

    prompt = compose_skeleton_still_prompt(
        visual_description=(
            "Exactly one canonical skeleton host in a modern psychology-studio "
            "environment matching the narration, empty hands in a presenter "
            "gesture, sharp commercial lighting, vertical 9:16"
        ),
        outfit="",
        topic="psychology",
        aspect_ratio="9:16",
        budget=300,
        cast_count=1,
    )
    assert "…" not in prompt, prompt
    assert not re.search(r"\b\w+…", prompt), prompt


@pytest.mark.parametrize("budget", (200, 240, 300))
@pytest.mark.parametrize("cast", (1, 2))
def test_the_identity_lock_survives_every_budget(budget: int, cast: int) -> None:
    """The lock is what keeps the character the character."""
    from skeleton_ai.prompt_compose import compose_skeleton_still_prompt

    prompt = compose_skeleton_still_prompt(
        visual_description="Skeleton host in a modern studio, presenter gesture, commercial lighting.",
        outfit="",
        topic="psychology",
        aspect_ratio="9:16",
        budget=budget,
        cast_count=cast,
    )
    assert "LOCK:" in prompt, prompt
    assert len(prompt) <= budget, prompt


def test_clip_keeps_whole_words() -> None:
    from skeleton_ai.prompt_compose import _clip

    clipped = _clip("a modern psychology-studio environment matching the narration", 40)
    assert "…" not in clipped
    assert clipped.split()[-1] in "a modern psychology-studio environment matching the narration".split()


# --- The eye contract must not contradict the rubric it is judged against -----

@pytest.mark.parametrize("cast_count", (1, 2))
def test_the_lock_does_not_demand_empty_sockets(cast_count: int) -> None:
    """One production failed scenes both ways, in the same run.

    Scenes 1 and 2 were rejected for showing "realistic irises/pupils placed in
    skull sockets rather than empty orbits". Scene 3 was rejected with
    missing_eyes because its iris could not be confirmed. The prompt said
    "Eyes in skull sockets only" - meant as placement - while the still QA
    rubric requires "a distinctly coloured iris with a dark pupil". No render
    could satisfy both, so no amount of repair could ever finish that video.
    """
    lock = compact_identity_locks(cast_count=cast_count).lower()
    assert "iris" in lock and "pupil" in lock, lock
    assert "sockets only" not in lock, lock


def test_the_lock_and_the_still_rubric_agree_about_eyes() -> None:
    """Pin the two specs to each other; drift here costs a whole render."""
    import inspect

    from studio_agent import visual_qa

    rubric = inspect.getsource(visual_qa)
    assert "distinctly coloured iris with a dark pupil" in rubric
    for cast in (1, 2):
        lock = compact_identity_locks(cast_count=cast).lower()
        assert "iris" in lock, lock
