"""Script length sets the price of a short, so the beat plan has to follow it.

Three defects chained together and killed a paid canary run:

1. The script prompt asked for "60 seconds" and got 203 words, which narrated
   to 73.8s - 23% over.
2. The beat count was fixed at 12, so those 73.8s became 6.15s beats. FAL bills
   video in whole tiers, so every clip jumped from $0.49 to $0.98 and the render
   cost ~$12 instead of ~$8.
3. `split_script_into_beats` returned `sentences[:target_count]`, silently
   dropping the 13th sentence from the visual plan while the voiceover still
   spoke it - narration with no shot behind it.

More, shorter beats are strictly cheaper than fewer, longer ones here, and they
match the ~5s average shot this channel already cuts to.
"""
from __future__ import annotations

import re

import pytest

from skeleton_ai.pipeline import (
    CHEAP_CLIP_SECONDS,
    NARRATION_WORDS_PER_SECOND,
    plan_beat_count,
    split_script_into_beats,
)
from skeleton_ai.scripting_grok import (
    SHORTFORM_TARGET_SECONDS,
    build_script_prompt,
    shortform_word_budget,
)


CANARY_SCRIPT = " ".join(
    f"Sentence number {i} carries about seventeen words of narration so the "
    f"whole script lands near two hundred words total."
    for i in range(13)
)


def test_no_sentence_is_dropped_from_the_visual_plan() -> None:
    """The truncation bug: narration was spoken with no shot behind it."""
    beats = split_script_into_beats(CANARY_SCRIPT, target_count=12)
    planned_words = len(re.findall(r"\w+", " ".join(beats)))
    script_words = len(re.findall(r"\w+", CANARY_SCRIPT))
    assert planned_words == script_words, (
        f"{script_words - planned_words} words of narration have no beat"
    )


def test_extra_sentences_are_merged_not_truncated() -> None:
    script = " ".join(f"Sentence {i} here." for i in range(20))
    beats = split_script_into_beats(script, target_count=8)
    assert len(beats) == 8
    for i in range(20):
        assert f"Sentence {i} here." in " ".join(beats)


def test_a_short_script_is_split_up_to_reach_the_target() -> None:
    script = (
        "The mind protects itself in ways that feel like sabotage, and the "
        "people closest to you absorb the damage because they are the safest "
        "target available."
    )
    beats = split_script_into_beats(script, target_count=3)
    assert len(beats) > 1
    assert len(re.findall(r"\w+", " ".join(beats))) == len(re.findall(r"\w+", script))


def test_beat_count_scales_so_the_average_beat_fits_the_cheap_tier() -> None:
    """The exact failure: 203 words over 12 beats bought twelve 10s clips."""
    words = 203
    script = " ".join(["word"] * words)
    count = plan_beat_count(script, requested=12)
    estimated_seconds = words / NARRATION_WORDS_PER_SECOND
    assert estimated_seconds / count <= CHEAP_CLIP_SECONDS + 1e-9
    assert count > 12, "a 74s narration still needs more than 12 beats"


def test_the_requested_count_is_a_floor_not_a_ceiling() -> None:
    """A short script must not be forced into fewer beats than asked for."""
    assert plan_beat_count(" ".join(["word"] * 40), requested=12) == 12


def test_an_empty_script_does_not_produce_zero_beats() -> None:
    assert plan_beat_count("", requested=12) == 12
    assert split_script_into_beats("", target_count=12) == []


def test_more_short_beats_beat_fewer_long_ones_on_price() -> None:
    """The economic claim the change rests on, asserted directly.

    Twelve 10s clips at $0.98 cost more than fifteen 5s clips at $0.49, even
    after paying for three extra stills at $0.04.
    """
    words = 203
    long_plan = 12 * (0.98 + 0.04)
    short_count = plan_beat_count(" ".join(["word"] * words), requested=12)
    short_plan = short_count * (0.49 + 0.04)
    assert short_plan < long_plan, f"{short_plan:.2f} is not cheaper than {long_plan:.2f}"


def test_the_script_prompt_states_a_word_budget_not_just_a_duration() -> None:
    prompt = build_script_prompt("system", "why you test the people who love you")
    low, high = shortform_word_budget()
    assert str(low) in prompt and str(high) in prompt
    assert "60-second" not in prompt


def test_the_word_budget_matches_the_target_duration() -> None:
    low, high = shortform_word_budget(SHORTFORM_TARGET_SECONDS)
    midpoint = (low + high) / 2
    assert midpoint / NARRATION_WORDS_PER_SECOND == pytest.approx(
        SHORTFORM_TARGET_SECONDS, abs=1.0
    )


def test_a_script_written_to_budget_stays_inside_the_cheap_tier() -> None:
    """End to end: budget -> beat count -> per-clip billing tier."""
    low, high = shortform_word_budget()
    for words in (low, high):
        script = " ".join(["word"] * words)
        count = plan_beat_count(script, requested=10)
        assert (words / NARRATION_WORDS_PER_SECOND) / count <= CHEAP_CLIP_SECONDS + 1e-9


# --- An explicit count is a contract, not a floor ------------------------------

def test_a_visual_proof_still_stays_one_still() -> None:
    """The regression this guards.

    The staged workflow asks for exactly one still so the creator can approve
    the look before paying for a short. Treating that 1 as a floor rendered
    eleven stills from a 144-word script - eleven times the cost, and an
    approval gate that no longer gated anything.
    """
    script = " ".join(["word"] * 144)
    assert plan_beat_count(script, 1, exact=True) == 1


def test_a_creator_specified_count_is_honoured() -> None:
    """Asking for six scenes must not silently produce eleven."""
    script = " ".join(["word"] * 144)
    assert plan_beat_count(script, 6, exact=True) == 6


def test_the_default_is_still_raised_to_fit_the_cheap_tier() -> None:
    """Without an explicit request the cost fix must still apply."""
    script = " ".join(["word"] * 203)
    assert plan_beat_count(script, 12) > 12


def test_exact_never_returns_zero_or_negative() -> None:
    for requested in (0, -3, None):
        assert plan_beat_count("some script", requested or 0, exact=True) >= 1


def test_the_staged_path_marks_explicit_counts_as_exact() -> None:
    """tools.py must pass the contract flag, or the fix never reaches production."""
    from pathlib import Path

    source = (Path(__file__).resolve().parents[1] / "studio_agent" / "tools.py").read_text(
        encoding="utf-8"
    )
    assert "beats_exact=bool(visual_proof_only or scene_count)" in source
