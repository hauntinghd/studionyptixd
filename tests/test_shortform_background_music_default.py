"""Shorts ship with a music bed unless the creator turns it off.

Short-form already had per-scene SFX, background-music generation, and a mixer.
It simply defaulted to "off" - so every short went out silent under the voice
unless the creator found the setting. At $0.001/sec a 50s bed is about five
cents against a roughly $6 render, so silence was never a cost decision, just
an unset default.

The one thing that must not regress: an explicit "off" stays off. A creator who
turned music off is not asking to be overridden.
"""
from __future__ import annotations

import pytest

from skeleton_ai.styled_pipeline import (
    SHORTFORM_BGM_DEFAULT,
    SHORTFORM_BGM_OFF,
    resolve_shortform_background_music,
)


def test_an_unset_choice_becomes_music_not_silence() -> None:
    resolved = resolve_shortform_background_music("", category_key="", topic="")
    assert resolved.lower() not in SHORTFORM_BGM_OFF
    assert resolved == SHORTFORM_BGM_DEFAULT


def test_none_is_treated_as_unset() -> None:
    assert resolve_shortform_background_music(None).lower() not in SHORTFORM_BGM_OFF


@pytest.mark.parametrize("choice", ["off", "none", "no", "no background music", "OFF"])
def test_an_explicit_off_is_honoured(choice: str) -> None:
    """A creator who turned music off must not be overridden."""
    resolved = resolve_shortform_background_music(
        choice, category_key="psychology", topic="why you test people"
    )
    assert resolved.lower() in SHORTFORM_BGM_OFF


def test_an_explicit_choice_is_passed_through_untouched() -> None:
    assert (
        resolve_shortform_background_music("lofi piano, very sparse")
        == "lofi piano, very sparse"
    )


def test_auto_resolves_to_a_profile_rather_than_the_literal_word() -> None:
    resolved = resolve_shortform_background_music("auto", category_key="education")
    assert resolved.lower() != "auto"
    assert resolved.lower() not in SHORTFORM_BGM_OFF


def test_the_lane_shapes_the_bed() -> None:
    psychology = resolve_shortform_background_music("", category_key="psychology")
    crime = resolve_shortform_background_music("", category_key="true crime")
    assert psychology != crime, "every lane got the same bed"


def test_the_topic_can_pick_the_profile_when_the_category_is_generic() -> None:
    resolved = resolve_shortform_background_music(
        "", category_key="education", topic="why you test the people who love you"
    )
    assert resolved.lower() not in SHORTFORM_BGM_OFF


@pytest.mark.parametrize(
    "category", ["psychology", "true crime", "history", "motivation", "", "unknown-lane"]
)
def test_every_lane_resolves_to_a_usable_prompt(category: str) -> None:
    resolved = resolve_shortform_background_music("", category_key=category)
    assert resolved.strip()
    # Beds must never be described in a way that competes with the narration.
    assert "vocal" not in resolved.lower()
    assert "lyrics" not in resolved.lower()


def test_resolution_is_stable_for_the_same_inputs() -> None:
    args = {"category_key": "psychology", "topic": "emotional walls"}
    assert resolve_shortform_background_music("", **args) == (
        resolve_shortform_background_music("", **args)
    )
