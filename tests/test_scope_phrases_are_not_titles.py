"""A scope phrase describes how much to build, not what it is about.

A creator spent a whole conversation planning "Why Emotionally Unavailable Men
Self-Sabotage When They Fall in Love", then said:

    All right, let's make the entire short.

The title extractor returned "the entire short", and that became the concept
card's working title, the Approve card's title, and - had it rendered - the
video's actual topic. Studio would have produced a short about the phrase "the
entire short".

The card then compounded it by instructing the creator to say
`render that plan for "the entire short"`, putting the bogus title in quotes so
the quoted-title branch re-extracted it on every retry.
"""
from __future__ import annotations

import pytest

from studio_agent import store


@pytest.mark.parametrize(
    "text",
    [
        "All right, let's make the entire short.",
        "lets make the entire short",
        "make the whole short",
        "make the entire video",
        "ok make the full video",
        "make it",
        "render the plan",
    ],
)
def test_a_scope_phrase_never_becomes_a_title(text: str) -> None:
    """Empty lets the caller fall back to the conversation's real topic."""
    assert store._requested_title_from_user_text(text) == ""


def test_the_quoted_scope_phrase_from_the_card_is_also_rejected() -> None:
    """The card's own suggested wording put the bogus title in quotes.

    Without this the quoted-title branch re-extracts it on every retry, so the
    creator cannot escape the wrong title by repeating the instruction.
    """
    text = 'yes make it — render that plan for "the entire short", only 30 seconds'
    assert store._requested_title_from_user_text(text) == ""


def test_a_real_title_still_extracts() -> None:
    text = "lets make Why Emotionally Unavailable Men Self-Sabotage When They Fall in Love"
    assert store._requested_title_from_user_text(text) == (
        "Why Emotionally Unavailable Men Self-Sabotage When They Fall in Love"
    )


def test_a_real_quoted_title_still_extracts() -> None:
    text = 'render that plan for "Why Men Pull Away Right When Things Get Serious"'
    assert "Pull Away" in store._requested_title_from_user_text(text)


@pytest.mark.parametrize(
    "value,expected",
    [
        ("the entire short", True),
        ("The Entire Short.", True),
        ("  the   whole video  ", True),
        ("it", True),
        ("Why Men Pull Away", False),
        ("The Entire History of Rome", False),
        # Empty is not a scope phrase, it is simply no title. The caller
        # discards it either way, so the predicate stays literal.
        ("", False),
    ],
)
def test_scope_detection_is_exact_not_substring(value: str, expected: bool) -> None:
    """"The Entire History of Rome" is a real title that contains a scope word.

    Matching on substrings would silently delete legitimate titles.
    """
    assert store._is_generic_scope_title(value) is expected


def test_punctuation_and_case_do_not_smuggle_a_scope_phrase_through() -> None:
    for variant in ("the entire short!", "THE ENTIRE SHORT", "the-entire-short"):
        assert store._is_generic_scope_title(variant) is True
