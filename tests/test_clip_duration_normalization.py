"""Clip length requests must be valid at the provider and cheapest at the till.

FAL's video lanes accept `duration` as a string from a fixed set and bill per
whole tier. Callers derive beat lengths from narration timing, so they ask for
5.5s or 6.0s - neither of which is a value the lane accepts.

The old behaviour differed by path and both were wrong. `styled_pipeline`
truncated with `int()`, serving a 5.5s beat from a 5s clip whose final frame was
then frozen for half a second - visible artifacting the creator paid for. The
skeleton pipeline rounded up, buying a 10s clip for a 5.1s beat at double price.
"""
from __future__ import annotations

import pytest

from skeleton_ai.i2v_engine import (
    KLING_PRO_ENDPOINT,
    SUPPORTED_CLIP_SECONDS,
    _build_args,
    normalize_clip_seconds,
)
from skeleton_ai.pipeline import CHEAP_CLIP_SECONDS
from skeleton_ai.styled_pipeline import _apply_shortform_story_pacing


@pytest.mark.parametrize("requested", [0.5, 2.0, 4.4, 5.0])
def test_short_beats_use_the_cheap_tier(requested: float) -> None:
    assert normalize_clip_seconds(requested) == 5


@pytest.mark.parametrize("requested", [5.1, 6.0, 8.0, 10.0])
def test_beats_past_the_cheap_tier_get_a_clip_that_covers_them(
    requested: float,
) -> None:
    """Never truncate: a clip shorter than its beat gets padded with a freeze."""
    assert normalize_clip_seconds(requested) >= requested


def test_every_result_is_a_value_the_lane_accepts() -> None:
    for tenth in range(1, 130):
        assert normalize_clip_seconds(tenth / 10) in SUPPORTED_CLIP_SECONDS


def test_an_over_long_request_clamps_to_the_longest_tier() -> None:
    assert normalize_clip_seconds(45.0) == SUPPORTED_CLIP_SECONDS[-1]


def test_the_request_sent_to_kling_is_normalized() -> None:
    """The regression that mattered: styled_pipeline could send duration=6."""
    args = _build_args(KLING_PRO_ENDPOINT, "slow push in", "https://x/y.png", 6, "9:16")
    assert args["duration"] in {str(value) for value in SUPPORTED_CLIP_SECONDS}


def test_story_pacing_never_pushes_a_beat_past_the_cheap_tier() -> None:
    """Pacing used to cap middles at 5.5s and the outro at 6.0s.

    Both sat just over the tier, so every one of them either froze a frame or
    doubled that clip's price.
    """
    scenes = [{"index": i, "duration_sec": 7.5} for i in range(6)]
    paced = _apply_shortform_story_pacing(scenes)
    assert paced, "pacing dropped the scenes"
    for scene in paced:
        assert float(scene["duration_sec"]) <= CHEAP_CLIP_SECONDS + 1e-9, scene


def test_story_pacing_still_tightens_the_hook() -> None:
    """The retention behaviour this function exists for must survive."""
    scenes = [{"index": i, "duration_sec": 7.5} for i in range(6)]
    paced = _apply_shortform_story_pacing(scenes)
    assert float(paced[0]["duration_sec"]) <= 4.5


def test_story_pacing_does_not_stretch_short_scenes() -> None:
    scenes = [{"index": i, "duration_sec": 3.0} for i in range(5)]
    paced = _apply_shortform_story_pacing(scenes)
    assert [float(scene["duration_sec"]) for scene in paced[:-1]] == [3.0] * 4
