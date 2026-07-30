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
    doubled that clip's price. Here the beats can fit, so none may exceed it.
    """
    scenes = [{"index": i, "duration_sec": 4.0} for i in range(8)]
    scenes[3]["duration_sec"] = 6.5
    scenes[4]["duration_sec"] = 5.5
    paced = _apply_shortform_story_pacing(scenes)
    assert paced, "pacing dropped the scenes"
    for scene in paced:
        assert float(scene["duration_sec"]) <= CHEAP_CLIP_SECONDS + 1e-9, scene


def test_beats_that_cannot_fit_are_left_alone_rather_than_truncated() -> None:
    """Squeezing an unfittable plan under the cap would only buy a freeze.

    Six 7.5s beats cannot be served by 5s clips no matter how the time is
    moved around. Shortening them would strip 15s off the video and the mux
    would pad it back by freezing the final frame, so the long clips are the
    honest cost and the durations must survive untouched.
    """
    scenes = [{"index": i, "duration_sec": 7.5} for i in range(6)]
    paced = _apply_shortform_story_pacing(scenes)
    assert sum(float(s["duration_sec"]) for s in paced) == pytest.approx(45.0)


def test_story_pacing_still_tightens_the_hook() -> None:
    """The retention behaviour this function exists for must survive."""
    scenes = [{"index": i, "duration_sec": 7.5} for i in range(6)]
    paced = _apply_shortform_story_pacing(scenes)
    assert float(paced[0]["duration_sec"]) <= 4.5


def test_story_pacing_does_not_stretch_short_scenes() -> None:
    scenes = [{"index": i, "duration_sec": 3.0} for i in range(5)]
    paced = _apply_shortform_story_pacing(scenes)
    assert [float(scene["duration_sec"]) for scene in paced[:-1]] == [3.0] * 4


def test_pacing_never_shortens_the_video() -> None:
    """The freeze-frame trap.

    mux_narration pads video to the narration clock by cloning the last frame,
    so any second the beats stop covering becomes a visible freeze at the end.
    Capping durations must redistribute the time, never discard it.
    """
    scenes = [{"index": i, "duration_sec": 7.5} for i in range(6)]
    before = sum(float(s["duration_sec"]) for s in scenes)
    paced = _apply_shortform_story_pacing(scenes)
    after = sum(float(s["duration_sec"]) for s in paced)
    assert after >= before - 1e-6, f"pacing lost {before - after:.2f}s of video"


def test_pacing_preserves_total_for_ordinary_beats() -> None:
    scenes = [{"index": i, "duration_sec": 4.0} for i in range(8)]
    scenes[2]["duration_sec"] = 6.5
    before = sum(float(s["duration_sec"]) for s in scenes)
    paced = _apply_shortform_story_pacing(scenes)
    assert sum(float(s["duration_sec"]) for s in paced) == pytest.approx(before)


def test_pacing_is_idempotent() -> None:
    """Finalize can run more than once; the video must not drift each time."""
    scenes = [{"index": i, "duration_sec": 6.0} for i in range(7)]
    once = _apply_shortform_story_pacing([dict(s) for s in scenes])
    twice = _apply_shortform_story_pacing([dict(s) for s in once])
    assert [float(s["duration_sec"]) for s in twice] == pytest.approx(
        [float(s["duration_sec"]) for s in once]
    )
