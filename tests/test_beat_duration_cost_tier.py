"""Beats must not cross a provider billing tier for the sake of 0.1s.

FAL video lanes bill in whole duration tiers. The pipeline asks for a 10s clip
whenever a beat exceeds 5.0s, then trims the excess away - so a 5.1s beat pays
$0.98 for a $0.49 cut and discards 4.9 seconds of paid footage.

Word-weighted retiming produced exactly that: a live canary bought three 10s
clips in its first five beats and burned through its budget with the video
two-thirds unfinished. The narration length is fixed, so the fix is to
redistribute time onto the short beats rather than to truncate anything.
"""
from __future__ import annotations

import pytest

from skeleton_ai.pipeline import (
    CHEAP_CLIP_SECONDS,
    Beat,
    _cap_beat_durations,
    _retime_beats_to_narration,
)


def _beats(durations: list[float]) -> list[Beat]:
    return [
        Beat(
            index=i,
            narration=f"sentence {i}",
            outfit="",
            scene_action="",
            motion_prompt="",
            duration_sec=value,
        )
        for i, value in enumerate(durations)
    ]


def test_overlong_beats_are_brought_under_the_cheap_tier() -> None:
    beats = _beats([6.2, 5.9, 5.4, 3.0, 2.5, 4.0])
    assert _cap_beat_durations(beats) is True
    assert all(beat.duration_sec <= CHEAP_CLIP_SECONDS + 1e-6 for beat in beats), [
        beat.duration_sec for beat in beats
    ]


def test_total_duration_is_preserved_exactly() -> None:
    """Beats mark cut points in one continuous voiceover.

    If they stop summing to the narration length the video desyncs from its
    own audio, which is a far worse defect than the cost it was fixing.
    """
    durations = [6.2, 5.9, 5.4, 3.0, 2.5, 4.0]
    beats = _beats(durations)
    _cap_beat_durations(beats)
    assert sum(beat.duration_sec for beat in beats) == pytest.approx(sum(durations))


def test_beats_that_genuinely_need_long_clips_are_left_alone() -> None:
    """When the average beat exceeds the cap the long clips are really needed."""
    durations = [8.0, 7.5, 9.0]
    beats = _beats(durations)
    assert _cap_beat_durations(beats) is False
    assert [beat.duration_sec for beat in beats] == durations


def test_already_cheap_beats_are_untouched() -> None:
    durations = [4.5, 3.2, 5.0]
    beats = _beats(durations)
    assert _cap_beat_durations(beats) is True
    assert [beat.duration_sec for beat in beats] == durations


def test_no_beat_is_starved_below_the_minimum() -> None:
    beats = _beats([9.9, 0.2, 0.2, 0.2])
    _cap_beat_durations(beats)
    assert all(beat.duration_sec > 0 for beat in beats)


def test_the_real_canary_shape_stops_buying_ten_second_clips() -> None:
    """Twelve beats over ~55s of narration must cost twelve 5s clips, not more.

    This is the exact configuration that failed: mean beat 4.58s, comfortably
    inside the cheap tier, yet three of the first five clips billed at 10s.
    """
    narration_seconds = 55.0
    beats = _beats([0.0] * 12)
    for i, beat in enumerate(beats):
        # Uneven sentence lengths are what pushed individual beats over.
        beat.narration = " ".join(["word"] * (6 + (i % 5) * 7))
    _retime_beats_to_narration(beats, narration_seconds)
    assert any(beat.duration_sec > CHEAP_CLIP_SECONDS for beat in beats), (
        "test no longer reproduces the overflow it exists to guard"
    )

    _cap_beat_durations(beats)

    ten_second_clips = [beat for beat in beats if beat.duration_sec > CHEAP_CLIP_SECONDS]
    assert not ten_second_clips, [beat.duration_sec for beat in beats]
    assert sum(beat.duration_sec for beat in beats) == pytest.approx(narration_seconds)
