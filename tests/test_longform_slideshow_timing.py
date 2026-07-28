"""Regression: the long-form slideshow must hold each still for its full
interval. The History Rewind disaster (300+ images in ~10s, then one frozen
frame for ~4 hours) was caused by zoompan d=1 collapsing every still to a
single output frame. zoompan emits `d` output frames per input still, so d must
be per_scene * fps.
"""
from __future__ import annotations

import pytest

from long_form.pipeline import _ken_burns_motion_filter


@pytest.mark.parametrize(
    "per_scene,fps,expected_d",
    [(2.0, 60, 120), (47.5, 60, 2850), (5.0, 30, 150), (0.5, 60, 30)],
)
def test_each_still_held_for_full_interval(per_scene, fps, expected_d) -> None:
    f = _ken_burns_motion_filter(per_scene, fps)
    assert f"d={expected_d}:" in f, f
    # The exact bug: d=1 collapsed every still to one frame.
    assert "d=1:" not in f


@pytest.mark.parametrize("per_scene,fps", [(2.0, 60), (47.5, 60), (5.0, 30)])
def test_motion_is_output_frame_based_not_input_time(per_scene, fps) -> None:
    # All `d` frames of a still share one input timestamp `it`; motion driven by
    # `it` would freeze the pan per still. It must use output-frame time `on`.
    f = _ken_burns_motion_filter(per_scene, fps)
    assert "on" in f
    assert "mod(it," not in f
    assert "sin(it" not in f
    assert "cos(it" not in f


def test_pathologically_short_interval_still_yields_one_frame() -> None:
    # Never emit d=0 (would drop the still entirely).
    assert "d=1:" in _ken_burns_motion_filter(0.001, 60)
