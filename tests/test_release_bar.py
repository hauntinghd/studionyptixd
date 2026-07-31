"""The release bar has to be passable AND failable. See RELEASE_BAR.md.

"Zero artifacting" could be neither, which is why it produced six paid canary
runs and one finished video. These tests pin the three clauses that replaced it,
and - more importantly - pin the two ways a bar quietly stops working:

  1. Unknown scoring as a pass. A render nobody inspected must not be
     release-ready just because no defect was recorded.
  2. A run grading itself on an easier scale. Threshold overrides must be
     visible in the verdict.
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from studio_agent import release_bar
from studio_agent.release_bar import (
    BeatDefect,
    RenderEvidence,
    evaluate_fleet,
    evaluate_render,
)


def _clean(**overrides) -> RenderEvidence:
    """A render that passes every clause, for one-property-at-a-time tests."""
    base = dict(
        completed=True,
        video_duration_sec=49.104,
        narration_duration_sec=49.104,
        freeze_spans=[],
        clips_total=12,
        clips_inspected=12,
        clips_with_visible_defect=0,
        beat_defects=[],
    )
    base.update(overrides)
    return RenderEvidence(**base)


def _clause(verdict: dict, name: str) -> dict:
    match = [c for c in verdict["clauses"] if c["clause"] == name]
    assert match, f"{name} missing from {[c['clause'] for c in verdict['clauses']]}"
    return match[0]


# --- The bar can be passed ----------------------------------------------------

def test_a_clean_inspected_render_is_release_ready() -> None:
    verdict = evaluate_render(_clean())
    assert verdict["release_ready"] is True, verdict["failed_clauses"]
    assert verdict["counts_as_completed"] is True


def test_one_defective_beat_repaired_in_one_attempt_still_passes() -> None:
    """The bar permits a defect. That is the point of replacing 'zero'."""
    verdict = evaluate_render(
        _clean(
            clips_with_visible_defect=0,
            beat_defects=[
                BeatDefect(beat_index=4, defect_class="continuity", repair_attempts=1, repaired=True)
            ],
        )
    )
    assert verdict["release_ready"] is True, verdict["failed_clauses"]


# --- The bar can be failed ----------------------------------------------------

def test_a_content_policy_death_fails_completion() -> None:
    verdict = evaluate_render(
        _clean(completed=False, content_policy_death=True, clips_inspected=0)
    )
    assert verdict["release_ready"] is False
    assert "completed" in verdict["failed_clauses"]
    assert verdict["counts_as_completed"] is False


def test_a_trailing_freeze_fails() -> None:
    """The mux pads to the narration clock by cloning the last frame.

    Every test in the suite passes while this ships, which is exactly why it is
    a clause rather than an assumption.
    """
    verdict = evaluate_render(_clean(freeze_spans=[(44.0, 5.0)]))
    assert verdict["release_ready"] is False
    assert "no_freeze" in verdict["failed_clauses"]
    assert verdict["counts_as_completed"] is False


def test_a_freeze_shorter_than_the_floor_is_tolerated() -> None:
    verdict = evaluate_render(_clean(freeze_spans=[(10.0, 0.2)]))
    assert _clause(verdict, "no_freeze")["passed"] is True


def test_desync_fails() -> None:
    verdict = evaluate_render(_clean(video_duration_sec=47.0, narration_duration_sec=49.104))
    assert verdict["release_ready"] is False
    assert "no_desync" in verdict["failed_clauses"]


def test_a_defect_rate_over_the_limit_fails() -> None:
    verdict = evaluate_render(_clean(clips_inspected=12, clips_with_visible_defect=3))
    assert verdict["release_ready"] is False
    assert "clip_defect_rate" in verdict["failed_clauses"]
    assert _clause(verdict, "clip_defect_rate")["defect_rate"] == pytest.approx(0.25)


def test_two_defective_beats_fail() -> None:
    verdict = evaluate_render(
        _clean(
            beat_defects=[
                BeatDefect(beat_index=3, repair_attempts=1, repaired=True),
                BeatDefect(beat_index=7, repair_attempts=1, repaired=True),
            ]
        )
    )
    assert verdict["release_ready"] is False
    assert "defective_beats" in verdict["failed_clauses"]


def test_a_beat_needing_two_attempts_fails() -> None:
    """Repair thrash is a structural limitation being paid for repeatedly."""
    verdict = evaluate_render(
        _clean(beat_defects=[BeatDefect(beat_index=5, repair_attempts=2, repaired=True)])
    )
    assert verdict["release_ready"] is False
    assert "repairable_in_one_attempt" in verdict["failed_clauses"]


def test_an_unrepaired_beat_fails() -> None:
    verdict = evaluate_render(
        _clean(beat_defects=[BeatDefect(beat_index=5, repair_attempts=1, repaired=False)])
    )
    assert "repairable_in_one_attempt" in verdict["failed_clauses"]


# --- Unknown is never a pass --------------------------------------------------

def test_an_uninspected_render_is_not_release_ready() -> None:
    """The honesty gate.

    A render nobody looked at records zero defects. Without this, absence of
    evidence scores identically to evidence of absence - and the bar becomes a
    rubber stamp on exactly the renders least examined.
    """
    verdict = evaluate_render(_clean(clips_inspected=0))
    assert verdict["release_ready"] is False
    assert set(verdict["unknown_clauses"]) >= {"clip_defect_rate", "defective_beats"}
    assert verdict["failed_clauses"] == []


def test_an_uninspected_render_can_still_count_as_completed() -> None:
    """Clause 1 is structural, so it does not need frame inspection."""
    verdict = evaluate_render(_clean(clips_inspected=0))
    assert verdict["counts_as_completed"] is True


def test_a_missing_narration_clock_is_unknown_not_pass() -> None:
    verdict = evaluate_render(_clean(narration_duration_sec=0.0))
    assert _clause(verdict, "no_desync")["status"] == "unknown"
    assert verdict["release_ready"] is False


# --- Fleet completion rate ----------------------------------------------------

def test_completion_rate_floor_is_enforced() -> None:
    verdicts = [evaluate_render(_clean()) for _ in range(19)]
    verdicts.append(evaluate_render(_clean(completed=False, clips_inspected=0)))
    fleet = evaluate_fleet(verdicts)
    assert fleet["completion_rate"] == pytest.approx(0.95)
    assert fleet["passed"] is True


def test_completion_rate_below_the_floor_fails() -> None:
    verdicts = [evaluate_render(_clean()) for _ in range(18)]
    verdicts += [evaluate_render(_clean(completed=False, clips_inspected=0)) for _ in range(2)]
    fleet = evaluate_fleet(verdicts)
    assert fleet["completion_rate"] == pytest.approx(0.90)
    assert fleet["passed"] is False


def test_the_six_run_canary_history_fails_the_bar() -> None:
    """Sanity check against reality: 1 of 6 runs finished.

    If the bar passed this history it would not be measuring anything.
    """
    history = [evaluate_render(_clean(completed=False, clips_inspected=0)) for _ in range(5)]
    history.append(evaluate_render(_clean()))
    fleet = evaluate_fleet(history)
    assert fleet["passed"] is False
    assert fleet["completion_rate"] == pytest.approx(1 / 6, abs=0.01)


def test_an_empty_fleet_is_not_a_pass() -> None:
    assert evaluate_fleet([])["passed"] is False


# --- Thresholds ---------------------------------------------------------------

def test_overrides_are_recorded_in_the_verdict(monkeypatch: pytest.MonkeyPatch) -> None:
    """A run must not be able to quietly grade itself on an easier scale."""
    monkeypatch.setenv("STUDIO_BAR_MAX_CLIP_DEFECT_RATE", "0.5")
    verdict = evaluate_render(_clean(clips_inspected=12, clips_with_visible_defect=3))
    assert verdict["release_ready"] is True
    assert "max_clip_defect_rate" in verdict["thresholds"]["overridden"]


def test_default_thresholds_report_no_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in (
        "STUDIO_BAR_MIN_COMPLETION_RATE", "STUDIO_BAR_MAX_CLIP_DEFECT_RATE",
        "STUDIO_BAR_MAX_DEFECTIVE_BEATS", "STUDIO_BAR_MAX_REPAIR_ATTEMPTS",
        "STUDIO_BAR_FREEZE_MIN_SEC", "STUDIO_BAR_DESYNC_TOLERANCE_SEC",
    ):
        monkeypatch.delenv(key, raising=False)
    assert evaluate_render(_clean())["thresholds"]["overridden"] == []


def test_a_malformed_override_falls_back_to_the_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STUDIO_BAR_MAX_CLIP_DEFECT_RATE", "not-a-number")
    assert release_bar.threshold("max_clip_defect_rate") == pytest.approx(0.05)


# --- Probes run against real media -------------------------------------------

@pytest.mark.skipif(
    not shutil.which("ffmpeg") or not shutil.which("ffprobe"),
    reason="ffmpeg/ffprobe required to probe real media",
)
def test_freeze_probe_finds_a_real_frozen_tail(tmp_path: Path) -> None:
    """Measured off a real file, not asserted against a filter string."""
    moving = tmp_path / "moving.mp4"
    frozen = tmp_path / "frozen.mp4"
    subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error", "-f", "lavfi",
         "-i", "testsrc=size=180x320:rate=30:duration=6",
         "-c:v", "libx264", "-pix_fmt", "yuv420p", str(moving)],
        check=True, capture_output=True,
    )
    subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error", "-f", "lavfi",
         "-i", "color=c=navy:size=180x320:rate=30:duration=6",
         "-c:v", "libx264", "-pix_fmt", "yuv420p", str(frozen)],
        check=True, capture_output=True,
    )
    assert release_bar.probe_freeze_spans(frozen), "a fully static clip was not detected"
    assert not release_bar.probe_freeze_spans(moving), "moving footage flagged as frozen"


@pytest.mark.skipif(
    not shutil.which("ffprobe"), reason="ffprobe required"
)
def test_duration_probe_reads_real_media(tmp_path: Path) -> None:
    clip = tmp_path / "clip.mp4"
    subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error", "-f", "lavfi",
         "-i", "testsrc=size=180x320:rate=30:duration=4",
         "-c:v", "libx264", "-pix_fmt", "yuv420p", str(clip)],
        check=True, capture_output=True,
    )
    assert release_bar.probe_duration(clip) == pytest.approx(4.0, abs=0.2)


def test_a_missing_file_probes_to_zero_rather_than_raising(tmp_path: Path) -> None:
    assert release_bar.probe_duration(tmp_path / "nope.mp4") == 0.0


@pytest.mark.skipif(
    not shutil.which("ffmpeg") or not shutil.which("ffprobe"),
    reason="ffmpeg/ffprobe required to probe real media",
)
def test_freeze_probe_catches_a_freeze_that_runs_to_end_of_file(tmp_path: Path) -> None:
    """The defect the bar exists for, and the one freezedetect reports worst.

    A freeze that ends emits start/duration/end. A freeze running to EOF emits
    only freeze_start - so a naive parser pairing starts with durations drops
    exactly the trailing freeze the mux produces when beats under-cover the
    narration, and reports the video clean.
    """
    clip = tmp_path / "motion_then_freeze.mp4"
    subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error",
         "-f", "lavfi", "-i", "testsrc=size=180x320:rate=30:duration=3",
         "-f", "lavfi", "-i", "color=c=navy:size=180x320:rate=30:duration=3",
         "-filter_complex", "[0:v][1:v]concat=n=2:v=1:a=0",
         "-c:v", "libx264", "-pix_fmt", "yuv420p", str(clip)],
        check=True, capture_output=True,
    )
    spans = release_bar.probe_freeze_spans(clip)
    assert spans, "a freeze running to EOF was reported as no freeze"
    start, duration = spans[-1]
    assert start == pytest.approx(3.0, abs=0.3)
    assert duration == pytest.approx(3.0, abs=0.5), "open freeze not closed against media duration"


@pytest.mark.skipif(
    not shutil.which("ffmpeg") or not shutil.which("ffprobe"),
    reason="ffmpeg/ffprobe required to probe real media",
)
def test_freeze_probe_measures_a_freeze_that_ends(tmp_path: Path) -> None:
    clip = tmp_path / "freeze_then_motion.mp4"
    subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error",
         "-f", "lavfi", "-i", "color=c=navy:size=180x320:rate=30:duration=3",
         "-f", "lavfi", "-i", "testsrc=size=180x320:rate=30:duration=3",
         "-filter_complex", "[0:v][1:v]concat=n=2:v=1:a=0",
         "-c:v", "libx264", "-pix_fmt", "yuv420p", str(clip)],
        check=True, capture_output=True,
    )
    spans = release_bar.probe_freeze_spans(clip)
    assert spans
    assert spans[0][1] == pytest.approx(3.0, abs=0.5)


@pytest.mark.skipif(
    not shutil.which("ffmpeg") or not shutil.which("ffprobe"),
    reason="ffmpeg/ffprobe required to probe real media",
)
def test_a_real_trailing_freeze_fails_the_bar_end_to_end(tmp_path: Path) -> None:
    """Probe and verdict wired together against real media, not fixtures."""
    clip = tmp_path / "trailing.mp4"
    subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error",
         "-f", "lavfi", "-i", "testsrc=size=180x320:rate=30:duration=3",
         "-f", "lavfi", "-i", "color=c=navy:size=180x320:rate=30:duration=3",
         "-filter_complex", "[0:v][1:v]concat=n=2:v=1:a=0",
         "-c:v", "libx264", "-pix_fmt", "yuv420p", str(clip)],
        check=True, capture_output=True,
    )
    duration = release_bar.probe_duration(clip)
    verdict = evaluate_render(
        RenderEvidence(
            completed=True,
            video_duration_sec=duration,
            narration_duration_sec=duration,
            freeze_spans=release_bar.probe_freeze_spans(clip),
            clips_total=1,
            clips_inspected=1,
            clips_with_visible_defect=0,
        )
    )
    assert verdict["release_ready"] is False
    assert "no_freeze" in verdict["failed_clauses"]
    assert verdict["counts_as_completed"] is False
