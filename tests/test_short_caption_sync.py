from __future__ import annotations

import json
from pathlib import Path

import pytest

from skeleton_ai import captions
from skeleton_ai import compose
from studio_agent import render_qa


def test_dense_heuristic_timeline_never_exceeds_final_clock() -> None:
    phrases = [f"WORD{i}" for i in range(25)]
    cues = captions.time_phrases(phrases, 3.0)

    assert len(cues) == len(phrases)
    assert cues[-1].start_sec + cues[-1].duration_sec == pytest.approx(3.0)
    assert all(cue.start_sec + cue.duration_sec <= 3.0 + 1e-9 for cue in cues)


def test_word_mode_fails_closed_without_verified_audio_timestamps() -> None:
    with pytest.raises(captions.CaptionTimingError, match="verified timestamps"):
        captions.build_timed_captions("one two", 1.0, caption_mode="word")


def test_verified_word_mode_preserves_real_timestamps_and_provenance() -> None:
    cues, source = captions.build_timed_captions(
        "one two",
        1.0,
        caption_mode="word",
        verified_word_timings=[
            {"text": "one", "start": 0.12, "end": 0.42},
            {"text": "two", "start": 0.50, "end": 0.94},
        ],
    )

    assert source == "verified_word"
    assert [(cue.text, cue.start_sec, cue.duration_sec) for cue in cues] == [
        ("ONE", 0.12, pytest.approx(0.30)),
        ("TWO", 0.50, pytest.approx(0.44)),
    ]


def test_global_verified_cues_do_not_guess_scene_token_boundaries() -> None:
    # Whisper may split a hyphenated number differently than the script.  The
    # export uses its one global audio clock, so no scene token-count match is
    # required and no paid alignment result is discarded.
    cues, source = captions.build_timed_captions(
        "A 28-year-old can't wait.",
        2.0,
        caption_mode="word",
        verified_word_timings=[
            {"text": "a", "start": 0.05, "end": 0.18},
            {"text": "twenty", "start": 0.20, "end": 0.48},
            {"text": "eight", "start": 0.50, "end": 0.70},
            {"text": "year", "start": 0.72, "end": 0.94},
            {"text": "old", "start": 0.96, "end": 1.10},
            {"text": "cannot", "start": 1.14, "end": 1.48},
            {"text": "wait", "start": 1.52, "end": 1.88},
        ],
    )

    assert source == "verified_word"
    assert len(cues) == 7
    assert cues[-1].start_sec + cues[-1].duration_sec <= 2.0


def test_trim_cache_uses_content_fingerprint_and_filter_pads_picture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    src = tmp_path / "source.mp4"
    out = tmp_path / "trimmed.mp4"
    src.write_bytes(b"a" * 2048)
    commands: list[list[str]] = []

    monkeypatch.setattr(compose, "has_audio", lambda _path: False)

    def fake_run(command, **_kwargs):
        commands.append([str(part) for part in command])
        Path(command[-1]).write_bytes(b"v" * 2048)
        return type("Result", (), {"stdout": "", "returncode": 0})()

    monkeypatch.setattr(compose.subprocess, "run", fake_run)
    timings = [
        {"text": "hello", "start": 0.1, "end": 0.4},
        {"text": "world", "start": 0.5, "end": 0.9},
    ]

    compose.trim_with_captions(
        src,
        out,
        duration_sec=1.0,
        narration_text="hello world",
        caption_mode="word",
        verified_word_timings=timings,
    )
    assert len(commands) == 1
    filter_text = (tmp_path / "trimmed_filter.txt").read_text(encoding="utf-8")
    assert "setpts=PTS-STARTPTS" in filter_text
    assert "tpad=stop_mode=clone" in filter_text
    assert "trim=duration=1.000000" in filter_text

    # Same bytes/config reuse the render; path existence alone is not enough.
    compose.trim_with_captions(
        src,
        out,
        duration_sec=1.0,
        narration_text="hello world",
        caption_mode="word",
        verified_word_timings=timings,
    )
    assert len(commands) == 1

    src.write_bytes(b"b" * 2048)
    compose.trim_with_captions(
        src,
        out,
        duration_sec=1.0,
        narration_text="hello world",
        caption_mode="word",
        verified_word_timings=timings,
    )
    assert len(commands) == 2
    manifest = json.loads((tmp_path / "trimmed.mp4.captions.json").read_text(encoding="utf-8"))
    assert manifest["timing_source"] == "verified_word"
    assert max(cue["end"] for cue in manifest["cues"]) <= 1.0


def test_mux_uses_audio_clock_and_never_shortest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    silent = tmp_path / "silent.mp4"
    audio = tmp_path / "voice.wav"
    out = tmp_path / "final.mp4"
    silent.write_bytes(b"v" * 2048)
    audio.write_bytes(b"a" * 2048)
    captured: list[str] = []

    monkeypatch.setattr(compose, "probe_duration", lambda _path: 4.25)
    monkeypatch.setattr(compose, "strip_clip_audio", lambda path: path)

    def fake_run(command, **_kwargs):
        captured.extend(str(part) for part in command)
        return type("Result", (), {"stdout": "", "returncode": 0})()

    monkeypatch.setattr(compose.subprocess, "run", fake_run)
    compose.mux_narration(
        silent,
        audio,
        out,
        caption_phrases=[captions.CaptionPhrase("SYNC", 0.25, 0.5, False)],
    )

    filter_text = out.with_suffix(out.suffix + ".mux-filter.txt").read_text(encoding="utf-8")
    assert "-shortest" not in captured
    assert "-filter_script:v" in captured
    assert "tpad=stop_mode=clone" in filter_text
    assert "trim=duration=4.250000" in filter_text
    assert "drawtext=" in filter_text
    assert captured[captured.index("-t") + 1] == "4.250000"


def test_qa_av_sync_enforces_one_frame_tolerance() -> None:
    passing: list[dict] = []
    render_qa._av_sync_check(
        passing,
        {
            "duration": 10.0,
            "fps": 30.0,
            "video_start": 0.0,
            "audio_start": 1 / 30,
            "video_duration": 10.0,
            "audio_duration": 10.0,
        },
    )
    assert passing[0]["status"] == "pass"

    failing: list[dict] = []
    render_qa._av_sync_check(
        failing,
        {
            "duration": 10.0,
            "fps": 30.0,
            "video_start": 0.0,
            "audio_start": 0.04,
            "video_duration": 10.0,
            "audio_duration": 10.0,
        },
    )
    assert failing[0]["status"] == "fail"


def test_qa_rejects_unverified_word_caption_manifest(tmp_path: Path) -> None:
    manifest = tmp_path / "captions.json"
    manifest.write_text(
        json.dumps({
            "enabled": True,
            "mode": "word",
            "timing_source": "script_weighted_estimate",
            "duration_sec": 1.0,
            "cues": [{"text": "NO", "start": 0.0, "end": 0.5}],
        }),
        encoding="utf-8",
    )
    checks: list[dict] = []
    render_qa._caption_timeline_check(checks, manifest, 1.0, 30.0)
    assert checks[0]["status"] == "fail"
    assert "verified" in checks[0]["detail"].lower()
