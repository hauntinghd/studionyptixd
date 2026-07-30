"""Short-form videos ship with a music bed under the narration.

The skeleton short-form pipeline had no music or sound design at all - it muxed
raw narration over silent video. `audio.py` already carried a BGM mixer, but
only long-form ever used it.

Two properties matter more than the mix sounding pleasant, because both are
silent failures a viewer would notice before any test did:

1. Adding music must not make the narration quieter. `amix` divides every input
   by the input count by default, so the naive graph drops the voice ~6dB.
2. A video with no bed configured must come out exactly as it does today.

These are asserted against real ffmpeg output, not against the filter string.
"""
from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

from skeleton_ai.compose import mux_narration, probe_duration
from skeleton_ai.pipeline import resolve_music_bed


pytestmark = pytest.mark.skipif(
    not shutil.which("ffmpeg") or not shutil.which("ffprobe"),
    reason="ffmpeg/ffprobe are required to measure the rendered mix",
)


def _run(args: list[str]) -> None:
    subprocess.run(args, check=True, capture_output=True)


def _mean_dbfs(path: Path, *, start: float, length: float) -> float:
    proc = subprocess.run(
        [
            "ffmpeg", "-hide_banner", "-nostats",
            "-ss", str(start), "-t", str(length),
            "-i", str(path), "-af", "volumedetect", "-f", "null", "-",
        ],
        capture_output=True,
        text=True,
    )
    match = re.search(r"mean_volume:\s*(-?\d+(?:\.\d+)?) dB", proc.stderr)
    assert match, f"volumedetect produced no reading:\n{proc.stderr[-800:]}"
    return float(match.group(1))


@pytest.fixture(scope="module")
def fixtures(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    d = tmp_path_factory.mktemp("musicbed")
    video, narration, music = d / "silent.mp4", d / "narr.mp3", d / "music.mp3"
    _run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-f", "lavfi", "-i", "color=c=navy:s=270x480:d=8:r=30",
        "-c:v", "libx264", "-pix_fmt", "yuv420p", str(video),
    ])
    # Speech for the first 4s, silence for the last 4s: the gap is where a bed
    # has to be audible, and the speech is where it must not intrude.
    _run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-f", "lavfi", "-i", "sine=frequency=220:duration=4",
        "-f", "lavfi", "-i", "anullsrc=r=44100:cl=mono:d=4",
        "-filter_complex", "[0:a][1:a]concat=n=2:v=0:a=1",
        "-c:a", "libmp3lame", str(narration),
    ])
    _run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-f", "lavfi", "-i", "sine=frequency=660:duration=3",
        "-c:a", "libmp3lame", str(music),
    ])
    return {"dir": d, "video": video, "narration": narration, "music": music}


@pytest.fixture(scope="module")
def rendered(fixtures: dict[str, Path]) -> dict[str, Path]:
    with_music = fixtures["dir"] / "with_music.mp4"
    without = fixtures["dir"] / "without_music.mp4"
    mux_narration(
        fixtures["video"], fixtures["narration"], with_music, music_track=fixtures["music"]
    )
    mux_narration(fixtures["video"], fixtures["narration"], without)
    return {"with_music": with_music, "without": without}


def test_music_does_not_shorten_the_video(rendered: dict[str, Path]) -> None:
    assert probe_duration(rendered["with_music"]) == pytest.approx(
        probe_duration(rendered["without"]), abs=0.05
    )


def test_the_narration_is_not_quieter_when_music_is_added(
    rendered: dict[str, Path],
) -> None:
    """The regression that `amix=normalize=0` exists to prevent."""
    speech_without = _mean_dbfs(rendered["without"], start=1, length=2)
    speech_with = _mean_dbfs(rendered["with_music"], start=1, length=2)
    assert speech_with > speech_without - 1.0, (
        f"narration lost {speech_without - speech_with:.1f}dB when music was added"
    )


def test_the_bed_is_audible_between_lines(rendered: dict[str, Path]) -> None:
    gap_without = _mean_dbfs(rendered["without"], start=5, length=2)
    gap_with = _mean_dbfs(rendered["with_music"], start=5, length=2)
    assert gap_with > gap_without + 20.0, (
        f"no bed in the silence: {gap_without:.1f}dB -> {gap_with:.1f}dB"
    )


def test_the_bed_stays_well_under_the_voice(rendered: dict[str, Path]) -> None:
    """Background music, not a duet."""
    speech = _mean_dbfs(rendered["with_music"], start=1, length=2)
    gap = _mean_dbfs(rendered["with_music"], start=5, length=2)
    assert gap < speech - 8.0, f"bed at {gap:.1f}dB is too close to voice {speech:.1f}dB"


def test_a_short_track_is_looped_to_cover_the_whole_video(
    fixtures: dict[str, Path], rendered: dict[str, Path]
) -> None:
    """The fixture bed is 3s against an 8s video."""
    assert probe_duration(fixtures["music"]) < probe_duration(rendered["with_music"])
    late = _mean_dbfs(rendered["with_music"], start=5, length=2)
    assert late > -80.0, "the bed ran out before the video ended"


def test_no_library_configured_means_no_music(monkeypatch: pytest.MonkeyPatch) -> None:
    """Silence must remain the default, not an accidental provider cost."""
    monkeypatch.delenv("STUDIO_MUSIC_BED_DIR", raising=False)
    assert resolve_music_bed(seed="psychology") is None


def test_bed_choice_is_stable_across_re_renders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Re-rendering a video must not shuffle its soundtrack."""
    library = tmp_path / "beds"
    library.mkdir()
    for name in ("a.mp3", "b.mp3", "c.mp3", "d.mp3"):
        (library / name).write_bytes(b"\x00" * 64)
    monkeypatch.setenv("STUDIO_MUSIC_BED_DIR", str(library))

    first = resolve_music_bed(seed="why you test the people who love you")
    again = resolve_music_bed(seed="why you test the people who love you")
    assert first is not None and first == again


def test_an_explicit_track_always_wins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    library = tmp_path / "beds"
    library.mkdir()
    (library / "library.mp3").write_bytes(b"\x00" * 64)
    monkeypatch.setenv("STUDIO_MUSIC_BED_DIR", str(library))
    pinned = tmp_path / "pinned.mp3"
    pinned.write_bytes(b"\x00" * 64)
    assert resolve_music_bed(pinned, seed="anything") == pinned


def test_a_missing_explicit_track_does_not_fail_the_render(tmp_path: Path) -> None:
    """A bad path must lose the music, never the video."""
    assert resolve_music_bed(tmp_path / "nope.mp3", seed="x") is None


def test_empty_files_are_not_treated_as_tracks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    library = tmp_path / "beds"
    library.mkdir()
    (library / "truncated.mp3").write_bytes(b"")
    monkeypatch.setenv("STUDIO_MUSIC_BED_DIR", str(library))
    assert resolve_music_bed(seed="x") is None
