"""Measure the production grammar of a reference video from the file itself.

Catalyst already reads a reference video's metadata, transcript and screenshots.
None of that answers the questions that actually decide whether our render looks
like the thing the creator admires: how fast does it cut, how long does a shot
hold, how loud is the master, how dense is the narration.

Those are measurable from the file with ffmpeg alone -- no provider spend, no
vision model -- and they turn "study this video" into a numeric target the
director can lock and the compile stage can be graded against.

This runs backend-side on purpose. The desktop app could shell out to a local
ffmpeg, but then web and desktop would report different numbers for the same
URL, and a target spec that changes with the client is not a spec.
"""
from __future__ import annotations

import json
import re
import shutil
import statistics
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

#: Scene-change score above which ffmpeg's `select` filter calls it a cut.
#: 0.30 is deliberately mid-range: lower floods on camera movement inside a
#: single shot, higher misses cuts between visually similar setups.
SCENE_THRESHOLD = 0.30

#: Silence floor for speech-gap detection. -32dB sits below narration but above
#: the noise of a music bed, so a continuous-score video reports few gaps
#: rather than none.
SILENCE_DB = -32.0
SILENCE_MIN_SEC = 0.35

#: Hook windows, in seconds. Retention is decided early, so pacing over the
#: opening is reported separately from the whole-file average.
HOOK_WINDOWS = (30, 60, 120)


class MeasurementUnavailable(RuntimeError):
    """ffmpeg/ffprobe is not on PATH, or the file could not be read."""


@dataclass
class ShotProfile:
    cut_count: int = 0
    mean_shot_sec: float = 0.0
    median_shot_sec: float = 0.0
    p10_sec: float = 0.0
    p25_sec: float = 0.0
    p75_sec: float = 0.0
    p90_sec: float = 0.0
    max_shot_sec: float = 0.0
    under_2s: int = 0
    from_2_to_5s: int = 0
    from_5_to_10s: int = 0
    over_10s: int = 0
    pct_under_5s: float = 0.0
    hook_pacing: dict[str, float] = field(default_factory=dict)


@dataclass
class AudioProfile:
    integrated_lufs: float | None = None
    loudness_range: float | None = None
    silence_gap_count: int = 0
    total_silence_sec: float = 0.0
    speech_density: float = 0.0


@dataclass
class ReferenceSpec:
    """The measured production grammar of one reference video."""

    duration_sec: float = 0.0
    width: int = 0
    height: int = 0
    fps: float = 0.0
    shots: ShotProfile = field(default_factory=ShotProfile)
    audio: AudioProfile = field(default_factory=AudioProfile)
    narration_wpm: float | None = None
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def as_target_directives(self) -> list[str]:
        """Render the measurement as instructions a director agent can lock.

        Deliberately phrased as targets rather than observations: this text is
        meant to be pasted into a style contract, not into a report.
        """
        out: list[str] = []
        s = self.shots
        if s.median_shot_sec:
            out.append(f"Target median shot length {s.median_shot_sec:.1f}s.")
        if s.pct_under_5s:
            out.append(f"Keep {s.pct_under_5s:.0f}% of shots under 5s.")
        hook = s.hook_pacing.get("60s")
        if hook:
            out.append(f"Open at {hook:.1f}s per shot across the first 60s.")
        if self.audio.integrated_lufs is not None:
            out.append(
                f"Master to {self.audio.integrated_lufs:.1f} LUFS integrated."
            )
        if self.narration_wpm:
            out.append(f"Write narration at ~{self.narration_wpm:.0f} words per minute.")
        return out


def _require_tools() -> None:
    for tool in ("ffmpeg", "ffprobe"):
        if not shutil.which(tool):
            raise MeasurementUnavailable(f"{tool} not found on PATH")


def _run(cmd: list[str], timeout: int = 900) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout, check=False
    )


def _probe_container(path: Path) -> tuple[float, int, int, float]:
    proc = _run([
        "ffprobe", "-v", "error", "-select_streams", "v:0",
        "-show_entries", "stream=width,height,r_frame_rate:format=duration",
        "-of", "json", str(path),
    ])
    try:
        data = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise MeasurementUnavailable(f"ffprobe returned no parseable data: {exc}") from exc

    stream = (data.get("streams") or [{}])[0]
    duration = float((data.get("format") or {}).get("duration") or 0.0)
    width = int(stream.get("width") or 0)
    height = int(stream.get("height") or 0)

    fps = 0.0
    raw_fps = str(stream.get("r_frame_rate") or "")
    if "/" in raw_fps:
        num, _, den = raw_fps.partition("/")
        try:
            fps = float(num) / float(den) if float(den) else 0.0
        except (ValueError, ZeroDivisionError):
            fps = 0.0
    return duration, width, height, fps


def _detect_cuts(path: Path) -> list[float]:
    """Return scene-change timestamps in seconds."""
    proc = _run([
        "ffmpeg", "-v", "error", "-i", str(path),
        "-filter:v", f"select='gt(scene,{SCENE_THRESHOLD})',metadata=print:file=-",
        "-f", "null", "-",
    ])
    return [
        float(m) for m in re.findall(r"pts_time:([0-9.]+)", proc.stdout or "")
    ]


def _shot_profile(cuts: list[float], duration: float) -> ShotProfile:
    prof = ShotProfile(cut_count=len(cuts))
    if not cuts or duration <= 0:
        return prof

    prof.mean_shot_sec = round(duration / max(len(cuts), 1), 2)
    gaps = [round(b - a, 3) for a, b in zip(cuts, cuts[1:])]
    if not gaps:
        return prof

    ordered = sorted(gaps)

    def pct(p: float) -> float:
        idx = min(len(ordered) - 1, max(0, int(len(ordered) * p)))
        return round(ordered[idx], 2)

    prof.median_shot_sec = round(statistics.median(gaps), 2)
    prof.p10_sec, prof.p25_sec = pct(0.10), pct(0.25)
    prof.p75_sec, prof.p90_sec = pct(0.75), pct(0.90)
    prof.max_shot_sec = round(max(gaps), 2)
    prof.under_2s = sum(1 for g in gaps if g < 2)
    prof.from_2_to_5s = sum(1 for g in gaps if 2 <= g < 5)
    prof.from_5_to_10s = sum(1 for g in gaps if 5 <= g < 10)
    prof.over_10s = sum(1 for g in gaps if g >= 10)
    prof.pct_under_5s = round(
        100.0 * sum(1 for g in gaps if g < 5) / len(gaps), 1
    )

    for window in HOOK_WINDOWS:
        if duration < window:
            continue
        early = [c for c in cuts if c <= window]
        prof.hook_pacing[f"{window}s"] = round(window / max(len(early), 1), 2)
    return prof


def _audio_profile(path: Path, duration: float) -> AudioProfile:
    prof = AudioProfile()

    loud = _run([
        "ffmpeg", "-i", str(path), "-filter:a", "ebur128=peak=true", "-f", "null", "-",
    ])
    # ebur128 streams a running per-frame readout and *then* prints a Summary.
    # The running values start at the -70 LUFS silence floor, so matching the
    # first "I:" in the stream reports silence for every file. Parse the
    # Summary block only; fall back to the last running value if it is absent.
    blob = loud.stderr or ""
    _, _, summary = blob.rpartition("Summary:")
    scope = summary or blob

    i_matches = re.findall(r"I:\s*(-?[0-9.]+)\s*LUFS", scope)
    if i_matches:
        prof.integrated_lufs = round(float(i_matches[-1]), 1)
    lra_matches = re.findall(r"LRA:\s*(-?[0-9.]+)\s*LU", scope)
    if lra_matches:
        prof.loudness_range = round(float(lra_matches[-1]), 1)

    sil = _run([
        "ffmpeg", "-i", str(path),
        "-af", f"silencedetect=noise={SILENCE_DB}dB:d={SILENCE_MIN_SEC}",
        "-f", "null", "-",
    ])
    gaps = [float(m) for m in re.findall(r"silence_duration:\s*([0-9.]+)", sil.stderr or "")]
    prof.silence_gap_count = len(gaps)
    prof.total_silence_sec = round(sum(gaps), 2)
    if duration > 0:
        prof.speech_density = round(
            100.0 * (1.0 - min(sum(gaps), duration) / duration), 1
        )
    return prof


def narration_wpm_from_vtt(vtt_text: str) -> float | None:
    """Words per minute from a WebVTT track.

    Auto-generated captions repeat each line as the rolling caption advances,
    so consecutive duplicates are collapsed before counting or the rate comes
    out roughly double.
    """
    if not vtt_text.strip():
        return None
    lines: list[str] = []
    for raw in vtt_text.splitlines():
        line = raw.strip()
        if not line or "-->" in line:
            continue
        if line.startswith(("WEBVTT", "Kind:", "Language:", "NOTE")):
            continue
        clean = re.sub(r"<[^>]+>", "", line)
        if not lines or lines[-1] != clean:
            lines.append(clean)
    words = sum(len(line.split()) for line in lines)

    stamps = re.findall(r"(\d\d):(\d\d):(\d\d)\.\d+\s*-->", vtt_text)
    if not stamps or not words:
        return None
    span = max(int(h) * 3600 + int(m) * 60 + int(s) for h, m, s in stamps)
    if span <= 0:
        return None
    return round(words / (span / 60.0), 1)


def measure_reference(
    video_path: str | Path, *, vtt_text: str = ""
) -> ReferenceSpec:
    """Measure one reference video and return a lockable target spec."""
    _require_tools()
    path = Path(video_path)
    if not path.is_file() or path.stat().st_size == 0:
        raise MeasurementUnavailable(f"reference file missing or empty: {path}")

    duration, width, height, fps = _probe_container(path)
    spec = ReferenceSpec(
        duration_sec=round(duration, 2), width=width, height=height, fps=round(fps, 2)
    )

    cuts = _detect_cuts(path)
    spec.shots = _shot_profile(cuts, duration)
    if not cuts:
        spec.notes.append(
            "No scene changes detected; the reference may be a single continuous "
            "take or a slideshow whose transitions fall below the cut threshold."
        )

    try:
        spec.audio = _audio_profile(path, duration)
    except subprocess.TimeoutExpired:
        spec.notes.append("Audio analysis timed out; loudness targets unavailable.")

    if vtt_text:
        spec.narration_wpm = narration_wpm_from_vtt(vtt_text)

    return spec
