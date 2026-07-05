"""Local multimodal signal extraction for ClipLab.

This is not a replacement for Opus-level proprietary models, but it gives
ClipLab real non-transcript evidence: motion, scene-change pressure, lighting,
and audio energy. The scorer can then rank moments that look/sound like clips,
not just moments that read well in a transcript.
"""
from __future__ import annotations

import logging
import math
import subprocess
from pathlib import Path
from typing import Any

import cv2
import numpy as np

from cliplab.models import TranscriptCue

_log = logging.getLogger("nyptid-studio.cliplab.signals")


def _duration(video_path: str) -> float:
    try:
        r = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                video_path,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        return float((r.stdout or "0").strip() or 0)
    except Exception:
        return 0.0


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    return float(np.percentile(np.asarray(values, dtype=np.float32), q))


def _visual_signals(video_path: str, *, sample_every_sec: float = 1.5, max_samples: int = 7200) -> list[dict[str, float]]:
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return []
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0
    duration = (frame_count / fps) if fps > 0 and frame_count > 0 else _duration(video_path)
    step = max(1, int(round(fps * max(0.5, sample_every_sec))))
    samples: list[dict[str, float]] = []
    prev_gray = None
    frame_idx = 0
    taken = 0
    while taken < max_samples:
        ok, frame = cap.read()
        if not ok:
            break
        if frame_idx % step != 0:
            frame_idx += 1
            continue
        t = frame_idx / fps if fps else taken * sample_every_sec
        if duration and t > duration:
            break
        small = cv2.resize(frame, (256, 144), interpolation=cv2.INTER_AREA)
        gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)
        hsv = cv2.cvtColor(small, cv2.COLOR_BGR2HSV)
        brightness = float(np.mean(gray))
        contrast = float(np.std(gray))
        saturation = float(np.mean(hsv[:, :, 1]))
        motion = 0.0
        if prev_gray is not None:
            motion = float(np.mean(cv2.absdiff(gray, prev_gray)))
        prev_gray = gray
        samples.append({
            "t": round(float(t), 3),
            "motion": round(motion, 4),
            "brightness": round(brightness, 4),
            "contrast": round(contrast, 4),
            "saturation": round(saturation, 4),
        })
        taken += 1
        frame_idx += 1
    cap.release()
    if not samples:
        return []
    motions = [s["motion"] for s in samples]
    threshold = max(_percentile(motions, 82), (sum(motions) / max(1, len(motions))) * 1.45)
    for row in samples:
        row["scene_change"] = 1.0 if row["motion"] >= threshold and row["motion"] > 3.0 else 0.0
    return samples


def _audio_energy(video_path: str, *, sample_rate: int = 8000, window_sec: float = 1.0) -> list[dict[str, float]]:
    if not Path(video_path).exists():
        return []
    cmd = [
        "ffmpeg", "-v", "error", "-i", video_path,
        "-vn", "-ac", "1", "-ar", str(sample_rate), "-f", "s16le", "-",
    ]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception:
        return []
    assert proc.stdout is not None
    chunk_bytes = max(2, int(sample_rate * window_sec) * 2)
    rows: list[dict[str, float]] = []
    idx = 0
    try:
        while True:
            chunk = proc.stdout.read(chunk_bytes)
            if not chunk:
                break
            if len(chunk) < 2:
                break
            arr = np.frombuffer(chunk, dtype=np.int16).astype(np.float32)
            if arr.size == 0:
                continue
            rms = float(np.sqrt(np.mean(np.square(arr))) / 32768.0)
            peak = float(np.max(np.abs(arr)) / 32768.0)
            rows.append({"t": round(idx * window_sec, 3), "rms": round(rms, 6), "peak": round(peak, 6)})
            idx += 1
    finally:
        try:
            proc.kill()
        except Exception:
            pass
    if not rows:
        return []
    rms_values = [r["rms"] for r in rows]
    hot = max(_percentile(rms_values, 84), (sum(rms_values) / max(1, len(rms_values))) * 1.35)
    for row in rows:
        row["audio_spike"] = 1.0 if row["rms"] >= hot and row["rms"] > 0.01 else 0.0
    return rows


def _rows_between(rows: list[dict[str, float]], start: float, end: float) -> list[dict[str, float]]:
    return [r for r in rows if start <= float(r.get("t", 0.0)) <= end]


def summarize_segment_signals(signals: dict[str, Any], start: float, end: float) -> dict[str, Any]:
    visuals = _rows_between(list(signals.get("visual") or []), start, end)
    audio = _rows_between(list(signals.get("audio") or []), start, end)
    motion = [float(r.get("motion") or 0.0) for r in visuals]
    contrast = [float(r.get("contrast") or 0.0) for r in visuals]
    saturation = [float(r.get("saturation") or 0.0) for r in visuals]
    scene_changes = sum(float(r.get("scene_change") or 0.0) for r in visuals)
    rms = [float(r.get("rms") or 0.0) for r in audio]
    audio_spikes = sum(float(r.get("audio_spike") or 0.0) for r in audio)
    motion_score = min(100.0, _percentile(motion, 85) * 4.0) if motion else 0.0
    visual_score = min(100.0, motion_score * 0.55 + min(100.0, _percentile(contrast, 75) * 1.5) * 0.25 + min(100.0, _percentile(saturation, 75) * 0.7) * 0.20)
    audio_score = min(100.0, (_percentile(rms, 85) * 1800.0) + audio_spikes * 6.0) if rms else 0.0
    change_rate = scene_changes / max(1.0, (end - start) / 10.0)
    return {
        "visual_score": round(visual_score, 2),
        "audio_score": round(audio_score, 2),
        "motion_score": round(motion_score, 2),
        "scene_changes": int(scene_changes),
        "scene_change_rate": round(change_rate, 2),
        "audio_spikes": int(audio_spikes),
        "avg_audio_rms": round(float(sum(rms) / len(rms)), 6) if rms else 0.0,
        "visual_notes": _visual_note(visual_score, int(scene_changes)),
        "audio_notes": _audio_note(audio_score, int(audio_spikes)),
    }


def _visual_note(score: float, scene_changes: int) -> str:
    if score >= 70 and scene_changes >= 2:
        return "High visual movement with multiple scene changes."
    if score >= 55:
        return "Visible motion/contrast should hold attention after reframing."
    if scene_changes:
        return "Contains at least one visual transition."
    return "Low visual movement; rely on hook, caption pacing, and crop."


def _audio_note(score: float, spikes: int) -> str:
    if score >= 70 and spikes:
        return "Strong audio energy spike; useful for hook or payoff emphasis."
    if score >= 45:
        return "Moderate audio energy; captions and pacing can carry the clip."
    return "Low audio energy; avoid overlong lead-in."


def extract_opus_style_signals(video_path: str, cues: list[TranscriptCue]) -> dict[str, Any]:
    """Return local visual/audio/cue signals that can be persisted with analysis jobs."""
    duration = max(_duration(video_path), max((float(c.end) for c in cues), default=0.0))
    try:
        visual = _visual_signals(video_path)
    except Exception as exc:
        _log.warning("ClipLab visual signal extraction failed: %s", str(exc)[:200])
        visual = []
    try:
        audio = _audio_energy(video_path)
    except Exception as exc:
        _log.warning("ClipLab audio signal extraction failed: %s", str(exc)[:200])
        audio = []
    return {
        "duration_sec": round(float(duration), 3),
        "visual_sample_count": len(visual),
        "audio_sample_count": len(audio),
        "visual": visual,
        "audio": audio,
    }


def compact_signal_context(signals: dict[str, Any], cues: list[TranscriptCue], *, max_rows: int = 80) -> str:
    """Small text block for LLM ranking without dumping thousands of samples."""
    if not signals:
        return ""
    rows: list[str] = []
    duration = float(signals.get("duration_sec") or max((float(c.end) for c in cues), default=0.0))
    window = 15.0
    starts = [i * window for i in range(max(1, int(math.ceil(duration / window))))]
    for start in starts[:max_rows]:
        end = min(duration, start + window)
        summary = summarize_segment_signals(signals, start, end)
        if summary["visual_score"] < 25 and summary["audio_score"] < 25 and summary["scene_changes"] == 0:
            continue
        rows.append(
            f"[{start:.1f}-{end:.1f}] visual={summary['visual_score']} audio={summary['audio_score']} "
            f"changes={summary['scene_changes']} spikes={summary['audio_spikes']}"
        )
    return "\n".join(rows[:max_rows])
