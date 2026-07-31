"""The checkable release bar for Studio renders. See RELEASE_BAR.md.

"Zero artifacting" cannot be measured, so it cannot be failed, so it cannot be
passed. This module replaces it with three clauses a render can be marked
against programmatically:

  1. render completion rate >= 95% (no content-policy death, freeze, or desync)
  2. per-clip visible-defect rate <= 5%, from extracted frames
  3. <= 1 defective beat per video, repairable in a single attempt

The structural half (completion, freeze, desync) is fully automated from the
finished media. The visible-defect half requires frame inspection, which is a
judgement this module does not attempt to make - it *enforces* that the
judgement exists. A render with no inspection evidence is not release-ready;
unknown never scores as a pass.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

RELEASE_BAR_VERSION = 1

_DEFAULTS: dict[str, float] = {
    "min_completion_rate": 0.95,
    "max_clip_defect_rate": 0.05,
    "max_defective_beats": 1.0,
    "max_repair_attempts": 1.0,
    "freeze_min_sec": 0.5,
    "desync_tolerance_sec": 0.10,
}

_ENV_KEYS: dict[str, str] = {
    "min_completion_rate": "STUDIO_BAR_MIN_COMPLETION_RATE",
    "max_clip_defect_rate": "STUDIO_BAR_MAX_CLIP_DEFECT_RATE",
    "max_defective_beats": "STUDIO_BAR_MAX_DEFECTIVE_BEATS",
    "max_repair_attempts": "STUDIO_BAR_MAX_REPAIR_ATTEMPTS",
    "freeze_min_sec": "STUDIO_BAR_FREEZE_MIN_SEC",
    "desync_tolerance_sec": "STUDIO_BAR_DESYNC_TOLERANCE_SEC",
}


def threshold(name: str) -> float:
    """Read one threshold, honouring an environment override."""
    if name not in _DEFAULTS:
        raise KeyError(f"unknown release-bar threshold: {name}")
    raw = str(os.getenv(_ENV_KEYS[name], "") or "").strip()
    if raw:
        try:
            return float(raw)
        except ValueError:
            pass
    return _DEFAULTS[name]


def active_thresholds() -> dict[str, Any]:
    """Every threshold in force, flagging any that were overridden.

    Recorded into each verdict so a run cannot quietly grade itself on an
    easier scale than the one this bar defines.
    """
    values = {name: threshold(name) for name in _DEFAULTS}
    overridden = sorted(
        name for name in _DEFAULTS if values[name] != _DEFAULTS[name]
    )
    return {"values": values, "overridden": overridden, "version": RELEASE_BAR_VERSION}


@dataclass
class BeatDefect:
    """One beat of a finished video carrying a visible defect."""

    beat_index: int
    defect_class: str = ""
    repair_attempts: int = 0
    repaired: bool = False


@dataclass
class RenderEvidence:
    """Everything needed to mark one render against the bar.

    ``clips_inspected`` is the honesty gate. It is the number of clips actually
    looked at via extracted frames; zero means the visible-defect clauses cannot
    be judged, and the render is not release-ready for that reason.
    """

    completed: bool = False
    failure_reason: str = ""
    content_policy_death: bool = False
    video_duration_sec: float = 0.0
    narration_duration_sec: float = 0.0
    freeze_spans: list[tuple[float, float]] = field(default_factory=list)
    clips_total: int = 0
    clips_inspected: int = 0
    clips_with_visible_defect: int = 0
    beat_defects: list[BeatDefect] = field(default_factory=list)


def _clause(name: str, passed: bool | None, detail: str, **extra: Any) -> dict[str, Any]:
    return {
        "clause": name,
        "status": "pass" if passed else ("unknown" if passed is None else "fail"),
        "passed": bool(passed),
        "detail": detail,
        **extra,
    }


def evaluate_render(evidence: RenderEvidence) -> dict[str, Any]:
    """Mark one render against the bar. See RELEASE_BAR.md for the contract."""
    clauses: list[dict[str, Any]] = []

    # --- Structural completion (clause 1, per-render half) --------------------
    freeze_min = threshold("freeze_min_sec")
    long_freezes = [
        (start, dur) for start, dur in evidence.freeze_spans if dur >= freeze_min - 1e-9
    ]
    desync_tolerance = threshold("desync_tolerance_sec")
    desync_delta = abs(
        float(evidence.video_duration_sec) - float(evidence.narration_duration_sec)
    )
    has_narration_clock = float(evidence.narration_duration_sec) > 0

    if not evidence.completed:
        reason = evidence.failure_reason or (
            "content policy" if evidence.content_policy_death else "did not finish"
        )
        clauses.append(_clause("completed", False, f"Render did not complete: {reason}"))
    else:
        clauses.append(_clause("completed", True, "Render produced a finished video"))

    clauses.append(
        _clause(
            "no_freeze",
            not long_freezes,
            f"{len(long_freezes)} frozen span(s) >= {freeze_min:g}s"
            if long_freezes
            else f"No frozen span >= {freeze_min:g}s",
            freeze_spans=long_freezes,
        )
    )

    if not has_narration_clock:
        clauses.append(
            _clause("no_desync", None, "No narration duration supplied; cannot judge desync")
        )
    else:
        clauses.append(
            _clause(
                "no_desync",
                desync_delta <= desync_tolerance + 1e-9,
                f"Video/narration differ by {desync_delta:.3f}s "
                f"(tolerance {desync_tolerance:g}s)",
                delta_sec=round(desync_delta, 4),
            )
        )

    # --- Visible defects (clauses 2 and 3) ------------------------------------
    # Unknown is never a pass. Without extracted-frame evidence these clauses
    # report `unknown` and the render is not release-ready.
    inspected = max(0, int(evidence.clips_inspected))
    if inspected <= 0:
        clauses.append(
            _clause(
                "clip_defect_rate",
                None,
                "No clips inspected; per-clip defect rate cannot be judged from frames",
            )
        )
        clauses.append(
            _clause(
                "defective_beats",
                None,
                "No clips inspected; defective-beat count cannot be judged from frames",
            )
        )
    else:
        max_rate = threshold("max_clip_defect_rate")
        defect_rate = max(0, int(evidence.clips_with_visible_defect)) / inspected
        clauses.append(
            _clause(
                "clip_defect_rate",
                defect_rate <= max_rate + 1e-9,
                f"{evidence.clips_with_visible_defect}/{inspected} clips defective "
                f"({defect_rate:.1%}), limit {max_rate:.0%}",
                defect_rate=round(defect_rate, 4),
                clips_inspected=inspected,
            )
        )

        max_beats = int(threshold("max_defective_beats"))
        defective = list(evidence.beat_defects)
        clauses.append(
            _clause(
                "defective_beats",
                len(defective) <= max_beats,
                f"{len(defective)} defective beat(s), limit {max_beats}",
                beats=[d.beat_index for d in defective],
            )
        )

        max_attempts = int(threshold("max_repair_attempts"))
        unrepaired = [
            d for d in defective if not d.repaired or d.repair_attempts > max_attempts
        ]
        clauses.append(
            _clause(
                "repairable_in_one_attempt",
                not unrepaired,
                "; ".join(
                    f"beat {d.beat_index} ({d.defect_class or 'defect'}): "
                    f"{'unrepaired' if not d.repaired else f'{d.repair_attempts} attempts'}"
                    for d in unrepaired
                )
                or f"Every defective beat repaired within {max_attempts} attempt(s)",
                beats=[d.beat_index for d in unrepaired],
            )
        )

    unknown = [c for c in clauses if c["status"] == "unknown"]
    failed = [c for c in clauses if c["status"] == "fail"]
    return {
        "release_bar_version": RELEASE_BAR_VERSION,
        "release_ready": not failed and not unknown,
        "counts_as_completed": bool(
            evidence.completed
            and not long_freezes
            and (not has_narration_clock or desync_delta <= desync_tolerance + 1e-9)
        ),
        "failed_clauses": [c["clause"] for c in failed],
        "unknown_clauses": [c["clause"] for c in unknown],
        "clauses": clauses,
        "thresholds": active_thresholds(),
    }


def evaluate_fleet(verdicts: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate clause 1, which is only meaningful across many renders."""
    total = len(verdicts)
    if not total:
        return {
            "release_bar_version": RELEASE_BAR_VERSION,
            "passed": False,
            "detail": "No renders supplied; completion rate is undefined",
            "renders": 0,
            "completion_rate": 0.0,
            "thresholds": active_thresholds(),
        }
    completed = sum(1 for v in verdicts if v.get("counts_as_completed"))
    rate = completed / total
    minimum = threshold("min_completion_rate")
    return {
        "release_bar_version": RELEASE_BAR_VERSION,
        "passed": rate >= minimum - 1e-9,
        "detail": f"{completed}/{total} renders completed ({rate:.1%}), floor {minimum:.0%}",
        "renders": total,
        "completed": completed,
        "completion_rate": round(rate, 4),
        "release_ready_renders": sum(1 for v in verdicts if v.get("release_ready")),
        "thresholds": active_thresholds(),
    }


# --- Probes: structural evidence read straight off the finished media ---------

_FREEZE_EVENT = re.compile(r"freeze_(start|duration|end):\s*(-?[0-9.]+)")


def probe_freeze_spans(video_path: Path, *, min_seconds: float | None = None) -> list[tuple[float, float]]:
    """Frozen spans in a finished video, via ffmpeg's freezedetect.

    freezedetect emits ``freeze_start`` / ``freeze_duration`` / ``freeze_end``
    for a freeze that ends, but a freeze running to end-of-file emits **only**
    ``freeze_start``. That unterminated case is precisely the defect this bar
    exists to catch - the mux clones the final frame to pad to the narration
    clock - so an open freeze is closed against the media duration rather than
    discarded.

    Returns [] when ffmpeg is unavailable or the probe fails; callers must not
    read an empty list as proof of no freeze unless ffmpeg is known present.
    """
    if not shutil.which("ffmpeg"):
        return []
    floor = min_seconds if min_seconds is not None else threshold("freeze_min_sec")
    try:
        proc = subprocess.run(
            [
                "ffmpeg", "-hide_banner", "-nostats", "-i", str(video_path),
                "-vf", f"freezedetect=n=-60dB:d={max(0.1, float(floor)):g}",
                "-map", "0:v:0", "-f", "null", "-",
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )
    except Exception:
        return []

    spans: list[tuple[float, float]] = []
    open_start: float | None = None
    for kind, raw in _FREEZE_EVENT.findall(proc.stderr or ""):
        try:
            value = float(raw)
        except ValueError:
            continue
        if kind == "start":
            open_start = value
        elif kind == "duration" and open_start is not None:
            spans.append((open_start, value))
            open_start = None
        elif kind == "end" and open_start is not None:
            spans.append((open_start, max(0.0, value - open_start)))
            open_start = None
    if open_start is not None:
        total = probe_duration(video_path)
        spans.append((open_start, max(0.0, total - open_start)))
    return spans


def probe_duration(media_path: Path) -> float:
    """Duration in seconds, or 0.0 when it cannot be read."""
    if not shutil.which("ffprobe"):
        return 0.0
    try:
        proc = subprocess.run(
            [
                "ffprobe", "-v", "error", "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1", str(media_path),
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        return float((proc.stdout or "0").strip() or 0.0)
    except Exception:
        return 0.0
