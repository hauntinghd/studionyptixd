"""Deterministic QA checks for completed Studio Agent renders.

This module is deliberately cheap and side-effect-light. It inspects the final
media/package files and caches a small report beside the render so polling does
not repeatedly invoke ffprobe.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

QA_VERSION = 2


def analyze_render(
    *,
    job_id: str,
    kind: str,
    video_path: Path,
    package_path: Path | None = None,
) -> dict[str, Any]:
    """Return a cached pass/warn/fail QA report for a finished render."""
    video_path = Path(video_path)
    package_path = Path(package_path) if package_path else None
    caption_path = video_path.parent / "captions.json"
    cache_path = video_path.parent / "render_qa.json"
    input_fingerprint = _qa_inputs_fingerprint(video_path, package_path, caption_path)
    cached = _read_cache(cache_path, video_path, package_path, caption_path, input_fingerprint)
    if cached:
        return cached

    checks: list[dict[str, Any]] = []
    file_size = video_path.stat().st_size if video_path.is_file() else 0
    _add_check(
        checks,
        "file_present",
        "Media file exists",
        "pass" if file_size > 256_000 else "fail",
        f"{_format_bytes(file_size)} at {video_path.name}" if file_size else "No media file found",
    )

    probe = _probe_video(video_path) if file_size else {"ok": False, "error": "media file missing"}
    if probe.get("ok"):
        duration = float(probe.get("duration") or 0)
        width = int(probe.get("width") or 0)
        height = int(probe.get("height") or 0)
        has_audio = bool(probe.get("has_audio"))
        aspect = (width / height) if height else 0.0
        _add_check(checks, "video_probe", "Video stream readable", "pass", f"{width}x{height}, {_format_duration(duration)}")
        _duration_check(checks, str(kind).lower(), duration)
        _aspect_check(checks, str(kind).lower(), aspect, width, height)
        _add_check(
            checks,
            "audio_stream",
            "Audio stream present",
            "pass" if has_audio else "fail",
            "Audio track detected" if has_audio else "No audio stream detected",
        )
        if has_audio:
            _av_sync_check(checks, probe)
        if caption_path.is_file():
            _caption_timeline_check(checks, caption_path, duration, float(probe.get("fps") or 30.0))
    else:
        _add_check(
            checks,
            "video_probe",
            "Video stream readable",
            "warn",
            str(probe.get("error") or "ffprobe unavailable"),
        )

    _package_check(checks, str(kind).lower(), package_path)
    score = _score(checks)
    status = "fail" if any(c["status"] == "fail" for c in checks) else "warn" if any(c["status"] == "warn" for c in checks) else "pass"
    if status == "pass" and score < 80:
        status = "warn"
    report = {
        "version": QA_VERSION,
        "job_id": job_id,
        "kind": str(kind or ""),
        "status": status,
        "score": score,
        "summary": _summary(status, score),
        "checks": checks,
        "input_fingerprint": input_fingerprint,
        "created_at": time.time(),
    }
    _write_cache(cache_path, report)
    return report


def _read_cache(
    cache_path: Path,
    video_path: Path,
    package_path: Path | None,
    caption_path: Path | None = None,
    input_fingerprint: str = "",
) -> dict[str, Any] | None:
    try:
        if not cache_path.is_file():
            return None
        cache_mtime = cache_path.stat().st_mtime
        if video_path.is_file() and cache_mtime < video_path.stat().st_mtime:
            return None
        if package_path and package_path.is_file() and cache_mtime < package_path.stat().st_mtime:
            return None
        if caption_path and caption_path.is_file() and cache_mtime < caption_path.stat().st_mtime:
            return None
        data = json.loads(cache_path.read_text(encoding="utf-8"))
        if (
            isinstance(data, dict)
            and data.get("version") == QA_VERSION
            and (not input_fingerprint or data.get("input_fingerprint") == input_fingerprint)
        ):
            return data
    except Exception:
        return None
    return None


def _qa_inputs_fingerprint(
    video_path: Path,
    package_path: Path | None,
    caption_path: Path | None,
) -> str:
    digest = hashlib.sha256()
    digest.update(str(QA_VERSION).encode("ascii"))
    for path, hash_content in (
        (video_path, False),
        (package_path, True),
        (caption_path, True),
    ):
        if not path or not Path(path).is_file():
            digest.update(b"missing\0")
            continue
        resolved = Path(path)
        stat = resolved.stat()
        digest.update(f"{resolved.name}:{stat.st_size}:{stat.st_mtime_ns}".encode("utf-8"))
        if hash_content:
            with resolved.open("rb") as handle:
                for block in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(block)
    return digest.hexdigest()


def _write_cache(cache_path: Path, report: dict[str, Any]) -> None:
    try:
        cache_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    except Exception:
        pass


def _probe_video(video_path: Path) -> dict[str, Any]:
    ffprobe = os.getenv("FFPROBE_BINARY") or shutil.which("ffprobe")
    if not ffprobe:
        return {"ok": False, "error": "ffprobe is not installed on this server"}
    try:
        proc = subprocess.run(
            [
                ffprobe,
                "-v",
                "error",
                "-print_format",
                "json",
                "-show_format",
                "-show_streams",
                str(video_path),
            ],
            text=True,
            capture_output=True,
            timeout=20,
            check=False,
        )
    except Exception as exc:
        return {"ok": False, "error": f"ffprobe failed: {exc}"}
    if proc.returncode != 0:
        return {"ok": False, "error": (proc.stderr or proc.stdout or "ffprobe failed")[:300]}
    try:
        data = json.loads(proc.stdout or "{}")
    except Exception as exc:
        return {"ok": False, "error": f"ffprobe returned invalid JSON: {exc}"}
    streams = data.get("streams") if isinstance(data.get("streams"), list) else []
    # ffprobe can occasionally emit non-dict stream entries; never call .get on those.
    dict_streams = [s for s in streams if isinstance(s, dict)]
    video = next((s for s in dict_streams if s.get("codec_type") == "video"), None)
    if not isinstance(video, dict):
        return {"ok": False, "error": "No video stream found"}
    audio_stream = next((s for s in dict_streams if s.get("codec_type") == "audio"), None)
    fmt = data.get("format") if isinstance(data.get("format"), dict) else {}
    format_duration = _float(fmt.get("duration"))
    format_start = _float(fmt.get("start_time"))
    video_duration = _float(video.get("duration")) or format_duration
    video_start = _float(video.get("start_time")) if video.get("start_time") is not None else format_start
    if isinstance(audio_stream, dict):
        audio_duration = _float(audio_stream.get("duration")) or format_duration
        audio_start = (
            _float(audio_stream.get("start_time"))
            if audio_stream.get("start_time") is not None
            else format_start
        )
    else:
        audio_duration = 0.0
        audio_start = 0.0
    return {
        "ok": True,
        "duration": format_duration or video_duration,
        "width": int(video.get("width") or 0),
        "height": int(video.get("height") or 0),
        "has_audio": isinstance(audio_stream, dict),
        "fps": _parse_rate(video.get("avg_frame_rate") or video.get("r_frame_rate")) or 30.0,
        "video_start": video_start,
        "video_duration": video_duration,
        "audio_start": audio_start,
        "audio_duration": audio_duration,
    }


def _parse_rate(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    if "/" in text:
        numerator, denominator = text.split("/", 1)
        den = _float(denominator)
        return _float(numerator) / den if den else 0.0
    return _float(text)


def _av_sync_check(checks: list[dict[str, Any]], probe: dict[str, Any]) -> None:
    fps = max(1.0, float(probe.get("fps") or 30.0))
    tolerance = 1.0 / fps
    duration = float(probe.get("duration") or 0.0)
    video_start = float(probe.get("video_start") or 0.0)
    audio_start = float(probe.get("audio_start") or 0.0)
    video_duration = float(probe.get("video_duration") or duration)
    audio_duration = float(probe.get("audio_duration") or duration)
    start_delta = abs(video_start - audio_start)
    end_delta = abs((video_start + video_duration) - (audio_start + audio_duration))
    status = "pass" if start_delta <= tolerance + 1e-6 and end_delta <= tolerance + 1e-6 else "fail"
    _add_check(
        checks,
        "av_sync",
        "Audio/video clocks aligned",
        status,
        (
            f"start delta {start_delta:.4f}s, end delta {end_delta:.4f}s; "
            f"maximum one-frame tolerance {tolerance:.4f}s at {fps:.3f}fps"
        ),
    )


def _caption_timeline_check(
    checks: list[dict[str, Any]],
    caption_path: Path,
    video_duration: float,
    fps: float,
) -> None:
    try:
        manifest = json.loads(caption_path.read_text(encoding="utf-8"))
    except Exception as exc:
        _add_check(checks, "captions", "Caption timeline valid", "fail", f"Invalid captions.json: {exc}")
        return
    if not isinstance(manifest, dict):
        _add_check(checks, "captions", "Caption timeline valid", "fail", "captions.json is not an object")
        return
    if not bool(manifest.get("enabled")):
        _add_check(checks, "captions", "Caption timeline valid", "pass", "Captions intentionally disabled")
        return

    mode = str(manifest.get("mode") or "").strip().lower()
    timing_source = str(manifest.get("timing_source") or "").strip().lower()
    if mode == "word" and timing_source not in {"verified_word", "fal_whisper_word"}:
        _add_check(
            checks,
            "captions",
            "Caption timeline valid",
            "fail",
            "Word captions are missing verified audio-timing provenance",
        )
        return
    cues = manifest.get("cues") if isinstance(manifest.get("cues"), list) else []
    tolerance = 1.0 / max(1.0, float(fps or 30.0))
    prior_end = 0.0
    problems: list[str] = []
    for index, cue in enumerate(cues):
        if not isinstance(cue, dict):
            problems.append(f"cue {index + 1} is not an object")
            continue
        start = _float(cue.get("start"))
        end = _float(cue.get("end"))
        if start < 0.0 or end <= start:
            problems.append(f"cue {index + 1} has an invalid interval")
        if start + tolerance < prior_end:
            problems.append(f"cue {index + 1} is non-monotonic")
        if end > video_duration + tolerance:
            problems.append(f"cue {index + 1} exceeds the final video clock")
        prior_end = max(prior_end, end)
    if not cues:
        problems.append("caption timeline contains no cues")
    manifest_duration = _float(manifest.get("duration_sec"))
    if manifest_duration and abs(manifest_duration - video_duration) > tolerance:
        problems.append("caption and video durations differ by more than one frame")
    _add_check(
        checks,
        "captions",
        "Caption timeline valid",
        "fail" if problems else "pass",
        "; ".join(problems[:4]) if problems else f"{len(cues)} cues, source={timing_source}",
    )


def _duration_check(checks: list[dict[str, Any]], kind: str, duration: float) -> None:
    if kind == "shortform":
        if 8 <= duration <= 75:
            status = "pass"
        elif 5 <= duration <= 90:
            status = "warn"
        else:
            status = "fail"
        detail = "Short-form target is 8-75 seconds"
    else:
        if duration >= 60:
            status = "pass"
        elif duration >= 20:
            status = "warn"
        else:
            status = "fail"
        detail = "Long-form/export target should not be a tiny clip"
    _add_check(checks, "duration", "Runtime looks plausible", status, f"{_format_duration(duration)}. {detail}.")


def _aspect_check(checks: list[dict[str, Any]], kind: str, aspect: float, width: int, height: int) -> None:
    if not width or not height:
        _add_check(checks, "aspect_ratio", "Aspect ratio", "fail", "Missing width/height")
        return
    if kind == "shortform":
        delta = abs(aspect - (9 / 16))
        status = "pass" if delta <= 0.04 else "warn" if delta <= 0.10 else "fail"
        detail = "Short-form should be vertical 9:16"
    else:
        status = "pass" if width >= height else "warn"
        detail = "Long-form usually ships landscape unless intentionally vertical"
    _add_check(checks, "aspect_ratio", "Aspect ratio", status, f"{width}x{height}. {detail}.")


def _package_check(checks: list[dict[str, Any]], kind: str, package_path: Path | None) -> None:
    if not package_path or not package_path.is_file():
        _add_check(checks, "package", "Packaging present", "fail", "Missing title/description/tags package")
        return
    try:
        text = package_path.read_text(encoding="utf-8")
    except Exception as exc:
        _add_check(checks, "package", "Packaging present", "fail", f"Could not read package: {exc}")
        return
    required = ["Title:", "Description:", "Tags:"]
    if kind == "shortform":
        required.append("Hashtags:")
    else:
        required.extend(["Timestamps:", "Thumbnail:"])
    missing = [name.rstrip(":") for name in required if name.lower() not in text.lower()]
    status = "pass" if not missing else "warn" if len(missing) <= 1 else "fail"
    detail = "All required sections found" if not missing else f"Missing sections: {', '.join(missing)}"
    _add_check(checks, "package", "Packaging present", status, detail)


def _add_check(checks: list[dict[str, Any]], check_id: str, label: str, status: str, detail: str) -> None:
    checks.append({
        "id": check_id,
        "label": label,
        "status": status,
        "status_label": status.upper(),
        "detail": detail,
    })


def _score(checks: list[dict[str, Any]]) -> int:
    score = 100
    for check in checks:
        if check.get("status") == "fail":
            score -= 25
        elif check.get("status") == "warn":
            score -= 8
    return max(0, min(100, score))


def _summary(status: str, score: int) -> str:
    if status == "pass":
        return f"PASS {score}/100 - ready for human review/posting."
    if status == "warn":
        return f"WARN {score}/100 - review before posting."
    return f"FAIL {score}/100 - fix required before posting."


def _float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _format_duration(seconds: float) -> str:
    seconds = max(0, float(seconds or 0))
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes}:{secs:02d}"


def _format_bytes(size: int) -> str:
    if size >= 1_000_000:
        return f"{size / 1_000_000:.1f} MB"
    if size >= 1_000:
        return f"{size / 1_000:.1f} KB"
    return f"{size} B"
