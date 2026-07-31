"""Deterministic QA checks for completed Studio Agent renders.

This module is deliberately cheap and side-effect-light. It inspects the final
media/package files and caches a small report beside the render so polling does
not repeatedly invoke ffprobe.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

QA_VERSION = 3


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
        _av_sync_check(checks, probe)
        _audio_level_check(checks, video_path)
        _frame_integrity_check(checks, video_path, duration)
        _workspace_edit_checks(
            checks,
            workspace=video_path.parent,
            kind=str(kind).lower(),
            duration=duration,
        )
        if caption_path.is_file():
            _caption_timeline_check(checks, caption_path, duration, float(probe.get("fps") or 30.0))
    else:
        _add_check(
            checks,
            "video_probe",
            "Video stream readable",
            "fail",
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
        "fingerprint": _render_fingerprint(video_path, package_path),
        "status": status,
        "score": score,
        "summary": _summary(status, score),
        "checks": checks,
        "release_bar": _release_bar_section(video_path, probe),
        "input_fingerprint": input_fingerprint,
        "created_at": time.time(),
    }
    _write_cache(cache_path, report)
    return report


def _render_fingerprint(video_path: Path, package_path: Path | None) -> str:
    digest = hashlib.sha256()
    for path in (video_path, package_path):
        if not path or not Path(path).is_file():
            digest.update(b"missing\0")
            continue
        asset = Path(path)
        try:
            digest.update(b"asset\0")
            digest.update(int(asset.stat().st_size).to_bytes(8, "big", signed=False))
            with asset.open("rb") as handle:
                while True:
                    chunk = handle.read(1024 * 1024)
                    if not chunk:
                        break
                    digest.update(chunk)
        except OSError:
            digest.update(b"unreadable")
    return digest.hexdigest()


def _workspace_edit_checks(
    checks: list[dict[str, Any]],
    *,
    workspace: Path,
    kind: str,
    duration: float,
) -> None:
    """Validate the durable edit contract that produced the staged render.

    These checks intentionally use source-of-truth sidecars rather than trying
    to infer story/caption semantics from pixels. Missing or stale evidence is
    blocking: an existing MP4 is not proof that the requested edit passed.
    """
    if kind != "shortform":
        _longform_edit_checks(checks, workspace, duration)
        return

    scenes_path = workspace / "scenes.json"
    try:
        raw = json.loads(scenes_path.read_text(encoding="utf-8"))
        scenes = [row for row in raw if isinstance(row, dict)] if isinstance(raw, list) else []
    except Exception as exc:
        _add_check(checks, "story_manifest", "Story edit manifest", "fail", f"scenes.json unavailable: {exc}")
        return
    if not scenes:
        _add_check(checks, "story_manifest", "Story edit manifest", "fail", "No ordered scenes were recorded")
        return

    ordered = sorted(scenes, key=lambda row: int(row.get("index") or 0))
    indices = [int(row.get("index") or 0) for row in ordered]
    narration = [str(row.get("narration") or "").strip() for row in ordered]
    contracts = [
        " ".join(
            part for part in (
                str(row.get("narration") or "").strip(),
                str(row.get("scene_action") or row.get("action") or row.get("prompt") or "").strip(),
            ) if part
        ).lower()
        for row in ordered
    ]
    expected = list(range(len(ordered)))
    story_ok = indices == expected and all(narration) and all(contracts)
    unique_contracts = len({re.sub(r"\s+", " ", text) for text in contracts if text})
    distinct_ok = unique_contracts >= max(1, int(len(ordered) * 0.75))
    _add_check(
        checks,
        "story_order",
        "Narration-to-visual story order",
        "pass" if story_ok and distinct_ok else "fail",
        (
            f"{len(ordered)} ordered, narration-bound scenes with {unique_contracts} distinct beats"
            if story_ok and distinct_ok
            else "Scene order, narration binding, or distinct story beats are incomplete"
        ),
    )

    durations = [_float(row.get("duration_sec") or 0) for row in ordered]
    total_scene_duration = sum(durations)
    plausible = all(0.5 <= value <= 20.0 for value in durations)
    aligned = abs(total_scene_duration - duration) <= max(1.5, duration * 0.08)
    has_hook = bool(narration[0])
    has_payoff = bool(narration[-1]) and narration[-1].lower() != narration[0].lower()
    _add_check(
        checks,
        "story_pacing",
        "Hook, payoff, and scene pacing",
        "pass" if plausible and aligned and has_hook and has_payoff else "fail",
        (
            f"Hook/payoff present; scene plan {total_scene_duration:.2f}s vs render {duration:.2f}s"
            if plausible and aligned and has_hook and has_payoff
            else f"Pacing contract failed; scene plan {total_scene_duration:.2f}s vs render {duration:.2f}s"
        ),
    )

    _current_scene_qa_check(checks, workspace, ordered)
    _caption_contract_check(checks, workspace, ordered)


def _current_scene_qa_check(
    checks: list[dict[str, Any]],
    workspace: Path,
    scenes: list[dict[str, Any]],
) -> None:
    try:
        from studio_agent import visual_qa

        failures: list[str] = []
        for position, scene in enumerate(scenes):
            current = scene.get("visual_qa") if isinstance(scene.get("visual_qa"), dict) else {}
            fresh = visual_qa.scene_visual_qa_is_fresh(
                workspace,
                scene,
                previous_scene=scenes[position - 1] if position else None,
                next_scene=scenes[position + 1] if position + 1 < len(scenes) else None,
            )
            if not fresh or current.get("status") != "pass" or current.get("pass") is not True:
                failures.append(str(int(scene.get("index") or position) + 1))
        _add_check(
            checks,
            "current_scene_qa",
            "Current still and clip QA fingerprints",
            "pass" if not failures else "fail",
            "All scene assets have current passing QA" if not failures else f"Missing, stale, or failing QA for scenes: {', '.join(failures)}",
        )
    except Exception as exc:
        _add_check(checks, "current_scene_qa", "Current still and clip QA fingerprints", "fail", f"QA evidence unavailable: {exc}")


def _caption_contract_check(
    checks: list[dict[str, Any]],
    workspace: Path,
    scenes: list[dict[str, Any]],
) -> None:
    try:
        spec = json.loads((workspace / "job_spec.json").read_text(encoding="utf-8"))
        if not isinstance(spec, dict):
            spec = {}
    except Exception:
        spec = {}
    enabled = bool(spec.get("captions_enabled", True)) and str(spec.get("caption_mode") or "word").lower() != "off"
    if not enabled:
        _add_check(checks, "captions", "Caption text, timing, and safe area", "pass", "Captions were explicitly disabled")
        return
    missing: list[str] = []
    invalid: list[str] = []
    trimmed = workspace / "trimmed"
    for position, scene in enumerate(scenes):
        sid = str(scene.get("sid") or f"b{position:02d}")
        filter_path = trimmed / f"{sid}_filter.txt"
        try:
            text = filter_path.read_text(encoding="utf-8")
        except Exception:
            missing.append(str(position + 1))
            continue
        narration_words = len(str(scene.get("narration") or "").split())
        drawtext_count = text.count("drawtext=")
        safe_area = "y=h*0.78" in text and "y=h*0.93" in text
        has_timestamps = "between(t\\," in text
        # One drawtext is the watermark; phrase mode may group at most 3 words.
        minimum_caption_rows = max(1, (narration_words + 2) // 3)
        if not safe_area or not has_timestamps or drawtext_count - 1 < minimum_caption_rows:
            invalid.append(str(position + 1))
    status = "pass" if not missing and not invalid else "fail"
    detail = (
        "Caption filters cover narration with timestamped text at the 78% safe-area line"
        if status == "pass"
        else f"Missing filters: {missing or 'none'}; invalid text/timing/safe-area: {invalid or 'none'}"
    )
    _add_check(checks, "captions", "Caption text, timing, and safe area", status, detail)


def _longform_edit_checks(checks: list[dict[str, Any]], workspace: Path, duration: float) -> None:
    candidates = (workspace / "state.json", workspace / "job_state.json", workspace / "result.json")
    state: dict[str, Any] = {}
    source = ""
    for path in candidates:
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                state = loaded
                source = path.name
                break
        except Exception:
            continue
    chapters = state.get("chapters") if isinstance(state.get("chapters"), list) else []
    if not chapters:
        chapters_path = workspace / "chapters.json"
        try:
            chapters_payload = json.loads(chapters_path.read_text(encoding="utf-8"))
            if isinstance(chapters_payload, dict):
                chapters = chapters_payload.get("chapters") if isinstance(chapters_payload.get("chapters"), list) else []
            elif isinstance(chapters_payload, list):
                chapters = chapters_payload
            if chapters:
                source = chapters_path.name
        except Exception:
            chapters = []
    story_ok = bool(chapters) and all(
        isinstance(chapter, dict) and str(chapter.get("narration") or chapter.get("script") or "").strip()
        for chapter in chapters
    )
    _add_check(
        checks,
        "longform_story",
        "Long-form chapter story and pacing manifest",
        "pass" if story_ok else "fail",
        f"{len(chapters)} narrated chapters from {source}" if story_ok else "Narrated chapter manifest is unavailable",
    )
    _add_check(
        checks,
        "longform_runtime",
        "Long-form edit duration",
        "pass" if duration >= 60 else "fail",
        f"Final runtime {duration:.2f}s",
    )


def _av_sync_check(checks: list[dict[str, Any]], probe: dict[str, Any]) -> None:
    video_duration = _float(probe.get("video_duration") or probe.get("duration"))
    audio_duration = _float(probe.get("audio_duration"))
    if not audio_duration or not video_duration:
        _add_check(checks, "av_sync", "Audio/video duration alignment", "fail", "Audio or video duration was not measurable")
        return
    delta = abs(video_duration - audio_duration)
    tolerance = max(0.35, video_duration * 0.015)
    _add_check(
        checks,
        "av_sync",
        "Audio/video duration alignment",
        "pass" if delta <= tolerance else "fail",
        f"Video {video_duration:.3f}s, audio {audio_duration:.3f}s, delta {delta:.3f}s (limit {tolerance:.3f}s)",
    )


def _audio_level_check(checks: list[dict[str, Any]], video_path: Path) -> None:
    levels = _probe_audio_levels(video_path)
    if not levels.get("ok"):
        _add_check(checks, "audio_levels", "Narration loudness and silence", "fail", str(levels.get("error") or "Audio levels unavailable"))
        return
    mean_db = _float(levels.get("mean_db"))
    max_db = _float(levels.get("max_db"))
    passed = -35.0 <= mean_db <= -8.0 and -12.0 <= max_db <= -0.5
    _add_check(
        checks,
        "audio_levels",
        "Narration loudness and silence",
        "pass" if passed else "fail",
        f"mean {mean_db:.1f} dB, peak {max_db:.1f} dB",
    )


def _probe_audio_levels(video_path: Path) -> dict[str, Any]:
    ffmpeg = os.getenv("FFMPEG_BINARY") or shutil.which("ffmpeg")
    if not ffmpeg:
        return {"ok": False, "error": "ffmpeg is not installed on this server"}
    try:
        proc = subprocess.run(
            [ffmpeg, "-hide_banner", "-nostats", "-i", str(video_path), "-af", "volumedetect", "-vn", "-f", "null", os.devnull],
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
    except Exception as exc:
        return {"ok": False, "error": f"audio probe failed: {exc}"}
    text = f"{proc.stdout}\n{proc.stderr}"
    mean = re.search(r"mean_volume:\s*(-?[0-9.]+)\s*dB", text)
    peak = re.search(r"max_volume:\s*(-?[0-9.]+)\s*dB", text)
    if not mean or not peak:
        return {"ok": False, "error": "ffmpeg did not return measurable audio levels"}
    return {"ok": True, "mean_db": float(mean.group(1)), "max_db": float(peak.group(1))}


def _frame_integrity_check(checks: list[dict[str, Any]], video_path: Path, duration: float) -> None:
    sampled = _sample_frame_hashes(video_path, duration)
    if not sampled.get("ok"):
        _add_check(checks, "frame_integrity", "Dead and duplicate frame integrity", "fail", str(sampled.get("error") or "Frame sampling failed"))
        return
    hashes = list(sampled.get("hashes") or [])
    unique = len(set(hashes))
    ratio = unique / max(1, len(hashes))
    passed = len(hashes) >= 3 and unique >= 2 and ratio >= 0.35
    _add_check(
        checks,
        "frame_integrity",
        "Dead and duplicate frame integrity",
        "pass" if passed else "fail",
        f"{unique}/{len(hashes)} sampled frames are distinct",
    )


def _sample_frame_hashes(video_path: Path, duration: float) -> dict[str, Any]:
    ffmpeg = os.getenv("FFMPEG_BINARY") or shutil.which("ffmpeg")
    if not ffmpeg:
        return {"ok": False, "error": "ffmpeg is not installed on this server"}
    fps = max(0.2, min(1.0, 8.0 / max(duration, 1.0)))
    try:
        proc = subprocess.run(
            [ffmpeg, "-v", "error", "-i", str(video_path), "-vf", f"fps={fps:.5f},scale=64:64,format=gray", "-f", "rawvideo", "-"],
            capture_output=True,
            timeout=35,
            check=False,
        )
    except Exception as exc:
        return {"ok": False, "error": f"frame sampling failed: {exc}"}
    if proc.returncode != 0:
        return {"ok": False, "error": (proc.stderr or b"ffmpeg frame sampling failed").decode("utf-8", "ignore")[:300]}
    frame_size = 64 * 64
    raw = bytes(proc.stdout or b"")
    hashes = [hashlib.sha256(raw[offset:offset + frame_size]).hexdigest() for offset in range(0, len(raw) - frame_size + 1, frame_size)]
    return {"ok": bool(hashes), "hashes": hashes, "error": "No frames sampled" if not hashes else ""}


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
            and str(data.get("fingerprint") or "") == _render_fingerprint(video_path, package_path)
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
    status = "pass" if not missing else "fail"
    detail = "All required sections found" if not missing else f"Missing sections: {', '.join(missing)}"
    _add_check(checks, "package", "Packaging present", status, detail)


def _release_bar_section(video_path: Path, probe: dict[str, Any]) -> dict[str, Any]:
    """Mark the finished media against RELEASE_BAR.md's structural clauses.

    Only the structural half is decidable here - completion, freeze, desync. The
    visible-defect clauses need extracted-frame inspection, which this module
    does not perform, so they come back `unknown` and the render is correctly
    not release-ready until a frame pass supplies them.
    """
    from studio_agent import release_bar

    narration = video_path.parent / "narration.mp3"
    evidence = release_bar.RenderEvidence(
        completed=bool(probe.get("ok")),
        failure_reason="" if probe.get("ok") else str(probe.get("error") or "media unreadable"),
        video_duration_sec=float(probe.get("duration") or 0.0),
        narration_duration_sec=(
            release_bar.probe_duration(narration) if narration.is_file() else 0.0
        ),
        freeze_spans=release_bar.probe_freeze_spans(video_path) if probe.get("ok") else [],
    )
    return release_bar.evaluate_render(evidence)


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
