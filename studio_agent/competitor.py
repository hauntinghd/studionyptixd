"""Competitor video analysis for Studio Agent.

Download a competitor video (e.g. a MrBeast upload) and extract the signal that
made it work — WITHOUT the storage blow-up of blind 1-FPS sampling.

Frame strategy (the right way):
  1. ffmpeg scene-detection (`select='gt(scene,THRESH)'`) — keep only frames where
     the shot actually changes. A 10-minute video yields ~20-60 meaningful cuts,
     not 600 near-duplicate stills.
  2. If scene detection is too sparse (static talking-head), fall back to an even
     interval sample capped at `max_frames`.
This gives the agent's vision model the hook, pacing, and B-roll structure at a
fraction of the frames/storage.

Audio is extracted once (mono 16kHz) for transcription by the caller.
yt-dlp + ffmpeg are both present in the Docker image.
"""
from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable

import httpx

from studio_agent import provider_policy

ROOT = Path(__file__).resolve().parents[1]
WORK_ROOT = Path(__import__("os").environ.get("STUDIO_AGENT_COMPETITOR_DIR", str(ROOT / "data" / "competitor_analysis")))

_VIDEO_EXTS = {".mp4", ".mkv", ".webm", ".mov", ".m4v"}


def _ytdlp_bin() -> str:
    return shutil.which("yt-dlp") or shutil.which("yt-dlp.exe") or ""


def _ffmpeg_bin() -> str:
    return shutil.which("ffmpeg") or shutil.which("ffmpeg.exe") or "ffmpeg"


def fetch_metadata(url: str) -> dict[str, Any]:
    """yt-dlp --dump-json metadata only (no download)."""
    yt = _ytdlp_bin()
    if not yt:
        return {"error": "yt-dlp not installed"}
    # Prefer android/web clients — default web often hangs or 403s on Fly.
    client_attempts = (
        ["--extractor-args", "youtube:player_client=android,web"],
        ["--extractor-args", "youtube:player_client=web"],
        [],
    )
    last_err = "yt-dlp failed"
    info: dict[str, Any] | None = None
    for extra in client_attempts:
        try:
            proc = subprocess.run(
                [yt, "--no-warnings", "--no-playlist", "--dump-json", *extra, url],
                capture_output=True,
                text=True,
                timeout=90,
            )
            if proc.returncode != 0:
                last_err = (proc.stderr or proc.stdout or "yt-dlp failed")[:300]
                continue
            lines = [line.strip() for line in (proc.stdout or "").strip().splitlines() if line.strip()]
            if not lines:
                last_err = "yt-dlp returned no metadata"
                continue
            info = json.loads(lines[0])
            break
        except Exception as exc:
            last_err = str(exc)[:300]
            continue
    if not isinstance(info, dict):
        return {"error": last_err}
    return {
        "id": info.get("id"),
        "title": info.get("title"),
        "channel": info.get("channel") or info.get("uploader"),
        "channel_id": info.get("channel_id"),
        "duration_sec": info.get("duration"),
        "view_count": info.get("view_count"),
        "like_count": info.get("like_count"),
        "comment_count": info.get("comment_count"),
        "upload_date": info.get("upload_date"),
        "categories": info.get("categories"),
        "tags": (info.get("tags") or [])[:30],
        "description": (info.get("description") or "")[:2000],
        "thumbnail": info.get("thumbnail"),
        "webpage_url": info.get("webpage_url") or url,
    }


def download_video(url: str, out_dir: Path, *, max_height: int = 720) -> dict[str, Any]:
    yt = _ytdlp_bin()
    if not yt:
        return {"error": "yt-dlp not installed"}
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        yt, "--no-warnings", "--no-playlist", "--merge-output-format", "mp4",
        "-f", f"bestvideo[height<={max_height}]+bestaudio/best[height<={max_height}]/best",
        "-o", str(out_dir / "%(id)s.%(ext)s"),
        "--extractor-args", "youtube:player_client=android,web",
        url,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    except Exception as exc:
        return {"error": str(exc)[:300]}
    if proc.returncode != 0:
        return {"error": (proc.stderr or proc.stdout or "download failed")[:300]}
    files = sorted(
        (p for p in out_dir.iterdir() if p.suffix.lower() in _VIDEO_EXTS),
        key=lambda p: p.stat().st_mtime, reverse=True,
    )
    if not files:
        return {"error": "no video file produced"}
    return {"video_path": str(files[0])}


def _probe_duration(video_path: str) -> float:
    ff = shutil.which("ffprobe") or "ffprobe"
    try:
        proc = subprocess.run(
            [ff, "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", video_path],
            capture_output=True, text=True, timeout=60,
        )
        return float((proc.stdout or "0").strip() or 0)
    except Exception:
        return 0.0


def extract_scene_keyframes(
    video_path: str,
    out_dir: Path,
    *,
    scene_threshold: float = 0.3,
    max_frames: int = 32,
) -> dict[str, Any]:
    """Scene-change keyframes via ffmpeg. Falls back to interval sampling."""
    out_dir.mkdir(parents=True, exist_ok=True)
    ff = _ffmpeg_bin()
    # Pass 1: scene detection.
    scene_pattern = str(out_dir / "scene_%04d.jpg")
    cmd = [
        ff, "-hide_banner", "-loglevel", "error", "-i", video_path,
        "-vf", f"select='gt(scene,{scene_threshold})',scale=640:-1",
        "-vsync", "vfr", "-frames:v", str(max_frames), scene_pattern,
    ]
    try:
        subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    except Exception as exc:
        return {"error": str(exc)[:300], "frames": []}
    frames = sorted(str(p) for p in out_dir.glob("scene_*.jpg"))

    method = "scene_detection"
    # Fallback: too few scenes (static video) -> even interval sample.
    if len(frames) < 6:
        duration = _probe_duration(video_path)
        if duration > 0:
            # Long/static explainers need denser interval sampling — 8 frames on a 170s
            # upload was missing most of the visual/story arc.
            interval_sec = 12 if duration <= 90 else 10
            n = min(max_frames, max(12, int(duration // interval_sec) or 12))
            fps = max(0.01, n / duration)
            interval_pattern = str(out_dir / "iv_%04d.jpg")
            cmd2 = [
                ff, "-hide_banner", "-loglevel", "error", "-i", video_path,
                "-vf", f"fps={fps:.4f},scale=640:-1", "-frames:v", str(n), interval_pattern,
            ]
            try:
                subprocess.run(cmd2, capture_output=True, text=True, timeout=600)
                iv = sorted(str(p) for p in out_dir.glob("iv_*.jpg"))
                if iv:
                    frames = iv
                    method = "interval_sample"
            except Exception:
                pass
    return {"method": method, "frame_count": len(frames), "frames": frames}


def _pick_representative_frames(frame_paths: list[str], *, limit: int = 8) -> list[str]:
    paths = [str(p) for p in frame_paths if p and Path(str(p)).is_file()]
    if not paths:
        return []
    if len(paths) <= limit:
        return paths
    idxs = sorted({0, len(paths) // 3, (2 * len(paths)) // 3, len(paths) - 1})
    return [paths[i] for i in idxs[:limit]]


def _fal_key() -> str:
    try:
        from backend_settings import FAL_AI_KEY
    except Exception:
        FAL_AI_KEY = ""
    return str(os.getenv("FAL_KEY", "") or FAL_AI_KEY or "").strip()


def _openrouter_key() -> str:
    # Compatibility helper: Studio provider policy denies OpenRouter even when
    # a stale key remains configured.
    return ""


def _vision_prompt_text(*, source_name: str, content_format: str) -> str:
    fmt = "YouTube Short" if _normalize_content_format(content_format) == "short" else "long-form video"
    label = str(source_name or "uploaded reference").strip()
    return (
        f"These are representative keyframes from a {fmt} reference ({label}). "
        "Describe ONLY what is visually observable. Include animation/render style "
        "(skeleton, 2D cartoon, 3D, motion graphics, live-action, mixed), subject types, "
        "on-screen text/titles, palette, and setting. "
        "Do NOT assume skeleton or any art style unless clearly visible. "
        "One factual paragraph, no production advice."
    )


def _encode_frame_data_urls(frame_paths: list[str]) -> tuple[list[str], str | None]:
    urls: list[str] = []
    for path in frame_paths:
        try:
            encoded = base64.standard_b64encode(Path(path).read_bytes()).decode("ascii")
        except Exception as exc:
            return [], f"frame_read_failed:{exc}"
        urls.append(f"data:image/jpeg;base64,{encoded}")
    return urls, None


def _summarize_keyframe_visuals_openrouter(
    picks: list[str],
    *,
    prompt_text: str,
    content_format: str,
) -> dict[str, Any]:
    _ = (prompt_text, content_format)
    return {
        "error": "openrouter_disabled_by_provider_policy",
        "summary": "",
        "frames_reviewed": len(picks),
    }


def _summarize_keyframe_visuals_fal(picks: list[str], *, prompt_text: str) -> dict[str, Any]:
    provider_policy.assert_provider_allowed("fal", provider_policy.SEMANTIC_QA_CAPABILITY)
    fal_key = _fal_key()
    if not fal_key:
        return {"error": "fal_not_configured", "summary": "", "frames_reviewed": len(picks)}
    image_urls, frame_err = _encode_frame_data_urls(picks)
    if frame_err:
        return {"error": frame_err, "summary": "", "frames_reviewed": 0}
    model = str(os.getenv("STUDIO_AGENT_VISION_MODEL_FAL", "anthropic/claude-haiku-4.5")).strip()
    try:
        with httpx.Client(timeout=120.0) as client:
            resp = client.post(
                "https://fal.run/fal-ai/any-llm/vision",
                headers={"Authorization": f"Key {fal_key}"},
                json={
                    "model": model,
                    "prompt": prompt_text,
                    "image_urls": image_urls,
                    "temperature": 0.1,
                },
            )
        if resp.status_code != 200:
            return {
                "error": f"fal_vision_failed:{resp.status_code}",
                "summary": "",
                "frames_reviewed": len(picks),
                "model": model,
            }
        from studio_agent.stt_utils import httpx_json_dict

        payload = httpx_json_dict(resp)
        summary = str(payload.get("output") or payload.get("result") or "").strip()
        if not summary:
            choices = payload.get("choices") if isinstance(payload, dict) else None
            message = choices[0].get("message") if isinstance(choices, list) and choices else {}
            summary = str((message or {}).get("content") or "").strip()
        if not summary:
            return {"error": "empty_vision_summary", "summary": "", "frames_reviewed": len(picks), "model": model}
        return {"summary": summary, "frames_reviewed": len(picks), "model": model, "provider": "fal"}
    except Exception as exc:
        return {"error": str(exc)[:240], "summary": "", "frames_reviewed": len(picks), "model": model}


def _summarize_keyframe_visuals_anthropic(
    picks: list[str],
    *,
    prompt_text: str,
) -> dict[str, Any]:
    from studio_agent.reference_providers import anthropic_messages_completion, reference_vision_model

    model = reference_vision_model()
    result = anthropic_messages_completion(
        prompt=prompt_text,
        model=model,
        max_tokens=1024,
        temperature=0.1,
        image_paths=picks,
    )
    summary = str(result.get("text") or "").strip()
    if summary:
        return {
            "summary": summary,
            "frames_reviewed": len(picks),
            "model": model,
            "provider": "anthropic",
        }
    return {
        "error": str(result.get("error") or "empty_vision_summary")[:240],
        "summary": "",
        "frames_reviewed": len(picks),
        "model": model,
    }


def summarize_keyframe_visuals(
    frame_paths: list[str],
    *,
    source_name: str = "",
    content_format: str = "short",
) -> dict[str, Any]:
    """Vision pass on extracted keyframes — factual visual style, not session art-style bias."""
    from studio_agent.reference_providers import run_provider_chain, vision_provider_order

    picks = _pick_representative_frames(frame_paths, limit=4)
    if not picks:
        return {"error": "no_keyframes", "summary": "", "frames_reviewed": 0}

    prompt_text = _vision_prompt_text(source_name=source_name, content_format=content_format)
    return run_provider_chain(
        vision_provider_order(),
        {
            "anthropic": lambda: _summarize_keyframe_visuals_anthropic(picks, prompt_text=prompt_text),
            "fal": lambda: _summarize_keyframe_visuals_fal(picks, prompt_text=prompt_text),
            "openrouter": lambda: _summarize_keyframe_visuals_openrouter(
                picks,
                prompt_text=prompt_text,
                content_format=content_format,
            ),
        },
        success_key="summary",
    )


def _pacing_quality_notes(pacing: dict[str, Any], *, content_format: str) -> dict[str, Any]:
    duration = float(pacing.get("duration_sec") or 0)
    cut_count = int(pacing.get("cut_count") or 0)
    notes: list[str] = []
    fmt = _normalize_content_format(content_format)
    if fmt == "short" and duration > 90:
        notes.append(
            f"Detected duration is {duration:.1f}s, which is unusually long for a Short. "
            "Confirm the uploaded file is the intended clip, not a padded/rehosted export."
        )
    if cut_count <= 2 and duration > 0:
        notes.append(
            "Very few scene cuts were detected. Static or minimally animated explainers often look like "
            "one continuous shot to ffmpeg — pacing metrics reflect cut detection, not storytelling quality."
        )
    if notes:
        return {"warnings": notes, "reliable_for_shorts_pacing": False}
    return {"warnings": [], "reliable_for_shorts_pacing": True}


def extract_cut_timeline(
    video_path: str,
    *,
    scene_threshold: float = 0.3,
    max_cuts: int = 80,
) -> dict[str, Any]:
    """Scene-change timestamps via ffmpeg showinfo (for pacing / blueprint)."""
    ff = _ffmpeg_bin()
    cmd = [
        ff, "-hide_banner", "-loglevel", "info", "-i", video_path,
        "-vf", f"select='gt(scene,{scene_threshold})',showinfo",
        "-an", "-f", "null", "-",
    ]
    cuts: list[float] = []
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        import re

        for line in (proc.stderr or "").splitlines():
            m = re.search(r"pts_time:([0-9.]+)", line)
            if m:
                cuts.append(round(float(m.group(1)), 3))
        cuts = sorted(set(cuts))[:max_cuts]
    except Exception as exc:
        return {"error": str(exc)[:200], "cuts_sec": [], "cut_count": 0}

    duration = _probe_duration(video_path)
    avg_shot = 0.0
    if len(cuts) >= 2:
        gaps = [cuts[i + 1] - cuts[i] for i in range(len(cuts) - 1)]
        avg_shot = sum(gaps) / len(gaps) if gaps else 0.0
    elif duration > 0 and cuts:
        avg_shot = duration / max(len(cuts), 1)

    return {
        "cuts_sec": cuts,
        "cut_count": len(cuts),
        "duration_sec": round(duration, 2),
        "avg_shot_sec": round(avg_shot, 2) if avg_shot else None,
        "hook_window_sec": 8,
    }


def extract_audio(video_path: str, out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    ff = _ffmpeg_bin()
    audio_path = out_dir / "audio.mp3"
    cmd = [
        ff, "-hide_banner", "-loglevel", "error", "-y", "-i", video_path,
        "-vn", "-ac", "1", "-ar", "16000", "-b:a", "96k", str(audio_path),
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if proc.returncode != 0 or not audio_path.exists():
            return {"error": (proc.stderr or "audio extract failed")[:300]}
    except Exception as exc:
        return {"error": str(exc)[:300]}
    return {"audio_path": str(audio_path)}


def _coerce_timestamp_sec(value: Any) -> float:
    """Normalize FAL/OpenRouter whisper timestamps (float, str, or [start, end] list)."""
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return 0.0
        try:
            return float(text)
        except ValueError:
            return 0.0
    if isinstance(value, (list, tuple)):
        if not value:
            return 0.0
        return _coerce_timestamp_sec(value[0])
    return 0.0


def _coerce_segment_times(chunk: dict[str, Any]) -> tuple[float, float]:
    timestamp = chunk.get("timestamp")
    start_raw = chunk.get("start", timestamp)
    end_raw = chunk.get("end")
    if isinstance(timestamp, (list, tuple)):
        if len(timestamp) >= 2:
            start_raw, end_raw = timestamp[0], timestamp[1]
        elif len(timestamp) == 1:
            start_raw = timestamp[0]
    start = _coerce_timestamp_sec(start_raw)
    end = _coerce_timestamp_sec(end_raw if end_raw is not None else start)
    if end < start:
        end = start
    return start, end


def transcribe_reference_audio(audio_path: str, *, audio_error: str = "") -> dict[str, Any]:
    """Transcribe extracted reference audio (xAI STT primary, FAL whisper fallback)."""
    from studio_agent.dictation import transcribe_file_path
    from studio_agent.reference_providers import run_provider_chain, stt_provider_order, transcribe_fal_segments

    path = Path(str(audio_path or ""))
    if not path.is_file():
        err = str(audio_error or "audio_missing").strip() or "audio_missing"
        return {"error": err, "text": "", "segments": []}

    def _xai_transcribe() -> dict[str, Any]:
        try:
            return transcribe_file_path(str(path))
        except Exception as exc:
            return {"error": str(exc)[:240], "text": "", "segments": []}

    return run_provider_chain(
        stt_provider_order(),
        {
            "xai": _xai_transcribe,
            "fal": lambda: transcribe_fal_segments(str(path)),
        },
        success_key="text",
    )


def _narration_beat_pacing(transcript: dict[str, Any], *, duration_sec: float) -> dict[str, Any]:
    segments = [s for s in (transcript.get("segments") or []) if isinstance(s, dict)]
    if not segments:
        return {}
    gaps: list[float] = []
    for idx in range(1, len(segments)):
        prev_end = float(segments[idx - 1].get("end_sec") or 0)
        start = float(segments[idx].get("start_sec") or 0)
        if start > prev_end:
            gaps.append(start - prev_end)
    beat_count = len(segments)
    avg_beat = (duration_sec / beat_count) if duration_sec > 0 and beat_count else 0.0
    return {
        "narration_beat_count": beat_count,
        "avg_narration_beat_sec": round(avg_beat, 2) if avg_beat else None,
        "pause_gaps_sec": [round(g, 2) for g in gaps[:12]],
    }


def _storytelling_prompt(
    *,
    transcript_text: str,
    segments: list[dict[str, Any]],
    visual_summary: str,
    pacing: dict[str, Any],
    content_format: str,
    source_name: str,
) -> str:
    fmt = "YouTube Short" if _normalize_content_format(content_format) == "short" else "long-form video"
    beat_lines = []
    for seg in segments[:16]:
        if not isinstance(seg, dict):
            continue
        beat_lines.append(
            f"- {seg.get('start_sec', 0):.1f}s: {str(seg.get('text') or '')[:180]}"
        )
    beats_block = "\n".join(beat_lines) if beat_lines else "(no segment timestamps)"
    return (
        f"Analyze this {fmt} reference ({source_name or 'uploaded video'}) for a YouTube strategist.\n\n"
        f"VISUAL SUMMARY:\n{visual_summary or '(none)'}\n\n"
        f"TRANSCRIPT:\n{transcript_text[:6000] or '(none)'}\n\n"
        f"NARRATION BEATS:\n{beats_block}\n\n"
        f"FFMPEG PACING (may under-count cuts on static explainers):\n"
        f"- duration {pacing.get('duration_sec')}s, cuts {pacing.get('cut_count')}, "
        f"avg shot {pacing.get('avg_shot_sec')}s\n\n"
        "Return JSON only with keys: hook (first 1-3s promise), story_beats (array of 4-8 strings), "
        "packaging (title/thumbnail angle), pacing_notes, cta_placement, retention_risks (array), "
        "summary (2-3 sentence plain-language readout for the creator)."
    )


def _parse_storytelling_json(raw: str, *, model: str, provider: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if text.startswith("```"):
        text = text.strip("`").removeprefix("json").strip()
    try:
        parsed = json.loads(text)
    except Exception:
        parsed = {"summary": text}
    if not isinstance(parsed, dict):
        parsed = {"summary": text}
    parsed["model"] = model
    parsed["provider"] = provider
    return parsed


def _analyze_storytelling_openrouter(prompt: str) -> dict[str, Any]:
    _ = prompt
    return {"error": "openrouter_disabled_by_provider_policy", "summary": ""}


def _analyze_storytelling_fal(prompt: str) -> dict[str, Any]:
    provider_policy.assert_provider_allowed("fal", provider_policy.SEMANTIC_QA_CAPABILITY)
    fal_key = _fal_key()
    if not fal_key:
        return {"error": "fal_not_configured", "summary": ""}
    model = str(os.getenv("STUDIO_AGENT_ANALYSIS_MODEL_FAL", "anthropic/claude-haiku-4.5")).strip()
    try:
        with httpx.Client(timeout=120.0) as client:
            resp = client.post(
                "https://fal.run/fal-ai/any-llm",
                headers={"Authorization": f"Key {fal_key}"},
                json={
                    "model": model,
                    "prompt": prompt,
                    "temperature": 0.2,
                },
            )
        if resp.status_code != 200:
            return {"error": f"fal_story_failed:{resp.status_code}", "summary": ""}
        from studio_agent.stt_utils import httpx_json_dict

        payload = httpx_json_dict(resp)
        raw = str(payload.get("output") or payload.get("result") or "").strip()
        if not raw:
            choices = payload.get("choices") if isinstance(payload, dict) else None
            message = choices[0].get("message") if isinstance(choices, list) and choices else {}
            raw = str((message or {}).get("content") or "").strip()
        return _parse_storytelling_json(raw, model=model, provider="fal")
    except Exception as exc:
        return {"error": str(exc)[:240], "summary": ""}


def _analyze_storytelling_anthropic(prompt: str) -> dict[str, Any]:
    from studio_agent.reference_providers import anthropic_messages_completion, reference_analysis_model

    model = reference_analysis_model()
    result = anthropic_messages_completion(
        prompt=prompt + "\n\nReturn JSON only.",
        model=model,
        max_tokens=2048,
        temperature=0.2,
    )
    raw = str(result.get("text") or "").strip()
    if raw:
        parsed = _parse_storytelling_json(raw, model=model, provider="anthropic")
        if str(parsed.get("summary") or parsed.get("hook") or "").strip():
            return parsed
    return {"error": str(result.get("error") or "empty_storytelling_summary")[:240], "summary": ""}


def analyze_storytelling_packaging(
    *,
    transcript_text: str,
    segments: list[dict[str, Any]],
    visual_summary: str,
    pacing: dict[str, Any],
    content_format: str,
    source_name: str = "",
) -> dict[str, Any]:
    """Synthesize hook, story beats, packaging, and CTA from transcript + visuals."""
    from studio_agent.reference_providers import analysis_provider_order, run_provider_chain

    if not str(transcript_text or "").strip() and not str(visual_summary or "").strip():
        return {"error": "no_story_inputs", "summary": ""}

    prompt = _storytelling_prompt(
        transcript_text=transcript_text,
        segments=segments,
        visual_summary=visual_summary,
        pacing=pacing,
        content_format=content_format,
        source_name=source_name,
    )

    def _story_success(payload: dict[str, Any]) -> bool:
        return bool(
            str(payload.get("summary") or payload.get("hook") or "").strip()
            and not payload.get("error")
        )

    order = analysis_provider_order()
    last: dict[str, Any] = {"error": "no_storytelling_providers_configured", "summary": ""}
    runners = {
        "anthropic": lambda: _analyze_storytelling_anthropic(prompt),
        "fal": lambda: _analyze_storytelling_fal(prompt),
    }
    for name in order:
        runner = runners.get(name)
        if not runner:
            continue
        result = runner()
        if _story_success(result if isinstance(result, dict) else {}):
            return result
        if isinstance(result, dict):
            last = result
    return last


def compute_analysis_gaps(
    *,
    visual: dict[str, Any],
    transcript: dict[str, Any],
    storytelling: dict[str, Any],
    frames: dict[str, Any],
    audio: dict[str, Any],
) -> dict[str, Any]:
    """Report which deep-analysis stages succeeded vs failed (pacing-only is not enough)."""
    gaps: list[dict[str, str]] = []
    if not str(visual.get("summary") or "").strip():
        gaps.append({
            "stage": "vision",
            "error": str(visual.get("error") or frames.get("error") or "empty_visual_summary")[:240],
        })
    if not str(transcript.get("text") or "").strip():
        gaps.append({
            "stage": "transcript",
            "error": str(transcript.get("error") or audio.get("error") or "empty_transcript")[:240],
        })
    has_story = bool(
        str(storytelling.get("summary") or "").strip()
        or str(storytelling.get("hook") or "").strip()
    )
    if not has_story:
        gaps.append({
            "stage": "storytelling",
            "error": str(storytelling.get("error") or "empty_storytelling")[:240],
        })
    has_depth = bool(
        str(visual.get("summary") or "").strip()
        or str(transcript.get("text") or "").strip()
        or has_story
    )
    if has_depth and not gaps:
        depth = "full"
    elif has_depth:
        depth = "partial"
    else:
        depth = "pacing_only"
    return {
        "depth": depth,
        "gaps": gaps,
        "stage_errors": {item["stage"]: item["error"] for item in gaps},
    }


# ─── Progress-tracked job lifecycle ────────────────────────────────────────
# Stages reported to the UI so the agent can show live progress (not a black box
# between "started" and "done"). Each stage writes status.json in the workspace.
STAGES = [
    ("queued", 0),
    ("fetching_metadata", 10),
    ("downloading_video", 35),
    ("extracting_keyframes", 55),
    ("analyzing_pacing", 70),
    ("extracting_audio", 82),
    ("transcribing_audio", 90),
    ("analyzing_story", 96),
    ("complete", 100),
]


def _normalize_content_format(value: str | None) -> str:
    value = str(value or "short").strip().lower().replace("-", "")
    if value in {"long", "longform"}:
        return "long"
    return "short"


def analysis_profile(content_format: str | None) -> dict[str, Any]:
    """Return the correct learning/evaluation contract for each video format."""
    fmt = _normalize_content_format(content_format)
    if fmt == "long":
        return {
            "content_format": "long",
            "label": "long-form",
            "reference_archetypes": [
                "Jake Tran",
                "Magnates Media",
                "Lume",
                "high-retention documentary channels",
            ],
            "observable_reference_metrics": [
                "cold_open_duration_sec",
                "avg_shot_sec",
                "cuts_per_minute",
                "chapter_lengths_sec",
                "pattern_interrupt_interval_sec",
                "visual_source_mix",
                "music_and_silence_transitions",
                "cta_timing_sec",
            ],
            "channel_learning_metrics": [
                "impressions_ctr",
                "first_30_second_retention",
                "average_view_duration",
                "average_percentage_viewed",
                "watch_time_hours",
                "chapter_retention_and_dropoffs",
                "returning_viewers",
                "end_screen_click_rate",
            ],
        }
    return {
        "content_format": "short",
        "label": "short-form",
        "reference_archetypes": [
            "high-retention Shorts in the selected niche",
            "channel-specific Shorts outliers",
        ],
        "observable_reference_metrics": [
            "first_visual_change_sec",
            "avg_shot_sec",
            "cuts_per_10_seconds",
            "hook_clarity_first_1_to_3_seconds",
            "caption_density",
            "loop_or_exit_pattern",
            "cta_timing_sec",
        ],
        "channel_learning_metrics": [
            "viewed_vs_swiped_away",
            "first_1_to_3_second_retention",
            "average_percentage_viewed",
            "completion_rate",
            "rewatch_or_loop_rate",
            "engaged_views",
            "likes_comments_shares_per_view",
            "subscribers_gained_per_1000_views",
        ],
    }


def _status_path(work: Path) -> Path:
    return work / "status.json"


def _write_status(work: Path, **fields: Any) -> None:
    try:
        work.mkdir(parents=True, exist_ok=True)
        existing: dict[str, Any] = {}
        sp = _status_path(work)
        if sp.exists():
            try:
                from studio_agent.stt_utils import safe_json_loads

                existing = safe_json_loads(sp.read_text(encoding="utf-8"), default={})
                if not isinstance(existing, dict):
                    existing = {}
            except Exception:
                existing = {}
        existing.update(fields)
        existing["updated_at"] = time.time()
        sp.write_text(json.dumps(existing, indent=2, ensure_ascii=True), encoding="utf-8")
    except Exception:
        pass


def _stage_percent(stage: str) -> int:
    for name, pct in STAGES:
        if name == stage:
            return pct
    return 0


def read_status(job_id: str) -> dict[str, Any]:
    work = (WORK_ROOT / job_id).resolve()
    sp = _status_path(work)
    if not sp.exists():
        return {"job_id": job_id, "status": "unknown", "error": "no such job"}
    try:
        from studio_agent.stt_utils import safe_json_loads

        status = safe_json_loads(sp.read_text(encoding="utf-8"), default={})
        if not isinstance(status, dict) or not status:
            return {"job_id": job_id, "status": "error", "error": "reference status file empty or invalid"}
    except Exception as exc:
        return {"job_id": job_id, "status": "error", "error": str(exc)[:200]}
    if status.get("status") == "complete":
        ar = work / "analysis_result.json"
        if ar.is_file():
            try:
                from studio_agent.stt_utils import safe_json_loads

                full = safe_json_loads(ar.read_text(encoding="utf-8"), default={})
                if isinstance(full, dict) and full:
                    status = {**status, **{k: v for k, v in full.items() if k not in ("status",)}}
            except Exception:
                pass
    return status


def retry_reference_stages(
    job_id: str,
    *,
    stages: list[str] | None = None,
) -> dict[str, Any]:
    """Re-run failed reference-analysis stages on an existing job workspace."""
    work = (WORK_ROOT / str(job_id or "").strip()).resolve()
    if not work.is_dir():
        return {"status": "failed", "job_id": job_id, "error": "reference job workspace not found"}
    status = read_status(str(job_id))
    meta = status.get("metadata") if isinstance(status.get("metadata"), dict) else {}
    pacing = status.get("pacing") if isinstance(status.get("pacing"), dict) else {}
    frames_data = status.get("frames") if isinstance(status.get("frames"), dict) else {}
    frame_paths = list(frames_data.get("paths") or [])
    if not frame_paths:
        frame_paths = sorted(str(p) for p in (work / "frames").glob("*.jpg"))
    profile = status.get("analysis_profile") if isinstance(status.get("analysis_profile"), dict) else analysis_profile("short")
    content_format = str(profile.get("content_format") or "short")
    requested = {str(stage or "").strip().lower() for stage in (stages or ["transcript", "vision", "storytelling"])}
    visual = status.get("visual_summary") if isinstance(status.get("visual_summary"), dict) else {}
    transcript = status.get("transcript") if isinstance(status.get("transcript"), dict) else {}
    audio = status.get("audio") if isinstance(status.get("audio"), dict) else {}
    audio_path = str(audio.get("path") or audio.get("audio_path") or (work / "audio" / "audio.mp3"))

    if "vision" in requested or "visual" in requested:
        _write_status(work, stage="extracting_keyframes", status="running", note="Retrying vision on saved keyframes…")
        visual = summarize_keyframe_visuals(
            frame_paths,
            source_name=str(meta.get("title") or ""),
            content_format=content_format,
        )
        _write_status(work, visual_summary=visual)

    if "transcript" in requested or "audio" in requested:
        _write_status(work, stage="transcribing_audio", status="running", note="Retrying transcript extraction…")
        if not Path(str(audio_path)).is_file():
            video_candidates = sorted(
                (p for p in work.rglob("*") if p.suffix.lower() in _VIDEO_EXTS),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            if video_candidates:
                audio = extract_audio(str(video_candidates[0]), work / "audio")
                audio_path = str(audio.get("audio_path") or "")
        transcript = transcribe_reference_audio(
            str(audio_path),
            audio_error=str(audio.get("error") or ""),
        )
        _write_status(work, transcript=transcript, audio=audio)

    visual_text = str(visual.get("summary") or "").strip() if isinstance(visual, dict) else ""
    if "storytelling" in requested or "story" in requested:
        _write_status(work, stage="analyzing_story", status="running", note="Retrying storytelling readout…")
        storytelling = analyze_storytelling_packaging(
            transcript_text=str(transcript.get("text") or ""),
            segments=list(transcript.get("segments") or []),
            visual_summary=visual_text,
            pacing=pacing,
            content_format=content_format,
            source_name=str(meta.get("title") or ""),
        )
        _write_status(work, storytelling=storytelling)
    else:
        storytelling = status.get("storytelling") if isinstance(status.get("storytelling"), dict) else {}

    analysis_gaps = compute_analysis_gaps(
        visual=visual if isinstance(visual, dict) else {},
        transcript=transcript if isinstance(transcript, dict) else {},
        storytelling=storytelling if isinstance(storytelling, dict) else {},
        frames=frames_data if isinstance(frames_data, dict) else {},
        audio=audio if isinstance(audio, dict) else {},
    )
    result = {
        **{k: v for k, v in status.items() if k not in {"status", "stage", "percent", "note"}},
        "status": "complete",
        "job_id": job_id,
        "visual_summary": visual,
        "transcript": transcript,
        "storytelling": storytelling,
        "analysis_gaps": analysis_gaps,
        "retried_stages": sorted(requested),
    }
    try:
        (work / "analysis_result.json").write_text(
            json.dumps(result, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        pass
    _write_status(
        work,
        **{k: v for k, v in result.items() if k != "metadata"},
        status="complete",
        stage="complete",
        percent=100,
        note="Reference stage retry complete.",
    )
    return result


def _analyze_video_file(
    *,
    job_id: str,
    work: Path,
    url: str,
    meta: dict[str, Any],
    video_path: str,
    scene_threshold: float,
    max_frames: int,
    keep_video: bool,
    content_format: str,
    progress: Callable[[str, str], None] | None = None,
) -> dict[str, Any]:
    profile = analysis_profile(content_format)

    def _step(stage: str, note: str = "") -> None:
        _write_status(
            work,
            job_id=job_id,
            url=url,
            status="running" if stage != "complete" else "complete",
            stage=stage,
            percent=_stage_percent(stage),
            note=note,
        )
        if progress:
            try:
                progress(stage, note)
            except Exception:
                pass

    _step("extracting_keyframes", "Detecting scene cuts (storage-efficient keyframes)…")
    frames = extract_scene_keyframes(video_path, work / "frames", scene_threshold=scene_threshold, max_frames=max_frames)
    _write_status(work, frames_extracted=frames.get("frame_count", 0), frame_method=frames.get("method"))

    pacing_note = (
        "Building documentary pacing, chapter, and pattern-interrupt signals…"
        if profile["content_format"] == "long"
        else "Building Shorts hook, cut-rhythm, caption, and loop signals…"
    )
    _step("analyzing_pacing", pacing_note)
    pacing = extract_cut_timeline(video_path, scene_threshold=scene_threshold)
    pacing_quality = _pacing_quality_notes(pacing, content_format=profile["content_format"])
    _write_status(work, pacing=pacing, pacing_quality=pacing_quality)

    visual = summarize_keyframe_visuals(
        list(frames.get("frames") or []),
        source_name=str(meta.get("title") or meta.get("channel") or ""),
        content_format=profile["content_format"],
    )
    _write_status(work, visual_summary=visual)

    _step("extracting_audio", "Extracting the audio track for downstream transcription and sound analysis…")
    audio = extract_audio(video_path, work / "audio")

    _step("transcribing_audio", "Transcribing narration for story beats, hook timing, and packaging signals…")
    transcript = transcribe_reference_audio(
        str(audio.get("audio_path") or ""),
        audio_error=str(audio.get("error") or ""),
    )
    narration_pacing = _narration_beat_pacing(
        transcript,
        duration_sec=float(pacing.get("duration_sec") or 0),
    )
    if narration_pacing and int(pacing.get("cut_count") or 0) <= 2:
        pacing = {**pacing, "narration_beats": narration_pacing}

    visual_text = ""
    if isinstance(visual, dict):
        visual_text = str(visual.get("summary") or "").strip()
    _step("analyzing_story", "Synthesizing hook, story structure, packaging, and retention readout…")
    storytelling = analyze_storytelling_packaging(
        transcript_text=str(transcript.get("text") or ""),
        segments=list(transcript.get("segments") or []),
        visual_summary=visual_text,
        pacing=pacing,
        content_format=profile["content_format"],
        source_name=str(meta.get("title") or meta.get("channel") or ""),
    )
    analysis_gaps = compute_analysis_gaps(
        visual=visual if isinstance(visual, dict) else {},
        transcript=transcript if isinstance(transcript, dict) else {},
        storytelling=storytelling if isinstance(storytelling, dict) else {},
        frames=frames if isinstance(frames, dict) else {},
        audio=audio if isinstance(audio, dict) else {},
    )
    _write_status(work, transcript=transcript, storytelling=storytelling, analysis_gaps=analysis_gaps)

    if not keep_video:
        try:
            Path(video_path).unlink(missing_ok=True)
        except Exception:
            pass

    engagement = {}
    try:
        vc = float(meta.get("view_count") or 0)
        lc = float(meta.get("like_count") or 0)
        cc = float(meta.get("comment_count") or 0)
        if vc > 0:
            engagement = {
                "like_rate_pct": round(lc / vc * 100, 4),
                "comment_rate_pct": round(cc / vc * 100, 4),
            }
    except Exception:
        pass

    result = {
        "status": "complete",
        "job_id": job_id,
        "url": url,
        "metadata": meta,
        "engagement": engagement,
        "pacing": pacing,
        "pacing_quality": pacing_quality,
        "visual_summary": visual,
        "transcript": transcript,
        "storytelling": storytelling,
        "analysis_gaps": analysis_gaps,
        "analysis_profile": profile,
        "frames": {
            "method": frames.get("method"),
            "count": frames.get("frame_count", 0),
            "paths": frames.get("frames", []),
            "error": frames.get("error"),
        },
        "audio": {"path": audio.get("audio_path", ""), "error": audio.get("error")},
        "workspace": str(work),
        "next_steps": [
            "Call build_scene_blueprint_from_reference(job_id, topic, characters_per_scene) to map "
            "cuts → scenes with Seedream v4.5 edit + i2v duration targets.",
            "load_skill script-writing — hardest step; beat-map narration to story_beat per scene.",
            "load_skill thumbnail-design — packaging/delivery is second-hardest after script.",
            "get_channel_analytics or recommend_video_topics to pick the user's next niche topic.",
        ],
        "style_reference_note": (
            "Long-form documentary pacing profile extracted. Evaluate chapter retention, first-30-second "
            "retention, AVD, APV, watch time, and drop-offs separately from Shorts."
            if profile["content_format"] == "long"
            else "Short-form pacing profile extracted. Evaluate viewed-vs-swiped, first-1-to-3-second "
            "retention, completion, APV, rewatches, and engagement separately from long-form."
        ),
    }
    try:
        (work / "analysis_result.json").write_text(
            json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    except Exception:
        pass
    gaps_depth = str(analysis_gaps.get("depth") or "").strip().lower()
    if gaps_depth == "pacing_only":
        completion_note = (
            "Reference pacing extracted, but vision/transcript/story stages did not return usable content. "
            "Retry deep analysis before planning from this upload."
        )
    elif gaps_depth == "partial":
        completion_note = (
            "Reference analysis partially complete — pacing and some deep-analysis stages succeeded, "
            "but one or more vision/transcript/story stages returned errors."
        )
    else:
        completion_note = (
            "Reference analysis complete. Pacing, keyframes, transcript, storytelling/packaging readout, "
            "and format-specific metrics are ready."
        )
    final_status = {k: v for k, v in result.items() if k != "metadata"}
    final_status.update({
        "status": "complete",
        "stage": "complete",
        "percent": 100,
        "note": completion_note,
    })
    _write_status(work, **final_status)
    return result


def analyze(
    url: str,
    *,
    scene_threshold: float = 0.3,
    max_frames: int = 32,
    keep_video: bool = False,
    job_id: str | None = None,
    content_format: str = "short",
    progress: Callable[[str, str], None] | None = None,
) -> dict[str, Any]:
    """Full competitor analysis: metadata + scene keyframes + audio."""
    job_id = job_id or uuid.uuid4().hex[:12]
    work = (WORK_ROOT / job_id).resolve()
    work.mkdir(parents=True, exist_ok=True)

    def _step(stage: str, note: str = "") -> None:
        _write_status(
            work,
            job_id=job_id,
            url=url,
            status="running" if stage != "complete" else "complete",
            stage=stage,
            percent=_stage_percent(stage),
            note=note,
        )
        if progress:
            try:
                progress(stage, note)
            except Exception:
                pass

    _step("fetching_metadata", "Reading title, views, duration…")
    meta = fetch_metadata(url)
    if meta.get("error"):
        _write_status(work, job_id=job_id, status="failed", stage="fetching_metadata", percent=10, error=meta["error"])
        return {"status": "failed", "stage": "metadata", "error": meta["error"], "job_id": job_id}
    _write_status(work, metadata=meta)

    _step("downloading_video", f"Downloading '{str(meta.get('title') or '')[:60]}'…")
    dl = download_video(url, work / "video")
    if dl.get("error"):
        _write_status(work, job_id=job_id, status="failed", stage="downloading_video", percent=35, error=dl["error"])
        return {"status": "failed", "stage": "download", "error": dl["error"], "metadata": meta, "job_id": job_id}
    return _analyze_video_file(
        job_id=job_id,
        work=work,
        url=url,
        meta=meta,
        video_path=dl["video_path"],
        scene_threshold=scene_threshold,
        max_frames=max_frames,
        keep_video=keep_video,
        content_format=content_format,
        progress=progress,
    )


def analyze_local_file(
    video_path: str,
    *,
    source_name: str = "",
    scene_threshold: float = 0.3,
    max_frames: int = 32,
    keep_video: bool = True,
    job_id: str | None = None,
    content_format: str = "short",
    progress: Callable[[str, str], None] | None = None,
) -> dict[str, Any]:
    """Analyze an uploaded reference video already persisted on disk."""
    path = Path(str(video_path or "")).resolve()
    if not path.is_file():
        job_id = job_id or uuid.uuid4().hex[:12]
        work = (WORK_ROOT / job_id).resolve()
        work.mkdir(parents=True, exist_ok=True)
        _write_status(
            work,
            job_id=job_id,
            url=str(path),
            status="failed",
            stage="loading_upload",
            percent=10,
            error="uploaded video file not found",
        )
        return {"status": "failed", "stage": "loading_upload", "error": "uploaded video file not found", "job_id": job_id}

    job_id = job_id or uuid.uuid4().hex[:12]
    work = (WORK_ROOT / job_id).resolve()
    work.mkdir(parents=True, exist_ok=True)
    label = str(source_name or path.name).strip() or path.name
    duration_sec = _probe_duration(str(path))
    meta = {
        "title": label,
        "channel": "Uploaded reference",
        "duration_sec": duration_sec,
        "source": "uploaded_reference",
        "local_path": str(path),
    }
    _write_status(work, job_id=job_id, url=str(path), metadata=meta, source="uploaded_reference")
    return _analyze_video_file(
        job_id=job_id,
        work=work,
        url=str(path),
        meta=meta,
        video_path=str(path),
        scene_threshold=scene_threshold,
        max_frames=max_frames,
        keep_video=keep_video,
        content_format=content_format,
        progress=progress,
    )


def start_analysis(
    url: str,
    *,
    user_id: str = "",
    scene_threshold: float = 0.3,
    max_frames: int = 32,
    content_format: str = "short",
) -> str:
    """Spawn analysis in a background thread; return job_id immediately.

    The agent polls poll_render_job(kind='competitor') to show live progress
    (downloading -> keyframes -> audio -> complete) instead of blocking.
    """
    job_id = uuid.uuid4().hex[:12]
    work = (WORK_ROOT / job_id).resolve()
    work.mkdir(parents=True, exist_ok=True)
    profile = analysis_profile(content_format)
    _write_status(
        work,
        job_id=job_id,
        user_id=str(user_id or "").strip(),
        url=url,
        status="running",
        stage="queued",
        percent=0,
        analysis_profile=profile,
    )

    def _work() -> None:
        try:
            analyze(
                url,
                scene_threshold=scene_threshold,
                max_frames=max_frames,
                job_id=job_id,
                content_format=profile["content_format"],
            )
        except Exception as exc:
            _write_status(work, job_id=job_id, status="failed", stage="error", error=str(exc)[:300])

    threading.Thread(target=_work, daemon=True, name=f"competitor-{job_id}").start()
    return job_id


def start_analysis_from_path(
    video_path: str,
    *,
    user_id: str = "",
    source_name: str = "",
    scene_threshold: float = 0.3,
    max_frames: int = 32,
    content_format: str = "short",
) -> str:
    """Spawn local-upload reference analysis in a background thread."""
    job_id = uuid.uuid4().hex[:12]
    work = (WORK_ROOT / job_id).resolve()
    work.mkdir(parents=True, exist_ok=True)
    profile = analysis_profile(content_format)
    _write_status(
        work,
        job_id=job_id,
        user_id=str(user_id or "").strip(),
        url=str(video_path),
        status="running",
        stage="queued",
        percent=0,
        analysis_profile=profile,
        source="uploaded_reference",
    )

    def _work() -> None:
        try:
            analyze_local_file(
                video_path,
                source_name=source_name,
                scene_threshold=scene_threshold,
                max_frames=max_frames,
                job_id=job_id,
                content_format=profile["content_format"],
            )
        except Exception as exc:
            _write_status(work, job_id=job_id, status="failed", stage="error", error=str(exc)[:300])

    threading.Thread(target=_work, daemon=True, name=f"competitor-local-{job_id}").start()
    return job_id
