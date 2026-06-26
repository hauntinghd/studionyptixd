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

import json
import shutil
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable

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
    try:
        proc = subprocess.run(
            [yt, "--no-warnings", "--no-playlist", "--dump-json", url],
            capture_output=True, text=True, timeout=120,
        )
        if proc.returncode != 0:
            return {"error": (proc.stderr or proc.stdout or "yt-dlp failed")[:300]}
        info = json.loads((proc.stdout or "").strip().splitlines()[0])
    except Exception as exc:
        return {"error": str(exc)[:300]}
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
            n = min(max_frames, max(6, int(duration // 20) or 6))
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


# ─── Progress-tracked job lifecycle ────────────────────────────────────────
# Stages reported to the UI so the agent can show live progress (not a black box
# between "started" and "done"). Each stage writes status.json in the workspace.
STAGES = [
    ("queued", 0),
    ("fetching_metadata", 10),
    ("downloading_video", 35),
    ("extracting_keyframes", 55),
    ("analyzing_pacing", 70),
    ("extracting_audio", 90),
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
                existing = json.loads(sp.read_text(encoding="utf-8"))
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
        status = json.loads(sp.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"job_id": job_id, "status": "error", "error": str(exc)[:200]}
    if status.get("status") == "complete":
        ar = work / "analysis_result.json"
        if ar.is_file():
            try:
                full = json.loads(ar.read_text(encoding="utf-8"))
                status = {**status, **{k: v for k, v in full.items() if k not in ("status",)}}
            except Exception:
                pass
    return status


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
    """Full competitor analysis: metadata + scene keyframes + audio.

    Writes progress to status.json at each stage. The video file is deleted by
    default after frames/audio are extracted to save volume space.
    """
    job_id = job_id or uuid.uuid4().hex[:12]
    profile = analysis_profile(content_format)
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
    video_path = dl["video_path"]

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
    _write_status(work, pacing=pacing)

    _step("extracting_audio", "Extracting the audio track for downstream transcription and sound analysis…")
    audio = extract_audio(video_path, work / "audio")

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
    final_status = {k: v for k, v in result.items() if k != "metadata"}
    final_status.update({
        "status": "complete",
        "stage": "complete",
        "percent": 100,
        "note": "Reference analysis complete. Pacing, keyframes, audio track, and format-specific metrics are ready.",
    })
    _write_status(work, **final_status)
    return result


def start_analysis(
    url: str,
    *,
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
