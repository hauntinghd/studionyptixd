"""ClipLab job orchestration."""
from __future__ import annotations

import json
import logging
import random
import time
from pathlib import Path
from typing import Any, Callable, Awaitable

from cliplab.config import (
    CLIPLAB_CREDITS_PER_MINUTE,
    CLIPLAB_JOBS_DIR,
    CLIPLAB_MIN_CREDITS,
    CLIPLAB_UPLOAD_DIR,
    REFRAME_BACKEND,
    VIRALITY_BACKEND,
)
from cliplab.intelligence import judge_segment_confidence, rank_segments
from cliplab.models import ClipSegment, TranscriptCue
from cliplab.render import render_clips_batch
from cliplab.transcribe import load_transcript, transcribe_video

_log = logging.getLogger("nyptid-studio.cliplab.pipeline")

JsonCompletionFn = Callable[..., Awaitable[dict]]


def _safe_user_dir(user: dict) -> str:
    uid = str((user or {}).get("id") or "anon").strip()
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in uid)[:64]


def video_upload_path(user: dict, upload_id: str) -> Path | None:
    base = CLIPLAB_UPLOAD_DIR / _safe_user_dir(user)
    if not base.exists():
        return None
    for p in base.iterdir():
        if p.is_file() and p.stem == upload_id:
            return p
    return None


def credits_for_duration(duration_sec: float) -> int:
    minutes = max(1, int((max(0.0, duration_sec) + 59) // 60))
    return max(CLIPLAB_MIN_CREDITS, minutes * CLIPLAB_CREDITS_PER_MINUTE)


def save_job_state(job_id: str, payload: dict) -> None:
    path = CLIPLAB_JOBS_DIR / f"{job_id}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_job_state(job_id: str) -> dict:
    path = CLIPLAB_JOBS_DIR / f"{job_id}.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


async def run_ingest_pipeline(
    job_id: str,
    jobs: dict[str, dict],
    user: dict,
    *,
    video_path: str,
    video_id: str,
    vtt_text: str = "",
    fal_key: str = "",
) -> None:
    try:
        jobs[job_id]["status"] = "transcribing"
        jobs[job_id]["progress"] = 15
        tx = await transcribe_video(video_path, video_id, vtt_text=vtt_text, fal_key=fal_key)
        cues = [TranscriptCue(**c) for c in list(load_transcript(video_id).get("cues") or [])]
        jobs[job_id]["status"] = "complete"
        jobs[job_id]["progress"] = 100
        jobs[job_id]["video_id"] = video_id
        jobs[job_id]["transcript_source"] = tx.get("source", "")
        jobs[job_id]["cue_count"] = len(cues)
        save_job_state(job_id, {
            "video_id": video_id,
            "video_path": video_path,
            "user_id": str(user.get("id") or ""),
            "cues": [c.model_dump() for c in cues],
        })
    except Exception as exc:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(exc)


async def run_analyze_pipeline(
    job_id: str,
    jobs: dict[str, dict],
    *,
    video_id: str,
    prompt: str,
    max_segments: int,
    json_completion: JsonCompletionFn,
) -> None:
    try:
        state: dict = {}
        for p in CLIPLAB_JOBS_DIR.glob("clipi_*.json"):
            try:
                row = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue
            if str(row.get("video_id") or "") == video_id:
                state = row
                break
        cues = [TranscriptCue(**c) for c in list(state.get("cues") or load_transcript(video_id).get("cues") or [])]
        if not cues:
            raise RuntimeError("No transcript — wait for ingest to finish or re-upload")
        jobs[job_id]["status"] = "analyzing"
        jobs[job_id]["progress"] = 30
        segments = await rank_segments(cues, prompt, max_segments=max_segments, json_completion=json_completion)
        for seg in segments:
            seg.confidence = judge_segment_confidence(seg, cues)
        jobs[job_id]["status"] = "complete"
        jobs[job_id]["progress"] = 100
        jobs[job_id]["segments"] = [s.model_dump() for s in segments]
        jobs[job_id]["prompt"] = prompt
        save_job_state(job_id, {
            "video_id": video_id,
            "prompt": prompt,
            "segments": [s.model_dump() for s in segments],
            "virality_backend": VIRALITY_BACKEND,
        })
    except Exception as exc:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(exc)


async def run_render_pipeline(
    job_id: str,
    jobs: dict[str, dict],
    *,
    video_id: str,
    analyze_job_id: str,
    segment_indices: list[int],
    burn_captions: bool,
) -> None:
    try:
        ingest_state = {}
        for p in CLIPLAB_JOBS_DIR.glob("*.json"):
            row = json.loads(p.read_text(encoding="utf-8"))
            if str(row.get("video_id") or "") == video_id and row.get("video_path"):
                ingest_state = row
                break
        video_path = str(ingest_state.get("video_path") or "")
        if not video_path or not Path(video_path).exists():
            raise RuntimeError("Source video not found — re-ingest first")

        analyze_state = load_job_state(analyze_job_id) if analyze_job_id else {}
        if not analyze_state:
            analyze_state = load_job_state(job_id.replace("clipr_", "clipa_"))
        segments = [ClipSegment(**s) for s in list(analyze_state.get("segments") or jobs.get(analyze_job_id, {}).get("segments") or [])]
        cues = [TranscriptCue(**c) for c in list(ingest_state.get("cues") or load_transcript(video_id).get("cues") or [])]

        jobs[job_id]["status"] = "rendering"
        jobs[job_id]["progress"] = 20
        rendered = await render_clips_batch(
            video_path, video_id, segments, segment_indices,
            cues=cues, burn_captions=burn_captions, reframe_backend=REFRAME_BACKEND,
        )
        jobs[job_id]["status"] = "complete"
        jobs[job_id]["progress"] = 100
        jobs[job_id]["clips"] = [
            {
                **r,
                "url": f"/api/cliplab/clips/{video_id}/{r['filename']}" if r.get("filename") else "",
            }
            for r in rendered
        ]
        save_job_state(job_id, {"video_id": video_id, "clips": jobs[job_id]["clips"], "reframe_backend": REFRAME_BACKEND})
    except Exception as exc:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(exc)


def new_job_id(prefix: str) -> str:
    return f"{prefix}_{int(time.time())}_{random.randint(1000, 9999)}"
