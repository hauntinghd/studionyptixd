"""ClipLab job orchestration."""
from __future__ import annotations

import asyncio
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
    CLIPLAB_PROVIDER,
    CLIPLAB_RENDER_DIR,
    CLIPLAB_UPLOAD_DIR,
    OPUSCLIP_API_KEY,
    OPUSCLIP_POLL_INTERVAL_SEC,
    OPUSCLIP_POLL_TIMEOUT_SEC,
    REFRAME_BACKEND,
    VIRALITY_BACKEND,
)
from cliplab.intelligence import judge_segment_confidence, rank_segments
from cliplab.models import ClipSegment, TranscriptCue
from cliplab.packaging import build_upload_packages
from cliplab.opus_provider import (
    OpusClipError,
    clip_to_payload,
    create_project as create_opus_project,
    upload_local_file as upload_opus_local_file,
    upload_packages_from_opus,
    wait_for_exportable_clips,
)
from cliplab.render import render_clips_batch, remix_short_video
from cliplab.signals import extract_opus_style_signals
from cliplab.transcribe import load_transcript, transcribe_video
from cliplab.catalyst_bridge import append_learning_event, segment_training_rows

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


def _latest_ingest_state_for_video(video_id: str) -> dict:
    matches = sorted(CLIPLAB_JOBS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    for p in matches:
        try:
            row = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(row.get("video_id") or "") == str(video_id or "") and row.get("video_path"):
            return row
    return {}


def _resolve_provider(requested: str = "auto") -> str:
    value = str(requested or "auto").strip().lower()
    if value not in {"auto", "local", "opus", "hybrid"}:
        value = "auto"
    configured = CLIPLAB_PROVIDER if CLIPLAB_PROVIDER in {"local", "opus", "hybrid"} else "local"
    if value == "auto":
        value = configured
    if value == "opus":
        return "opus"
    if value == "hybrid":
        return "opus" if OPUSCLIP_API_KEY else "local"
    return "local"


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
        payload = {
            "video_id": video_id,
            "video_path": video_path,
            "user_id": str(user.get("id") or ""),
            "status": "complete",
            "type": "cliplab_ingest",
            "progress": 100,
            "transcript_source": tx.get("source", ""),
            "cue_count": len(cues),
            "cues": [c.model_dump() for c in cues],
            "next_action": "analyze_cliplab_video",
        }
        save_job_state(job_id, payload)
        try:
            append_learning_event("cliplab_ingest_complete", {
                "job_id": job_id,
                "video_id": video_id,
                "user_id": str(user.get("id") or ""),
                "transcript_source": tx.get("source", ""),
                "cue_count": len(cues),
                "duration_sec": max((float(c.end) for c in cues), default=0.0),
            })
        except Exception:
            _log.warning("Could not append ClipLab ingest learning event", exc_info=True)
    except Exception as exc:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(exc)
        save_job_state(job_id, {"status": "error", "video_id": video_id, "error": str(exc)})


async def run_opus_pipeline(
    job_id: str,
    jobs: dict[str, dict],
    *,
    video_id: str,
    prompt: str,
    max_segments: int,
    user_id: str = "",
    channel_id: str = "",
    registry_key: str = "",
    source: str = "cliplab_opus",
) -> None:
    try:
        ingest_state = _latest_ingest_state_for_video(video_id)
        video_path = str(ingest_state.get("video_path") or "")
        if not video_path or not Path(video_path).exists():
            raise OpusClipError("Source video not found - re-ingest the ClipLab upload before using OpusClip")

        title = f"ClipLab {registry_key or channel_id or video_id}"
        jobs[job_id]["provider"] = "opusclip"
        jobs[job_id]["status"] = "uploading_to_opusclip"
        jobs[job_id]["stage"] = "uploading_to_opusclip"
        jobs[job_id]["progress"] = 12
        save_job_state(job_id, {
            **jobs[job_id],
            "provider": "opusclip",
            "prompt": prompt,
            "video_id": video_id,
            "source_video_path": video_path,
        })

        upload_id = await upload_opus_local_file(video_path)
        jobs[job_id]["status"] = "creating_opusclip_project"
        jobs[job_id]["stage"] = "creating_opusclip_project"
        jobs[job_id]["progress"] = 28

        project = await create_opus_project(upload_id, title=title, prompt=prompt)
        project_id = str(project.get("project_id") or "")
        if not project_id:
            raise OpusClipError("OpusClip did not return a project id")
        jobs[job_id]["opus_project_id"] = project_id
        jobs[job_id]["status"] = "waiting_for_opusclip_clips"
        jobs[job_id]["stage"] = "waiting_for_opusclip_clips"
        jobs[job_id]["progress"] = 45
        save_job_state(job_id, {
            **jobs[job_id],
            "provider": "opusclip",
            "type": "cliplab_render",
            "prompt": prompt,
            "video_id": video_id,
            "opus_project_id": project_id,
            "opus_upload_id": upload_id,
            "next_action": "poll_opusclip_project",
        })

        tick = 0

        def _on_progress() -> None:
            nonlocal tick
            tick += 1
            jobs[job_id]["progress"] = min(88, 45 + tick * 3)
            jobs[job_id]["stage"] = "waiting_for_opusclip_clips"
            save_job_state(job_id, {
                **jobs[job_id],
                "provider": "opusclip",
                "type": "cliplab_render",
                "prompt": prompt,
                "video_id": video_id,
                "opus_project_id": project_id,
                "message": "OpusClip is still processing exportable clips",
            })

        rows = await wait_for_exportable_clips(
            project_id,
            timeout_sec=OPUSCLIP_POLL_TIMEOUT_SEC,
            interval_sec=OPUSCLIP_POLL_INTERVAL_SEC,
            page_size=max(1, min(int(max_segments or 12), 40)),
            on_progress=_on_progress,
        )
        clips = [clip_to_payload(row, idx) for idx, row in enumerate(rows[: max(1, min(max_segments, 40))])]
        clips = [clip for clip in clips if clip.get("url")]
        if not clips:
            raise OpusClipError("OpusClip returned clips, but none included a preview/export URL")

        upload_packages = upload_packages_from_opus(
            video_id=video_id,
            clips=clips,
            prompt=prompt,
            channel_id=channel_id,
            registry_key=registry_key,
        )
        complete_state = {
            "status": "complete",
            "stage": "complete",
            "type": "cliplab_render",
            "provider": "opusclip",
            "progress": 100,
            "video_id": video_id,
            "prompt": prompt,
            "clips": clips,
            "clip_count": len(clips),
            "upload_packages": upload_packages,
            "upload_package_count": len(upload_packages),
            "opus_project_id": project_id,
            "opus_upload_id": upload_id,
            "user_id": user_id,
            "channel_id": channel_id,
            "registry_key": registry_key,
            "next_action": "Review/download OpusClip clips and publish only the strongest channel-fit packages.",
        }
        jobs[job_id].update(complete_state)
        save_job_state(job_id, complete_state)
        try:
            append_learning_event("cliplab_opus_complete", {
                "job_id": job_id,
                "video_id": video_id,
                "user_id": user_id,
                "channel_id": channel_id,
                "registry_key": registry_key,
                "opus_project_id": project_id,
                "clip_count": len(clips),
                "source": source,
                "prompt": prompt,
            })
        except Exception:
            _log.warning("Could not append ClipLab Opus learning event", exc_info=True)
    except Exception as exc:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["stage"] = "failed"
        jobs[job_id]["error"] = str(exc)
        save_job_state(job_id, {
            "status": "error",
            "stage": "failed",
            "type": "cliplab_render",
            "provider": "opusclip",
            "video_id": video_id,
            "prompt": prompt,
            "error": str(exc),
        })


async def run_analyze_pipeline(
    job_id: str,
    jobs: dict[str, dict],
    *,
    video_id: str,
    prompt: str,
    max_segments: int,
    json_completion: JsonCompletionFn,
    user_id: str = "",
    channel_id: str = "",
    registry_key: str = "",
    source: str = "cliplab",
    provider: str = "auto",
) -> None:
    try:
        selected_provider = _resolve_provider(provider)
        if selected_provider == "opus":
            await run_opus_pipeline(
                job_id,
                jobs,
                video_id=video_id,
                prompt=prompt,
                max_segments=max_segments,
                user_id=user_id,
                channel_id=channel_id,
                registry_key=registry_key,
                source=source,
            )
            return

        state = _latest_ingest_state_for_video(video_id)
        cues = [TranscriptCue(**c) for c in list(state.get("cues") or load_transcript(video_id).get("cues") or [])]
        if not cues:
            raise RuntimeError("No transcript — wait for ingest to finish or re-upload")
        jobs[job_id]["status"] = "analyzing"
        jobs[job_id]["progress"] = 30
        video_path = str(state.get("video_path") or "")
        signals: dict[str, Any] = {}
        if video_path and Path(video_path).exists():
            jobs[job_id]["stage"] = "extracting_visual_audio_signals"
            jobs[job_id]["progress"] = 42
            signals = await asyncio.to_thread(extract_opus_style_signals, video_path, cues)
        jobs[job_id]["stage"] = "ranking_clip_candidates"
        jobs[job_id]["progress"] = 58
        segments = await rank_segments(
            cues,
            prompt,
            max_segments=max_segments,
            json_completion=json_completion,
            signals=signals,
        )
        for seg in segments:
            seg.confidence = judge_segment_confidence(seg, cues)
        jobs[job_id]["status"] = "complete"
        jobs[job_id]["progress"] = 100
        jobs[job_id]["stage"] = "complete"
        jobs[job_id]["segments"] = [s.model_dump() for s in segments]
        jobs[job_id]["prompt"] = prompt
        signal_summary = {
            "duration_sec": signals.get("duration_sec"),
            "visual_sample_count": signals.get("visual_sample_count", 0),
            "audio_sample_count": signals.get("audio_sample_count", 0),
        }
        segment_rows = segment_training_rows(
            user_id=user_id,
            video_id=video_id,
            prompt=prompt,
            segments=[s.model_dump() for s in segments],
            source=source,
            channel_id=channel_id,
            registry_key=registry_key,
            analyze_job_id=job_id,
        )
        save_job_state(job_id, {
            "status": "complete",
            "type": "cliplab_analyze",
            "progress": 100,
            "video_id": video_id,
            "prompt": prompt,
            "segments": [s.model_dump() for s in segments],
            "signal_summary": signal_summary,
            "provider": "local",
            "virality_backend": VIRALITY_BACKEND,
            "user_id": user_id,
            "channel_id": channel_id,
            "registry_key": registry_key,
            "learning_rows": segment_rows,
            "next_action": "render_cliplab_segments",
        })
        try:
            append_learning_event("cliplab_analyze_complete", {
                "job_id": job_id,
                "video_id": video_id,
                "user_id": user_id,
                "channel_id": channel_id,
                "registry_key": registry_key,
                "prompt": prompt,
                "segment_count": len(segments),
                "top_score": max((float(s.virality_score or 0) for s in segments), default=0.0),
                "signal_summary": signal_summary,
                "source": source,
            })
            for row in segment_rows:
                append_learning_event("cliplab_segment_candidate", row, dataset="cliplab_feedback.jsonl")
        except Exception:
            _log.warning("Could not append ClipLab analyze learning event", exc_info=True)
    except Exception as exc:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(exc)
        save_job_state(job_id, {"status": "error", "video_id": video_id, "prompt": prompt, "error": str(exc)})


async def run_render_pipeline(
    job_id: str,
    jobs: dict[str, dict],
    *,
    video_id: str,
    analyze_job_id: str,
    segment_indices: list[int],
    burn_captions: bool,
    user_id: str = "",
    channel_id: str = "",
    registry_key: str = "",
    source: str = "cliplab",
) -> None:
    try:
        ingest_state = _latest_ingest_state_for_video(video_id)
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
        segment_payloads = [s.model_dump() for s in segments]
        clips = [
            {
                **r,
                "url": f"/api/cliplab/clips/{video_id}/{r['filename']}" if r.get("filename") else "",
            }
            for r in rendered
        ]
        upload_packages = build_upload_packages(
            video_id=video_id,
            rendered=clips,
            segments=segment_payloads,
            prompt=str(analyze_state.get("prompt") or ""),
            channel_id=channel_id,
            registry_key=registry_key,
        )
        jobs[job_id]["status"] = "complete"
        jobs[job_id]["progress"] = 100
        jobs[job_id]["clips"] = clips
        jobs[job_id]["upload_packages"] = upload_packages
        save_job_state(job_id, {
            "status": "complete",
            "type": "cliplab_render",
            "progress": 100,
            "video_id": video_id,
            "clips": jobs[job_id]["clips"],
            "upload_packages": upload_packages,
            "reframe_backend": REFRAME_BACKEND,
            "analyze_job_id": analyze_job_id,
            "segment_indices": segment_indices,
            "user_id": user_id,
            "channel_id": channel_id,
            "registry_key": registry_key,
        })
        try:
            append_learning_event("cliplab_render_complete", {
                "job_id": job_id,
                "video_id": video_id,
                "user_id": user_id,
                "channel_id": channel_id,
                "registry_key": registry_key,
                "analyze_job_id": analyze_job_id,
                "segment_indices": segment_indices,
                "clip_count": len(jobs[job_id]["clips"]),
                "upload_package_count": len(upload_packages),
                "burn_captions": bool(burn_captions),
                "reframe_backend": REFRAME_BACKEND,
                "source": source,
            })
        except Exception:
            _log.warning("Could not append ClipLab render learning event", exc_info=True)
    except Exception as exc:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(exc)
        save_job_state(job_id, {"status": "error", "video_id": video_id, "error": str(exc)})


async def run_remix_pipeline(
    job_id: str,
    jobs: dict[str, dict],
    *,
    video_id: str,
    style_preset: str,
    caption_style: str,
    edit_intensity: str,
    background_mode: str,
    burn_captions: bool,
    catalyst_channel_id: str = "",
    notes: str = "",
) -> None:
    try:
        ingest_state = _latest_ingest_state_for_video(video_id)
        video_path = str(ingest_state.get("video_path") or "")
        if not video_path or not Path(video_path).exists():
            raise RuntimeError("Source video not found - upload the short before remixing")
        cues = [TranscriptCue(**c) for c in list(ingest_state.get("cues") or load_transcript(video_id).get("cues") or [])]

        jobs[job_id]["status"] = "remixing"
        jobs[job_id]["progress"] = 18
        jobs[job_id]["stage"] = "format_and_polish"
        out_dir = CLIPLAB_RENDER_DIR / video_id
        out_dir.mkdir(parents=True, exist_ok=True)
        fname = f"remix_{job_id}.mp4"
        rendered = await remix_short_video(
            video_path,
            video_id,
            str(out_dir / fname),
            cues=cues,
            style_preset=style_preset,
            caption_style=caption_style,
            edit_intensity=edit_intensity,
            background_mode=background_mode,
            burn_captions=burn_captions,
        )
        jobs[job_id]["status"] = "complete"
        jobs[job_id]["progress"] = 100
        jobs[job_id]["stage"] = "complete"
        jobs[job_id]["remix"] = {**rendered, "url": f"/api/cliplab/clips/{video_id}/{fname}"}
        catalyst_record = {
            "mode": "remix_lab_v1",
            "job_id": job_id,
            "video_id": video_id,
            "user_id": str(jobs[job_id].get("user_id") or ""),
            "catalyst_channel_id": str(catalyst_channel_id or ""),
            "style_preset": style_preset,
            "caption_style": caption_style,
            "edit_intensity": edit_intensity,
            "background_mode": background_mode,
            "burn_captions": bool(burn_captions),
            "notes": str(notes or "")[:500],
            "source_video_path": video_path,
            "output_filename": fname,
            "caption_cues": len(cues),
            "created_at": time.time(),
            "learning_status": "awaiting_user_feedback_or_youtube_outcome",
        }
        jobs[job_id]["catalyst_remix_record"] = catalyst_record
        save_job_state(job_id, {
            "video_id": video_id,
            "remix": jobs[job_id]["remix"],
            "catalyst_remix_record": catalyst_record,
        })
        try:
            append_learning_event("cliplab_remix_complete", catalyst_record, dataset="remix_lab_learning.jsonl")
        except Exception:
            _log.warning("Could not append Remix Lab learning record", exc_info=True)
    except Exception as exc:
        jobs[job_id]["status"] = "error"
        jobs[job_id]["stage"] = "failed"
        jobs[job_id]["error"] = str(exc)


def new_job_id(prefix: str) -> str:
    return f"{prefix}_{int(time.time())}_{random.randint(1000, 9999)}"
