"""ClipLab job orchestration."""
from __future__ import annotations

import json
import logging
import random
import re
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Awaitable

from cliplab.config import (
    CLIPLAB_CREDITS_PER_MINUTE,
    CLIPLAB_JOBS_DIR,
    CLIPLAB_MIN_CREDITS,
    CLIPLAB_RENDER_DIR,
    CLIPLAB_UPLOAD_DIR,
    REFRAME_BACKEND,
    VIRALITY_BACKEND,
)
from cliplab.intelligence import judge_segment_confidence, rank_segments
from cliplab.models import ClipSegment, TranscriptCue
from cliplab.render import render_clips_batch, render_remix_short
from cliplab.transcribe import load_transcript, transcribe_video

_log = logging.getLogger("nyptid-studio.cliplab.pipeline")

JsonCompletionFn = Callable[..., Awaitable[dict]]
_SAFE_JOB_ID = re.compile(r"^[A-Za-z0-9_-]{1,96}$")
_SAFE_VIDEO_ID = re.compile(r"^[A-Za-z0-9_-]{1,96}$")


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


def _job_state_path(job_id: str) -> Path:
    jid = str(job_id or "").strip()
    if not _SAFE_JOB_ID.fullmatch(jid):
        raise ValueError("invalid ClipLab job id")
    return CLIPLAB_JOBS_DIR / f"{jid}.json"


def save_job_state(job_id: str, payload: dict) -> None:
    """Atomically persist a complete ClipLab job snapshot."""
    path = _job_state_path(job_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_text(json.dumps(dict(payload or {}), indent=2), encoding="utf-8")
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


def load_job_state(job_id: str) -> dict:
    try:
        path = _job_state_path(job_id)
    except ValueError:
        return {}
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _persist_job_update(job_id: str, jobs: dict[str, dict], **updates: Any) -> dict:
    """Merge updates into both in-memory and durable state without dropping ownership."""
    memory = dict(jobs.get(job_id) or {})
    durable = load_job_state(job_id)
    merged = {**durable, **memory, **updates, "updated_at": time.time()}
    jobs.setdefault(job_id, {}).update(updates)
    jobs[job_id]["updated_at"] = merged["updated_at"]
    save_job_state(job_id, merged)
    return merged


def _iter_job_states() -> list[tuple[str, dict]]:
    rows: list[tuple[float, str, dict]] = []
    for path in CLIPLAB_JOBS_DIR.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                continue
            rows.append((path.stat().st_mtime, path.stem, payload))
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            continue
    rows.sort(key=lambda row: row[0], reverse=True)
    return [(job_id, payload) for _, job_id, payload in rows]


def find_ingest_state(video_id: str, *, user_id: str = "") -> tuple[str, dict]:
    """Return the newest owner-scoped ingest state for a ClipLab video."""
    vid = str(video_id or "").strip()
    uid = str(user_id or "").strip()
    if not _SAFE_VIDEO_ID.fullmatch(vid):
        return "", {}
    for job_id, payload in _iter_job_states():
        if str(payload.get("video_id") or "").strip() != vid:
            continue
        if not str(payload.get("video_path") or "").strip():
            continue
        if uid and str(payload.get("user_id") or "").strip() != uid:
            continue
        return job_id, payload
    return "", {}


def user_owns_video(video_id: str, user_id: str) -> bool:
    uid = str(user_id or "").strip()
    return bool(uid and find_ingest_state(video_id, user_id=uid)[1])


def resolve_owned_clip_path(video_id: str, filename: str, user_id: str) -> Path | None:
    """Resolve only a rendered artifact recorded in the authenticated owner's state."""
    vid = str(video_id or "").strip()
    name = str(filename or "").strip()
    uid = str(user_id or "").strip()
    if not uid or not _SAFE_VIDEO_ID.fullmatch(vid) or Path(name).name != name:
        return None
    base = (CLIPLAB_RENDER_DIR / vid).resolve()
    for _, payload in _iter_job_states():
        if str(payload.get("video_id") or "").strip() != vid:
            continue
        if str(payload.get("user_id") or "").strip() != uid:
            continue
        artifacts = [row for row in list(payload.get("clips") or []) if isinstance(row, dict)]
        remix = payload.get("remix")
        if isinstance(remix, dict):
            artifacts.append(remix)
        for artifact in artifacts:
            if str(artifact.get("filename") or "").strip() != name:
                continue
            candidates = [Path(str(artifact.get("path") or base / name)).resolve(), (base / name).resolve()]
            for candidate in candidates:
                try:
                    candidate.relative_to(base)
                except ValueError:
                    continue
                if candidate.is_file():
                    return candidate
    return None


def _owner_id(job_id: str, jobs: dict[str, dict], explicit_user_id: str = "") -> str:
    return str(
        explicit_user_id
        or (jobs.get(job_id) or {}).get("user_id")
        or load_job_state(job_id).get("user_id")
        or ""
    ).strip()


def _response_content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("text"):
                parts.append(str(item.get("text")))
            elif item is not None:
                parts.append(str(item))
        return "\n".join(parts)
    return str(content or "")


def _parse_json_object(text: str) -> dict:
    raw = str(text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```[A-Za-z0-9_-]*\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw).strip()
    start = raw.find("{")
    end = raw.rfind("}") + 1
    if start < 0 or end <= start:
        raise ValueError("ClipLab analysis model returned no JSON object")
    parsed = json.loads(raw[start:end])
    if not isinstance(parsed, dict):
        raise ValueError("ClipLab analysis model returned a non-object JSON value")
    return parsed


async def _studio_json_completion(
    system_prompt: str,
    user_prompt: str,
    *,
    temperature: float = 0.35,
    timeout_sec: int = 120,
    model: str = "",
) -> dict:
    """Use Studio's model-agnostic chat router when no legacy callback is supplied."""
    del timeout_sec  # Studio's provider router owns its per-model timeout policy.
    from studio_agent import openrouter

    response = await openrouter.chat_completion(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        model=str(model or "").strip() or None,
        temperature=float(temperature),
        reasoning_depth="balanced",
    )
    message = openrouter.message_from_response(response)
    return _parse_json_object(_response_content_text(message.get("content")))


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
        user_id = str((user or {}).get("id") or "").strip()
        if not user_id:
            raise RuntimeError("ClipLab job owner is required")
        _persist_job_update(
            job_id,
            jobs,
            status="transcribing",
            progress=15,
            video_id=video_id,
            video_path=video_path,
            user_id=user_id,
        )
        tx = await transcribe_video(video_path, video_id, vtt_text=vtt_text, fal_key=fal_key)
        cues = [TranscriptCue(**c) for c in list(load_transcript(video_id).get("cues") or [])]
        _persist_job_update(
            job_id,
            jobs,
            status="complete",
            progress=100,
            video_id=video_id,
            video_path=video_path,
            user_id=user_id,
            transcript_source=tx.get("source", ""),
            cue_count=len(cues),
            cues=[c.model_dump() for c in cues],
        )
    except Exception as exc:
        _persist_job_update(job_id, jobs, status="error", error=str(exc), progress=100)


async def run_analyze_pipeline(
    job_id: str,
    jobs: dict[str, dict],
    *,
    video_id: str,
    prompt: str,
    max_segments: int,
    json_completion: JsonCompletionFn | None,
    user_id: str = "",
    channel_id: str = "",
    registry_key: str = "",
    source: str = "",
    provider: str = "auto",
    model: str = "",
) -> None:
    try:
        owner_id = _owner_id(job_id, jobs, user_id)
        if not owner_id:
            raise RuntimeError("ClipLab job owner is required")
        requested_provider = str(provider or "auto").strip().lower() or "auto"
        if requested_provider not in {"auto", "local"}:
            raise RuntimeError(
                f"ClipLab provider '{requested_provider}' is not implemented in this release; use provider='local'."
            )
        ingest_job_id, state = find_ingest_state(video_id, user_id=owner_id)
        if not state:
            raise RuntimeError("Source video not found for this user - re-ingest first")
        cues = [TranscriptCue(**c) for c in list(state.get("cues") or load_transcript(video_id).get("cues") or [])]
        if not cues:
            raise RuntimeError("No transcript - wait for ingest to finish or re-upload")
        _persist_job_update(
            job_id,
            jobs,
            status="analyzing",
            progress=30,
            video_id=video_id,
            user_id=owner_id,
            channel_id=str(channel_id or "").strip(),
            registry_key=str(registry_key or "").strip(),
            source=str(source or "").strip(),
            provider=requested_provider,
            provider_used="local",
            model=str(model or "").strip(),
            source_ingest_job_id=ingest_job_id,
            prompt=prompt,
        )
        completion_errors: list[Exception] = []
        supplied_completion = json_completion

        async def completion(system_prompt: str, user_prompt: str, **kwargs: Any) -> dict:
            try:
                if supplied_completion is not None:
                    return await supplied_completion(system_prompt, user_prompt, **kwargs)
                return await _studio_json_completion(
                    system_prompt,
                    user_prompt,
                    model=str(model or "").strip(),
                    **kwargs,
                )
            except Exception as exc:
                completion_errors.append(exc)
                raise

        segments = await rank_segments(cues, prompt, max_segments=max_segments, json_completion=completion)
        if completion_errors:
            raise RuntimeError(f"ClipLab analysis model failed: {completion_errors[-1]}")
        for seg in segments:
            seg.confidence = judge_segment_confidence(seg, cues)
        _persist_job_update(
            job_id,
            jobs,
            status="complete",
            progress=100,
            segments=[s.model_dump() for s in segments],
            segment_count=len(segments),
            prompt=prompt,
            virality_backend=VIRALITY_BACKEND,
        )
    except Exception as exc:
        _persist_job_update(job_id, jobs, status="error", error=str(exc), progress=100)


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
    source: str = "",
) -> None:
    try:
        owner_id = _owner_id(job_id, jobs, user_id)
        if not owner_id:
            raise RuntimeError("ClipLab job owner is required")
        ingest_job_id, ingest_state = find_ingest_state(video_id, user_id=owner_id)
        video_path = str(ingest_state.get("video_path") or "")
        if not video_path or not Path(video_path).exists():
            raise RuntimeError("Source video not found for this user - re-ingest first")

        analyze_state = load_job_state(analyze_job_id) if analyze_job_id else {}
        if not analyze_state:
            raise RuntimeError("Analyze job not found")
        if str(analyze_state.get("video_id") or "").strip() != str(video_id or "").strip():
            raise RuntimeError("Analyze job does not belong to this video")
        if owner_id and str(analyze_state.get("user_id") or "").strip() != owner_id:
            raise RuntimeError("Analyze job not found")
        segments = [ClipSegment(**s) for s in list(analyze_state.get("segments") or [])]
        if not segments:
            raise RuntimeError("Analyze job has no segments to render")
        indices = list(dict.fromkeys(int(idx) for idx in segment_indices))
        if not indices or any(idx < 0 or idx >= len(segments) for idx in indices):
            raise RuntimeError("One or more segment indices are outside the analyzed segment list")
        cues = [TranscriptCue(**c) for c in list(ingest_state.get("cues") or load_transcript(video_id).get("cues") or [])]

        _persist_job_update(
            job_id,
            jobs,
            status="rendering",
            progress=20,
            video_id=video_id,
            user_id=owner_id,
            channel_id=str(channel_id or "").strip(),
            registry_key=str(registry_key or "").strip(),
            source=str(source or "").strip(),
            source_ingest_job_id=ingest_job_id,
            analyze_job_id=analyze_job_id,
            segment_indices=indices,
        )
        rendered = await render_clips_batch(
            video_path, video_id, segments, indices,
            cues=cues, burn_captions=burn_captions, reframe_backend=REFRAME_BACKEND,
        )
        clips = [
            {
                **r,
                "url": f"/api/cliplab/clips/{video_id}/{r['filename']}" if r.get("filename") else "",
            }
            for r in rendered
        ]
        if not any(row.get("filename") and not row.get("error") for row in clips):
            detail = "; ".join(str(row.get("error") or "render failed") for row in clips[:3])
            raise RuntimeError(f"No selected ClipLab segment rendered successfully: {detail}")
        _persist_job_update(
            job_id,
            jobs,
            status="complete",
            progress=100,
            clips=clips,
            clip_count=sum(1 for row in clips if row.get("filename") and not row.get("error")),
            reframe_backend=REFRAME_BACKEND,
        )
    except Exception as exc:
        _persist_job_update(job_id, jobs, status="error", error=str(exc), progress=100)


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
    user_id: str = "",
    registry_key: str = "",
    source: str = "",
) -> None:
    """Polish an ingested short and persist an owner-scoped delivery artifact."""
    try:
        owner_id = _owner_id(job_id, jobs, user_id)
        if not owner_id:
            raise RuntimeError("ClipLab job owner is required")
        ingest_job_id, ingest_state = find_ingest_state(video_id, user_id=owner_id)
        video_path = str(ingest_state.get("video_path") or "")
        if not video_path or not Path(video_path).is_file():
            raise RuntimeError("Source video not found for this user - re-ingest first")
        cues = [
            TranscriptCue(**cue)
            for cue in list(ingest_state.get("cues") or load_transcript(video_id).get("cues") or [])
        ]
        _persist_job_update(
            job_id,
            jobs,
            status="rendering",
            progress=20,
            video_id=video_id,
            user_id=owner_id,
            channel_id=str(catalyst_channel_id or "").strip(),
            registry_key=str(registry_key or "").strip(),
            source=str(source or "").strip(),
            source_ingest_job_id=ingest_job_id,
            style_preset=style_preset,
            caption_style=caption_style,
            edit_intensity=edit_intensity,
            background_mode=background_mode,
            burn_captions=bool(burn_captions),
            notes=str(notes or "")[:500],
        )
        remix = await render_remix_short(
            video_path,
            video_id,
            job_id=job_id,
            cues=cues,
            style_preset=style_preset,
            caption_style=caption_style,
            edit_intensity=edit_intensity,
            background_mode=background_mode,
            burn_captions=burn_captions,
        )
        if not remix.get("filename") or not Path(str(remix.get("path") or "")).is_file():
            raise RuntimeError("ClipLab remix renderer returned no playable output")
        remix = {
            **remix,
            "url": f"/api/cliplab/clips/{video_id}/{remix['filename']}",
        }
        _persist_job_update(
            job_id,
            jobs,
            status="complete",
            progress=100,
            remix=remix,
        )
    except Exception as exc:
        _persist_job_update(job_id, jobs, status="error", error=str(exc), progress=100)


def new_job_id(prefix: str) -> str:
    return f"{prefix}_{int(time.time())}_{random.randint(1000, 9999)}"
