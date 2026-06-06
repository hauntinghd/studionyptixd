"""Unified production job status for Studio Agent UI polling."""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from studio_agent import telemetry

ROOT = Path(__file__).resolve().parents[1]
SKELETON_OUTPUT = Path(os.getenv("SKELETON_AI_OUTPUT_ROOT", "skeleton_ai/output"))
SKELETON_OUTPUT.mkdir(parents=True, exist_ok=True)

# Shortform jobs with no progress.json / heartbeat update for this long are marked failed.
# Heartbeat is written by a sidecar thread during long blocking ops (fal subscribe etc.)
# so that slow-but-alive i2v / still renders don't get falsely timed out.
SHORTFORM_STALE_SEC = int(os.getenv("SHORTFORM_JOB_STALE_SEC", "14400"))  # 4 hours default (was 2h)

JOB_START_TOOLS = frozenset({
    "start_longform_render",
    "start_shortform_generate",
    "analyze_reference_video",
    "finalize_longform_render",
})

LONGFORM_PHASE_LABELS: dict[str, str] = {
    "starting": "Starting pipeline",
    "chapters": "Writing chapters",
    "scenes": "Generating scene stills",
    "awaiting_approval": "Awaiting your approval",
    "narration": "Generating voiceover",
    "ambient": "Building ambience",
    "thumbnails": "Packaging thumbnails",
    "compose": "Compositing final MP4",
    "done": "Complete",
    "failed": "Failed",
}


def extract_jobs_from_tool(tool_name: str, result_text: str) -> list[dict[str, Any]]:
    """Parse tool JSON output for trackable background jobs."""
    try:
        data = json.loads(result_text or "{}")
    except json.JSONDecodeError:
        return []
    if not isinstance(data, dict):
        return []
    job_id = str(data.get("job_id") or "").strip()
    if not job_id:
        return []
    if tool_name == "start_longform_render":
        kind = "longform"
        title = str(data.get("outline_title") or data.get("channel_key") or "Long-form render")
    elif tool_name == "start_shortform_generate":
        kind = "shortform"
        title = str(data.get("topic") or data.get("category_key") or "Short-form render")
    elif tool_name == "analyze_reference_video":
        kind = "competitor"
        title = "Reference video analysis"
    elif tool_name == "finalize_longform_render":
        kind = "longform"
        title = "Long-form finalize"
    else:
        return []
    return [{
        "job_id": job_id,
        "kind": kind,
        "title": title[:120],
        "started_at": time.time(),
    }]


def merge_active_jobs(existing: list[dict[str, Any]], new_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id = {str(j.get("job_id") or ""): j for j in existing if j.get("job_id")}
    for item in new_items:
        jid = str(item.get("job_id") or "")
        if jid:
            by_id[jid] = {**by_id.get(jid, {}), **item}
    return list(by_id.values())


def _longform_status(job_id: str) -> dict[str, Any]:
    from long_form import pipeline as lf_pipeline

    live = lf_pipeline.get_status(job_id) or {}
    st = lf_pipeline.load_state(job_id) or {}
    phase = str(live.get("phase") or st.get("phase") or "unknown")
    percent = int(live.get("percent") or st.get("percent") or 0)
    error = str(live.get("error") or st.get("error") or "")
    mp4_path = st.get("mp4_path")
    running = phase not in ("done", "failed", "awaiting_approval")
    status = "complete" if phase == "done" else "failed" if phase == "failed" else (
        "awaiting_approval" if phase == "awaiting_approval" else "running"
    )
    snap: dict[str, Any] = {
        "job_id": job_id,
        "kind": "longform",
        "status": status,
        "progress": max(0, min(100, percent)),
        "stage": phase,
        "stage_label": LONGFORM_PHASE_LABELS.get(phase, phase.replace("_", " ").title()),
        "error": error or None,
        "current_scene": int(live.get("scene_done") or 0),
        "total_scenes": int(live.get("scene_total") or 0),
        "current_chapter": int(live.get("chapter_done") or 0),
        "total_chapters": int(live.get("chapter_total") or 0),
        "title": str(st.get("outline", {}).get("title") or st.get("channel_key") or "Long-form"),
        "running": running,
    }
    scenes_gen = int(st.get("scenes_generated") or live.get("scene_total") or 0)
    if phase == "awaiting_approval" and scenes_gen > 0:
        snap["can_finalize"] = True
        snap["still_count"] = scenes_gen
        snap["still_preview_urls"] = [
            f"/api/studio-agent/jobs/{job_id}/still/{i}"
            for i in range(min(scenes_gen, 12))
        ]
    if phase == "done" and mp4_path:
        snap["mp4_url"] = f"/api/studio-agent/jobs/{job_id}/media?kind=longform"
        snap["download_url"] = snap["mp4_url"]
    thumbs = int(st.get("thumbnails_generated") or 0)
    if thumbs > 0:
        snap["preview_url"] = f"/api/studio-agent/jobs/{job_id}/thumbnail/1"
    return snap


def finalize_longform_job(job_id: str) -> dict[str, Any]:
    from long_form import pipeline as lf_pipeline

    lf_pipeline.start_finalize(job_id)
    return {
        "job_id": job_id,
        "kind": "longform",
        "status": "running",
        "stage": "narration",
        "message": "Finalize started — voice, sound, and MP4 export running.",
    }


def resolve_still_path(job_id: str, scene_idx: int) -> Path | None:
    if not job_id.replace("_", "").isalnum() or len(job_id) > 48:
        return None
    from long_form import pipeline as lf_pipeline

    path = lf_pipeline.job_still_path(job_id, scene_idx)
    return path if path and path.exists() else None


def _write_shortform_result(workspace: Path, *, status: str, error: str = "", **extra: Any) -> None:
    workspace.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {"status": status, "error": error or None, **extra}
    (workspace / "result.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _shortform_failed_snap(job_id: str, error: str, *, progress: int = 0) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "kind": "shortform",
        "status": "failed",
        "progress": progress,
        "stage": "failed",
        "stage_label": "Failed",
        "stage_detail": error[:240],
        "error": error,
        "running": False,
        "title": "Short-form video",
    }


def _shortform_status(job_id: str) -> dict[str, Any]:
    workspace = (ROOT / SKELETON_OUTPUT / job_id).resolve()
    result_path = workspace / "result.json"
    progress_path = workspace / "progress.json"
    spec_path = workspace / "job_spec.json"

    if not workspace.is_dir() and not result_path.is_file():
        err = (
            "Production workspace was lost (likely server restart). "
            "Tap Retry in the render dock to run again."
        )
        return _shortform_failed_snap(job_id, err)

    if not result_path.is_file():
        last_touch = 0.0
        for p in (progress_path, spec_path, workspace / "script.txt", workspace / "heartbeat.txt"):
            if p.is_file():
                last_touch = max(last_touch, p.stat().st_mtime)
        if last_touch and (time.time() - last_touch) > SHORTFORM_STALE_SEC:
            # Include diagnostic info so user (and future training data) can see why it looked dead.
            ages = {}
            for p in (progress_path, spec_path, workspace / "script.txt", workspace / "heartbeat.txt"):
                if p.is_file():
                    ages[p.name] = int(time.time() - p.stat().st_mtime)
            err = (
                f"No progress for {SHORTFORM_STALE_SEC // 3600}+ hours — "
                "production timed out. Tap Retry to run again."
                f" (last file ages: {ages})"
            )
            _write_shortform_result(workspace, status="failed", error=err, job_id=job_id)
            return _shortform_failed_snap(job_id, err)
        progress = 12
        stage = "pipeline"
        stage_label = "Building short"
        stage_detail = "Script, stills, motion, and composite running server-side."
        if progress_path.is_file():
            try:
                prog = json.loads(progress_path.read_text(encoding="utf-8"))
                if isinstance(prog, dict):
                    progress = int(prog.get("progress") or progress)
                    stage = str(prog.get("stage") or stage)
                    stage_label = stage.replace("_", " ").title()
                    stage_detail = str(prog.get("detail") or stage_detail)
            except Exception:
                pass
        return {
            "job_id": job_id,
            "kind": "shortform",
            "status": "running",
            "progress": progress,
            "stage": stage,
            "stage_label": stage_label,
            "stage_detail": stage_detail,
            "running": True,
            "title": "Short-form video",
        }
    try:
        data = json.loads(result_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "job_id": job_id,
            "kind": "shortform",
            "status": "failed",
            "progress": 0,
            "stage": "error",
            "stage_label": "Failed",
            "error": str(exc)[:200],
            "running": False,
        }
    st = str(data.get("status") or "").lower()
    cancelled = st == "cancelled"
    complete = st == "complete"
    terminal_fail = st == "failed" or cancelled
    snap: dict[str, Any] = {
        "job_id": job_id,
        "kind": "shortform",
        "status": "complete" if complete else "failed" if terminal_fail else "running",
        "progress": 100 if complete else 0 if terminal_fail else 55,
        "stage": st or "running",
        "stage_label": "Complete" if complete else ("Cancelled" if cancelled else "Failed") if terminal_fail else "Rendering",
        "error": ("Cancelled by user" if cancelled else data.get("error")),
        "running": not complete and not terminal_fail,
        "title": str(data.get("topic") or data.get("category") or "Short-form"),
    }
    if complete:
        snap["mp4_url"] = f"/api/studio-agent/jobs/{job_id}/media?kind=shortform"
        snap["download_url"] = snap["mp4_url"]
    return snap


def _competitor_status(job_id: str) -> dict[str, Any]:
    from studio_agent import competitor

    raw = competitor.read_status(job_id)
    stage = str(raw.get("status") or raw.get("stage") or "queued")
    pct = int(raw.get("percent") or competitor._stage_percent(stage) or 0)
    complete = stage == "complete"
    failed = stage in ("error", "failed")
    pacing = raw.get("pacing") if isinstance(raw.get("pacing"), dict) else {}
    meta = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
    engagement = raw.get("engagement") if isinstance(raw.get("engagement"), dict) else {}
    frames = raw.get("frames") if isinstance(raw.get("frames"), dict) else {}
    snap: dict[str, Any] = {
        "job_id": job_id,
        "kind": "competitor",
        "status": "complete" if complete else "failed" if failed else "running",
        "progress": max(0, min(100, pct)),
        "stage": stage,
        "stage_label": stage.replace("_", " ").title(),
        "stage_detail": str(raw.get("note") or raw.get("message") or ""),
        "error": raw.get("error"),
        "running": not complete and not failed,
        "title": str(meta.get("title") or "Reference analysis")[:120],
        "analysis_ready": complete,
    }
    if pacing:
        snap["pacing"] = {
            "avg_shot_sec": pacing.get("avg_shot_sec"),
            "cut_count": pacing.get("cut_count"),
            "duration_sec": pacing.get("duration_sec"),
            "hook_window_sec": pacing.get("hook_window_sec"),
        }
    if engagement:
        snap["engagement"] = engagement
    if complete:
        snap["frame_count"] = int(frames.get("count") or 0)
        note = str(raw.get("style_reference_note") or "").strip()
        steps = raw.get("next_steps")
        if note:
            snap["blueprint_hint"] = note
        elif isinstance(steps, list) and steps:
            snap["blueprint_hint"] = str(steps[0])[:400]
    return snap


def prune_session_job(session_id: str, job_id: str, *, user_id: str | None = None) -> None:
    """Remove a terminal job from session active_jobs so the UI stops polling."""
    from studio_agent import store

    session = store.get_session(session_id, user_id=user_id)
    if not session:
        return
    jobs = list(session.get("active_jobs") or [])
    filtered = [j for j in jobs if str(j.get("job_id") or "") != job_id]
    if len(filtered) != len(jobs):
        store.update_session(session_id, active_jobs=filtered)


def get_job_snapshot(job_id: str, kind: str) -> dict[str, Any]:
    kind = str(kind or "longform").strip().lower()
    if kind == "shortform":
        snap = _shortform_status(job_id)
    elif kind == "competitor":
        snap = _competitor_status(job_id)
    else:
        snap = _longform_status(job_id)
    snap["polled_at"] = time.time()
    return snap


def resolve_media_path(job_id: str, kind: str) -> Path | None:
    kind = str(kind or "").strip().lower()
    if not job_id.replace("_", "").isalnum() or len(job_id) > 48:
        return None
    if kind == "shortform":
        ws = (ROOT / SKELETON_OUTPUT / job_id).resolve()
        # Support both pipelines: skeleton uses skeleton_short.mp4, styled (cinematic etc) uses styled_short.mp4
        for candidate in ("skeleton_short.mp4", "styled_short.mp4"):
            p = (ws / candidate).resolve()
            if p.is_file():
                return p
        return None
    if kind == "longform":
        from long_form import pipeline as lf_pipeline

        path = lf_pipeline.job_mp4_path(job_id)
        return path if path and path.exists() else None
    return None


def record_production_complete_telemetry(
    user_id: str,
    snap: dict[str, Any],
    *,
    session_id: str | None = None,
) -> None:
    """Once per job — training/product signal without selling user data."""
    jid = str(snap.get("job_id") or "")
    kind = str(snap.get("kind") or "")
    if not jid or snap.get("status") != "complete":
        return
    marker_dir = ROOT / "data" / "studio_agent_job_markers"
    if os.environ.get("APP_DATA_DIR"):
        marker_dir = Path(os.environ["APP_DATA_DIR"]) / "studio_agent_job_markers"
    marker_dir.mkdir(parents=True, exist_ok=True)
    marker = marker_dir / f"{user_id}_{kind}_{jid}.done"
    if marker.exists():
        return
    try:
        marker.write_text(str(time.time()), encoding="utf-8")
    except Exception:
        pass
    telemetry.record_event(
        user_id,
        "production_complete",
        {
            "job_id": jid,
            "kind": kind,
            "progress": snap.get("progress"),
            "title": (snap.get("title") or "")[:200],
            "has_mp4": bool(snap.get("mp4_url")),
        },
        session_id=session_id,
    )
