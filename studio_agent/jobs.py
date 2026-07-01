"""Unified production job status for Studio Agent UI polling."""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any

from studio_agent import production_budget, telemetry

ROOT = Path(__file__).resolve().parents[1]
SKELETON_OUTPUT = Path(os.getenv("SKELETON_AI_OUTPUT_ROOT", "skeleton_ai/output"))
SKELETON_OUTPUT.mkdir(parents=True, exist_ok=True)

# Shortform jobs with no progress.json / heartbeat update for this long are marked failed.
# Heartbeat is written by a sidecar thread during long blocking ops (fal subscribe etc.)
# so that slow-but-alive i2v / still renders don't get falsely timed out.
SHORTFORM_STALE_SEC = int(os.getenv("SHORTFORM_JOB_STALE_SEC", "900"))  # 15 min before surfacing failure
SHORTFORM_RECLAIM_SEC = int(os.getenv("SHORTFORM_JOB_RECLAIM_SEC", "180"))  # resume after deploy/restart
SHORTFORM_RECLAIM_MARKER_SEC = int(os.getenv("SHORTFORM_JOB_RECLAIM_MARKER_SEC", "240"))

JOB_START_TOOLS = frozenset({
    "start_longform_render",
    "start_shortform_generate",
    "analyze_reference_video",
    "finalize_production",
    "finalize_longform_render",
    "re_edit_production",
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
    elif tool_name in {"finalize_production", "re_edit_production"}:
        kind = "shortform"
        title = "Short-form re-edit" if tool_name == "re_edit_production" else "Short-form finalize"
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
        _attach_production_control(
            snap,
            "start_longform_render",
            job_id=job_id,
            awaiting_user_approval=True,
            next_action=(
                "Review chapter stills, edit or regenerate weak frames, then approve "
                "the long-form render for final composition."
            ),
        )
    elif running:
        _attach_production_control(
            snap,
            "start_longform_render",
            job_id=job_id,
            next_action="Wait for the long-form production stage to update or complete.",
        )
    if phase == "done" and mp4_path:
        snap["mp4_url"] = f"/api/studio-agent/jobs/{job_id}/media?kind=longform"
        snap["download_url"] = snap["mp4_url"]
        snap["package_url"] = f"/api/studio-agent/jobs/{job_id}/package?kind=longform"
        _attach_production_control(
            snap,
            "finalize_longform_render",
            job_id=job_id,
            next_action="Download, package, or start a controlled revision pass.",
        )
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
    ws = (ROOT / SKELETON_OUTPUT / job_id).resolve()
    scenes_path = ws / "scenes.json"
    if scenes_path.is_file():
        try:
            scenes = json.loads(scenes_path.read_text(encoding="utf-8"))
            if isinstance(scenes, list):
                for sc in scenes:
                    if not isinstance(sc, dict):
                        continue
                    if int(sc.get("index") or 0) != int(scene_idx):
                        continue
                    rel = str(sc.get("still_rel") or "").strip()
                    if rel:
                        path = (ws / rel).resolve()
                        try:
                            path.relative_to(ws)
                        except ValueError:
                            return None
                        return path if path.is_file() else None
        except Exception:
            pass
    shortform_fallback = ws / "stills" / f"b{int(scene_idx):02d}.png"
    if shortform_fallback.is_file():
        return shortform_fallback
    from long_form import pipeline as lf_pipeline

    path = lf_pipeline.job_still_path(job_id, scene_idx)
    return path if path and path.exists() else None


def _shortform_scene_count(workspace: Path) -> int:
    scenes_path = workspace / "scenes.json"
    if scenes_path.is_file():
        try:
            scenes = json.loads(scenes_path.read_text(encoding="utf-8"))
            if isinstance(scenes, list):
                return len([sc for sc in scenes if isinstance(sc, dict)])
        except Exception:
            pass
    stills_dir = workspace / "stills"
    if stills_dir.is_dir():
        return len([p for p in stills_dir.glob("*.png") if p.is_file()])
    return 0


def _shortform_scene_snapshots(job_id: str, workspace: Path) -> list[dict[str, Any]]:
    scenes_path = workspace / "scenes.json"
    if not scenes_path.is_file():
        count = _shortform_scene_count(workspace)
        return [
            {
                "index": i,
                "duration_sec": 5.0,
                "approved_for_video": False,
                "approved_for_animation": False,
                "animate": False,
                "still_preview_url": f"/api/studio-agent/jobs/{job_id}/still/{i}",
            }
            for i in range(count)
        ]
    try:
        raw = json.loads(scenes_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    scenes: list[dict[str, Any]] = []
    for fallback_idx, sc in enumerate(raw):
        if not isinstance(sc, dict):
            continue
        try:
            idx = int(sc.get("index", fallback_idx))
        except Exception:
            idx = fallback_idx
        try:
            duration = float(sc.get("duration_sec", 5.0) or 5.0)
        except Exception:
            duration = 5.0
        scenes.append({
            "index": idx,
            "sid": sc.get("sid"),
            "narration": sc.get("narration"),
            "scene_action": sc.get("scene_action") or sc.get("action"),
            "duration_sec": duration,
            "status": sc.get("status"),
            "animate": bool(sc.get("animate", False)),
            "approved_for_video": bool(sc.get("approved_for_video", False)),
            "approved_for_animation": bool(sc.get("approved_for_animation", False)),
            "has_clip": bool(sc.get("clip_rel")),
            "video_model": sc.get("video_model"),
            "last_edit": sc.get("last_edit"),
            "still_preview_url": f"/api/studio-agent/jobs/{job_id}/still/{idx}",
        })
    return scenes


def _write_shortform_result(workspace: Path, *, status: str, error: str = "", **extra: Any) -> None:
    workspace.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {"status": status, "error": error or None, **extra}
    (workspace / "result.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _heartbeat_loop(stop_event: threading.Event, hb_path: Path, interval: float = 20.0) -> None:
    while not stop_event.wait(interval):
        try:
            hb_path.touch(exist_ok=True)
        except Exception:
            pass


def _start_shortform_reclaim_job(workspace: Path, job_id: str) -> bool:
    """Relaunch a shortform still-planning worker from its durable job spec."""
    spec_path = workspace / "job_spec.json"
    if not spec_path.is_file() or (workspace / "result.json").is_file():
        return False
    marker = workspace / "RECLAIMING"
    now = time.time()
    if marker.is_file() and (now - marker.stat().st_mtime) < SHORTFORM_RECLAIM_MARKER_SEC:
        return False
    try:
        spec = json.loads(spec_path.read_text(encoding="utf-8"))
        if not isinstance(spec, dict):
            return False
        marker.write_text(str(now), encoding="utf-8")
        (workspace / "progress.json").write_text(
            json.dumps({
                "stage": "restarting",
                "progress": 22,
                "detail": "Worker was interrupted; resuming still generation from saved job spec.",
            }, indent=2),
            encoding="utf-8",
        )
    except Exception:
        return False

    def _relaunch() -> None:
        import traceback as _tb

        hb = workspace / "heartbeat.txt"
        stop = threading.Event()
        hb_thread = threading.Thread(target=_heartbeat_loop, args=(stop, hb), daemon=True, name=f"hb-reclaim-{job_id}")
        hb_thread.start()
        try:
            hb.touch(exist_ok=True)
            from skeleton_ai.styled_pipeline import plan_scenes

            plan_scenes(
                category_key=str(spec.get("category_key") or "people_blogs"),
                topic=spec.get("topic"),
                workspace=workspace,
                render_style=str(spec.get("render_style") or "cinematic"),
                tier=str(spec.get("tier") or "standard"),
                video_model=spec.get("video_model"),
                visual_brief=spec.get("visual_brief"),
                script_override=spec.get("script"),
                user_id=spec.get("user_id"),
                default_animate=False,
                reference_images=list(spec.get("reference_images") or []),
                sound_design_brief=str(spec.get("sound_design_brief") or ""),
            )
        except Exception as exc:
            try:
                (workspace / "job.log").write_text(
                    f"RECLAIM FAILED {time.time()}\n{exc}\n\n{_tb.format_exc()}",
                    encoding="utf-8",
                )
            except Exception:
                pass
            try:
                _write_shortform_result(workspace, status="failed", error=str(exc), job_id=job_id)
            except Exception:
                pass
        finally:
            stop.set()
            try:
                hb.touch(exist_ok=True)
                marker.unlink(missing_ok=True)
            except Exception:
                pass

    threading.Thread(target=_relaunch, daemon=True, name=f"reclaim-{job_id}").start()
    return True


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


def _existing_shortform_video_path(workspace: Path, data: dict[str, Any] | None = None) -> Path | None:
    """Return a real final MP4 if the workspace already has one.

    This intentionally treats media on disk as the source of truth. Approval tools
    can rewrite result.json while the final MP4 remains valid; polling must not
    move a finished job back to scene review.
    """
    candidates: list[Path] = []
    raw = str((data or {}).get("video_path") or "").strip()
    if raw:
        p = Path(raw)
        candidates.append(p if p.is_absolute() else workspace / p)
    candidates.extend(workspace / name for name in ("skeleton_short.mp4", "styled_short.mp4", "final.mp4", "short.mp4"))
    for candidate in candidates:
        try:
            if candidate.is_file() and candidate.stat().st_size > 1024:
                return candidate.resolve()
        except OSError:
            continue
    return None


def _attach_production_control(
    snap: dict[str, Any],
    tool_name: str,
    *,
    job_id: str,
    awaiting_user_approval: bool = False,
    next_action: str | None = None,
) -> dict[str, Any]:
    """Expose durable render/approval metadata for UI polling without changing execution."""
    control = production_budget.production_control_metadata(tool_name, {"job_id": job_id})
    control["awaiting_user_approval"] = bool(awaiting_user_approval)
    if next_action:
        control["next_action"] = next_action
    snap["production_control"] = control
    snap["queue_lane"] = control.get("lane")
    snap["queue_priority"] = control.get("queue_priority")
    snap["stage_gates"] = control.get("stage_gates") or []
    snap["awaiting_user_approval"] = bool(awaiting_user_approval)
    return snap


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
        age = time.time() - last_touch if last_touch else 0.0
        if last_touch and age > SHORTFORM_STALE_SEC:
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
            err = (
                f"No progress for {max(1, SHORTFORM_STALE_SEC // 60)}+ minutes - "
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
        if last_touch and age > SHORTFORM_RECLAIM_SEC and spec_path.is_file():
            if _start_shortform_reclaim_job(workspace, job_id):
                progress = max(progress, 22)
                stage = "restarting"
                stage_label = "Restarting"
                stage_detail = "The worker was interrupted, likely by a deploy/restart. Resuming from the saved job spec."
        snap = {
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
        return _attach_production_control(
            snap,
            "start_shortform_generate",
            job_id=job_id,
            next_action="Wait for the running production stage to update or complete.",
        )
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
    video_path = _existing_shortform_video_path(workspace, data)
    if video_path and st != "failed":
        complete = True
        st = "complete"
        data["status"] = "complete"
        data["video_path"] = str(video_path)
        data.pop("error", None)
        try:
            result_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            progress_path.write_text(
                json.dumps(
                    {
                        "stage": "complete",
                        "progress": 100,
                        "detail": "Final MP4 ready.",
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
        except Exception:
            pass
    if st == "cancelled":
        # If a re-edit/retry reused the workspace, a stale cancelled result can
        # briefly coexist with a finished MP4. Prefer the actual deliverable.
        if video_path:
            st = "complete"
            data["status"] = "complete"
            data.setdefault("video_path", str(video_path))
            data.pop("error", None)
    cancelled = st == "cancelled"
    complete = st == "complete"
    awaiting_scene_review = st in {
        "awaiting_scene_review",
        "awaiting_approval",
        "stills_done",
        "review_scenes",
        "scenes_approved",
        "awaiting_animation_review",
    }
    terminal_fail = st == "failed" or cancelled
    scene_snapshots = _shortform_scene_snapshots(job_id, workspace)
    scene_count = len(scene_snapshots) or _shortform_scene_count(workspace)
    approved_scene_count = sum(
        1 for scene in scene_snapshots
        if scene.get("approved_for_video") or scene.get("approved_for_animation")
    )
    all_scenes_approved = scene_count > 0 and approved_scene_count == scene_count
    animation_pending_count = sum(
        1 for scene in scene_snapshots
        if scene.get("approved_for_animation") and not scene.get("has_clip")
    )
    animation_complete_count = sum(
        1 for scene in scene_snapshots
        if scene.get("approved_for_animation") and scene.get("has_clip")
    )
    review_stage_label = (
        "Ready to animate"
        if all_scenes_approved and animation_pending_count
        else "Animation ready for review"
        if all_scenes_approved and animation_complete_count
        else "Scenes approved"
        if all_scenes_approved
        else "Review stills"
    )
    review_stage_detail = (
        f"All {scene_count} scenes are approved. {animation_pending_count} approved animation clip(s) still need rendering."
        if all_scenes_approved and animation_pending_count
        else f"All {scene_count} scenes are approved. Review the rendered animation, then finalize."
        if all_scenes_approved and animation_complete_count
        else f"All {scene_count} scenes are approved and ready for final export."
        if all_scenes_approved
        else "Review each still. Reply to edit any bad scene, then approve scenes for animation/final export."
    )
    snap: dict[str, Any] = {
        "job_id": job_id,
        "kind": "shortform",
        "status": "complete" if complete else "failed" if terminal_fail else "awaiting_approval" if awaiting_scene_review else "running",
        "progress": 100 if complete else 0 if terminal_fail else 85 if all_scenes_approved else 80 if awaiting_scene_review else 55,
        "stage": st or "running",
        "stage_label": "Complete" if complete else ("Cancelled" if cancelled else "Failed") if terminal_fail else review_stage_label if awaiting_scene_review else "Rendering",
        "stage_detail": (
            review_stage_detail
            if awaiting_scene_review else data.get("detail")
        ),
        "error": ("Cancelled by user" if cancelled else data.get("error")),
        "running": not complete and not terminal_fail and not awaiting_scene_review,
        "title": str(data.get("topic") or data.get("category") or "Short-form"),
    }
    if scene_count > 0:
        snap["current_scene"] = scene_count
        snap["total_scenes"] = scene_count
        snap["approved_scene_count"] = approved_scene_count
        snap["all_scenes_approved"] = all_scenes_approved
        snap["animation_pending_count"] = animation_pending_count
        snap["animation_complete_count"] = animation_complete_count
        if scene_snapshots:
            snap["scenes"] = scene_snapshots
    if awaiting_scene_review and scene_count > 0:
        snap["can_finalize"] = all_scenes_approved and animation_pending_count == 0
        snap["still_count"] = scene_count
        snap["still_preview_urls"] = [
            str(scene.get("still_preview_url"))
            for scene in scene_snapshots[:12]
            if scene.get("still_preview_url")
        ] or [
            f"/api/studio-agent/jobs/{job_id}/still/{i}"
            for i in range(min(scene_count, 12))
        ]
        _attach_production_control(
            snap,
            "start_shortform_generate",
            job_id=job_id,
            awaiting_user_approval=not all_scenes_approved,
            next_action=(
                "Animate the approved scenes, then review and finalize."
                if all_scenes_approved and animation_pending_count
                else "Review the rendered animation, then finalize."
                if all_scenes_approved and animation_complete_count
                else "Finalize the approved production."
                if all_scenes_approved
                else "Review stills, edit or regenerate bad scenes, then approve scenes before animation or final export."
            ),
        )
    elif not complete and not terminal_fail:
        _attach_production_control(
            snap,
            "finalize_production",
            job_id=job_id,
            next_action="Track the server-side render until a terminal result is available.",
        )
    if complete:
        snap["mp4_url"] = f"/api/studio-agent/jobs/{job_id}/media?kind=shortform"
        snap["download_url"] = snap["mp4_url"]
        snap["package_url"] = f"/api/studio-agent/jobs/{job_id}/package?kind=shortform"
        _attach_production_control(
            snap,
            "finalize_production",
            job_id=job_id,
            next_action="Download, upload package, or start a reply-and-edit pass.",
        )
    return snap


def _competitor_status(job_id: str) -> dict[str, Any]:
    from studio_agent import competitor

    raw = competitor.read_status(job_id)
    status = str(raw.get("status") or "running").strip().lower()
    stage = str(raw.get("stage") or status or "queued").strip().lower()
    pct = int(raw.get("percent") or competitor._stage_percent(stage) or 0)
    complete = status == "complete" or stage == "complete"
    failed = status in ("error", "failed") or stage in ("error", "failed")
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
    if isinstance(raw.get("analysis_profile"), dict):
        snap["analysis_profile"] = raw["analysis_profile"]
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
    if kind == "competitor":
        shortform_workspace = (ROOT / SKELETON_OUTPUT / job_id).resolve()
        if shortform_workspace.is_dir():
            kind = "shortform"
    if kind == "shortform":
        snap = _shortform_status(job_id)
    elif kind == "competitor":
        snap = _competitor_status(job_id)
    else:
        snap = _longform_status(job_id)
    if snap.get("status") == "complete" and kind in {"shortform", "longform"}:
        _attach_render_qa(snap, job_id, kind)
    snap["polled_at"] = time.time()
    return snap


def _attach_render_qa(snap: dict[str, Any], job_id: str, kind: str) -> None:
    """Attach cached deterministic QA without allowing QA failures to break polling."""
    try:
        video_path = resolve_media_path(job_id, kind)
        if not video_path:
            return
        package_path = resolve_package_path(job_id, kind)
        from studio_agent import render_qa

        report = render_qa.analyze_render(
            job_id=job_id,
            kind=kind,
            video_path=video_path,
            package_path=package_path,
        )
        snap["render_qa"] = report
        snap["ready_to_post"] = report.get("status") == "pass"
    except Exception as exc:
        snap["render_qa"] = {
            "version": 1,
            "job_id": job_id,
            "kind": kind,
            "status": "warn",
            "score": 0,
            "summary": "WARN 0/100 - QA could not complete; review manually.",
            "checks": [
                {
                    "id": "qa_exception",
                    "label": "QA system",
                    "status": "warn",
                    "status_label": "WARN",
                    "detail": str(exc)[:300],
                }
            ],
            "created_at": time.time(),
        }
        snap["ready_to_post"] = False


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


def _format_ts(seconds: float) -> str:
    seconds = max(0, float(seconds or 0))
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def _build_longform_package(job_id: str) -> Path | None:
    from long_form import pipeline as lf_pipeline

    state = lf_pipeline.load_state(job_id) or {}
    outline = state.get("outline") if isinstance(state.get("outline"), dict) else {}
    if not outline:
        return None
    job_dir = lf_pipeline.LF_OUTPUT_ROOT / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    path = job_dir / "package.txt"
    title = str(outline.get("title") or state.get("title") or "Untitled long-form video").strip()
    tags = outline.get("tags") if isinstance(outline.get("tags"), list) else []
    description = str(outline.get("description") or outline.get("hook") or title).strip()
    chapters = outline.get("chapters") if isinstance(outline.get("chapters"), list) else []
    timestamps: list[str] = []
    cursor = 0.0
    for idx, ch in enumerate(chapters):
        if not isinstance(ch, dict):
            continue
        ch_title = str(ch.get("title") or ch.get("name") or f"Chapter {idx + 1}").strip()
        timestamps.append(f"{_format_ts(cursor)} - {ch_title}")
        minutes = float(ch.get("minutes") or 0)
        duration = float(ch.get("duration_sec") or 0)
        cursor += duration if duration > 0 else minutes * 60 if minutes > 0 else 60
    if not timestamps:
        timestamps = ["00:00 - Opening"]
    text = f"""Title:
{title}

Description:
{description}

Timestamps:
{chr(10).join(timestamps)}

Tags:
{", ".join(str(t).strip() for t in tags if str(t).strip())}

Thumbnail:
Generated for long-form. Use the thumbnail candidates attached to this job and pick the strongest CTR-safe option.
"""
    path.write_text(text, encoding="utf-8")
    return path


def _build_shortform_package(job_id: str) -> Path | None:
    ws = (ROOT / SKELETON_OUTPUT / job_id).resolve()
    scenes_path = ws / "scenes.json"
    result_path = ws / "result.json"
    if not scenes_path.is_file() and not result_path.is_file():
        return None
    try:
        scenes = json.loads(scenes_path.read_text(encoding="utf-8")) if scenes_path.is_file() else []
        if not isinstance(scenes, list):
            scenes = []
    except Exception:
        scenes = []
    try:
        result = json.loads(result_path.read_text(encoding="utf-8")) if result_path.is_file() else {}
        if not isinstance(result, dict):
            result = {}
    except Exception:
        result = {}
    try:
        spec_path = ws / "job_spec.json"
        spec = json.loads(spec_path.read_text(encoding="utf-8")) if spec_path.is_file() else {}
        if not isinstance(spec, dict):
            spec = {}
    except Exception:
        spec = {}
    title = str(result.get("topic") or result.get("category") or "Untitled Short").strip()
    category = str(result.get("category") or "short").strip()
    render_style = str(result.get("render_style") or "cinematic").strip()
    safe_topic_tag = "".join(ch for ch in title.lower() if ch.isalnum())[:32] or "shorts"
    watermark_text = str(spec.get("watermark_text") or result.get("watermark_text") or "").strip()
    brand_tag = "".join(ch for ch in watermark_text.lower() if ch.isalnum())[:32]
    tags = [
        safe_topic_tag,
        category,
        render_style,
        "shorts",
        "youtube shorts",
        "ai video",
        "nyptid studio",
    ]
    if brand_tag:
        tags.append(brand_tag)
    hashtags = ["#shorts", f"#{safe_topic_tag}", "#nyptidstudio"]
    if brand_tag:
        hashtags.append(f"#{brand_tag}")
    timestamps: list[str] = []
    cursor = 0.0
    for idx, sc in enumerate(scenes):
        if not isinstance(sc, dict):
            continue
        label = str(sc.get("narration") or sc.get("prompt") or f"Scene {idx + 1}").strip()
        timestamps.append(f"{_format_ts(cursor)} - {label[:70].rstrip(' .,') or f'Scene {idx + 1}'}")
        cursor += float(sc.get("duration_sec") or 0) or 0
    path = ws / "package.txt"
    text = f"""Title:
{title}

Description:
{title}

Watch the full story unfold in a fast, tightly edited short. Subscribe for more.

Timestamps:
{chr(10).join(timestamps) if timestamps else "00:00 - Full short"}

Tags:
{", ".join(dict.fromkeys(t for t in tags if t))}

Hashtags:
{" ".join(dict.fromkeys(h for h in hashtags if h))}

Thumbnail:
Not generated for short-form by default. Use the strongest frame/cover from the finished Short unless the user explicitly asks for a custom thumbnail.
"""
    path.write_text(text, encoding="utf-8")
    return path


def resolve_package_path(job_id: str, kind: str) -> Path | None:
    kind = str(kind or "").strip().lower()
    if not job_id.replace("_", "").isalnum() or len(job_id) > 48:
        return None
    if kind == "shortform":
        ws = (ROOT / SKELETON_OUTPUT / job_id).resolve()
        p = (ws / "package.txt").resolve()
        if p.is_file():
            return p
        return _build_shortform_package(job_id)
    if kind == "longform":
        from long_form import pipeline as lf_pipeline

        p = (lf_pipeline.LF_OUTPUT_ROOT / job_id / "package.txt").resolve()
        if p.is_file():
            return p
        return _build_longform_package(job_id)
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
