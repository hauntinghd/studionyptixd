"""Unified production job status for Studio Agent UI polling."""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any

from studio_agent import production_budget, production_costs, telemetry
from studio_agent.fs_paths import skeleton_output_root
from studio_agent.production_slots import slot_snapshot
from studio_agent.studio_identity import promotion_mode_from_metadata, upload_package_promotion

ROOT = Path(__file__).resolve().parents[1]
SKELETON_OUTPUT = skeleton_output_root()

# Shortform jobs with no progress.json / heartbeat update for this long are marked failed.
# Heartbeat is written by a sidecar thread during long blocking ops (fal subscribe etc.)
# so that slow-but-alive i2v / still renders don't get falsely timed out.
SHORTFORM_STALE_SEC = int(os.getenv("SHORTFORM_JOB_STALE_SEC", "3600"))  # 1h default for multi-scene I2V
SHORTFORM_RECLAIM_SEC = int(os.getenv("SHORTFORM_JOB_RECLAIM_SEC", "180"))  # resume after deploy/restart
SHORTFORM_RECLAIM_MARKER_SEC = int(os.getenv("SHORTFORM_JOB_RECLAIM_MARKER_SEC", "240"))


def shortform_job_terminal_fast(job_id: str) -> bool:
    """Cheap terminal check for session active_jobs pruning (no QA / scene scans)."""
    jid = str(job_id or "").strip()
    if not jid:
        return False
    workspace = (ROOT / SKELETON_OUTPUT / jid).resolve()
    result_path = workspace / "result.json"
    if not result_path.is_file():
        return False
    try:
        from studio_agent.stt_utils import safe_json_loads

        data = safe_json_loads(result_path.read_text(encoding="utf-8"), default={})
        if not isinstance(data, dict):
            return False
        status = str(data.get("status") or "").lower()
        if status in {"complete", "done", "success", "failed", "cancelled", "canceled"}:
            return True
        return bool(_existing_shortform_video_path(workspace, data))
    except Exception:
        return False

JOB_START_TOOLS = frozenset({
    "start_longform_render",
    "start_shortform_generate",
    "analyze_reference_video",
    "ingest_cliplab_attachment",
    "analyze_cliplab_video",
    "render_cliplab_segments",
    "remix_cliplab_short",
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
    "thumbnail_review": "Review thumbnail",
    "compose": "Compositing final MP4",
    "finalizing": "Finalizing export",
    "done": "Complete",
    "failed": "Failed",
}


def extract_jobs_from_tool(tool_name: str, result_text: str) -> list[dict[str, Any]]:
    """Parse tool JSON output for trackable background jobs."""
    from studio_agent.stt_utils import tool_result_dict

    data = tool_result_dict(result_text)
    if not data:
        return []
    job_id = str(data.get("job_id") or "").strip()
    if not job_id:
        return []
    if tool_name == "start_longform_render":
        kind = "longform"
        title = str(data.get("outline_title") or data.get("channel_key") or "Long-form render")
    elif tool_name == "generate_longform_thumbnails":
        kind = "longform"
        title = str(data.get("title") or "Long-form thumbnail")
    elif tool_name == "start_shortform_generate":
        kind = "shortform"
        title = str(data.get("topic") or data.get("category_key") or "Short-form render")
    elif tool_name == "analyze_reference_video":
        kind = "competitor"
        title = "Reference video analysis"
    elif tool_name in {"ingest_cliplab_attachment", "analyze_cliplab_video", "render_cliplab_segments", "remix_cliplab_short"}:
        kind = "cliplab"
        title = str(data.get("video_id") or data.get("style_preset") or "ClipLab job")
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


def prune_orphan_shortform_jobs(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop stale shortform entries when reference analysis owns the session."""
    has_reference = any(str(j.get("kind") or "") == "competitor" for j in jobs)
    if not has_reference:
        return jobs
    kept: list[dict[str, Any]] = []
    for job in jobs:
        kind = str(job.get("kind") or "")
        if kind != "shortform":
            kept.append(job)
            continue
        jid = str(job.get("job_id") or "").strip()
        if not jid:
            continue
        workspace = (ROOT / SKELETON_OUTPUT / jid).resolve()
        if not workspace.is_dir():
            continue
        result_path = workspace / "result.json"
        if result_path.is_file():
            try:
                from studio_agent.stt_utils import safe_json_loads

                data = safe_json_loads(result_path.read_text(encoding="utf-8"), default={})
                if isinstance(data, dict) and str(data.get("status") or "").lower() in {
                    "complete",
                    "running",
                    "awaiting_scene_review",
                    "awaiting_approval",
                    "stills_done",
                    "review_scenes",
                    "scenes_approved",
                    "awaiting_animation_review",
                }:
                    kept.append(job)
                    continue
            except Exception:
                pass
        progress_path = workspace / "progress.json"
        if progress_path.is_file():
            kept.append(job)
    return kept


def merge_active_jobs(existing: list[dict[str, Any]], new_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id = {str(j.get("job_id") or ""): j for j in existing if j.get("job_id")}
    for item in new_items:
        jid = str(item.get("job_id") or "")
        if jid:
            by_id[jid] = {**by_id.get(jid, {}), **item}
    merged = list(by_id.values())
    new_has_reference = any(str(item.get("kind") or "") == "competitor" for item in new_items)
    if new_has_reference:
        merged = [j for j in merged if str(j.get("kind") or "") != "shortform"]
    return prune_orphan_shortform_jobs(merged)


def _longform_still_count(job_id: str) -> int:
    try:
        from long_form import pipeline as lf_pipeline

        stills_dir = lf_pipeline._ensure_job_dir(job_id) / "stills"
        return len(lf_pipeline._list_scenes_sorted(stills_dir))
    except Exception:
        return 0


_LONGFORM_RESUME_PHASES = frozenset({
    "scenes", "narration", "ambient", "thumbnails", "compose",
    "scene_assembly", "i2v", "vo", "sfx", "finalizing",
})


def _longform_live_phase(job_id: str) -> tuple[str, dict[str, Any], dict[str, Any]]:
    from long_form import pipeline as lf_pipeline

    live = lf_pipeline.get_status(job_id) or {}
    st = lf_pipeline.load_state(job_id) or {}
    disk_phase = str(st.get("phase") or "unknown")
    live_phase = str(live.get("phase") or "")
    phase = live_phase or disk_phase
    mp4_rel = str(st.get("mp4_path") or "").strip()
    if mp4_rel:
        try:
            mp4_abs = lf_pipeline.LF_OUTPUT_ROOT / mp4_rel
            if mp4_abs.is_file() and mp4_abs.stat().st_size > 65536:
                phase = "done"
        except Exception:
            pass
    # Disk can stay "failed" with a stale error while finalize was re-kicked in-memory.
    if disk_phase == "failed" and live_phase in _LONGFORM_RESUME_PHASES:
        phase = live_phase
    elif disk_phase == "failed" and not live_phase:
        narration = lf_pipeline._ensure_job_dir(job_id) / "audio" / "narration.mp3"
        if narration.is_file() and narration.stat().st_size > 8192:
            phase = "finalizing"
    return phase, live, st


def _longform_thumbnail_count(job_id: str) -> int:
    try:
        from long_form import pipeline as lf_pipeline

        thumb_dir = lf_pipeline._ensure_job_dir(job_id) / "thumbnails"
        if not thumb_dir.is_dir():
            return 0
        return len([
            p for p in thumb_dir.iterdir()
            if p.is_file() and p.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}
        ])
    except Exception:
        return 0


def _longform_output_mp4_ready(job_id: str, st: dict[str, Any]) -> bool:
    from long_form import pipeline as lf_pipeline

    mp4_rel = str(st.get("mp4_path") or "").strip()
    if mp4_rel:
        try:
            mp4_abs = lf_pipeline.LF_OUTPUT_ROOT / mp4_rel
            if mp4_abs.is_file() and mp4_abs.stat().st_size > 65536:
                return True
        except Exception:
            pass
    try:
        job_dir = lf_pipeline._ensure_job_dir(job_id)
        for folder in (job_dir / "output", job_dir):
            if not folder.is_dir():
                continue
            for candidate in folder.glob("*.mp4"):
                if candidate.is_file() and candidate.stat().st_size > 65536:
                    return True
    except Exception:
        pass
    return False


def _longform_idle_has_artifacts(job_id: str, st: dict[str, Any]) -> bool:
    if not st:
        try:
            from long_form import pipeline as lf_pipeline

            st = lf_pipeline.load_state(job_id) or {}
        except Exception:
            st = {}
    thumb_count = max(int(st.get("thumbnails_generated") or 0), _longform_thumbnail_count(job_id))
    if thumb_count > 0 or bool(st.get("thumbnail_only")):
        return True
    if _longform_output_mp4_ready(job_id, st):
        return True
    if int(st.get("scenes_generated") or 0) > 0 or _longform_still_count(job_id) > 0:
        return True
    try:
        from long_form import pipeline as lf_pipeline

        narration = lf_pipeline._ensure_job_dir(job_id) / "audio" / "narration.mp3"
        if narration.is_file() and narration.stat().st_size > 8192:
            return True
    except Exception:
        pass
    return bool(st)


def _longform_status(job_id: str) -> dict[str, Any]:
    from long_form import pipeline as lf_pipeline

    phase, live, st = _longform_live_phase(job_id)
    percent = int(live.get("percent") or st.get("percent") or 0)
    error = str(live.get("error") or st.get("error") or "")
    mp4_rel = str(st.get("mp4_path") or "").strip()
    if mp4_rel:
        try:
            mp4_abs = lf_pipeline.LF_OUTPUT_ROOT / mp4_rel
            if mp4_abs.is_file() and mp4_abs.stat().st_size > 65536:
                phase = "done"
                percent = 100
                error = ""
        except Exception:
            pass
    if phase == "unknown" and percent == 0 and not str(live.get("phase") or ""):
        thumb_count = max(int(st.get("thumbnails_generated") or 0), _longform_thumbnail_count(job_id))
        if thumb_count > 0 or bool(st.get("thumbnail_only")):
            phase = "done"
            percent = 100
            error = ""
        elif _longform_output_mp4_ready(job_id, st):
            phase = "done"
            percent = 100
            error = ""
        elif int(st.get("scenes_generated") or 0) > 0 or _longform_still_count(job_id) > 0:
            phase = "awaiting_approval"
            error = ""
        elif _longform_idle_has_artifacts(job_id, st):
            phase = "done"
            percent = 100
            error = ""
        else:
            try:
                from long_form import pipeline as lf_pipeline

                has_workspace = lf_pipeline._job_dir(job_id).is_dir() or bool(st)
            except Exception:
                has_workspace = bool(st)
            if not has_workspace:
                return {
                    "job_id": job_id,
                    "kind": "longform",
                    "status": "failed",
                    "progress": 0,
                    "stage": "failed",
                    "stage_label": "Failed",
                    "error": "Long-form workspace has no active render",
                    "running": False,
                    "title": str(
                        (st.get("outline") if isinstance(st.get("outline"), dict) else {}).get("title")
                        or st.get("channel_key")
                        or "Long-form"
                    ),
                }
            phase = "done"
            percent = 100
            error = ""
    if error and phase in {"", "unknown", "queued", "starting"} and not str(live.get("phase") or ""):
        if _longform_idle_has_artifacts(job_id, st):
            error = ""
            phase = "done" if _longform_output_mp4_ready(job_id, st) else "awaiting_approval"
            percent = 100 if phase == "done" else percent
        else:
            phase = "failed"
    running = phase not in ("done", "failed", "awaiting_approval", "thumbnail_review")
    status = "complete" if phase == "done" else "failed" if phase == "failed" else (
        "awaiting_approval" if phase in {"awaiting_approval", "thumbnail_review"} else "running"
    )
    if status == "running":
        error = ""
    elif status == "failed" and not error:
        error = "Long-form production failed"
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
        "title": str(
            (st.get("outline") if isinstance(st.get("outline"), dict) else {}).get("title")
            or st.get("channel_key")
            or "Long-form"
        ),
        "running": running,
    }
    if bool(st.get("thumbnail_only")) or phase == "thumbnail_review":
        thumb_count = max(0, int(st.get("thumbnails_generated") or 0))
        thumbnail_urls = [
            f"/api/studio-agent/jobs/{job_id}/thumbnail/{idx}"
            for idx in range(1, thumb_count + 1)
        ]
        snap.update({
            "thumbnail_only": True,
            "thumbnail_urls": thumbnail_urls,
            "preview_url": thumbnail_urls[0] if thumbnail_urls else None,
            "next_action": "Review the thumbnail candidates and describe the change you want; this does not start video production.",
        })
        return snap
    proof_only = (
        bool(st.get("visual_proof_only") or (st.get("outline") or {}).get("visual_proof_only"))
        and phase == "awaiting_approval"
        and not bool(st.get("proof_scene_approved"))
    )
    if proof_only:
        snap["visual_proof_only"] = True
    scenes_gen = int(st.get("scenes_generated") or live.get("scene_total") or 0)
    disk_stills = _longform_still_count(job_id) if phase in {"scenes", "awaiting_approval", "done"} else 0
    if phase == "awaiting_approval" and scenes_gen > 0:
        snap["can_finalize"] = not proof_only
        snap["still_count"] = scenes_gen
        snap["still_preview_urls"] = [
            f"/api/studio-agent/jobs/{job_id}/still/{i}"
            for i in range(min(scenes_gen, 12))
        ]
        snap["total_scenes"] = scenes_gen
        _attach_production_control(
            snap,
            "start_longform_render",
            job_id=job_id,
            awaiting_user_approval=True,
            next_action=(
                "Review the visual proof still. Approve the look to build the full scene gallery, "
                "or regenerate scene 1 before expanding."
                if proof_only
                else "Review chapter stills, edit or regenerate weak frames, then approve "
                "the long-form render for final composition."
            ),
        )
    elif running:
        scene_done = max(int(live.get("scene_done") or 0), disk_stills)
        scene_total = int(live.get("scene_total") or 0)
        narr_done = int(live.get("narration_done") or st.get("narration_done") or 0)
        narr_total = int(live.get("narration_total") or st.get("narration_total") or 0)
        if phase in {"narration", "ambient", "thumbnails", "compose", "finalizing"}:
            if narr_total > 0:
                snap["current_chapter"] = narr_done
                snap["total_chapters"] = narr_total
            snap["stage_detail"] = str(
                live.get("detail")
                or st.get("stage_detail")
                or snap.get("stage_label")
                or ""
            )
        if phase == "scenes" and scene_done > 0:
            snap["still_count"] = scene_done
            snap["current_scene"] = scene_done
            snap["total_scenes"] = scene_total or scene_done
            snap["still_preview_urls"] = [
                f"/api/studio-agent/jobs/{job_id}/still/{i}"
                for i in range(min(scene_done, 12))
            ]
            snap["stage_detail"] = str(
                live.get("detail")
                or f"Building gallery — {scene_done}/{scene_total or '?'} scenes"
            )
        _attach_production_control(
            snap,
            "start_longform_render",
            job_id=job_id,
            next_action="Wait for the long-form production stage to update or complete.",
        )
    if phase == "done" and mp4_rel:
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
        thumbnail_urls = [
            f"/api/studio-agent/jobs/{job_id}/thumbnail/{idx}"
            for idx in range(1, thumbs + 1)
        ]
        snap["thumbnail_urls"] = thumbnail_urls
        snap["preview_url"] = thumbnail_urls[0]
    return snap


def resolve_longform_thumbnail_path(job_id: str, index: int) -> Path | None:
    """Return a thumbnail candidate from a Studio Agent long-form job."""
    try:
        from long_form import pipeline as lf_pipeline

        return lf_pipeline.job_thumbnail_path(str(job_id or ""), int(index))
    except Exception:
        return None


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


def resolve_clip_path(job_id: str, scene_idx: int) -> Path | None:
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
                    rel = str(sc.get("clip_rel") or "").strip()
                    if not rel:
                        sid = str(sc.get("sid") or f"b{int(scene_idx):02d}").strip()
                        rel = f"clips/{sid}.mp4"
                    path = (ws / rel).resolve()
                    try:
                        path.relative_to(ws)
                    except ValueError:
                        return None
                    return path if path.is_file() and path.stat().st_size > 0 else None
        except Exception:
            pass
    fallback = ws / "clips" / f"b{int(scene_idx):02d}.mp4"
    return fallback if fallback.is_file() and fallback.stat().st_size > 0 else None


def _shortform_planned_scene_count(workspace: Path) -> int:
    spec_path = workspace / "job_spec.json"
    if not spec_path.is_file():
        return 0
    try:
        spec = json.loads(spec_path.read_text(encoding="utf-8"))
        if not isinstance(spec, dict):
            return 0
        if bool(spec.get("visual_proof_only")):
            return 1
        raw_count = spec.get("scene_count")
        if raw_count is not None:
            return max(1, min(60, int(raw_count)))
    except Exception:
        return 0
    return 0


def _shortform_job_title(workspace: Path) -> str:
    spec_path = workspace / "job_spec.json"
    if spec_path.is_file():
        try:
            spec = json.loads(spec_path.read_text(encoding="utf-8"))
            if isinstance(spec, dict):
                topic = str(spec.get("topic") or "").strip()
                if topic:
                    return topic
        except Exception:
            pass
    return ""


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
        count = len([p for p in stills_dir.glob("*.png") if p.is_file()])
        if count > 0:
            return count
    return _shortform_planned_scene_count(workspace)


def _still_preview_url(job_id: str, scene_idx: int) -> str:
    base = f"/api/studio-agent/jobs/{job_id}/still/{scene_idx}"
    path = resolve_still_path(job_id, scene_idx)
    if path and path.is_file():
        try:
            return f"{base}?v={int(path.stat().st_mtime)}"
        except OSError:
            pass
    return base


def _clip_preview_url(job_id: str, scene_idx: int) -> str | None:
    path = resolve_clip_path(job_id, scene_idx)
    if not path or not path.is_file() or path.stat().st_size <= 0:
        return None
    base = f"/api/studio-agent/jobs/{job_id}/clip/{scene_idx}"
    try:
        return f"{base}?v={int(path.stat().st_mtime)}"
    except OSError:
        return base


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
                "still_preview_url": _still_preview_url(job_id, i),
            }
            for i in range(count)
        ]
    try:
        raw = json.loads(scenes_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    # Cached semantic QA is part of the persisted asset state.  When the QA
    # contract changes, re-evaluate only outdated skeleton reports before
    # sending the review grid; otherwise an old false failure can leave a card
    # permanently black/blocked even though its current still is valid.
    try:
        spec = json.loads((workspace / "job_spec.json").read_text(encoding="utf-8"))
    except Exception:
        spec = {}
    if "skeleton" in str((spec or {}).get("render_style") or "").lower():
        try:
            from studio_agent.visual_qa import STILL_SEMANTIC_QA_VERSION, _workspace_skeleton_reference, audit_skeleton_still

            changed = False
            reference = _workspace_skeleton_reference(workspace)
            for fallback_idx, item in enumerate(raw):
                if not isinstance(item, dict):
                    continue
                cached = item.get("still_qa") if isinstance(item.get("still_qa"), dict) else {}
                if int(cached.get("version", 0) or 0) >= STILL_SEMANTIC_QA_VERSION:
                    continue
                idx = int(item.get("index", fallback_idx) or fallback_idx)
                sid = str(item.get("sid") or f"b{idx:02d}")
                still = workspace / str(item.get("still_rel") or f"stills/{sid}.png")
                qa = audit_skeleton_still(
                    still,
                    reference=reference,
                    locked_outfit=str((spec or {}).get("locked_outfit") or item.get("outfit") or ""),
                    force=True,
                )
                item["still_qa"] = qa
                passed = qa.get("status") == "pass" and qa.get("pass") is True
                item["status"] = "clip_ready" if item.get("clip_rel") else ("still_ready" if passed else "qa_blocked")
                if not passed:
                    item["approved_for_video"] = False
                    item["approved_for_animation"] = False
                    item["animate"] = False
                changed = True
            if changed:
                scenes_path.write_text(json.dumps(raw, indent=2), encoding="utf-8")
        except Exception:
            # The snapshot remains readable even if a provider QA call is
            # temporarily unavailable; the normal approval path will retry.
            pass
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
            "prompt": sc.get("prompt"),
            "prompt_user_override": bool(sc.get("prompt_user_override", False)),
            "duration_sec": duration,
            "status": sc.get("status"),
            "animate": bool(sc.get("animate", False)),
            "approved_for_video": bool(sc.get("approved_for_video", False)),
            "approved_for_animation": bool(sc.get("approved_for_animation", False)),
            "has_clip": bool(sc.get("clip_rel")),
            "video_model": sc.get("video_model"),
            "still_qa": sc.get("still_qa"),
            "last_edit": sc.get("last_edit"),
            "still_preview_url": _still_preview_url(job_id, idx),
            "clip_preview_url": (
                _clip_preview_url(job_id, idx)
                if bool(sc.get("clip_rel"))
                else None
            ),
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
                image_model_id=spec.get("image_model_id"),
                video_model=spec.get("video_model"),
                visual_brief=spec.get("visual_brief"),
                beats_target=1 if bool(spec.get("visual_proof_only")) else int(spec.get("scene_count") or 12),
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

    def _attach_cost(snap: dict[str, Any]) -> dict[str, Any]:
        try:
            production_costs.attach_to_progress(workspace, snap)
        except Exception:
            pass
        return snap

    if not workspace.is_dir() and not result_path.is_file():
        err = (
            "Production workspace was lost (likely server restart). "
            "Tap Retry in the render dock to run again."
        )
        return _attach_cost(_shortform_failed_snap(job_id, err))

    if not result_path.is_file():
        last_touch = 0.0
        for p in (progress_path, spec_path, workspace / "script.txt", workspace / "heartbeat.txt"):
            if p.is_file():
                last_touch = max(last_touch, p.stat().st_mtime)
        age = time.time() - last_touch if last_touch else 0.0
        if last_touch and age > SHORTFORM_RECLAIM_SEC and spec_path.is_file():
            if _start_shortform_reclaim_job(workspace, job_id):
                snap = {
                    "job_id": job_id,
                    "kind": "shortform",
                    "status": "running",
                    "progress": 22,
                    "stage": "restarting",
                    "stage_label": "Restarting",
                    "stage_detail": "The worker was interrupted, likely by a deploy/restart. Resuming from the saved job spec.",
                    "running": True,
                    "title": _shortform_job_title(workspace) or "Short-form video",
                }
                scene_snapshots = _shortform_scene_snapshots(job_id, workspace)
                scene_count = len(scene_snapshots) or _shortform_scene_count(workspace)
                expected_scene_count = max(scene_count, _shortform_planned_scene_count(workspace))
                if scene_count > 0:
                    snap["current_scene"] = scene_count
                    snap["total_scenes"] = expected_scene_count
                    snap["still_count"] = scene_count
                    snap["scenes"] = scene_snapshots
                    snap["still_preview_urls"] = [
                        str(scene.get("still_preview_url"))
                        for scene in scene_snapshots[:12]
                        if scene.get("still_preview_url")
                    ] or [
                        _still_preview_url(job_id, i)
                        for i in range(min(scene_count, 12))
                    ]
                _attach_cost(snap)
                return _attach_production_control(
                    snap,
                    "start_shortform_generate",
                    job_id=job_id,
                    next_action="Resuming the interrupted production from saved stills.",
                )
        if last_touch and age > SHORTFORM_STALE_SEC:
            active_stage = ""
            if progress_path.is_file():
                try:
                    from studio_agent.stt_utils import safe_json_loads

                    prog = safe_json_loads(progress_path.read_text(encoding="utf-8"), default={})
                    if isinstance(prog, dict):
                        active_stage = str(prog.get("stage") or "").strip().lower()
                except Exception:
                    pass
            if active_stage in {
                "animate", "expand_animate", "compose", "finalize", "audio_queue",
                "render_queue", "restarting", "pipeline",
            }:
                pass
            else:
                # Include diagnostic info so user (and future training data) can see why it looked dead.
                ages = {}
                for p in (progress_path, spec_path, workspace / "script.txt", workspace / "heartbeat.txt"):
                    if p.is_file():
                        ages[p.name] = int(time.time() - p.stat().st_mtime)
                err = (
                    f"No progress for {max(1, SHORTFORM_STALE_SEC // 60)}+ minutes - "
                    "production timed out. Tap Retry to run again."
                    f" (last file ages: {ages})"
                )
                _write_shortform_result(workspace, status="failed", error=err, job_id=job_id)
                return _attach_cost(_shortform_failed_snap(job_id, err))
        progress = 12
        stage = "pipeline"
        stage_label = "Building short"
        stage_detail = "Script, stills, motion, and composite running server-side."
        if progress_path.is_file():
            try:
                from studio_agent.stt_utils import safe_json_loads

                prog = safe_json_loads(progress_path.read_text(encoding="utf-8"), default={})
                if isinstance(prog, dict):
                    progress = int(prog.get("progress") or progress)
                    stage = str(prog.get("stage") or stage)
                    stage_label = stage.replace("_", " ").title()
                    stage_detail = str(prog.get("detail") or stage_detail)
            except Exception:
                pass
        snap = {
            "job_id": job_id,
            "kind": "shortform",
            "status": "running",
            "progress": progress,
            "stage": stage,
            "stage_label": stage_label,
            "stage_detail": stage_detail,
            "running": True,
            "title": _shortform_job_title(workspace) or "Short-form video",
        }
        scene_snapshots = _shortform_scene_snapshots(job_id, workspace)
        scene_count = len(scene_snapshots) or _shortform_scene_count(workspace)
        expected_scene_count = max(scene_count, _shortform_planned_scene_count(workspace))
        if scene_count > 0:
            snap["current_scene"] = scene_count
            snap["total_scenes"] = expected_scene_count
            snap["still_count"] = scene_count
            snap["scenes"] = scene_snapshots
            snap["still_preview_urls"] = [
                str(scene.get("still_preview_url"))
                for scene in scene_snapshots[:12]
                if scene.get("still_preview_url")
            ] or [
                _still_preview_url(job_id, i)
                for i in range(min(scene_count, 12))
            ]
        _attach_cost(snap)
        return _attach_production_control(
            snap,
            "start_shortform_generate",
            job_id=job_id,
            next_action="Wait for the running production stage to update or complete.",
        )
    try:
        from studio_agent.stt_utils import safe_json_loads

        data = safe_json_loads(result_path.read_text(encoding="utf-8"), default={})
        if not isinstance(data, dict) or not data:
            return _attach_cost(_shortform_failed_snap(
                job_id,
                "Production result file was empty or invalid — tap Retry to run again.",
            ))
    except Exception as exc:
        return _attach_cost(_shortform_failed_snap(job_id, str(exc)[:200]))
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
    # Prefer live background stages (animate / compose) over a stale review
    # status so the chat keeps polling and shows "Animating…" then the clip.
    bg_stage = ""
    for stage_name in ("animate", "compose", "expand_animate"):
        marker = workspace / f".{stage_name}.running"
        if not marker.is_file():
            continue
        try:
            age = time.time() - marker.stat().st_mtime
        except OSError:
            age = 0.0
        if age < 2 * 3600:
            bg_stage = stage_name
            break
    if bg_stage and not complete and not cancelled and st != "failed":
        scene_snapshots = _shortform_scene_snapshots(job_id, workspace)
        scene_count = len(scene_snapshots) or _shortform_scene_count(workspace)
        expected_scene_count = max(scene_count, _shortform_planned_scene_count(workspace))
        animation_complete_count = sum(
            1 for scene in scene_snapshots
            if scene.get("approved_for_animation") and scene.get("has_clip")
        )
        stage_label = (
            "Animating scenes"
            if bg_stage in {"animate", "expand_animate"}
            else "Composing MP4"
        )
        snap: dict[str, Any] = {
            "job_id": job_id,
            "kind": "shortform",
            "status": "running",
            "progress": 70 if bg_stage in {"animate", "expand_animate"} else 90,
            "stage": bg_stage,
            "stage_label": stage_label,
            "stage_detail": (
                "Image-to-video is rendering. The clip will appear in this chat card when ready."
                if bg_stage in {"animate", "expand_animate"}
                else "Voice, captions, and final MP4 are composing."
            ),
            "error": None,
            "running": True,
            "title": str(data.get("topic") or data.get("category") or _shortform_job_title(workspace) or "Short-form"),
            "animation_complete_count": animation_complete_count,
        }
        if isinstance(data.get("cost"), dict):
            snap["cost"] = data.get("cost")
        _attach_cost(snap)
        if scene_count > 0:
            snap["current_scene"] = scene_count
            snap["total_scenes"] = expected_scene_count
            snap["still_count"] = scene_count
            snap["scenes"] = scene_snapshots
            snap["still_preview_urls"] = [
                str(scene.get("still_preview_url"))
                for scene in scene_snapshots[:12]
                if scene.get("still_preview_url")
            ]
        return _attach_production_control(
            snap,
            "animate_production_scenes" if bg_stage in {"animate", "expand_animate"} else "finalize_production",
            job_id=job_id,
            next_action="Wait for the running stage to finish — clips and exports update this card automatically.",
        )
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
    expected_scene_count = max(scene_count, _shortform_planned_scene_count(workspace))
    approved_scene_count = sum(
        1 for scene in scene_snapshots
        if scene.get("approved_for_video") or scene.get("approved_for_animation")
    )
    all_scenes_approved = expected_scene_count > 0 and scene_count == expected_scene_count and approved_scene_count == scene_count
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
    if isinstance(data.get("cost"), dict):
        snap["cost"] = data.get("cost")
    _attach_cost(snap)
    if scene_count > 0:
        snap["current_scene"] = scene_count
        snap["total_scenes"] = expected_scene_count
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
            _still_preview_url(job_id, i)
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
    analysis_incomplete = False
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
        visual = raw.get("visual_summary") if isinstance(raw.get("visual_summary"), dict) else {}
        summary = str(visual.get("summary") or "").strip()
        if summary:
            snap["visual_summary"] = summary
        elif visual.get("error"):
            snap["visual_error"] = str(visual.get("error") or "")[:240]
        transcript = raw.get("transcript") if isinstance(raw.get("transcript"), dict) else {}
        transcript_text = str(transcript.get("text") or "").strip()
        if transcript_text:
            snap["transcript_excerpt"] = transcript_text[:500]
        elif transcript.get("error"):
            snap["transcript_error"] = str(transcript.get("error") or "")[:240]
        storytelling = raw.get("storytelling") if isinstance(raw.get("storytelling"), dict) else {}
        story_summary = str(storytelling.get("summary") or "").strip()
        if story_summary:
            snap["storytelling_summary"] = story_summary
        hook = str(storytelling.get("hook") or "").strip()
        if hook:
            snap["hook_summary"] = hook
        packaging = str(storytelling.get("packaging") or "").strip()
        if packaging:
            snap["packaging_notes"] = packaging
        if storytelling.get("error") and not (story_summary or hook):
            snap["storytelling_error"] = str(storytelling.get("error") or "")[:240]
        beats = storytelling.get("story_beats") if isinstance(storytelling.get("story_beats"), list) else []
        if beats:
            snap["story_beats"] = [str(item).strip() for item in beats[:8] if str(item).strip()]
        pacing_quality = raw.get("pacing_quality") if isinstance(raw.get("pacing_quality"), dict) else {}
        warnings = pacing_quality.get("warnings")
        if isinstance(warnings, list) and warnings:
            snap["pacing_warnings"] = [str(item) for item in warnings[:3]]
        analysis_gaps = raw.get("analysis_gaps") if isinstance(raw.get("analysis_gaps"), dict) else {}
        if analysis_gaps:
            snap["analysis_depth"] = str(analysis_gaps.get("depth") or "").strip()
            stage_errors = analysis_gaps.get("stage_errors")
            if isinstance(stage_errors, dict) and stage_errors:
                snap["stage_errors"] = {
                    str(stage): str(err)[:240]
                    for stage, err in stage_errors.items()
                    if str(err).strip()
                }
        if not snap.get("stage_errors"):
            inferred: dict[str, str] = {}
            for key, stage in (
                ("visual_error", "vision"),
                ("transcript_error", "transcript"),
                ("storytelling_error", "storytelling"),
            ):
                err = str(snap.get(key) or "").strip()
                if err:
                    inferred[stage] = err
            if inferred:
                snap["stage_errors"] = inferred
        if not snap.get("analysis_depth") and (
            snap.get("stage_errors") or snap.get("visual_error") or snap.get("transcript_error")
        ):
            snap["analysis_depth"] = "pacing_only" if not snap.get("visual_summary") else "partial"
        depth = str(snap.get("analysis_depth") or "").strip().lower()
        if depth == "pacing_only" or snap.get("stage_errors"):
            snap["analysis_incomplete"] = True
            snap["analysis_ready"] = False
            snap["status"] = "incomplete"
            snap["running"] = False
        note = str(raw.get("style_reference_note") or "").strip()
        steps = raw.get("next_steps")
        if note:
            snap["blueprint_hint"] = note
        elif isinstance(steps, list) and steps:
            snap["blueprint_hint"] = str(steps[0])[:400]
    return snap


def _cliplab_status(job_id: str) -> dict[str, Any]:
    try:
        from cliplab.pipeline import load_job_state

        raw = load_job_state(job_id)
    except Exception as exc:
        raw = {"status": "error", "error": str(exc)}
    status = str(raw.get("status") or "").strip().lower()
    if not status:
        status = "complete" if (raw.get("segments") or raw.get("clips") or raw.get("remix")) else "running"
    job_type = str(raw.get("type") or "").strip()
    if not job_type:
        if str(job_id).startswith("clipi_"):
            job_type = "cliplab_ingest"
        elif str(job_id).startswith("clipa_"):
            job_type = "cliplab_analyze"
        elif str(job_id).startswith("clipr_"):
            job_type = "cliplab_render"
        elif str(job_id).startswith("remix_"):
            job_type = "cliplab_remix"
    failed = status in {"error", "failed"}
    complete = status == "complete"
    progress = int(raw.get("progress") or (100 if complete else 30 if raw else 0))
    snap: dict[str, Any] = {
        "job_id": job_id,
        "kind": "cliplab",
        "status": "failed" if failed else "complete" if complete else "running",
        "progress": max(0, min(100, progress)),
        "stage": str(raw.get("stage") or job_type or status or "queued"),
        "stage_label": str(raw.get("stage") or job_type or status or "queued").replace("_", " ").title(),
        "error": raw.get("error"),
        "running": not complete and not failed,
        "title": str(raw.get("video_id") or "ClipLab")[:120],
        "video_id": raw.get("video_id"),
        "job_type": job_type,
        "provider": raw.get("provider"),
        "next_action": raw.get("next_action"),
        "cue_count": raw.get("cue_count"),
        "signal_summary": raw.get("signal_summary"),
    }
    if raw.get("segments"):
        snap["segments"] = raw.get("segments")
        snap["segment_count"] = len(raw.get("segments") or [])
        snap["next_action"] = "Choose segment_indices and call render_cliplab_segments."
    if raw.get("clips"):
        snap["clips"] = raw.get("clips")
        snap["clip_count"] = len(raw.get("clips") or [])
    if raw.get("upload_packages"):
        snap["upload_packages"] = raw.get("upload_packages")
        snap["upload_package_count"] = len(raw.get("upload_packages") or [])
    if raw.get("remix"):
        snap["remix"] = raw.get("remix")
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


def _job_payload_from_message_content(content: str) -> dict[str, Any]:
    """Extract a tool-result JSON object from tool/system/assistant message bodies."""
    from studio_agent.stt_utils import tool_result_dict

    text = str(content or "").strip()
    if not text:
        return {}
    direct = tool_result_dict(text)
    if direct.get("job_id"):
        return direct
    if "{" not in text:
        return {}
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return {}
    try:
        data = json.loads(text[start : end + 1])
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _longform_job_ids_from_session(session: dict[str, Any], *, message_scan_limit: int = 36) -> list[str]:
    """Collect durable long-form job ids referenced by this chat transcript."""
    import re

    found: list[str] = []
    seen: set[str] = set()

    def _add(job_id: str) -> None:
        jid = str(job_id or "").strip()
        if not jid or jid in seen:
            return
        if _resolve_poll_kind(jid, "longform") != "longform":
            return
        try:
            from long_form import pipeline as lf_pipeline

            if not lf_pipeline._job_dir(jid).is_dir() and not lf_pipeline.load_state(jid):
                return
        except Exception:
            return
        seen.add(jid)
        found.append(jid)

    for track in list(session.get("active_jobs") or []):
        if isinstance(track, dict) and str(track.get("kind") or "") == "longform":
            _add(str(track.get("job_id") or ""))
    review = session.get("thumbnail_review")
    if isinstance(review, dict):
        _add(str(review.get("job_id") or review.get("review_id") or ""))
    lp = session.get("last_production")
    if isinstance(lp, dict):
        args = lp.get("arguments")
        if isinstance(args, dict):
            _add(str(args.get("job_id") or ""))

    messages = list(session.get("messages") or [])
    tail = messages[-max(1, int(message_scan_limit or 36)) :]
    for msg in reversed(tail):
        role = str(msg.get("role") or "")
        if role not in {"tool", "system", "assistant", "user"}:
            continue
        data = _job_payload_from_message_content(str(msg.get("content") or ""))
        job_id = str(data.get("job_id") or "").strip()
        if not job_id:
            for match in re.findall(r"\b([a-f0-9]{12})\b", str(msg.get("content") or ""))[:2]:
                job_id = str(match or "").strip()
                if job_id:
                    break
        if job_id:
            _add(job_id)
    return found


def longform_failed_is_terminal(job_id: str) -> bool:
    """True when a failed snapshot should prune the session track (not mid-resume)."""
    phase, _live, _st = _longform_live_phase(job_id)
    return phase == "failed"


def longform_idle_failure(snap: dict[str, Any]) -> bool:
    """True when a failed long-form poll is just an idle workspace, not a live error."""
    if str(snap.get("kind") or "") != "longform" or str(snap.get("status") or "") != "failed":
        return False
    err = str(snap.get("error") or "")
    if "no active render" in err.lower():
        return True
    return int(snap.get("progress") or 0) == 0 and str(snap.get("stage") or "") in {"failed", "unknown", ""}


def reconcile_terminal_active_jobs(
    session_id: str,
    *,
    user_id: str | None = None,
    session: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Drop completed or idle long-form tracks so Sync does not resurrect ghost renders."""
    from studio_agent import store

    if session is None:
        session = store.get_session(session_id, user_id=user_id, reconcile_jobs=False)
    if not session:
        return None
    existing = list(session.get("active_jobs") or [])
    if not existing:
        return session
    kept: list[dict[str, Any]] = []
    changed = False
    for track in existing:
        if not isinstance(track, dict):
            changed = True
            continue
        jid = str(track.get("job_id") or "")
        kind = str(track.get("kind") or "")
        if not jid:
            changed = True
            continue
        if kind not in {"longform", "shortform"}:
            kept.append(track)
            continue
        snap = get_job_snapshot(jid, kind, lightweight=True)
        status = str(snap.get("status") or "")
        if status == "complete":
            changed = True
            continue
        if longform_idle_failure(snap):
            changed = True
            continue
        kept.append(track)
    if not changed:
        return session
    return store.update_session(session_id, active_jobs=kept) or session


def reconcile_running_longform_jobs(session_id: str, *, user_id: str | None = None, session: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """Reattach in-flight long-form jobs after refresh, deploy, or transient failure."""
    from studio_agent import store

    if session is None:
        session = store.get_session(session_id, user_id=user_id, reconcile_jobs=False)
    if not session or session.get("skip_job_recovery"):
        return session
    existing = list(session.get("active_jobs") or [])
    reinjected: list[dict[str, Any]] = []
    for job_id in _longform_job_ids_from_session(session):
        snap = get_job_snapshot(job_id, "longform", lightweight=True)
        if snap.get("status") not in {"running", "awaiting_approval"}:
            continue
        if snap.get("thumbnail_only"):
            continue
        reinjected.append({
            "job_id": job_id,
            "kind": "longform",
            "title": str(snap.get("title") or "Long-form render")[:120],
            "started_at": float(snap.get("polled_at") or time.time()),
        })
    if not reinjected:
        return session
    merged = merge_active_jobs(existing, reinjected)
    if merged == existing:
        return session
    return store.update_session(session_id, active_jobs=merged) or session


def _thumbnail_files_mtime(job_id: str) -> float:
    try:
        from long_form import pipeline as lf_pipeline

        thumb_dir = lf_pipeline._ensure_job_dir(job_id) / "thumbnails"
        if not thumb_dir.is_dir():
            return 0.0
        mtimes = [p.stat().st_mtime for p in thumb_dir.iterdir() if p.is_file()]
        return max(mtimes) if mtimes else 0.0
    except Exception:
        return 0.0


def _longform_job_is_dead_stale(snap: dict[str, Any]) -> bool:
    if snap.get("thumbnail_only"):
        return False
    if str(snap.get("status") or "") != "running":
        return False
    if int(snap.get("progress") or 0) > 0:
        return False
    phase = str(snap.get("stage") or "").lower()
    label = str(snap.get("stage_label") or "").lower()
    return phase in {"", "unknown"} or label == "unknown"


def reconcile_thumbnail_only_active_jobs(
    session_id: str,
    *,
    user_id: str | None = None,
    session: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Remove legacy thumbnail tracks and preserve the newest thumbnail review."""
    from studio_agent import store

    if session is None:
        session = store.get_session(session_id, user_id=user_id, reconcile_jobs=False)
    if not session:
        return None
    kept: list[dict[str, Any]] = []
    reviews: list[tuple[float, dict[str, Any]]] = []
    seen_review_jobs: set[str] = set()

    def _maybe_review(job_id: str, title: str, started_at: float = 0.0) -> None:
        if not job_id or job_id in seen_review_jobs:
            return
        snap = get_job_snapshot(job_id, "longform", lightweight=True)
        urls = list(snap.get("thumbnail_urls") or [])
        if snap.get("thumbnail_only") and urls:
            seen_review_jobs.add(job_id)
            recency = max(
                float(started_at or 0),
                float(snap.get("polled_at") or 0),
                _thumbnail_files_mtime(job_id),
            )
            reviews.append((recency, {
                "review_id": job_id,
                "job_id": job_id,
                "title": str(snap.get("title") or title or "Thumbnail review"),
                "candidate_urls": urls,
                "updated_at": time.time(),
            }))

    for track in list(session.get("active_jobs") or []):
        if not isinstance(track, dict):
            continue
        job_id = str(track.get("job_id") or "").strip()
        if str(track.get("kind") or "") == "longform" and job_id:
            _maybe_review(job_id, str(track.get("title") or ""), float(track.get("started_at") or 0))
            if job_id in seen_review_jobs:
                continue
            snap = get_job_snapshot(job_id, "longform", lightweight=True)
            if _longform_job_is_dead_stale(snap):
                continue
            if snap.get("status") == "failed" and longform_failed_is_terminal(job_id):
                continue
        kept.append(track)

    for job_id in _longform_job_ids_from_session(session):
        _maybe_review(job_id, "", 0.0)

    if not reviews and kept == list(session.get("active_jobs") or []):
        return session
    updates: dict[str, Any] = {}
    if kept != list(session.get("active_jobs") or []):
        updates["active_jobs"] = kept
    if reviews:
        reviews.sort(key=lambda item: item[0])
        updates["thumbnail_review"] = reviews[-1][1]
    return store.update_session(session_id, **updates) or session


def _resolve_poll_kind(job_id: str, kind: str) -> str:
    """Prefer durable workspace markers over client-supplied kind (avoids shortform misroutes)."""
    jid = str(job_id or "").strip()
    requested = str(kind or "longform").strip().lower()
    if not jid:
        return requested or "longform"
    if jid.startswith(("clipi_", "clipa_", "clipr_", "remix_")):
        return "cliplab"
    try:
        from studio_agent.competitor import WORK_ROOT

        competitor_workspace = (WORK_ROOT / jid).resolve()
        if competitor_workspace.is_dir():
            return "competitor"
    except Exception:
        pass
    shortform_workspace = (ROOT / SKELETON_OUTPUT / jid).resolve()
    if shortform_workspace.is_dir():
        return "shortform"
    return requested if requested in {"shortform", "longform", "competitor", "cliplab"} else "shortform"


def get_job_snapshot(job_id: str, kind: str, *, lightweight: bool = False) -> dict[str, Any]:
    kind = _resolve_poll_kind(job_id, kind)
    # A durable RunPod receipt transfers execution ownership away from this
    # control-plane process.  Check it before local status helpers: short-form
    # local polling can reclaim stale jobs, which would duplicate billable work
    # while the RunPod job is already queued/running.
    try:
        from studio_agent.runpod_reconciliation import project_runpod_job_snapshot

        runpod_snap = project_runpod_job_snapshot(
            job_id,
            kind,
            {
                "job_id": job_id,
                "kind": kind,
                "status": "queued",
                "stage": "runpod_dispatch",
                "stage_label": "Checking RunPod production",
                "progress": 0,
                "running": True,
            },
        )
        if "runpod" in runpod_snap or runpod_snap.get("execution_backend") == "runpod_serverless":
            runpod_snap["polled_at"] = time.time()
            return runpod_snap
    except Exception:
        # If RunPod is disabled or no receipt exists, normal local status below
        # remains authoritative. Projection itself normally converts errors to
        # annotations, so this is only a final defensive boundary.
        pass
    try:
        if kind == "shortform":
            snap = _shortform_status(job_id)
        elif kind == "competitor":
            snap = _competitor_status(job_id)
        elif kind == "cliplab":
            snap = _cliplab_status(job_id)
        else:
            snap = _longform_status(job_id)
        if not isinstance(snap, dict):
            snap = {
                "job_id": job_id,
                "kind": kind,
                "status": "failed",
                "error": f"status snapshot was {type(snap).__name__}, expected dict",
                "progress": 0,
                "running": False,
            }
        if snap.get("status") == "complete" and kind in {"shortform", "longform"} and not lightweight:
            _attach_render_qa(snap, job_id, kind)
        if kind == "shortform":
            try:
                snap["production_slots"] = slot_snapshot()
            except Exception:
                pass
        snap["kind"] = kind
        snap["polled_at"] = time.time()
        try:
            from studio_agent.runpod_reconciliation import project_runpod_job_snapshot

            snap = project_runpod_job_snapshot(job_id, kind, snap)
        except Exception as exc:
            # A RunPod control-plane projection must never make the normal
            # Studio status endpoint unavailable.
            snap["runpod"] = {
                "status": "projection_unavailable",
                "detail": str(exc)[:240],
            }
        snap["polled_at"] = time.time()
        return snap
    except Exception as exc:
        # Never let status polling surface raw AttributeError to the chat banner.
        snap = {
            "job_id": job_id,
            "kind": kind,
            "status": "failed",
            "error": str(exc)[:240],
            "progress": 0,
            "running": False,
            "stage": "error",
            "stage_label": "Status error",
            "polled_at": time.time(),
        }
        try:
            # A RunPod-only job intentionally has no local workspace.  Receipt
            # lookup and GET /status can still replace this local error with
            # the worker's live progress without enqueuing anything.
            from studio_agent.runpod_reconciliation import project_runpod_job_snapshot

            snap = project_runpod_job_snapshot(job_id, kind, snap)
        except Exception as projection_exc:
            snap["runpod"] = {
                "status": "projection_unavailable",
                "detail": str(projection_exc)[:240],
            }
        snap["polled_at"] = time.time()
        return snap


def _attach_render_qa(snap: dict[str, Any], job_id: str, kind: str) -> None:
    """Attach cached deterministic QA without allowing QA failures to break polling."""
    if not isinstance(snap, dict):
        return
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
        if not isinstance(report, dict):
            report = {
                "version": 1,
                "job_id": job_id,
                "kind": kind,
                "status": "warn",
                "score": 0,
                "summary": "QA returned a non-dict report",
                "checks": [],
            }
        snap["render_qa"] = report
        ready = report.get("status") == "pass"
        # Short-form: also require visual identity QA (not just ffprobe).
        if str(kind or "") == "shortform":
            try:
                from studio_agent import visual_qa

                ws = (ROOT / SKELETON_OUTPUT / job_id).resolve()
                vq = visual_qa.analyze_shortform_workspace(ws)
                snap["visual_qa"] = vq
                if isinstance(vq, dict):
                    if vq.get("status") == "fail" or vq.get("ready_to_publish") is False:
                        ready = False
                    # Surface worst summary
                    if vq.get("status") != "pass":
                        snap["visual_qa_summary"] = vq.get("summary")
            except Exception as vq_exc:
                ready = False
                snap["visual_qa"] = {
                    "status": "fail",
                    "ready_to_publish": False,
                    "summary": f"Visual QA unavailable: {str(vq_exc)[:200]}",
                }
        snap["ready_to_post"] = ready
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
    try:
        from long_form.prompts.channels import get_channel

        channel = get_channel(str(state.get("channel_key") or ""))
        tail = str(channel.get("description_tail") or "").strip()
        if tail and tail not in description:
            description = f"{description}{tail}"
    except Exception:
        pass
    promotion = upload_package_promotion(
        format_kind="longform",
        mode=promotion_mode_from_metadata(outline),
    )
    chapters_path = job_dir / "chapters.json"
    chapters_data: list[dict] = []
    if chapters_path.is_file():
        try:
            loaded = json.loads(chapters_path.read_text(encoding="utf-8"))
            chapters_data = list(loaded.get("chapters") or [])
        except Exception:
            chapters_data = []
    audio_dir = job_dir / "audio"
    timestamps: list[str] = []
    cursor = 0.0
    source_chapters = chapters_data or (
        outline.get("chapters") if isinstance(outline.get("chapters"), list) else []
    )
    for idx, ch in enumerate(source_chapters):
        if not isinstance(ch, dict):
            continue
        ch_idx = int(ch.get("chapter_index", idx))
        ch_title = str(ch.get("title") or ch.get("name") or f"Chapter {idx + 1}").strip()
        timestamps.append(f"{_format_ts(cursor)} - {ch_title}")
        chapter_mp3 = audio_dir / f"chapter_{ch_idx:02d}.mp3"
        if chapter_mp3.is_file():
            try:
                from long_form.pipeline import _ffprobe_dur

                cursor += max(0.0, float(_ffprobe_dur(chapter_mp3)))
                continue
            except Exception:
                pass
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

Studio promotion:
{promotion or "Disabled for this production."}
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
    promotion = upload_package_promotion(
        format_kind="shortform",
        mode=promotion_mode_from_metadata(spec),
    )
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
    if category.lower() in {"male_psychology", "psychology", "dark_psychology", "relationship"} or "men" in title.lower():
        tags.extend(["psychology", "relationship psychology", "self sabotage", "attachment style"])
        hashtags.extend(["#psychology", "#relationshippsychology", "#selfsabotage"])
    psychology_topic = (
        category.lower() in {"male_psychology", "psychology", "dark_psychology", "relationship"}
        or any(mark in title.lower() for mark in ("men", "women", "love", "relationship", "psychology", "self-sabotage", "self sabotage", "attachment"))
    )
    if psychology_topic:
        description = (
            "Why do people sabotage love right when it starts to feel real?\n\n"
            f"This short breaks down the hidden fear response behind {title.lower()}.\n\n"
            f"Follow {watermark_text or 'Studio'} for psychology shorts about attachment, silence, avoidance, and the patterns people rarely say out loud."
        )
    else:
        description = (
            f"{title}\n\n"
            "A fast, tightly edited short. Follow for more visual stories, sharp hooks, and creator-first experiments."
        )
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
{description}

Timestamps:
{chr(10).join(timestamps) if timestamps else "00:00 - Full short"}

Tags:
{", ".join(dict.fromkeys(t for t in tags if t))}

Hashtags:
{" ".join(dict.fromkeys(h for h in hashtags if h))}

Thumbnail:
Not generated for short-form by default. Use the strongest frame/cover from the finished Short unless the user explicitly asks for a custom thumbnail.

Studio promotion:
{promotion or "Disabled for this production."}
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
    try:
        from studio_agent import continuous_evaluation

        continuous_evaluation.record_evidence(
            session={"session_id": session_id or "", "render_style": snap.get("render_style") or ""},
            event_type="production_complete",
            outcome="success",
            evidence={"job_id": jid, "kind": kind, "status": "complete"},
        )
    except Exception:
        pass
