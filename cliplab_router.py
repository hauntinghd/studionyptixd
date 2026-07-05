"""
ClipLab FastAPI router — Opus-style long-to-short inside Studio.

Mount:
    from cliplab_router import build_cliplab_router
    app.include_router(build_cliplab_router(...))
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse

from cliplab.config import (
    CLIPLAB_MAX_RENDER_CLIPS,
    CLIPLAB_UPLOAD_DIR,
    CLIPLAB_RENDER_DIR,
)
from cliplab.model_registry import registry_status
from cliplab.models import ClipLabAnalyzeRequest, ClipLabFeedbackRequest, ClipLabIngestRequest, ClipLabRenderRequest
from cliplab.models import ClipLabRemixRequest
from cliplab.pipeline import (
    credits_for_duration,
    load_job_state,
    new_job_id,
    run_analyze_pipeline,
    run_ingest_pipeline,
    run_render_pipeline,
    run_remix_pipeline,
    save_job_state,
    video_upload_path,
    _safe_user_dir,
)
from cliplab.transcribe import probe_duration

_log = logging.getLogger("nyptid-studio.cliplab.router")


def build_cliplab_router(
    *,
    require_auth: Callable,
    jobs: dict[str, dict[str, Any]],
    fal_json_completion: Callable,
    fal_ai_key: str = "",
    debit_credits: Callable | None = None,
    refund_credits: Callable | None = None,
    is_admin_check: Callable[[dict], bool] | None = None,
) -> APIRouter:
    router = APIRouter(tags=["cliplab"])

    def _is_internal_user(user: dict) -> bool:
        if is_admin_check:
            try:
                return bool(is_admin_check(user))
            except Exception:
                return False
        return bool(
            user.get("owner_override")
            or user.get("is_admin")
            or str(user.get("role") or "").lower() == "admin"
        )

    def _require_internal(user: dict) -> None:
        if not _is_internal_user(user):
            raise HTTPException(403, "ClipLab is internal beta only")

    @router.get("/api/cliplab/status")
    async def cliplab_status(user: dict = Depends(require_auth)):
        _require_internal(user)
        return {"ok": True, "internal_beta": True, **registry_status()}

    @router.post("/api/cliplab/ingest/upload")
    async def cliplab_upload(
        background_tasks: BackgroundTasks,
        file: UploadFile = File(...),
        user: dict = Depends(require_auth),
    ):
        _require_internal(user)
        if not file or not file.filename:
            raise HTTPException(400, "No video file")
        ext = Path(file.filename).suffix.lower() or ".mp4"
        if ext not in {".mp4", ".mov", ".mkv", ".webm", ".m4v"}:
            raise HTTPException(400, "Unsupported format")
        upload_id = new_job_id("clipvid").replace("clipvid_", "vid_")
        out_dir = CLIPLAB_UPLOAD_DIR / _safe_user_dir(user)
        out_dir.mkdir(parents=True, exist_ok=True)
        dest = out_dir / f"{upload_id}{ext}"
        size = 0
        with dest.open("wb") as fh:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                fh.write(chunk)
        duration = probe_duration(str(dest))
        credit_cost = credits_for_duration(duration)
        user_id = str(user.get("id") or "")
        if debit_credits and user_id:
            ok = debit_credits(user_id, credit_cost, "cliplab_ingest", {"upload_id": upload_id})
            if not ok:
                dest.unlink(missing_ok=True)
                raise HTTPException(402, f"Need {credit_cost} credits ({credit_cost} min @ 1 cr/min)")

        job_id = new_job_id("clipi")
        jobs[job_id] = {
            "status": "queued",
            "progress": 0,
            "type": "cliplab_ingest",
            "lane": "cliplab",
            "upload_id": upload_id,
            "credit_cost": credit_cost,
            "user_id": user_id,
            "created_at": time.time(),
        }
        background_tasks.add_task(
            run_ingest_pipeline,
            job_id, jobs, user,
            video_path=str(dest), video_id=upload_id, fal_key=fal_ai_key,
        )
        return {
            "status": "accepted",
            "job_id": job_id,
            "video_id": upload_id,
            "duration_sec": duration,
            "credit_cost": credit_cost,
        }

    @router.post("/api/cliplab/ingest/youtube")
    async def cliplab_youtube(
        req: ClipLabIngestRequest,
        background_tasks: BackgroundTasks,
        user: dict = Depends(require_auth),
    ):
        _require_internal(user)
        url = str(req.youtube_url or "").strip()
        if not url:
            raise HTTPException(400, "youtube_url required")
        try:
            import yt_dlp
        except ImportError:
            raise HTTPException(500, "yt-dlp not available")
        upload_id = new_job_id("clipyt").replace("clipyt_", "yt_")
        out_dir = CLIPLAB_UPLOAD_DIR / _safe_user_dir(user)
        out_dir.mkdir(parents=True, exist_ok=True)
        dest = out_dir / f"{upload_id}.mp4"
        vtt_text = ""
        info: dict = {}
        def _download():
            nonlocal vtt_text, info
            opts = {
                "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
                "outtmpl": str(dest.with_suffix("")),
                "merge_output_format": "mp4",
                "writesubtitles": True,
                "writeautomaticsub": True,
                "subtitleslangs": ["en"],
                "skip_download": False,
            }
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=True) or {}
            for sub_path in dest.parent.glob(f"{upload_id}*.vtt"):
                vtt_text = sub_path.read_text(encoding="utf-8", errors="ignore")
                break
        await asyncio.to_thread(_download)
        if not dest.exists():
            # yt-dlp may omit extension in outtmpl
            for cand in dest.parent.glob(f"{upload_id}*"):
                if cand.suffix == ".mp4":
                    dest = cand
                    break
        if not dest.exists():
            raise HTTPException(400, "Could not download video")
        duration = float(info.get("duration") or probe_duration(str(dest)))
        credit_cost = credits_for_duration(duration)
        user_id = str(user.get("id") or "")
        if debit_credits and user_id:
            ok = debit_credits(user_id, credit_cost, "cliplab_ingest", {"video_id": upload_id})
            if not ok:
                raise HTTPException(402, f"Need {credit_cost} credits")

        job_id = new_job_id("clipi")
        jobs[job_id] = {
            "status": "queued", "progress": 0, "type": "cliplab_ingest", "lane": "cliplab",
            "video_id": upload_id, "credit_cost": credit_cost, "user_id": user_id,
        }
        background_tasks.add_task(
            run_ingest_pipeline,
            job_id, jobs, user,
            video_path=str(dest), video_id=upload_id, vtt_text=vtt_text, fal_key=fal_ai_key,
        )
        return {"status": "accepted", "job_id": job_id, "video_id": upload_id, "credit_cost": credit_cost}

    @router.post("/api/cliplab/analyze")
    async def cliplab_analyze(
        req: ClipLabAnalyzeRequest,
        background_tasks: BackgroundTasks,
        user: dict = Depends(require_auth),
    ):
        _require_internal(user)
        job_id = new_job_id("clipa")
        jobs[job_id] = {
            "status": "queued", "progress": 0, "type": "cliplab_analyze", "lane": "cliplab",
            "video_id": req.video_id, "user_id": str(user.get("id") or ""),
        }
        background_tasks.add_task(
            run_analyze_pipeline,
            job_id, jobs,
            video_id=req.video_id, prompt=req.prompt, max_segments=req.max_segments,
            json_completion=fal_json_completion,
            user_id=str(user.get("id") or ""),
            channel_id=req.channel_id,
            registry_key=req.registry_key,
            source="cliplab_ui",
            provider=req.provider,
        )
        return {"status": "accepted", "job_id": job_id}

    @router.post("/api/cliplab/render")
    async def cliplab_render(
        req: ClipLabRenderRequest,
        background_tasks: BackgroundTasks,
        user: dict = Depends(require_auth),
    ):
        _require_internal(user)
        indices = list(req.segment_indices or [])
        if not indices:
            raise HTTPException(400, "segment_indices required")
        if len(indices) > CLIPLAB_MAX_RENDER_CLIPS:
            raise HTTPException(400, f"Max {CLIPLAB_MAX_RENDER_CLIPS} clips per render")
        job_id = new_job_id("clipr")
        jobs[job_id] = {
            "status": "queued", "progress": 0, "type": "cliplab_render", "lane": "cliplab",
            "video_id": req.video_id, "user_id": str(user.get("id") or ""),
        }
        background_tasks.add_task(
            run_render_pipeline,
            job_id, jobs,
            video_id=req.video_id,
            analyze_job_id=req.prompt_run_id,
            segment_indices=indices,
            burn_captions=req.burn_captions,
            user_id=str(user.get("id") or ""),
            channel_id=req.channel_id,
            registry_key=req.registry_key,
            source="cliplab_ui",
        )
        return {"status": "accepted", "job_id": job_id}

    @router.post("/api/cliplab/remix")
    async def cliplab_remix(
        req: ClipLabRemixRequest,
        background_tasks: BackgroundTasks,
        user: dict = Depends(require_auth),
    ):
        _require_internal(user)
        if not str(req.video_id or "").strip():
            raise HTTPException(400, "video_id required")
        style_preset = str(req.style_preset or "clean_viral").strip().lower()
        caption_style = str(req.caption_style or "bold").strip().lower()
        edit_intensity = str(req.edit_intensity or "medium").strip().lower()
        background_mode = str(req.background_mode or "blur").strip().lower()
        if style_preset not in {"clean_viral", "empire", "empire_magnates", "documentary", "streamer", "high_energy"}:
            style_preset = "clean_viral"
        if caption_style not in {"bold", "minimal", "empire"}:
            caption_style = "bold"
        if edit_intensity not in {"low", "medium", "high"}:
            edit_intensity = "medium"
        if background_mode not in {"blur", "solid"}:
            background_mode = "blur"
        job_id = new_job_id("remix")
        jobs[job_id] = {
            "status": "queued",
            "progress": 0,
            "type": "cliplab_remix",
            "lane": "cliplab",
            "video_id": req.video_id,
            "user_id": str(user.get("id") or ""),
            "style_preset": style_preset,
            "caption_style": caption_style,
            "edit_intensity": edit_intensity,
            "background_mode": background_mode,
            "created_at": time.time(),
        }
        background_tasks.add_task(
            run_remix_pipeline,
            job_id,
            jobs,
            video_id=req.video_id,
            style_preset=style_preset,
            caption_style=caption_style,
            edit_intensity=edit_intensity,
            background_mode=background_mode,
            burn_captions=bool(req.burn_captions),
            catalyst_channel_id=req.catalyst_channel_id,
            notes=req.notes,
        )
        return {"status": "accepted", "job_id": job_id, "video_id": req.video_id}

    @router.post("/api/cliplab/feedback")
    async def cliplab_feedback(req: ClipLabFeedbackRequest, user: dict = Depends(require_auth)):
        _require_internal(user)
        from cliplab.config import CLIPLAB_DATASETS_DIR
        CLIPLAB_DATASETS_DIR.mkdir(parents=True, exist_ok=True)
        row = {
            "clip_id": req.clip_id,
            "kept": req.kept,
            "published": req.published,
            "edited_hook": req.edited_hook,
            "notes": req.notes,
            "user_id": str(user.get("id") or ""),
            "ts": time.time(),
        }
        path = CLIPLAB_DATASETS_DIR / "cliplab_feedback.jsonl"
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row) + "\n")
        return {"ok": True}

    @router.get("/api/cliplab/clips/{video_id}/{filename}")
    async def serve_clip(video_id: str, filename: str, user: dict = Depends(require_auth)):
        _require_internal(user)
        safe_vid = re.sub(r"[^\w\-]", "", video_id)
        safe_name = Path(filename).name
        path = CLIPLAB_RENDER_DIR / safe_vid / safe_name
        if not path.exists():
            raise HTTPException(404, "Clip not found")
        return FileResponse(str(path), media_type="video/mp4")

    return router
