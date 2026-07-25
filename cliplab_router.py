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
import time
from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse

from cliplab.config import (
    CLIPLAB_MAX_RENDER_CLIPS,
    CLIPLAB_UPLOAD_DIR,
)
from cliplab.model_registry import registry_status
from cliplab.models import ClipLabAnalyzeRequest, ClipLabFeedbackRequest, ClipLabIngestRequest, ClipLabRenderRequest
from cliplab.pipeline import (
    credits_for_duration,
    load_job_state,
    new_job_id,
    run_analyze_pipeline,
    run_ingest_pipeline,
    run_render_pipeline,
    resolve_owned_clip_path,
    user_owns_video,
    _safe_user_dir,
)
from cliplab.transcribe import probe_duration
from upload_limits import MAX_CLIPLAB_VIDEO_BYTES, UploadTooLargeError, write_upload_limited
from studio_agent.direct_production import (
    claim_direct_production,
    require_idempotency_key,
    upload_content_contract,
)

_log = logging.getLogger("nyptid-studio.cliplab.router")


def build_cliplab_router(
    *,
    require_auth: Callable,
    jobs: dict[str, dict[str, Any]],
    fal_json_completion: Callable,
    fal_ai_key: str = "",
    debit_credits: Callable | None = None,
    refund_credits: Callable | None = None,
) -> APIRouter:
    router = APIRouter(tags=["cliplab"])

    @router.get("/api/cliplab/status")
    async def cliplab_status(user: dict = Depends(require_auth)):
        return {"ok": True, **registry_status()}

    @router.post("/api/cliplab/ingest/upload")
    async def cliplab_upload(
        background_tasks: BackgroundTasks,
        request: Request,
        file: UploadFile = File(...),
        user: dict = Depends(require_auth),
    ):
        if not file or not file.filename:
            raise HTTPException(400, "No video file")
        ext = Path(file.filename).suffix.lower() or ".mp4"
        if ext not in {".mp4", ".mov", ".mkv", ".webm", ".m4v"}:
            raise HTTPException(400, "Unsupported format")
        user_id = str(user.get("id") or "")
        require_idempotency_key(request)
        upload_contract = await upload_content_contract(file)
        with claim_direct_production(
            "ingest_cliplab_attachment",
            {
                "source": "upload",
                "upload": upload_contract,
            },
            request=request,
            user_id=user_id,
            content_format="long",
        ) as command:
            if command.replay is not None:
                return command.replay
            upload_id = new_job_id("clipvid").replace("clipvid_", "vid_")
            out_dir = CLIPLAB_UPLOAD_DIR / _safe_user_dir(user)
            out_dir.mkdir(parents=True, exist_ok=True)
            dest = out_dir / f"{upload_id}{ext}"
            try:
                size = await write_upload_limited(
                    file,
                    dest,
                    max_bytes=MAX_CLIPLAB_VIDEO_BYTES,
                    label="ClipLab source video",
                )
            except UploadTooLargeError as exc:
                raise HTTPException(413, "ClipLab source video exceeds 2GB") from exc
            except ValueError as exc:
                raise HTTPException(400, str(exc)) from exc
            try:
                duration = probe_duration(str(dest))
            except Exception as exc:
                dest.unlink(missing_ok=True)
                raise HTTPException(400, "Uploaded file is not a readable video") from exc
            credit_cost = credits_for_duration(duration)
            if debit_credits and user_id:
                try:
                    ok = debit_credits(user_id, credit_cost, "cliplab_ingest", {"upload_id": upload_id})
                except Exception:
                    dest.unlink(missing_ok=True)
                    raise
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
            return command.complete({
                "status": "accepted",
                "job_id": job_id,
                "video_id": upload_id,
                "duration_sec": duration,
                "credit_cost": credit_cost,
                "uploaded_bytes": size,
            })

    @router.post("/api/cliplab/ingest/youtube")
    async def cliplab_youtube(
        req: ClipLabIngestRequest,
        request: Request,
        background_tasks: BackgroundTasks,
        user: dict = Depends(require_auth),
    ):
        url = str(req.youtube_url or "").strip()
        if not url:
            raise HTTPException(400, "youtube_url required")
        try:
            import yt_dlp
        except ImportError:
            raise HTTPException(500, "yt-dlp not available")
        user_id = str(user.get("id") or "")
        with claim_direct_production(
            "ingest_cliplab_attachment",
            {"source": "youtube", "youtube_url": url},
            request=request,
            user_id=user_id,
            content_format="long",
        ) as command:
            if command.replay is not None:
                return command.replay
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
            if debit_credits and user_id:
                ok = debit_credits(user_id, credit_cost, "cliplab_ingest", {"video_id": upload_id})
                if not ok:
                    dest.unlink(missing_ok=True)
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
            return command.complete({
                "status": "accepted",
                "job_id": job_id,
                "video_id": upload_id,
                "credit_cost": credit_cost,
            })

    @router.post("/api/cliplab/analyze")
    async def cliplab_analyze(
        req: ClipLabAnalyzeRequest,
        request: Request,
        background_tasks: BackgroundTasks,
        user: dict = Depends(require_auth),
    ):
        user_id = str(user.get("id") or "").strip()
        if not user_owns_video(req.video_id, user_id):
            raise HTTPException(404, "ClipLab video not found")
        with claim_direct_production(
            "analyze_cliplab_video",
            {
                "video_id": req.video_id,
                "prompt": req.prompt,
                "max_segments": req.max_segments,
            },
            request=request,
            user_id=user_id,
            content_format="short",
        ) as command:
            if command.replay is not None:
                return command.replay
            job_id = new_job_id("clipa")
            jobs[job_id] = {
                "status": "queued", "progress": 0, "type": "cliplab_analyze", "lane": "cliplab",
                "video_id": req.video_id, "user_id": user_id,
            }
            background_tasks.add_task(
                run_analyze_pipeline,
                job_id, jobs,
                video_id=req.video_id, prompt=req.prompt, max_segments=req.max_segments,
                json_completion=fal_json_completion,
                user_id=user_id,
                source="cliplab_api",
            )
            return command.complete({"status": "accepted", "job_id": job_id})

    @router.post("/api/cliplab/render")
    async def cliplab_render(
        req: ClipLabRenderRequest,
        request: Request,
        background_tasks: BackgroundTasks,
        user: dict = Depends(require_auth),
    ):
        user_id = str(user.get("id") or "").strip()
        if not user_owns_video(req.video_id, user_id):
            raise HTTPException(404, "ClipLab video not found")
        indices = list(req.segment_indices or [])
        if not indices:
            raise HTTPException(400, "segment_indices required")
        if len(indices) > CLIPLAB_MAX_RENDER_CLIPS:
            raise HTTPException(400, f"Max {CLIPLAB_MAX_RENDER_CLIPS} clips per render")
        analyze_state = load_job_state(req.prompt_run_id)
        if (
            not analyze_state
            or str(analyze_state.get("video_id") or "").strip() != req.video_id
            or str(analyze_state.get("user_id") or "").strip() != user_id
        ):
            raise HTTPException(404, "ClipLab analyze job not found")
        with claim_direct_production(
            "render_cliplab_segments",
            {
                "video_id": req.video_id,
                "analyze_job_id": req.prompt_run_id,
                "segment_indices": indices,
                "burn_captions": req.burn_captions,
            },
            request=request,
            user_id=user_id,
            content_format="short",
        ) as command:
            if command.replay is not None:
                return command.replay
            job_id = new_job_id("clipr")
            jobs[job_id] = {
                "status": "queued", "progress": 0, "type": "cliplab_render", "lane": "cliplab",
                "video_id": req.video_id, "user_id": user_id,
            }
            background_tasks.add_task(
                run_render_pipeline,
                job_id, jobs,
                video_id=req.video_id,
                analyze_job_id=req.prompt_run_id,
                segment_indices=indices,
                burn_captions=req.burn_captions,
                user_id=user_id,
                source="cliplab_api",
            )
            return command.complete({"status": "accepted", "job_id": job_id})

    @router.post("/api/cliplab/feedback")
    async def cliplab_feedback(req: ClipLabFeedbackRequest, user: dict = Depends(require_auth)):
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
        path = resolve_owned_clip_path(video_id, filename, str(user.get("id") or ""))
        if not path:
            raise HTTPException(404, "Clip not found")
        return FileResponse(str(path), media_type="video/mp4")

    return router
