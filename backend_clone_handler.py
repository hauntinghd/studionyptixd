"""Clone route handler for the Studio API."""

from __future__ import annotations

import random
import time
from pathlib import Path

from fastapi import HTTPException

from queue_manager import QueueFullError


def build_clone_video_handler(
    *,
    xai_api_key: str,
    elevenlabs_api_key: str,
    get_current_user_from_request,
    user_has_paid_access,
    normalize_output_resolution,
    normalize_external_source_url,
    temp_dir: Path,
    jobs_ref: dict,
    enqueue_generation_job,
    run_clone_pipeline,
    persist_job_state,
):
    async def clone_video(
        topic: str = "",
        resolution: str = "720p",
        source_url: str = "",
        analytics_notes: str = "",
        file=None,
        background_tasks=None,
        request=None,
    ):
        if not xai_api_key or not elevenlabs_api_key:
            raise HTTPException(500, "API keys not configured")

        user = await get_current_user_from_request(request) if request else None
        if not user:
            raise HTTPException(401, "Auth required")
        if not user_has_paid_access(user):
            raise HTTPException(402, "Active subscription required. Please choose a plan.")

        res = normalize_output_resolution(resolution, priority_allowed=False)
        normalized_source_url = normalize_external_source_url(source_url)
        if not str(topic or "").strip() and not normalized_source_url and not (file and file.filename):
            raise HTTPException(400, "Provide a new topic, a source URL, or an uploaded source video.")

        video_path = None
        if file and file.filename:
            video_path = str(temp_dir / f"clone_upload_{int(time.time())}.mp4")
            with open(video_path, "wb") as handle:
                while chunk := await file.read(1024 * 1024):
                    handle.write(chunk)

        job_id = f"clone_{int(time.time())}_{random.randint(1000, 9999)}"
        jobs_ref[job_id] = {
            "status": "queued",
            "progress": 0,
            "template": "analyzing...",
            "topic": topic,
            "source_url": normalized_source_url,
            "lane": "clone",
            "mode": "clone_rebuild",
            "resolution": res,
            "credit_cost": 0,
            "billing_source": "workspace_access",
            "user_id": str(user.get("id", "") or ""),
            "created_at": time.time(),
        }

        try:
            await enqueue_generation_job(
                job_id,
                "starter",
                run_clone_pipeline,
                (job_id, topic, video_path, normalized_source_url, analytics_notes, res),
            )
        except QueueFullError as exc:
            jobs_ref[job_id]["status"] = "error"
            jobs_ref[job_id]["error"] = str(exc)
            await persist_job_state(job_id, jobs_ref[job_id])
            raise HTTPException(429, str(exc))
        return {"status": "accepted", "job_id": job_id}

    return clone_video
