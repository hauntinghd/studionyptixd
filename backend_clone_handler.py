"""Clone route handler for the Studio API."""

from __future__ import annotations

import random
import secrets
import time
from pathlib import Path

from fastapi import HTTPException

from upload_limits import UploadTooLargeError, write_upload_limited


MAX_CLONE_VIDEO_BYTES = 512 * 1024 * 1024
CLONE_VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm", ".m4v"}


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
    queue_full_error,
    run_clone_pipeline,
    persist_job_state,
    resolve_user_plan_for_limits,
    billing_active_for_user,
    is_admin_user,
    reserve_generation_credit,
    refund_generation_credit,
    clone_credit_cost: int,
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

        user_plan, _plan_limits = resolve_user_plan_for_limits(user)
        is_admin = bool(is_admin_user(user))
        credits_required = max(1, int(clone_credit_cost or 1))
        can_render, credit_source, credit_state = await reserve_generation_credit(
            user,
            user_plan if not is_admin else "pro",
            bool(billing_active_for_user(user)),
            is_admin=is_admin,
            usage_kind="animated",
            credits_needed=credits_required,
        )
        if not can_render:
            available = int(credit_state.get("credits_total_remaining", 0) or 0)
            raise HTTPException(
                402,
                f"Style Clone needs {credits_required} Catalyst credits, but only {available} are available.",
            )

        async def refund_reserved_credit() -> None:
            if credit_source not in {"monthly", "topup"}:
                return
            await refund_generation_credit(
                str(user.get("id", "") or ""),
                credit_source,
                month_key=str(credit_state.get("month_key", "") or ""),
                credits=credits_required,
            )

        video_path = None
        if file and file.filename:
            ext = Path(str(file.filename)).suffix.lower() or ".mp4"
            if ext not in CLONE_VIDEO_EXTENSIONS:
                raise HTTPException(400, "Unsupported source video format")
            video_path = str(
                temp_dir / f"clone_upload_{int(time.time() * 1000)}_{secrets.token_hex(4)}{ext}"
            )
            try:
                await write_upload_limited(
                    file,
                    Path(video_path),
                    max_bytes=MAX_CLONE_VIDEO_BYTES,
                    label="clone source video",
                )
            except UploadTooLargeError as exc:
                await refund_reserved_credit()
                raise HTTPException(413, "Clone source video exceeds 512MB") from exc
            except ValueError as exc:
                await refund_reserved_credit()
                raise HTTPException(400, str(exc)) from exc
            except BaseException:
                await refund_reserved_credit()
                raise

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
            "plan": user_plan,
            "credit_charged": True,
            "credit_source": credit_source,
            "credit_amount": credits_required,
            "credit_cost": credits_required,
            "credit_month_key": str(credit_state.get("month_key", "") or ""),
            "credit_refunded": False,
            "billing_source": "owner_override" if is_admin else credit_source,
            "user_id": str(user.get("id", "") or ""),
            "created_at": time.time(),
        }

        try:
            await enqueue_generation_job(
                job_id,
                user_plan,
                run_clone_pipeline,
                (job_id, topic, video_path, normalized_source_url, analytics_notes, res),
            )
        except queue_full_error as exc:
            if video_path:
                Path(video_path).unlink(missing_ok=True)
            await refund_reserved_credit()
            jobs_ref[job_id]["status"] = "error"
            jobs_ref[job_id]["error"] = str(exc)
            jobs_ref[job_id]["credit_refunded"] = credit_source in {"monthly", "topup"}
            await persist_job_state(job_id, jobs_ref[job_id])
            raise HTTPException(429, str(exc))
        except BaseException:
            # No worker accepted this job, so do not strand a large upload in
            # the shared temporary directory.
            if video_path:
                Path(video_path).unlink(missing_ok=True)
            await refund_reserved_credit()
            jobs_ref.pop(job_id, None)
            raise
        return {"status": "accepted", "job_id": job_id}

    return clone_video
