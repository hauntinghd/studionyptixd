"""Media response and render handlers for the Studio API."""

from __future__ import annotations

import asyncio
import json
import random
import re
import shutil
import sys
import time
from pathlib import Path
from urllib.parse import quote

from fastapi import HTTPException
from fastapi.responses import FileResponse

from upload_limits import UploadTooLargeError, write_upload_limited


MAX_CHAT_STORY_PAYLOAD_BYTES = 1024 * 1024
MAX_CHAT_STORY_AVATAR_BYTES = 12 * 1024 * 1024
MAX_CHAT_STORY_BACKGROUND_BYTES = 512 * 1024 * 1024
CHAT_STORY_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}
CHAT_STORY_VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm", ".m4v"}

from backend_job_payloads import job_access_allowed


def validate_job_id_component(job_id: str) -> str:
    normalized = str(job_id or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", normalized):
        raise ValueError("Invalid job id")
    return normalized


def resolve_auto_scene_directory(root: Path, job_id: str, *, create: bool) -> Path:
    directory = root / validate_job_id_component(job_id)
    if create:
        directory.mkdir(parents=True, exist_ok=True)
    return directory


def _job_id_candidates_for_output(filename: str) -> list[str]:
    stem = Path(filename).stem
    parts = [part for part in stem.split("_") if part]
    candidates: list[str] = []
    for index in range(len(parts)):
        candidate = "_".join(parts[index:])
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    return candidates[-8:]


def _job_output_matches(job: dict, filename: str) -> bool:
    output_file = str(job.get("output_file", "") or "").strip()
    return bool(output_file and Path(output_file).name == filename)


def build_download_video_response(
    *,
    output_dir: Path,
    jobs_ref: dict,
    get_persisted_job_state,
    admin_emails: set[str],
    export_access_check=None,
):
    async def download_video_response(filename: str, *, user: dict):
        caller_is_admin = job_access_allowed({}, user, admin_emails)
        if not caller_is_admin and export_access_check is not None:
            try:
                export_allowed = bool(export_access_check(user))
            except Exception:
                export_allowed = False
            if not export_allowed:
                raise HTTPException(403, "Final video export is disabled for controlled-beta accounts.")
        safe_filename = Path(filename).name
        if not safe_filename or safe_filename != filename:
            raise HTTPException(400, "Invalid filename")
        path = output_dir / safe_filename

        if not caller_is_admin:
            matching_jobs = [
                job
                for job in jobs_ref.values()
                if isinstance(job, dict) and _job_output_matches(job, safe_filename)
            ]
            allowed = any(job_access_allowed(job, user, admin_emails) for job in matching_jobs)
            if not allowed:
                for job_id in _job_id_candidates_for_output(safe_filename):
                    try:
                        persisted = await get_persisted_job_state(job_id)
                    except Exception:
                        persisted = None
                    if not isinstance(persisted, dict) or not _job_output_matches(persisted, safe_filename):
                        continue
                    allowed = job_access_allowed(persisted, user, admin_emails)
                    break
            if not allowed:
                # Use the same response as a genuinely missing file so callers
                # cannot enumerate another account's generated media.
                raise HTTPException(404, "Video not found")
        if not path.exists():
            raise HTTPException(404, "Video not found")
        return FileResponse(str(path), media_type="video/mp4", filename=safe_filename)

    return download_video_response


def build_render_chat_story_handler(
    *,
    get_current_user_from_request,
    chat_story_access_for_user,
    is_admin_user,
    temp_dir: Path,
    output_dir: Path,
    render_script_path: Path,
    log,
    jobs_ref: dict | None = None,
    persist_job_state=None,
):
    async def render_chat_story(
        request,
        payload: str,
        avatar=None,
        background_video=None,
    ):
        user = await get_current_user_from_request(request)
        if not user:
            raise HTTPException(401, "Authentication required")
        if not chat_story_access_for_user(user):
            raise HTTPException(403, "Chat Story requires an active Starter, Creator, or Pro monthly plan.")

        if len(str(payload or "").encode("utf-8")) > MAX_CHAT_STORY_PAYLOAD_BYTES:
            raise HTTPException(413, "Chat story payload exceeds 1MB")

        try:
            parsed_payload = json.loads(payload or "{}")
        except Exception:
            raise HTTPException(400, "Invalid chat story payload")

        messages = parsed_payload.get("messages") or []
        has_text = any(
            str(item.get("text", "") or "").strip()
            for item in messages
            if isinstance(item, dict)
        )
        if not isinstance(messages, list) or not has_text:
            raise HTTPException(400, "Add at least one chat message before rendering.")

        render_id = f"chatstory_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
        work_dir = temp_dir / render_id
        work_dir.mkdir(parents=True, exist_ok=True)
        avatar_path = ""
        bg_video_path = ""

        try:
            if avatar and avatar.filename:
                avatar_ext = Path(str(avatar.filename)).suffix.lower() or ".png"
                if avatar_ext not in CHAT_STORY_IMAGE_EXTENSIONS:
                    raise HTTPException(400, "Unsupported avatar image format")
                avatar_path = str(work_dir / f"avatar{avatar_ext}")
                try:
                    await write_upload_limited(
                        avatar,
                        Path(avatar_path),
                        max_bytes=MAX_CHAT_STORY_AVATAR_BYTES,
                        label="chat story avatar",
                    )
                except UploadTooLargeError as exc:
                    raise HTTPException(413, "Chat story avatar exceeds 12MB") from exc
                except ValueError as exc:
                    raise HTTPException(400, str(exc)) from exc

            if background_video and background_video.filename:
                bg_ext = Path(str(background_video.filename)).suffix.lower() or ".mp4"
                if bg_ext not in CHAT_STORY_VIDEO_EXTENSIONS:
                    raise HTTPException(400, "Unsupported chat story background video format")
                bg_video_path = str(work_dir / f"background{bg_ext}")
                try:
                    await write_upload_limited(
                        background_video,
                        Path(bg_video_path),
                        max_bytes=MAX_CHAT_STORY_BACKGROUND_BYTES,
                        label="chat story background video",
                    )
                except UploadTooLargeError as exc:
                    raise HTTPException(413, "Chat story background video exceeds 512MB") from exc
                except ValueError as exc:
                    raise HTTPException(400, str(exc)) from exc

            payload_path = work_dir / "payload.json"
            payload_path.write_text(json.dumps(parsed_payload, ensure_ascii=False, indent=2), encoding="utf-8")

            output_name = f"{render_id}.mp4"
            output_path = output_dir / output_name

            proc = await asyncio.create_subprocess_exec(
                sys.executable,
                str(render_script_path),
                "--payload",
                str(payload_path),
                "--output",
                str(output_path),
                "--avatar",
                avatar_path,
                "--background-video",
                bg_video_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode != 0:
                log.error("Chat Story render failed: %s", (stderr or b"").decode("utf-8", errors="replace"))
                raise HTTPException(500, "Chat Story render failed.")

            meta = {}
            try:
                meta = json.loads((stdout or b"{}").decode("utf-8", errors="replace").strip() or "{}")
            except Exception:
                meta = {}

            if not output_path.exists():
                raise HTTPException(500, "Chat Story render did not produce an output video.")

            if jobs_ref is not None:
                job_state = {
                    "status": "complete",
                    "progress": 100,
                    "lane": "chatstory",
                    "mode": "chatstory_render",
                    "user_id": str(user.get("id", "") or ""),
                    "output_file": output_name,
                    "created_at": time.time(),
                }
                jobs_ref[render_id] = job_state
                if persist_job_state is not None:
                    try:
                        await persist_job_state(render_id, job_state)
                    except Exception as exc:
                        log.warning("Chat Story output ownership persistence failed for %s: %s", render_id, exc)

            return {
                "ok": True,
                "output_file": output_name,
                "download_url": f"/api/download/{quote(output_name)}",
                "lane": "chatstory",
                "mode": "chatstory_render",
                "credit_cost": 0,
                "billing_source": "workspace_access" if not is_admin_user(user) else "owner_override",
                "duration_sec": meta.get("duration_sec"),
                "message_count": meta.get("message_count"),
                "voice": meta.get("voice"),
                "theme": meta.get("theme"),
                "background": meta.get("background"),
                "used_background_video": bool(meta.get("used_background_video")),
            }
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)

    return render_chat_story
