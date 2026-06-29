"""Media response and render handlers for the Studio API."""

from __future__ import annotations

import asyncio
import json
import random
import shutil
import sys
import time
from pathlib import Path
from urllib.parse import quote

from fastapi import HTTPException
from fastapi.responses import FileResponse


def build_download_video_response(*, output_dir: Path):
    async def download_video_response(filename: str):
        safe_filename = Path(filename).name
        if not safe_filename:
            raise HTTPException(400, "Invalid filename")
        path = output_dir / safe_filename
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
                avatar_ext = Path(avatar.filename).suffix or ".png"
                avatar_path = str(work_dir / f"avatar{avatar_ext}")
                with open(avatar_path, "wb") as handle:
                    while chunk := await avatar.read(1024 * 1024):
                        handle.write(chunk)

            if background_video and background_video.filename:
                bg_ext = Path(background_video.filename).suffix or ".mp4"
                bg_video_path = str(work_dir / f"background{bg_ext}")
                with open(bg_video_path, "wb") as handle:
                    while chunk := await background_video.read(1024 * 1024):
                        handle.write(chunk)

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
