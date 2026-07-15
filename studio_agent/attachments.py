"""Persisted Studio Agent chat attachments (reference videos, images)."""
from __future__ import annotations

import os
import re
import secrets
import time
from pathlib import Path
from typing import Any

from upload_limits import MAX_STUDIO_ATTACHMENT_VIDEO_BYTES, UploadTooLargeError, write_upload_limited

VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm", ".m4v"}
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}
MAX_IMAGE_ATTACHMENT_BYTES = 12 * 1024 * 1024
MAX_VIDEO_ATTACHMENT_BYTES = MAX_STUDIO_ATTACHMENT_VIDEO_BYTES

ROOT = Path(__file__).resolve().parents[1]
APP_DATA_DIR = Path(os.getenv("APP_DATA_DIR", str(ROOT / "data")))
LEGACY_SKELETON_OUTPUT = Path(os.getenv("SKELETON_AI_OUTPUT_ROOT", str(ROOT / "skeleton_ai" / "output")))


def _safe_session_key(session_id: str) -> str:
    return "".join(c for c in str(session_id or "") if c.isalnum() or c in "-_")[:80]


def session_attachment_dir(session_id: str) -> Path:
    key = _safe_session_key(session_id)
    if not key:
        raise ValueError("invalid session id")
    target = (APP_DATA_DIR / "studio_agent_attachments" / key).resolve()
    target.mkdir(parents=True, exist_ok=True)
    return target


def legacy_session_input_dir(session_id: str) -> Path:
    key = _safe_session_key(session_id)
    if not key:
        raise ValueError("invalid session id")
    target = (LEGACY_SKELETON_OUTPUT / "_session_inputs" / key).resolve()
    target.mkdir(parents=True, exist_ok=True)
    return target


def _attachment_filename(prefix: str, ext: str) -> str:
    return f"{prefix}_{int(time.time() * 1000)}_{secrets.token_hex(4)}{ext}"


def save_image_attachment(session_id: str, filename: str, payload: bytes) -> dict[str, str | int]:
    ext = Path(filename).suffix.lower() or ".png"
    if ext not in IMAGE_EXTENSIONS:
        raise ValueError("unsupported image format")
    if not payload:
        raise ValueError("empty image file")
    if len(payload) > MAX_IMAGE_ATTACHMENT_BYTES:
        raise UploadTooLargeError("image attachment exceeds 12MB")
    target_dir = session_attachment_dir(session_id)
    dest = target_dir / _attachment_filename("agent_image", ext)
    dest.write_bytes(payload)
    resolved = str(dest.resolve())
    mime = "image/png"
    if ext in {".jpg", ".jpeg"}:
        mime = "image/jpeg"
    elif ext == ".webp":
        mime = "image/webp"
    return {
        "path": resolved,
        "name": str(filename or dest.name),
        "size": len(payload),
        "mime_type": mime,
    }


def save_video_attachment(session_id: str, filename: str, payload: bytes) -> dict[str, str | int]:
    ext = Path(filename).suffix.lower() or ".mp4"
    if ext not in VIDEO_EXTENSIONS:
        raise ValueError("unsupported video format")
    if not payload:
        raise ValueError("empty video file")
    if len(payload) > MAX_VIDEO_ATTACHMENT_BYTES:
        raise UploadTooLargeError("video attachment exceeds 3GB")
    target_dir = session_attachment_dir(session_id)
    dest = target_dir / _attachment_filename("agent_video", ext)
    dest.write_bytes(payload)
    resolved = str(dest.resolve())
    return {
        "path": resolved,
        "name": str(filename or dest.name),
        "size": len(payload),
        "mime_type": "video/mp4" if ext == ".mp4" else f"video/{ext.lstrip('.')}",
    }


async def save_video_upload_attachment(
    session_id: str,
    filename: str,
    upload: Any,
) -> dict[str, str | int]:
    """Stream a Studio video attachment to its session directory.

    Router integration should use this instead of ``await file.read()`` so a
    malicious multipart upload cannot force an unbounded in-memory allocation.
    """
    ext = Path(filename).suffix.lower() or ".mp4"
    if ext not in VIDEO_EXTENSIONS:
        raise ValueError("unsupported video format")
    target_dir = session_attachment_dir(session_id)
    dest = target_dir / _attachment_filename("agent_video", ext)
    size = await write_upload_limited(
        upload,
        dest,
        max_bytes=MAX_VIDEO_ATTACHMENT_BYTES,
        label="video attachment",
    )
    return {
        "path": str(dest.resolve()),
        "name": str(filename or dest.name),
        "size": size,
        "mime_type": "video/mp4" if ext == ".mp4" else f"video/{ext.lstrip('.')}",
    }


def _is_video_file(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in VIDEO_EXTENSIONS


def _is_within(path: Path, roots: tuple[Path, ...]) -> bool:
    for root in roots:
        try:
            path.relative_to(root)
            return True
        except ValueError:
            continue
    return False


def _paths_from_messages(messages: list[dict[str, Any]] | None) -> list[str]:
    """Recover persisted upload paths referenced in prior chat turns."""
    found: list[str] = []
    seen: set[str] = set()
    for msg in reversed(list(messages or [])):
        text = str(msg.get("content") or "")
        if "agent_video_" not in text and "studio_agent_attachments" not in text:
            continue
        for match in re.finditer(
            r"(?:local_path:\s*|studio_agent_attachments[/\\][^\s\"']+agent_video_[^\s\"']+\.(?:mp4|mov|mkv|webm|m4v))",
            text,
            flags=re.I,
        ):
            raw = str(match.group(0) or "").strip()
            if raw.lower().startswith("local_path:"):
                raw = raw.split(":", 1)[1].strip()
            raw = raw.strip("\"'")
            if not raw or raw in seen:
                continue
            seen.add(raw)
            found.append(raw)
    return found


def resolve_video_attachment_path(
    session_id: str | None,
    user_id: str,
    *,
    hint: str = "",
    messages: list[dict[str, Any]] | None = None,
) -> str:
    # Resolving an attachment is an authenticated, session-owned operation. Do
    # not accept a raw path when there is no owned session to anchor it to.
    if not session_id or not str(user_id or "").strip():
        return ""

    from studio_agent import store

    session = store.get_session(str(session_id), user_id=str(user_id or "")) or {}
    if not session:
        return ""

    allowed_roots = (
        session_attachment_dir(str(session_id)).resolve(),
        legacy_session_input_dir(str(session_id)).resolve(),
    )
    candidates: list[Path] = []
    hint_path = Path(str(hint or "").strip())
    if str(hint or "").strip():
        candidates.append(hint_path)
        if not hint_path.is_absolute():
            candidates.append((ROOT / hint_path).resolve())

    for raw in _paths_from_messages(messages):
        candidates.append(Path(str(raw or "")))

    session_paths = list(session.get("latest_attachment_paths") or [])
    for raw in reversed(session_paths):
        candidates.append(Path(str(raw or "")))

    for folder in allowed_roots:
        if folder.is_dir():
            for path in sorted(folder.glob("agent_video_*"), key=lambda p: p.stat().st_mtime, reverse=True):
                candidates.append(path)

    seen: set[str] = set()
    for raw in candidates:
        try:
            path = raw.resolve()
        except Exception:
            continue
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        if _is_within(path, allowed_roots) and _is_video_file(path):
            return key
    return ""
