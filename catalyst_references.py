"""
Catalyst Reference Videos — per-user library of inspiration / "winning"
YouTube videos. Studio's generators read these to bias output toward
patterns that already work in the wild.

Phase 1 (this module):
  - yt-dlp pulls metadata for any pasted YT URL
  - Stores into Supabase `catalyst_reference_videos` (migration 2026-05-06)
  - Tagged to a channel_key (long_form or shorts) so each generation pulls
    only the references the user marked as inspiration for THAT channel
  - List / delete / update notes endpoints

Phase 2 (deferred — separate session):
  - Whisper transcription
  - Keyframe vision analysis
  - Grok-decoded pattern_summary (hook structure, pacing, title formula, etc.)
  - Auto-inject summaries into Skeleton AI / Long Form Grok prompts

Usage:
    from catalyst_references import (
        ingest_reference_video,
        list_user_references,
        delete_reference_video,
        update_reference_notes,
    )

    rec = ingest_reference_video(
        user_id=user["id"],
        url="https://youtube.com/shorts/iQo8u5y4BjY",
        channel_key="zerotier",
        notes="Top performer — 'The Time Wally West' format",
    )
"""
from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from typing import Any

import httpx

log = logging.getLogger("nyptid-studio")


SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
TABLE = "catalyst_reference_videos"


_VIDEO_ID_RE = re.compile(
    r"(?:youtube\.com/(?:watch\?v=|shorts/|embed/|v/)|youtu\.be/)([\w-]{11})"
)


def extract_video_id(url: str) -> str:
    """Pull the 11-char video ID from any YT URL form. Empty string if invalid."""
    if not url:
        return ""
    url = url.strip()
    m = _VIDEO_ID_RE.search(url)
    if m:
        return m.group(1)
    if len(url) == 11 and re.fullmatch(r"[\w-]{11}", url):
        return url
    return ""


class ReferenceError(RuntimeError):
    pass


def _supabase_headers() -> dict[str, str]:
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def _ytdlp_metadata(url: str) -> dict[str, Any]:
    """Run `yt-dlp -J` to dump JSON metadata for the URL. No download."""
    bin_path = "yt-dlp"
    try:
        result = subprocess.run(
            [bin_path, "-J", "--no-playlist", "--no-warnings", url],
            capture_output=True, text=True, timeout=60,
        )
    except FileNotFoundError:
        # Fall back to module call if `yt-dlp` not on PATH.
        result = subprocess.run(
            [sys.executable, "-m", "yt_dlp", "-J", "--no-playlist", "--no-warnings", url],
            capture_output=True, text=True, timeout=60,
        )
    if result.returncode != 0:
        raise ReferenceError(
            f"yt-dlp failed ({result.returncode}): {result.stderr.strip()[:300]}"
        )
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise ReferenceError(f"yt-dlp output not JSON: {e}")


def _shape_metadata(meta: dict[str, Any]) -> dict[str, Any]:
    """Trim yt-dlp's huge metadata blob to the columns we store."""
    upload_date = meta.get("upload_date")
    upload_iso = None
    if upload_date and len(str(upload_date)) == 8:
        try:
            upload_iso = (
                f"{str(upload_date)[0:4]}-{str(upload_date)[4:6]}-{str(upload_date)[6:8]}"
            )
        except Exception:
            upload_iso = None

    tags_raw = meta.get("tags") or []
    if not isinstance(tags_raw, list):
        tags_raw = []
    tags = [str(t) for t in tags_raw if isinstance(t, str)][:50]

    return {
        "yt_video_id": str(meta.get("id", ""))[:11],
        "yt_url": str(meta.get("webpage_url") or meta.get("original_url") or ""),
        "title": str(meta.get("title") or "")[:500],
        "description": str(meta.get("description") or "")[:5000],
        "tags": tags,
        "yt_channel_id": str(meta.get("channel_id") or ""),
        "channel_title": str(meta.get("channel") or meta.get("uploader") or "")[:200],
        "duration_sec": int(meta.get("duration") or 0),
        "view_count": int(meta.get("view_count") or 0),
        "like_count": int(meta.get("like_count") or 0),
        "comment_count": int(meta.get("comment_count") or 0),
        "thumbnail_url": str(meta.get("thumbnail") or ""),
        "upload_date": upload_iso,
    }


def ingest_reference_video(
    *, user_id: str, url: str, channel_key: str = "", notes: str = "",
) -> dict[str, Any]:
    """Pull yt-dlp metadata + upsert into Supabase. Returns the stored row."""
    if not user_id:
        raise ReferenceError("user_id required")
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise ReferenceError("supabase config missing (SUPABASE_URL / SUPABASE_SERVICE_KEY)")

    video_id = extract_video_id(url)
    if not video_id:
        raise ReferenceError(f"could not parse YT video id from {url!r}")

    canonical_url = f"https://www.youtube.com/watch?v={video_id}"
    meta = _ytdlp_metadata(canonical_url)
    shaped = _shape_metadata(meta)
    if shaped["yt_video_id"] != video_id:
        # Some yt-dlp versions return shorts as different ids; trust the URL parser.
        shaped["yt_video_id"] = video_id
        shaped["yt_url"] = canonical_url

    row = {
        "user_id": user_id,
        "channel_key": channel_key or "",
        "user_notes": (notes or "")[:1000],
        **shaped,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    with httpx.Client(timeout=30) as c:
        # Upsert on (user_id, yt_video_id) unique index.
        r = c.post(
            f"{SUPABASE_URL}/rest/v1/{TABLE}?on_conflict=user_id,yt_video_id",
            headers={**_supabase_headers(), "Prefer": "return=representation,resolution=merge-duplicates"},
            json=row,
        )
    if r.status_code not in (200, 201):
        raise ReferenceError(f"supabase insert failed {r.status_code}: {r.text[:300]}")
    rows = r.json()
    if not rows:
        raise ReferenceError("supabase insert returned no rows")
    return rows[0]


def list_user_references(user_id: str, channel_key: str | None = None) -> list[dict[str, Any]]:
    """Return all references for a user, newest first.
    channel_key='' (or None) returns everything; specific key filters."""
    if not user_id:
        return []
    params = [f"user_id=eq.{user_id}", "order=created_at.desc"]
    if channel_key is not None and channel_key != "":
        params.append(f"channel_key=eq.{channel_key}")
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?{'&'.join(params)}&select=*"
    with httpx.Client(timeout=30) as c:
        r = c.get(url, headers=_supabase_headers())
    if r.status_code != 200:
        raise ReferenceError(f"supabase list failed {r.status_code}: {r.text[:300]}")
    return r.json() or []


def delete_reference_video(*, user_id: str, ref_id: str) -> bool:
    if not user_id or not ref_id:
        return False
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?user_id=eq.{user_id}&id=eq.{ref_id}"
    with httpx.Client(timeout=30) as c:
        r = c.delete(url, headers=_supabase_headers())
    return r.status_code in (200, 204)


def update_reference_notes(
    *, user_id: str, ref_id: str, notes: str = "", channel_key: str | None = None,
) -> dict[str, Any] | None:
    if not user_id or not ref_id:
        return None
    payload: dict[str, Any] = {
        "user_notes": (notes or "")[:1000],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if channel_key is not None:
        payload["channel_key"] = channel_key
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?user_id=eq.{user_id}&id=eq.{ref_id}"
    with httpx.Client(timeout=30) as c:
        r = c.patch(url, headers=_supabase_headers(), json=payload)
    if r.status_code not in (200, 204):
        return None
    rows = r.json() or []
    return rows[0] if rows else None


def references_as_grok_context(refs: list[dict[str, Any]], max_refs: int = 6) -> str:
    """
    Format references as plain-text system-prompt context for Grok.
    Caps at max_refs to fit in token budget.
    """
    if not refs:
        return ""
    lines: list[str] = []
    lines.append("Reference videos the user picked as inspiration (mimic these patterns):")
    for r in refs[:max_refs]:
        views = int(r.get("view_count", 0) or 0)
        title = r.get("title", "")
        notes = (r.get("user_notes") or "").strip()
        ch = r.get("channel_title") or ""
        dur = int(r.get("duration_sec", 0) or 0)
        line = f"  - [{views:,} views | {dur}s | {ch}] {title}"
        if notes:
            line += f"   (note: {notes})"
        lines.append(line)
    return "\n".join(lines)
