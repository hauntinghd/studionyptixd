"""Internal OpusClip provider bridge for ClipLab testing.

This is intentionally server-side only. It never returns or logs the API key.
"""
from __future__ import annotations

import asyncio
import re
import time
from pathlib import Path
from typing import Any

import httpx

from cliplab.config import OPUSCLIP_API_BASE, OPUSCLIP_API_KEY, OPUSCLIP_ORG_ID


class OpusClipError(RuntimeError):
    """Raised when OpusClip cannot produce clips for a ClipLab job."""


def _headers() -> dict[str, str]:
    if not OPUSCLIP_API_KEY:
        raise OpusClipError("OPUSCLIP_API_KEY is not configured on the server")
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OPUSCLIP_API_KEY}",
    }
    if OPUSCLIP_ORG_ID:
        headers["x-opus-org-id"] = OPUSCLIP_ORG_ID
    return headers


def _safe_detail(resp: httpx.Response) -> str:
    try:
        data = resp.json()
        if isinstance(data, dict):
            for key in ("message", "error", "detail"):
                if data.get(key):
                    return str(data.get(key))[:500]
            return str(data)[:500]
    except Exception:
        pass
    return (resp.text or resp.reason_phrase or "request failed")[:500]


def _raise_for_status(resp: httpx.Response, action: str) -> None:
    if resp.status_code >= 400:
        raise OpusClipError(f"OpusClip {action} failed ({resp.status_code}): {_safe_detail(resp)}")


def _extract_project_id(data: dict[str, Any]) -> str:
    for key in ("projectId", "project_id", "id"):
        value = data.get(key)
        if value:
            return str(value)
    nested = data.get("project")
    if isinstance(nested, dict):
        for key in ("id", "projectId"):
            value = nested.get(key)
            if value:
                return str(value)
    raise OpusClipError("OpusClip project was created, but no project id was returned")


def _keywords_from_prompt(prompt: str) -> list[str]:
    words = re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]{2,}", prompt.lower())
    blocked = {
        "the", "and", "for", "with", "from", "that", "this", "into", "video", "short",
        "clips", "clip", "moments", "strongest", "highest", "find", "render", "upload",
    }
    out: list[str] = []
    for word in words:
        if word in blocked or word in out:
            continue
        out.append(word)
        if len(out) >= 8:
            break
    return out or ["viral"]


def _clipanything_payload(video_ref: str, *, title: str, prompt: str) -> dict[str, Any]:
    return {
        "videoUrl": video_ref,
        "uploadedVideoAttr": {"title": title[:180] or "ClipLab source"},
        "curationPref": {
            "model": "ClipAnything",
            "clipDurations": [[15, 90]],
            "genre": "Auto",
            "customPrompt": prompt[:4000],
        },
        "renderPref": {
            "layoutAspectRatio": "portrait",
            "quickstartConfig": {"enableRemoveFillerWords": True},
        },
        "importPref": {"sourceLang": "auto"},
    }


def _clipbasic_payload(video_ref: str, *, title: str, prompt: str) -> dict[str, Any]:
    return {
        "videoUrl": video_ref,
        "uploadedVideoAttr": {"title": title[:180] or "ClipLab source"},
        "curationPref": {
            "model": "ClipBasic",
            "clipDurations": [[15, 90]],
            "genre": "Auto",
            "topicKeywords": _keywords_from_prompt(prompt),
        },
        "renderPref": {
            "layoutAspectRatio": "portrait",
            "quickstartConfig": {"enableRemoveFillerWords": True},
        },
        "importPref": {"sourceLang": "auto"},
    }


def _sync_upload_local_file(video_path: str) -> str:
    path = Path(video_path)
    if not path.exists():
        raise OpusClipError("Source video file is missing before OpusClip upload")
    with httpx.Client(timeout=None, follow_redirects=True) as client:
        create = client.post(
            f"{OPUSCLIP_API_BASE}/upload-links",
            headers=_headers(),
            json={"video": {"usecase": "LocalUpload"}},
        )
        _raise_for_status(create, "upload-link")
        data = create.json()
        upload_url = str(data.get("url") or "")
        upload_id = str(data.get("uploadId") or "")
        if not upload_url or not upload_id:
            raise OpusClipError("OpusClip upload-link response did not include url/uploadId")

        start = client.post(upload_url, headers={"x-goog-resumable": "start", "Content-Length": "0"})
        _raise_for_status(start, "resumable-start")
        location = str(start.headers.get("location") or "")
        if not location:
            raise OpusClipError("OpusClip upload did not return a resumable location")

        def _chunks():
            with path.open("rb") as fh:
                while True:
                    chunk = fh.read(1024 * 1024)
                    if not chunk:
                        break
                    yield chunk

        put = client.put(location, headers={"Content-Type": "application/octet-stream"}, content=_chunks())
        _raise_for_status(put, "video-upload")
        return upload_id


async def upload_local_file(video_path: str) -> str:
    return await asyncio.to_thread(_sync_upload_local_file, video_path)


async def create_project(video_ref: str, *, title: str, prompt: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        payload = _clipanything_payload(video_ref, title=title, prompt=prompt)
        resp = await client.post(f"{OPUSCLIP_API_BASE}/clip-projects", headers=_headers(), json=payload)
        if resp.status_code in {400, 422}:
            # The docs list ClipAnything/customPrompt, but older accounts may only accept ClipBasic.
            fallback = _clipbasic_payload(video_ref, title=title, prompt=prompt)
            resp = await client.post(f"{OPUSCLIP_API_BASE}/clip-projects", headers=_headers(), json=fallback)
        _raise_for_status(resp, "project-create")
        data = resp.json()
        if not isinstance(data, dict):
            raise OpusClipError("OpusClip project-create response was not an object")
        data["project_id"] = _extract_project_id(data)
        return data


async def fetch_exportable_clips(project_id: str, *, page_size: int = 20) -> list[dict[str, Any]]:
    params = {"q": "findByProjectId", "projectId": project_id, "pageNum": "1", "pageSize": str(page_size)}
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        resp = await client.get(f"{OPUSCLIP_API_BASE}/exportable-clips", headers=_headers(), params=params)
        _raise_for_status(resp, "get-clips")
        data = resp.json()
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        for key in ("clips", "data", "items", "results"):
            rows = data.get(key)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
    return []


async def wait_for_exportable_clips(
    project_id: str,
    *,
    timeout_sec: float,
    interval_sec: float,
    page_size: int = 20,
    on_progress: Any = None,
) -> list[dict[str, Any]]:
    deadline = time.time() + max(30.0, float(timeout_sec))
    interval = max(5.0, float(interval_sec))
    while True:
        clips = await fetch_exportable_clips(project_id, page_size=page_size)
        if clips:
            return clips
        if time.time() >= deadline:
            raise OpusClipError("OpusClip project is still processing and no exportable clips were returned before timeout")
        if callable(on_progress):
            try:
                on_progress()
            except Exception:
                pass
        await asyncio.sleep(interval)


def _range_seconds(row: dict[str, Any]) -> tuple[float | None, float | None]:
    ranges = row.get("timeRanges")
    if not isinstance(ranges, list) or not ranges:
        return None, None
    first = ranges[0]
    if not isinstance(first, list) or len(first) < 2:
        return None, None
    try:
        start = float(first[0])
        end = float(first[1])
    except Exception:
        return None, None
    if max(abs(start), abs(end)) > 10000:
        start /= 1000.0
        end /= 1000.0
    return round(start, 2), round(end, 2)


def clip_to_payload(row: dict[str, Any], idx: int) -> dict[str, Any]:
    start, end = _range_seconds(row)
    duration_ms = row.get("durationMs")
    duration_sec = round(float(duration_ms) / 1000.0, 2) if duration_ms else (
        round(float(end) - float(start), 2) if start is not None and end is not None else None
    )
    title = str(row.get("title") or f"OpusClip pick {idx + 1}").strip()
    text = str(row.get("text") or "").strip()
    return {
        "index": idx,
        "filename": f"opusclip_{idx + 1}.mp4",
        "url": str(row.get("uriForExport") or row.get("uriForPreview") or ""),
        "external_provider": "opusclip",
        "external_id": str(row.get("id") or row.get("curationId") or ""),
        "project_id": str(row.get("projectId") or ""),
        "start": start,
        "end": end,
        "duration_sec": duration_sec,
        "title": title,
        "hook_text": title,
        "transcript_snippet": text[:500],
        "why_it_matches": "Selected by OpusClip from the uploaded source video, then passed back into ClipLab for review and Catalyst outcome tracking.",
        "retention_reason": "OpusClip selected this as an exportable highlight; validate it against channel fit before publishing.",
        "visual_notes": str(row.get("promptName") or row.get("subgenre") or "OpusClip-selected visual/story moment"),
        "audio_notes": "",
        "narrative_role": "opusclip_candidate",
        "edit_plan": [
            "Review the first 2 seconds for hook clarity.",
            "Keep captions readable and verify no context is missing.",
            "Publish only if the clip matches the selected channel promise.",
        ],
        "score_breakdown": {"opusclip_selected": 100.0},
        "virality_score": None,
        "description": str(row.get("description") or ""),
        "hashtags": str(row.get("hashtags") or ""),
        "keywords": [str(x) for x in list(row.get("keywords") or []) if str(x).strip()],
        "raw_opus": {
            "id": row.get("id"),
            "curationId": row.get("curationId"),
            "durationMs": row.get("durationMs"),
            "genre": row.get("genre"),
            "subgenre": row.get("subgenre"),
        },
    }


def upload_packages_from_opus(
    *,
    video_id: str,
    clips: list[dict[str, Any]],
    prompt: str,
    channel_id: str,
    registry_key: str,
) -> list[dict[str, Any]]:
    packages: list[dict[str, Any]] = []
    base_tags = ["shorts", "viralshorts", "youtube shorts"]
    if any(k in f"{registry_key} {prompt}".lower() for k in ("anime", "manhua", "manhwa", "manga", "lexi")):
        base_tags += ["anime", "manhua", "manhwa", "manga", "anime edit"]
    for idx, clip in enumerate(clips):
        hashtags = [tag.lstrip("#") for tag in str(clip.get("hashtags") or "").split() if tag.startswith("#")]
        keywords = [str(x).strip() for x in list(clip.get("keywords") or []) if str(x).strip()]
        tags: list[str] = []
        for tag in [*base_tags, *keywords, *hashtags]:
            clean = tag.lower().strip()
            if clean and clean not in tags:
                tags.append(clean)
        title = str(clip.get("title") or f"Clip {idx + 1}").strip()[:82]
        description_bits = [
            str(clip.get("description") or "").strip(),
            str(clip.get("transcript_snippet") or "").strip()[:240],
            "Cut and packaged with Studio ClipLab + OpusClip.",
            "#shorts",
        ]
        packages.append({
            "clip_index": idx,
            "video_id": video_id,
            "channel_id": channel_id,
            "registry_key": registry_key,
            "title": title,
            "description": "\n\n".join(bit for bit in description_bits if bit),
            "tags": tags[:18],
            "hook": str(clip.get("hook_text") or title)[:120],
            "rationale": str(clip.get("why_it_matches") or "")[:240],
            "visual_notes": str(clip.get("visual_notes") or "")[:180],
            "audio_notes": str(clip.get("audio_notes") or "")[:180],
            "narrative_role": str(clip.get("narrative_role") or ""),
            "retention_reason": str(clip.get("retention_reason") or "")[:220],
            "edit_plan": list(clip.get("edit_plan") or []),
            "score_breakdown": dict(clip.get("score_breakdown") or {}),
            "start": clip.get("start"),
            "end": clip.get("end"),
            "virality_score": clip.get("virality_score"),
            "external_provider": "opusclip",
            "external_id": clip.get("external_id"),
        })
    return packages
