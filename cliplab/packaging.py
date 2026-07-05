"""Upload package helpers for ClipLab outputs."""
from __future__ import annotations

import re
from typing import Any


def _clean(value: Any, *, limit: int = 160) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:limit].rstrip()


def _title_from_segment(segment: dict[str, Any], *, registry_key: str = "") -> str:
    hook = _clean(segment.get("hook_text") or segment.get("transcript_snippet"), limit=72)
    if not hook:
        hook = "The Moment Everyone Missed"
    if len(hook) < 28 and registry_key:
        hook = f"{hook} ({registry_key.replace('_', ' ').title()})"
    return hook[:82].rstrip(" .,-")


def _tags_for_channel(registry_key: str, prompt: str) -> list[str]:
    base = ["shorts", "viralshorts", "youtube shorts", "storytelling"]
    hay = f"{registry_key} {prompt}".lower()
    if any(k in hay for k in ("anime", "manhua", "manhwa", "manga", "lexi")):
        base += ["anime", "manhua", "manhwa", "manga", "webtoon", "anime edit"]
    if any(k in hay for k in ("stream", "twitch", "kick", "clip")):
        base += ["stream highlights", "gaming clips", "funny moments"]
    if any(k in hay for k in ("empire", "magnates", "business", "money")):
        base += ["business", "money", "documentary", "true story"]
    out: list[str] = []
    for tag in base:
        if tag not in out:
            out.append(tag)
    return out[:18]


def build_upload_packages(
    *,
    video_id: str,
    rendered: list[dict[str, Any]],
    segments: list[dict[str, Any]],
    prompt: str = "",
    channel_id: str = "",
    registry_key: str = "",
) -> list[dict[str, Any]]:
    packages: list[dict[str, Any]] = []
    tags = _tags_for_channel(registry_key, prompt)
    for clip in rendered:
        idx = int(clip.get("index") or 0)
        segment = segments[idx] if 0 <= idx < len(segments) else {}
        title = _title_from_segment(segment, registry_key=registry_key)
        hook = _clean(segment.get("hook_text") or title, limit=120)
        why = _clean(segment.get("why_it_matches"), limit=220)
        edit_plan = [str(x).strip() for x in list(segment.get("edit_plan") or []) if str(x).strip()]
        visual_notes = _clean(segment.get("visual_notes"), limit=180)
        audio_notes = _clean(segment.get("audio_notes"), limit=180)
        description_bits = [
            hook,
            why,
            "Cut and packaged with Studio ClipLab.",
            "#shorts #anime #manhua" if any(t in tags for t in ("anime", "manhua", "manhwa")) else "#shorts",
        ]
        packages.append({
            "clip_index": idx,
            "video_id": video_id,
            "channel_id": channel_id,
            "registry_key": registry_key,
            "title": title,
            "description": "\n\n".join(bit for bit in description_bits if bit),
            "tags": tags,
            "hook": hook,
            "rationale": why,
            "visual_notes": visual_notes,
            "audio_notes": audio_notes,
            "narrative_role": _clean(segment.get("narrative_role"), limit=80),
            "retention_reason": _clean(segment.get("retention_reason"), limit=180),
            "edit_plan": edit_plan,
            "score_breakdown": dict(segment.get("score_breakdown") or {}),
            "start": clip.get("start"),
            "end": clip.get("end"),
            "virality_score": clip.get("virality_score"),
        })
    return packages
