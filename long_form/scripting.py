"""
Long-form outline generator.

The script is too large to ask Grok for in one shot. Instead:
  1. Outline pass — Grok returns a chapter list with titles + 1-line synopses.
  2. Per-chapter pass — Grok expands each chapter into beats (one per scene).

Per-chapter expansion happens lazily during render (so user can edit the
top-level outline first without burning tokens on chapters they'll cut).

Reuses skeleton_ai.scripting_grok.GrokClient.
"""
from __future__ import annotations
import json
from typing import Any

from skeleton_ai.scripting_grok import GrokClient


def _strip_json_fences(s: str) -> str:
    s = s.strip().strip("`").strip()
    if s.lower().startswith("json"):
        s = s[4:].strip()
    return s


def generate_outline(
    grok: GrokClient,
    channel_system_prompt: str,
    *,
    topic: str,
    target_minutes: int,
    catalyst_context: str = "",
) -> dict[str, Any]:
    """
    Top-level outline pass. Returns a dict:
      {
        "title": "...",
        "hook": "...",            # cold-open one-liner
        "chapters": [
            {"index": 0, "title": "...", "minutes": 5, "synopsis": "..."}
        ],
        "tags": ["...", "..."],
      }

    catalyst_context is optional — if provided, it's injected as
    'Use these channel-performance signals to bias topic + framing'.
    """
    chapter_count = max(3, min(20, round(target_minutes / 5)))

    sys_lines = [channel_system_prompt]
    if catalyst_context:
        sys_lines.append(
            "Channel performance signals (use to bias hook + framing — "
            "echo what works, avoid what doesn't):\n" + catalyst_context.strip()
        )
    sys_lines.append(
        "Output strict JSON with this shape:\n"
        '  { "title": "...", "hook": "<cold-open 1-line>", '
        '"chapters": [ {"index": 0, "title": "...", "minutes": <int>, '
        '"synopsis": "<1-2 sentences>"} ], "tags": ["..."] }\n'
        f"Aim for {chapter_count} chapters totaling {target_minutes} minutes. "
        "No markdown fences, no commentary."
    )
    sys = "\n\n".join(sys_lines)
    user = f"Topic: {topic.strip()}\nReturn the outline JSON now."

    raw = grok.complete(sys, user, max_tokens=2000, temperature=0.65)
    raw = _strip_json_fences(raw)
    try:
        outline = json.loads(raw)
    except json.JSONDecodeError:
        return {
            "title": topic.strip(),
            "hook": "",
            "chapters": [],
            "tags": [],
            "_parse_error": True,
            "_raw": raw[:600],
        }
    # Normalize.
    outline.setdefault("title", topic.strip())
    outline.setdefault("hook", "")
    chapters = outline.get("chapters") or []
    norm_chapters = []
    for i, c in enumerate(chapters):
        if not isinstance(c, dict):
            continue
        norm_chapters.append({
            "index": int(c.get("index", i)),
            "title": str(c.get("title", f"Chapter {i + 1}")),
            "minutes": int(c.get("minutes", max(1, target_minutes // max(1, len(chapters))))),
            "synopsis": str(c.get("synopsis", "")),
        })
    outline["chapters"] = norm_chapters
    outline.setdefault("tags", [])
    return outline


def expand_chapter(
    grok: GrokClient,
    channel_system_prompt: str,
    *,
    outline_title: str,
    chapter: dict[str, Any],
    fps: int,
) -> list[dict[str, Any]]:
    """
    Second-pass: turn ONE chapter into a beat list.

    Returns: [{ "beat_index": 0, "narration": "...", "scene_action": "...",
                "duration_sec": 5.0, "motion_prompt": "..." }]
    """
    chapter_minutes = max(1, int(chapter.get("minutes", 5)))
    target_beats = chapter_minutes * 12  # ~5 sec per beat

    sys = (
        f"{channel_system_prompt}\n\n"
        f"Expand the chapter below into approximately {target_beats} beats of "
        f"~5 seconds each. Each beat is one narration sentence + one visual "
        f"description. Output strict JSON:\n"
        '  { "beats": [ {"beat_index": 0, "narration": "...", '
        '"scene_action": "...", "duration_sec": 5.0, "motion_prompt": "..."} ] }\n'
        "scene_action is what the still shows. motion_prompt is what changes "
        f"in the {fps}fps i2v animation. No markdown fences."
    )
    user = (
        f"Documentary title: {outline_title}\n"
        f"Chapter title: {chapter.get('title')}\n"
        f"Synopsis: {chapter.get('synopsis')}\n"
        f"Target minutes for this chapter: {chapter_minutes}\n\n"
        "Return the JSON beat list now."
    )

    raw = grok.complete(sys, user, max_tokens=4000, temperature=0.7)
    raw = _strip_json_fences(raw)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return []
    beats = data.get("beats") or []
    out: list[dict[str, Any]] = []
    for i, b in enumerate(beats):
        if not isinstance(b, dict):
            continue
        out.append({
            "beat_index": int(b.get("beat_index", i)),
            "narration": str(b.get("narration", "")),
            "scene_action": str(b.get("scene_action", "")),
            "duration_sec": float(b.get("duration_sec", 5.0)),
            "motion_prompt": str(b.get("motion_prompt", "")),
        })
    return out
