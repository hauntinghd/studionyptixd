"""Segment ranking + virality scoring with RunPod model hook."""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Callable, Awaitable

import httpx

from cliplab.config import RUNPOD_CLIPLAB_URL, VIRALITY_BACKEND
from cliplab.model_registry import active_checkpoint, load_registry
from cliplab.models import ClipSegment, TranscriptCue
from cliplab.transcribe import transcript_plain

_log = logging.getLogger("nyptid-studio.cliplab.intelligence")

SEGMENT_RANK_PROMPT = """You are ClipLab — an Opus-grade long-to-short intelligence engine.

Given a full timestamped transcript and a user prompt, return EVERY segment that matches the prompt.
Score each segment for short-form virality (hook strength, emotional arc, clarity, payoff).

Output MUST be valid JSON:
{
  "segments": [
    {
      "start": 184.2,
      "end": 197.8,
      "confidence": 0.94,
      "virality_score": 87,
      "why_it_matches": "one sentence",
      "hook_text": "3-8 word cold open if reordering hook",
      "suggested_hook_reorder": true,
      "transcript_snippet": "exact quote from transcript"
    }
  ]
}

Rules:
- Segments 8-60 seconds (prefer 15-45s)
- Non-overlapping when possible; merge adjacent matches
- virality_score 0-100
- Return up to {max_segments} best segments sorted by virality_score desc
"""

JsonCompletionFn = Callable[..., Awaitable[dict]]


async def _score_with_runpod(segments: list[ClipSegment], transcript: str, prompt: str) -> list[ClipSegment]:
    runpod_key = os.getenv("RUNPOD_API_KEY", "").strip()
    if not runpod_key or not RUNPOD_CLIPLAB_URL:
        return segments
    active_id, weights = active_checkpoint("virality_scorer")
    if active_id == "local_llm" or not weights or not weights.exists():
        return segments
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                RUNPOD_CLIPLAB_URL,
                headers={"Authorization": f"Bearer {runpod_key}"},
                json={
                    "input": {
                        "task": "score_segments",
                        "prompt": prompt,
                        "transcript_excerpt": transcript[:8000],
                        "segments": [s.model_dump() for s in segments],
                        "weights_path": str(weights),
                    }
                },
            )
        if resp.status_code != 200:
            return segments
        data = resp.json()
        output = dict((data.get("output") or data) if isinstance(data, dict) else {})
        rows = list(output.get("segments") or [])
        if not rows:
            return segments
        rescored: list[ClipSegment] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            rescored.append(ClipSegment(**{**row, "model_source": "runpod_custom_v1"}))
        return rescored or segments
    except Exception as exc:
        _log.warning("RunPod virality scorer failed: %s", str(exc)[:200])
        return segments


async def rank_segments(
    cues: list[TranscriptCue],
    prompt: str,
    *,
    max_segments: int = 12,
    json_completion: JsonCompletionFn | None = None,
) -> list[ClipSegment]:
    if not cues or not json_completion:
        return []
    plain = transcript_plain(cues)
    system = SEGMENT_RANK_PROMPT.format(max_segments=max_segments)
    user_msg = f"USER PROMPT:\n{prompt.strip()}\n\nTRANSCRIPT:\n{plain[:100_000]}"
    try:
        result = await json_completion(system, user_msg, temperature=0.35, timeout_sec=120)
    except Exception as exc:
        _log.warning("Segment rank LLM failed: %s", str(exc)[:200])
        return []
    rows = list(result.get("segments") or [])
    segments: list[ClipSegment] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            seg = ClipSegment(**row, model_source="local_llm")
            if seg.end > seg.start and (seg.end - seg.start) >= 5:
                segments.append(seg)
        except Exception:
            continue
    segments.sort(key=lambda s: -float(s.virality_score or 0))
    segments = segments[:max_segments]

    reg = load_registry()
    backend = str(reg.get("virality_scorer", {}).get("active") or VIRALITY_BACKEND)
    if backend not in ("local_llm", "") and segments:
        segments = await _score_with_runpod(segments, plain, prompt)
        segments.sort(key=lambda s: -float(s.virality_score or 0))
    return segments


def judge_segment_confidence(segment: ClipSegment, cues: list[TranscriptCue]) -> float:
    """Heuristic second-pass confidence boost/penalty."""
    snippet = str(segment.transcript_snippet or "").strip().lower()
    if not snippet:
        return float(segment.confidence or 0)
    hits = sum(1 for c in cues if snippet[:40] in c.text.lower())
    boost = min(0.15, hits * 0.05)
    return min(1.0, float(segment.confidence or 0) + boost)
