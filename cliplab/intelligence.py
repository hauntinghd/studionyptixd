"""Segment ranking + virality scoring with RunPod model hook."""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Callable, Awaitable

import httpx

from cliplab.config import RUNPOD_CLIPLAB_URL, VIRALITY_BACKEND
from cliplab.model_registry import active_checkpoint, load_registry
from cliplab.models import ClipSegment, TranscriptCue
from cliplab.signals import compact_signal_context, summarize_segment_signals
from cliplab.transcribe import transcript_plain

_log = logging.getLogger("nyptid-studio.cliplab.intelligence")

SEGMENT_RANK_PROMPT = """You are ClipLab — an Opus-grade long-to-short intelligence engine.

Given a full timestamped transcript and a user prompt, return EVERY segment that matches the prompt.
Score each segment for short-form virality using transcript, visual/audio signal context,
hook strength, emotional arc, clarity, payoff, and rewatch potential.

Output MUST be valid JSON:
{{
  "segments": [
    {{
      "start": 184.2,
      "end": 197.8,
      "confidence": 0.94,
      "virality_score": 87,
      "score_breakdown": {{"hook": 88, "visual": 72, "audio": 64, "emotion": 81, "payoff": 84}},
      "why_it_matches": "one sentence",
      "hook_text": "3-8 word cold open if reordering hook",
      "suggested_hook_reorder": true,
      "transcript_snippet": "exact quote from transcript",
      "visual_notes": "what the viewer sees that helps the clip",
      "audio_notes": "audio/emotion cue that helps the clip",
      "narrative_role": "hook | escalation | reveal | payoff | cliffhanger",
      "retention_reason": "why viewers would keep watching",
      "edit_plan": ["cut cold into the reveal", "caption the first sentence as a hook"]
    }}
  ]
}}

Rules:
- Segments 8-60 seconds (prefer 15-45s)
- Non-overlapping when possible; merge adjacent matches
- virality_score 0-100
- Prefer moments that have transcript stakes AND visible movement, scene changes, audio spikes, emotional escalation, or clear payoff.
- If transcript is weak, only select a visual-heavy moment when signal context shows enough motion/audio change to make the clip understandable.
- Return up to {max_segments} best segments sorted by virality_score desc
"""

JsonCompletionFn = Callable[..., Awaitable[dict]]

_HIGH_SIGNAL_TERMS = {
    "but", "however", "because", "truth", "secret", "never", "always", "money", "risk",
    "scam", "fraud", "illegal", "caught", "exposed", "mistake", "problem", "reason",
    "proof", "shocking", "insane", "danger", "hidden", "revealed", "highest", "stakes",
}


def _heuristic_segments(cues: list[TranscriptCue], prompt: str, *, max_segments: int) -> list[ClipSegment]:
    """Transcript-only fallback so ClipLab still works when the LLM scorer is unavailable."""
    if not cues:
        return []
    prompt_terms = {
        w.lower()
        for w in re.findall(r"[a-zA-Z][a-zA-Z']{3,}", str(prompt or ""))
        if w.lower() not in {"find", "strongest", "highest", "moments", "segments", "hooks"}
    }
    windows: list[ClipSegment] = []
    n = len(cues)
    for start_i in range(0, n, 3):
        start = float(cues[start_i].start)
        text_parts: list[str] = []
        end = float(cues[start_i].end)
        j = start_i
        while j < n and (end - start) < 35:
            text_parts.append(str(cues[j].text or "").strip())
            end = float(cues[j].end)
            j += 1
        if end - start < 8:
            continue
        text = re.sub(r"\s+", " ", " ".join(text_parts)).strip()
        lower = text.lower()
        hits = sum(1 for term in _HIGH_SIGNAL_TERMS if term in lower)
        prompt_hits = sum(1 for term in prompt_terms if term in lower)
        question_bonus = 2 if "?" in text else 0
        number_bonus = 2 if re.search(r"\b\d+|\$|%|million|billion\b", lower) else 0
        score = min(96, 45 + hits * 5 + prompt_hits * 7 + question_bonus + number_bonus)
        if score < 50:
            continue
        windows.append(ClipSegment(
            start=round(start, 3),
            end=round(end, 3),
            confidence=min(0.88, 0.48 + hits * 0.05 + prompt_hits * 0.08),
            virality_score=score,
            why_it_matches="Transcript-only fallback found dense hook/stakes language.",
            hook_text=" ".join(text.split()[:8]),
            suggested_hook_reorder=False,
            transcript_snippet=text[:700],
            narrative_role="hook",
            retention_reason="Dense stakes language gives the viewer a reason to wait for the payoff.",
            edit_plan=[
                "Open on the strongest sentence with no lead-in.",
                "Use fast captions for the first 3 seconds.",
                "Cut before the point repeats or loses tension.",
            ],
            model_source="heuristic_transcript_v1",
        ))
    windows.sort(key=lambda s: -float(s.virality_score or 0))
    selected: list[ClipSegment] = []
    for seg in windows:
        if any(not (seg.end <= prev.start or seg.start >= prev.end) for prev in selected):
            continue
        selected.append(seg)
        if len(selected) >= max_segments:
            break
    if not selected and cues:
        start = float(cues[0].start)
        text_parts: list[str] = []
        end = start
        for cue in cues:
            text_parts.append(str(cue.text or "").strip())
            end = float(cue.end)
            if end - start >= 18:
                break
        text = re.sub(r"\s+", " ", " ".join(text_parts)).strip()
        if text:
            selected.append(ClipSegment(
                start=round(start, 3),
                end=round(max(end, start + 8), 3),
                confidence=0.42,
                virality_score=50,
                why_it_matches="Fallback segment created because no LLM-ranked clips were available.",
                hook_text=" ".join(text.split()[:8]),
                transcript_snippet=text[:700],
                narrative_role="hook",
                retention_reason="Fallback clip needs manual review because no stronger scored moment was found.",
                edit_plan=["Review manually before publishing.", "Tighten intro and remove dead air."],
                model_source="heuristic_transcript_v1",
            ))
    return selected[:max_segments]


def _default_edit_plan(seg: ClipSegment, signal_summary: dict[str, Any]) -> list[str]:
    plan = list(seg.edit_plan or [])
    if not plan:
        plan = [
            "Cut directly into the hook; remove setup before the first high-stakes line.",
            "Keep captions large and paced to speech.",
            "End immediately after the reveal/payoff or on a cliffhanger.",
        ]
    if signal_summary.get("scene_changes", 0) >= 2:
        plan.append("Use the scene changes as natural jump cuts instead of adding generated visuals.")
    if float(signal_summary.get("audio_score") or 0) >= 60:
        plan.append("Emphasize the audio spike with a caption pop or subtle zoom.")
    if float(signal_summary.get("visual_score") or 0) < 35:
        plan.append("Use tighter face/subject crop and stronger caption rhythm because visual motion is low.")
    return plan[:7]


def _apply_signal_breakdown(segments: list[ClipSegment], signals: dict[str, Any] | None) -> list[ClipSegment]:
    if not segments:
        return segments
    if not signals:
        for seg in segments:
            if not seg.score_breakdown:
                seg.score_breakdown = {
                    "hook": round(float(seg.virality_score or 0), 2),
                    "visual": 0.0,
                    "audio": 0.0,
                    "emotion": round(float(seg.confidence or 0) * 100, 2),
                    "payoff": round(float(seg.virality_score or 0) * 0.85, 2),
                }
            if not seg.edit_plan:
                seg.edit_plan = _default_edit_plan(seg, {})
        return segments

    enriched: list[ClipSegment] = []
    for seg in segments:
        summary = summarize_segment_signals(signals, float(seg.start), float(seg.end))
        transcript_score = float(seg.virality_score or 0)
        visual_score = float(summary.get("visual_score") or 0)
        audio_score = float(summary.get("audio_score") or 0)
        duration = max(1.0, float(seg.end) - float(seg.start))
        duration_fit = 100.0 if 15 <= duration <= 45 else 78.0 if 8 <= duration <= 60 else 45.0
        emotion_score = max(float(seg.confidence or 0) * 100.0, transcript_score * 0.82)
        payoff_score = max(transcript_score * 0.86, 50.0 if str(seg.retention_reason or "").strip() else 0.0)
        blended = (
            transcript_score * 0.50
            + visual_score * 0.18
            + audio_score * 0.14
            + emotion_score * 0.10
            + payoff_score * 0.05
            + duration_fit * 0.03
        )
        seg.virality_score = round(min(100.0, max(transcript_score, blended)), 2)
        seg.score_breakdown = {
            "hook": round(transcript_score, 2),
            "visual": round(visual_score, 2),
            "audio": round(audio_score, 2),
            "emotion": round(emotion_score, 2),
            "payoff": round(payoff_score, 2),
            "duration_fit": round(duration_fit, 2),
        }
        if not seg.visual_notes:
            seg.visual_notes = str(summary.get("visual_notes") or "")
        if not seg.audio_notes:
            seg.audio_notes = str(summary.get("audio_notes") or "")
        if not seg.narrative_role:
            seg.narrative_role = "reveal" if re.search(r"\b(but|truth|real|reason|because|revealed|secret)\b", seg.transcript_snippet, re.I) else "hook"
        if not seg.retention_reason:
            seg.retention_reason = "Selected because transcript stakes align with visual/audio activity." if (visual_score or audio_score) else "Selected mainly from transcript stakes; needs manual watch-through."
        seg.edit_plan = _default_edit_plan(seg, summary)
        if seg.model_source == "local_llm":
            seg.model_source = "local_llm_multimodal_signals_v1"
        elif seg.model_source == "heuristic_transcript_v1":
            seg.model_source = "heuristic_multimodal_signals_v1"
        enriched.append(seg)
    enriched.sort(key=lambda s: -float(s.virality_score or 0))
    return enriched


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
    signals: dict[str, Any] | None = None,
) -> list[ClipSegment]:
    if not cues:
        return []
    plain = transcript_plain(cues)
    signal_context = compact_signal_context(signals or {}, cues) if signals else ""
    if not json_completion:
        return _apply_signal_breakdown(_heuristic_segments(cues, prompt, max_segments=max_segments), signals)
    system = SEGMENT_RANK_PROMPT.format(max_segments=max_segments)
    user_msg = (
        f"USER PROMPT:\n{prompt.strip()}\n\n"
        f"LOCAL VISUAL/AUDIO SIGNAL WINDOWS:\n{signal_context or '(not available)'}\n\n"
        f"TRANSCRIPT:\n{plain[:100_000]}"
    )
    try:
        result = await json_completion(system, user_msg, temperature=0.35, timeout_sec=120)
    except Exception as exc:
        _log.warning("Segment rank LLM failed: %s", str(exc)[:200])
        return _apply_signal_breakdown(_heuristic_segments(cues, prompt, max_segments=max_segments), signals)
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
    if not segments:
        return _apply_signal_breakdown(_heuristic_segments(cues, prompt, max_segments=max_segments), signals)

    reg = load_registry()
    backend = str(reg.get("virality_scorer", {}).get("active") or VIRALITY_BACKEND)
    if backend not in ("local_llm", "") and segments:
        segments = await _score_with_runpod(segments, plain, prompt)
        segments.sort(key=lambda s: -float(s.virality_score or 0))
    return _apply_signal_breakdown(segments, signals)


def judge_segment_confidence(segment: ClipSegment, cues: list[TranscriptCue]) -> float:
    """Heuristic second-pass confidence boost/penalty."""
    snippet = str(segment.transcript_snippet or "").strip().lower()
    if not snippet:
        return float(segment.confidence or 0)
    hits = sum(1 for c in cues if snippet[:40] in c.text.lower())
    boost = min(0.15, hits * 0.05)
    return min(1.0, float(segment.confidence or 0) + boost)
