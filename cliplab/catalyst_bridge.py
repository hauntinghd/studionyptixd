"""ClipLab learning events for Catalyst and RunPod training."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from cliplab.config import CLIPLAB_DATASETS_DIR


def _safe_row(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in dict(row or {}).items():
        if isinstance(value, str):
            out[str(key)] = value[:4000]
        elif isinstance(value, (int, float, bool)) or value is None:
            out[str(key)] = value
        elif isinstance(value, list):
            out[str(key)] = value[:80]
        elif isinstance(value, dict):
            out[str(key)] = {str(k): v for k, v in list(value.items())[:80]}
        else:
            out[str(key)] = str(value)[:1000]
    return out


def append_learning_event(event_type: str, payload: dict[str, Any], *, dataset: str = "cliplab_learning.jsonl") -> dict[str, Any]:
    """Append a compact, durable ClipLab learning event.

    This is intentionally provider-neutral: Catalyst can read it for channel memory,
    and RunPod training scripts can later turn accepted/rejected clips into labels.
    """
    CLIPLAB_DATASETS_DIR.mkdir(parents=True, exist_ok=True)
    path = CLIPLAB_DATASETS_DIR / Path(dataset).name
    row = {
        "ts": time.time(),
        "event_type": str(event_type or "cliplab_event"),
        **_safe_row(payload or {}),
    }
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=True) + "\n")
    return {"ok": True, "path": str(path), "event_type": row["event_type"]}


def segment_training_rows(
    *,
    user_id: str,
    video_id: str,
    prompt: str,
    segments: list[dict[str, Any]],
    source: str,
    channel_id: str = "",
    registry_key: str = "",
    analyze_job_id: str = "",
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, seg in enumerate(list(segments or [])):
        if not isinstance(seg, dict):
            continue
        rows.append({
            "user_id": str(user_id or ""),
            "channel_id": str(channel_id or ""),
            "registry_key": str(registry_key or ""),
            "video_id": str(video_id or ""),
            "analyze_job_id": str(analyze_job_id or ""),
            "segment_index": index,
            "prompt": str(prompt or "")[:1000],
            "source": str(source or "unknown"),
            "start": float(seg.get("start") or 0),
            "end": float(seg.get("end") or 0),
            "confidence": float(seg.get("confidence") or 0),
            "virality_score": float(seg.get("virality_score") or 0),
            "why_it_matches": str(seg.get("why_it_matches") or "")[:1000],
            "hook_text": str(seg.get("hook_text") or "")[:400],
            "transcript_snippet": str(seg.get("transcript_snippet") or "")[:2000],
            "model_source": str(seg.get("model_source") or ""),
            "label_status": "candidate_unreviewed",
        })
    return rows
