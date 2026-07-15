"""Durable continuous evaluation for Studio Agent release readiness.

This is deliberately separate from training capture: it records operational
evidence and promotes repeated verified failures into regression cases.  It
never trains on raw user media or OAuth data.
"""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
APP_DATA = Path(os.getenv("APP_DATA_DIR", str(ROOT / "data"))).expanduser()
EVAL_DIR = Path(os.getenv("STUDIO_CONTINUOUS_EVAL_DIR", str(APP_DATA / "continuous_evaluation")))
EVENTS_PATH = EVAL_DIR / "events.jsonl"
CASES_PATH = EVAL_DIR / "regression_cases.json"
SUMMARY_PATH = EVAL_DIR / "summary.json"


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _failure_key(value: str) -> str:
    text = re.sub(r"\d+", "#", str(value or "").lower())
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text[:100] or "unknown_failure"


def _load_cases() -> dict[str, Any]:
    try:
        loaded = json.loads(CASES_PATH.read_text(encoding="utf-8"))
        return dict(loaded) if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def record_evidence(
    *,
    session: dict[str, Any] | None,
    event_type: str,
    outcome: str = "neutral",
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Append verified operational evidence and promote repeated failures."""
    session = dict(session or {})
    evidence = dict(evidence or {})
    now = time.time()
    row = {
        "ts": now,
        "event_type": str(event_type or "unknown")[:80],
        "outcome": str(outcome or "neutral")[:40],
        "session_id": str(session.get("session_id") or "")[:120],
        "channel_key": str(session.get("registry_key") or session.get("channel_id") or "")[:120],
        "render_style": str(session.get("render_style") or "")[:80],
        "title": str((session.get("conversation_intent") or {}).get("locked_title") or "")[:180],
        "evidence": evidence,
    }
    try:
        EVAL_DIR.mkdir(parents=True, exist_ok=True)
        with EVENTS_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception:
        return {"ok": False, "reason": "event_write_failed"}

    failure = str(evidence.get("failure") or evidence.get("qa_failure") or "").strip()
    if outcome not in {"failure", "blocked"} or not failure:
        return refresh_summary()
    cases = _load_cases()
    key = _failure_key(failure)
    case = dict(cases.get(key) or {})
    case.update({
        "id": key,
        "failure": failure[:280],
        "count": int(case.get("count") or 0) + 1,
        "last_seen": now,
        "status": "promoted" if int(case.get("count") or 0) + 1 >= 2 else "observed",
        "example": {
            "event_type": row["event_type"], "channel_key": row["channel_key"],
            "render_style": row["render_style"], "title": row["title"],
        },
    })
    cases[key] = case
    _atomic_json(CASES_PATH, cases)
    return refresh_summary()


def refresh_summary() -> dict[str, Any]:
    cases = _load_cases()
    promoted = [dict(v) for v in cases.values() if isinstance(v, dict) and v.get("status") == "promoted"]
    total_events = 0
    try:
        if EVENTS_PATH.is_file():
            with EVENTS_PATH.open("r", encoding="utf-8") as handle:
                total_events = sum(1 for _ in handle)
    except Exception:
        pass
    summary = {
        "status": "attention" if promoted else "healthy",
        "checked_at": time.time(),
        "evidence_events": total_events,
        "observed_failures": len(cases),
        "promoted_regressions": len(promoted),
        "release_ready": not promoted,
        "regressions": sorted(promoted, key=lambda item: float(item.get("last_seen") or 0), reverse=True)[:12],
    }
    try:
        _atomic_json(SUMMARY_PATH, summary)
    except Exception:
        pass
    return summary


def evaluation_health() -> dict[str, Any]:
    """Read-only health payload for the Studio admin/release surface."""
    try:
        payload = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return refresh_summary()
