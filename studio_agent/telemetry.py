"""Append-only telemetry for Studio Agent — training + product analytics.

All events land in JSONL per user under APP_DATA_DIR (Fly volume in prod).
Never store raw OAuth tokens or passwords.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
_APP_DATA = Path(os.environ.get("APP_DATA_DIR", "")).expanduser()
_DEFAULT = ROOT / "data" / "studio_agent_telemetry"
if _APP_DATA.is_dir():
    _DEFAULT = _APP_DATA / "studio_agent_telemetry"
TELEMETRY_DIR = Path(os.environ.get("STUDIO_AGENT_TELEMETRY_DIR", str(_DEFAULT)))

_REDACT_KEYS = frozenset({
    "access_token", "refresh_token", "password", "secret", "api_key", "authorization",
})


def _redact(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {
            k: "***" if str(k).lower() in _REDACT_KEYS else _redact(v)
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_redact(x) for x in obj[:50]]
    if isinstance(obj, str) and len(obj) > 4000:
        return obj[:4000] + "…"
    return obj


def record_event(
    user_id: str,
    event_type: str,
    payload: dict[str, Any] | None = None,
    *,
    session_id: str | None = None,
) -> None:
    uid = str(user_id or "").strip()
    if not uid:
        return
    try:
        TELEMETRY_DIR.mkdir(parents=True, exist_ok=True)
        row = {
            "ts": time.time(),
            "event_type": event_type,
            "user_id": uid,
            "session_id": session_id,
            "payload": _redact(payload or {}),
        }
        path = TELEMETRY_DIR / uid / "events.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=True) + "\n")
    except Exception:
        pass


def record_session_turn(
    user_id: str,
    session_id: str,
    *,
    role: str,
    content_preview: str,
    model: str | None = None,
    content_format: str | None = None,
) -> None:
    text = (content_preview or "").strip().replace("\n", " ")[:500]
    record_event(
        user_id,
        "chat_turn",
        {
            "role": role,
            "content_preview": text,
            "model": model,
            "content_format": content_format,
        },
        session_id=session_id,
    )


def record_tool_call(
    user_id: str,
    tool: str,
    arguments: dict[str, Any] | None,
    *,
    session_id: str | None = None,
    result_preview: str | None = None,
) -> None:
    record_event(
        user_id,
        "tool_call",
        {
            "tool": tool,
            "arguments": arguments or {},
            "result_preview": (result_preview or "")[:800],
        },
        session_id=session_id,
    )
