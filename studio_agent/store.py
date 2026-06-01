"""In-memory + disk session store for Studio Agent."""
from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Literal

ApprovalMode = Literal["auto", "confirm"]
ContentFormat = Literal["short", "long", "both"]

ROOT = Path(__file__).resolve().parents[1]
SESSIONS_DIR = Path(
    __import__("os").environ.get("STUDIO_AGENT_SESSIONS_DIR", str(ROOT / "data" / "studio_agent_sessions"))
)


def _now() -> float:
    return time.time()


def _session_path(session_id: str) -> Path:
    return SESSIONS_DIR / f"{session_id}.json"


def create_session(
    *,
    user_id: str,
    model: str,
    approval_mode: ApprovalMode = "confirm",
    content_format: ContentFormat = "both",
) -> dict[str, Any]:
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    sid = f"sa_{uuid.uuid4().hex[:16]}"
    session = {
        "session_id": sid,
        "user_id": user_id,
        "model": model,
        "approval_mode": approval_mode,
        "content_format": content_format,
        "created_at": _now(),
        "updated_at": _now(),
        "messages": [],
        "pending_actions": [],
    }
    _save(session)
    return session


def get_session(session_id: str, *, user_id: str | None = None) -> dict[str, Any] | None:
    path = _session_path(session_id)
    if not path.exists():
        return None
    session = json.loads(path.read_text(encoding="utf-8"))
    if user_id and session.get("user_id") != user_id:
        return None
    return session


def _save(session: dict[str, Any]) -> None:
    session["updated_at"] = _now()
    _session_path(session["session_id"]).write_text(
        json.dumps(session, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def update_session(session_id: str, **fields: Any) -> dict[str, Any]:
    session = get_session(session_id)
    if not session:
        raise KeyError(session_id)
    session.update(fields)
    _save(session)
    return session


def append_messages(session_id: str, new_messages: list[dict[str, Any]]) -> dict[str, Any]:
    session = get_session(session_id)
    if not session:
        raise KeyError(session_id)
    session.setdefault("messages", []).extend(new_messages)
    _save(session)
    return session


def set_pending_actions(session_id: str, actions: list[dict[str, Any]]) -> dict[str, Any]:
    session = get_session(session_id)
    if not session:
        raise KeyError(session_id)
    session["pending_actions"] = actions
    _save(session)
    return session


def pop_pending_action(session_id: str, action_id: str) -> dict[str, Any] | None:
    session = get_session(session_id)
    if not session:
        return None
    pending = session.get("pending_actions") or []
    hit = None
    rest = []
    for a in pending:
        if hit is None and a.get("id") == action_id:
            hit = a
        else:
            rest.append(a)
    if hit is None:
        return None
    session["pending_actions"] = rest
    _save(session)
    return hit
