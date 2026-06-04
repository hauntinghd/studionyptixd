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
_DEFAULT_SESSIONS = ROOT / "data" / "studio_agent_sessions"
_APP_DATA = Path(__import__("os").environ.get("APP_DATA_DIR", "")).expanduser()
if _APP_DATA.is_dir():
    _DEFAULT_SESSIONS = _APP_DATA / "studio_agent_sessions"
SESSIONS_DIR = Path(
    __import__("os").environ.get("STUDIO_AGENT_SESSIONS_DIR", str(_DEFAULT_SESSIONS))
)


def _now() -> float:
    return time.time()


def _session_path(session_id: str) -> Path:
    return SESSIONS_DIR / f"{session_id}.json"


def derive_title(session: dict[str, Any]) -> str:
    title = str(session.get("title") or "").strip()
    if title:
        return title
    for msg in session.get("messages") or []:
        if msg.get("role") == "user":
            text = str(msg.get("content") or "").strip().replace("\n", " ")
            if text:
                return (text[:72] + "…") if len(text) > 72 else text
    return "New chat"


def touch_title_from_user_message(session_id: str, user_text: str) -> None:
    session = get_session(session_id)
    if not session or session.get("title"):
        return
    text = str(user_text or "").strip().replace("\n", " ")
    if not text:
        return
    session["title"] = (text[:72] + "…") if len(text) > 72 else text
    _save(session)


def list_sessions(user_id: str, *, limit: int = 50) -> list[dict[str, Any]]:
    """List sessions for a user, newest first."""
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    for path in SESSIONS_DIR.glob("sa_*.json"):
        try:
            session = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if session.get("user_id") != user_id:
            continue
        rows.append(session)
    rows.sort(key=lambda s: float(s.get("updated_at") or 0), reverse=True)
    return rows[: max(1, min(limit, 200))]


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
        "title": "",
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
