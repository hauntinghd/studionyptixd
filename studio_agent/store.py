"""In-memory + disk session store for Studio Agent."""
from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Literal

ApprovalMode = Literal["auto", "confirm"]
ContentFormat = Literal["short", "long", "both"]
ReasoningDepth = Literal["fast", "balanced", "deep"]

DEFAULT_RENDER_STYLE = "cinematic"

# Cap context sent to OpenRouter (full transcript still stored on disk).
MAX_MESSAGES_FOR_MODEL = 80
MAX_SYNC_PENDING_SCAN = 400

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


def trim_messages_for_model(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep system row + the most recent turns so long chats stay within model limits."""
    if len(messages) <= MAX_MESSAGES_FOR_MODEL + 1:
        return messages
    head: list[dict[str, Any]] = []
    tail_start = 0
    if messages and messages[0].get("role") == "system":
        head = [messages[0]]
        tail_start = 1
    tail = messages[tail_start:]
    if len(tail) <= MAX_MESSAGES_FOR_MODEL:
        return head + tail
    omitted = len(tail) - MAX_MESSAGES_FOR_MODEL
    note = {
        "role": "system",
        "content": (
            f"[Earlier conversation truncated — {omitted} older messages omitted from model context. "
            "The full transcript is still saved in this session.]"
        ),
    }
    if head:
        return [head[0], note, *tail[-MAX_MESSAGES_FOR_MODEL:]]
    return [note, *tail[-MAX_MESSAGES_FOR_MODEL:]]


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


def rollover_session(session_id: str, *, user_id: str) -> dict[str, Any] | None:
    """Fork a session so the user can continue with full transcript + pending + jobs."""
    old = get_session(session_id, user_id=user_id)
    if not old:
        return None
    base_title = derive_title(old)
    continued = (
        f"{base_title[:58]} (continued)"
        if base_title and not base_title.endswith("(continued)")
        else base_title or "Continued chat"
    )
    fresh = create_session(
        user_id=user_id,
        model=str(old.get("model") or ""),
        approval_mode=old.get("approval_mode") or "confirm",
        content_format=old.get("content_format") or "both",
        reasoning_depth=old.get("reasoning_depth") or "balanced",
        render_style=old.get("render_style") or DEFAULT_RENDER_STYLE,
        web_search=bool(old.get("web_search", True)),
        animate=bool(old.get("animate", True)),
    )
    prior = list(old.get("messages") or [])
    prior.append({
        "role": "user",
        "content": (
            "[Session rolled over — pick up from the transcript above. "
            "Do not re-ask for topic, style, or channel setup unless something is missing.]"
        ),
    })
    return update_session(
        fresh["session_id"],
        title=continued[:72],
        messages=prior,
        pending_actions=list(old.get("pending_actions") or []),
        active_jobs=list(old.get("active_jobs") or []),
    )


def create_session(
    *,
    user_id: str,
    model: str,
    approval_mode: ApprovalMode = "confirm",
    content_format: ContentFormat = "both",
    reasoning_depth: ReasoningDepth = "balanced",
    render_style: str = DEFAULT_RENDER_STYLE,
    web_search: bool = True,
    animate: bool = True,
) -> dict[str, Any]:
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    sid = f"sa_{uuid.uuid4().hex[:16]}"
    session = {
        "session_id": sid,
        "user_id": user_id,
        "model": model,
        "approval_mode": approval_mode,
        "content_format": content_format,
        "reasoning_depth": reasoning_depth,
        "render_style": render_style or DEFAULT_RENDER_STYLE,
        "web_search": bool(web_search),
        "animate": bool(animate),
        "created_at": _now(),
        "updated_at": _now(),
        "title": "",
        "messages": [],
        "pending_actions": [],
        "active_jobs": [],
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


def _action_already_approved(messages: list[dict[str, Any]], tool_index: int) -> bool:
    """True if user already approved/rejected after this awaiting tool message."""
    for later in messages[tool_index + 1 :]:
        role = later.get("role")
        content = str(later.get("content") or "")
        if role == "user" and (
            content.startswith("[User approved ")
            or content.startswith("[Rejected ")
        ):
            return True
    return False


def recover_pending_action_from_messages(
    session: dict[str, Any],
    action_id: str,
) -> dict[str, Any] | None:
    """Rebuild a pending action from assistant tool_calls when pending_actions was lost."""
    import json

    aid = str(action_id or "").strip()
    if not aid:
        return None
    messages = list(session.get("messages") or [])

    for i, msg in enumerate(messages):
        if msg.get("role") != "tool":
            continue
        raw = msg.get("content") or ""
        try:
            body = json.loads(raw) if isinstance(raw, str) else raw
        except json.JSONDecodeError:
            continue
        if not isinstance(body, dict):
            continue
        if str(body.get("action_id") or "") != aid:
            continue
        if body.get("status") != "awaiting_user_approval":
            continue
        if _action_already_approved(messages, i):
            return None

        tool_call_id = msg.get("tool_call_id")
        for j in range(i - 1, -1, -1):
            am = messages[j]
            if am.get("role") != "assistant":
                continue
            for tc in am.get("tool_calls") or []:
                if tool_call_id and tc.get("id") != tool_call_id:
                    continue
                fn = tc.get("function") or {}
                name = str(fn.get("name") or "").strip()
                if not name:
                    continue
                raw_args = fn.get("arguments") or "{}"
                try:
                    args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                except json.JSONDecodeError:
                    args = {}
                if not isinstance(args, dict):
                    args = {}
                return {
                    "id": aid,
                    "tool": name,
                    "arguments": args,
                    "summary": f"{name}({json.dumps(args)[:200]})",
                    "recovered": True,
                }
    return None


def recover_last_production(session: dict[str, Any]) -> dict[str, Any] | None:
    """Return last_production or rebuild from approved/retried tool rows in the transcript."""
    import json

    from studio_agent.jobs import JOB_START_TOOLS

    lp = session.get("last_production")
    if isinstance(lp, dict):
        tool = str(lp.get("tool") or "").strip()
        args = lp.get("arguments")
        if tool in JOB_START_TOOLS and isinstance(args, dict) and args:
            return lp

    messages = list(session.get("messages") or [])
    for i in range(len(messages) - 1, -1, -1):
        msg = messages[i]
        if msg.get("role") != "user":
            continue
        content = str(msg.get("content") or "")
        matched_tool = ""
        for tool in JOB_START_TOOLS:
            if content.startswith(f"[User approved {tool}]") or content.startswith(f"[User retried {tool}]"):
                matched_tool = tool
                break
        if not matched_tool:
            continue

        for j in range(i - 1, -1, -1):
            am = messages[j]
            if am.get("role") != "assistant":
                continue
            for tc in am.get("tool_calls") or []:
                fn = tc.get("function") or {}
                if str(fn.get("name") or "").strip() != matched_tool:
                    continue
                raw_args = fn.get("arguments") or "{}"
                try:
                    args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                except json.JSONDecodeError:
                    args = {}
                if isinstance(args, dict) and args:
                    return {
                        "tool": matched_tool,
                        "arguments": args,
                        "recovered": True,
                        "updated_at": _now(),
                    }

        if "Tool result:" in content:
            blob = content.split("Tool result:", 1)[1].strip()
            for tail in ("\nSummarize", "\n[Session"):
                if tail in blob:
                    blob = blob.split(tail, 1)[0].strip()
            try:
                parsed = json.loads(blob)
            except json.JSONDecodeError:
                parsed = {}
            if isinstance(parsed, dict) and parsed:
                args: dict[str, Any] = {}
                if matched_tool == "start_shortform_generate":
                    args = {
                        "category_key": parsed.get("category_key") or "outcast",
                        "topic": parsed.get("topic"),
                        "visual_brief": parsed.get("visual_brief"),
                        "video_model": parsed.get("video_model") or "seedance",
                        "render_style": parsed.get("render_style"),
                    }
                    if parsed.get("script"):
                        args["script"] = parsed.get("script")
                elif matched_tool == "start_longform_render":
                    for key in (
                        "channel_key", "topic", "outline_title", "beats",
                        "script_override", "registry_key",
                    ):
                        if parsed.get(key) is not None:
                            args[key] = parsed.get(key)
                if args:
                    return {
                        "tool": matched_tool,
                        "arguments": args,
                        "recovered": True,
                        "updated_at": _now(),
                    }
    return None


def _production_already_approved(messages: list[dict[str, Any]]) -> bool:
    """True once the user approved a job-start tool (shortform/longform spawn)."""
    for msg in messages:
        if msg.get("role") != "user":
            continue
        content = str(msg.get("content") or "")
        if content.startswith("[User approved start_shortform_generate]"):
            return True
        if content.startswith("[User approved start_longform_render]"):
            return True
    return False


def sync_pending_from_messages(session_id: str) -> list[dict[str, Any]]:
    """If pending_actions is empty, restore from awaiting tool rows in the transcript."""
    session = get_session(session_id)
    if not session:
        return []
    pending = list(session.get("pending_actions") or [])
    if pending:
        return pending

    import json

    messages = list(session.get("messages") or [])
    if _production_already_approved(messages):
        return []
    scan_from = max(0, len(messages) - MAX_SYNC_PENDING_SCAN)
    rebuilt: list[dict[str, Any]] = []
    seen: set[str] = set()
    for i, msg in enumerate(messages[scan_from:], start=scan_from):
        if msg.get("role") != "tool":
            continue
        raw = msg.get("content") or ""
        try:
            body = json.loads(raw) if isinstance(raw, str) else raw
        except json.JSONDecodeError:
            continue
        if not isinstance(body, dict):
            continue
        if body.get("status") != "awaiting_user_approval":
            continue
        aid = str(body.get("action_id") or "").strip()
        if not aid or aid in seen:
            continue
        if _action_already_approved(messages, i):
            continue
        rec = recover_pending_action_from_messages(session, aid)
        if rec:
            rebuilt.append(rec)
            seen.add(aid)

    if rebuilt:
        set_pending_actions(session_id, rebuilt)
    latest = get_session(session_id) or {}
    return rebuilt or list(latest.get("pending_actions") or [])


def delete_session(session_id: str, *, user_id: str | None = None) -> bool:
    """Remove a session file. Returns False if missing or not owned by user."""
    session = get_session(session_id, user_id=user_id)
    if not session:
        return False
    path = _session_path(session_id)
    try:
        path.unlink(missing_ok=True)
    except OSError:
        return False
    return True
