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
COMPACT_AT_MESSAGES = int(MAX_MESSAGES_FOR_MODEL * 0.8)
MAX_SYNC_PENDING_SCAN = 400
MAX_RUN_EVENTS = 120
ACTIVE_RUN_STATUSES = {"queued", "running", "stream_disconnected"}
STALE_RUN_AFTER_SEC = int(__import__("os").environ.get("STUDIO_AGENT_STALE_RUN_SEC", "180") or "180")

ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_SESSIONS = ROOT / "data" / "studio_agent_sessions"
_APP_DATA = Path(__import__("os").environ.get("APP_DATA_DIR", "")).expanduser()
if _APP_DATA.is_dir():
    _DEFAULT_SESSIONS = _APP_DATA / "studio_agent_sessions"
SESSIONS_DIR = Path(
    __import__("os").environ.get("STUDIO_AGENT_SESSIONS_DIR", str(_DEFAULT_SESSIONS))
)
ARCHIVE_DIR = Path(
    __import__("os").environ.get("STUDIO_AGENT_ARCHIVE_DIR", str(SESSIONS_DIR.parent / "studio_agent_session_archive"))
)


def _now() -> float:
    return time.time()


def _session_path(session_id: str) -> Path:
    return SESSIONS_DIR / f"{session_id}.json"


def _message_text(message: dict[str, Any], *, limit: int = 900) -> str:
    content = message.get("content") or ""
    if isinstance(content, list):
        chunks = []
        for item in content:
            if isinstance(item, dict):
                chunks.append(str(item.get("text") or item.get("content") or ""))
            else:
                chunks.append(str(item))
        content = " ".join(chunks)
    return " ".join(str(content).split())[:limit]


def _compact_transcript(messages: list[dict[str, Any]], *, omitted_count: int) -> str:
    """Cheap deterministic compaction. Full raw messages remain stored on disk."""
    older = [m for m in messages if m.get("role") != "system"][:omitted_count]
    if not older:
        return ""
    user_goals: list[str] = []
    assistant_facts: list[str] = []
    tool_facts: list[str] = []
    for msg in older:
        role = str(msg.get("role") or "")
        text = _message_text(msg)
        if not text:
            continue
        if role == "user":
            user_goals.append(text)
        elif role == "assistant":
            assistant_facts.append(text)
        elif role == "tool":
            tool_facts.append(text)
    parts = [
        f"Compacted transcript memory ({omitted_count} older messages summarized; raw transcript is still stored):",
    ]
    if user_goals:
        parts.append("User instructions/preferences:")
        parts.extend(f"- {x}" for x in user_goals[-10:])
    if assistant_facts:
        parts.append("Prior agent decisions/results:")
        parts.extend(f"- {x}" for x in assistant_facts[-8:])
    if tool_facts:
        parts.append("Tool observations:")
        parts.extend(f"- {x}" for x in tool_facts[-6:])
    return "\n".join(parts)[:9000]


def _channel_label(session: dict[str, Any]) -> str:
    return (
        str(session.get("channel_title") or "").strip()
        or str(session.get("registry_key") or "").strip()
        or str(session.get("channel_id") or "").strip()
    )


def compact_session_if_needed(session_id: str) -> dict[str, Any] | None:
    """Persist a rolling summary once a chat reaches 80% of the model message cap."""
    session = get_session(session_id)
    if not session:
        return None
    messages = list(session.get("messages") or [])
    tail_start = 1 if messages and messages[0].get("role") == "system" else 0
    non_system_count = max(0, len(messages) - tail_start)
    if non_system_count < COMPACT_AT_MESSAGES:
        return session
    keep_recent = max(20, MAX_MESSAGES_FOR_MODEL // 2)
    omitted = max(0, non_system_count - keep_recent)
    if omitted <= 0 or int(session.get("compacted_message_count") or 0) >= omitted:
        return session
    summary = _compact_transcript(messages[tail_start:], omitted_count=omitted)
    if not summary:
        return session
    channel = _channel_label(session)
    if channel:
        summary = f"Selected YouTube channel for this chat: {channel}\n{summary}"
    session["context_summary"] = summary
    session["compacted_message_count"] = omitted
    _save(session)
    return session


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


_legacy_trim_messages_for_model = trim_messages_for_model


def trim_messages_for_model(
    messages: list[dict[str, Any]],
    *,
    session: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Keep system row + compacted memory + recent turns so long chats stay within model limits."""
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
    summary = str((session or {}).get("context_summary") or "").strip()
    if not summary:
        summary = _compact_transcript(tail, omitted_count=omitted)
    note = {
        "role": "system",
        "content": (
            f"[Earlier conversation compacted - {omitted} older messages are represented below. "
            "The full raw transcript is still saved in this session.]\n"
            f"{summary}"
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
        if session.get("deleted_at"):
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
        channel_id=str(old.get("channel_id") or ""),
        registry_key=str(old.get("registry_key") or ""),
        channel_title=str(old.get("channel_title") or ""),
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
    captions_enabled: bool = True,
    caption_mode: str = "word",
    channel_id: str = "",
    registry_key: str = "",
    channel_title: str = "",
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
        "channel_id": str(channel_id or "").strip(),
        "registry_key": str(registry_key or "").strip(),
        "channel_title": str(channel_title or "").strip(),
        "web_search": bool(web_search),
        "animate": bool(animate),
        "captions_enabled": bool(captions_enabled),
        "caption_mode": "off" if str(caption_mode or "").strip().lower() == "off" else "word",
        "created_at": _now(),
        "updated_at": _now(),
        "title": "",
        "messages": [],
        "pending_actions": [],
        "active_jobs": [],
        "runs": [],
        "context_summary": "",
        "compacted_message_count": 0,
    }
    _save(session)
    return session


def _public_run(run: dict[str, Any], *, include_events: bool = True) -> dict[str, Any]:
    events = list(run.get("events") or [])
    out = {
        "run_id": str(run.get("run_id") or ""),
        "session_id": str(run.get("session_id") or ""),
        "status": str(run.get("status") or "running"),
        "message_preview": str(run.get("message_preview") or ""),
        "created_at": float(run.get("created_at") or 0),
        "updated_at": float(run.get("updated_at") or 0),
        "completed_at": run.get("completed_at"),
        "last_event": events[-1] if events else None,
        "event_count": len(events),
    }
    if include_events:
        out["events"] = events[-MAX_RUN_EVENTS:]
    if run.get("error"):
        out["error"] = str(run.get("error") or "")
    return out


def _normalize_runs(session: dict[str, Any]) -> list[dict[str, Any]]:
    runs = session.setdefault("runs", [])
    if not isinstance(runs, list):
        runs = []
        session["runs"] = runs
    return runs


def reconcile_stale_runs(session: dict[str, Any]) -> dict[str, Any]:
    """Mark deploy-killed/disconnected chat runs terminal so UI does not spin forever."""
    now = _now()
    changed = False
    for run in _normalize_runs(session):
        status = str(run.get("status") or "running")
        if status not in ACTIVE_RUN_STATUSES:
            continue
        updated = float(run.get("updated_at") or run.get("created_at") or 0)
        if updated and now - updated <= STALE_RUN_AFTER_SEC:
            continue
        run["status"] = "interrupted"
        run["updated_at"] = now
        run["completed_at"] = now
        run["error"] = "Run was interrupted during a deploy or stream disconnect. Send Resume or retry the last message."
        run.setdefault("events", []).append({
            "event_id": f"evt_{uuid.uuid4().hex[:12]}",
            "event": "interrupted",
            "created_at": now,
            "data": {"message": run["error"]},
        })
        changed = True
    if changed:
        _save(session)
    return session


def create_run(session_id: str, *, user_text: str) -> dict[str, Any]:
    session = get_session(session_id)
    if not session:
        raise KeyError(session_id)
    run = {
        "run_id": f"run_{uuid.uuid4().hex[:16]}",
        "session_id": session_id,
        "status": "running",
        "message_preview": str(user_text or "").strip().replace("\n", " ")[:220],
        "created_at": _now(),
        "updated_at": _now(),
        "events": [],
    }
    runs = _normalize_runs(session)
    runs.append(run)
    session["runs"] = runs[-80:]
    _save(session)
    return run


def get_run(session_id: str, run_id: str, *, user_id: str | None = None) -> dict[str, Any] | None:
    session = get_session(session_id, user_id=user_id)
    if not session:
        return None
    session = reconcile_stale_runs(session)
    for run in _normalize_runs(session):
        if run.get("run_id") == run_id:
            return _public_run(run)
    return None


def list_runs(session_id: str, *, user_id: str | None = None, active_only: bool = False) -> list[dict[str, Any]]:
    session = get_session(session_id, user_id=user_id)
    if not session:
        return []
    session = reconcile_stale_runs(session)
    rows = [_public_run(run, include_events=False) for run in _normalize_runs(session)]
    if active_only:
        rows = [r for r in rows if r.get("status") in ACTIVE_RUN_STATUSES]
    rows.sort(key=lambda r: float(r.get("updated_at") or 0), reverse=True)
    return rows


def active_runs(session: dict[str, Any]) -> list[dict[str, Any]]:
    session = reconcile_stale_runs(session)
    rows = [_public_run(run, include_events=False) for run in _normalize_runs(session)]
    return [r for r in rows if r.get("status") in ACTIVE_RUN_STATUSES]


def append_run_event(session_id: str, run_id: str, event: str, data: dict[str, Any] | None = None) -> dict[str, Any] | None:
    session = get_session(session_id)
    if not session:
        return None
    now = _now()
    for run in _normalize_runs(session):
        if run.get("run_id") != run_id:
            continue
        payload = {
            "event_id": f"evt_{uuid.uuid4().hex[:12]}",
            "event": str(event or "status"),
            "created_at": now,
            "data": data or {},
        }
        run.setdefault("events", []).append(payload)
        run["events"] = list(run.get("events") or [])[-MAX_RUN_EVENTS:]
        run["updated_at"] = now
        if event == "stream_disconnected":
            run["status"] = "stream_disconnected"
        elif run.get("status") in {"queued", "running", "stream_disconnected"}:
            run["status"] = "running"
        _save(session)
        return _public_run(run)
    return None


def finish_run(session_id: str, run_id: str, *, status: str = "complete", error: str = "") -> dict[str, Any] | None:
    session = get_session(session_id)
    if not session:
        return None
    now = _now()
    for run in _normalize_runs(session):
        if run.get("run_id") != run_id:
            continue
        run["status"] = status
        run["updated_at"] = now
        run["completed_at"] = now
        if error:
            run["error"] = error
        _save(session)
        return _public_run(run)
    return None


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
    compact_session_if_needed(session_id)
    session = get_session(session_id) or session
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


def archive_session_context(session: dict[str, Any]) -> dict[str, Any] | None:
    """Archive a session summary before deletion so channel memory survives."""
    sid = str(session.get("session_id") or "").strip()
    if not sid:
        return None
    messages = list(session.get("messages") or [])
    summary = str(session.get("context_summary") or "").strip()
    if not summary:
        summary = _compact_transcript(messages, omitted_count=len(messages))
    archive = {
        "session_id": sid,
        "user_id": session.get("user_id"),
        "title": derive_title(session),
        "channel_id": session.get("channel_id") or "",
        "registry_key": session.get("registry_key") or "",
        "channel_title": session.get("channel_title") or "",
        "created_at": session.get("created_at"),
        "updated_at": session.get("updated_at"),
        "archived_at": _now(),
        "message_count": len(messages),
        "summary": summary[:9000],
        "messages": messages,
    }
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    (ARCHIVE_DIR / f"{sid}.json").write_text(
        json.dumps(archive, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return archive


def channel_archive_context(
    user_id: str,
    *,
    channel_id: str = "",
    registry_key: str = "",
    channel_title: str = "",
    limit: int = 3,
) -> str:
    """Return recent archived chat summaries for the selected channel."""
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    for path in ARCHIVE_DIR.glob("sa_*.json"):
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if str(row.get("user_id") or "") != str(user_id or ""):
            continue
        if channel_id and str(row.get("channel_id") or "") != channel_id:
            continue
        if registry_key and str(row.get("registry_key") or "") != registry_key:
            continue
        if not channel_id and not registry_key and channel_title:
            if str(row.get("channel_title") or "").strip().lower() != channel_title.strip().lower():
                continue
        rows.append(row)
    rows.sort(key=lambda r: float(r.get("archived_at") or r.get("updated_at") or 0), reverse=True)
    if not rows:
        return ""
    parts = ["Archived chat memory for this selected channel:"]
    for row in rows[: max(1, min(limit, 8))]:
        title = str(row.get("title") or "Deleted chat").strip()
        parts.append(f"- {title}: {str(row.get('summary') or '').strip()[:1600]}")
    return "\n".join(parts)[:7000]


def delete_session(session_id: str, *, user_id: str | None = None) -> bool:
    """Archive then remove a session file. Returns False if missing or not owned by user."""
    session = get_session(session_id, user_id=user_id)
    if not session:
        return False
    path = _session_path(session_id)
    try:
        archive_session_context(session)
        path.unlink(missing_ok=True)
    except OSError:
        return False
    return True
