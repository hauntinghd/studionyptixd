"""In-memory + disk session store for Studio Agent."""
from __future__ import annotations

import json
import hashlib
import re
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal

from . import provider_policy

ApprovalMode = Literal["auto", "confirm"]
ContentFormat = Literal["short", "long", "both"]
ReasoningDepth = Literal["fast", "balanced", "deep"]

DEFAULT_RENDER_STYLE = "cinematic"
DEFAULT_IMAGE_MODEL = "ernie_image"
DEFAULT_VIDEO_MODEL = "seedance"
SKELETON_DEFAULT_IMAGE_MODEL = "seedream_edit"
SKELETON_DEFAULT_VIDEO_MODEL = "seedance"
IMAGE_MODELS = {
    "recraft_v4",
    "seedream45",
    "ernie_image",
    "flux_2_pro",
    "recraft_v4_pro",
    "flux_lora_skeleton",
    "seedream_edit",
    "seedream_v45_edit",
    "seedream_v4",
    "seedream_v5_lite",
}


_IMAGE_MODEL_ALIASES = {
    "seedream_v45_edit": "seedream_edit",
    "seedream4": "seedream_v4",
    "seedream_v4_edit": "seedream_v4",
    "seedream5_lite": "seedream_v5_lite",
    "seedream_v5_lite_edit": "seedream_v5_lite",
}
VIDEO_MODELS = {
    "ltx_budget",
    "seedance",
    "pixverse",
    "kling_pro",
    "kling21_standard",
    "pixverse_v6",
    "pixverse_c1",
    "kling21_pro",
    "kling21_master",
}


def _has_denied_media_provider(value: Any) -> bool:
    provider = provider_policy.model_provider(value)
    return provider not in {"fal", "unknown"}


def _migrated_image_model(value: Any) -> str:
    if _has_denied_media_provider(value):
        return provider_policy.DEFAULT_FAL_IMAGE_MODEL
    return provider_policy.migrated_image_model(value)


def _migrated_video_model(value: Any) -> str:
    if _has_denied_media_provider(value):
        return provider_policy.DEFAULT_FAL_VIDEO_MODEL
    return provider_policy.migrated_video_model(value)


def normalize_image_model(value: Any) -> str:
    model = str(value or "").strip().lower().replace(" ", "_")
    if provider_policy.is_denied_image_model(model) or _has_denied_media_provider(model):
        raise provider_policy.ProviderPolicyDenied(
            f"Studio image model {model} is denied by {provider_policy.POLICY_VERSION}; select a FAL model."
        )
    model = _IMAGE_MODEL_ALIASES.get(model, model)
    return model if model in IMAGE_MODELS else DEFAULT_IMAGE_MODEL


def normalize_video_model(value: Any) -> str:
    model = str(value or "").strip().lower().replace(" ", "_").replace("-", "_")
    if provider_policy.is_denied_video_model(model) or _has_denied_media_provider(model):
        raise provider_policy.ProviderPolicyDenied(
            f"Studio video model {model} is denied by {provider_policy.POLICY_VERSION}; select a FAL model."
        )
    return model if model in VIDEO_MODELS else DEFAULT_VIDEO_MODEL


_POLICY_IMAGE_FIELDS = frozenset({
    "image_model", "image_model_id", "stills_model", "fallback_image_model_id",
    "requested_image_model", "image_model_default",
})
_POLICY_VIDEO_FIELDS = frozenset({
    "video_model", "video_model_id", "i2v_model", "fallback_video_model",
    "requested_video_model", "i2v_model_default",
})
_POLICY_RUNNER_FIELDS = frozenset({"runner_model", "chat_model", "llm_model"})
_POLICY_TTS_PROVIDER_FIELDS = frozenset({"voice_provider", "tts_provider", "voice_provider_default"})
_POLICY_STT_PROVIDER_FIELDS = frozenset({"stt_provider", "dictation_provider"})
_POLICY_SEMANTIC_PROVIDER_FIELDS = frozenset({"qa_provider", "visual_qa_provider", "analysis_provider"})


def _policy_migration_entry(
    *,
    path: str,
    capability: str,
    previous: Any,
    replacement: Any,
    migrated_at: float,
) -> dict[str, Any]:
    return {
        "policy_version": provider_policy.POLICY_VERSION,
        "migrated_at": migrated_at,
        "field_path": path,
        "capability": capability,
        "previous": previous,
        "replacement": replacement,
    }


def _migrate_session_provider_policy(
    session: dict[str, Any],
    *,
    recursive: bool = True,
) -> bool:
    """Migrate durable legacy routes once, with a versioned audit trail.

    Loading old state is intentionally different from accepting a new picker
    choice: persisted denied routes move to direct Anthropic/FAL equivalents,
    while the public normalizers above reject a new denied selection.
    """

    if not isinstance(session, dict):
        return False
    migrated_at = time.time()
    migrations: list[dict[str, Any]] = []
    route_changed = False

    def replace(
        row: dict[str, Any],
        key: str,
        replacement: Any,
        *,
        path: str,
        capability: str,
    ) -> None:
        nonlocal route_changed
        previous = row.get(key)
        if replacement == previous:
            return
        row[key] = replacement
        migrations.append(_policy_migration_entry(
            path=path,
            capability=capability,
            previous=previous,
            replacement=replacement,
            migrated_at=migrated_at,
        ))
        if capability in {provider_policy.IMAGE_CAPABILITY, provider_policy.I2V_CAPABILITY}:
            route_changed = True

    def visit(value: Any, path: str) -> None:
        if isinstance(value, list):
            for index, item in enumerate(value):
                visit(item, f"{path}[{index}]")
            return
        if not isinstance(value, dict):
            return
        for key in list(value):
            if key == "provider_policy_migrations":
                continue
            current = value.get(key)
            field_path = f"{path}.{key}" if path else key
            if isinstance(current, str) and key in _POLICY_IMAGE_FIELDS:
                if provider_policy.is_denied_image_model(current) or _has_denied_media_provider(current):
                    replace(
                        value,
                        key,
                        _migrated_image_model(current),
                        path=field_path,
                        capability=provider_policy.IMAGE_CAPABILITY,
                    )
                    current = value.get(key)
            elif isinstance(current, str) and key in _POLICY_VIDEO_FIELDS:
                if provider_policy.is_denied_video_model(current) or _has_denied_media_provider(current):
                    replace(
                        value,
                        key,
                        _migrated_video_model(current),
                        path=field_path,
                        capability=provider_policy.I2V_CAPABILITY,
                    )
                    current = value.get(key)
            elif isinstance(current, str) and key in _POLICY_RUNNER_FIELDS:
                model_provider = provider_policy.model_provider(current)
                if model_provider not in {"anthropic", "unknown"}:
                    replace(
                        value,
                        key,
                        provider_policy.DEFAULT_RUNNER_MODEL,
                        path=field_path,
                        capability=provider_policy.RUNNER_CAPABILITY,
                    )
                    current = value.get(key)
                elif model_provider == "anthropic":
                    direct = provider_policy.normalize_anthropic_model_id(current)
                    if direct != current:
                        replace(
                            value,
                            key,
                            direct,
                            path=field_path,
                            capability=provider_policy.RUNNER_CAPABILITY,
                        )
                        current = value.get(key)
            elif isinstance(current, str) and key in _POLICY_TTS_PROVIDER_FIELDS:
                if not provider_policy.is_provider_allowed(current, provider_policy.TTS_CAPABILITY):
                    replace(
                        value,
                        key,
                        provider_policy.DEFAULT_FAL_VOICE_PROVIDER,
                        path=field_path,
                        capability=provider_policy.TTS_CAPABILITY,
                    )
                    current = value.get(key)
            elif isinstance(current, str) and key in _POLICY_STT_PROVIDER_FIELDS:
                if not provider_policy.is_provider_allowed(current, provider_policy.STT_CAPABILITY):
                    replace(
                        value,
                        key,
                        "fal",
                        path=field_path,
                        capability=provider_policy.STT_CAPABILITY,
                    )
                    current = value.get(key)
            elif isinstance(current, str) and key in _POLICY_SEMANTIC_PROVIDER_FIELDS:
                if not provider_policy.is_provider_allowed(current, provider_policy.SEMANTIC_QA_CAPABILITY):
                    replace(
                        value,
                        key,
                        "anthropic",
                        path=field_path,
                        capability=provider_policy.SEMANTIC_QA_CAPABILITY,
                    )
                    current = value.get(key)
            if recursive:
                visit(current, field_path)

    root_model = str(session.get("model") or "").strip()
    root_provider = provider_policy.model_provider(root_model)
    if root_provider not in {"anthropic", "unknown"}:
        replace(
            session,
            "model",
            provider_policy.DEFAULT_RUNNER_MODEL,
            path="model",
            capability=provider_policy.RUNNER_CAPABILITY,
        )
    elif root_provider == "anthropic":
        direct_model = provider_policy.normalize_anthropic_model_id(root_model)
        if direct_model != root_model:
            replace(
                session,
                "model",
                direct_model,
                path="model",
                capability=provider_policy.RUNNER_CAPABILITY,
            )

    visit(session, "")
    prior_version = str(session.get("provider_policy_version") or "")
    if prior_version != provider_policy.POLICY_VERSION:
        session["provider_policy_version"] = provider_policy.POLICY_VERSION
        session["provider_policy_migrated_at"] = migrated_at
    if migrations:
        audit = list(session.get("provider_policy_migrations") or [])
        audit.extend(migrations)
        session["provider_policy_migrations"] = audit[-200:]
        session["provider_policy_migrated_at"] = migrated_at
    if route_changed:
        try:
            revision = max(1, int(session.get("media_route_revision") or 1))
        except (TypeError, ValueError):
            revision = 1
        session["media_route_revision"] = revision + 1
        session["media_route_updated_at"] = migrated_at
    return bool(migrations or prior_version != provider_policy.POLICY_VERSION)


def migrate_provider_policy_state(state: dict[str, Any]) -> bool:
    """Migrate a persisted Studio state tree before reusing provider choices."""

    if not isinstance(state, dict):
        return False
    return _migrate_session_provider_policy(state)


def migrate_provider_policy_summary_state(state: dict[str, Any]) -> bool:
    """Sanitize top-level persisted routes before building a list summary.

    Session history boot intentionally avoids walking every nested message and
    receipt in every chat. Full recursive migration still runs when a session
    is opened or mutated; the summary path only needs the root runner and media
    picker fields that its strict serializers expose.
    """

    if not isinstance(state, dict):
        return False
    return _migrate_session_provider_policy(state, recursive=False)


def media_route_snapshot(session: dict[str, Any] | None) -> dict[str, Any]:
    """Return the current immutable media-routing token for one dispatch.

    Long-running production tools re-read this snapshot before every provider
    call.  A picker change increments ``revision`` so an older provider result
    can be rejected before it replaces an approved asset.
    """

    row = session if isinstance(session, dict) else {}
    try:
        revision = max(1, int(row.get("media_route_revision") or 1))
    except (TypeError, ValueError):
        revision = 1
    try:
        route_updated_at = float(row.get("media_route_updated_at") or row.get("updated_at") or 0.0)
    except (TypeError, ValueError):
        route_updated_at = 0.0
    return {
        "revision": revision,
        "image_model_id": normalize_image_model(row.get("image_model")),
        "video_model": normalize_video_model(row.get("video_model")),
        "updated_at": route_updated_at,
    }

# Cap context sent to OpenRouter (full transcript still stored on disk).
MAX_MESSAGES_FOR_MODEL = 80
COMPACT_AT_MESSAGES = int(MAX_MESSAGES_FOR_MODEL * 0.8)


def _assistant_tool_call_ids(message: dict[str, Any]) -> list[str]:
    """Return tool-call ids from an assistant message."""
    ids: list[str] = []
    for call in list(message.get("tool_calls") or []):
        if not isinstance(call, dict):
            continue
        call_id = str(call.get("id") or "").strip()
        if call_id:
            ids.append(call_id)
    return ids


def align_tool_message_boundary(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep only complete assistant-to-tool sequences in provider context.

    A tool result must follow the assistant request with the same call id, and
    every requested call must have a result before the next conversation turn.
    Truncation can otherwise leave Anthropic with an invalid message history.
    """
    if not messages:
        return []
    out: list[dict[str, Any]] = []
    i = 0
    n = len(messages)
    while i < n:
        msg = messages[i]
        if not isinstance(msg, dict):
            i += 1
            continue
        role = str(msg.get("role") or "")
        if role == "tool":
            # Drop orphan tool results with no preceding assistant request.
            i += 1
            continue
        tool_ids = _assistant_tool_call_ids(msg) if role == "assistant" else []
        if role == "assistant" and tool_ids:
            needed = set(tool_ids)
            collected: list[dict[str, Any]] = []
            j = i + 1
            while j < n:
                candidate = messages[j]
                if not isinstance(candidate, dict) or str(candidate.get("role") or "") != "tool":
                    break
                collected.append(candidate)
                tool_call_id = str(candidate.get("tool_call_id") or "").strip()
                if tool_call_id in needed:
                    needed.discard(tool_call_id)
                j += 1
            if needed:
                # Preserve any assistant prose but remove an incomplete call set.
                text = str(msg.get("content") or "").strip()
                if text:
                    cleaned = {key: value for key, value in msg.items() if key != "tool_calls"}
                    cleaned["content"] = text
                    out.append(cleaned)
                i = j
                continue
            out.append(msg)
            out.extend(collected)
            i = j
            continue
        out.append(msg)
        i += 1
    return out


MAX_SYNC_PENDING_SCAN = 400
MAX_RUN_EVENTS = 120
ACTIVE_RUN_STATUSES = {"queued", "running", "stream_disconnected"}
STALE_RUN_AFTER_SEC = int(__import__("os").environ.get("STUDIO_AGENT_STALE_RUN_SEC", "180") or "180")
# The current session store is process-local JSON on disk. Serialize the tiny
# create-or-return critical section so concurrent HTTP retries with the same
# request ID cannot both observe absence and create separate runs.
_RUN_CREATE_LOCK = threading.Lock()
_SESSION_LOCKS_GUARD = threading.Lock()
_SESSION_LOCKS: dict[str, threading.RLock] = {}
_JOB_MUTATION_LOCKS_GUARD = threading.Lock()
_JOB_MUTATION_LOCKS: dict[str, threading.RLock] = {}
SINGLETON_PRODUCTION_APPROVAL_TOOLS = {
    "start_shortform_generate",
    "start_longform_render",
}


def _session_write_lock(session_id: str) -> threading.RLock:
    key = str(session_id or "").strip() or "__unknown__"
    with _SESSION_LOCKS_GUARD:
        lock = _SESSION_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _SESSION_LOCKS[key] = lock
        return lock


def _job_mutation_thread_lock(job_id: str) -> threading.RLock:
    key = str(job_id or "").strip() or "__unknown__"
    with _JOB_MUTATION_LOCKS_GUARD:
        lock = _JOB_MUTATION_LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _JOB_MUTATION_LOCKS[key] = lock
        return lock
_TITLE_STOPWORDS = {
    "the", "and", "for", "with", "that", "this", "when", "they", "them", "you", "your",
    "into", "from", "short", "video", "scene", "test", "make", "making", "going", "title",
    "lets", "let", "will", "we", "one", "exactly",
}


def _normalize_quote_chars(text: str) -> str:
    return (
        str(text or "")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
        .replace("\u2018", "'")
        .replace("\u2019", "'")
    )


def _clean_title_candidate(candidate: str) -> str:
    cleaned = str(candidate or "").strip(" -:,.")
    cleaned = re.sub(
        r"^(?:yes\.?|okay\.?|ok\.?|sure\.?|let'?s\s+see|let\s+us\s+see|maybe)\s*,?\s*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"^(?:make|do|start)\s+", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip(" -:,.")


def _requested_title_from_user_text(text: str) -> str:
    """Best-effort title/topic extraction from the latest user instruction."""
    value = _normalize_quote_chars(_user_message_before_attachments(text))
    if not value.strip():
        return ""
    if is_ideation_request(value):
        return ""

    # Prefer conversation-module extractor (handles "if we are making Title").
    try:
        from studio_agent.conversation import extract_user_locked_title

        locked = extract_user_locked_title(value)
        if locked:
            return locked
    except Exception:
        pass

    quoted = [
        _clean_title_candidate(q)
        for q in re.findall(r'"([^"\n]{8,140})"', value)
        if len(_title_keywords(q)) >= 2
    ]
    if quoted:
        return quoted[-1]

    # Existing-scene repair instructions often contain phrases such as
    # "repair for scenes two through six". The broad legacy ``for ...``
    # extractor below used that scene selector as a brand-new video title,
    # which detached and hid the repaired job during Sync. Scene mutations are
    # continuations unless the creator explicitly supplies a quoted/title form.
    if re.search(
        r"\b(?:audit|fix|repair|correct|redo|regenerate|rerender|re-render|"
        r"reanimate|re-animate|edit|revise|restage|re-stage)\b.{0,180}"
        r"\b(?:scenes?|stills?|clips?|animations?|shots?)\b",
        value,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        return ""

    loose_patterns = [
        r"(?:one\s+still\s+for|still\s+for|short\s+for|video\s+for|make(?:\s+exactly)?\s+one\s+still\s+for)\s+(.{8,140}?)(?:\s+using|\s+with|\.|$)",
        r"(?:title\s+(?:we'?re\s+going\s+to\s+go\s+with|is|it)\s*[:,]?\s*)([^.\n]{8,140})",
        r"(?:we\s+will\s+do|we'?ll\s+do|let'?s\s+do|lets\s+do)\s+(?:the\s+)?title\s*[,:\-]?\s*([^.\n]{8,140})",
        r"(?:we\s+will\s+do|we'?ll\s+do|let'?s\s+do|lets\s+do)\s+([^.\n]{8,140})",
        r"(?:if we are|we are|we'?re)\s+making\s+([^.\n?]{8,140})",
        r"for\s+['\"]?([^'\".\n]{8,140})['\"]?(?:\s+using|\s+with|\.|$)",
    ]
    for pattern in loose_patterns:
        matches = [_clean_title_candidate(m) for m in re.findall(pattern, value, flags=re.IGNORECASE)]
        matches = [
            re.sub(r"(?i)^(the\s+)?title\s*[,:\-]?\s*", "", m).strip()
            for m in matches
        ]
        matches = [m for m in matches if len(_title_keywords(m)) >= 2 and not is_boilerplate_production_topic(m)]
        if matches:
            return matches[-1]
    return ""


def _production_action_title(action: dict[str, Any]) -> str:
    args = action.get("arguments") if isinstance(action.get("arguments"), dict) else {}
    return str(
        args.get("title")
        or args.get("video_title")
        or args.get("topic")
        or ""
    ).strip()


def _title_overlap_score(left: str, right: str) -> float:
    left_words = _title_keywords(left)
    right_words = _title_keywords(right)
    if not left_words or not right_words:
        return 0.0
    return len(left_words & right_words) / max(1, min(len(left_words), len(right_words)))


def normalize_pending_actions(
    actions: list[dict[str, Any]],
    messages: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Keep production-start approvals single-current while preserving other approvals."""
    rows = [a for a in actions if isinstance(a, dict)]
    production_indices = [
        idx
        for idx, action in enumerate(rows)
        if str(action.get("tool") or "") in SINGLETON_PRODUCTION_APPROVAL_TOOLS
    ]
    if not production_indices:
        return rows
    if len(production_indices) == 1:
        keep_index = production_indices[0]
    else:
        keep_index = production_indices[-1]
        if messages:
            requested = _requested_title_from_user_text(_latest_user_text(messages))
            if requested:
                keep_index = max(
                    production_indices,
                    key=lambda idx: _title_overlap_score(_production_action_title(rows[idx]), requested),
                )
    normalized: list[dict[str, Any]] = []
    for idx, action in enumerate(rows):
        tool = str(action.get("tool") or "")
        if tool in SINGLETON_PRODUCTION_APPROVAL_TOOLS and idx != keep_index:
            continue
        normalized.append(action)
    return normalized

ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_SESSIONS = ROOT / "data" / "studio_agent_sessions"
_APP_DATA_RAW = str(__import__("os").environ.get("APP_DATA_DIR", "") or "").strip()
_APP_DATA = Path(_APP_DATA_RAW).expanduser() if _APP_DATA_RAW else None
if _APP_DATA is not None:
    try:
        _APP_DATA.mkdir(parents=True, exist_ok=True)
        _DEFAULT_SESSIONS = _APP_DATA / "studio_agent_sessions"
    except OSError:
        from studio_agent.fs_paths import data_root

        _DEFAULT_SESSIONS = data_root() / "studio_agent_sessions"
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


@contextmanager
def _run_create_file_lock(session_id: str):
    """Serialize chat run creation across API worker processes.

    ``WEB_CONCURRENCY`` means a normal ``threading.Lock`` is not sufficient:
    two workers can receive the same retried HTTP request at once and both
    create a run.  The lock file lives beside the shared session store, so all
    workers that can mutate the session participate in the same critical
    section.
    """

    digest = hashlib.sha256(str(session_id or "").encode("utf-8")).hexdigest()
    lock_dir = SESSIONS_DIR / ".run_create_locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{digest}.lock"
    with lock_path.open("a+b") as handle:
        handle.seek(0, 2)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        if __import__("os").name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            handle.seek(0)
            if __import__("os").name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def _session_write_file_lock(session_id: str):
    """Serialize whole-session replacements across API worker processes."""

    digest = hashlib.sha256(str(session_id or "").encode("utf-8")).hexdigest()
    lock_dir = SESSIONS_DIR / ".session_write_locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{digest}.lock"
    with lock_path.open("a+b") as handle:
        handle.seek(0, 2)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        if __import__("os").name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            handle.seek(0)
            if __import__("os").name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def production_job_mutation_lock(job_id: str):
    """Serialize validated mutations of one production job across workers."""

    normalized = str(job_id or "").strip()
    if not normalized:
        raise ValueError("job_id is required for a production mutation lock")
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    lock_dir = SESSIONS_DIR / ".job_mutation_locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{digest}.lock"
    with _job_mutation_thread_lock(normalized), lock_path.open("a+b") as handle:
        handle.seek(0, 2)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        if __import__("os").name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            handle.seek(0)
            if __import__("os").name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _read_session_file(path: Path) -> dict[str, Any] | None:
    """Load a session JSON file, tolerating trailing garbage from interrupted writes."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    if not str(text).strip():
        return None
    try:
        data = json.loads(text)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError as exc:
        if "Extra data" not in str(exc):
            return None
        try:
            data, _end = json.JSONDecoder().raw_decode(str(text).lstrip())
            return data if isinstance(data, dict) else None
        except Exception:
            return None


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


def _title_keywords(value: str) -> set[str]:
    words = re.findall(r"[a-z0-9]+", str(value or "").lower())
    return {w for w in words if len(w) > 2 and w not in _TITLE_STOPWORDS}


def _explicit_title_candidate(text: str) -> str:
    return _requested_title_from_user_text(text)


def _latest_user_text(messages: list[dict[str, Any]] | dict[str, Any] | None, *, limit: int = 4) -> str:
    """Return the latest user message text.

    Accepts either a message list or a full session dict. Callers historically
    mixed the two; iterating a session dict yields string keys and crashes with
    ``'str' object has no attribute 'get'`` (Approve path).
    """
    if messages is None:
        return ""
    if isinstance(messages, dict):
        # Session-shaped dict → pull messages. Plain dict without messages → empty.
        rows_src = list(messages.get("messages") or []) if "messages" in messages or "session_id" in messages else []
    else:
        rows_src = list(messages or [])
    rows = [
        str(m.get("content") or "")
        for m in rows_src
        if isinstance(m, dict) and str(m.get("role") or "") == "user"
    ]
    return "\n".join(rows[-limit:])


def _is_production_diagnostic_text(text: str) -> bool:
    value = str(text or "").lower()
    if re.search(r"\b(?:let'?s|lets)\s+(?:do|make|produce|create|start|generate|render)\b", value):
        return False
    if re.search(r"\b(?:start|go ahead|do it|render|generate|make|begin)\b.*\b(?:it|this|video|render|production)\b", value):
        return False
    diagnostic_terms = (
        "wrong short", "wrong video", "wrong one", "wrong title", "previous short", "previous video",
        "old short", "old video", "same video", "same short", "already made",
        "already been made", "why are you", "why is it", "why do you keep",
        "keep trying to make", "keeps trying to make", "what is causing",
        "what's causing", "causing it", "stuck", "do i need to start a new chat",
        "need to start a new chat", "trying to build", "keeps trying", "keep getting stuck",
    )
    return any(term in value for term in diagnostic_terms)


_EXPAND_SHORT_TERMS = (
    "make the rest",
    "make the full",
    "make the other",
    "make the remaining",
    "other scenes",
    "other five scenes",
    "rest of the short",
    "rest of the video",
    "remaining scenes",
    "continue the short",
    "finish the short",
    "finish the video",
    "finish making the video",
    "finish making",
    "full short",
    "all scenes",
    "generate the rest",
    "build the rest",
    "complete the short",
    "make the entire short",
    "make the whole short",
    "use this scene",
    "using this scene",
    "use this as scene",
    "using this as scene",
    "keep scene 1",
    "keep this scene",
    "use scene 1",
    "using scene 1",
    "as scene 1",
    "this as scene",
)


def strip_agent_mode_prefix(text: str) -> str:
    """Remove frontend-injected mode banners so intent detection sees the user's words."""
    cleaned = str(text or "").strip()
    cleaned = re.sub(
        r"^\[Studio Agent mode:[^\]]+\]\s*(?:Use normal Studio Agent behavior[^\n]*\n+)?",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(
        r"^\[Studio Agent mode: ClipLab\][^\n]*\n+",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    return cleaned.strip()


_BULK_SCENE_SHIP_FIX_TERMS = (
    "fix scene",
    "scene 1",
    "scene 2",
    "scene 3",
    "scene 4",
    "scene 5",
    "scene 6",
    "regenerate",
    "re-generate",
    "extra hand",
    "diptych",
    "split screen",
    "artifact",
    "eyeball",
    "eyeballs",
)


def is_bulk_scene_ship_request(text: str) -> bool:
    """True when the user wants to approve all scenes, animate, and finish export."""
    low = strip_agent_mode_prefix(text).lower()
    if not low:
        return False
    if any(term in low for term in _BULK_SCENE_SHIP_FIX_TERMS):
        return False
    has_approve = any(
        phrase in low
        for phrase in (
            "approve all",
            "approve every",
            "approve the scenes",
            "approve them",
            "approved all",
            "i approve",
            "looks good",
            "look good",
            "looks great",
            "perfect",
            "ship it",
        )
    )
    has_ship = any(
        term in low
        for term in (
            "animate",
            "finish",
            "finalize",
            "export",
            "complete",
            "ship",
            "build the video",
            "make the video",
            "the video",
        )
    )
    # A creator who says "animate them and make the finished video" is
    # authorizing the existing scene set, not asking Studio to create more
    # scenes. Keep explicit proof-expansion language on the expansion path.
    has_expansion_scope = bool(re.search(
        r"\b(?:other|rest|remaining|more)\s+(?:\d+\s+)?scenes?\b|"
        r"\b(?:make|build|generate|render)\s+(?:the\s+)?(?:other|rest|remaining)\b|"
        r"\b(?:keep|approve|use)\s+scene\s*(?:1|one|first)\b",
        low,
    ))
    has_plural_animation = bool(re.search(
        r"\banimate\s+(?:all|every|each|them|the\s+scenes?)\b|"
        r"\banimation\s+for\s+(?:all|every|each|the\s+scenes?)\b",
        low,
    ))
    has_finished_output = bool(re.search(
        r"\b(?:make|build|render|produce)\s+(?:the\s+)?(?:finished|final|complete)\s+(?:video|short|mp4)\b|"
        r"\b(?:finish|finalize|complete|export)\b.{0,40}\b(?:video|short|mp4|it)\b|"
        r"\b(?:finished|final|complete)\s+(?:video|short|mp4)\b",
        low,
    ))
    if has_plural_animation and has_finished_output and not has_expansion_scope:
        return True
    has_all = any(term in low for term in ("all", "every", "each", "rest of", "of the scenes"))
    if "approve all" in low and has_ship:
        return True
    if has_approve and has_all and has_ship:
        return True
    if "approve" in low and "scene" in low and has_ship:
        return True
    return False


def is_expand_short_request(text: str) -> bool:
    """True when the user wants to keep an approved first scene and generate the rest."""
    low = strip_agent_mode_prefix(text).lower()
    if not low:
        return False
    if is_bulk_scene_ship_request(low):
        return False
    if re.search(r"\b(?:start|create|generate)\s+(?:a\s+)?(?:new|brand new)\s+(?:short|video)\b", low):
        return False
    # "Do not make the full short yet" is a visual-proof request, NOT expand.
    if re.search(
        r"\b(?:do\s+not|don't|dont|never|not\s+yet)\b.{0,40}\b(?:make\s+the\s+full|full\s+short|entire\s+short|whole\s+short)\b",
        low,
    ) or re.search(
        r"\b(?:make\s+the\s+full|full\s+short|entire\s+short).{0,20}\b(?:yet|now)\b",
        low,
    ) and re.search(r"\b(?:do\s+not|don't|dont|not)\b", low):
        return False
    if any(term in low for term in _EXPAND_SHORT_TERMS):
        return True
    if re.search(
        r"\b(?:use|using|keep)\s+(?:this|the|my)\s+(?:as\s+)?scene\s*(?:1|one|first)?\b",
        low,
    ):
        return True
    if re.search(r"\bscene\s*(?:1|one|first)\b", low) and re.search(
        r"\b(?:rest|remaining|finish|complete|other|more)\b",
        low,
    ):
        return True
    # "I like scene one. Make the other five / go ahead and make the other scenes"
    if re.search(r"\b(?:like|love|approve[sd]?)\s+scene\s*(?:1|one|first)\b", low) and re.search(
        r"\b(?:make|build|generate|do|finish|continue|go\s+ahead)\b",
        low,
    ):
        return True
    if re.search(
        r"\b(?:make|build|generate)\s+(?:the\s+)?other\s+(?:\d+\s+)?scenes?\b|"
        r"\bgo\s+ahead\s+and\s+make\s+(?:the\s+)?(?:other|rest|remaining)\b|"
        r"\b(?:other|remaining)\s+\d+\s+scenes?\b",
        low,
    ):
        return True
    return False


def _is_visual_proof_request(text: str) -> bool:
    low = str(text or "").lower()
    if is_expand_short_request(low):
        return False
    if re.search(r"\bscene\s+\d+\b", low) and any(
        term in low for term in ("rest", "remaining", "finish", "complete", "use this", "using this", "keep")
    ):
        return False
    if re.search(r"\b(?:exactly\s+)?(?:one|1|single)\s+(?:scene|still|image|frame)\b", low):
        return True
    if re.search(
        r"\b(?:make|render|start|build|produce|generate)\s+(?:the\s+)?(?:very\s+)?(?:first|1st)\s+scene\b|"
        r"\b(?:make|render|start|build|produce|generate)\s+scene\s*(?:#?\s*1|one)\b",
        low,
    ):
        return True
    proof_terms = (
        "first image",
        "first still",
        "visual proof",
        "proof image",
        "proof still",
        "test still",
        "test image",
        "approve the look",
        "approve of it being able to generate the entire short",
        "animate that one scene",
        "animate exactly one scene",
        "do not make the full short",
        "not make the full short",
        "stop after the first still",
        "stop after the first scene",
    )
    return any(term in low for term in proof_terms)


_PRODUCT_AD_TERMS = (
    "product ad",
    "product ads",
    "advertisement",
    "advertising",
    "ad creative",
    "ad from",
    "make an ad",
    "make a ad",
    "create an ad",
    "promo video",
    "promotional video",
    "dropshipping",
    "e-com",
    "ecommerce",
    "e-commerce",
    "landing page",
    "my website",
    "product website",
    "product page",
    "shopify",
    "tiktok ad",
    "meta ad",
    "facebook ad",
    "google ad",
    "youtube ad",
    "performance ad",
    "conversion ad",
    "signup ad",
    "sign up ad",
    "trial ad",
    "saas ad",
    "product demo",
    "product video",
)


def is_product_ad_request(text: str) -> bool:
    low = str(text or "").lower()
    if any(term in low for term in _PRODUCT_AD_TERMS):
        return True
    if re.search(r"\b(?:https?://|www\.)[^\s]+", low) and any(
        word in low for word in ("product", "shop", "store", "buy", "pricing", "signup", "sign up", "trial")
    ):
        return True
    return bool(re.search(r"\b(?:sell|promote|market)\b.+\b(?:product|offer|course|membership|app)\b", low))


def strip_attachment_boilerplate(text: str) -> str:
    """Remove frontend-injected attachment instructions so intent detection sees user words."""
    cleaned = str(text or "")
    cleaned = re.sub(r"\[video attachment ready for cliplab:[^\]]*\][^\n]*", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\[uploaded reference video:[^\]]*\][^\n]*", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\[attachment:[^\]]*\][^\n]*", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\blocal_path:\s*\S+", "", cleaned, flags=re.I)
    boilerplate_lines = (
        r"analyze this uploaded reference[^\n]*",
        r"please analyze the attached reference video[^\n]*",
        r"use this upload as creative context for planning and ideation[^\n]*",
        r"discuss channel strategy, niche research, and art direction conversationally[^\n]*",
        r"only analyze pacing/structure or start rendering if i explicitly ask[^\n]*",
        r"reference video is available for creative context[^\n]*",
        r"only call analyze_reference_video if i explicitly ask[^\n]*",
        r"only start production if i explicitly ask to render[^\n]*",
        r"i attached a reference video for planning context[^\n]*",
        r"use ingest_cliplab_attachment[^\n]*",
        r"analyze_cliplab_video[^\n]*",
    )
    for pattern in boilerplate_lines:
        cleaned = re.sub(pattern, "", cleaned, flags=re.I)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned


def _user_message_before_attachments(text: str) -> str:
    """Keep only the human-authored portion before auto-injected attachment blocks."""
    cleaned = strip_agent_mode_prefix(str(text or ""))
    for marker in (
        "[Uploaded reference video:",
        "[Video attachment ready for ClipLab:",
        "[Attachment:",
    ):
        if marker.lower() in cleaned.lower():
            cleaned = re.split(re.escape(marker), cleaned, maxsplit=1, flags=re.I)[0]
    return strip_attachment_boilerplate(cleaned).strip()


_IDEATION_PHRASES = (
    "how would i",
    "how should i",
    "how could i",
    "how do i start a",
    "how do i build a",
    "how do i make a youtube channel",
    "help me plan",
    "help me think",
    "help me figure",
    "brainstorm",
    "ideation",
    "market research",
    "niche research",
    "competitor research",
    "content strategy",
    "channel strategy",
    "youtube channel for",
    "start a channel",
    "start a youtube",
    "art style",
    "visual style",
    "positioning",
    "what niche",
    "what kind of channel",
    "what should i post",
    "topic ideas",
    "video ideas",
    "content ideas",
    "plan out",
    "planning out",
    "think through",
    "figure out what",
    "using this as reference",
    "using this video as reference",
    "as a reference",
    "content like this",
    "similar content",
    "public youtube data",
    "recent public",
    "don't know what to make",
    "dont know what to make",
    "what to make",
    "what to film",
    "what to create",
    "channel for content",
    "make a youtube channel",
    "build a youtube channel",
    "create a youtube channel",
)


def is_hard_production_commit(text: str) -> bool:
    """True only when the user clearly commits to starting a render/build now."""
    low = strip_attachment_boilerplate(strip_agent_mode_prefix(text)).lower().strip()
    if not low:
        return False
    # Expanding Scene 1 into the rest of the short is not a brand-new production commit.
    if is_expand_short_request(low):
        return False
    if re.search(r"\b(?:how|what if|maybe|should we|could we)\b", low) and "?" in low:
        # Soft questions never hard-commit even if they contain "make"
        if not re.search(r"\b(?:now|right now|approve|render that plan)\b", low):
            return False
    if re.search(
        r"\b(?:start_shortform_generate|start_longform_render|approve and run|build the scenes|"
        r"start production now|render (?:it|this|the short|the video|the ad|that plan)?\s*now|"
        r"generate (?:it|this|the short|the video|the ad)?\s*now|"
        r"render that plan|make that plan|use that plan|lock (?:the |this )?concept|"
        r"looks good[,.]?\s*(?:make|render)|commit (?:to )?(?:this|that) plan)\b",
        low,
    ):
        return True
    if re.search(
        r"\b(?:go ahead and|please)\s+(?:make|create|render|generate|produce|start)\b",
        low,
    ):
        return True
    # Scene/still commits after a plan is ready ("make the first scene", "render scene 1").
    if re.search(
        r"\b(?:make|render|start|build|produce|generate)\s+(?:the\s+)?(?:very\s+)?(?:first|1st)\s+scene\b|"
        r"\b(?:make|render|start|build|produce|generate)\s+scene\s*(?:#?\s*1|one)\b|"
        r"\b(?:make|render|start|build|produce|generate)\s+(?:the\s+)?(?:first\s+)?(?:still|frame|image)\b|"
        r"\b(?:let'?s|please)\s+(?:make|render|start|build)\s+(?:the\s+)?(?:very\s+)?(?:first|1st)\s+scene\b",
        low,
    ):
        return True
    # "yes, make it" / "yes make it, but only 30 seconds"
    if re.search(
        r"\b(?:yes|yeah|yep|sure|ok(?:ay)?|do it|go ahead)\b.+\b(?:make|render|generate|produce|start|build)\b",
        low,
    ) and not re.search(r"\b(?:how|what if|maybe)\b", low):
        return True
    # Standalone "make it" / "render it" after a plan (not advice: "make it better").
    if re.search(r"\b(?:just\s+)?(?:make|render|start|build)\s+it\b", low):
        if not re.search(r"\b(?:how|what if|maybe|better|should we|could we)\b", low):
            return True
    if re.search(
        r"\b(?:make|create|render|generate|produce|start)\b.+\b(?:short|video|production)\b.+\b(?:now|right now)\b",
        low,
    ):
        return True
    if re.search(
        r"\b(?:make|create|render|generate|produce|start)\b.+\b(?:now|right now)\b.+\b(?:short|video)\b",
        low,
    ):
        return True
    return False


def is_scene_one_proof_commit(text: str) -> bool:
    """True when the user is asking to render/build scene 1 only (visual proof)."""
    low = strip_attachment_boilerplate(strip_agent_mode_prefix(text)).lower().strip()
    if not low:
        return False
    return bool(
        re.search(
            r"\b(?:make|render|start|build|produce|generate)\s+(?:the\s+)?(?:very\s+)?(?:first|1st)\s+scene\b|"
            r"\b(?:make|render|start|build|produce|generate)\s+scene\s*(?:#?\s*1|one)\b|"
            r"\b(?:let'?s|please)\s+(?:make|render|start|build|generate)\s+(?:the\s+)?(?:very\s+)?(?:first|1st)\s+scene\b|"
            r"\b(?:make|render|generate)\s+(?:the\s+)?(?:first|1st)\s+(?:still|frame|image)\b|"
            r"\bscene\s*(?:#?\s*1|one)\s+(?:please|now)\b",
            low,
        )
    )


def is_soft_production_proposal(text: str) -> bool:
    """Soft 'let's make…' / proposal — plan first, never Approve-to-render.

    Root cause fix: bare \"let's make [title]\" (no short/video word, no 'now')
    used to fall through as production and open Approve during research chat.
    That is always planning unless hard-commit words are present.
    """
    low = strip_attachment_boilerplate(strip_agent_mode_prefix(text)).lower().strip()
    if not low:
        return False
    # A cost/scope question can mention "make the short", "video", and contain
    # a question mark without proposing a new concept. It must reach the
    # deterministic estimator with the existing plan intact.
    if any(term in low for term in ("how much", "cost", "price", "pricing", "budget", "spend")):
        return False
    # Readiness is a status question, not permission and not a new concept.
    if re.search(
        r"\b(?:are|is) (?:we|it|this|the plan) (?:actually )?ready\b|"
        r"\bdoes this mean (?:we are|we'?re|it is|it'?s) ready\b",
        low,
    ):
        return False
    # Topic discovery is conversation/research, even when naturally phrased as
    # "what is another topic we can make a video on?". The make+video tokens
    # must not steal the turn and open a placeholder concept card.
    if is_exact_topic_discovery_request(text):
        return False
    if is_hard_production_commit(text):
        return False
    # Bare "let's make …" / "alright let's make Title" without now/render = plan.
    if re.search(
        r"\b(?:alright|okay|ok|so|then)?\s*(?:let'?s|lets|we could|we should)\s+"
        r"(?:make|create|do|produce|plan|pitch)\b",
        low,
    ):
        if not re.search(
            r"\b(?:now|right now|render|approve|start production|build the scenes|"
            r"start_shortform|start_longform)\b",
            low,
        ):
            return True
    mentions_content = bool(
        re.search(
            r"\b(?:short|short-form|shortform|video|long[- ]?form|longform|documentary|"
            r"product\s+ad|saas|app\s+ad|promo|ad\s+video)\b",
            low,
        )
        and re.search(r"\b(?:make|create|do|produce|generate|build|start|plan|pitch)\b", low)
    )
    if not mentions_content:
        # Quoted title after make: let's make "Why Men..."
        if re.search(r"\b(?:make|create|produce)\b.+[\"“'][^\"”']{8,}[\"”']", low):
            if not re.search(r"\b(?:now|right now|render|approve)\b", low):
                return True
        return False
    softener = bool(
        re.search(
            r"\b(?:how(?:'s|s)? that|how about|what do you think|thoughts|instead|"
            r"maybe|could we|should we|what if|if we|would that work|sound good|"
            r"wdyt|idea|pitch|concept|plan)\b",
            low,
        )
        or ("?" in low)
    )
    if softener:
        return True
    if re.search(r"\b(?:let'?s|lets|we could|we should)\s+(?:make|create|do|produce|plan)\b", low):
        if not re.search(r"\b(?:now|right now|render|approve|start production|build the scenes)\b", low):
            return True
    # "make a short about X" without now = still plan (explicit_production already False)
    if re.search(
        r"\b(?:make|create|produce)\b.+\b(?:short|video|long[- ]?form|ad)\b",
        low,
    ) and not re.search(r"\b(?:now|right now|please make|go ahead|render|approve)\b", low):
        return True
    return False


def is_production_strategy_question(text: str) -> bool:
    """Advice / how-to / guarantee questions — discuss, do not start a render.

    Examples: "how can we make it better and guarantee 3k views?"
    """
    low = strip_attachment_boilerplate(strip_agent_mode_prefix(text)).lower().strip()
    if not low:
        return False
    # Cost/pricing questions are scope calculations, not a request to replace
    # the current concept with a new generic plan. "Better question, how much
    # will this cost?" previously matched the loose word "better" below.
    if any(term in low for term in ("how much", "cost", "price", "pricing", "budget", "spend")):
        return False
    if is_hard_production_commit(text):
        return False
    if is_soft_production_proposal(text):
        return True
    if re.search(
        r"\b(?:how (?:can|do|would|should) (?:we|i|you)|what (?:should|can|would) (?:we|i)|"
        r"how to|ways to|tips? (?:to|for)|advice|recommend|improve|better|"
        r"guarantee|increase views|get more views|hit \d+k|average \d+k)\b",
        low,
    ):
        return True
    if "?" in low and re.search(
        r"\b(?:make it better|improve|optimize|package|hook|retention|views?)\b",
        low,
    ):
        return True
    if re.search(r"\bmake it better\b", low) and not re.search(
        r"\b(?:render|generate|start|produce|build)\b.+\b(?:now|please|short|video)\b",
        low,
    ):
        return True
    return False


def is_explicit_production_request(text: str) -> bool:
    """True when the user is asking to render/resume production, not just plan.

    Soft proposals ("let's make a short, how's that?") stay in planning until a
    hard commit ("render it now", "go ahead and make the short").
    """
    low = strip_attachment_boilerplate(strip_agent_mode_prefix(text)).lower()
    if not low:
        return False
    # Expanding an approved Scene 1 proof is NOT a brand-new short.
    if is_expand_short_request(low):
        return False
    # Soft proposals / strategy never open the Approve card.
    if is_soft_production_proposal(text) or is_production_strategy_question(text):
        return False
    # Channel download/analyze is research, never production.
    if is_channel_video_analysis_request(text):
        return False
    # Hard commit path
    if is_hard_production_commit(text):
        return True
    # Research / demand questions with optional future production stay in research.
    try:
        from studio_agent.live_demand import has_demand_signal, is_research_execution_request

        if has_demand_signal(text) or is_research_execution_request(text):
            if re.search(
                r"\b(?:if you can|if we can|then we can|once we know|after (?:that|research|demand)|"
                r"figure out|find out|what people|public (?:youtube )?data)\b",
                low,
            ):
                return False
            if re.search(r"\b(?:make|create|produce)\b.+\bshort\b", low) and re.search(
                r"\b(?:from it|based on|after|then|once)\b",
                low,
            ):
                return False
    except Exception:
        pass
    if re.search(
        r"\b(?:start|render|generate|animate|finalize|export|ship)\b.+\b(?:short|video|production|render|job)\b",
        low,
    ):
        # Without hard commit words, still require now/please for generic make-short
        if re.search(r"\b(?:now|right now|please|go ahead)\b", low):
            return True
        return False
    if re.search(
        r"\b(?:make|create|generate|render|produce|build)\b.+\b(?:short|short-form|shortform)\b",
        low,
    ):
        if re.search(r"\b(?:now|right now|please make|go ahead|start making|render)\b", low):
            return True
        if re.search(r"\b(?:if |then |once |after |can we |could we |how|what if|maybe)\b", low):
            return False
        # Bare "make a short" without softener or commit = still plan first
        return False
    # "let's make" without hard commit is handled by soft proposal (False above)
    if any(term in low for term in ("start_shortform_generate", "finalize_production", "animate_production_scenes")):
        return True
    return False


def is_reference_description_correction(text: str) -> bool:
    """True when the user is correcting a prior visual/content misread — not requesting a new analysis run."""
    low = _user_message_before_attachments(text).lower()
    if not low:
        return False
    correction_phrases = (
        "not a skeleton",
        "isn't a skeleton",
        "is not a skeleton",
        "not skeleton",
        "that's wrong",
        "that is wrong",
        "you got it wrong",
        "you're wrong",
        "you are wrong",
        "misidentified",
        "wrong about the video",
        "wrong about my video",
        "wrong about what i uploaded",
        "wrong about the upload",
        "that's not what",
        "that is not what",
        "you described the wrong",
    )
    if any(phrase in low for phrase in correction_phrases):
        return True
    if re.search(
        r"\b(?:not|isn't|is not|wasn't|was not)\b.+\b(?:skeleton|live[- ]?action|filmed|real person|motion graphics?|3d animated?|2d animated?)\b",
        low,
    ):
        return True
    return False


def is_uploaded_video_analysis_request(text: str) -> bool:
    """True when the user wants the attached/uploaded video analyzed (not channel latest)."""
    low = _user_message_before_attachments(text).lower()
    if not low:
        return False
    if re.search(r"\b(?:only|unless|if i explicitly ask|don't|do not)\b.+\banaly", low):
        return False
    if re.search(r"\banaly(?:ze|sis|zing)\b", low):
        if re.search(
            r"\b(?:the |this |my |that |uploaded |attached )?(?:video|upload|reference|clip|mp4|file)\b",
            low,
        ):
            return True
        if re.search(r"\banaly(?:ze|sis)\b.+\b(?:it|again)\b", low):
            return True
        if "you will see" in low or "you'll see" in low:
            return True
    if re.search(r"\b(?:look at|watch|see)\b.+\b(?:the |this |my )?(?:video|upload|clip|file)\b", low):
        return True
    if re.search(r"\bwatch\b.+\b(?:uploaded|attached)\b.+\b(?:video|clip|file)\b", low):
        return True
    return False


def is_channel_video_analysis_request(text: str) -> bool:
    """True when user wants channel uploads downloaded + analyzed (research, not production).

    Matches follow-ups like "go ahead and download all three of the public videos
    that are on that channel and analyze them" even without a fresh URL paste.
    """
    low = _user_message_before_attachments(text).lower()
    if not low:
        return False
    # Pure production language wins only when no analysis/download intent.
    has_analysis = bool(
        re.search(
            r"\b(?:download|watch|analy[sz]e|study|review|break down|audit|inspect)\b",
            low,
        )
    )
    has_channel_videos = bool(
        re.search(
            r"\b(?:channel|uploads?|that channel|the channel|my channel|on (?:that|the|my) channel|"
            r"public videos?|all (?:\d+\s+)?(?:of\s+)?(?:the\s+)?(?:public\s+)?videos?)\b",
            low,
        )
    )
    if has_analysis and has_channel_videos:
        return True
    if has_analysis and re.search(r"\b(?:youtube\.com/@|youtube\.com/channel/)\b", low):
        return True
    # "download all three videos" / "watch them and analyze" after a channel was named
    if re.search(r"\bdownload\b.+\b(?:all|three|3|videos?)\b", low) and re.search(
        r"\b(?:analy[sz]e|watch|study)\b",
        low,
    ):
        return True
    return False


def is_best_vs_worst_shorts_request(text: str) -> bool:
    """True when user wants best/worst (or top/underperforming) Shorts compared."""
    low = _user_message_before_attachments(text).lower()
    if not low:
        return False
    has_rank = bool(
        re.search(
            r"\b(?:best|worst|top|underperform(?:ing)?|highest|lowest|winner|loser)\b",
            low,
        )
    )
    has_shorts = bool(
        re.search(
            r"\b(?:short|shorts|perform(?:ing|ance)?|retention|views|avd|analytics)\b",
            low,
        )
    )
    return has_rank and has_shorts


def is_youtube_channel_url_reference_request(text: str) -> bool:
    """True when the user wants a YouTube channel analyzed (URL may be in this turn)."""
    try:
        from studio_agent.turn_plan import extract_youtube_channel_urls_from_text
    except Exception:
        return False
    has_url = bool(extract_youtube_channel_urls_from_text(text))
    low = _user_message_before_attachments(text).lower()
    # Name-only competitor channels ("analyze Lume") are not URL-reference turns.
    if is_channel_video_analysis_request(text) and has_url:
        return True
    if not has_url:
        return False
    # Explicit analysis / download / review verbs
    if re.search(
        r"\b(?:analyze|analyse|study|watch|download|review|break down|audit|inspect|"
        r"same niche|in (?:this|the|my) niche)\b",
        low,
    ):
        return True
    # Owned-channel diagnostics (common Studio Agent path)
    if re.search(
        r"\b(?:i own|my channel|this is my|what i(?:'|’)ve been posting|what iv been posting|"
        r"what i been posting|my uploads?|my videos?|go to (?:it|the channel)|"
        r"look at (?:my|this|the) channel)\b",
        low,
    ):
        return True
    if re.search(r"\bthis is (?:the\s+)?\w[\w\s]{0,20}\s+youtube channel\b", low):
        return True
    # Channel URL alone + view/performance complaint → treat as analysis request
    if re.search(r"\b(?:views?|subscribers?|avg|average|underperform|not getting)\b", low):
        return True
    return False


def is_competitor_channel_reference_request(text: str) -> bool:
    """True when the user names an external/reference YouTube channel to study."""
    if is_youtube_channel_url_reference_request(text):
        return True
    low = _user_message_before_attachments(text).lower()
    if not low:
        return False
    if is_connected_channel_performance_request(text):
        return False
    markers = (
        "channel to analyze",
        "channel to analyse",
        "reference channel",
        "competitor channel",
        "here is a channel",
        "this is a channel",
        "here is the channel",
        "look at this channel",
        "check out this channel",
        "youtube channel is in this niche",
        "channel in this niche",
        "channel for this niche",
    )
    if any(marker in low for marker in markers):
        return True
    if re.search(
        r"\b(?:lume|jake tran|fern|wendover|johnny harris|magnates|coldfusion)\b.+\bchannel\b",
        low,
    ):
        return True
    if re.search(
        r"\bchannel\b.+\b(?:lume|jake tran|fern|wendover|johnny harris|magnates|coldfusion)\b",
        low,
    ):
        return True
    return False


def is_youtube_url_reference_request(text: str) -> bool:
    """True when the user pasted a YouTube link and wants it downloaded/analyzed."""
    try:
        from studio_agent.turn_plan import extract_youtube_urls_from_text
    except Exception:
        return False
    if not extract_youtube_urls_from_text(text):
        return False
    low = _user_message_before_attachments(text).lower()
    if re.search(r"\b(?:download|watch|analyze|analyse|study|break down|deconstruct)\b", low):
        return True
    if re.search(r"\b(?:reference|example|sample)\s+video\b", low):
        return True
    if re.search(r"\bthis\s+(?:video|link|url)\b", low):
        return True
    return False


def is_contextual_reference_video_request(text: str) -> bool:
    """True when the user wants a prior-mentioned video downloaded/analyzed (no URL in message)."""
    low = _user_message_before_attachments(text).lower()
    if not low:
        return False
    if is_youtube_url_reference_request(text):
        return False
    if not re.search(r"\b(?:download|watch|analyze|analyse|study|break down|see)\b", low):
        return False
    return bool(
        re.search(
            r"\b(?:that|this|the|specific)\s+(?:video|upload|clip|reference)\b|"
            r"\b(?:download|watch|analyze|analyse|see)\s+(?:it|that)\b|"
            r"\bthat\s+specific\s+video\b",
            low,
        )
    )


def is_explicit_reference_analysis_request(text: str) -> bool:
    """True when the user explicitly wants pacing/editing analysis of an upload."""
    low = _user_message_before_attachments(text).lower()
    if is_contextual_reference_video_request(text):
        return True
    if is_youtube_url_reference_request(text):
        return True
    if is_reference_description_correction(low) and not is_uploaded_video_analysis_request(text):
        return False
    if not low:
        return False
    if re.search(r"\b(?:only|unless|if i explicitly ask|don't|do not)\b.+\banaly", low):
        return False
    if is_uploaded_video_analysis_request(text):
        return True
    if "analyze_reference_video" in low:
        return True
    if re.search(
        r"\banaly(?:ze|sis)\b.+\b(?:pacing|editing|hook|structure|reference|cuts?|scene structure)\b",
        low,
    ):
        return True
    if re.search(
        r"\b(?:study|break down|deconstruct)\b.+\b(?:pacing|editing|hook|cuts?|structure)\b",
        low,
    ):
        return True
    if re.search(r"\banaly(?:ze|sis)\b.+\b(?:attached|uploaded)\b.+\b(?:reference|video)\b", low):
        return True
    return False


def is_explicit_tool_go_ahead(text: str) -> bool:
    """User explicitly approved running tools, research, or analysis."""
    low = _user_message_before_attachments(text).lower().strip()
    if not low:
        return False
    phrases = (
        "yes you can",
        "yes, you can",
        "yes do it",
        "yes, do it",
        "yes run",
        "yes, run",
        "yes continue",
        "yes, continue",
        "please continue",
        "try again",
        "go ahead",
        "go for it",
        "run that",
        "run it",
        "do that",
        "do it",
        "please run",
        "please pull",
        "please analyze",
        "you can do that",
        "you can run",
        "sounds good run",
        "ok run",
        "okay run",
        "sure run",
        "approved",
        "execute that",
        "execute it",
    )
    if any(phrase in low for phrase in phrases):
        return True
    if re.search(
        r"\b(?:yes|yeah|yep|sure|ok(?:ay)?)\b.+\b(?:run|pull|analyze|fetch|do it|go ahead|continue|try again)\b",
        low,
    ):
        return True
    if re.search(r"\btry again\b.+\b(?:watch|video|upload|analyze)\b", low):
        return True
    return False


def is_imperative_tool_command(text: str) -> bool:
    """Unambiguous execute-now commands, not open planning questions."""
    low = _user_message_before_attachments(text).lower()
    if not low:
        return False
    if "?" in low and not is_explicit_tool_go_ahead(text):
        return False
    if is_uploaded_video_analysis_request(text):
        if is_explicit_tool_go_ahead(text) or re.search(
            r"\b(?:continue|watch|see)\b.+\b(?:video|upload)\b",
            low,
        ):
            return True
        if not re.search(r"\b(?:now|right now|immediately|go ahead)\b", low):
            return False
    patterns = (
        r"\b(?:pull|fetch|refresh|run|execute|start)\b.+\b(?:now|right now|immediately)\b",
        r"\b(?:go ahead and|please)\s+(?:pull|run|analyze|fetch|start|get|watch|download)\b",
        r"\b(?:pull|run|analyze|fetch|watch|download)\s+(?:the|my|this)\s+(?:channel|analytics|data|video|reference|upload)\b",
        r"\bwatch\b.+\b(?:the|my|this)\s+(?:uploaded|attached)\b.+\b(?:video|clip|file)\b",
        r"\b(?:download|watch)\b.+\b(?:video|youtu)\b",
    )
    return any(re.search(pattern, low) for pattern in patterns)


def is_public_youtube_research_request(text: str) -> bool:
    """True when the user wants public YouTube niche/market performance pulled now.

    Conversation-first: natural demand questions auto-research. Only pure capability
    questions ("can you pull data sometime?") still wait for go-ahead.
    """
    low = _user_message_before_attachments(text).lower()
    if not low:
        return False
    if is_explicit_production_request(low):
        return False
    # A natural-language request can be phrased as a capability question while
    # still clearly asking for execution now (for example: "can you pull the
    # most recent public YouTube data in this channel's niche so we can see what
    # works?").  Do not let the leading "can you" downgrade that compound
    # request into conversation-only mode.  Pure capability questions without
    # a concrete target, timeframe, or purpose still wait for a go-ahead.
    concrete_pull_request = bool(
        re.search(r"\b(?:pull|search|find|get|fetch|look up|check|run|grab|show me)\b", low)
        and re.search(r"\b(?:youtube|public|niche|market|trend|performance|data)\b", low)
        and (
            re.search(r"\b(?:most recent|recent|latest|current|fresh|live|today|right now)\b", low)
            or re.search(r"\b(?:this|that|my|the)\s+(?:channel|niche|market|topic|space)\b", low)
            or re.search(r"\b(?:so|to)\s+(?:we|i)\s+can\b", low)
            or re.search(r"\bwhat\b.+\b(?:work|works|working|perform|performs|performing)\b", low)
        )
    )
    # Live Demand phrases always count as research intent (even with ?)
    try:
        from studio_agent.live_demand import has_demand_signal, is_research_execution_request

        if is_research_execution_request(text) or has_demand_signal(text):
            # Soft "can you eventually..." without demand/urgency still waits
            if re.search(r"\b(?:can|could|would)\s+you\b", low) and not concrete_pull_request and not re.search(
                r"\b(?:now|right now|please|what(?:'s| is)|working|performing|viral|trending|research)\b",
                low,
            ):
                return False
            return True
    except Exception:
        pass
    has_question = "?" in low
    # Polite capability-only questions wait for go-ahead unless urgent.
    if has_question and not is_explicit_tool_go_ahead(text) and not concrete_pull_request:
        if not re.search(
            r"\b(?:now|right now|immediately|go ahead|please pull|please run|"
            r"what(?:'s| is)|working|performing|viral|trending)\b",
            low,
        ):
            return False
    imperative = bool(
        re.search(
            r"\b(?:pull|search|find|get|fetch|look up|check|run|need to search|want to search|show me)\b",
            low,
        )
    )
    research_topic = bool(
        re.search(
            r"\b(?:public youtube|youtube niche|niche performance|niche data|public search|"
            r"youtube performance|public demand|market data|performance data|search trends|"
            r"youtube data|what(?:'s| is) performing|what(?:'s| is) working)\b",
            low,
        )
    )
    if research_topic and imperative:
        return True
    if research_topic and not has_question:
        return True
    if research_topic and has_question:
        return True
    if imperative and any(term in low for term in ("youtube", "niche", "public", "trend", "market")):
        if any(term in low for term in ("data", "performance", "search", "demand", "trend", "niche")):
            return True
    return False


def should_auto_run_tools(text: str) -> bool:
    """True when Studio may execute backend tools without waiting for another approval turn.

    Conversation-first rule (ChatGPT/Grok-style): if the user is clearly asking what is
    working / in demand / trending in a niche, research tools run immediately. Haiku is
    weaker at pure inference, so Live Demand tools are how it understands intent.
    """
    if is_channel_video_analysis_request(text):
        return True
    # Uploaded / explicit reference analysis must run tools, not sit as ideation chat.
    if is_uploaded_video_analysis_request(text) or is_explicit_reference_analysis_request(text):
        return True
    if is_explicit_tool_go_ahead(text):
        return True
    if is_imperative_tool_command(text):
        return True
    if is_explicit_production_request(text):
        return True
    if is_public_youtube_research_request(text):
        return True
    if is_youtube_url_reference_request(text):
        return True
    if is_contextual_reference_video_request(text):
        return True
    if is_competitor_channel_reference_request(text):
        return True
    if is_youtube_channel_url_reference_request(text):
        return True
    # Topic discovery is an execution request even when the creator phrases it
    # conversationally ("we need to find an exact topic first").  Do not make
    # them discover a magic prompt just to get Catalyst to use their channel
    # evidence and public demand data.
    if is_exact_topic_discovery_request(text):
        return True
    # Live Demand: demand language is enough to auto-research (even as a question).
    try:
        from studio_agent.live_demand import has_demand_signal, is_research_execution_request

        low = _user_message_before_attachments(text).lower()
        if is_research_execution_request(text):
            return True
        if has_demand_signal(text):
            # Questions like "what's working for day trading?" should research, not wait.
            if re.search(
                r"\b(?:pull|fetch|research|check|find|get|based on|need to|we need|"
                r"what(?:'s| is| are)|show|tell|verify|confirm)\b",
                low,
            ):
                return True
            if is_explicit_production_request(text) or is_product_ad_request(text):
                return True
            # Niche + demand phrase without an explicit verb still means "look this up"
            if re.search(
                r"\b(?:day\s*trad|trading|crypto|forex|psycholog|fitness|niche|shorts?|youtube)\b",
                low,
            ):
                return True
    except Exception:
        pass
    return False


def is_exact_topic_discovery_request(text: str) -> bool:
    """True when the creator wants Studio to choose the next concrete topic."""
    low = _user_message_before_attachments(text).lower()
    if not low:
        return False
    return bool(re.search(
        r"\b(?:need(?:s)?\s+to\s+)?(?:find|pick|choose|give|recommend|figure\s+out)\s+"
        r"(?:an?\s+)?(?:exact\s+|new\s+|next\s+)?topic\b"
        r"|\b(?:need|want)\s+(?:an?\s+)?exact\s+topic\b"
        r"|\btopic\s+first\b"
        r"|\bwhat\s+(?:exact\s+|new\s+|next\s+)?topic\b"
        r"|\bwhat\s+(?:is|are|'s)\s+(?:an?\s+)?(?:another|other|new|next)\s+(?:[a-z]+\s+){0,3}topics?\b"
        r"|\b(?:give|show|recommend)\s+(?:me\s+)?(?:another|other|new|next|some)\s+(?:[a-z]+\s+){0,3}topics?\b",
        low,
    ))


def is_conversational_planning_turn(text: str) -> bool:
    """Pure planning/discussion — converse first, tools only after explicit go-ahead.

    Demand/research questions are NOT pure planning: they auto-run Live Demand tools
    (conversation → inferred intent → tools), matching Grok/ChatGPT behavior.
    """
    if is_hard_production_commit(text) or is_explicit_production_request(text):
        return False
    if is_soft_production_proposal(text) or is_production_strategy_question(text):
        return True
    if should_auto_run_tools(text):
        return False
    try:
        from studio_agent.live_demand import has_demand_signal

        if has_demand_signal(text):
            return False
    except Exception:
        pass
    return is_ideation_request(text) or bool(
        re.search(
            r"\b(?:how would|what would|could you|can you|should i|help me|brainstorm|plan|ideate|think about)\b",
            _user_message_before_attachments(text).lower(),
        )
    )


def is_connected_channel_performance_request(text: str) -> bool:
    """True when ideation should still use private connected-channel analytics."""
    low = strip_attachment_boilerplate(strip_agent_mode_prefix(text)).lower()
    if any(
        phrase in low
        for phrase in (
            "my channel",
            "for my channel",
            "our channel",
            "connected channel",
            "channel analytics",
            "channel data",
            "channel performance",
            "posted on the channel",
            "current video we posted",
            "latest video",
            "latest short",
            "pull all of the data",
            "pull all data",
            "pull all the data",
            "data from my",
        )
    ):
        return True
    return bool(re.search(r"\bmy\s+\w[\w\s]{0,40}\s+channel\b", low))


_BOILERPLATE_PRODUCTION_TOPICS = frozenset({
    "planning and ideation",
    "reference video",
    "uploaded reference",
    "attached reference",
    "creative context",
    "planning context",
    "go ahead and render",
    "go ahead and make",
    "render it",
    "make it",
    "render that",
    "yes make it",
})


def is_production_commit_phrase(text: str) -> bool:
    """True when text is a bare production commit, not a video title."""
    low = re.sub(r"\s+", " ", str(text or "").strip().lower())
    if not low:
        return True
    if low in _BOILERPLATE_PRODUCTION_TOPICS:
        return True
    if re.match(
        r"^(?:go ahead and |please )?(?:make|render|produce|start|build|generate)(?:\s+it)?[.!?]*$",
        low,
    ):
        return True
    if re.match(
        r"^(?:yes|yeah|yep|sure|ok(?:ay)?)[,.]?\s+(?:make|render|produce|start)(?:\s+it)?[.!?]*$",
        low,
    ):
        return True
    return False


def is_boilerplate_production_topic(topic: str) -> bool:
    """True when a recovered title came from attachment/system text, not a real video topic."""
    low = str(topic or "").strip().strip('"').strip("'").strip().lower()
    if not low:
        return True
    if is_production_commit_phrase(low):
        return True
    if "planning" in low and "ideation" in low:
        return True
    if low.startswith("rendering if i explicitly ask"):
        return True
    # Beat-sheet labels like "Close (26–30s)" / "Trigger from #3" / "Behavior angle from #2"
    if re.search(
        r"^(?:hook|open(?:ing)?|close|closer|payoff|twist|beat|cta|outro|intro|setup|midpoint|trigger|behavior|angle)\b",
        low,
    ) and (
        re.search(r"\(\s*\d", low)
        or re.search(r"\bfrom\s*#?\s*\d+\b", low)
        or re.search(r"#\s*\d+\b", low)
    ):
        return True
    if re.search(r"^(?:trigger|beat|hook|close|payoff|twist|behavior|angle)\s+from\b", low):
        return True
    if re.search(r"\bangle from\s*#?\s*\d+\b", low):
        return True
    if re.search(r"\(\s*\d+\s*[–\-to]+\s*\d+\s*s(?:ec(?:onds)?)?\s*\)", low):
        return True
    if re.fullmatch(r"close(?:r)?(?:\s*\([^)]*\))?", low):
        return True
    if low in {"shorts metrics missing", "best two shorts", "make a 20s short about that"}:
        return True
    if re.search(r"\bbeat structure\b|\bbeat sheet\b|\bseconds?\b.*\bbeat\b|\b\d+\s*[-–]\s*\d+\s*s(?:ec)?\b", low):
        return True
    if re.search(r"^\d+\s*[-–]?\s*(?:second|sec|s)\b", low):
        return True
    return False


def is_ideation_request(text: str) -> bool:
    """True when the user is planning/strategizing, not asking to render yet."""
    low = _user_message_before_attachments(text).lower()
    if not low:
        return False
    if is_explicit_production_request(low):
        return False
    if is_explicit_reference_analysis_request(low) and should_auto_run_tools(text):
        return False
    if is_uploaded_video_analysis_request(text) and not should_auto_run_tools(text):
        return True
    if is_product_ad_request(low):
        return False
    if is_expand_short_request(low):
        return False
    if is_bulk_scene_ship_request(low):
        return False
    if any(phrase in low for phrase in _IDEATION_PHRASES):
        return True
    if "?" in low and any(
        word in low
        for word in ("channel", "niche", "topic", "style", "market", "audience", "position", "research")
    ):
        if not re.search(r"\b(?:render|generate|start|animate|finalize|export)\b", low):
            return True
    return False


def detect_production_intent(text: str, session: dict[str, Any] | None = None) -> str:
    """Return durable production mode: content_creation | product_ad."""
    if is_product_ad_request(text):
        return "product_ad"
    session = session or {}
    if str(session.get("production_intent") or "").strip() == "product_ad" and is_product_ad_request(
        _latest_user_text(list(session.get("messages") or []), limit=2)
    ):
        return "product_ad"
    return "content_creation"


_NEW_PRODUCTION_TERMS = (
    "new video",
    "new short",
    "another video",
    "another short",
    "another one",
    "different video",
    "different short",
    "next video",
    "next short",
    "next one",
    "the next short",
    "onto the next",
    "on to the next",
    "move on",
    "brand new",
    "start over",
    "start fresh",
    "fresh video",
    "fresh short",
    "completely new",
    "make a new",
    "create a new",
    "generate a new",
    "let's make a",
    "lets make a",
    "let us make a",
    "work on a new",
    "move on to",
    "switch to a new",
    "different title",
    "new title",
)


def get_locked_working_title(session: dict[str, Any] | None) -> str:
    """User-locked working title from conversation_intent (never competitor comps)."""
    session = session or {}
    intent = session.get("conversation_intent") if isinstance(session.get("conversation_intent"), dict) else {}
    title = str(
        intent.get("locked_title")
        or intent.get("working_title")
        or ""
    ).strip()
    if not title or is_boilerplate_production_topic(title):
        return ""
    cleaned = _clean_outline_title_candidate(title) or title
    if is_boilerplate_production_topic(cleaned):
        return ""
    return cleaned


def resolve_production_title(
    user_text: str,
    session: dict[str, Any] | None = None,
    *,
    fallback: str = "",
) -> str:
    """Single source of truth for the short we are about to prepare.

    Priority:
      1) Title in THIS user message (yes make Title… / if we are making Title)
      2) Session locked_title
      3) Explicit fallback (e.g. concept plan) — only if not conflicting with 1–2
      4) Empty (caller must not invent from last_production)
    """
    session = session or {}
    from_msg = ""
    try:
        from studio_agent.conversation import extract_user_locked_title

        from_msg = extract_user_locked_title(user_text)
    except Exception:
        from_msg = ""
    if not from_msg:
        from_msg = _requested_title_from_user_text(user_text)
    if from_msg:
        return from_msg[:120]
    locked = get_locked_working_title(session)
    prior = prior_production_title(session)
    latest = _latest_user_text(list(session.get("messages") or []), limit=2) or user_text
    if locked and prior and _title_overlap_score(locked, prior) >= 0.75:
        if is_hard_production_commit(latest) or is_scene_one_proof_commit(latest):
            locked = ""
    if locked:
        return locked[:120]
    pending = session.get("pending_concept")
    if isinstance(pending, dict):
        pending_title = str(pending.get("title") or "").strip()
        if pending_title and not is_production_commit_phrase(pending_title):
            if not prior or _title_overlap_score(pending_title, prior) < 0.75:
                return pending_title[:120]
    if is_hard_production_commit(latest) or is_scene_one_proof_commit(latest):
        from_outline = extract_production_title_from_assistant(list(session.get("messages") or []))
        if from_outline:
            return from_outline[:120]
    fb = str(fallback or "").strip()
    if fb and "untitled" not in fb.lower():
        # Never accept a fallback that matches only the old Ready short when the
        # user just named a different title in recent messages.
        recent = _latest_user_text(list(session.get("messages") or []), limit=4)
        recent_title = ""
        try:
            from studio_agent.conversation import extract_user_locked_title

            recent_title = extract_user_locked_title(recent)
        except Exception:
            recent_title = _requested_title_from_user_text(recent)
        if recent_title and _title_overlap_score(recent_title, fb) < 0.75:
            return recent_title[:120]
        return fb[:120]
    return ""


def prior_production_title(session: dict[str, Any] | None) -> str:
    session = session or {}
    lp = session.get("last_production") if isinstance(session.get("last_production"), dict) else {}
    args = lp.get("arguments") if isinstance(lp.get("arguments"), dict) else {}
    title = str(args.get("title") or args.get("topic") or "").strip()
    if title:
        return title
    for job in reversed(list(session.get("active_jobs") or [])):
        if not isinstance(job, dict):
            continue
        if str(job.get("kind") or "") not in {"shortform", "longform", ""}:
            continue
        t = str(job.get("title") or job.get("topic") or "").strip()
        if t:
            return t
    return ""


def _user_affirms_assistant_topic(text: str) -> bool:
    """User accepted the assistant's proposed outline/topic without repeating the title."""
    low = str(text or "").lower()
    return bool(
        re.search(
            r"\b(?:that topic|this topic|do that topic|we(?:'|')?ll do (?:that|this)|"
            r"i like it|sounds good|go ahead and make|make the (?:very )?first scene|"
            r"(?:the )?first scene|let'?s do (?:that|this))\b",
            low,
        )
    )


def _clean_outline_title_candidate(raw: str) -> str:
    s = re.sub(r"\s+", " ", str(raw or "")).strip(" -:,.?!")
    s = s.strip().strip('"').strip("'").strip()
    # Strip dictation wrapper: "the title, Why Men Love Bomb…"
    s = re.sub(r"(?i)^(the\s+)?title\s*[,:\-]\s*", "", s).strip()
    low = s.lower()
    if any(
        token in low
        for token in (
            "hook (",
            "close (",
            "closer (",
            "open (",
            "opening (",
            "payoff (",
            "twist (",
            "main beat",
            "twist/close",
            "catalyst note",
            "0-3 sec",
            "25-45 sec",
            "skeleton outline",
            "concept plan",
            "not rendering yet",
            "render style",
            "seedream",
            "grok_imagine",
            "skeleton_host",
        )
    ):
        return ""
    if re.search(r"\(\s*\d+\s*[–\-to]+\s*\d+\s*s", low):
        return ""
    if re.search(r"\bscene\s*#?\s*\d+\b", low):
        return ""
    if re.search(r"^(?:yes make it|render that|make the first|go ahead and render)\b", low):
        return ""
    if is_boilerplate_production_topic(s):
        return ""
    if len(s) < 10 or len(re.findall(r"[A-Za-z0-9']+", s)) < 3:
        return ""
    return s[:120]


def _extract_title_from_assistant_text(text: str) -> str:
    """Best-effort title from one assistant turn (scene breakdowns may omit it)."""
    body = str(text or "")
    if not body.strip():
        return ""
    m = re.search(r"\*\*Working title:\*\*\s*(.+)", body, re.I)
    if m:
        cleaned = _clean_outline_title_candidate(m.group(1))
        if cleaned:
            return cleaned
    m = re.search(r"Skeleton outline for \*\*([^*\n]{10,140})\*\*", body, re.I)
    if m:
        cleaned = _clean_outline_title_candidate(m.group(1))
        if cleaned:
            return cleaned
    m = re.search(r"\*\*Hook:\*\*\s*(.+)", body, re.I)
    if m:
        hook = re.split(r"[.!?]\s+", str(m.group(1) or "").strip())[0]
        cleaned = _clean_outline_title_candidate(hook) or hook.strip()[:120]
        if cleaned and len(cleaned) >= 10:
            return cleaned
    bold = re.findall(r"\*\*([^*\n]{10,140})\*\*", body)
    for cand in reversed(bold):
        cleaned = _clean_outline_title_candidate(cand)
        if cleaned:
            return cleaned
    for pattern in (
        r"(?:working title|title|topic|outline)[:\s]+[\"“']?([^\"'\n]{10,140})",
        r"(?:let'?s make|make)\s+[\"“']([^\"'\n]{10,140})",
    ):
        m = re.search(pattern, body, re.I)
        if m:
            cleaned = _clean_outline_title_candidate(m.group(1))
            if cleaned:
                return cleaned
    block = re.search(r"Skeleton outline[^\n]*\n+([^\n]{12,160})", body, re.I)
    if block:
        cleaned = _clean_outline_title_candidate(block.group(1))
        if cleaned:
            return cleaned
    return ""


def extract_production_title_from_assistant(messages: list[dict[str, Any]]) -> str:
    """Pull the working title from the latest assistant outline/plan."""
    for msg in reversed(messages):
        if str(msg.get("role") or "") != "assistant":
            continue
        body = str(msg.get("content") or "")
        low = body.lower()
        if any(
            token in low
            for token in (
                "concept locked",
                "prepared production for",
                "approve when you're ready",
                "i've prepared production",
            )
        ):
            continue
        title = _extract_title_from_assistant_text(body)
        if title and not is_boilerplate_production_topic(title):
            return title
    return ""


def resolve_current_production_target(
    session: dict[str, Any] | None,
    messages: list[dict[str, Any]],
) -> str:
    """Single source of truth for which short/video the user is making RIGHT NOW."""
    session = session or {}
    # Prefer the newest user turn alone — joined history was poisoning titles.
    users = [
        str(m.get("content") or "")
        for m in messages
        if isinstance(m, dict) and str(m.get("role") or "") == "user"
    ]
    latest = users[-1] if users else _latest_user_text(messages)

    explicit = ""
    try:
        from studio_agent.conversation import extract_user_locked_title

        explicit = extract_user_locked_title(latest)
    except Exception:
        pass
    if not explicit:
        explicit = _requested_title_from_user_text(latest)
    if explicit and not is_boilerplate_production_topic(explicit):
        return explicit[:120]

    # Affirming the assistant outline beats a stale session locked_title.
    if _user_affirms_assistant_topic(latest) or _latest_user_allows_production_pending(latest):
        from_outline = extract_production_title_from_assistant(messages)
        if from_outline and not is_boilerplate_production_topic(from_outline):
            return from_outline
        # Hard scene-1 commit with no parseable outline: never rubberband to the
        # prior Ready short via a stale locked_title on session.
        prior = prior_production_title(session)
        locked = get_locked_working_title(session)
        pending_title = ""
        pending = session.get("pending_concept")
        if isinstance(pending, dict):
            pending_title = str(pending.get("title") or "").strip()
        if is_scene_one_proof_commit(latest) or is_hard_production_commit(latest):
            if pending_title and not is_boilerplate_production_topic(pending_title):
                if not prior or _title_overlap_score(pending_title, prior) < 0.75:
                    return pending_title[:120]
            if locked and prior and _title_overlap_score(locked, prior) >= 0.75:
                return ""

    canonical = _canonical_production_topic(messages, session=session)
    if canonical and not is_boilerplate_production_topic(canonical):
        return canonical

    locked = get_locked_working_title(session)
    if locked and not is_boilerplate_production_topic(locked):
        return locked
    return ""


def is_new_production_request(
    text: str,
    session: dict[str, Any] | None = None,
    *,
    reply_to: dict[str, Any] | None = None,
) -> bool:
    """True when the user is starting a different production, not continuing the prior job."""
    if reply_to:
        return False
    low = strip_agent_mode_prefix(text).lower()
    if is_expand_short_request(low) or is_bulk_scene_ship_request(low):
        return False
    if _is_production_diagnostic_text(low):
        return False
    if any(term in low for term in _NEW_PRODUCTION_TERMS):
        return True
    session = session or {}
    locked = get_locked_working_title(session)
    prior = prior_production_title(session)
    # Locked title vs finished/active short — same niche words (pull/away) must not
    # glue two different titles together. Require high overlap to treat as same short.
    if locked and prior and _title_overlap_score(locked, prior) < 0.75:
        return True
    requested = _explicit_title_candidate(text) or _requested_title_from_user_text(text)
    if locked and not requested:
        requested = locked
    if requested and prior and _title_overlap_score(requested, prior) < 0.75:
        return True
    if re.search(r"\b(?:make|create|generate|render|produce|build)\b.+\b(?:short|video)\b", low):
        lp = session.get("last_production")
        if not isinstance(lp, dict) or not lp:
            return True
        if prior and requested and _title_overlap_score(prior, requested) < 0.75:
            return True
    return False


def clear_stale_production_context(
    session: dict[str, Any],
    *,
    keep_pending_for_title: str = "",
) -> dict[str, Any]:
    """Drop all durable jobs from active tracking when a new production begins.

    If ``keep_pending_for_title`` is set, preserve Approve cards that already
    match that title — otherwise force_sync/reconcile wipes the card the user
    is about to click.
    """
    active_jobs = list(session.get("active_jobs") or [])
    blocked = [str(job_id).strip() for job_id in (session.get("blocked_job_ids") or []) if str(job_id).strip()]
    for job in active_jobs:
        job_id = str(job.get("job_id") or "").strip()
        if job_id:
            blocked.append(job_id)
    cleared = dict(session)
    cleared["active_jobs"] = []
    cleared["last_production"] = {}
    keep_title = str(keep_pending_for_title or "").strip()
    kept_pending: list[dict[str, Any]] = []
    if keep_title and not is_boilerplate_production_topic(keep_title):
        for action in list(session.get("pending_actions") or []):
            if not isinstance(action, dict):
                continue
            if str(action.get("tool") or "") not in SINGLETON_PRODUCTION_APPROVAL_TOOLS:
                kept_pending.append(action)
                continue
            action_title = _production_action_title(action)
            if action_title and not is_boilerplate_production_topic(action_title):
                if _title_overlap_score(action_title, keep_title) >= 0.5:
                    kept_pending.append(action)
                    # Also keep last_production for the matching approve.
                    cleared["last_production"] = {
                        "tool": str(action.get("tool") or ""),
                        "arguments": dict(action.get("arguments") or {})
                        if isinstance(action.get("arguments"), dict)
                        else {},
                        "updated_at": _now(),
                    }
    cleared["pending_actions"] = kept_pending
    cleared["blocked_job_ids"] = list(dict.fromkeys(blocked))[-48:]
    cleared["skip_job_recovery"] = True
    return cleared


_EXPAND_PRODUCTION_RE = re.compile(
    r"\b(?:expand|rest of|remaining|full short|complete the|build the rest|render the rest)\b",
    re.I,
)


def get_production_state(session: dict[str, Any] | None) -> dict[str, Any]:
    """Durable production cycle ledger — invalidates prior jobs/cards by epoch."""
    session = session or {}
    raw = session.get("production_state")
    if isinstance(raw, dict):
        return {
            "epoch": max(1, int(raw.get("epoch") or 1)),
            "target_title": str(raw.get("target_title") or "").strip()[:200],
            "advanced_at": float(raw.get("advanced_at") or 0.0),
            "reason": str(raw.get("reason") or "").strip()[:80],
        }
    return {"epoch": 1, "target_title": "", "advanced_at": 0.0, "reason": ""}


def collect_tracked_production_job_ids(
    session: dict[str, Any],
    messages: list[dict[str, Any]] | None = None,
) -> list[str]:
    """Every shortform/longform job id still tied to this session."""
    found: list[str] = []
    for job in list(session.get("active_jobs") or []):
        if not isinstance(job, dict):
            continue
        jid = str(job.get("job_id") or "").strip()
        if jid:
            found.append(jid)
    lp = session.get("last_production")
    if isinstance(lp, dict):
        args = lp.get("arguments") if isinstance(lp.get("arguments"), dict) else {}
        jid = str(args.get("job_id") or "").strip()
        if jid:
            found.append(jid)
    for msg in messages or list(session.get("messages") or []):
        if not isinstance(msg, dict):
            continue
        deliverable = msg.get("jobDeliverable")
        if isinstance(deliverable, dict):
            jid = str(deliverable.get("job_id") or "").strip()
            if jid:
                found.append(jid)
        content = str(msg.get("content") or "")
        for match in re.finditer(r'"job_id"\s*:\s*"([a-f0-9]{8,16})"', content, re.I):
            found.append(match.group(1))
    return list(dict.fromkeys(found))[-48:]


def _clear_stale_locked_intent(
    session: dict[str, Any],
    prior_title: str,
) -> dict[str, Any] | None:
    intent = dict(session.get("conversation_intent") or {})
    locked = str(intent.get("locked_title") or intent.get("working_title") or "").strip()
    if locked and prior_title and _title_overlap_score(locked, prior_title) >= 0.75:
        intent.pop("locked_title", None)
        intent.pop("working_title", None)
        intent.pop("last_topic", None)
        return intent
    return None


def _job_ids_matching_title(
    session: dict[str, Any],
    title: str,
    messages: list[dict[str, Any]] | None = None,
) -> set[str]:
    """Job ids already tied to ``title`` — must not be blocked when that cycle stays live."""
    wanted = str(title or "").strip()
    if not wanted or is_boilerplate_production_topic(wanted):
        return set()
    keep: set[str] = set()
    for job in list(session.get("active_jobs") or []):
        if not isinstance(job, dict):
            continue
        jid = str(job.get("job_id") or "").strip()
        job_title = str(job.get("title") or job.get("topic") or "").strip()
        if jid and job_title and _title_overlap_score(job_title, wanted) >= 0.75:
            keep.add(jid)
    for msg in messages or list(session.get("messages") or []):
        if not isinstance(msg, dict):
            continue
        deliverable = msg.get("jobDeliverable")
        if not isinstance(deliverable, dict):
            continue
        jid = str(deliverable.get("job_id") or "").strip()
        job_title = str(deliverable.get("title") or deliverable.get("topic") or "").strip()
        status = str(deliverable.get("status") or "").strip().lower()
        if (
            jid
            and job_title
            and _title_overlap_score(job_title, wanted) >= 0.75
            and status in {"awaiting_approval", "running", "complete", ""}
        ):
            keep.add(jid)
    lp = session.get("last_production")
    if isinstance(lp, dict):
        args = lp.get("arguments") if isinstance(lp.get("arguments"), dict) else {}
        jid = str(args.get("job_id") or "").strip()
        job_title = str(args.get("title") or args.get("topic") or lp.get("title") or "").strip()
        if jid and job_title and _title_overlap_score(job_title, wanted) >= 0.75:
            keep.add(jid)
    return keep


def _session_has_live_job_for_title(
    session: dict[str, Any],
    title: str,
    messages: list[dict[str, Any]] | None = None,
) -> bool:
    return bool(_job_ids_matching_title(session, title, messages))


def advance_production_cycle(
    session: dict[str, Any],
    *,
    target_title: str = "",
    messages: list[dict[str, Any]] | None = None,
    reason: str = "",
    persist: bool = True,
) -> dict[str, Any]:
    """Atomic boundary for a new short/video — blocks prior jobs and bumps epoch."""
    sid = str(session.get("session_id") or "").strip()
    prior = prior_production_title(session)
    state = get_production_state(session)
    next_epoch = int(state["epoch"]) + 1
    resolved_target_early = str(target_title or "").strip()
    keep_live = _job_ids_matching_title(session, resolved_target_early, messages)
    blocked = [
        str(job_id).strip()
        for job_id in (session.get("blocked_job_ids") or [])
        if str(job_id).strip() and str(job_id).strip() not in keep_live
    ]
    for jid in collect_tracked_production_job_ids(session, messages):
        if jid and jid not in blocked and jid not in keep_live:
            blocked.append(jid)
    cleared = clear_stale_production_context(
        session,
        keep_pending_for_title=str(target_title or "").strip(),
    )
    # Preserve same-title live jobs — clear_stale empties active_jobs by design.
    if keep_live:
        cleared["active_jobs"] = [
            job
            for job in list(session.get("active_jobs") or [])
            if isinstance(job, dict) and str(job.get("job_id") or "").strip() in keep_live
        ]
        cleared["skip_job_recovery"] = False
    cleared["blocked_job_ids"] = list(dict.fromkeys(blocked))[-48:]
    resolved_target = str(target_title or "").strip()
    if not resolved_target:
        resolved_target = str(state.get("target_title") or "").strip()
    if resolved_target and (
        is_production_commit_phrase(resolved_target)
        or is_boilerplate_production_topic(resolved_target)
    ):
        resolved_target = ""
    if not resolved_target:
        try:
            resolved_target = extract_production_title_from_assistant(
                list(messages or session.get("messages") or []),
            )
        except Exception:
            resolved_target = ""
    if resolved_target and is_boilerplate_production_topic(resolved_target):
        resolved_target = ""
    pending = cleared.get("pending_concept")
    if isinstance(pending, dict):
        pt = str(pending.get("title") or "").strip()
        latest = _latest_user_text(list(messages or session.get("messages") or []))
        if is_scene_one_proof_commit(latest):
            pending = dict(pending)
            pending["status"] = "confirmed"
            pending["scene_count"] = 1
            cleared["pending_concept"] = pending
        elif pt and prior and _title_overlap_score(pt, prior) >= 0.75:
            if resolved_target and _title_overlap_score(resolved_target, prior) < 0.75:
                pending = dict(pending)
                pending["title"] = resolved_target[:200]
                pending["status"] = "awaiting_confirm"
                cleared["pending_concept"] = pending
            elif not resolved_target:
                cleared["pending_concept"] = None
        elif str(pending.get("status") or "") == "started":
            pending = dict(pending)
            pending["status"] = "confirmed"
            cleared["pending_concept"] = pending
    cleared["production_state"] = {
        "epoch": next_epoch,
        "target_title": (resolved_target or "")[:200],
        "advanced_at": _now(),
        "reason": str(reason or "advance")[:80],
    }
    intent = _clear_stale_locked_intent(cleared, prior)
    if intent is not None:
        cleared["conversation_intent"] = intent
    if persist and sid:
        payload: dict[str, Any] = {
            "active_jobs": cleared.get("active_jobs") or [],
            "last_production": cleared.get("last_production") or {},
            "pending_actions": cleared.get("pending_actions") or [],
            "blocked_job_ids": cleared.get("blocked_job_ids") or [],
            "skip_job_recovery": True,
            "production_state": cleared["production_state"],
            "pending_concept": cleared.get("pending_concept"),
        }
        if intent is not None:
            payload["conversation_intent"] = intent
        return update_session(sid, **payload) or cleared
    return cleared


def should_advance_production_cycle(
    session: dict[str, Any],
    messages: list[dict[str, Any]],
    user_text: str = "",
) -> tuple[bool, str]:
    """True when the user moved to a new production boundary."""
    latest = str(user_text or "").strip() or _latest_user_text(messages)
    if not latest:
        return False, ""
    if not (
        _latest_user_allows_production_pending(latest)
        or is_hard_production_commit(latest)
        or is_scene_one_proof_commit(latest)
    ):
        return False, ""
    target = resolve_current_production_target(session, messages)
    expand_intent = bool(_EXPAND_PRODUCTION_RE.search(latest))
    if expand_intent:
        return False, target
    state = get_production_state(session)
    state_target = str(state.get("target_title") or "").strip()
    # Same title already live (awaiting review / running) — do not re-advance.
    # Re-advancing on every Sync blocked the job the user just started because
    # the commit phrase stays as the latest user message.
    if target and (
        (state_target and _title_overlap_score(target, state_target) >= 0.75)
        or _session_has_live_job_for_title(session, target, messages)
    ):
        return False, target
    # Hard commits and scene-1 proof open a fresh boundary for a NEW title.
    # Prior production may already be cleared from active_jobs while the UI
    # still holds ghost deliverables — advance prevents rubberbanding.
    if is_hard_production_commit(latest) or is_scene_one_proof_commit(latest):
        return True, target
    prior = prior_production_title(session)
    if prior:
        if not target:
            return True, ""
        if _title_overlap_score(target, prior) < 0.75:
            return True, target
    if target and state_target and _title_overlap_score(target, state_target) < 0.75:
        return True, target
    return False, target


def reconcile_production_state(
    session: dict[str, Any],
    *,
    messages: list[dict[str, Any]] | None = None,
    user_text: str = "",
    persist: bool = True,
) -> dict[str, Any]:
    """Single entry point: advance cycle when the latest user turn starts new production."""
    messages = list(messages or session.get("messages") or [])
    should, target = should_advance_production_cycle(session, messages, user_text)
    if should:
        return advance_production_cycle(
            session,
            target_title=target,
            messages=messages,
            reason="reconcile",
            persist=persist,
        )
    return session


def filter_active_jobs_for_session(session: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Drop blocked ghost jobs before any API/stream payload."""
    session = session or {}
    blocked = {
        str(value).strip()
        for value in (session.get("blocked_job_ids") or [])
        if str(value).strip()
    }
    kept: list[dict[str, Any]] = []
    for job in list(session.get("active_jobs") or []):
        if not isinstance(job, dict):
            continue
        jid = str(job.get("job_id") or "").strip()
        if jid and jid in blocked:
            continue
        kept.append(job)
    return kept


def production_session_fields(session: dict[str, Any] | None) -> dict[str, Any]:
    """Fields every chat/sync response must include for the UI ledger."""
    session = session or {}
    payload = {
        "blocked_job_ids": list(session.get("blocked_job_ids") or []),
        "production_state": get_production_state(session),
        "active_jobs": filter_active_jobs_for_session(session),
    }
    try:
        from studio_agent.production_command_state import build_session_production_view

        payload["production_view"] = build_session_production_view(session).model_dump(
            mode="json"
        )
    except Exception:
        # Old/malformed sessions still load through the legacy fields while the
        # migration layer repairs them; never emit a partial canonical view.
        pass
    return payload


def _align_shortform_topic_args(
    args: dict[str, Any],
    messages: list[dict[str, Any]],
    *,
    session: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Force shortform production onto the CURRENT user title.

    Root bug: old code kept the Ready-job title whenever keyword overlap with the
    new title was >= 0.34 (e.g. both contain \"Why Men Pull Away\"), so Approve
    kept showing the previous short forever.
    """
    aligned = dict(args or {})
    session = session or {}
    users = [
        str(m.get("content") or "")
        for m in messages
        if isinstance(m, dict) and str(m.get("role") or "") == "user"
    ]
    latest = users[-1] if users else _latest_user_text(messages, limit=2)
    current = _production_action_title({"arguments": aligned})
    # Never replace a real pending title with beat-sheet chrome from outlines.
    if current and not is_boilerplate_production_topic(current):
        requested = _requested_title_from_user_text(latest)
        if not requested:
            try:
                from studio_agent.conversation import extract_user_locked_title

                requested = extract_user_locked_title(latest)
            except Exception:
                requested = ""
        if requested and not is_boilerplate_production_topic(requested):
            if _title_overlap_score(current, requested) >= 0.5:
                aligned["topic"] = requested
                aligned["title"] = requested[:120]
                return aligned
            # User named a different title this turn — use theirs.
            aligned["topic"] = requested
            aligned["title"] = requested[:120]
            return aligned
        # Keep the existing good title; do not run outline extraction.
        return aligned

    canonical = resolve_current_production_target(session, messages)
    if canonical and is_boilerplate_production_topic(canonical):
        canonical = ""
    if not canonical:
        canonical = resolve_production_title(
            latest,
            session,
            fallback=str(aligned.get("title") or aligned.get("topic") or ""),
        )
    if canonical and is_boilerplate_production_topic(canonical):
        canonical = ""
    if not canonical:
        canonical = _canonical_production_topic(messages, session=session)
    if canonical and is_boilerplate_production_topic(canonical):
        canonical = ""
    if not canonical:
        return aligned
    # Overwrite unless titles are nearly the same short (high overlap).
    if current and _title_overlap_score(current, canonical) >= 0.75:
        if resolve_production_title(latest, session):
            aligned["topic"] = canonical
            aligned["title"] = canonical[:120]
        return aligned
    aligned["topic"] = canonical
    aligned["title"] = canonical[:120]
    return aligned


def _apply_visual_proof_args(
    args: dict[str, Any],
    messages: list[dict[str, Any]],
) -> dict[str, Any]:
    """Honor one-still / visual-proof requests from the live chat, not only tool args."""
    aligned = dict(args or {})
    intent_parts = [
        str(aligned.get("user_request") or ""),
        str(aligned.get("brief") or ""),
        str(aligned.get("instruction") or ""),
        str(aligned.get("topic") or ""),
        str(aligned.get("visual_brief") or ""),
        _latest_user_text(messages, limit=6),
    ]
    intent_text = "\n".join(part for part in intent_parts if part).lower()
    if is_expand_short_request(intent_text):
        return aligned
    # Permanent short-form production contract: every new Short starts with
    # exactly one still.  The creator approves/repairs that still, animates and
    # approves Scene 1, then explicitly expands through the duration + creative
    # intake.  No model-generated tool args may bypass this cost/quality gate.
    visual_proof_only = True
    if visual_proof_only:
        aligned["visual_proof_only"] = True
        aligned["scene_count"] = 1
        aligned["animate"] = False
        aligned["staged_shortform_workflow"] = True
        if not str(aligned.get("user_request") or "").strip():
            for msg in reversed(messages):
                if str(msg.get("role") or "") != "user":
                    continue
                content = str(msg.get("content") or "")
                if _is_visual_proof_request(content) or _requested_title_from_user_text(content):
                    aligned["user_request"] = content
                    break
    return aligned


def _prepare_shortform_execution_args(
    args: dict[str, Any],
    messages: list[dict[str, Any]],
    *,
    session: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Normalize shortform tool args from session context before approval or execution."""
    aligned = _align_shortform_topic_args(args, messages, session=session)
    return _apply_visual_proof_args(aligned, messages)


def _canonical_production_topic(
    messages: list[dict[str, Any]],
    *,
    session: dict[str, Any] | None = None,
    sibling_pending: list[dict[str, Any]] | None = None,
) -> str:
    """Best-effort current production title when the latest user line omits it.

    Intentionally does NOT fall back to last_production or sibling pending titles —
    those self-justify stale Approve cards after research turns.
    """
    requested = _requested_title_from_user_text(_latest_user_text(messages))
    if requested:
        return requested
    return ""


def _latest_user_allows_production_pending(text: str) -> bool:
    """True only when the latest user message is a real production commit.

    Must stay aligned with runner._allows_brand_new_production_tool (default DENY).
    Research, soft \"let's make [title]\", and strategy must never keep Approve.
    """
    low = strip_attachment_boilerplate(strip_agent_mode_prefix(text)).lower().strip()
    if not low:
        return False
    # Soft proposals never allow pending (even if they contain make/create).
    if is_soft_production_proposal(text) or is_production_strategy_question(text):
        return False
    if is_hard_production_commit(text) or is_explicit_production_request(text):
        return True
    # One-still / first-scene visual proof is a deliberate production step.
    if re.search(
        r"\bmake\s+(?:exactly\s+)?(?:one|1|a|single)\s+(?:still|scene|image|frame)\b|"
        r"\b(?:make|render|start|build|produce|generate)\s+(?:the\s+)?(?:very\s+)?(?:first|1st)\s+scene\b|"
        r"\b(?:make|render|start|build|produce|generate)\s+scene\s*(?:#?\s*1|one)\b|"
        r"\bvisual\s+proof\b|\bproof\s+(?:still|image)\b|\btest\s+(?:still|image)\b",
        low,
    ):
        return True
    return False


def _latest_assistant_is_research_reply(messages: list[dict[str, Any]]) -> bool:
    """True when the newest assistant turn is public-data research, not production prep."""
    for msg in reversed(list(messages or [])[-12:]):
        if str(msg.get("role") or "") != "assistant":
            continue
        text = str(msg.get("content") or "").lower()
        if not text.strip():
            continue
        research_hits = sum(
            1
            for term in (
                "public data",
                "views",
                "view counts",
                "traction",
                "performing",
                "demand",
                "niche",
                "outperform",
                "hits ",
                " lands at ",
            )
            if term in text
        )
        if research_hits >= 2:
            return True
        return False
    return False


def _shortform_action_stale_for_latest_user(
    action: dict[str, Any],
    messages: list[dict[str, Any]],
    *,
    session: dict[str, Any] | None = None,
    sibling_pending: list[dict[str, Any]] | None = None,
) -> bool:
    tool = str(action.get("tool") or "")
    if tool not in SINGLETON_PRODUCTION_APPROVAL_TOOLS:
        return False
    latest_text = _latest_user_text(messages)
    if _is_production_diagnostic_text(latest_text):
        return True
    # Absolute rule: no hard production commit on the latest user turn → drop Approve.
    if not _latest_user_allows_production_pending(latest_text):
        return True
    # Research-shaped assistant reply after a soft/ambiguous commit → still drop.
    if _latest_assistant_is_research_reply(messages) and not is_hard_production_commit(latest_text):
        return True
    args = action.get("arguments") if isinstance(action.get("arguments"), dict) else {}
    # One-still proof is stale unless the latest user asked for one still.
    # Exception: the staged short-form contract forces every new Short into
    # one-still mode, so a workflow-forced card must survive a plain hard
    # commit ("go ahead and render it") — otherwise Approve silently vanishes.
    if args.get("visual_proof_only") or int(args.get("scene_count") or 0) == 1:
        forced_by_workflow = bool(args.get("staged_shortform_workflow"))
        if not forced_by_workflow and not re.search(
            r"\b(?:one|1|single|first)\s+(?:still|scene|image|frame)\b|"
            r"\bvisual\s+proof\b|\bproof\s+(?:still|image)\b|\btest\s+(?:still|image)\b",
            latest_text,
            re.I,
        ):
            return True
    action_title = _production_action_title(action)
    # Pending for a previous short while user locked a different title → always stale.
    locked = get_locked_working_title(session)
    if locked and is_boilerplate_production_topic(locked):
        locked = ""
    if locked and action_title and _title_overlap_score(locked, action_title) < 0.75:
        # Prefer THIS-turn requested title over a poisoned locked_title.
        requested_now = _requested_title_from_user_text(latest_text)
        if requested_now and _title_overlap_score(action_title, requested_now) >= 0.5:
            locked = ""
        else:
            return True
    # Pending start for a title the user is moving past ("next short") → stale.
    # Do NOT drop a pending card that already matches the locked/current title —
    # that was killing Approve right before start_shortform_generate ran.
    if is_new_production_request(latest_text, session) and action_title:
        locked_now = get_locked_working_title(session) or resolve_production_title(latest_text, session)
        if locked_now and _title_overlap_score(action_title, locked_now) >= 0.75:
            pass  # this IS the next short — keep Approve
        else:
            prior = prior_production_title(session)
            if prior and _title_overlap_score(action_title, prior) >= 0.75:
                return True
    canonical_title = _canonical_production_topic(
        messages,
        session=session,
        sibling_pending=sibling_pending,
    )
    if locked and not canonical_title:
        canonical_title = locked
    # If user committed without naming a title, keep the prepared card.
    if not action_title or not canonical_title:
        return False
    return _title_overlap_score(action_title, canonical_title) < 0.34


CONTEXT_SUMMARY_REVISION = 2


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
            # Do not fossilize transient tool narration, safety-audit retries,
            # or a prior Live Demand brief into every later turn. Those records
            # are often narrow to one old question and were the source of
            # "60 percent"-style context contamination in unrelated planning.
            low = text.lower()
            if any(marker in low for marker in (
                "[studio agent preflight tool result",
                "anti-hallucination audit",
                "[live demand brief",
                "i verified public youtube demand",
            )):
                continue
            assistant_facts.append(text)
        elif role == "tool":
            # Full tool payloads belong in the durable data stores, not the
            # compacted conversational memory where they can become stale.
            continue
    parts = [
        f"Compacted transcript memory ({omitted_count} older messages summarized; raw transcript is still stored):",
    ]
    if user_goals:
        parts.append("User instructions/preferences:")
        parts.extend(f"- {x}" for x in user_goals[-6:])
    if assistant_facts:
        parts.append("Prior agent decisions/results:")
        parts.extend(f"- {x}" for x in assistant_facts[-4:])
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
    if (
        omitted <= 0
        or (
            int(session.get("compacted_message_count") or 0) >= omitted
            and int(session.get("context_summary_revision") or 0) >= CONTEXT_SUMMARY_REVISION
        )
    ):
        return session
    summary = _compact_transcript(messages[tail_start:], omitted_count=omitted)
    if not summary:
        return session
    channel = _channel_label(session)
    if channel:
        summary = f"Selected YouTube channel for this chat: {channel}\n{summary}"
    session["context_summary"] = summary
    session["compacted_message_count"] = omitted
    session["context_summary_revision"] = CONTEXT_SUMMARY_REVISION
    _save(session)
    return session


def trim_messages_for_model(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep system row + the most recent turns so long chats stay within model limits."""
    aligned = align_tool_message_boundary(list(messages or []))
    if len(aligned) <= MAX_MESSAGES_FOR_MODEL + 1:
        return align_tool_message_boundary(aligned)
    head: list[dict[str, Any]] = []
    tail_start = 0
    if aligned and aligned[0].get("role") == "system":
        head = [aligned[0]]
        tail_start = 1
    tail = aligned[tail_start:]
    if len(tail) <= MAX_MESSAGES_FOR_MODEL:
        return align_tool_message_boundary(head + tail)
    omitted = len(tail) - MAX_MESSAGES_FOR_MODEL
    note = {
        "role": "system",
        "content": (
            f"[Earlier conversation truncated — {omitted} older messages omitted from model context. "
            "The full transcript is still saved in this session.]"
        ),
    }
    window = list(tail[-MAX_MESSAGES_FOR_MODEL:])
    if head:
        return align_tool_message_boundary([head[0], note, *window])
    return align_tool_message_boundary([note, *window])


_legacy_trim_messages_for_model = trim_messages_for_model


def trim_messages_for_model(
    messages: list[dict[str, Any]],
    *,
    session: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Keep system row + compacted memory + recent turns so long chats stay within model limits."""
    aligned = align_tool_message_boundary(list(messages or []))
    if len(aligned) <= MAX_MESSAGES_FOR_MODEL + 1:
        return align_tool_message_boundary(aligned)
    head: list[dict[str, Any]] = []
    tail_start = 0
    if aligned and aligned[0].get("role") == "system":
        head = [aligned[0]]
        tail_start = 1
    tail = aligned[tail_start:]
    if len(tail) <= MAX_MESSAGES_FOR_MODEL:
        return align_tool_message_boundary(head + tail)
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
    window = list(tail[-MAX_MESSAGES_FOR_MODEL:])
    if head:
        return align_tool_message_boundary([head[0], note, *window])
    return align_tool_message_boundary([note, *window])


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
    cap = max(1, min(limit, 200))
    # Sort by file mtime first so we only parse the newest files — reading every
    # session JSON on Agent boot was blocking the sidebar for heavy accounts.
    paths = sorted(
        SESSIONS_DIR.glob("sa_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    rows: list[dict[str, Any]] = []
    scan_budget = max(cap * 4, 80)
    for path in paths[:scan_budget]:
        if len(rows) >= cap:
            break
        try:
            session = _read_session_file(path)
            if not session:
                continue
            if session.get("user_id") != user_id:
                continue
            if session.get("deleted_at"):
                continue
            rows.append(session)
        except Exception:
            continue
    rows.sort(key=lambda s: float(s.get("updated_at") or 0), reverse=True)
    return rows[:cap]


_CONTEXT_INGEST_TERMS = (
    "ingest context",
    "ingest all context",
    "pull context",
    "carry over context",
    "carry context",
    "previous chat",
    "prior chat",
    "last chat",
    "old chat",
    "pick up where",
    "pick up from where",
    "continue from the previous",
    "continue from my previous",
    "load context from",
    "import context",
    "use context from",
    "context from the previous",
)


def is_context_ingest_request(text: str) -> bool:
    low = str(text or "").lower()
    return any(term in low for term in _CONTEXT_INGEST_TERMS)


def _session_channel_key(session: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(session.get("channel_id") or "").strip(),
        str(session.get("registry_key") or "").strip().lower(),
        str(session.get("channel_title") or "").strip().lower(),
    )


def resolve_ingest_parent_session(
    session: dict[str, Any],
    text: str,
    *,
    user_id: str,
) -> str | None:
    """Find the prior chat to ingest when the user asks to continue with context."""
    explicit = re.search(r"\b(sa_[A-Za-z0-9]{8,24})\b", str(text or ""))
    if explicit:
        parent = get_session(explicit.group(1), user_id=user_id)
        if parent and str(parent.get("session_id") or "") != str(session.get("session_id") or ""):
            return str(parent.get("session_id") or "")
    sid = str(session.get("session_id") or "").strip()
    forked = str(session.get("forked_from") or "").strip()
    if forked:
        parent = get_session(forked, user_id=user_id)
        if parent:
            return forked
    channel_id, registry_key, channel_title = _session_channel_key(session)
    candidates = [
        row for row in list_sessions(user_id, limit=80)
        if str(row.get("session_id") or "") != sid and not row.get("deleted_at")
    ]
    if channel_id or registry_key or channel_title:
        scoped = [
            row for row in candidates
            if _session_channel_key(row) == (channel_id, registry_key, channel_title)
        ]
        if scoped:
            candidates = scoped
    if not candidates:
        return None
    return str(candidates[0].get("session_id") or "") or None


def _build_ingested_context_messages(parent: dict[str, Any]) -> list[dict[str, Any]]:
    parent_messages = list(parent.get("messages") or [])
    summary = str(parent.get("context_summary") or "").strip()
    if not summary:
        non_system = [m for m in parent_messages if str(m.get("role") or "") != "system"]
        summary = _compact_transcript(non_system, omitted_count=len(non_system))
    channel = _channel_label(parent)
    if channel and summary and channel not in summary:
        summary = f"Selected YouTube channel for the prior chat: {channel}\n{summary}"
    recent: list[dict[str, Any]] = []
    for msg in parent_messages:
        role = str(msg.get("role") or "")
        if role not in {"user", "assistant"}:
            continue
        text = _message_text(msg)
        if not text or "[tool:" in text.lower():
            continue
        recent.append({"role": role, "content": text})
    ingested: list[dict[str, Any]] = []
    if summary:
        ingested.append({
            "role": "system",
            "content": (
                "[Studio Agent ingested prior chat context]\n"
                f"Parent session: {parent.get('session_id')}\n"
                f"Parent title: {derive_title(parent)}\n"
                f"{summary[:9000]}"
            ),
        })
    if recent:
        ingested.append({
            "role": "system",
            "content": (
                "[Recent turns from the prior chat — for conversational continuity only. "
                "Do not resume, poll, or finalize any prior production jobs unless the user "
                "explicitly replies to that deliverable card.]"
            ),
        })
        ingested.extend(recent[-18:])
    return ingested


def ingest_parent_context_into_session(
    session_id: str,
    *,
    user_id: str,
    parent_session_id: str,
) -> dict[str, Any] | None:
    """Attach compacted context from a prior chat to the current session."""
    current = get_session(session_id, user_id=user_id)
    parent = get_session(parent_session_id, user_id=user_id)
    if not current or not parent:
        return None
    if str(current.get("context_ingested") or "").lower() == "true" or current.get("context_ingested") is True:
        if str(current.get("forked_from") or "") == parent_session_id:
            return current
    ingested = _build_ingested_context_messages(parent)
    if not ingested:
        return current
    messages = list(current.get("messages") or [])
    if not any(
        parent_session_id in str(msg.get("content") or "")
        for msg in messages
        if str(msg.get("role") or "") == "system"
    ):
        messages = ingested + messages
    blocked = list(current.get("blocked_job_ids") or [])
    for job in parent.get("active_jobs") or []:
        job_id = str(job.get("job_id") or "").strip()
        if job_id:
            blocked.append(job_id)
    return update_session(
        session_id,
        messages=messages,
        context_ingested=True,
        forked_from=parent_session_id,
        context_summary=str(parent.get("context_summary") or ingested[0].get("content") or "")[:9000],
        active_jobs=[],
        pending_actions=[],
        last_production={},
        blocked_job_ids=list(dict.fromkeys(blocked))[-24:],
        skip_job_recovery=True,
    )


def fork_session_with_context(session_id: str, *, user_id: str) -> dict[str, Any] | None:
    """Start a fresh chat with channel settings + compacted prior context, but no prior jobs."""
    old = get_session(session_id, user_id=user_id)
    if not old:
        return None
    base_title = derive_title(old)
    continued = (
        f"{base_title[:52]} (context)"
        if base_title and "(context)" not in base_title
        else base_title or "Continued chat"
    )
    fresh = create_session(
        user_id=user_id,
        model=str(old.get("model") or ""),
        agent_mode=str(old.get("agent_mode") or "plan"),
        approval_mode=old.get("approval_mode") or "confirm",
        content_format=old.get("content_format") or "both",
        reasoning_depth=old.get("reasoning_depth") or "balanced",
        render_style=old.get("render_style") or DEFAULT_RENDER_STYLE,
        image_model=normalize_image_model(old.get("image_model")),
        video_model=normalize_video_model(old.get("video_model")),
        web_search=bool(old.get("web_search", True)),
        animate=bool(old.get("animate", True)),
        channel_id=str(old.get("channel_id") or ""),
        registry_key=str(old.get("registry_key") or ""),
        channel_title=str(old.get("channel_title") or ""),
        product_website=str(old.get("product_website") or ""),
    )
    ingested = _build_ingested_context_messages(old)
    messages = ingested + [{
        "role": "user",
        "content": (
            "[New chat with prior context ingested — pick up planning and creative direction "
            "from the summary above. Do not resume old renders or scene-review jobs unless I "
            "explicitly reply to that deliverable card.]"
        ),
    }]
    blocked = [
        str(job.get("job_id") or "").strip()
        for job in (old.get("active_jobs") or [])
        if str(job.get("job_id") or "").strip()
    ]
    return update_session(
        fresh["session_id"],
        title=continued[:72],
        messages=messages,
        context_ingested=True,
        forked_from=session_id,
        context_summary=str(old.get("context_summary") or "")[:9000] or (
            ingested[0].get("content") if ingested else ""
        ),
        blocked_job_ids=list(dict.fromkeys(blocked))[-24:],
        skip_job_recovery=True,
        active_jobs=[],
        pending_actions=[],
    )


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
        agent_mode=str(old.get("agent_mode") or "plan"),
        approval_mode=old.get("approval_mode") or "confirm",
        content_format=old.get("content_format") or "both",
        reasoning_depth=old.get("reasoning_depth") or "balanced",
        render_style=old.get("render_style") or DEFAULT_RENDER_STYLE,
        image_model=normalize_image_model(old.get("image_model")),
        video_model=normalize_video_model(old.get("video_model")),
        web_search=bool(old.get("web_search", True)),
        animate=bool(old.get("animate", True)),
        channel_id=str(old.get("channel_id") or ""),
        registry_key=str(old.get("registry_key") or ""),
        channel_title=str(old.get("channel_title") or ""),
        product_website=str(old.get("product_website") or ""),
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
    agent_mode: str = "plan",
    approval_mode: ApprovalMode = "confirm",
    content_format: ContentFormat = "both",
    reasoning_depth: ReasoningDepth = "balanced",
    render_style: str = DEFAULT_RENDER_STYLE,
    image_model: str = DEFAULT_IMAGE_MODEL,
    video_model: str = DEFAULT_VIDEO_MODEL,
    web_search: bool = True,
    animate: bool = True,
    captions_enabled: bool = True,
    caption_mode: str = "word",
    channel_id: str = "",
    registry_key: str = "",
    channel_title: str = "",
    product_website: str = "",
) -> dict[str, Any]:
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    sid = f"sa_{uuid.uuid4().hex[:16]}"
    requested_model = str(model or "").strip()
    if provider_policy.model_provider(requested_model) != "unknown":
        requested_model = provider_policy.assert_runner_model_allowed(requested_model)
    session = {
        "session_id": sid,
        "user_id": user_id,
        "model": requested_model,
        "agent_mode": agent_mode if agent_mode in {"plan", "studio", "cliplab"} else "plan",
        "approval_mode": approval_mode,
        "content_format": content_format,
        "reasoning_depth": reasoning_depth,
        "render_style": render_style or DEFAULT_RENDER_STYLE,
        "image_model": normalize_image_model(image_model),
        "video_model": normalize_video_model(video_model),
        "channel_id": str(channel_id or "").strip(),
        "registry_key": str(registry_key or "").strip(),
        "channel_title": str(channel_title or "").strip(),
        "product_website": str(product_website or "").strip(),
        "production_intent": "content_creation",
        "last_live_demand": {},
        "web_search": bool(web_search),
        "animate": bool(animate),
        "captions_enabled": bool(captions_enabled),
        "caption_mode": "off" if str(caption_mode or "").strip().lower() == "off" else "word",
        "created_at": _now(),
        "updated_at": _now(),
        "title": "",
        "messages": [],
        "pending_actions": [],
        "pending_concept": None,
        # Packaging proof is intentionally separate from active production.
        "thumbnail_review": None,
        "active_jobs": [],
        "runs": [],
        "context_summary": "",
        "compacted_message_count": 0,
        "forked_from": "",
        "context_ingested": False,
        "skip_job_recovery": False,
        "blocked_job_ids": [],
        "production_state": {"epoch": 1, "target_title": "", "advanced_at": 0.0, "reason": "create"},
        "media_route_revision": 1,
        "media_route_updated_at": _now(),
        "interaction_state": "plan",
        "production_gate_open": False,
        "active_command_id": "",
        "provider_policy_version": provider_policy.POLICY_VERSION,
        "provider_policy_migrated_at": _now(),
        "provider_policy_migrations": [],
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
    if run.get("request_id"):
        out["request_id"] = str(run.get("request_id") or "")
    if include_events:
        out["events"] = events[-MAX_RUN_EVENTS:]
    if isinstance(run.get("result"), dict):
        out["result"] = dict(run.get("result") or {})
    if run.get("error"):
        out["error"] = str(run.get("error") or "")
    return out


def _normalize_runs(session: dict[str, Any]) -> list[dict[str, Any]]:
    runs = session.setdefault("runs", [])
    if not isinstance(runs, list):
        runs = []
        session["runs"] = runs
    return runs


def _run_command_id(run: dict[str, Any]) -> str:
    """Return the typed command that a durable chat run was executing, if any."""

    for event in reversed(list(run.get("events") or [])):
        data = event.get("data") if isinstance(event, dict) else None
        if not isinstance(data, dict):
            continue
        command = data.get("command")
        if isinstance(command, dict) and str(command.get("command_id") or "").strip():
            return str(command.get("command_id") or "").strip()
        command_id = str(data.get("command_id") or "").strip()
        if command_id:
            return command_id
    return ""


def _terminal_run_owns_production_gate(session: dict[str, Any], command_id: str) -> bool:
    """Whether a deploy-disconnected run left behind this exact gate lease.

    A production gate is deliberately exclusive while a command is live.  A
    Fly deploy, however, can terminate the worker after the command has
    started and before its ``finally`` closes the gate.  The chat run is then
    marked ``interrupted`` during reconciliation, so retaining the gate would
    permanently reject every later retry in that session.
    """

    normalized = str(command_id or "").strip()
    if not normalized:
        return False
    for workflow in list(session.get("production_workflows") or []):
        if not isinstance(workflow, dict):
            continue
        if str(workflow.get("command_id") or "").strip() != normalized:
            continue
        if str(workflow.get("status") or "") not in _PRODUCTION_WORKFLOW_TERMINAL:
            # The HTTP/chat run may be terminal because it only admitted the
            # work. Its durable workflow still owns the production lease.
            return False
    for run in reversed(_normalize_runs(session)):
        if _run_command_id(run) != normalized:
            continue
        return str(run.get("status") or "running") not in ACTIVE_RUN_STATUSES
    return False


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
        active_command = str(session.get("active_command_id") or "").strip()
        if session.get("production_gate_open") and _terminal_run_owns_production_gate(
            session,
            active_command,
        ):
            # The interrupted run cannot execute any more.  Closing only the
            # matching lease preserves mutual exclusion for genuinely active
            # commands while making a retry possible after deploy recovery.
            session.update({
                "interaction_state": "verification",
                "production_gate_open": False,
                "active_command_id": "",
                "active_command_job_id": "",
            })
        _save(session)
    return session


def create_run(
    session_id: str,
    *,
    user_text: str,
    request_id: str = "",
) -> dict[str, Any]:
    normalized_request_id = str(request_id or "").strip()[:128]
    with (
        _RUN_CREATE_LOCK,
        _run_create_file_lock(session_id),
        _session_write_lock(session_id),
        _session_write_file_lock(session_id),
    ):
        session = _read_session_file(_session_path(session_id))
        if not session:
            raise KeyError(session_id)
        runs = _normalize_runs(session)
        if normalized_request_id:
            for existing in reversed(runs):
                if str(existing.get("request_id") or "") == normalized_request_id:
                    return {**existing, "idempotent_replay": True}

        now = _now()
        run = {
            "run_id": f"run_{uuid.uuid4().hex[:16]}",
            "session_id": session_id,
            "request_id": normalized_request_id,
            "status": "running",
            "message_preview": str(user_text or "").strip().replace("\n", " ")[:220],
            "created_at": now,
            "updated_at": now,
            "events": [],
        }
        runs.append(run)
        session["runs"] = runs[-80:]
        _write_session_unlocked(session)
        return {**run, "idempotent_replay": False}


def record_production_command_transition(
    session_id: str,
    *,
    authority: dict[str, Any],
    mutation: dict[str, Any],
    transition: str,
    result_status: str = "",
    error: str = "",
) -> dict[str, Any] | None:
    """Persist the revisioned backend command ledger under the session lock.

    This ledger is intentionally control-plane only: it stores opaque ids,
    exact target/scene scope, hashes, and lifecycle state, never raw prompts,
    provider credentials, or private runtime arguments.
    """

    normalized_session = str(session_id or "").strip()
    command_id = str((authority or {}).get("command_id") or "").strip()
    mutation_id = str((mutation or {}).get("mutation_id") or "").strip()
    if not normalized_session or not command_id or not mutation_id:
        return None
    lifecycle = str(transition or "").strip().lower()
    if lifecycle not in {"authorized", "executing", "accepted", "completed", "failed", "rejected"}:
        raise ValueError(f"unsupported production command transition: {transition}")

    with _session_write_lock(normalized_session), _session_write_file_lock(normalized_session):
        session = _read_session_file(_session_path(normalized_session))
        if not session:
            return None
        now = _now()
        revision = max(0, int(session.get("production_command_revision") or 0)) + 1
        rows = [
            dict(row)
            for row in list(session.get("production_commands") or [])
            if isinstance(row, dict) and str(row.get("command_id") or "").strip()
        ]
        command = next(
            (row for row in rows if str(row.get("command_id") or "") == command_id),
            None,
        )
        if command is None:
            root_envelope = (
                dict((mutation or {}).get("command_envelope") or {})
                if isinstance((mutation or {}).get("command_envelope"), dict)
                else {}
            )
            command = {
                "schema": str((authority or {}).get("schema") or "studio.production-command.v2"),
                "command_id": command_id,
                "user_id": str((authority or {}).get("user_id") or ""),
                "session_id": normalized_session,
                "source": str((authority or {}).get("source") or "server_workflow"),
                "request_sha256": str((authority or {}).get("request_sha256") or ""),
                "state_revision": int((authority or {}).get("state_revision") or 0),
                "status": "authorized",
                "created_at": float((authority or {}).get("issued_at") or now),
                "updated_at": now,
                "revision": revision,
                "steps": [],
            }
            if root_envelope:
                command["command_envelope"] = root_envelope
                command["action"] = str(root_envelope.get("action") or mutation.get("action") or "")
            rows.append(command)
        elif (
            str(command.get("user_id") or "") != str((authority or {}).get("user_id") or "")
            or str(command.get("session_id") or "") != normalized_session
        ):
            # A command id is immutable and cannot be rebound across principals.
            raise RuntimeError("production command identity is already bound to another owner")

        steps = [
            dict(step)
            for step in list(command.get("steps") or [])
            if isinstance(step, dict) and str(step.get("mutation_id") or "").strip()
        ]
        step = next(
            (row for row in steps if str(row.get("mutation_id") or "") == mutation_id),
            None,
        )
        if step is None:
            step = {
                "schema": str((mutation or {}).get("schema") or "studio.production-mutation.v2"),
                "mutation_id": mutation_id,
                "action": str((mutation or {}).get("action") or ""),
                "tool_name": str((mutation or {}).get("tool_name") or ""),
                "target_kind": str((mutation or {}).get("target_kind") or ""),
                "target_id": str((mutation or {}).get("target_id") or ""),
                "scene_indices": list((mutation or {}).get("scene_indices") or []),
                "arguments_sha256": str((mutation or {}).get("arguments_sha256") or ""),
                "status": "authorized",
                "created_at": float((mutation or {}).get("authorized_at") or now),
            }
            if isinstance((mutation or {}).get("command_envelope"), dict):
                step["command_envelope"] = dict((mutation or {}).get("command_envelope") or {})
            steps.append(step)
        else:
            immutable = {
                "action": str((mutation or {}).get("action") or ""),
                "tool_name": str((mutation or {}).get("tool_name") or ""),
                "target_kind": str((mutation or {}).get("target_kind") or ""),
                "target_id": str((mutation or {}).get("target_id") or ""),
                "arguments_sha256": str((mutation or {}).get("arguments_sha256") or ""),
            }
            if any(str(step.get(key) or "") != value for key, value in immutable.items()):
                raise RuntimeError("production mutation identity was reused for a different contract")

        step["status"] = lifecycle
        step["updated_at"] = now
        step["revision"] = revision
        if result_status:
            step["result_status"] = str(result_status)[:120]
        if error:
            step["error"] = str(error)[:1000]
        elif lifecycle not in {"failed", "rejected"}:
            step.pop("error", None)

        command["steps"] = steps[-80:]
        command["updated_at"] = now
        command["revision"] = revision
        is_ship_workflow = str(command.get("action") or "") == "ship_existing_short"
        is_ship_root_step = bool(
            str((mutation or {}).get("action") or "") == "ship_existing_short"
            and str((mutation or {}).get("tool_name") or "") == "production_workflow"
        )
        if is_ship_workflow and not is_ship_root_step:
            # Child mutation receipts are evidence beneath the root workflow;
            # they cannot declare the creator's multi-step command terminal.
            command["status"] = "running"
        elif lifecycle in {"failed", "rejected"}:
            command["status"] = lifecycle
            command["error"] = str(error or result_status or lifecycle)[:1000]
        elif lifecycle in {"accepted", "completed"}:
            command["status"] = lifecycle
            command.pop("error", None)
        else:
            command["status"] = "running"
        rows = rows[-80:]
        session["production_commands"] = rows
        session["production_command_revision"] = revision
        session["latest_production_command"] = command
        _write_session_unlocked(session)
        return json.loads(json.dumps(command, default=str))


_PRODUCTION_WORKFLOW_TERMINAL = frozenset({"completed", "failed", "cancelled"})


def _finish_workflow_root_command_unlocked(
    session: dict[str, Any],
    workflow: dict[str, Any],
    *,
    succeeded: bool,
    error: str,
    now: float,
    promote_latest: bool = True,
) -> dict[str, Any]:
    """Make a workflow's terminal root transition in the same session write."""

    authority = (
        dict(workflow.get("authority") or {})
        if isinstance(workflow.get("authority"), dict)
        else {}
    )
    mutation = (
        dict(workflow.get("root_mutation") or {})
        if isinstance(workflow.get("root_mutation"), dict)
        else {}
    )
    command_id = str(workflow.get("command_id") or authority.get("command_id") or "").strip()
    mutation_id = str(mutation.get("mutation_id") or "").strip()
    if not command_id or not mutation_id:
        raise RuntimeError("terminal workflow is missing its immutable root command identity")

    rows = [
        dict(row)
        for row in list(session.get("production_commands") or [])
        if isinstance(row, dict) and str(row.get("command_id") or "").strip()
    ]
    command = next(
        (row for row in rows if str(row.get("command_id") or "") == command_id),
        None,
    )
    revision = max(0, int(session.get("production_command_revision") or 0)) + 1
    if command is None:
        root_envelope = (
            dict(mutation.get("command_envelope") or {})
            if isinstance(mutation.get("command_envelope"), dict)
            else {}
        )
        command = {
            "schema": str(authority.get("schema") or "studio.production-command.v2"),
            "command_id": command_id,
            "user_id": str(authority.get("user_id") or workflow.get("user_id") or ""),
            "session_id": str(workflow.get("session_id") or session.get("session_id") or ""),
            "source": str(authority.get("source") or "server_workflow"),
            "request_sha256": str(authority.get("request_sha256") or ""),
            "state_revision": int(authority.get("state_revision") or 0),
            "status": "authorized",
            "created_at": float(authority.get("issued_at") or workflow.get("created_at") or now),
            "updated_at": now,
            "revision": revision,
            "steps": [],
        }
        if root_envelope:
            command["command_envelope"] = root_envelope
            command["action"] = str(
                root_envelope.get("action") or mutation.get("action") or ""
            )
        rows.append(command)
    elif (
        str(command.get("user_id") or "")
        != str(authority.get("user_id") or workflow.get("user_id") or "")
        or str(command.get("session_id") or "")
        != str(workflow.get("session_id") or session.get("session_id") or "")
    ):
        raise RuntimeError("terminal workflow root command owner no longer matches")

    steps = [
        dict(step)
        for step in list(command.get("steps") or [])
        if isinstance(step, dict) and str(step.get("mutation_id") or "").strip()
    ]
    step = next(
        (candidate for candidate in steps if str(candidate.get("mutation_id") or "") == mutation_id),
        None,
    )
    if step is None:
        step = {
            "schema": str(mutation.get("schema") or "studio.production-mutation.v2"),
            "mutation_id": mutation_id,
            "action": str(mutation.get("action") or "ship_existing_short"),
            "tool_name": str(mutation.get("tool_name") or "production_workflow"),
            "target_kind": str(mutation.get("target_kind") or "shortform"),
            "target_id": str(mutation.get("target_id") or workflow.get("job_id") or ""),
            "scene_indices": list(mutation.get("scene_indices") or []),
            "arguments_sha256": str(mutation.get("arguments_sha256") or ""),
            "created_at": float(mutation.get("authorized_at") or workflow.get("created_at") or now),
        }
        steps.append(step)
    else:
        immutable = {
            "action": str(mutation.get("action") or ""),
            "tool_name": str(mutation.get("tool_name") or ""),
            "target_kind": str(mutation.get("target_kind") or ""),
            "target_id": str(mutation.get("target_id") or ""),
            "arguments_sha256": str(mutation.get("arguments_sha256") or ""),
        }
        if any(str(step.get(key) or "") != value for key, value in immutable.items()):
            raise RuntimeError("terminal workflow root mutation identity changed")

    terminal_status = "completed" if succeeded else "failed"
    expected_error = str(error or "production workflow failed")[:1000]
    already_terminal = bool(
        str(command.get("status") or "") == terminal_status
        and str(step.get("status") or "") == terminal_status
        and (
            succeeded
            or (
                str(command.get("error") or "") == expected_error
                and str(step.get("error") or "") == expected_error
            )
        )
    )
    if already_terminal:
        latest = session.get("latest_production_command")
        latest_id = str(latest.get("command_id") or "") if isinstance(latest, dict) else ""
        if promote_latest or not latest_id or latest_id == command_id:
            session["latest_production_command"] = command
        return command

    step["status"] = terminal_status
    step["result_status"] = "complete" if succeeded else "failed"
    step["updated_at"] = now
    step["revision"] = revision
    if succeeded:
        step.pop("error", None)
    else:
        step["error"] = expected_error
    command["steps"] = steps[-80:]
    command["status"] = terminal_status
    command["updated_at"] = now
    command["revision"] = revision
    if succeeded:
        command.pop("error", None)
    else:
        command["error"] = expected_error
    session["production_commands"] = rows[-80:]
    session["production_command_revision"] = revision
    latest = session.get("latest_production_command")
    latest_id = str(latest.get("command_id") or "") if isinstance(latest, dict) else ""
    if promote_latest or not latest_id or latest_id == command_id:
        session["latest_production_command"] = command
    return command


def create_shortform_ship_workflow(
    session_id: str,
    *,
    authority: dict[str, Any],
    root_mutation: dict[str, Any],
    job_id: str,
    scene_indices: list[int],
    animation_scene_indices: list[int],
    animate: bool,
) -> tuple[dict[str, Any], bool]:
    """Persist one idempotent backend-owned ship workflow.

    The browser is deliberately absent from this contract. The immutable
    command/job pair owns every later approval, animation, and finalization
    step, including recovery after a process restart.
    """

    normalized_session = str(session_id or "").strip()
    normalized_command = str((authority or {}).get("command_id") or "").strip()
    normalized_user = str((authority or {}).get("user_id") or "").strip()
    normalized_job = str(job_id or "").strip()
    if not all((normalized_session, normalized_command, normalized_user, normalized_job)):
        raise ValueError("shortform ship workflow requires session, command, user, and job")
    workflow_id = f"ship_{hashlib.sha256(
        f'{normalized_command}:{normalized_job}'.encode('utf-8')
    ).hexdigest()[:32]}"
    exact_scenes = sorted({int(value) for value in scene_indices if int(value) >= 0})
    exact_animation_scenes = sorted({
        int(value) for value in animation_scene_indices if int(value) >= 0
    })

    with _session_write_lock(normalized_session), _session_write_file_lock(normalized_session):
        session = _read_session_file(_session_path(normalized_session))
        if not session:
            raise KeyError(normalized_session)
        if str(session.get("user_id") or "").strip() != normalized_user:
            raise RuntimeError("shortform ship workflow owner does not match session owner")
        rows = [
            dict(row)
            for row in list(session.get("production_workflows") or [])
            if isinstance(row, dict) and str(row.get("workflow_id") or "").strip()
        ]
        existing = next(
            (row for row in rows if str(row.get("workflow_id") or "") == workflow_id),
            None,
        )
        if existing is not None:
            immutable_matches = bool(
                str(existing.get("user_id") or "") == normalized_user
                and str(existing.get("job_id") or "") == normalized_job
                and bool(existing.get("animate")) is bool(animate)
                and list(existing.get("scene_indices") or []) == exact_scenes
                and list(existing.get("animation_scene_indices") or [])
                == exact_animation_scenes
                and str(
                    (existing.get("root_mutation") or {}).get("arguments_sha256")
                    if isinstance(existing.get("root_mutation"), dict)
                    else ""
                )
                == str((root_mutation or {}).get("arguments_sha256") or "")
            )
            if not immutable_matches:
                raise RuntimeError(
                    "production command identity was reused for a different ship workflow"
                )
            return json.loads(json.dumps(existing, default=str)), False
        conflicting = next(
            (
                row
                for row in rows
                if str(row.get("job_id") or "") == normalized_job
                and str(row.get("status") or "") not in _PRODUCTION_WORKFLOW_TERMINAL
            ),
            None,
        )
        if conflicting is not None:
            raise RuntimeError(
                "another backend production command already owns this short-form ship workflow"
            )
        now = _now()
        workflow = {
            "schema": "studio.production-workflow.v1",
            "workflow_id": workflow_id,
            "workflow_kind": "ship_existing_short",
            "command_id": normalized_command,
            "session_id": normalized_session,
            "user_id": normalized_user,
            "job_id": normalized_job,
            "animate": bool(animate),
            "scene_indices": exact_scenes,
            "animation_scene_indices": exact_animation_scenes,
            "status": "queued",
            "stage": "approve",
            "revision": 1,
            "created_at": now,
            "updated_at": now,
            "next_attempt_at": now,
            "authority": {
                key: value
                for key, value in dict(authority or {}).items()
                if key
                in {
                    "schema",
                    "command_id",
                    "user_id",
                    "session_id",
                    "source",
                    "request_sha256",
                    "execution_quote",
                    "state_revision",
                    "issued_at",
                }
            },
            "root_mutation": dict(root_mutation or {}),
            "step_receipts": {},
        }
        rows.append(workflow)
        session["production_workflows"] = rows[-80:]
        _write_session_unlocked(session)
        return json.loads(json.dumps(workflow, default=str)), True


def get_production_workflow(
    session_id: str,
    workflow_id: str,
) -> dict[str, Any] | None:
    session = get_session(
        str(session_id or ""),
        reconcile_jobs=False,
        _prune_active_jobs=False,
    )
    if not session:
        return None
    wanted = str(workflow_id or "").strip()
    for row in list(session.get("production_workflows") or []):
        if isinstance(row, dict) and str(row.get("workflow_id") or "") == wanted:
            return json.loads(json.dumps(row, default=str))
    return None


def list_pending_production_workflows(*, limit: int = 200) -> list[dict[str, Any]]:
    """Return resumable workflows across sessions without reconciling jobs."""

    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    for path in sorted(
        SESSIONS_DIR.glob("sa_*.json"),
        key=lambda candidate: candidate.stat().st_mtime,
        reverse=True,
    ):
        if len(rows) >= max(1, int(limit)):
            break
        try:
            session = _read_session_file(path)
        except Exception:
            continue
        if not session or session.get("deleted_at"):
            continue
        for raw in list(session.get("production_workflows") or []):
            if not isinstance(raw, dict):
                continue
            if str(raw.get("status") or "") in _PRODUCTION_WORKFLOW_TERMINAL:
                continue
            row = dict(raw)
            row.setdefault("session_id", str(session.get("session_id") or ""))
            rows.append(json.loads(json.dumps(row, default=str)))
            if len(rows) >= max(1, int(limit)):
                break
    return rows


def claim_production_workflow(
    session_id: str,
    workflow_id: str,
    *,
    lease_owner: str,
    lease_seconds: float = 45.0,
) -> dict[str, Any] | None:
    """Acquire or renew the cross-process workflow lease."""

    normalized_session = str(session_id or "").strip()
    wanted = str(workflow_id or "").strip()
    owner = str(lease_owner or "").strip()
    if not normalized_session or not wanted or not owner:
        return None
    with _session_write_lock(normalized_session), _session_write_file_lock(normalized_session):
        session = _read_session_file(_session_path(normalized_session))
        if not session:
            return None
        now = _now()
        rows = list(session.get("production_workflows") or [])
        for index, raw in enumerate(rows):
            if not isinstance(raw, dict) or str(raw.get("workflow_id") or "") != wanted:
                continue
            row = dict(raw)
            if str(row.get("status") or "") in _PRODUCTION_WORKFLOW_TERMINAL:
                return None
            current_owner = str(row.get("lease_owner") or "").strip()
            lease_expires_at = float(row.get("lease_expires_at") or 0.0)
            if current_owner and current_owner != owner and lease_expires_at > now:
                return None
            row["lease_owner"] = owner
            row["lease_expires_at"] = now + max(10.0, float(lease_seconds))
            row["heartbeat_at"] = now
            row["updated_at"] = now
            rows[index] = row
            session["production_workflows"] = rows
            _write_session_unlocked(session)
            return json.loads(json.dumps(row, default=str))
    return None


def update_production_workflow(
    session_id: str,
    workflow_id: str,
    *,
    lease_owner: str,
    fields: dict[str, Any],
) -> dict[str, Any] | None:
    """Advance a workflow only while the caller owns its durable lease."""

    normalized_session = str(session_id or "").strip()
    wanted = str(workflow_id or "").strip()
    owner = str(lease_owner or "").strip()
    allowed = {
        "status",
        "stage",
        "last_error",
        "last_snapshot",
        "step_receipts",
        "next_attempt_at",
        "completed_at",
        "failed_at",
        "lease_expires_at",
        "heartbeat_at",
    }
    patch = {key: value for key, value in dict(fields or {}).items() if key in allowed}
    with _session_write_lock(normalized_session), _session_write_file_lock(normalized_session):
        session = _read_session_file(_session_path(normalized_session))
        if not session:
            return None
        rows = list(session.get("production_workflows") or [])
        for index, raw in enumerate(rows):
            if not isinstance(raw, dict) or str(raw.get("workflow_id") or "") != wanted:
                continue
            row = dict(raw)
            if str(row.get("lease_owner") or "").strip() != owner:
                return None
            row.update(patch)
            row["revision"] = max(0, int(row.get("revision") or 0)) + 1
            row["updated_at"] = _now()
            rows[index] = row
            session["production_workflows"] = rows
            _write_session_unlocked(session)
            return json.loads(json.dumps(row, default=str))
    return None


def finish_production_workflow(
    session_id: str,
    workflow_id: str,
    *,
    lease_owner: str,
    succeeded: bool,
    assistant_text: str,
    snapshot: dict[str, Any],
    error: str = "",
) -> dict[str, Any] | None:
    """Atomically finish the workflow and append its transcript card once."""

    normalized_session = str(session_id or "").strip()
    wanted = str(workflow_id or "").strip()
    owner = str(lease_owner or "").strip()
    with _session_write_lock(normalized_session), _session_write_file_lock(normalized_session):
        session = _read_session_file(_session_path(normalized_session))
        if not session:
            return None
        rows = list(session.get("production_workflows") or [])
        found: dict[str, Any] | None = None
        now = _now()
        for index, raw in enumerate(rows):
            if not isinstance(raw, dict) or str(raw.get("workflow_id") or "") != wanted:
                continue
            row = dict(raw)
            if str(row.get("status") or "") in _PRODUCTION_WORKFLOW_TERMINAL:
                # Terminal receipts are immutable. A late/stale worker may
                # observe the winner, but it can never flip success to failure
                # (or vice versa) after its lease expired.
                terminal_succeeded = str(row.get("status") or "") == "completed"
                _finish_workflow_root_command_unlocked(
                    session,
                    row,
                    succeeded=terminal_succeeded,
                    error=str(row.get("last_error") or ""),
                    now=now,
                    promote_latest=False,
                )
                _write_session_unlocked(session)
                return json.loads(json.dumps(row, default=str))
            if str(row.get("lease_owner") or "").strip() != owner:
                return None
            row.update({
                "status": "completed" if succeeded else "failed",
                "stage": "completed" if succeeded else "failed",
                "last_snapshot": dict(snapshot or {}),
                "last_error": str(error or "")[:1000],
                "completed_at" if succeeded else "failed_at": now,
                "lease_expires_at": 0.0,
                "revision": max(0, int(row.get("revision") or 0)) + 1,
                "updated_at": now,
            })
            _finish_workflow_root_command_unlocked(
                session,
                row,
                succeeded=succeeded,
                error=str(error or ""),
                now=now,
            )
            messages = list(session.get("messages") or [])
            already_written = any(
                isinstance(message, dict)
                and str(message.get("productionWorkflowId") or "") == wanted
                for message in messages
            )
            if not already_written:
                message: dict[str, Any] = {
                    "role": "assistant",
                    "content": str(assistant_text or ""),
                    "productionWorkflowId": wanted,
                    "productionCommandId": str(row.get("command_id") or ""),
                }
                if snapshot:
                    message["jobDeliverable"] = dict(snapshot)
                messages.append(message)
                session["messages"] = messages
            row["completion_message_written"] = True
            rows[index] = row
            found = row
            command_id = str(row.get("command_id") or "")
            if str(session.get("active_command_id") or "") == command_id:
                session.update({
                    "interaction_state": "verification",
                    "production_gate_open": False,
                    "active_command_id": "",
                    "active_command_job_id": "",
                })
            job_id = str(row.get("job_id") or "")
            active_jobs = [
                dict(job)
                for job in list(session.get("active_jobs") or [])
                if isinstance(job, dict)
                and not (
                    succeeded
                    and str(job.get("job_id") or "") == job_id
                )
            ]
            if not succeeded and job_id and not any(
                str(job.get("job_id") or "") == job_id for job in active_jobs
            ):
                active_jobs.append({
                    "job_id": job_id,
                    "kind": "shortform",
                    "title": str((snapshot or {}).get("title") or "Short-form video"),
                    "status": str((snapshot or {}).get("status") or "failed"),
                    "stage": str((snapshot or {}).get("stage") or "failed"),
                    "started_at": float(row.get("created_at") or now),
                })
            session["active_jobs"] = active_jobs
            break
        if found is None:
            return None
        session["production_workflows"] = rows
        _write_session_unlocked(session)
        return json.loads(json.dumps(found, default=str))


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
    with _session_write_lock(session_id), _session_write_file_lock(session_id):
        session = _read_session_file(_session_path(session_id))
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
            _write_session_unlocked(session)
            return _public_run(run)
    return None


def finish_run(
    session_id: str,
    run_id: str,
    *,
    status: str = "complete",
    error: str = "",
    result: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    with _session_write_lock(session_id), _session_write_file_lock(session_id):
        session = _read_session_file(_session_path(session_id))
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
            if result is not None:
                run["result"] = dict(result)
            _write_session_unlocked(session)
            return _public_run(run)
    return None


def get_session(
    session_id: str,
    *,
    user_id: str | None = None,
    reconcile_jobs: bool = False,
    _prune_active_jobs: bool = False,
) -> dict[str, Any] | None:
    """Read a Studio session without starting or reclaiming production.

    Reconciliation is opt-in for legacy maintenance callers only. Browser
    refresh, history, chat bootstrap, and status reads must remain pure; a
    durable backend command/worker owns every production continuation.
    """
    path = _session_path(session_id)
    if not path.exists():
        return None
    session = _read_session_file(path)
    if not session:
        return None
    if user_id and session.get("user_id") != user_id:
        return None
    # Reads may project compatibility migrations, but only explicit mutation
    # paths persist them. Browser refresh must never advance revisions or
    # rewrite pending/production state.
    _migrate_session_provider_policy(session)
    session = _reconcile_session_concept(
        _sanitize_session_pending(session, persist=False),
        persist=False,
    )
    if _prune_active_jobs:
        session = prune_stale_active_jobs(session, persist=True)
    if not reconcile_jobs:
        return session
    try:
        from studio_agent import jobs as agent_jobs

        session = agent_jobs.reconcile_running_longform_jobs(
            session_id,
            user_id=user_id,
            session=session,
        ) or session
        session = agent_jobs.reconcile_thumbnail_only_active_jobs(
            session_id,
            user_id=user_id,
            session=session,
        ) or session
        session = agent_jobs.reconcile_terminal_active_jobs(
            session_id,
            user_id=user_id,
            session=session,
        ) or session
    except Exception:
        pass
    return session


def _reconcile_session_concept(
    session: dict[str, Any],
    *,
    persist: bool = True,
) -> dict[str, Any]:
    """Repair a stale stored long-form concept every time a session is read.

    Concept fixes (channel-aware beats/hooks, cross-format title guards) only
    apply when a plan is rebuilt — but Sync/resume rehydrate the stored
    pending_concept as-is, so an old broken card would survive every deploy.
    Reconciling at read time makes fixes retroactive for existing sessions."""
    plan = session.get("pending_concept")
    if not isinstance(plan, dict) or str(plan.get("format") or "") != "longform":
        return session
    try:
        from studio_agent import concept_plan as concept_plan_mod

        before = json.dumps(plan, sort_keys=True, default=str)
        fixed = concept_plan_mod.reconcile_longform_plan(dict(plan), session=session)
        if json.dumps(fixed, sort_keys=True, default=str) != before:
            session["pending_concept"] = fixed
            if persist:
                _save(session)
    except Exception:
        pass
    return session


def _launch_render_style_key(value: str) -> str:
    """Resolve a session picker value to a launch-gated render style key, else ""."""
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        from studio_agent.render_styles import LAUNCH_RENDER_STYLE_KEYS, get_render_style

        style = get_render_style(raw)
        return style.key if style.key in LAUNCH_RENDER_STYLE_KEYS else ""
    except Exception:
        return ""


def _align_pending_picker_state(session: dict[str, Any]) -> bool:
    """Retarget waiting Approve cards to the session's live picker values.

    Returns True when session fields were mutated."""
    pending = list(session.get("pending_actions") or [])
    if not pending:
        return False
    changed = False
    session_style = _launch_render_style_key(str(session.get("render_style") or ""))
    session_image = normalize_image_model(str(session.get("image_model") or session.get("image_model_id") or ""))
    session_video = normalize_video_model(str(session.get("video_model") or ""))
    aligned: list[dict[str, Any]] = []
    for action in pending:
        if not isinstance(action, dict):
            continue
        row = dict(action)
        tool = str(row.get("tool") or "")
        if tool not in {"start_longform_render", "start_shortform_generate"}:
            aligned.append(row)
            continue
        args = row.get("arguments") if isinstance(row.get("arguments"), dict) else {}
        merged = dict(args)
        if session_style and str(merged.get("render_style") or "") != session_style:
            merged["render_style"] = session_style
            changed = True
        if session_image and str(merged.get("image_model_id") or merged.get("image_model") or "") != session_image:
            merged["image_model_id"] = session_image
            changed = True
        if session_video and str(merged.get("video_model") or "") != session_video:
            merged["video_model"] = session_video
            changed = True
        if merged != args:
            row["arguments"] = merged
        aligned.append(row)
    if changed:
        messages = list(session.get("messages") or [])
        session["pending_actions"] = normalize_pending_actions(aligned, messages)
        last_prod = session.get("last_production") if isinstance(session.get("last_production"), dict) else {}
        if last_prod and str(last_prod.get("tool") or "") in {"start_longform_render", "start_shortform_generate"}:
            prod_args = last_prod.get("arguments") if isinstance(last_prod.get("arguments"), dict) else {}
            merged_prod = dict(prod_args)
            if session_style:
                merged_prod["render_style"] = session_style
            if session_image:
                merged_prod["image_model_id"] = session_image
            if session_video:
                merged_prod["video_model"] = session_video
            last_prod["arguments"] = merged_prod
            session["last_production"] = last_prod
        concept = session.get("pending_concept") if isinstance(session.get("pending_concept"), dict) else {}
        if concept:
            if session_style and str(concept.get("visual_style") or "") != session_style:
                concept["visual_style"] = session_style
                try:
                    from studio_agent.render_styles import get_render_style

                    concept["visual_style_label"] = get_render_style(session_style).label
                except Exception:
                    pass
                session["pending_concept"] = concept
    return changed


def _sanitize_session_pending(
    session: dict[str, Any],
    *,
    persist: bool = True,
) -> dict[str, Any]:
    """Normalize and prune stale pending approvals every time a session is read."""
    messages = list(session.get("messages") or [])
    pending = list(session.get("pending_actions") or [])
    picker_changed = _align_pending_picker_state(session)
    if picker_changed:
        pending = list(session.get("pending_actions") or [])
    if not pending:
        if picker_changed and persist:
            _save(session)
        return session
    aligned_pending: list[dict[str, Any]] = []
    for action in pending:
        if not isinstance(action, dict):
            continue
        row = dict(action)
        if str(row.get("tool") or "") == "start_shortform_generate":
            args = row.get("arguments") if isinstance(row.get("arguments"), dict) else {}
            row["arguments"] = _prepare_shortform_execution_args(args, messages, session=session)
        aligned_pending.append(row)
    kept = [
        action
        for action in aligned_pending
        if not _shortform_action_stale_for_latest_user(
            action,
            messages,
            session=session,
            sibling_pending=aligned_pending,
        )
    ]
    # Hard keep: latest user committed production and pending matches that title.
    users = [
        str(m.get("content") or "")
        for m in messages
        if isinstance(m, dict) and str(m.get("role") or "") == "user"
    ]
    latest_text = users[-1] if users else _latest_user_text(messages)
    if _latest_user_allows_production_pending(latest_text):
        requested = _requested_title_from_user_text(latest_text)
        for action in aligned_pending:
            if str(action.get("tool") or "") not in SINGLETON_PRODUCTION_APPROVAL_TOOLS:
                continue
            action_title = _production_action_title(action)
            if is_boilerplate_production_topic(action_title):
                continue
            if requested and action_title and _title_overlap_score(action_title, requested) >= 0.5:
                if action not in kept:
                    kept.append(action)
            elif not requested and action not in kept:
                kept.append(action)
    normalized = normalize_pending_actions(kept, messages)
    changed = picker_changed or normalized != pending
    if changed:
        session["pending_actions"] = normalized
        # Drop last_production when every production approve was scrubbed — stops
        # recover/sync loops from resurrecting the same wrong-title card.
        dropped_prod = any(
            str(a.get("tool") or "") in SINGLETON_PRODUCTION_APPROVAL_TOOLS for a in pending
        ) and not any(
            str(a.get("tool") or "") in SINGLETON_PRODUCTION_APPROVAL_TOOLS for a in normalized
        )
        if dropped_prod:
            session["last_production"] = {}
        if persist:
            _save(session)
    return session


def _write_session_unlocked(session: dict[str, Any]) -> None:
    """Atomically replace one session while its process and file locks are held."""

    _migrate_session_provider_policy(session)
    # Every persisted control-plane change advances the single frontend
    # projection revision. Messages, confirmations, picker locks, command
    # lifecycle, jobs, and cancellation therefore share one total order.
    session["production_view_revision"] = max(
        0,
        int(session.get("production_view_revision") or 0),
    ) + 1
    destination = _session_path(str(session["session_id"]))
    destination.parent.mkdir(parents=True, exist_ok=True)
    session["updated_at"] = _now()
    temporary = destination.parent / f".{destination.name}.{uuid.uuid4().hex}.tmp"
    temporary.write_text(
        json.dumps(session, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    temporary.replace(destination)


def _save(session: dict[str, Any]) -> None:
    _migrate_session_provider_policy(session)
    session_id = str(session["session_id"])
    destination = _session_path(session_id)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with _session_write_lock(session_id), _session_write_file_lock(session_id):
        # A heartbeat or stale runner snapshot must never roll a live picker
        # switch backward. Preserve the newest monotonic media route even when
        # another writer began from an older copy of the session.
        existing = _read_session_file(destination) if destination.is_file() else None
        if isinstance(existing, dict):
            _migrate_session_provider_policy(existing)
            session["production_view_revision"] = max(
                int(session.get("production_view_revision") or 0),
                int(existing.get("production_view_revision") or 0),
            )
            existing_route = media_route_snapshot(existing)
            incoming_route = media_route_snapshot(session)
            existing_is_newer = (
                int(existing_route["revision"]) > int(incoming_route["revision"])
                or (
                    int(existing_route["revision"]) == int(incoming_route["revision"])
                    and float(existing_route["updated_at"]) > float(incoming_route["updated_at"])
                )
            )
            if existing_is_newer:
                for key in (
                    "image_model",
                    "video_model",
                    "media_route_revision",
                    "media_route_updated_at",
                ):
                    if key in existing:
                        session[key] = existing[key]
        _write_session_unlocked(session)


def update_session(session_id: str, **fields: Any) -> dict[str, Any]:
    with _session_write_lock(session_id), _session_write_file_lock(session_id):
        session = _read_session_file(_session_path(session_id))
        if not session:
            raise KeyError(session_id)
        return _update_session_locked(session, **fields)


def claim_production_gate(
    session_id: str,
    *,
    command_id: str,
    job_id: str,
) -> dict[str, Any] | None:
    """Atomically claim a session's spend-capable gate for one command."""

    normalized_command = str(command_id or "").strip()
    normalized_job = str(job_id or "").strip()
    if not normalized_command or not normalized_job:
        return None
    with _session_write_lock(session_id), _session_write_file_lock(session_id):
        session = _read_session_file(_session_path(session_id))
        if not session:
            raise KeyError(session_id)
        current_command = str(session.get("active_command_id") or "").strip()
        if session.get("production_gate_open") and current_command not in {"", normalized_command}:
            if _terminal_run_owns_production_gate(session, current_command):
                # A terminal run (including an interrupted deploy) is not an
                # active mutation. Reclaim its orphaned lease atomically here,
                # where the next command is about to be admitted.
                session.update({
                    "interaction_state": "verification",
                    "production_gate_open": False,
                    "active_command_id": "",
                    "active_command_job_id": "",
                })
            else:
                return None
        session.update({
            "agent_mode": "studio",
            "interaction_state": "production",
            "production_gate_open": True,
            "active_command_id": normalized_command,
            "active_command_job_id": normalized_job,
        })
        _write_session_unlocked(session)
        return session


def close_production_gate(
    session_id: str,
    *,
    command_id: str,
    interaction_state: str = "verification",
) -> dict[str, Any] | None:
    """Close only the gate owned by ``command_id``; never clear a newer claim."""

    normalized_command = str(command_id or "").strip()
    with _session_write_lock(session_id), _session_write_file_lock(session_id):
        session = _read_session_file(_session_path(session_id))
        if not session:
            return None
        current_command = str(session.get("active_command_id") or "").strip()
        if current_command and current_command != normalized_command:
            return session
        session.update({
            "interaction_state": str(interaction_state or "verification"),
            "production_gate_open": False,
            "active_command_id": "",
            "active_command_job_id": "",
        })
        _write_session_unlocked(session)
        return session


def _update_session_locked(session: dict[str, Any], **fields: Any) -> dict[str, Any]:
    """Apply fields to the latest on-disk value under the session file lock."""

    _migrate_session_provider_policy(session)
    previous_route = media_route_snapshot(session)
    if "model" in fields:
        requested_model = str(fields.get("model") or "").strip()
        if provider_policy.model_provider(requested_model) != "unknown":
            fields["model"] = provider_policy.assert_runner_model_allowed(requested_model)
    if "video_model" in fields:
        fields["video_model"] = normalize_video_model(fields.get("video_model"))
    if "image_model" in fields:
        fields["image_model"] = normalize_image_model(fields.get("image_model"))
    if "image_model_id" in fields:
        fields["image_model_id"] = normalize_image_model(fields.get("image_model_id"))
    route_requested = "image_model" in fields or "video_model" in fields
    next_image_model = str(fields.get("image_model") or previous_route["image_model_id"])
    next_video_model = str(fields.get("video_model") or previous_route["video_model"])
    if route_requested and (
        next_image_model != previous_route["image_model_id"]
        or next_video_model != previous_route["video_model"]
    ):
        fields["media_route_revision"] = int(previous_route["revision"]) + 1
        fields["media_route_updated_at"] = _now()
    elif "media_route_revision" not in session:
        fields.setdefault("media_route_revision", int(previous_route["revision"]))
        fields.setdefault(
            "media_route_updated_at",
            float(previous_route["updated_at"] or _now()),
        )
    if "pending_actions" in fields:
        session_messages = list(fields.get("messages") or session.get("messages") or [])
        fields["pending_actions"] = normalize_pending_actions(
            list(fields.get("pending_actions") or []),
            session_messages,
        )
    session.update(fields)
    if any(k in fields for k in ("render_style", "image_model", "image_model_id", "video_model")):
        if _align_pending_picker_state(session):
            pass  # pending_actions / last_production / pending_concept updated in-place
    _write_session_unlocked(session)
    return session


def append_messages(session_id: str, new_messages: list[dict[str, Any]]) -> dict[str, Any]:
    with _session_write_lock(session_id), _session_write_file_lock(session_id):
        session = _read_session_file(_session_path(session_id))
        if not session:
            raise KeyError(session_id)
        session.setdefault("messages", []).extend(new_messages)
        _write_session_unlocked(session)
    compact_session_if_needed(session_id)
    session = get_session(session_id) or session
    return session


def set_pending_actions(session_id: str, actions: list[dict[str, Any]]) -> dict[str, Any]:
    with _session_write_lock(session_id), _session_write_file_lock(session_id):
        session = _read_session_file(_session_path(session_id))
        if not session:
            raise KeyError(session_id)
        session_messages = list(session.get("messages") or [])
        session["pending_actions"] = normalize_pending_actions(actions, session_messages)
        _write_session_unlocked(session)
        return session


def pop_pending_action(session_id: str, action_id: str) -> dict[str, Any] | None:
    with _session_write_lock(session_id), _session_write_file_lock(session_id):
        session = _read_session_file(_session_path(session_id))
        if not session:
            return None
        pending = session.get("pending_actions") or []
        hit = None
        rest = []
        for a in pending:
            if not isinstance(a, dict):
                continue
            if hit is None and a.get("id") == action_id:
                hit = a
            else:
                rest.append(a)
        if hit is None:
            return None
        session_messages = list(session.get("messages") or [])
        session["pending_actions"] = normalize_pending_actions(rest, session_messages)
        _write_session_unlocked(session)
        return hit


def coerce_tool_arguments(raw: Any) -> dict[str, Any]:
    """Normalize pending/tool arguments that may be a JSON string or non-dict."""
    import json as _json

    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = _json.loads(raw)
        except Exception:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _action_already_approved(messages: list[dict[str, Any]], tool_index: int) -> bool:
    """True if user already approved/rejected after this awaiting tool message."""
    for later in messages[tool_index + 1 :]:
        role = later.get("role")
        content = str(later.get("content") or "")
        if role == "user" and (
            content.startswith("[User approved ")
            or content.startswith("[User retried ")
            or content.startswith("[Rejected ")
        ):
            return True
        if role == "system" and content.startswith("[Studio Agent preflight tool result:"):
            return True
    return False


def _action_superseded_by_user_message(messages: list[dict[str, Any]], tool_index: int) -> bool:
    """True when a normal user message arrived after an approval was prepared.

    Approval cards are intentionally turn-local. If the user keeps chatting,
    asks a diagnostic question, or gives a new title, the old pending action is
    no longer safe to run.
    """
    for later in messages[tool_index + 1 :]:
        if later.get("role") != "user":
            continue
        content = str(later.get("content") or "")
        if content.startswith("[User approved ") or content.startswith("[User retried ") or content.startswith("[Rejected "):
            continue
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
        if _action_superseded_by_user_message(messages, i):
            return None
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
                blocked_jobs = {
                    str(value).strip()
                    for value in (session.get("blocked_job_ids") or [])
                    if str(value).strip()
                }
                if str(args.get("job_id") or "").strip() in blocked_jobs:
                    return None
                return {
                    "id": aid,
                    "tool": name,
                    "arguments": args,
                    "summary": f"{name}({json.dumps(args)[:200]})",
                    "recovered": True,
                }
    return None


def recover_last_production(session: dict[str, Any]) -> dict[str, Any] | None:
    """Return durable last_production.

    Do not rebuild production starts from visible transcript logs. Older sessions
    may contain raw approved-tool JSON as chat messages, and reusing those rows
    can resurrect an already-finished or wrong-title production.
    """
    from studio_agent.jobs import JOB_START_TOOLS

    lp = session.get("last_production")
    if isinstance(lp, dict):
        tool = str(lp.get("tool") or "").strip()
        args = lp.get("arguments")
        if tool in JOB_START_TOOLS and isinstance(args, dict) and args:
            return lp
    return None


def _production_already_approved(messages: list[dict[str, Any]]) -> bool:
    """True once the user approved a job-start tool (shortform/longform spawn)."""
    for msg in messages:
        if msg.get("role") != "user":
            if msg.get("role") == "system" and str(msg.get("content") or "").startswith("[Studio Agent preflight tool result: start_shortform_generate]"):
                return True
            if msg.get("role") == "system" and str(msg.get("content") or "").startswith("[Studio Agent preflight tool result: start_longform_render]"):
                return True
            continue
        content = str(msg.get("content") or "")
        if content.startswith("[User approved start_shortform_generate]"):
            return True
        if content.startswith("[User approved start_longform_render]"):
            return True
    return False


def _active_job_stale_for_latest_user(
    job: dict[str, Any],
    messages: list[dict[str, Any]],
    *,
    session: dict[str, Any] | None = None,
) -> bool:
    """True when a dock track belongs to a prior short the user has moved past."""
    session = session or {}
    title = str(job.get("title") or job.get("topic") or "").strip()
    if not title:
        return False
    latest = _latest_user_text(messages)
    target = resolve_current_production_target(session, messages)
    prior = prior_production_title(session)
    # Scene-1 / hard commit with no resolved title — prior Ready short is stale.
    if not target and _latest_user_allows_production_pending(latest):
        if prior and _title_overlap_score(title, prior) >= 0.75:
            return True
    if target and _title_overlap_score(title, target) < 0.34:
        return True
    if target and (
        _latest_user_allows_production_pending(latest) or _user_affirms_assistant_topic(latest)
    ):
        if _title_overlap_score(title, target) < 0.75:
            return True
    if _is_production_diagnostic_text(latest):
        return True
    if is_new_production_request(latest, session):
        if prior and _title_overlap_score(title, prior) >= 0.75:
            return True
    canonical = _canonical_production_topic(messages, session=session)
    if canonical and _title_overlap_score(title, canonical) < 0.34:
        return True
    if not _latest_user_allows_production_pending(latest):
        if prior and _title_overlap_score(title, prior) >= 0.75:
            if is_research_only_user_text(latest) or re.search(
                r"\b(?:plan(?:ning)?|next(?:\s+one)?|similar to|retention|views|analytics|stats|what worked)\b",
                latest,
                re.I,
            ):
                return True
    return False


def is_research_only_user_text(text: str) -> bool:
    """Mirror AgentPanel research-only turns that must not keep production gates."""
    low = str(text or "").lower()
    if not low.strip():
        return False
    if re.search(
        r"\b(?:yes|yeah|yep)\b.+\b(?:make|render|generate|produce)\b",
        low,
    ) or re.search(r"\b(?:go ahead and|please)\s+(?:make|render|generate|produce)\b", low):
        return False
    if re.search(
        r"\b(?:public (?:youtube )?data|live demand|what(?:'s| is) (?:working|performing|viral|trending)|"
        r"niche (?:data|performance|demand)|search trends|view counts?|retention|how (?:can|do) we make it better)\b",
        low,
    ):
        return True
    if re.search(
        r"\b(?:research|analyze|analysis|competitor|channel (?:stats|analytics|data))\b",
        low,
    ) and not re.search(r"\b(?:render|approve|start production)\b.+\bnow\b", low):
        return True
    if re.search(
        r"\b(?:let'?s|lets|maybe|what if|could we|should we)\b.+\b(?:make|create|do)\b.+\b(?:short|video|ad)\b",
        low,
    ) and (re.search(r"\?", low) or re.search(r"\b(?:how(?:'s|s)? that|how about|thoughts|instead|maybe|plan|concept)\b", low)):
        return True
    return False


def prune_stale_active_jobs(
    session: dict[str, Any],
    *,
    persist: bool = True,
) -> dict[str, Any]:
    """Drop ghost production tracks when the user has moved to a new short/video."""
    sid = str(session.get("session_id") or "").strip()
    jobs = list(session.get("active_jobs") or [])
    if not jobs:
        return session
    messages = list(session.get("messages") or [])
    kept: list[dict[str, Any]] = []
    changed = False
    try:
        from studio_agent import jobs as agent_jobs
    except Exception:
        agent_jobs = None  # type: ignore[assignment]

    for job in jobs:
        if not isinstance(job, dict):
            changed = True
            continue
        kind = str(job.get("kind") or "").strip()
        jid = str(job.get("job_id") or "").strip()
        if kind not in {"shortform", "longform", ""}:
            kept.append(job)
            continue
        stale = False
        if jid and agent_jobs and kind == "shortform" and agent_jobs.shortform_job_terminal_fast(jid):
            stale = True
        if not stale and _active_job_stale_for_latest_user(job, messages, session=session):
            stale = True
        if stale:
            # Never block a live awaiting-review job that still matches the
            # current production target — Sync was wiping the chat scene card.
            target = resolve_current_production_target(session, messages)
            job_title = str(job.get("title") or job.get("topic") or "").strip()
            status = str(job.get("status") or "").strip().lower()
            if (
                jid
                and target
                and job_title
                and _title_overlap_score(job_title, target) >= 0.75
                and status in {"awaiting_approval", "running", ""}
            ):
                kept.append(job)
                continue
            changed = True
            if jid:
                blocked = list(session.get("blocked_job_ids") or [])
                if jid not in blocked:
                    blocked.append(jid)
                    session = {**session, "blocked_job_ids": blocked[-24:]}
            continue
        kept.append(job)
    if not changed:
        return session
    if persist and sid:
        blocked_job_ids = list(session.get("blocked_job_ids") or [])
        return update_session(
            sid,
            active_jobs=kept,
            blocked_job_ids=blocked_job_ids,
        ) or {**session, "active_jobs": kept, "blocked_job_ids": blocked_job_ids}
    return {**session, "active_jobs": kept}


def force_sync_session(session_id: str, *, user_id: str | None = None) -> dict[str, Any] | None:
    """Authoritative chat sync: prune stale approvals, clear dead runs, return session.

    Used by the UI "Sync chat" button — must always return the live Fly session
    state, never rehydrate production Approves from old transcript tool rows.
    """
    session = get_session(session_id, user_id=user_id, reconcile_jobs=False)
    if not session:
        return None
    try:
        session = reconcile_production_state(session, persist=True) or session
    except Exception:
        pass
    try:
        session = prune_stale_active_jobs(session, persist=True) or session
    except Exception:
        pass
    try:
        session = reconcile_stale_runs(session) or session
    except Exception:
        pass
    messages = list(session.get("messages") or [])
    pending = list(session.get("pending_actions") or [])
    # Scrub poisoned beat-sheet locked titles so they cannot kill Approve.
    intent = session.get("conversation_intent") if isinstance(session.get("conversation_intent"), dict) else {}
    locked_raw = str(intent.get("locked_title") or intent.get("working_title") or "").strip()
    if locked_raw and is_boilerplate_production_topic(locked_raw):
        intent = dict(intent)
        intent.pop("locked_title", None)
        intent.pop("working_title", None)
        if is_boilerplate_production_topic(str(intent.get("last_topic") or "")):
            intent.pop("last_topic", None)
        session = update_session(session_id, conversation_intent=intent) or session
        messages = list(session.get("messages") or [])
        pending = list(session.get("pending_actions") or [])
    kept = [
        action
        for action in pending
        if isinstance(action, dict)
        and not _shortform_action_stale_for_latest_user(
            action,
            messages,
            session=session,
            sibling_pending=pending,
        )
    ]
    # Hard keep: latest user committed production and this pending matches that title.
    latest_text = _latest_user_text(messages)
    if _latest_user_allows_production_pending(latest_text):
        requested = _requested_title_from_user_text(latest_text)
        for action in pending:
            if not isinstance(action, dict):
                continue
            if str(action.get("tool") or "") not in SINGLETON_PRODUCTION_APPROVAL_TOOLS:
                continue
            action_title = _production_action_title(action)
            if requested and action_title and _title_overlap_score(action_title, requested) >= 0.5:
                if action not in kept:
                    kept.append(action)
            elif not requested and action not in kept:
                # Hard commit without a parseable title — still keep the prepared card.
                kept.append(action)
    normalized = normalize_pending_actions(kept, messages)
    dropped_prod = any(
        str(a.get("tool") or "") in SINGLETON_PRODUCTION_APPROVAL_TOOLS for a in pending
    ) and not any(
        str(a.get("tool") or "") in SINGLETON_PRODUCTION_APPROVAL_TOOLS for a in normalized
    )
    fields: dict[str, Any] = {}
    if normalized != pending:
        fields["pending_actions"] = normalized
    if dropped_prod or not _latest_user_allows_production_pending(_latest_user_text(messages)):
        # Research / soft turns must not keep last_production for recover loops.
        if session.get("last_production"):
            fields["last_production"] = {}
    if fields:
        session = update_session(session_id, **fields) or session
    # Never rebuild production pending from transcript on sync.
    # Job reconcile (QA, long-form still scans) runs on dedicated poll endpoints —
    # doing it here blocked session resume for 120s+ on Fly.
    return get_session(session_id, user_id=user_id, reconcile_jobs=False)


def sync_pending_from_messages(session_id: str) -> list[dict[str, Any]]:
    """Prune stale pending; only restore non-production approvals from transcript.

    Production start cards (start_shortform / start_longform) are NEVER rebuilt
    from old tool rows — that was resurrecting Approve after research Sync.
    """
    session = get_session(session_id)
    if not session:
        return []
    messages = list(session.get("messages") or [])
    pending = list(session.get("pending_actions") or [])
    blocked_jobs = {
        str(value).strip()
        for value in (session.get("blocked_job_ids") or [])
        if str(value).strip()
    }
    if pending:
        kept = [
            action
            for action in pending
            if str(((action.get("arguments") or {}) if isinstance(action.get("arguments"), dict) else {}).get("job_id") or "").strip() not in blocked_jobs
            if not _shortform_action_stale_for_latest_user(
                action,
                messages,
                session=session,
                sibling_pending=pending,
            )
        ]
        normalized = normalize_pending_actions(kept, messages)
        if normalized != pending:
            session["pending_actions"] = normalized
            dropped_prod = any(
                str(a.get("tool") or "") in SINGLETON_PRODUCTION_APPROVAL_TOOLS for a in pending
            ) and not any(
                str(a.get("tool") or "") in SINGLETON_PRODUCTION_APPROVAL_TOOLS for a in normalized
            )
            if dropped_prod:
                session["last_production"] = {}
            _save(session)
        return normalized

    # Do not resurrect production-start approvals from transcript.
    # Older sessions store awaiting_user_approval tool rows that re-open Approve
    # after Sync even when the user is mid research.
    import json

    if _production_already_approved(messages):
        return []
    if not _latest_user_allows_production_pending(_latest_user_text(messages)):
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
        if _action_superseded_by_user_message(messages, i):
            seen.add(aid)
            continue
        if _action_already_approved(messages, i):
            continue
        rec = recover_pending_action_from_messages(session, aid)
        if not rec:
            continue
        # Never rebuild brand-new production tools from history.
        if str(rec.get("tool") or "") in SINGLETON_PRODUCTION_APPROVAL_TOOLS:
            seen.add(aid)
            continue
        if _shortform_action_stale_for_latest_user(
            rec,
            messages,
            session=session,
            sibling_pending=rebuilt,
        ):
            seen.add(aid)
            continue
        rebuilt.append(rec)
        seen.add(aid)

    if rebuilt:
        normalized = normalize_pending_actions(rebuilt, messages)
        set_pending_actions(session_id, normalized)
        return normalized
    latest = get_session(session_id) or {}
    return list(latest.get("pending_actions") or [])


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
