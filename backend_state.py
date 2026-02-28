import asyncio
import json
import logging
import random
import time

from backend_settings import (
    CREATIVE_SESSIONS_FILE,
    CREATIVE_SESSION_PERSISTENCE_ENABLED,
    PROJECTS_STORE_FILE,
)

log = logging.getLogger("nyptid-studio")

_creative_sessions: dict = {}
_creative_sessions_lock = asyncio.Lock()
_projects: dict = {}
_projects_lock = asyncio.Lock()


def _prune_creative_sessions(max_age_seconds: int = 72 * 3600):
    now = time.time()
    stale_ids = [
        sid for sid, sess in _creative_sessions.items()
        if now - float(sess.get("created_at", now)) > max_age_seconds
    ]
    for sid in stale_ids:
        _creative_sessions.pop(sid, None)


def _load_creative_sessions_from_disk():
    if not CREATIVE_SESSION_PERSISTENCE_ENABLED:
        return
    if not CREATIVE_SESSIONS_FILE.exists():
        return
    try:
        data = json.loads(CREATIVE_SESSIONS_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            _creative_sessions.clear()
            _creative_sessions.update(data)
            _prune_creative_sessions()
            log.info(f"Loaded {len(_creative_sessions)} creative sessions from disk")
    except Exception as e:
        log.warning(f"Failed to load creative sessions store: {e}")


def _save_creative_sessions_to_disk():
    if not CREATIVE_SESSION_PERSISTENCE_ENABLED:
        return
    try:
        _prune_creative_sessions()
        tmp_path = CREATIVE_SESSIONS_FILE.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(_creative_sessions, ensure_ascii=True), encoding="utf-8")
        tmp_path.replace(CREATIVE_SESSIONS_FILE)
    except Exception as e:
        log.warning(f"Failed to persist creative sessions store: {e}")


def _get_creative_session(session_id: str):
    """Fetch a creative session; on miss, refresh from disk and retry."""
    session = _creative_sessions.get(session_id)
    if session is not None:
        return session
    if CREATIVE_SESSION_PERSISTENCE_ENABLED:
        _load_creative_sessions_from_disk()
        session = _creative_sessions.get(session_id)
    return session


def _load_projects_store():
    if not PROJECTS_STORE_FILE.exists():
        return
    try:
        data = json.loads(PROJECTS_STORE_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            _projects.clear()
            _projects.update(data)
            log.info(f"Loaded {len(_projects)} projects from disk")
    except Exception as e:
        log.warning(f"Failed to load projects store: {e}")


def _save_projects_store():
    try:
        tmp_path = PROJECTS_STORE_FILE.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(_projects, ensure_ascii=True), encoding="utf-8")
        tmp_path.replace(PROJECTS_STORE_FILE)
    except Exception as e:
        log.warning(f"Failed to persist projects store: {e}")


def _new_project_id() -> str:
    return f"prj_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"


_load_creative_sessions_from_disk()
_load_projects_store()
