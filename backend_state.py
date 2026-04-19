import asyncio
import json
import logging
import os
import random
import time

from backend_settings import (
    CREATIVE_SESSIONS_FILE,
    CREATIVE_SESSION_PERSISTENCE_ENABLED,
    PROJECTS_STORE_FILE,
    SUPABASE_URL,
    SUPABASE_SERVICE_KEY,
    SUPABASE_ANON_KEY,
)

log = logging.getLogger("nyptid-studio")

_creative_sessions: dict = {}
_creative_sessions_lock = asyncio.Lock()
_projects: dict = {}
_projects_lock = asyncio.Lock()

# Supabase write-through for creative_sessions. RunPod serverless workers
# have per-worker ephemeral disk, so the legacy in-memory + per-worker
# file persistence dropped sessions whenever a worker rotated (cold boot,
# template bump, idle-timeout-kill). Casey's screenshots 2026-04-19
# showed "Creative session not found" mid-image-batch even at
# workersMax=1. Supabase upsert means any worker can pick up any session.
_REMOTE_SESSIONS_ENABLED = bool(
    (os.getenv("CREATIVE_SESSIONS_SUPABASE_ENABLED", "1").lower() in ("1", "true", "yes", "on"))
    and SUPABASE_URL
    and (SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY)
)


def _remote_sessions_key() -> str:
    return SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY or ""


async def _upsert_session_remote(session_id: str, session: dict) -> bool:
    """Write-through a single session to Supabase. Fire-and-forget-safe."""
    if not _REMOTE_SESSIONS_ENABLED or not session_id:
        return False
    try:
        import httpx  # local import to avoid heavy import at module load
        key = _remote_sessions_key()
        payload = {
            "id": session_id,
            "user_id": str(session.get("user_id", "") or ""),
            "data": session,
        }
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{SUPABASE_URL}/rest/v1/creative_sessions",
                headers={
                    "apikey": key,
                    "Authorization": f"Bearer {key}",
                    "Content-Type": "application/json",
                    "Prefer": "resolution=merge-duplicates,return=minimal",
                },
                json=payload,
            )
            if resp.status_code >= 300:
                log.warning(f"creative_sessions upsert {resp.status_code}: {resp.text[:200]}")
                return False
        return True
    except Exception as e:
        log.warning(f"creative_sessions upsert failed (non-fatal): {e}")
        return False


async def _fetch_session_remote(session_id: str) -> dict | None:
    """Pull a session from Supabase on local miss."""
    if not _REMOTE_SESSIONS_ENABLED or not session_id:
        return None
    try:
        import httpx
        key = _remote_sessions_key()
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/creative_sessions",
                headers={"apikey": key, "Authorization": f"Bearer {key}"},
                params={"id": f"eq.{session_id}", "select": "data", "limit": 1},
            )
            if resp.status_code != 200:
                return None
            rows = resp.json() or []
            if not rows:
                return None
            data = rows[0].get("data") if isinstance(rows[0], dict) else None
            if isinstance(data, dict):
                return data
    except Exception as e:
        log.warning(f"creative_sessions fetch failed (non-fatal): {e}")
    return None


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


def _save_creative_sessions_to_disk(remote_session_id: str | None = None):
    """Persist the creative sessions dict to disk and optionally mirror one
    specific session to Supabase. Pass `remote_session_id` to fire a
    fire-and-forget async upsert for just that row — cheaper than
    re-uploading every session on every tiny mutation.
    """
    if not CREATIVE_SESSION_PERSISTENCE_ENABLED:
        return
    try:
        _prune_creative_sessions()
        tmp_path = CREATIVE_SESSIONS_FILE.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(_creative_sessions, ensure_ascii=True), encoding="utf-8")
        tmp_path.replace(CREATIVE_SESSIONS_FILE)
    except Exception as e:
        log.warning(f"Failed to persist creative sessions store: {e}")
    # Fire-and-forget Supabase mirror for the just-modified session, if an
    # event loop is available (we're in an async context like a FastAPI
    # handler). Silent on failure — disk is still primary, Supabase is
    # best-effort cross-worker sync.
    if remote_session_id and _REMOTE_SESSIONS_ENABLED:
        session_obj = _creative_sessions.get(remote_session_id)
        if isinstance(session_obj, dict):
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(_upsert_session_remote(remote_session_id, session_obj))
            except RuntimeError:
                pass  # no running loop (tests / import-time); skip remote sync


async def _get_creative_session(session_id: str):
    """Fetch a creative session; on miss, refresh from disk, then Supabase.

    Async because the Supabase fallback is async. All callers should `await`.
    Sync in-memory hit is the fast path and doesn't block.
    """
    session = _creative_sessions.get(session_id)
    if session is not None:
        return session
    if CREATIVE_SESSION_PERSISTENCE_ENABLED:
        _load_creative_sessions_from_disk()
        session = _creative_sessions.get(session_id)
        if session is not None:
            return session
    # Last resort: Supabase shared state. Populates the local dict for
    # cheap subsequent lookups.
    remote = await _fetch_session_remote(session_id)
    if remote is not None and isinstance(remote, dict):
        _creative_sessions[session_id] = remote
        return remote
    return None


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
