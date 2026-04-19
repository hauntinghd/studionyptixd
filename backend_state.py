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

if not _REMOTE_SESSIONS_ENABLED:
    log.warning(
        "[session] Supabase mirror DISABLED at import "
        f"(url_set={bool(SUPABASE_URL)} service_key_set={bool(SUPABASE_SERVICE_KEY)} "
        f"anon_key_set={bool(SUPABASE_ANON_KEY)}) — cross-worker session loss is expected"
    )


def _remote_sessions_key() -> str:
    return SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY or ""


async def _upsert_session_remote(session_id: str, session: dict) -> bool:
    """Write-through a single session to Supabase. Fire-and-forget-safe."""
    if not _REMOTE_SESSIONS_ENABLED:
        log.warning(f"[session] upsert sid={str(session_id)[:40]} SKIPPED (remote disabled)")
        return False
    if not session_id:
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
                log.warning(
                    f"[session] upsert sid={str(session_id)[:40]} FAILED "
                    f"status={resp.status_code} body={resp.text[:200]}"
                )
                return False
        return True
    except Exception as e:
        log.warning(f"[session] upsert sid={str(session_id)[:40]} EXC {type(e).__name__}: {e}")
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
                log.warning(
                    f"[session] fetch sid={str(session_id)[:40]} FAILED "
                    f"status={resp.status_code} body={resp.text[:200]}"
                )
                return None
            rows = resp.json() or []
            if not rows:
                return None
            data = rows[0].get("data") if isinstance(rows[0], dict) else None
            if isinstance(data, dict):
                return data
    except Exception as e:
        log.warning(f"[session] fetch sid={str(session_id)[:40]} EXC {type(e).__name__}: {e}")
    return None


async def _upsert_session_remote_with_verify(
    session_id: str,
    session: dict,
    attempts: int = 3,
) -> tuple[bool, str]:
    """Upsert with retry + read-after-write verify.

    Returns (ok, error_detail). `ok=True` means either (a) remote persistence is
    intentionally disabled (dev/local, disk is authoritative), or (b) Supabase
    confirmed the row is readable after the write.

    `ok=False` means remote IS enabled but every attempt either failed or the
    row didn't come back on verify — the caller MUST surface this to the user
    because the session will 404 on any cross-worker scene-image call.
    """
    if not _REMOTE_SESSIONS_ENABLED:
        return True, ""
    if not session_id:
        return False, "missing_session_id"
    last_err = "unknown"
    for attempt in range(max(1, int(attempts))):
        ok = await _upsert_session_remote(session_id, session)
        if ok:
            verified = await _fetch_session_remote(session_id)
            if isinstance(verified, dict):
                if attempt > 0:
                    log.info(f"[session] upsert sid={str(session_id)[:40]} succeeded on attempt {attempt+1}")
                return True, ""
            last_err = "upsert_ok_but_verify_missed"
            log.warning(f"[session] sid={str(session_id)[:40]} attempt={attempt+1} {last_err}")
        else:
            last_err = "upsert_failed"
        if attempt < attempts - 1:
            await asyncio.sleep(0.15 * (2 ** attempt))
    return False, last_err


async def _delete_session_remote(session_id: str) -> bool:
    """Remove a session row from Supabase after in-memory deletion."""
    if not _REMOTE_SESSIONS_ENABLED or not session_id:
        return False
    try:
        import httpx
        key = _remote_sessions_key()
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.delete(
                f"{SUPABASE_URL}/rest/v1/creative_sessions",
                headers={"apikey": key, "Authorization": f"Bearer {key}"},
                params={"id": f"eq.{session_id}"},
            )
            if resp.status_code not in (200, 204):
                log.warning(
                    f"[session] delete sid={str(session_id)[:40]} FAILED "
                    f"status={resp.status_code}"
                )
                return False
        return True
    except Exception as e:
        log.warning(f"[session] delete sid={str(session_id)[:40]} EXC {type(e).__name__}: {e}")
        return False


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
    sid_log = str(session_id or "")[:40]
    session = _creative_sessions.get(session_id)
    if session is not None:
        log.debug(f"[session] sid={sid_log} hit=memory")
        return session
    if CREATIVE_SESSION_PERSISTENCE_ENABLED:
        _load_creative_sessions_from_disk()
        session = _creative_sessions.get(session_id)
        if session is not None:
            log.info(f"[session] sid={sid_log} hit=disk")
            return session
    # Last resort: Supabase shared state. Populates the local dict for
    # cheap subsequent lookups.
    remote = await _fetch_session_remote(session_id)
    if remote is not None and isinstance(remote, dict):
        _creative_sessions[session_id] = remote
        log.info(f"[session] sid={sid_log} hit=supabase")
        return remote
    log.warning(
        f"[session] sid={sid_log} hit=NONE "
        f"remote_enabled={_REMOTE_SESSIONS_ENABLED} disk_enabled={CREATIVE_SESSION_PERSISTENCE_ENABLED}"
    )
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
