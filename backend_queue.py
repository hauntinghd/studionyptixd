import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

from backend_settings import (
    JOB_MAX_QUEUE_DEPTH,
    JOB_QUEUE_WORKERS,
    REDIS_QUEUE_ENABLED,
    REDIS_QUEUE_PREFIX,
    REDIS_URL,
    TEMP_DIR,
)

try:
    from redis.asyncio import Redis
except Exception:
    Redis = None


class QueueFullError(RuntimeError):
    pass


class QueueStateUnavailableError(RuntimeError):
    pass


_job_queue: asyncio.PriorityQueue | None = None
_queued_job_meta: dict[str, tuple[int, int]] = {}
_job_workers_started = False
_job_seq = 0
_jobs_ref: dict[str, dict[str, Any]] | None = None
_log = logging.getLogger("nyptid-studio")
_redis_client: Redis | None = None
_redis_healthy = True
_job_state_file_lock = asyncio.Lock()
_JOB_STATE_DIR = TEMP_DIR / "job_state_cache"
_JOB_STATE_DIR.mkdir(parents=True, exist_ok=True)

# Background tasks we've spawned from enqueue_generation_job. Python's asyncio
# garbage-collects tasks that nothing references — silently cancelling the
# coroutine on the next yield. Holding refs here keeps them alive until they
# finish. Tasks self-remove via the done callback.
_background_tasks: set[asyncio.Task] = set()

# Fly runs the Redis consumer inside the one Uvicorn process so the API and
# production pipelines share the same file-backed sessions, credit ledgers,
# and mounted volume.  Keeping explicit runtime state lets /api/health prove
# that the consumer task is alive instead of merely reporting that Redis is
# configured.
_embedded_worker_task: asyncio.Task | None = None
_embedded_worker_stop: asyncio.Event | None = None
_embedded_worker_started_at = 0.0
_embedded_worker_last_poll_at = 0.0
_embedded_worker_last_error = ""
_embedded_worker_active_job = ""
_embedded_worker_processed = 0
_embedded_worker_recovered = 0

_QUEUE_RECEIPT_FIELD = "__studio_queue_receipt"
_SAFE_JOB_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_PRODUCTION_QUEUE_TASK_NAMES = frozenset(
    {
        "run_generation_pipeline",
        "_run_creative_pipeline",
        "_run_longform_pipeline",
        "run_clone_pipeline",
        "_run_cliplab_pipeline",
    }
)
_TERMINAL_JOB_STATUSES = {
    "cancelled",
    "canceled",
    "complete",
    "completed",
    "error",
    "failed",
    "rendered",
}


# Capacity admission and LPUSH must be one Redis operation.  A separate LLEN
# followed by LPUSH lets concurrent API workers all observe the same free slot
# and overfill the queue by an arbitrary amount.
_REDIS_ENQUEUE_IF_CAPACITY_LUA = """
if redis.call('EXISTS', KEYS[4]) == 1 then
    return -2
end
local total = 0
for index = 1, 3 do
    total = total + redis.call('LLEN', KEYS[index])
end
local maximum = tonumber(ARGV[1])
if total >= maximum then
    return -1
end
local target = tonumber(ARGV[2])
redis.call('LPUSH', KEYS[target], ARGV[3])
redis.call('SET', KEYS[4], '1', 'EX', tonumber(ARGV[4]))
return total + 1
"""

# Claiming must be atomic: RPOPLPUSH moves the oldest item from the selected
# priority lane into an in-flight list before it is returned to Python.  If the
# process is terminated at any point after this operation, startup recovery can
# put the unacknowledged item back on its original priority lane.
_REDIS_CLAIM_HIGHEST_PRIORITY_LUA = """
for index = 1, 3 do
    local raw = redis.call('RPOPLPUSH', KEYS[index], KEYS[4])
    if raw then
        return raw
    end
end
return false
"""

_REDIS_RECOVER_INFLIGHT_LUA = """
local recovered = 0
while true do
    local raw = redis.call('LPOP', KEYS[4])
    if not raw then
        break
    end
    local ok, payload = pcall(cjson.decode, raw)
    if ok and type(payload) == 'table' then
        local priority = tonumber(payload['priority']) or 2
        if priority < 0 then priority = 0 end
        if priority > 2 then priority = 2 end
        redis.call('RPUSH', KEYS[priority + 1], raw)
        recovered = recovered + 1
    else
        redis.call('LPUSH', KEYS[5], raw)
        redis.call('LTRIM', KEYS[5], 0, 999)
    end
end
return recovered
"""

_REDIS_REJECT_INFLIGHT_LUA = """
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed > 0 then
    redis.call('LPUSH', KEYS[2], ARGV[2])
    redis.call('LTRIM', KEYS[2], 0, 999)
    if KEYS[3] ~= '' then
        redis.call('DEL', KEYS[3])
    end
end
return removed
"""

_REDIS_ACK_INFLIGHT_LUA = """
local removed = redis.call('LREM', KEYS[1], 1, ARGV[1])
if removed > 0 then
    redis.call('DEL', KEYS[2])
end
return removed
"""

_REDIS_REQUEUE_INFLIGHT_LUA = """
local removed = redis.call('LREM', KEYS[4], 1, ARGV[1])
if removed > 0 then
    local priority = tonumber(ARGV[2]) or 2
    if priority < 0 then priority = 0 end
    if priority > 2 then priority = 2 end
    redis.call('LPUSH', KEYS[priority + 1], ARGV[1])
end
return removed
"""


def _job_state_file(job_id: str) -> Path:
    if not _SAFE_JOB_ID_RE.fullmatch(str(job_id or "")):
        raise ValueError("Invalid production job id")
    return _JOB_STATE_DIR / f"{job_id}.json"


def _redis_enabled() -> bool:
    return bool(REDIS_QUEUE_ENABLED and REDIS_URL and Redis is not None)


def embedded_worker_enabled() -> bool:
    return str(os.getenv("RUN_EMBEDDED_WORKER", "false") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _embedded_worker_shutdown_grace_sec() -> float:
    try:
        return min(
            115.0,
            max(0.1, float(os.getenv("EMBEDDED_WORKER_SHUTDOWN_GRACE_SEC", "110") or 110)),
        )
    except (TypeError, ValueError):
        return 110.0


def _queue_key(priority: int) -> str:
    return f"{REDIS_QUEUE_PREFIX}:queue:p{priority}"


def _processing_key() -> str:
    return f"{REDIS_QUEUE_PREFIX}:queue:processing"


def _dead_letter_key() -> str:
    return f"{REDIS_QUEUE_PREFIX}:queue:dead-letter"


def _job_key(job_id: str) -> str:
    return f"{REDIS_QUEUE_PREFIX}:job:{job_id}"


def _queue_admission_key(job_id: str) -> str:
    return f"{REDIS_QUEUE_PREFIX}:queue:admitted:{job_id}"


async def _get_redis() -> Redis | None:
    global _redis_client
    if not _redis_enabled():
        return None
    if _redis_client is None:
        try:
            _redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
        except Exception:
            return None
    return _redis_client


async def _redis_available() -> bool:
    global _redis_healthy
    if not _redis_enabled():
        return False
    redis = await _get_redis()
    if redis is None:
        return False
    try:
        await redis.ping()
        _redis_healthy = True
        return True
    except Exception:
        _redis_healthy = False
        return False


def init_queue_runtime(jobs_ref: dict[str, dict[str, Any]], logger: logging.Logger | None = None):
    global _jobs_ref, _log
    _jobs_ref = jobs_ref
    if logger is not None:
        _log = logger


def _get_job_queue() -> asyncio.PriorityQueue:
    global _job_queue
    if _job_queue is None:
        _job_queue = asyncio.PriorityQueue()
    return _job_queue


def _plan_queue_priority(plan: str) -> int:
    if plan in ("creator", "pro", "demo_pro", "admin"):
        return 0
    if plan == "starter":
        return 1
    return 2


def _update_queue_positions():
    if _jobs_ref is None:
        return
    ordered = sorted(_queued_job_meta.items(), key=lambda item: (item[1][0], item[1][1]))
    total = len(ordered)
    for i, (qjid, _meta) in enumerate(ordered):
        if qjid in _jobs_ref:
            _jobs_ref[qjid]["queue_position"] = i + 1
            _jobs_ref[qjid]["queue_total"] = total


async def _job_queue_worker(worker_idx: int):
    q = _get_job_queue()
    while True:
        _priority, _seq, job_id, coro_func, args = await q.get()
        try:
            _queued_job_meta.pop(job_id, None)
            _update_queue_positions()
            await coro_func(*args)
        except Exception as e:
            _log.error(f"[{job_id}] Queue worker {worker_idx} error: {e}", exc_info=True)
            if _jobs_ref is not None and job_id in _jobs_ref:
                _jobs_ref[job_id]["status"] = "error"
                _jobs_ref[job_id]["error"] = str(e)
        finally:
            q.task_done()


async def _ensure_job_workers():
    global _job_workers_started
    if _job_workers_started:
        return
    _job_workers_started = True
    for i in range(JOB_QUEUE_WORKERS):
        asyncio.get_event_loop().create_task(_job_queue_worker(i + 1))


async def _persist_job_state_supabase(job_id: str, job_state: dict[str, Any]) -> bool:
    """Write-through job state to Supabase `jobs` table so other workers can read it.
    Required for cross-worker state when workersMax > 1 — the local-disk path only
    covers the same worker that created the job."""
    from backend_settings import SUPABASE_URL, SUPABASE_SERVICE_KEY, SUPABASE_ANON_KEY
    key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not (SUPABASE_URL and key):
        return False
    try:
        import httpx
        # Map our in-memory job dict onto the existing jobs table schema.
        # Full state goes in `payload` (jsonb); top-level columns mirror the
        # hot fields so the status endpoint can return them fast without
        # parsing the blob. user_id is NOT NULL on the table.
        row = {
            "id": job_id,
            "user_id": str(job_state.get("user_id", "") or "unknown"),
            "status": str(job_state.get("status", "") or ""),
            "payload": job_state,
            "total_chunks": int(job_state.get("total_scenes", 0) or 0),
            "done_chunks": int(job_state.get("progress", 0) or 0),
            "error": str(job_state.get("error", "") or "") or None,
        }
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(
                f"{SUPABASE_URL}/rest/v1/jobs",
                headers={
                    "apikey": key,
                    "Authorization": f"Bearer {key}",
                    "Content-Type": "application/json",
                    "Prefer": "resolution=merge-duplicates,return=minimal",
                },
                json=row,
            )
            if resp.status_code >= 300:
                body = resp.text[:200]
                hint = ""
                if resp.status_code in (400, 409) and "user_id" in body and ("integer" in body.lower() or "type" in body.lower()):
                    hint = " -- SCHEMA MISMATCH: `jobs.user_id` must be text (not integer). Apply migrations/2026-04-20_jobs.sql."
                _log.error(f"Supabase job persist for {job_id} FAILED status={resp.status_code} body={body}{hint}")
                return False
        return True
    except Exception as e:
        _log.warning(f"Supabase job persist EXC for {job_id}: {type(e).__name__}: {e}")
        return False


async def _fetch_job_state_supabase(job_id: str) -> dict[str, Any] | None:
    """Cross-worker fallback: read job state from Supabase."""
    from backend_settings import SUPABASE_URL, SUPABASE_SERVICE_KEY, SUPABASE_ANON_KEY
    key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY
    if not (SUPABASE_URL and key):
        return None
    try:
        import httpx
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/jobs",
                headers={"apikey": key, "Authorization": f"Bearer {key}"},
                params={"id": f"eq.{job_id}", "select": "payload", "limit": 1},
            )
            if resp.status_code != 200:
                return None
            rows = resp.json() or []
            if not rows:
                return None
            payload = rows[0].get("payload") if isinstance(rows[0], dict) else None
            if isinstance(payload, dict):
                return payload
    except Exception as e:
        _log.warning(f"Supabase job fetch EXC for {job_id}: {type(e).__name__}: {e}")
    return None


async def _persist_job_state_primary(
    job_id: str,
    job_state: dict[str, Any],
) -> tuple[dict[str, Any], bool, bool]:
    """Write an immutable snapshot to Redis and the attached volume."""
    global _redis_healthy

    if not _SAFE_JOB_ID_RE.fullmatch(str(job_id or "")):
        raise ValueError("Invalid production job id")
    payload = json.dumps(job_state, ensure_ascii=True)
    snapshot = json.loads(payload)
    redis_ok = False
    disk_ok = False
    redis = await _get_redis()
    if redis is not None:
        try:
            await redis.set(_job_key(job_id), payload, ex=60 * 60 * 6)
            redis_ok = True
            _redis_healthy = True
        except Exception as e:
            _redis_healthy = False
            _log.warning(f"Redis job persistence failed for {job_id}: {e}")
    try:
        async with _job_state_file_lock:
            path = _job_state_file(job_id)
            tmp = path.with_suffix(".tmp")
            tmp.write_text(payload, encoding="utf-8")
            tmp.replace(path)
            disk_ok = True
    except Exception as e:
        _log.warning(f"Local job persistence failed for {job_id}: {e}")
    return snapshot, redis_ok, disk_ok


async def persist_job_state(job_id: str, job_state: dict[str, Any]) -> bool:
    snapshot, redis_ok, disk_ok = await _persist_job_state_primary(job_id, job_state)
    # Cross-worker write-through. Fire-and-forget on a separate task so this
    # call stays fast — the status endpoint reads back from Supabase on miss.
    # STRONG REF REQUIRED: the Task returned by create_task() must be held
    # somewhere reachable, otherwise Python's GC can collect it on the next
    # await inside _persist_job_state_supabase (the httpx POST) and silently
    # cancel the coroutine. This is the exact same trap that caused the main
    # pipeline to hang at scene 1 before the strong-ref fix in
    # enqueue_generation_job. Symptom here: `jobs` table stays empty, cross-
    # worker status polls 404 with "Job not found" after a completed render.
    try:
        loop = asyncio.get_running_loop()
        task = loop.create_task(_persist_job_state_supabase(job_id, snapshot))
        _background_tasks.add(task)
        task.add_done_callback(_background_tasks.discard)
    except RuntimeError:
        pass
    return bool(redis_ok or disk_ok)


def schedule_persist_job_state(job_id: str, job_state: dict[str, Any]) -> None:
    """Fire-and-forget wrapper around persist_job_state that holds a strong
    reference to the scheduled Task.

    Callers that use `asyncio.create_task(persist_job_state(...))` directly
    and discard the return value hit the same asyncio GC trap that empties
    the Supabase `jobs` table silently: Python collects the Task on the
    first await inside persist_job_state (the Redis SET / disk write /
    httpx POST), cancelling the whole chain. Every caller that needs
    non-blocking persistence should route through this helper.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    task = loop.create_task(persist_job_state(job_id, job_state))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def persist_job_state_awaited(job_id: str, job_state: dict[str, Any]) -> bool:
    """Fully-awaited persist: Redis + disk + Supabase all complete before
    returning. Use this from endpoints where the caller will start polling
    status immediately (e.g. /api/creative/finalize) — if the row isn't in
    Supabase before the HTTP response is sent, the first status poll that
    routes to a different worker 404s with "Job not found".

    Later in-pipeline updates (progress ticks, stage changes) can stay
    fire-and-forget via schedule_persist_job_state since the row already
    exists and upserts are idempotent.

    Returns True if the Supabase write succeeded, False otherwise.
    Redis + disk failures are logged but don't fail the call.
    """
    snapshot, redis_ok, disk_ok = await _persist_job_state_primary(job_id, job_state)
    try:
        ok = await _persist_job_state_supabase(job_id, snapshot)
        if not (redis_ok or disk_ok or ok):
            _log.error(f"[{job_id}] AWAITED Supabase persist returned False — status polls from other workers will 404")
        durable = bool(redis_ok or disk_ok or ok)
        if not durable:
            _log.error("[%s] All durable job-state persistence paths failed", job_id)
        return durable
    except Exception as e:
        _log.error(f"[{job_id}] AWAITED Supabase persist EXC: {type(e).__name__}: {e}")
        durable = bool(redis_ok or disk_ok)
        if not durable:
            _log.error("[%s] All durable job-state persistence paths failed", job_id)
        return durable


async def persist_terminal_job_state(job_id: str, job_state: dict[str, Any]) -> bool:
    """Confirm a terminal state is durable before removing its queue receipt."""
    status = str((job_state or {}).get("status", "") or "").strip().lower()
    if status not in _TERMINAL_JOB_STATUSES:
        _log.error("[%s] Refusing to acknowledge non-terminal queue state", job_id)
        return False
    return await persist_job_state_awaited(job_id, job_state)


async def get_persisted_job_state(job_id: str) -> dict[str, Any] | None:
    redis = await _get_redis()
    raw = None
    if redis is not None:
        try:
            raw = await redis.get(_job_key(job_id))
        except Exception as e:
            _log.warning(f"Redis job load failed for {job_id}: {e}")
    if not raw:
        try:
            path = _job_state_file(job_id)
            if path.exists():
                raw = path.read_text(encoding="utf-8")
        except Exception as e:
            _log.warning(f"Local job load failed for {job_id}: {e}")
            raw = None
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    # Supabase fallback — works across workers. This is the case that was
    # failing before: worker A created the job + persisted to its local disk,
    # but a subsequent /api/status/{job_id} poll routed to worker B whose
    # local disk has no file for this job_id. Without this path, worker B
    # returned 404 "Job not found" even though worker A was still rendering.
    remote = await _fetch_job_state_supabase(job_id)
    if isinstance(remote, dict):
        return remote
    return None


async def enqueue_generation_job(
    job_id: str,
    plan: str,
    coro_func: Callable[..., Awaitable[Any]],
    args: tuple[Any, ...],
):
    task_name = str(getattr(coro_func, "__name__", "") or "").strip()
    if _redis_enabled() and task_name not in _PRODUCTION_QUEUE_TASK_NAMES:
        raise ValueError(f"Unsupported production queue task: {task_name or 'unnamed'}")
    if not _SAFE_JOB_ID_RE.fullmatch(str(job_id or "")):
        raise ValueError("Invalid production job id")

    # A Fly deployment configured for the embedded consumer must never fall
    # back to a detached in-process task when Redis is missing or its client
    # failed to import.  That would bypass queue depth, acknowledgement, and
    # restart recovery precisely when production is degraded.
    if embedded_worker_enabled() and not _redis_enabled():
        raise QueueFullError("Production queue is temporarily unavailable. Please retry shortly.")

    if _redis_enabled():
        if not await _redis_available():
            raise QueueFullError("Production queue is temporarily unavailable. Please retry shortly.")
        try:
            redis = await _get_redis()
            if redis is not None:
                priority = _plan_queue_priority(plan)
                if _jobs_ref is None or job_id not in _jobs_ref:
                    raise QueueFullError("Production job state is unavailable. Please retry shortly.")
                _jobs_ref[job_id]["queue_priority"] = priority
                _jobs_ref[job_id]["queue_mode"] = "redis"
                if not await persist_job_state(job_id, _jobs_ref[job_id]):
                    raise QueueFullError("Production job state could not be persisted. Please retry shortly.")
                payload = {
                    "job_id": job_id,
                    "task_name": task_name,
                    "args": list(args),
                    "priority": priority,
                    "queued_at": time.time(),
                }
                admitted_depth = await redis.eval(
                    _REDIS_ENQUEUE_IF_CAPACITY_LUA,
                    4,
                    _queue_key(0),
                    _queue_key(1),
                    _queue_key(2),
                    _queue_admission_key(job_id),
                    JOB_MAX_QUEUE_DEPTH,
                    priority + 1,
                    json.dumps(payload, ensure_ascii=True),
                    60 * 60 * 24 * 7,
                )
                if int(admitted_depth or -1) == -2:
                    # Retrying the same job id is idempotent; the existing
                    # pending/in-flight receipt remains authoritative.
                    return
                if int(admitted_depth or -1) < 0:
                    raise QueueFullError(f"Queue is full ({JOB_MAX_QUEUE_DEPTH}). Please retry shortly.")
                return
        except QueueFullError:
            raise
        except Exception as e:
            # A Redis timeout can be ambiguous: the atomic script may already
            # have committed the job even when the response was lost.  Never
            # start an in-process duplicate after that boundary.
            _log.error(f"Redis enqueue failed closed: {e}")
            raise QueueFullError("Production queue is temporarily unavailable. Please retry shortly.") from e

    # Inprocess path: production runs uvicorn under RunPod serverless workers
    # which keep a SINGLE long-lived event loop across requests for the worker's
    # idleTimeout window (~300s between requests, but the loop itself never
    # tears down between requests on a warm worker). Spawning the pipeline as
    # asyncio.create_task() lets the HTTP handler return immediately while the
    # render continues on the same loop. The frontend polls /api/status/{job_id}
    # every 2s for progress.
    #
    # Why we ABANDONED the prior inline-await design:
    #   The old code did `await coro_func(*args)` here so the request blocked
    #   until the full render completed. That kept the loop alive (good for
    #   asyncio task survival) but the HTTP response could take 3-5 minutes for
    #   animation+TTS+compose. Cloudflare's proxy in front of
    #   api-studio.nyptidindustries.com (CF-RAY confirmed) returns 504 at ~100s
    #   — the client gets a timeout error, the project never appears in the
    #   Projects tab, and there is no way to recover.
    #
    # TestClient note (the original reason inline-await existed):
    #   FastAPI's TestClient creates a fresh event loop per request and tears
    #   it down on response. asyncio.create_task() under TestClient WILL get
    #   cancelled when the loop closes. That breaks tests but not production —
    #   tests that exercise this path should `await` the pipeline directly or
    #   stub enqueue.
    priority = _plan_queue_priority(plan)
    if _jobs_ref is not None and job_id in _jobs_ref:
        _jobs_ref[job_id]["queue_priority"] = priority
        _jobs_ref[job_id]["queue_mode"] = "inprocess"
        _jobs_ref[job_id]["queue_position"] = 1
        _jobs_ref[job_id]["queue_total"] = 1

    async def _runner():
        try:
            await coro_func(*args)
        except Exception as e:
            _log.error(f"[{job_id}] Background pipeline error: {e}", exc_info=True)
            if _jobs_ref is not None and job_id in _jobs_ref:
                _jobs_ref[job_id]["status"] = "error"
                _jobs_ref[job_id]["error"] = str(e)
                try:
                    await persist_job_state(job_id, _jobs_ref[job_id])
                except Exception:
                    pass

    # CRITICAL: hold a strong reference to the task. Without this, Python's
    # asyncio can garbage-collect the Task object on the next yield point
    # (e.g. the first `await` inside _run_creative_pipeline that hits I/O),
    # which silently cancels the coroutine. Empirically seen: the render would
    # fire `animation_start` for scene 1, then freeze forever on `await
    # animate_scene(...)` because the _runner() Task got GC'd while awaiting
    # fal's queue poll. The set+discard pattern keeps the Task alive until it
    # actually finishes.
    task = asyncio.create_task(_runner())
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def dequeue_generation_job() -> dict[str, Any] | None:
    global _redis_healthy
    redis = await _get_redis()
    if redis is None:
        return None
    # Highest priority first (p0 -> p1 -> p2). The Lua claim atomically moves
    # the raw payload to the processing list, so cancellation or a machine
    # restart after dequeue cannot silently discard the job.
    try:
        raw = await redis.eval(
            _REDIS_CLAIM_HIGHEST_PRIORITY_LUA,
            4,
            _queue_key(0),
            _queue_key(1),
            _queue_key(2),
            _processing_key(),
        )
        _redis_healthy = True
    except Exception as e:
        _redis_healthy = False
        _log.warning(f"Redis dequeue failed: {e}")
        return None
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        while not await _reject_raw_inflight(str(raw), "invalid_json"):
            await asyncio.sleep(0.5)
        return None
    if not isinstance(payload, dict):
        while not await _reject_raw_inflight(str(raw), "payload_not_object"):
            await asyncio.sleep(0.5)
        return None
    payload[_QUEUE_RECEIPT_FIELD] = str(raw)
    return payload


async def acknowledge_generation_job(payload: dict[str, Any]) -> bool:
    """Remove a successfully persisted claim from the in-flight list."""
    global _redis_healthy
    raw = str(payload.get(_QUEUE_RECEIPT_FIELD, "") or "")
    if not raw:
        return False
    redis = await _get_redis()
    if redis is None:
        return False
    try:
        job_id = str(payload.get("job_id", "") or "").strip()
        if not _SAFE_JOB_ID_RE.fullmatch(job_id):
            return False
        removed = await redis.eval(
            _REDIS_ACK_INFLIGHT_LUA,
            2,
            _processing_key(),
            _queue_admission_key(job_id),
            raw,
        )
        _redis_healthy = True
        return int(removed or 0) > 0
    except Exception as exc:
        _redis_healthy = False
        _log.warning("Redis acknowledgement failed for %s: %s", payload.get("job_id", ""), exc)
        return False


async def _reject_raw_inflight(raw: str, reason: str, *, job_id: str = "") -> bool:
    """Move malformed/unsupported work to a bounded dead-letter list."""
    global _redis_healthy
    redis = await _get_redis()
    if redis is None:
        return False
    record = json.dumps(
        {"raw": raw, "reason": str(reason or "rejected")[:160], "rejected_at": time.time()},
        ensure_ascii=True,
    )
    try:
        removed = await redis.eval(
            _REDIS_REJECT_INFLIGHT_LUA,
            3,
            _processing_key(),
            _dead_letter_key(),
            _queue_admission_key(job_id) if _SAFE_JOB_ID_RE.fullmatch(job_id) else "",
            raw,
            record,
        )
        _redis_healthy = True
        return int(removed or 0) > 0
    except Exception as exc:
        _redis_healthy = False
        _log.error("Redis dead-letter transfer failed: %s", exc)
        return False


async def reject_generation_job(payload: dict[str, Any], reason: str) -> bool:
    raw = str(payload.get(_QUEUE_RECEIPT_FIELD, "") or "")
    if not raw:
        return False
    return await _reject_raw_inflight(
        raw,
        reason,
        job_id=str(payload.get("job_id", "") or "").strip(),
    )


async def requeue_generation_job(payload: dict[str, Any]) -> bool:
    """Atomically return a claim to its priority lane without clearing dedupe."""
    global _redis_healthy
    raw = str(payload.get(_QUEUE_RECEIPT_FIELD, "") or "")
    if not raw:
        return False
    try:
        priority = min(2, max(0, int(payload.get("priority", 2) or 2)))
    except (TypeError, ValueError):
        priority = 2
    redis = await _get_redis()
    if redis is None:
        return False
    try:
        removed = await redis.eval(
            _REDIS_REQUEUE_INFLIGHT_LUA,
            4,
            _queue_key(0),
            _queue_key(1),
            _queue_key(2),
            _processing_key(),
            raw,
            priority,
        )
        _redis_healthy = True
        return int(removed or 0) > 0
    except Exception as exc:
        _redis_healthy = False
        _log.error("Redis claim requeue failed: %s", exc)
        return False


async def recover_inflight_generation_jobs() -> int:
    """Requeue every unacknowledged claim while preserving its priority/FIFO order."""
    global _redis_healthy
    redis = await _get_redis()
    if redis is None:
        return 0
    try:
        recovered = await redis.eval(
            _REDIS_RECOVER_INFLIGHT_LUA,
            5,
            _queue_key(0),
            _queue_key(1),
            _queue_key(2),
            _processing_key(),
            _dead_letter_key(),
        )
        _redis_healthy = True
        return max(0, int(recovered or 0))
    except Exception as exc:
        _redis_healthy = False
        _log.error("Redis in-flight recovery failed: %s", exc)
        raise RuntimeError("Unable to recover the production queue") from exc


async def _reject_generation_claim_reliably(
    payload: dict[str, Any],
    reason: str,
    *,
    stop_event: asyncio.Event | None,
) -> bool:
    """Keep a rejected receipt in-flight until Redis confirms dead-lettering."""
    global _embedded_worker_last_error
    while True:
        if await reject_generation_job(payload, reason):
            return True
        _embedded_worker_last_error = "queue_dead_letter_pending"
        if stop_event is not None and stop_event.is_set():
            return False
        await asyncio.sleep(0.5)


async def _requeue_generation_claim_reliably(
    payload: dict[str, Any],
    *,
    stop_event: asyncio.Event | None,
) -> bool:
    global _embedded_worker_last_error
    while True:
        if await requeue_generation_job(payload):
            return True
        _embedded_worker_last_error = "queue_requeue_pending"
        if stop_event is not None and stop_event.is_set():
            return False
        await asyncio.sleep(0.5)


async def _persist_and_ack_terminal_claim(
    payload: dict[str, Any],
    job_id: str,
    job_state: dict[str, Any],
    *,
    stop_event: asyncio.Event | None,
) -> bool:
    """Never remove an in-flight receipt before terminal durability is proven."""
    global _embedded_worker_last_error

    durable = False
    while True:
        if not durable:
            durable = await persist_terminal_job_state(job_id, job_state)
        if durable and await acknowledge_generation_job(payload):
            return True
        _embedded_worker_last_error = (
            "queue_ack_pending" if durable else "terminal_persistence_pending"
        )
        if stop_event is not None and stop_event.is_set():
            return False
        await asyncio.sleep(0.5)


async def run_generation_consumer(
    task_map: dict[str, Callable[..., Awaitable[Any]]],
    *,
    stop_event: asyncio.Event | None = None,
    recover_inflight: bool = True,
) -> None:
    """Consume Redis generation work until cancelled or explicitly stopped.

    The queue is intentionally limited to concrete render functions supplied
    by the application. Planning and conversation have no task-map entry and
    therefore cannot execute through this production queue.
    """
    global _embedded_worker_active_job
    global _embedded_worker_last_error
    global _embedded_worker_last_poll_at
    global _embedded_worker_processed
    global _embedded_worker_recovered

    if not _redis_enabled():
        raise RuntimeError("Redis production queue is not configured")
    if _jobs_ref is None:
        raise RuntimeError("Queue runtime has no job-state registry")
    if recover_inflight:
        recovered = await recover_inflight_generation_jobs()
        _embedded_worker_recovered += recovered
        if recovered:
            _log.warning("Recovered %s unacknowledged production job(s)", recovered)

    while stop_event is None or not stop_event.is_set():
        _embedded_worker_last_poll_at = time.time()
        payload = await dequeue_generation_job()
        if payload is None:
            await asyncio.sleep(0.25)
            continue

        job_id = str(payload.get("job_id", "") or "").strip()
        task_name = str(payload.get("task_name", "") or "").strip()
        raw_args = payload.get("args", [])
        args = tuple(raw_args) if isinstance(raw_args, list) else ()
        task_func = task_map.get(task_name)
        valid_job_id = bool(_SAFE_JOB_ID_RE.fullmatch(job_id))
        task_allowed = task_name in _PRODUCTION_QUEUE_TASK_NAMES
        if not valid_job_id or task_func is None or not task_allowed or not isinstance(raw_args, list):
            if not valid_job_id:
                reason = "invalid_job_id"
            elif not isinstance(raw_args, list):
                reason = "invalid_args"
            else:
                reason = f"unsupported_task:{task_name[:100]}"
            _log.error("Rejecting production queue payload: %s", reason)
            await _reject_generation_claim_reliably(payload, reason, stop_event=stop_event)
            continue

        _embedded_worker_active_job = job_id
        try:
            seed = await get_persisted_job_state(job_id)
            seed_status = str((seed or {}).get("status", "") or "").strip().lower()
            if seed_status in _TERMINAL_JOB_STATUSES:
                # An acknowledgement can fail after the pipeline's terminal
                # state was durably written. On recovery, acknowledge that
                # receipt without repeating billable provider work.
                if await _persist_and_ack_terminal_claim(
                    payload,
                    job_id,
                    seed or {},
                    stop_event=stop_event,
                ):
                    _embedded_worker_processed += 1
                    _embedded_worker_last_error = ""
                continue

            if seed:
                _jobs_ref[job_id] = seed
            elif job_id not in _jobs_ref:
                raise QueueStateUnavailableError("Durable production job state is unavailable")
            _jobs_ref[job_id]["status"] = "processing"
            _jobs_ref[job_id]["queue_mode"] = "redis"
            _jobs_ref[job_id]["queue_claimed_at"] = time.time()
            if not await persist_job_state(job_id, _jobs_ref[job_id]):
                raise QueueStateUnavailableError("Processing job state could not be persisted")

            await task_func(*args)
            terminal_state = _jobs_ref.get(job_id, {})
            terminal_status = str(terminal_state.get("status", "") or "").strip().lower()
            if terminal_status not in _TERMINAL_JOB_STATUSES:
                raise RuntimeError("Production pipeline returned without a terminal job state")
            if await _persist_and_ack_terminal_claim(
                payload,
                job_id,
                terminal_state,
                stop_event=stop_event,
            ):
                _embedded_worker_processed += 1
                _embedded_worker_last_error = ""
        except asyncio.CancelledError:
            # Do not acknowledge. The raw payload is still in Redis processing
            # and will be recovered on the next process startup.
            raise
        except QueueStateUnavailableError as exc:
            _embedded_worker_last_error = "job_state_unavailable"
            _log.error("[%s] Queue claim deferred: %s", job_id, exc)
            await _requeue_generation_claim_reliably(payload, stop_event=stop_event)
            if stop_event is None or not stop_event.is_set():
                await asyncio.sleep(0.5)
        except Exception as exc:
            _embedded_worker_last_error = f"{type(exc).__name__}: {exc}"[:300]
            _log.error("[%s] Queue consumer execution failed: %s", job_id, exc, exc_info=True)
            if _jobs_ref is not None:
                _jobs_ref.setdefault(job_id, {})
                _jobs_ref[job_id]["status"] = "error"
                _jobs_ref[job_id]["error"] = str(exc)
                if await _persist_and_ack_terminal_claim(
                    payload,
                    job_id,
                    _jobs_ref[job_id],
                    stop_event=stop_event,
                ):
                    _embedded_worker_processed += 1
        finally:
            _embedded_worker_active_job = ""


def get_queue_runtime_health() -> dict[str, Any]:
    task = _embedded_worker_task
    required = embedded_worker_enabled()
    running = bool(task is not None and not task.done())
    redis_ready = bool(_redis_enabled() and _redis_healthy)
    now = time.time()
    return {
        "required": required,
        "running": running,
        "ready": bool((not required) or (running and redis_ready)),
        "redis_configured": _redis_enabled(),
        "redis_healthy": bool(_redis_healthy) if _redis_enabled() else False,
        "workers": 1 if running else 0,
        # The health endpoint is public; expose activity without leaking a
        # creator's private job identifier.
        "active": bool(_embedded_worker_active_job),
        "processed_jobs": int(_embedded_worker_processed),
        "recovered_jobs": int(_embedded_worker_recovered),
        "started_ago_sec": int(max(0.0, now - _embedded_worker_started_at)) if _embedded_worker_started_at else -1,
        "last_poll_ago_sec": int(max(0.0, now - _embedded_worker_last_poll_at)) if _embedded_worker_last_poll_at else -1,
        # Never expose provider errors, prompts, paths, or user content from a
        # failed job on the public health endpoint.
        "has_error": bool(_embedded_worker_last_error),
        "error_code": "queue_consumer_error" if _embedded_worker_last_error else "",
    }


async def start_embedded_generation_worker(
    task_map: dict[str, Callable[..., Awaitable[Any]]],
) -> None:
    global _embedded_worker_last_error
    global _embedded_worker_started_at
    global _embedded_worker_stop
    global _embedded_worker_task

    if not embedded_worker_enabled():
        return
    if JOB_QUEUE_WORKERS != 1:
        raise RuntimeError("Embedded production queue requires JOB_QUEUE_WORKERS=1")
    if not _redis_enabled() or not await _redis_available():
        raise RuntimeError("Embedded production queue requires a healthy Redis connection")
    if _embedded_worker_task is not None and not _embedded_worker_task.done():
        return

    recovered = await recover_inflight_generation_jobs()
    global _embedded_worker_recovered
    _embedded_worker_recovered += recovered
    _embedded_worker_stop = asyncio.Event()
    _embedded_worker_started_at = time.time()
    _embedded_worker_last_error = ""
    _embedded_worker_task = asyncio.create_task(
        run_generation_consumer(task_map, stop_event=_embedded_worker_stop, recover_inflight=False),
        name="studio-redis-production-consumer",
    )
    await asyncio.sleep(0)
    if _embedded_worker_task.done():
        try:
            _embedded_worker_task.result()
        except Exception as exc:
            _embedded_worker_last_error = f"{type(exc).__name__}: {exc}"[:300]
            raise RuntimeError("Embedded production queue consumer failed to start") from exc
        raise RuntimeError("Embedded production queue consumer stopped during startup")
    _log.info("Embedded Redis production consumer started (recovered=%s)", recovered)


async def stop_embedded_generation_worker() -> None:
    global _embedded_worker_active_job
    global _embedded_worker_stop
    global _embedded_worker_task
    global _redis_client

    task = _embedded_worker_task
    if _embedded_worker_stop is not None:
        _embedded_worker_stop.set()
    if task is not None and not task.done():
        try:
            # Stop claiming new work, but let an active render finish inside
            # Fly's SIGTERM window. Immediate cancellation can repeat an
            # already-paid provider call after startup recovery.
            await asyncio.wait_for(
                asyncio.shield(task),
                timeout=_embedded_worker_shutdown_grace_sec(),
            )
        except asyncio.TimeoutError:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    _embedded_worker_task = None
    _embedded_worker_stop = None
    _embedded_worker_active_job = ""
    redis = _redis_client
    _redis_client = None
    if redis is not None:
        try:
            await redis.aclose()
        except Exception:
            pass
    _log.info("Embedded Redis production consumer stopped")


async def get_queue_depth() -> int:
    if _redis_enabled() and await _redis_available():
        redis = await _get_redis()
        if redis is None:
            return len(_queued_job_meta)
        try:
            p0, p1, p2 = await asyncio.gather(
                redis.llen(_queue_key(0)),
                redis.llen(_queue_key(1)),
                redis.llen(_queue_key(2)),
            )
            return int(p0 or 0) + int(p1 or 0) + int(p2 or 0)
        except Exception as e:
            _log.warning(f"Redis queue depth read failed: {e}")
            return len(_queued_job_meta)
    return len(_queued_job_meta)


def get_queue_workers() -> int:
    return JOB_QUEUE_WORKERS


def get_queue_max_depth() -> int:
    return JOB_MAX_QUEUE_DEPTH

