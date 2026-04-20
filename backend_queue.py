import asyncio
import json
import logging
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


def _job_state_file(job_id: str) -> Path:
    return _JOB_STATE_DIR / f"{job_id}.json"


def _redis_enabled() -> bool:
    return bool(REDIS_QUEUE_ENABLED and REDIS_URL and Redis is not None)


def _queue_key(priority: int) -> str:
    return f"{REDIS_QUEUE_PREFIX}:queue:p{priority}"


def _job_key(job_id: str) -> str:
    return f"{REDIS_QUEUE_PREFIX}:job:{job_id}"


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


async def persist_job_state(job_id: str, job_state: dict[str, Any]):
    payload = json.dumps(job_state, ensure_ascii=True)
    redis = await _get_redis()
    if redis is not None:
        try:
            await redis.set(_job_key(job_id), payload, ex=60 * 60 * 6)
        except Exception as e:
            _log.warning(f"Redis job persistence failed for {job_id}: {e}")
    try:
        async with _job_state_file_lock:
            path = _job_state_file(job_id)
            tmp = path.with_suffix(".tmp")
            tmp.write_text(payload, encoding="utf-8")
            tmp.replace(path)
    except Exception as e:
        _log.warning(f"Local job persistence failed for {job_id}: {e}")
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
        task = loop.create_task(_persist_job_state_supabase(job_id, job_state))
        _background_tasks.add(task)
        task.add_done_callback(_background_tasks.discard)
    except RuntimeError:
        pass


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
    if _redis_enabled() and await _redis_available():
        try:
            redis = await _get_redis()
            if redis is not None:
                depth = await get_queue_depth()
                if depth >= JOB_MAX_QUEUE_DEPTH:
                    raise QueueFullError(f"Queue is full ({JOB_MAX_QUEUE_DEPTH}). Please retry shortly.")
                priority = _plan_queue_priority(plan)
                if _jobs_ref is not None and job_id in _jobs_ref:
                    _jobs_ref[job_id]["queue_priority"] = priority
                    _jobs_ref[job_id]["queue_mode"] = "redis"
                    await persist_job_state(job_id, _jobs_ref[job_id])
                payload = {
                    "job_id": job_id,
                    "task_name": getattr(coro_func, "__name__", ""),
                    "args": list(args),
                    "priority": priority,
                    "queued_at": time.time(),
                }
                await redis.lpush(_queue_key(priority), json.dumps(payload, ensure_ascii=True))
                return
        except QueueFullError:
            raise
        except Exception as e:
            _log.warning(f"Redis enqueue failed; falling back to inprocess queue: {e}")

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
    redis = await _get_redis()
    if redis is None:
        return None
    # Highest priority first (p0 -> p1 -> p2)
    try:
        result = await redis.brpop([_queue_key(0), _queue_key(1), _queue_key(2)], timeout=2)
    except Exception as e:
        _log.warning(f"Redis dequeue failed: {e}")
        return None
    if not result:
        return None
    _key, raw = result
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


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

