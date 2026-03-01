import asyncio
import json
import logging
import time
from typing import Any, Awaitable, Callable

from backend_settings import (
    JOB_MAX_QUEUE_DEPTH,
    JOB_QUEUE_WORKERS,
    REDIS_QUEUE_ENABLED,
    REDIS_QUEUE_PREFIX,
    REDIS_URL,
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
        _redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
    return _redis_client


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


async def persist_job_state(job_id: str, job_state: dict[str, Any]):
    redis = await _get_redis()
    if redis is None:
        return
    await redis.set(_job_key(job_id), json.dumps(job_state, ensure_ascii=True), ex=60 * 60 * 6)


async def get_persisted_job_state(job_id: str) -> dict[str, Any] | None:
    redis = await _get_redis()
    if redis is None:
        return None
    raw = await redis.get(_job_key(job_id))
    if not raw:
        return None
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except Exception:
        return None
    return None


async def enqueue_generation_job(
    job_id: str,
    plan: str,
    coro_func: Callable[..., Awaitable[Any]],
    args: tuple[Any, ...],
):
    if _redis_enabled():
        redis = await _get_redis()
        if redis is None:
            raise QueueFullError("Redis queue is enabled but unavailable.")
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

    global _job_seq
    if len(_queued_job_meta) >= JOB_MAX_QUEUE_DEPTH:
        raise QueueFullError(f"Queue is full ({JOB_MAX_QUEUE_DEPTH}). Please retry shortly.")
    await _ensure_job_workers()
    priority = _plan_queue_priority(plan)
    _job_seq += 1
    _queued_job_meta[job_id] = (priority, _job_seq)
    if _jobs_ref is not None and job_id in _jobs_ref:
        _jobs_ref[job_id]["queue_priority"] = priority
        _jobs_ref[job_id]["queue_mode"] = "inprocess"
    _update_queue_positions()
    await _get_job_queue().put((priority, _job_seq, job_id, coro_func, args))


async def dequeue_generation_job() -> dict[str, Any] | None:
    redis = await _get_redis()
    if redis is None:
        return None
    # Highest priority first (p0 -> p1 -> p2)
    result = await redis.brpop([_queue_key(0), _queue_key(1), _queue_key(2)], timeout=2)
    if not result:
        return None
    _key, raw = result
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


async def get_queue_depth() -> int:
    if _redis_enabled():
        redis = await _get_redis()
        if redis is None:
            return 0
        p0, p1, p2 = await asyncio.gather(
            redis.llen(_queue_key(0)),
            redis.llen(_queue_key(1)),
            redis.llen(_queue_key(2)),
        )
        return int(p0 or 0) + int(p1 or 0) + int(p2 or 0)
    return len(_queued_job_meta)


def get_queue_workers() -> int:
    return JOB_QUEUE_WORKERS


def get_queue_max_depth() -> int:
    return JOB_MAX_QUEUE_DEPTH

