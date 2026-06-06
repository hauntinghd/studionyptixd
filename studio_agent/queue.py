"""
Distributed concurrency gate for Studio Agent (OpenRouter chat + fal tool spawns).

When active turns exceed STUDIO_AGENT_MAX_CONCURRENT (default 250), additional
chat/approve requests block until a slot frees (up to STUDIO_AGENT_QUEUE_MAX_WAIT_SEC).

Configure via env:
  STUDIO_AGENT_MAX_CONCURRENT=250   # start queueing above this (100–500 typical)
  STUDIO_AGENT_MAX_QUEUE_DEPTH=500  # max waiters (503 when exceeded)
  STUDIO_AGENT_QUEUE_MAX_WAIT_SEC=900

Redis (REDIS_URL) shares the counter across Fly machines; otherwise a local
asyncio.Semaphore is used per worker process.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator

from backend_settings import (
    REDIS_QUEUE_PREFIX,
    REDIS_QUEUE_ENABLED,
    REDIS_URL,
    STUDIO_AGENT_MAX_CONCURRENT,
    STUDIO_AGENT_MAX_QUEUE_DEPTH,
    STUDIO_AGENT_QUEUE_ENABLED,
    STUDIO_AGENT_QUEUE_MAX_WAIT_SEC,
    STUDIO_AGENT_QUEUE_POLL_SEC,
)

try:
    from redis.asyncio import Redis
except Exception:
    Redis = None  # type: ignore[misc, assignment]

_log = logging.getLogger("nyptid-studio.studio-agent-queue")

_ACTIVE_KEY = f"{REDIS_QUEUE_PREFIX}:studio_agent:active"
_WAITING_KEY = f"{REDIS_QUEUE_PREFIX}:studio_agent:waiting"

_TRY_ACQUIRE_LUA = """
local active = tonumber(redis.call('GET', KEYS[1]) or '0')
local maxc = tonumber(ARGV[1])
if active < maxc then
  redis.call('INCR', KEYS[1])
  return {1, active + 1}
end
return {0, active}
"""

_RELEASE_LUA = """
local active = tonumber(redis.call('GET', KEYS[1]) or '0')
if active > 0 then
  redis.call('DECR', KEYS[1])
  return active - 1
end
return 0
"""

_redis_client: Redis | None = None
_local_active = 0
_local_waiting = 0
_local_lock = asyncio.Lock()


class StudioAgentQueueFullError(RuntimeError):
    """Too many waiters — user should retry later."""


class StudioAgentQueueTimeoutError(RuntimeError):
    """Waited longer than STUDIO_AGENT_QUEUE_MAX_WAIT_SEC for a slot."""


def should_bypass_queue(
    *,
    plan: str = "",
    operation: str = "chat",
    unlimited: bool = False,
) -> bool:
    """Owners and fast paths (approve/reject) should not block behind chat turns."""
    if unlimited or str(plan or "").strip().lower() in ("owner", "admin"):
        return True
    op = str(operation or "chat").strip().lower()
    return op in ("approve", "reject", "read", "retry_production")


@dataclass
class QueueAdmission:
    waited_sec: float
    queue_position: int
    active_sessions: int
    mode: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "waited_sec": round(self.waited_sec, 2),
            "queue_position": int(self.queue_position),
            "active_sessions": int(self.active_sessions),
            "mode": self.mode,
        }


def _redis_enabled() -> bool:
    return bool(STUDIO_AGENT_QUEUE_ENABLED and REDIS_QUEUE_ENABLED and REDIS_URL and Redis is not None)


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


async def _redis_ping() -> bool:
    redis = await _get_redis()
    if redis is None:
        return False
    try:
        await redis.ping()
        return True
    except Exception:
        return False


def queue_config() -> dict[str, Any]:
    return {
        "enabled": bool(STUDIO_AGENT_QUEUE_ENABLED),
        "max_concurrent": int(STUDIO_AGENT_MAX_CONCURRENT),
        "max_queue_depth": int(STUDIO_AGENT_MAX_QUEUE_DEPTH),
        "max_wait_sec": int(STUDIO_AGENT_QUEUE_MAX_WAIT_SEC),
        "redis_backed": bool(_redis_enabled()),
    }


async def queue_snapshot() -> dict[str, Any]:
    cfg = queue_config()
    active = 0
    waiting = 0
    mode = "disabled"
    if not STUDIO_AGENT_QUEUE_ENABLED:
        return {**cfg, "active_sessions": 0, "waiting": 0, "mode": mode}

    if await _redis_ping():
        redis = await _get_redis()
        if redis is not None:
            try:
                active = int(await redis.get(_ACTIVE_KEY) or 0)
                waiting = int(await redis.get(_WAITING_KEY) or 0)
                mode = "redis"
            except Exception as exc:
                _log.warning("studio agent queue snapshot redis failed: %s", exc)

    if mode != "redis":
        async with _local_lock:
            active = _local_active
            waiting = _local_waiting
            mode = "local"

    utilization = round((active / max(cfg["max_concurrent"], 1)) * 100.0, 1)
    return {
        **cfg,
        "active_sessions": active,
        "waiting": waiting,
        "mode": mode,
        "utilization_pct": utilization,
        "queued": active >= cfg["max_concurrent"] or waiting > 0,
    }


async def _redis_try_acquire() -> tuple[bool, int]:
    redis = await _get_redis()
    if redis is None:
        return False, 0
    try:
        raw = await redis.eval(
            _TRY_ACQUIRE_LUA,
            1,
            _ACTIVE_KEY,
            str(STUDIO_AGENT_MAX_CONCURRENT),
        )
        return bool(int(raw[0])), int(raw[1])
    except Exception as exc:
        _log.warning("studio agent redis try_acquire failed: %s", exc)
        return False, 0


async def _redis_release() -> None:
    redis = await _get_redis()
    if redis is None:
        return
    try:
        await redis.eval(_RELEASE_LUA, 1, _ACTIVE_KEY)
    except Exception as exc:
        _log.warning("studio agent redis release failed: %s", exc)


async def _incr_waiting() -> int:
    if await _redis_ping():
        redis = await _get_redis()
        if redis is not None:
            try:
                return int(await redis.incr(_WAITING_KEY))
            except Exception:
                pass
    async with _local_lock:
        global _local_waiting
        _local_waiting += 1
        return _local_waiting


async def _decr_waiting() -> None:
    if await _redis_ping():
        redis = await _get_redis()
        if redis is not None:
            try:
                cur = int(await redis.decr(_WAITING_KEY))
                if cur < 0:
                    await redis.set(_WAITING_KEY, 0)
                return
            except Exception:
                pass
    async with _local_lock:
        global _local_waiting
        if _local_waiting > 0:
            _local_waiting -= 1


async def _local_try_acquire() -> tuple[bool, int]:
    global _local_active
    async with _local_lock:
        if _local_active > STUDIO_AGENT_MAX_CONCURRENT:
            _log.warning(
                "studio agent local active counter leaked (%s > %s); resetting",
                _local_active,
                STUDIO_AGENT_MAX_CONCURRENT,
            )
            _local_active = 0
        if _local_active >= STUDIO_AGENT_MAX_CONCURRENT:
            return False, _local_active
        _local_active += 1
        return True, _local_active


async def _local_release() -> None:
    global _local_active
    async with _local_lock:
        if _local_active > 0:
            _local_active -= 1


async def reset_queue_counters() -> dict[str, Any]:
    """Admin recovery when active/waiting counters drift after worker crashes."""
    global _local_active, _local_waiting
    cleared: dict[str, Any] = {"local_active": 0, "local_waiting": 0, "redis_active": None, "redis_waiting": None}
    async with _local_lock:
        cleared["local_active"] = _local_active
        cleared["local_waiting"] = _local_waiting
        _local_active = 0
        _local_waiting = 0
    if await _redis_ping():
        redis = await _get_redis()
        if redis is not None:
            try:
                cleared["redis_active"] = int(await redis.get(_ACTIVE_KEY) or 0)
                cleared["redis_waiting"] = int(await redis.get(_WAITING_KEY) or 0)
                await redis.set(_ACTIVE_KEY, 0)
                await redis.set(_WAITING_KEY, 0)
            except Exception as exc:
                _log.warning("studio agent queue reset redis failed: %s", exc)
    return cleared


async def acquire_slot(
    *,
    user_id: str = "",
    plan: str = "",
    operation: str = "chat",
    unlimited: bool = False,
) -> QueueAdmission:
    if not STUDIO_AGENT_QUEUE_ENABLED:
        return QueueAdmission(0.0, 0, 0, "disabled")
    if should_bypass_queue(plan=plan, operation=operation, unlimited=unlimited):
        return QueueAdmission(0.0, 0, 0, "bypass")

    start = time.time()
    deadline = start + STUDIO_AGENT_QUEUE_MAX_WAIT_SEC
    use_redis = await _redis_ping()
    mode = "redis" if use_redis else "local"
    registered_wait = False
    queue_position = 0

    try:
        while time.time() < deadline:
            if use_redis:
                ok, active = await _redis_try_acquire()
            else:
                ok, active = await _local_try_acquire()

            if ok:
                if registered_wait:
                    await _decr_waiting()
                waited = time.time() - start
                return QueueAdmission(waited, queue_position, active, mode)

            if not registered_wait:
                queue_position = await _incr_waiting()
                registered_wait = True
                if queue_position > STUDIO_AGENT_MAX_QUEUE_DEPTH:
                    raise StudioAgentQueueFullError(
                        f"Studio Agent queue is full ({queue_position} waiting). "
                        "OpenRouter and fal are at capacity — try again in a few minutes."
                    )

            poll = STUDIO_AGENT_QUEUE_POLL_SEC * (1.0 + min(queue_position, 30) * 0.04)
            poll += random.uniform(0, 0.3)
            await asyncio.sleep(min(poll, max(0.1, deadline - time.time())))

        raise StudioAgentQueueTimeoutError(
            f"Timed out after {STUDIO_AGENT_QUEUE_MAX_WAIT_SEC}s waiting for Studio Agent capacity."
        )
    except Exception:
        if registered_wait:
            await _decr_waiting()
        raise


async def release_slot(admission: QueueAdmission | None = None) -> None:
    if not STUDIO_AGENT_QUEUE_ENABLED:
        return
    mode = admission.mode if admission else "local"
    if mode == "redis" or await _redis_ping():
        await _redis_release()
    else:
        await _local_release()


@asynccontextmanager
async def studio_agent_slot(
    *,
    user_id: str = "",
    plan: str = "",
    operation: str = "chat",
    unlimited: bool = False,
) -> AsyncIterator[QueueAdmission]:
    admission: QueueAdmission | None = None
    try:
        admission = await acquire_slot(
            user_id=user_id,
            plan=plan,
            operation=operation,
            unlimited=unlimited,
        )
        yield admission
    finally:
        if admission is not None and admission.mode not in ("disabled", "bypass"):
            await release_slot(admission)

