"""Stage-level production concurrency gates.

These slots protect the expensive parts of a render. They are intentionally
stage-specific so one finished I2V call frees an I2V slot immediately without
waiting for unrelated voice/SFX/compose work.
"""
from __future__ import annotations

import os
import random
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator

try:
    import redis
except Exception:  # pragma: no cover - optional production dependency
    redis = None  # type: ignore[assignment]


DEFAULT_LIMITS = {
    "render": 6,
    "stills": 12,
    "i2v": 4,
    "i2v_premium": 1,
    "audio": 8,
    "compose": 4,
}

REDIS_URL = os.getenv("REDIS_URL", "")
REDIS_QUEUE_PREFIX = os.getenv("REDIS_QUEUE_PREFIX", "studio")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _limit(lane: str) -> int:
    key = f"STUDIO_PRODUCTION_{lane.upper()}_SLOTS"
    fallback = DEFAULT_LIMITS.get(lane, 4)
    try:
        return max(1, int(os.getenv(key, str(fallback)) or fallback))
    except Exception:
        return fallback


def _max_wait_sec() -> int:
    try:
        return max(30, int(os.getenv("STUDIO_PRODUCTION_SLOT_MAX_WAIT_SEC", "1800") or 1800))
    except Exception:
        return 1800


def _poll_sec() -> float:
    try:
        return max(0.25, float(os.getenv("STUDIO_PRODUCTION_SLOT_POLL_SEC", "2.0") or 2.0))
    except Exception:
        return 2.0


def _lease_sec() -> int:
    return max(_max_wait_sec() * 4, 7200)


@dataclass
class ProductionSlotAdmission:
    lane: str
    waited_sec: float
    queue_position: int
    active: int
    limit: int
    mode: str
    lease_id: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "lane": self.lane,
            "waited_sec": round(self.waited_sec, 2),
            "queue_position": int(self.queue_position),
            "active": int(self.active),
            "limit": int(self.limit),
            "mode": self.mode,
        }


_local_lock = threading.RLock()
_local_active: dict[str, int] = {}
_local_waiting: dict[str, int] = {}
_redis_client: Any = None

_TRY_ACQUIRE_LUA = """
local active = tonumber(redis.call('GET', KEYS[1]) or '0')
local reclaimed = tonumber(redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[2]) or '0')
if reclaimed > 0 then
  active = math.max(active - reclaimed, 0)
  redis.call('SET', KEYS[1], active)
end
local maxc = tonumber(ARGV[1])
if active < maxc then
  redis.call('INCR', KEYS[1])
  redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
  redis.call('EXPIRE', KEYS[2], ARGV[5])
  return {1, active + 1, reclaimed}
end
return {0, active, reclaimed}
"""

_RELEASE_LUA = """
local active = tonumber(redis.call('GET', KEYS[1]) or '0')
local lease_id = ARGV[1] or ''
local removed = 0
if lease_id ~= '' then
  removed = tonumber(redis.call('ZREM', KEYS[2], lease_id) or '0')
end
if active > 0 and removed > 0 then
  redis.call('DECR', KEYS[1])
  return active - 1
end
return active
"""


def _redis_enabled() -> bool:
    return bool(_env_bool("REDIS_QUEUE_ENABLED", True) and REDIS_URL and redis is not None)


def _get_redis() -> Any:
    global _redis_client
    if not _redis_enabled():
        return None
    if _redis_client is None:
        try:
            _redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        except Exception:
            return None
    return _redis_client


def _redis_ping() -> bool:
    client = _get_redis()
    if client is None:
        return False
    try:
        return bool(client.ping())
    except Exception:
        return False


def _keys(lane: str) -> tuple[str, str, str]:
    base = f"{REDIS_QUEUE_PREFIX}:production_slots:{lane}"
    return f"{base}:active", f"{base}:leases", f"{base}:waiting"


def _redis_try_acquire(lane: str, lease_id: str, limit: int) -> tuple[bool, int]:
    client = _get_redis()
    if client is None:
        return False, 0
    active_key, leases_key, _waiting_key = _keys(lane)
    now = time.time()
    try:
        raw = client.eval(
            _TRY_ACQUIRE_LUA,
            2,
            active_key,
            leases_key,
            str(limit),
            str(now),
            str(now + _lease_sec()),
            lease_id,
            str(_lease_sec() + 60),
        )
        return bool(int(raw[0])), int(raw[1])
    except Exception:
        return False, 0


def _redis_release(lane: str, lease_id: str) -> None:
    client = _get_redis()
    if client is None:
        return
    active_key, leases_key, _waiting_key = _keys(lane)
    try:
        client.eval(_RELEASE_LUA, 2, active_key, leases_key, lease_id or "")
    except Exception:
        pass


def _incr_waiting(lane: str, use_redis: bool) -> int:
    if use_redis:
        client = _get_redis()
        if client is not None:
            try:
                return int(client.incr(_keys(lane)[2]))
            except Exception:
                pass
    with _local_lock:
        _local_waiting[lane] = int(_local_waiting.get(lane, 0)) + 1
        return _local_waiting[lane]


def _decr_waiting(lane: str, use_redis: bool) -> None:
    if use_redis:
        client = _get_redis()
        if client is not None:
            try:
                cur = int(client.decr(_keys(lane)[2]))
                if cur < 0:
                    client.set(_keys(lane)[2], 0)
                return
            except Exception:
                pass
    with _local_lock:
        if int(_local_waiting.get(lane, 0)) > 0:
            _local_waiting[lane] = int(_local_waiting.get(lane, 0)) - 1


def _local_try_acquire(lane: str, limit: int) -> tuple[bool, int]:
    with _local_lock:
        active = int(_local_active.get(lane, 0))
        if active >= limit:
            return False, active
        active += 1
        _local_active[lane] = active
        return True, active


def _local_release(lane: str) -> None:
    with _local_lock:
        active = int(_local_active.get(lane, 0))
        if active > 0:
            _local_active[lane] = active - 1


def slot_snapshot() -> dict[str, Any]:
    lanes = sorted(DEFAULT_LIMITS)
    use_redis = _redis_ping()
    rows: dict[str, Any] = {}
    for lane in lanes:
        limit = _limit(lane)
        if use_redis:
            client = _get_redis()
            try:
                active = int(client.get(_keys(lane)[0]) or 0) if client else 0
                waiting = int(client.get(_keys(lane)[2]) or 0) if client else 0
            except Exception:
                active, waiting = 0, 0
        else:
            with _local_lock:
                active = int(_local_active.get(lane, 0))
                waiting = int(_local_waiting.get(lane, 0))
        rows[lane] = {
            "active": active,
            "waiting": waiting,
            "limit": limit,
            "utilization_pct": round((active / max(limit, 1)) * 100.0, 1),
        }
    return {"mode": "redis" if use_redis else "local", "lanes": rows}


def acquire_slot(
    lane: str,
    *,
    on_wait: Callable[[ProductionSlotAdmission], None] | None = None,
) -> ProductionSlotAdmission:
    lane = str(lane or "render").strip().lower()
    limit = _limit(lane)
    start = time.time()
    deadline = start + _max_wait_sec()
    use_redis = _redis_ping()
    mode = "redis" if use_redis else "local"
    lease_id = uuid.uuid4().hex if use_redis else ""
    registered_wait = False
    position = 0

    try:
        while time.time() < deadline:
            if use_redis:
                ok, active = _redis_try_acquire(lane, lease_id, limit)
            else:
                ok, active = _local_try_acquire(lane, limit)
            if ok:
                if registered_wait:
                    _decr_waiting(lane, use_redis)
                return ProductionSlotAdmission(
                    lane=lane,
                    waited_sec=time.time() - start,
                    queue_position=position,
                    active=active,
                    limit=limit,
                    mode=mode,
                    lease_id=lease_id,
                )
            if not registered_wait:
                position = _incr_waiting(lane, use_redis)
                registered_wait = True
            if on_wait:
                on_wait(
                    ProductionSlotAdmission(
                        lane=lane,
                        waited_sec=time.time() - start,
                        queue_position=position,
                        active=active,
                        limit=limit,
                        mode=mode,
                        lease_id=lease_id,
                    )
                )
            sleep = _poll_sec() * (1.0 + min(position, 30) * 0.04) + random.uniform(0, 0.25)
            time.sleep(min(sleep, max(0.1, deadline - time.time())))
        raise TimeoutError(f"Timed out waiting for Studio production {lane} slot")
    except Exception:
        if registered_wait:
            _decr_waiting(lane, use_redis)
        raise


def release_slot(admission: ProductionSlotAdmission) -> None:
    if admission.mode == "redis":
        _redis_release(admission.lane, admission.lease_id)
    elif admission.mode == "local":
        _local_release(admission.lane)


@contextmanager
def production_slot(
    lane: str,
    *,
    on_wait: Callable[[ProductionSlotAdmission], None] | None = None,
) -> Iterator[ProductionSlotAdmission]:
    admission = acquire_slot(lane, on_wait=on_wait)
    try:
        yield admission
    finally:
        release_slot(admission)
