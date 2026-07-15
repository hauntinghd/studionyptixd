from __future__ import annotations

import asyncio

import pytest

import backend_queue


class _FakeRedis:
    def __init__(self, result: int | Exception):
        self.result = result
        self.calls: list[tuple] = []

    async def eval(self, *args):
        self.calls.append(args)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def _install_redis(monkeypatch, fake: _FakeRedis) -> None:
    monkeypatch.setattr(backend_queue, "_redis_enabled", lambda: True)

    async def available() -> bool:
        return True

    async def get_redis():
        return fake

    monkeypatch.setattr(backend_queue, "_redis_available", available)
    monkeypatch.setattr(backend_queue, "_get_redis", get_redis)
    monkeypatch.setattr(backend_queue, "_jobs_ref", None)


def test_redis_admission_is_one_atomic_eval(monkeypatch):
    fake = _FakeRedis(7)
    _install_redis(monkeypatch, fake)

    async def unused():
        raise AssertionError("queued coroutine must not execute inline")

    asyncio.run(backend_queue.enqueue_generation_job("job-1", "creator", unused, ()))

    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call[0] == backend_queue._REDIS_ENQUEUE_IF_CAPACITY_LUA
    assert call[1] == 3
    assert call[2:5] == (
        backend_queue._queue_key(0),
        backend_queue._queue_key(1),
        backend_queue._queue_key(2),
    )
    assert call[5] == backend_queue.JOB_MAX_QUEUE_DEPTH
    assert call[6] == 1  # creator priority p0 maps to Lua's one-based KEYS index


def test_redis_capacity_rejection_never_falls_back_locally(monkeypatch):
    fake = _FakeRedis(-1)
    _install_redis(monkeypatch, fake)

    with pytest.raises(backend_queue.QueueFullError, match="Queue is full"):
        asyncio.run(backend_queue.enqueue_generation_job("job-full", "starter", lambda: None, ()))


def test_ambiguous_redis_failure_is_fail_closed(monkeypatch):
    fake = _FakeRedis(TimeoutError("response lost"))
    _install_redis(monkeypatch, fake)

    with pytest.raises(backend_queue.QueueFullError, match="temporarily unavailable"):
        asyncio.run(backend_queue.enqueue_generation_job("job-timeout", "starter", lambda: None, ()))


def test_configured_but_unhealthy_redis_never_falls_back_locally(monkeypatch):
    monkeypatch.setattr(backend_queue, "_redis_enabled", lambda: True)

    async def unavailable() -> bool:
        return False

    monkeypatch.setattr(backend_queue, "_redis_available", unavailable)
    with pytest.raises(backend_queue.QueueFullError, match="temporarily unavailable"):
        asyncio.run(backend_queue.enqueue_generation_job("job-no-redis", "starter", lambda: None, ()))
