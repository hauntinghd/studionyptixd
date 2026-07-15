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
    monkeypatch.setattr(backend_queue, "_jobs_ref", {})

    async def persist(_job_id, _state):
        return True

    monkeypatch.setattr(backend_queue, "persist_job_state", persist)


async def run_generation_pipeline(*_args):
    raise AssertionError("queued coroutine must not execute inline")


def test_redis_admission_is_one_atomic_eval(monkeypatch):
    fake = _FakeRedis(7)
    _install_redis(monkeypatch, fake)

    backend_queue._jobs_ref["job-1"] = {"status": "queued"}
    asyncio.run(backend_queue.enqueue_generation_job("job-1", "creator", run_generation_pipeline, ()))

    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call[0] == backend_queue._REDIS_ENQUEUE_IF_CAPACITY_LUA
    assert call[1] == 4
    assert call[2:6] == (
        backend_queue._queue_key(0),
        backend_queue._queue_key(1),
        backend_queue._queue_key(2),
        backend_queue._queue_admission_key("job-1"),
    )
    assert call[6] == backend_queue.JOB_MAX_QUEUE_DEPTH
    assert call[7] == 1  # creator priority p0 maps to Lua's one-based KEYS index


def test_redis_capacity_rejection_never_falls_back_locally(monkeypatch):
    fake = _FakeRedis(-1)
    _install_redis(monkeypatch, fake)
    backend_queue._jobs_ref["job-full"] = {"status": "queued"}

    with pytest.raises(backend_queue.QueueFullError, match="Queue is full"):
        asyncio.run(backend_queue.enqueue_generation_job("job-full", "starter", run_generation_pipeline, ()))


def test_ambiguous_redis_failure_is_fail_closed(monkeypatch):
    fake = _FakeRedis(TimeoutError("response lost"))
    _install_redis(monkeypatch, fake)
    backend_queue._jobs_ref["job-timeout"] = {"status": "queued"}

    with pytest.raises(backend_queue.QueueFullError, match="temporarily unavailable"):
        asyncio.run(backend_queue.enqueue_generation_job("job-timeout", "starter", run_generation_pipeline, ()))


def test_configured_but_unhealthy_redis_never_falls_back_locally(monkeypatch):
    monkeypatch.setattr(backend_queue, "_redis_enabled", lambda: True)

    async def unavailable() -> bool:
        return False

    monkeypatch.setattr(backend_queue, "_redis_available", unavailable)
    with pytest.raises(backend_queue.QueueFullError, match="temporarily unavailable"):
        asyncio.run(backend_queue.enqueue_generation_job("job-no-redis", "starter", run_generation_pipeline, ()))


def test_queue_admission_fails_before_redis_when_state_is_not_durable(monkeypatch):
    fake = _FakeRedis(1)
    _install_redis(monkeypatch, fake)
    backend_queue._jobs_ref["job-no-state"] = {"status": "queued"}

    async def persist(_job_id, _state):
        return False

    monkeypatch.setattr(backend_queue, "persist_job_state", persist)
    with pytest.raises(backend_queue.QueueFullError, match="could not be persisted"):
        asyncio.run(
            backend_queue.enqueue_generation_job(
                "job-no-state", "creator", run_generation_pipeline, ()
            )
        )

    assert fake.calls == []


def test_duplicate_job_admission_is_idempotent(monkeypatch):
    fake = _FakeRedis(-2)
    _install_redis(monkeypatch, fake)
    backend_queue._jobs_ref["job-duplicate"] = {"status": "queued"}

    asyncio.run(
        backend_queue.enqueue_generation_job(
            "job-duplicate", "creator", run_generation_pipeline, ()
        )
    )

    assert len(fake.calls) == 1
