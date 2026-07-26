from __future__ import annotations

import asyncio
import json
from pathlib import Path

import backend_queue
import pytest


class _ClaimRedis:
    def __init__(self, raw: str | None = None, *, recovered: int = 0):
        self.raw = raw
        self.recovered = recovered
        self.eval_calls: list[tuple] = []

    async def eval(self, *args):
        self.eval_calls.append(args)
        if args[0] == backend_queue._REDIS_CLAIM_HIGHEST_PRIORITY_LUA:
            value, self.raw = self.raw, None
            return value
        if args[0] == backend_queue._REDIS_RECOVER_INFLIGHT_LUA:
            return self.recovered
        return 1

def test_embedded_mode_never_falls_back_when_redis_is_unconfigured(monkeypatch) -> None:
    monkeypatch.setenv("RUN_EMBEDDED_WORKER", "true")
    monkeypatch.setattr(backend_queue, "_redis_enabled", lambda: False)

    with pytest.raises(backend_queue.QueueFullError, match="temporarily unavailable"):
        asyncio.run(
            backend_queue.enqueue_generation_job(
                "job-no-redis", "creator", lambda: None, ()
            )
        )


def test_dequeue_claims_atomically_and_acknowledges_exact_receipt(monkeypatch) -> None:
    raw = json.dumps(
        {"job_id": "job-1", "task_name": "render", "args": ["job-1"], "priority": 0}
    )
    fake = _ClaimRedis(raw)

    async def get_redis():
        return fake

    monkeypatch.setattr(backend_queue, "_get_redis", get_redis)

    async def exercise():
        payload = await backend_queue.dequeue_generation_job()
        assert payload is not None
        assert payload["job_id"] == "job-1"
        assert await backend_queue.acknowledge_generation_job(payload) is True

    asyncio.run(exercise())

    claim = fake.eval_calls[0]
    assert claim[0] == backend_queue._REDIS_CLAIM_HIGHEST_PRIORITY_LUA
    assert claim[1] == 4
    assert claim[2:6] == (
        backend_queue._queue_key(0),
        backend_queue._queue_key(1),
        backend_queue._queue_key(2),
        backend_queue._processing_key(),
    )
    ack = fake.eval_calls[1]
    assert ack[0] == backend_queue._REDIS_ACK_INFLIGHT_LUA
    assert ack[1:4] == (
        2,
        backend_queue._processing_key(),
        backend_queue._queue_admission_key("job-1"),
    )
    assert ack[4] == raw


def test_startup_recovery_uses_all_priority_lanes_and_processing_list(monkeypatch) -> None:
    fake = _ClaimRedis(recovered=3)

    async def get_redis():
        return fake

    monkeypatch.setattr(backend_queue, "_get_redis", get_redis)
    assert asyncio.run(backend_queue.recover_inflight_generation_jobs()) == 3
    call = fake.eval_calls[0]
    assert call[0] == backend_queue._REDIS_RECOVER_INFLIGHT_LUA
    assert call[1] == 5
    assert call[2:7] == (
        backend_queue._queue_key(0),
        backend_queue._queue_key(1),
        backend_queue._queue_key(2),
        backend_queue._processing_key(),
        backend_queue._dead_letter_key(),
    )


def test_consumer_executes_only_whitelisted_task_then_acks(monkeypatch) -> None:
    jobs: dict[str, dict] = {}
    backend_queue.init_queue_runtime(jobs)
    stop = asyncio.Event()
    payloads = [
        {
            "job_id": "job-2",
            "task_name": "run_generation_pipeline",
            "args": ["job-2", "scene"],
            backend_queue._QUEUE_RECEIPT_FIELD: "raw-job-2",
        }
    ]
    persisted: list[tuple[str, str]] = []
    acknowledgements: list[str] = []
    executed: list[tuple[str, str]] = []

    monkeypatch.setattr(backend_queue, "_redis_enabled", lambda: True)

    async def dequeue():
        return payloads.pop(0) if payloads else None

    async def get_state(_job_id):
        return {"status": "queued", "user_id": "creator-1"}

    async def persist(job_id, state):
        persisted.append((job_id, str(state.get("status", ""))))
        return True

    async def persist_terminal(job_id, state):
        persisted.append((job_id, str(state.get("status", ""))))
        return True

    async def acknowledge(payload):
        acknowledgements.append(str(payload.get("job_id", "")))
        return True

    async def render(job_id, scene):
        executed.append((job_id, scene))
        jobs[job_id]["status"] = "complete"
        stop.set()

    monkeypatch.setattr(backend_queue, "dequeue_generation_job", dequeue)
    monkeypatch.setattr(backend_queue, "get_persisted_job_state", get_state)
    monkeypatch.setattr(backend_queue, "persist_job_state", persist)
    monkeypatch.setattr(backend_queue, "persist_terminal_job_state", persist_terminal)
    monkeypatch.setattr(backend_queue, "acknowledge_generation_job", acknowledge)

    asyncio.run(
        backend_queue.run_generation_consumer(
            {"run_generation_pipeline": render}, stop_event=stop, recover_inflight=False
        )
    )

    assert executed == [("job-2", "scene")]
    assert acknowledgements == ["job-2"]
    assert persisted == [("job-2", "processing"), ("job-2", "complete")]


def test_cliplab_descriptor_uses_same_recoverable_consumer(monkeypatch) -> None:
    jobs: dict[str, dict] = {}
    backend_queue.init_queue_runtime(jobs)
    stop = asyncio.Event()
    descriptor = {
        "operation": "analyze",
        "video_id": "vid-1",
        "prompt": "Find hooks",
        "max_segments": 12,
        "user_id": "creator-1",
    }
    payloads = [
        {
            "job_id": "clipa-recovered",
            "task_name": "_run_cliplab_pipeline",
            "args": ["clipa-recovered", descriptor],
            backend_queue._QUEUE_RECEIPT_FIELD: "raw-cliplab",
        }
    ]
    acknowledgements: list[str] = []
    executed: list[tuple[str, dict]] = []

    monkeypatch.setattr(backend_queue, "_redis_enabled", lambda: True)

    async def dequeue():
        return payloads.pop(0) if payloads else None

    async def get_state(_job_id):
        return {
            "status": "queued",
            "type": "cliplab_analyze",
            "user_id": "creator-1",
            "queue_descriptor": descriptor,
        }

    async def persist(_job_id, _state):
        return True

    async def acknowledge(payload):
        acknowledgements.append(str(payload.get("job_id") or ""))
        return True

    async def run_cliplab(job_id, queued_descriptor):
        executed.append((job_id, dict(queued_descriptor)))
        jobs[job_id]["status"] = "complete"
        stop.set()

    monkeypatch.setattr(backend_queue, "dequeue_generation_job", dequeue)
    monkeypatch.setattr(backend_queue, "get_persisted_job_state", get_state)
    monkeypatch.setattr(backend_queue, "persist_job_state", persist)
    monkeypatch.setattr(backend_queue, "persist_terminal_job_state", persist)
    monkeypatch.setattr(backend_queue, "acknowledge_generation_job", acknowledge)

    asyncio.run(
        backend_queue.run_generation_consumer(
            {"_run_cliplab_pipeline": run_cliplab},
            stop_event=stop,
            recover_inflight=False,
        )
    )

    assert executed == [("clipa-recovered", descriptor)]
    assert acknowledgements == ["clipa-recovered"]


def test_consumer_never_acks_when_every_terminal_store_fails(monkeypatch) -> None:
    jobs: dict[str, dict] = {}
    backend_queue.init_queue_runtime(jobs)
    stop = asyncio.Event()
    payloads = [
        {
            "job_id": "job-durability-fail",
            "task_name": "run_generation_pipeline",
            "args": ["job-durability-fail"],
            backend_queue._QUEUE_RECEIPT_FIELD: "raw-durability-fail",
        }
    ]
    acknowledgements: list[str] = []

    monkeypatch.setattr(backend_queue, "_redis_enabled", lambda: True)

    async def dequeue():
        return payloads.pop(0) if payloads else None

    async def get_state(_job_id):
        return {"status": "queued", "user_id": "creator-1"}

    async def persist_processing(_job_id, _state):
        return True

    async def persist_terminal(_job_id, _state):
        return False

    async def acknowledge(payload):
        acknowledgements.append(str(payload.get("job_id", "")))
        return True

    async def render(job_id):
        jobs[job_id]["status"] = "complete"
        stop.set()

    monkeypatch.setattr(backend_queue, "dequeue_generation_job", dequeue)
    monkeypatch.setattr(backend_queue, "get_persisted_job_state", get_state)
    monkeypatch.setattr(backend_queue, "persist_job_state", persist_processing)
    monkeypatch.setattr(backend_queue, "persist_terminal_job_state", persist_terminal)
    monkeypatch.setattr(backend_queue, "acknowledge_generation_job", acknowledge)

    asyncio.run(
        backend_queue.run_generation_consumer(
            {"run_generation_pipeline": render},
            stop_event=stop,
            recover_inflight=False,
        )
    )

    assert jobs["job-durability-fail"]["status"] == "complete"
    assert acknowledgements == []


def test_conversation_payload_cannot_enter_production_executor(monkeypatch) -> None:
    stop = asyncio.Event()
    payloads = [
        {
            "job_id": "chat-1",
            "task_name": "chat",
            "args": ["plan the next scene"],
            backend_queue._QUEUE_RECEIPT_FIELD: "raw-chat-1",
        }
    ]
    rejected: list[str] = []

    monkeypatch.setattr(backend_queue, "_redis_enabled", lambda: True)

    async def dequeue():
        return payloads.pop(0) if payloads else None

    async def reject(_payload, reason):
        rejected.append(reason)
        stop.set()
        return True

    monkeypatch.setattr(backend_queue, "dequeue_generation_job", dequeue)
    monkeypatch.setattr(backend_queue, "reject_generation_job", reject)

    asyncio.run(
        backend_queue.run_generation_consumer(
            {}, stop_event=stop, recover_inflight=False
        )
    )

    assert rejected == ["unsupported_task:chat"]


def test_embedded_lifecycle_is_visible_to_health(monkeypatch) -> None:
    monkeypatch.setenv("RUN_EMBEDDED_WORKER", "true")
    monkeypatch.setattr(backend_queue, "JOB_QUEUE_WORKERS", 1)
    monkeypatch.setattr(backend_queue, "_redis_enabled", lambda: True)
    monkeypatch.setattr(backend_queue, "_redis_healthy", True)
    monkeypatch.setattr(backend_queue, "_embedded_worker_task", None)
    monkeypatch.setattr(backend_queue, "_embedded_worker_stop", None)
    monkeypatch.setattr(backend_queue, "_embedded_worker_recovered", 0)

    async def available():
        return True

    async def recover():
        return 2

    async def consumer(_task_map, *, stop_event, recover_inflight):
        assert recover_inflight is False
        await stop_event.wait()

    monkeypatch.setattr(backend_queue, "_redis_available", available)
    monkeypatch.setattr(backend_queue, "recover_inflight_generation_jobs", recover)
    monkeypatch.setattr(backend_queue, "run_generation_consumer", consumer)

    async def exercise():
        await backend_queue.start_embedded_generation_worker({"render": lambda: None})
        health = backend_queue.get_queue_runtime_health()
        assert health["required"] is True
        assert health["running"] is True
        assert health["ready"] is True
        assert health["workers"] == 1
        assert health["recovered_jobs"] == 2
        await backend_queue.stop_embedded_generation_worker()
        assert backend_queue.get_queue_runtime_health()["running"] is False

    asyncio.run(exercise())


def test_public_queue_health_never_leaks_raw_worker_errors(monkeypatch) -> None:
    monkeypatch.setenv("RUN_EMBEDDED_WORKER", "true")
    monkeypatch.setattr(backend_queue, "_embedded_worker_last_error", "secret prompt /var/data/private.mp4")
    health = backend_queue.get_queue_runtime_health()

    assert "last_error" not in health
    assert health["has_error"] is True
    assert health["error_code"] == "queue_consumer_error"
    assert "secret prompt" not in repr(health)


def test_fly_launches_one_api_process_with_embedded_worker() -> None:
    root = Path(__file__).resolve().parents[1]
    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
    fly = (root / "fly.toml").read_text(encoding="utf-8")

    assert "ENV WEB_CONCURRENCY=1" in dockerfile
    assert '"--workers", "1"' in dockerfile
    assert 'WEB_CONCURRENCY = "1"' in fly
    assert 'RUN_EMBEDDED_WORKER = "true"' in fly
    assert 'STUDIO_RUNPOD_PRODUCTION_ENABLED = "false"' in fly
    assert 'STUDIO_RUNPOD_LONGFORM_ENABLED = "false"' in fly

    render_script = (root / "ops" / "run_render_service.sh").read_text(encoding="utf-8")
    assert "python -u backend_worker.py" not in render_script
    assert "exec uvicorn" in render_script


def test_api_health_fails_when_required_consumer_is_not_ready(monkeypatch) -> None:
    import backend

    async def degraded_payload():
        return {
            "status": "degraded",
            "backend_commit": "test-sha",
            "deployment_target": "contabo",
            "release_id": "studio-release-test",
            "instance_id": "studio-api-01",
            "queue_consumer_ready": False,
            "queue_consumer": {"required": True, "running": False, "ready": False},
        }

    monkeypatch.setattr(backend, "_base_health_payload", degraded_payload)

    with pytest.raises(backend.HTTPException) as caught:
        asyncio.run(backend._health_payload())

    assert caught.value.status_code == 503
    assert caught.value.detail["queue_consumer"]["running"] is False
    assert caught.value.detail["deployment_target"] == "contabo"
    assert caught.value.detail["release_id"] == "studio-release-test"
    assert caught.value.detail["instance_id"] == "studio-api-01"
