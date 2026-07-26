from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import get_context
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

import studio_agent_router
from studio_agent import provider_policy, store


def _bootstrap_process(
    sessions_dir: str,
    bootstrap_key: str,
    start_event,
    result_queue,
) -> None:
    """Spawn-safe worker used to exercise the cross-process file lock."""

    from studio_agent import provider_policy as child_policy
    from studio_agent import store as child_store

    child_store.SESSIONS_DIR = Path(sessions_dir)
    if not start_event.wait(timeout=15):
        result_queue.put(("error", "start timeout"))
        return
    try:
        result = child_store.bootstrap_session(
            user_id="multi-worker-owner",
            bootstrap_key=bootstrap_key,
            create_session_factory=lambda: child_store.create_session(
                user_id="multi-worker-owner",
                model=child_policy.DEFAULT_RUNNER_MODEL,
            ),
        )
        result_queue.put(("ok", result["session"]["session_id"]))
    except Exception as exc:  # pragma: no cover - reported to the parent assert
        result_queue.put(("error", repr(exc)))


def _client(tmp_path, monkeypatch, *, user_id: str = "owner") -> TestClient:
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path)
    monkeypatch.setattr(studio_agent_router.openrouter, "api_key", lambda: "test-key")
    monkeypatch.setattr(
        studio_agent_router.model_registry,
        "assert_model_selectable",
        lambda _model: None,
    )
    monkeypatch.setattr(
        studio_agent_router,
        "_membership_plan_for_user",
        lambda _user: "owner",
    )
    app = FastAPI()
    app.include_router(
        studio_agent_router.build_studio_agent_router(
            require_auth=lambda: {"id": user_id},
            is_admin_check=lambda _user: True,
        )
    )
    return TestClient(app)


def test_authoritative_discovery_finds_user_behind_more_than_200_foreign_sessions(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path)
    target = store.create_session(
        user_id="quiet-owner",
        model=provider_policy.DEFAULT_RUNNER_MODEL,
    )

    # Reproduce the old false-empty condition: the target account's only
    # session is older than the bounded newest-global scan.
    for index in range(225):
        store.create_session(
            user_id=f"busy-owner-{index % 3}",
            model=provider_policy.DEFAULT_RUNNER_MODEL,
        )

    discovered = store.list_sessions("quiet-owner", limit=50)

    assert [row["session_id"] for row in discovered] == [target["session_id"]]

    # The first authoritative pass seals a per-user index. Steady-state
    # sidebar/bootstrap discovery must no longer parse foreign accounts.
    original_read = store._read_session_file
    read_paths = []

    def tracked_read(path):
        read_paths.append(path)
        return original_read(path)

    monkeypatch.setattr(store, "_read_session_file", tracked_read)
    discovered_again = store.list_sessions("quiet-owner", limit=50)
    assert [row["session_id"] for row in discovered_again] == [target["session_id"]]
    assert read_paths == [tmp_path / f"{target['session_id']}.json"]


def test_concurrent_bootstrap_requests_create_exactly_one_session(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path)
    created = 0
    counter_lock = threading.Lock()

    def create_once() -> dict:
        nonlocal created
        with counter_lock:
            created += 1
        # Widen the race window so the test would reliably fail without the
        # per-user bootstrap lock.
        time.sleep(0.025)
        return store.create_session(
            user_id="concurrent-owner",
            model=provider_policy.DEFAULT_RUNNER_MODEL,
        )

    def bootstrap(index: int) -> dict:
        return store.bootstrap_session(
            user_id="concurrent-owner",
            bootstrap_key=f"client-key-{index % 2}",
            create_session_factory=create_once,
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(bootstrap, range(8)))

    session_ids = {
        result["session"]["session_id"]
        for result in results
    }
    assert created == 1
    assert len(session_ids) == 1
    assert len(store.list_sessions("concurrent-owner", limit=50)) == 1


def test_concurrent_api_workers_converge_through_the_file_lock(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path)
    context = get_context("spawn")
    start_event = context.Event()
    result_queue = context.Queue()
    processes = [
        context.Process(
            target=_bootstrap_process,
            args=(
                str(tmp_path),
                f"worker-client-key-{index}",
                start_event,
                result_queue,
            ),
        )
        for index in range(4)
    ]
    for process in processes:
        process.start()
    start_event.set()
    for process in processes:
        process.join(timeout=30)
    try:
        assert all(not process.is_alive() for process in processes)
        assert all(process.exitcode == 0 for process in processes)
        results = [result_queue.get(timeout=5) for _ in processes]
        assert all(status == "ok" for status, _value in results), results
        assert len({value for _status, value in results}) == 1
        assert len(store.list_sessions("multi-worker-owner", limit=50)) == 1
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
        result_queue.close()
        result_queue.join_thread()


def test_bootstrap_endpoint_is_idempotent_but_new_chat_is_not(
    tmp_path,
    monkeypatch,
) -> None:
    client = _client(tmp_path, monkeypatch)
    body = {
        "bootstrap_key": "persisted-client-key",
        "model": provider_policy.DEFAULT_RUNNER_MODEL,
    }

    first = client.post("/api/studio-agent/sessions/bootstrap", json=body)
    second = client.post("/api/studio-agent/sessions/bootstrap", json=body)

    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert first.json()["mode"] == "created"
    assert second.json()["mode"] == "resumed"
    assert second.json()["idempotent_replay"] is True
    assert (
        first.json()["session"]["session_id"]
        == second.json()["session"]["session_id"]
    )

    new_one = client.post(
        "/api/studio-agent/sessions",
        json={"model": provider_policy.DEFAULT_RUNNER_MODEL},
    )
    new_two = client.post(
        "/api/studio-agent/sessions",
        json={"model": provider_policy.DEFAULT_RUNNER_MODEL},
    )
    assert new_one.status_code == 200, new_one.text
    assert new_two.status_code == 200, new_two.text
    assert (
        new_one.json()["session"]["session_id"]
        != new_two.json()["session"]["session_id"]
    )


def test_bootstrap_directly_recovers_preferred_session_outside_first_page(
    tmp_path,
    monkeypatch,
) -> None:
    client = _client(tmp_path, monkeypatch)
    preferred = store.create_session(
        user_id="owner",
        model=provider_policy.DEFAULT_RUNNER_MODEL,
    )
    for _ in range(55):
        store.create_session(
            user_id="owner",
            model=provider_policy.DEFAULT_RUNNER_MODEL,
        )

    response = client.post(
        "/api/studio-agent/sessions/bootstrap?message_tail=120",
        json={
            "bootstrap_key": "preferred-session-key",
            "preferred_session_id": preferred["session_id"],
            "model": provider_policy.DEFAULT_RUNNER_MODEL,
        },
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["mode"] == "resumed"
    assert payload["source"] == "preferred"
    assert payload["session"]["session_id"] == preferred["session_id"]
    assert len(payload["sessions"]) == 50
    assert preferred["session_id"] not in {
        row["session_id"]
        for row in payload["sessions"]
    }
