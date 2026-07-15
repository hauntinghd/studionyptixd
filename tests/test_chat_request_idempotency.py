from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from fastapi import FastAPI
from fastapi.testclient import TestClient

import studio_agent_router
from studio_agent import store


def _create_session(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path)
    return store.create_session(user_id="user-1", model="test-model")


def _test_app(monkeypatch) -> TestClient:
    user = {"id": "user-1"}

    def require_auth():
        return user

    monkeypatch.setattr(studio_agent_router.openrouter, "api_key", lambda: "test-key")
    monkeypatch.setattr(studio_agent_router, "_membership_plan_for_user", lambda _user: "owner")
    app = FastAPI()
    app.include_router(
        studio_agent_router.build_studio_agent_router(
            require_auth=require_auth,
            is_admin_check=lambda _user: True,
        )
    )
    return TestClient(app)


def test_create_run_sequential_retry_returns_the_same_run(tmp_path, monkeypatch) -> None:
    session = _create_session(tmp_path, monkeypatch)

    first = store.create_run(
        session["session_id"],
        user_text="Plan the next five scenes",
        request_id="request-sequential",
    )
    retry = store.create_run(
        session["session_id"],
        user_text="Plan the next five scenes",
        request_id="request-sequential",
    )

    assert first["run_id"] == retry["run_id"]
    assert first["idempotent_replay"] is False
    assert retry["idempotent_replay"] is True
    assert len(store.list_runs(session["session_id"])) == 1


def test_create_run_concurrent_retry_creates_one_run(tmp_path, monkeypatch) -> None:
    session = _create_session(tmp_path, monkeypatch)
    workers = 8
    ready = Barrier(workers)

    def create():
        ready.wait()
        return store.create_run(
            session["session_id"],
            user_text="Animate the approved scenes",
            request_id="request-concurrent",
        )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        rows = list(pool.map(lambda _index: create(), range(workers)))

    assert len({row["run_id"] for row in rows}) == 1
    assert sum(not row["idempotent_replay"] for row in rows) == 1
    assert sum(bool(row["idempotent_replay"]) for row in rows) == workers - 1
    assert len(store.list_runs(session["session_id"])) == 1


def test_stream_retry_replays_completed_run_without_calling_model_twice(tmp_path, monkeypatch) -> None:
    session = _create_session(tmp_path, monkeypatch)
    calls: list[str] = []

    async def fake_stream_turn(current_session, user_text, *, run_id=None, **_kwargs):
        assert run_id
        calls.append(run_id)
        result = {
            "assistant_message": "Saved answer",
            "pending_actions": [],
            "active_jobs": [],
            "run_id": run_id,
        }
        store.append_run_event(
            current_session["session_id"],
            run_id,
            "model_round",
            {"event": "model_round", "round": 1},
        )
        store.append_run_event(current_session["session_id"], run_id, "done", {"event": "done"})
        store.finish_run(current_session["session_id"], run_id, status="complete", result=result)
        yield f"event: done\ndata: {json.dumps({'event': 'done', **result})}\n\n"

    monkeypatch.setattr(studio_agent_router.runner, "stream_turn", fake_stream_turn)
    client = _test_app(monkeypatch)
    payload = {
        "request_id": "request-router-retry",
        "message": "Plan the next five scenes",
        "agent_mode": "plan",
    }

    first = client.post(f"/api/studio-agent/sessions/{session['session_id']}/chat/stream", json=payload)
    retry = client.post(f"/api/studio-agent/sessions/{session['session_id']}/chat/stream", json=payload)

    assert first.status_code == 200
    assert retry.status_code == 200
    assert len(calls) == 1
    assert calls[0] in first.text
    assert calls[0] in retry.text
    assert "Saved answer" in retry.text
    assert '"idempotent_replay": true' in retry.text
    assert len(store.list_runs(session["session_id"])) == 1


def test_stream_retry_for_active_run_returns_resume_status_without_model_call(tmp_path, monkeypatch) -> None:
    session = _create_session(tmp_path, monkeypatch)
    existing = store.create_run(
        session["session_id"],
        user_text="Plan the next five scenes",
        request_id="request-active-retry",
    )
    calls: list[str] = []

    async def fake_stream_turn(*_args, **_kwargs):
        calls.append("called")
        yield ""

    monkeypatch.setattr(studio_agent_router.runner, "stream_turn", fake_stream_turn)
    client = _test_app(monkeypatch)
    response = client.post(
        f"/api/studio-agent/sessions/{session['session_id']}/chat/stream",
        json={
            "request_id": "request-active-retry",
            "message": "Plan the next five scenes",
            "agent_mode": "plan",
        },
    )

    assert response.status_code == 200
    assert not calls
    assert existing["run_id"] in response.text
    assert '"resume_required": true' in response.text
    assert "event: done" not in response.text

