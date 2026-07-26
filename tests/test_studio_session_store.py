from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.testclient import TestClient

import studio_agent_router
from studio_agent import jobs, runner, store


def _create_session(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path)
    return store.create_session(user_id="user-1", model="test-model")


def _test_app(monkeypatch) -> TestClient:
    monkeypatch.setattr(studio_agent_router.openrouter, "api_key", lambda: "test-key")
    monkeypatch.setattr(studio_agent_router, "_membership_plan_for_user", lambda _user: "owner")
    app = FastAPI()
    app.include_router(
        studio_agent_router.build_studio_agent_router(
            require_auth=lambda: {"id": "user-1"},
            is_admin_check=lambda _user: True,
        )
    )
    return TestClient(app)


def test_get_session_prunes_and_persists_terminal_active_job_without_recursion(
    tmp_path,
    monkeypatch,
) -> None:
    session = _create_session(tmp_path, monkeypatch)
    session_id = session["session_id"]
    store.update_session(
        session_id,
        active_jobs=[
            {
                "job_id": "terminal-short",
                "kind": "shortform",
                "title": "Old short",
                "status": "complete",
            }
        ],
    )
    monkeypatch.setattr(jobs, "shortform_job_terminal_fast", lambda _job_id: True)

    reconciled = store.get_session(
        session_id,
        reconcile_jobs=False,
        _prune_active_jobs=True,
    )

    assert reconciled is not None
    assert reconciled["active_jobs"] == []
    assert reconciled["blocked_job_ids"] == ["terminal-short"]
    persisted = json.loads((tmp_path / f"{session_id}.json").read_text(encoding="utf-8"))
    assert persisted["active_jobs"] == []
    assert persisted["blocked_job_ids"] == ["terminal-short"]


def test_update_session_bypasses_read_time_active_job_pruning(tmp_path, monkeypatch) -> None:
    session = _create_session(tmp_path, monkeypatch)

    def unexpected_prune(*_args, **_kwargs):
        raise AssertionError("update_session must not enter read-time job pruning")

    monkeypatch.setattr(store, "prune_stale_active_jobs", unexpected_prune)

    updated = store.update_session(session["session_id"], title="Visible terminal result")

    assert updated["title"] == "Visible terminal result"


def test_job_reconciler_can_persist_through_update_session_once(tmp_path, monkeypatch) -> None:
    session = _create_session(tmp_path, monkeypatch)
    calls: list[str] = []

    def reconcile_once(session_id, *, user_id=None, session=None):
        calls.append(session_id)
        return store.update_session(session_id, title="Reconciled")

    monkeypatch.setattr(jobs, "reconcile_running_longform_jobs", reconcile_once)
    monkeypatch.setattr(
        jobs,
        "reconcile_thumbnail_only_active_jobs",
        lambda _session_id, *, user_id=None, session=None: session,
    )
    monkeypatch.setattr(
        jobs,
        "reconcile_terminal_active_jobs",
        lambda _session_id, *, user_id=None, session=None: session,
    )

    reconciled = store.get_session(session["session_id"], reconcile_jobs=True)

    assert calls == [session["session_id"]]
    assert reconciled is not None
    assert reconciled["title"] == "Reconciled"


def test_sync_endpoint_prunes_terminal_job_instead_of_returning_500(tmp_path, monkeypatch) -> None:
    session = _create_session(tmp_path, monkeypatch)
    session_id = session["session_id"]
    store.update_session(
        session_id,
        active_jobs=[
            {
                "job_id": "terminal-short",
                "kind": "shortform",
                "title": "Old short",
                "status": "complete",
            }
        ],
    )
    monkeypatch.setattr(jobs, "shortform_job_terminal_fast", lambda _job_id: True)
    client = _test_app(monkeypatch)

    response = client.post(f"/api/studio-agent/sessions/{session_id}/sync")

    assert response.status_code == 200
    payload = response.json()
    assert payload["synced"] is True
    assert payload["session"]["active_jobs"] == []
    assert payload["session"]["blocked_job_ids"] == ["terminal-short"]


def test_get_session_is_pure_even_when_legacy_sync_query_is_true(
    tmp_path,
    monkeypatch,
) -> None:
    session = _create_session(tmp_path, monkeypatch)
    session_id = session["session_id"]
    store.update_session(
        session_id,
        active_jobs=[
            {
                "job_id": "still-visible",
                "kind": "shortform",
                "title": "Existing production",
                "status": "running",
            }
        ],
    )
    path = tmp_path / f"{session_id}.json"
    before = path.read_bytes()
    monkeypatch.setattr(
        store,
        "force_sync_session",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("GET must not enter force_sync_session")
        ),
    )

    response = _test_app(monkeypatch).get(
        f"/api/studio-agent/sessions/{session_id}?sync_pending=true"
    )

    assert response.status_code == 200
    assert response.json()["session"]["active_jobs"][0]["job_id"] == "still-visible"
    assert path.read_bytes() == before


def test_stream_persists_terminal_assistant_result_after_job_prune(tmp_path, monkeypatch) -> None:
    session = _create_session(tmp_path, monkeypatch)
    session_id = session["session_id"]
    store.update_session(
        session_id,
        active_jobs=[
            {
                "job_id": "terminal-short",
                "kind": "shortform",
                "title": "Old short",
                "status": "complete",
            }
        ],
    )
    run = store.create_run(
        session_id,
        user_text="Fix scenes 2 through 6",
        request_id="repair-terminal-job",
    )
    monkeypatch.setattr(jobs, "shortform_job_terminal_fast", lambda _job_id: True)

    @asynccontextmanager
    async def admitted(**_kwargs):
        yield type("Admission", (), {"mode": "disabled", "as_dict": lambda self: {}})()

    async def fake_turn(current, _user_text, *, emit=None, **_kwargs):
        assert emit is not None
        await emit({"event": "tool_start", "tool": "audit_and_repair_production_scenes"})
        fresh = store.get_session(current["session_id"]) or current
        assistant_text = "The repair ran, but Scenes 4-6 still need attention."
        messages = list(fresh.get("messages") or [])
        messages.append({"role": "assistant", "content": assistant_text})
        store.update_session(current["session_id"], messages=messages, active_jobs=[])
        return {
            "session_id": current["session_id"],
            "assistant_message": assistant_text,
            "pending_actions": [],
            "active_jobs": [],
        }

    monkeypatch.setattr(runner, "studio_agent_slot", admitted)
    monkeypatch.setattr(runner, "_run_turn_impl", fake_turn)

    async def collect() -> list[str]:
        return [
            chunk
            async for chunk in runner.stream_turn(
                session,
                "Fix scenes 2 through 6",
                run_id=run["run_id"],
            )
        ]

    chunks = asyncio.run(collect())
    saved_run = store.get_run(session_id, run["run_id"])
    saved_session = store.get_session(session_id, reconcile_jobs=False)

    assert saved_run is not None
    assert saved_run["status"] == "complete"
    assert saved_run["result"]["assistant_message"] == "The repair ran, but Scenes 4-6 still need attention."
    assert any(event["event"] == "tool_start" for event in saved_run["events"])
    assert any(event["event"] == "done" for event in saved_run["events"])
    assert "event: done" in "".join(chunks)
    assert saved_session is not None
    assert saved_session["active_jobs"] == []
    assert saved_session["messages"][-1]["content"] == "The repair ran, but Scenes 4-6 still need attention."


def test_disconnected_stream_worker_heartbeats_past_stale_window_and_keeps_gate(
    tmp_path,
    monkeypatch,
) -> None:
    session = _create_session(tmp_path, monkeypatch)
    session_id = session["session_id"]
    run = store.create_run(
        session_id,
        user_text="Repair Scene 1",
        request_id="disconnect-worker-heartbeat",
    )
    monkeypatch.setattr(store, "STALE_RUN_AFTER_SEC", 0.01)
    monkeypatch.setattr(store, "RUN_WORKER_HEARTBEAT_SEC", 0.02)
    monkeypatch.setattr(store, "RUN_WORKER_LEASE_SEC", 0.12)

    @asynccontextmanager
    async def admitted(**_kwargs):
        yield type("Admission", (), {"mode": "disabled", "as_dict": lambda self: {}})()

    async def scenario() -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        owner: dict[str, str] = {}

        async def fake_turn(
            current,
            _user_text,
            *,
            emit=None,
            run_id="",
            worker_id="",
            **_kwargs,
        ):
            assert emit is not None
            owner.update({"run_id": run_id, "worker_id": worker_id})
            claimed = store.claim_production_gate(
                current["session_id"],
                command_id="worker-owned-command",
                job_id="job-one",
                run_id=run_id,
                worker_id=worker_id,
            )
            assert claimed is not None
            await emit({
                "event": "studio_command",
                "command": {"command_id": "worker-owned-command"},
            })
            started.set()
            await release.wait()
            store.close_production_gate(
                current["session_id"],
                command_id="worker-owned-command",
                worker_id=worker_id,
            )
            return {
                "session_id": current["session_id"],
                "assistant_message": "Repair finished.",
                "pending_actions": [],
                "active_jobs": [],
            }

        monkeypatch.setattr(runner, "studio_agent_slot", admitted)
        monkeypatch.setattr(runner, "_run_turn_impl", fake_turn)
        stream = runner.stream_turn(
            session,
            "Repair Scene 1",
            run_id=run["run_id"],
        )
        first_event = await anext(stream)
        assert "event: studio_command" in first_event
        await started.wait()
        initial = store.get_session(session_id, reconcile_jobs=False)
        assert initial is not None
        initial_run = next(row for row in initial["runs"] if row["run_id"] == run["run_id"])
        initial_lease_expiry = float(initial_run["worker_lease_expires_at"])

        # Closing the HTTP/SSE consumer must detach only the browser. The
        # backend worker and its heartbeat continue independently.
        await stream.aclose()
        await asyncio.sleep(0.18)

        active = store.list_runs(session_id, active_only=True)
        assert [row["run_id"] for row in active] == [run["run_id"]]
        persisted = store.get_session(session_id, reconcile_jobs=False)
        assert persisted is not None
        persisted_run = next(row for row in persisted["runs"] if row["run_id"] == run["run_id"])
        assert persisted_run["status"] == "stream_disconnected"
        assert float(persisted_run["worker_lease_expires_at"]) > initial_lease_expiry
        assert persisted["active_command_id"] == "worker-owned-command"
        assert persisted["active_command_run_id"] == owner["run_id"]
        assert persisted["active_command_worker_id"] == owner["worker_id"]
        assert store.claim_production_gate(
            session_id,
            command_id="second-command",
            job_id="job-one",
        ) is None

        release.set()
        deadline = asyncio.get_running_loop().time() + 2
        while asyncio.get_running_loop().time() < deadline:
            finished = store.get_session(session_id, reconcile_jobs=False)
            finished_run = next(
                row for row in finished["runs"] if row["run_id"] == run["run_id"]
            )
            if finished_run["status"] == "complete":
                assert finished["production_gate_open"] is False
                return
            await asyncio.sleep(0.01)
        raise AssertionError("detached backend worker did not finish")

    asyncio.run(scenario())


def test_run_turn_persists_user_prompt_before_semantic_tool_execution(tmp_path, monkeypatch) -> None:
    session = _create_session(tmp_path, monkeypatch)
    prompt = "Scenes 2-6 do not adhere to the script. Please fix them."
    persisted_prompts: list[str] = []

    async def semantic_command(*, session, **_kwargs):
        persisted = store.get_session(session["session_id"], reconcile_jobs=False)
        assert persisted is not None
        user_messages = [
            str(message.get("content") or "")
            for message in persisted.get("messages") or []
            if message.get("role") == "user"
        ]
        persisted_prompts.extend(user_messages)
        return {
            "session_id": session["session_id"],
            "assistant_message": "Repair result",
            "pending_actions": [],
            "active_jobs": [],
        }

    monkeypatch.setattr(runner, "_apply_model_agnostic_studio_command", semantic_command)
    monkeypatch.setattr(runner.training_capture, "capture_event", lambda *_args, **_kwargs: None)

    result = asyncio.run(runner._run_turn_impl(session, prompt, agent_mode="studio"))

    assert result["assistant_message"] == "Repair result"
    assert persisted_prompts[-1] == prompt


def test_scene_repair_instruction_does_not_replace_current_production_title() -> None:
    title = "Why Men Love Bomb Then Disappear"
    prompt = (
        "So then fix the errors and do a total repair for scenes two, three, four, five, and six, please. "
        "Make sure they adhere to their prompts and the script of the video."
    )
    session = {"conversation_intent": {"locked_title": title}}
    messages = [{"role": "user", "content": prompt}]

    assert store._requested_title_from_user_text(prompt) == ""
    assert store.resolve_current_production_target(session, messages) == title
