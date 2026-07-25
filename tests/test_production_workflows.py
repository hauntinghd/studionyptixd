from __future__ import annotations

import time

import pytest

from studio_agent import production_workflows, store


def _session(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path)
    return store.create_session(user_id="creator-1", model="claude-haiku-4-5")


def _authority(session_id: str, command_id: str = "ship-command") -> dict:
    return {
        "schema": "studio.production-command.v2",
        "command_id": command_id,
        "user_id": "creator-1",
        "session_id": session_id,
        "source": "server_workflow",
        "request_sha256": "request-hash",
        "execution_quote": "animate them and make the finished video",
        "state_revision": 4,
        "issued_at": time.time(),
    }


def _root_mutation(command_id: str = "ship-command") -> dict:
    return {
        "schema": "studio.production-mutation.v2",
        "mutation_id": f"workflow_{command_id}",
        "action": "ship_existing_short",
        "tool_name": "production_workflow",
        "target_kind": "shortform",
        "target_id": "short-1",
        "scene_indices": [0, 1],
        "arguments_sha256": "arguments-hash",
        "authorized_at": time.time(),
        "command_envelope": {"action": "ship_existing_short"},
    }


def _create_workflow(tmp_path, monkeypatch):
    session = _session(tmp_path, monkeypatch)
    workflow, created = store.create_shortform_ship_workflow(
        session["session_id"],
        authority=_authority(session["session_id"]),
        root_mutation=_root_mutation(),
        job_id="short-1",
        scene_indices=[0, 1],
        animation_scene_indices=[1],
        animate=True,
    )
    assert created is True
    claimed = store.claim_production_workflow(
        session["session_id"],
        workflow["workflow_id"],
        lease_owner="test-owner",
        lease_seconds=60,
    )
    assert claimed is not None
    return session, claimed


def test_ship_workflow_never_finalizes_before_animation_postcondition(
    tmp_path,
    monkeypatch,
):
    session, workflow = _create_workflow(tmp_path, monkeypatch)
    calls: list[str] = []
    approval_checks = iter([False, True])
    animation_ready = {"value": False}
    final_snapshot = {
        "job_id": "short-1",
        "kind": "shortform",
        "status": "complete",
        "stage": "complete",
        "ready_to_post": True,
        "download_url": "/api/studio-agent/jobs/short-1/media?kind=shortform",
    }

    monkeypatch.setattr(
        production_workflows,
        "_approval_is_durable",
        lambda _workflow: next(approval_checks),
    )
    monkeypatch.setattr(
        production_workflows,
        "_animation_is_durable",
        lambda _workflow: animation_ready["value"],
    )
    monkeypatch.setattr(
        production_workflows,
        "_missing_animation_indices",
        lambda _workflow: [1],
    )
    monkeypatch.setattr(
        production_workflows,
        "_finalize_preflight",
        lambda _job_id: {"status": "ready"},
    )
    snapshots = {"value": {"status": "running", "running": True}}
    monkeypatch.setattr(
        production_workflows,
        "_job_snapshot",
        lambda _job_id: dict(snapshots["value"]),
    )
    monkeypatch.setattr(production_workflows, "_root_transition", lambda *_args, **_kwargs: None)

    def execute(current, owner, *, step, tool_name, arguments):
        calls.append(tool_name)
        receipts = dict(current.get("step_receipts") or {})
        receipts[step] = {"status": "accepted", "arguments": dict(arguments)}
        updated = store.update_production_workflow(
            session["session_id"],
            current["workflow_id"],
            lease_owner=owner,
            fields={"step_receipts": receipts},
        )
        assert updated is not None
        return updated, {"status": "accepted"}

    monkeypatch.setattr(production_workflows, "_execute_tool_step", execute)

    workflow, _ = production_workflows.advance_shortform_ship_workflow(
        workflow,
        "test-owner",
    )
    assert workflow is not None and workflow["stage"] == "animate"
    assert calls == ["set_production_scenes_animate"]

    workflow, _ = production_workflows.advance_shortform_ship_workflow(
        workflow,
        "test-owner",
    )
    assert workflow is not None and workflow["stage"] == "wait_animation"
    assert calls == ["set_production_scenes_animate", "animate_production_scenes"]

    workflow, _ = production_workflows.advance_shortform_ship_workflow(
        workflow,
        "test-owner",
    )
    assert workflow is not None and workflow["stage"] == "wait_animation"
    assert "finalize_production" not in calls

    animation_ready["value"] = True
    workflow, _ = production_workflows.advance_shortform_ship_workflow(
        workflow,
        "test-owner",
    )
    assert workflow is not None and workflow["stage"] == "finalize"
    workflow, _ = production_workflows.advance_shortform_ship_workflow(
        workflow,
        "test-owner",
    )
    assert workflow is not None and workflow["stage"] == "wait_finalize"
    assert calls[-1] == "finalize_production"

    snapshots["value"] = final_snapshot
    workflow, _ = production_workflows.advance_shortform_ship_workflow(
        workflow,
        "test-owner",
    )
    assert workflow is None
    persisted = store.get_session(
        session["session_id"],
        user_id="creator-1",
        reconcile_jobs=False,
        _prune_active_jobs=False,
    )
    assert persisted is not None
    finished = persisted["production_workflows"][0]
    assert finished["status"] == "completed"
    assert persisted["latest_production_command"]["status"] == "completed"
    root_step = next(
        step
        for step in persisted["latest_production_command"]["steps"]
        if step["mutation_id"] == _root_mutation()["mutation_id"]
    )
    assert root_step["status"] == "completed"
    delivered = [
        message
        for message in persisted["messages"]
        if message.get("productionWorkflowId") == finished["workflow_id"]
    ]
    assert len(delivered) == 1
    assert delivered[0]["jobDeliverable"]["ready_to_post"] is True


def test_ship_workflow_is_idempotent_per_command_and_job(tmp_path, monkeypatch):
    session = _session(tmp_path, monkeypatch)
    args = {
        "authority": _authority(session["session_id"]),
        "root_mutation": _root_mutation(),
        "job_id": "short-1",
        "scene_indices": [0, 1],
        "animation_scene_indices": [1],
        "animate": True,
    }

    first, first_created = store.create_shortform_ship_workflow(
        session["session_id"],
        **args,
    )
    replay, replay_created = store.create_shortform_ship_workflow(
        session["session_id"],
        **args,
    )

    assert first_created is True
    assert replay_created is False
    assert replay["workflow_id"] == first["workflow_id"]
    persisted = store.get_session(
        session["session_id"],
        reconcile_jobs=False,
        _prune_active_jobs=False,
    )
    assert persisted is not None
    assert len(persisted["production_workflows"]) == 1

    with pytest.raises(RuntimeError, match="different ship workflow"):
        store.create_shortform_ship_workflow(
            session["session_id"],
            **{**args, "scene_indices": [0]},
        )


def test_pending_workflow_can_be_reclaimed_after_process_restart(
    tmp_path,
    monkeypatch,
):
    session = _session(tmp_path, monkeypatch)
    workflow, _ = store.create_shortform_ship_workflow(
        session["session_id"],
        authority=_authority(session["session_id"]),
        root_mutation=_root_mutation(),
        job_id="short-1",
        scene_indices=[0, 1],
        animation_scene_indices=[1],
        animate=True,
    )
    first = store.claim_production_workflow(
        session["session_id"],
        workflow["workflow_id"],
        lease_owner="old-process",
        lease_seconds=60,
    )
    assert first is not None
    assert store.claim_production_workflow(
        session["session_id"],
        workflow["workflow_id"],
        lease_owner="new-process",
        lease_seconds=60,
    ) is None
    expired = store.update_production_workflow(
        session["session_id"],
        workflow["workflow_id"],
        lease_owner="old-process",
        fields={"lease_expires_at": time.time() - 1},
    )
    assert expired is not None

    resumed = store.claim_production_workflow(
        session["session_id"],
        workflow["workflow_id"],
        lease_owner="new-process",
        lease_seconds=60,
    )

    assert resumed is not None
    assert resumed["stage"] == "approve"
    assert resumed["lease_owner"] == "new-process"


def test_animation_restart_reuses_the_persisted_exact_arguments(
    tmp_path,
    monkeypatch,
):
    session, workflow = _create_workflow(tmp_path, monkeypatch)
    persisted_arguments = {"job_id": "short-1", "scene_indices": [0, 1]}
    workflow = store.update_production_workflow(
        session["session_id"],
        workflow["workflow_id"],
        lease_owner="test-owner",
        fields={
            "stage": "animate",
            "step_receipts": {
                "animate": {
                    "status": "executing",
                    "arguments": persisted_arguments,
                }
            },
        },
    )
    assert workflow is not None
    monkeypatch.setattr(production_workflows, "_animation_is_durable", lambda _row: False)
    monkeypatch.setattr(
        production_workflows,
        "_missing_animation_indices",
        lambda _row: (_ for _ in ()).throw(AssertionError("must not recompute retry scope")),
    )
    monkeypatch.setattr(production_workflows, "_root_transition", lambda *_args, **_kwargs: None)
    observed: list[dict] = []

    def execute(current, owner, *, step, tool_name, arguments):
        observed.append(dict(arguments))
        receipts = dict(current.get("step_receipts") or {})
        receipts[step] = {"status": "accepted", "arguments": dict(arguments)}
        updated = store.update_production_workflow(
            session["session_id"],
            current["workflow_id"],
            lease_owner=owner,
            fields={"step_receipts": receipts},
        )
        assert updated is not None
        return updated, {"status": "accepted"}

    monkeypatch.setattr(production_workflows, "_execute_tool_step", execute)

    advanced, _ = production_workflows.advance_shortform_ship_workflow(
        workflow,
        "test-owner",
    )

    assert advanced is not None and advanced["stage"] == "wait_animation"
    assert observed == [persisted_arguments]


def test_completion_message_is_exactly_once_on_replay(tmp_path, monkeypatch):
    session, workflow = _create_workflow(tmp_path, monkeypatch)
    snapshot = {
        "job_id": "short-1",
        "status": "complete",
        "ready_to_post": True,
        "download_url": "/media",
    }
    for _ in range(2):
        finished = store.finish_production_workflow(
            session["session_id"],
            workflow["workflow_id"],
            lease_owner="test-owner",
            succeeded=True,
            assistant_text="Finished.",
            snapshot=snapshot,
        )
        assert finished is not None

    persisted = store.get_session(
        session["session_id"],
        reconcile_jobs=False,
        _prune_active_jobs=False,
    )
    assert persisted is not None
    delivered = [
        message
        for message in persisted["messages"]
        if message.get("productionWorkflowId") == workflow["workflow_id"]
    ]
    assert len(delivered) == 1

    late_failure = store.finish_production_workflow(
        session["session_id"],
        workflow["workflow_id"],
        lease_owner="stale-worker",
        succeeded=False,
        assistant_text="Late failure must not replace success.",
        snapshot={"job_id": "short-1", "status": "failed"},
        error="late stale worker",
    )
    assert late_failure is not None
    assert late_failure["status"] == "completed"


def test_child_receipt_cannot_complete_root_ship_command(tmp_path, monkeypatch):
    session = _session(tmp_path, monkeypatch)
    authority = _authority(session["session_id"])
    root = _root_mutation()
    store.record_production_command_transition(
        session["session_id"],
        authority=authority,
        mutation=root,
        transition="authorized",
    )
    child = {
        "schema": "studio.production-mutation.v2",
        "mutation_id": "mut-animate",
        "action": "animate_scenes",
        "tool_name": "animate_production_scenes",
        "target_kind": "shortform",
        "target_id": "short-1",
        "scene_indices": [1],
        "arguments_sha256": "child-hash",
        "authorized_at": time.time(),
    }

    after_child = store.record_production_command_transition(
        session["session_id"],
        authority=authority,
        mutation=child,
        transition="completed",
    )
    assert after_child is not None
    assert after_child["status"] == "running"

    after_root = store.record_production_command_transition(
        session["session_id"],
        authority=authority,
        mutation=root,
        transition="completed",
    )
    assert after_root is not None
    assert after_root["status"] == "completed"


def test_terminal_admission_run_cannot_release_a_live_workflow_gate() -> None:
    session = {
        "production_workflows": [{
            "command_id": "ship-command",
            "status": "running",
        }],
        "runs": [{
            "status": "complete",
            "events": [{
                "data": {"command_id": "ship-command"},
            }],
        }],
    }

    assert store._terminal_run_owns_production_gate(session, "ship-command") is False


def test_stale_worker_cannot_flip_the_root_command(monkeypatch) -> None:
    transitions: list[str] = []
    monkeypatch.setattr(
        store,
        "finish_production_workflow",
        lambda *_args, **_kwargs: {"status": "completed"},
    )
    monkeypatch.setattr(
        production_workflows,
        "_root_transition",
        lambda _workflow, transition, **_kwargs: transitions.append(transition),
    )
    workflow = {
        "session_id": "sa_test",
        "workflow_id": "ship_test",
        "command_id": "ship-command",
    }

    production_workflows._fail(
        workflow,
        "expired-owner",
        "late failure",
        {"status": "failed"},
    )

    assert transitions == []
