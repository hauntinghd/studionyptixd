from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager

from studio_agent import production_workflows, runner, store, tools
from studio_agent.execution_context import production_command_scope


ONE_SCENE_PROOF_COMMIT = (
    'yes make it \N{EM DASH} render that plan for "1 scene first so i can see what it looks like, '
    'and yes we keep the skeleton-anatomy visual style", only 30 seconds'
)


def test_one_scene_first_commit_is_not_routed_as_existing_proof_expansion() -> None:
    assert not store.is_expand_short_request(ONE_SCENE_PROOF_COMMIT)
    assert store.is_hard_production_commit(ONE_SCENE_PROOF_COMMIT)
    assert store._is_visual_proof_request(ONE_SCENE_PROOF_COMMIT)


def test_scene_one_with_explicit_expansion_language_still_routes_to_expand() -> None:
    for prompt in (
        "Scene 1 looks good. Finish the short at 30 seconds.",
        "Keep scene one and make the remaining scenes for a 30 second video.",
        "I approve scene 1. Make the other five scenes.",
    ):
        assert store.is_expand_short_request(prompt)
        assert not store.is_hard_production_commit(prompt)


def test_animate_existing_scenes_and_finish_is_not_proof_expansion() -> None:
    prompt = "animate them and make the finished video"

    assert store.is_bulk_scene_ship_request(prompt)
    assert runner._wants_bulk_scene_ship_request(prompt)
    assert not store.is_expand_short_request(prompt)
    assert not runner._wants_expand_visual_proof_short(prompt)
    assert not store.is_new_production_request(prompt, {"last_production": {}})
    assert runner._effective_agent_mode("plan", prompt) == "studio"


def test_explicit_remaining_scene_scope_stays_on_proof_expansion() -> None:
    prompt = "good, now make the rest of the scenes and animate them"

    assert not store.is_bulk_scene_ship_request(prompt)
    assert store.is_expand_short_request(prompt)
    assert runner._wants_expand_visual_proof_short(prompt)


def test_bulk_ship_preserves_existing_clips_and_animates_only_missing_scenes() -> None:
    snapshot = {
        "job_id": "job-six-scenes",
        "status": "awaiting_approval",
        "scenes": [
            {"index": 0, "has_clip": True},
            {"index": 1, "has_clip": True},
            {"index": 2, "has_clip": False},
            {"index": 3, "has_clip": False},
            {"index": 4, "has_clip": False},
            {"index": 5, "has_clip": False},
        ],
    }

    assert runner._shortform_bulk_ship_plan(snapshot, animate_all=True) == [
        ("set_production_scenes_animate", {"job_id": "job-six-scenes", "animate": True}),
        (
            "animate_production_scenes",
            {"job_id": "job-six-scenes", "scene_indices": [2, 3, 4, 5]},
        ),
        ("finalize_production", {"job_id": "job-six-scenes"}),
    ]


def test_bulk_ship_respects_explicit_animation_negation() -> None:
    prompt = "Do not animate them; make the finished video from the stills."

    assert store.is_bulk_scene_ship_request(prompt)
    assert runner._wants_bulk_scene_ship_request(prompt)
    assert runner._wants_animate_all_in_ship(prompt) is False


def test_bulk_ship_target_prefers_latest_same_session_multiscene_card(tmp_path, monkeypatch) -> None:
    session_id = "sa_current"
    user_id = "user_current"
    proof_id = "proof-old"
    production_id = "production-six"
    workspaces = {}
    for job_id, scene_count in ((proof_id, 1), (production_id, 6)):
        workspace = tmp_path / job_id
        workspace.mkdir()
        (workspace / "job_spec.json").write_text(
            json.dumps({"job_id": job_id, "user_id": user_id, "session_id": session_id}),
            encoding="utf-8",
        )
        (workspace / "scenes.json").write_text(
            json.dumps([{"index": index} for index in range(scene_count)]),
            encoding="utf-8",
        )
        workspaces[job_id] = workspace

    session = {
        "session_id": session_id,
        "user_id": user_id,
        "active_jobs": [],
        "blocked_job_ids": [],
        "messages": [
            {"role": "assistant", "jobDeliverable": {"job_id": proof_id, "kind": "shortform"}},
            {"role": "assistant", "jobDeliverable": {"job_id": production_id, "kind": "shortform"}},
            {"role": "user", "content": "animate them and make the finished video"},
        ],
    }
    snapshots = {
        proof_id: {"job_id": proof_id, "status": "awaiting_approval", "stage": "awaiting_scene_review"},
        production_id: {
            "job_id": production_id,
            "status": "awaiting_approval",
            "stage": "awaiting_animation_review",
            "total_scenes": 6,
        },
    }
    monkeypatch.setattr(tools, "_shortform_workspace", lambda job_id: workspaces[job_id])
    monkeypatch.setattr(runner, "get_job_snapshot", lambda job_id, _kind: dict(snapshots[job_id]))
    monkeypatch.setattr(runner.store, "get_session", lambda _sid, **_kwargs: session)
    monkeypatch.setattr(
        runner.store,
        "claim_production_gate",
        lambda *_args, **_kwargs: session,
    )

    assert runner._recover_shortform_job_from_session(session) == production_id
    assert runner._recover_poll_target(session) == (production_id, "shortform")
    assert runner._recover_bulk_ship_target(session) == (production_id, "shortform")
    assert runner._verified_bulk_ship_command_target(session, proof_id) is False


def test_bulk_ship_never_skips_latest_owned_proof_to_mutate_older_multiscene_job(tmp_path, monkeypatch) -> None:
    session_id = "sa_current"
    user_id = "user_current"
    proof_id = "proof-latest"
    production_id = "production-older"
    workspaces = {}
    for job_id, scene_count in ((production_id, 6), (proof_id, 1)):
        workspace = tmp_path / job_id
        workspace.mkdir()
        (workspace / "job_spec.json").write_text(
            json.dumps({"job_id": job_id, "user_id": user_id, "session_id": session_id}),
            encoding="utf-8",
        )
        (workspace / "scenes.json").write_text(
            json.dumps([{"index": index} for index in range(scene_count)]),
            encoding="utf-8",
        )
        workspaces[job_id] = workspace
    session = {
        "session_id": session_id,
        "user_id": user_id,
        "active_jobs": [],
        "messages": [
            {"role": "assistant", "jobDeliverable": {"job_id": production_id, "kind": "shortform"}},
            {"role": "assistant", "jobDeliverable": {"job_id": proof_id, "kind": "shortform"}},
        ],
    }
    monkeypatch.setattr(tools, "_shortform_workspace", lambda job_id: workspaces[job_id])
    monkeypatch.setattr(
        runner,
        "get_job_snapshot",
        lambda job_id, _kind: {"job_id": job_id, "status": "awaiting_approval", "stage": "awaiting_scene_review"},
    )
    monkeypatch.setattr(runner.store, "get_session", lambda _sid, **_kwargs: session)

    assert runner._recover_bulk_ship_target(session) is None


def test_bulk_ship_rejects_same_user_job_without_same_session_binding(tmp_path, monkeypatch) -> None:
    job_id = "legacy-unbound-six"
    workspace = tmp_path / job_id
    workspace.mkdir()
    (workspace / "job_spec.json").write_text(
        json.dumps({"job_id": job_id, "user_id": "user_current"}),
        encoding="utf-8",
    )
    (workspace / "scenes.json").write_text(
        json.dumps([{"index": index} for index in range(6)]),
        encoding="utf-8",
    )
    session = {"session_id": "sa_current", "user_id": "user_current", "blocked_job_ids": []}
    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: workspace)
    monkeypatch.setattr(
        runner,
        "get_job_snapshot",
        lambda *_args, **_kwargs: {"job_id": job_id, "status": "awaiting_approval"},
    )

    assert runner._verified_bulk_ship_command_target(session, job_id) is False


def test_bulk_ship_hands_mutations_to_durable_backend_workflow(monkeypatch) -> None:
    job_id = "production-six"
    session = {
        "session_id": "sa_bulk_gate",
        "user_id": "user_bulk_gate",
        "active_jobs": [],
        "messages": [{"role": "user", "content": "animate them and make the finished video"}],
    }
    snapshot = {
        "job_id": job_id,
        "kind": "shortform",
        "title": "Six scene video",
        "status": "awaiting_approval",
        "stage": "awaiting_animation_review",
        "scenes": [{"index": index, "has_clip": index < 2} for index in range(6)],
    }
    calls: list[str] = []

    monkeypatch.setattr(runner, "_recover_bulk_ship_target", lambda _session: (job_id, "shortform"))
    monkeypatch.setattr(runner, "get_job_snapshot", lambda *_args, **_kwargs: dict(snapshot))

    def fake_execute(tool_name, _args, **_kwargs):
        calls.append(tool_name)
        if tool_name != "set_production_scenes_animate":
            raise AssertionError("paid animation/finalization must not run after the QA gate fails")
        return json.dumps({
            "ok": False,
            "status": "failed",
            "error": "Still approval blocked by aggregate visual QA. scene 1: narrative mismatch",
        })

    monkeypatch.setattr(runner, "execute_tool_logged", fake_execute)

    @asynccontextmanager
    async def fake_slot(**_kwargs):
        yield

    monkeypatch.setattr(runner, "studio_agent_slot", fake_slot)
    monkeypatch.setattr(runner.store, "get_session", lambda _sid, **_kwargs: session)
    monkeypatch.setattr(
        runner.store,
        "claim_production_gate",
        lambda *_args, **_kwargs: session,
    )
    monkeypatch.setattr(
        runner.store,
        "record_production_command_transition",
        lambda *_args, **_kwargs: {},
    )
    enqueued: list[dict] = []

    def enqueue(**kwargs):
        enqueued.append(kwargs)
        return {
            "workflow_id": "ship-workflow-1",
            "session_id": session["session_id"],
        }, True

    monkeypatch.setattr(production_workflows, "enqueue_shortform_ship_workflow", enqueue)
    monkeypatch.setattr(production_workflows, "schedule_production_workflow", lambda _row: True)

    def update_session(_sid, **updates):
        session.update(updates)
        return session

    monkeypatch.setattr(runner.store, "update_session", update_session)

    with production_command_scope(
        "ship-command-1",
        user_id=session["user_id"],
        session_id=session["session_id"],
        source="server_workflow",
        user_text="animate them and make the finished video",
    ):
        result = asyncio.run(runner._apply_bulk_scene_ship(
            session=session,
            user_id=session["user_id"],
            user_text="animate them and make the finished video",
            content_format="short",
            emit=None,
            membership_plan="pro",
            billing_profile={"unlimited": True},
        ))

    assert result is not None
    assert calls == []
    assert len(enqueued) == 1
    assert enqueued[0]["job_id"] == job_id
    assert enqueued[0]["animation_scene_indices"] == [2, 3, 4, 5]
    assert "backend-owned production command" in result["assistant_message"]
