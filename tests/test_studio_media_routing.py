from __future__ import annotations

import json

from studio_agent import anti_hallucination, idempotent_mutations, jobs, production_budget, store
from studio_agent.runpod_contract import RUNPOD_PRODUCTION_TOOL_ALLOWLIST


REPAIR_TOOL = "audit_and_repair_production_scenes"


def test_awaiting_review_snapshot_keeps_durable_production_title(tmp_path, monkeypatch) -> None:
    job_id = "repair-review-job"
    output_name = "shortform-output"
    workspace = tmp_path / output_name / job_id
    workspace.mkdir(parents=True)
    title = 'Scene Blueprint: "Why Men Lose Interest After The Chase Ends" (30s)'
    (workspace / "job_spec.json").write_text(
        json.dumps({"topic": title, "scene_count": 1}),
        encoding="utf-8",
    )
    (workspace / "result.json").write_text(
        json.dumps({"status": "awaiting_animation_review"}),
        encoding="utf-8",
    )
    (workspace / "scenes.json").write_text(
        json.dumps([{
            "index": 0,
            "status": "clip_ready",
            "approved_for_video": True,
            "approved_for_animation": True,
            "clip_rel": "clips/b00.mp4",
        }]),
        encoding="utf-8",
    )
    monkeypatch.setattr(jobs, "ROOT", tmp_path)
    monkeypatch.setattr(jobs, "SKELETON_OUTPUT", output_name)

    snapshot = jobs._shortform_status(job_id)

    assert snapshot["status"] == "awaiting_approval"
    assert snapshot["stage"] == "awaiting_animation_review"
    assert snapshot["title"] == title


def test_media_route_revision_changes_only_when_picker_changes(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path)
    session = store.create_session(
        user_id="route-user",
        model="test-model",
        image_model="grok_imagine",
        video_model="grok_imagine_video",
    )
    session_id = session["session_id"]

    assert store.media_route_snapshot(session) == {
        "revision": 1,
        "image_model_id": "grok_imagine",
        "video_model": "grok_imagine_video",
        "updated_at": session["media_route_updated_at"],
    }

    unchanged = store.update_session(session_id, title="Conversation only")
    assert unchanged["media_route_revision"] == 1

    image_switched = store.update_session(session_id, image_model="seedream_edit")
    assert image_switched["media_route_revision"] == 2
    assert store.media_route_snapshot(image_switched)["image_model_id"] == "seedream_edit"

    same_picker = store.update_session(session_id, image_model="seedream_edit")
    assert same_picker["media_route_revision"] == 2

    video_switched = store.update_session(session_id, video_model="seedance")
    assert video_switched["media_route_revision"] == 3
    assert store.media_route_snapshot(video_switched)["video_model"] == "seedance"


def test_stale_session_writer_cannot_roll_back_a_newer_media_route(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path)
    session = store.create_session(
        user_id="route-user",
        model="test-model",
        image_model="grok_imagine",
        video_model="grok_imagine_video",
    )
    stale = dict(session)

    switched = store.update_session(
        session["session_id"],
        image_model="seedream_edit",
        video_model="seedance",
    )
    assert switched["media_route_revision"] == 2

    stale["title"] = "Late heartbeat snapshot"
    store._save(stale)
    persisted = store.get_session(session["session_id"], reconcile_jobs=False)

    assert persisted is not None
    assert persisted["media_route_revision"] == 2
    assert persisted["image_model"] == "seedream_edit"
    assert persisted["video_model"] == "seedance"


def test_production_gate_is_atomic_and_only_its_owner_can_close_it(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path)
    session = store.create_session(user_id="gate-user", model="test-model")
    session_id = session["session_id"]

    first = store.claim_production_gate(
        session_id,
        command_id="command-one",
        job_id="job-one",
    )
    assert first is not None
    assert first["production_gate_open"] is True
    assert first["active_command_id"] == "command-one"

    assert store.claim_production_gate(
        session_id,
        command_id="command-two",
        job_id="job-one",
    ) is None

    not_owner = store.close_production_gate(
        session_id,
        command_id="command-two",
    )
    assert not_owner is not None
    assert not_owner["production_gate_open"] is True
    assert not_owner["active_command_id"] == "command-one"

    closed = store.close_production_gate(
        session_id,
        command_id="command-one",
    )
    assert closed is not None
    assert closed["production_gate_open"] is False
    assert closed["active_command_id"] == ""


def test_terminal_run_releases_orphaned_production_gate_for_retry(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path)
    session = store.create_session(user_id="gate-user", model="test-model")
    session_id = session["session_id"]
    assert store.claim_production_gate(
        session_id,
        command_id="deploy-interrupted-command",
        job_id="job-one",
    ) is not None

    # This is the durable state left behind when a deploy kills a repair after
    # tool_start but before the runner's finally block can close its gate.
    store.update_session(
        session_id,
        runs=[{
            "run_id": "run-interrupted",
            "status": "interrupted",
            "events": [{
                "event": "studio_command",
                "data": {"command": {"command_id": "deploy-interrupted-command"}},
            }],
        }],
    )

    retried = store.claim_production_gate(
        session_id,
        command_id="retry-command",
        job_id="job-one",
    )
    assert retried is not None
    assert retried["production_gate_open"] is True
    assert retried["active_command_id"] == "retry-command"


def test_scene_repair_is_registered_as_a_guarded_billable_mutation() -> None:
    # Repair is synchronous because typed postconditions verify immediately;
    # RunPod's accepted-then-sync bridge cannot safely satisfy that contract.
    assert REPAIR_TOOL not in RUNPOD_PRODUCTION_TOOL_ALLOWLIST
    assert REPAIR_TOOL in idempotent_mutations.LOCAL_IDEMPOTENT_TOOLS
    assert REPAIR_TOOL in production_budget.EXPENSIVE_TOOLS
    assert REPAIR_TOOL in production_budget.APPROVAL_REQUIRED_TOOLS
    assert REPAIR_TOOL in anti_hallucination._ACTION_TOOLS
    assert REPAIR_TOOL in anti_hallucination._PRODUCTION_TOOLS


def test_scene_repair_budget_estimates_selected_stills_and_animation() -> None:
    estimate = production_budget.estimate_tool_cost(
        REPAIR_TOOL,
        {
            "scene_indices": [1, 2, 3, 4, 5],
            "image_model_id": "seedream_edit",
            "video_model": "seedance",
            "seconds_per_scene": 5,
        },
    )

    assert estimate.estimated_usd > 0
    assert estimate.breakdown["scene_count"] == 5
    assert estimate.breakdown["image_edit_count"] == 5
    assert estimate.breakdown["video_seconds"] == 25
    assert production_budget.tool_lane(REPAIR_TOOL) == "render"
