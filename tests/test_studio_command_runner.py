from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager, contextmanager

from studio_agent import command_execution, command_postconditions, model_registry, runner, tools
from studio_agent.command_execution import InMemoryExecutionLedger
from studio_agent.command_validation import SceneAssetFingerprint


JOB_ID = "36067466bd73"


def _snapshot():
    return {
        "job_id": JOB_ID,
        "kind": "shortform",
        "status": "awaiting_approval",
        "stage": "awaiting_animation_review",
        "running": False,
        "current_scene": 6,
        "total_scenes": 6,
        "title": "The Night Ghosted Her and Here's What Actually Happened",
        "scenes": [
            {
                "index": index,
                "status": "clip_ready",
                "approved_for_video": True,
                "approved_for_animation": True,
                "animate": True,
                "has_clip": True,
                "qa_stale": False,
                "visual_qa": {"status": "pass", "pass": True},
                "duration_sec": 5.0,
                "narration": f"Narration beat {index + 1}",
                "scene_action": f"Visual action {index + 1}",
            }
            for index in range(6)
        ],
    }


def test_partial_scene_repair_message_reports_only_verified_successes():
    message = runner._scene_repair_failure_message(
        [2, 3, 4, 5, 6],
        {
            "ok": False,
            "repaired_stills": [5],
            "repaired_animations": [1, 2, 3, 4],
            "failed": [3, 4, 5],
        },
        "tool failed",
    )

    assert "re-animated Scene(s) 2, 3" in message
    assert "Scene(s) 4, 5, 6 still failed" in message
    assert "not claiming those scenes are fixed" in message
    assert "rebuilt stills" not in message


def test_repair_reconciliation_fails_closed_without_fresh_aggregate_qa(tmp_path):
    workspace = tmp_path / "stale-repair"
    workspace.mkdir()
    (workspace / "scenes.json").write_text(
        json.dumps([
            {
                "index": 0,
                "status": "qa_blocked",
                "qa_stale": True,
                "approved_for_video": False,
                "approved_for_animation": False,
                "clip_rel": "clips/b00.mp4",
                "still_rel": "stills/b00.png",
                "still_qa": {"status": "pass", "pass": True},
            }
        ]),
        encoding="utf-8",
    )
    (workspace / "result.json").write_text(
        json.dumps({"status": "awaiting_animation_review", "approved_scene_count": 1}),
        encoding="utf-8",
    )

    failed = tools._reconcile_audit_repair_state(
        workspace,
        job_id="stale-repair",
        selected=[0],
        failed=[],
        reports=[{"scene_index": 0, "status": "repaired_correspondence"}],
        repaired_stills=[0],
        repaired_animations=[],
    )

    result = json.loads((workspace / "result.json").read_text(encoding="utf-8"))
    assert failed == [0]
    assert result["status"] == "failed"
    assert result["repair_status"] == "failed"
    assert result["approved_scene_count"] == 0
    assert result["qa_blocked"] is True
    assert result["repair_failure_details"][0]["stage"] == "aggregate_qa"


def test_failed_owned_short_remains_repairable_from_durable_command(tmp_path, monkeypatch):
    workspace = tmp_path / JOB_ID
    workspace.mkdir()
    (workspace / "job_spec.json").write_text(
        json.dumps({"user_id": "user_test", "topic": "Ghosted"}),
        encoding="utf-8",
    )
    (workspace / "scenes.json").write_text(
        json.dumps([{"index": index} for index in range(6)]),
        encoding="utf-8",
    )
    snapshot = {"status": "failed", "stage": "failed", "total_scenes": 6}
    monkeypatch.setattr(tools, "_shortform_workspace", lambda _job_id: workspace)
    monkeypatch.setattr(runner, "get_job_snapshot", lambda *_args, **_kwargs: dict(snapshot))

    session = {
        "user_id": "user_test",
        "active_jobs": [],
        "blocked_job_ids": [JOB_ID],
        "messages": [],
        "last_studio_command": {
            "receipt": {
                "status": "failed",
                "target_job_id": JOB_ID,
                "result": {"job_id": JOB_ID},
            }
        },
    }

    assert runner._verified_scene_repair_command_target(session, JOB_ID) is True
    assert runner._find_repairable_shortform_job(session) == JOB_ID

    snapshot.update({"status": "cancelled", "stage": "cancelled"})
    assert runner._verified_scene_repair_command_target(session, JOB_ID) is False
    assert runner._find_repairable_shortform_job(session) is None

    snapshot.update({"status": "failed", "stage": "failed"})
    assert runner._verified_scene_repair_command_target(
        {**session, "user_id": "different_user"},
        JOB_ID,
    ) is False


def test_runner_routes_direct_defect_through_typed_repair_and_clears_pending(monkeypatch):
    text = "Scenes 2 through 6 do not perfectly adhere to the prompt and script of the short."
    stored = {
        "session_id": "sa_runner_repair",
        "user_id": "user_test",
        "agent_mode": "studio",
        "approval_mode": "confirm",
        "content_format": "short",
        "image_model": "seedream_edit",
        "video_model": "seedance",
        "media_route_revision": 4,
        "updated_at": 123.0,
        "messages": [{"role": "user", "content": text}],
        "active_jobs": [{"job_id": JOB_ID, "kind": "shortform", "title": "Ghosted"}],
        "blocked_job_ids": [JOB_ID],
        "pending_actions": [],
    }

    monkeypatch.setattr(runner, "_find_repairable_shortform_job", lambda *_args, **_kwargs: JOB_ID)
    monkeypatch.setattr(runner, "_verified_scene_repair_command_target", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runner, "get_job_snapshot", lambda *_args, **_kwargs: _snapshot())
    monkeypatch.setattr(model_registry, "assert_model_selectable", lambda _model: None)
    monkeypatch.setattr(runner.training_capture, "capture_event", lambda *_args, **_kwargs: None)

    async def fake_chat_completion(**_kwargs):
        # Malformed provider output proves the deterministic safety layer is
        # provider-independent and still uses the selected model lane.
        return {
            "id": "response-runner-test",
            "model": "provider-selected-model",
            "provider": "test-provider",
            "choices": [{"message": {"content": "not valid command json"}}],
        }

    monkeypatch.setattr(runner.openrouter, "chat_completion", fake_chat_completion)
    monkeypatch.setattr(runner.openrouter, "usage_from_response", lambda _response: {})

    async def fake_bill(**_kwargs):
        return {
            "credits_charged": 0,
            "provider_usd": 0.0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
        }

    monkeypatch.setattr(runner, "_bill_studio_command_compiler", fake_bill)

    @asynccontextmanager
    async def fake_slot(**_kwargs):
        yield

    monkeypatch.setattr(runner, "studio_agent_slot", fake_slot)

    def fingerprints(_workspace, scene_numbers):
        return [
            SceneAssetFingerprint(
                scene_number=number,
                still_sha256=f"still-{number}",
                clip_sha256=f"clip-{number}",
            )
            for number in scene_numbers
        ]

    monkeypatch.setattr(command_postconditions, "fingerprint_workspace_scenes", fingerprints)
    ledger = InMemoryExecutionLedger()
    real_execute = command_execution.execute_validated_command

    def execute_with_test_ledger(validation, **kwargs):
        return real_execute(validation, ledger=ledger, **kwargs)

    monkeypatch.setattr(command_execution, "execute_validated_command", execute_with_test_ledger)
    real_to_thread = asyncio.to_thread
    threaded_calls = []

    async def tracked_to_thread(function, /, *args, **kwargs):
        threaded_calls.append(function)
        return await real_to_thread(function, *args, **kwargs)

    monkeypatch.setattr(runner.asyncio, "to_thread", tracked_to_thread)
    calls = []

    def fake_tool(name, arguments, **kwargs):
        calls.append((name, arguments, kwargs))
        return json.dumps(
            {
                "ok": True,
                "job_id": JOB_ID,
                "audited": [1, 2, 3, 4, 5],
                "passed_without_changes": [1, 3, 4, 5],
                "repaired_stills": [2],
                "repaired_animations": [2],
                "failed": [],
            }
        )

    monkeypatch.setattr(runner, "execute_tool_logged", fake_tool)
    monkeypatch.setattr(runner.store, "get_session", lambda _sid, **_kwargs: stored)

    def update_session(_sid, **updates):
        stored.update(updates)
        return stored

    monkeypatch.setattr(runner.store, "update_session", update_session)

    def claim_gate(_sid, *, command_id, job_id):
        stored.update({
            "agent_mode": "studio",
            "interaction_state": "production",
            "production_gate_open": True,
            "active_command_id": command_id,
            "active_command_job_id": job_id,
        })
        return stored

    def close_gate(_sid, *, command_id, interaction_state="verification"):
        if stored.get("active_command_id") == command_id:
            stored.update({
                "interaction_state": interaction_state,
                "production_gate_open": False,
                "active_command_id": "",
                "active_command_job_id": "",
            })
        return stored

    @contextmanager
    def job_lock(_job_id):
        yield

    monkeypatch.setattr(runner.store, "claim_production_gate", claim_gate)
    monkeypatch.setattr(runner.store, "close_production_gate", close_gate)
    monkeypatch.setattr(runner.store, "production_job_mutation_lock", job_lock)

    result = asyncio.run(
        runner._apply_model_agnostic_studio_command(
            session=stored,
            user_id="user_test",
            user_text=text,
            content_format="short",
            model="any-selectable-model",
            emit=None,
            membership_plan="owner",
            billing_profile={"unlimited": True},
            approval_mode="confirm",
            reasoning_depth="fast",
            reply_to=None,
        )
    )

    assert result is not None
    assert calls[0][0] == "audit_and_repair_production_scenes"
    assert calls[0][1]["job_id"] == JOB_ID
    assert calls[0][1]["scene_indices"] == [1, 2, 3, 4, 5]
    assert calls[0][1]["reason"] == text
    assert calls[0][1]["image_model_id"] == "seedream_edit"
    assert calls[0][1]["video_model"] == "seedance"
    assert calls[0][1]["media_route_revision"] == 4
    assert result["postcondition_verdict"]["status"] == "passed"
    assert result["postcondition_verdict"]["safe_claim"] == "completed"
    assert stored["pending_scene_repair"] == {}
    assert stored["agent_mode"] == "studio"
    assert stored["interaction_state"] == "verification"
    assert stored["production_gate_open"] is False
    assert stored["active_command_id"] == ""
    assert len(threaded_calls) == 1
    assert threaded_calls[0].__name__ == "_execute_with_job_lock"
    assert "Every unselected scene was verified unchanged" in result["assistant_message"]
    persisted_reply = stored["messages"][-1]
    assert persisted_reply["role"] == "assistant"
    assert persisted_reply["jobDeliverable"]["job_id"] == JOB_ID
    assert len(persisted_reply["jobDeliverable"]["scenes"]) == 6
    assert stored["blocked_job_ids"] == []
    repaired_track = next(job for job in stored["active_jobs"] if job["job_id"] == JOB_ID)
    assert repaired_track["title"] == "The Night Ghosted Her and Here's What Actually Happened"
    assert repaired_track["status"] == "awaiting_approval"
    assert repaired_track["stage"] == "awaiting_animation_review"


def test_pending_repair_cancellation_clears_state_without_model_or_tool(monkeypatch):
    stored = {
        "session_id": "sa_runner_cancel",
        "user_id": "user_test",
        "messages": [{"role": "user", "content": "No, never mind."}],
        "active_jobs": [{"job_id": JOB_ID, "kind": "shortform"}],
        "pending_actions": [],
        "pending_scene_repair": {
            "job_id": JOB_ID,
            "scene_numbers": [2, 3, 4, 5, 6],
            "repair_scope": "narrative_alignment",
            "instruction": "Are scenes 2 through 6 wrong?",
            "execution_requested": False,
        },
    }
    monkeypatch.setattr(runner.store, "get_session", lambda _sid: stored)

    def update_session(_sid, **updates):
        stored.update(updates)
        return stored

    monkeypatch.setattr(runner.store, "update_session", update_session)
    monkeypatch.setattr(
        runner.openrouter,
        "chat_completion",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("model must not run")),
    )
    monkeypatch.setattr(
        runner,
        "execute_tool_logged",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("tool must not run")),
    )

    result = asyncio.run(
        runner._apply_model_agnostic_studio_command(
            session=stored,
            user_id="user_test",
            user_text="No, never mind.",
            content_format="short",
            model="any-model",
            emit=None,
            membership_plan="owner",
            billing_profile={"unlimited": True},
            approval_mode="confirm",
            reasoning_depth="fast",
            reply_to=None,
        )
    )
    assert result is not None
    assert result["assistant_message"] == "Understood — I will not repair or change those scenes."
    assert stored["pending_scene_repair"] == {}


def test_unrelated_turn_abandons_pending_repair_without_hijacking_conversation(monkeypatch):
    stored = {
        "session_id": "sa_runner_unrelated",
        "user_id": "user_test",
        "messages": [],
        "pending_scene_repair": {
            "job_id": JOB_ID,
            "scene_numbers": [],
            "repair_scope": "narrative_alignment",
            "instruction": "The scenes are wrong.",
            "execution_requested": True,
        },
    }
    monkeypatch.setattr(runner.store, "get_session", lambda _sid: stored)

    def update_session(_sid, **updates):
        stored.update(updates)
        return stored

    monkeypatch.setattr(runner.store, "update_session", update_session)
    result = asyncio.run(
        runner._apply_model_agnostic_studio_command(
            session=stored,
            user_id="user_test",
            user_text="Let's work on the title instead.",
            content_format="short",
            model="any-model",
            emit=None,
            membership_plan="owner",
            billing_profile={"unlimited": True},
            approval_mode="confirm",
            reasoning_depth="fast",
            reply_to=None,
        )
    )
    assert result is None
    assert stored["pending_scene_repair"] == {}
