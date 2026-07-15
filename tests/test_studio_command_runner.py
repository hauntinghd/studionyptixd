from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager

from studio_agent import command_execution, command_postconditions, model_registry, runner
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
                "duration_sec": 5.0,
                "narration": f"Narration beat {index + 1}",
                "scene_action": f"Visual action {index + 1}",
            }
            for index in range(6)
        ],
    }


def test_runner_routes_direct_defect_through_typed_repair_and_clears_pending(monkeypatch):
    text = "Scenes 2 through 6 do not perfectly adhere to the prompt and script of the short."
    stored = {
        "session_id": "sa_runner_repair",
        "user_id": "user_test",
        "agent_mode": "studio",
        "approval_mode": "confirm",
        "content_format": "short",
        "updated_at": 123.0,
        "messages": [{"role": "user", "content": text}],
        "active_jobs": [{"job_id": JOB_ID, "kind": "shortform", "title": "Ghosted"}],
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
    monkeypatch.setattr(runner.store, "get_session", lambda _sid: stored)

    def update_session(_sid, **updates):
        stored.update(updates)
        return stored

    monkeypatch.setattr(runner.store, "update_session", update_session)

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
    assert result["postcondition_verdict"]["status"] == "passed"
    assert result["postcondition_verdict"]["safe_claim"] == "completed"
    assert stored["pending_scene_repair"] == {}
    assert "Every unselected scene was verified unchanged" in result["assistant_message"]


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
