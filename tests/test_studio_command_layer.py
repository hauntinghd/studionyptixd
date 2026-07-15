from __future__ import annotations

import asyncio
import json

import pytest

from studio_agent.command_execution import (
    ExecutionReceipt,
    InMemoryExecutionLedger,
    execute_validated_command,
)
from studio_agent.command_planner import plan_studio_command
from studio_agent.command_postconditions import verify_execution
from studio_agent.command_state import build_studio_state_context
from studio_agent.command_validation import SceneAssetFingerprint, validate_studio_command


JOB_ID = "36067466bd73"


def _snapshot(*, stage: str = "awaiting_scene_review", scene_count: int = 1, clips: bool = True):
    return {
        "job_id": JOB_ID,
        "kind": "shortform",
        "status": "awaiting_approval" if stage == "awaiting_scene_review" else "running",
        "stage": stage,
        "running": stage not in {"awaiting_scene_review", "complete"},
        "current_scene": scene_count,
        "total_scenes": scene_count,
        "title": "Skeleton psychology short",
        "scenes": [
            {
                "index": index,
                "status": "clip_ready" if clips else "still_ready",
                "approved_for_video": index == 0,
                "approved_for_animation": clips,
                "animate": clips,
                "has_clip": clips,
                "duration_sec": 5.0,
            }
            for index in range(scene_count)
        ],
    }


def _state(snapshot=None, *, session_updates=None):
    snap = snapshot or _snapshot()
    session = {
        "session_id": "sa_test_command_layer",
        "user_id": "user_test",
        "agent_mode": "studio",
        "approval_mode": "confirm",
        "content_format": "short",
        "updated_at": 123.0,
        "active_jobs": [
            {"job_id": JOB_ID, "kind": "shortform", "title": "Skeleton psychology short"}
        ],
    }
    session.update(session_updates or {})
    return build_studio_state_context(
        session,
        expandable_job_id=JOB_ID,
        snapshot_loader=lambda _job_id, _kind: snap,
    )


def _proposal(**updates):
    payload = {
        "action": "conversation",
        "target_source": "none",
        "target_job_id": "",
        "additional_scene_count": 0,
        "target_total_scene_count": 0,
        "preserve_scene_numbers": [],
        "animation_scope": "unspecified",
        "animation_scene_numbers": [],
        "duration_seconds": 0,
        "creative_direction": "",
        "existing_work_approved": False,
        "approval_evidence": "",
        "execution_requested": False,
        "execution_evidence": "",
        "clarification_question": "",
        "confidence": 0.2,
    }
    payload.update(updates)
    return payload


def _tool_response(payload):
    return {
        "id": "response-test",
        "model": "selected-test-model",
        "provider": "test-provider",
        "choices": [
            {
                "message": {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call-test",
                            "type": "function",
                            "function": {
                                "name": "emit_studio_command",
                                "arguments": json.dumps(payload),
                            },
                        }
                    ],
                }
            }
        ],
    }


def _plan(text: str, response, *, capture=None, state=None):
    async def fake_chat_completion(**kwargs):
        if capture is not None:
            capture.update(kwargs)
        return response

    return asyncio.run(
        plan_studio_command(
            text,
            state or _state(),
            model="claude-haiku-test",
            chat_completion=fake_chat_completion,
        )
    )


def test_exact_prompt_is_grounded_to_same_job_plus_five_and_new_scene_animation():
    text = "I like scene one. Let's go ahead and make the other five scenes and animate them."
    capture = {}
    command = _plan(text, _tool_response(_proposal()), capture=capture)

    assert capture["model"] == "claude-haiku-test"
    assert len(capture["tools"]) == 1
    assert capture["tools"][0]["function"]["name"] == "emit_studio_command"
    assert command.action == "expand_existing_short"
    assert command.expand is not None
    assert command.expand.additional_scene_count == 5
    assert command.expand.target_total_scene_count == 6
    assert command.expand.preserve_scene_numbers == [1]
    assert command.expand.animation.scope == "new_scenes"
    assert command.authorization.existing_work_approved is True
    assert command.authorization.execution_requested is True

    validation = validate_studio_command(command, _state(), user_text=text)
    assert validation.can_execute
    assert validation.resolved_action is not None
    assert validation.resolved_action.tool_name == "expand_visual_proof_shortform"
    assert validation.resolved_action.arguments.command_id == command.command_id
    assert validation.resolved_action.arguments.existing_scene_count == 1
    assert validation.resolved_action.arguments.scene_count == 6
    assert validation.resolved_action.arguments.preserve_scene_indices == [0]
    assert validation.resolved_action.arguments.animate_scene_indices == [1, 2, 3, 4, 5]
    assert validation.resolved_action.arguments.animate_policy == "all"
    assert validation.resolved_action.expected.expected_animated_scene_numbers == [2, 3, 4, 5, 6]


def test_contextual_good_now_make_rest_reuses_prior_grounded_count_and_executes():
    prior = "I like scene one. Let's go ahead and make the other five scenes and animate them."
    state = _state(session_updates={
        "production_state": {"advanced_at": 100.0},
        "runs": [{"created_at": 101.0, "message_preview": prior}],
    })
    text = "good, now make the rest of the scenes and animate them"
    model_payload = _proposal(
        action="expand_existing_short",
        target_source="active_job",
        additional_scene_count=12,
        target_total_scene_count=13,
        preserve_scene_numbers=[1],
        animation_scope="all_scenes",
        existing_work_approved=False,
        execution_requested=False,
    )

    command = _plan(text, _tool_response(model_payload), state=state)

    assert state.recent_expansion_additional_scene_count == 5
    assert state.recent_expansion_total_scene_count == 6
    assert command.expand is not None
    assert command.expand.additional_scene_count == 5
    assert command.expand.target_total_scene_count == 6
    assert command.expand.animation.scope == "new_scenes"
    assert command.authorization.existing_work_approved is True
    assert command.authorization.approval_quote.lower() == "good"
    assert command.authorization.execution_requested is True
    assert "now make" in command.authorization.execution_quote.lower()

    validation = validate_studio_command(command, state, user_text=text)
    assert validation.can_execute
    assert validation.resolved_action is not None
    assert validation.resolved_action.tool_name == "expand_visual_proof_shortform"
    assert validation.resolved_action.arguments.scene_count == 6
    assert validation.resolved_action.arguments.animate_scene_indices == [1, 2, 3, 4, 5]


def test_contextual_rest_without_prior_count_never_trusts_model_cardinality():
    state = _state()
    text = "good, now make the rest of the scenes and animate them"
    model_payload = _proposal(
        action="expand_existing_short",
        target_source="active_job",
        additional_scene_count=5,
        target_total_scene_count=6,
        preserve_scene_numbers=[1],
        animation_scope="all_scenes",
        existing_work_approved=True,
        approval_evidence="good",
        execution_requested=True,
        execution_evidence="now make",
    )

    command = _plan(text, _tool_response(model_payload), state=state)

    assert command.expand is not None
    assert command.expand.additional_scene_count is None
    assert command.expand.target_total_scene_count is None
    validation = validate_studio_command(command, state, user_text=text)
    assert not validation.can_execute
    assert validation.clarification is not None
    assert validation.clarification.code == "missing_scene_count"


def test_high_confidence_grounding_removes_model_invented_duration_and_echoed_command():
    text = "I like scene one. Let's go ahead and make the other five scenes and animate them."
    model_payload = _proposal(
        action="expand_existing_short",
        target_source="active_job",
        additional_scene_count=5,
        target_total_scene_count=6,
        preserve_scene_numbers=[1],
        animation_scope="new_scenes",
        duration_seconds=45,
        creative_direction=text,
        existing_work_approved=True,
        approval_evidence="I like scene one",
        execution_requested=True,
        execution_evidence=text,
    )

    command = _plan(text, _tool_response(model_payload))

    assert command.expand is not None
    assert command.expand.duration_seconds is None
    assert command.expand.creative_direction == ""


def test_unrequested_animation_cannot_be_invented_by_model():
    text = "I like scene one. Let's go ahead and make the other five scenes."
    model_payload = _proposal(
        action="expand_existing_short",
        target_source="active_job",
        additional_scene_count=5,
        target_total_scene_count=6,
        preserve_scene_numbers=[1],
        animation_scope="all_scenes",
        existing_work_approved=True,
        approval_evidence="I like scene one",
        execution_requested=True,
        execution_evidence=text,
    )

    command = _plan(text, _tool_response(model_payload))

    assert command.expand is not None
    assert command.expand.animation.scope == "none"


@pytest.mark.parametrize(
    ("phrase", "additional", "total"),
    [
        ("the other seven scenes", 7, 8),
        ("7 more scenes", 7, 8),
        ("the remaining three scenes", 3, 4),
    ],
)
def test_additional_scene_cardinality_is_not_discarded(phrase, additional, total):
    text = f"I approve scene 1. Let's go ahead and make {phrase} and animate them."
    command = _plan(text, _tool_response(_proposal()))
    assert command.expand is not None
    assert command.expand.additional_scene_count == additional
    assert command.expand.target_total_scene_count == total


def test_animation_negation_overrides_model_claim_that_everything_should_animate():
    text = (
        "I like scene one. Let's go ahead and make the other five scenes, "
        "but don't animate them."
    )
    model_payload = _proposal(
        action="expand_existing_short",
        target_source="active_job",
        additional_scene_count=5,
        target_total_scene_count=6,
        preserve_scene_numbers=[1],
        animation_scope="all_scenes",
        existing_work_approved=True,
        approval_evidence="I like scene one",
        execution_requested=True,
        execution_evidence=text,
    )
    command = _plan(text, _tool_response(model_payload))
    assert command.expand is not None
    assert command.expand.animation.scope == "none"
    validation = validate_studio_command(command, _state(), user_text=text)
    assert validation.can_execute
    assert validation.resolved_action.arguments.animate_policy == "none"
    assert validation.resolved_action.expected.expected_animated_scene_numbers == []


def test_malformed_model_output_uses_deterministic_exact_prompt_fallback():
    text = "I like scene one. Let's go ahead and make the other five scenes and animate them."
    malformed = {
        "id": "bad-response",
        "model": "weak-model",
        "choices": [
            {
                "message": {
                    "tool_calls": [
                        {
                            "function": {
                                "name": "emit_studio_command",
                                "arguments": "{this is not valid JSON",
                            }
                        }
                    ]
                }
            }
        ],
    }
    command = _plan(text, malformed)
    assert command.action == "expand_existing_short"
    assert command.expand.additional_scene_count == 5
    assert command.compiler.transport == "deterministic_fallback"


def test_malformed_output_without_high_confidence_request_never_mutates():
    command = _plan("What do you think of the first scene?", {"choices": [{"message": {"content": "oops"}}]})
    assert command.action == "clarify"
    validation = validate_studio_command(
        command,
        _state(),
        user_text="What do you think of the first scene?",
    )
    assert validation.decision == "clarify"
    assert not validation.can_execute


@pytest.mark.parametrize(
    "text",
    [
        "I like scene one. Don't make the other five scenes yet.",
        "I like scene one. Let's not go ahead and make the other five scenes.",
        "I like scene one. Maybe later; do not start the other five scenes now.",
        "I like scene one. Don't actually go ahead and make the other five scenes yet.",
    ],
)
def test_execution_negation_or_deferral_never_authorizes_expansion(text):
    command = _plan(text, _tool_response(_proposal()))
    assert command.authorization.execution_requested is False
    validation = validate_studio_command(command, _state(), user_text=text)
    assert validation.decision == "clarify"
    assert validation.clarification is not None
    assert validation.clarification.code == "execution_deferred"
    assert not validation.can_execute


@pytest.mark.parametrize(
    "text",
    [
        "I don't like scene one. Let's go ahead and make the other five scenes.",
        "I do not approve scene 1. Make the other five scenes.",
        "I don't think I like scene one. Let's go ahead and make the other five scenes.",
        "I'm not sure I like scene one. Let's go ahead and make the other five scenes.",
        "I can't say I approve scene 1. Go ahead and make the other five scenes.",
    ],
)
def test_negated_scene_approval_never_authorizes_expansion(text):
    command = _plan(text, _tool_response(_proposal()))
    assert command.authorization.existing_work_approved is False
    validation = validate_studio_command(command, _state(), user_text=text)
    assert validation.decision == "clarify"
    assert validation.clarification is not None
    assert validation.clarification.code == "proof_not_approved"
    assert not validation.can_execute


def test_hypothetical_expand_question_never_authorizes_production():
    text = "How would you make the other five scenes if I approved scene one?"
    command = _plan(text, _tool_response(_proposal()))
    assert command.authorization.existing_work_approved is False
    assert command.authorization.execution_requested is False
    validation = validate_studio_command(command, _state(), user_text=text)
    assert validation.decision == "clarify"
    assert validation.clarification is not None
    assert validation.clarification.code == "hypothetical_only"
    assert not validation.can_execute


def _validated_exact(*, fingerprints=False):
    text = "I like scene one. Let's go ahead and make the other five scenes and animate them."
    command = _plan(text, _tool_response(_proposal()))
    loader = None
    if fingerprints:
        loader = lambda _job_id, _numbers: [
            SceneAssetFingerprint(scene_number=1, still_sha256="still-before", clip_sha256="clip-before")
        ]
    return validate_studio_command(
        command,
        _state(),
        user_text=text,
        fingerprint_loader=loader,
    )


def test_async_tool_receipt_is_accepted_not_completed_and_duplicate_executes_once():
    validation = _validated_exact()
    calls = []

    def executor(name, arguments, **kwargs):
        calls.append((name, arguments, kwargs))
        return json.dumps(
            {
                "ok": True,
                "job_id": JOB_ID,
                "scene_count": 6,
                "animate_policy": "all",
            }
        )

    ledger = InMemoryExecutionLedger()
    receipt = execute_validated_command(
        validation,
        user_id="user_test",
        session_id="sa_test_command_layer",
        content_format="short",
        tool_executor=executor,
        ledger=ledger,
    )
    duplicate = execute_validated_command(
        validation,
        user_id="user_test",
        session_id="sa_test_command_layer",
        content_format="short",
        tool_executor=executor,
        ledger=ledger,
    )

    assert receipt.status == "accepted"
    assert receipt.status != "completed"
    assert duplicate.status == "duplicate"
    assert duplicate.duplicate_of == receipt.execution_id
    assert len(calls) == 1
    assert calls[0][0] == "expand_visual_proof_shortform"
    assert calls[0][1]["scene_count"] == 6


def test_durable_tool_replay_is_reported_as_duplicate_not_new_acceptance():
    validation = _validated_exact()
    receipt = execute_validated_command(
        validation,
        user_id="user_test",
        session_id="sa_test_command_layer",
        content_format="short",
        tool_executor=lambda *_args, **_kwargs: json.dumps(
            {
                "ok": True,
                "job_id": JOB_ID,
                "status": "started",
                "idempotent_replay": True,
            }
        ),
        ledger=InMemoryExecutionLedger(),
    )

    assert receipt.status == "duplicate"
    assert receipt.status != "accepted"


def test_cross_process_claim_without_receipt_never_claims_dispatch():
    validation = _validated_exact()

    class ClaimHeldLedger:
        def get(self, _key):
            return None

        def claim(self, _key, _command_id):
            return False

        def save(self, _receipt):
            raise AssertionError("a pending foreign claim must not be overwritten")

    calls = []
    receipt = execute_validated_command(
        validation,
        user_id="user_test",
        session_id="sa_test_command_layer",
        content_format="short",
        tool_executor=lambda *_args, **_kwargs: calls.append(True),
        ledger=ClaimHeldLedger(),
    )

    assert receipt.status == "duplicate"
    assert receipt.result["idempotency_claim_pending"] is True
    assert receipt.duplicate_of == ""
    assert calls == []


def test_lost_claim_replays_concurrent_failure_without_claiming_acceptance():
    validation = _validated_exact()
    failed = ExecutionReceipt(
        execution_id="exec_concurrent_failure",
        idempotency_key="idem_concurrent_failure",
        command_id=validation.command_id,
        status="failed",
        tool_name="expand_visual_proof_shortform",
        target_job_id=JOB_ID,
        result={"ok": False, "status": "conflict"},
        error="different expansion already in progress",
        started_at=1.0,
    )

    class LostClaimLedger:
        def __init__(self):
            self.reads = 0

        def get(self, _key):
            self.reads += 1
            return None if self.reads == 1 else failed

        def claim(self, _key, _command_id):
            return False

        def save(self, _receipt):
            raise AssertionError("a concurrent receipt must not be overwritten")

    calls = []
    receipt = execute_validated_command(
        validation,
        user_id="user_test",
        session_id="sa_test_command_layer",
        content_format="short",
        tool_executor=lambda *_args, **_kwargs: calls.append(True),
        ledger=LostClaimLedger(),
    )

    assert receipt.status == "failed"
    assert receipt.error == failed.error
    assert receipt.result == failed.result
    assert receipt.duplicate_of == failed.execution_id
    assert calls == []


def test_failed_command_replay_stays_failed_instead_of_claiming_acceptance():
    validation = _validated_exact()
    calls = []

    def conflict_executor(*_args, **_kwargs):
        calls.append(True)
        return json.dumps(
            {
                "ok": False,
                "status": "conflict",
                "error": "different expansion already in progress",
            }
        )

    ledger = InMemoryExecutionLedger()
    first = execute_validated_command(
        validation,
        user_id="user_test",
        session_id="sa_test_command_layer",
        content_format="short",
        tool_executor=conflict_executor,
        ledger=ledger,
    )
    replay = execute_validated_command(
        validation,
        user_id="user_test",
        session_id="sa_test_command_layer",
        content_format="short",
        tool_executor=conflict_executor,
        ledger=ledger,
    )

    assert first.status == "failed"
    assert replay.status == "failed"
    assert replay.duplicate_of == first.execution_id
    assert replay.result["status"] == "conflict"
    assert len(calls) == 1


def test_postconditions_are_pending_while_running_and_completed_only_after_observation():
    validation = _validated_exact(fingerprints=True)
    receipt = execute_validated_command(
        validation,
        user_id="user_test",
        session_id="sa_test_command_layer",
        content_format="short",
        tool_executor=lambda *_args, **_kwargs: json.dumps({"ok": True, "job_id": JOB_ID}),
        ledger=InMemoryExecutionLedger(),
    )
    fingerprint_loader = lambda _job_id, _numbers: [
        SceneAssetFingerprint(scene_number=1, still_sha256="still-before", clip_sha256="clip-before")
    ]
    pending = verify_execution(
        receipt,
        snapshot_loader=lambda _job_id, _kind: _snapshot(stage="restarting", scene_count=1),
        fingerprint_loader=fingerprint_loader,
    )
    assert pending.status == "pending"
    assert pending.safe_claim == "started"
    assert not pending.can_report_completion

    completed_snapshot = _snapshot(stage="awaiting_scene_review", scene_count=6, clips=True)
    completed = verify_execution(
        receipt,
        snapshot_loader=lambda _job_id, _kind: completed_snapshot,
        fingerprint_loader=fingerprint_loader,
    )
    assert completed.status == "passed"
    assert completed.safe_claim == "completed"
    assert completed.can_report_completion


def test_postcondition_fails_when_preserved_scene_changes():
    validation = _validated_exact(fingerprints=True)
    receipt = execute_validated_command(
        validation,
        user_id="user_test",
        session_id="sa_test_command_layer",
        content_format="short",
        tool_executor=lambda *_args, **_kwargs: json.dumps({"ok": True, "job_id": JOB_ID}),
        ledger=InMemoryExecutionLedger(),
    )
    verdict = verify_execution(
        receipt,
        snapshot_loader=lambda _job_id, _kind: _snapshot(
            stage="awaiting_scene_review", scene_count=6, clips=True
        ),
        fingerprint_loader=lambda _job_id, _numbers: [
            SceneAssetFingerprint(scene_number=1, still_sha256="changed", clip_sha256="clip-before")
        ],
    )
    assert verdict.status == "failed"
    assert verdict.safe_claim == "none"
