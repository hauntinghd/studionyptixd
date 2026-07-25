from __future__ import annotations

import pytest
from pydantic import ValidationError

from studio_agent.command_contract import (
    AnalyzeClipLabOperation,
    AnalyzeReferenceOperation,
    AnimateScenesOperation,
    AnimationDirective,
    ApproveScenesOperation,
    AuditAndRepairScenesOperation,
    AuthorizationEvidence,
    CancelOperation,
    CommandTarget,
    CompilerProvenance,
    ExpandExistingShortOperation,
    ExpandExistingShortRequest,
    ExpandLongformChapterOperation,
    ExpandLongformOperation,
    FinalizeOperation,
    GenerateLongformOutlineOperation,
    GenerateThumbnailOperation,
    ProductionAuthorizationV2,
    ProductionCommandEnvelopeV2,
    ProductionCommandTargetV2,
    ProductionMediaRouteV2,
    ProductionPostconditionV2,
    RenderClipLabOperation,
    RetryReferenceAnalysisOperation,
    SceneRepairRequest,
    ShipExistingShortOperation,
    StartLongformOperation,
    StartClipLabOperation,
    StartProductAdOperation,
    StartShortOperation,
    StudioCommand,
)
from studio_agent.production_command_state import (
    CommandCancelledEvent,
    CommandCompiledEvent,
    CommandCompletedEvent,
    CommandExecutionStartedEvent,
    CommandFailedEvent,
    CommandProgressEvent,
    CommandReadyEvent,
    CommandVerificationStartedEvent,
    ProductionCommandState,
    build_session_production_view,
    build_production_view,
    parse_production_command_event,
    production_view_event_payload,
    reduce_production_command,
    replay_production_command,
)
from studio_agent.production_command_service import compile_authorized_mutation


SESSION_ID = "sa_contract_v2"
USER_ID = "user_contract_v2"
JOB_ID = "116de45c8205"


def _envelope(operation, *, confirmation_required: bool = False):
    action = operation.action
    is_start = action in {
        "generate_longform_outline",
        "expand_longform_chapter",
        "analyze_reference",
        "start_short",
        "start_longform",
        "start_product_ad",
        "start_cliplab",
    }
    target = ProductionCommandTargetV2(
        source="none" if is_start else "explicit_job_id",
        job_id="" if is_start else JOB_ID,
        kind=(
            "longform"
            if action
            in {
                "generate_longform_outline",
                "expand_longform_chapter",
                "start_longform",
                "expand_longform",
            }
            else "cliplab"
            if action in {"start_cliplab", "analyze_cliplab", "render_cliplab"}
            else "product_ad"
            if action == "start_product_ad"
            else "reference_analysis"
            if action in {"analyze_reference", "retry_reference_analysis"}
            else "shortform"
        ),
        owner_session_id="" if is_start else SESSION_ID,
        owner_user_id="" if is_start else USER_ID,
        expected_job_revision="" if is_start else "job-rev-7",
    )
    return ProductionCommandEnvelopeV2(
        command_id=f"cmd_{action}",
        turn_id="turn_contract_v2",
        session_id=SESSION_ID,
        user_id=USER_ID,
        state_revision="state-rev-19",
        action=action,
        target=target,
        operation=operation,
        authorization=ProductionAuthorizationV2(
            execution_requested=True,
            execution_quote="do it",
            confirmation_required=confirmation_required,
        ),
        media_route=ProductionMediaRouteV2(
            revision=7,
            image_model="seedream",
            video_model="seedance",
            route_sha256="a" * 64,
        ),
        expected_postconditions=[
            ProductionPostconditionV2(kind="artifact_ready", artifact_type="mp4")
        ],
        idempotency_key=f"idem_{action}",
        source_text_sha256="b" * 64,
        created_at=100.0,
    )


@pytest.mark.parametrize(
    "operation",
    [
        StartShortOperation(brief="A six-scene short", scene_count=6),
        GenerateLongformOutlineOperation(
            topic="A documentary",
            channel_key="empire_magnates",
            target_minutes=12,
        ),
        ExpandLongformChapterOperation(
            outline_title="A documentary",
            chapter_index=1,
        ),
        StartLongformOperation(brief="A documentary", target_duration_seconds=720),
        StartProductAdOperation(brief="Sell the product", product_name="Widget"),
        ExpandExistingShortOperation(
            request=ExpandExistingShortRequest(
                additional_scene_count=5,
                preserve_scene_numbers=[1],
                animation=AnimationDirective(scope="new_scenes"),
            )
        ),
        ExpandLongformOperation(instruction="Expand the approved visual proof."),
        AuditAndRepairScenesOperation(
            request=SceneRepairRequest(
                scene_numbers=[1],
                scope="narrative_alignment",
                instruction="Match the narration.",
            )
        ),
        ApproveScenesOperation(scene_numbers=[1, 2]),
        AnimateScenesOperation(scene_numbers=[2, 3], only_missing=True),
        ShipExistingShortOperation(scene_numbers=[1, 2, 3, 4, 5, 6]),
        FinalizeOperation(),
        CancelOperation(reason="User cancelled."),
        GenerateThumbnailOperation(prompt="A clean thumbnail", scene_number=1),
        StartClipLabOperation(brief="Clip the uploaded interview."),
        AnalyzeClipLabOperation(prompt="Rank the strongest hooks.", max_segments=8),
        RenderClipLabOperation(instruction="Render segments 1 and 2."),
        AnalyzeReferenceOperation(source="url", content_format="short"),
        RetryReferenceAnalysisOperation(stages=["vision", "storytelling"]),
    ],
)
def test_v2_envelope_round_trips_every_production_action(operation) -> None:
    envelope = _envelope(operation)

    reparsed = ProductionCommandEnvelopeV2.model_validate_json(envelope.model_dump_json())

    assert reparsed.schema_version == "production-command-v2"
    assert reparsed.action == operation.action
    assert reparsed.operation.action == operation.action
    if operation.action.startswith("start_") or operation.action in {
        "generate_longform_outline",
        "expand_longform_chapter",
        "analyze_reference",
    }:
        assert reparsed.target.job_id == ""
    else:
        assert reparsed.target.job_id == JOB_ID
        assert reparsed.target.owner_session_id == SESSION_ID
        assert reparsed.target.owner_user_id == USER_ID


def test_thumbnail_command_may_create_a_new_packaging_job_without_fake_target() -> None:
    payload = _envelope(GenerateThumbnailOperation(prompt="A clean thumbnail")).model_dump(
        mode="json"
    )
    payload["target"] = ProductionCommandTargetV2(
        source="none",
        job_id="",
        kind="longform",
        owner_session_id="",
        owner_user_id="",
    ).model_dump(mode="json")

    command = ProductionCommandEnvelopeV2.model_validate(payload)

    assert command.action == "generate_thumbnail"
    assert command.target.job_id == ""


def test_trusted_compiler_normalizes_expand_animation_scope_and_exact_target() -> None:
    command = compile_authorized_mutation(
        authority={
            "command_id": "turn-expand",
            "session_id": SESSION_ID,
            "user_id": USER_ID,
            "execution_quote": "animate scenes 2 and 3",
            "request_sha256": "c" * 64,
        },
        mutation={
            "mutation_id": "mut-expand",
            "tool_name": "expand_visual_proof_shortform",
            "action": "expand_existing_short",
            "target_id": JOB_ID,
            "authorized_at": 100.0,
        },
        arguments={
            "job_id": JOB_ID,
            "existing_scene_count": 1,
            "scene_count": 6,
            "animate_policy": "selected_scenes",
            "animate_scene_indices": [1, 2],
        },
        session={
            "session_id": SESSION_ID,
            "user_id": USER_ID,
            "production_view_revision": 7,
            "production_command_revision": 3,
            "media_route_revision": 2,
            "image_model": "seedream",
            "video_model": "seedance",
        },
    )

    assert command.target.job_id == JOB_ID
    assert command.target.owner_session_id == SESSION_ID
    assert command.operation.action == "expand_existing_short"
    assert command.operation.request.animation.scope == "explicit"
    assert command.operation.request.animation.scene_numbers == [2, 3]


def test_trusted_compiler_promotes_product_reference_to_product_ad() -> None:
    command = compile_authorized_mutation(
        authority={
            "command_id": "turn-product",
            "session_id": SESSION_ID,
            "user_id": USER_ID,
            "execution_quote": "make this product ad",
        },
        mutation={
            "mutation_id": "mut-product",
            "tool_name": "start_shortform_generate",
            "action": "start_short",
            "target_id": "",
            "authorized_at": 100.0,
        },
        arguments={
            "product_reference_id": "product-ref-1",
            "product_name": "Widget",
            "visual_brief": "Show the widget solving the problem.",
        },
        session={
            "session_id": SESSION_ID,
            "user_id": USER_ID,
            "production_view_revision": 7,
            "production_command_revision": 3,
            "media_route_revision": 2,
        },
    )

    assert command.action == "start_product_ad"
    assert command.target.kind == "product_ad"
    assert command.target.job_id == ""
    assert command.operation.action == "start_product_ad"


@pytest.mark.parametrize(
    ("tool_name", "action", "arguments", "expected_kind"),
    [
        (
            "expand_longform_visual_proof",
            "expand_longform",
            {"job_id": JOB_ID, "instruction": "Expand the approved proof."},
            "longform",
        ),
        (
            "analyze_cliplab_video",
            "analyze_cliplab",
            {
                "video_id": JOB_ID,
                "prompt": "Rank the strongest hooks.",
                "max_segments": 8,
            },
            "cliplab",
        ),
    ],
)
def test_trusted_compiler_retains_exact_nonstart_target(
    tool_name: str,
    action: str,
    arguments: dict,
    expected_kind: str,
) -> None:
    command = compile_authorized_mutation(
        authority={
            "command_id": f"turn-{action}",
            "session_id": SESSION_ID,
            "user_id": USER_ID,
            "execution_quote": f"run {action}",
        },
        mutation={
            "mutation_id": f"mut-{action}",
            "tool_name": tool_name,
            "action": action,
            "target_id": JOB_ID,
            "authorized_at": 100.0,
        },
        arguments=arguments,
        session={
            "session_id": SESSION_ID,
            "user_id": USER_ID,
            "production_view_revision": 7,
            "production_command_revision": 3,
            "media_route_revision": 2,
        },
    )

    assert command.action == action
    assert command.operation.action == action
    assert command.target.source == "explicit_job_id"
    assert command.target.job_id == JOB_ID
    assert command.target.kind == expected_kind
    assert command.target.owner_session_id == SESSION_ID
    assert command.target.owner_user_id == USER_ID


def test_v2_envelope_fails_closed_on_action_target_or_owner_drift() -> None:
    envelope = _envelope(FinalizeOperation())
    payload = envelope.model_dump(mode="json")
    payload["action"] = "cancel"
    with pytest.raises(ValidationError, match="envelope action must match"):
        ProductionCommandEnvelopeV2.model_validate(payload)

    payload = envelope.model_dump(mode="json")
    payload["target"]["owner_session_id"] = "another-session"
    with pytest.raises(ValidationError, match="owner_session_id"):
        ProductionCommandEnvelopeV2.model_validate(payload)

    payload = envelope.model_dump(mode="json")
    payload["target"]["job_id"] = ""
    with pytest.raises(ValidationError, match="exact target job_id"):
        ProductionCommandEnvelopeV2.model_validate(payload)


def test_studio_command_v1_remains_unchanged_and_parseable() -> None:
    command = StudioCommand(
        command_id="cmd_v1",
        turn_id="turn_v1",
        action="conversation",
        target=CommandTarget(source="none"),
        authorization=AuthorizationEvidence(),
        source_text_sha256="0" * 64,
        compiler=CompilerProvenance(
            requested_model="test",
            resolved_model="test",
            provider="test",
            transport="deterministic_fallback",
        ),
    )

    reparsed = StudioCommand.model_validate_json(command.model_dump_json())

    assert reparsed.schema_version == "studio-command-v1"
    assert reparsed.action == "conversation"
    with pytest.raises(ValidationError):
        StudioCommand.model_validate(
            {**command.model_dump(mode="json"), "action": "ship_existing_short"}
        )


def _event(event_class, sequence: int, **updates):
    payload = {
        "event_id": f"event-{sequence}",
        "command_id": "cmd_ship_existing_short",
        "sequence": sequence,
        "occurred_at": float(100 + sequence),
    }
    payload.update(updates)
    return event_class(**payload)


def _compiled_event(*, confirmation_required: bool = False) -> CommandCompiledEvent:
    return _event(
        CommandCompiledEvent,
        1,
        envelope=_envelope(
            ShipExistingShortOperation(scene_numbers=[1, 2, 3]),
            confirmation_required=confirmation_required,
        ),
    )


def test_reducer_replays_ordered_lifecycle_and_is_idempotent() -> None:
    events = [
        _compiled_event(),
        _event(
            CommandReadyEvent,
            2,
            planned_steps=["verify_scene_qa", "animate_missing", "finalize"],
        ),
        _event(
            CommandExecutionStartedEvent,
            3,
            execution_id="execution-1",
            active_step="verify_scene_qa",
        ),
        _event(
            CommandProgressEvent,
            4,
            progress_percent=70,
            active_step="animate_missing",
        ),
        _event(
            CommandVerificationStartedEvent,
            5,
            receipt_id="receipt-1",
        ),
        _event(
            CommandCompletedEvent,
            6,
            receipt_id="receipt-1",
            result={"job_id": JOB_ID, "artifact_url": "https://example.test/final.mp4"},
        ),
    ]

    state = replay_production_command(events)

    assert state.lifecycle == "completed"
    assert state.revision == 6
    assert state.last_event_sequence == 6
    assert state.progress_percent == 100
    assert state.receipt_id == "receipt-1"
    assert state.result["job_id"] == JOB_ID
    assert reduce_production_command(state, events[-1]) is state


def test_session_projection_uses_terminal_root_over_accepted_child() -> None:
    view = build_session_production_view(
        {
            "session_id": SESSION_ID,
            "agent_mode": "studio",
            "production_command_revision": 9,
            "latest_production_command": {
                "command_id": "cmd_ship",
                "action": "ship_existing_short",
                "status": "completed",
                "steps": [
                    {
                        "mutation_id": "root",
                        "action": "ship_existing_short",
                        "tool_name": "production_workflow",
                        "target_id": JOB_ID,
                        "status": "completed",
                    },
                    {
                        "mutation_id": "animate",
                        "action": "animate_scenes",
                        "tool_name": "animate_production_scenes",
                        "target_id": JOB_ID,
                        "status": "accepted",
                    },
                ],
            },
            "production_workflows": [
                {
                    "command_id": "cmd_ship",
                    "job_id": JOB_ID,
                    "status": "completed",
                    "stage": "completed",
                    "revision": 8,
                }
            ],
            "active_jobs": [],
            "messages": [],
            "pending_actions": [],
        }
    )

    assert view.command is not None
    assert view.command.lifecycle == "completed"
    assert view.command.active_step == ""
    assert view.command.progress_percent == 100


def test_reducer_rejects_skipped_illegal_or_backward_events() -> None:
    state = reduce_production_command(None, _compiled_event())
    with pytest.raises(ValueError, match="event sequence"):
        reduce_production_command(
            state,
            _event(CommandExecutionStartedEvent, 3, execution_id="execution-1"),
        )
    with pytest.raises(ValueError, match="cannot apply completed"):
        reduce_production_command(state, _event(CommandCompletedEvent, 2))

    state = reduce_production_command(state, _event(CommandReadyEvent, 2))
    state = reduce_production_command(
        state,
        _event(CommandExecutionStartedEvent, 3, execution_id="execution-1"),
    )
    state = reduce_production_command(
        state,
        _event(CommandProgressEvent, 4, progress_percent=80),
    )
    with pytest.raises(ValueError, match="cannot move backwards"):
        reduce_production_command(
            state,
            _event(CommandProgressEvent, 5, progress_percent=79),
        )


def test_reducer_requires_confirmation_before_ready_and_models_failure_cancel() -> None:
    state = reduce_production_command(None, _compiled_event(confirmation_required=True))
    with pytest.raises(ValueError, match="confirmation evidence"):
        reduce_production_command(state, _event(CommandReadyEvent, 2))

    state = reduce_production_command(
        state,
        _event(CommandReadyEvent, 2, confirmation_id="confirmation-1"),
    )
    failed = reduce_production_command(
        state,
        _event(CommandFailedEvent, 3, error="Scene 1 QA failed.", retryable=True),
    )
    assert failed.lifecycle == "failed"
    assert failed.retryable is True
    with pytest.raises(ValueError, match="while command is failed"):
        reduce_production_command(
            failed,
            _event(CommandCancelledEvent, 4, reason="too late"),
        )


def test_projection_is_one_versioned_top_level_frontend_payload() -> None:
    state: ProductionCommandState = reduce_production_command(
        None,
        _compiled_event(confirmation_required=True),
    )
    view = build_production_view(
        session_id=SESSION_ID,
        view_revision=21,
        state_revision="state-rev-21",
        effective_mode="studio",
        command_state=state,
        jobs=[
            {
                "job_id": JOB_ID,
                "kind": "shortform",
                "title": "Why Men Lose Interest",
                "status": "awaiting_approval",
                "stage": "awaiting_animation_review",
                "progress_percent": 80,
            }
        ],
        cards=[
            {
                "card_id": f"job:{JOB_ID}",
                "kind": "shortform_job",
                "job_id": JOB_ID,
                "title": "Review scenes",
                "status": "awaiting_approval",
                "actions": ["approve_scenes", "audit_and_repair_scenes"],
            }
        ],
        allowed_actions=["audit_and_repair_scenes", "ship_existing_short"],
        notices=[
            {
                "notice_id": "qa-1",
                "level": "warning",
                "message": "Scene 1 needs correspondence repair.",
            }
        ],
    )
    payload = production_view_event_payload(view)

    assert payload["type"] == "production_view"
    assert payload["schema_version"] == "production-view-v1"
    assert payload["session_id"] == SESSION_ID
    assert payload["view_revision"] == 21
    assert payload["state_revision"] == "state-rev-21"
    assert payload["effective_mode"] == "studio"
    assert payload["command"]["lifecycle"] == "compiled"
    assert payload["pending_confirmation"]["command_id"] == state.command.command_id
    assert payload["jobs"][0]["job_id"] == JOB_ID
    assert payload["cards"][0]["actions"] == [
        "approve_scenes",
        "audit_and_repair_scenes",
    ]


def test_event_parser_uses_discriminator_and_rejects_unknown_event() -> None:
    parsed = parse_production_command_event(
        _event(CommandCancelledEvent, 2, reason="User cancelled.").model_dump(mode="json")
    )
    assert isinstance(parsed, CommandCancelledEvent)
    with pytest.raises(ValidationError):
        parse_production_command_event(
            {
                "schema_version": "production-command-event-v1",
                "event_id": "bad-event",
                "command_id": "cmd_ship_existing_short",
                "sequence": 2,
                "occurred_at": 102.0,
                "event_type": "pretend_completed",
            }
        )
