"""Durable lifecycle reducer and UI projection for production-command-v2.

This module is deliberately pure: it does not resolve jobs, call providers, or
write storage. A persistence adapter can append typed events, rebuild the state
with :func:`reduce_production_command`, and emit the versioned projection made
by :func:`build_production_view`.
"""
from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import Field, TypeAdapter, model_validator

from studio_agent.command_contract import (
    ContractModel,
    ProductionCommandAction,
    ProductionCommandEnvelopeV2,
    ProductionKind,
)


CommandLifecycle = Literal[
    "compiled",
    "ready",
    "executing",
    "verifying",
    "completed",
    "failed",
    "cancelled",
]
EffectiveAgentMode = Literal["plan", "studio", "cliplab"]
PRODUCTION_VIEW_EVENT = "production_view"


class ProductionCommandEventBase(ContractModel):
    schema_version: Literal["production-command-event-v1"] = "production-command-event-v1"
    event_id: str = Field(min_length=1, max_length=128)
    command_id: str = Field(min_length=1, max_length=128)
    sequence: int = Field(ge=1)
    occurred_at: float = Field(ge=0)


class CommandCompiledEvent(ProductionCommandEventBase):
    event_type: Literal["compiled"] = "compiled"
    envelope: ProductionCommandEnvelopeV2

    @model_validator(mode="after")
    def _match_command(self) -> "CommandCompiledEvent":
        if self.command_id != self.envelope.command_id:
            raise ValueError("compiled event command_id must match its envelope")
        return self


class CommandReadyEvent(ProductionCommandEventBase):
    event_type: Literal["ready"] = "ready"
    confirmation_id: str = Field(default="", max_length=128)
    planned_steps: list[str] = Field(default_factory=list, max_length=100)


class CommandExecutionStartedEvent(ProductionCommandEventBase):
    event_type: Literal["executing"] = "executing"
    execution_id: str = Field(min_length=1, max_length=128)
    active_step: str = Field(default="", max_length=256)


class CommandProgressEvent(ProductionCommandEventBase):
    event_type: Literal["progress"] = "progress"
    progress_percent: int = Field(ge=0, le=100)
    active_step: str = Field(default="", max_length=256)


class CommandVerificationStartedEvent(ProductionCommandEventBase):
    event_type: Literal["verifying"] = "verifying"
    receipt_id: str = Field(default="", max_length=128)
    active_step: str = Field(default="verify_postconditions", max_length=256)


class CommandCompletedEvent(ProductionCommandEventBase):
    event_type: Literal["completed"] = "completed"
    receipt_id: str = Field(default="", max_length=128)
    result: dict[str, Any] = Field(default_factory=dict)


class CommandFailedEvent(ProductionCommandEventBase):
    event_type: Literal["failed"] = "failed"
    error: str = Field(min_length=1, max_length=4_000)
    retryable: bool = False
    receipt_id: str = Field(default="", max_length=128)
    result: dict[str, Any] = Field(default_factory=dict)


class CommandCancelledEvent(ProductionCommandEventBase):
    event_type: Literal["cancelled"] = "cancelled"
    reason: str = Field(default="", max_length=1_000)


ProductionCommandEvent = Annotated[
    CommandCompiledEvent
    | CommandReadyEvent
    | CommandExecutionStartedEvent
    | CommandProgressEvent
    | CommandVerificationStartedEvent
    | CommandCompletedEvent
    | CommandFailedEvent
    | CommandCancelledEvent,
    Field(discriminator="event_type"),
]
_EVENT_ADAPTER = TypeAdapter(ProductionCommandEvent)


class ProductionCommandState(ContractModel):
    schema_version: Literal["production-command-state-v1"] = "production-command-state-v1"
    command: ProductionCommandEnvelopeV2
    lifecycle: CommandLifecycle = "compiled"
    revision: int = Field(default=1, ge=1)
    last_event_sequence: int = Field(default=1, ge=1)
    applied_event_ids: list[str] = Field(default_factory=list, max_length=10_000)
    confirmation_id: str = Field(default="", max_length=128)
    execution_id: str = Field(default="", max_length=128)
    receipt_id: str = Field(default="", max_length=128)
    planned_steps: list[str] = Field(default_factory=list, max_length=100)
    active_step: str = Field(default="", max_length=256)
    progress_percent: int = Field(default=0, ge=0, le=100)
    result: dict[str, Any] = Field(default_factory=dict)
    error: str = Field(default="", max_length=4_000)
    retryable: bool = False
    updated_at: float = Field(ge=0)


_TRANSITIONS: dict[CommandLifecycle, set[str]] = {
    "compiled": {"ready", "failed", "cancelled"},
    "ready": {"executing", "failed", "cancelled"},
    "executing": {"progress", "verifying", "completed", "failed", "cancelled"},
    "verifying": {"progress", "completed", "failed", "cancelled"},
    "completed": set(),
    "failed": set(),
    "cancelled": set(),
}


def parse_production_command_event(payload: Any) -> ProductionCommandEvent:
    """Validate a stored/wire event into its discriminated concrete type."""

    return _EVENT_ADAPTER.validate_python(payload)


def reduce_production_command(
    state: ProductionCommandState | None,
    event: ProductionCommandEvent,
) -> ProductionCommandState:
    """Apply exactly one ordered event.

    Duplicate event IDs are idempotent. Any gap, command mismatch, or illegal
    lifecycle transition fails closed so a process cannot skip validation and
    claim completion.
    """

    if state is None:
        if not isinstance(event, CommandCompiledEvent):
            raise ValueError("the first production command event must be compiled")
        if event.sequence != 1:
            raise ValueError("the first production command event must have sequence 1")
        return ProductionCommandState(
            command=event.envelope,
            lifecycle="compiled",
            revision=1,
            last_event_sequence=1,
            applied_event_ids=[event.event_id],
            updated_at=event.occurred_at,
        )

    if event.command_id != state.command.command_id:
        raise ValueError("event command_id does not match reducer state")
    if event.event_id in state.applied_event_ids:
        return state
    if event.sequence != state.last_event_sequence + 1:
        raise ValueError(
            f"event sequence must be {state.last_event_sequence + 1}, got {event.sequence}"
        )
    if isinstance(event, CommandCompiledEvent):
        raise ValueError("compiled may only be the first command event")
    if event.event_type not in _TRANSITIONS[state.lifecycle]:
        raise ValueError(f"cannot apply {event.event_type} while command is {state.lifecycle}")

    updates: dict[str, Any] = {
        "revision": state.revision + 1,
        "last_event_sequence": event.sequence,
        "applied_event_ids": [*state.applied_event_ids, event.event_id],
        "updated_at": event.occurred_at,
    }
    if isinstance(event, CommandReadyEvent):
        if (
            state.command.authorization.confirmation_required
            and not state.command.authorization.confirmed
            and not event.confirmation_id
        ):
            raise ValueError("ready requires confirmation evidence for this command")
        updates.update(
            lifecycle="ready",
            confirmation_id=(
                event.confirmation_id or state.command.authorization.confirmation_id
            ),
            planned_steps=event.planned_steps,
            active_step="",
            progress_percent=max(state.progress_percent, 1),
            error="",
        )
    elif isinstance(event, CommandExecutionStartedEvent):
        updates.update(
            lifecycle="executing",
            execution_id=event.execution_id,
            active_step=event.active_step,
            progress_percent=max(state.progress_percent, 2),
            error="",
        )
    elif isinstance(event, CommandProgressEvent):
        if event.progress_percent < state.progress_percent:
            raise ValueError("command progress cannot move backwards")
        updates.update(
            active_step=event.active_step or state.active_step,
            progress_percent=event.progress_percent,
        )
    elif isinstance(event, CommandVerificationStartedEvent):
        updates.update(
            lifecycle="verifying",
            receipt_id=event.receipt_id or state.receipt_id,
            active_step=event.active_step,
            progress_percent=max(state.progress_percent, 90),
        )
    elif isinstance(event, CommandCompletedEvent):
        updates.update(
            lifecycle="completed",
            receipt_id=event.receipt_id or state.receipt_id,
            active_step="",
            progress_percent=100,
            result=event.result,
            error="",
            retryable=False,
        )
    elif isinstance(event, CommandFailedEvent):
        updates.update(
            lifecycle="failed",
            receipt_id=event.receipt_id or state.receipt_id,
            active_step="",
            result=event.result,
            error=event.error,
            retryable=event.retryable,
        )
    elif isinstance(event, CommandCancelledEvent):
        updates.update(
            lifecycle="cancelled",
            active_step="",
            error=event.reason,
            retryable=False,
        )
    return state.model_copy(update=updates)


def replay_production_command(
    events: list[ProductionCommandEvent | dict[str, Any]],
) -> ProductionCommandState:
    """Rebuild command state from an append-only event stream."""

    state: ProductionCommandState | None = None
    for raw in events:
        event = raw if isinstance(raw, ProductionCommandEventBase) else parse_production_command_event(raw)
        state = reduce_production_command(state, event)
    if state is None:
        raise ValueError("cannot replay an empty production command event stream")
    return state


class ProductionCommandViewV1(ContractModel):
    command_id: str
    action: ProductionCommandAction
    lifecycle: CommandLifecycle
    target_job_id: str = ""
    progress_percent: int = Field(ge=0, le=100)
    active_step: str = ""
    error: str = ""


class ProductionJobViewV1(ContractModel):
    job_id: str = Field(min_length=1, max_length=48)
    # The same authoritative projection also carries analysis and ClipLab jobs;
    # production command targets remain restricted by ``ProductionKind``.
    kind: str = Field(min_length=1, max_length=128)
    title: str = Field(default="", max_length=1_000)
    status: str = Field(default="", max_length=128)
    stage: str = Field(default="", max_length=128)
    progress_percent: int = Field(default=0, ge=0, le=100)


class ProductionCardViewV1(ContractModel):
    card_id: str = Field(min_length=1, max_length=128)
    kind: str = Field(min_length=1, max_length=128)
    job_id: str = Field(default="", max_length=48)
    title: str = Field(default="", max_length=1_000)
    status: str = Field(default="", max_length=128)
    body: str = Field(default="", max_length=8_000)
    actions: list[str] = Field(default_factory=list, max_length=100)


class PendingConfirmationViewV1(ContractModel):
    command_id: str = Field(min_length=1, max_length=128)
    prompt: str = Field(min_length=1, max_length=1_000)
    approve_action: str = Field(default="confirm", max_length=128)
    cancel_action: str = Field(default="cancel", max_length=128)


class ProductionNoticeViewV1(ContractModel):
    notice_id: str = Field(min_length=1, max_length=128)
    level: Literal["info", "success", "warning", "error"]
    message: str = Field(min_length=1, max_length=4_000)


class ProductionViewV1(ContractModel):
    """Single authoritative projection consumed by the Studio frontend."""

    schema_version: Literal["production-view-v1"] = "production-view-v1"
    session_id: str = Field(min_length=1, max_length=128)
    view_revision: int = Field(ge=1)
    state_revision: str = Field(min_length=1, max_length=128)
    effective_mode: EffectiveAgentMode
    command: ProductionCommandViewV1 | None = None
    jobs: list[ProductionJobViewV1] = Field(default_factory=list, max_length=1_000)
    cards: list[ProductionCardViewV1] = Field(default_factory=list, max_length=1_000)
    allowed_actions: list[ProductionCommandAction] = Field(default_factory=list)
    pending_confirmation: PendingConfirmationViewV1 | None = None
    notices: list[ProductionNoticeViewV1] = Field(default_factory=list, max_length=1_000)


def build_production_view(
    *,
    session_id: str,
    view_revision: int,
    state_revision: str,
    effective_mode: EffectiveAgentMode,
    command_state: ProductionCommandState | None = None,
    jobs: list[ProductionJobViewV1 | dict[str, Any]] | None = None,
    cards: list[ProductionCardViewV1 | dict[str, Any]] | None = None,
    allowed_actions: list[ProductionCommandAction] | None = None,
    pending_confirmation: PendingConfirmationViewV1 | dict[str, Any] | None = None,
    notices: list[ProductionNoticeViewV1 | dict[str, Any]] | None = None,
) -> ProductionViewV1:
    """Build the only frontend-authoritative production projection."""

    command_view: ProductionCommandViewV1 | None = None
    if command_state is not None:
        command_view = ProductionCommandViewV1(
            command_id=command_state.command.command_id,
            action=command_state.command.action,
            lifecycle=command_state.lifecycle,
            target_job_id=command_state.command.target.job_id,
            progress_percent=command_state.progress_percent,
            active_step=command_state.active_step,
            error=command_state.error,
        )
        if (
            pending_confirmation is None
            and command_state.lifecycle == "compiled"
            and command_state.command.authorization.confirmation_required
            and not command_state.command.authorization.confirmed
        ):
            pending_confirmation = PendingConfirmationViewV1(
                command_id=command_state.command.command_id,
                prompt=f"Confirm {command_state.command.action.replace('_', ' ')}.",
            )
    return ProductionViewV1(
        session_id=session_id,
        view_revision=view_revision,
        state_revision=state_revision,
        effective_mode=effective_mode,
        command=command_view,
        jobs=jobs or [],
        cards=cards or [],
        allowed_actions=allowed_actions or [],
        pending_confirmation=pending_confirmation,
        notices=notices or [],
    )


def production_view_event_payload(view: ProductionViewV1) -> dict[str, Any]:
    """Return the canonical top-level JSON payload for ``production_view``."""

    return {"type": PRODUCTION_VIEW_EVENT, **view.model_dump(mode="json")}


_VALID_ACTIONS: set[str] = {
    "analyze_reference",
    "retry_reference_analysis",
    "generate_longform_outline",
    "expand_longform_chapter",
    "start_short",
    "start_longform",
    "start_product_ad",
    "expand_existing_short",
    "expand_longform",
    "audit_and_repair_scenes",
    "approve_scenes",
    "animate_scenes",
    "ship_existing_short",
    "finalize",
    "cancel",
    "generate_thumbnail",
    "start_cliplab",
    "analyze_cliplab",
    "render_cliplab",
}

_TOOL_ACTIONS: dict[str, ProductionCommandAction] = {
    "analyze_reference_video": "analyze_reference",
    "analyze_competitor_video": "analyze_reference",
    "retry_reference_analysis": "retry_reference_analysis",
    "generate_longform_outline": "generate_longform_outline",
    "expand_longform_chapter": "expand_longform_chapter",
    "start_shortform_generate": "start_short",
    "start_longform_render": "start_longform",
    "expand_visual_proof_shortform": "expand_existing_short",
    "expand_longform_visual_proof": "expand_longform",
    "audit_and_repair_production_scenes": "audit_and_repair_scenes",
    "repair_production_scene_animation": "audit_and_repair_scenes",
    "regenerate_production_scene": "audit_and_repair_scenes",
    "regenerate_production_scenes": "audit_and_repair_scenes",
    "regenerate_production_scene_still": "audit_and_repair_scenes",
    "edit_production_scene_still": "audit_and_repair_scenes",
    "edit_production_scenes_still": "audit_and_repair_scenes",
    "set_production_scene_prompt": "audit_and_repair_scenes",
    "set_production_scene_duration": "audit_and_repair_scenes",
    "set_production_scenes_animate": "approve_scenes",
    "animate_production_scenes": "animate_scenes",
    "finalize_production": "finalize",
    "finalize_longform_render": "finalize",
    "cancel_production_job": "cancel",
    "cancel_longform_render": "cancel",
    "generate_longform_thumbnails": "generate_thumbnail",
    "regenerate_longform_thumbnail": "generate_thumbnail",
    "regenerate_longform_still": "audit_and_repair_scenes",
    "re_edit_production": "audit_and_repair_scenes",
    "ingest_cliplab_attachment": "start_cliplab",
    "analyze_cliplab_video": "analyze_cliplab",
    "render_cliplab_segments": "render_cliplab",
    "remix_cliplab_short": "render_cliplab",
}


def _projection_action(value: Any, *, tool_name: str = "") -> ProductionCommandAction | None:
    action = str(value or "").strip()
    if action in _VALID_ACTIONS:
        return action  # type: ignore[return-value]
    return _TOOL_ACTIONS.get(str(tool_name or "").strip())


def _projection_lifecycle(value: Any) -> CommandLifecycle:
    status = str(value or "").strip().lower()
    if status in {"failed", "rejected", "error"}:
        return "failed"
    if status in {"cancelled", "canceled"}:
        return "cancelled"
    if status in {"completed", "complete"}:
        return "completed"
    if status in {"verifying", "verification"}:
        return "verifying"
    if status in {"ready", "authorized"}:
        return "ready"
    return "executing"


def _job_progress(job: dict[str, Any]) -> int:
    raw = job.get("progress_percent")
    if raw is None:
        raw = job.get("percent")
    if raw is None:
        raw = job.get("progress")
    try:
        return max(0, min(100, int(float(raw or 0))))
    except (TypeError, ValueError):
        return 0


def build_session_production_view(
    session: dict[str, Any],
    *,
    effective_mode: EffectiveAgentMode | str | None = None,
) -> ProductionViewV1:
    """Build the complete browser projection from one persisted session.

    No transcript parsing happens in the browser. Legacy pending actions and
    rich deliverables are projected here under exact opaque ids while their
    execution paths migrate onto V2 commands.
    """

    sid = str(session.get("session_id") or "").strip()
    if not sid:
        raise ValueError("production view requires a session_id")
    mode = str(effective_mode or session.get("agent_mode") or "plan").strip().lower()
    if mode not in {"plan", "studio", "cliplab"}:
        mode = "plan"
    blocked = {
        str(value).strip()
        for value in list(session.get("blocked_job_ids") or [])
        if str(value).strip()
    }

    jobs: list[ProductionJobViewV1] = []
    job_ids: set[str] = set()
    for raw in list(session.get("active_jobs") or []):
        if not isinstance(raw, dict):
            continue
        job_id = str(raw.get("job_id") or "").strip()
        if not job_id or job_id in blocked or job_id in job_ids:
            continue
        job_ids.add(job_id)
        jobs.append(
            ProductionJobViewV1(
                job_id=job_id,
                kind=str(raw.get("kind") or "shortform").strip() or "shortform",
                title=str(raw.get("title") or raw.get("topic") or "")[:1000],
                # ``active_jobs`` is itself the durable active-set. Older
                # entries intentionally contain only identity/title fields;
                # projecting an empty status makes the strict frontend drop
                # the exact job and recreates the "now I see nothing" failure.
                status=str(raw.get("status") or "running")[:128],
                stage=str(raw.get("stage") or raw.get("phase") or "")[:128],
                progress_percent=_job_progress(raw),
            )
        )

    deliverables: dict[str, dict[str, Any]] = {}
    for message in list(session.get("messages") or [])[-160:]:
        if not isinstance(message, dict):
            continue
        deliverable = message.get("jobDeliverable")
        if not isinstance(deliverable, dict):
            continue
        job_id = str(deliverable.get("job_id") or "").strip()
        if job_id and job_id not in blocked:
            deliverables[job_id] = deliverable
    cards: list[ProductionCardViewV1] = []
    for job_id, deliverable in deliverables.items():
        cards.append(
            ProductionCardViewV1(
                card_id=f"job:{job_id}",
                kind=str(deliverable.get("kind") or "job"),
                job_id=job_id,
                title=str(deliverable.get("title") or deliverable.get("topic") or "")[:1000],
                status=str(deliverable.get("status") or deliverable.get("stage") or "")[:128],
                body="",
                actions=[],
            )
        )

    pending = [
        row for row in list(session.get("pending_actions") or []) if isinstance(row, dict)
    ]
    for row in pending:
        action_id = str(row.get("id") or "").strip()
        if not action_id:
            continue
        arguments = row.get("arguments") if isinstance(row.get("arguments"), dict) else {}
        target_job_id = str(arguments.get("job_id") or "").strip()
        cards.append(
            ProductionCardViewV1(
                card_id=f"confirmation:{action_id}",
                kind="confirmation",
                job_id=target_job_id,
                title=str(row.get("summary") or row.get("tool") or "Production confirmation")[:1000],
                status="awaiting_confirmation",
                body="",
                actions=["confirm", "cancel"],
            )
        )

    command_view: ProductionCommandViewV1 | None = None
    notices: list[ProductionNoticeViewV1] = []
    latest = session.get("latest_production_command")
    if isinstance(latest, dict):
        steps = [step for step in list(latest.get("steps") or []) if isinstance(step, dict)]
        step = steps[-1] if steps else {}
        command_id = str(latest.get("command_id") or "")
        matching_workflows = [
            row
            for row in list(session.get("production_workflows") or [])
            if isinstance(row, dict)
            and str(row.get("command_id") or "") == command_id
        ]
        workflow = (
            max(
                matching_workflows,
                key=lambda row: (
                    int(row.get("revision") or 0),
                    float(row.get("updated_at") or 0.0),
                ),
            )
            if matching_workflows
            else {}
        )
        root_envelope = (
            latest.get("command_envelope")
            if isinstance(latest.get("command_envelope"), dict)
            else {}
        )
        action = _projection_action(
            latest.get("action") or root_envelope.get("action") or step.get("action"),
            tool_name=str(step.get("tool_name") or ""),
        )
        if action is not None:
            # The root command/workflow owns lifecycle. A child animation
            # receipt may remain "accepted" after the durable MP4 passes; it
            # must never override the terminal root and recreate an endless
            # "executing" card after refresh.
            lifecycle = _projection_lifecycle(
                workflow.get("status") or latest.get("status") or step.get("status")
            )
            progress = {
                "ready": 1,
                "executing": 35,
                "verifying": 90,
                "completed": 100,
                "failed": 100,
                "cancelled": 100,
                "compiled": 0,
            }[lifecycle]
            command_view = ProductionCommandViewV1(
                command_id=command_id,
                action=action,
                lifecycle=lifecycle,
                target_job_id=str(
                    (
                        root_envelope.get("target")
                        if isinstance(root_envelope.get("target"), dict)
                        else {}
                    ).get("job_id")
                    or step.get("target_id")
                    or workflow.get("job_id")
                    or ""
                ),
                progress_percent=progress,
                active_step=(
                    str(workflow.get("stage") or step.get("tool_name") or "")
                    if lifecycle == "executing"
                    else ""
                ),
                error=str(
                    workflow.get("last_error")
                    or latest.get("error")
                    or step.get("error")
                    or ""
                )[:4000],
            )
            if lifecycle == "failed" and command_view.error:
                notices.append(
                    ProductionNoticeViewV1(
                        notice_id=f"command-error:{command_view.command_id}",
                        level="error",
                        message=command_view.error,
                    )
                )

    allowed: list[ProductionCommandAction] = []
    kinds = {str(job.kind).lower() for job in jobs}
    if not jobs:
        allowed.extend(["start_short", "start_longform", "start_product_ad", "start_cliplab"])
    if "shortform" in kinds or "product_ad" in kinds:
        allowed.extend(
            [
                "expand_existing_short",
                "audit_and_repair_scenes",
                "approve_scenes",
                "animate_scenes",
                "ship_existing_short",
                "finalize",
                "cancel",
            ]
        )
    if "longform" in kinds:
        allowed.extend(
            [
                "expand_longform",
                "audit_and_repair_scenes",
                "generate_thumbnail",
                "finalize",
                "cancel",
            ]
        )
    if "cliplab" in kinds:
        allowed.extend(["analyze_cliplab", "render_cliplab", "cancel"])
    if mode == "plan":
        allowed = [
            value
            for value in allowed
            if value
            in {
                "start_short",
                "start_longform",
                "start_product_ad",
                "start_cliplab",
                "generate_thumbnail",
            }
        ]
    allowed = list(dict.fromkeys(allowed))

    pending_confirmation: PendingConfirmationViewV1 | None = None
    if pending:
        row = pending[0]
        action_id = str(row.get("id") or "").strip()
        if action_id:
            pending_confirmation = PendingConfirmationViewV1(
                command_id=action_id,
                prompt=str(row.get("summary") or f"Confirm {row.get('tool') or 'production action'}.")[:1000],
            )

    state = session.get("production_state") if isinstance(session.get("production_state"), dict) else {}
    state_revision = (
        f"epoch:{int(state.get('epoch') or 1)}:"
        f"route:{int(session.get('media_route_revision') or 1)}:"
        f"command:{int(session.get('production_command_revision') or 0)}:"
        f"view:{int(session.get('production_view_revision') or 1)}"
    )
    return ProductionViewV1(
        session_id=sid,
        view_revision=max(1, int(session.get("production_view_revision") or 1)),
        state_revision=state_revision,
        effective_mode=mode,  # type: ignore[arg-type]
        command=command_view,
        jobs=jobs,
        cards=cards,
        allowed_actions=allowed,
        pending_confirmation=pending_confirmation,
        notices=notices,
    )
