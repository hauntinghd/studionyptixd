"""Deterministic semantic and policy validation for typed Studio commands."""
from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from pydantic import Field

from studio_agent.command_contract import (
    AnimationScope,
    ContractModel,
    RepairScope,
    StudioCommand,
    approval_authorization_evidence,
    contextual_confirmation_evidence,
    execution_authorization_evidence,
    execution_block_reason,
    extract_scene_numbers_request,
    infer_scene_repair_scope,
    normalized_text_contains,
    scene_repair_authorization_evidence,
    scene_repair_block_reason,
    scene_repair_candidate,
)
from studio_agent.command_state import CompactJobState, StudioStateContext


Decision = Literal["execute", "clarify", "reject", "no_op"]
IssueSeverity = Literal["info", "warning", "error"]


class ValidationIssue(ContractModel):
    code: str
    severity: IssueSeverity
    field: str = ""
    message: str


class ClarificationRequest(ContractModel):
    code: str
    question: str
    missing_fields: list[str] = Field(default_factory=list)


class LegacyExpandArguments(ContractModel):
    command_id: str
    job_id: str
    existing_scene_count: int = Field(ge=1, le=59)
    scene_count: int = Field(ge=2, le=60)
    preserve_scene_indices: list[int] = Field(default_factory=list, max_length=60)
    animate_scene_indices: list[int] = Field(default_factory=list, max_length=60)
    duration_seconds: float | None = Field(default=None, gt=0, le=43_200)
    creative_direction: str = Field(default="", max_length=2_000)
    animate_policy: Literal["heroes", "all", "none"]

    def as_legacy_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude_none=True)


class LegacySceneRepairArguments(ContractModel):
    command_id: str
    job_id: str
    scene_indices: list[int] = Field(min_length=1, max_length=60)
    reason: str = Field(default="", max_length=2_000)

    def as_legacy_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude_none=True)


class SceneAssetFingerprint(ContractModel):
    scene_number: int = Field(ge=1)
    still_sha256: str = ""
    clip_sha256: str = ""


class ExpandPostconditions(ContractModel):
    kind: Literal["expand_existing_short"] = "expand_existing_short"
    job_id: str
    expected_existing_scene_count: int = Field(ge=1)
    expected_total_scene_count: int = Field(ge=2, le=60)
    expected_added_scene_count: int = Field(ge=1, le=59)
    preserved_scene_numbers: list[int]
    animation_scope: AnimationScope
    expected_animated_scene_numbers: list[int]
    preserved_assets: list[SceneAssetFingerprint] = Field(default_factory=list)


class SceneRepairPostconditions(ContractModel):
    kind: Literal["scene_repair"] = "scene_repair"
    job_id: str
    selected_scene_numbers: list[int] = Field(min_length=1, max_length=60)
    untouched_scene_numbers: list[int] = Field(default_factory=list, max_length=60)
    expected_clip_scene_numbers: list[int] = Field(default_factory=list, max_length=60)
    repair_scope: RepairScope
    untouched_assets: list[SceneAssetFingerprint] = Field(default_factory=list)


class ResolvedToolAction(ContractModel):
    tool_name: Literal[
        "expand_visual_proof_shortform",
        "audit_and_repair_production_scenes",
    ]
    arguments: LegacyExpandArguments | LegacySceneRepairArguments
    expected: ExpandPostconditions | SceneRepairPostconditions
    state_revision: str


class CommandValidationResult(ContractModel):
    decision: Decision
    command_id: str
    issues: list[ValidationIssue] = Field(default_factory=list)
    clarification: ClarificationRequest | None = None
    resolved_action: ResolvedToolAction | None = None

    @property
    def can_execute(self) -> bool:
        return self.decision == "execute" and self.resolved_action is not None


FingerprintLoader = Callable[[str, list[int]], list[SceneAssetFingerprint] | list[dict[str, Any]]]


def _issue(code: str, message: str, *, field: str = "", severity: IssueSeverity = "error") -> ValidationIssue:
    return ValidationIssue(code=code, severity=severity, field=field, message=message)


def _clarify(
    command: StudioCommand,
    code: str,
    question: str,
    *,
    fields: list[str],
    issues: list[ValidationIssue] | None = None,
) -> CommandValidationResult:
    return CommandValidationResult(
        decision="clarify",
        command_id=command.command_id,
        issues=issues or [_issue(code, question, field=fields[0] if fields else "")],
        clarification=ClarificationRequest(code=code, question=question, missing_fields=fields),
    )


def _reject(
    command: StudioCommand,
    code: str,
    message: str,
    *,
    field: str = "",
) -> CommandValidationResult:
    return CommandValidationResult(
        decision="reject",
        command_id=command.command_id,
        issues=[_issue(code, message, field=field)],
    )


def _resolve_target(command: StudioCommand, state: StudioStateContext) -> tuple[CompactJobState | None, str]:
    explicit = str(command.target.job_id or "").strip()
    if explicit:
        return state.job(explicit), "explicit"
    if command.target.source == "reply_to" and state.reply_target_job_id:
        return state.job(state.reply_target_job_id), "reply_to"
    eligible = (
        state.repairable_short_jobs()
        if command.action == "audit_and_repair_scenes"
        else state.expandable_short_jobs()
    )
    if len(eligible) == 1:
        return eligible[0], "single_expandable"
    return None, "ambiguous" if len(eligible) > 1 else "missing"


def _duration_scene_count(duration_seconds: float, *, seconds_per_scene: float = 5.0) -> int:
    per = max(3.0, float(seconds_per_scene or 5.0))
    return max(2, min(60, int(round(float(duration_seconds) / per))))


def _load_fingerprints(
    loader: FingerprintLoader | None,
    job_id: str,
    scene_numbers: list[int],
) -> list[SceneAssetFingerprint]:
    if loader is None:
        return []
    try:
        raw = loader(job_id, scene_numbers)
    except Exception:
        return []
    out: list[SceneAssetFingerprint] = []
    for item in raw or []:
        try:
            out.append(item if isinstance(item, SceneAssetFingerprint) else SceneAssetFingerprint.model_validate(item))
        except Exception:
            continue
    return out


def _pending_repair_state(state: StudioStateContext) -> dict[str, Any]:
    pending = state.pending_command
    if pending is None or pending.action != "audit_and_repair_scenes":
        return {}
    return {
        "target_job_id": pending.target_job_id,
        **dict(pending.known_fields),
    }


def _validate_scene_repair(
    command: StudioCommand,
    state: StudioStateContext,
    *,
    user_text: str,
    fingerprint_loader: FingerprintLoader | None,
) -> CommandValidationResult:
    repair = command.repair
    if repair is None:
        return _reject(command, "missing_repair_payload", "The scene-repair request is incomplete.")

    block = scene_repair_block_reason(user_text)
    job, target_reason = _resolve_target(command, state)
    if job is None:
        if str(command.target.job_id or "").strip():
            return _reject(
                command,
                "target_job_not_found",
                "The requested production job is not part of this Studio session.",
                field="target.job_id",
            )
        if target_reason == "ambiguous":
            return _clarify(
                command,
                "ambiguous_target_job",
                "Which short should I repair? Reply to its production card.",
                fields=["target.job_id"],
            )
        return _clarify(
            command,
            "missing_target_job",
            "I could not find the short to repair. Reply directly to its production card.",
            fields=["target.job_id"],
        )
    if not job.ownership_verified:
        return _reject(
            command,
            "target_not_owned",
            "The target job is not verified as belonging to this session.",
            field="target.job_id",
        )
    if job.kind != "shortform" or not job.repairable_scene_review:
        return _reject(
            command,
            "target_not_repairable",
            "That job is not a verified short-form scene-review job.",
            field="target.job_id",
        )

    total = max(1, int(job.scene_count or len(job.scenes) or 1))
    selected = sorted(dict.fromkeys(int(number) for number in repair.scene_numbers))
    if not selected:
        return _clarify(
            command,
            "missing_scene_selection",
            "Which scene or scene range should I audit and repair?",
            fields=["repair.scene_numbers"],
        )
    if any(number < 1 or number > total for number in selected):
        return _reject(
            command,
            "invalid_scene_selection",
            f"This short has {total} scene(s); the requested range is outside that job.",
            field="repair.scene_numbers",
        )

    pending = _pending_repair_state(state)
    literal_selected = extract_scene_numbers_request(
        user_text,
        total_scenes=total,
        allow_bare=bool(pending),
    )
    pending_selected = sorted(
        dict.fromkeys(
            int(number)
            for number in pending.get("scene_numbers") or []
            if str(number).isdigit() and 1 <= int(number) <= total
        )
    )
    grounded_selected = literal_selected or pending_selected
    if grounded_selected != selected:
        return _reject(
            command,
            "ungrounded_scene_selection",
            "The selected scenes were not grounded in your message or the pending clarification.",
            field="repair.scene_numbers",
        )

    if block:
        return _clarify(
            command,
            "repair_confirmation_required",
            "Do you want me to audit and repair those scenes now, or are you only asking about them?",
            fields=["authorization.execution_quote"],
        )

    direct = scene_repair_authorization_evidence(user_text)
    confirmation = contextual_confirmation_evidence(user_text) if pending else ""
    scoped_followup = bool(
        pending
        and pending.get("execution_requested")
        and literal_selected
        and literal_selected == selected
    )
    authorization_signal = direct or confirmation or (str(user_text or "").strip() if scoped_followup else "")
    execution_grounded = bool(
        command.authorization.execution_requested
        and authorization_signal
        and normalized_text_contains(user_text, command.authorization.execution_quote)
    )
    if not execution_grounded:
        human = ", ".join(str(number) for number in selected)
        return _clarify(
            command,
            "repair_not_authorized",
            f"Do you want me to audit and repair Scene(s) {human} now?",
            fields=["authorization.execution_quote"],
        )

    if scene_repair_candidate(user_text):
        reason = str(user_text or "").strip()[:2_000]
        repair_scope = infer_scene_repair_scope(user_text)
    else:
        reason = str(pending.get("instruction") or "").strip()[:2_000]
        repair_scope = str(pending.get("repair_scope") or repair.scope)
    if repair_scope not in {
        "general_scene_quality",
        "narrative_alignment",
        "visual_quality",
        "animation_quality",
        "full_quality",
    }:
        repair_scope = "general_scene_quality"
    if not reason:
        reason = "Creator requested fresh quality and narrative-correspondence QA for the selected scenes."

    untouched = [number for number in range(1, total + 1) if number not in selected]
    untouched_assets = _load_fingerprints(fingerprint_loader, job.job_id, untouched)
    by_number = {scene.scene_number: scene for scene in job.scenes}
    expected_clips = [number for number in selected if by_number.get(number) and by_number[number].has_clip]
    expected = SceneRepairPostconditions(
        job_id=job.job_id,
        selected_scene_numbers=selected,
        untouched_scene_numbers=untouched,
        expected_clip_scene_numbers=expected_clips,
        repair_scope=repair_scope,  # type: ignore[arg-type]
        untouched_assets=untouched_assets,
    )
    resolved = ResolvedToolAction(
        tool_name="audit_and_repair_production_scenes",
        arguments=LegacySceneRepairArguments(
            command_id=command.command_id,
            job_id=job.job_id,
            scene_indices=[number - 1 for number in selected],
            reason=reason,
        ),
        expected=expected,
        state_revision=state.state_revision,
    )
    return CommandValidationResult(
        decision="execute",
        command_id=command.command_id,
        resolved_action=resolved,
        issues=[
            _issue(
                "resolved_scene_repair",
                f"Audit and repair only Scene(s) {selected} on the same job.",
                severity="info",
            )
        ],
    )


def validate_studio_command(
    command: StudioCommand,
    state: StudioStateContext,
    *,
    user_text: str,
    fingerprint_loader: FingerprintLoader | None = None,
) -> CommandValidationResult:
    """Resolve a proposal to one safe legacy action or one precise question."""

    if command.action == "conversation":
        return CommandValidationResult(decision="no_op", command_id=command.command_id)
    if command.action == "clarify":
        question = command.clarification_question or "What would you like Studio to do next?"
        return _clarify(command, "model_requested_clarification", question, fields=[])
    if command.action == "audit_and_repair_scenes":
        return _validate_scene_repair(
            command,
            state,
            user_text=user_text,
            fingerprint_loader=fingerprint_loader,
        )
    if command.action != "expand_existing_short" or command.expand is None:
        return _reject(command, "unsupported_action", f"Unsupported command action: {command.action}")

    execution_block = execution_block_reason(user_text)
    if execution_block in {"negated", "deferred"}:
        return _clarify(
            command,
            "execution_deferred",
            "Understood — I will keep Scene 1 unchanged and will not start the remaining scenes until you explicitly tell me to continue.",
            fields=["authorization.execution_quote"],
        )
    if execution_block == "hypothetical":
        return _clarify(
            command,
            "hypothetical_only",
            "I can explain the expansion approach, but I will not start production unless you explicitly approve Scene 1 and tell me to continue.",
            fields=["authorization.execution_quote"],
        )

    job, target_reason = _resolve_target(command, state)
    if job is None:
        if str(command.target.job_id or "").strip():
            return _reject(
                command,
                "target_job_not_found",
                "The requested production job is not part of this Studio session.",
                field="target.job_id",
            )
        if target_reason == "ambiguous":
            return _clarify(
                command,
                "ambiguous_target_job",
                "Which short should I expand? Reply to its Scene 1 card.",
                fields=["target.job_id"],
            )
        return _clarify(
            command,
            "missing_target_job",
            "I could not find the Scene 1 proof to expand. Reply directly to that production card.",
            fields=["target.job_id"],
        )
    if not job.ownership_verified:
        return _reject(
            command,
            "target_not_owned",
            "The target job is not verified as belonging to this session.",
            field="target.job_id",
        )
    if job.kind != "shortform":
        return _reject(command, "target_not_shortform", "Only a short-form proof can use this expansion action.")
    if not job.expandable_proof:
        return _reject(
            command,
            "target_not_expandable",
            "That job is not an expandable one-scene proof short.",
            field="target.job_id",
        )

    auth = command.authorization
    approval_evidence = approval_authorization_evidence(
        user_text,
        contextual_scene_review=bool(job.expandable_proof and job.ownership_verified),
    )
    approval_grounded = bool(
        auth.existing_work_approved
        and approval_evidence
        and normalized_text_contains(user_text, auth.approval_quote)
    )
    execution_grounded = bool(
        auth.execution_requested
        and execution_authorization_evidence(user_text)
        and normalized_text_contains(user_text, auth.execution_quote)
    )
    if not approval_grounded:
        return _clarify(
            command,
            "proof_not_approved",
            "Do you approve the current Scene 1 and want me to preserve it?",
            fields=["authorization.approval_quote"],
        )
    if not execution_grounded:
        return _clarify(
            command,
            "execution_not_authorized",
            "Should I start expanding this same short now?",
            fields=["authorization.execution_quote"],
        )

    current = max(1, int(job.scene_count or len(job.scenes) or 1))
    additional = command.expand.additional_scene_count
    total = command.expand.target_total_scene_count
    if additional is None and total is None and command.expand.duration_seconds:
        total = _duration_scene_count(command.expand.duration_seconds)
        additional = total - current
    elif additional is not None and total is None:
        total = current + additional
    elif total is not None and additional is None:
        additional = total - current
    if additional is None or total is None:
        return _clarify(
            command,
            "missing_scene_count",
            "How many additional scenes should I make?",
            fields=["expand.additional_scene_count"],
        )
    if additional <= 0 or total <= current:
        return _reject(
            command,
            "non_expanding_scene_count",
            "The requested scene count would not add any scenes to this short.",
            field="expand.additional_scene_count",
        )
    if total > 60:
        return _reject(
            command,
            "scene_limit_exceeded",
            "Short-form expansion supports at most 60 total scenes.",
            field="expand.target_total_scene_count",
        )
    if current + additional != total:
        return _clarify(
            command,
            "inconsistent_scene_count",
            f"This short has {current} scene(s). Do you want {additional} more ({current + additional} total) or {total} total?",
            fields=["expand.additional_scene_count", "expand.target_total_scene_count"],
        )

    preserved = command.expand.preserve_scene_numbers or list(range(1, current + 1))
    if any(number < 1 or number > current for number in preserved):
        return _reject(
            command,
            "invalid_preserve_selection",
            "A preserved scene must already exist in the current proof.",
            field="expand.preserve_scene_numbers",
        )
    preserved = list(dict.fromkeys(preserved))
    new_scene_numbers = list(range(current + 1, total + 1))
    animation_scope = command.expand.animation.scope
    animate_policy: Literal["heroes", "all", "none"]
    expected_animated: list[int]
    if animation_scope in {"new_scenes", "all_scenes"}:
        animate_policy = "all"
        expected_animated = new_scene_numbers if animation_scope == "new_scenes" else list(range(1, total + 1))
    elif animation_scope == "none":
        animate_policy = "none"
        expected_animated = []
    elif animation_scope in {"heroes", "unspecified"}:
        animate_policy = "heroes"
        expected_animated = []
    else:
        requested = sorted(set(command.expand.animation.scene_numbers))
        if requested == new_scene_numbers:
            animate_policy = "all"
            expected_animated = new_scene_numbers
            animation_scope = "new_scenes"
        elif not requested:
            animate_policy = "none"
            expected_animated = []
            animation_scope = "none"
        else:
            return _clarify(
                command,
                "explicit_animation_requires_followup",
                "Should I animate every new scene, only hero scenes, or none during this expansion?",
                fields=["expand.animation.scope"],
            )

    fingerprints = _load_fingerprints(fingerprint_loader, job.job_id, preserved)
    expected = ExpandPostconditions(
        job_id=job.job_id,
        expected_existing_scene_count=current,
        expected_total_scene_count=total,
        expected_added_scene_count=additional,
        preserved_scene_numbers=preserved,
        animation_scope=animation_scope,
        expected_animated_scene_numbers=expected_animated,
        preserved_assets=fingerprints,
    )
    resolved = ResolvedToolAction(
        tool_name="expand_visual_proof_shortform",
        arguments=LegacyExpandArguments(
            command_id=command.command_id,
            job_id=job.job_id,
            existing_scene_count=current,
            scene_count=total,
            preserve_scene_indices=[number - 1 for number in preserved],
            animate_scene_indices=[number - 1 for number in expected_animated],
            duration_seconds=command.expand.duration_seconds,
            creative_direction=command.expand.creative_direction,
            animate_policy=animate_policy,
        ),
        expected=expected,
        state_revision=state.state_revision,
    )
    return CommandValidationResult(
        decision="execute",
        command_id=command.command_id,
        resolved_action=resolved,
        issues=[
            _issue(
                "resolved_expand",
                f"Expand the same job from {current} to {total} scenes.",
                severity="info",
            )
        ],
    )
