"""Trusted compiler for the backend-owned production command contract.

Legacy runner branches may still decide *when* to request a tool while they are
being removed, but they cannot execute that tool directly.  At the common
mutation boundary this module compiles the exact authenticated principal,
session, target, scene scope, media route, idempotency key, authorization
evidence, and observable postconditions into ``ProductionCommandEnvelopeV2``.
Provider work is rejected if that compilation fails.
"""
from __future__ import annotations

import hashlib
import json
import time
from typing import Any

from studio_agent.command_contract import (
    AnalyzeReferenceOperation,
    AnimationDirective,
    AnalyzeClipLabOperation,
    AnimateScenesOperation,
    ApproveScenesOperation,
    AuditAndRepairScenesOperation,
    CancelOperation,
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
)


def _scene_numbers(arguments: dict[str, Any]) -> list[int]:
    numbers: list[int] = []
    explicit = arguments.get("scene_numbers")
    if isinstance(explicit, list):
        for value in explicit:
            try:
                number = int(value)
            except (TypeError, ValueError):
                continue
            if number > 0:
                numbers.append(number)
    for key in ("scene_indices", "selected_scene_indices", "repair_scene_indices"):
        values = arguments.get(key)
        if not isinstance(values, list):
            continue
        for value in values:
            try:
                index = int(value)
            except (TypeError, ValueError):
                continue
            if index >= 0:
                numbers.append(index + 1)
    if arguments.get("scene_index") is not None:
        try:
            index = int(arguments.get("scene_index"))
            if index >= 0:
                numbers.append(index + 1)
        except (TypeError, ValueError):
            pass
    return list(dict.fromkeys(numbers))


def _kind(tool_name: str, arguments: dict[str, Any], action: str) -> str:
    if action in {"analyze_reference", "retry_reference_analysis"}:
        return "reference_analysis"
    if action in {"start_cliplab", "analyze_cliplab", "render_cliplab"}:
        return "cliplab"
    if action == "start_product_ad" or arguments.get("product_reference_id"):
        return "product_ad"
    if action in {
        "generate_longform_outline",
        "expand_longform_chapter",
    } or "longform" in str(tool_name or "").lower():
        return "longform"
    return "shortform"


def _operation(action: str, arguments: dict[str, Any], scenes: list[int]):
    if action == "analyze_reference":
        return AnalyzeReferenceOperation(
            source="url" if str(arguments.get("url") or "").strip() else "upload",
            content_format=str(arguments.get("content_format") or "short"),
        )
    if action == "retry_reference_analysis":
        return RetryReferenceAnalysisOperation(
            stages=[
                str(stage).strip()
                for stage in list(arguments.get("stages") or [])
                if str(stage).strip()
            ],
        )
    if action == "generate_longform_outline":
        return GenerateLongformOutlineOperation(
            topic=str(arguments.get("topic") or ""),
            channel_key=str(arguments.get("channel_key") or ""),
            target_minutes=arguments.get("target_minutes"),
        )
    if action == "expand_longform_chapter":
        chapter = arguments.get("chapter") if isinstance(arguments.get("chapter"), dict) else {}
        return ExpandLongformChapterOperation(
            outline_title=str(arguments.get("outline_title") or ""),
            chapter_index=max(0, int(chapter.get("index") or 0)),
        )
    if action == "start_short":
        return StartShortOperation(
            brief=str(
                arguments.get("visual_brief")
                or arguments.get("script")
                or arguments.get("topic")
                or arguments.get("title")
                or ""
            ),
            scene_count=arguments.get("scene_count"),
            duration_seconds=arguments.get("duration_seconds") or arguments.get("duration_sec"),
        )
    if action == "start_longform":
        return StartLongformOperation(
            brief=str(
                arguments.get("brief")
                or arguments.get("topic")
                or arguments.get("title")
                or ""
            ),
            target_duration_seconds=(
                arguments.get("target_duration_seconds")
                or arguments.get("duration_seconds")
            ),
        )
    if action == "start_product_ad":
        return StartProductAdOperation(
            brief=str(
                arguments.get("visual_brief")
                or arguments.get("script")
                or arguments.get("topic")
                or ""
            ),
            product_name=str(arguments.get("product_name") or arguments.get("title") or ""),
            duration_seconds=arguments.get("duration_seconds") or arguments.get("duration_sec"),
        )
    if action == "expand_existing_short":
        existing = int(arguments.get("existing_scene_count") or 0)
        total = int(arguments.get("scene_count") or arguments.get("target_total_scene_count") or 0)
        additional = int(arguments.get("additional_scene_count") or max(0, total - existing))
        preserve = [
            int(value) + 1
            for value in list(arguments.get("preserve_scene_indices") or [])
            if str(value).lstrip("-").isdigit() and int(value) >= 0
        ]
        animate = [
            int(value) + 1
            for value in list(arguments.get("animate_scene_indices") or [])
            if str(value).lstrip("-").isdigit() and int(value) >= 0
        ]
        requested_animation_scope = str(
            arguments.get("animate_policy") or "unspecified"
        ).strip()
        animation_scope = {
            "selected_scenes": "explicit",
            "hero_scenes": "heroes",
            "all_scenes": "all_scenes",
            "new_scenes": "new_scenes",
            "none": "none",
            "unspecified": "unspecified",
        }.get(requested_animation_scope, "unspecified")
        if animate:
            animation_scope = "explicit"
        return ExpandExistingShortOperation(
            request=ExpandExistingShortRequest(
                additional_scene_count=additional or None,
                target_total_scene_count=total or None,
                preserve_scene_numbers=preserve,
                duration_seconds=arguments.get("duration_seconds"),
                creative_direction=str(arguments.get("creative_direction") or ""),
                animation=AnimationDirective(
                    scope=animation_scope,
                    scene_numbers=animate,
                ),
            )
        )
    if action == "expand_longform":
        return ExpandLongformOperation(
            instruction=str(
                arguments.get("instruction")
                or arguments.get("brief")
                or arguments.get("topic")
                or ""
            )
        )
    if action == "audit_and_repair_scenes":
        return AuditAndRepairScenesOperation(
            request=SceneRepairRequest(
                scene_numbers=scenes,
                scope="general_scene_quality",
                instruction=str(
                    arguments.get("instruction")
                    or arguments.get("feedback")
                    or arguments.get("prompt")
                    or ""
                ),
            )
        )
    if action == "approve_scenes":
        if not scenes:
            raise ValueError("approve_scenes requires backend-resolved exact scene scope")
        return ApproveScenesOperation(scene_numbers=scenes)
    if action == "animate_scenes":
        if not scenes:
            raise ValueError("animate_scenes requires backend-resolved exact scene scope")
        return AnimateScenesOperation(scene_numbers=scenes, only_missing=True)
    if action == "finalize":
        return FinalizeOperation()
    if action == "cancel":
        return CancelOperation(reason=str(arguments.get("reason") or "Creator requested cancellation."))
    if action == "generate_thumbnail":
        return GenerateThumbnailOperation(
            prompt=str(arguments.get("prompt") or arguments.get("feedback") or ""),
            scene_number=(scenes[0] if scenes else None),
        )
    if action == "start_cliplab":
        return StartClipLabOperation(
            brief=str(
                arguments.get("brief")
                or arguments.get("instruction")
                or arguments.get("attachment_name")
                or ""
            )
        )
    if action == "analyze_cliplab":
        return AnalyzeClipLabOperation(
            prompt=str(arguments.get("prompt") or arguments.get("instruction") or ""),
            max_segments=max(1, min(int(arguments.get("max_segments") or 12), 40)),
        )
    if action == "render_cliplab":
        return RenderClipLabOperation(
            instruction=str(
                arguments.get("instruction")
                or arguments.get("feedback")
                or arguments.get("brief")
                or ""
            )
        )
    raise ValueError(f"unsupported production command action: {action}")


def _postconditions(action: str, scenes: list[int]) -> list[ProductionPostconditionV2]:
    if action == "analyze_reference":
        return [ProductionPostconditionV2(kind="job_created")]
    if action == "retry_reference_analysis":
        return [ProductionPostconditionV2(kind="reference_analysis_ready")]
    if action == "generate_longform_outline":
        return [ProductionPostconditionV2(kind="outline_ready")]
    if action == "expand_longform_chapter":
        return [ProductionPostconditionV2(kind="chapter_ready")]
    if action in {
        "start_short",
        "start_longform",
        "start_product_ad",
        "start_cliplab",
        "expand_existing_short",
    }:
        return [ProductionPostconditionV2(kind="job_created")]
    if action == "expand_longform":
        return [ProductionPostconditionV2(kind="job_updated")]
    if action == "audit_and_repair_scenes":
        return [ProductionPostconditionV2(kind="scene_qa_pass", scene_numbers=scenes)]
    if action == "approve_scenes":
        return [ProductionPostconditionV2(kind="scenes_approved", scene_numbers=scenes)]
    if action == "animate_scenes":
        return [ProductionPostconditionV2(kind="clips_ready", scene_numbers=scenes)]
    if action == "finalize":
        return [ProductionPostconditionV2(kind="artifact_ready", artifact_type="mp4")]
    if action == "cancel":
        return [ProductionPostconditionV2(kind="job_cancelled")]
    if action == "generate_thumbnail":
        return [ProductionPostconditionV2(kind="thumbnail_ready", artifact_type="thumbnail")]
    if action == "render_cliplab":
        return [ProductionPostconditionV2(kind="artifact_ready", artifact_type="mp4")]
    if action == "analyze_cliplab":
        return [ProductionPostconditionV2(kind="analysis_ready")]
    return []


def compile_authorized_mutation(
    *,
    authority: dict[str, Any],
    mutation: dict[str, Any],
    arguments: dict[str, Any],
    session: dict[str, Any],
) -> ProductionCommandEnvelopeV2:
    """Compile one exact, already-authenticated tool step into V2."""

    session_id = str(authority.get("session_id") or "").strip()
    user_id = str(authority.get("user_id") or "").strip()
    if not session_id or not user_id:
        raise ValueError("production-command-v2 requires authenticated session and user bindings")
    tool_name = str(mutation.get("tool_name") or "").strip()
    action = str(mutation.get("action") or "").strip()
    if tool_name == "start_shortform_generate" and arguments.get("product_reference_id"):
        action = "start_product_ad"
    if action not in {
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
        "finalize",
        "cancel",
        "generate_thumbnail",
        "start_cliplab",
        "analyze_cliplab",
        "render_cliplab",
    }:
        raise ValueError(f"{tool_name} is not mapped to production-command-v2")

    target_job_id = str(mutation.get("target_id") or "").strip()
    start = action in {
        "analyze_reference",
        "generate_longform_outline",
        "expand_longform_chapter",
        "start_short",
        "start_longform",
        "start_product_ad",
        "start_cliplab",
    } or (
        action == "generate_thumbnail" and not target_job_id
    )
    kind = _kind(tool_name, arguments, action)
    scenes = _scene_numbers(arguments)
    route = {
        "revision": int(
            arguments.get("_media_route_revision")
            or session.get("media_route_revision")
            or 1
        ),
        "image_model": str(
            arguments.get("image_model_id")
            or arguments.get("image_model")
            or session.get("image_model")
            or ""
        ),
        "video_model": str(
            arguments.get("video_model")
            or session.get("video_model")
            or ""
        ),
        "speech_model": str(session.get("speech_model") or "fal_minimax"),
    }
    route_sha = hashlib.sha256(
        json.dumps(route, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    quote = str(authority.get("execution_quote") or "").strip()
    if not quote:
        quote = f"Explicit backend {authority.get('source') or 'workflow'} action."
    return ProductionCommandEnvelopeV2(
        command_id=str(mutation.get("mutation_id") or ""),
        turn_id=str(authority.get("command_id") or ""),
        session_id=session_id,
        user_id=user_id,
        state_revision=(
            f"view:{int(session.get('production_view_revision') or 1)}:"
            f"command:{int(session.get('production_command_revision') or 0)}"
        ),
        action=action,
        target=ProductionCommandTargetV2(
            source="none" if start else "explicit_job_id",
            job_id="" if start else target_job_id,
            kind=kind,
            owner_session_id="" if start else session_id,
            owner_user_id="" if start else user_id,
            expected_job_revision=(
                ""
                if start
                else str(arguments.get("_expected_job_revision") or session.get("production_view_revision") or 1)
            ),
        ),
        operation=_operation(action, arguments, scenes),
        authorization=ProductionAuthorizationV2(
            execution_requested=True,
            execution_quote=quote,
            confirmation_required=False,
            confirmed=bool(authority.get("source") in {"approval", "server_workflow"}),
            confirmation_id=(
                str(authority.get("command_id") or "")
                if authority.get("source") in {"approval", "server_workflow"}
                else ""
            ),
        ),
        media_route=ProductionMediaRouteV2(**route, route_sha256=route_sha),
        expected_postconditions=_postconditions(action, scenes),
        idempotency_key=str(mutation.get("mutation_id") or ""),
        source_text_sha256=str(authority.get("request_sha256") or ""),
        created_at=float(mutation.get("authorized_at") or time.time()),
    )


def compile_ship_existing_short_workflow(
    *,
    authority: dict[str, Any],
    session: dict[str, Any],
    job_id: str,
    scene_numbers: list[int],
    animate: bool,
) -> ProductionCommandEnvelopeV2:
    """Compile approve/QA/animate/finalize as one creator-facing command."""

    session_id = str(authority.get("session_id") or "").strip()
    user_id = str(authority.get("user_id") or "").strip()
    target_job_id = str(job_id or "").strip()
    if not session_id or not user_id or not target_job_id or not scene_numbers:
        raise ValueError("ship_existing_short requires exact owner, job, and scene scope")
    route = {
        "revision": int(session.get("media_route_revision") or 1),
        "image_model": str(session.get("image_model") or ""),
        "video_model": str(session.get("video_model") or ""),
        "speech_model": str(session.get("speech_model") or "fal_minimax"),
    }
    route_sha = hashlib.sha256(
        json.dumps(route, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    quote = str(authority.get("execution_quote") or "").strip() or "Ship this existing short."
    postconditions = [
        ProductionPostconditionV2(kind="scene_qa_pass", scene_numbers=scene_numbers),
        ProductionPostconditionV2(kind="scenes_approved", scene_numbers=scene_numbers),
    ]
    if animate:
        postconditions.append(
            ProductionPostconditionV2(kind="clips_ready", scene_numbers=scene_numbers)
        )
    postconditions.append(
        ProductionPostconditionV2(kind="artifact_ready", artifact_type="mp4")
    )
    command_id = str(authority.get("command_id") or "").strip()
    return ProductionCommandEnvelopeV2(
        command_id=command_id,
        turn_id=command_id,
        session_id=session_id,
        user_id=user_id,
        state_revision=(
            f"view:{int(session.get('production_view_revision') or 1)}:"
            f"command:{int(session.get('production_command_revision') or 0)}"
        ),
        action="ship_existing_short",
        target=ProductionCommandTargetV2(
            source="explicit_job_id",
            job_id=target_job_id,
            kind="shortform",
            owner_session_id=session_id,
            owner_user_id=user_id,
            expected_job_revision=str(session.get("production_view_revision") or 1),
        ),
        operation=ShipExistingShortOperation(
            scene_numbers=scene_numbers,
            preserve_passing_assets=True,
            repair_failed_scenes=True,
            animate_only_missing=True,
        ),
        authorization=ProductionAuthorizationV2(
            execution_requested=True,
            execution_quote=quote,
            confirmation_required=False,
            confirmed=False,
        ),
        media_route=ProductionMediaRouteV2(**route, route_sha256=route_sha),
        expected_postconditions=postconditions,
        idempotency_key=command_id,
        source_text_sha256=str(authority.get("request_sha256") or ""),
        created_at=float(authority.get("issued_at") or time.time()),
    )


__all__ = ["compile_authorized_mutation", "compile_ship_existing_short_workflow"]
