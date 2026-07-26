"""Backend-owned production-command authority.

Every mutation/spend-capable Studio tool must execute inside one authority
issued by the backend.  The authority lives in a ``ContextVar`` so concurrent
chat streams, approval clicks, and retries cannot borrow one another's command
identity.

The public ``command_id`` identifies the creator's whole intent.  Each tool
step receives a deterministic ``mutation_id`` derived from that command plus
its exact target and arguments.  This distinction is important: one command
such as "approve, animate, and finish" legitimately contains several
idempotent mutations, while replaying any individual step must still be safe.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass
from typing import Any, Iterator, Literal


PRODUCTION_COMMAND_SCHEMA = "studio.production-command.v2"
PRODUCTION_MUTATION_SCHEMA = "studio.production-mutation.v2"

CommandSource = Literal[
    "chat_stream",
    "chat",
    "approval",
    "retry",
    "server_workflow",
    "test",
]


class ProductionCommandViolation(RuntimeError):
    """Raised before budget reservation when command authority is invalid."""


@dataclass(frozen=True)
class ProductionCommandAuthority:
    schema: str
    command_id: str
    user_id: str
    session_id: str
    source: str
    request_sha256: str
    execution_quote: str
    state_revision: int
    issued_at: float

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AuthorizedProductionMutation:
    schema: str
    command_id: str
    mutation_id: str
    user_id: str
    session_id: str
    source: str
    action: str
    tool_name: str
    target_kind: str
    target_id: str
    scene_indices: tuple[int, ...]
    arguments_sha256: str
    authorized_at: float

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["scene_indices"] = list(self.scene_indices)
        return payload


_PRODUCTION_COMMAND: ContextVar[ProductionCommandAuthority | None] = ContextVar(
    "studio_production_command",
    default=None,
)


def current_production_command_id() -> str:
    """Return the stable identity bound to the current agent turn, if any."""

    authority = _PRODUCTION_COMMAND.get()
    return str(authority.command_id if authority else "").strip()


def current_production_command() -> ProductionCommandAuthority | None:
    """Return the complete backend-issued authority for this execution path."""

    return _PRODUCTION_COMMAND.get()


def _request_sha256(user_text: str | None) -> str:
    text = str(user_text or "")
    return hashlib.sha256(text.encode("utf-8")).hexdigest() if text else ""


def _canonical_public_arguments(arguments: dict[str, Any] | None) -> dict[str, Any]:
    """Return stable command input without runtime billing/provider internals."""

    return {
        str(key): value
        for key, value in dict(arguments or {}).items()
        if not str(key).startswith("_credit_")
        and not str(key).startswith("_billing_")
        and str(key)
        not in {
            "_production_command_id",
            "_production_mutation_id",
            "_runpod_command_id",
        }
    }


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )


def _target(arguments: dict[str, Any]) -> tuple[str, str]:
    tool_name = str(arguments.get("_tool_name") or "").strip()
    job_id = str(
        arguments.get("job_id")
        or arguments.get("studio_job_id")
        or arguments.get("_resume_job_id")
        or arguments.get("_requested_job_id")
        or arguments.get("analyze_job_id")
        or (
            arguments.get("session_id")
            if "longform" in tool_name
            else ""
        )
        or (
            arguments.get("video_id")
            if "cliplab" in tool_name and tool_name != "ingest_cliplab_attachment"
            else ""
        )
        or ""
    ).strip()
    if job_id:
        kind = str(arguments.get("kind") or "").strip().lower()
        if not kind:
            kind = (
                "reference_analysis"
                if tool_name == "retry_reference_analysis"
                else
                "cliplab"
                if "cliplab" in tool_name
                else "longform"
                if "longform" in tool_name
                else "shortform"
            )
        return kind, job_id
    if str(arguments.get("_tool_name") or "") == "generate_longform_thumbnails":
        return "new_production", ""
    channel = str(arguments.get("channel_key") or arguments.get("channel_id") or "").strip()
    if channel:
        return "channel", channel
    return "new_production", ""


def _scene_indices(arguments: dict[str, Any]) -> tuple[int, ...]:
    values: list[Any] = []
    for key in ("scene_indices", "selected_scene_indices", "repair_scene_indices"):
        candidate = arguments.get(key)
        if isinstance(candidate, list):
            values.extend(candidate)
    single = arguments.get("scene_index")
    if single is not None:
        values.append(single)
    normalized: set[int] = set()
    for value in values:
        try:
            index = int(value)
        except (TypeError, ValueError):
            continue
        if index >= 0:
            normalized.add(index)
    return tuple(sorted(normalized))


def _action_for_tool(tool_name: str) -> str:
    """Map implementation tools onto stable creator-facing command actions."""

    name = str(tool_name or "").strip()
    explicit = {
        "start_shortform_generate": "start_short",
        "expand_visual_proof_shortform": "expand_existing_short",
        "set_production_scenes_animate": "approve_scenes",
        "animate_production_scenes": "animate_scenes",
        "repair_production_scene_animation": "audit_and_repair_scenes",
        "audit_and_repair_production_scenes": "audit_and_repair_scenes",
        "regenerate_production_scene": "audit_and_repair_scenes",
        "regenerate_production_scenes": "audit_and_repair_scenes",
        "regenerate_production_scene_still": "audit_and_repair_scenes",
        "edit_production_scene_still": "audit_and_repair_scenes",
        "edit_production_scenes_still": "audit_and_repair_scenes",
        "set_production_scene_duration": "audit_and_repair_scenes",
        "set_production_scene_prompt": "audit_and_repair_scenes",
        "finalize_production": "finalize",
        "re_edit_production": "audit_and_repair_scenes",
        "cancel_production_job": "cancel",
        "generate_longform_outline": "generate_longform_outline",
        "expand_longform_chapter": "expand_longform_chapter",
        "start_longform_render": "start_longform",
        "expand_longform_visual_proof": "expand_longform",
        "finalize_longform_render": "finalize",
        "generate_longform_thumbnails": "generate_thumbnail",
        "regenerate_longform_thumbnail": "generate_thumbnail",
        "regenerate_longform_still": "audit_and_repair_scenes",
        "cancel_longform_render": "cancel",
        "ingest_cliplab_attachment": "start_cliplab",
        "analyze_cliplab_video": "analyze_cliplab",
        "render_cliplab_segments": "render_cliplab",
        "remix_cliplab_short": "render_cliplab",
        "analyze_reference_video": "analyze_reference",
        "analyze_competitor_video": "analyze_reference",
        "retry_reference_analysis": "retry_reference_analysis",
    }
    return explicit.get(name, name)


def command_contract_enforcement() -> str:
    """Return ``strict`` or ``audit``; malformed values fail closed to strict."""

    value = str(os.getenv("STUDIO_PRODUCTION_COMMAND_ENFORCEMENT", "strict")).strip().lower()
    return "audit" if value in {"audit", "shadow", "warn"} else "strict"


def validate_production_command_authority(
    tool_name: str,
    *,
    user_id: str,
    session_id: str | None,
) -> ProductionCommandAuthority | None:
    """Validate principal/session authority before any state or provider read."""

    authority = current_production_command()
    actual_user = str(user_id or "").strip()
    actual_session = str(session_id or "").strip()
    if authority is None:
        if command_contract_enforcement() == "audit":
            return None
        raise ProductionCommandViolation(
            f"{tool_name} rejected: no backend production command authority"
        )
    if authority.user_id and authority.user_id != actual_user:
        raise ProductionCommandViolation(
            f"{tool_name} rejected: command user does not match authenticated user"
        )
    if authority.session_id and authority.session_id != actual_session:
        raise ProductionCommandViolation(
            f"{tool_name} rejected: command session does not match execution session"
        )
    if not actual_user:
        raise ProductionCommandViolation(f"{tool_name} rejected: authenticated user is required")
    return authority


def authorize_production_mutation(
    tool_name: str,
    arguments: dict[str, Any] | None,
    *,
    user_id: str,
    session_id: str | None,
) -> tuple[dict[str, Any], AuthorizedProductionMutation | None]:
    """Authorize one exact mutation beneath the current backend command.

    Callers decide which tools are protected and invoke this function before
    any budget estimate, provider request, or credit reservation.
    """

    args = dict(arguments or {})
    actual_user = str(user_id or "").strip()
    actual_session = str(session_id or "").strip()
    authority = validate_production_command_authority(
        tool_name,
        user_id=actual_user,
        session_id=actual_session,
    )
    if authority is None:
        return args, None

    public_args = _canonical_public_arguments(args)
    action = _action_for_tool(tool_name)
    target_args = {**public_args, "_tool_name": str(tool_name or "")}
    target_kind, target_id = _target(target_args)
    arguments_sha256 = hashlib.sha256(_canonical_json(public_args).encode("utf-8")).hexdigest()
    derived = hashlib.sha256(
        _canonical_json(
            {
                "schema": PRODUCTION_MUTATION_SCHEMA,
                "command_id": authority.command_id,
                "action": action,
                "tool_name": str(tool_name or ""),
                "target_kind": target_kind,
                "target_id": target_id,
                "arguments_sha256": arguments_sha256,
            }
        ).encode("utf-8")
    ).hexdigest()
    mutation_id = f"mut_{derived[:32]}"
    supplied_parent = str(args.get("_production_command_id") or "").strip()
    if supplied_parent and supplied_parent != authority.command_id:
        raise ProductionCommandViolation(
            f"{tool_name} rejected: supplied production command does not own this execution"
        )

    # Runtime/provider code may still read command_id. Always use the
    # deterministic mutation identity there; retain the parent separately.
    args["_production_command_id"] = authority.command_id
    args["_production_mutation_id"] = mutation_id
    args["_runpod_command_id"] = mutation_id
    args["command_id"] = mutation_id
    mutation = AuthorizedProductionMutation(
        schema=PRODUCTION_MUTATION_SCHEMA,
        command_id=authority.command_id,
        mutation_id=mutation_id,
        user_id=actual_user,
        session_id=actual_session,
        source=authority.source,
        action=action,
        tool_name=str(tool_name or "").strip(),
        target_kind=target_kind,
        target_id=target_id,
        scene_indices=_scene_indices(public_args),
        arguments_sha256=arguments_sha256,
        authorized_at=time.time(),
    )
    return args, mutation


@contextmanager
def production_command_scope(
    command_id: str | None,
    *,
    user_id: str = "",
    session_id: str = "",
    source: CommandSource | str = "server_workflow",
    user_text: str = "",
    state_revision: int = 0,
) -> Iterator[ProductionCommandAuthority]:
    """Issue and bind one server-owned command for a complete workflow."""

    normalized = str(command_id or "").strip()
    if not normalized:
        raise ProductionCommandViolation("backend production command_id is required")
    authority = ProductionCommandAuthority(
        schema=PRODUCTION_COMMAND_SCHEMA,
        command_id=normalized,
        user_id=str(user_id or "").strip(),
        session_id=str(session_id or "").strip(),
        source=str(source or "server_workflow").strip(),
        request_sha256=_request_sha256(user_text),
        execution_quote=str(user_text or "").strip()[:300],
        state_revision=max(0, int(state_revision or 0)),
        issued_at=time.time(),
    )
    token = _PRODUCTION_COMMAND.set(authority)
    try:
        yield authority
    finally:
        _PRODUCTION_COMMAND.reset(token)


__all__ = [
    "AuthorizedProductionMutation",
    "PRODUCTION_COMMAND_SCHEMA",
    "PRODUCTION_MUTATION_SCHEMA",
    "ProductionCommandAuthority",
    "ProductionCommandViolation",
    "authorize_production_mutation",
    "command_contract_enforcement",
    "current_production_command",
    "current_production_command_id",
    "production_command_scope",
    "validate_production_command_authority",
]
