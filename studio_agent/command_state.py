"""Compact, authoritative state supplied to the Studio command planner."""
from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Callable
from typing import Any, Literal

from pydantic import Field

from studio_agent.command_contract import (
    CommandAction,
    ContractModel,
    execution_block_reason,
    extract_scene_count_request,
)


JobKind = Literal["shortform", "longform", "competitor", "cliplab"]
JobRole = Literal["reply_target", "active", "recent"]


class CompactSceneState(ContractModel):
    scene_number: int = Field(ge=1)
    status: str = ""
    approved_for_video: bool = False
    approved_for_animation: bool = False
    animate: bool = False
    has_clip: bool = False
    duration_seconds: float = Field(default=0.0, ge=0.0)
    narration_excerpt: str = Field(default="", max_length=500)
    scene_action_excerpt: str = Field(default="", max_length=500)


class CompactJobState(ContractModel):
    job_id: str
    kind: JobKind
    role: JobRole
    title: str = ""
    status: str = ""
    stage: str = ""
    scene_count: int = Field(default=0, ge=0)
    planned_scene_count: int = Field(default=0, ge=0)
    expandable_proof: bool = False
    repairable_scene_review: bool = False
    ownership_verified: bool = False
    scenes: list[CompactSceneState] = Field(default_factory=list, max_length=60)


class PendingCommandState(ContractModel):
    action: CommandAction
    target_job_id: str = ""
    known_fields: dict[str, Any] = Field(default_factory=dict)
    missing_fields: list[str] = Field(default_factory=list)


class StudioStateContext(ContractModel):
    schema_version: Literal["studio-state-v1"] = "studio-state-v1"
    session_id: str = Field(exclude=True)
    state_revision: str
    agent_mode: Literal["plan", "studio", "cliplab"] = "studio"
    approval_mode: Literal["auto", "confirm"] = "confirm"
    content_format: Literal["short", "long", "both"] = "both"
    reply_target_job_id: str = ""
    reply_target_scene_number: int | None = Field(default=None, ge=1)
    jobs: list[CompactJobState] = Field(default_factory=list, max_length=8)
    pending_command: PendingCommandState | None = None
    recent_expansion_additional_scene_count: int = Field(default=0, ge=0, le=60)
    recent_expansion_total_scene_count: int = Field(default=0, ge=0, le=60)
    recent_expansion_evidence: str = Field(default="", max_length=300)
    available_actions: list[CommandAction] = Field(default_factory=list)

    def model_payload(self) -> dict[str, Any]:
        """State safe to serialize to an external model (no user/session ids)."""

        return self.model_dump(mode="json", exclude={"session_id"}, exclude_none=True)

    def job(self, job_id: str) -> CompactJobState | None:
        wanted = str(job_id or "").strip()
        return next((item for item in self.jobs if item.job_id == wanted), None)

    def expandable_short_jobs(self) -> list[CompactJobState]:
        return [
            item
            for item in self.jobs
            if item.kind == "shortform" and item.expandable_proof and item.ownership_verified
        ]

    def repairable_short_jobs(self) -> list[CompactJobState]:
        return [
            item
            for item in self.jobs
            if item.kind == "shortform"
            and item.repairable_scene_review
            and item.ownership_verified
        ]


def _safe_kind(raw: Any) -> JobKind:
    value = str(raw or "shortform").strip().lower()
    if value in {"shortform", "longform", "competitor", "cliplab"}:
        return value  # type: ignore[return-value]
    return "shortform"


def compact_job_snapshot(
    snapshot: dict[str, Any],
    *,
    role: JobRole,
    expandable_proof: bool,
    ownership_verified: bool,
    repairable_scene_review: bool = False,
) -> CompactJobState:
    """Reduce a UI job snapshot to command-relevant, 1-based scene state."""

    raw_scenes = list(snapshot.get("scenes") or [])
    scenes: list[CompactSceneState] = []
    for fallback_index, raw in enumerate(raw_scenes):
        if not isinstance(raw, dict):
            continue
        try:
            zero_index = int(raw.get("index", fallback_index))
        except (TypeError, ValueError):
            zero_index = fallback_index
        try:
            duration = float(raw.get("duration_sec", raw.get("duration_seconds", 0.0)) or 0.0)
        except (TypeError, ValueError):
            duration = 0.0
        scenes.append(
            CompactSceneState(
                scene_number=max(1, zero_index + 1),
                status=str(raw.get("status") or ""),
                approved_for_video=bool(raw.get("approved_for_video")),
                approved_for_animation=bool(raw.get("approved_for_animation")),
                animate=bool(raw.get("animate")),
                has_clip=bool(raw.get("has_clip")),
                duration_seconds=max(0.0, duration),
                narration_excerpt=str(raw.get("narration") or "")[:500],
                scene_action_excerpt=str(
                    raw.get("scene_action") or raw.get("action") or raw.get("prompt") or ""
                )[:500],
            )
        )
    scene_count = _as_nonnegative_int(snapshot.get("current_scene"), default=len(scenes))
    if scene_count <= 0:
        scene_count = len(scenes)
    planned = _as_nonnegative_int(snapshot.get("total_scenes"), default=scene_count)
    planned = max(scene_count, planned)
    return CompactJobState(
        job_id=str(snapshot.get("job_id") or "").strip(),
        kind=_safe_kind(snapshot.get("kind")),
        role=role,
        title=str(snapshot.get("title") or "")[:240],
        status=str(snapshot.get("status") or ""),
        stage=str(snapshot.get("stage") or ""),
        scene_count=scene_count,
        planned_scene_count=planned,
        expandable_proof=bool(expandable_proof),
        repairable_scene_review=bool(repairable_scene_review),
        ownership_verified=bool(ownership_verified),
        scenes=scenes,
    )


def _as_nonnegative_int(value: Any, *, default: int = 0) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return max(0, int(default))


def _call_snapshot_loader(
    loader: Callable[..., dict[str, Any]],
    job_id: str,
    kind: str,
) -> dict[str, Any]:
    result = loader(job_id, kind)
    if inspect.isawaitable(result):
        raise TypeError("snapshot_loader must be synchronous when building command state")
    return result if isinstance(result, dict) else {}


def _legacy_pending_command(session: dict[str, Any]) -> PendingCommandState | None:
    pending_repair = session.get("pending_scene_repair")
    if isinstance(pending_repair, dict) and pending_repair:
        known = {
            key: pending_repair[key]
            for key in (
                "scene_numbers",
                "repair_scope",
                "instruction",
                "execution_requested",
            )
            if pending_repair.get(key) not in (None, "", [])
        }
        return PendingCommandState(
            action="audit_and_repair_scenes",
            target_job_id=str(pending_repair.get("job_id") or ""),
            known_fields=known,
            missing_fields=[str(item) for item in pending_repair.get("missing_fields") or []],
        )
    raw = session.get("short_expansion_intake")
    if not isinstance(raw, dict) or not raw:
        return None
    step = str(raw.get("step") or "").strip().lower()
    missing = {
        "duration": ["duration_or_scene_count"],
        "creative": ["creative_direction"],
        "confirm": ["confirmation"],
    }.get(step, [])
    known = {
        key: raw[key]
        for key in ("duration_seconds", "creative_direction", "animate_policy")
        if raw.get(key) not in (None, "")
    }
    return PendingCommandState(
        action="expand_existing_short",
        target_job_id=str(raw.get("job_id") or ""),
        known_fields=known,
        missing_fields=missing,
    )


def _recent_expansion_cardinality(
    session: dict[str, Any],
    *,
    existing_count: int,
) -> tuple[int, int, str]:
    """Carry forward the latest explicit, non-negated scene count on this job."""

    production_state = session.get("production_state")
    try:
        cutoff = float(production_state.get("advanced_at") or 0.0) if isinstance(production_state, dict) else 0.0
    except (TypeError, ValueError):
        cutoff = 0.0
    seen: set[str] = set()
    for run in reversed(list(session.get("runs") or [])[-48:]):
        if not isinstance(run, dict):
            continue
        try:
            created_at = float(run.get("created_at") or 0.0)
        except (TypeError, ValueError):
            created_at = 0.0
        if cutoff and created_at and created_at < cutoff:
            continue
        text = str(run.get("message_preview") or "").strip()
        normalized = " ".join(text.casefold().split())
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        if execution_block_reason(text):
            continue
        additional, total = extract_scene_count_request(text, existing_count=existing_count)
        if additional and total and total > existing_count:
            return int(additional), int(total), text[:300]
    return 0, 0, ""


def build_studio_state_context(
    session: dict[str, Any],
    *,
    reply_to: dict[str, Any] | None = None,
    expandable_job_id: str | None = None,
    repairable_job_ids: list[str] | tuple[str, ...] | set[str] | None = None,
    snapshot_loader: Callable[..., dict[str, Any]],
    ownership_verifier: Callable[[str], bool] | None = None,
) -> StudioStateContext:
    """Build compact state from a user-scoped session and supplied proof job.

    ``expandable_job_id`` is deliberately injected by the current runner during
    the compatibility phase, so this layer does not duplicate disk discovery or
    import private runner helpers.
    """

    reply_to = reply_to if isinstance(reply_to, dict) else {}
    reply_job_id = str(reply_to.get("job_id") or "").strip()
    expandable_id = str(expandable_job_id or "").strip()
    repairable_ids = {
        str(job_id or "").strip()
        for job_id in (repairable_job_ids or [])
        if str(job_id or "").strip()
    }
    tracked: dict[str, dict[str, Any]] = {}
    ordered_ids: list[str] = []

    def _remember(job_id: str, row: dict[str, Any] | None = None) -> None:
        jid = str(job_id or "").strip()
        if not jid:
            return
        if jid not in tracked:
            tracked[jid] = dict(row or {})
            ordered_ids.append(jid)
        elif row:
            tracked[jid].update(row)

    if reply_job_id:
        _remember(reply_job_id, reply_to)
    for row in list(session.get("active_jobs") or []):
        if isinstance(row, dict):
            _remember(str(row.get("job_id") or ""), row)
    if expandable_id:
        _remember(expandable_id, {"kind": "shortform"})
    for repairable_id in repairable_ids:
        _remember(repairable_id, {"kind": "shortform"})

    jobs: list[CompactJobState] = []
    for jid in ordered_ids[:8]:
        row = tracked[jid]
        role: JobRole = "reply_target" if jid == reply_job_id else "active"
        kind = _safe_kind(row.get("kind"))
        try:
            snapshot = _call_snapshot_loader(snapshot_loader, jid, kind)
        except Exception as exc:
            snapshot = {
                "job_id": jid,
                "kind": kind,
                "status": "unknown",
                "stage": "snapshot_error",
                "title": str(row.get("title") or ""),
                "error": str(exc)[:200],
            }
        snapshot.setdefault("job_id", jid)
        snapshot.setdefault("kind", kind)
        snapshot.setdefault("title", row.get("title") or "")
        # Active-job membership is only a routing hint, never ownership proof.
        # During compatibility rollout, the runner-supplied expandable id is a
        # trusted capability token. The runner can additionally inject its
        # authoritative job-spec/user verifier here.
        ownership_verified = jid == expandable_id or jid in repairable_ids
        if ownership_verifier is not None:
            try:
                ownership_verified = bool(ownership_verifier(jid))
            except Exception:
                ownership_verified = False
        jobs.append(
            compact_job_snapshot(
                snapshot,
                role=role,
                expandable_proof=jid == expandable_id,
                repairable_scene_review=jid in repairable_ids,
                ownership_verified=ownership_verified,
            )
        )

    try:
        reply_scene = int(reply_to.get("scene_index")) + 1 if reply_to.get("scene_index") is not None else None
    except (TypeError, ValueError):
        reply_scene = None

    expandable_jobs = [job for job in jobs if job.expandable_proof and job.ownership_verified]
    context_existing_count = (
        max(1, int(expandable_jobs[0].scene_count or len(expandable_jobs[0].scenes) or 1))
        if len(expandable_jobs) == 1
        else 1
    )
    recent_additional, recent_total, recent_evidence = _recent_expansion_cardinality(
        session,
        existing_count=context_existing_count,
    )

    revision_payload = {
        "updated_at": session.get("updated_at"),
        "reply_job_id": reply_job_id,
        "jobs": [job.model_dump(mode="json") for job in jobs],
        "pending": session.get("short_expansion_intake") or {},
        "pending_scene_repair": session.get("pending_scene_repair") or {},
        "recent_expansion": {
            "additional": recent_additional,
            "total": recent_total,
            "evidence": recent_evidence,
        },
    }
    revision = hashlib.sha256(
        json.dumps(revision_payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:24]
    actions: list[CommandAction] = ["conversation", "clarify"]
    if any(job.expandable_proof and job.ownership_verified for job in jobs):
        actions.append("expand_existing_short")
    if any(job.repairable_scene_review and job.ownership_verified for job in jobs):
        actions.append("audit_and_repair_scenes")
    agent_mode = str(session.get("agent_mode") or "studio").lower()
    if agent_mode not in {"plan", "studio", "cliplab"}:
        agent_mode = "studio"
    approval_mode = str(session.get("approval_mode") or "confirm").lower()
    if approval_mode not in {"auto", "confirm"}:
        approval_mode = "confirm"
    content_format = str(session.get("content_format") or "both").lower()
    if content_format not in {"short", "long", "both"}:
        content_format = "both"
    return StudioStateContext(
        session_id=str(session.get("session_id") or ""),
        state_revision=revision,
        agent_mode=agent_mode,  # type: ignore[arg-type]
        approval_mode=approval_mode,  # type: ignore[arg-type]
        content_format=content_format,  # type: ignore[arg-type]
        reply_target_job_id=reply_job_id,
        reply_target_scene_number=reply_scene,
        jobs=jobs,
        pending_command=_legacy_pending_command(session),
        recent_expansion_additional_scene_count=recent_additional,
        recent_expansion_total_scene_count=recent_total,
        recent_expansion_evidence=recent_evidence,
        available_actions=actions,
    )
