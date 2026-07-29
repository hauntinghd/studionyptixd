"""Budget contract for Studio Agent production tools.

The goal is not perfect accounting. The goal is a hard preflight barrier:
expensive tools must have an estimated spend and must not start if the estimate
is above the approved cap.
"""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from studio_agent.image_model_catalog import (
    normalize_seedream_model_id,
    seedream_provider,
)


class BudgetExceededError(RuntimeError):
    pass


# Incremental provider attempts are checked and recorded under one process-wide
# lock.  A long-form render fans scenes out across a thread pool, so a plain
# "check remaining, then append cost" sequence lets several workers all spend
# the same last few cents.  Keeping both operations in one critical section
# makes the persisted cost ledger the reservation mechanism as well as the
# audit trail.
_INCREMENTAL_SPEND_LOCK = threading.RLock()


@dataclass(frozen=True)
class BudgetEstimate:
    tool: str
    estimated_usd: float
    max_budget_usd: float
    mode: str
    breakdown: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "tool": self.tool,
            "estimated_usd": round(float(self.estimated_usd), 4),
            "max_budget_usd": round(float(self.max_budget_usd), 4),
            "mode": self.mode,
            "breakdown": self.breakdown,
        }


EXPENSIVE_TOOLS = frozenset({
    "analyze_reference_video",
    "analyze_competitor_video",
    "retry_reference_analysis",
    "generate_longform_outline",
    "expand_longform_chapter",
    "start_shortform_generate",
    "expand_visual_proof_shortform",
    "start_longform_render",
    "expand_longform_visual_proof",
    "finalize_longform_render",
    "generate_longform_thumbnails",
    "regenerate_longform_thumbnail",
    "edit_production_scene_still",
    "edit_production_scenes_still",
    "regenerate_production_scene_still",
    "regenerate_production_scene",
    "regenerate_production_scenes",
    "animate_production_scenes",
    "repair_production_scene_animation",
    "audit_and_repair_production_scenes",
    "finalize_production",
    "re_edit_production",
    "regenerate_longform_still",
    "analyze_cliplab_video",
})


DEFAULT_CAPS_USD = {
    "analyze_reference_video": 1.0,
    "analyze_competitor_video": 1.0,
    "retry_reference_analysis": 1.0,
    "generate_longform_outline": 1.0,
    "expand_longform_chapter": 1.0,
    "start_shortform_generate": 5.0,
    # A normal five-scene expansion now quotes the bounded two-candidate still
    # envelope plus current FAL video pricing. Keep the ceiling finite, but high
    # enough for that reviewed 30-second path; larger scopes still require an
    # explicit higher max_budget_usd or a cheaper animation route.
    "expand_visual_proof_shortform": 12.0,
    "start_longform_render": 8.0,
    "expand_longform_visual_proof": 12.0,
    "finalize_longform_render": 35.0,
    "generate_longform_thumbnails": 1.0,
    "regenerate_longform_thumbnail": 0.25,
    "edit_production_scene_still": 0.25,
    "edit_production_scenes_still": 1.0,
    "regenerate_production_scene_still": 0.50,
    "regenerate_production_scene": 5.0,
    # A normal Short can contain twelve scenes. Quote every bounded still and
    # i2v retry, then enforce one finite ceiling for the entire selected batch.
    "regenerate_production_scenes": 60.0,
    # Multi-scene i2v batches routinely exceed $3 when Seedance is priced in;
    # LTX is cheap, but the estimate must not hard-fail a normal 30s short.
    "animate_production_scenes": 12.0,
    "repair_production_scene_animation": 2.0,
    "audit_and_repair_production_scenes": 12.0,
    "finalize_production": 1.0,
    "re_edit_production": 1.5,
    "regenerate_longform_still": 0.25,
    "analyze_cliplab_video": 1.0,
}


FALLBACK_USD = {
    "seedream_v45_per_image": 0.04,
    "seedream_v45_edit_per_image": 0.04,
    "seedream_v4_per_image": 0.03,
    "seedream_v4_edit_per_image": 0.03,
    "seedream_v5_lite_per_image": 0.035,
    "seedream_v5_lite_edit_per_image": 0.035,
    "fal_minimax_per_1k_chars": 0.10,
    "kling_v21_standard_per_second": 0.056,
    "kling_v21_pro_per_second": 0.098,
    "pixverse_v6_per_second": 0.045,
    "seedance_20_i2v_per_second": 0.3024,
    "ltx_098_distilled_per_second": 0.02,
    "mmaudio_v2_per_second": 0.001,
    "shortform_compose_allowance_usd": 0.05,
}


APPROVAL_REQUIRED_TOOLS = frozenset({
    "start_shortform_generate",
    "expand_visual_proof_shortform",
    "start_longform_render",
    "expand_longform_visual_proof",
    "generate_longform_thumbnails",
    "edit_production_scene_still",
    "edit_production_scenes_still",
    "regenerate_production_scene_still",
    "regenerate_production_scene",
    "regenerate_production_scenes",
    "set_production_scenes_animate",
    "set_production_scene_prompt",
    "set_production_scene_duration",
    "animate_production_scenes",
    "repair_production_scene_animation",
    "audit_and_repair_production_scenes",
    "finalize_production",
    "re_edit_production",
    "cancel_production_job",
    "finalize_longform_render",
    "regenerate_longform_still",
    "regenerate_longform_thumbnail",
    "cancel_longform_render",
    "analyze_cliplab_video",
})


TOOL_LANES = {
    "generate_longform_outline": "analysis",
    "expand_longform_chapter": "analysis",
    "start_shortform_generate": "render",
    "expand_visual_proof_shortform": "render",
    "start_longform_render": "render",
    "finalize_longform_render": "render",
    "generate_longform_thumbnails": "render",
    "regenerate_longform_thumbnail": "render",
    "edit_production_scene_still": "render",
    "edit_production_scenes_still": "render",
    "regenerate_production_scene_still": "render",
    "regenerate_production_scene": "render",
    "regenerate_production_scenes": "render",
    "regenerate_longform_still": "render",
    "animate_production_scenes": "render",
    "repair_production_scene_animation": "render",
    "audit_and_repair_production_scenes": "render",
    "finalize_production": "render",
    "re_edit_production": "render",
    "analyze_reference_video": "analysis",
    "analyze_competitor_video": "analysis",
    "retry_reference_analysis": "analysis",
    "build_scene_blueprint_from_reference": "analysis",
    "get_channel_analytics": "analysis",
    "analyze_cliplab_video": "analysis",
}


STAGE_GATES = {
    "analyze_reference_video": [
        "pricing_preflight",
        "credit_hold",
        "vision_stt_story_analysis",
        "await_analysis_receipt",
    ],
    "analyze_competitor_video": [
        "pricing_preflight",
        "credit_hold",
        "vision_stt_story_analysis",
        "await_analysis_receipt",
    ],
    "retry_reference_analysis": [
        "owned_analysis_required",
        "pricing_preflight",
        "retry_selected_stages",
        "await_analysis_receipt",
    ],
    "generate_longform_outline": ["pricing_preflight", "credit_hold", "outline", "settle_actual_tokens"],
    "expand_longform_chapter": ["pricing_preflight", "credit_hold", "expand_chapter", "settle_actual_tokens"],
    "start_shortform_generate": ["cost_preflight", "create_stills", "await_scene_review"],
    "expand_visual_proof_shortform": [
        "proof_approved",
        "cost_preflight",
        "create_remaining_stills",
        "animate_selected_new_scenes",
        "await_scene_review",
    ],
    "edit_production_scene_still": ["cost_preflight", "edit_still", "await_scene_review"],
    "edit_production_scenes_still": ["cost_preflight", "batch_edit_stills", "await_scene_review"],
    "regenerate_production_scene_still": ["cost_preflight", "regenerate_still", "await_scene_review"],
    "regenerate_production_scene": ["cost_preflight", "regenerate_still", "image_to_video", "await_animation_result"],
    "regenerate_production_scenes": ["cost_preflight", "regenerate_stills", "image_to_video", "await_animation_result"],
    "animate_production_scenes": ["scene_approval_required", "image_to_video", "await_animation_result"],
    "repair_production_scene_animation": ["approved_still_preserved", "image_to_video", "sampled_frame_qa", "await_animation_result"],
    "audit_and_repair_production_scenes": [
        "confirmed_scene_scope",
        "cost_preflight",
        "transactional_still_repair",
        "semantic_qa",
        "image_to_video",
        "atomic_commit",
    ],
    "finalize_production": ["compose", "package", "publish_ready_artifact"],
    "start_longform_render": ["cost_preflight", "outline", "create_stills", "await_chapter_review"],
    "expand_longform_visual_proof": ["proof_approved", "cost_preflight", "gallery_stills", "await_chapter_review"],
    "finalize_longform_render": ["chapter_approval_required", "compose", "package"],
    "generate_longform_thumbnails": ["cost_preflight", "thumbnail_variants", "await_packaging_review"],
    "regenerate_longform_thumbnail": ["cost_preflight", "thumbnail_candidate", "await_packaging_review"],
    "analyze_cliplab_video": ["pricing_preflight", "credit_hold", "analyze", "rank_segments"],
}


QUEUE_PRIORITIES = {
    "chat": 100,
    "analysis": 70,
    "render": 40,
}


def is_budgeted_tool(tool_name: str) -> bool:
    return str(tool_name or "") in EXPENSIVE_TOOLS


def enforce_budget(tool_name: str, args: dict[str, Any]) -> BudgetEstimate | None:
    """Raise before execution when a tool would exceed its approved budget."""
    if not is_budgeted_tool(tool_name):
        return None
    estimate = estimate_tool_cost(tool_name, args)
    if estimate.estimated_usd > estimate.max_budget_usd:
        raise BudgetExceededError(
            json.dumps(
                {
                    "error": "budget_exceeded",
                    "message": (
                        f"{tool_name} estimated ${estimate.estimated_usd:.2f}, "
                        f"above max budget ${estimate.max_budget_usd:.2f}. "
                        "Approve a higher max_budget_usd or lower quality/scope before running."
                    ),
                    "budget": estimate.as_dict(),
                },
                indent=2,
            )
        )
    return estimate


def persist_execution_budget_context(
    workspace: Path,
    *,
    reservation: dict[str, Any] | None,
    budget: dict[str, Any] | None,
    user_id: str = "",
    tool: str = "",
    session_id: str = "",
) -> dict[str, Any]:
    """Atomically persist the approved cap before asynchronous provider work.

    The wallet reservation and the execution cap are related but distinct: an
    unlimited/admin reservation still needs a hard approved dollar ceiling.
    Therefore ``budget.max_budget_usd`` is required even when ``reservation``
    is empty or unlimited.
    """

    workspace = Path(workspace)
    budget_payload = dict(budget or {})
    cap = _float(budget_payload.get("max_budget_usd"), None)
    if cap is None or float(cap) < 0:
        raise BudgetExceededError(
            "A durable max_budget_usd is required before provider work can start."
        )
    from studio_agent import production_costs

    with _INCREMENTAL_SPEND_LOCK:
        workspace.mkdir(parents=True, exist_ok=True)
        baseline = max(
            0.0,
            float(
                production_costs.load_summary(workspace).get(
                    "total_usd_decimal", 0.0
                )
                or 0.0
            ),
        )
        payload = {
            "version": 1,
            "reservation": dict(reservation or {}),
            "user_id": str(user_id or ""),
            "tool": str(tool or budget_payload.get("tool") or ""),
            "session_id": str(session_id or ""),
            "budget": budget_payload,
            "cost_baseline_usd": baseline,
            "created_at": time.time(),
        }
        target = workspace / "credit_reservation.json"
        temp = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
        try:
            temp.write_text(
                json.dumps(payload, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            os.replace(temp, target)
        finally:
            temp.unlink(missing_ok=True)
        return payload


def execution_budget_context(workspace: Path) -> dict[str, Any]:
    """Load the approved cap and cost baseline persisted for the active tool."""

    path = Path(workspace) / "credit_reservation.json"
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    budget = payload.get("budget") if isinstance(payload.get("budget"), dict) else {}
    cap = _float(budget.get("max_budget_usd"), None)
    if cap is None:
        return {}
    return {
        "tool": str(payload.get("tool") or budget.get("tool") or ""),
        "max_budget_usd": max(0.0, float(cap)),
        "baseline_usd": max(0.0, float(_float(payload.get("cost_baseline_usd"), 0.0) or 0.0)),
    }


def _incremental_attempt_receipt(
    workspace: Path,
    attempt_id: str,
) -> dict[str, Any] | None:
    """Find a prior cost-ledger row for an idempotent provider attempt."""

    if not attempt_id:
        return None
    path = Path(workspace) / "cost_ledger.jsonl"
    if not path.is_file():
        return None
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return None
    for line in reversed(lines):
        try:
            row = json.loads(line)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        if str(metadata.get("budget_attempt_id") or "") == attempt_id:
            return row
    return None


def record_incremental_spend_attempt(
    workspace: Path,
    next_estimated_usd: Any,
    *,
    attempt_id: str,
    operation: str,
    provider: str = "",
    model: str = "",
    require_context: bool = False,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Reserve and record one provider submission exactly once.

    Call this immediately before the network submission.  The durable ledger
    entry is written before control returns, so failed provider requests still
    consume their approved attempt and concurrent workers cannot oversubscribe
    the same remaining budget.  Reusing ``attempt_id`` replays the existing
    receipt without charging twice.
    """

    workspace = Path(workspace)
    normalized_attempt_id = str(attempt_id or "").strip()
    if not normalized_attempt_id:
        raise ValueError("incremental provider attempts require a stable attempt_id")
    try:
        estimate = max(0.0, float(Decimal(str(next_estimated_usd or 0))))
    except Exception:
        estimate = 0.0

    from studio_agent import production_costs

    with _INCREMENTAL_SPEND_LOCK:
        replay = _incremental_attempt_receipt(workspace, normalized_attempt_id)
        if replay is not None:
            replay_operation = str(replay.get("operation") or "")
            replay_endpoint = str(replay.get("endpoint") or "")
            if replay_operation != str(operation or "") or replay_endpoint != str(model or ""):
                raise RuntimeError(
                    "incremental provider attempt_id collision with different operation/model"
                )
            return {
                "enforced": bool(execution_budget_context(workspace)),
                "recorded": False,
                "idempotent_replay": True,
                "attempt_id": normalized_attempt_id,
                "event": replay,
            }

        state = enforce_incremental_spend(
            workspace,
            estimate,
            operation=operation,
            provider=provider,
            model=model,
        )
        if require_context and not state.get("enforced"):
            raise BudgetExceededError(
                json.dumps(
                    {
                        "error": "execution_budget_context_missing",
                        "message": (
                            "No durable approved execution budget exists for this "
                            "provider attempt. The provider was not called."
                        ),
                        "operation": str(operation or ""),
                        "provider": str(provider or ""),
                        "model": str(model or ""),
                    },
                    indent=2,
                )
            )
        event_metadata = dict(metadata or {})
        event_metadata.update(
            {
                "budget_guarded": bool(state.get("enforced")),
                "budget_attempt_id": normalized_attempt_id,
                "submission_state": "about_to_submit",
            }
        )
        event = production_costs.record_event(
            workspace,
            stage="longform",
            provider=str(provider or "fal"),
            operation=str(operation or "longform_provider_attempt"),
            usd=estimate,
            quantity=1,
            unit="provider_attempt",
            endpoint=str(model or ""),
            metadata=event_metadata,
        )
        return {
            **state,
            "recorded": True,
            "idempotent_replay": False,
            "attempt_id": normalized_attempt_id,
            "event": event,
        }


def remaining_execution_budget(
    workspace: Path,
    *,
    in_flight_usd: float = 0.0,
) -> dict[str, Any]:
    context = execution_budget_context(Path(workspace))
    if not context:
        return {"enforced": False}
    from studio_agent import production_costs

    summary = production_costs.load_summary(Path(workspace))
    actual = max(
        0.0,
        float(summary.get("total_usd_decimal", summary.get("total_usd", 0.0)) or 0.0),
    )
    baseline = float(context.get("baseline_usd") or 0.0)
    spent = max(0.0, actual - baseline)
    pending = max(0.0, float(in_flight_usd or 0.0))
    cap = max(0.0, float(context.get("max_budget_usd") or 0.0))
    remaining = max(0.0, cap - spent - pending)
    return {
        **context,
        "enforced": True,
        "actual_job_usd": round(actual, 6),
        "spent_this_tool_usd": round(spent, 6),
        "in_flight_usd": round(pending, 6),
        "remaining_usd": round(remaining, 6),
    }


def job_lifetime_cap_usd() -> float | None:
    """Whole-job spend ceiling from STUDIO_MAX_JOB_USD, or None when unset."""

    raw = str(os.getenv("STUDIO_MAX_JOB_USD", "") or "").strip()
    if not raw:
        return None
    try:
        cap = float(raw)
    except (TypeError, ValueError):
        return None
    return cap if cap > 0 else None


def enforce_incremental_spend(
    workspace: Path,
    next_estimated_usd: Any,
    *,
    operation: str,
    provider: str = "",
    model: str = "",
    in_flight_usd: float = 0.0,
) -> dict[str, Any]:
    """Fail before each paid dispatch when an approved cap has no room."""

    try:
        estimate = max(0.0, float(Decimal(str(next_estimated_usd or 0))))
    except Exception:
        estimate = 0.0

    # A whole-job ceiling, independent of the per-tool approved cap. Every tool
    # call persists its own max_budget_usd measured from its own cost baseline,
    # so N calls can each stay inside their cap and still spend N x cap over a
    # single job's life - which is how a repair loop turns a ~$4 short into
    # $18+. This is the one chokepoint every paid dispatch passes through, so the
    # lifetime ceiling is enforced here, and deliberately BEFORE the per-tool
    # early return: it has to hold even for tools that never approved a cap.
    lifetime_cap = job_lifetime_cap_usd()
    lifetime: dict[str, Any] = {}
    if lifetime_cap is not None:
        from studio_agent import production_costs

        summary = production_costs.load_summary(Path(workspace))
        actual = max(
            0.0,
            float(summary.get("total_usd_decimal", summary.get("total_usd", 0.0)) or 0.0),
        )
        pending = max(0.0, float(in_flight_usd or 0.0))
        lifetime = {
            "job_lifetime_cap_usd": round(lifetime_cap, 6),
            "job_lifetime_spent_usd": round(actual, 6),
            "job_lifetime_remaining_usd": round(max(0.0, lifetime_cap - actual - pending), 6),
        }
        if actual + pending + estimate > lifetime_cap + 1e-9:
            raise BudgetExceededError(
                json.dumps(
                    {
                        "error": "budget_exceeded_job_lifetime",
                        "message": (
                            f"{operation} needs about ${estimate:.4f}, but this job has "
                            f"already spent ${actual:.4f} of its ${lifetime_cap:.2f} "
                            "whole-job ceiling (STUDIO_MAX_JOB_USD)."
                        ),
                        "provider": str(provider or ""),
                        "model": str(model or ""),
                        "next_estimated_usd": round(estimate, 6),
                        "budget": lifetime,
                    },
                    indent=2,
                )
            )

    state = remaining_execution_budget(
        Path(workspace),
        in_flight_usd=in_flight_usd,
    )
    if not state.get("enforced"):
        if lifetime:
            return {**state, **lifetime, "next_estimated_usd": round(estimate, 6)}
        return state
    if estimate > float(state.get("remaining_usd") or 0.0) + 1e-9:
        raise BudgetExceededError(
            json.dumps(
                {
                    "error": "budget_exceeded_mid_job",
                    "message": (
                        f"{operation} needs about ${estimate:.4f}, but only "
                        f"${float(state.get('remaining_usd') or 0.0):.4f} remains "
                        "under the approved execution cap."
                    ),
                    "provider": str(provider or ""),
                    "model": str(model or ""),
                    "next_estimated_usd": round(estimate, 6),
                    "budget": state,
                },
                indent=2,
            )
        )
    return {**state, **lifetime, "next_estimated_usd": round(estimate, 6)}


def _policy_normalize_budget_args(args: dict[str, Any]) -> dict[str, Any]:
    """Quote only provider routes Studio policy can actually execute."""
    normalized = dict(args or {})
    for key in ("image_model_id", "image_model", "stills_model"):
        value = str(normalized.get(key) or "").strip().lower()
        if value:
            normalized[key] = _policy_image_model(value)
    for key in ("video_model", "fallback_video_model"):
        value = str(normalized.get(key) or "").strip().lower()
        if value:
            normalized[key] = _policy_video_model(value)
    voice = str(normalized.get("voice_provider") or "").strip().lower()
    if voice and voice not in {"fal", "fal_only", "minimax", "minimax_only"}:
        normalized["voice_provider"] = "fal"
    return normalized


def _policy_image_model(value: Any) -> str:
    """Migrate persisted non-FAL image choices before quoting them."""
    model = str(value or "").strip().lower()
    if not model:
        return "seedream_edit"
    if any(marker in model for marker in ("grok", "xai", "imagen", "google", "modal")):
        return "seedream_edit"
    return model


def _policy_video_model(value: Any) -> str:
    """Migrate persisted non-FAL video choices before quoting them."""
    model = str(value or "").strip().lower()
    if not model:
        return "kling_pro"
    if any(marker in model for marker in ("grok", "xai", "google", "veo")):
        return "kling_pro"
    return model


def estimate_tool_cost(tool_name: str, args: dict[str, Any] | None = None) -> BudgetEstimate:
    args = _policy_normalize_budget_args(dict(args or {}))
    name = str(tool_name or "")
    explicit_cap = _float(args.get("max_budget_usd"), None)
    max_budget = explicit_cap if explicit_cap is not None else _default_cap(name)
    mode = "explicit" if explicit_cap is not None else "default"
    if name == "start_shortform_generate":
        est, breakdown = _estimate_shortform_start(args)
    elif name == "generate_longform_outline":
        est, breakdown = _estimate_longform_text_call(args, max_output_tokens=2000)
    elif name == "expand_longform_chapter":
        est, breakdown = _estimate_longform_text_call(args, max_output_tokens=4000)
    elif name == "expand_visual_proof_shortform":
        est, breakdown = _estimate_shortform_expand(args)
    elif name == "start_longform_render":
        est, breakdown = _estimate_longform_start(args)
    elif name == "expand_longform_visual_proof":
        est, breakdown = _estimate_longform_expand(args)
    elif name == "finalize_longform_render":
        est, breakdown = _estimate_longform_finalize(args)
    elif name == "generate_longform_thumbnails":
        count = max(1, min(3, int(args.get("count") or 3)))
        image_model = str(args.get("image_model_id") or args.get("image_model") or "seedream_edit")
        est, note, pricing_key = _seedream_image_estimate(image_model, edit=False, quantity=count)
        breakdown = {
            "thumbnails": count,
            "image_model_pricing_unit": pricing_key,
            "image_usd_per_image": _unit_rate(est, count),
            "pricing_note": note,
        }
    elif name == "regenerate_longform_thumbnail":
        image_model = str(
            args.get("image_model_id")
            or args.get("image_model")
            or "seedream_edit"
        )
        est, note, pricing_key = _seedream_image_estimate(
            image_model,
            edit=False,
            quantity=1,
        )
        breakdown = {
            "thumbnails": 1,
            "image_model_pricing_unit": pricing_key,
            "image_usd_per_image": est,
            "pricing_note": note,
        }
    elif name == "edit_production_scenes_still":
        raw_indices = args.get("scene_indices") or []
        try:
            count = len(raw_indices) if isinstance(raw_indices, list) and raw_indices else int(args.get("scene_count") or 12)
        except Exception:
            count = 12
        count = max(1, min(60, count))
        image_model = str(args.get("image_model_id") or args.get("image_model") or "seedream_edit")
        est, note, pricing_key = _seedream_image_estimate(image_model, edit=True, quantity=count)
        breakdown = {
            "image_model_pricing_unit": pricing_key,
            "image_edit_count": count,
            "image_edit_usd_per_image": _unit_rate(est, count),
            "scope": str(args.get("scope") or "character"),
            "pricing_note": note,
        }
    elif name in {"edit_production_scene_still", "regenerate_longform_still"}:
        image_model = str(args.get("image_model_id") or args.get("image_model") or "seedream_edit")
        est, note, pricing_key = _seedream_image_estimate(image_model, edit=True, quantity=1)
        breakdown = {"image_model_pricing_unit": pricing_key, "image_edit_count": 1, "image_edit_usd_per_image": est, "pricing_note": note}
    elif name == "regenerate_production_scene_still":
        image_model = str(args.get("image_model_id") or args.get("image_model") or "seedream_edit")
        est, envelope = _image_retry_envelope(
            image_model,
            edit=True,
            quantity=1,
            attempts=3,
        )
        breakdown = {
            "image_model_pricing_unit": envelope["pricing_unit"],
            "image_edit_count": 1,
            "image_retry_envelope": envelope,
            "image_edit_usd": round(est, 4),
            "pricing_note": envelope["pricing_note"],
        }
    elif name in {"regenerate_production_scene", "regenerate_production_scenes"}:
        count = 1 if name == "regenerate_production_scene" else _scene_index_count(args, default=12)
        image_model = str(args.get("image_model_id") or args.get("image_model") or "seedream_edit")
        still_est, image_envelope = _image_retry_envelope(
            image_model,
            edit=True,
            quantity=count,
            attempts=3,
        )
        seconds = _video_seconds(args, count=count, default_per_scene=5.0)
        video_model = _resolve_video_model_for_budget(args)
        video_est, video_envelope = _video_retry_envelope(
            video_model,
            seconds,
            attempts=2,
        )
        est = still_est + video_est
        breakdown = {
            "scene_count": count,
            "image_model_pricing_unit": image_envelope["pricing_unit"],
            "image_edit_count": count,
            "image_edit_usd": round(still_est, 4),
            "image_retry_envelope": image_envelope,
            "video_seconds": seconds,
            "video_model": video_model,
            "video_usd_per_second": video_envelope["primary_usd_per_second"],
            "video_usd": round(video_est, 4),
            "video_retry_envelope": video_envelope,
            "pricing_note": [
                image_envelope["pricing_note"],
                video_envelope["pricing_note"],
            ],
        }
    elif name == "audit_and_repair_production_scenes":
        count = _scene_index_count(args, default=1)
        image_model = str(args.get("image_model_id") or args.get("image_model") or "seedream_edit")
        still_est, image_envelope = _image_retry_envelope(
            image_model,
            edit=True,
            quantity=count,
            attempts=3,
        )
        seconds = _video_seconds(args, count=count, default_per_scene=5.0)
        video_model = _resolve_video_model_for_budget(args)
        video_est, video_envelope = _video_retry_envelope(
            video_model,
            seconds,
            attempts=2,
        )
        est = still_est + video_est
        breakdown = {
            "scene_count": count,
            "image_model_pricing_unit": image_envelope["pricing_unit"],
            "image_edit_count": count,
            "image_edit_usd": round(still_est, 4),
            "image_retry_envelope": image_envelope,
            "video_seconds": seconds,
            "video_model": video_model,
            "video_usd_per_second": video_envelope["primary_usd_per_second"],
            "video_usd": round(video_est, 4),
            "video_retry_envelope": video_envelope,
            "pricing_note": [
                image_envelope["pricing_note"],
                video_envelope["pricing_note"],
            ],
        }
    elif name in {"animate_production_scenes", "repair_production_scene_animation"}:
        count = _scene_index_count(args, default=3)
        seconds = _video_seconds(args, count=count, default_per_scene=5.0)
        video_model = _resolve_video_model_for_budget(args)
        est, video_envelope = _video_retry_envelope(
            video_model,
            seconds,
            attempts=2,
        )
        breakdown = {
            "scene_count": count,
            "video_seconds": seconds,
            "video_model": video_model,
            "video_usd_per_second": video_envelope["primary_usd_per_second"],
            "video_usd": round(est, 4),
            "video_retry_envelope": video_envelope,
            "pricing_note": video_envelope["pricing_note"],
        }
    elif name in {"finalize_production", "re_edit_production"}:
        est, breakdown = _estimate_shortform_finalize(args)
    elif name in {
        "analyze_reference_video",
        "analyze_competitor_video",
        "retry_reference_analysis",
    }:
        est = 0.75
        breakdown = {
            "analysis_calls": 1,
            "pricing_mode": "conservative_multimodal_analysis_envelope",
        }
    elif name == "analyze_cliplab_video":
        # ClipLab analysis starts an asynchronous Studio model call. Reserve a
        # finite conservative envelope before that worker is allowed to start;
        # the public ClipLab router may separately charge duration/render work.
        est = 0.25
        breakdown = {
            "analysis_calls": 1,
            "max_segments": max(1, min(int(args.get("max_segments") or 12), 40)),
            "pricing_mode": "conservative_async_envelope",
        }
    else:
        est = 0.0
        breakdown = {}
    return BudgetEstimate(name, round(max(0.0, est), 4), max_budget, mode, breakdown)


def with_budget_metadata(result: str, estimate: BudgetEstimate | None, args: dict[str, Any] | None = None) -> str:
    if not estimate:
        return result
    try:
        data = json.loads(result or "{}")
    except Exception:
        return result
    if not isinstance(data, dict):
        return result
    data["budget"] = estimate.as_dict()
    data["production_control"] = production_control_metadata(estimate.tool, args, estimate)
    return json.dumps(data, indent=2, ensure_ascii=False)


def production_control_metadata(
    tool_name: str,
    args: dict[str, Any] | None = None,
    estimate: BudgetEstimate | None = None,
) -> dict[str, Any]:
    args = dict(args or {})
    name = str(tool_name or "")
    lane = tool_lane(name)
    return {
        "tool": name,
        "lane": lane,
        "queue_priority": QUEUE_PRIORITIES.get(lane, 50),
        "requires_approval": name in APPROVAL_REQUIRED_TOOLS,
        "stage_gates": list(STAGE_GATES.get(name, ())),
        "durable_state": durable_state_contract(name, args),
        "resume_safe": name in EXPENSIVE_TOOLS,
        "budget_mode": estimate.mode if estimate else "none",
    }


def tool_lane(tool_name: str) -> str:
    return TOOL_LANES.get(str(tool_name or ""), "chat")


def durable_state_contract(tool_name: str, args: dict[str, Any] | None = None) -> dict[str, Any]:
    args = dict(args or {})
    name = str(tool_name or "")
    job_id = str(args.get("job_id") or "").strip()
    if name.startswith("start_shortform"):
        return {"kind": "shortform", "key": "job_id", "job_id": job_id or None, "must_persist": True}
    if name in {
        "expand_visual_proof_shortform",
        "edit_production_scene_still",
        "edit_production_scenes_still",
        "regenerate_production_scene_still",
        "regenerate_production_scene",
        "regenerate_production_scenes",
        "animate_production_scenes",
        "repair_production_scene_animation",
        "audit_and_repair_production_scenes",
        "finalize_production",
        "re_edit_production",
    }:
        return {"kind": "shortform", "key": "job_id", "job_id": job_id or None, "must_persist": True}
    if "longform" in name:
        return {"kind": "longform", "key": "job_id", "job_id": job_id or None, "must_persist": True}
    return {"kind": "generic", "must_persist": False}


def _estimate_longform_text_call(
    args: dict[str, Any],
    *,
    max_output_tokens: int,
) -> tuple[float, dict[str, Any]]:
    """Reserve a conservative upper bound before a paid planning call.

    Provider pricing is resolved inside the logged tool after its durable
    idempotency claim and before any inference or credit reservation. A missing
    rate is a hard stop: Studio must never call an unpriced model and hope it
    can charge the creator afterward.
    """

    prompt_ppm = _float(args.get("_billing_prompt_price_per_m"), None)
    completion_ppm = _float(args.get("_billing_completion_price_per_m"), None)
    if prompt_ppm is None or completion_ppm is None or prompt_ppm < 0 or completion_ppm < 0:
        raise BudgetExceededError(
            json.dumps(
                {
                    "error": "model_pricing_unavailable",
                    "message": (
                        "Studio could not verify pricing for the selected text model. "
                        "No provider request was sent; choose a priced model or retry after pricing refreshes."
                    ),
                    "model": str(args.get("model") or ""),
                },
                indent=2,
            )
        )

    # One Unicode character can be one or more tokens depending on language.
    # Counting every supplied character as a token plus a prompt-template
    # cushion deliberately over-reserves; settlement refunds to actual usage.
    input_chars = max(0, int(args.get("_billing_input_chars") or 0))
    input_token_cap = max(2048, input_chars + 2048)
    output_token_cap = max(512, int(max_output_tokens or 0))
    estimate = (
        float(prompt_ppm) * (input_token_cap / 1_000_000.0)
        + float(completion_ppm) * (output_token_cap / 1_000_000.0)
    )
    return round(max(0.0001, estimate), 6), {
        "stage": "longform_text",
        "model": str(args.get("model") or ""),
        "input_token_cap": input_token_cap,
        "output_token_cap": output_token_cap,
        "prompt_price_per_m": float(prompt_ppm),
        "completion_price_per_m": float(completion_ppm),
        "settlement": "provider_reported_tokens_with_estimate_fallback",
    }


def _seedream_image_estimate(
    model_id: str,
    *,
    edit: bool,
    quantity: int,
) -> tuple[float, str, str]:
    normalized = normalize_seedream_model_id(_policy_image_model(model_id)) or "seedream_edit"
    if seedream_provider(normalized) == "modal":
        normalized = "seedream_edit"
    stem = {
        "seedream_v4": "seedream_v4",
        "seedream_v5_lite": "seedream_v5_lite",
    }.get(normalized, "seedream_v45")
    key = f"{stem}_edit" if edit else stem
    fallback_key = f"{stem}_edit_per_image" if edit else f"{stem}_per_image"
    amount, note = _priced_unit(key, fallback_key=fallback_key, quantity=quantity)
    return amount, note, key


def _image_retry_envelope(
    model_id: str,
    *,
    edit: bool,
    quantity: int,
    attempts: int,
) -> tuple[float, dict[str, Any]]:
    model = _policy_image_model(model_id)
    qty = max(1, int(quantity or 1))
    tries = max(1, int(attempts or 1))
    per_attempt, note, pricing_key = _seedream_image_estimate(
        model, edit=edit, quantity=1
    )
    total = round(per_attempt * qty * tries, 4)
    return total, {
        "requested_model": model,
        "pricing_unit": pricing_key,
        "quantity": qty,
        "max_attempts": tries,
        "usd_per_attempt": round(per_attempt, 4),
        "fallback_included": False,
        "pricing_note": note,
    }


def _video_retry_envelope(
    video_model: str,
    seconds: float,
    *,
    attempts: int,
) -> tuple[float, dict[str, Any]]:
    model = _policy_video_model(video_model or "ltx_budget")
    tries = max(1, int(attempts or 1))
    primary, primary_rate, primary_note = _video_cost(model, seconds)
    fallback = 0.0
    fallback_notes: list[str] = []
    if "seedance" in model:
        pixverse, _rate, note = _video_cost("pixverse", seconds)
        fallback = pixverse
        fallback_notes.append(note)
    per_attempt = primary + fallback
    total = round(per_attempt * tries, 4)
    return total, {
        "video_model": model,
        "video_seconds": seconds,
        "max_attempts": tries,
        "primary_usd": round(primary, 4),
        "primary_usd_per_second": primary_rate,
        "fallback_usd": round(fallback, 4),
        "usd_per_attempt": round(per_attempt, 4),
        "pricing_note": [primary_note, *fallback_notes],
    }


def _estimate_shortform_start(args: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    # Every new Short is a one-still proof at this boundary. Remaining scenes
    # have a separate, explicit expansion decision after Scene 1 animation.
    scenes = 1
    full_auto = False
    animate = False
    image_model = _policy_image_model(args.get("image_model_id") or args.get("image_model"))
    if image_model in {"seedream_edit", "seedream_v45_edit", "seedream_v4", "seedream_v5_lite"}:
        stills, still_note, still_model_note = _seedream_image_estimate(image_model, edit=True, quantity=scenes)
        still_rate = _unit_rate(stills, scenes)
    else:
        stills, still_note = _priced_unit("seedream_v45_edit", fallback_key="seedream_v45_edit_per_image", quantity=scenes)
        still_rate = _unit_rate(stills, scenes)
        still_model_note = "seedream_v45_edit"
    stills, image_envelope = _image_retry_envelope(
        image_model or "seedream_edit",
        edit=True,
        quantity=scenes,
        attempts=2,
    )
    still_rate = _unit_rate(stills, scenes)
    still_note = image_envelope["pricing_note"]
    requested_seconds = _video_seconds(args, count=scenes, default_per_scene=5.0)
    seconds = requested_seconds if animate else 0.0
    video_model = _policy_video_model(args.get("video_model") or "kling_pro")
    video, video_rate, video_note = _video_cost(video_model, seconds)
    script_chars = max(1000, int(args.get("script_char_count") or len(str(args.get("script") or "")) or scenes * 140))
    if full_auto:
        tts, tts_note, tts_provider, tts_unit, tts_unit_rate = _estimate_tts(script_chars)
    else:
        tts, tts_note, tts_provider, tts_unit, tts_unit_rate = 0.0, "deferred_until_finalize", _tts_provider(), "char", 0.0
    cushion = _cushion_pct()
    total = (stills + video + tts) * (1.0 + cushion)
    return total, {
        "scene_count": scenes,
        "visual_proof_only": True,
        "review_gate": not full_auto,
        "i2v_deferred_until_scene_approval": not full_auto,
        "image_model": image_model or still_model_note,
        "image_model_pricing_unit": still_model_note,
        "still_usd_per_image": still_rate,
        "seedream_edit_per_image": _unit_rate(stills, scenes),
        "stills_usd": round(stills, 4),
        "stills_pricing_note": still_note,
        "image_retry_envelope": image_envelope,
        "video_seconds": seconds,
        "requested_video_seconds": requested_seconds,
        "video_model": video_model,
        "video_usd_per_second": video_rate,
        "video_usd": round(video, 4),
        "video_pricing_note": video_note,
        "tts_chars": script_chars,
        "tts_provider": tts_provider,
        "tts_unit": tts_unit,
        "tts_unit_rate": tts_unit_rate,
        "tts_allowance_usd": round(tts, 4),
        "tts_pricing_note": tts_note,
        "cushion_pct": cushion,
    }


def _estimate_shortform_expand(args: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    """Estimate only the incremental work added to an approved proof short.

    ``scene_count`` is the target total. ``existing_scene_count`` and
    ``animate_scene_indices`` make the semantic command contract explicit, but
    legacy callers that only send ``animate_policy`` remain supported.
    """
    job_id = str(args.get("job_id") or "").strip()
    target_scene_count = max(2, min(60, int(args.get("scene_count") or 12)))
    existing_scene_count = max(1, min(target_scene_count, int(args.get("existing_scene_count") or 1)))
    image_model = _policy_image_model(args.get("image_model_id") or args.get("image_model"))
    video_model = _policy_video_model(args.get("video_model") or args.get("model"))
    spec_seconds_per_scene: float | None = None

    try:
        if job_id:
            from studio_agent import jobs

            ws = (jobs.ROOT / jobs.SKELETON_OUTPUT / job_id).resolve()
            scenes_path = ws / "scenes.json"
            spec_path = ws / "job_spec.json"
            if scenes_path.is_file():
                scenes = json.loads(scenes_path.read_text(encoding="utf-8"))
                if isinstance(scenes, list) and scenes:
                    actual_existing = len([scene for scene in scenes if isinstance(scene, dict)])
                    if not args.get("existing_scene_count") and actual_existing:
                        existing_scene_count = max(1, min(target_scene_count, actual_existing))
            if spec_path.is_file():
                spec = json.loads(spec_path.read_text(encoding="utf-8"))
                if isinstance(spec, dict):
                    if not args.get("image_model_id") and not args.get("image_model"):
                        image_model = _policy_image_model(spec.get("image_model_id") or spec.get("image_model"))
                    if not args.get("video_model") and not args.get("model"):
                        video_model = _policy_video_model(spec.get("video_model"))
                    spec_seconds_per_scene = _float(spec.get("seconds_per_scene"), None)
    except Exception:
        pass

    additional_scene_count = max(0, target_scene_count - existing_scene_count)
    stills, still_note, priced_model = _seedream_image_estimate(
        image_model,
        edit=True,
        quantity=additional_scene_count,
    )
    still_rate = _unit_rate(stills, additional_scene_count)
    still_model_note = image_model or priced_model
    if additional_scene_count:
        stills, image_envelope = _image_retry_envelope(
            image_model or "seedream_edit",
            edit=True,
            quantity=additional_scene_count,
            attempts=2,
        )
        still_rate = _unit_rate(stills, additional_scene_count)
        still_note = image_envelope["pricing_note"]
    else:
        image_envelope = {
            "requested_model": image_model or "seedream_edit",
            "quantity": 0,
            "max_attempts": 2,
            "usd_per_attempt": 0.0,
            "fallback_included": False,
            "pricing_note": "zero_quantity",
        }

    new_scene_indices = list(range(existing_scene_count, target_scene_count))
    raw_animate_indices = args.get("animate_scene_indices")
    explicit_animation_targets = isinstance(raw_animate_indices, list)
    if explicit_animation_targets:
        animate_scene_indices = sorted({
            int(index)
            for index in raw_animate_indices[:60]
            if str(index).strip().lstrip("-").isdigit()
            and existing_scene_count <= int(index) < target_scene_count
        })
        animation_estimate_mode = "explicit_indices"
    else:
        policy = str(args.get("animate_policy") or "heroes").strip().lower()
        animate_scene_indices = [] if policy == "none" else new_scene_indices
        animation_estimate_mode = (
            "legacy_policy_all" if policy == "all" else
            "legacy_policy_none" if policy == "none" else
            "legacy_heroes_upper_bound"
        )

    explicit_duration = _float(args.get("duration_seconds"), None)
    explicit_per_scene = _float(args.get("seconds_per_scene"), None)
    if explicit_per_scene is not None and explicit_per_scene > 0:
        seconds_per_scene = explicit_per_scene
    elif explicit_duration is not None and explicit_duration > 0:
        seconds_per_scene = explicit_duration / float(target_scene_count)
    elif spec_seconds_per_scene is not None and spec_seconds_per_scene > 0:
        seconds_per_scene = spec_seconds_per_scene
    else:
        seconds_per_scene = 5.0
    seconds_per_scene = max(1.0, min(60.0, float(seconds_per_scene)))
    animated_seconds = round(len(animate_scene_indices) * seconds_per_scene, 4)
    video_model = video_model or "kling_pro"
    video, video_rate, video_note = _video_cost(video_model, animated_seconds)
    cushion = _cushion_pct()
    total = (stills + video) * (1.0 + cushion)
    return total, {
        "stage": "expand/proof_to_full_short",
        "job_id": job_id or None,
        "target_scene_count": target_scene_count,
        "existing_scene_count": existing_scene_count,
        "additional_scene_count": additional_scene_count,
        "preserve_scene_indices": list(args.get("preserve_scene_indices") or [0]),
        "animate_scene_indices": animate_scene_indices,
        "animation_estimate_mode": animation_estimate_mode,
        "image_model": still_model_note,
        "still_usd_per_image": still_rate,
        "stills_usd": round(stills, 4),
        "stills_pricing_note": still_note,
        "image_retry_envelope": image_envelope,
        "video_model": video_model,
        "seconds_per_animated_scene": round(seconds_per_scene, 4),
        "animated_new_scene_count": len(animate_scene_indices),
        "animated_new_scene_seconds": animated_seconds,
        "video_usd_per_second": video_rate,
        "video_usd": round(video, 4),
        "video_pricing_note": video_note,
        "cushion_pct": cushion,
    }


def _estimate_shortform_finalize(args: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    job_id = str(args.get("job_id") or "").strip()
    scene_count = max(1, min(60, int(args.get("scene_count") or 12)))
    total_seconds = _video_seconds(args, count=scene_count, default_per_scene=5.0)
    script_chars = max(1000, int(args.get("script_char_count") or 0))
    sfx_enabled = bool(args.get("sfx_enabled", False))
    background_music = str(args.get("background_music") or "off").strip().lower()
    try:
        if job_id:
            from studio_agent import jobs

            ws = (jobs.ROOT / jobs.SKELETON_OUTPUT / job_id).resolve()
            scenes_path = ws / "scenes.json"
            spec_path = ws / "job_spec.json"
            scenes = json.loads(scenes_path.read_text(encoding="utf-8")) if scenes_path.exists() else []
            if isinstance(scenes, list) and scenes:
                dict_scenes = [sc for sc in scenes[:60] if isinstance(sc, dict)]
                scene_count = max(1, min(60, len(scenes)))
                if dict_scenes:
                    total_seconds = round(
                        sum(float(sc.get("duration_sec") or 5.0) for sc in dict_scenes),
                        4,
                    )
                    script_chars = max(
                        1000,
                        sum(len(str(sc.get("narration") or "")) for sc in dict_scenes),
                    )
            if spec_path.exists():
                spec = json.loads(spec_path.read_text(encoding="utf-8"))
                if isinstance(spec, dict):
                    sfx_enabled = bool(spec.get("sfx_enabled", sfx_enabled))
                    background_music = str(spec.get("background_music") or background_music or "off").strip().lower()
    except Exception:
        pass

    tts, tts_note, tts_provider, tts_unit, tts_unit_rate = _estimate_tts(script_chars)
    sfx_seconds = total_seconds if sfx_enabled else 0.0
    sfx, sfx_note = _priced_unit("mmaudio_v2", fallback_key="mmaudio_v2_per_second", quantity=sfx_seconds) if sfx_seconds else (0.0, "disabled")
    bgm_seconds = total_seconds if background_music not in {"", "off", "none", "no", "no background music"} else 0.0
    bgm, bgm_note = _priced_unit("mmaudio_v2", fallback_key="mmaudio_v2_per_second", quantity=bgm_seconds) if bgm_seconds else (0.0, "disabled")
    local_compose_allowance = _fallback("shortform_compose_allowance_usd")
    cushion = _cushion_pct()
    subtotal = tts + sfx + bgm + local_compose_allowance
    total = subtotal * (1.0 + cushion)
    return total, {
        "stage": "finalize",
        "job_id": job_id or None,
        "scene_count": scene_count,
        "estimated_duration_seconds": total_seconds,
        "tts_chars": script_chars,
        "tts_provider": tts_provider,
        "tts_unit": tts_unit,
        "tts_unit_rate": tts_unit_rate,
        "tts_allowance_usd": round(tts, 4),
        "tts_pricing_note": tts_note,
        "sfx_enabled": sfx_enabled,
        "mmaudio_sfx_seconds": sfx_seconds,
        "mmaudio_sfx_usd": round(sfx, 4),
        "mmaudio_sfx_pricing_note": sfx_note,
        "background_music": background_music or "off",
        "mmaudio_bgm_seconds": bgm_seconds,
        "mmaudio_bgm_usd": round(bgm, 4),
        "mmaudio_bgm_pricing_note": bgm_note,
        "local_compose_allowance_usd": round(local_compose_allowance, 4),
        "cushion_pct": cushion,
    }


def _estimate_longform_start(args: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    try:
        from long_form.prompts.channels import get_channel
        from long_form.pipeline import compute_render_cost
        from studio_agent.tools import _build_outline_from_args

        channel = dict(get_channel(str(args.get("channel_key") or "").strip()))
        selected_image_model = _policy_image_model(
            args.get("image_model_id") or channel.get("image_model_default")
        )
        channel["image_model_default"] = selected_image_model
        outline = _build_outline_from_args(args)
        outline["motion_policy"] = str(args.get("motion_policy") or outline.get("motion_policy") or "balanced")
        if args.get("hero_motion_ratio") is not None:
            outline["hero_motion_ratio"] = max(0.0, min(1.0, float(args["hero_motion_ratio"])))
        cost = compute_render_cost(
            channel,
            outline,
            scenes_per_chapter_override=int(channel.get("scenes_per_chapter") or 0) or None,
        )
        projected = float(cost.get("all_in_usd") or cost.get("total_usd") or 0.0)
        proof_only = bool(args.get("visual_proof_only", True))
        if proof_only:
            proof_still = float(cost.get("still_usd_per_image") or 0.0)
            est = proof_still * (1.0 + _cushion_pct())
            return est, {
                "stage": "start/proof_still",
                **cost,
                "visual_proof_only": True,
                "charged_now_usd": round(est, 4),
                "projected_full_project_usd": round(projected, 2),
                "paid_i2v_deferred": True,
            }
        est = float(cost.get("stage_1_usd") or cost.get("total_usd") or 0.0)
        return est, {"stage": "start/stills", **cost, "projected_full_project_usd": round(projected, 2)}
    except Exception as exc:
        chapters = max(1, _chapter_count(args))
        scenes = chapters * 12
        est = scenes * _fallback("seedream_v45_per_image") * 1.15
        return est, {"fallback": str(exc)[:160], "chapters": chapters, "scene_count": scenes}


def _estimate_longform_expand(args: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    """Gallery expansion after proof approval — this is where HR burns xAI/fal budget."""
    job_id = str(args.get("job_id") or "").strip()
    try:
        from long_form import pipeline as lf
        from long_form.prompts.channels import get_channel

        state = lf.load_state(job_id) or {}
        outline = state.get("outline") if isinstance(state.get("outline"), dict) else {}
        channel = dict(get_channel(str(state.get("channel_key") or outline.get("channel_key") or "")))
        image_model = _policy_image_model(
            outline.get("image_model_id") or channel.get("image_model_default") or "ernie_image"
        )
        channel["image_model_default"] = image_model
        chapters_path = lf._chapters_path(job_id) if hasattr(lf, "_chapters_path") else None
        n_chapters = len(outline.get("chapters") or [])
        if chapters_path and chapters_path.is_file():
            import json as _json

            chapters_data = _json.loads(chapters_path.read_text(encoding="utf-8"))
            n_chapters = max(n_chapters, len(chapters_data.get("chapters") or []))
        scenes_per = int(
            state.get("scenes_per_chapter")
            or outline.get("scenes_per_chapter")
            or channel.get("scenes_per_chapter")
            or 12
        )
        max_scenes = max(1, int(os.getenv("STUDIO_LONGFORM_MAX_SCENES", "144") or 144))
        n_scenes = min(max_scenes, max(1, n_chapters * scenes_per))
        # Proof still already rendered — bill the remaining gallery.
        billable_scenes = max(0, n_scenes - 1)
        cost = lf.compute_render_cost(
            channel,
            outline,
            scenes_per_chapter_override=scenes_per,
        )
        still_per = float(cost.get("still_usd_per_image") or 0.04)
        est = still_per * billable_scenes * (1.0 + _cushion_pct())
        return est, {
            "stage": "expand_gallery",
            "job_id": job_id,
            "billable_scenes": billable_scenes,
            "n_chapters": n_chapters,
            "scenes_per_chapter": scenes_per,
            "image_model": image_model,
            "still_usd_per_image": still_per,
            "projected_full_project_usd": round(float(cost.get("all_in_usd") or cost.get("total_usd") or 0.0), 2),
        }
    except Exception as exc:
        est = 12.0 * (1.0 + _cushion_pct())
        return est, {"fallback": str(exc)[:160], "job_id": job_id}


def _estimate_longform_finalize(args: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    job_id = str(args.get("job_id") or "").strip()
    try:
        from long_form import pipeline as lf

        state = lf.load_state(job_id) or {}
        channel_key = str(state.get("channel_key") or "").strip()
        from long_form.prompts.channels import get_channel

        channel = get_channel(channel_key)
        cost = lf.compute_render_cost(channel, state.get("outline") or {})
        est = float(cost.get("stage_2_usd") or cost.get("total_usd") or 0.0)
        return est, {"stage": "finalize", **cost}
    except Exception as exc:
        return _default_cap("finalize_longform_render") * 0.75, {"fallback": str(exc)[:160], "job_id": job_id}


def _chapter_count(args: dict[str, Any]) -> int:
    raw = args.get("chapters_json")
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict) and isinstance(parsed.get("chapters"), list):
                return len(parsed["chapters"])
        except Exception:
            pass
    return 1


def _scene_index_count(args: dict[str, Any], *, default: int) -> int:
    if bool(args.get("visual_proof_only")):
        return 1
    raw = args.get("scene_indices")
    if isinstance(raw, list) and raw:
        return max(1, min(60, len(raw)))
    return max(1, min(60, int(args.get("scene_count") or default)))


def _resolve_video_model_for_budget(args: dict[str, Any]) -> str:
    """Prefer explicit args, then job_spec, then a cheap default.

    Missing video_model used to default to Seedance (~$0.30/s), which made
    LTX-budget UI jobs look like a $4+ animate and trip the $3 hard cap.
    """
    explicit = str(args.get("video_model") or args.get("model") or "").strip().lower()
    if explicit:
        return _policy_video_model(explicit)
    job_id = str(args.get("job_id") or "").strip()
    if job_id:
        try:
            from studio_agent.tools import _shortform_workspace

            spec_path = _shortform_workspace(job_id) / "job_spec.json"
            if spec_path.is_file():
                spec = json.loads(spec_path.read_text(encoding="utf-8"))
                if isinstance(spec, dict):
                    from_job = str(spec.get("video_model") or "").strip().lower()
                    if from_job:
                        return _policy_video_model(from_job)
        except Exception:
            pass
    return "ltx_budget"


def _video_seconds(args: dict[str, Any], *, count: int, default_per_scene: float) -> float:
    explicit_total = _float(args.get("duration_seconds"), None)
    if explicit_total is not None and explicit_total > 0:
        return round(max(float(count), min(3600.0, explicit_total)), 4)
    per_scene = _float(args.get("seconds_per_scene"), None)
    if per_scene is not None and per_scene > 0:
        return round(max(float(count), min(3600.0, float(count) * per_scene)), 4)
    durations = args.get("scene_durations")
    if isinstance(durations, list) and durations:
        total = 0.0
        for item in durations[:60]:
            val = _float(item, None)
            if val is not None and val > 0:
                total += val
        if total > 0:
            return round(max(float(count), min(3600.0, total)), 4)
    return round(max(float(count), float(count) * default_per_scene), 4)


def _video_rate(video_model: str) -> float:
    _cost, rate, _note = _video_cost(video_model, 1.0)
    return rate


def _video_cost(video_model: str, seconds: float) -> tuple[float, float, str]:
    model = _policy_video_model(video_model)
    qty = max(0.0, seconds)
    if model == "kling21_master":
        cost = round(0.28 * qty, 4)
        return cost, _unit_rate(cost, qty), "fallback:kling21_master_per_second"
    if model in {"kling_pro", "premium"} or ("kling" in model and "pro" in model):
        cost, note = _priced_unit("kling_v21_pro", fallback_key="kling_v21_pro_per_second", quantity=max(0.0, seconds))
        return cost, _unit_rate(cost, max(0.0, seconds)), note
    if "pixverse" in model:
        cost, note = _priced_unit("pixverse_v6", fallback_key="pixverse_v6_per_second", quantity=max(0.0, seconds))
        return cost, _unit_rate(cost, max(0.0, seconds)), note
    if "ltx_budget" in model or "ltxv" in model or "ltx_098" in model:
        cost, note = _priced_unit("ltx_098_distilled", fallback_key="ltx_098_distilled_per_second", quantity=max(0.0, seconds))
        return cost, _unit_rate(cost, max(0.0, seconds)), note
    if "seedance" in model:
        cost, note = _priced_unit("seedance_20_i2v", fallback_key="seedance_20_i2v_per_second", quantity=max(0.0, seconds))
        return cost, _unit_rate(cost, max(0.0, seconds)), note
    cost, note = _priced_unit("kling_v21_standard", fallback_key="kling_v21_standard_per_second", quantity=max(0.0, seconds))
    return cost, _unit_rate(cost, max(0.0, seconds)), note


def _cushion_pct() -> float:
    raw = _fallback("cushion_pct")
    if raw <= 0:
        raw = 0.25
    return max(0.25, min(0.5, raw))


def _default_cap(tool_name: str) -> float:
    env_name = f"STUDIO_BUDGET_CAP_{tool_name.upper()}_USD"
    return max(0.0, _float(os.getenv(env_name), DEFAULT_CAPS_USD.get(tool_name, 1.0)) or 0.0)


def _fallback(key: str) -> float:
    try:
        from long_form import fal_pricing

        live_fallback = float(fal_pricing.FALLBACK_USD.get(key, 0.0) or 0.0)
        if live_fallback > 0:
            return live_fallback
    except Exception:
        pass
    return FALLBACK_USD.get(key, 0.0)


def _tts_provider() -> str:
    return "fal"


def _estimate_tts(script_chars: int) -> tuple[float, str, str, str, float]:
    provider = _tts_provider()
    chars = max(1, int(script_chars or 1))
    units = max(1.0, chars / 1000.0)
    amount, note = _priced_unit("minimax_speech", fallback_key="fal_minimax_per_1k_chars", quantity=units)
    return amount, note, "fal", "1k_chars", _unit_rate(amount, units)


def _priced_unit(key: str, *, fallback_key: str, quantity: float) -> tuple[float, str]:
    qty = max(0.0, float(quantity or 0.0))
    if qty <= 0:
        return 0.0, "zero_quantity"
    try:
        from long_form import fal_pricing

        snapshot = fal_pricing.get_pricing_snapshot()
        cost, note = fal_pricing.unit_cost(snapshot, key, fallback_key=fallback_key, quantity=qty)
        source = str(snapshot.get("source") or "unknown")
        error = str(snapshot.get("error") or "").strip()
        if error:
            note = f"{note}; source={source}; error={error[:120]}"
        else:
            note = f"{note}; source={source}"
        return round(max(0.0, float(cost or 0.0)), 4), note
    except Exception as exc:
        return round(_fallback(fallback_key) * qty, 4), f"fallback:{fallback_key}; pricing_error={str(exc)[:120]}"


def _unit_rate(total: float, quantity: float) -> float:
    qty = max(0.0, float(quantity or 0.0))
    if qty <= 0:
        return 0.0
    return round(float(total or 0.0) / qty, 6)


def _float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


MODEL_DISPLAY_NAMES = {
    "grok_imagine": "Grok Imagine Quality",
    "grok_imagine_standard": "Grok Imagine",
    "grok-imagine-image": "Grok Imagine",
    "grok-imagine-image-quality": "Grok Imagine Quality",
    "seedream_edit": "Seedream 4.5 Edit",
    "seedream_v45_edit": "Seedream 4.5 Edit",
    "seedream45": "Seedream 4.5 T2I",
    "seedream_v45": "Seedream 4.5 T2I",
    "seedream_v4": "Seedream 4.0",
    "seedream_v5_lite": "Seedream 5.0 Lite",
    "seedream_v5_lite_modal": "Seedream 5.0 Lite (Modal)",
    "ernie_image": "ERNIE-Image",
    "grok_imagine_video": "Grok Imagine Video",
    "grok_imagine_video_15": "Grok Imagine Video 1.5",
    "grok_imagine_video_15_1080p": "Grok Imagine Video 1.5 1080p",
    "ltx_budget": "LTX 0.9.8 Budget",
    "seedance": "Seedance 2.0",
    "pixverse": "PixVerse V6",
    "kling_pro": "Kling 2.1 Pro",
    "kling21_standard": "Kling 2.1 Standard",
}


def _model_label(model_id: str) -> str:
    clean = str(model_id or "").strip().lower()
    return MODEL_DISPLAY_NAMES.get(clean, clean or "unknown")


def format_shortform_cost_quote(
    start_estimate: BudgetEstimate,
    *,
    finalize_estimate: BudgetEstimate | None = None,
) -> str:
    """Human-readable shortform quote grounded in production_budget math."""
    breakdown = dict(start_estimate.breakdown or {})
    image_model = str(breakdown.get("image_model") or breakdown.get("image_model_pricing_unit") or "")
    video_model = str(breakdown.get("video_model") or "")
    scenes = int(breakdown.get("scene_count") or 1)
    seconds = float(breakdown.get("video_seconds") or breakdown.get("requested_video_seconds") or 0.0)
    still_rate = float(breakdown.get("still_usd_per_image") or 0.0)
    stills_usd = float(breakdown.get("stills_usd") or 0.0)
    video_rate = float(breakdown.get("video_usd_per_second") or 0.0)
    video_usd = float(breakdown.get("video_usd") or 0.0)
    start_tts = float(breakdown.get("tts_allowance_usd") or 0.0)
    cushion = float(breakdown.get("cushion_pct") or 0.0)
    lines = [
        "Cost estimate (grounded in your active Studio session models):",
        "",
        "Image generation:",
        f"- {scenes} scene still(s) via {_model_label(image_model)}: ${stills_usd:.2f}"
        + (f" (${still_rate:.3f}/image)" if still_rate else ""),
        "",
        "Video:",
    ]
    if seconds > 0 and video_usd > 0:
        lines.append(
            f"- {seconds:.0f}s {_model_label(video_model)} i2v: ${video_usd:.2f}"
            + (f" (${video_rate:.3f}/sec)" if video_rate else "")
        )
    else:
        lines.append(
            f"- i2v deferred until scene approval ({_model_label(video_model)} selected in session)"
        )
    lines.extend(["", "Audio:"])
    if start_tts > 0:
        lines.append(
            f"- Voiceover allowance at start: ${start_tts:.3f} ({breakdown.get('tts_provider') or 'tts'})"
        )
    finalize_usd = 0.0
    if finalize_estimate is not None:
        fin = dict(finalize_estimate.breakdown or {})
        finalize_usd = float(finalize_estimate.estimated_usd or 0.0)
        tts = float(fin.get("tts_allowance_usd") or 0.0)
        if tts > 0:
            lines.append(f"- Finalize narration ({fin.get('tts_provider') or 'tts'}): ${tts:.3f}")
        sfx = float(fin.get("mmaudio_sfx_usd") or 0.0)
        if sfx > 0:
            lines.append(f"- SFX (mmaudio): ${sfx:.3f}")
        bgm = float(fin.get("mmaudio_bgm_usd") or 0.0)
        if bgm > 0:
            lines.append(f"- Background music (mmaudio): ${bgm:.3f}")
    elif breakdown.get("tts_pricing_note") == "deferred_until_finalize":
        lines.append("- Voiceover/SFX finalize cost is deferred until you approve scenes and run finalize_production.")
    total = float(start_estimate.estimated_usd or 0.0) + finalize_usd
    lines.extend([
        "",
        f"Start-stage subtotal (incl. {cushion * 100:.0f}% cushion): ${float(start_estimate.estimated_usd or 0.0):.2f}",
    ])
    if finalize_estimate is not None:
        lines.append(f"Finalize-stage subtotal (incl. cushion): ${finalize_usd:.2f}")
    lines.append(f"Total grounded estimate: ${total:.2f}")
    lines.extend([
        "",
        "Pricing source: Studio production_budget preflight using your session image_model_id + video_model.",
        "Do not substitute other models unless they are the active session selections above.",
    ])
    return "\n".join(lines)
