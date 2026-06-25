"""Budget contract for Studio Agent production tools.

The goal is not perfect accounting. The goal is a hard preflight barrier:
expensive tools must have an estimated spend and must not start if the estimate
is above the approved cap.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any


class BudgetExceededError(RuntimeError):
    pass


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
    "start_shortform_generate",
    "start_longform_render",
    "finalize_longform_render",
    "generate_longform_thumbnails",
    "edit_production_scene_still",
    "edit_production_scenes_still",
    "regenerate_production_scene_still",
    "animate_production_scenes",
    "finalize_production",
    "re_edit_production",
    "regenerate_longform_still",
})


DEFAULT_CAPS_USD = {
    "start_shortform_generate": 5.0,
    "start_longform_render": 25.0,
    "finalize_longform_render": 85.0,
    "generate_longform_thumbnails": 1.0,
    "edit_production_scene_still": 0.25,
    "edit_production_scenes_still": 1.0,
    "regenerate_production_scene_still": 0.25,
    "animate_production_scenes": 3.0,
    "finalize_production": 1.0,
    "re_edit_production": 1.5,
    "regenerate_longform_still": 0.25,
}


APPROVAL_REQUIRED_TOOLS = frozenset({
    "start_shortform_generate",
    "start_longform_render",
    "set_production_scenes_animate",
    "animate_production_scenes",
    "finalize_production",
    "finalize_longform_render",
})


TOOL_LANES = {
    "start_shortform_generate": "render",
    "start_longform_render": "render",
    "finalize_longform_render": "render",
    "generate_longform_thumbnails": "render",
    "edit_production_scene_still": "render",
    "edit_production_scenes_still": "render",
    "regenerate_production_scene_still": "render",
    "regenerate_longform_still": "render",
    "animate_production_scenes": "render",
    "finalize_production": "render",
    "re_edit_production": "render",
    "analyze_reference_video": "analysis",
    "analyze_competitor_video": "analysis",
    "build_scene_blueprint_from_reference": "analysis",
    "get_channel_analytics": "analysis",
}


STAGE_GATES = {
    "start_shortform_generate": ["cost_preflight", "create_stills", "await_scene_review"],
    "edit_production_scene_still": ["cost_preflight", "edit_still", "await_scene_review"],
    "edit_production_scenes_still": ["cost_preflight", "batch_edit_stills", "await_scene_review"],
    "regenerate_production_scene_still": ["cost_preflight", "regenerate_still", "await_scene_review"],
    "animate_production_scenes": ["scene_approval_required", "image_to_video", "await_animation_result"],
    "finalize_production": ["compose", "package", "publish_ready_artifact"],
    "start_longform_render": ["cost_preflight", "outline", "create_stills", "await_chapter_review"],
    "finalize_longform_render": ["chapter_approval_required", "compose", "package"],
    "generate_longform_thumbnails": ["cost_preflight", "thumbnail_variants", "await_packaging_review"],
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


def estimate_tool_cost(tool_name: str, args: dict[str, Any] | None = None) -> BudgetEstimate:
    args = dict(args or {})
    name = str(tool_name or "")
    explicit_cap = _float(args.get("max_budget_usd"), None)
    max_budget = explicit_cap if explicit_cap is not None else _default_cap(name)
    mode = "explicit" if explicit_cap is not None else "default"
    if name == "start_shortform_generate":
        est, breakdown = _estimate_shortform_start(args)
    elif name == "start_longform_render":
        est, breakdown = _estimate_longform_start(args)
    elif name == "finalize_longform_render":
        est, breakdown = _estimate_longform_finalize(args)
    elif name == "generate_longform_thumbnails":
        count = max(1, min(3, int(args.get("count") or 3)))
        est = count * _fallback("seedream_v45_per_image")
        breakdown = {"thumbnails": count, "seedream_v45_per_image": _fallback("seedream_v45_per_image")}
    elif name == "edit_production_scenes_still":
        raw_indices = args.get("scene_indices") or []
        try:
            count = len(raw_indices) if isinstance(raw_indices, list) and raw_indices else int(args.get("scene_count") or 12)
        except Exception:
            count = 12
        count = max(1, min(60, count))
        est = count * _fallback("seedream_v45_per_image")
        breakdown = {"seedream_v45_edit_images": count, "scope": str(args.get("scope") or "character")}
    elif name in {"edit_production_scene_still", "regenerate_production_scene_still", "regenerate_longform_still"}:
        est = _fallback("seedream_v45_per_image")
        breakdown = {"seedream_v45_edit_images": 1}
    elif name == "animate_production_scenes":
        count = len(args.get("scene_indices") or []) or 3
        seconds = count * 5.0
        est = seconds * _fallback("kling_v21_standard_per_second")
        breakdown = {"scene_count": count, "video_seconds": seconds, "model": "standard_i2v"}
    elif name in {"finalize_production", "re_edit_production"}:
        est = _fallback("fal_minimax_per_1k_chars") + 0.25
        breakdown = {"recompose": "local_or_cached", "tts_safety_allowance_usd": round(est, 4)}
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
        "edit_production_scene_still",
        "edit_production_scenes_still",
        "regenerate_production_scene_still",
        "animate_production_scenes",
        "finalize_production",
        "re_edit_production",
    }:
        return {"kind": "shortform", "key": "job_id", "job_id": job_id or None, "must_persist": True}
    if "longform" in name:
        return {"kind": "longform", "key": "job_id", "job_id": job_id or None, "must_persist": True}
    return {"kind": "generic", "must_persist": False}


def _estimate_shortform_start(args: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    scenes = max(1, int(args.get("scene_count") or 12))
    full_auto = bool(args.get("_full_auto") or args.get("full_auto"))
    animate = bool(args.get("animate", False)) and full_auto
    stills = scenes * _fallback("seedream_v45_per_image")
    seconds = scenes * 5.0 if animate else 0.0
    video_model = str(args.get("video_model") or "seedance").strip()
    if video_model == "kling_pro":
        video_rate = _fallback("kling_v21_pro_per_second")
    elif video_model == "pixverse":
        video_rate = _fallback("pixverse_v6_per_second")
    else:
        video_rate = _fallback("kling_v21_standard_per_second")
    video = seconds * video_rate
    tts = _fallback("fal_minimax_per_1k_chars")
    total = (stills + video + tts) * 1.15
    return total, {
        "scene_count": scenes,
        "review_gate": not full_auto,
        "i2v_deferred_until_scene_approval": not full_auto,
        "stills_usd": round(stills, 4),
        "video_seconds": seconds,
        "video_model": video_model,
        "video_usd": round(video, 4),
        "tts_allowance_usd": round(tts, 4),
        "cushion_pct": 0.15,
    }


def _estimate_longform_start(args: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    try:
        from long_form.prompts.channels import get_channel
        from long_form.pipeline import compute_render_cost
        from studio_agent.tools import _build_outline_from_args

        channel = get_channel(str(args.get("channel_key") or "").strip())
        outline = _build_outline_from_args(args)
        outline["motion_policy"] = str(args.get("motion_policy") or outline.get("motion_policy") or "balanced")
        if args.get("hero_motion_ratio") is not None:
            outline["hero_motion_ratio"] = max(0.0, min(1.0, float(args["hero_motion_ratio"])))
        cost = compute_render_cost(channel, outline)
        est = float(cost.get("stage_1_usd") or cost.get("total_usd") or 0.0)
        return est, {"stage": "start/stills", **cost}
    except Exception as exc:
        chapters = max(1, _chapter_count(args))
        scenes = chapters * 12
        est = scenes * _fallback("seedream_v45_per_image") * 1.15
        return est, {"fallback": str(exc)[:160], "chapters": chapters, "scene_count": scenes}


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


def _default_cap(tool_name: str) -> float:
    env_name = f"STUDIO_BUDGET_CAP_{tool_name.upper()}_USD"
    return max(0.0, _float(os.getenv(env_name), DEFAULT_CAPS_USD.get(tool_name, 1.0)) or 0.0)


def _fallback(key: str) -> float:
    try:
        from long_form import fal_pricing

        return float(fal_pricing.FALLBACK_USD.get(key, 0.0) or 0.0)
    except Exception:
        return {
            "seedream_v45_per_image": 0.04,
            "fal_minimax_per_1k_chars": 0.10,
            "kling_v21_standard_per_second": 0.056,
            "kling_v21_pro_per_second": 0.098,
            "pixverse_v6_per_second": 0.045,
        }.get(key, 0.0)


def _float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default
