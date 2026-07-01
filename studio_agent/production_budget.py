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
        est, note = _priced_unit("seedream_v45", fallback_key="seedream_v45_per_image", quantity=count)
        breakdown = {
            "thumbnails": count,
            "seedream_v45_per_image": _unit_rate(est, count),
            "pricing_note": note,
        }
    elif name == "edit_production_scenes_still":
        raw_indices = args.get("scene_indices") or []
        try:
            count = len(raw_indices) if isinstance(raw_indices, list) and raw_indices else int(args.get("scene_count") or 12)
        except Exception:
            count = 12
        count = max(1, min(60, count))
        est, note = _priced_unit("seedream_v45_edit", fallback_key="seedream_v45_per_image", quantity=count)
        breakdown = {
            "seedream_v45_edit_images": count,
            "seedream_v45_edit_per_image": _unit_rate(est, count),
            "scope": str(args.get("scope") or "character"),
            "pricing_note": note,
        }
    elif name in {"edit_production_scene_still", "regenerate_production_scene_still", "regenerate_longform_still"}:
        est, note = _priced_unit("seedream_v45_edit", fallback_key="seedream_v45_per_image", quantity=1.0)
        breakdown = {"seedream_v45_edit_images": 1, "seedream_v45_edit_per_image": est, "pricing_note": note}
    elif name == "animate_production_scenes":
        count = _scene_index_count(args, default=3)
        seconds = _video_seconds(args, count=count, default_per_scene=5.0)
        video_model = str(args.get("video_model") or args.get("model") or "seedance").strip().lower()
        est, video_rate, note = _video_cost(video_model, seconds)
        breakdown = {
            "scene_count": count,
            "video_seconds": seconds,
            "video_model": video_model,
            "video_usd_per_second": video_rate,
            "video_usd": round(est, 4),
            "pricing_note": note,
        }
    elif name in {"finalize_production", "re_edit_production"}:
        est, breakdown = _estimate_shortform_finalize(args)
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
    scenes = max(1, min(60, int(args.get("scene_count") or args.get("beats") or 12)))
    full_auto = bool(args.get("_full_auto") or args.get("full_auto"))
    animate = bool(args.get("animate", False)) and full_auto
    stills, still_note = _priced_unit("seedream_v45_edit", fallback_key="seedream_v45_per_image", quantity=scenes)
    requested_seconds = _video_seconds(args, count=scenes, default_per_scene=5.0)
    seconds = requested_seconds if animate else 0.0
    video_model = str(args.get("video_model") or "seedance").strip().lower()
    video, video_rate, video_note = _video_cost(video_model, seconds)
    script_chars = max(1000, int(args.get("script_char_count") or len(str(args.get("script") or "")) or scenes * 140))
    tts_units = max(1.0, script_chars / 1000.0)
    if full_auto:
        tts, tts_note = _priced_unit("minimax_speech", fallback_key="fal_minimax_per_1k_chars", quantity=tts_units)
    else:
        tts, tts_note = 0.0, "deferred_until_finalize"
    cushion = _cushion_pct()
    total = (stills + video + tts) * (1.0 + cushion)
    return total, {
        "scene_count": scenes,
        "review_gate": not full_auto,
        "i2v_deferred_until_scene_approval": not full_auto,
        "seedream_v45_edit_per_image": _unit_rate(stills, scenes),
        "stills_usd": round(stills, 4),
        "stills_pricing_note": still_note,
        "video_seconds": seconds,
        "requested_video_seconds": requested_seconds,
        "video_model": video_model,
        "video_usd_per_second": video_rate,
        "video_usd": round(video, 4),
        "video_pricing_note": video_note,
        "tts_chars": script_chars,
        "fal_minimax_per_1k_chars": _unit_rate(tts, tts_units) if tts else 0.0,
        "tts_allowance_usd": round(tts, 4),
        "tts_pricing_note": tts_note,
        "cushion_pct": cushion,
    }


def _estimate_shortform_finalize(args: dict[str, Any]) -> tuple[float, dict[str, Any]]:
    job_id = str(args.get("job_id") or "").strip()
    scene_count = max(1, min(60, int(args.get("scene_count") or 12)))
    total_seconds = _video_seconds(args, count=scene_count, default_per_scene=5.0)
    script_chars = max(1000, int(args.get("script_char_count") or 0))
    sfx_enabled = bool(args.get("sfx_enabled", True))
    background_music = str(args.get("background_music") or "auto").strip().lower()
    try:
        if job_id:
            from studio_agent import jobs

            ws = (jobs.ROOT / jobs.SKELETON_OUTPUT / job_id).resolve()
            scenes_path = ws / "scenes.json"
            spec_path = ws / "job_spec.json"
            scenes = json.loads(scenes_path.read_text(encoding="utf-8")) if scenes_path.exists() else []
            if isinstance(scenes, list) and scenes:
                scene_count = max(1, min(60, len(scenes)))
                total_seconds = round(
                    sum(float(sc.get("duration_sec") or 5.0) for sc in scenes[:60]),
                    4,
                )
                script_chars = max(
                    1000,
                    sum(len(str(sc.get("narration") or "")) for sc in scenes[:60]),
                )
            if spec_path.exists():
                spec = json.loads(spec_path.read_text(encoding="utf-8"))
                if isinstance(spec, dict):
                    sfx_enabled = bool(spec.get("sfx_enabled", sfx_enabled))
                    background_music = str(spec.get("background_music") or background_music or "auto").strip().lower()
    except Exception:
        pass

    tts_units = max(1.0, script_chars / 1000.0)
    tts, tts_note = _priced_unit("minimax_speech", fallback_key="fal_minimax_per_1k_chars", quantity=tts_units)
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
        "fal_minimax_per_1k_chars": _unit_rate(tts, tts_units),
        "tts_allowance_usd": round(tts, 4),
        "tts_pricing_note": tts_note,
        "sfx_enabled": sfx_enabled,
        "mmaudio_sfx_seconds": sfx_seconds,
        "mmaudio_sfx_usd": round(sfx, 4),
        "mmaudio_sfx_pricing_note": sfx_note,
        "background_music": background_music or "auto",
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


def _scene_index_count(args: dict[str, Any], *, default: int) -> int:
    raw = args.get("scene_indices")
    if isinstance(raw, list) and raw:
        return max(1, min(60, len(raw)))
    return max(1, min(60, int(args.get("scene_count") or default)))


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
    model = str(video_model or "").strip().lower()
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

        return float(fal_pricing.FALLBACK_USD.get(key, 0.0) or 0.0)
    except Exception:
        return {
            "seedream_v45_per_image": 0.04,
            "fal_minimax_per_1k_chars": 0.10,
            "kling_v21_standard_per_second": 0.056,
            "kling_v21_pro_per_second": 0.098,
            "pixverse_v6_per_second": 0.045,
            "seedance_20_i2v_per_second": 0.03,
            "ltx_098_distilled_per_second": 0.02,
            "mmaudio_v2_per_second": 0.001,
            "shortform_compose_allowance_usd": 0.05,
        }.get(key, 0.0)


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
