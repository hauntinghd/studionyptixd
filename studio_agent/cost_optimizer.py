"""Quality-aware production cost and gross-margin optimizer.

Read-only: these functions never reserve credits or start provider work.
"""
from __future__ import annotations

from typing import Any


SHORT_IMAGE_RATES = {
    "grok_imagine": (0.10, 5.0),
    "grok_imagine_standard": (0.04, 4.4),
    "seedream_edit": (0.04, 4.6),
    "ernie_image": (0.03, 3.8),
}

SHORT_VIDEO_ROUTES = (
    ("grok_imagine_video_15_1080p", 5.0),
    ("grok_imagine_video_15", 4.8),
    ("kling_pro", 4.7),
    ("grok_imagine_video", 4.5),
    ("kling21_standard", 4.2),
    ("pixverse", 4.0),
    ("seedance", 3.9),
)


def _margin_fields(provider_cost: float, selling_price_usd: float | None, target_margin: float) -> dict[str, Any]:
    target = max(0.0, min(0.95, float(target_margin)))
    minimum_price = provider_cost / max(0.05, 1.0 - target)
    result: dict[str, Any] = {
        "target_gross_margin_pct": round(target * 100, 1),
        "minimum_customer_price_usd": round(minimum_price, 2),
    }
    if selling_price_usd is not None and float(selling_price_usd) > 0:
        price = float(selling_price_usd)
        result.update({
            "selling_price_usd": round(price, 2),
            "gross_profit_usd": round(price - provider_cost, 2),
            "gross_margin_pct": round(((price - provider_cost) / price) * 100, 1),
            "meets_target_margin": price >= minimum_price,
        })
    return result


def optimize_shortform(
    *,
    scene_count: int,
    duration_seconds: float,
    image_model_id: str,
    video_model: str,
    animate: bool = True,
    selling_price_usd: float | None = None,
    target_margin: float = 0.70,
    max_provider_cost_usd: float | None = None,
) -> dict[str, Any]:
    from studio_agent import production_budget, store

    scenes = max(1, min(60, int(scene_count or 1)))
    seconds = max(1.0, min(600.0, float(duration_seconds or scenes * 5)))
    image_model = store.normalize_image_model(image_model_id)
    video = store.normalize_video_model(video_model)
    still_rate, quality_score = SHORT_IMAGE_RATES.get(image_model, (0.04, 4.0))
    still_cost = scenes * still_rate
    video_cost, video_rate, _ = production_budget._video_cost(video, seconds if animate else 0.0)
    narration_chars = max(400, round(seconds * 3.0))
    narration_cost = narration_chars / 1000.0 * 0.015
    provider_cost = (still_cost + video_cost + narration_cost) * 1.25
    options = []
    for candidate, (rate, score) in SHORT_IMAGE_RATES.items():
        video_routes = SHORT_VIDEO_ROUTES if animate else (("none", 5.0),)
        for candidate_video, motion_score in video_routes:
            candidate_video_cost = 0.0
            if animate:
                candidate_video_cost, _, _ = production_budget._video_cost(candidate_video, seconds)
            candidate_cost = (scenes * rate + candidate_video_cost + narration_cost) * 1.25
            combined_quality = score * 0.45 + motion_score * 0.55
            margin = _margin_fields(candidate_cost, selling_price_usd, target_margin)
            options.append({
                "image_model_id": candidate,
                "video_model": candidate_video,
                "visual_quality_score": round(combined_quality, 2),
                "still_quality_score": score,
                "motion_quality_score": motion_score,
                "projected_provider_cost_usd": round(candidate_cost, 2),
                "within_budget": max_provider_cost_usd is None or candidate_cost <= float(max_provider_cost_usd),
                **margin,
            })
    eligible = [row for row in options if row["within_budget"]]
    recommended = max(
        eligible or options,
        key=lambda row: (row["visual_quality_score"], -row["projected_provider_cost_usd"]),
    )
    result = {
        "format": "shortform",
        "scene_count": scenes,
        "duration_seconds": seconds,
        "animate": bool(animate),
        "image_model_id": image_model,
        "video_model": video,
        "still_usd": round(still_cost, 3),
        "video_usd": round(video_cost, 3),
        "video_usd_per_second": round(video_rate, 4),
        "narration_usd": round(narration_cost, 3),
        "projected_full_provider_cost_usd": round(provider_cost, 2),
        "active_route": {
            "image_model_id": image_model,
            "video_model": video if animate else "none",
            "projected_provider_cost_usd": round(provider_cost, 2),
        },
        "recommended_route": recommended,
        "quality_cost_options": sorted(
            options,
            key=lambda row: (-row["visual_quality_score"], row["projected_provider_cost_usd"]),
        ),
    }
    result.update(_margin_fields(provider_cost, selling_price_usd, target_margin))
    return result


def optimize_longform(
    *,
    target_duration_sec: int,
    image_model_id: str = "grok_imagine_standard",
    selling_price_usd: float | None = None,
    target_margin: float = 0.70,
    max_provider_cost_usd: float | None = None,
) -> dict[str, Any]:
    from long_form.pipeline import compute_render_cost
    from long_form.prompts.channels import get_channel
    from studio_agent.tools import _build_outline_from_args

    duration = max(60, int(target_duration_sec or 1200))
    outline = _build_outline_from_args({"title": "Cost projection", "topic": "Cost projection", "target_duration_sec": duration})
    profiles = [
        ("premium", 30, "grok_imagine", 5.0),
        ("balanced", 20, "grok_imagine_standard", 4.6),
        ("economy", 15, "grok_imagine_standard", 4.2),
    ]
    options = []
    for label, scenes_per_chapter, model, score in profiles:
        channel = dict(get_channel("history_rewind"))
        channel["image_model_default"] = model
        channel["voice_provider_default"] = "xai"
        cost = compute_render_cost(channel, outline, scenes_per_chapter_override=scenes_per_chapter)
        total = float(cost.get("all_in_usd") or 0.0)
        options.append({
            "profile": label,
            "image_model_id": model,
            "scenes_per_chapter": scenes_per_chapter,
            "scene_count": int(cost.get("n_scenes") or 0),
            "quality_score": score,
            "projected_provider_cost_usd": round(total, 2),
            "breakdown": dict(cost.get("breakdown") or {}),
            "within_budget": max_provider_cost_usd is None or total <= float(max_provider_cost_usd),
            **_margin_fields(total, selling_price_usd, target_margin),
        })
    selected_model = str(image_model_id or "").strip().lower()
    preferred = [row for row in options if row["image_model_id"] == selected_model and row["within_budget"]]
    eligible = [row for row in options if row["within_budget"]]
    recommended = max(preferred or eligible or options, key=lambda row: (row["quality_score"], -row["projected_provider_cost_usd"]))
    provider_cost = float(recommended["projected_provider_cost_usd"])
    result = {
        "format": "longform",
        "target_duration_sec": duration,
        "recommended_route": recommended,
        "quality_cost_options": options,
        "projected_full_provider_cost_usd": provider_cost,
        "profitability": _margin_fields(provider_cost, selling_price_usd, target_margin),
    }
    result.update(_margin_fields(provider_cost, selling_price_usd, target_margin))
    return result


def optimize_from_session(session: dict[str, Any]) -> dict[str, Any] | None:
    concept = session.get("pending_concept") if isinstance(session.get("pending_concept"), dict) else {}
    if not concept:
        return None
    fmt = str(concept.get("format") or session.get("content_format") or "shortform").lower()
    if "long" in fmt:
        return optimize_longform(
            target_duration_sec=int(concept.get("duration_sec") or 1200),
            image_model_id=str(session.get("image_model") or concept.get("image_model") or "grok_imagine_standard"),
        )
    return optimize_shortform(
        scene_count=int(concept.get("scene_count") or 6),
        duration_seconds=float(concept.get("duration_sec") or 30),
        image_model_id=str(session.get("image_model") or concept.get("image_model") or "grok_imagine"),
        video_model=str(session.get("video_model") or concept.get("video_model") or "grok_imagine_video"),
        animate=True,
    )
