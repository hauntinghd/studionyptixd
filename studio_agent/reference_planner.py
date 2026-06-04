"""Turn yt-dlp reference analysis into scene blueprints for Seedream edit + i2v."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from studio_agent import competitor
from studio_agent.production_standards import AUDIO_MIX_DEFAULTS, PACING_BENCHMARKS

WORK_ROOT = competitor.WORK_ROOT


def _load_job_result(job_id: str) -> dict[str, Any]:
    work = (WORK_ROOT / job_id).resolve()
    status = competitor.read_status(job_id)
    if status.get("status") != "complete":
        return {"error": "analysis not complete", "status": status}
    blueprint_path = work / "scene_blueprint.json"
    if blueprint_path.is_file():
        try:
            return json.loads(blueprint_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    result_path = work / "analysis_result.json"
    if result_path.is_file():
        try:
            return json.loads(result_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    meta = status.get("metadata") or {}
    frames = status.get("frames", {}).get("paths") if isinstance(status.get("frames"), dict) else []
    pacing = status.get("pacing") or {}
    return {
        "job_id": job_id,
        "metadata": meta,
        "frames": {"paths": frames or []},
        "pacing": pacing,
        "audio": status.get("audio") or {},
        "workspace": str(work),
    }


def build_scene_blueprint(
    job_id: str,
    *,
    topic: str,
    channel_style: str = "premium_doc",
    characters_per_scene: int = 1,
    visual_brief: str = "",
    target_scene_count: int | None = None,
) -> dict[str, Any]:
    """
    Map reference keyframes + pacing into per-scene production rows.

    characters_per_scene: 1–5 depending on channel (skeleton host = 1; cast channels up to 5).
    """
    raw = _load_job_result(job_id)
    if raw.get("error"):
        return raw

    meta = raw.get("metadata") or {}
    pacing = raw.get("pacing") or {}
    frame_paths = list((raw.get("frames") or {}).get("paths") or [])
    duration = float(meta.get("duration_sec") or pacing.get("duration_sec") or 0)
    avg_shot = float(pacing.get("avg_shot_sec") or 0)
    if not avg_shot and duration > 0 and frame_paths:
        avg_shot = duration / max(len(frame_paths), 1)

    bench = PACING_BENCHMARKS.get(channel_style, PACING_BENCHMARKS["premium_doc"])
    pace_label = "fast"
    if avg_shot >= 8:
        pace_label = "slow_doc"
    elif avg_shot >= 4:
        pace_label = "medium"
    elif avg_shot > 0:
        pace_label = "fast"

    n_chars = max(1, min(5, int(characters_per_scene or 1)))
    n_scenes = target_scene_count or min(max(6, len(frame_paths)), 24)
    if frame_paths:
        n_scenes = min(n_scenes, len(frame_paths))

    cuts = pacing.get("cuts_sec") or []
    scenes: list[dict[str, Any]] = []
    for i in range(n_scenes):
        t_start = cuts[i] if i < len(cuts) else (i * (duration / n_scenes) if duration else i * 5.0)
        ref_frame = frame_paths[i] if i < len(frame_paths) else ""
        i2v = 5.0
        if avg_shot > 0:
            i2v = max(3.0, min(8.0, round(avg_shot * 0.85, 1)))
        if pace_label == "fast":
            i2v = min(i2v, 5.0)
        scenes.append({
            "scene_index": i,
            "time_sec": round(float(t_start), 2),
            "reference_frame": ref_frame,
            "characters_in_scene": n_chars,
            "story_beat": (
                "cold_open_hook" if i == 0 else
                "pattern_interrupt" if i % 4 == 2 else
                "escalation" if i == n_scenes - 2 else
                "develop"
            ),
            "visual_brief": visual_brief,
            "seedream_edit": {
                "rule": "Same canonical character(s); edit background, wardrobe, props, pose only.",
                "background": f"Cinematic environment matching topic: {topic[:120]}",
                "outfit_props": visual_brief or "Match reference composition; adapt wardrobe to topic",
            },
            "i2v": {
                "duration_sec": i2v,
                "motion": "Subtle premium motion — weight shift, environmental parallax, no chaos",
            },
            "bgm_cue": f"Bed shift at scene {i + 1} — {pace_label} pacing, documentary tension",
        })

    blueprint = {
        "job_id": job_id,
        "reference_title": meta.get("title"),
        "reference_channel": meta.get("channel"),
        "topic": topic,
        "channel_style": channel_style,
        "pacing_analysis": {
            "avg_shot_sec": round(avg_shot, 2) if avg_shot else None,
            "pace_label": pace_label,
            "benchmark": bench,
            "hook_window_sec": pacing.get("hook_window_sec", 8),
            "pattern_interrupt_every_sec": bench.get("pattern_interrupt_sec", 60),
        },
        "audio_mix": dict(AUDIO_MIX_DEFAULTS),
        "production_notes": {
            "hardest": ["script-writing (story + retention)", "packaging (title + thumbnail CTR)"],
            "delivery": "VO slightly above BGM; scene BGM must match emotional beat",
            "quality_bar": "Lume / MrBeast / Jake Tran / Magnates Media tier pacing",
        },
        "scenes": scenes,
        "next_steps": [
            "load_skill script-writing — draft narration beat-for-beat using scene story_beat labels",
            "load_skill thumbnail-design — 3 packaging variants before render",
            "start_shortform_generate OR start_longform_render with visual_brief + approved outline",
        ],
    }

    work = (WORK_ROOT / job_id).resolve()
    try:
        work.mkdir(parents=True, exist_ok=True)
        (work / "scene_blueprint.json").write_text(
            json.dumps(blueprint, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        pass
    return blueprint
