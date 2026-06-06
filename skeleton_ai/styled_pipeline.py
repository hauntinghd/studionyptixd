"""
Styled shortform orchestrator — Seedream T2I per beat (no skeleton master).

Used when Studio Agent render_style is anything except skeleton_host.
"""
from __future__ import annotations

import json
import re
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from studio_agent.render_styles import RenderStyle, get_render_style

from .compose import concat_demuxer, mux_narration, trim_with_captions
from .i2v_engine import ac_cost_for_video_model, generate as gen_clip, resolve_video_model_chain
from .pipeline import Beat, _write_progress, check_cancelled, split_script_into_beats
from .prompts.category_registry import get_category
from .scripting_grok import GrokClient, build_script_prompt
from .styled_stills import build_styled_scene_prompt, generate_still_t2i
from .voice_fal import FalVoiceClient


def _plan_system(style: RenderStyle) -> str:
    return (
        f"You are the visual planner for a short-form YouTube video rendered in "
        f"{style.label} style.\n\n"
        "Characters are REAL subjects for the scene (humans, historical figures, "
        "fictional characters, objects) — NOT a recurring skeleton mascot unless "
        "the script explicitly calls for one.\n\n"
        "Given a narration script, list recurring subjects or roles and lock ONE "
        "outfit/appearance description per named subject for continuity across beats.\n\n"
        "Output strict JSON:\n"
        "  {\n"
        '    "characters": { "<subject>": "<appearance + outfit, ~20-35 words>" },\n'
        '    "topic_setting": "<environment / era / lighting in one sentence>",\n'
        '    "fallback_outfit": "<default look when no named subject>"\n'
        "  }\n"
        "No markdown fences."
    )


def analyze_script_styled(
    grok: GrokClient,
    script_text: str,
    *,
    style: RenderStyle,
    category_label: str = "",
    topic: str | None = None,
    visual_brief: str | None = None,
) -> dict:
    user_lines = [
        f"Render style: {style.label}",
        f"Style notes: {style.prompt_prefix[:400]}",
    ]
    if category_label:
        user_lines.append(f"Category: {category_label}")
    if topic:
        user_lines.append(f"Topic: {topic}")
    if visual_brief:
        user_lines.append(f"USER VISUAL LOCK: {visual_brief}")
    user_lines.append("Script:")
    user_lines.append(script_text.strip())
    user_lines.append("\nReturn JSON now.")
    raw = grok.complete(_plan_system(style), "\n".join(user_lines), max_tokens=900, temperature=0.5)
    raw = raw.strip().strip("`").strip()
    if raw.lower().startswith("json"):
        raw = raw[4:].strip()
    try:
        plan = json.loads(raw)
    except json.JSONDecodeError:
        plan = {}
    if not isinstance(plan.get("characters"), dict):
        plan["characters"] = {}
    plan.setdefault("fallback_outfit", "period-appropriate clothing matching the topic")
    plan.setdefault("topic_setting", "")
    if visual_brief:
        plan["visual_brief_lock"] = visual_brief.strip()
    return plan


def derive_beat_visuals_styled(
    grok: GrokClient,
    narration: str,
    category_label: str,
    *,
    style: RenderStyle,
    plan: dict | None = None,
    visual_brief: str | None = None,
) -> tuple[str, str, str]:
    plan = plan or {"characters": {}, "fallback_outfit": "topic-appropriate wardrobe"}
    chars_json = json.dumps(plan.get("characters", {}), ensure_ascii=False)
    setting = plan.get("topic_setting", "")
    fallback = plan.get("fallback_outfit", "")
    vbl = (visual_brief or plan.get("visual_brief_lock") or "").strip()

    sys = (
        f"You compose ONE scene still prompt for a {style.label} short.\n"
        "Describe WHO is in frame, WHAT they wear, WHERE they are, and the POSE/action.\n"
        "Use real characters appropriate to the narration — historical figures as humans, "
        "comic characters in full costume, etc.\n"
        "Never describe an anatomical skeleton host unless narration demands it.\n"
        "Output JSON: {\"outfit\": \"...\", \"scene_action\": \"...\", \"motion_prompt\": \"...\"}"
    )
    user = (
        f"Style: {style.label}\n"
        f"Category: {category_label}\n"
        f"Narration beat: {narration}\n"
        f"Locked characters: {chars_json}\n"
        f"Setting: {setting}\n"
        f"Fallback look: {fallback}\n"
    )
    if vbl:
        user += f"Visual lock: {vbl}\n"

    raw = grok.complete(sys, user, max_tokens=500, temperature=0.55)
    raw = raw.strip().strip("`").strip()
    if raw.lower().startswith("json"):
        raw = raw[4:].strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = {}
    outfit = str(data.get("outfit") or fallback or "").strip()
    action = str(data.get("scene_action") or narration).strip()
    motion = str(data.get("motion_prompt") or action).strip()
    return outfit, action, motion


def _still_to_clip(
    still_path: Path,
    out_path: Path,
    *,
    duration_sec: float,
    fps: int = 30,
) -> Path:
    """Non-animated path: turn a still into a clip with a gentle Ken Burns push.

    Used when the user turns OFF motion (animate=False) — no i2v cost, but still
    a premium documentary feel instead of a dead static frame.
    """
    dur = max(1.0, float(duration_sec))
    frames = int(dur * fps)
    vf = (
        "scale=1080:1920:force_original_aspect_ratio=increase,"
        "crop=1080:1920,"
        f"zoompan=z='min(zoom+0.0009,1.12)':d={frames}:s=1080x1920:fps={fps}"
    )
    subprocess.run(
        [
            "ffmpeg", "-y", "-loop", "1", "-i", str(still_path),
            "-t", f"{dur:.3f}", "-vf", vf,
            "-c:v", "libx264", "-pix_fmt", "yuv420p", "-r", str(fps),
            str(out_path),
        ],
        check=True,
        capture_output=True,
    )
    return out_path


SCENES_FILE = "scenes.json"


def _scenes_path(workspace: Path) -> Path:
    return Path(workspace) / SCENES_FILE


def load_scenes(workspace: Path) -> list[dict[str, Any]]:
    p = _scenes_path(workspace)
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    return data if isinstance(data, list) else []


def save_scenes(workspace: Path, scenes: list[dict[str, Any]]) -> None:
    _scenes_path(workspace).write_text(
        json.dumps(scenes, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def _write_result(workspace: Path, payload: dict[str, Any]) -> None:
    (Path(workspace) / "result.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def _read_result(workspace: Path) -> dict[str, Any]:
    p = Path(workspace) / "result.json"
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _setup_dirs(workspace: Path) -> tuple[Path, Path, Path, Path]:
    workspace = Path(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    stills_dir = workspace / "stills"
    clips_dir = workspace / "clips"
    trimmed_dir = workspace / "trimmed"
    work_dir = workspace / "work"
    for d in (stills_dir, clips_dir, trimmed_dir, work_dir):
        d.mkdir(parents=True, exist_ok=True)
    return stills_dir, clips_dir, trimmed_dir, work_dir


def _style_for(workspace: Path, fallback: str = "cinematic") -> RenderStyle:
    key = str(_read_result(workspace).get("render_style") or fallback)
    return get_render_style(key)


# ─── Stage 1: plan + per-scene stills → scene-review gate ──────────────────────

def plan_scenes(
    category_key: str,
    topic: str | None,
    workspace: Path,
    *,
    render_style: str,
    tier: str = "standard",
    video_model: str | None = None,
    visual_brief: str | None = None,
    beats_target: int = 12,
    grok: GrokClient | None = None,
    script_override: str | None = None,
    user_id: str | None = None,
    default_animate: bool = True,
) -> dict[str, Any]:
    """Write the script, plan beats, render one Seedream still per scene, then
    stop at the awaiting_scene_review gate so the user can edit/animate per scene."""
    style = get_render_style(render_style)
    workspace = Path(workspace)
    stills_dir, _clips, _trim, _work = _setup_dirs(workspace)
    grok = grok or GrokClient()
    cat = get_category(category_key, user_id=user_id)

    _write_progress(workspace, stage="script", progress=8, detail="Writing script")
    existing_script = workspace / "script.txt"
    if script_override and script_override.strip():
        script_text = script_override.strip()
    elif existing_script.exists() and existing_script.read_text(encoding="utf-8").strip():
        script_text = existing_script.read_text(encoding="utf-8").strip()
    else:
        user_prompt = build_script_prompt(cat["system_prompt"], topic)
        script_text = grok.complete(cat["system_prompt"], user_prompt, max_tokens=1500)
    (workspace / "script.txt").write_text(script_text, encoding="utf-8")

    _write_progress(workspace, stage="scene_plan", progress=16, detail=f"Planning {style.label} scenes")
    plan = analyze_script_styled(
        grok, script_text, style=style, category_label=cat["label"],
        topic=topic, visual_brief=visual_brief,
    )
    (workspace / "scene_plan.json").write_text(
        json.dumps({**plan, "render_style": style.key}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    sentences = split_script_into_beats(script_text, target_count=beats_target)
    if not sentences:
        raise RuntimeError("Grok returned empty script")

    _, resolved_default_vm = resolve_video_model_chain(video_model=video_model, tier=tier)
    existing = {s.get("index"): s for s in load_scenes(workspace)}
    scenes: list[dict[str, Any]] = []
    total = max(len(sentences), 1)
    for i, narration in enumerate(sentences):
        check_cancelled(workspace)
        _write_progress(workspace, stage="stills", progress=20 + int(i / total * 60),
                        detail=f"Scene {i + 1}/{total} — {style.label} still")
        prev = existing.get(i, {})
        prompt = prev.get("prompt")
        outfit = prev.get("outfit")
        action = prev.get("scene_action")
        motion = prev.get("motion_prompt")
        if not prompt:
            outfit, action, motion = derive_beat_visuals_styled(
                grok, narration, cat["label"], style=style, plan=plan, visual_brief=visual_brief,
            )
            prompt = build_styled_scene_prompt(
                style_prefix=style.prompt_prefix, scene_action=action, outfit=outfit,
                topic=topic or cat["label"], visual_brief=visual_brief or "",
            )
        sid = f"b{i:02d}"
        still_target = stills_dir / f"{sid}.png"
        if not (still_target.exists() and still_target.stat().st_size > 0):
            generate_still_t2i(prompt, still_target, negative_prompt=style.negative_prompt, seed=420042 + i)
        scenes.append({
            "index": i, "sid": sid, "narration": narration, "prompt": prompt,
            "outfit": outfit, "scene_action": action, "motion_prompt": motion,
            "still_rel": f"stills/{sid}.png", "clip_rel": None,
            "animate": bool(prev.get("animate", default_animate)),
            "video_model": prev.get("video_model") or resolved_default_vm,
            "status": "still_ready", "duration_sec": float(prev.get("duration_sec", 5.0)),
        })
    save_scenes(workspace, scenes)

    _write_progress(workspace, stage="awaiting_scene_review", progress=80, detail="Review scenes")
    _write_result(workspace, {
        "status": "awaiting_scene_review", "job_id": workspace.name,
        "render_style": style.key, "render_style_label": style.label,
        "stills_model": f"seedream_v45_t2i_{style.key}",
        "category": category_key, "topic": topic, "tier": tier,
        "scene_count": len(scenes),
    })
    return {"status": "awaiting_scene_review", "scene_count": len(scenes), "job_id": workspace.name}


# ─── Per-scene edit / regenerate (Seedream v4.5 + v4.5 edit) ───────────────────

def regenerate_scene(workspace: Path, index: int, *, seed: int | None = None) -> dict[str, Any]:
    """Re-render one scene's still from its stored prompt with a fresh seed."""
    workspace = Path(workspace)
    stills_dir, _c, _t, _w = _setup_dirs(workspace)
    scenes = load_scenes(workspace)
    sc = next((s for s in scenes if s.get("index") == index), None)
    if not sc:
        raise RuntimeError(f"scene {index} not found")
    style = _style_for(workspace)
    still_target = stills_dir / f"{sc['sid']}.png"
    still_target.unlink(missing_ok=True)
    generate_still_t2i(
        sc["prompt"], still_target, negative_prompt=style.negative_prompt,
        seed=int(seed if seed is not None else (990000 + index)),
    )
    # New still invalidates any existing animation for this scene.
    (workspace / "clips" / f"{sc['sid']}.mp4").unlink(missing_ok=True)
    sc["clip_rel"] = None
    sc["status"] = "still_ready"
    save_scenes(workspace, scenes)
    return {"index": index, "still_rel": sc["still_rel"], "status": "still_ready"}


def edit_scene(workspace: Path, index: int, instruction: str) -> dict[str, Any]:
    """Natural-language edit of one scene's still via Seedream v4.5 edit.

    Uploads the current still as the reference image and applies the user's
    change ("make the lighting darker", "put him in ancient Rome", ...).
    """
    import fal_client
    from .canonical_edit import generate_still_edit

    workspace = Path(workspace)
    stills_dir, _c, _t, _w = _setup_dirs(workspace)
    scenes = load_scenes(workspace)
    sc = next((s for s in scenes if s.get("index") == index), None)
    if not sc:
        raise RuntimeError(f"scene {index} not found")
    still_target = stills_dir / f"{sc['sid']}.png"
    if not still_target.exists():
        raise RuntimeError(f"scene {index} has no still to edit")

    current_url = fal_client.upload_file(str(still_target))
    edit_prompt = (
        f"{sc.get('prompt', '')}\n\nApply this change: {instruction.strip()}"
    ).strip()[:3500]
    out_tmp = stills_dir / f"{sc['sid']}_edit.png"
    out_tmp.unlink(missing_ok=True)
    generate_still_edit(edit_prompt, out_tmp, master_url=current_url)
    # Promote the edit to the canonical still; drop stale animation.
    still_target.unlink(missing_ok=True)
    out_tmp.rename(still_target)
    (workspace / "clips" / f"{sc['sid']}.mp4").unlink(missing_ok=True)
    sc["clip_rel"] = None
    sc["status"] = "still_ready"
    sc["last_edit"] = instruction.strip()[:300]
    save_scenes(workspace, scenes)
    return {"index": index, "still_rel": sc["still_rel"], "status": "still_ready"}


def set_scene_settings(
    workspace: Path, index: int, *, animate: bool | None = None, video_model: str | None = None,
) -> dict[str, Any]:
    scenes = load_scenes(workspace)
    sc = next((s for s in scenes if s.get("index") == index), None)
    if not sc:
        raise RuntimeError(f"scene {index} not found")
    if animate is not None:
        sc["animate"] = bool(animate)
    if video_model:
        sc["video_model"] = video_model
    save_scenes(Path(workspace), scenes)
    return sc


# ─── Stage 2: animate selected scenes (i2v) ───────────────────────────────────

def animate_scenes_stage(
    workspace: Path, *, indices: list[int] | None = None, tier: str = "standard",
) -> dict[str, Any]:
    """Animate the selected scenes (or all with animate=True) via i2v."""
    workspace = Path(workspace)
    _stills, clips_dir, _t, _w = _setup_dirs(workspace)
    scenes = load_scenes(workspace)
    if not scenes:
        raise RuntimeError("no scenes planned")
    if indices is None:
        targets = [s for s in scenes if s.get("animate")]
    else:
        idx_set = set(indices)
        targets = [s for s in scenes if s.get("index") in idx_set]
    total = max(len(targets), 1)
    for n, sc in enumerate(targets):
        check_cancelled(workspace)
        _write_progress(workspace, stage="animate", progress=10 + int(n / total * 80),
                        detail=f"Animating scene {sc['index'] + 1}/{len(scenes)}")
        sc["animate"] = True
        still = workspace / sc["still_rel"]
        clip = clips_dir / f"{sc['sid']}.mp4"
        clip.unlink(missing_ok=True)  # re-animate fresh
        try:
            gen_clip(
                still, sc.get("motion_prompt") or sc["narration"], clip,
                tier=tier, video_model=sc.get("video_model"),
                duration_sec=int(sc.get("duration_sec", 5)),
            )
            sc["clip_rel"] = f"clips/{sc['sid']}.mp4"
            sc["status"] = "clip_ready"
        except Exception as exc:  # noqa: BLE001 — surface per-scene, keep others
            sc["status"] = "error"
            sc["error"] = str(exc)[:300]
    save_scenes(workspace, scenes)
    _write_progress(workspace, stage="awaiting_scene_review", progress=80, detail="Review animation")
    return {"status": "animated", "animated": [s["index"] for s in targets]}


# ─── Stage 3: finalize → compose final MP4 ────────────────────────────────────

def finalize_stage(
    workspace: Path, *, tier: str = "standard", voice_id: str | None = None, el: Any = None,
    reedit_instruction: str | None = None,
) -> dict[str, Any]:
    """Ensure every scene has a clip (Ken Burns for non-animated), then VO + compose.
    If reedit_instruction is provided (from Reply & re-edit or re_edit_production tool), we apply
    the intent: force subscribe CTA on the final beat, keep strict 3-word caption rhythm, etc.
    The visuals (stills/clips) are re-used from the existing job — this is the surgical re-edit path.
    """
    workspace = Path(workspace)
    _stills, clips_dir, trimmed_dir, work_dir = _setup_dirs(workspace)
    el = el or FalVoiceClient()
    scenes = sorted(load_scenes(workspace), key=lambda s: s.get("index", 0))
    if not scenes:
        raise RuntimeError("no scenes to finalize")
    script_text = (workspace / "script.txt").read_text(encoding="utf-8").strip()

    # Apply re-edit instruction effects (surgical, no new generation of stills)
    reedit = (reedit_instruction or "").lower()
    wants_cta = "cta" in reedit or "subscribe" in reedit or "subscribers" in reedit or "packag" in reedit
    if wants_cta and scenes:
        last = scenes[-1]
        narr = last.get("narration", "") or ""
        if "subscribe" not in narr.lower() and "hit the sub" not in narr.lower():
            # Append a clean subscribe CTA to the final narration for packaging
            last["narration"] = (narr.rstrip() + " If you enjoyed this, subscribe for more.").strip()
            save_scenes(workspace, scenes)

    trimmed_paths: list[Path] = []
    narration_audios: list[Path] = []
    total = max(len(scenes), 1)
    for n, sc in enumerate(scenes):
        check_cancelled(workspace)
        _write_progress(workspace, stage="compose", progress=10 + int(n / total * 60),
                        detail=f"Composing scene {sc['index'] + 1}/{total}")
        still = workspace / sc["still_rel"]
        clip = clips_dir / f"{sc['sid']}.mp4"
        if not (clip.exists() and clip.stat().st_size > 0):
            _still_to_clip(still, clip, duration_sec=float(sc.get("duration_sec", 5.0)))
            sc["clip_rel"] = f"clips/{sc['sid']}.mp4"
        # Per-scene narration audio for PERFECT sync (visual + 3-word captions timed to voice chunk)
        na_path = work_dir / f"nar_{sc['sid']}.mp3"
        na = el.synthesize(text=sc["narration"], out_path=na_path, voice_id=voice_id)
        na_dur = probe_duration(na) or float(sc.get("duration_sec", 5.0))
        trimmed = trim_with_captions(
            clip, trimmed_dir / f"{sc['sid']}.mp4",
            duration_sec=na_dur, narration_text=sc["narration"],
            watermark_text="ZeroTier",  # dynamic per channel
        )
        trimmed_paths.append(trimmed)
        narration_audios.append(na)
    save_scenes(workspace, scenes)

    check_cancelled(workspace)
    _write_progress(workspace, stage="narration", progress=78, detail="Voiceover")
    # Concat per-scene narration audios for the final VO track (ensures exact sync with visuals)
    narration_target = workspace / "narration.mp3"
    if narration_audios:
        concat_list = work_dir / "nar_concat.txt"
        with open(concat_list, "w") as f:
            for na in narration_audios:
                f.write(f"file '{na.as_posix()}'\n")
        subprocess.run([
            "ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", str(concat_list),
            "-c", "copy", str(narration_target)
        ], check=True, capture_output=True)
        narration_audio = narration_target
    else:
        narration_audio = el.synthesize(text=script_text, out_path=narration_target, voice_id=voice_id)

    _write_progress(workspace, stage="compose", progress=92, detail="Muxing final MP4")
    silent = concat_demuxer(trimmed_paths, workspace / "silent.mp4", work_dir)
    final = mux_narration(silent, narration_audio, workspace / "styled_short.mp4")

    animated = [s["index"] for s in scenes if s.get("animate")]
    meta = _read_result(workspace)
    ac_cost = sum(
        ac_cost_for_video_model(video_model=s.get("video_model"), tier=tier)
        for s in scenes if s.get("animate")
    )
    result = {
        **{k: meta.get(k) for k in ("render_style", "render_style_label", "stills_model", "category", "topic")},
        "status": "complete", "job_id": workspace.name,
        "video_path": str(final), "script_path": str(workspace / "script.txt"),
        "narration_path": str(narration_audio),
        "scene_count": len(scenes), "animated_scenes": animated,
        "tier": tier, "ac_charged": ac_cost,
    }
    _write_result(workspace, result)

    # Auto package.txt for upload (title, tags, desc) — user request for shorts
    # Includes subscribe CTA
    pkg = f"""Title: {topic or 'Untitled Short'}

Tags: {category_key}, {render_style}, short, nyptid, zero tier

Description:
{topic or 'Watch the full story.'}

Subscribe for more.

"""
    (workspace / "package.txt").write_text(pkg, encoding="utf-8")
    return result


def run_styled(
    category_key: str,
    topic: str | None,
    workspace: Path,
    *,
    render_style: str,
    tier: str = "standard",
    video_model: str | None = None,
    visual_brief: str | None = None,
    beats_target: int = 12,
    grok: GrokClient | None = None,
    el: Any = None,
    voice_id: str | None = None,
    script_override: str | None = None,
    user_id: str | None = None,
    animate: bool = True,
) -> dict:
    """Straight-through render (no review gate) — fallback / auto path."""
    workspace = Path(workspace)
    plan_scenes(
        category_key, topic, workspace, render_style=render_style, tier=tier,
        video_model=video_model, visual_brief=visual_brief, beats_target=beats_target,
        grok=grok, script_override=script_override, user_id=user_id, default_animate=animate,
    )
    if animate:
        animate_scenes_stage(workspace, indices=None, tier=tier)
    return finalize_stage(workspace, tier=tier, voice_id=voice_id, el=el)
