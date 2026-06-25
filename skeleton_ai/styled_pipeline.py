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

from .compose import concat_demuxer, mux_narration, probe_duration, trim_with_captions
from .i2v_engine import ac_cost_for_video_model, generate as gen_clip, resolve_video_model_chain
from .pipeline import Beat, _write_progress, apply_wardrobe_motion_lock, check_cancelled, split_script_into_beats
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
    reference_images: list[str] | None = None,
) -> dict[str, Any]:
    """Write the script, plan beats, render one Seedream still per scene, then
    stop at the awaiting_scene_review gate so the user can edit/animate per scene."""
    style = get_render_style(render_style)
    is_skeleton = style.pipeline == "skeleton_host"
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
    if is_skeleton:
        from .pipeline import analyze_script

        plan = analyze_script(
            grok,
            script_text,
            category_label=cat["label"],
            topic=topic,
            visual_brief=visual_brief,
        )
    else:
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
            if is_skeleton:
                from .canonical_edit import build_scene_edit_prompt
                from .pipeline import derive_beat_visuals

                outfit, action, motion = derive_beat_visuals(
                    grok,
                    narration,
                    cat["label"],
                    plan=plan,
                    visual_brief=visual_brief,
                    beat_index=i,
                )
                prompt = build_scene_edit_prompt(
                    topic=topic or cat["label"],
                    visual_description=action,
                    outfit=outfit,
                )
            else:
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
            if is_skeleton:
                from .canonical_edit import generate_still_edit

                generate_still_edit(prompt, still_target, seed=420042 + i)
            else:
                if reference_images:
                    from .styled_stills import generate_still_reference_edit

                    generate_still_reference_edit(
                        prompt,
                        still_target,
                        reference_paths=reference_images,
                        negative_prompt=style.negative_prompt,
                        seed=420042 + i,
                    )
                else:
                    generate_still_t2i(
                        prompt,
                        still_target,
                        negative_prompt=style.negative_prompt,
                        seed=420042 + i,
                    )
        scenes.append({
            "index": i, "sid": sid, "narration": narration, "prompt": prompt,
            "outfit": outfit, "scene_action": action,
            "motion_prompt": apply_wardrobe_motion_lock(motion, outfit) if is_skeleton else motion,
            "still_rel": f"stills/{sid}.png", "clip_rel": None,
            "animate": bool(prev.get("animate", default_animate)),
            "approved_for_video": bool(prev.get("approved_for_video", False)),
            "approved_for_animation": bool(prev.get("approved_for_animation", False)),
            "video_model": prev.get("video_model") or resolved_default_vm,
            "status": "still_ready", "duration_sec": float(prev.get("duration_sec", 5.0)),
        })
    save_scenes(workspace, scenes)

    _write_progress(workspace, stage="awaiting_scene_review", progress=80, detail="Review scenes")
    _write_result(workspace, {
        "status": "awaiting_scene_review", "job_id": workspace.name,
        "render_style": style.key, "render_style_label": style.label,
        "stills_model": "seedream_v45_edit_canonical" if is_skeleton else f"seedream_v45_t2i_{style.key}",
        "category": category_key, "topic": topic, "tier": tier,
        "scene_count": len(scenes),
        "product_reference_count": len(reference_images or []),
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
    if style.pipeline == "skeleton_host":
        from .canonical_edit import generate_still_edit

        generate_still_edit(
            sc["prompt"],
            still_target,
            seed=int(seed if seed is not None else (990000 + index)),
        )
    else:
        generate_still_t2i(
            sc["prompt"], still_target, negative_prompt=style.negative_prompt,
            seed=int(seed if seed is not None else (990000 + index)),
        )
    # New still invalidates any existing animation for this scene.
    (workspace / "clips" / f"{sc['sid']}.mp4").unlink(missing_ok=True)
    sc["clip_rel"] = None
    sc["status"] = "still_ready"
    sc["approved_for_video"] = False
    sc["approved_for_animation"] = False
    save_scenes(workspace, scenes)
    return {"index": index, "still_rel": sc["still_rel"], "status": "still_ready"}


def _scoped_edit_prompt(scene_prompt: str, instruction: str, scope: str) -> tuple[str, str]:
    raw_scope = (scope or "full").strip().lower().replace("-", "_")
    aliases = {
        "subject": "character",
        "person": "character",
        "wardrobe": "character",
        "environment": "background",
        "setting": "background",
        "prop": "props",
    }
    normalized = aliases.get(raw_scope, raw_scope)
    if normalized not in {"character", "background", "props", "full"}:
        normalized = "full"

    guardrails = {
        "character": (
            "Edit only the foreground character or subject. Preserve the current background, "
            "camera angle, lens, lighting direction, composition, and scene continuity unless "
            "the instruction explicitly says otherwise. Keep identity consistent while changing "
            "wardrobe, pose, expression, body color, or held items requested by the instruction."
        ),
        "background": (
            "Edit only the background/environment. Preserve the foreground character identity, "
            "face/head shape, body shape, clothing, pose, props, camera framing, and lighting on "
            "the subject. Do not redesign the character."
        ),
        "props": (
            "Edit only the requested object, prop, screen content, or held item. Preserve the "
            "character identity, clothing, pose, background, camera angle, and lighting."
        ),
        "full": (
            "Apply the requested change while preserving as much scene continuity, character "
            "identity, camera framing, and visual style as possible."
        ),
    }[normalized]
    compact_scene = " ".join(str(scene_prompt or "").split())[:1600]
    prompt = (
        f"REQUESTED CHANGE — EXECUTE THIS FIRST:\n{instruction.strip()}\n\n"
        f"Edit scope: {normalized}.\n"
        f"Continuity rules: {guardrails}\n\n"
        f"Original scene intent for context only:\n{compact_scene}"
    ).strip()[:3500]
    return prompt, normalized


def edit_scene(workspace: Path, index: int, instruction: str, scope: str = "full") -> dict[str, Any]:
    """Natural-language edit of one scene's still via Seedream v4.5 edit.

    Uploads the current still as the reference image and applies the user's
    change ("make the lighting darker", "put him in ancient Rome", ...).
    The scope controls preservation for premium character/background passes.
    """
    import fal_client
    from .canonical_edit import _ensure_fal, generate_still_edit

    workspace = Path(workspace)
    stills_dir, _c, _t, _w = _setup_dirs(workspace)
    scenes = load_scenes(workspace)
    sc = next((s for s in scenes if s.get("index") == index), None)
    if not sc:
        raise RuntimeError(f"scene {index} not found")
    still_target = stills_dir / f"{sc['sid']}.png"
    if not still_target.exists():
        raise RuntimeError(f"scene {index} has no still to edit")

    _ensure_fal()
    current_url = fal_client.upload_file(str(still_target))
    edit_prompt, normalized_scope = _scoped_edit_prompt(
        str(sc.get("prompt") or ""),
        instruction,
        scope,
    )
    out_tmp = stills_dir / f"{sc['sid']}_edit.png"
    out_tmp.unlink(missing_ok=True)
    extra_refs: list[str] | None = None
    if _style_for(workspace).pipeline == "skeleton_host":
        public_dir = Path(__file__).resolve().parents[1] / "ViralShorts-App" / "public"
        canonical = next(
            (
                path
                for path in (
                    public_dir / "canonical-skeleton-master-hires.png",
                    public_dir / "canonical-skeleton-master.png",
                )
                if path.is_file()
            ),
            None,
        )
        if canonical is not None:
            extra_refs = [str(canonical)]
    generate_still_edit(
        edit_prompt,
        out_tmp,
        master_url=current_url,
        extra_refs=extra_refs,
    )
    # Promote the edit to the canonical still; drop stale animation.
    still_target.unlink(missing_ok=True)
    out_tmp.rename(still_target)
    (workspace / "clips" / f"{sc['sid']}.mp4").unlink(missing_ok=True)
    sc["clip_rel"] = None
    sc["status"] = "still_ready"
    sc["approved_for_video"] = False
    sc["approved_for_animation"] = False
    sc["last_edit"] = instruction.strip()[:300]
    sc["last_edit_scope"] = normalized_scope
    save_scenes(workspace, scenes)
    return {
        "index": index,
        "still_rel": sc["still_rel"],
        "status": "still_ready",
        "last_edit_scope": normalized_scope,
    }


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
        targets = [s for s in scenes if s.get("animate") and s.get("approved_for_video")]
    else:
        idx_set = set(indices)
        targets = [
            s for s in scenes
            if s.get("index") in idx_set and s.get("approved_for_video")
        ]
    if not targets:
        raise RuntimeError(
            "no approved scenes to animate. Review the stills first, then approve scenes with set_production_scenes_animate before running i2v."
        )
    total = max(len(targets), 1)
    animated: list[int] = []
    failed: list[int] = []
    for n, sc in enumerate(targets):
        check_cancelled(workspace)
        _write_progress(workspace, stage="animate", progress=10 + int(n / total * 80),
                        detail=f"Animating scene {sc['index'] + 1}/{len(scenes)}")
        sc["animate"] = True
        still = workspace / sc["still_rel"]
        clip = clips_dir / f"{sc['sid']}.mp4"
        clip.unlink(missing_ok=True)  # re-animate fresh
        try:
            motion_prompt = sc.get("motion_prompt") or sc["narration"]
            skeleton_scene_text = " ".join(
                str(sc.get(key) or "") for key in ("render_style", "outfit", "prompt", "scene_action")
            ).lower()
            if "skeleton_host" in skeleton_scene_text or "skeleton" in skeleton_scene_text:
                motion_prompt = apply_wardrobe_motion_lock(motion_prompt, sc.get("outfit"))
                sc["motion_prompt"] = motion_prompt
            gen_clip(
                still, motion_prompt, clip,
                tier=tier, video_model=sc.get("video_model"),
                duration_sec=int(sc.get("duration_sec", 5)),
            )
            sc["clip_rel"] = f"clips/{sc['sid']}.mp4"
            sc["status"] = "clip_ready"
            sc.pop("error", None)
            animated.append(int(sc["index"]))
        except Exception as exc:  # noqa: BLE001 — surface per-scene, keep others
            sc["status"] = "error"
            sc["error"] = str(exc)[:300]
            failed.append(int(sc["index"]))
        save_scenes(workspace, scenes)
    save_scenes(workspace, scenes)
    if failed:
        _write_progress(
            workspace,
            stage="awaiting_scene_review",
            progress=80,
            detail=f"Animation needs review: {len(failed)} scene(s) failed",
        )
        return {"status": "partial", "animated": animated, "failed": failed}
    _write_progress(workspace, stage="awaiting_scene_review", progress=80, detail="Review animation")
    return {"status": "animated", "animated": animated, "failed": []}


# ─── Stage 3: finalize → compose final MP4 ────────────────────────────────────

def finalize_stage(
    workspace: Path, *, tier: str = "standard", voice_id: str | None = None, el: Any = None,
    reedit_instruction: str | None = None,
    watermark_text: str = "Studio",
    captions_enabled: bool = True,
    caption_mode: str = "word",
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
    unapproved = [int(s.get("index", -1)) for s in scenes if not s.get("approved_for_video")]
    if unapproved:
        raise RuntimeError(
            "cannot finalize before still approval. Unapproved scenes: "
            + ", ".join(str(i + 1) for i in unapproved if i >= 0)
        )
    script_text = (workspace / "script.txt").read_text(encoding="utf-8").strip()

    def _normalize_tts_text(text: str) -> str:
        clean = str(text or "")
        # Common LLM/script artifacts that make TTS pronounce numbers as two
        # separate ages ("two eight-year-old") instead of a single compound age.
        clean = re.sub(r"\b2\s+8\s*-\s*year\s*-\s*old\b", "28-year-old", clean, flags=re.I)
        clean = re.sub(r"\b2\s+8\s+year\s+old\b", "28-year-old", clean, flags=re.I)
        clean = re.sub(r"\b(\d{2})\s+year\s+old\b", r"\1-year-old", clean, flags=re.I)
        clean = re.sub(r"\bSarah\s+Chene\b", "Sarah Chen", clean, flags=re.I)
        return clean

    # Apply re-edit instruction effects (surgical, no new generation of stills)
    reedit = (reedit_instruction or "").lower()
    wants_cta = "cta" in reedit or "subscribe" in reedit or "subscribers" in reedit or "packag" in reedit
    wants_word_captions = any(
        marker in reedit
        for marker in (
            "one word",
            "single word",
            "word-by-word",
            "word by word",
            "each word",
            "every single word",
            "single caption for every",
            "separate caption",
        )
    ) or str(caption_mode or "").lower() in {"word", "single_word", "one_word"}
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
        sc["narration"] = _normalize_tts_text(sc["narration"])
        na_path = work_dir / f"nar_{sc['sid']}.mp3"
        na = el.synthesize(text=sc["narration"], out_path=na_path, voice_id=voice_id)
        na_dur = probe_duration(na) or float(sc.get("duration_sec", 5.0))
        trimmed = trim_with_captions(
            clip, trimmed_dir / f"{sc['sid']}.mp4",
            duration_sec=na_dur, narration_text=sc["narration"],
            watermark_text=watermark_text,
            caption_mode="word" if wants_word_captions else "phrase",
            captions_enabled=captions_enabled,
            force=bool(reedit_instruction),
        )
        trimmed_paths.append(trimmed)
        narration_audios.append(na)
    save_scenes(workspace, scenes)

    check_cancelled(workspace)
    _write_progress(workspace, stage="narration", progress=78, detail="Voiceover")
    # Concat per-scene narration audios for the final VO track (ensures exact sync with visuals)
    narration_target = workspace / "narration.mp3"
    if narration_audios:
        cmd = ["ffmpeg", "-y", "-loglevel", "error"]
        for na in narration_audios:
            cmd.extend(["-i", str(na)])
        concat_inputs = "".join(f"[{i}:a:0]" for i in range(len(narration_audios)))
        cmd.extend([
            "-filter_complex",
            f"{concat_inputs}concat=n={len(narration_audios)}:v=0:a=1[aout]",
            "-map", "[aout]",
            "-c:a", "libmp3lame",
            "-q:a", "2",
            str(narration_target),
        ])
        subprocess.run(cmd, check=True, capture_output=True)
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

    # Auto package.txt for upload. Shorts get title/tags/description/timestamps,
    # but no generated thumbnail by default; YouTube Shorts use the frame/cover flow.
    topic = str(meta.get("topic") or "Untitled Short").strip()
    category_key = str(meta.get("category") or "short").strip()
    render_style = str(meta.get("render_style") or "cinematic").strip()
    title = topic or "Untitled Short"
    safe_topic_tag = re.sub(r"[^a-z0-9]+", "", title.lower())[:32] or "shorts"
    brand_tag = re.sub(r"[^a-z0-9]+", "", str(watermark_text or "").lower())[:32]
    tags = [
        safe_topic_tag,
        category_key,
        render_style,
        "shorts",
        "youtube shorts",
        "ai video",
        "nyptid studio",
    ]
    if brand_tag:
        tags.append(brand_tag)
    if category_key not in tags:
        tags.append(category_key)

    timestamps: list[str] = []
    cursor = 0.0
    for sc in scenes:
        mm = int(cursor // 60)
        ss = int(cursor % 60)
        scene_num = int(sc.get("index") or 0) + 1
        label = str(sc.get("narration") or sc.get("prompt") or f"Scene {scene_num}").strip()
        label = re.sub(r"\s+", " ", label)[:70].rstrip(" .,")
        timestamps.append(f"{mm:02d}:{ss:02d} - {label or f'Scene {scene_num}'}")
        cursor += float(sc.get("duration_sec") or 0) or 0

    brand_hashtag = f" #{brand_tag}" if brand_tag else ""
    pkg = f"""Title:
{title}

Alternate Titles:
1. {title}
2. {title} | Full Story in 60 Seconds
3. The Part Everyone Missed About {title[:70]}

Description:
{title}

Watch the full story unfold in a fast, tightly edited short. Subscribe to {watermark_text} for more.

Timestamps:
{chr(10).join(timestamps) if timestamps else "00:00 - Full short"}

Tags:
{", ".join(dict.fromkeys(t for t in tags if t))}

Hashtags:
#shorts #{safe_topic_tag} #nyptidstudio{brand_hashtag}

Thumbnail:
Not generated for short-form by default. Use the strongest frame/cover from the finished Short unless the user explicitly asks for a custom thumbnail.

CTA:
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
    watermark_text: str = "Studio",
    captions_enabled: bool = True,
    caption_mode: str = "word",
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
    return finalize_stage(
        workspace,
        tier=tier,
        voice_id=voice_id,
        el=el,
        watermark_text=watermark_text,
        captions_enabled=captions_enabled,
        caption_mode=caption_mode,
    )
