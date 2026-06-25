"""
Skeleton AI end-to-end orchestrator.

Inputs:
  - category_key (one of: human_limits, marvel_vs_dc, ancient_history, futuristic_socrates)
  - topic_text (optional — Grok picks if absent)
  - tier ("standard" → Seedance 2.0, "premium" → Kling 2.1 Pro)
  - voice_id (ElevenLabs voice id; defaults to Brian)
  - workspace_dir (where clips/stills/output land)

Pipeline steps:
  1. Grok writes the 60s script (~12 beats).
  2. Per beat: derive outfit + scene_action via small Grok call.
  3. Seedream v4.5 *edit* per beat from canonical-skeleton-master.png
     (same skeleton every scene; only background/props/wardrobe change).
  4. Seedance 2.0 (or Kling Pro) i2v on each still.
  5. ElevenLabs TTS narration of full script.
  6. ffmpeg compose: trim each clip to its beat duration with captions, concat, mux.

Output: a single .mp4 at workspace_dir/skeleton_short.mp4 + metadata files.
"""
from __future__ import annotations
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

from .scripting_grok import GrokClient, build_script_prompt
from .canonical_edit import build_scene_edit_prompt, generate_still_edit, sanitize_skeleton_outfit
from .i2v_engine import (
    ac_cost_for_video_model,
    generate as gen_clip,
    resolve_video_model_chain,
)
from .voice_fal import FalVoiceClient
from .compose import probe_duration, trim_with_captions, concat_demuxer, mux_narration
from .prompts.category_registry import get_category

CANCEL_FLAG = "CANCELLED"


class RenderCancelled(Exception):
    """Raised inside a render loop when the user requests cancellation."""


def check_cancelled(workspace: Path) -> None:
    """Stop the render at the next checkpoint if a CANCELLED flag is present."""
    if (Path(workspace) / CANCEL_FLAG).exists():
        raise RenderCancelled("Render cancelled by user")


@dataclass
class Beat:
    index: int
    narration: str
    outfit: str
    scene_action: str
    motion_prompt: str
    duration_sec: float


def _retime_beats_to_narration(beats: list[Beat], total_duration: float) -> None:
    """Set beat durations from the real voiceover length instead of fixed 5s blocks."""
    if not beats or total_duration <= 0:
        return
    weights = [max(1, len(re.findall(r"\w+", beat.narration or ""))) for beat in beats]
    total_weight = max(1, sum(weights))
    raw = [max(2.4, total_duration * (w / total_weight)) for w in weights]
    scale = total_duration / max(0.001, sum(raw))
    for beat, dur in zip(beats, raw):
        beat.duration_sec = max(2.0, dur * scale)


def split_script_into_beats(script_text: str, target_count: int = 12) -> list[str]:
    """Naive sentence split — Grok prompt told it to write one sentence per beat."""
    sentences = re.split(r"(?<=[.!?])\s+", script_text.strip())
    sentences = [s.strip() for s in sentences if s.strip()]
    if not sentences:
        return []
    return sentences[:target_count]


_PLAN_SYSTEM_PROMPT = (
    "You are the visual planner for NYPTID Studio Skeleton AI shorts.\n\n"
    "THE HOST IS LOCKED — one canonical 3D skeleton from the master reference: "
    "ivory-white anatomical bones and translucent glass body shell. The eyes already "
    "present in the reference remain unchanged. No skin, flesh, human limbs, or muscle tissue. "
    "Identity never changes. Scene-to-scene you may only "
    "change OUTFIT (worn over the shell), BACKGROUND, PROPS, and POSE.\n\n"
    "Given a full narration script and optional topic hint, list recurring "
    "comparison subjects or roles and lock ONE outfit description per named "
    "subject so every beat stays consistent. Be specific: colors, fabrics, "
    "footwear, accessories, era-appropriate gear when the script is historical.\n\n"
    "Output strict JSON:\n"
    "  {\n"
    '    "characters": { "<subject or role>": "<outfit worn on the same skeleton, ~20-35 words>" },\n'
    '    "topic_setting": "<one sentence: environment / location / lighting / era vibe>",\n'
    '    "fallback_outfit": "<default outfit when no named subject, ~15 words>"\n'
    "  }\n"
    "No markdown fences, no commentary outside the JSON."
)


WARDROBE_MOTION_LOCK = (
    "Wardrobe continuity lock: keep every garment solid and unchanged for the entire clip; "
    "white T-shirt or undershirt stays opaque under any open coat or jacket; "
    "black pants and shoes stay complete; no shirtless frames, bare chest, exposed sternum, "
    "exposed ribcage, transparent fabric, disappearing clothing, wardrobe popping, or outfit morphing."
)


def apply_wardrobe_motion_lock(motion: str, outfit: str | None = None) -> str:
    """Append a hard continuity guard to image-to-video prompts."""
    motion_text = str(motion or "").strip() or "Subtle idle motion, soft ambient movement"
    outfit_text = str(outfit or "").strip()
    if "wardrobe continuity lock" in motion_text.lower():
        return motion_text[:900]
    lock = WARDROBE_MOTION_LOCK
    if outfit_text:
        lock = f"{lock} Locked outfit remains unchanged: {outfit_text[:240]}."
    return f"{motion_text}. {lock}"[:900]


def _merge_visual_brief(outfit: str, visual_brief: str | None) -> str:
    vb = (visual_brief or "").strip()
    if not vb:
        return outfit
    base = (outfit or "").strip()
    if base and vb.lower() not in base.lower():
        return f"{base}. Session wardrobe lock: {vb}"
    return vb or base


def _visual_brief_requests_wardrobe(visual_brief: str | None) -> bool:
    low = str(visual_brief or "").lower()
    wardrobe_terms = (
        "wearing", "wears", "wardrobe", "outfit", "clothing", "clothes",
        "shirt", "hoodie", "jacket", "coat", "pants", "jeans", "shorts",
        "dress", "suit", "uniform", "armor", "cape", "hat", "helmet",
        "shoes", "sneakers", "boots",
    )
    return any(term in low for term in wardrobe_terms)


def _visual_brief_beat_direction(
    visual_brief: str | None,
    beat_index: int | None,
) -> str:
    """Extract a numbered Beat N directive from a user-authored visual brief."""
    if beat_index is None:
        return ""
    text = str(visual_brief or "").strip()
    if not text:
        return ""
    number = int(beat_index) + 1
    match = re.search(
        rf"\bBeat\s*{number}\s*:\s*(.+?)(?=\s+\bBeat\s*\d+\s*:|$)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    return re.sub(r"\s+", " ", match.group(1)).strip(" .;") if match else ""


def analyze_script(grok: GrokClient, script_text: str, *, category_label: str = "",
                   topic: str | None = None, visual_brief: str | None = None) -> dict:
    """
    Pre-pass: read the full script and lock a canonical character + style sheet.

    The returned plan is then passed to derive_beat_visuals so every beat
    that mentions the same character renders the SAME outfit. Without this
    pre-pass, Grok would re-invent Thor's cape color on every beat.
    """
    user_lines = []
    if category_label:
        user_lines.append(f"Category: {category_label}")
    if topic:
        user_lines.append(f"Topic hint: {topic}")
    if visual_brief:
        user_lines.append(
            f"USER VISUAL DIRECTION (environment, pose, props, overlays, and wardrobe only if explicitly named): {visual_brief}"
        )
    user_lines.append("Script:")
    user_lines.append(script_text.strip())
    user_lines.append("\nReturn the JSON plan now.")
    user = "\n".join(user_lines)

    raw = grok.complete(_PLAN_SYSTEM_PROMPT, user, max_tokens=900, temperature=0.5)
    raw = raw.strip().strip("`").strip()
    if raw.lower().startswith("json"):
        raw = raw[4:].strip()
    try:
        plan = json.loads(raw)
    except json.JSONDecodeError:
        plan = {}
    if not isinstance(plan.get("characters"), dict):
        plan["characters"] = {}
    plan.setdefault(
        "fallback_outfit",
        "neutral charcoal turtleneck and dark jeans, simple leather sneakers",
    )
    plan.setdefault("topic_setting", "")
    if visual_brief:
        plan["visual_brief_lock"] = visual_brief.strip()
    if not _visual_brief_requests_wardrobe(visual_brief):
        plan["fallback_outfit"] = (
            "no clothing; preserve the complete canonical transparent glass shell "
            "and ivory bone anatomy from the master reference"
        )
        plan["characters"]["skeleton_host"] = plan["fallback_outfit"]
    return plan


def derive_beat_visuals(
    grok: GrokClient,
    narration: str,
    category_label: str,
    *,
    plan: dict | None = None,
    visual_brief: str | None = None,
    beat_index: int | None = None,
) -> tuple[str, str, str]:
    """Per-beat visuals for canonical skeleton Seedream edit (background/outfit/props only)."""
    plan = plan or {"characters": {}, "fallback_outfit": "charcoal hoodie and dark joggers"}
    chars_json = json.dumps(plan.get("characters", {}), ensure_ascii=False)
    setting = plan.get("topic_setting", "")
    fallback = plan.get("fallback_outfit", "")
    vbl = (visual_brief or plan.get("visual_brief_lock") or "").strip()
    locked_scene = _visual_brief_beat_direction(vbl, beat_index)

    sys = (
        "You compose ONE per-scene visual prompt for a NYPTID Skeleton AI short.\n\n"
        "THE HOST IS LOCKED — the same canonical ivory skeleton with glass shell and "
        "the unchanged eyes from the master reference. Do NOT describe a different character, "
        "porcelain mannequin, or human actor. Only wardrobe, environment, props, and pose "
        "may change. Every exposed body part must remain ivory bone inside clear glass. "
        "Never output skin, flesh, muscles, human hands, human feet, or the phrase 'bare feet'.\n\n"
        "Output strict JSON:\n"
        "  outfit — clothing, armor, or visible muscle definition worn ON the same skeleton "
        "(from character sheet when named). Example: 'lean athletic muscle overlay on glass shell' — "
        "NOT a different body type.\n"
        "  scene_action — photoreal 9:16 environment + props + pose around the skeleton "
        "(gym, court, office, battlefield tableau, etc.). No text overlays.\n"
        "  motion_prompt — one subtle 5-second i2v motion (breath, weight shift, prop move)\n"
        "  bare_torso — always false\n"
        "No markdown."
    )
    user_lines = [
                  f"Mandatory visual direction: {vbl}" if vbl else "",
                  f"MANDATORY EXACT SCENE FOR THIS BEAT: {locked_scene}" if locked_scene else "",
                  f"Topic setting: {setting}" if setting else "",
                  f"Character sheet (JSON): {chars_json}",
                  f"Fallback outfit: {fallback}",
                  f"Category label: {category_label}",
                  f"Narration beat: {narration}",
                  "Return JSON now."]
    user = "\n".join(line for line in user_lines if line)

    raw = grok.complete(sys, user, max_tokens=500, temperature=0.6)
    raw = raw.strip().strip("`").strip()
    if raw.lower().startswith("json"):
        raw = raw[4:].strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return (
            fallback or "charcoal hoodie and dark joggers",
            "Canonical skeleton host in a cinematic photoreal environment matching the narration, sharp commercial lighting, vertical 9:16",
            "Subtle weight shift and ambient light flicker over five seconds",
        )
    outfit = sanitize_skeleton_outfit(
        data.get("outfit", fallback) or fallback or "charcoal hoodie and dark joggers"
    )
    if bool(data.get("bare_torso", False)):
        outfit = f"[BARE_TORSO] {outfit}"
    generated_action = data.get(
        "scene_action",
        "Canonical skeleton in a photoreal environment matching the narration, premium 9:16 framing",
    )
    action = (
        f"Canonical skeleton host; {locked_scene}. Do not add unrelated props or change this location."
        if locked_scene
        else generated_action
    )
    return (
        outfit,
        action,
        apply_wardrobe_motion_lock(
            data.get("motion_prompt", "Subtle idle motion, soft ambient movement"),
            outfit,
        ),
    )


def _write_progress(workspace: Path, *, stage: str, progress: int, detail: str = "") -> None:
    try:
        payload = {
            "stage": stage,
            "progress": max(0, min(100, int(progress))),
            "detail": str(detail or "")[:240],
        }
        (workspace / "progress.json").write_text(
            json.dumps(payload, indent=2),
            encoding="utf-8",
        )
        # Also bump heartbeat so status poller (and stale detector) sees liveness even between coarse stages.
        try:
            (workspace / "heartbeat.txt").touch(exist_ok=True)
        except Exception:
            pass
    except Exception:
        pass


def run(
    category_key: str,
    topic: str | None,
    workspace: Path,
    *,
    tier: str = "standard",
    video_model: str | None = None,
    visual_brief: str | None = None,
    beats_target: int = 12,
    grok: GrokClient | None = None,
    el: Any = None,
    voice_id: str | None = None,
    script_override: str | None = None,
    user_id: str | None = None,
    watermark_text: str = "Studio",
    captions_enabled: bool = True,
    caption_mode: str = "word",
) -> dict:
    """Run the full Skeleton AI pipeline. Returns a result dict."""
    workspace = Path(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    stills_dir = workspace / "stills"
    clips_dir = workspace / "clips"
    trimmed_dir = workspace / "trimmed"
    work_dir = workspace / "work"
    for d in (stills_dir, clips_dir, trimmed_dir, work_dir):
        d.mkdir(exist_ok=True)

    grok = grok or GrokClient()
    el = el or FalVoiceClient()
    _write_progress(workspace, stage="script", progress=8, detail="Writing script")

    # 1. Generate the script (or use user-edited override from Create panel).
    cat = get_category(category_key, user_id=user_id)
    if script_override and script_override.strip():
        script_text = script_override.strip()
    else:
        user_prompt = build_script_prompt(cat["system_prompt"], topic)
        script_text = grok.complete(cat["system_prompt"], user_prompt, max_tokens=1500)
    (workspace / "script.txt").write_text(script_text, encoding="utf-8")

    # 2. Pre-pass: analyze the full script to lock a canonical character +
    # style sheet. Every beat that mentions the same subject will reuse the
    # identical outfit description, guaranteeing visual continuity across
    # all 12 beats. Works for the 4 idea-list categories AND custom topics.
    _write_progress(workspace, stage="scene_plan", progress=18, detail="Locking character sheet")
    plan = analyze_script(
        grok,
        script_text,
        category_label=cat["label"],
        topic=topic,
        visual_brief=visual_brief,
    )
    (workspace / "scene_plan.json").write_text(
        json.dumps(plan, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # 3. Split into beats.
    sentences = split_script_into_beats(script_text, target_count=beats_target)
    if not sentences:
        raise RuntimeError("Grok returned empty script")

    # 4. For each beat, derive outfit/action/motion using the locked plan.
    beats: list[Beat] = []
    for i, narration in enumerate(sentences):
        outfit, action, motion = derive_beat_visuals(
            grok,
            narration,
            cat["label"],
            plan=plan,
            visual_brief=visual_brief,
            beat_index=i,
        )
        beats.append(Beat(
            index=i,
            narration=narration,
            outfit=outfit,
            scene_action=action,
            motion_prompt=motion,
            duration_sec=5.0,
        ))

    _write_progress(workspace, stage="narration", progress=23, detail="Voiceover timing")
    narration_audio = el.synthesize(
        text=script_text,
        out_path=workspace / "narration.mp3",
        voice_id=voice_id,
    )
    narration_duration = probe_duration(narration_audio)
    _retime_beats_to_narration(beats, narration_duration)

    # 4. Render stills + clips per beat (canonical master edit — identity locked).
    trimmed_paths: list[Path] = []
    roster_cache: dict[str, Path] = {}
    total_beats = max(len(beats), 1)
    for beat in beats:
        check_cancelled(workspace)
        pct = 25 + int((beat.index / total_beats) * 55)
        _write_progress(
            workspace,
            stage="render",
            progress=pct,
            detail=f"Beat {beat.index + 1}/{total_beats} — stills + motion",
        )
        sid = f"b{beat.index:02d}"
        outfit_key = (beat.outfit or "default").strip()[:120]
        extra_refs: list[str] = []
        if outfit_key and outfit_key != "default":
            roster_path = roster_cache.get(outfit_key)
            if not roster_path or not roster_path.exists():
                roster_path = stills_dir / f"roster_{beat.index:02d}_{outfit_key[:32].replace(' ', '_')}.png"
                roster_prompt = build_scene_edit_prompt(
                    topic=topic or cat["label"],
                    visual_description="Plain neutral studio backdrop, full body front view.",
                    outfit=beat.outfit,
                )
                generate_still_edit(roster_prompt, roster_path, seed=420100 + beat.index)
                roster_cache[outfit_key] = roster_path
            extra_refs = [str(roster_path)]

        edit_prompt = build_scene_edit_prompt(
            topic=topic or cat["label"],
            visual_description=beat.scene_action,
            outfit=beat.outfit,
        )
        still_result = generate_still_edit(
            edit_prompt,
            stills_dir / f"{sid}.png",
            extra_refs=extra_refs,
            seed=420042 + beat.index,
        )
        still_path = Path(
            still_result["local_path"]
            if isinstance(still_result, dict)
            else still_result
        )
        clip_path = gen_clip(
            still_path,
            beat.motion_prompt,
            clips_dir / f"{sid}.mp4",
            tier=tier,
            video_model=video_model,
            duration_sec=10 if float(beat.duration_sec or 0) > 5 else 5,
        )
        trimmed = trim_with_captions(
            clip_path,
            trimmed_dir / f"{sid}.mp4",
            duration_sec=beat.duration_sec,
            narration_text=beat.narration,
            watermark_text=watermark_text,
            caption_mode=caption_mode,
            captions_enabled=captions_enabled,
            force=True,
        )
        trimmed_paths.append(trimmed)

    _write_progress(workspace, stage="compose", progress=94, detail="Muxing final MP4")
    # 6. Concat + mux.
    silent = concat_demuxer(trimmed_paths, workspace / "silent.mp4", work_dir)
    final = mux_narration(silent, narration_audio, workspace / "skeleton_short.mp4")

    # 7. Cost / AC tracking.
    _, resolved_vm = resolve_video_model_chain(video_model=video_model, tier=tier)
    ac_cost = ac_cost_for_video_model(video_model=resolved_vm, tier=tier)
    result = {
        "video_path": str(final),
        "script_path": str(workspace / "script.txt"),
        "narration_path": str(narration_audio),
        "beats": [asdict(b) for b in beats],
        "tier": tier,
        "video_model": resolved_vm,
        "stills_model": "seedream_v45_edit_canonical",
        "ac_charged": ac_cost,
        "category": category_key,
        "topic": topic,
    }
    (workspace / "result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    safe_topic = str(topic or cat["label"] or "Skeleton Short").strip()
    safe_topic_tag = re.sub(r"[^a-z0-9]+", "", safe_topic.lower())[:32] or "shorts"
    brand_tag = re.sub(r"[^a-z0-9]+", "", str(watermark_text or "").lower())[:32]
    timestamps: list[str] = []
    cursor = 0.0
    for beat in beats:
        mm = int(cursor // 60)
        ss = int(cursor % 60)
        label = re.sub(r"\s+", " ", beat.narration).strip()[:70].rstrip(" .,")
        timestamps.append(f"{mm:02d}:{ss:02d} - {label or f'Beat {beat.index + 1}'}")
        cursor += float(beat.duration_sec or 0)
    tags = list(dict.fromkeys([
        safe_topic_tag,
        category_key,
        "shorts",
        "youtube shorts",
        "ai video",
        "nyptid studio",
        brand_tag,
    ]))
    brand_hashtag = f" #{brand_tag}" if brand_tag else ""
    pkg = f"""Title:
{safe_topic}

Alternate Titles:
1. {safe_topic}
2. {safe_topic} | Full Story in 60 Seconds
3. The Part Everyone Missed About {safe_topic[:70]}

Description:
{safe_topic}

Watch the full story unfold in a fast, tightly edited short. Subscribe to {watermark_text} for more.

Timestamps:
{chr(10).join(timestamps) if timestamps else "00:00 - Full short"}

Tags:
{", ".join(t for t in tags if t)}

Hashtags:
#shorts #{safe_topic_tag} #nyptidstudio{brand_hashtag}

Thumbnail:
Not generated for short-form by default. Use the strongest frame/cover from the finished Short unless the user explicitly asks for a custom thumbnail.

CTA:
Subscribe for more.

"""
    (workspace / "package.txt").write_text(pkg, encoding="utf-8")
    return result
