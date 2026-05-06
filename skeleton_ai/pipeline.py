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
  3. seedream v4.5 still per beat (mint BG, anatomical skull, real clothes).
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

from .scripting_grok import GrokClient, build_script_prompt
from .stills_seedream import generate as gen_still
from .i2v_engine import generate as gen_clip, AC_COST_STANDARD, AC_COST_PREMIUM
from .voice_elevenlabs import ElevenLabsClient
from .compose import probe_duration, trim_with_captions, concat_demuxer, mux_narration
from .prompts.base_style import assemble_scene_prompt, NEG_STILL
from .prompts.idea_lists import get_category


@dataclass
class Beat:
    index: int
    narration: str
    outfit: str
    scene_action: str
    motion_prompt: str
    duration_sec: float


def split_script_into_beats(script_text: str, target_count: int = 12) -> list[str]:
    """Naive sentence split — Grok prompt told it to write one sentence per beat."""
    sentences = re.split(r"(?<=[.!?])\s+", script_text.strip())
    sentences = [s.strip() for s in sentences if s.strip()]
    if not sentences:
        return []
    return sentences[:target_count]


_PLAN_SYSTEM_PROMPT = (
    "You are the visual planner for Cryptic Science / Skeleton AI YouTube Shorts. "
    "Every scene shows a stylized white anatomical SKELETON on a mint green backdrop. "
    "The skull and bones are FIXED — what changes scene to scene is the OUTFIT, "
    "PROPS, and POSE. Character identity must come 100% from costume + props.\n\n"
    "Given a full narration script and an optional topic hint, identify every "
    "named subject (real person, fictional character, profession, era role) and "
    "lock a SINGLE canonical outfit description for each. The same subject must "
    "look identical in every beat where they appear, so be specific: name colors, "
    "logos, signature props, era-correct details.\n\n"
    "Subject-rendering rules:\n"
    "  - Marvel/DC heroes: render their canonical costume. "
    "Thor → winged silver helmet, red flowing cape, gold-and-silver Asgardian "
    "breastplate with circular bosses, brown leather wrist bracers, Mjolnir hammer. "
    "Superman → blue spandex bodysuit with red-and-yellow S-shield, red flowing "
    "cape, red trunks over blue tights, yellow belt, red boots. "
    "Hulk → torn purple pants, bare bone torso with faint green energy aura "
    "(NEVER green skin — body stays white bone), exaggerated wide shoulders. "
    "Iron Man → red-and-gold full plate armor, glowing arc reactor on chest, "
    "helmet with triangular slit eyes. "
    "Batman → black cowl with pointed ears, gray bodysuit, yellow utility belt, "
    "flowing black cape, bat-symbol on chest. "
    "Spider-Man → red-and-blue webbed full suit with black spider on chest. "
    "Wonder Woman → red-and-gold bustier, blue star-spangled briefs, silver "
    "bracers, golden tiara, lasso of truth on hip. "
    "Wolverine → yellow-and-blue tight suit OR brown leather jacket and jeans, "
    "metal claws extended from knuckles, fur-shoulder cowl. "
    "  - Real people: workplace-correct attire (Tony Stark casual = goatee-ish "
    "facial markings on skull, dark band tee + blazer; Bruce Wayne formal = "
    "tailored black tuxedo, white pocket square). "
    "  - Era roles: period-correct details. "
    "Roman centurion → red tunic, lorica segmentata, plumed galea helmet, "
    "gladius sword, leather sandals. "
    "Egyptian pharaoh → white linen kilt, broad gold collar with lapis inlay, "
    "nemes headcloth striped blue and gold. "
    "Spartan hoplite → bronze muscle cuirass, crested helmet, large round "
    "shield with lambda, dory spear. "
    "Mars colonist 2050 → white sci-fi flight suit with insignia patches, "
    "transparent visor helmet, magnetic boots. "
    "WW2 fighter pilot → brown leather flight jacket, white silk scarf, "
    "leather flying cap with goggles, parachute harness. "
    "  - Generic / unknown subjects: pick concrete era + role + at least 4 "
    "specific clothing details. NEVER 'plain suit', NEVER 'casual clothes'.\n\n"
    "Output strict JSON:\n"
    "  {\n"
    '    "characters": { "<subject name>": "<full canonical outfit, ~25-40 words>" },\n'
    '    "topic_setting": "<one sentence describing the world / genre / mood>",\n'
    '    "fallback_outfit": "<outfit for narration beats with no named subject, ~20 words>"\n'
    "  }\n"
    "No markdown fences, no commentary outside the JSON."
)


def analyze_script(grok: GrokClient, script_text: str, *, category_label: str = "",
                   topic: str | None = None) -> dict:
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
    return plan


def derive_beat_visuals(grok: GrokClient, narration: str, category_label: str,
                        *, plan: dict | None = None) -> tuple[str, str, str]:
    """
    Per-beat visual prompt. Uses the locked character/style plan from
    analyze_script() so every beat featuring the same subject renders the
    same outfit. Falls back to per-beat inference if no plan is supplied
    (older callers).
    """
    plan = plan or {"characters": {}, "fallback_outfit":
                    "neutral charcoal turtleneck and dark jeans"}
    chars_json = json.dumps(plan.get("characters", {}), ensure_ascii=False)
    setting = plan.get("topic_setting", "")
    fallback = plan.get("fallback_outfit", "")

    sys = (
        "You compose ONE per-scene visual prompt for the Cryptic Science short.\n\n"
        "THE CHARACTER IS FIXED at render time:\n"
        "  - Anatomically accurate adult human skeleton, life-size proportions "
        "(~7.5 head heights, real adult height — NOT chibi, NOT cartoon-mascot, "
        "NOT Funko, NOT big-head).\n"
        "  - Off-white slightly luminous bone with subtle translucent quality.\n"
        "  - Hollow empty black eye sockets — no pupils, no dots, no glow.\n"
        "  - Stands at human scale alongside real people / props, integrated "
        "into the environment like a real character.\n\n"
        "Your job: pick OUTFIT (or none), CINEMATIC SCENE, and MOTION for this beat.\n\n"
        "RULES:\n"
        "  1. OUTFIT — if narration mentions a subject locked in the character "
        "sheet below, COPY that subject's outfit verbatim. If the beat is "
        "narrator-style or the skeleton is just BEING (e.g., walking down a "
        "hallway, standing in a classroom), set outfit to 'no clothing' so the "
        "skeleton is rendered as raw anatomical bone — that's the canonical look "
        "Casey wants for non-costume beats.\n"
        "  2. SCENE_ACTION — describe the FULL cinematic still: setting + pose + "
        "props + nearby people + lighting. Real-world environments only: school "
        "hallway with students walking past, NYC rooftop at night with rain, "
        "Asgardian golden throne hall, Krypton crystalline city, midnight "
        "Metropolis skyline, sun-drenched Roman forum, dusty Mars colony with "
        "biodome. NEVER 'mint backdrop', NEVER 'plain background'. Always real "
        "depth + scale + atmosphere. Other humans CAN appear in the scene — the "
        "skeleton stands among them at adult height.\n"
        "  3. MOTION_PROMPT — one subtle change over 5 sec: skeleton turns head, "
        "students walk past, dust drifts, cape billows, hammer raises with "
        "lightning. NO camera moves, NO scene cuts.\n"
        "  4. NEVER add red laser eyes, demonic glow, or 'pupils' — sockets stay "
        "hollow and empty. For Superman heat-vision beats, the orange glow is on "
        "the TARGET (a tank heating up ahead), not the eyes.\n\n"
        "Output strict JSON: { \"outfit\": ..., \"scene_action\": ..., "
        "\"motion_prompt\": ... }. No markdown fences."
    )
    user_lines = [f"Topic setting: {setting}" if setting else "",
                  f"Character sheet (JSON): {chars_json}",
                  f"Fallback outfit: {fallback}",
                  f"Category label: {category_label}",
                  f"Narration beat: {narration}",
                  "Return JSON now."]
    user = "\n".join(line for line in user_lines if line)

    raw = grok.complete(sys, user, max_tokens=400, temperature=0.6)
    raw = raw.strip().strip("`").strip()
    if raw.lower().startswith("json"):
        raw = raw[4:].strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return (fallback or "plain dark suit",
                "skeleton stands centered facing camera",
                "subtle head tilt, eyes blink once")
    return (
        data.get("outfit", fallback) or fallback or "plain dark suit",
        data.get("scene_action", "skeleton stands centered"),
        data.get("motion_prompt", "subtle head tilt"),
    )


def run(
    category_key: str,
    topic: str | None,
    workspace: Path,
    *,
    tier: str = "standard",
    beats_target: int = 12,
    grok: GrokClient | None = None,
    el: ElevenLabsClient | None = None,
    voice_id: str | None = None,
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
    el = el or ElevenLabsClient()

    # 1. Generate the script.
    cat = get_category(category_key)
    user_prompt = build_script_prompt(cat["system_prompt"], topic)
    script_text = grok.complete(cat["system_prompt"], user_prompt, max_tokens=1500)
    (workspace / "script.txt").write_text(script_text, encoding="utf-8")

    # 2. Pre-pass: analyze the full script to lock a canonical character +
    # style sheet. Every beat that mentions the same subject will reuse the
    # identical outfit description, guaranteeing visual continuity across
    # all 12 beats. Works for the 4 idea-list categories AND custom topics.
    plan = analyze_script(grok, script_text, category_label=cat["label"], topic=topic)
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
            grok, narration, cat["label"], plan=plan
        )
        beats.append(Beat(
            index=i,
            narration=narration,
            outfit=outfit,
            scene_action=action,
            motion_prompt=motion,
            duration_sec=5.0,  # uniform; refined later if we measure TTS chunks
        ))

    # 4. Render stills + clips per beat.
    trimmed_paths: list[Path] = []
    for beat in beats:
        sid = f"b{beat.index:02d}"
        still_prompt = assemble_scene_prompt(beat.scene_action, beat.outfit, mint_bg=True)
        still_path = gen_still(
            still_prompt,
            stills_dir / f"{sid}.png",
            negative_prompt=NEG_STILL,
        )
        clip_path = gen_clip(
            still_path,
            beat.motion_prompt,
            clips_dir / f"{sid}.mp4",
            tier=tier,
            duration_sec=int(beat.duration_sec),
        )
        trimmed = trim_with_captions(
            clip_path,
            trimmed_dir / f"{sid}.mp4",
            duration_sec=beat.duration_sec,
            narration_text=beat.narration,
        )
        trimmed_paths.append(trimmed)

    # 5. Narration audio (full script).
    narration_audio = el.synthesize(
        text=script_text,
        out_path=workspace / "narration.mp3",
        voice_id=voice_id,
    )

    # 6. Concat + mux.
    silent = concat_demuxer(trimmed_paths, workspace / "silent.mp4", work_dir)
    final = mux_narration(silent, narration_audio, workspace / "skeleton_short.mp4")

    # 7. Cost / AC tracking.
    ac_cost = AC_COST_PREMIUM if tier == "premium" else AC_COST_STANDARD
    result = {
        "video_path": str(final),
        "script_path": str(workspace / "script.txt"),
        "narration_path": str(narration_audio),
        "beats": [asdict(b) for b in beats],
        "tier": tier,
        "ac_charged": ac_cost,
        "category": category_key,
        "topic": topic,
    }
    (workspace / "result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result
