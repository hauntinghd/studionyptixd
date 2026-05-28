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
    "You are the visual planner for the Cryptic Science alt-history "
    "battle YouTube Shorts channel. PR #153 — Casey 2026-05-12: "
    "'CrypticScience is no longer skeleton AI.'\n\n"
    "Every scene shows PORCELAIN MANNEQUINS — smooth glazed ceramic "
    "bodies with no facial features (no eyes, no mouth, subtle brow + "
    "nose ridge only) — wearing period-correct armor, robes, and "
    "weapons painted ONTO the porcelain shell. The world is photoreal "
    "cinematic battlefield. The mannequin BODY is FIXED — what changes "
    "scene to scene is the OUTFIT (period gear), PROPS, and POSE.\n\n"
    "Given a full narration script and an optional topic hint, "
    "identify every named subject (real historical commander, era "
    "role, unit type) and lock a SINGLE canonical outfit description "
    "for each. The same subject must look identical in every beat. Be "
    "specific: name colors, period-correct armor types, signature "
    "weapons, real historical insignia.\n\n"
    "Subject-rendering rules (period-correct gear painted onto "
    "porcelain mannequin bodies):\n"
    "  - Named historical commanders: research-accurate signature "
    "look. "
    "Napoleon Bonaparte → dark blue Imperial tailcoat with gold "
    "epaulettes, white waistcoat with brass buttons, white breeches "
    "tucked into black riding boots, black bicorne hat worn sideways "
    "with tricolor cockade, red sash, gloved hand tucked into coat. "
    "Hannibal Barca → bronze muscle cuirass with embossed lion motif, "
    "purple-dyed wool cloak fastened at right shoulder, Hellenistic "
    "crested helmet with red horsehair plume, leather pteruges skirt, "
    "bronze greaves, curved falcata sword at hip. "
    "Julius Caesar → red paludamentum cloak, polished lorica musculata "
    "cuirass, gilded greaves, leather pteruges, golden corona civica "
    "wreath, gladius hispaniensis at right hip. "
    "Alexander the Great → linothorax cuirass with bronze scales, "
    "Phrygian-style helmet with white horsehair plume, purple cape, "
    "kopis sword, gold-stamped greaves. "
    "Genghis Khan → lamellar leather armor over felt undercoat, fur-"
    "trimmed leather helmet, lacquered composite recurve bow, curved "
    "saber, deel robe in indigo blue. "
    "  - Unit / formation types: era-correct gear. "
    "Roman legionary → lorica segmentata, gladius, scutum shield with "
    "lightning bolts, galea helmet with transverse crest, caligae. "
    "Spartan hoplite → bronze muscle cuirass, Corinthian helmet with "
    "crest, large hoplon shield with lambda, dory spear. "
    "Mongol horse archer → lamellar leather armor, fur-trimmed conical "
    "helmet, composite bow + quiver, deel robe, hardened leather "
    "boots. "
    "Napoleonic line infantry → dark blue wool tailcoat with white "
    "crossbelts, white breeches, black gaiters, black shako with brass "
    "plate and plume, Charleville musket with bayonet. "
    "  - Generic / unknown subjects: pick concrete era + role + at "
    "least 4 specific clothing details. NEVER 'plain suit', NEVER "
    "'modern casual'.\n\n"
    "Output strict JSON:\n"
    "  {\n"
    '    "characters": { "<subject name>": "<full canonical period '
    'outfit, ~25-40 words>" },\n'
    '    "topic_setting": "<one sentence describing the battlefield / '
    'terrain / weather / era>",\n'
    '    "fallback_outfit": "<period-correct gear for narration beats '
    "with no named subject, ~20 words>\"\n"
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
    Per-beat visual prompt. PR #153 — rewritten to lock the porcelain
    mannequin character per PR #145's `base_style.py` ABSOLUTE CAST RULE.
    Casey 2026-05-12: 'CrypticScience is no longer skeleton AI.'

    The prior system prompt baked in a naked anatomical skeleton as the
    locked cast, which leaked into every scene_action regardless of
    PR #145's assemble_scene_prompt mannequin override (the mannequin
    grammar got prepended to skeleton-referencing actions — visual
    mashup). This rewrite uses porcelain mannequins in period-correct
    gear as the cast lock, matching the Cryptic Science alt-history
    battle channel signature.
    """
    plan = plan or {"characters": {}, "fallback_outfit":
                    "period-correct historical military gear"}
    chars_json = json.dumps(plan.get("characters", {}), ensure_ascii=False)
    setting = plan.get("topic_setting", "")
    fallback = plan.get("fallback_outfit", "")

    sys = (
        "You compose ONE per-scene visual prompt for the Cryptic Science "
        "alt-history battle YouTube short.\n\n"
        "THE CAST IS LOCKED — every human figure is a PORCELAIN MANNEQUIN:\n"
        "  - Smooth, stylized porcelain mannequin body — clean glazed "
        "ceramic, no facial features beyond a subtle brow + nose ridge, "
        "no eyes, no mouth.\n"
        "  - Off-white / pale porcelain shell with period-correct armor, "
        "helmets, robes, banners, and weapons painted or strapped ONTO "
        "the mannequin body. Cracked-glaze accents on commander chest "
        "plates for visual hierarchy.\n"
        "  - NEVER real human faces, NEVER bare skin, NEVER anatomical "
        "skeletons, NEVER modern action figures, NEVER costumed actors.\n\n"
        "THE WORLD IS LOCKED — photoreal cinematic battlefield:\n"
        "  - Real terrain (snow / dirt / grass / stone / water / sand), "
        "real atmospheric haze, real volumetric lighting, real cinematic "
        "depth-of-field. The porcelain cast moves through a "
        "photographically rendered world.\n\n"
        "Your only job per beat: write SCENE_ACTION (the photoreal world "
        "around the porcelain cast) and MOTION_PROMPT (one subtle 5-sec "
        "movement).\n\n"
        "RULES:\n"
        "  1. SCENE_ACTION — describe a TABLEAU composition: formations "
        "of porcelain mannequins in period gear facing each other or "
        "arrayed across the photoreal terrain. Reference the character "
        "sheet outfits (e.g. 'Carthaginian Libyan spearmen in bronze "
        "Montefortino helmets, red tunics, oval scutum shields with "
        "Carthaginian symbols') — these get painted onto the porcelain "
        "mannequin bodies. Background props from the topic setting "
        "(snow-capped peaks, banner poles, cannon batteries, war "
        "elephants in formation). Period-correct flag colors. NO active "
        "combat — show a frozen moment.\n"
        "  2. MOTION_PROMPT — one subtle change over 5 sec: a banner "
        "ripples in the wind, snow drifts past, a porcelain commander "
        "slowly turns his head, ranks of mannequins hold position, "
        "atmospheric mist rolls in. NO camera moves. NO graphic "
        "violence motion.\n"
        "  3. OUTFIT — describe the PERIOD GEAR painted onto the focal "
        "commander/unit mannequin (e.g. 'gold-trimmed bicorne, dark "
        "blue greatcoat with tricolor sash, white breeches, black "
        "riding boots, Charleville musket'). Pull from the character "
        "sheet when the narration mentions a named figure. BARE_TORSO "
        "is always false.\n\n"
        "Output strict JSON: { \"outfit\": ..., \"scene_action\": ..., "
        "\"motion_prompt\": ..., \"bare_torso\": false }. No markdown."
    )
    user_lines = [f"Topic setting: {setting}" if setting else "",
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
        return (fallback or "period-correct historical military gear",
                "Porcelain mannequins in period armor stand in formation across the photoreal battlefield, banners raised, atmospheric weather, no active combat",
                "Banner ripples in the wind, snow drifts past, atmospheric mist rolls in")
    outfit = data.get("outfit", fallback) or fallback or "period-correct historical military gear"
    # Bare-torso retained as a sentinel for legacy callers, but
    # mannequins never go bare under the new cast lock.
    if bool(data.get("bare_torso", False)):
        outfit = f"[BARE_TORSO] {outfit}"
    return (
        outfit,
        data.get("scene_action", "Porcelain mannequins in period armor stand in formation"),
        data.get("motion_prompt", "Banner ripples in the wind"),
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
    script_override: str | None = None,
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

    # 1. Generate the script (or use user-edited override from Create panel).
    cat = get_category(category_key)
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
