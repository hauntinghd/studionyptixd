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


def derive_beat_visuals(grok: GrokClient, narration: str, category_label: str) -> tuple[str, str, str]:
    """
    For one narration beat, ask Grok to return:
      - OUTFIT description (short)
      - SCENE_ACTION description (short)
      - MOTION_PROMPT for i2v (short)
    """
    sys = (
        "You design single-scene visual prompts for the Cryptic Science skeleton "
        "character on a mint green backdrop. Given a narration sentence and a "
        "topic category, output JSON with three fields: outfit, scene_action, "
        "motion_prompt. Each field is one sentence, concrete and visual. "
        "OUTFIT is the real clothing the skeleton wears in this beat. "
        "SCENE_ACTION is what's happening in the still frame. "
        "MOTION_PROMPT is what changes in the 5-second i2v animation. "
        "Output ONLY the JSON, no markdown fences."
    )
    user = f"Category: {category_label}\nNarration: {narration}\n\nReturn JSON now."
    raw = grok.complete(sys, user, max_tokens=400, temperature=0.7)
    raw = raw.strip().strip("`").strip()
    if raw.startswith("json"):
        raw = raw[4:].strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # Fall back to neutral defaults if Grok gave us garbage.
        return ("plain dark suit", "skeleton stands centered facing camera",
                "subtle head tilt, eyes blink once")
    return (
        data.get("outfit", "plain dark suit"),
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

    # 2. Split into beats.
    sentences = split_script_into_beats(script_text, target_count=beats_target)
    if not sentences:
        raise RuntimeError("Grok returned empty script")

    # 3. For each beat, get visual prompt + create a Beat record.
    beats: list[Beat] = []
    for i, narration in enumerate(sentences):
        outfit, action, motion = derive_beat_visuals(grok, narration, cat["label"])
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
