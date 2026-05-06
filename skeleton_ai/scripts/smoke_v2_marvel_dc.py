"""
Smoke test v2 — fresh end-to-end Skeleton AI run after prompt trim
+ Seedance audio fix + two-pass scene planner ship.

Validates:
  1. Grok script gen for Marvel vs DC category produces clean text.
  2. analyze_script locks canonical hero costumes.
  3. Trimmed SKELETON_BASE_STYLE produces visibly clean renders.
  4. Seedance 2.0 i2v passes content-policy with generate_audio=False.
  5. ffmpeg compose w/ filter_script handles caption escapes.
  6. Final mp4 plays cleanly with narration mux.

Two-stage execution:
  STAGE A (cheap, ~$0.48): stills only — exits after 12 stills land.
                            Set RUN_I2V=0 to stop here.
  STAGE B (full, +$1.20):  i2v + ElevenLabs + mux. Set RUN_I2V=1.

Output: skeleton_ai/output/smoke_v2/<stills, clips, narration.mp3, skeleton_short.mp4>
"""
from __future__ import annotations
import json
import os
import sys
import time
from pathlib import Path

# Bootstrap: load /d/Games/asd/.env
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(ROOT))
ENV_FILE = Path(r"D:/Games/asd/.env")
if ENV_FILE.exists():
    for line in ENV_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

from skeleton_ai.scripting_grok import GrokClient
from skeleton_ai.pipeline import (
    analyze_script, derive_beat_visuals, split_script_into_beats,
)
from skeleton_ai.prompts.base_style import assemble_scene_prompt, NEG_STILL
from skeleton_ai.prompts.idea_lists import get_category
from skeleton_ai.stills_engine import generate as gen_still
from skeleton_ai.i2v_engine import generate as gen_clip
from skeleton_ai.voice_elevenlabs import ElevenLabsClient
from skeleton_ai.compose import trim_with_captions, concat_demuxer, mux_narration


WORKSPACE = ROOT / "skeleton_ai" / "output" / "smoke_v2"
CATEGORY = "marvel_vs_dc"
TOPIC = "Iron Man vs Doctor Strange. Who would win?"
IMAGE_MODEL = "seedream_45"
BEATS_TARGET = 12
RUN_I2V = os.environ.get("RUN_I2V", "0") == "1"


def main() -> int:
    t0 = time.time()
    WORKSPACE.mkdir(parents=True, exist_ok=True)
    stills_dir = WORKSPACE / "stills"
    clips_dir = WORKSPACE / "clips"
    trimmed_dir = WORKSPACE / "trimmed"
    work_dir = WORKSPACE / "work"
    for d in (stills_dir, clips_dir, trimmed_dir, work_dir):
        d.mkdir(exist_ok=True)

    grok = GrokClient()
    cat = get_category(CATEGORY)

    # --- 1. Script ---
    script_path = WORKSPACE / "script.txt"
    if script_path.exists() and script_path.stat().st_size > 200:
        print(f"[1/6] cached script ({script_path.stat().st_size} bytes)")
        script_text = script_path.read_text(encoding="utf-8")
    else:
        print(f"[1/6] generating script (Grok, ~{TOPIC!r})...")
        from skeleton_ai.scripting_grok import build_script_prompt
        user_prompt = build_script_prompt(cat["system_prompt"], TOPIC)
        script_text = grok.complete(cat["system_prompt"], user_prompt, max_tokens=1500)
        script_path.write_text(script_text, encoding="utf-8")
        print(f"      wrote {len(script_text)} chars")

    # --- 2. Plan (locks character sheet for Iron Man + Dr Strange) ---
    plan_path = WORKSPACE / "scene_plan.json"
    if plan_path.exists():
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        print(f"[2/6] cached plan: {list((plan.get('characters') or {}).keys())}")
    else:
        print(f"[2/6] analyzing script (locking character sheet)...")
        plan = analyze_script(grok, script_text, category_label=cat["label"], topic=TOPIC)
        plan_path.write_text(json.dumps(plan, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"      characters locked: {list((plan.get('characters') or {}).keys())}")

    # --- 3. Beats ---
    sentences = split_script_into_beats(script_text, target_count=BEATS_TARGET)
    if not sentences:
        print("[abort] empty script")
        return 2
    print(f"[3/6] split into {len(sentences)} beats")

    # --- 4. Stills ---
    print(f"[4/6] rendering {len(sentences)} stills via {IMAGE_MODEL}...")
    beats_meta: list[dict] = []
    for i, narration in enumerate(sentences):
        sid = f"b{i:02d}"
        outfit, action, motion = derive_beat_visuals(grok, narration, cat["label"], plan=plan)
        prompt = assemble_scene_prompt(action, outfit, mint_bg=True)
        still_path = stills_dir / f"{sid}.png"
        if not still_path.exists():
            ts = time.time()
            gen_still(IMAGE_MODEL, prompt, still_path, negative_prompt=NEG_STILL)
            print(f"      [{i+1:2}/{len(sentences)}] {sid} ({time.time()-ts:.1f}s) {narration[:60]!r}")
        else:
            print(f"      [{i+1:2}/{len(sentences)}] {sid} cached")
        beats_meta.append({
            "beat_index": i, "sid": sid, "narration": narration,
            "outfit": outfit, "action": action, "motion": motion,
        })
    (WORKSPACE / "beats.json").write_text(json.dumps(beats_meta, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[4/6] all stills ready in {stills_dir}")

    if not RUN_I2V:
        print()
        print(f"[STAGE A DONE] elapsed {time.time()-t0:.1f}s")
        print(f"  stills: {stills_dir}")
        print(f"  Inspect the 12 PNGs. If they look 400x better than Korpi, run again with RUN_I2V=1 for full pipeline.")
        return 0

    # --- 5. i2v ---
    print(f"[5/6] running i2v (Seedance 2.0, generate_audio=False)...")
    trimmed_paths = []
    for b in beats_meta:
        sid = b["sid"]
        clip_path = clips_dir / f"{sid}.mp4"
        if not clip_path.exists():
            ts = time.time()
            gen_clip(stills_dir / f"{sid}.png", b["motion"], clip_path,
                     tier="standard", duration_sec=5)
            print(f"      [{b['beat_index']+1:2}/{len(beats_meta)}] {sid} clip ({time.time()-ts:.1f}s)")
        trimmed_path = trimmed_dir / f"{sid}.mp4"
        trim_with_captions(clip_path, trimmed_path, duration_sec=5.0,
                           narration_text=b["narration"])
        trimmed_paths.append(trimmed_path)
    print(f"[5/6] all clips trimmed + captioned")

    # --- 6. Voice + mux ---
    silent = WORKSPACE / "silent.mp4"
    if not silent.exists():
        concat_demuxer(trimmed_paths, silent, work_dir)
    narration_path = WORKSPACE / "narration.mp3"
    if not narration_path.exists():
        el = ElevenLabsClient()
        el.synthesize(script_text, narration_path)
    final = WORKSPACE / "skeleton_short.mp4"
    if not final.exists():
        mux_narration(silent, narration_path, final)
    print(f"[6/6] FINAL: {final}")
    print(f"      elapsed {time.time()-t0:.1f}s")

    result = {
        "video": str(final),
        "stills_dir": str(stills_dir),
        "category": CATEGORY,
        "topic": TOPIC,
        "image_model": IMAGE_MODEL,
        "beats": beats_meta,
        "plan": plan,
    }
    (WORKSPACE / "result.json").write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
