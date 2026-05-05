"""
Resume the smoke_test_1 run.

Prior run state (from skeleton_ai/output/smoke_test_1/):
  - script.txt cached (12 sentences, Peak Brain Plasticity topic)
  - 7 stills b00..b06
  - 6 clips b00..b05  (b06.mp4 missing — Seedance content-policy block)
  - 6 trimmed b00..b05

Today's fix: i2v_engine.generate() now passes generate_audio=False to Seedance,
which clears the partner_validation_failed reject. We just need to:

  1. Re-derive motion prompt for beat 6 via Grok.
  2. Generate clips/b06.mp4 via fixed Seedance.
  3. Trim+caption b06 → trimmed/b06.mp4.
  4. Concat all 7 trimmed clips → silent.mp4.
  5. ElevenLabs TTS the first 7 sentences → narration.mp3.
  6. Mux narration over silent → skeleton_short.mp4.

Idempotent: every step short-circuits if its output already exists.
"""
from __future__ import annotations
import json
import os
import sys
from pathlib import Path

# Make repo root importable.
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(ROOT))

# Load .env from /d/Games/asd/.env (canonical for this box).
ENV_FILE = Path(r"D:/Games/asd/.env")
if ENV_FILE.exists():
    for line in ENV_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

from skeleton_ai.scripting_grok import GrokClient
from skeleton_ai.i2v_engine import generate as gen_clip
from skeleton_ai.voice_elevenlabs import ElevenLabsClient
from skeleton_ai.compose import trim_with_captions, concat_demuxer, mux_narration
from skeleton_ai.pipeline import split_script_into_beats, derive_beat_visuals
from skeleton_ai.prompts.idea_lists import get_category


WORKSPACE = Path(r"D:/Games/asd/.claude/worktrees/laughing-mclean-b5c91d/skeleton_ai/output/smoke_test_1")
BEATS_TARGET = 7  # match the cached 7-still run
CATEGORY_KEY = "human_limits"
TIER = "standard"
BEAT_DURATION_SEC = 5.0


def main() -> int:
    stills_dir = WORKSPACE / "stills"
    clips_dir = WORKSPACE / "clips"
    trimmed_dir = WORKSPACE / "trimmed"
    work_dir = WORKSPACE / "work"
    for d in (stills_dir, clips_dir, trimmed_dir, work_dir):
        d.mkdir(parents=True, exist_ok=True)

    script_path = WORKSPACE / "script.txt"
    if not script_path.exists():
        print(f"[abort] cached script not found at {script_path}")
        return 2
    script_text = script_path.read_text(encoding="utf-8")

    sentences = split_script_into_beats(script_text, target_count=BEATS_TARGET)
    if len(sentences) < BEATS_TARGET:
        print(f"[warn] only {len(sentences)} sentences in script.txt (wanted {BEATS_TARGET})")
    print(f"[1/6] using {len(sentences)} cached beats from script.txt")

    # Narration that lines up with the 7-beat video.
    narration_text = " ".join(sentences)

    cat = get_category(CATEGORY_KEY)
    grok = GrokClient()

    trimmed_paths: list[Path] = []
    for i, narration in enumerate(sentences):
        sid = f"b{i:02d}"
        still_path = stills_dir / f"{sid}.png"
        if not still_path.exists():
            print(f"[abort] still {still_path} missing — re-run full pipeline")
            return 3
        clip_path = clips_dir / f"{sid}.mp4"
        if not clip_path.exists():
            print(f"[2/6] beat {sid} clip missing — calling Grok for motion prompt")
            outfit, action, motion = derive_beat_visuals(grok, narration, cat["label"])
            print(f"       motion: {motion!r}")
            clip_path = gen_clip(
                still_path,
                motion,
                clip_path,
                tier=TIER,
                duration_sec=int(BEAT_DURATION_SEC),
            )
            print(f"       wrote {clip_path}")
        trimmed_path = trimmed_dir / f"{sid}.mp4"
        trimmed_path = trim_with_captions(
            clip_path,
            trimmed_path,
            duration_sec=BEAT_DURATION_SEC,
            narration_text=narration,
        )
        trimmed_paths.append(trimmed_path)
    print(f"[3/6] all {len(trimmed_paths)} trimmed clips ready")

    silent = WORKSPACE / "silent.mp4"
    if not silent.exists():
        silent = concat_demuxer(trimmed_paths, silent, work_dir)
    print(f"[4/6] silent spine at {silent}")

    narration_mp3 = WORKSPACE / "narration.mp3"
    if not narration_mp3.exists():
        el = ElevenLabsClient()
        narration_mp3 = el.synthesize(narration_text, narration_mp3)
    print(f"[5/6] narration audio at {narration_mp3}")

    final = WORKSPACE / "skeleton_short.mp4"
    if not final.exists():
        final = mux_narration(silent, narration_mp3, final)
    print(f"[6/6] FINAL VIDEO: {final}")

    result = {
        "video_path": str(final),
        "script_path": str(script_path),
        "narration_path": str(narration_mp3),
        "beats_count": len(trimmed_paths),
        "tier": TIER,
        "category": CATEGORY_KEY,
    }
    (WORKSPACE / "result.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
