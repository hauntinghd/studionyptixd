"""ZeroTier Pope S04 — clone best stayed-to-watch format (Barry Allen).

Pope doctrine applied:
  - Title reinforces hook (56.5% stayed on original)
  - 33s / 4 scenes / Tier A Ken Burns (~$0.25 fal)
  - Frame-1 ALL CAPS center caption
  - No listicle mid-video

Run from repo root:
  python zerotier_private/build_pope_s04_barry_lost.py
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from zerotier_private.pipeline import render_ken_burns_short
from zerotier_private.pope_doctrine import validate_pope_script

OUT_ROOT = Path(r"D:/recaps/ZeroTier/pope_s04_BarryLost")
FINAL_NAME = "ZeroTier_PopeS04_BarryLost.mp4"
DOWNLOADS = Path.home() / "Downloads"
ENV_PATHS = (Path(r"D:\Games\asd\.env"), ROOT / ".env")


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    for p in ENV_PATHS:
        if p.exists():
            load_dotenv(p)
            break

SCRIPT = {
    "title": "How Wally West Lost To Barry Allen",
    "scenes": [
        {
            "scene_num": 1,
            "duration_sec": 8,
            "text_overlay": "HE LOST TO BARRY",
            "narration": (
                "Central City. Two Flashes. One finish line. "
                "Wally West had never lost a race — until Barry Allen stopped holding back."
            ),
            "visual_description": (
                "Wally West and Barry Allen in red Flash costumes facing each other on a "
                "storm-lit empty highway, lightning crackling between them. Vertical 9:16 "
                "comic splash. Extreme tension. Jim Lee scale. Both Flashes full body visible."
            ),
        },
        {
            "scene_num": 2,
            "duration_sec": 8,
            "text_overlay": "same speed",
            "narration": (
                "Barry did not run faster. He ran at Wally's exact speed — "
                "and that was the trap. You cannot beat the man who taught you the track."
            ),
            "visual_description": (
                "Side-by-side race: Wally and Barry running parallel, tied golden lightning "
                "trails, motion blur equal on both. Francis Manapul Flash run style. "
                "High contrast cel-shaded comic panel."
            ),
        },
        {
            "scene_num": 3,
            "duration_sec": 9,
            "text_overlay": "mentor wins",
            "narration": (
                "Barry crossed first by a single step. Not power. Not the Speed Force. "
                "Experience — the one thing Wally could not outrun."
            ),
            "visual_description": (
                "Barry Allen breaking the finish tape of light ahead of Wally by inches. "
                "Wally reaching forward, expression shocked. Dynamic diagonal composition. "
                "Speed lines. Comic book finish-line moment."
            ),
        },
        {
            "scene_num": 4,
            "duration_sec": 8,
            "text_overlay": "subscribe for part 2",
            "narration": (
                "Subscribe — next is the tragedy even Wally West could not outrun."
            ),
            "visual_description": (
                "Wally West kneeling on the highway, Barry's hand on his shoulder. "
                "Rain and golden lightning. Emotional comic panel. Quiet after the race."
            ),
        },
    ],
    "description": "",
    "tags": ["flash", "wally west", "barry allen", "dc comics", "shorts"],
}


def main() -> None:
    _load_env()
    issues = validate_pope_script(SCRIPT)
    if issues:
        print("Pope validation warnings (proceeding anyway):")
        for msg in issues:
            print(f"  - {msg}")

    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    (OUT_ROOT / "script.json").write_text(
        json.dumps(SCRIPT, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"Rendering Pope S04 -> {OUT_ROOT}", flush=True)
    result = render_ken_burns_short(
        script_json=SCRIPT,
        workspace=OUT_ROOT,
        final_filename=FINAL_NAME,
    )
    print(json.dumps(result, indent=2), flush=True)

    src = Path(result["mp4_path"])
    if src.exists():
        dest = DOWNLOADS / FINAL_NAME
        shutil.copy2(src, dest)
        print(f"Copied to {dest}", flush=True)

    print(
        "\nPost-upload Pope checklist:",
        "  1. Engagement tab -> stayed-to-watch (target >=45%, stretch 56%+)",
        "  2. Compare to Lost To Barry original (56.5%)",
        "  3. If <40% stayed -> change frame-1 text only, re-render scene 1 (~$0.04)",
        sep="\n",
        flush=True,
    )


if __name__ == "__main__":
    main()
