"""ZeroTier Pope S05 — Barry Lost using REAL comic panels (Tier P).

Same script as S04 Pope winner — visuals from actual DC panel scans, not Seedream.
FAL spend: ~$0.10 for MiniMax narration.

Before render:
  1. Drop 4 panel images into D:/recaps/ZeroTier/pope_s05_BarryLost_panels/panels/
  2. Name them to match panel_image below (or edit script.json)

Run:
  python zerotier_private/build_pope_s05_barry_panels.py
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from zerotier_private.pipeline import render_comic_panel_short

OUT_ROOT = Path(r"D:/recaps/ZeroTier/pope_s05_BarryLost_panels")
FINAL_NAME = "ZeroTier_PopeS05_BarryLost_Panels.mp4"
DOWNLOADS = Path.home() / "Downloads"
ENV_PATHS = (Path(r"D:\Games\asd\.env"), ROOT / ".env")

SCRIPT = {
    "title": "How Wally West Lost To Barry Allen",
    "scenes": [
        {
            "scene_num": 1,
            "duration_sec": 8,
            "text_overlay": "HE LOST TO BARRY",
            "panel_image": "01_race_faceoff.jpg",
            "comic_ref": "The Flash (Wally West era) — Francis Manapul / Brian Buccellato run",
            "narration": (
                "Central City. Two Flashes. One finish line. "
                "Wally West had never lost a race — until Barry Allen stopped holding back."
            ),
            "visual_description": "Panel: Wally and Barry facing off before a race — use Manapul Flash run.",
        },
        {
            "scene_num": 2,
            "duration_sec": 8,
            "text_overlay": "same speed",
            "panel_image": "02_parallel_run.jpg",
            "comic_ref": "The Flash — side-by-side speedster race panel",
            "narration": (
                "Barry did not run faster. He ran at Wally's exact speed — "
                "and that was the trap. You cannot beat the man who taught you the track."
            ),
            "visual_description": "Panel: parallel Flash run, golden lightning trails.",
        },
        {
            "scene_num": 3,
            "duration_sec": 9,
            "text_overlay": "mentor wins",
            "panel_image": "03_finish_line.jpg",
            "comic_ref": "The Flash — Barry crosses ahead of Wally",
            "narration": (
                "Barry crossed first by a single step. Not power. Not the Speed Force. "
                "Experience — the one thing Wally could not outrun."
            ),
            "visual_description": "Panel: finish-line beat, Barry inches ahead.",
        },
        {
            "scene_num": 4,
            "duration_sec": 8,
            "text_overlay": "subscribe for part 2",
            "panel_image": "04_aftermath.jpg",
            "comic_ref": "The Flash — emotional aftermath / mentor moment",
            "narration": (
                "Subscribe — next is the tragedy even Wally West could not outrun."
            ),
            "visual_description": "Panel: quiet aftermath, Barry and Wally.",
        },
    ],
    "description": "",
    "tags": ["flash", "wally west", "barry allen", "dc comics", "shorts"],
}


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    for p in ENV_PATHS:
        if p.exists():
            load_dotenv(p)
            break


def main() -> None:
    _load_env()

    panels_dir = OUT_ROOT / "panels"
    panels_dir.mkdir(parents=True, exist_ok=True)
    missing = [s["panel_image"] for s in SCRIPT["scenes"] if not (panels_dir / s["panel_image"]).exists()]
    if missing:
        print(f"Missing panels in {panels_dir}:")
        for m in missing:
            print(f"  - {m}")
        print("\nDrop comic panel JPG/PNG scans, then re-run.")
        (OUT_ROOT / "script.json").write_text(
            json.dumps(SCRIPT, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        sys.exit(1)

    (OUT_ROOT / "script.json").write_text(
        json.dumps(SCRIPT, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"Rendering Tier P (panels + Ken Burns) -> {OUT_ROOT}  vo=fal_minimax", flush=True)
    result = render_comic_panel_short(
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

    credits = result.get("comic_credits") or []
    upload_desc = (
        "Panel sources:\n" + "\n".join(f"- {c}" for c in credits)
        + "\n\nArt used under commentary / educational fair use. DC Comics."
    )
    (OUT_ROOT / "UPLOAD.txt").write_text(
        f"TITLE: {SCRIPT['title']}\n\nDESCRIPTION:\n{upload_desc}\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
