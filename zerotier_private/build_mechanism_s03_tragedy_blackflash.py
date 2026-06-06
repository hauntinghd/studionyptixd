"""ZeroTier Arc Part 3 — Black Flash tragedy + hard subscribe CTA.

Part 1: How Black Flash Hunts Every Speedster (TsQlpCJo-Xg)
Part 2: Wally West Outran Black Flash Once
Part 3: The second meeting — emotional payoff + on-screen SUBSCRIBE block

Run from repo root:
  python zerotier_private/build_mechanism_s03_tragedy_blackflash.py
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

OUT_ROOT = Path(r"D:/recaps/ZeroTier/mechanism_s03_BlackFlashTragedy")
FINAL_NAME = "ZeroTier_MechanismS03_BlackFlashTragedy.mp4"
DOWNLOADS = Path.home() / "Downloads"
S01_VIDEO_ID = "TsQlpCJo-Xg"
# Paste your Part 2 shorts ID after upload (studio URL slug):
S02_VIDEO_ID = "fjhtruhxQ-g"


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    for p in (
        Path(r"D:\Games\asd\.env"),
        ROOT / ".env",
        ROOT.parents[2] / ".env",
    ):
        if p.exists():
            load_dotenv(p)
            break


SCRIPT = {
    "title": "The Tragedy Wally Couldn't Outrun 😨",
    "scenes": [
        {
            "scene_num": 1,
            "duration_sec": 6,
            "text_overlay": "part 3",
            "narration": (
                "Wally West outran Black Flash once. Every speedster knows that story."
            ),
            "visual_description": (
                "Wally West in red Flash costume sprinting into golden Speed Force light, "
                "Black Flash falling behind in shadow. Vertical 9:16 comic splash. "
                "Triumphant but ominous. Manapul Flash run style."
            ),
        },
        {
            "scene_num": 2,
            "duration_sec": 6,
            "text_overlay": "the second time",
            "narration": (
                "The second time they met — he was not running. He was standing still."
            ),
            "visual_description": (
                "Wally West standing frozen in empty dark street, head bowed. "
                "Black Flash hooded figure materializing directly in front of him, "
                "faceless skull close. Rain. Cold blue lighting. Heavy emotional weight."
            ),
        },
        {
            "scene_num": 3,
            "duration_sec": 6,
            "text_overlay": "speed can't fix this",
            "narration": (
                "Because the tragedy was never death chasing him. "
                "It was what he already lost."
            ),
            "visual_description": (
                "Wally West kneeling beside hospital bed silhouette, Linda Park figure "
                "unreachable in soft focus. Red lightning faded to gray. "
                "Intimate comic panel. Devin Grayson era emotional Flash tone."
            ),
        },
        {
            "scene_num": 4,
            "duration_sec": 5,
            "text_overlay": "SUBSCRIBE",
            "narration": (
                "Subscribe to ZeroTier if Flash canon stories hit you like this."
            ),
            "visual_description": (
                "Wally West and Black Flash frozen face-to-face, dramatic chiaroscuro. "
                "Bold empty space at top third for text. High contrast cel-shaded panel."
            ),
        },
        {
            "scene_num": 5,
            "duration_sec": 6,
            "text_overlay": "SUBSCRIBE NOW",
            "narration": (
                "Part 1 and 2 are on the channel. New Wally West breakdown every week. "
                "Subscribe now so you don't miss the next one."
            ),
            "visual_description": (
                "Clean black background with red Flash lightning bolt emblem centered, "
                "ZeroTier end-card style. Minimal. Subscribe button energy. "
                "White and red typography space. Comic book graphic design."
            ),
        },
    ],
    "description": (
        "Part 3 — Black Flash arc finale. Wally outran death once. The second time broke him.\n\n"
        f"Part 1: https://youtube.com/shorts/{S01_VIDEO_ID}\n"
        f"Part 2: https://youtube.com/shorts/{S02_VIDEO_ID}\n\n"
        "Subscribe for ZeroTier — Flash canon mechanisms and tragedies, every week.\n\n"
        "#Shorts #WallyWest #BlackFlash #TheFlash #LindaPark #DCComics #ZeroTier"
    ),
    "tags": [
        "wally west",
        "black flash",
        "the flash",
        "linda park",
        "flash tragedy",
        "dc comics",
        "dc shorts",
        "flash canon",
        "speed force",
        "comic explained",
        "ZeroTier",
    ],
}

UPLOAD_TXT = """=== ZeroTier Part 3 — UPLOAD PACK ===
Arc: Black Flash (Part 1 → 2 → 3). This is the SUB CONVERTER — CTA baked in.

PRIMARY TITLE:
The Tragedy Wally Couldn't Outrun 😨

ALT TITLES:
The Second Time Black Flash Found Wally West 😨
Wally Stopped Running 😨

PINNED COMMENT (post in 5 min):
Full arc —
Part 1: https://youtube.com/shorts/TsQlpCJo-Xg
Part 2: https://youtube.com/shorts/fjhtruhxQ-g
Part 3: you are here.

Subscribe if ZeroTier Flash canon hits different. Who do you think broke Wally more — Black Flash or losing Linda?

DESCRIPTION:
{description}

TAGS:
{tags}

THUMB:
Use stills/03_speed_can_t_fix_this.png or 01_part_3.png from workspace.

END SCREEN:
- Subscribe (required)
- Link: The Only Time Wally West Lost To Barry Allen

UPDATE PINS ON PART 1 & 2:
- S01 pin → "Part 2 & 3 live — subscribe for the full arc"
- S02 pin → "Part 3 finale — subscribe here"

TARGET: >= 5 subs @ 48h (this is the conversion short)
"""


def main() -> None:
    _load_env()
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    (OUT_ROOT / "script.json").write_text(
        json.dumps(SCRIPT, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"Rendering Part 3 -> {OUT_ROOT}", flush=True)
    result = render_ken_burns_short(
        script_json=SCRIPT,
        workspace=OUT_ROOT,
        final_filename=FINAL_NAME,
    )

    mp4 = Path(result["mp4_path"])
    DOWNLOADS.mkdir(parents=True, exist_ok=True)
    dest_mp4 = DOWNLOADS / FINAL_NAME
    shutil.copy2(mp4, dest_mp4)

    thumb_src = OUT_ROOT / "stills" / "03_speed_can_t_fix_this.png"
    if not thumb_src.exists():
        stills = sorted((OUT_ROOT / "stills").glob("*.png"))
        thumb_src = stills[2] if len(stills) > 2 else (stills[0] if stills else None)

    txt_path = DOWNLOADS / "ZeroTier_MechanismS03_BlackFlashTragedy_UPLOAD.txt"
    txt_path.write_text(
        UPLOAD_TXT.format(
            description=SCRIPT["description"],
            tags=", ".join(SCRIPT["tags"]),
        ),
        encoding="utf-8",
    )

    if thumb_src and Path(thumb_src).exists():
        shutil.copy2(thumb_src, DOWNLOADS / "ZeroTier_MechanismS03_THUMB.png")

    print("\n=== DONE ===", flush=True)
    print(f"MP4:    {dest_mp4}")
    print(f"Upload: {txt_path}")
    print(f"Duration: {result['duration_total_sec']}s")
    print(f"Est cost: ${result['fal_cost_estimate_usd']}")
    if S02_VIDEO_ID.startswith("REPLACE"):
        print("NOTE: Set S02_VIDEO_ID in script + re-run description, or edit UPLOAD.txt Part 2 URL.")


if __name__ == "__main__":
    main()
