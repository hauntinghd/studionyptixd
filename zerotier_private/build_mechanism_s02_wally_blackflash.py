"""ZeroTier Mechanism S02 — Wally outruns Black Flash (conversion sequel).

Applies retention playbook v2:
  - 28s / 4 scenes (no mid-listicle)
  - Loop hook + chase + receipt + subscribe ask
  - Pairs with S01 pin → this → emotional Lane B short

Run from repo root:
  python zerotier_private/build_mechanism_s02_wally_blackflash.py
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

OUT_ROOT = Path(r"D:/recaps/ZeroTier/mechanism_s02_WallyBlackFlash")
FINAL_NAME = "ZeroTier_MechanismS02_WallyBlackFlash.mp4"
DOWNLOADS = Path.home() / "Downloads"
S01_VIDEO_ID = "TsQlpCJo-Xg"


def _load_env() -> None:
    import os
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
    "title": "Wally West Outran Black Flash Once 😨",
    "scenes": [
        {
            "scene_num": 1,
            "duration_sec": 7,
            "text_overlay": "he felt it slow",
            "narration": (
                "Central City, 1998. For the first time, Wally West felt the Speed Force slowing inside him."
            ),
            "visual_description": (
                "Wally West in classic red Flash costume sprinting on rain-slick rooftop. "
                "Motion blur trails fading to static. Black Flash hooded silhouette emerging "
                "over his shoulder. Vertical 9:16 comic panel. High contrast. Manapul Flash run."
            ),
        },
        {
            "scene_num": 2,
            "duration_sec": 7,
            "text_overlay": "same speed",
            "narration": (
                "Black Flash behind him — running at his exact speed. Not faster. That's the trap."
            ),
            "visual_description": (
                "Side-by-side race: red and black lightning trails perfectly parallel. "
                "Two speedsters tied in velocity through Speed Force tunnel. Kinetic diagonal. "
                "Cel-shaded comic splash. Ominous golden-red sky."
            ),
        },
        {
            "scene_num": 3,
            "duration_sec": 7,
            "text_overlay": "one move left",
            "narration": (
                "He didn't beat death. He dove into the Speed Force — at the speed of thought."
            ),
            "visual_description": (
                "Wally West accelerating into golden Speed Force vortex ahead. "
                "Black Flash reaching from behind with bony hand. Abstract energy walls. "
                "Mark Waid era composition. Wally pulling ahead into light."
            ),
        },
        {
            "scene_num": 4,
            "duration_sec": 7,
            "text_overlay": "part 2 ends here",
            "narration": (
                "Subscribe — next is the tragedy even Wally couldn't outrun."
            ),
            "visual_description": (
                "Black Flash bony hand reaching toward camera foreground. "
                "Wally West small figure escaping into distant golden light. "
                "Final punchline frame. Chiaroscuro. ZeroTier end-card energy."
            ),
        },
    ],
    "description": (
        "Part 2 — after Black Flash hunts every speedster, this is the one Flash who outran it.\n\n"
        f"Part 1: https://youtube.com/shorts/{S01_VIDEO_ID}\n\n"
        "Central City, 1998. Wally West felt the Speed Force slow. Black Flash matched his speed. "
        "One escape — accelerate into the Force itself.\n\n"
        "Subscribe for ZeroTier — DC canon mechanisms, not hype.\n\n"
        "#Shorts #WallyWest #BlackFlash #TheFlash #SpeedForce #DCComics #ZeroTier"
    ),
    "tags": [
        "wally west",
        "black flash",
        "the flash",
        "speed force",
        "outran death",
        "flash 1998",
        "dc comics",
        "dc shorts",
        "comic explained",
        "flash canon",
        "speedster",
        "ZeroTier",
    ],
}

UPLOAD_TXT = """=== ZeroTier Mechanism S02 — UPLOAD PACK ===
Pair with S01 (Black Flash hunts) — pin S01 comment linking HERE within 1h of publish.

PRIMARY TITLE:
Wally West Outran Black Flash Once 😨

ALT TITLES (48h swap if impressions stall):
The Only Flash Who Outran Death Itself
Wally West Felt The Speed Force Slowing 😨

PINNED COMMENT (post immediately):
Part 1 — how Black Flash hunts: https://youtube.com/shorts/TsQlpCJo-Xg
Part 3 (subs) — the tragedy Wally couldn't outrun: [link when live]
Who wins if Black Flash catches Wally for real? Comment below.

DESCRIPTION:
{description}

TAGS:
{tags}

END SCREEN:
- Subscribe
- Link: The Only Time Wally West Lost To Barry Allen (sub converter)

TARGET METRICS (48h):
- Stayed to watch: >= 42%
- End retention: >= 35%
- Subs: >= 5
- Views: 4K+ (S01 pin + sequel boost)

PLAYBOOK REF:
Downloads/cross_channel_retention_playbook_2026-05-27.md
"""


def main() -> None:
    _load_env()
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    (OUT_ROOT / "script.json").write_text(
        json.dumps(SCRIPT, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"Rendering S02 Tier A short -> {OUT_ROOT}", flush=True)
    result = render_ken_burns_short(
        script_json=SCRIPT,
        workspace=OUT_ROOT,
        final_filename=FINAL_NAME,
    )

    mp4 = Path(result["mp4_path"])
    DOWNLOADS.mkdir(parents=True, exist_ok=True)
    dest_mp4 = DOWNLOADS / FINAL_NAME
    shutil.copy2(mp4, dest_mp4)

    txt_path = DOWNLOADS / "ZeroTier_MechanismS02_WallyBlackFlash_UPLOAD.txt"
    txt_path.write_text(
        UPLOAD_TXT.format(
            description=SCRIPT["description"],
            tags=", ".join(SCRIPT["tags"]),
        ),
        encoding="utf-8",
    )

    print("\n=== DONE ===", flush=True)
    print(f"MP4:    {dest_mp4}")
    print(f"Upload: {txt_path}")
    print(f"Duration: {result['duration_total_sec']}s")
    print(f"Est cost: ${result['fal_cost_estimate_usd']}")


if __name__ == "__main__":
    main()
