"""ZeroTier Comic Mechanism S01 — Black Flash (Tier A Ken Burns).

Discovery-first packaging to break the ~1K impression ceiling:
  - Broad title (search + browse): "How Black Flash Hunts Every Speedster"
  - Instant frame-1 identity: grim reaper + Flash silhouette
  - Mechanism structure (Zack D pacing) not Conflict Arc
  - 48s / 5 scenes — Ken Burns only (~$0.30 fal spend)

Run from repo root:
  python zerotier_private/build_mechanism_s01_blackflash.py
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

OUT_ROOT = Path(r"D:/recaps/ZeroTier/mechanism_s01_BlackFlash")
FINAL_NAME = "ZeroTier_MechanismS01_BlackFlash.mp4"
DOWNLOADS = Path.home() / "Downloads"

SCRIPT = {
    "title": "How Black Flash Hunts Every Speedster 😨",
    "scenes": [
        {
            "scene_num": 1,
            "duration_sec": 9,
            "text_overlay": "speedsters have a grim reaper",
            "narration": (
                "Every speedster who breaks the Speed Force's rules gets one visitor. "
                "Not a villain. A sentence."
            ),
            "visual_description": (
                "Black Flash — hooded skeletal speedster in black and crimson robes with "
                "white lightning eyes — towering over a tiny red Flash silhouette running "
                "on an empty dark highway. Vertical 9:16 comic splash page. Extreme size "
                "contrast. Ominous golden-red sky. Jim Lee dramatic scale."
            ),
        },
        {
            "scene_num": 2,
            "duration_sec": 10,
            "text_overlay": "it does not chase you",
            "narration": (
                "Black Flash does not chase you through cities. "
                "It appears the exact moment your body crosses the line the Speed Force drew."
            ),
            "visual_description": (
                "Wally West in red Flash costume mid-sprint on a rain-slick rooftop. "
                "Black Flash materializing from a crack of black lightning directly behind him. "
                "Frozen speed lines. High contrast cel-shaded comic panel. Francis Manapul Flash run style."
            ),
        },
        {
            "scene_num": 3,
            "duration_sec": 10,
            "text_overlay": "three signs you're next",
            "narration": (
                "Three signs. Uncontrolled speed bursts. Voices from the Force itself. "
                "And time moving wrong around your body."
            ),
            "visual_description": (
                "Vertical triptych comic layout: top panel blur trail out of control, "
                "middle panel whispering golden Speed Force energy faces, bottom panel "
                "melting clock face. Wally West centered looking up in alarm. Bold linework."
            ),
        },
        {
            "scene_num": 4,
            "duration_sec": 10,
            "text_overlay": "wally raced it anyway",
            "narration": (
                "Wally West raced it inside the Speed Force exile. "
                "He did not outrun death. He outran the second it caught him."
            ),
            "visual_description": (
                "Wally West and Black Flash racing parallel through abstract Speed Force tunnel. "
                "Red and black lightning intertwined. Mark Waid era Flash comic aesthetic. "
                "Kinetic diagonal composition. Speed Force energy walls."
            ),
        },
        {
            "scene_num": 5,
            "duration_sec": 9,
            "text_overlay": "death keeps receipts",
            "narration": (
                "The Flash can save almost anyone. "
                "Black Flash is the one receipt the Speed Force never loses."
            ),
            "visual_description": (
                "Black Flash bony hand reaching toward camera foreground. "
                "Wally West running away into distant golden light background. "
                "Cinematic depth in comic panel. Chiaroscuro. Final punchline frame."
            ),
        },
    ],
    "description": (
        "Black Flash is not a rogue. It is the Speed Force's death sentence — "
        "and every speedster who breaks the rules gets visited.\n\n"
        "How it hunts. The three warning signs. And why Wally West could not "
        "simply outrun it.\n\n"
        "Canon grounded: Speed Force lore, Mark Waid Flash run, Black Flash as "
        "the grim reaper of speedsters.\n\n"
        "#Shorts #TheFlash #BlackFlash #WallyWest #DCComics #SpeedForce #ZeroTier"
    ),
    "tags": [
        "black flash",
        "the flash",
        "wally west",
        "speed force",
        "dc comics",
        "dc shorts",
        "flash comics",
        "speedster",
        "comic book shorts",
        "flash grim reaper",
        "dc lore",
        "comic explained",
        "flash explained",
        "ZeroTier",
    ],
}

UPLOAD_TXT = """=== ZeroTier Mechanism S01 — UPLOAD PACK ===
Format: Comic Mechanism (Tier A) — discovery-first

PRIMARY TITLE (use this):
How Black Flash Hunts Every Speedster 😨

ALT TITLES (A/B if primary stalls at 24h):
Every Speedster Gets Visited By THIS 😨
The Speed Force Has A Death Sentence

DESCRIPTION:
{description}

TAGS (paste as comma-separated):
{tags}

THUMBNAIL:
Use stills/00_speedsters_have_a_grim_reaper.png from the workspace folder.
Crop tight on Black Flash face + hand if needed. Add NO extra text — the still
already reads at phone size.

WHY THIS SHOULD BEAT 1K VIEW JAIL:
1. Title matches SEARCH ("black flash", "speedster") not only subs who know Wally arcs
2. Frame 1 = instant identity (grim reaper + runner) — fixes Lost Identity problem
3. Mechanism pacing = new format A/B vs Conflict Arc — YouTube tests it fresh
4. 48s length = retention sweet spot without long-form drag

POSTING NOTES:
- Category: Entertainment (NOT Film & Animation)
- Do NOT pin to the underperforming long-form
- Post within 24-48h of your last ZT short to keep inventory fresh
- Watch 48h: if impressions <200, swap to ALT TITLE #2 (same video, new metadata only)
"""


def main() -> None:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    (OUT_ROOT / "script.json").write_text(
        json.dumps(SCRIPT, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"Rendering Tier A short -> {OUT_ROOT}", flush=True)
    result = render_ken_burns_short(
        script_json=SCRIPT,
        workspace=OUT_ROOT,
        final_filename=FINAL_NAME,
    )

    mp4 = Path(result["mp4_path"])
    thumb_src = OUT_ROOT / "stills" / "00_speedsters_have_a_grim_reaper.png"
    if not thumb_src.exists():
        stills = sorted((OUT_ROOT / "stills").glob("*.png"))
        thumb_src = stills[0] if stills else None

    DOWNLOADS.mkdir(parents=True, exist_ok=True)
    dest_mp4 = DOWNLOADS / FINAL_NAME
    shutil.copy2(mp4, dest_mp4)

    txt_path = DOWNLOADS / "ZeroTier_MechanismS01_BlackFlash_UPLOAD.txt"
    txt_path.write_text(
        UPLOAD_TXT.format(
            description=SCRIPT["description"],
            tags=", ".join(SCRIPT["tags"]),
        ),
        encoding="utf-8",
    )

    if thumb_src and thumb_src.exists():
        shutil.copy2(thumb_src, DOWNLOADS / "ZeroTier_MechanismS01_THUMB.png")

    print("\n=== DONE ===", flush=True)
    print(f"MP4:       {dest_mp4}")
    print(f"Upload:    {txt_path}")
    if thumb_src:
        print(f"Thumbnail: {DOWNLOADS / 'ZeroTier_MechanismS01_THUMB.png'}")
    print(f"Duration:  {result['duration_total_sec']}s")
    print(f"Est cost:  ${result['fal_cost_estimate_usd']}")
    print(f"Title:     {result['title'].encode('ascii', 'replace').decode()}")


if __name__ == "__main__":
    main()
