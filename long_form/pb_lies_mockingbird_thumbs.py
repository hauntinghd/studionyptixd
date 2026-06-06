"""Generate 3 PB Lies Mockingbird thumbnail variants for CTR testing."""
from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import fal_client

from long_form.pipeline import _download, _fal_post

SEEDREAM_EDIT = "https://fal.run/fal-ai/bytedance/seedream/v4.5/edit"
STILLS = Path(r"D:/recaps/pb_lies/cast_kit_white3d/stills")
OUT = Path(r"D:/recaps/pb_lies/mockingbird_s01/thumbs")
DL = Path.home() / "Downloads"

THUMBS = [
    {
        "id": "thumb_a_classified",
        "ref": "scene_01_classified_desk_white_suit_default.png",
        "prompt": (
            "YouTube thumbnail 16:9. Cinematic forensic documentary. White faceless "
            "3D mannequin at black desk with redacted CIA manila folder. Bold white "
            "sans-serif text MOCKINGBIRD top third. TOP SECRET red stamp. Cold tungsten "
            "light, deep blacks. High contrast click-worthy. NO faces, NO arrows."
        ),
    },
    {
        "id": "thumb_b_400_journalists",
        "ref": "scene_24_four_hundred_card_forensic_red.png",
        "prompt": (
            "YouTube thumbnail 16:9. Forensic red glow. Giant UI number 400 with "
            "subtitle JOURNALISTS. White faceless 3D figure silhouette. Bold text "
            "CIA BOUGHT THE PRESS lower third. Classified documentary aesthetic. "
            "High contrast, no human faces."
        ),
    },
    {
        "id": "thumb_c_corkboard",
        "ref": "scene_19_corkboard_evidence_yellow_caution.png",
        "prompt": (
            "YouTube thumbnail 16:9. Evidence corkboard with red string and polaroids. "
            "White faceless mannequin in suit. Red banner AMERICAN PRESS. Bold 2-3 word "
            "title MOCKINGBIRD EXPOSED. Cold Case Files documentary grade. No gore."
        ),
    },
]


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    for p in (Path(r"D:\Games\asd\.env"), ROOT / ".env"):
        if p.exists():
            load_dotenv(p)
            break


def main() -> None:
    _load_env()
    os.environ["FAL_KEY"] = os.environ.get("FAL_AI_KEY", "")
    OUT.mkdir(parents=True, exist_ok=True)
    for t in THUMBS:
        ref = STILLS / t["ref"]
        if not ref.exists():
            alt = STILLS / t["ref"].replace("_white_suit_default", "")
            ref = alt if alt.exists() else ref
        out = OUT / f"{t['id']}.png"
        if out.exists() and out.stat().st_size > 1024:
            print(f"  [skip] {out.name}")
        else:
            url = fal_client.upload_file(str(ref))
            data = _fal_post(
                SEEDREAM_EDIT,
                {"prompt": t["prompt"], "image_urls": [url], "image_size": "auto_2K", "num_images": 1},
                timeout_s=240,
            )
            img = (data.get("images") or [{}])[0].get("url")
            if not img:
                raise RuntimeError(data)
            _download(img, out)
            print(f"  [thumb] {out.name}")
        shutil.copy2(out, DL / f"PB_Lies_{t['id']}.png")
    print(f"Downloads: {DL}/PB_Lies_thumb_*.png")


if __name__ == "__main__":
    main()
