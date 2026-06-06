"""EM yellow-porcelain cast kit — ~$0.20 validation run.

Generates:
  - 3-angle cast reference sheet (Seedream t2i)
  - 2 test scenes (Seedream edit + refs)
  - 1 test thumbnail (Seedream edit + refs)

Run from repo root:
  python long_form/em_yellow_cast_kit.py
"""
from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import fal_client

from long_form.pipeline import SEEDREAM_URL, _download, _fal_post

SEEDREAM_EDIT_URL = "https://fal.run/fal-ai/bytedance/seedream/v4.5/edit"

OUT_ROOT = Path(r"D:/recaps/empire_magnates/cast_kit_yellow")
APPROVED_DIR = OUT_ROOT / "approved"
DOWNLOADS = Path.home() / "Downloads"

# Locked for this kit session — reuse on edit calls.
CAST_SEED = 880031

CAST_IDENTITY = (
    "EM CAST IDENTITY LOCK: smooth premium saffron-yellow golden porcelain mannequin "
    "— matte ceramic finish like fine yellow-gold statuary, warm ochre undertone, "
    "yellow porcelain head AND matching yellow porcelain business suit, "
    "crisp white dress shirt, black tie. NO real human skin, NO photographic face, "
    "NO eyes, NO mouth, only a subtle sculpted brow ridge. Adult male proportions."
)

NEG = (
    "real human face, photographic skin, photorealistic person, real eyes, real mouth, "
    "red porcelain, white porcelain, grey mannequin, cartoon, illustration, anime, "
    "low quality, blur, watermark, text unless specified"
)

LIGHTING_BIBLE = (
    "Cinematic documentary lighting LOCK: single warm overhead spotlight on mannequin "
    "(3200K), cool teal cyan fill from background holographic screen (6500K), deep black "
    "shadows, restrained saturation, reflective polished floor, photoreal 3D render, "
    "NOT illustration."
)

CAST_SHEETS = [
    {
        "id": "ref_a_front",
        "prompt": (
            f"{CAST_IDENTITY} Full-body front view, neutral standing pose, arms relaxed at "
            "sides, plain medium-grey seamless studio backdrop, soft even studio lighting, "
            "no environment, character reference sheet, 1920x1080."
        ),
    },
    {
        "id": "ref_b_portrait",
        "prompt": (
            f"{CAST_IDENTITY} Chest-up portrait, slight 3/4 angle facing camera left, "
            "plain medium-grey seamless studio backdrop, soft even studio lighting, "
            "character reference sheet, shallow depth of field."
        ),
    },
    {
        "id": "ref_c_back",
        "prompt": (
            f"{CAST_IDENTITY} Three-quarter back view, head turned slightly toward camera, "
            "showing yellow porcelain head profile and suit shoulders, plain medium-grey "
            "seamless studio backdrop, soft even studio lighting, character reference sheet."
        ),
    },
]

TEST_SCENES = [
    {
        "id": "scene_01_presentation",
        "prompt": (
            f"{CAST_IDENTITY} {LIGHTING_BIBLE} "
            "PRESENTATION archetype: yellow-porcelain mannequin executive stands at a dark "
            "wooden podium center-frame in a grand dim auditorium. Behind him a massive "
            "teal holographic wireframe display shows an offshore bank vault schematic with "
            "UI chrome corners. Symmetrical composition, reflective floor, Fern documentary "
            "aesthetic, medium-wide shot."
        ),
    },
    {
        "id": "scene_02_walk",
        "prompt": (
            f"{CAST_IDENTITY} {LIGHTING_BIBLE} "
            "WALK archetype: same yellow-porcelain mannequin mid-stride walking down a "
            "center aisle toward a lit stage in the same grand auditorium, warm spotlight "
            "pool on polished floor, rows of beige seats visible, cinematic depth, "
            "medium shot from slight low angle."
        ),
    },
]

TEST_THUMB = {
    "id": "thumb_01_brex",
    "prompt": (
        f"{CAST_IDENTITY} {LIGHTING_BIBLE} "
        "YouTube documentary thumbnail 16:9. Yellow-porcelain mannequin at podium left-third, "
        "dominant teal holographic wireframe of a jungle gold-drill site map filling right "
        "side of frame. Small UI badge bottom-right reads 6B in teal chrome — NOT giant "
        "floating text. High contrast, click-worthy, Empire Magnates Loophole Files style."
    ),
}


def _ensure_fal() -> None:
    key = os.getenv("FAL_AI_KEY", "").strip() or os.getenv("FAL_KEY", "").strip()
    if not key:
        raise RuntimeError("FAL_AI_KEY not set")
    os.environ["FAL_KEY"] = key


def _gen_t2i(prompt: str, out: Path) -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists() and out.stat().st_size > 1024:
        print(f"  [cache hit] {out.name}")
        return out
    data = _fal_post(
        SEEDREAM_URL,
        {
            "prompt": prompt[:3500],
            "negative_prompt": NEG,
            "image_size": {"width": 1920, "height": 1080},
            "seed": CAST_SEED,
            "num_images": 1,
        },
        timeout_s=240,
    )
    images = data.get("images") or []
    if not images or not images[0].get("url"):
        raise RuntimeError(f"t2i returned no image: {data}")
    _download(images[0]["url"], out, timeout_s=120)
    print(f"  [t2i] {out.name} ({out.stat().st_size // 1024} KB)")
    return out


def _gen_edit(prompt: str, ref_paths: list[Path], out: Path) -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists() and out.stat().st_size > 1024:
        print(f"  [cache hit] {out.name}")
        return out
    urls = [fal_client.upload_file(str(p)) for p in ref_paths]
    data = _fal_post(
        SEEDREAM_EDIT_URL,
        {
            "prompt": prompt[:3500],
            "image_urls": urls,
            "image_size": {"width": 1920, "height": 1080},
            "seed": CAST_SEED,
            "num_images": 1,
        },
        timeout_s=300,
    )
    images = data.get("images") or []
    if not images or not images[0].get("url"):
        raise RuntimeError(f"edit returned no image: {data}")
    _download(images[0]["url"], out, timeout_s=120)
    print(f"  [edit] {out.name} ({out.stat().st_size // 1024} KB)")
    return out


def main() -> None:
    _ensure_fal()
    cast_dir = OUT_ROOT / "cast"
    test_dir = OUT_ROOT / "tests"
    cast_dir.mkdir(parents=True, exist_ok=True)
    test_dir.mkdir(parents=True, exist_ok=True)

    print("=== EM Yellow Porcelain Cast Kit ===")
    print(f"Output: {OUT_ROOT}")
    print(f"Seed:   {CAST_SEED}\n")

    print("Step 1/3 — Cast reference sheet (3x t2i, ~$0.12)")
    refs: list[Path] = []
    for spec in CAST_SHEETS:
        p = cast_dir / f"{spec['id']}.png"
        refs.append(_gen_t2i(spec["prompt"], p))

    print("\nStep 2/3 — Test scenes (2x edit, ~$0.08)")
    scenes: list[Path] = []
    for spec in TEST_SCENES:
        p = test_dir / f"{spec['id']}.png"
        scenes.append(_gen_edit(spec["prompt"], refs, p))

    print("\nStep 3/3 — Test thumbnail (1x edit, ~$0.04)")
    thumb_path = test_dir / f"{TEST_THUMB['id']}.png"
    _gen_edit(TEST_THUMB["prompt"], refs, thumb_path)

    manifest = {
        "cast_identity": "yellow_porcelain",
        "seed": CAST_SEED,
        "cast_refs": [str(p) for p in refs],
        "test_scenes": [str(p) for p in scenes],
        "test_thumb": str(thumb_path),
        "est_cost_usd": 0.20,
    }
    manifest_path = OUT_ROOT / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    DOWNLOADS.mkdir(parents=True, exist_ok=True)
    for src in refs + scenes + [thumb_path]:
        dest = DOWNLOADS / f"EM_yellow_{src.name}"
        shutil.copy2(src, dest)
        print(f"  -> Downloads/{dest.name}")

    print("\n=== DONE ===")
    print(f"Cast refs:  {cast_dir}")
    print(f"Tests:      {test_dir}")
    print(f"Manifest:   {manifest_path}")
    print("Est spend:  ~$0.20")


if __name__ == "__main__":
    main()
