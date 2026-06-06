"""Generate CrypticScience CTR+SS thumbnail via Seedream v4.5 edit (host + Markus style ref)."""
from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import fal_client

from long_form.pipeline import SEEDREAM_URL, _download, _fal_post

SEEDREAM_EDIT = "https://fal.run/fal-ai/bytedance/seedream/v4.5/edit"

HOST = Path(r"D:/recaps/cryptic_science/ctr_ss_rook_v1/assets/host_v2.png")
MARKUS = Path(r"D:/recaps/cryptic_science/_thumb_ref/markus_bank_UfLIPjy0F_4.jpg")
OUT_DIR = Path(r"D:/recaps/cryptic_science/ctr_ss_rook_v1/thumbnails")
FINAL = Path(r"D:/recaps/cryptic_science/ctr_ss_rook_v1/CrypticScience_CTR_SS_Rook_v1_THUMB.jpg")
DL = Path.home() / "Downloads" / FINAL.name

PROMPT = (
    "YouTube finance explainer thumbnail, 16:9, 1920x1080, Markus Graves high-CTR style. "
    "RIGHT 45%: preserve the EXACT man from reference photo 1 — same face, salt-and-pepper hair, "
    "navy button-down shirt, lapel mic, serious direct eye contact, chest-up portrait anchored "
    "bottom-right edge, studio lighting, no rectangular box border. "
    "CENTER-LEFT: large bold stacked typography with black outline: "
    "line1 '$10,000' white, line2 'BANK RULE' white on thick red horizontal brush stroke, "
    "line3 '+ SOCIAL' white, line4 'SECURITY' white on thick red brush stroke, "
    "small pill badge 'VERIFIED' below. "
    "FAR LEFT: two clean circular badges stacked — top green coin '$10K' / 'CASH CTR', "
    "bottom blue coin '2.8%' / 'SS COLA', yellow highlight bar 'FOR SENIORS'. "
    "Background: warm dark parchment texture like reference image 2, subtle vignette, "
    "high contrast, professional YouTube finance thumbnail. "
    "NO Donald Trump, NO skulls, NO political portraits, NO messy overlapping text, "
    "NO watermark, perfectly legible English text."
)

NEG = (
    "blurry text, misspelled words, extra fingers, deformed face, different person, "
    "Trump, skull, watermark, low quality, cluttered layout, white rectangle border around host, "
    "generic stock photo, cartoon"
)

VARIANTS = [
    {"seed": 420017, "hint": "Tighter text, host slightly larger."},
    {"seed": 420018, "hint": "More parchment texture, bolder red bars."},
    {"seed": 420019, "hint": "Cleaner minimal badges, maximum text legibility."},
]


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    for p in (Path(r"D:\Games\asd\.env"), ROOT / ".env"):
        if p.exists():
            load_dotenv(p)


def _ensure_fal() -> None:
    _load_env()
    key = os.getenv("FAL_AI_KEY", "").strip() or os.getenv("FAL_KEY", "").strip()
    if not key:
        raise RuntimeError("FAL_AI_KEY / FAL_KEY not set")
    os.environ["FAL_KEY"] = key


def _gen_edit(prompt: str, refs: list[Path], out: Path, seed: int) -> Path:
    out.parent.mkdir(parents=True, exist_ok=True)
    urls = [fal_client.upload_file(str(p)) for p in refs]
    data = _fal_post(
        SEEDREAM_EDIT,
        {
            "prompt": prompt[:4000],
            "image_urls": urls,
            "image_size": {"width": 1920, "height": 1080},
            "negative_prompt": NEG,
            "seed": seed,
            "num_images": 1,
        },
        timeout_s=300,
    )
    images = data.get("images") or []
    if not images or not images[0].get("url"):
        raise RuntimeError(f"Seedream edit returned no image: {data}")
    _download(images[0]["url"], out, timeout_s=120)
    print(f"  saved {out} ({out.stat().st_size // 1024} KB)")
    return out


def _gen_t2i(prompt: str, out: Path, seed: int) -> Path:
    data = _fal_post(
        SEEDREAM_URL,
        {
            "prompt": prompt[:4000],
            "negative_prompt": NEG,
            "image_size": {"width": 1920, "height": 1080},
            "seed": seed,
            "num_images": 1,
        },
        timeout_s=240,
    )
    images = data.get("images") or []
    if not images or not images[0].get("url"):
        raise RuntimeError(f"Seedream t2i returned no image: {data}")
    _download(images[0]["url"], out, timeout_s=120)
    print(f"  saved {out} ({out.stat().st_size // 1024} KB)")
    return out


def main() -> None:
    _ensure_fal()
    if not HOST.exists():
        raise FileNotFoundError(HOST)
    if not MARKUS.exists():
        raise FileNotFoundError(MARKUS)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    refs = [HOST, MARKUS]
    paths: list[Path] = []

    for i, v in enumerate(VARIANTS, 1):
        p = OUT_DIR / f"thumb_seedream_v{i}.jpg"
        full = f"{PROMPT}\n\nVariant note: {v['hint']}"
        try:
            _gen_edit(full, refs, p, v["seed"])
            paths.append(p)
        except Exception as e:
            print(f"  edit v{i} failed: {e}")

    if not paths:
        print("Edit failed — trying t2i with host-only ref via edit...")
        p = OUT_DIR / "thumb_seedream_fallback.jpg"
        _gen_edit(PROMPT, [HOST], p, 420020)
        paths.append(p)

    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--pick", type=int, choices=(1, 2, 3), help="User-selected variant (required for final)")
    args = ap.parse_args()

    if args.pick is None:
        print("\nVariants ready — pick one before shipping:")
        for i, p in enumerate(paths, 1):
            print(f"  {i}: {p} ({p.stat().st_size // 1024} KB)")
        print("\nRe-run: python long_form/generate_cryptic_ctr_ss_thumb.py --pick 1|2|3")
        print("Zero-fal fallback: python long_form/compose_cryptic_ctr_ss_thumb.py")
        return

    pick = paths[args.pick - 1]
    shutil.copy2(pick, FINAL)
    shutil.copy2(pick, DL)
    print(f"\nFinal (user pick {args.pick}): {FINAL}")
    print(f"Downloads: {DL}")


if __name__ == "__main__":
    main()
