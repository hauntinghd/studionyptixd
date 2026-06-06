"""Fork PB Lies white 3D roster → Empire Magnates yellow porcelain roster.

Same mesh topology / wardrobe tags — material swap only via Seedream edit.
Run after PB roster is approved:

  python long_form/pb_to_em_cast_fork.py              # all PB roster PNGs
  python long_form/pb_to_em_cast_fork.py --limit 8   # starter
  python long_form/pb_to_em_cast_fork.py --id white_suit_default

Output: D:/recaps/empire_magnates/cast_kit_yellow/roster_from_pb/
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import fal_client

from long_form.em_yellow_cast_kit import CAST_IDENTITY, NEG as EM_NEG
from long_form.pipeline import _download, _fal_post

SEEDREAM_EDIT_URL = "https://fal.run/fal-ai/bytedance/seedream/v4.5/edit"

PB_ROSTER = Path(r"D:/recaps/pb_lies/cast_kit_white3d/roster")
PB_MASTER = Path(r"D:/recaps/pb_lies/cast_kit_white3d/approved/cast_master_front.png")
EM_OUT = Path(r"D:/recaps/empire_magnates/cast_kit_yellow/roster_from_pb")
EM_APPROVED = Path(r"D:/recaps/empire_magnates/cast_kit_yellow/approved/cast_master_front.png")
FORK_SEED = 880032
COST_PER = 0.04


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    for p in (Path(r"D:\Games\asd\.env"), ROOT / ".env"):
        if p.exists():
            load_dotenv(p)
            break


def _ensure_fal() -> None:
    key = (os.environ.get("FAL_AI_KEY") or os.environ.get("FAL_KEY") or "").strip()
    if not key:
        raise RuntimeError("FAL_AI_KEY not set")
    os.environ["FAL_KEY"] = key


def _fork_prompt(variant_id: str) -> str:
    return (
        f"{CAST_IDENTITY} "
        "FORK LOCK: Keep IDENTICAL pose, body proportions, camera angle, and "
        "wardrobe layout as reference — only change material from white/grey "
        "mannequin to saffron-yellow golden porcelain ceramic. "
        f"Variant id: {variant_id}. Plain medium-grey studio backdrop, 16:9."
    )


def _edit(refs: list[Path], prompt: str, out: Path) -> None:
    if out.exists() and out.stat().st_size > 1024:
        print(f"  [skip] {out.name}")
        return
    _ensure_fal()
    urls = [fal_client.upload_file(str(p)) for p in refs[:3]]
    data = _fal_post(
        SEEDREAM_EDIT_URL,
        {
            "prompt": prompt[:3500],
            "image_urls": urls,
            "negative_prompt": EM_NEG,
            "image_size": "auto_2K",
            "num_images": 1,
            "seed": FORK_SEED,
        },
        timeout_s=240,
    )
    images = data.get("images") or []
    if not images or not (url := images[0].get("url")):
        raise RuntimeError(f"fork edit failed: {data}")
    _download(url, out, timeout_s=120)
    print(f"  [fork] {out.name} ({out.stat().st_size // 1024} KB)")


def fork_roster(*, limit: int | None = None, variant_id: str | None = None) -> float:
    EM_OUT.mkdir(parents=True, exist_ok=True)
    pb_pngs = sorted(PB_ROSTER.glob("*.png"))
    if variant_id:
        pb_pngs = [p for p in pb_pngs if p.stem == variant_id]
        if not pb_pngs:
            raise SystemExit(f"No PB roster PNG for {variant_id}")
    elif limit:
        pb_pngs = pb_pngs[:limit]

    em_master = EM_APPROVED if EM_APPROVED.exists() else None
    cost = 0.0
    print(f"=== PB → EM fork ({len(pb_pngs)} variants) ===")
    for pb in pb_pngs:
        vid = pb.stem
        refs = [pb]
        if em_master:
            refs.append(em_master)
        if PB_MASTER.exists():
            refs.append(PB_MASTER)
        _edit(refs[:3], _fork_prompt(vid), EM_OUT / f"{vid}.png")
        cost += COST_PER

    meta = {
        "source": str(PB_ROSTER),
        "output": str(EM_OUT),
        "fork_seed": FORK_SEED,
        "identity": CAST_IDENTITY,
        "variants_forked": [p.stem for p in pb_pngs],
    }
    (EM_OUT / "fork_manifest.json").write_text(
        json.dumps(meta, indent=2), encoding="utf-8"
    )
    return cost


def main() -> None:
    _load_env()
    ap = argparse.ArgumentParser(description="Fork PB Lies roster to EM yellow porcelain")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--id", type=str, default=None, dest="variant_id")
    args = ap.parse_args()
    cost = fork_roster(limit=args.limit, variant_id=args.variant_id)
    print(f"\n=== DONE — est fal ~${cost:.2f} → {EM_OUT} ===")


if __name__ == "__main__":
    main()
