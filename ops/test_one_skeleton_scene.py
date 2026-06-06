#!/usr/bin/env python3
"""Generate ONE skeleton still — mirrors production (ERNIE) + optional paid model.

Usage:
  python ops/test_one_skeleton_scene.py
  python ops/test_one_skeleton_scene.py --model seedream_45
  python ops/test_one_skeleton_scene.py --model ernie_image --topic "FBI vs CIA budget"

Output: ops/output/skeleton_scene_test/<timestamp>_<model>.png
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Load env from common locations
for env_path in (
    ROOT / ".env",
    Path(r"D:/Games/asd/.env"),
    Path(r"D:\Games\asd\.env"),
):
    if env_path.is_file():
        for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
        break

DEFAULT_SCENE = (
    "Medium shot in a modern glass-walled office. The canonical skeleton host stands at a desk "
    "comparing two props: a glowing brain model on the left and a stack of hundred-dollar bills on the right. "
    "Warm cinematic key light, shallow depth of field, 9:16 vertical framing."
)
DEFAULT_TOPIC = "why your brain chooses money over logic"

CANONICAL_ANCHOR = (
    "CANONICAL SKELETON IDENTITY LOCK (gold-standard reference, MUST match every scene exactly): "
    "Photorealistic 3D rendered humanoid figure with the EXACT canonical Studio skeleton identity: "
    "(1) FULL ivory-white anatomical skeleton inside a translucent glass-like soft body shell, "
    "ribcage, spine, pelvis, arm bones, leg bones all clearly visible through the glass torso/limbs; "
    "(2) the head has a realistic anatomical skull with proper eye sockets containing LARGE REALISTIC HUMAN EYES "
    "(visible iris with natural color, pupil, white sclera, wet specular highlights, NEVER glowing, NEVER cartoon eyes); "
    "(3) smooth ivory-white bones, glass body shell with soft caustic highlights; "
    "(4) identical skull proportions, jaw, eye spacing, body shell silhouette in every scene; "
    "(5) Unreal Engine 5 / Octane render quality, premium commercial lighting, crisp focus on the figure. "
    "The translucent body shell ALWAYS exists. Reference style match: Studio gold-standard skeleton lock image. "
)


def _build_prompt(topic: str, visual: str) -> str:
    return (
        f"{CANONICAL_ANCHOR} "
        f"TOPIC: {topic}. "
        f"SCENE: The canonical Studio skeleton figure in a setting directly related to {topic}. {visual} "
        "Negative: no glowing eyes, no cartoon style, no anime, no chibi, no missing bones, "
        "no bone color drift (bones must stay ivory-white), no skin opaqueness loss on the glass shell, "
        "no extra limbs, no melted face, no warped skull."
    ).strip()


def _run_stills_engine(model: str, prompt: str, neg: str, out: Path) -> dict:
    from skeleton_ai.stills_engine import generate as gen_still

    gen_still(model, prompt, out, negative_prompt=neg, width=720, height=1280)
    return {"provider": model, "bytes": out.stat().st_size}


def main() -> None:
    parser = argparse.ArgumentParser(description="One-scene skeleton still smoke test")
    parser.add_argument(
        "--model",
        default="ernie_image",
        choices=["seedream_edit", "ernie_image", "seedream_45", "imagen4", "flux_2_pro", "recraft_v4_pro"],
        help="seedream_edit = canonical master edit (production default); ernie_image = legacy T2I",
    )
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--scene", default=DEFAULT_SCENE)
    args = parser.parse_args()

    topic = str(args.topic or "").strip()
    visual = str(args.scene or "").strip()
    full_prompt = _build_prompt(topic, visual)

    out_dir = ROOT / "ops" / "output" / "skeleton_scene_test"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"{stamp}_{args.model}.png"

    print(f"Model: {args.model}")
    print(f"Topic: {topic}")
    print(f"Output: {out_path}")
    print(f"Prompt ({len(full_prompt)} chars):\n{full_prompt[:500]}...\n")

    t0 = time.time()
    if args.model == "seedream_edit":
        from skeleton_ai.canonical_edit import build_scene_edit_prompt, generate_still_edit

        edit_prompt = build_scene_edit_prompt(
            topic=topic,
            visual_description=visual,
        )
        meta = generate_still_edit(edit_prompt, out_path, seed=420099)
    else:
        from skeleton_ai.prompts.base_style import NEG_STILL

        meta = _run_stills_engine(args.model, full_prompt, NEG_STILL, out_path)

    elapsed = time.time() - t0
    print(f"Done in {elapsed:.1f}s — {meta}")
    print(f"Open: {out_path.resolve()}")


if __name__ == "__main__":
    main()
