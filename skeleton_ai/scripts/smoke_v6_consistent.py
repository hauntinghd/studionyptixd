"""v6 — locked translucent anatomical skeleton, ALWAYS naked, across topics."""
from __future__ import annotations
import os, sys, time
from pathlib import Path
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(ROOT))
ENV = Path(r"D:/Games/asd/.env")
for line in ENV.read_text(encoding="utf-8", errors="ignore").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line: continue
    k, _, v = line.partition("=")
    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

from skeleton_ai.prompts.base_style import assemble_scene_prompt, NEG_STILL
from skeleton_ai.stills_engine import generate as gen_still

WS = ROOT / "skeleton_ai" / "output" / "smoke_v6_consistent"
WS.mkdir(parents=True, exist_ok=True)
(WS / "stills").mkdir(exist_ok=True)

# 4 different topic categories — character must look IDENTICAL in all 4.
TESTS = [
    {
        "sid": "v6_marvel",
        "scene": "Standing in a brightly-lit modern comic book shop with glass display cases, rows of comic books on the shelves behind, a Marvel vs DC promotional poster on the wall showing real Iron Man and Hulk fighting. A real Iron Man action figure sits on a shelf at chest height. A real teen customer in a hoodie browses comics in the background. The skeleton holds a single open comic book in its hand showing 'IRON MAN VS HULK' on the cover.",
    },
    {
        "sid": "v6_human_limits",
        "scene": "Standing in a high school hallway, lockers on both sides, real teenage students walking past in jeans and t-shirts, fluorescent ceiling lights. The skeleton holds a small triangle musical instrument while a real teen with glasses stands next to it holding a striker about to play.",
    },
    {
        "sid": "v6_ancient",
        "scene": "Standing in a sun-drenched ancient Roman forum with weathered marble columns and stepped stone forum floor, real togaed Roman citizens walking past in the background, a Roman centurion in red tunic and lorica segmentata armor stands a few feet away holding a gladius sword. A real burning wooden cart smolders on the cobblestones behind.",
    },
    {
        "sid": "v6_futuristic",
        "scene": "Standing inside a Mars 2050 colony habitat with a transparent biodome ceiling showing the red Martian landscape outside, real engineers in white jumpsuits working at glowing blue holographic control panels in the background, a hovering drone drifts past at head height. The skeleton holds a small data pad with floating Mars geology readouts.",
    },
]

for t in TESTS:
    out = WS / "stills" / f"{t['sid']}.png"
    if out.exists() and out.stat().st_size > 1024:
        print(f"[skip] {t['sid']} cached")
        continue
    prompt = assemble_scene_prompt(t["scene"], outfit="no clothing", mint_bg=False)
    print(f"\n=== {t['sid']} ===")
    print(f"PROMPT:\n{prompt[:600]}...\n")
    ts = time.time()
    gen_still("seedream_45", prompt, out, negative_prompt=NEG_STILL)
    print(f"rendered in {time.time()-ts:.1f}s -> {out}")

print("\nDONE — 4 stills across 4 topics. Skeleton must look IDENTICAL in all.")
