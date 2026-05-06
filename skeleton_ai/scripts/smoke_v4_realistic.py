"""v3 spec test — anatomically realistic adult skeleton, no dot pupils, photoreal env."""
from __future__ import annotations
import json, os, sys, time
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

WS = ROOT / "skeleton_ai" / "output" / "smoke_v4_realistic"
WS.mkdir(parents=True, exist_ok=True)
(WS / "stills").mkdir(exist_ok=True)

# 3 hand-crafted test beats matching Casey's reference image style.
TESTS = [
    {
        "sid": "ref_school",
        "outfit": "no clothing",
        "scene": "Standing in a brightly-lit modern American high school hallway, fluorescent ceiling lights, locker rows on both sides, real teenage students walking past in jeans and t-shirts, the skeleton holds a small triangle musical instrument with a striker. The skeleton is the same height as the teen standing next to it. Photoreal, real-life proportions, depth of field on the background students.",
    },
    {
        "sid": "ref_thor",
        "outfit": "winged silver helmet, red flowing cape, gold-and-silver Asgardian breastplate with circular bosses, brown leather wrist bracers, gripping Mjolnir hammer in hand",
        "scene": "Standing in the golden Asgardian throne hall with high vaulted ceilings, polished gold pillars on either side, rainbow Bifrost arc visible through the open doorway behind, dramatic god-rays of light from above, mist drifting near the floor. The skeleton (in Thor's armor, hammer raised) stands at adult human height. Photoreal cinematic scale.",
    },
    {
        "sid": "ref_iron",
        "outfit": "red-and-gold full Mark 85 plate armor with circular arc reactor glowing blue on the chest, helmet with triangular slit eyes",
        "scene": "Standing on a rain-slicked NYC rooftop at midnight, lightning crack in the storm sky behind, soaked Empire State Building visible in the background, neon signs reflecting off the wet rooftop. The skeleton-Iron-Man stands at adult height, ready stance, repulsor glowing on one palm. Photoreal cinematic depth.",
    },
]

for t in TESTS:
    out = WS / "stills" / f"{t['sid']}.png"
    if out.exists() and out.stat().st_size > 1024:
        print(f"[skip] {t['sid']} cached")
        continue
    prompt = assemble_scene_prompt(t["scene"], t["outfit"], mint_bg=False)
    print(f"\n=== {t['sid']} ===")
    print(f"PROMPT:\n{prompt}\n")
    ts = time.time()
    gen_still("seedream_45", prompt, out, negative_prompt=NEG_STILL)
    print(f"rendered in {time.time()-ts:.1f}s -> {out}")

print("\nDONE — inspect", WS / "stills")
