"""v4 canonical-restored spec test — mint backdrop + dot pupils + opaque costume + props-on-mint."""
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

WS = ROOT / "skeleton_ai" / "output" / "smoke_v5_canonical"
WS.mkdir(parents=True, exist_ok=True)
(WS / "stills").mkdir(exist_ok=True)

TESTS = [
    {
        "sid": "canon_mcd",
        "outfit": "red short-sleeve polo with golden M arches logo on chest, name tag pinned over the heart, brown trousers, black belt, red visor cap with white top",
        "scene": "Standing behind a McDonald's grill, real burger patties cooking on the hot grill with steam rising, real customers eating at tables in the background, McDonald's menu boards on the wall behind. The character holds a metal spatula in one hand. Composited on the mint-green stage with photoreal kitchen equipment and real customers as props.",
    },
    {
        "sid": "canon_iron",
        "outfit": "full red-and-gold Iron Man Mark 85 plate armor covering the entire body — chest plate with the circular blue glowing arc reactor, gauntlets, shoulder pauldrons, hip plates, leg armor, boot plates. The helmet sits open at the faceplate revealing the skull above the gold neck collar. Bony skeletal hands appear at the very edge of the gauntlet wrist openings",
        "scene": "Standing in a high-tech Stark lab interior with holographic blue interface displays floating at chest height, Mark suit-up rigs against the walls, JARVIS-style ambient blue light. Composited on the mint-green stage with real lab equipment props and a real engineer figure at a workstation in the background.",
    },
    {
        "sid": "canon_thor",
        "outfit": "wears a long-sleeved black tunic completely covering the entire torso, abdomen, and both arms (sleeves go all the way to the wrists, no bare skin or bones showing on chest or arms). OVER the black tunic sits a SOLID silver-and-gold opaque metal chest plate with five circular bosses arranged across it — the chest plate is plain solid metal with NO ribcage carving, NO skeletal motifs, NO anatomical engraving on its surface. Brown leather wrist bracers wrap each forearm cuff. A red flowing cape drapes from the shoulders. Black trousers and silver-and-gold greaves cover the legs to the boots. A winged silver helmet sits atop the skull — skull visible between the wings, no faceplate covering the skull",
        "scene": "Standing in the golden Asgardian throne hall — polished gold pillars, raised dais with Odin's throne behind, real Asgardian citizens in tunics seated at the sides watching, the rainbow Bifrost arc visible through tall arched windows. Composited on the mint-green stage with photoreal Asgardian architecture and citizen figures. The character grips Mjolnir, hammer raised.",
    },
    {
        "sid": "canon_hulk",
        "outfit": "[BARE_TORSO] only torn purple ripped trousers covering the lower body — bare-chested, full anatomical ribcage and torso visible. Faint green energy aura hugging the body as a rage glow.",
        "scene": "Standing in a destroyed Manhattan street with cracked asphalt, real overturned yellow taxis, debris and rubble piles, a shattered fire hydrant spraying water in the background, real terrified pedestrians fleeing. Composited on the mint-green stage with photoreal destruction props.",
    },
]

for t in TESTS:
    out = WS / "stills" / f"{t['sid']}.png"
    if out.exists() and out.stat().st_size > 1024:
        print(f"[skip] {t['sid']} cached")
        continue
    prompt = assemble_scene_prompt(t["scene"], t["outfit"], mint_bg=True)
    print(f"\n=== {t['sid']} ===")
    print(f"PROMPT:\n{prompt}\n")
    ts = time.time()
    gen_still("seedream_45", prompt, out, negative_prompt=NEG_STILL)
    print(f"rendered in {time.time()-ts:.1f}s -> {out}")

print("\nDONE — inspect", WS / "stills")
