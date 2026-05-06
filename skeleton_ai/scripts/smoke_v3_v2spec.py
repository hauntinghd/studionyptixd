"""Re-render Marvel vs DC stills with v2 spec — no mint default, scene_action drives setting."""
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

from skeleton_ai.scripting_grok import GrokClient, build_script_prompt
from skeleton_ai.pipeline import analyze_script, derive_beat_visuals, split_script_into_beats
from skeleton_ai.prompts.base_style import assemble_scene_prompt, NEG_STILL
from skeleton_ai.prompts.idea_lists import get_category
from skeleton_ai.stills_engine import generate as gen_still

WS = ROOT / "skeleton_ai" / "output" / "smoke_v3_v2spec"
TOPIC = "Iron Man vs Doctor Strange. Who would win?"
CAT_KEY = "marvel_vs_dc"

WS.mkdir(parents=True, exist_ok=True)
(WS / "stills").mkdir(exist_ok=True)
grok = GrokClient()
cat = get_category(CAT_KEY)
print("[1/4] script gen...")
user_prompt = build_script_prompt(cat["system_prompt"], TOPIC)
script = grok.complete(cat["system_prompt"], user_prompt, max_tokens=1500)
(WS / "script.txt").write_text(script, encoding="utf-8")
print(f"  {len(script)} chars")

print("[2/4] analyze_script...")
plan = analyze_script(grok, script, category_label=cat["label"], topic=TOPIC)
(WS / "scene_plan.json").write_text(json.dumps(plan, indent=2, ensure_ascii=False), encoding="utf-8")
print(f"  characters: {list((plan.get('characters') or {}).keys())}")

sentences = split_script_into_beats(script, target_count=12)
print(f"[3/4] {len(sentences)} beats. Rendering...")
beats_meta = []
for i, narration in enumerate(sentences[:6]):  # only 6 stills for cost — verify spec works
    sid = f"b{i:02d}"
    outfit, action, motion = derive_beat_visuals(grok, narration, cat["label"], plan=plan)
    prompt = assemble_scene_prompt(action, outfit, mint_bg=False)
    print(f"  [{i+1}/6] {sid}")
    print(f"    PROMPT: {prompt[:280]}...")
    ts = time.time()
    gen_still("seedream_45", prompt, WS / "stills" / f"{sid}.png", negative_prompt=NEG_STILL)
    print(f"    rendered in {time.time()-ts:.1f}s")
    beats_meta.append({"sid": sid, "narration": narration, "outfit": outfit, "action": action})
(WS / "beats.json").write_text(json.dumps(beats_meta, indent=2, ensure_ascii=False), encoding="utf-8")
print("[4/4] DONE — inspect", WS / "stills")
