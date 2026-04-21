"""Quick validation test for the trained Skeleton LoRA.

Generates ONE image using fal-ai/flux-lora with the trained LoRA URL
and the nyptid_skeleton trigger word. If the output shows a canonical
skeleton wearing an FBI suit + fedora in the Oval Office, the LoRA is
working. If it's nude, generic, or wrong identity, the LoRA is weak
and we need more training steps or a better subset.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import fal_client
from dotenv import load_dotenv

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

load_dotenv()

if not os.getenv("FAL_KEY") and os.getenv("FAL_AI_KEY"):
    os.environ["FAL_KEY"] = os.getenv("FAL_AI_KEY", "")

DATASET_DIR = Path("skeleton_training_dataset")
LORA_RESULT = json.loads((DATASET_DIR / "lora_training_result.json").read_text())
LORA_URL = LORA_RESULT["result"]["diffusers_lora_file"]["url"]
TRIGGER = LORA_RESULT["trigger_word"]

print(f"LoRA URL: {LORA_URL}")
print(f"Trigger: {TRIGGER}")

PROMPTS = [
    (
        "hoover_fbi",
        f"{TRIGGER}, canonical ivory-white anatomical skeleton wearing a dark 1920s pinstripe FBI suit with white collared shirt, black silk necktie, and black felt fedora. Standing confidently in the Oval Office holding a thick manila folder stamped CONFIDENTIAL. Period-accurate 1930s decor with American flag and presidential desk. Cinematic low-angle lighting, warm tungsten key light, film grain, photorealistic 3D render.",
    ),
    (
        "anime_uniform",
        f"{TRIGGER}, canonical ivory-white anatomical skeleton wearing a black slim-fit high-collar sorcerer uniform with silver trim, blindfold over eye sockets, dark fitted pants, black combat boots. Dynamic fighting stance mid-exchange against an anime rival. Mint green studio backdrop. Cinematic lighting, photorealistic 3D render.",
    ),
]

for label, prompt in PROMPTS:
    print(f"\n[{label}] Submitting...")
    try:
        result = fal_client.subscribe(
            "fal-ai/flux-lora",
            arguments={
                "prompt": prompt,
                "loras": [{"path": LORA_URL, "scale": 1.0}],
                "num_images": 1,
                "image_size": "portrait_16_9",
                "output_format": "png",
                "enable_safety_checker": True,
            },
        )
        images = result.get("images", [])
        if not images:
            print(f"  [FAIL] no images returned: {result}")
            continue
        url = images[0].get("url", "")
        ts = int(time.time())
        out_path = DATASET_DIR / f"lora_validation_{label}_{ts}.png"
        # Download
        import httpx
        with httpx.Client(timeout=60, follow_redirects=True) as client:
            r = client.get(url)
            if r.status_code == 200:
                out_path.write_bytes(r.content)
                print(f"  [OK] Saved: {out_path} ({out_path.stat().st_size / 1024:.0f} KB)")
            else:
                print(f"  [FAIL] image download HTTP {r.status_code}")
    except Exception as e:
        print(f"  [ERR] {type(e).__name__}: {e}")
