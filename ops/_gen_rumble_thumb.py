"""One-off: hyperreal Rumble click thumbnail via Fal Seedream."""
from __future__ import annotations

import json
import os
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
SEEDREAM_URL = "https://fal.run/fal-ai/bytedance/seedream/v4.5/text-to-image"


def _load_env() -> None:
    for candidate in (ROOT / ".env", ROOT.parent / ".env", Path(r"D:\Games\asd\.env")):
        if not candidate.is_file():
            continue
        for line in candidate.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))
        break


def main() -> None:
    _load_env()
    key = os.getenv("FAL_AI_KEY", "").strip()
    if not key:
        raise SystemExit("FAL_AI_KEY missing")

    prompt = (
        "Ultra-photoreal hyperrealistic viral video platform thumbnail, 16:9 cinematic frame. "
        "A tense whistleblower moment: middle-aged man in rumpled dress shirt and loosened tie "
        "in a dim parking garage at night, rain-slick concrete, single harsh overhead sodium light. "
        "He holds a manila folder stamped TOP SECRET in red ink toward camera; expression is controlled fury "
        "and disbelief. Background: blurred black SUV and chain-link fence. "
        "Shallow depth of field, film grain, documentary thriller color grade, teal shadows and warm skin tones. "
        "No logos, no text overlays, no watermarks, no platform UI. "
        "Feels like the frame before a viral exposé drops — irresistible curiosity gap."
    )
    neg = (
        "cartoon, anime, illustration, skeleton, deformed face, extra fingers, blur, low quality, "
        "watermark, logo, text, meme, gore, child"
    )
    payload = {
        "prompt": prompt,
        "negative_prompt": neg,
        "image_size": {"width": 1280, "height": 720},
        "num_images": 1,
        "guidance_scale": 5.5,
        "num_inference_steps": 50,
        "enable_safety_checker": True,
        "seed": 880042,
    }

    print("Generating via Fal Seedream v4.5...")
    response = httpx.post(
        SEEDREAM_URL,
        json=payload,
        headers={"Authorization": f"Key {key}", "Content-Type": "application/json"},
        timeout=300,
    )
    response.raise_for_status()
    images = list((response.json() or {}).get("images") or [])
    if not images:
        raise SystemExit(f"no images returned: {response.text[:400]}")

    img_url = str(images[0].get("url") or "").strip()
    out = ROOT / "analysis" / "rumble_hyperreal_clickbait.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    with httpx.stream("GET", img_url, timeout=180) as stream:
        stream.raise_for_status()
        out.write_bytes(stream.read())

    meta_path = ROOT / "analysis" / "rumble_hyperreal_clickbait.json"
    meta_path.write_text(
        json.dumps(
            {"prompt": prompt, "seed": 880042, "cdn_url": img_url, "local_path": str(out)},
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"Saved: {out}")
    print(f"CDN:   {img_url}")


if __name__ == "__main__":
    main()
