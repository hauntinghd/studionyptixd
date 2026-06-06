"""Seedream v4.5 text-to-image stills for styled shortform (non-skeleton)."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import httpx

SEEDREAM_T2I_URL = "https://fal.run/fal-ai/bytedance/seedream/v4.5/text-to-image"


class StyledStillError(RuntimeError):
    pass


def _ensure_fal() -> None:
    key = os.getenv("FAL_AI_KEY", "").strip()
    if not key:
        raise StyledStillError("FAL_AI_KEY not set")
    os.environ["FAL_KEY"] = key


def _download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with httpx.stream("GET", url, timeout=180) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=1024 * 256):
                f.write(chunk)


def build_styled_scene_prompt(
    *,
    style_prefix: str,
    scene_action: str,
    outfit: str = "",
    topic: str = "",
    visual_brief: str = "",
) -> str:
    parts = [style_prefix.strip()]
    if visual_brief:
        parts.append(f"CREATIVE LOCK: {visual_brief.strip()}")
    if outfit:
        parts.append(f"WARDROBE: {outfit.strip()}")
    if topic:
        parts.append(f"TOPIC: {topic.strip()}")
    parts.append(f"SCENE: {scene_action.strip()}")
    parts.append("Vertical 9:16 frame, single clear focal subject, premium production quality.")
    return " ".join(p for p in parts if p)[:3500]


def generate_still_t2i(
    prompt: str,
    out_path: Path,
    *,
    negative_prompt: str,
    seed: int = 420042,
) -> dict[str, Any]:
    out_path = Path(out_path)
    if out_path.exists() and out_path.stat().st_size > 1024:
        return {
            "local_path": str(out_path),
            "provider": "seedream_v45_t2i",
            "cached": True,
        }

    _ensure_fal()
    payload = {
        "prompt": str(prompt or "")[:3500],
        "negative_prompt": str(negative_prompt or "")[:1500],
        "image_size": {"width": 720, "height": 1280},
        "num_images": 1,
        "guidance_scale": 5.5,
        "num_inference_steps": 50,
        "enable_safety_checker": True,
        "seed": int(seed),
    }
    with httpx.Client(timeout=240, follow_redirects=True) as client:
        response = client.post(
            SEEDREAM_T2I_URL,
            json=payload,
            headers={
                "Authorization": f"Key {os.environ['FAL_KEY']}",
                "Content-Type": "application/json",
            },
        )
    if response.status_code not in (200, 201):
        raise StyledStillError(
            f"seedream t2i {response.status_code}: {response.text[:300]}"
        )
    images = list((response.json() or {}).get("images") or [])
    if not images:
        raise StyledStillError(f"seedream t2i returned no images: {response.text[:200]}")
    url = str((images[0] or {}).get("url") or "").strip()
    if not url:
        raise StyledStillError("seedream t2i image missing url")
    _download(url, out_path)
    return {
        "local_path": str(out_path),
        "cdn_url": url,
        "provider": "seedream_v45_t2i",
        "seed": seed,
        "bytes": out_path.stat().st_size,
    }
