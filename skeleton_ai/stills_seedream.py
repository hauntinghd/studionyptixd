"""
Seedream v4.5 client for Skeleton AI scene stills.

Endpoint: https://fal.run/fal-ai/bytedance/seedream/v4.5/text-to-image
Auth:     Authorization: Key {FAL_AI_KEY}

Premium params locked from the optimization bake-off:
  1440x2560, guidance_scale 7.0, num_inference_steps 80, safety_checker off.

Cost: $0.04 per image (flat rate, any resolution up to 4MP).
"""
from __future__ import annotations
import os
import time
import httpx
from pathlib import Path

from .fal_auth import require_fal_key

SEEDREAM_URL = "https://fal.run/fal-ai/bytedance/seedream/v4.5/text-to-image"


class SeedreamError(RuntimeError):
    pass


def _load_key() -> str:
    try:
        return require_fal_key("Seedream text-to-image")
    except RuntimeError as exc:
        raise SeedreamError(str(exc)) from exc


def generate(
    prompt: str,
    out_path: Path,
    *,
    negative_prompt: str = "",
    width: int = 1440,
    height: int = 2560,
    guidance_scale: float = 7.0,
    num_inference_steps: int = 80,
    api_key: str | None = None,
) -> Path:
    """Generate one still and save to out_path. Returns the Path."""
    out_path = Path(out_path)
    if out_path.exists() and out_path.stat().st_size > 1024:
        return out_path

    key = api_key or _load_key()
    payload = {
        "prompt": prompt[:759],
        "negative_prompt": negative_prompt[:1500],
        "image_size": {"width": width, "height": height},
        "num_images": 1,
        "guidance_scale": guidance_scale,
        "num_inference_steps": num_inference_steps,
        "enable_safety_checker": False,
    }
    headers = {"Authorization": f"Key {key}", "Content-Type": "application/json"}

    with httpx.Client(timeout=240, follow_redirects=True) as c:
        r = c.post(SEEDREAM_URL, json=payload, headers=headers)
    if r.status_code in (401, 403):
        raise SeedreamError(f"FAL auth failed {r.status_code}: {r.text[:200]}")
    if r.status_code not in (200, 201):
        raise SeedreamError(f"seedream {r.status_code}: {r.text[:300]}")

    data = r.json()
    images = data.get("images") or []
    if not images:
        raise SeedreamError(f"seedream returned no images: {data}")
    image_url = images[0].get("url")
    if not image_url:
        raise SeedreamError(f"seedream image entry missing url: {images[0]}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    _download(image_url, out_path)
    return out_path


def _download(url: str, dest: Path, retries: int = 3) -> None:
    last_exc = None
    for attempt in range(retries):
        try:
            with httpx.stream("GET", url, timeout=180) as r:
                if r.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"server {r.status_code}", request=r.request, response=r
                    )
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r.iter_bytes(chunk_size=1024 * 256):
                        f.write(chunk)
            return
        except (httpx.HTTPError, httpx.RequestError) as e:
            last_exc = e
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
    if last_exc:
        raise last_exc
