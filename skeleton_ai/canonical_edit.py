"""
Canonical Skeleton — Seedream v4.5 edit lock.

One approved master still (gym dumbbell reference) is the identity anchor.
Every scene is an *edit* of that master: change background, props, and wardrobe
only. Mesh, skull, glass shell, and eyes stay fixed — no per-scene T2I drift.

Pattern mirrors long_form/pb_lies_cast_kit.py (master + roster + scene edits).
"""
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import fal_client
import httpx

SEEDREAM_EDIT_ENDPOINT = "fal-ai/bytedance/seedream/v4.5/edit"
SEEDREAM_EDIT_URL = f"https://fal.run/{SEEDREAM_EDIT_ENDPOINT}"
DEFAULT_SEED = int(os.getenv("SKELETON_CANONICAL_SEED", "420042"))

IDENTITY_LOCK = (
    "CANONICAL SKELETON EDIT LOCK: preserve the EXACT same character from the "
    "reference image — same ivory-white anatomical skeleton, same translucent "
    "glass body shell hugging the bones, same skull proportions, same realistic "
    "human eyes in the sockets, same limb lengths and pose mass. "
    "Do NOT redesign the character. Do NOT change bone color, eye style, or shell shape."
)

ARTIFACT_GUARD = (
    "Exactly ONE canonical skeleton host in frame unless the scene explicitly "
    "requires background people. Anatomically correct hands, no extra limbs, "
    "no melted skull, no cartoon eyes, no missing glass shell, photoreal 3D render. "
    "Wardrobe must be physically coherent and complete: shirts cover the torso as a real shirt, "
    "pants cover both legs as real pants, shoes fit both feet, sleeves and hems are clean, "
    "fabric never melts into bones or glass, no half-clothes, no floating straps, no torn accidental seams."
)

NEG_EDIT = (
    "different character, redesigned skeleton, alternate mascot, chibi, anime, "
    "cartoon eyes, glowing eyes, missing bones, exposed ribcage outside shell, "
    "opaque skin replacing glass, duplicate skeleton, twin bodies, "
    "melted clothing, fused fabric, incomplete pants, missing shoes, half shirt, "
    "extra fingers, broken hands, low quality, blurry, watermark, text overlay"
)


class CanonicalEditError(RuntimeError):
    pass


def _ensure_fal() -> None:
    key = os.getenv("FAL_AI_KEY", "").strip()
    if not key:
        raise CanonicalEditError("FAL_AI_KEY not set")
    os.environ["FAL_KEY"] = key


def _download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with httpx.stream("GET", url, timeout=180) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=1024 * 256):
                f.write(chunk)


def _reference_url_to_local(reference_url: str) -> Path | None:
    source = str(reference_url or "").strip()
    if not source:
        return None
    if Path(source).exists():
        return Path(source)
    try:
        parsed = urlparse(source)
    except Exception:
        return None
    if parsed.scheme not in {"http", "https"}:
        return None
    rel = parsed.path.lstrip("/")
    if not rel:
        return None
    # Vercel public assets served from ViralShorts-App/public
    candidates = [
        Path(__file__).resolve().parents[1] / "ViralShorts-App" / "public" / rel,
        Path(__file__).resolve().parents[1] / "ViralShorts-App" / "dist" / rel,
    ]
    for c in candidates:
        if c.is_file() and c.stat().st_size > 1024:
            return c
    return None


def resolve_master_reference_urls(
    *,
    master_url: str = "",
    extra_refs: list[str] | None = None,
) -> list[str]:
    """Return fal-uploadable URLs for edit (uploads local files when needed)."""
    _ensure_fal()
    urls: list[str] = []
    master = str(master_url or os.getenv("SKELETON_GLOBAL_REFERENCE_IMAGE_URL", "")).strip()
    if not master:
        public_dir = Path(__file__).resolve().parents[1] / "ViralShorts-App" / "public"
        for local_default in (
            public_dir / "canonical-skeleton-master-hires.png",
            public_dir / "canonical-skeleton-master.png",
        ):
            if local_default.is_file():
                master = str(local_default)
                break
        if not master:
            raise CanonicalEditError("SKELETON_GLOBAL_REFERENCE_IMAGE_URL not configured")

    local = _reference_url_to_local(master)
    if local:
        urls.append(fal_client.upload_file(str(local)))
    else:
        urls.append(master)

    for ref in list(extra_refs or []):
        ref = str(ref or "").strip()
        if not ref or ref in urls:
            continue
        local_extra = _reference_url_to_local(ref)
        if local_extra:
            urls.append(fal_client.upload_file(str(local_extra)))
        else:
            urls.append(ref)
    return urls[:3]


def build_scene_edit_prompt(
    *,
    topic: str = "",
    visual_description: str = "",
    outfit: str = "",
) -> str:
    """Prompt for background/prop/wardrobe delta only — identity comes from refs."""
    topic = str(topic or "").strip()
    visual = str(visual_description or "").strip()
    outfit = str(outfit or "").strip()
    parts = [IDENTITY_LOCK, ARTIFACT_GUARD]
    if outfit:
        parts.append(
            f"WARDROBE / BODY EDIT ONLY on the SAME canonical skeleton: {outfit}. "
            "Clothes, muscle definition, armor, or props are layered ON the existing glass shell and bones — "
            "never replace the skeleton, never swap to a human or new character, never remove the glass shell. "
            "If clothing is requested, render real finished garments: complete shirt/jacket, complete pants, "
            "matching shoes when visible, clean edges, believable fabric folds, no partial transparent fabric errors."
        )
    if topic:
        parts.append(f"TOPIC CONTEXT: {topic}.")
    if visual:
        parts.append(
            f"CHANGE ONLY the environment, camera, props, and pose as needed: {visual}. "
            "Keep the canonical skeleton character identical to the reference."
        )
    else:
        parts.append("Change only the background environment; keep the skeleton identical.")
    parts.append("Vertical 9:16 cinematic framing, premium commercial lighting, sharp focus.")
    return " ".join(parts)[:3500]


def generate_still_edit(
    prompt: str,
    out_path: Path,
    *,
    master_url: str = "",
    extra_refs: list[str] | None = None,
    seed: int = DEFAULT_SEED,
    negative_prompt: str = NEG_EDIT,
) -> dict[str, Any]:
    """Sync Seedream edit — one scene still from canonical master."""
    out_path = Path(out_path)
    if out_path.exists() and out_path.stat().st_size > 1024:
        return {"local_path": str(out_path), "provider": "seedream_v45_edit", "cached": True}

    _ensure_fal()
    image_urls = resolve_master_reference_urls(master_url=master_url, extra_refs=extra_refs)
    try:
        result = fal_client.subscribe(
            SEEDREAM_EDIT_ENDPOINT,
            arguments={
                "prompt": str(prompt or "")[:3500],
                "image_urls": image_urls,
                "negative_prompt": str(negative_prompt or NEG_EDIT)[:1500],
                "image_size": "auto_2K",
                "num_images": 1,
                "seed": int(seed),
            },
        )
    except Exception as exc:
        raise CanonicalEditError(f"seedream edit failed: {exc}") from exc

    images = list((result or {}).get("images") or [])
    if not images:
        raise CanonicalEditError(f"seedream edit returned no images: {result!r}")
    url = str((images[0] or {}).get("url") or "").strip()
    if not url:
        raise CanonicalEditError("seedream edit image missing url")

    _download(url, out_path)
    return {
        "local_path": str(out_path),
        "cdn_url": url,
        "provider": "seedream_v45_edit",
        "provider_label": "Seedream 4.5 Edit (canonical)",
        "seed": seed,
        "bytes": out_path.stat().st_size,
    }


async def generate_still_edit_async(
    prompt: str,
    out_path: str | Path,
    *,
    master_url: str = "",
    extra_refs: list[str] | None = None,
    seed: int = DEFAULT_SEED,
) -> dict[str, Any]:
    """Async wrapper for FastAPI pipeline (runs sync fal in thread if needed)."""
    import asyncio

    return await asyncio.to_thread(
        generate_still_edit,
        prompt,
        Path(out_path),
        master_url=master_url,
        extra_refs=extra_refs,
        seed=seed,
    )
