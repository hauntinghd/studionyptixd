"""
Train the NYPTID Skeleton LoRA on fal.ai.

Uses fal-ai/flux-lora-fast-training. Default cost: $2 base (scales with
steps). Casey's remaining fal balance: $19.95, so we have room for
~10 iterations at default steps.

Pipeline:
  1. Pick a curated ~200-image subset from skeleton_training_dataset/
     (one or two images per profession/outfit category — LoRA quality
     rewards VARIETY over quantity).
  2. Assemble a zip containing images + their paired caption .txt files.
  3. Upload the zip to fal storage (returns a fal CDN URL).
  4. POST to fal-ai/flux-lora-fast-training with trigger_word
     `nyptid_skeleton`.
  5. Poll until training finishes.
  6. Save the `diffusers_lora_file` URL + config URL to
     skeleton_training_dataset/lora_training_result.json so the backend
     can reference it from the image gen pipeline.

Usage:
  python train_skeleton_lora_fal.py [--dry-run] [--target-count 200] [--steps 1000]
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import time
import zipfile
from collections import defaultdict
from pathlib import Path

import fal_client
from dotenv import load_dotenv

# Windows console default is cp1252 — fal's training logs contain characters
# (progress bar glyphs, unicode) that crash print() when the codec can't
# encode them. Reconfigure stdout to UTF-8 with safe fallback, otherwise a
# single bad log line kills the whole subscribe() loop and burns the $2
# training run.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

load_dotenv()

DATASET_DIR = Path("skeleton_training_dataset")
CAPTIONS_DIR = DATASET_DIR / "captions"
RESULT_PATH = DATASET_DIR / "lora_training_result.json"
ZIP_PATH = DATASET_DIR / "lora_training_subset.zip"

TRIGGER_WORD = "nyptid_skeleton"


def _extract_category(filename: str) -> str:
    """Pull the category suffix out of a skeleton_NNNN_<category>.png name."""
    stem = Path(filename).stem
    # stem = "skeleton_0045_business_CEO"
    m = re.match(r"skeleton_\d+_(.+)$", stem)
    if not m:
        return "other"
    return m.group(1).lower()


def _pick_diverse_subset(all_images: list[Path], target_count: int) -> list[Path]:
    """Pick up to `target_count` images with max variety across categories."""
    by_cat: dict[str, list[Path]] = defaultdict(list)
    for p in all_images:
        by_cat[_extract_category(p.name)].append(p)
    # Sort within each category so the selection is reproducible
    for cat in by_cat:
        by_cat[cat].sort()
    # Round-robin: pick 1 from each category, then 2, then 3, etc.,
    # until we hit target_count.
    picked: list[Path] = []
    cats = sorted(by_cat.keys())
    rng = random.Random(2026)
    rr_idx = 0
    while len(picked) < target_count and any(by_cat[c] for c in cats):
        # For each category, pick one image this round
        for cat in cats:
            if not by_cat[cat]:
                continue
            # Use a deterministic-yet-spread pick across each category
            idx = rr_idx % len(by_cat[cat])
            picked.append(by_cat[cat].pop(idx))
            if len(picked) >= target_count:
                break
        rr_idx += 1
    # Shuffle final selection so LoRA training doesn't see category clusters
    rng.shuffle(picked)
    return picked[:target_count]


def _assemble_zip(images: list[Path], zip_path: Path) -> int:
    """Build a zip containing images + their paired caption .txt files.

    Returns byte size of the zip.
    """
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for img in images:
            zf.write(img, arcname=img.name)
            # Pair caption file (same stem, .txt suffix in captions/)
            caption_path = CAPTIONS_DIR / f"{img.stem}.txt"
            if caption_path.exists():
                zf.write(caption_path, arcname=f"{img.stem}.txt")
    return zip_path.stat().st_size


def _upload_to_fal(zip_path: Path) -> str:
    """Upload the zip to fal storage. Returns a public-ish CDN URL."""
    print(f"  Uploading {zip_path} ({zip_path.stat().st_size / 1024 / 1024:.1f} MB) to fal storage…")
    url = fal_client.upload_file(str(zip_path))
    print(f"  Upload done: {url}")
    return url


def _trigger_training(images_data_url: str, steps: int) -> dict:
    """Submit + poll flux-lora-fast-training.

    Uses submit() + status()/result() instead of subscribe() so the
    request_id can be written to disk IMMEDIATELY. If anything crashes
    mid-poll, Casey can resume with the saved request_id instead of
    losing the $2 training run.
    """
    print(f"  Submitting flux-lora-fast-training (steps={steps}, trigger_word={TRIGGER_WORD})...")

    handler = fal_client.submit(
        "fal-ai/flux-lora-fast-training",
        arguments={
            "images_data_url": images_data_url,
            "trigger_word": TRIGGER_WORD,
            "steps": steps,
            "create_masks": True,
        },
    )
    request_id = getattr(handler, "request_id", None) or str(handler)
    print(f"  Request ID: {request_id}")

    # Persist the request_id to disk IMMEDIATELY so we can always resume
    # polling even if this process dies.
    resume_path = DATASET_DIR / "lora_training_in_progress.json"
    resume_path.write_text(json.dumps({
        "request_id": request_id,
        "images_data_url": images_data_url,
        "steps": steps,
        "trigger_word": TRIGGER_WORD,
        "submitted_at": time.time(),
    }, indent=2))
    print(f"  Resume info saved: {resume_path}")

    # Poll until done. Keep log lines ASCII-safe so Windows cp1252 never
    # crashes us again.
    last_logged_status = ""
    while True:
        try:
            status = handler.status(with_logs=True)
        except Exception as e:
            print(f"  [WARN] status() failed: {e}. Retrying in 10s...")
            time.sleep(10)
            continue
        # Surface recent logs
        logs = getattr(status, "logs", None) or []
        for entry in logs[-5:]:
            msg = entry.get("message", "") if isinstance(entry, dict) else str(entry)
            safe = msg.encode("ascii", errors="replace").decode("ascii")
            print(f"    [fal] {safe[:200]}")
        status_str = type(status).__name__
        if status_str != last_logged_status:
            print(f"  Status: {status_str}")
            last_logged_status = status_str
        if status_str == "Completed":
            break
        time.sleep(15)

    result = handler.get()
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target-count", type=int, default=200,
                        help="Number of images to include in the training subset (default 200)")
    parser.add_argument("--steps", type=int, default=1000,
                        help="Training steps (default 1000 — fal docs say cost scales linearly)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Pick images + zip them, but do NOT upload or trigger training")
    args = parser.parse_args()

    if not os.getenv("FAL_KEY") and not os.getenv("FAL_AI_KEY"):
        print("ERROR: FAL_KEY or FAL_AI_KEY not set in env/.env")
        return
    # fal_client reads FAL_KEY by convention; alias from FAL_AI_KEY if needed
    if not os.getenv("FAL_KEY") and os.getenv("FAL_AI_KEY"):
        os.environ["FAL_KEY"] = os.getenv("FAL_AI_KEY", "")

    print(f"\n{'='*60}")
    print(f"  NYPTID Skeleton LoRA Trainer (fal.ai)")
    print(f"  Dataset: {DATASET_DIR.absolute()}")
    print(f"  Trigger word: {TRIGGER_WORD}")
    print(f"  Target subset: {args.target_count} images")
    print(f"  Steps: {args.steps}")
    print(f"  Dry run: {args.dry_run}")
    print(f"{'='*60}\n")

    all_images = sorted(p for p in DATASET_DIR.glob("skeleton_*.png") if p.is_file())
    print(f"  Total dataset images on disk: {len(all_images)}")

    subset = _pick_diverse_subset(all_images, args.target_count)
    print(f"  Diverse subset picked: {len(subset)} images")

    # Category breakdown — sanity check variety
    cat_counts: dict[str, int] = defaultdict(int)
    for p in subset:
        cat_counts[_extract_category(p.name)] += 1
    top_cats = sorted(cat_counts.items(), key=lambda x: -x[1])[:10]
    print(f"  Top 10 categories in subset:")
    for cat, n in top_cats:
        print(f"    {cat}: {n}")

    zip_bytes = _assemble_zip(subset, ZIP_PATH)
    print(f"  Zip built: {ZIP_PATH} ({zip_bytes / 1024 / 1024:.1f} MB)")

    if args.dry_run:
        print("\n  DRY RUN — skipping upload + training. Exiting.")
        return

    uploaded_url = _upload_to_fal(ZIP_PATH)
    result = _trigger_training(uploaded_url, args.steps)

    # Save result to disk so the backend wire-up step can reference it later
    record = {
        "trigger_word": TRIGGER_WORD,
        "steps": args.steps,
        "subset_count": len(subset),
        "images_data_url": uploaded_url,
        "completed_at": time.time(),
        "result": result,
    }
    RESULT_PATH.write_text(json.dumps(record, indent=2, default=str))
    print(f"\n{'='*60}")
    print(f"  TRAINING COMPLETE")
    print(f"  LoRA weights URL: {result.get('diffusers_lora_file', {}).get('url') if isinstance(result.get('diffusers_lora_file'), dict) else result.get('diffusers_lora_file')}")
    print(f"  Config URL: {result.get('config_file', {}).get('url') if isinstance(result.get('config_file'), dict) else result.get('config_file')}")
    print(f"  Record saved: {RESULT_PATH}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
