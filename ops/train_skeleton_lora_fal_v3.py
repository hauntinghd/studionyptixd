"""Retrain NYPTID Skeleton LoRA on v3 dataset ONLY (240 images with
large eyeballs + translucent gel shell baked in). Reuses the fal
submit+poll pipeline from train_skeleton_lora_fal.py with a
different input glob.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import zipfile
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
CAPTIONS_DIR = DATASET_DIR / "captions"
ZIP_PATH = DATASET_DIR / "lora_training_v3_subset.zip"
RESULT_PATH = DATASET_DIR / "lora_training_result_v3.json"
RESUME_PATH = DATASET_DIR / "lora_training_v3_in_progress.json"

TRIGGER_WORD = "nyptid_skeleton"
STEPS = 1000


def main():
    v3_images = sorted(DATASET_DIR.glob("skeleton_3*.png"))
    print(f"v3 images: {len(v3_images)}")
    if not v3_images:
        print("ERROR: no v3 images")
        return

    # Zip images + matched captions
    if ZIP_PATH.exists():
        ZIP_PATH.unlink()
    with zipfile.ZipFile(ZIP_PATH, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for img in v3_images:
            zf.write(img, arcname=img.name)
            caption = CAPTIONS_DIR / f"{img.stem}.txt"
            if caption.exists():
                zf.write(caption, arcname=f"{img.stem}.txt")
    print(f"Zip built: {ZIP_PATH} ({ZIP_PATH.stat().st_size / 1024 / 1024:.1f} MB)")

    print("Uploading to fal...")
    url = fal_client.upload_file(str(ZIP_PATH))
    print(f"Uploaded: {url}")

    print(f"Submitting flux-lora-fast-training (steps={STEPS})...")
    handler = fal_client.submit(
        "fal-ai/flux-lora-fast-training",
        arguments={
            "images_data_url": url,
            "trigger_word": TRIGGER_WORD,
            "steps": STEPS,
            "create_masks": True,
        },
    )
    request_id = getattr(handler, "request_id", None) or str(handler)
    print(f"Request ID: {request_id}")
    RESUME_PATH.write_text(json.dumps({
        "request_id": request_id,
        "images_data_url": url,
        "steps": STEPS,
        "trigger_word": TRIGGER_WORD,
        "submitted_at": time.time(),
    }, indent=2))

    last_status = ""
    while True:
        try:
            status = handler.status(with_logs=True)
        except Exception as e:
            print(f"[WARN] status() failed: {e}, retry 10s")
            time.sleep(10)
            continue
        status_str = type(status).__name__
        if status_str != last_status:
            print(f"Status: {status_str}")
            last_status = status_str
        logs = getattr(status, "logs", None) or []
        for entry in logs[-3:]:
            msg = entry.get("message", "") if isinstance(entry, dict) else str(entry)
            safe = msg.encode("ascii", errors="replace").decode("ascii")
            print(f"  [fal] {safe[:150]}")
        if status_str == "Completed":
            break
        time.sleep(15)

    result = handler.get()
    record = {
        "trigger_word": TRIGGER_WORD,
        "steps": STEPS,
        "subset_count": len(v3_images),
        "images_data_url": url,
        "completed_at": time.time(),
        "result": result,
    }
    RESULT_PATH.write_text(json.dumps(record, indent=2, default=str))
    print(f"\nDONE. Result saved: {RESULT_PATH}")
    lora_file = result.get("diffusers_lora_file")
    if isinstance(lora_file, dict):
        print(f"LoRA URL: {lora_file.get('url')}")
    else:
        print(f"LoRA URL: {lora_file}")


if __name__ == "__main__":
    main()
