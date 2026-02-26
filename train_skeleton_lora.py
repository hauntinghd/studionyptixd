"""
NYPTID Skeleton LoRA Training Script for RunPod A40 (48GB VRAM)
DreamBooth + LoRA fine-tuning on SDXL base 1.0.

Run on RunPod after uploading the skeleton_training_dataset/ folder.
"""

import subprocess
import sys
import os
import json
import shutil
from pathlib import Path

WORKSPACE = Path("/workspace")
DATASET_DIR = WORKSPACE / "skeleton_training_dataset"
TRAINING_DIR = WORKSPACE / "skeleton_lora_training"
OUTPUT_DIR = WORKSPACE / "skeleton_lora_output"
SDXL_MODEL = WORKSPACE / "ComfyUI/models/checkpoints/sd_xl_base_1.0.safetensors"
HF_MODEL_ID = "stabilityai/stable-diffusion-xl-base-1.0"

TRIGGER_TOKEN = "nyptid_skeleton"
LORA_RANK = 64
LEARNING_RATE = 1e-4
TEXT_ENCODER_LR = 5e-5
MAX_TRAIN_STEPS = 2000
TRAIN_BATCH_SIZE = 1
GRADIENT_ACCUMULATION = 4
RESOLUTION = 1024
LR_SCHEDULER = "cosine"
LR_WARMUP_STEPS = 100
SAVE_EVERY_N_STEPS = 500
MIXED_PRECISION = "fp16"
SEED = 42


def install_deps():
    print("[1/5] Installing training dependencies...")
    pkgs = [
        "accelerate",
        "transformers",
        "diffusers[torch]",
        "peft",
        "bitsandbytes",
        "datasets",
        "safetensors",
        "Pillow",
        "prodigyopt",
        "tensorboard",
        "xformers",
    ]
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade"] + pkgs, stdout=subprocess.DEVNULL)
    subprocess.check_call([sys.executable, "-m", "pip", "install", "git+https://github.com/huggingface/diffusers.git"], stdout=subprocess.DEVNULL)
    print("  Dependencies installed.")


def prepare_dataset():
    print("[2/5] Preparing dataset...")
    TRAINING_DIR.mkdir(parents=True, exist_ok=True)
    instance_dir = TRAINING_DIR / "instance_images"
    instance_dir.mkdir(exist_ok=True)

    captions_src = DATASET_DIR / "captions"
    images = sorted(DATASET_DIR.glob("*.png"))

    if not images:
        raise FileNotFoundError(f"No PNG images found in {DATASET_DIR}")

    count = 0
    for img_path in images:
        dst_img = instance_dir / img_path.name
        if not dst_img.exists():
            shutil.copy2(img_path, dst_img)

        caption_file = captions_src / img_path.name.replace(".png", ".txt")
        dst_caption = instance_dir / img_path.name.replace(".png", ".txt")
        if caption_file.exists() and not dst_caption.exists():
            shutil.copy2(caption_file, dst_caption)
        elif not dst_caption.exists():
            with open(dst_caption, "w") as f:
                f.write(f"{TRIGGER_TOKEN}, photorealistic 3D skeleton, teal studio backdrop")
        count += 1

    print(f"  Prepared {count} image-caption pairs in {instance_dir}")
    return instance_dir


def run_training(instance_dir: Path):
    print("[3/5] Starting DreamBooth LoRA training...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, "-m", "accelerate", "launch",
        "--mixed_precision", MIXED_PRECISION,
        "--num_processes", "1",
        "-m", "diffusers.examples.dreambooth.train_dreambooth_lora_sdxl",
        "--pretrained_model_name_or_path", HF_MODEL_ID,
        "--instance_data_dir", str(instance_dir),
        "--output_dir", str(OUTPUT_DIR),
        "--instance_prompt", TRIGGER_TOKEN,
        "--resolution", str(RESOLUTION),
        "--train_batch_size", str(TRAIN_BATCH_SIZE),
        "--gradient_accumulation_steps", str(GRADIENT_ACCUMULATION),
        "--learning_rate", str(LEARNING_RATE),
        "--text_encoder_lr", str(TEXT_ENCODER_LR),
        "--lr_scheduler", LR_SCHEDULER,
        "--lr_warmup_steps", str(LR_WARMUP_STEPS),
        "--max_train_steps", str(MAX_TRAIN_STEPS),
        "--rank", str(LORA_RANK),
        "--seed", str(SEED),
        "--mixed_precision", MIXED_PRECISION,
        "--train_text_encoder",
        "--enable_xformers_memory_efficient_attention",
        "--gradient_checkpointing",
        "--checkpointing_steps", str(SAVE_EVERY_N_STEPS),
        "--validation_prompt", f"{TRIGGER_TOKEN}, single skeleton, NASCAR driver in racing suit, standing with arms crossed, teal studio backdrop, photorealistic 3D render, 8K",
        "--validation_epochs", "5",
        "--report_to", "tensorboard",
        "--logging_dir", str(OUTPUT_DIR / "logs"),
    ]

    print(f"  Command: {' '.join(cmd[:10])}...")
    print(f"  Steps: {MAX_TRAIN_STEPS} | Batch: {TRAIN_BATCH_SIZE} | Grad Accum: {GRADIENT_ACCUMULATION}")
    print(f"  Effective batch: {TRAIN_BATCH_SIZE * GRADIENT_ACCUMULATION}")
    print(f"  LoRA Rank: {LORA_RANK} | LR: {LEARNING_RATE} | Text Encoder LR: {TEXT_ENCODER_LR}")
    print(f"  Resolution: {RESOLUTION}x{RESOLUTION}")
    print(f"  Output: {OUTPUT_DIR}")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    log_file = OUTPUT_DIR / "training.log"
    with open(log_file, "w") as lf:
        for line in process.stdout:
            print(line, end="")
            lf.write(line)

    process.wait()
    if process.returncode != 0:
        print(f"\n  [ERROR] Training failed with exit code {process.returncode}")
        print(f"  Check log: {log_file}")
        return False

    print(f"\n  Training complete! LoRA saved to {OUTPUT_DIR}")
    return True


def copy_lora_to_comfyui():
    print("[4/5] Copying LoRA to ComfyUI...")
    comfy_lora_dir = WORKSPACE / "ComfyUI/models/loras"
    comfy_lora_dir.mkdir(parents=True, exist_ok=True)

    lora_files = list(OUTPUT_DIR.glob("*.safetensors"))
    if not lora_files:
        lora_files = list(OUTPUT_DIR.glob("pytorch_lora_weights*.safetensors"))

    if not lora_files:
        for checkpoint_dir in sorted(OUTPUT_DIR.glob("checkpoint-*")):
            lora_files.extend(checkpoint_dir.glob("*.safetensors"))

    if not lora_files:
        print("  [WARN] No .safetensors LoRA files found in output. Check training output.")
        return None

    best_lora = lora_files[-1]
    dest = comfy_lora_dir / "nyptid_skeleton_v1.safetensors"
    shutil.copy2(best_lora, dest)
    print(f"  LoRA copied: {best_lora.name} -> {dest}")
    return str(dest)


def save_training_info(lora_path):
    print("[5/5] Saving training metadata...")
    info = {
        "model_name": "nyptid_skeleton_v1",
        "trigger_token": TRIGGER_TOKEN,
        "base_model": HF_MODEL_ID,
        "training_method": "DreamBooth LoRA (SDXL)",
        "lora_rank": LORA_RANK,
        "learning_rate": LEARNING_RATE,
        "text_encoder_lr": TEXT_ENCODER_LR,
        "max_train_steps": MAX_TRAIN_STEPS,
        "resolution": RESOLUTION,
        "mixed_precision": MIXED_PRECISION,
        "lora_path": lora_path,
        "comfyui_lora_path": "models/loras/nyptid_skeleton_v1.safetensors",
        "usage_prompt_template": f"{TRIGGER_TOKEN}, single glossy white skeleton wearing [OUTFIT], [POSE], [PROP], solid teal mint green studio backdrop, photorealistic 3D render, Unreal Engine 5, 8K",
        "negative_prompt": "blurry, low quality, text, watermark, deformed, ugly, bad anatomy, non-skeleton, human skin, flesh, muscles, realistic human, cartoon, anime, painting, 2D, illustration",
    }
    info_path = OUTPUT_DIR / "training_info.json"
    with open(info_path, "w") as f:
        json.dump(info, f, indent=2)
    print(f"  Training info saved to {info_path}")
    return info


def main():
    print("=" * 60)
    print("  NYPTID Skeleton LoRA Trainer")
    print(f"  GPU: {os.environ.get('NVIDIA_VISIBLE_DEVICES', 'auto')}")
    print(f"  Dataset: {DATASET_DIR}")
    print("=" * 60)

    if not DATASET_DIR.exists():
        print(f"\nERROR: Dataset not found at {DATASET_DIR}")
        print("Upload the skeleton_training_dataset/ folder to /workspace/ first.")
        return

    install_deps()
    instance_dir = prepare_dataset()
    ok = run_training(instance_dir)
    if not ok:
        return

    lora_path = copy_lora_to_comfyui()
    info = save_training_info(lora_path)

    print("\n" + "=" * 60)
    print("  TRAINING COMPLETE!")
    print(f"  LoRA: {lora_path}")
    print(f"  Trigger token: {TRIGGER_TOKEN}")
    print(f"  Usage: include '{TRIGGER_TOKEN}' at the start of your prompt")
    print("=" * 60)


if __name__ == "__main__":
    main()
