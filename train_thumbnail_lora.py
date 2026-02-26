"""
NYPTID Thumbnail LoRA Trainer
Continuously watches /workspace/thumbnail_training/images/ for new thumbnails.
When images arrive (or change), trains/refines an SDXL LoRA and saves to ComfyUI.
Designed for NVIDIA A40 (48GB VRAM).
"""

import os
import sys
import time
import hashlib
import json
import signal
import gc
from pathlib import Path

IMAGES_DIR = Path("/workspace/thumbnail_training/images")
OUTPUT_DIR = Path("/workspace/thumbnail_training/output")
LORA_OUTPUT = Path("/workspace/ComfyUI/models/loras/nyptid_thumbnails.safetensors")
STATE_FILE = OUTPUT_DIR / "training_state.json"
LOCK_FILE = OUTPUT_DIR / "training.lock"

IMAGES_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MIN_IMAGES_TO_TRAIN = 5
CHECK_INTERVAL = 30
TRAINING_STEPS = 800
LEARNING_RATE = 1e-4
LORA_RANK = 32
RESOLUTION = 1024
BATCH_SIZE = 1
GRADIENT_ACCUMULATION = 4


def get_image_hash(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def scan_images():
    valid_ext = {".png", ".jpg", ".jpeg", ".webp"}
    images = []
    for f in IMAGES_DIR.iterdir():
        if f.suffix.lower() in valid_ext and f.stat().st_size > 1000:
            images.append(f)
    return sorted(images, key=lambda p: p.stat().st_mtime)


def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"trained_hashes": [], "image_count": 0, "last_train": 0, "version": 0}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


def needs_training(images, state):
    current_hashes = set()
    for img in images:
        current_hashes.add(get_image_hash(img))
    trained_hashes = set(state.get("trained_hashes", []))
    new_images = current_hashes - trained_hashes
    if len(images) < MIN_IMAGES_TO_TRAIN:
        print(f"Only {len(images)} images, need {MIN_IMAGES_TO_TRAIN} minimum. Waiting...")
        return False, current_hashes
    if len(new_images) == 0:
        return False, current_hashes
    print(f"Found {len(new_images)} new images ({len(images)} total). Training needed.")
    return True, current_hashes


def create_dataset(images):
    from datasets import Dataset
    from PIL import Image as PILImage

    data = {"image": [], "text": []}
    caption = (
        "professional YouTube thumbnail, high contrast, bold colors, "
        "click-worthy composition, attention-grabbing design, "
        "viral thumbnail style, clean layout, high quality"
    )

    for img_path in images:
        try:
            img = PILImage.open(img_path).convert("RGB")
            w, h = img.size
            target_w, target_h = RESOLUTION, RESOLUTION
            if w / h > 1.5:
                target_w, target_h = RESOLUTION, int(RESOLUTION * 0.5625)
            elif h / w > 1.5:
                target_w, target_h = int(RESOLUTION * 0.5625), RESOLUTION
            img = img.resize((target_w, target_h), PILImage.LANCZOS)

            pad_img = PILImage.new("RGB", (RESOLUTION, RESOLUTION), (0, 0, 0))
            offset_x = (RESOLUTION - target_w) // 2
            offset_y = (RESOLUTION - target_h) // 2
            pad_img.paste(img, (offset_x, offset_y))

            data["image"].append(pad_img)
            data["text"].append(caption)
        except Exception as e:
            print(f"  Skipping {img_path.name}: {e}")

    return Dataset.from_dict(data)


def run_training(images):
    import torch
    from diffusers import StableDiffusionXLPipeline, AutoencoderKL
    from diffusers.optimization import get_scheduler
    from peft import LoraConfig, get_peft_model
    from transformers import CLIPTokenizer
    from PIL import Image as PILImage
    import numpy as np

    print(f"\n{'='*60}")
    print(f"  NYPTID LoRA Training - {len(images)} images")
    print(f"  Steps: {TRAINING_STEPS}, LR: {LEARNING_RATE}, Rank: {LORA_RANK}")
    print(f"{'='*60}\n")

    LOCK_FILE.write_text(json.dumps({"started": time.time(), "images": len(images)}))

    try:
        model_id = "stabilityai/stable-diffusion-xl-base-1.0"

        print("Loading VAE...")
        vae = AutoencoderKL.from_pretrained(model_id, subfolder="vae", torch_dtype=torch.float16)
        vae.to("cuda")
        vae.eval()

        print("Loading tokenizers...")
        tokenizer_1 = CLIPTokenizer.from_pretrained(model_id, subfolder="tokenizer")
        tokenizer_2 = CLIPTokenizer.from_pretrained(model_id, subfolder="tokenizer_2")

        print("Loading UNet...")
        from diffusers import UNet2DConditionModel
        unet = UNet2DConditionModel.from_pretrained(model_id, subfolder="unet", torch_dtype=torch.float16)
        unet.to("cuda")

        print("Loading text encoders...")
        from transformers import CLIPTextModel, CLIPTextModelWithProjection
        text_encoder_1 = CLIPTextModel.from_pretrained(model_id, subfolder="text_encoder", torch_dtype=torch.float16)
        text_encoder_2 = CLIPTextModelWithProjection.from_pretrained(model_id, subfolder="text_encoder_2", torch_dtype=torch.float16)
        text_encoder_1.to("cuda")
        text_encoder_2.to("cuda")
        text_encoder_1.eval()
        text_encoder_2.eval()

        lora_config = LoraConfig(
            r=LORA_RANK,
            lora_alpha=LORA_RANK,
            target_modules=["to_k", "to_q", "to_v", "to_out.0"],
            lora_dropout=0.05,
        )
        unet = get_peft_model(unet, lora_config)
        unet.print_trainable_parameters()
        unet.train()

        optimizer = torch.optim.AdamW(unet.parameters(), lr=LEARNING_RATE, weight_decay=1e-2)
        scheduler = get_scheduler(
            "cosine",
            optimizer=optimizer,
            num_warmup_steps=int(TRAINING_STEPS * 0.1),
            num_training_steps=TRAINING_STEPS,
        )

        noise_scheduler_config = {
            "num_train_timesteps": 1000,
            "beta_start": 0.00085,
            "beta_end": 0.012,
            "beta_schedule": "scaled_linear",
        }
        from diffusers import DDPMScheduler
        noise_scheduler = DDPMScheduler(**noise_scheduler_config)

        caption = (
            "professional YouTube thumbnail, high contrast, bold colors, "
            "click-worthy composition, attention-grabbing design, "
            "viral thumbnail style, clean layout, high quality"
        )

        print("Encoding caption...")
        with torch.no_grad():
            tokens_1 = tokenizer_1(caption, padding="max_length", max_length=77, truncation=True, return_tensors="pt").input_ids.to("cuda")
            tokens_2 = tokenizer_2(caption, padding="max_length", max_length=77, truncation=True, return_tensors="pt").input_ids.to("cuda")
            enc_1 = text_encoder_1(tokens_1).last_hidden_state
            enc_2_out = text_encoder_2(tokens_2)
            enc_2 = enc_2_out.last_hidden_state
            pooled = enc_2_out.text_embeds
            prompt_embeds = torch.cat([enc_1, enc_2], dim=-1)

        print("Pre-encoding images into latents...")
        all_latents = []
        for img_path in images:
            try:
                img = PILImage.open(img_path).convert("RGB")
                w, h = img.size
                tw, th = RESOLUTION, RESOLUTION
                if w / h > 1.5:
                    tw, th = RESOLUTION, int(RESOLUTION * 0.5625)
                elif h / w > 1.5:
                    tw, th = int(RESOLUTION * 0.5625), RESOLUTION
                img = img.resize((tw, th), PILImage.LANCZOS)
                pad_img = PILImage.new("RGB", (RESOLUTION, RESOLUTION), (0, 0, 0))
                pad_img.paste(img, ((RESOLUTION - tw) // 2, (RESOLUTION - th) // 2))

                arr = np.array(pad_img).astype(np.float32) / 127.5 - 1.0
                tensor = torch.from_numpy(arr).permute(2, 0, 1).unsqueeze(0).half().to("cuda")
                with torch.no_grad():
                    latent = vae.encode(tensor).latent_dist.sample() * vae.config.scaling_factor
                all_latents.append(latent)
            except Exception as e:
                print(f"  Skip {img_path.name}: {e}")

        if not all_latents:
            print("ERROR: No valid latents produced. Aborting.")
            return False

        del vae
        gc.collect()
        torch.cuda.empty_cache()
        print(f"Encoded {len(all_latents)} images. VAE freed. Starting training...\n")

        add_time_ids = torch.tensor([[RESOLUTION, RESOLUTION, 0, 0, RESOLUTION, RESOLUTION]], dtype=torch.float16, device="cuda")

        global_step = 0
        epoch = 0
        best_loss = float("inf")
        loss_history = []

        while global_step < TRAINING_STEPS:
            epoch += 1
            epoch_loss = 0.0
            indices = torch.randperm(len(all_latents))

            for idx in indices:
                if global_step >= TRAINING_STEPS:
                    break

                latent = all_latents[idx]
                noise = torch.randn_like(latent)
                timesteps = torch.randint(0, noise_scheduler.config.num_train_timesteps, (1,), device="cuda").long()
                noisy_latent = noise_scheduler.add_noise(latent, noise, timesteps)

                model_pred = unet(
                    noisy_latent,
                    timesteps,
                    encoder_hidden_states=prompt_embeds,
                    added_cond_kwargs={"text_embeds": pooled, "time_ids": add_time_ids},
                ).sample

                loss = torch.nn.functional.mse_loss(model_pred.float(), noise.float())
                loss = loss / GRADIENT_ACCUMULATION
                loss.backward()

                if (global_step + 1) % GRADIENT_ACCUMULATION == 0:
                    torch.nn.utils.clip_grad_norm_(unet.parameters(), 1.0)
                    optimizer.step()
                    scheduler.step()
                    optimizer.zero_grad()

                epoch_loss += loss.item() * GRADIENT_ACCUMULATION
                global_step += 1

                if global_step % 50 == 0:
                    avg = epoch_loss / (indices.tolist().index(idx.item()) + 1)
                    print(f"  Step {global_step}/{TRAINING_STEPS} | Loss: {loss.item() * GRADIENT_ACCUMULATION:.4f} | Avg: {avg:.4f} | LR: {scheduler.get_last_lr()[0]:.2e}")

            avg_epoch_loss = epoch_loss / max(len(all_latents), 1)
            loss_history.append(avg_epoch_loss)
            print(f"  Epoch {epoch} complete | Avg loss: {avg_epoch_loss:.4f}")

            if avg_epoch_loss < best_loss:
                best_loss = avg_epoch_loss

        print(f"\nTraining complete! Best loss: {best_loss:.4f}")
        print("Saving LoRA weights...")

        from peft import get_peft_model_state_dict
        from safetensors.torch import save_file

        lora_state = get_peft_model_state_dict(unet)
        cleaned_state = {}
        for k, v in lora_state.items():
            clean_key = k.replace("base_model.model.", "").replace(".default", "")
            cleaned_state[clean_key] = v.cpu().float()

        save_file(cleaned_state, str(LORA_OUTPUT))
        print(f"LoRA saved to {LORA_OUTPUT} ({LORA_OUTPUT.stat().st_size / 1024 / 1024:.1f} MB)")

        backup = OUTPUT_DIR / f"nyptid_thumbnails_v{load_state().get('version', 0) + 1}.safetensors"
        save_file(cleaned_state, str(backup))
        print(f"Backup saved to {backup}")

        del unet, text_encoder_1, text_encoder_2, optimizer
        del all_latents, prompt_embeds, pooled
        gc.collect()
        torch.cuda.empty_cache()

        return True

    except Exception as e:
        print(f"TRAINING ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        LOCK_FILE.unlink(missing_ok=True)


def main():
    print("=" * 60)
    print("  NYPTID Thumbnail LoRA Trainer")
    print(f"  Watching: {IMAGES_DIR}")
    print(f"  Output:   {LORA_OUTPUT}")
    print(f"  Min images: {MIN_IMAGES_TO_TRAIN}")
    print(f"  Check interval: {CHECK_INTERVAL}s")
    print("=" * 60)

    running = True
    def handle_signal(sig, frame):
        nonlocal running
        print("\nShutdown signal received. Finishing current cycle...")
        running = False
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    while running:
        try:
            images = scan_images()
            state = load_state()

            should_train, current_hashes = needs_training(images, state)
            if should_train:
                print(f"\n--- Starting training with {len(images)} images ---")
                success = run_training(images)
                if success:
                    state["trained_hashes"] = list(current_hashes)
                    state["image_count"] = len(images)
                    state["last_train"] = time.time()
                    state["version"] = state.get("version", 0) + 1
                    save_state(state)
                    print(f"State saved: v{state['version']}, {len(images)} images trained\n")
                else:
                    print("Training failed, will retry next cycle.\n")

            for i in range(CHECK_INTERVAL):
                if not running:
                    break
                time.sleep(1)

        except Exception as e:
            print(f"Loop error: {e}")
            time.sleep(10)

    print("Trainer shut down cleanly.")


if __name__ == "__main__":
    main()
