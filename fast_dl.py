import os, shutil, time
os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "1"
os.environ["HF_XET_HIGH_PERFORMANCE"] = "1"
from huggingface_hub import hf_hub_download, login

login(token="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")

BASE = "/workspace/ComfyUI/models"
downloads = [
    ("Comfy-Org/Wan_2.2_ComfyUI_Repackaged", "split_files/diffusion_models/wan2.2_t2v_high_noise_14B_fp8_scaled.safetensors", f"{BASE}/diffusion_models/wan2.2_t2v_high_noise_14B_fp8_scaled.safetensors"),
    ("Comfy-Org/Wan_2.2_ComfyUI_Repackaged", "split_files/diffusion_models/wan2.2_t2v_low_noise_14B_fp8_scaled.safetensors", f"{BASE}/diffusion_models/wan2.2_t2v_low_noise_14B_fp8_scaled.safetensors"),
    ("Comfy-Org/Wan_2.2_ComfyUI_Repackaged", "split_files/text_encoders/umt5_xxl_fp8_e4m3fn_scaled.safetensors", f"{BASE}/text_encoders/umt5_xxl_fp8_e4m3fn_scaled.safetensors"),
    ("Comfy-Org/Wan_2.2_ComfyUI_Repackaged", "split_files/vae/wan2.2_vae.safetensors", f"{BASE}/vae/wan2.2_vae.safetensors"),
    ("Comfy-Org/Wan_2.1_ComfyUI_repackaged", "split_files/clip_vision/clip_vision_h.safetensors", f"{BASE}/clip_vision/clip_vision_h.safetensors"),
    ("stabilityai/stable-diffusion-xl-base-1.0", "sd_xl_base_1.0.safetensors", f"{BASE}/checkpoints/sd_xl_base_1.0.safetensors"),
]

for repo, filename, dest in downloads:
    name = os.path.basename(dest)
    if os.path.exists(dest) and os.path.getsize(dest) > 1000000:
        sz = os.path.getsize(dest) / (1024**3)
        print(f"SKIP {name} (already {sz:.1f}GB)")
        continue
    print(f"DOWNLOADING {name}...")
    t = time.time()
    path = hf_hub_download(repo_id=repo, filename=filename, resume_download=True)
    shutil.copy2(path, dest)
    elapsed = time.time() - t
    size = os.path.getsize(dest) / (1024**3)
    speed = (size * 1024) / elapsed if elapsed > 0 else 0
    print(f"DONE {name} ({size:.1f}GB in {elapsed:.0f}s = {speed:.1f} MB/s)")

print("ALL_DOWNLOADS_COMPLETE")
