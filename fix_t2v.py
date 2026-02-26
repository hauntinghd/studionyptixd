import os, shutil
os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "1"
os.environ["HF_XET_HIGH_PERFORMANCE"] = "1"
from huggingface_hub import hf_hub_download, login
login(token="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")

dest = "/workspace/ComfyUI/models/diffusion_models/wan2.2_t2v_high_noise_14B_fp8_scaled.safetensors"
if os.path.exists(dest):
    os.remove(dest)
print("DOWNLOADING T2V HIGH NOISE...")
p = hf_hub_download("Comfy-Org/Wan_2.2_ComfyUI_Repackaged", "split_files/diffusion_models/wan2.2_t2v_high_noise_14B_fp8_scaled.safetensors")
shutil.copy2(p, dest)
sz = os.path.getsize(dest) / (1024**3)
print(f"DONE T2V HIGH NOISE ({sz:.1f}GB)")
