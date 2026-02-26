#!/bin/bash
set -e
echo "=== NYPTID Studio - RunPod Full Setup ==="
echo "This will take ~30-60 min (mostly downloads)"
echo ""
echo ">>> [1/6] Updating ComfyUI..."
cd /workspace/ComfyUI
git fetch --all
git checkout master 2>/dev/null || git checkout main
git pull
pip install -r requirements.txt --quiet 2>/dev/null || true
echo "ComfyUI updated."
echo ""
echo ">>> [2/6] Creating model directories..."
mkdir -p /workspace/ComfyUI/models/diffusion_models
mkdir -p /workspace/ComfyUI/models/text_encoders
mkdir -p /workspace/ComfyUI/models/vae
mkdir -p /workspace/ComfyUI/models/clip_vision
mkdir -p /workspace/ComfyUI/models/checkpoints
echo "Directories ready."
echo ""
echo ">>> [3/6] Downloading Wan 2.2 models..."
WAN_BASE="https://huggingface.co/Comfy-Org/Wan_2.2_ComfyUI_Repackaged/resolve/main/split_files"
WAN21_BASE="https://huggingface.co/Comfy-Org/Wan_2.1_ComfyUI_repackaged/resolve/main/split_files"
if [ ! -f /workspace/ComfyUI/models/diffusion_models/wan2.2_i2v_high_noise_14B_fp8_scaled.safetensors ]; then
echo "Downloading Wan 2.2 I2V High Noise (14.3GB)..."
wget -q --show-progress -O /workspace/ComfyUI/models/diffusion_models/wan2.2_i2v_high_noise_14B_fp8_scaled.safetensors "${WAN_BASE}/diffusion_models/wan2.2_i2v_high_noise_14B_fp8_scaled.safetensors"
else
echo "Wan 2.2 I2V High Noise already exists, skip."
fi
if [ ! -f /workspace/ComfyUI/models/diffusion_models/wan2.2_i2v_low_noise_14B_fp8_scaled.safetensors ]; then
echo "Downloading Wan 2.2 I2V Low Noise (14.3GB)..."
wget -q --show-progress -O /workspace/ComfyUI/models/diffusion_models/wan2.2_i2v_low_noise_14B_fp8_scaled.safetensors "${WAN_BASE}/diffusion_models/wan2.2_i2v_low_noise_14B_fp8_scaled.safetensors"
else
echo "Wan 2.2 I2V Low Noise already exists, skip."
fi
if [ ! -f /workspace/ComfyUI/models/diffusion_models/wan2.2_t2v_high_noise_14B_fp8_scaled.safetensors ]; then
echo "Downloading Wan 2.2 T2V High Noise (14.3GB)..."
wget -q --show-progress -O /workspace/ComfyUI/models/diffusion_models/wan2.2_t2v_high_noise_14B_fp8_scaled.safetensors "${WAN_BASE}/diffusion_models/wan2.2_t2v_high_noise_14B_fp8_scaled.safetensors"
else
echo "Wan 2.2 T2V High Noise already exists, skip."
fi
if [ ! -f /workspace/ComfyUI/models/diffusion_models/wan2.2_t2v_low_noise_14B_fp8_scaled.safetensors ]; then
echo "Downloading Wan 2.2 T2V Low Noise (14.3GB)..."
wget -q --show-progress -O /workspace/ComfyUI/models/diffusion_models/wan2.2_t2v_low_noise_14B_fp8_scaled.safetensors "${WAN_BASE}/diffusion_models/wan2.2_t2v_low_noise_14B_fp8_scaled.safetensors"
else
echo "Wan 2.2 T2V Low Noise already exists, skip."
fi
if [ ! -f /workspace/ComfyUI/models/text_encoders/umt5_xxl_fp8_e4m3fn_scaled.safetensors ]; then
echo "Downloading UMT5-XXL text encoder (6.7GB)..."
wget -q --show-progress -O /workspace/ComfyUI/models/text_encoders/umt5_xxl_fp8_e4m3fn_scaled.safetensors "${WAN_BASE}/text_encoders/umt5_xxl_fp8_e4m3fn_scaled.safetensors"
else
echo "Text encoder already exists, skip."
fi
if [ ! -f /workspace/ComfyUI/models/vae/wan2.2_vae.safetensors ]; then
echo "Downloading Wan 2.2 VAE (1.4GB)..."
wget -q --show-progress -O /workspace/ComfyUI/models/vae/wan2.2_vae.safetensors "${WAN_BASE}/vae/wan2.2_vae.safetensors"
else
echo "Wan 2.2 VAE already exists, skip."
fi
if [ ! -f /workspace/ComfyUI/models/clip_vision/clip_vision_h.safetensors ]; then
echo "Downloading CLIP Vision (1.3GB)..."
wget -q --show-progress -O /workspace/ComfyUI/models/clip_vision/clip_vision_h.safetensors "${WAN21_BASE}/clip_vision/clip_vision_h.safetensors"
else
echo "CLIP Vision already exists, skip."
fi
echo "Wan 2.2 models complete."
echo ""
echo ">>> [4/6] Checking SDXL base model..."
if [ ! -f /workspace/ComfyUI/models/checkpoints/sd_xl_base_1.0.safetensors ]; then
echo "Downloading SDXL (6.9GB)..."
wget -q --show-progress -O /workspace/ComfyUI/models/checkpoints/sd_xl_base_1.0.safetensors "https://huggingface.co/stabilityai/stable-diffusion-xl-base-1.0/resolve/main/sd_xl_base_1.0.safetensors"
else
echo "SDXL already present."
fi
echo ""
echo ">>> [5/6] Creating auto-start script..."
echo '#!/bin/bash' > /workspace/start_nyptid.sh
echo 'echo "[NYPTID] Starting ComfyUI..."' >> /workspace/start_nyptid.sh
echo 'cd /workspace/ComfyUI' >> /workspace/start_nyptid.sh
echo 'pkill -f "main.py --listen" 2>/dev/null || true' >> /workspace/start_nyptid.sh
echo 'sleep 2' >> /workspace/start_nyptid.sh
echo 'nohup python main.py --listen 0.0.0.0 --port 8188 --preview-method auto > /workspace/comfyui.log 2>&1 &' >> /workspace/start_nyptid.sh
echo 'echo "[NYPTID] ComfyUI starting on port 8188 (PID: $!)"' >> /workspace/start_nyptid.sh
echo 'for i in $(seq 1 60); do' >> /workspace/start_nyptid.sh
echo '  if curl -s http://127.0.0.1:8188/system_stats > /dev/null 2>&1; then' >> /workspace/start_nyptid.sh
echo '    echo "[NYPTID] ComfyUI is ready!"' >> /workspace/start_nyptid.sh
echo '    break' >> /workspace/start_nyptid.sh
echo '  fi' >> /workspace/start_nyptid.sh
echo '  sleep 2' >> /workspace/start_nyptid.sh
echo 'done' >> /workspace/start_nyptid.sh
chmod +x /workspace/start_nyptid.sh
echo "Auto-start script created."
echo ""
echo ">>> [6/6] Starting ComfyUI..."
/workspace/start_nyptid.sh
echo ""
echo "=== SETUP COMPLETE ==="
echo "Models: SDXL, Wan 2.2 I2V/T2V (high+low), UMT5-XXL, VAE, CLIP Vision"
echo "Auto-start: /workspace/start_nyptid.sh"
echo ""
echo "IMPORTANT: Set Docker Start Command to:"
echo "bash -c '/workspace/start_nyptid.sh && sleep infinity'"
