#!/bin/bash
set -e

echo "============================================"
echo "  VIRAL SHORTS ENGINE - RunPod GPU Setup"
echo "============================================"

apt-get update && apt-get install -y ffmpeg git wget curl

pip install --upgrade pip
pip install fastapi uvicorn httpx python-multipart aiofiles

cd /workspace

if [ ! -d "ComfyUI" ]; then
    echo "[1/5] Cloning ComfyUI..."
    git clone https://github.com/comfyanonymous/ComfyUI.git
    cd ComfyUI
    pip install -r requirements.txt
    cd ..
else
    echo "[1/5] ComfyUI already exists, updating..."
    cd ComfyUI && git pull && pip install -r requirements.txt && cd ..
fi

MODELS_DIR="/workspace/ComfyUI/models/checkpoints"
mkdir -p "$MODELS_DIR"

if [ ! -f "$MODELS_DIR/sd_xl_base_1.0.safetensors" ]; then
    echo "[2/5] Downloading SDXL base model (~7GB)..."
    wget -q --show-progress -O "$MODELS_DIR/sd_xl_base_1.0.safetensors" \
        "https://huggingface.co/stabilityai/stable-diffusion-xl-base-1.0/resolve/main/sd_xl_base_1.0.safetensors"
else
    echo "[2/5] SDXL base model already downloaded"
fi

LORA_DIR="/workspace/ComfyUI/models/loras"
mkdir -p "$LORA_DIR"
echo "[3/5] LoRA directory ready at $LORA_DIR"
echo "  You can manually add LoRAs from CivitAI later for specific styles"

echo "[4/5] Installing custom nodes..."
cd /workspace/ComfyUI/custom_nodes

if [ ! -d "ComfyUI-AnimateDiff-Evolved" ]; then
    git clone https://github.com/Kosinkadink/ComfyUI-AnimateDiff-Evolved.git
    cd ComfyUI-AnimateDiff-Evolved && pip install -r requirements.txt 2>/dev/null || true && cd ..
fi

if [ ! -d "comfyui-art-venture" ]; then
    git clone https://github.com/artventureX/comfyui-art-venture.git
    cd comfyui-art-venture && pip install -r requirements.txt 2>/dev/null || true && cd ..
fi

cd /workspace

echo "[5/5] Creating launch script..."
cat > /workspace/start_comfyui.sh << 'LAUNCH'
#!/bin/bash
cd /workspace/ComfyUI
python main.py --listen 0.0.0.0 --port 8188 --preview-method auto &
echo "ComfyUI started on port 8188"
LAUNCH
chmod +x /workspace/start_comfyui.sh

cat > /workspace/start_backend.sh << 'BACKEND'
#!/bin/bash
cd /workspace/viral-shorts
export COMFYUI_URL="http://127.0.0.1:8188"
python -m uvicorn backend:app --host 0.0.0.0 --port 8081 --reload
BACKEND
chmod +x /workspace/start_backend.sh

cat > /workspace/start_all.sh << 'ALL'
#!/bin/bash
echo "Starting ComfyUI..."
cd /workspace/ComfyUI
python main.py --listen 0.0.0.0 --port 8188 --preview-method auto &
COMFY_PID=$!
echo "ComfyUI PID: $COMFY_PID"

sleep 15
echo "Waiting for ComfyUI to load model..."
for i in $(seq 1 60); do
    if curl -s http://127.0.0.1:8188/system_stats > /dev/null 2>&1; then
        echo "ComfyUI is ready!"
        break
    fi
    sleep 2
done

echo "Starting Viral Shorts Backend..."
cd /workspace/viral-shorts
export COMFYUI_URL="http://127.0.0.1:8188"
python -m uvicorn backend:app --host 0.0.0.0 --port 8081

ALL
chmod +x /workspace/start_all.sh

mkdir -p /workspace/viral-shorts

echo ""
echo "============================================"
echo "  SETUP COMPLETE!"
echo "============================================"
echo ""
echo "Next steps:"
echo "  1. Copy your backend.py to /workspace/viral-shorts/"
echo "  2. Set your API keys:"
echo "     export XAI_API_KEY='your-xai-key'"
echo "     export ELEVENLABS_API_KEY='your-elevenlabs-key'"
echo "  3. Run: /workspace/start_all.sh"
echo ""
echo "Ports:"
echo "  8188 = ComfyUI (image generation)"
echo "  8081 = Viral Shorts API (your backend)"
echo ""
