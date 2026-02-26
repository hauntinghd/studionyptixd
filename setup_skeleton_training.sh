#!/bin/bash
# NYPTID Skeleton LoRA Training Setup for RunPod A40
# Run this AFTER uploading skeleton_training_dataset/ to /workspace/

set -e
echo "============================================================"
echo "  NYPTID Skeleton LoRA Training Setup"
echo "  GPU: $(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null || echo 'checking...')"
echo "  VRAM: $(nvidia-smi --query-gpu=memory.total --format=csv,noheader 2>/dev/null || echo 'checking...')"
echo "============================================================"

cd /workspace

echo ""
echo "[1/6] Installing training dependencies..."
pip install --upgrade pip
pip install accelerate transformers peft bitsandbytes safetensors Pillow prodigyopt tensorboard xformers datasets
pip install "diffusers[torch]>=0.28.0"
pip install git+https://github.com/huggingface/diffusers.git

echo ""
echo "[2/6] Verifying SDXL base model..."
SDXL_PATH="/workspace/ComfyUI/models/checkpoints/sd_xl_base_1.0.safetensors"
if [ -f "$SDXL_PATH" ]; then
    SIZE=$(du -h "$SDXL_PATH" | cut -f1)
    echo "  Found SDXL base: $SDXL_PATH ($SIZE)"
else
    echo "  SDXL not found locally, will download from HuggingFace during training..."
fi

echo ""
echo "[3/6] Checking dataset..."
DATASET_DIR="/workspace/skeleton_training_dataset"
if [ ! -d "$DATASET_DIR" ]; then
    echo "  ERROR: Dataset not found at $DATASET_DIR"
    echo "  Upload skeleton_training_dataset/ to /workspace/ first!"
    exit 1
fi

IMG_COUNT=$(ls -1 "$DATASET_DIR"/*.png 2>/dev/null | wc -l)
CAP_COUNT=$(ls -1 "$DATASET_DIR"/captions/*.txt 2>/dev/null | wc -l)
echo "  Found $IMG_COUNT images, $CAP_COUNT caption files"

echo ""
echo "[4/6] Preparing training directory..."
TRAIN_DIR="/workspace/skeleton_lora_training/instance_images"
mkdir -p "$TRAIN_DIR"

for img in "$DATASET_DIR"/*.png; do
    base=$(basename "$img" .png)
    cp -n "$img" "$TRAIN_DIR/$base.png" 2>/dev/null || true
    
    caption_file="$DATASET_DIR/captions/${base}.txt"
    if [ -f "$caption_file" ]; then
        cp -n "$caption_file" "$TRAIN_DIR/${base}.txt" 2>/dev/null || true
    else
        echo "nyptid_skeleton, photorealistic 3D skeleton, teal studio backdrop" > "$TRAIN_DIR/${base}.txt"
    fi
done

PREPPED=$(ls -1 "$TRAIN_DIR"/*.png 2>/dev/null | wc -l)
echo "  Prepared $PREPPED image-caption pairs in $TRAIN_DIR"

echo ""
echo "[5/6] Setting up accelerate config..."
mkdir -p ~/.cache/huggingface/accelerate
cat > ~/.cache/huggingface/accelerate/default_config.yaml << 'ACCEOF'
compute_environment: LOCAL_MACHINE
distributed_type: 'NO'
downcast_bf16: 'no'
machine_rank: 0
main_training_function: main
mixed_precision: fp16
num_machines: 1
num_processes: 1
rdzv_backend: static
same_network: true
tpu_env: []
tpu_use_cluster: false
tpu_use_sudo: false
use_cpu: false
ACCEOF
echo "  Accelerate config created"

echo ""
echo "[6/6] Creating training launch script..."
OUTPUT_DIR="/workspace/skeleton_lora_output"
mkdir -p "$OUTPUT_DIR/logs"

cat > /workspace/run_skeleton_training.sh << 'TRAINEOF'
#!/bin/bash
set -e
echo "Starting NYPTID Skeleton LoRA Training at $(date)"
echo "GPU: $(nvidia-smi --query-gpu=name,memory.total --format=csv,noheader)"

export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True

accelerate launch \
    --mixed_precision=fp16 \
    --num_processes=1 \
    -m diffusers.examples.dreambooth.train_dreambooth_lora_sdxl \
    --pretrained_model_name_or_path="stabilityai/stable-diffusion-xl-base-1.0" \
    --instance_data_dir="/workspace/skeleton_lora_training/instance_images" \
    --output_dir="/workspace/skeleton_lora_output" \
    --instance_prompt="nyptid_skeleton" \
    --resolution=1024 \
    --train_batch_size=1 \
    --gradient_accumulation_steps=4 \
    --learning_rate=1e-4 \
    --text_encoder_lr=5e-5 \
    --lr_scheduler="cosine" \
    --lr_warmup_steps=100 \
    --max_train_steps=2000 \
    --rank=64 \
    --seed=42 \
    --mixed_precision="fp16" \
    --train_text_encoder \
    --enable_xformers_memory_efficient_attention \
    --gradient_checkpointing \
    --checkpointing_steps=500 \
    --validation_prompt="nyptid_skeleton, single skeleton, NASCAR driver in racing suit, standing with arms crossed, teal studio backdrop, photorealistic 3D render, 8K" \
    --validation_epochs=5 \
    --report_to="tensorboard" \
    --logging_dir="/workspace/skeleton_lora_output/logs"

echo ""
echo "Training complete at $(date)!"
echo "LoRA files in /workspace/skeleton_lora_output/"
ls -lh /workspace/skeleton_lora_output/*.safetensors 2>/dev/null || echo "(checking checkpoints...)"
ls -lh /workspace/skeleton_lora_output/checkpoint-*/*.safetensors 2>/dev/null || true

echo ""
echo "Copying best LoRA to ComfyUI..."
COMFY_LORA_DIR="/workspace/ComfyUI/models/loras"
mkdir -p "$COMFY_LORA_DIR"

LORA_FILE=$(ls -t /workspace/skeleton_lora_output/pytorch_lora_weights.safetensors 2>/dev/null | head -1)
if [ -z "$LORA_FILE" ]; then
    LORA_FILE=$(find /workspace/skeleton_lora_output -name "*.safetensors" -type f | sort | tail -1)
fi

if [ -n "$LORA_FILE" ]; then
    cp "$LORA_FILE" "$COMFY_LORA_DIR/nyptid_skeleton_v1.safetensors"
    echo "LoRA copied to: $COMFY_LORA_DIR/nyptid_skeleton_v1.safetensors"
    ls -lh "$COMFY_LORA_DIR/nyptid_skeleton_v1.safetensors"
else
    echo "WARNING: No LoRA file found. Check training output."
fi
TRAINEOF

chmod +x /workspace/run_skeleton_training.sh

echo ""
echo "============================================================"
echo "  SETUP COMPLETE!"
echo "  Dataset: $PREPPED images prepared"
echo "  Output dir: $OUTPUT_DIR"
echo ""
echo "  TO START TRAINING, RUN:"
echo "    nohup bash /workspace/run_skeleton_training.sh > /workspace/training.log 2>&1 &"
echo ""
echo "  MONITOR PROGRESS:"
echo "    tail -f /workspace/training.log"
echo ""
echo "  ESTIMATED TIME: 1-3 hours on A40 (48GB)"
echo "============================================================"
