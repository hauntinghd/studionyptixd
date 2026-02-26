#!/bin/bash
pip install -q peft accelerate bitsandbytes diffusers[torch] datasets pillow
mkdir -p /workspace/thumbnail_training/images
mkdir -p /workspace/thumbnail_training/output
mkdir -p /workspace/ComfyUI/models/loras
echo "DEPS_INSTALLED"
