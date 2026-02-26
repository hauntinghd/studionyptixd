#!/bin/bash
python3 -c "
try:
    import peft; print('peft:', peft.__version__)
except: print('peft: NOT INSTALLED')
try:
    import diffusers; print('diffusers:', diffusers.__version__)
except: print('diffusers: NOT INSTALLED')
try:
    import accelerate; print('accelerate:', accelerate.__version__)
except: print('accelerate: NOT INSTALLED')
try:
    import transformers; print('transformers:', transformers.__version__)
except: print('transformers: NOT INSTALLED')
try:
    import bitsandbytes; print('bitsandbytes:', bitsandbytes.__version__)
except: print('bitsandbytes: NOT INSTALLED')
"
echo "---LORA DIR---"
ls -la /workspace/ComfyUI/models/loras/ 2>/dev/null || echo "NO LORA DIR"
echo "---DISK---"
df -h /workspace
echo "---GPU---"
nvidia-smi --query-gpu=name,memory.total,memory.free --format=csv,noheader 2>/dev/null
echo "DONE"
