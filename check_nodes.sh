#!/bin/bash
curl -s http://localhost:8188/object_info | python3 -c "
import sys, json
d = json.load(sys.stdin)
targets = ['wan','kling','ip_adapter','ipadapter','pulid','instant','face','lora','controlnet']
for k in sorted(d.keys()):
    kl = k.lower()
    if any(t in kl for t in targets):
        print(k)
"
echo "=== LORAS ==="
ls /workspace/ComfyUI/models/loras/ 2>/dev/null
echo "=== CLIP_VISION ==="
ls /workspace/ComfyUI/models/clip_vision/ 2>/dev/null
echo "=== CONTROLNET ==="
ls /workspace/ComfyUI/models/controlnet/ 2>/dev/null
echo "=== IPADAPTER ==="
ls /workspace/ComfyUI/models/ipadapter/ 2>/dev/null || ls /workspace/ComfyUI/models/ip_adapter/ 2>/dev/null
