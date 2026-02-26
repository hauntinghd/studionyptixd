#!/bin/bash
cd /workspace/ComfyUI/custom_nodes
if [ ! -d "Comfyui-SadTalker" ]; then
    git clone https://github.com/haomole/Comfyui-SadTalker.git
    cd Comfyui-SadTalker
    pip install -r requirements.txt 2>/dev/null
    echo "SadTalker node installed"
else
    echo "SadTalker already installed"
fi

mkdir -p /workspace/ComfyUI/models/sadtalker
cd /workspace/ComfyUI/models/sadtalker

for f in epoch_20.pth SadTalker_V0.0.2_256.safetensors SadTalker_V0.0.2_512.safetensors mapping_00109-model.pth.tar mapping_00229-model.pth.tar; do
    if [ ! -f "$f" ]; then
        echo "Downloading $f..."
        wget -q "https://huggingface.co/vinthony/SadTalker/resolve/main/$f" -O "$f"
    else
        echo "$f already exists"
    fi
done

wget -q -nc "https://huggingface.co/vinthony/SadTalker/resolve/main/BFM_Fitting/BFM09_model_info.mat" -O BFM09_model_info.mat 2>/dev/null
wget -q -nc "https://huggingface.co/vinthony/SadTalker/resolve/main/BFM_Fitting/BFM_model_front.mat" -O BFM_model_front.mat 2>/dev/null

ls -la /workspace/ComfyUI/models/sadtalker/
echo "SadTalker setup complete"
