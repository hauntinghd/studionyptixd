#!/bin/bash
python3 -c "
from PIL import Image
Image.new('RGB',(512,512),'blue').save('/tmp/test_scp.png')
print('created test image')
"
scp -o StrictHostKeyChecking=no -P 22092 /tmp/test_scp.png root@69.30.85.41:/workspace/ComfyUI/input/test_scp_upload.png
if [ $? -eq 0 ]; then
    echo "SCP_SUCCESS"
    ssh -o StrictHostKeyChecking=no -p 22092 root@69.30.85.41 "ls -la /workspace/ComfyUI/input/test_scp_upload.png"
else
    echo "SCP_FAILED"
fi
