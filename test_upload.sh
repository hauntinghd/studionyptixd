#!/bin/bash
python3 -c "
from PIL import Image
img = Image.new('RGB', (512,512), 'red')
img.save('/tmp/test_upload.png')
print('image created')
"
echo "Testing upload..."
curl -sv --max-time 10 -X POST -F "image=@/tmp/test_upload.png" http://localhost:8188/upload/image
echo ""
echo "Done"
