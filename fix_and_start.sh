#!/bin/bash
sed -i 's/\r//' /workspace/train_thumbnail_lora.py
pkill -f train_thumbnail_lora 2>/dev/null
sleep 1
mkdir -p /workspace/thumbnail_training/images /workspace/thumbnail_training/output
nohup python3 -u /workspace/train_thumbnail_lora.py > /workspace/thumbnail_training/trainer.log 2>&1 &
echo "STARTED PID=$!"
sleep 3
cat /workspace/thumbnail_training/trainer.log
