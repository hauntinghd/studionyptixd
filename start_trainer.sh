#!/bin/bash
pkill -f train_thumbnail_lora.py 2>/dev/null
sleep 1
mkdir -p /workspace/thumbnail_training/images /workspace/thumbnail_training/output
nohup python3 /workspace/train_thumbnail_lora.py > /workspace/thumbnail_training/trainer.log 2>&1 &
echo "TRAINER_PID=$!"
sleep 2
tail -5 /workspace/thumbnail_training/trainer.log
