#!/bin/bash
while true; do
    if ! curl -s http://localhost:8188/system_stats > /dev/null 2>&1; then
        echo "$(date) Tunnel down, reconnecting..."
        pkill -f 'ssh.*-L 8188' 2>/dev/null
        sleep 2
        ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -fN -L 8188:localhost:8188 root@69.30.85.41 -p 22092
        echo "$(date) Tunnel reconnected"
    fi
    sleep 30
done
