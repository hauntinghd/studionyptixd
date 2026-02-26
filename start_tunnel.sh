#!/bin/bash
pkill cloudflared 2>/dev/null
nohup cloudflared tunnel --url http://localhost:8188 > /tmp/cloudflared.log 2>&1 &
sleep 5
URL=$(grep -o 'https://[^ ]*trycloudflare.com' /tmp/cloudflared.log | head -1)
echo "$URL" > /workspace/tunnel_url.txt
echo "TUNNEL: $URL"
