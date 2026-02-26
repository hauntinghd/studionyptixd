#!/bin/bash
grep -q '9188' /etc/nginx/nginx.conf && echo "ALREADY_EXISTS" && exit 0

cat >> /tmp/comfyui_proxy.conf << 'BLOCK'

    # ComfyUI API proxy
    server {
        listen 9188;
        client_max_body_size 100M;
        location / {
            proxy_pass http://localhost:8188;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_read_timeout 900s;
            proxy_send_timeout 900s;
        }
    }
BLOCK

# Insert before the final closing brace
head -n -1 /etc/nginx/nginx.conf > /tmp/nginx_new.conf
cat /tmp/comfyui_proxy.conf >> /tmp/nginx_new.conf
echo "}" >> /tmp/nginx_new.conf
cp /tmp/nginx_new.conf /etc/nginx/nginx.conf
nginx -t && nginx -s reload && echo "PROXY_ADDED" || echo "NGINX_ERROR"
