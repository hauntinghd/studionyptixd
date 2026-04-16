# ─────────────────────────────────────────────────────────────
# Studio backend — RunPod Serverless image
#
# Optimized for:
#   - Fast cold-start (multi-stage, aggressive layer cache)
#   - FFmpeg with NVENC (works on RunPod GPU OR CPU workers)
#   - Warm FastAPI app load (imports happen in handler init, not per-req)
# ─────────────────────────────────────────────────────────────
FROM node:20-alpine AS frontend-builder

WORKDIR /frontend
COPY ViralShorts-App/package*.json ./
RUN npm ci --prefer-offline --no-audit
COPY ViralShorts-App/ ./
RUN npm run build

# ─────────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# System deps — ffmpeg with NVENC codec support, fonts for subtitle rendering
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    fonts-noto-cjk \
    fonts-noto-core \
    fonts-freefont-ttf \
    fontconfig \
    curl \
    && fc-cache -f \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps (cache-friendly layer)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir runpod>=1.7.0

# Copy all top-level Python modules (backend is ~30 files including routes.py)
COPY *.py ./
COPY client_secrets.json ./
COPY ops ./ops

# Frontend static assets
COPY --from=frontend-builder /frontend/dist/ ./ViralShorts-App/dist/
COPY ViralShorts-App/public/ ./ViralShorts-App/public/
COPY ViralShorts-App/src/studio/lib/storyArtStyles.json ./ViralShorts-App/src/studio/lib/storyArtStyles.json

# Serverless handler
COPY runpod-serverless/handler.py ./handler.py

# Normalize line endings on shell scripts
RUN sed -i 's/\r$//' ./ops/run_render_service.sh \
    && chmod +x ./ops/run_render_service.sh

# Scratch dirs
RUN mkdir -p generated_videos temp_assets demo_uploads

# Pre-compile ALL Python bytecode so the first runtime import doesn't
# spend 60+ seconds compiling 22K lines on a cold serverless CPU.
RUN python -m compileall -q /app/ 2>/dev/null || true

# Runpod serverless invokes handler.py as module
CMD ["python", "-u", "handler.py"]
