FROM node:20-alpine AS frontend-builder

WORKDIR /frontend
COPY ViralShorts-App/package*.json ./
RUN npm ci
COPY ViralShorts-App/ ./
RUN npm run build

FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    fonts-noto-cjk \
    fonts-noto-core \
    fonts-freefont-ttf \
    fontconfig \
    && fc-cache -f \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all top-level Python modules so backend splits are always packaged.
COPY *.py ./
COPY client_secrets.json .
COPY ops ./ops
# Skeleton AI short-form pipeline package — Casey 2026-05-05 rebuild.
# Top-level *.py glob above does NOT recurse into subpackages.
COPY skeleton_ai ./skeleton_ai
# Long-form pipeline package — Casey 2026-05-05 rebuild (6 channels +
# Catalyst-fed outlines). Same recursion gotcha as skeleton_ai.
COPY long_form ./long_form
# ZeroTier (Private) niche pipeline package — Casey 2026-05-08 (Phase 2b).
# zerotier_private_router.py imports from zerotier_private.pipeline; without
# this COPY the router fails to mount and /api/zerotier-private/* returns 404.
COPY zerotier_private ./zerotier_private
# Studio Agent (OpenRouter orchestrator) + Rookcast skills library.
# Trailing slash + verify step busts stale Depot cache layers from pre-package builds.
COPY studio_agent/ ./studio_agent/
RUN test -f ./studio_agent/__init__.py && test -f ./studio_agent/runner.py
# Unified wallet + media clients used by studio_agent tools.
COPY unified_credits.py ./
COPY media_sources ./media_sources
# Free / public-domain external media source clients (archival, music, SFX).
COPY cliplab ./cliplab
COPY studio/skills ./studio/skills
COPY studio/channels ./studio/channels
COPY --from=frontend-builder /frontend/dist/ ./ViralShorts-App/dist/
COPY ViralShorts-App/public/ ./ViralShorts-App/public/
COPY ViralShorts-App/src/studio/lib/storyArtStyles.json ./ViralShorts-App/src/studio/lib/storyArtStyles.json

RUN sed -i 's/\r$//' ./ops/run_render_service.sh \
    && chmod +x ./ops/run_render_service.sh

RUN mkdir -p generated_videos temp_assets demo_uploads

ENV PORT=10000
EXPOSE 10000

CMD uvicorn backend:app --host 0.0.0.0 --port $PORT
