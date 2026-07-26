FROM node:20-alpine AS frontend-builder

WORKDIR /frontend
COPY ViralShorts-App/package*.json ./
RUN npm ci
COPY ViralShorts-App/ ./
ARG FRONTEND_BUILD_ID
ARG GIT_SHA
RUN test -n "${FRONTEND_BUILD_ID}" \
    && test -n "${GIT_SHA}" \
    && test "${GIT_SHA}" != "unknown" \
    && echo "frontend build ${FRONTEND_BUILD_ID} git=${GIT_SHA}" \
    && VITE_STUDIO_BUILD_ID="${FRONTEND_BUILD_ID}" npm run build \
    && grep -R -F -q -- "${FRONTEND_BUILD_ID}" dist

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
# The release API reads the same Minisign trust anchor embedded in the desktop
# application. If this file is absent or malformed, release publication fails
# closed.
COPY ViralShorts-App/src-tauri/tauri.conf.json ./ViralShorts-App/src-tauri/tauri.conf.json

# Fail the image build before it can ever reach production if source syntax
# is not compatible with the exact Python runtime used by this image.
RUN python -m compileall -q \
    *.py \
    cliplab \
    long_form \
    media_sources \
    skeleton_ai \
    studio \
    studio_agent \
    zerotier_private

RUN sed -i 's/\r$//' ./ops/run_render_service.sh \
    && chmod +x ./ops/run_render_service.sh

RUN mkdir -p generated_videos temp_assets demo_uploads

ARG FRONTEND_BUILD_ID
ARG GIT_SHA
RUN test -n "${FRONTEND_BUILD_ID}" \
    && test -n "${GIT_SHA}" \
    && test "${GIT_SHA}" != "unknown"
LABEL org.opencontainers.image.source="https://github.com/hauntinghd/studionyptixd" \
      org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.version="${FRONTEND_BUILD_ID}"
ENV PORT=10000
ENV STUDIO_BUILD_ID=${FRONTEND_BUILD_ID}
ENV STUDIO_GIT_SHA=${GIT_SHA}
EXPOSE 10000

# Studio's sessions, credit ledger, job state, and attached Fly volume are
# process-local/file-backed. Exactly one Uvicorn process owns those resources;
# its asyncio loop also runs the Redis production consumer. Provider calls are
# async and do not require extra OS processes.
ENV WEB_CONCURRENCY=1

# Write immutable deploy metadata into the image so /api/health can prove
# exactly which Git snapshot and frontend bundle are serving customers.
RUN printf '{"build_id":"%s","git_sha":"%s"}\n' "$STUDIO_BUILD_ID" "$STUDIO_GIT_SHA" > /app/ops/deploy_meta.json

CMD ["uvicorn", "backend:app", "--host", "0.0.0.0", "--port", "10000", "--workers", "1"]
