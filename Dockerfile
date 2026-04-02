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

# Copy backend entrypoint and all extracted backend modules.
COPY backend.py .
COPY backend_settings.py .
COPY backend_catalog.py .
COPY backend_image_prompts.py .
COPY backend_models.py .
COPY backend_demo.py .
COPY backend_state.py .
COPY backend_queue.py .
COPY backend_worker.py .
COPY backend_catalyst_core.py .
COPY backend_catalyst_profiles.py .
COPY backend_catalyst_learning.py .
COPY backend_catalyst_blueprint.py .
COPY backend_catalyst_reference.py .
COPY ops ./ops
COPY --from=frontend-builder /frontend/dist/ ./ViralShorts-App/dist/
COPY ViralShorts-App/public/ ./ViralShorts-App/public/
COPY ViralShorts-App/src/studio/lib/storyArtStyles.json ./ViralShorts-App/src/studio/lib/storyArtStyles.json

RUN chmod +x ./ops/run_render_service.sh

RUN mkdir -p generated_videos temp_assets demo_uploads

ENV PORT=10000
EXPOSE 10000

CMD uvicorn backend:app --host 0.0.0.0 --port $PORT
