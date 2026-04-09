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
COPY --from=frontend-builder /frontend/dist/ ./ViralShorts-App/dist/
COPY ViralShorts-App/public/ ./ViralShorts-App/public/
COPY ViralShorts-App/src/studio/lib/storyArtStyles.json ./ViralShorts-App/src/studio/lib/storyArtStyles.json

RUN sed -i 's/\r$//' ./ops/run_render_service.sh \
    && chmod +x ./ops/run_render_service.sh

RUN mkdir -p generated_videos temp_assets demo_uploads

ENV PORT=10000
EXPOSE 10000

CMD uvicorn backend:app --host 0.0.0.0 --port $PORT
