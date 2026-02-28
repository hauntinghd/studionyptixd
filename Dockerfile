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
COPY ViralShorts-App/dist/ ./ViralShorts-App/dist/

RUN mkdir -p generated_videos temp_assets demo_uploads

ENV PORT=10000
EXPOSE 10000

CMD uvicorn backend:app --host 0.0.0.0 --port $PORT
