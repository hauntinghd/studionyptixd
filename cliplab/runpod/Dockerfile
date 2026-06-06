# ClipLab inference — separate RunPod serverless endpoint
FROM runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1-mesa-glx libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY cliplab/runpod/requirements-train.txt ./requirements-train.txt
RUN pip install --no-cache-dir -r requirements-train.txt

COPY cliplab ./cliplab

ENV PYTHONPATH=/app
ENV STUDIO_APP_DATA_DIR=/runpod-volume/studio

CMD ["python", "-u", "cliplab/runpod/inference_handler.py"]
