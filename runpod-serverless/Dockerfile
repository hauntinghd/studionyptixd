FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_DATA_DIR=/runpod-volume/studio

WORKDIR /app

RUN apt-get update \
    && apt-get install --no-install-recommends -y ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./requirements.txt
COPY runpod-serverless/requirements.txt ./runpod-serverless/requirements.txt
RUN pip install --no-cache-dir -r runpod-serverless/requirements.txt

COPY . .

CMD ["python", "-u", "runpod-serverless/handler.py"]
