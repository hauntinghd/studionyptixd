"""Speech-to-text for Studio Agent dictation (Firefox-safe server fallback)."""
from __future__ import annotations

import asyncio
import logging
import os
import tempfile
from pathlib import Path

import httpx

from backend_settings import FAL_AI_KEY

_log = logging.getLogger("nyptid-studio.studio-agent-dictation")

MAX_AUDIO_BYTES = 12 * 1024 * 1024  # 12 MB
ALLOWED_SUFFIXES = {".webm", ".ogg", ".mp4", ".m4a", ".wav", ".mp3", ".mpeg"}


def _fal_key() -> str:
    return str(os.getenv("FAL_KEY", "") or FAL_AI_KEY or "").strip()


async def transcribe_audio_bytes(
    data: bytes,
    *,
    filename: str = "dictation.webm",
    content_type: str = "",
) -> str:
    """Transcribe uploaded microphone audio via fal whisper."""
    if not data:
        raise ValueError("Empty audio upload")
    if len(data) > MAX_AUDIO_BYTES:
        raise ValueError(f"Audio too large (max {MAX_AUDIO_BYTES // (1024 * 1024)} MB)")

    suffix = Path(str(filename or "audio.webm")).suffix.lower() or ".webm"
    if suffix not in ALLOWED_SUFFIXES:
        suffix = ".webm"

    fal_key = _fal_key()
    if not fal_key:
        raise RuntimeError("Speech transcription is not configured (missing FAL_AI_KEY)")

    def _run() -> str:
        import fal_client

        os.environ.setdefault("FAL_KEY", fal_key)
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp.write(data)
            tmp_path = tmp.name
        try:
            audio_url = fal_client.upload_file(tmp_path)
            with httpx.Client(timeout=120.0) as client:
                resp = client.post(
                    "https://fal.run/fal-ai/whisper",
                    headers={"Authorization": f"Key {fal_key}"},
                    json={
                        "audio_url": audio_url,
                        "task": "transcribe",
                        "chunk_level": "segment",
                        "language": "en",
                    },
                )
            if resp.status_code != 200:
                raise RuntimeError(f"Whisper failed ({resp.status_code}): {resp.text[:240]}")
            payload = resp.json()
            text = str(payload.get("text") or "").strip()
            if not text:
                chunks = payload.get("chunks") or payload.get("segments") or []
                parts = [
                    str(c.get("text") or "").strip()
                    for c in chunks
                    if isinstance(c, dict) and str(c.get("text") or "").strip()
                ]
                text = " ".join(parts).strip()
            return text
        finally:
            try:
                Path(tmp_path).unlink(missing_ok=True)
            except Exception:
                pass

    text = await asyncio.to_thread(_run)
    if not text:
        raise ValueError("No speech detected — try speaking closer to the mic and record again.")
    return text
