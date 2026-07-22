"""FAL-only speech-to-text for Studio Agent recorded dictation."""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path
from typing import Any

import httpx

from backend_settings import FAL_AI_KEY
from studio_agent.stt_utils import httpx_json_dict

MAX_AUDIO_BYTES = 28 * 1024 * 1024
ALLOWED_SUFFIXES = {".webm", ".ogg", ".mp4", ".m4a", ".wav", ".mp3", ".mpeg"}
FAL_STT_ENDPOINT = "https://fal.run/fal-ai/whisper"


def _fal_key() -> str:
    return str(os.getenv("FAL_KEY", "") or FAL_AI_KEY or "").strip()


def _stt_language() -> str:
    return str(
        os.getenv("STUDIO_STT_LANGUAGE", "")
        or os.getenv("FAL_STT_LANGUAGE", "")
        or "en"
    ).strip() or "en"


def _transcribe_fal(data: bytes, *, filename: str) -> str:
    fal_key = _fal_key()
    if not fal_key:
        raise RuntimeError("Speech transcription is not configured (missing FAL_KEY/FAL_AI_KEY)")

    suffix = Path(str(filename or "audio.webm")).suffix.lower() or ".webm"
    if suffix not in ALLOWED_SUFFIXES:
        suffix = ".webm"

    import fal_client

    os.environ.setdefault("FAL_KEY", fal_key)
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(data)
        tmp_path = tmp.name
    try:
        try:
            audio_url = fal_client.upload_file(tmp_path)
        except json.JSONDecodeError:
            raise RuntimeError("FAL upload returned an empty or invalid response") from None
        if not str(audio_url or "").strip():
            raise RuntimeError("FAL upload returned no audio URL")
        with httpx.Client(timeout=120.0) as client:
            resp = client.post(
                FAL_STT_ENDPOINT,
                headers={"Authorization": f"Key {fal_key}"},
                json={
                    "audio_url": audio_url,
                    "task": "transcribe",
                    "chunk_level": "segment",
                    "language": _stt_language(),
                },
            )
        if resp.status_code != 200:
            raise RuntimeError(f"Whisper failed ({resp.status_code}): {resp.text[:240]}")
        payload = httpx_json_dict(resp)
        if not payload:
            raise RuntimeError("Whisper returned an empty or non-JSON payload")
        text = str(payload.get("text") or "").strip()
        if not text:
            chunks = payload.get("chunks") or payload.get("segments") or []
            parts = [
                str(chunk.get("text") or "").strip()
                for chunk in chunks
                if isinstance(chunk, dict) and str(chunk.get("text") or "").strip()
            ]
            text = " ".join(parts).strip()
        return text
    finally:
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except Exception:
            pass


async def transcribe_audio_bytes(
    data: bytes,
    *,
    filename: str = "dictation.webm",
    content_type: str = "",
) -> tuple[str, str]:
    """Transcribe uploaded microphone audio. Returns ``(text, "fal")``."""
    del content_type
    if not data:
        raise ValueError("Empty audio upload")
    if len(data) > MAX_AUDIO_BYTES:
        raise ValueError(f"Audio too large (max {MAX_AUDIO_BYTES // (1024 * 1024)} MB)")

    text = await asyncio.to_thread(_transcribe_fal, data, filename=filename)
    if not text:
        raise ValueError("No speech detected - try speaking closer to the mic and record again.")
    return text, "fal"


def transcribe_file_path(audio_path: str) -> dict[str, Any]:
    """Synchronously transcribe a reference-analysis audio file through FAL."""
    path = Path(str(audio_path or ""))
    if not path.is_file():
        return {"error": "audio_missing", "text": "", "segments": []}
    data = path.read_bytes()
    if not data:
        return {"error": "audio_empty", "text": "", "segments": []}
    try:
        text = _transcribe_fal(data, filename=path.name)
    except Exception as exc:
        return {"error": str(exc)[:240], "text": "", "segments": []}
    if not text:
        return {"error": "no_speech_detected", "text": "", "segments": []}
    return {
        "text": text,
        "segments": [{"start_sec": 0.0, "end_sec": 0.0, "text": text}],
        "word_count": len(text.split()),
        "provider": "fal",
    }


__all__ = [
    "ALLOWED_SUFFIXES",
    "FAL_STT_ENDPOINT",
    "MAX_AUDIO_BYTES",
    "transcribe_audio_bytes",
    "transcribe_file_path",
]
