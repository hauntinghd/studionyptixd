"""Speech-to-text for Studio Agent dictation.

Chrome/Edge/Safari usually use browser-native live speech recognition. This
module is the server fallback for browsers such as Firefox that record audio and
upload it after the user stops speaking.
"""
from __future__ import annotations

import os
from pathlib import Path

import httpx

from backend_settings import XAI_API_KEY

MAX_AUDIO_BYTES = 12 * 1024 * 1024  # 12 MB
ALLOWED_SUFFIXES = {
    ".aac",
    ".flac",
    ".m4a",
    ".mkv",
    ".mp3",
    ".mp4",
    ".mpeg",
    ".ogg",
    ".opus",
    ".wav",
    ".webm",
}
XAI_STT_ENDPOINT = "https://api.x.ai/v1/stt"


def _xai_key() -> str:
    return str(os.getenv("XAI_API_KEY", "") or XAI_API_KEY or "").strip()


def _dictation_language() -> str:
    language = str(os.getenv("XAI_STT_LANGUAGE", "en") or "en").strip().lower()
    return language or "en"


def _media_type_for(suffix: str, content_type: str) -> str:
    explicit = str(content_type or "").strip()
    if explicit:
        return explicit
    return {
        ".aac": "audio/aac",
        ".flac": "audio/flac",
        ".m4a": "audio/mp4",
        ".mkv": "video/x-matroska",
        ".mp3": "audio/mpeg",
        ".mp4": "video/mp4",
        ".mpeg": "audio/mpeg",
        ".ogg": "audio/ogg",
        ".opus": "audio/opus",
        ".wav": "audio/wav",
        ".webm": "audio/webm",
    }.get(suffix, "application/octet-stream")


async def transcribe_audio_bytes(
    data: bytes,
    *,
    filename: str = "dictation.webm",
    content_type: str = "",
) -> str:
    """Transcribe uploaded microphone audio via xAI Grok STT."""
    if not data:
        raise ValueError("Empty audio upload")
    if len(data) > MAX_AUDIO_BYTES:
        raise ValueError(f"Audio too large (max {MAX_AUDIO_BYTES // (1024 * 1024)} MB)")

    suffix = Path(str(filename or "audio.webm")).suffix.lower() or ".webm"
    if suffix not in ALLOWED_SUFFIXES:
        suffix = ".webm"

    xai_key = _xai_key()
    if not xai_key:
        raise RuntimeError("Speech transcription is not configured (missing XAI_API_KEY)")

    async with httpx.AsyncClient(timeout=120.0) as client:
        resp = await client.post(
            XAI_STT_ENDPOINT,
            headers={"Authorization": f"Bearer {xai_key}"},
            data=[
                ("format", "true"),
                ("language", _dictation_language()),
                ("keyterm", "Studio Agent"),
                ("keyterm", "ClipLab"),
                ("keyterm", "Catalyst"),
            ],
            files={
                "file": (
                    f"dictation{suffix}",
                    data,
                    _media_type_for(suffix, content_type),
                ),
            },
        )
    if resp.status_code != 200:
        detail = str(resp.text or "").replace(xai_key, "[redacted]")[:260]
        raise RuntimeError(f"Grok STT failed ({resp.status_code}): {detail}")

    try:
        payload = resp.json()
    except Exception as exc:
        raise RuntimeError("Grok STT returned invalid JSON") from exc

    text = str(payload.get("text") or "").strip()
    if not text:
        channels = payload.get("channels") or []
        text = " ".join(
            str(channel.get("text") or "").strip()
            for channel in channels
            if isinstance(channel, dict) and str(channel.get("text") or "").strip()
        ).strip()
    if not text:
        raise ValueError("No speech detected - try speaking closer to the mic and record again.")
    return text
