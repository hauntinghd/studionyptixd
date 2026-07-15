"""Speech-to-text for Studio Agent dictation (xAI primary, FAL fallback)."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any

import httpx

from backend_settings import FAL_AI_KEY
from studio_agent.stt_utils import httpx_json_dict

_log = logging.getLogger("nyptid-studio.studio-agent-dictation")

MAX_AUDIO_BYTES = 28 * 1024 * 1024  # ~28 MB — long paragraph / multi-minute recordings
ALLOWED_SUFFIXES = {".webm", ".ogg", ".mp4", ".m4a", ".wav", ".mp3", ".mpeg"}
XAI_STT_ENDPOINT = "https://api.x.ai/v1/stt"


def _fal_key() -> str:
    return str(os.getenv("FAL_KEY", "") or FAL_AI_KEY or "").strip()


def _xai_key() -> str:
    return str(os.getenv("XAI_API_KEY", "") or "").strip()


def _stt_language() -> str:
    return str(os.getenv("XAI_STT_LANGUAGE", "en") or "en").strip() or "en"


def _mime_for_suffix(suffix: str) -> str:
    mapping = {
        ".webm": "audio/webm",
        ".ogg": "audio/ogg",
        ".mp4": "audio/mp4",
        ".m4a": "audio/mp4",
        ".wav": "audio/wav",
        ".mp3": "audio/mpeg",
        ".mpeg": "audio/mpeg",
    }
    return mapping.get(suffix, "application/octet-stream")


def _transcribe_xai(data: bytes, *, filename: str, content_type: str) -> str:
    api_key = _xai_key()
    if not api_key:
        raise RuntimeError("XAI_API_KEY not configured")
    suffix = Path(str(filename or "audio.webm")).suffix.lower() or ".webm"
    if suffix not in ALLOWED_SUFFIXES:
        suffix = ".webm"
    mime = str(content_type or "").strip() or _mime_for_suffix(suffix)
    language = _stt_language()
    with httpx.Client(timeout=180.0) as client:
        resp = client.post(
            XAI_STT_ENDPOINT,
            headers={"Authorization": f"Bearer {api_key}"},
            data=[
                ("format", "true"),
                ("language", language),
            ],
            files={"file": (Path(filename).name or f"dictation{suffix}", data, mime)},
        )
    if resp.status_code != 200:
        raise RuntimeError(f"xAI STT failed ({resp.status_code}): {resp.text[:240]}")
    payload = httpx_json_dict(resp)
    if not payload:
        raise RuntimeError("xAI STT returned an empty or non-JSON payload")
    text = str(payload.get("text") or "").strip()
    if not text:
        words = payload.get("words") or []
        if isinstance(words, list):
            text = " ".join(
                str(word.get("text") or "").strip()
                for word in words
                if isinstance(word, dict) and str(word.get("text") or "").strip()
            ).strip()
    return text


def _transcribe_fal(data: bytes, *, filename: str) -> str:
    fal_key = _fal_key()
    if not fal_key:
        raise RuntimeError("Speech transcription is not configured (missing XAI_API_KEY and FAL_AI_KEY)")

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
        payload = httpx_json_dict(resp)
        if not payload:
            raise RuntimeError("Whisper returned an empty or non-JSON payload")
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


async def transcribe_audio_bytes(
    data: bytes,
    *,
    filename: str = "dictation.webm",
    content_type: str = "",
) -> tuple[str, str]:
    """Transcribe uploaded microphone audio. Returns (text, provider)."""
    if not data:
        raise ValueError("Empty audio upload")
    if len(data) > MAX_AUDIO_BYTES:
        raise ValueError(f"Audio too large (max {MAX_AUDIO_BYTES // (1024 * 1024)} MB)")

    suffix = Path(str(filename or "audio.webm")).suffix.lower() or ".webm"
    if suffix not in ALLOWED_SUFFIXES:
        suffix = ".webm"

    if _xai_key():
        try:
            text = await asyncio.to_thread(
                _transcribe_xai,
                data,
                filename=filename,
                content_type=content_type,
            )
            if text:
                return text, "xai"
        except Exception as exc:
            _log.warning("xAI STT failed, falling back to FAL whisper: %s", exc)

    text = await asyncio.to_thread(_transcribe_fal, data, filename=filename)
    if not text:
        raise ValueError("No speech detected — try speaking closer to the mic and record again.")
    return text, "fal"


def transcribe_file_path(audio_path: str) -> dict[str, Any]:
    """Sync STT for reference-analysis audio files (xAI primary, FAL fallback)."""
    path = Path(str(audio_path or ""))
    if not path.is_file():
        return {"error": "audio_missing", "text": "", "segments": []}
    data = path.read_bytes()
    if not data:
        return {"error": "audio_empty", "text": "", "segments": []}
    suffix = path.suffix.lower() or ".mp3"
    provider = ""
    text = ""
    try:
        if _xai_key():
            text = _transcribe_xai(data, filename=path.name, content_type=_mime_for_suffix(suffix))
            provider = "xai"
    except Exception as exc:
        _log.warning("reference xAI STT failed, falling back to FAL: %s", exc)
    if not text:
        try:
            text = _transcribe_fal(data, filename=path.name)
            provider = "fal"
        except Exception as exc:
            return {"error": str(exc)[:240], "text": "", "segments": []}
    if not text:
        return {"error": "no_speech_detected", "text": "", "segments": []}
    return {
        "text": text,
        "segments": [{"start_sec": 0.0, "end_sec": 0.0, "text": text}],
        "word_count": len(text.split()),
        "provider": provider or "unknown",
    }