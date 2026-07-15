"""xAI Grok TTS narration client.

Batch endpoint:
    POST https://api.x.ai/v1/tts

The endpoint returns audio bytes directly. This client mirrors the existing
voice_fal call shape so production code can switch providers without changing
the render flow.
"""
from __future__ import annotations

import os
from pathlib import Path

import httpx

from . import render_simulation

TTS_ENDPOINT = "https://api.x.ai/v1/tts"
VOICES_ENDPOINT = "https://api.x.ai/v1/tts/voices"
DEFAULT_VOICE = "leo"
DEFAULT_LANGUAGE = "en"


class XAITTSError(RuntimeError):
    pass


def _ensure_xai_key() -> str:
    key = str(os.getenv("XAI_API_KEY") or "").strip()
    if not key:
        raise XAITTSError("XAI_API_KEY not set - cannot synthesize narration")
    return key


def resolve_voice_id(*, skeleton: bool = False, explicit: str | None = None) -> str:
    """Pick the xAI voice id for Studio narration."""
    chosen = str(explicit or "").strip()
    if chosen:
        return chosen
    if skeleton:
        return (
            str(os.getenv("SKELETON_XAI_VOICE_ID") or "").strip()
            or str(os.getenv("XAI_TTS_VOICE_ID") or "").strip()
            or DEFAULT_VOICE
        )
    return str(os.getenv("XAI_TTS_VOICE_ID") or "").strip() or "rex"


def list_voices() -> list[dict[str, str]]:
    """Return built-in and custom xAI voices visible to this API key."""
    api_key = _ensure_xai_key()
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        response = client.get(
            VOICES_ENDPOINT,
            headers={"Authorization": f"Bearer {api_key}"},
        )
    if response.status_code not in (200, 201):
        detail = str(response.text or "")[:300]
        raise XAITTSError(f"xAI voice list failed ({response.status_code}): {detail}")
    payload = response.json()
    rows = payload.get("voices") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return []
    output: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        voice_id = str(row.get("voice_id") or "").strip()
        if not voice_id:
            continue
        output.append({
            "voice_id": voice_id,
            "name": str(row.get("name") or voice_id).strip(),
            "type": str(row.get("type") or row.get("kind") or "").strip(),
        })
    return output


def synthesize(
    *,
    text: str,
    out_path: Path,
    voice_id: str | None = None,
    speed: float = 0.95,
    language: str | None = None,
) -> Path:
    """Synthesize narration to out_path and return the path."""
    clean = (text or "").strip()
    if not clean:
        raise XAITTSError("empty narration text")
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if render_simulation.enabled():
        return render_simulation.write_audio(out_path, duration_sec=max(1.0, len(clean) / 18.0))

    api_key = _ensure_xai_key()
    payload = {
        "text": clean,
        "voice_id": resolve_voice_id(explicit=voice_id),
        "language": language or os.getenv("XAI_TTS_LANGUAGE", DEFAULT_LANGUAGE) or DEFAULT_LANGUAGE,
        "output_format": {
            "codec": "mp3",
            "sample_rate": 24000,
            "bit_rate": 128000,
        },
    }
    with httpx.Client(timeout=240, follow_redirects=True) as client:
        response = client.post(
            TTS_ENDPOINT,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
    if response.status_code not in (200, 201):
        detail = str(response.text or "").replace(api_key, "[redacted]")[:300]
        raise XAITTSError(f"xAI TTS failed ({response.status_code}): {detail}")
    if not response.content:
        raise XAITTSError("xAI TTS returned empty audio")
    out_path.write_bytes(response.content)
    return out_path


class XAiVoiceClient:
    provider = "xai"
    operation = "grok_tts"

    def synthesize(
        self,
        *,
        text: str,
        out_path: Path,
        voice_id: str | None = None,
        speed: float = 0.95,
    ) -> Path:
        return synthesize(text=text, out_path=out_path, voice_id=voice_id, speed=speed)