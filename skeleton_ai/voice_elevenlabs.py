"""
ElevenLabs TTS client for Skeleton AI narration.

Endpoint: https://api.elevenlabs.io/v1/text-to-speech/{voice_id}
Auth:     xi-api-key header

Default voice: Brian (confident male documentary narrator).
Voice list endpoint: GET /v1/voices

Server returns 401 with "Invalid API key" if key is dead — caller surfaces
to user as a config error, not blind retry.
"""
from __future__ import annotations
import os
import time
import httpx
from pathlib import Path

EL_BASE = "https://api.elevenlabs.io/v1"


class ElevenLabsAuthError(RuntimeError):
    """Raised when ELEVENLABS_API_KEY is missing or rejected by server."""


class ElevenLabsClient:
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("ELEVENLABS_API_KEY", "").strip()
        if not self.api_key:
            raise ElevenLabsAuthError("ELEVENLABS_API_KEY not set in env")
        self._headers = {"xi-api-key": self.api_key}

    def list_voices(self) -> list[dict]:
        """Return full list of voices available to the account."""
        with httpx.Client(timeout=30) as c:
            r = c.get(f"{EL_BASE}/voices", headers=self._headers)
        if r.status_code == 401:
            raise ElevenLabsAuthError(f"ElevenLabs rejected key: {r.text[:200]}")
        r.raise_for_status()
        return r.json().get("voices", [])

    def synthesize(
        self,
        text: str,
        out_path: Path,
        *,
        voice_id: str | None = None,
        model_id: str = "eleven_turbo_v2_5",
        stability: float = 0.5,
        similarity_boost: float = 0.7,
        style: float = 0.0,
        speed: float = 1.0,
    ) -> Path:
        """Synthesize speech and save to out_path. Returns the path."""
        out_path = Path(out_path)
        if out_path.exists() and out_path.stat().st_size > 1024:
            return out_path

        voice_id = voice_id or os.getenv("ELEVENLABS_VOICE_DEFAULT", "")
        if not voice_id:
            # Fallback to Brian (a stable preset voice id).
            voice_id = "nPczCjzI2devNBz1zQrb"  # Brian

        payload = {
            "text": text,
            "model_id": model_id,
            "voice_settings": {
                "stability": stability,
                "similarity_boost": similarity_boost,
                "style": style,
                "use_speaker_boost": True,
                "speed": speed,
            },
        }

        url = f"{EL_BASE}/text-to-speech/{voice_id}"
        with httpx.Client(timeout=180) as c:
            r = c.post(url, headers={**self._headers, "Content-Type": "application/json"},
                       json=payload)
        if r.status_code == 401:
            raise ElevenLabsAuthError(f"ElevenLabs rejected key: {r.text[:200]}")
        if r.status_code != 200:
            raise RuntimeError(f"ElevenLabs {r.status_code}: {r.text[:300]}")

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "wb") as f:
            f.write(r.content)
        return out_path


# Approx cost per minute of TTS at ElevenLabs Creator tier.
# 1000 chars ≈ 1 minute speech ≈ ~$0.10-0.15.
EST_COST_PER_1K_CHARS = 0.10
