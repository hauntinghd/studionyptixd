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

DEFAULT_VOICE = "leo"
DEFAULT_LANGUAGE = "en"


class XAITTSError(RuntimeError):
    pass


def _ensure_xai_key() -> str:
    raise XAITTSError(
        "xAI TTS is disabled by Studio provider policy; use FAL MiniMax narration."
    )


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
    """Compatibility boundary that cannot contact xAI."""
    _ensure_xai_key()
    return []


def synthesize(
    *,
    text: str,
    out_path: Path,
    voice_id: str | None = None,
    speed: float = 0.95,
    language: str | None = None,
) -> Path:
    """Synthesize narration to out_path and return the path."""
    _ = (text, out_path, voice_id, speed, language)
    _ensure_xai_key()
    raise AssertionError("unreachable")


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
