"""Provider-selecting narration client for Studio production."""
from __future__ import annotations

import os
from pathlib import Path

from .voice_fal import FalVoiceClient
from .voice_xai import XAiVoiceClient


class AutoVoiceClient:
    """Prefer fal MiniMax TTS; xAI remains an optional fallback when not fal-only."""

    def __init__(self, provider: str | None = None):
        self.provider_preference = str(provider or os.getenv("STUDIO_TTS_PROVIDER", "fal") or "fal").strip().lower()
        self.last_provider = ""
        self.last_error = ""
        self._xai = XAiVoiceClient()
        self._fal = FalVoiceClient()

    def synthesize(
        self,
        *,
        text: str,
        out_path: Path,
        voice_id: str | None = None,
        speed: float = 0.95,
    ) -> Path:
        providers = ["fal", "xai"] if self.provider_preference not in {"xai", "grok"} else ["xai", "fal"]
        if self.provider_preference in {"xai_only", "grok_only"}:
            providers = ["xai"]
        if self.provider_preference in {"fal_only", "minimax_only"}:
            providers = ["fal"]

        errors: list[str] = []
        for provider in providers:
            try:
                if provider == "xai":
                    result = self._xai.synthesize(text=text, out_path=out_path, voice_id=voice_id, speed=speed)
                else:
                    result = self._fal.synthesize(text=text, out_path=out_path, voice_id=voice_id, speed=speed)
                self.last_provider = provider
                self.last_error = ""
                return result
            except Exception as exc:  # noqa: BLE001 - fallback provider should capture any client failure
                errors.append(f"{provider}: {str(exc)[:220]}")
                self.last_error = "; ".join(errors)
                continue
        raise RuntimeError(f"All TTS providers failed: {'; '.join(errors)}")