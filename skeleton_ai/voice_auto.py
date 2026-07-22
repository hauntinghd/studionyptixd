"""Provider-selecting narration client for Studio production."""
from __future__ import annotations

import os
from pathlib import Path

from .voice_fal import FalVoiceClient


class AutoVoiceClient:
    """Studio narration client. Provider policy permits FAL MiniMax only."""

    def __init__(self, provider: str | None = None):
        requested = str(provider or os.getenv("STUDIO_TTS_PROVIDER", "fal") or "fal").strip().lower()
        # Persisted xAI/auto preferences are migrated, never honored.
        self.provider_preference = "fal"
        self.migrated_from = requested if requested not in {"", "fal", "fal_only", "minimax", "minimax_only"} else ""
        self.last_provider = ""
        self.last_error = ""
        self._fal = FalVoiceClient()

    def synthesize(
        self,
        *,
        text: str,
        out_path: Path,
        voice_id: str | None = None,
        speed: float = 0.95,
    ) -> Path:
        try:
            result = self._fal.synthesize(text=text, out_path=out_path, voice_id=voice_id, speed=speed)
        except Exception as exc:
            self.last_error = f"fal: {str(exc)[:220]}"
            raise RuntimeError(f"FAL TTS failed: {str(exc)[:300]}") from exc
        self.last_provider = "fal"
        self.last_error = ""
        return result
