"""fal MiniMax TTS (speech-02-hd) narration.

Drop-in replacement for the retired ElevenLabs client — same call shape:
    synthesize(text=..., out_path=..., voice_id=...)

ElevenLabs was retired (dead key returned 302 redirects in production). fal
MiniMax `speech-02-hd` is the canonical narration tier across NYPTID pipelines.
"""
from __future__ import annotations

import os
import urllib.parse
import urllib.request
from pathlib import Path

try:
    import fal_client
except Exception:  # pragma: no cover - optional when simulation mode is active
    fal_client = None  # type: ignore[assignment]

from . import render_simulation

TTS_ENDPOINT = "fal-ai/minimax/speech-02-hd"
DEFAULT_VOICE = "English_Trustworthy_Man"


def _require_https_url(url: object) -> str:
    value = str(url or "").strip()
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme.lower() != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise RuntimeError("fal returned a non-HTTPS or malformed audio URL")
    return value


def resolve_voice_id(*, skeleton: bool = False, explicit: str | None = None) -> str:
    """Pick the fal MiniMax voice id for Studio narration."""
    chosen = str(explicit or "").strip()
    if chosen:
        return chosen
    if skeleton:
        return (
            str(os.getenv("SKELETON_FAL_VOICE_ID") or "").strip()
            or str(os.getenv("FAL_TTS_VOICE_ID") or "").strip()
            or DEFAULT_VOICE
        )
    return str(os.getenv("FAL_TTS_VOICE_ID") or "").strip() or DEFAULT_VOICE


def _ensure_fal_key() -> str:
    key = (os.getenv("FAL_AI_KEY", "") or os.getenv("FAL_KEY", "")).strip()
    if not key:
        raise RuntimeError("FAL_AI_KEY not set — cannot synthesize narration")
    os.environ["FAL_KEY"] = key  # fal_client reads this
    return key


def synthesize(
    *,
    text: str,
    out_path: Path,
    voice_id: str | None = None,
    speed: float = 0.95,
) -> Path:
    """Synthesize narration to out_path (mp3) and return the path."""
    clean = (text or "").strip()
    if not clean:
        raise RuntimeError("empty narration text")
    if render_simulation.enabled():
        return render_simulation.write_audio(out_path, duration_sec=max(1.0, len(clean) / 18.0))
    _ensure_fal_key()
    result = fal_client.subscribe(
        TTS_ENDPOINT,
        arguments={
            "text": clean,
            "voice_setting": {
                "voice_id": voice_id or DEFAULT_VOICE,
                "speed": speed,
                "emotion": "neutral",
            },
            "audio_setting": {
                "sample_rate": 32000,
                "bitrate": 128000,
                "format": "mp3",
            },
        },
    )
    audio = result.get("audio") if isinstance(result, dict) else None
    audio_url = audio.get("url") if isinstance(audio, dict) else (result or {}).get("audio_url")
    if not audio_url:
        raise RuntimeError(
            f"minimax returned no audio url; keys={list((result or {}).keys())}"
        )
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    safe_audio_url = _require_https_url(audio_url)
    # The download URL is validated as HTTPS above.
    urllib.request.urlretrieve(safe_audio_url, str(out_path))  # nosec B310
    return out_path


class FalVoiceClient:
    """Object-shim so existing `el.synthesize(...)` call sites work unchanged."""

    def synthesize(
        self,
        *,
        text: str,
        out_path: Path,
        voice_id: str | None = None,
        speed: float = 0.95,
    ) -> Path:
        return synthesize(text=text, out_path=out_path, voice_id=voice_id, speed=speed)
