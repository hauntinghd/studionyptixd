"""Fail-closed compatibility endpoint for retired live dictation streaming."""
from __future__ import annotations

from typing import Any

LIVE_DICTATION_DISABLED_CODE = "live_dictation_disabled"
LIVE_DICTATION_DISABLED_MESSAGE = (
    "Live streaming dictation is disabled. Record audio and use the FAL "
    "transcription endpoint instead."
)


async def proxy_dictation_stream(
    client_ws: Any,
    *,
    language: str = "",
) -> None:
    """Reject the retired websocket route without opening an upstream socket."""
    del language
    try:
        await client_ws.send_json(
            {
                "type": "error",
                "code": LIVE_DICTATION_DISABLED_CODE,
                "message": LIVE_DICTATION_DISABLED_MESSAGE,
            }
        )
    finally:
        try:
            await client_ws.close()
        except Exception:
            pass


__all__ = [
    "LIVE_DICTATION_DISABLED_CODE",
    "LIVE_DICTATION_DISABLED_MESSAGE",
    "proxy_dictation_stream",
]
