"""Authenticated WebSocket proxy for xAI streaming STT (live dictation)."""
from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any
from urllib.parse import urlencode

_log = logging.getLogger("nyptid-studio.studio-agent-dictation-stream")

XAI_STT_WS = "wss://api.x.ai/v1/stt"


def _xai_key() -> str:
    return str(os.getenv("XAI_API_KEY", "") or "").strip()


def _upstream_url(*, language: str = "", interim_results: bool = True) -> str:
    """Build xAI streaming STT URL.

    Long paragraph dictation: Smart Turn ends an *utterance*, not the whole session.
    Use a high confidence threshold + max silence timeout so mid-sentence pauses
    (thinking, breathing) do not force speech_final too early.
    smart_turn_timeout max is 5000ms per xAI docs.
    """
    # Higher threshold = fewer false end-of-turn detections while dictating.
    smart_turn = str(os.getenv("XAI_STT_SMART_TURN", "0.9") or "0.9").strip() or "0.9"
    try:
        timeout_ms = int(os.getenv("XAI_STT_SMART_TURN_TIMEOUT_MS", "5000") or "5000")
    except (TypeError, ValueError):
        timeout_ms = 5000
    timeout_ms = max(1, min(timeout_ms, 5000))
    params: dict[str, Any] = {
        "sample_rate": 16000,
        "encoding": "pcm",
        "interim_results": "true" if interim_results else "false",
        "smart_turn": smart_turn,
        "smart_turn_timeout": str(timeout_ms),
    }
    lang = str(language or os.getenv("XAI_STT_LANGUAGE", "en") or "en").strip()
    if lang:
        params["language"] = lang
    return f"{XAI_STT_WS}?{urlencode(params)}"


def _is_asr_timeout_error(exc: BaseException) -> bool:
    text = str(exc or "").lower()
    return "asr stream timed out" in text or "timed out" in text and "asr" in text


async def _bridge_upstream(
    client_ws: Any,
    upstream: Any,
) -> None:
    """Relay xAI transcript events to the browser until either side closes."""

    async def client_to_upstream() -> None:
        try:
            while True:
                message = await client_ws.receive()
                msg_type = str(message.get("type") or "")
                if msg_type == "websocket.disconnect":
                    break
                if msg_type != "websocket.receive":
                    continue
                text = message.get("text")
                if text is not None:
                    payload = str(text or "").strip()
                    if not payload:
                        continue
                    # Ignore client auth frames (token already validated by the router).
                    try:
                        parsed = json.loads(payload)
                        if isinstance(parsed, dict) and str(parsed.get("type") or "").lower() in {
                            "auth",
                            "ping",
                            "hello",
                        }:
                            continue
                    except Exception:
                        pass
                    await upstream.send(payload)
                    continue
                data = message.get("bytes")
                if data:
                    await upstream.send(data)
        except Exception as exc:
            _log.debug("dictation client->upstream ended: %s", exc)
        finally:
            try:
                await upstream.send(json.dumps({"type": "audio.done"}))
            except Exception:
                pass

    async def upstream_to_client() -> None:
        try:
            async for raw in upstream:
                if isinstance(raw, bytes):
                    continue
                event = json.loads(raw)
                # Forward every partial / speech_final / done. Do NOT tear down
                # the stream on the first transcript.done — long paragraphs pause
                # and resume; the client only finalizes when the user stops the mic.
                await client_ws.send_json(event)
                et = str(event.get("type") or "")
                # Mark utterance boundary for the client without closing the socket.
                if et == "transcript.done" or (
                    et == "transcript.partial"
                    and (event.get("speech_final") or event.get("is_final"))
                ):
                    try:
                        await client_ws.send_json({
                            "type": "utterance_boundary",
                            "text": str(event.get("text") or ""),
                        })
                    except Exception:
                        pass
        except Exception as exc:
            _log.debug("dictation upstream->client ended: %s", exc)
            raise

    await asyncio.gather(client_to_upstream(), upstream_to_client())


async def proxy_dictation_stream(
    client_ws: Any,
    *,
    language: str = "",
) -> None:
    """Bridge browser PCM frames to xAI streaming STT and relay transcript events."""
    api_key = _xai_key()
    if not api_key:
        await client_ws.send_json({"type": "error", "message": "XAI_API_KEY not configured"})
        await client_ws.close()
        return

    try:
        import websockets
    except ImportError as exc:
        await client_ws.send_json({"type": "error", "message": f"websockets package missing: {exc}"})
        await client_ws.close()
        return

    upstream_url = _upstream_url(language=language)
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        ping_interval = float(os.getenv("XAI_STT_PING_INTERVAL_SEC", "18") or "18")
    except (TypeError, ValueError):
        ping_interval = 18.0
    try:
        ping_timeout = float(os.getenv("XAI_STT_PING_TIMEOUT_SEC", "45") or "45")
    except (TypeError, ValueError):
        ping_timeout = 45.0
    try:
        max_reconnects = int(os.getenv("XAI_STT_MAX_RECONNECTS", "3") or "3")
    except (TypeError, ValueError):
        max_reconnects = 3

    reconnects = 0
    while reconnects <= max_reconnects:
        try:
            async with websockets.connect(
                upstream_url,
                additional_headers=headers,
                max_size=8 * 1024 * 1024,
                ping_interval=ping_interval,
                ping_timeout=ping_timeout,
                close_timeout=5,
            ) as upstream:
                ready = json.loads(await asyncio.wait_for(upstream.recv(), timeout=20))
                if str(ready.get("type") or "") != "transcript.created":
                    await client_ws.send_json({
                        "type": "error",
                        "message": f"Unexpected xAI STT handshake: {ready}",
                    })
                    await client_ws.close()
                    return
                if reconnects == 0:
                    await client_ws.send_json({"type": "ready"})
                else:
                    await client_ws.send_json({"type": "reconnected"})
                await _bridge_upstream(client_ws, upstream)
                break
        except Exception as exc:
            if _is_asr_timeout_error(exc) and reconnects < max_reconnects:
                reconnects += 1
                _log.info("dictation ASR timeout — reconnect %s/%s", reconnects, max_reconnects)
                try:
                    await client_ws.send_json({
                        "type": "status",
                        "message": "Reconnecting voice stream…",
                    })
                except Exception:
                    pass
                await asyncio.sleep(0.35)
                continue
            _log.warning("dictation stream proxy failed: %s", exc)
            message = str(exc)[:300]
            if _is_asr_timeout_error(exc):
                message = (
                    "Voice stream paused (ASR timeout). Tap the mic again — "
                    "record mode will capture your line if live voice keeps timing out."
                )
            try:
                await client_ws.send_json({"type": "error", "message": message})
            except Exception:
                pass
            break
    # The loop handles connection failures itself; always close the browser
    # socket once it exits.
    try:
        await client_ws.close()
    except Exception:
        pass
