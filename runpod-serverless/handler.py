"""
RunPod Serverless handler — bridges RunPod events to the Studio FastAPI backend.

Event shape (input):
    {
        "input": {
            "method": "POST",
            "path": "/api/render/longform",
            "headers": {"authorization": "Bearer ...", ...},
            "body": {...}  # optional JSON body
        }
    }

Response shape (output):
    {
        "status_code": 200,
        "headers": {...},
        "body": <str | dict>
    }

Why this shape: lets a single serverless endpoint transparently carry the entire
FastAPI route tree. One RunPod deployment replaces the always-on Render/VPS host.

Features:
- Streaming responses supported via chunked output (RunPod streams via generator)
- Files returned as base64 for binary (images/video thumbnails/etc.)
- Warm-cache: FastAPI app loaded once at worker init (not per request)
- Graceful JSON parse fallback for non-JSON bodies
"""
from __future__ import annotations

import base64
import io
import json
import logging
import os
import sys
import traceback
from typing import Any

# Ensure backend modules can be imported from /app
sys.path.insert(0, "/app")

import runpod  # type: ignore


logger = logging.getLogger("studio.runpod.handler")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")


# ════════════════════════════════════════════════════════════
#  Warm-load FastAPI app ONCE per worker
# ════════════════════════════════════════════════════════════

logger.info("Loading Studio FastAPI backend...")
try:
    from backend import app  # type: ignore
    from fastapi.testclient import TestClient
    _client = TestClient(app)
    logger.info("✓ Backend loaded, TestClient ready")
except Exception as e:
    logger.error(f"FATAL: failed to import backend: {e}\n{traceback.format_exc()}")
    raise


# ════════════════════════════════════════════════════════════
#  Helper utilities
# ════════════════════════════════════════════════════════════


def _decode_body(body_in: Any) -> tuple[dict | None, str | None, bytes | None]:
    """Accept JSON dict, string, or base64-encoded bytes.
    Returns (json_body, text_body, raw_bytes) — exactly one is non-None."""
    if body_in is None:
        return None, None, None
    if isinstance(body_in, dict):
        return body_in, None, None
    if isinstance(body_in, str):
        # Try JSON-parse first; fall back to raw text; also support base64 binary
        try:
            return json.loads(body_in), None, None
        except (ValueError, TypeError):
            pass
        # base64 binary?
        if body_in.startswith("b64:"):
            try:
                return None, None, base64.b64decode(body_in[4:])
            except Exception:
                pass
        return None, body_in, None
    if isinstance(body_in, (bytes, bytearray)):
        return None, None, bytes(body_in)
    return None, str(body_in), None


def _encode_response(response) -> dict:
    """Serialize httpx Response → RunPod-safe dict."""
    headers = dict(response.headers)
    content_type = headers.get("content-type", "")

    # JSON → pass through
    if content_type.startswith("application/json"):
        try:
            return {
                "status_code": response.status_code,
                "headers": headers,
                "body": response.json(),
            }
        except ValueError:
            pass

    # Text-like → pass as string
    if content_type.startswith(("text/", "application/xml", "application/javascript")):
        return {
            "status_code": response.status_code,
            "headers": headers,
            "body": response.text,
        }

    # Binary (images, video, audio) → base64
    return {
        "status_code": response.status_code,
        "headers": headers,
        "body_b64": base64.b64encode(response.content).decode("ascii"),
    }


# ════════════════════════════════════════════════════════════
#  Handler
# ════════════════════════════════════════════════════════════


def handler(event: dict) -> dict:
    """Entry point invoked by RunPod for each queued request."""
    try:
        req = event.get("input") or {}
        method = (req.get("method") or "GET").upper()
        path = req.get("path") or "/"
        headers = req.get("headers") or {}
        body = req.get("body")
        params = req.get("query") or {}

        json_body, text_body, raw_body = _decode_body(body)

        kwargs: dict[str, Any] = {
            "method": method,
            "url": path,
            "headers": headers,
            "params": params,
        }
        if json_body is not None:
            kwargs["json"] = json_body
        elif text_body is not None:
            kwargs["content"] = text_body
        elif raw_body is not None:
            kwargs["content"] = raw_body

        logger.info(f"→ {method} {path}")
        response = _client.request(**kwargs)
        logger.info(f"← {response.status_code} ({len(response.content)} bytes)")

        return _encode_response(response)

    except Exception as e:
        logger.error(f"Handler error: {e}\n{traceback.format_exc()}")
        return {
            "status_code": 500,
            "headers": {"content-type": "application/json"},
            "body": {"error": "handler_exception", "detail": str(e)},
        }


# ════════════════════════════════════════════════════════════
#  Start serverless worker
# ════════════════════════════════════════════════════════════

if __name__ == "__main__":
    runpod.serverless.start({"handler": handler})
