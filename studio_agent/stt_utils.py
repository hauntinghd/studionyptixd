"""Shared STT + safe JSON helpers for dictation and reference analysis."""
from __future__ import annotations

import json
from typing import Any

import httpx


def httpx_json_dict(resp: httpx.Response) -> dict[str, Any]:
    """Parse an HTTP response body as JSON without raising on empty/non-JSON payloads."""
    text = str(resp.text or "").strip()
    if not text:
        return {}
    try:
        data = json.loads(text)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def tool_result_dict(raw: str | None) -> dict[str, Any]:
    """Parse tool/agent JSON text; never raise on empty or whitespace-only bodies."""
    text = str(raw or "").strip()
    if not text:
        return {}
    data = safe_json_loads(text, default={})
    return data if isinstance(data, dict) else {}


def safe_json_loads(raw: str, *, default: Any = None) -> Any:
    text = str(raw or "").strip()
    if not text:
        return {} if default is None else default
    try:
        return json.loads(text)
    except Exception:
        return {} if default is None else default