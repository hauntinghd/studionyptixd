"""OpenRouter chat client (OpenAI-compatible tool calling)."""
from __future__ import annotations

import json
import os
from typing import Any

import httpx

OPENROUTER_BASE = "https://openrouter.ai/api/v1"
DEFAULT_MODEL = os.getenv("STUDIO_AGENT_MODEL", "anthropic/claude-sonnet-4")

# Curated models with tool-use support; full list via GET /models.
RECOMMENDED_MODELS = [
    "anthropic/claude-sonnet-4",
    "anthropic/claude-3.5-sonnet",
    "openai/gpt-4o",
    "openai/gpt-4o-mini",
    "google/gemini-2.5-pro-preview",
    "google/gemini-2.0-flash-001",
    "meta-llama/llama-3.3-70b-instruct",
    "deepseek/deepseek-chat",
    "qwen/qwen-2.5-72b-instruct",
]


def api_key() -> str:
    key = (
        os.getenv("OPENROUTER_API_KEY", "").strip()
        or os.getenv("OPEN_ROUTER_API_KEY", "").strip()
    )
    if not key:
        raise RuntimeError(
            "OPENROUTER_API_KEY is not set. Add it to .env (never commit the key)."
        )
    return key


def _headers() -> dict[str, str]:
    h = {
        "Authorization": f"Bearer {api_key()}",
        "Content-Type": "application/json",
    }
    referer = os.getenv("OPENROUTER_HTTP_REFERER", "https://studio.nyptidindustries.com")
    title = os.getenv("OPENROUTER_APP_TITLE", "NYPTID Studio Agent")
    if referer:
        h["HTTP-Referer"] = referer
    if title:
        h["X-Title"] = title
    return h


async def list_models() -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{OPENROUTER_BASE}/models", headers=_headers())
        r.raise_for_status()
        data = r.json()
    items = data.get("data") if isinstance(data, dict) else data
    if not isinstance(items, list):
        return [{"id": m, "name": m} for m in RECOMMENDED_MODELS]
    return items


async def chat_completion(
    *,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    model: str | None = None,
    temperature: float = 0.4,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "model": model or DEFAULT_MODEL,
        "messages": messages,
        "temperature": temperature,
    }
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"

    async with httpx.AsyncClient(timeout=120.0) as client:
        r = await client.post(
            f"{OPENROUTER_BASE}/chat/completions",
            headers=_headers(),
            json=payload,
        )
        if r.status_code >= 400:
            detail = r.text[:2000]
            raise RuntimeError(f"OpenRouter {r.status_code}: {detail}")
        return r.json()


def message_from_response(resp: dict[str, Any]) -> dict[str, Any]:
    choice = (resp.get("choices") or [{}])[0]
    return choice.get("message") or {}


def usage_from_response(resp: dict[str, Any]) -> dict[str, Any]:
    return resp.get("usage") or {}
