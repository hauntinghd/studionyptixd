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

# Display metadata for Studio Agent model picker (merged with live OpenRouter pricing).
CURATED_META: dict[str, dict[str, Any]] = {
    "anthropic/claude-sonnet-4": {
        "name": "Claude Sonnet 4",
        "provider": "Anthropic",
        "description": "Best balance of tool use, reasoning, and production planning.",
        "recommended": True,
        "intelligence": 5,
        "speed": 4,
    },
    "anthropic/claude-3.5-sonnet": {
        "name": "Claude 3.5 Sonnet",
        "provider": "Anthropic",
        "description": "Reliable orchestrator with strong instruction following.",
        "recommended": True,
        "intelligence": 5,
        "speed": 4,
    },
    "openai/gpt-4o": {
        "name": "GPT-4o",
        "provider": "OpenAI",
        "description": "General-purpose runner with solid tool calling.",
        "recommended": True,
        "intelligence": 5,
        "speed": 4,
    },
    "openai/gpt-4o-mini": {
        "name": "GPT-4o Mini",
        "provider": "OpenAI",
        "description": "Cheaper OpenAI runner for drafts and iteration.",
        "recommended": False,
        "intelligence": 4,
        "speed": 5,
    },
    "google/gemini-2.5-pro-preview": {
        "name": "Gemini 2.5 Pro",
        "provider": "Google",
        "description": "Long context + strong multimodal reasoning.",
        "recommended": True,
        "intelligence": 5,
        "speed": 3,
    },
    "google/gemini-2.0-flash-001": {
        "name": "Gemini 2.0 Flash",
        "provider": "Google",
        "description": "Fast, low-cost runner for high-volume chat loops.",
        "recommended": True,
        "intelligence": 4,
        "speed": 5,
    },
    "meta-llama/llama-3.3-70b-instruct": {
        "name": "Llama 3.3 70B",
        "provider": "Meta",
        "description": "Open-weight workhorse for tool-heavy sessions.",
        "recommended": False,
        "intelligence": 4,
        "speed": 4,
    },
    "deepseek/deepseek-chat": {
        "name": "DeepSeek Chat",
        "provider": "DeepSeek",
        "description": "Cost-efficient runner with good coding/tool use.",
        "recommended": True,
        "intelligence": 4,
        "speed": 5,
    },
    "qwen/qwen-2.5-72b-instruct": {
        "name": "Qwen 2.5 72B",
        "provider": "Qwen",
        "description": "Strong multilingual + structured output.",
        "recommended": False,
        "intelligence": 4,
        "speed": 4,
    },
}


def _provider_from_id(model_id: str) -> str:
    slug = model_id.split("/")[0] if "/" in model_id else model_id
    return slug.replace("-", " ").title()


def _price_per_mtok(raw: Any) -> float | None:
    try:
        if raw is None:
            return None
        per_token = float(raw)
        return round(per_token * 1_000_000, 4)
    except (TypeError, ValueError):
        return None


def build_model_catalog(live: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Merge curated display metadata with live OpenRouter model list + pricing."""
    live_by_id: dict[str, dict[str, Any]] = {}
    for item in live or []:
        mid = str(item.get("id") or "")
        if mid:
            live_by_id[mid] = item

    catalog: list[dict[str, Any]] = []
    seen: set[str] = set()

    for mid in RECOMMENDED_MODELS:
        seen.add(mid)
        meta = CURATED_META.get(mid, {})
        live_row = live_by_id.get(mid, {})
        pricing = live_row.get("pricing") if isinstance(live_row.get("pricing"), dict) else {}
        catalog.append({
            "id": mid,
            "name": meta.get("name") or live_row.get("name") or mid.split("/")[-1],
            "provider": meta.get("provider") or _provider_from_id(mid),
            "description": meta.get("description") or live_row.get("description") or "",
            "context_length": live_row.get("context_length"),
            "prompt_price_per_m": _price_per_mtok(pricing.get("prompt")),
            "completion_price_per_m": _price_per_mtok(pricing.get("completion")),
            "recommended": bool(meta.get("recommended", mid in RECOMMENDED_MODELS[:5])),
            "intelligence": meta.get("intelligence"),
            "speed": meta.get("speed"),
        })

    for mid, live_row in live_by_id.items():
        if mid in seen:
            continue
        if not any(k in mid for k in ("claude", "gpt", "gemini", "deepseek", "llama", "qwen", "grok")):
            continue
        meta = CURATED_META.get(mid, {})
        pricing = live_row.get("pricing") if isinstance(live_row.get("pricing"), dict) else {}
        catalog.append({
            "id": mid,
            "name": meta.get("name") or live_row.get("name") or mid,
            "provider": meta.get("provider") or _provider_from_id(mid),
            "description": meta.get("description") or "",
            "context_length": live_row.get("context_length"),
            "prompt_price_per_m": _price_per_mtok(pricing.get("prompt")),
            "completion_price_per_m": _price_per_mtok(pricing.get("completion")),
            "recommended": bool(meta.get("recommended")),
            "intelligence": meta.get("intelligence"),
            "speed": meta.get("speed"),
        })

    return catalog


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
