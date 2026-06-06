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


def _infer_speed(model_id: str, prompt_ppm: float | None) -> int | None:
    slug = model_id.lower()
    if any(k in slug for k in ("flash", "mini", "turbo", "lite", "fast", "haiku")):
        return 5
    if any(k in slug for k in ("opus", "pro", "reasoning", "o1", "o3")):
        return 2
    if prompt_ppm is not None and prompt_ppm >= 15:
        return 2
    if prompt_ppm is not None and prompt_ppm <= 0.5:
        return 5
    return 3


def _infer_intelligence(model_id: str, prompt_ppm: float | None) -> int | None:
    slug = model_id.lower()
    if any(k in slug for k in ("opus", "o1", "o3", "sonnet-4", "gpt-4o", "gemini-2.5-pro")):
        return 5
    if any(k in slug for k in ("mini", "nano", "lite", "8b", "7b")):
        return 3
    if prompt_ppm is not None and prompt_ppm >= 10:
        return 5
    if prompt_ppm is not None and prompt_ppm >= 3:
        return 4
    return 3


def _catalog_row(mid: str, live_row: dict[str, Any]) -> dict[str, Any]:
    meta = CURATED_META.get(mid, {})
    pricing = live_row.get("pricing") if isinstance(live_row.get("pricing"), dict) else {}
    prompt_ppm = _price_per_mtok(pricing.get("prompt"))
    completion_ppm = _price_per_mtok(pricing.get("completion"))
    return {
        "id": mid,
        "name": meta.get("name") or live_row.get("name") or mid.split("/")[-1],
        "provider": meta.get("provider") or _provider_from_id(mid),
        "description": meta.get("description") or live_row.get("description") or "",
        "context_length": live_row.get("context_length"),
        "prompt_price_per_m": prompt_ppm,
        "completion_price_per_m": completion_ppm,
        "recommended": bool(meta.get("recommended")),
        "intelligence": meta.get("intelligence") or _infer_intelligence(mid, prompt_ppm),
        "speed": meta.get("speed") or _infer_speed(mid, prompt_ppm),
    }


def build_model_catalog(live: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Merge curated display metadata with the full live OpenRouter model list + pricing."""
    live_by_id: dict[str, dict[str, Any]] = {}
    for item in live or []:
        mid = str(item.get("id") or "")
        if mid:
            live_by_id[mid] = item

    catalog: list[dict[str, Any]] = []
    seen: set[str] = set()

    for mid in RECOMMENDED_MODELS:
        seen.add(mid)
        live_row = live_by_id.get(mid, {"id": mid})
        row = _catalog_row(mid, live_row)
        if not row.get("recommended"):
            row["recommended"] = mid in RECOMMENDED_MODELS[:6]
        catalog.append(row)

    extras: list[dict[str, Any]] = []
    for mid, live_row in live_by_id.items():
        if mid in seen:
            continue
        arch = live_row.get("architecture") if isinstance(live_row.get("architecture"), dict) else {}
        modality = str(arch.get("modality") or "").lower()
        if modality and "text" not in modality:
            continue
        extras.append(_catalog_row(mid, live_row))

    extras.sort(key=lambda r: (r.get("provider") or "", r.get("name") or r["id"]))
    catalog.extend(extras)
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


# In-memory model-pricing cache for per-turn credit metering (TTL seconds).
_MODELS_CACHE: dict[str, Any] = {"at": 0.0, "by_id": {}}
_MODELS_TTL = 1800.0


async def model_pricing(model_id: str) -> tuple[float | None, float | None]:
    """Return (prompt_price_per_m, completion_price_per_m) USD for a model.

    Cached for _MODELS_TTL to avoid a /models round-trip on every chat turn.
    """
    import time as _t

    now = _t.time()
    if now - float(_MODELS_CACHE.get("at", 0) or 0) > _MODELS_TTL or not _MODELS_CACHE.get("by_id"):
        try:
            live = await list_models()
            _MODELS_CACHE["by_id"] = {str(m.get("id")): m for m in live if m.get("id")}
            _MODELS_CACHE["at"] = now
        except Exception:
            pass
    row = (_MODELS_CACHE.get("by_id") or {}).get(model_id) or {}
    pricing = row.get("pricing") if isinstance(row.get("pricing"), dict) else {}
    return _price_per_mtok(pricing.get("prompt")), _price_per_mtok(pricing.get("completion"))


REASONING_DEPTHS = ("fast", "balanced", "deep")


def reasoning_params(
    depth: str = "balanced",
    *,
    model: str | None = None,
) -> tuple[float, dict[str, Any] | None]:
    """Map UI thinking depth → OpenRouter reasoning + temperature."""
    key = str(depth or "balanced").strip().lower()
    if key not in REASONING_DEPTHS:
        key = "balanced"
    mid = str(model or "").lower()

    if key == "fast":
        return 0.25, None

    if key == "deep":
        temp = 0.35
        reasoning: dict[str, Any] = {"enabled": True, "effort": "high"}
        if "claude" in mid or "anthropic" in mid:
            reasoning = {"enabled": True, "max_tokens": 10_000}
        return temp, reasoning

    # balanced
    return 0.4, {"enabled": True, "effort": "low"}


async def chat_completion(
    *,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    model: str | None = None,
    temperature: float | None = None,
    reasoning_depth: str = "balanced",
    web_search: bool = False,
) -> dict[str, Any]:
    temp, reasoning = reasoning_params(reasoning_depth, model=model or DEFAULT_MODEL)
    if temperature is not None:
        temp = float(temperature)
    payload: dict[str, Any] = {
        "model": model or DEFAULT_MODEL,
        "messages": messages,
        "temperature": temp,
    }
    if reasoning:
        payload["reasoning"] = reasoning
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"
    if web_search:
        # OpenRouter's built-in web plugin: runs a live web search (Exa) and
        # injects results before the model answers — works with any model slug.
        try:
            max_results = int(os.getenv("STUDIO_AGENT_WEB_MAX_RESULTS", "5"))
        except (TypeError, ValueError):
            max_results = 5
        payload["plugins"] = [{"id": "web", "max_results": max(1, min(max_results, 10))}]

    timeout = 180.0 if reasoning_depth == "deep" else 120.0
    async with httpx.AsyncClient(timeout=timeout) as client:
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
