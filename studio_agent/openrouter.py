"""OpenRouter chat client (OpenAI-compatible tool calling)."""
from __future__ import annotations

import json
import os
from typing import Any

import httpx

from studio_agent import model_registry

OPENROUTER_BASE = "https://openrouter.ai/api/v1"
XAI_BASE = os.getenv("XAI_BASE_URL", "https://api.x.ai/v1").rstrip("/")
DEFAULT_MODEL = os.getenv("STUDIO_AGENT_MODEL", "claude-sonnet-4-6")
PRIMARY_PROVIDER = os.getenv("STUDIO_AGENT_PRIMARY_PROVIDER", os.getenv("STUDIO_AGENT_LLM_PROVIDER", "anthropic")).strip().lower()
ANTHROPIC_BASE = os.getenv("ANTHROPIC_BASE_URL", "https://api.anthropic.com/v1").rstrip("/")
_ANTHROPIC_FALLBACK_MODEL_ENV = os.getenv("ANTHROPIC_FALLBACK_MODEL", "").strip()
_ANTHROPIC_FALLBACK_MODELS_ENV = os.getenv("ANTHROPIC_FALLBACK_MODELS", "").strip()


def _normalize_anthropic_model(model: str) -> str:
    """Normalize formatting aliases without changing the selected model version."""
    model = model.strip()
    aliases = {
        "anthropic/claude-haiku-4-5": "claude-haiku-4-5-20251001",
        "claude-haiku-4-5": "claude-haiku-4-5-20251001",
        "claude-4.5-haiku": "claude-haiku-4-5-20251001",
        "claude-haiku-4.5": "claude-haiku-4-5-20251001",
        "anthropic/claude-haiku-4.5": "claude-haiku-4-5-20251001",
        "anthropic/claude-opus-4-8": "claude-opus-4-8",
        "opus": "claude-opus-4-8",
        "claude-opus-4.8": "claude-opus-4-8",
        "anthropic/claude-fable-5": "claude-fable-5",
        "claude-fable-5": "claude-fable-5",
        "anthropic/claude-sonnet-4-6": "claude-sonnet-4-6",
        "claude-sonnet-4.6": "claude-sonnet-4-6",
        "sonnet": "claude-sonnet-4-6",
    }
    return aliases.get(model, model)


def _anthropic_fallback_model_list() -> list[str]:
    raw = _ANTHROPIC_FALLBACK_MODELS_ENV or _ANTHROPIC_FALLBACK_MODEL_ENV or "claude-haiku-4-5-20251001"
    models: list[str] = []
    for value in raw.split(","):
        model = _normalize_anthropic_model(value)
        if model and model not in models:
            models.append(model)
    if "claude-haiku-4-5-20251001" not in models:
        models.append("claude-haiku-4-5-20251001")
    return models


ANTHROPIC_FALLBACK_MODELS = _anthropic_fallback_model_list()
ANTHROPIC_FALLBACK_MODEL = ANTHROPIC_FALLBACK_MODELS[0] if ANTHROPIC_FALLBACK_MODELS else "claude-haiku-4-5-20251001"
ANTHROPIC_FALLBACK_PROMPT_CHAR_BUDGET = int(os.getenv("ANTHROPIC_FALLBACK_PROMPT_CHAR_BUDGET", "70000"))
ANTHROPIC_FALLBACK_KEEP_RECENT_MESSAGES = int(os.getenv("ANTHROPIC_FALLBACK_KEEP_RECENT_MESSAGES", "10"))
ANTHROPIC_FALLBACK_MAX_MESSAGE_CHARS = int(os.getenv("ANTHROPIC_FALLBACK_MAX_MESSAGE_CHARS", "2500"))
ANTHROPIC_FALLBACK_MAX_SYSTEM_CHARS = int(os.getenv("ANTHROPIC_FALLBACK_MAX_SYSTEM_CHARS", "10000"))
ANTHROPIC_FALLBACK_HARD_PROMPT_CHAR_BUDGET = int(os.getenv("ANTHROPIC_FALLBACK_HARD_PROMPT_CHAR_BUDGET", "36000"))
ANTHROPIC_FALLBACK_TOOL_CHAR_BUDGET = int(os.getenv("ANTHROPIC_FALLBACK_TOOL_CHAR_BUDGET", "32000"))

# Curated models with tool-use support; full list via GET /models (Anthropic + xAI + OpenRouter).
RECOMMENDED_MODELS = [
    "claude-sonnet-4-6",
    "claude-opus-4-8",
    "claude-haiku-4-5-20251001",
    "claude-fable-5",
    "grok-4.5",
    "grok-4.3",
    "grok-4.20-0309-reasoning",
    "grok-4.20-0309-non-reasoning",
]

# Static USD / 1M tokens when the provider /models response omits pricing.
# Prefer live API pricing when present. Sources: Anthropic + xAI public pricing tables.
# Display metadata for Studio Agent model picker (merged with live API pricing).
CURATED_META: dict[str, dict[str, Any]] = {
    "claude-sonnet-4-6": {
        "name": "Claude Sonnet 4.6",
        "provider": "Anthropic",
        "description": "Default Studio runner: strong tool use, planning, and production orchestration.",
        "recommended": True,
        "intelligence": 5,
        "speed": 4,
        "context_length": 200_000,
        "prompt_price_per_m": 3.0,
        "completion_price_per_m": 15.0,
    },
    "claude-opus-4-8": {
        "name": "Claude Opus 4.8",
        "provider": "Anthropic",
        "description": "Highest-depth Claude runner for complex planning and long production sessions.",
        "recommended": True,
        "intelligence": 5,
        "speed": 2,
        "context_length": 200_000,
        "prompt_price_per_m": 15.0,
        "completion_price_per_m": 75.0,
    },
    "claude-haiku-4-5-20251001": {
        "name": "Claude Haiku 4.5",
        "provider": "Anthropic",
        "description": "Fast, lower-cost Claude runner for status checks and lightweight tool loops.",
        "recommended": True,
        "intelligence": 4,
        "speed": 5,
        "context_length": 200_000,
        "prompt_price_per_m": 1.0,
        "completion_price_per_m": 5.0,
    },
    "claude-fable-5": {
        "name": "Claude Fable 5",
        "provider": "Anthropic",
        "description": "Anthropic creative model when enabled for this API account.",
        "recommended": False,
        "intelligence": 5,
        "speed": 3,
        "context_length": 200_000,
        "prompt_price_per_m": 3.0,
        "completion_price_per_m": 15.0,
    },
    # xAI Grok chat models (api.x.ai) — same key as speech dictation / Imagine when configured
    "grok-4.5": {
        "name": "Grok 4.5",
        "provider": "xAI",
        "description": "xAI flagship for code, agentic tool calling, and low-hallucination Studio runs.",
        "recommended": True,
        "intelligence": 5,
        "speed": 4,
        "context_length": 500_000,
        "prompt_price_per_m": 2.0,
        "completion_price_per_m": 6.0,
    },
    "grok-4.3": {
        "name": "Grok 4.3",
        "provider": "xAI",
        "description": "Strong general-purpose Grok runner with 1M context and solid tool use.",
        "recommended": True,
        "intelligence": 5,
        "speed": 4,
        "context_length": 1_000_000,
        "prompt_price_per_m": 1.25,
        "completion_price_per_m": 2.50,
    },
    "grok-4.20-0309-reasoning": {
        "name": "Grok 4.20 Reasoning",
        "provider": "xAI",
        "description": "Reasoning-optimized Grok 4.20 for deep planning and multi-step tool loops.",
        "recommended": True,
        "intelligence": 5,
        "speed": 3,
        "context_length": 1_000_000,
        "prompt_price_per_m": 1.25,
        "completion_price_per_m": 2.50,
    },
    "grok-4.20-0309-non-reasoning": {
        "name": "Grok 4.20 Non-Reasoning",
        "provider": "xAI",
        "description": "Fast Grok 4.20 without extra reasoning overhead — good for light orchestration.",
        "recommended": True,
        "intelligence": 4,
        "speed": 5,
        "context_length": 1_000_000,
        "prompt_price_per_m": 1.25,
        "completion_price_per_m": 2.50,
    },
    "grok-4.20-multi-agent-0309": {
        "name": "Grok 4.20 Multi-Agent",
        "provider": "xAI",
        "description": "Multi-agent orchestration SKU for long context and agent loops.",
        "recommended": False,
        "intelligence": 5,
        "speed": 3,
        "context_length": 1_000_000,
        "prompt_price_per_m": 1.25,
        "completion_price_per_m": 2.50,
    },
    # Code API (chat-completions compatible) — official docs.x.ai pricing
    "grok-build-0.1": {
        "name": "Grok Build 0.1",
        "provider": "xAI",
        "description": "xAI code-focused model (Code API pricing). Solid for tool-heavy agent loops.",
        "recommended": False,
        "intelligence": 4,
        "speed": 4,
        "context_length": 256_000,
        "prompt_price_per_m": 1.0,
        "completion_price_per_m": 2.0,
    },
    "grok-4": {
        "name": "Grok 4",
        "provider": "xAI",
        "description": "Earlier Grok 4 flagship (if enabled on your xAI account).",
        "recommended": False,
        "intelligence": 5,
        "speed": 3,
        "context_length": 256_000,
        "prompt_price_per_m": 3.0,
        "completion_price_per_m": 15.0,
    },
    "grok-3": {
        "name": "Grok 3",
        "provider": "xAI",
        "description": "Legacy Grok 3 chat model when available on the account.",
        "recommended": False,
        "intelligence": 4,
        "speed": 4,
        "context_length": 131_072,
        "prompt_price_per_m": 2.0,
        "completion_price_per_m": 10.0,
    },
    "grok-3-mini": {
        "name": "Grok 3 Mini",
        "provider": "xAI",
        "description": "Cheapest Grok chat SKU for quick status / light tool loops.",
        "recommended": False,
        "intelligence": 3,
        "speed": 5,
        "context_length": 131_072,
        "prompt_price_per_m": 0.30,
        "completion_price_per_m": 0.50,
    },
    "grok-3-mini-fast": {
        "name": "Grok 3 Mini Fast",
        "provider": "xAI",
        "description": "Fast mini Grok for high-volume lightweight turns.",
        "recommended": False,
        "intelligence": 3,
        "speed": 5,
        "context_length": 131_072,
        "prompt_price_per_m": 0.30,
        "completion_price_per_m": 0.50,
    },
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
    mid = str(model_id or "").strip()
    if mid.lower().startswith("grok-") or mid.lower().startswith("x-ai/") or mid.lower().startswith("xai/"):
        return "xAI"
    slug = mid.split("/")[0] if "/" in mid else mid
    labels = {
        "openai": "OpenAI",
        "anthropic": "Anthropic",
        "google": "Google",
        "x-ai": "xAI",
        "xai": "xAI",
        "deepseek": "DeepSeek",
        "moonshotai": "Moonshot",
        "meta-llama": "Meta",
        "qwen": "Qwen",
        "z-ai": "Z AI",
        "mistralai": "Mistral",
        "nvidia": "Nvidia",
        "minimax": "MiniMax",
        "alibaba": "Alibaba",
        "bytedance": "ByteDance",
        "cohere": "Cohere",
        "perplexity": "Perplexity",
        "claude-haiku-4-5-20251001": "Anthropic",
        "claude-sonnet-4-6": "Anthropic",
        "claude-opus-4-8": "Anthropic",
        "claude-fable-5": "Anthropic",
    }
    if slug.lower().startswith("claude"):
        return "Anthropic"
    return labels.get(slug, slug.replace("-", " ").title())


def _is_alias_or_redirect(model_id: str, live_row: dict[str, Any] | None = None) -> bool:
    """Hide OpenRouter alias rows such as ~anthropic/claude-opus-latest."""
    mid = model_id.lower().strip()
    if mid.startswith("~"):
        return True
    row = live_row or {}
    name = str(row.get("name") or "").lower()
    desc = str(row.get("description") or "").lower()
    return "latest" in mid and ("redirect" in desc or "redirect" in name)


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
    if any(k in slug for k in ("opus", "o1", "o3", "o4", "sonnet-4", "gpt-4", "gpt-5", "gemini", "grok-4", "kimi-k2")):
        return 5
    if any(k in slug for k in ("mini", "nano", "lite", "8b", "7b")):
        return 3
    if prompt_ppm is not None and prompt_ppm >= 10:
        return 5
    if prompt_ppm is not None and prompt_ppm >= 3:
        return 4
    return 3


def _catalog_row(mid: str, live_row: dict[str, Any]) -> dict[str, Any]:
    meta = CURATED_META.get(mid, {}) or CURATED_META.get(_normalize_xai_model(mid), {})
    pricing = live_row.get("pricing") if isinstance(live_row.get("pricing"), dict) else {}
    # Prefer live API pricing; fall back to curated static tables for perfect UI cost display.
    prompt_ppm = _price_per_mtok(pricing.get("prompt") or pricing.get("input"))
    completion_ppm = _price_per_mtok(pricing.get("completion") or pricing.get("output"))
    if prompt_ppm is None:
        try:
            prompt_ppm = float(meta["prompt_price_per_m"]) if meta.get("prompt_price_per_m") is not None else None
        except (TypeError, ValueError, KeyError):
            prompt_ppm = None
    if completion_ppm is None:
        try:
            completion_ppm = (
                float(meta["completion_price_per_m"]) if meta.get("completion_price_per_m") is not None else None
            )
        except (TypeError, ValueError, KeyError):
            completion_ppm = None
    ctx = live_row.get("context_length") or live_row.get("context_window") or meta.get("context_length")
    try:
        ctx_i = int(ctx) if ctx is not None else None
    except (TypeError, ValueError):
        ctx_i = None
    policy_fields = model_registry.catalog_policy_fields(mid)
    return {
        "id": mid,
        "name": meta.get("name") or live_row.get("name") or live_row.get("display_name") or mid.split("/")[-1],
        "provider": meta.get("provider") or live_row.get("provider") or _provider_from_id(mid),
        "description": meta.get("description") or live_row.get("description") or "",
        "context_length": ctx_i,
        "prompt_price_per_m": prompt_ppm,
        "completion_price_per_m": completion_ppm,
        "recommended": bool(meta.get("recommended")) and bool(policy_fields["selectable"]),
        "intelligence": meta.get("intelligence") or _infer_intelligence(mid, prompt_ppm),
        "speed": meta.get("speed") or _infer_speed(mid, prompt_ppm),
        # Sample turn estimate used by the picker UI (10k in + 2k out).
        "est_cost_10k_2k": _est_turn_cost_usd(prompt_ppm, completion_ppm, 10_000, 2_000),
        **policy_fields,
    }


def _est_turn_cost_usd(
    prompt_ppm: float | None,
    completion_ppm: float | None,
    prompt_tokens: int = 10_000,
    completion_tokens: int = 2_000,
) -> float | None:
    if prompt_ppm is None and completion_ppm is None:
        return None
    pin = float(prompt_ppm or 0.0) * (prompt_tokens / 1_000_000)
    pout = float(completion_ppm or 0.0) * (completion_tokens / 1_000_000)
    return round(pin + pout, 6)


def xai_api_key() -> str:
    return (
        os.getenv("XAI_API_KEY", "").strip()
        or os.getenv("X_AI_API_KEY", "").strip()
        or os.getenv("GROK_API_KEY", "").strip()
    )


def _normalize_xai_model(model: str) -> str:
    return model_registry.normalize_xai_model_id(model)


def is_xai_model(model: str | None) -> bool:
    return model_registry.is_xai_model_id(model)


def is_anthropic_model(model: str | None) -> bool:
    return model_registry.is_anthropic_model_id(model)


def resolve_chat_route(model: str | None) -> model_registry.ModelRoute:
    """Resolve the exact selected model to a configured provider route."""
    selected = str(model or DEFAULT_MODEL).strip() or DEFAULT_MODEL
    return model_registry.resolve_model_route(
        selected,
        xai_configured=bool(xai_api_key()),
        anthropic_configured=bool(anthropic_api_key()),
        openrouter_configured=bool(_openrouter_api_key_optional()),
    )


def resolve_chat_model(model: str | None) -> tuple[str, str]:
    """Return the provider model ID without substituting another model."""
    route = resolve_chat_route(model)
    route_note = ""
    if route.provider_model_id != route.requested_model:
        route_note = (
            f"Routing {route.requested_model} as {route.provider_model_id} via "
            f"{route.route_provider}; the selected model is unchanged."
        )
    return route.provider_model_id, route_note


# Official Chat + Code API models (docs.x.ai) — always surface when key is set.
# Live GET /models may also return legacy SKUs still enabled on the account.
_XAI_CHAT_MODEL_IDS = (
    "grok-4.5",
    "grok-4.3",
    "grok-4.20-0309-reasoning",
    "grok-4.20-0309-non-reasoning",
    "grok-4.20-multi-agent-0309",
    "grok-build-0.1",
    "grok-4",
    "grok-3",
    "grok-3-mini",
    "grok-3-mini-fast",
)

_XAI_NON_CHAT_MARKERS = (
    "imagine",
    "image",
    "video",
    "tts",
    "voice",
    "speech",
    "embedding",
    "embed",
    "realtime",
)


def _xai_is_chat_model_id(mid: str) -> bool:
    low = str(mid or "").strip().lower()
    if not low.startswith("grok-") and not low.startswith("x-ai/") and not low.startswith("xai/"):
        return False
    bare = _normalize_xai_model(low)
    return not any(x in bare for x in _XAI_NON_CHAT_MARKERS)


def _xai_model_sort_key(mid: str) -> tuple:
    """Stable picker order: current official chat → code → legacy."""
    order = {m: i for i, m in enumerate(_XAI_CHAT_MODEL_IDS)}
    return (order.get(mid, 500), mid)


async def list_xai_models() -> list[dict[str, Any]]:
    """List xAI chat models for the runner picker (requires XAI_API_KEY).

    Pricing: prefer live /models fields when present; otherwise official curated
    tables from docs.x.ai (Chat + Code API).
    """
    key = xai_api_key()
    if not key:
        return []
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    live_by_id: dict[str, dict[str, Any]] = {}
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            r = await client.get(f"{XAI_BASE}/models", headers=headers)
            if r.status_code < 400:
                data = r.json()
                items = data.get("data") if isinstance(data, dict) else data
                for item in items or []:
                    if not isinstance(item, dict):
                        continue
                    mid = _normalize_xai_model(str(item.get("id") or ""))
                    if not mid or not _xai_is_chat_model_id(mid):
                        continue
                    live_by_id[mid] = item
    except Exception:
        pass

    # Union of live account models + curated official chat/code SKUs.
    ids = set(live_by_id.keys()) | set(_XAI_CHAT_MODEL_IDS)
    ordered = sorted(ids, key=_xai_model_sort_key)

    rows: list[dict[str, Any]] = []
    for mid in ordered:
        meta = CURATED_META.get(mid, {})
        live = live_by_id.get(mid) or {}
        # Live pricing (rare on xAI /models) — accept several shapes.
        live_pricing = live.get("pricing") if isinstance(live.get("pricing"), dict) else {}
        prompt_per_tok = live_pricing.get("prompt") or live_pricing.get("input")
        completion_per_tok = live_pricing.get("completion") or live_pricing.get("output")
        # Some payloads use $/M directly
        if prompt_per_tok is None and live.get("input_price_per_million_tokens") is not None:
            try:
                prompt_per_tok = float(live["input_price_per_million_tokens"]) / 1_000_000
            except (TypeError, ValueError):
                prompt_per_tok = None
        if completion_per_tok is None and live.get("output_price_per_million_tokens") is not None:
            try:
                completion_per_tok = float(live["output_price_per_million_tokens"]) / 1_000_000
            except (TypeError, ValueError):
                completion_per_tok = None
        if prompt_per_tok is None and meta.get("prompt_price_per_m") is not None:
            prompt_per_tok = float(meta["prompt_price_per_m"]) / 1_000_000
        if completion_per_tok is None and meta.get("completion_price_per_m") is not None:
            completion_per_tok = float(meta["completion_price_per_m"]) / 1_000_000
        ctx = (
            live.get("context_length")
            or live.get("context_window")
            or meta.get("context_length")
        )
        rows.append({
            "id": mid,
            "name": meta.get("name") or live.get("name") or live.get("display_name") or mid,
            "description": meta.get("description") or live.get("description") or "",
            "provider": "xAI",
            "context_length": ctx,
            "pricing": {
                # OpenRouter-style per-token so _price_per_mtok / catalog works
                "prompt": prompt_per_tok,
                "completion": completion_per_tok,
            },
        })
    return rows


def build_model_catalog(live: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Merge curated display metadata with the full live OpenRouter model list + pricing."""
    live_by_id: dict[str, dict[str, Any]] = {}
    live_by_canonical: dict[str, dict[str, Any]] = {}
    for item in live or []:
        mid = str(item.get("id") or "")
        if mid and not _is_alias_or_redirect(mid, item):
            live_by_id[mid] = item
            live_by_canonical.setdefault(model_registry.canonical_model_id(mid), item)

    catalog: list[dict[str, Any]] = []
    seen: set[str] = set()
    seen_canonical: set[str] = set()

    curated_ids = list(dict.fromkeys([
        *RECOMMENDED_MODELS,
        *model_registry.ALWAYS_VISIBLE_MODEL_IDS,
    ]))
    for mid in curated_ids:
        if _is_alias_or_redirect(mid):
            continue
        seen.add(mid)
        canonical = model_registry.canonical_model_id(mid)
        seen_canonical.add(canonical)
        live_row = live_by_id.get(mid) or live_by_canonical.get(canonical) or {"id": mid}
        row = _catalog_row(mid, live_row)
        if not row.get("recommended"):
            row["recommended"] = mid in RECOMMENDED_MODELS[:6]
        if not row.get("selectable", True):
            row["recommended"] = False
        catalog.append(row)

    extras: list[dict[str, Any]] = []
    for mid, live_row in live_by_id.items():
        if mid in seen or model_registry.canonical_model_id(mid) in seen_canonical:
            continue
        if _is_alias_or_redirect(mid, live_row):
            continue
        arch = live_row.get("architecture") if isinstance(live_row.get("architecture"), dict) else {}
        modality = str(arch.get("modality") or "").lower()
        if modality and "text" not in modality:
            continue
        extras.append(_catalog_row(mid, live_row))

    _mark_dynamic_recommendations(extras, existing=catalog)
    extras.sort(key=lambda r: (
        0 if r.get("recommended") else 1,
        r.get("provider") or "",
        -(int(r.get("intelligence") or 0)),
        r.get("name") or r["id"],
    ))
    catalog.extend(extras)
    return catalog


def _mark_dynamic_recommendations(extras: list[dict[str, Any]], *, existing: list[dict[str, Any]]) -> None:
    """Pick strong current models from the live catalog without hard-coding one tiny era."""
    already = {str(r.get("provider") or "") for r in existing if r.get("recommended")}
    preferred_terms = (
        "opus", "sonnet", "claude", "gpt-5", "gpt-4", "gemini", "grok",
        "kimi", "deepseek", "llama", "qwen", "glm", "mistral", "command",
    )
    by_provider: dict[str, list[dict[str, Any]]] = {}
    for row in extras:
        mid = str(row.get("id") or "").lower()
        provider = str(row.get("provider") or "Other")
        if any(bad in mid for bad in ("free", "vision-preview", "moderation", "embedding", "rerank")):
            continue
        if not any(term in mid for term in preferred_terms):
            continue
        by_provider.setdefault(provider, []).append(row)
    for provider, rows in by_provider.items():
        if provider in already and provider not in {"Moonshotai", "Moonshot Ai", "X Ai", "Xai"}:
            continue
        rows.sort(key=lambda r: (
            -(int(r.get("intelligence") or 0)),
            int(r.get("speed") or 0),
            float(r.get("prompt_price_per_m") or 9999),
        ))
        for row in rows[:1]:
            row["recommended"] = True


def _openrouter_api_key_optional() -> str:
    return (
        os.getenv("OPENROUTER_API_KEY", "").strip()
        or os.getenv("OPEN_ROUTER_API_KEY", "").strip()
    )


def any_llm_provider_configured() -> bool:
    """True when OpenRouter and/or direct xAI/Anthropic keys are present."""
    return bool(_openrouter_api_key_optional() or xai_api_key() or anthropic_api_key())


def api_key() -> str:
    key = _openrouter_api_key_optional()
    if key:
        return key
    # Local / direct-provider mode: allow Studio Agent when only XAI or Anthropic is set.
    if xai_api_key() or anthropic_api_key():
        return ""
    raise RuntimeError(
        "No LLM API key set. Add OPENROUTER_API_KEY, XAI_API_KEY, or ANTHROPIC_API_KEY to .env."
    )


def anthropic_api_key() -> str:
    key = (
        os.getenv("ANTHROPIC_API_KEY", "").strip()
        or os.getenv("CLAUDE_API_KEY", "").strip()
    )
    return key


def _use_anthropic_primary() -> bool:
    if PRIMARY_PROVIDER in {"anthropic", "claude", "anthropic_direct", "direct_anthropic"}:
        return bool(anthropic_api_key())
    if PRIMARY_PROVIDER in {"auto", ""}:
        return bool(anthropic_api_key()) and not bool(_openrouter_api_key_optional())
    return False


def _headers() -> dict[str, str]:
    key = _openrouter_api_key_optional()
    if not key:
        raise RuntimeError(
            "OPENROUTER_API_KEY is not set (required for OpenRouter-routed models)."
        )
    h = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    referer = os.getenv("OPENROUTER_HTTP_REFERER", "https://studio.nyptidindustries.com")
    title = os.getenv("OPENROUTER_APP_TITLE", "NYPTID Studio Agent")
    if referer:
        h["HTTP-Referer"] = referer
    if title:
        h["X-Title"] = title
    return h


def _should_try_anthropic(status_code: int, detail: str) -> bool:
    detail_l = (detail or "").lower()
    if status_code in {402, 429}:
        return True
    if status_code in {401, 403} and any(
        needle in detail_l
        for needle in ("quota", "credit", "balance", "limit", "rate", "insufficient")
    ):
        return True
    return any(
        needle in detail_l
        for needle in (
            "insufficient",
            "quota",
            "credit",
            "balance",
            "rate limit",
            "too many requests",
        )
    )


def _content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                if item.get("type") == "text" and item.get("text"):
                    parts.append(str(item.get("text")))
                else:
                    parts.append(json.dumps(item, ensure_ascii=False))
            else:
                parts.append(str(item))
        return "\n".join(p for p in parts if p)
    if content is None:
        return ""
    return json.dumps(content, ensure_ascii=False)


def _clip_text(text: str, max_chars: int) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}\n\n[trimmed {len(text) - max_chars} chars for fallback context budget]"


def _message_text_size(msg: dict[str, Any]) -> int:
    return len(str(msg.get("role") or "")) + len(_content_to_text(msg.get("content"))) + len(
        json.dumps(msg.get("tool_calls") or [], ensure_ascii=False)
    )


def _anthropic_payload_size(system_text: str | None, messages: list[dict[str, Any]], tools: list[dict[str, Any]] | None = None) -> int:
    return len(json.dumps({"system": system_text or "", "messages": messages, "tools": tools or []}, ensure_ascii=False))


def _compact_messages_for_anthropic_fallback(messages: list[dict[str, Any]], *, hard: bool = False) -> list[dict[str, Any]]:
    """Build a bounded fallback prompt without deleting persisted chat history.

    Anthropic direct fallback has a smaller hard context limit than some OpenRouter
    routes. Keep the recent working turn and a compact note about older saved
    state so a long Studio Agent chat does not hard-fail at the model boundary.
    """
    keep_recent = max(4, min(ANTHROPIC_FALLBACK_KEEP_RECENT_MESSAGES, 8) if hard else ANTHROPIC_FALLBACK_KEEP_RECENT_MESSAGES)
    budget = ANTHROPIC_FALLBACK_HARD_PROMPT_CHAR_BUDGET if hard else ANTHROPIC_FALLBACK_PROMPT_CHAR_BUDGET
    max_message_chars = max(1200, ANTHROPIC_FALLBACK_MAX_MESSAGE_CHARS // (3 if hard else 1))
    max_system_chars = max(4000, ANTHROPIC_FALLBACK_MAX_SYSTEM_CHARS // (3 if hard else 1))

    system_parts: list[str] = []
    non_system: list[dict[str, Any]] = []
    for msg in messages:
        role = str(msg.get("role") or "user")
        text = _content_to_text(msg.get("content")).strip()
        if role == "system":
            if text:
                system_parts.append(text)
            continue
        if not text and not msg.get("tool_calls"):
            continue
        clean_role = role if role in {"user", "assistant", "tool"} else "user"
        if clean_role == "tool":
            tool_id = str(msg.get("tool_call_id") or msg.get("id") or "tool")
            text = f"Tool result context ({tool_id}):\n{text}"
            clean_role = "user"
        non_system.append({"role": clean_role, "content": _clip_text(text, max_message_chars)})

    compacted: list[dict[str, Any]] = []
    if system_parts:
        compacted.append({"role": "system", "content": _clip_text("\n\n".join(system_parts), max_system_chars)})

    omitted = max(0, len(non_system) - keep_recent)
    if omitted:
        compacted.append(
            {
                "role": "system",
                "content": (
                    f"{omitted} older Studio Agent messages were omitted only from this Anthropic fallback request "
                    "because the provider rejected the full prompt as too long. The complete chat, approvals, "
                    "scene/job state, and production assets remain persisted server-side. Use available tool/state "
                    "results as source of truth; do not claim a tool ran unless its result is present."
                ),
            }
        )

    compacted.extend(non_system[-keep_recent:])

    while sum(_message_text_size(m) for m in compacted) > budget and len(compacted) > 3:
        # Drop the oldest non-system working message first. System instructions and
        # the newest user turn are more important than stale chat turns.
        drop_idx = next((idx for idx, msg in enumerate(compacted[:-2]) if msg.get("role") != "system"), 1)
        compacted.pop(drop_idx)

    if hard:
        for msg in compacted:
            if msg.get("role") != "system":
                msg["content"] = _clip_text(_content_to_text(msg.get("content")), max_message_chars)

    if not any(m.get("role") != "system" for m in compacted):
        compacted.append({"role": "user", "content": "Continue from the saved Studio Agent state."})
    return compacted


def _json_obj(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(str(raw))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _anthropic_tools(tools: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in tools or []:
        if not isinstance(row, dict):
            continue
        fn = row.get("function") if isinstance(row.get("function"), dict) else {}
        name = str(fn.get("name") or "").strip()
        if not name:
            continue
        schema = fn.get("parameters")
        if not isinstance(schema, dict):
            schema = {"type": "object", "properties": {}}
        out.append(
            {
                "name": name,
                "description": str(fn.get("description") or ""),
                "input_schema": schema,
            }
        )
    return out


def _strip_anthropic_tools(payload: dict[str, Any]) -> None:
    payload.pop("tools", None)
    payload.pop("tool_choice", None)


def _anthropic_tools_by_name(tools: list[dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("name") or ""): row
        for row in _anthropic_tools(tools)
        if str(row.get("name") or "").strip()
    }


def _anthropic_preserve_tool_subset(
    tools: list[dict[str, Any]] | None,
    preserve_names: set[str] | frozenset[str],
) -> list[dict[str, Any]]:
    by_name = _anthropic_tools_by_name(tools)
    return [by_name[name] for name in preserve_names if name in by_name]


def _anthropic_tool_size(tools: list[dict[str, Any]] | None) -> int:
    if not tools:
        return 0
    return len(json.dumps(tools, ensure_ascii=False))


_ANTHROPIC_CORE_TOOL_PRIORITY = (
    "poll_render_job",
    "list_production_scenes",
    "edit_production_scene_still",
    "edit_production_scenes_still",
    "regenerate_production_scene_still",
    "set_production_scenes_animate",
    "set_production_scene_duration",
    "animate_production_scenes",
    "repair_production_scene_animation",
    "finalize_production",
    "ingest_product_reference",
    "start_shortform_generate",
    "start_longform_render",
    "get_studio_credits",
    "get_channel_analytics",
    "recommend_video_topics",
    "search_youtube_public",
    "analyze_reference_video",
    "retry_reference_analysis",
    "estimate_shortform_render_cost",
    "get_fal_pricing",
)


def _select_anthropic_tools(
    tools: list[dict[str, Any]] | None,
    messages: list[dict[str, Any]],
    *,
    budget: int = ANTHROPIC_FALLBACK_TOOL_CHAR_BUDGET,
) -> list[dict[str, Any]]:
    """Keep relevant tools callable instead of dropping the complete tool set."""
    converted = _anthropic_tools(tools)
    full_toolset = str(os.getenv("STUDIO_AGENT_FULL_TOOLSET", "1") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    # Full toolset is allowed only when it fits the caller's budget. When the
    # payload is oversized (or a tight budget is passed), always trim by priority.
    if _anthropic_tool_size(converted) <= budget:
        return converted
    if full_toolset and budget >= max(ANTHROPIC_FALLBACK_TOOL_CHAR_BUDGET, 12_000):
        return converted

    recent_text = " ".join(
        _content_to_text(msg.get("content"))
        for msg in (messages or [])[-8:]
        if str(msg.get("role") or "") in {"user", "assistant", "system"}
    ).lower()
    priority = list(_ANTHROPIC_CORE_TOOL_PRIORITY)
    keyword_tools = {
        "watch": ("analyze_reference_video", "poll_render_job"),
        "upload": ("analyze_reference_video", "poll_render_job"),
        "uploaded": ("analyze_reference_video", "poll_render_job"),
        "video": ("analyze_reference_video", "poll_render_job"),
        "analyze": ("analyze_reference_video", "poll_render_job"),
        "try again": ("retry_reference_analysis", "analyze_reference_video", "poll_render_job"),
        "retry": ("retry_reference_analysis", "analyze_reference_video", "poll_render_job"),
        "transcript": ("retry_reference_analysis", "analyze_reference_video", "poll_render_job"),
        "poll": ("poll_render_job", "list_production_scenes"),
        "continue": ("poll_render_job", "list_production_scenes", "analyze_reference_video"),
        "status": ("poll_render_job", "list_production_scenes"),
        "scene": ("list_production_scenes", "edit_production_scene_still", "set_production_scenes_animate"),
        "edit": ("edit_production_scene_still", "edit_production_scenes_still", "re_edit_production"),
        "render": ("start_shortform_generate", "start_longform_render", "poll_render_job"),
        "product": ("ingest_product_reference", "start_shortform_generate"),
        "advertisement": ("ingest_product_reference", "start_shortform_generate"),
        "website": ("ingest_product_reference", "start_shortform_generate"),
        "animate": ("set_production_scenes_animate", "animate_production_scenes", "repair_production_scene_animation"),
        "animation": ("repair_production_scene_animation", "animate_production_scenes"),
        "motion": ("repair_production_scene_animation", "animate_production_scenes"),
        "static": ("repair_production_scene_animation",),
        "reanimate": ("repair_production_scene_animation", "animate_production_scenes"),
        "final": ("finalize_production", "finalize_longform_render"),
        "channel": ("get_channel_analytics", "recommend_video_topics"),
        "youtube": ("search_youtube_public", "analyze_reference_video"),
        "cost": ("estimate_shortform_render_cost", "get_studio_credits"),
        "pricing": ("estimate_shortform_render_cost", "get_fal_pricing"),
        "how much": ("estimate_shortform_render_cost",),
        "per short": ("estimate_shortform_render_cost",),
    }
    for keyword, names in keyword_tools.items():
        if keyword in recent_text:
            priority = [*names, *priority]

    by_name = {str(row.get("name") or ""): row for row in converted}
    ordered_names = list(dict.fromkeys([*priority, *by_name.keys()]))
    selected: list[dict[str, Any]] = []
    for name in ordered_names:
        row = by_name.get(name)
        if not row:
            continue
        candidate = [*selected, row]
        if _anthropic_tool_size(candidate) > budget:
            continue
        selected.append(row)
    if not selected:
        selected = [by_name[name] for name in priority if name in by_name]
    return selected or converted


def _anthropic_payload_messages(messages: list[dict[str, Any]]) -> tuple[str | None, list[dict[str, Any]]]:
    system_parts: list[str] = []
    out: list[dict[str, Any]] = []
    known_tool_ids: set[str] = set()
    for msg in messages:
        role = str(msg.get("role") or "user")
        text = _content_to_text(msg.get("content")).strip()
        if role == "system":
            if text:
                system_parts.append(text)
            continue
        if role == "tool":
            tool_use_id = str(msg.get("tool_call_id") or msg.get("id") or f"tool_{len(out)}")
            if tool_use_id in known_tool_ids:
                out.append(
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "tool_result",
                                "tool_use_id": tool_use_id,
                                "content": text or "",
                            }
                        ],
                    }
                )
            elif text:
                out.append({"role": "user", "content": f"Tool result context ({tool_use_id}):\n{text}"})
            continue
        if role == "assistant":
            blocks: list[dict[str, Any]] = []
            if text:
                blocks.append({"type": "text", "text": text})
            for idx, call in enumerate(msg.get("tool_calls") or []):
                if not isinstance(call, dict):
                    continue
                fn = call.get("function") if isinstance(call.get("function"), dict) else {}
                name = str(fn.get("name") or "").strip()
                if not name:
                    continue
                blocks.append(
                    {
                        "type": "tool_use",
                        "id": str(call.get("id") or f"toolu_{len(out)}_{idx}"),
                        "name": name,
                        "input": _json_obj(fn.get("arguments")),
                    }
                )
                known_tool_ids.add(str(call.get("id") or f"toolu_{len(out)}_{idx}"))
            if blocks:
                out.append({"role": "assistant", "content": blocks})
            continue
        if text:
            out.append({"role": "user", "content": text})
    if not out:
        out.append({"role": "user", "content": "Continue."})
    elif out[0].get("role") != "user":
        out.insert(0, {"role": "user", "content": "Continue from the saved Studio Agent state."})
    return ("\n\n".join(system_parts) if system_parts else None), out


async def _anthropic_chat_completion(
    *,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    temperature: float,
    timeout: float,
    provider_label: str = "anthropic_fallback",
    model_override: str | None = None,
    force_tool_call: bool = False,
    preserve_tool_names: frozenset[str] | None = None,
) -> dict[str, Any]:
    if model_override:
        model_registry.assert_model_selectable(model_override)
    key = anthropic_api_key()
    if not key:
        raise RuntimeError("ANTHROPIC_API_KEY is not set.")
    compacted_messages = _compact_messages_for_anthropic_fallback(messages)
    system_text, anth_messages = _anthropic_payload_messages(compacted_messages)
    payload_base: dict[str, Any] = {
        "messages": anth_messages,
        "max_tokens": int(os.getenv("ANTHROPIC_FALLBACK_MAX_TOKENS", "2048")),
        "temperature": temperature,
    }
    if system_text:
        payload_base["system"] = system_text
    anth_tools = _select_anthropic_tools(tools, messages)
    include_tools = bool(anth_tools)
    if anth_tools and include_tools:
        payload_base["tools"] = anth_tools
        payload_base["tool_choice"] = {"type": "any" if force_tool_call else "auto"}
    if _anthropic_payload_size(system_text, anth_messages, payload_base.get("tools")) > ANTHROPIC_FALLBACK_PROMPT_CHAR_BUDGET:
        compacted_messages = _compact_messages_for_anthropic_fallback(messages, hard=True)
        system_text, anth_messages = _anthropic_payload_messages(compacted_messages)
        payload_base["messages"] = anth_messages
        if system_text:
            payload_base["system"] = system_text
        else:
            payload_base.pop("system", None)
        if _anthropic_payload_size(system_text, anth_messages, payload_base.get("tools")) > ANTHROPIC_FALLBACK_HARD_PROMPT_CHAR_BUDGET:
            preserved = _anthropic_preserve_tool_subset(tools, preserve_tool_names or frozenset())
            if preserved:
                payload_base["tools"] = preserved
                payload_base["tool_choice"] = {"type": "any" if force_tool_call else "auto"}
                anth_tools = preserved
            else:
                _strip_anthropic_tools(payload_base)
                anth_tools = []
    if _anthropic_payload_size(payload_base.get("system"), payload_base["messages"], payload_base.get("tools")) > ANTHROPIC_FALLBACK_HARD_PROMPT_CHAR_BUDGET:
        preserved = _anthropic_preserve_tool_subset(tools, preserve_tool_names or frozenset())
        if preserved:
            payload_base["tools"] = preserved
            payload_base["tool_choice"] = {"type": "any" if force_tool_call else "auto"}
        else:
            _strip_anthropic_tools(payload_base)
    headers = {
        "x-api-key": key,
        "anthropic-version": os.getenv("ANTHROPIC_VERSION", "2023-06-01"),
        "content-type": "application/json",
    }
    model_candidates = list(ANTHROPIC_FALLBACK_MODELS)
    if model_override:
        override = _normalize_anthropic_model(model_override)
        if override and ("/" not in override or override.startswith("claude-")):
            # An explicit creator selection is exact. A 404 must surface for
            # that model instead of silently changing Opus/Sonnet/Fable to a
            # cheaper fallback model.
            model_candidates = [override]
    last_error = ""
    selected_model = model_candidates[0] if model_candidates else ANTHROPIC_FALLBACK_MODEL
    async with httpx.AsyncClient(timeout=timeout) as client:
        for model in model_candidates:
            selected_model = model
            payload = dict(payload_base)
            payload["model"] = model
            r = await client.post(f"{ANTHROPIC_BASE}/messages", headers=headers, json=payload)
            if r.status_code == 400 and "prompt is too long" in r.text.lower():
                hard_messages = _compact_messages_for_anthropic_fallback(messages, hard=True)
                hard_system_text, hard_anth_messages = _anthropic_payload_messages(hard_messages)
                payload = dict(payload_base)
                payload["model"] = model
                payload["messages"] = hard_anth_messages
                if hard_system_text:
                    payload["system"] = hard_system_text
                else:
                    payload.pop("system", None)
                if _anthropic_payload_size(payload.get("system"), hard_anth_messages, payload.get("tools")) > ANTHROPIC_FALLBACK_HARD_PROMPT_CHAR_BUDGET:
                    preserved = _anthropic_preserve_tool_subset(tools, preserve_tool_names or frozenset())
                    if preserved:
                        payload["tools"] = preserved
                        payload["tool_choice"] = {"type": "any" if force_tool_call else "auto"}
                    else:
                        _strip_anthropic_tools(payload)
                r = await client.post(f"{ANTHROPIC_BASE}/messages", headers=headers, json=payload)
            if r.status_code < 400:
                data = r.json()
                break
            last_error = f"Anthropic {r.status_code}: {r.text[:2000]}"
            if r.status_code != 404:
                raise RuntimeError(last_error)
        else:
            models = ", ".join(model_candidates)
            raise RuntimeError(f"{last_error}. Tried Anthropic fallback models: {models}")
    text_parts: list[str] = []
    tool_calls: list[dict[str, Any]] = []
    for idx, block in enumerate(data.get("content") or []):
        if isinstance(block, dict) and block.get("type") == "text":
            text_parts.append(str(block.get("text") or ""))
        elif isinstance(block, dict) and block.get("type") == "tool_use":
            tool_calls.append(
                {
                    "id": str(block.get("id") or f"toolu_{idx}"),
                    "type": "function",
                    "function": {
                        "name": str(block.get("name") or ""),
                        "arguments": json.dumps(block.get("input") or {}, ensure_ascii=False),
                    },
                }
            )
    usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
    message: dict[str, Any] = {"role": "assistant", "content": "\n".join(text_parts).strip()}
    if tool_calls:
        message["tool_calls"] = tool_calls
    return {
        "id": data.get("id"),
        "model": data.get("model") or selected_model,
        "provider": provider_label,
        "choices": [{"message": message}],
        "usage": {
            "prompt_tokens": usage.get("input_tokens", 0),
            "completion_tokens": usage.get("output_tokens", 0),
            "total_tokens": int(usage.get("input_tokens", 0) or 0) + int(usage.get("output_tokens", 0) or 0),
        },
    }


async def list_models() -> list[dict[str, Any]]:
    """Return runner models for the Studio picker (Anthropic + xAI Grok + optional OpenRouter)."""
    rows: list[dict[str, Any]] = []
    if _use_anthropic_primary() or anthropic_api_key():
        try:
            headers = {
                "x-api-key": anthropic_api_key(),
                "anthropic-version": os.getenv("ANTHROPIC_VERSION", "2023-06-01"),
            }
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.get(f"{ANTHROPIC_BASE}/models", headers=headers)
                r.raise_for_status()
                data = r.json()
            items = data.get("data") if isinstance(data, dict) else data
            available = {
                str(item.get("id") or ""): item
                for item in (items or [])
                if isinstance(item, dict) and item.get("id")
            }
            for model_id in RECOMMENDED_MODELS:
                if is_xai_model(model_id):
                    continue
                item = available.get(model_id)
                if not item and model_id not in CURATED_META:
                    continue
                meta = CURATED_META.get(model_id, {})
                rows.append({
                    "id": model_id,
                    "name": meta.get("name") or (item or {}).get("display_name") or model_id,
                    "description": meta.get("description") or "",
                    "provider": "Anthropic",
                    "context_length": (item or {}).get("context_window") or meta.get("context_length"),
                    "pricing": {
                        "prompt": (float(meta["prompt_price_per_m"]) / 1_000_000)
                        if meta.get("prompt_price_per_m") is not None
                        else None,
                        "completion": (float(meta["completion_price_per_m"]) / 1_000_000)
                        if meta.get("completion_price_per_m") is not None
                        else None,
                    },
                })
            # Include every live Anthropic chat model, not only the hand-curated
            # launch set. Newly released Claude/Fable/Sonnet/Opus variants then
            # become selectable without a Studio code release.
            seen_anthropic_ids = {str(row.get("id") or "") for row in rows}
            for model_id, item in sorted(available.items()):
                if model_id in seen_anthropic_ids:
                    continue
                rows.append({
                    **item,
                    "id": model_id,
                    "name": item.get("display_name") or item.get("name") or model_id,
                    "provider": "Anthropic",
                    "context_length": item.get("context_window") or item.get("context_length"),
                })
            if not rows:
                for model_id in ("claude-haiku-4-5-20251001", "claude-sonnet-4-6", "claude-opus-4-8"):
                    meta = CURATED_META[model_id]
                    rows.append({
                        "id": model_id,
                        "name": meta["name"],
                        "provider": "Anthropic",
                        "description": meta.get("description") or "",
                        "context_length": meta.get("context_length"),
                        "pricing": {
                            "prompt": float(meta["prompt_price_per_m"]) / 1_000_000,
                            "completion": float(meta["completion_price_per_m"]) / 1_000_000,
                        },
                    })
        except Exception:
            for model_id in ("claude-haiku-4-5-20251001", "claude-sonnet-4-6", "claude-opus-4-8"):
                meta = CURATED_META[model_id]
                rows.append({
                    "id": model_id,
                    "name": meta["name"],
                    "provider": "Anthropic",
                    "description": meta.get("description") or "",
                    "context_length": meta.get("context_length"),
                    "pricing": {
                        "prompt": float(meta["prompt_price_per_m"]) / 1_000_000,
                        "completion": float(meta["completion_price_per_m"]) / 1_000_000,
                    },
                })

    # Always attach Grok chat models when XAI_API_KEY is configured (same key as dictation).
    if xai_api_key():
        try:
            xai_rows = await list_xai_models()
            seen = {str(r.get("id")) for r in rows}
            for xr in xai_rows:
                if str(xr.get("id")) not in seen:
                    rows.append(xr)
        except Exception:
            pass

    # OpenRouter is additive, not an either/or fallback. A creator with direct
    # Anthropic or xAI credentials must still see every compatible marketplace
    # model instead of having the catalog silently collapse to those providers.
    # Provider-qualified IDs also preserve an explicit OpenRouter route for the
    # exact selected model.
    if _openrouter_api_key_optional():
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.get(f"{OPENROUTER_BASE}/models", headers=_headers())
                r.raise_for_status()
                data = r.json()
            items = data.get("data") if isinstance(data, dict) else data
            if isinstance(items, list):
                rows.extend(item for item in items if isinstance(item, dict) and item.get("id"))
        except Exception:
            if not rows:
                raise
    if rows:
        return rows
    return [{"id": m, "name": CURATED_META.get(m, {}).get("name") or m} for m in RECOMMENDED_MODELS[:6]]


# In-memory model-pricing cache for per-turn credit metering (TTL seconds).
_MODELS_CACHE: dict[str, Any] = {"at": 0.0, "by_id": {}}
_MODELS_TTL = 1800.0


async def model_pricing(model_id: str) -> tuple[float | None, float | None]:
    """Return (prompt_price_per_m, completion_price_per_m) USD for a model.

    Cached for _MODELS_TTL to avoid a /models round-trip on every chat turn.
    """
    import time as _t

    mid = _normalize_xai_model(model_id) if is_xai_model(model_id) else str(model_id or "").strip()
    now = _t.time()
    if now - float(_MODELS_CACHE.get("at", 0) or 0) > _MODELS_TTL or not _MODELS_CACHE.get("by_id"):
        try:
            live = await list_models()
            catalog = build_model_catalog(live)
            _MODELS_CACHE["by_id"] = {str(m.get("id")): m for m in catalog if m.get("id")}
            _MODELS_CACHE["at"] = now
        except Exception:
            pass
    row = (_MODELS_CACHE.get("by_id") or {}).get(mid) or (_MODELS_CACHE.get("by_id") or {}).get(model_id) or {}
    # Catalog rows already have $/M; raw live rows may have pricing.prompt per-token
    if row.get("prompt_price_per_m") is not None or row.get("completion_price_per_m") is not None:
        try:
            pin = float(row["prompt_price_per_m"]) if row.get("prompt_price_per_m") is not None else None
        except (TypeError, ValueError):
            pin = None
        try:
            pout = float(row["completion_price_per_m"]) if row.get("completion_price_per_m") is not None else None
        except (TypeError, ValueError):
            pout = None
        if pin is not None or pout is not None:
            return pin, pout
    pricing = row.get("pricing") if isinstance(row.get("pricing"), dict) else {}
    pin = _price_per_mtok(pricing.get("prompt") or pricing.get("input"))
    pout = _price_per_mtok(pricing.get("completion") or pricing.get("output"))
    if pin is None and pout is None:
        meta = CURATED_META.get(mid) or CURATED_META.get(str(model_id or ""))
        if meta:
            try:
                pin = float(meta["prompt_price_per_m"]) if meta.get("prompt_price_per_m") is not None else None
            except (TypeError, ValueError):
                pin = None
            try:
                pout = float(meta["completion_price_per_m"]) if meta.get("completion_price_per_m") is not None else None
            except (TypeError, ValueError):
                pout = None
    return pin, pout


REASONING_DEPTHS = ("fast", "balanced", "deep")


def _env_int(name: str, default: int, *, floor: int = 1, ceiling: int | None = None) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    value = max(floor, value)
    if ceiling is not None:
        value = min(value, ceiling)
    return value


def _completion_token_cap(depth: str, *, has_tools: bool) -> int:
    """Always send an explicit output cap so low OpenRouter credit cannot request huge defaults."""
    key = str(depth or "balanced").strip().lower()
    default = 4096
    if has_tools:
        default = 2048
    elif key == "deep":
        default = 6144
    elif key == "fast":
        default = 2048
    return _env_int("STUDIO_AGENT_MAX_COMPLETION_TOKENS", default, floor=512, ceiling=8192)


def _reasoning_token_cap(depth: str) -> int:
    key = str(depth or "balanced").strip().lower()
    default = 3072 if key == "deep" else 1024
    return _env_int("STUDIO_AGENT_REASONING_MAX_TOKENS", default, floor=256, ceiling=4096)


def _is_openrouter_credit_limit(status_code: int, detail: str) -> bool:
    if status_code != 402:
        return False
    text = str(detail or "").lower()
    return "requires more credits" in text or "can only afford" in text or "credits" in text


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
            reasoning = {"enabled": True, "max_tokens": _reasoning_token_cap(key)}
        return temp, reasoning

    # balanced
    return 0.4, {"enabled": True, "effort": "low", "max_tokens": _reasoning_token_cap(key)}


async def _xai_chat_completion(
    *,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    temperature: float = 0.4,
    timeout: float = 120.0,
    model: str,
    force_tool_call: bool = False,
    max_tokens: int | None = None,
) -> dict[str, Any]:
    """OpenAI-compatible chat completions against api.x.ai (Grok)."""
    model_registry.assert_model_selectable(model)
    key = xai_api_key()
    if not key:
        raise RuntimeError("XAI_API_KEY is not set. Add it to use Grok runner models.")
    mid = _normalize_xai_model(model)
    payload: dict[str, Any] = {
        "model": mid,
        "messages": messages,
        "temperature": float(temperature),
    }
    if max_tokens is not None:
        payload["max_tokens"] = int(max_tokens)
    if tools:
        # xAI accepts OpenAI-style tools; tool_schemas() already emit that shape.
        payload["tools"] = tools
        payload["tool_choice"] = "required" if force_tool_call else "auto"
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(f"{XAI_BASE}/chat/completions", headers=headers, json=payload)
        if r.status_code >= 400:
            raise RuntimeError(f"xAI {r.status_code}: {r.text[:2000]}")
        data = r.json()
    # Normalize provider label for billing/telemetry
    if isinstance(data, dict):
        data.setdefault("provider", "xai_direct")
        data["model"] = data.get("model") or mid
    return data


async def chat_completion(
    *,
    messages: list[dict[str, Any]],
    tools: list[dict[str, Any]] | None = None,
    model: str | None = None,
    temperature: float | None = None,
    reasoning_depth: str = "balanced",
    web_search: bool = False,
    force_tool_call: bool = False,
    preserve_tool_names: frozenset[str] | None = None,
) -> dict[str, Any]:
    requested_model = str(model or DEFAULT_MODEL).strip() or DEFAULT_MODEL
    # Final policy gate immediately before any provider request. The route may
    # change providers for the same model, but never changes the selected model.
    route = resolve_chat_route(requested_model)
    selected_model = route.provider_model_id
    temp, reasoning = reasoning_params(reasoning_depth, model=route.canonical_model)
    if temperature is not None:
        temp = float(temperature)
    timeout = 180.0 if reasoning_depth == "deep" else 120.0

    route_messages = list(messages)

    # Bare Grok IDs use xAI direct when configured. Provider-qualified x-ai/*
    # IDs and OpenRouter-only Grok routes stay on OpenRouter.
    if route.route_provider == "xai_direct":
        xai_messages = list(route_messages)
        if web_search:
            xai_messages = [
                *xai_messages,
                {
                    "role": "system",
                    "content": (
                        "Direct xAI mode does not provide OpenRouter web search. "
                        "Use only Studio tool results already present in this turn. "
                        "Do not claim live web or private analytics unless a tool result is present."
                    ),
                },
            ]
        return await _xai_chat_completion(
            messages=xai_messages,
            tools=tools,
            temperature=temp,
            timeout=timeout,
            model=selected_model,
            force_tool_call=force_tool_call,
            max_tokens=_completion_token_cap(reasoning_depth, has_tools=bool(tools)),
        )

    # Bare Claude IDs use Anthropic direct. Provider-qualified anthropic/* IDs
    # stay on OpenRouter when that exact route was selected.
    if route.route_provider == "anthropic_direct":
        direct_messages = list(route_messages)
        if web_search:
            direct_messages = [
                *direct_messages,
                {
                    "role": "system",
                    "content": (
                        "Direct Anthropic mode does not provide OpenRouter web search. "
                        "Use only Studio tool results already present in this turn. "
                        "Do not claim live web, YouTube, or private analytics access "
                        "unless a tool result is present."
                    ),
                },
            ]
        return await _anthropic_chat_completion(
            messages=direct_messages,
            tools=tools,
            temperature=temp,
            timeout=timeout,
            provider_label="anthropic_direct",
            model_override=selected_model,
            force_tool_call=force_tool_call,
            preserve_tool_names=preserve_tool_names,
        )

    if route.route_provider != "openrouter":
        raise RuntimeError(f"Unsupported Studio Agent model route: {route.route_provider}")

    payload: dict[str, Any] = {
        "model": selected_model,
        "messages": route_messages,
        "temperature": temp,
        "max_tokens": _completion_token_cap(reasoning_depth, has_tools=bool(tools)),
    }
    if reasoning:
        payload["reasoning"] = reasoning
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "required" if force_tool_call else "auto"
    if web_search:
        # OpenRouter's built-in web plugin: runs a live web search (Exa) and
        # injects results before the model answers — works with any model slug.
        try:
            max_results = int(os.getenv("STUDIO_AGENT_WEB_MAX_RESULTS", "5"))
        except (TypeError, ValueError):
            max_results = 5
        payload["plugins"] = [{"id": "web", "max_results": max(1, min(max_results, 10))}]

    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(f"{OPENROUTER_BASE}/chat/completions", headers=_headers(), json=payload)
        if _is_openrouter_credit_limit(r.status_code, r.text):
            # Retry once with a cheap answer budget. Keep tools enabled so normal
            # Studio tool routing can still work, but disable reasoning because it
            # is the easiest hidden multiplier on low balances.
            retry_payload = dict(payload)
            retry_payload["max_tokens"] = min(int(retry_payload.get("max_tokens") or 2048), 1024)
            retry_payload.pop("reasoning", None)
            r = await client.post(f"{OPENROUTER_BASE}/chat/completions", headers=_headers(), json=retry_payload)
        if r.status_code >= 400:
            detail = r.text[:2000]
            raise RuntimeError(
                f"OpenRouter {r.status_code} for selected model "
                f"{route.requested_model}: {detail}"
            )
        return r.json()


def message_from_response(resp: dict[str, Any]) -> dict[str, Any]:
    choice = (resp.get("choices") or [{}])[0]
    return choice.get("message") or {}


def usage_from_response(resp: dict[str, Any]) -> dict[str, Any]:
    return resp.get("usage") or {}
