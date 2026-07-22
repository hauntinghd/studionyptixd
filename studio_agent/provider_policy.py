"""Effective provider policy for every Studio AI capability.

This module is intentionally dependency-light so provider adapters, catalogs,
session migration, and QA can all enforce the same boundary before attempting
network I/O. Saved environment keys are configuration, not authorization:
only the providers below are effective for Studio.
"""
from __future__ import annotations

from typing import Any


POLICY_VERSION = "studio-anthropic-fal-2026-07-21-v1"

RUNNER_CAPABILITY = "runner"
SEMANTIC_QA_CAPABILITY = "semantic_qa"
IMAGE_CAPABILITY = "image"
I2V_CAPABILITY = "i2v"
TTS_CAPABILITY = "tts"
STT_CAPABILITY = "stt"

DEFAULT_RUNNER_MODEL = "claude-sonnet-5"
DEFAULT_FAL_IMAGE_MODEL = "seedream_edit"
DEFAULT_FAL_VIDEO_MODEL = "seedance"
DEFAULT_FAL_VOICE_PROVIDER = "fal_minimax"


class ProviderPolicyError(RuntimeError):
    """Base class for an effective-policy rejection."""


class ProviderPolicyDenied(ProviderPolicyError):
    """Raised before I/O when a provider/model is outside Studio policy."""


_PROVIDER_ALIASES = {
    "claude": "anthropic",
    "anthropic_direct": "anthropic",
    # Historical response label retained by the compatibility-named adapter.
    # It is still the direct Anthropic API and must share the same policy and
    # billing treatment; it does not authorize a second provider route.
    "anthropic_fallback": "anthropic",
    "direct_anthropic": "anthropic",
    "fal_ai": "fal",
    "fal-ai": "fal",
    "fal_minimax": "fal",
    "minimax_fal": "fal",
    "x.ai": "xai",
    "x-ai": "xai",
    "grok": "xai",
    "open_router": "openrouter",
    "gemini": "google",
    "imagen": "google",
}

_ALLOWED_PROVIDERS = {
    RUNNER_CAPABILITY: frozenset({"anthropic"}),
    SEMANTIC_QA_CAPABILITY: frozenset({"anthropic"}),
    IMAGE_CAPABILITY: frozenset({"fal"}),
    I2V_CAPABILITY: frozenset({"fal"}),
    TTS_CAPABILITY: frozenset({"fal"}),
    STT_CAPABILITY: frozenset({"fal"}),
}

_SONNET_5_ALIASES = {
    "sonnet",
    "sonnet-5",
    "claude-sonnet-5",
    "claude-sonnet-5-latest",
    "claude-sonnet-5.0",
    "anthropic/claude-sonnet-5",
}

DENIED_IMAGE_MODEL_IDS = frozenset({
    "grok_imagine",
    "grok_imagine_quality",
    "grok_imagine_standard",
    "grok-imagine-image",
    "grok-imagine-image-quality",
    "imagen4_fast",
    "imagen4_preview",
    "imagen4_ultra",
    "imagen4",
    "nano_banana_pro",
    "nano_banana_free",
    "seedream_v5_lite_modal",
    "seedream5_lite_modal",
})

DENIED_VIDEO_MODEL_IDS = frozenset({
    "grok_imagine_video",
    "grok_imagine_video_15",
    "grok_imagine_video_15_1080p",
    "grok-imagine-video",
    "veo3_fast",
    "veo_3_fast",
    "veo3",
})


def normalize_provider(provider: Any) -> str:
    raw = str(provider or "").strip().lower().replace(" ", "_")
    return _PROVIDER_ALIASES.get(raw, raw)


def allowed_providers(capability: str) -> frozenset[str]:
    return _ALLOWED_PROVIDERS.get(str(capability or "").strip().lower(), frozenset())


def is_provider_allowed(provider: Any, capability: str) -> bool:
    return normalize_provider(provider) in allowed_providers(capability)


def assert_provider_allowed(provider: Any, capability: str) -> str:
    normalized = normalize_provider(provider)
    allowed = allowed_providers(capability)
    if normalized not in allowed:
        expected = ", ".join(sorted(allowed)) or "no provider"
        raise ProviderPolicyDenied(
            f"Studio provider policy {POLICY_VERSION} denies {normalized or 'unknown'} "
            f"for {capability}; allowed: {expected}."
        )
    return normalized


def model_provider(model_id: Any) -> str:
    model = str(model_id or "").strip().lower()
    if model.startswith("anthropic/") or model.startswith("claude-") or model in {
        "sonnet", "sonnet-5", "opus", "haiku", "fable",
    }:
        return "anthropic"
    if model.startswith(("x-ai/", "xai/", "x.ai/", "grok-")):
        return "xai"
    if model.startswith(("openai/", "gpt-", "chatgpt-", "o1", "o3", "o4")):
        return "openai"
    if model.startswith(("google/", "gemini", "imagen", "veo", "nano_banana")):
        return "google"
    if model.startswith("openrouter/"):
        return "openrouter"
    if "/" in model:
        # Marketplace-style provider-qualified IDs are not direct Anthropic.
        return model.split("/", 1)[0]
    return "unknown"


def normalize_anthropic_model_id(model_id: Any) -> str:
    model = str(model_id or "").strip()
    lowered = model.lower()
    if lowered in _SONNET_5_ALIASES:
        return DEFAULT_RUNNER_MODEL
    if lowered.startswith("anthropic/"):
        return model.split("/", 1)[1]
    return model


def assert_runner_model_allowed(model_id: Any) -> str:
    requested = str(model_id or "").strip()
    provider = model_provider(requested)
    assert_provider_allowed(provider, RUNNER_CAPABILITY)
    direct_id = normalize_anthropic_model_id(requested)
    if not direct_id.lower().startswith("claude-"):
        raise ProviderPolicyDenied(
            f"Studio provider policy {POLICY_VERSION} requires a direct Claude model; "
            f"{requested or 'empty selection'} is denied."
        )
    return direct_id


def is_sonnet_5(model_id: Any) -> bool:
    direct_id = normalize_anthropic_model_id(model_id).lower()
    return direct_id == DEFAULT_RUNNER_MODEL or direct_id.startswith("claude-sonnet-5-")


def sanitize_anthropic_payload(model_id: Any, payload: dict[str, Any]) -> dict[str, Any]:
    """Return an Anthropic request payload compliant with model controls.

    Sonnet 5 uses its service defaults. Studio must not send manual sampling
    controls or a hand-authored thinking/reasoning block for that family.
    """

    cleaned = dict(payload)
    if is_sonnet_5(model_id):
        for key in ("temperature", "top_p", "top_k", "thinking", "reasoning"):
            cleaned.pop(key, None)
    return cleaned


def normalize_media_model_id(model_id: Any) -> str:
    return str(model_id or "").strip().lower().replace(" ", "_")


def is_denied_image_model(model_id: Any) -> bool:
    model = normalize_media_model_id(model_id)
    return model in DENIED_IMAGE_MODEL_IDS or model.startswith(("grok", "imagen", "google/", "gemini"))


def is_denied_video_model(model_id: Any) -> bool:
    model = normalize_media_model_id(model_id).replace("-", "_")
    denied = {item.replace("-", "_") for item in DENIED_VIDEO_MODEL_IDS}
    return model in denied or model.startswith(("grok", "veo", "google/"))


def migrated_image_model(model_id: Any) -> str:
    model = normalize_media_model_id(model_id)
    if model in {"seedream_v5_lite_modal", "seedream5_lite_modal"}:
        return "seedream_v5_lite"
    if is_denied_image_model(model):
        return DEFAULT_FAL_IMAGE_MODEL
    return model


def migrated_video_model(model_id: Any) -> str:
    model = normalize_media_model_id(model_id).replace("-", "_")
    if is_denied_video_model(model):
        return DEFAULT_FAL_VIDEO_MODEL
    return model
