"""Canonical Studio Agent model policy and provider routing.

The model a creator selects and the API route used to call that model are two
different decisions.  Keep them separate so a provider outage or missing key
can never silently turn one selected model into another model.
"""
from __future__ import annotations

from dataclasses import dataclass

from studio_agent import provider_policy


class ModelSelectionError(ValueError):
    """Base error for an invalid Studio Agent model selection."""


class ModelDisabledError(ModelSelectionError):
    """Raised when product policy deliberately disables a model."""


class ModelUnavailableError(ModelSelectionError):
    """Raised when no configured provider can call the selected model."""


@dataclass(frozen=True)
class ModelPolicy:
    selectable: bool = True
    disabled_reason: str = ""


@dataclass(frozen=True)
class ModelRoute:
    """One exact API route for the creator's selected model."""

    requested_model: str
    canonical_model: str
    route_provider: str
    provider_model_id: str


# Provider policy, rather than a one-off model blacklist, owns Studio runner
# selection. Denied providers are hidden from the catalog and rejected before
# a provider adapter can attempt network I/O.
MODEL_POLICY_OVERRIDES: dict[str, ModelPolicy] = {}
ALWAYS_VISIBLE_MODEL_IDS: tuple[str, ...] = ()


_XAI_PREFIXES = ("x-ai/", "xai/", "x.ai/")
_XAI_ALIASES = {
    "grok-4.5-latest": "grok-4.5",
    "grok-4-5": "grok-4.5",
    "grok4.5": "grok-4.5",
    "grok-4.3-latest": "grok-4.3",
    "grok-4-3": "grok-4.3",
    "grok-4.20-reasoning": "grok-4.20-0309-reasoning",
    "grok-4.20-non-reasoning": "grok-4.20-0309-non-reasoning",
    "grok-4.20": "grok-4.20-0309-reasoning",
}

_ANTHROPIC_OPENROUTER_IDS = {
    "claude-sonnet-5": "anthropic/claude-sonnet-5",
    "claude-haiku-4-5-20251001": "anthropic/claude-haiku-4.5",
    "claude-sonnet-4-6": "anthropic/claude-sonnet-4.6",
    "claude-opus-4-8": "anthropic/claude-opus-4.8",
    "claude-fable-5": "anthropic/claude-fable-5",
}


def normalize_xai_model_id(model_id: str) -> str:
    model = str(model_id or "").strip()
    lowered = model.lower()
    for prefix in _XAI_PREFIXES:
        if lowered.startswith(prefix):
            model = model[len(prefix) :]
            break
    return _XAI_ALIASES.get(model.lower(), model)


def canonical_model_id(model_id: str) -> str:
    """Return a stable identity used for policy and catalog de-duplication."""
    model = str(model_id or "").strip()
    lowered = model.lower()
    if lowered.startswith(_XAI_PREFIXES) or lowered.startswith("grok-"):
        return normalize_xai_model_id(model).lower()
    if provider_policy.model_provider(model) == "anthropic":
        return provider_policy.normalize_anthropic_model_id(model).lower()
    return lowered


def is_xai_model_id(model_id: str | None) -> bool:
    model = str(model_id or "").strip().lower()
    return bool(model) and (
        model.startswith(_XAI_PREFIXES)
        or model.startswith("grok-")
    )


def is_anthropic_model_id(model_id: str | None) -> bool:
    return provider_policy.model_provider(model_id) == "anthropic"


def model_policy(model_id: str) -> ModelPolicy:
    override = MODEL_POLICY_OVERRIDES.get(canonical_model_id(model_id))
    if override is not None:
        return override
    try:
        provider_policy.assert_runner_model_allowed(model_id)
    except provider_policy.ProviderPolicyDenied as exc:
        return ModelPolicy(selectable=False, disabled_reason=str(exc))
    return ModelPolicy()


def catalog_policy_fields(model_id: str) -> dict[str, object]:
    policy = model_policy(model_id)
    return {
        "selectable": policy.selectable,
        "disabled": not policy.selectable,
        "disabled_reason": policy.disabled_reason or None,
    }


def assert_model_selectable(model_id: str) -> None:
    model = str(model_id or "").strip()
    if not model:
        raise ModelSelectionError("A Studio Agent model must be selected.")
    policy = model_policy(model)
    if not policy.selectable:
        raise ModelDisabledError(policy.disabled_reason or f"{model} is disabled.")


def _openrouter_xai_model_id(model_id: str) -> str:
    model = str(model_id or "").strip()
    if model.lower().startswith("x-ai/"):
        return model
    return f"x-ai/{normalize_xai_model_id(model)}"


def _openrouter_anthropic_model_id(model_id: str) -> str:
    model = str(model_id or "").strip()
    if model.lower().startswith("anthropic/"):
        return model
    canonical = canonical_model_id(model)
    return _ANTHROPIC_OPENROUTER_IDS.get(canonical, f"anthropic/{model}")


def resolve_model_route(
    model_id: str,
    *,
    xai_configured: bool,
    anthropic_configured: bool,
    openrouter_configured: bool,
) -> ModelRoute:
    """Resolve an allowed runner model to direct Anthropic only.

    The legacy configuration flags remain in the signature for release-call
    compatibility. xAI and OpenRouter configuration is deliberately inert.
    """
    requested = str(model_id or "").strip()
    assert_model_selectable(requested)
    direct_id = provider_policy.assert_runner_model_allowed(requested)
    if not anthropic_configured:
        raise ModelUnavailableError(
            f"Selected model {requested} requires ANTHROPIC_API_KEY."
        )
    return ModelRoute(
        requested,
        direct_id.lower(),
        "anthropic_direct",
        direct_id,
    )
