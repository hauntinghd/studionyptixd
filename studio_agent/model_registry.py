"""Canonical Studio Agent model policy and provider routing.

The model a creator selects and the API route used to call that model are two
different decisions.  Keep them separate so a provider outage or missing key
can never silently turn one selected model into another model.
"""
from __future__ import annotations

from dataclasses import dataclass


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


GROK_BUILD_DISABLED_REASON = (
    "Grok Build 0.1 is not available as a Studio Agent runner."
)

# Keep policy overrides small.  Model capability support belongs in the
# provider adapter; this table is only for deliberate product-level policy.
MODEL_POLICY_OVERRIDES: dict[str, ModelPolicy] = {
    "grok-build-0.1": ModelPolicy(
        selectable=False,
        disabled_reason=GROK_BUILD_DISABLED_REASON,
    ),
}

# Policy-disabled rows stay in the catalog so the picker can explain why they
# cannot be selected instead of silently making them disappear.
ALWAYS_VISIBLE_MODEL_IDS: tuple[str, ...] = tuple(MODEL_POLICY_OVERRIDES)


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
    if lowered.startswith("anthropic/"):
        return model.split("/", 1)[1].lower()
    return lowered


def is_xai_model_id(model_id: str | None) -> bool:
    model = str(model_id or "").strip().lower()
    return bool(model) and (
        model.startswith(_XAI_PREFIXES)
        or model.startswith("grok-")
    )


def is_anthropic_model_id(model_id: str | None) -> bool:
    model = str(model_id or "").strip().lower()
    return bool(model) and (
        model.startswith("anthropic/")
        or model.startswith("claude-")
        or model in {"sonnet", "opus", "haiku"}
    )


def model_policy(model_id: str) -> ModelPolicy:
    return MODEL_POLICY_OVERRIDES.get(canonical_model_id(model_id), ModelPolicy())


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
    """Resolve one provider route without changing the selected model.

    Provider-qualified OpenRouter IDs remain on OpenRouter when it is
    configured. Bare xAI/Anthropic IDs prefer their direct API and fall back to
    OpenRouter only for the *same* model.
    """
    requested = str(model_id or "").strip()
    assert_model_selectable(requested)
    canonical = canonical_model_id(requested)
    lowered = requested.lower()

    if is_xai_model_id(requested):
        explicitly_openrouter = lowered.startswith("x-ai/")
        if explicitly_openrouter and openrouter_configured:
            return ModelRoute(requested, canonical, "openrouter", requested)
        if xai_configured:
            return ModelRoute(
                requested,
                canonical,
                "xai_direct",
                normalize_xai_model_id(requested),
            )
        if openrouter_configured:
            return ModelRoute(
                requested,
                canonical,
                "openrouter",
                _openrouter_xai_model_id(requested),
            )
        raise ModelUnavailableError(
            f"Selected model {requested} requires XAI_API_KEY or OPENROUTER_API_KEY."
        )

    if is_anthropic_model_id(requested):
        explicitly_openrouter = lowered.startswith("anthropic/")
        if explicitly_openrouter and openrouter_configured:
            return ModelRoute(requested, canonical, "openrouter", requested)
        if anthropic_configured:
            direct_id = requested.split("/", 1)[1] if explicitly_openrouter else requested
            return ModelRoute(requested, canonical, "anthropic_direct", direct_id)
        if openrouter_configured:
            return ModelRoute(
                requested,
                canonical,
                "openrouter",
                _openrouter_anthropic_model_id(requested),
            )
        raise ModelUnavailableError(
            f"Selected model {requested} requires ANTHROPIC_API_KEY or OPENROUTER_API_KEY."
        )

    if openrouter_configured:
        return ModelRoute(requested, canonical, "openrouter", requested)

    raise ModelUnavailableError(
        f"Selected model {requested} requires OPENROUTER_API_KEY."
    )
