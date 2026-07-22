from __future__ import annotations

import pytest

from studio_agent import provider_policy, reference_providers


def test_reference_orders_ignore_stale_denied_provider_configuration(monkeypatch) -> None:
    monkeypatch.setenv("STUDIO_REFERENCE_VISION_PROVIDER_ORDER", "openrouter,google,xai,fal")
    monkeypatch.setenv("STUDIO_REFERENCE_ANALYSIS_PROVIDER_ORDER", "openrouter")
    monkeypatch.setenv("STUDIO_REFERENCE_STT_PROVIDER_ORDER", "xai")

    assert reference_providers.vision_provider_order() == ["anthropic"]
    assert reference_providers.analysis_provider_order() == ["anthropic"]
    assert reference_providers.stt_provider_order() == ["fal"]


def test_denied_reference_adapters_reject_before_io() -> None:
    with pytest.raises(provider_policy.ProviderPolicyDenied):
        reference_providers.reference_openrouter_vision_model()
    with pytest.raises(provider_policy.ProviderPolicyDenied):
        reference_providers.reference_fal_analysis_model()


def test_reference_chain_rejects_denied_provider_before_runner_callback() -> None:
    called = False

    def denied_runner() -> dict:
        nonlocal called
        called = True
        return {"summary": "should not run"}

    result = reference_providers.run_provider_chain(
        ["openrouter"],
        {"openrouter": denied_runner},
        success_key="summary",
    )

    assert called is False
    assert "denies openrouter" in result["error"]


def test_fal_stt_chain_remains_actionable(monkeypatch) -> None:
    monkeypatch.setattr(reference_providers, "_provider_available", lambda name: name == "fal")
    result = reference_providers.run_provider_chain(
        reference_providers.stt_provider_order(),
        {"fal": lambda: {"text": "transcribed", "provider": "fal"}},
        success_key="text",
    )
    assert result == {"text": "transcribed", "provider": "fal"}
