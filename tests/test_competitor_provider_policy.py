from __future__ import annotations

import pytest

from studio_agent import competitor, provider_policy, reference_providers


def test_denied_semantic_qa_adapters_fail_before_http(monkeypatch) -> None:
    monkeypatch.setenv("FAL_KEY", "stale-fal-key")
    monkeypatch.setenv("OPENROUTER_API_KEY", "stale-openrouter-key")
    monkeypatch.setenv("STUDIO_AGENT_VISION_MODEL", "google/gemini-2.5-flash")

    class _NoClient:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("blocked semantic QA constructed an HTTP client")

    monkeypatch.setattr(competitor.httpx, "Client", _NoClient)

    with pytest.raises(provider_policy.ProviderPolicyDenied):
        competitor._summarize_keyframe_visuals_fal(
            ["unused.jpg"],
            prompt_text="judge this",
        )
    with pytest.raises(provider_policy.ProviderPolicyDenied):
        competitor._analyze_storytelling_fal("judge this")

    openrouter_visual = competitor._summarize_keyframe_visuals_openrouter(
        ["unused.jpg"],
        prompt_text="judge this",
        content_format="short",
    )
    openrouter_story = competitor._analyze_storytelling_openrouter("judge this")
    assert openrouter_visual["error"] == "openrouter_disabled_by_provider_policy"
    assert openrouter_story["error"] == "openrouter_disabled_by_provider_policy"
    assert competitor._openrouter_key() == ""


def test_visual_summary_runs_only_direct_anthropic(monkeypatch, tmp_path) -> None:
    frame = tmp_path / "frame.jpg"
    frame.write_bytes(b"frame")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "configured-for-test")
    monkeypatch.setenv("CLAUDE_API_KEY", "")
    called: list[str] = []

    def anthropic_runner(picks: list[str], *, prompt_text: str) -> dict:
        called.append("anthropic")
        assert picks == [str(frame)]
        assert prompt_text
        return {
            "summary": "A factual visual summary.",
            "frames_reviewed": 1,
            "model": "claude-haiku-4-5-20251001",
            "provider": "anthropic",
        }

    monkeypatch.setattr(competitor, "_summarize_keyframe_visuals_anthropic", anthropic_runner)
    monkeypatch.setattr(
        competitor,
        "_summarize_keyframe_visuals_fal",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("FAL semantic QA ran")),
    )
    monkeypatch.setattr(
        competitor,
        "_summarize_keyframe_visuals_openrouter",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("OpenRouter semantic QA ran")),
    )

    result = competitor.summarize_keyframe_visuals([str(frame)], source_name="test")

    assert result["provider"] == "anthropic"
    assert called == ["anthropic"]


def test_story_packaging_runs_only_direct_anthropic(monkeypatch) -> None:
    called: list[str] = []

    def anthropic_runner(prompt: str) -> dict:
        called.append("anthropic")
        assert prompt
        return {"summary": "Hook and pacing analysis.", "provider": "anthropic"}

    monkeypatch.setattr(competitor, "_analyze_storytelling_anthropic", anthropic_runner)
    monkeypatch.setattr(
        competitor,
        "_analyze_storytelling_fal",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("FAL semantic QA ran")),
    )
    monkeypatch.setattr(
        competitor,
        "_analyze_storytelling_openrouter",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("OpenRouter semantic QA ran")),
    )

    result = competitor.analyze_storytelling_packaging(
        transcript_text="The opening line establishes the challenge.",
        segments=[],
        visual_summary="A host stands beside the challenge set.",
        pacing={"duration_sec": 30},
        content_format="short",
    )

    assert result["provider"] == "anthropic"
    assert called == ["anthropic"]


def test_fal_remains_available_for_reference_audio_media(monkeypatch, tmp_path) -> None:
    audio = tmp_path / "reference.wav"
    audio.write_bytes(b"audio")
    monkeypatch.setattr(
        reference_providers,
        "_provider_available",
        lambda provider: provider == "fal",
    )
    monkeypatch.setattr(
        reference_providers,
        "transcribe_fal_segments",
        lambda path: {"text": "media transcript", "segments": [], "provider": "fal"},
    )

    result = competitor.transcribe_reference_audio(str(audio))

    assert result["provider"] == "fal"
    assert result["text"] == "media transcript"
