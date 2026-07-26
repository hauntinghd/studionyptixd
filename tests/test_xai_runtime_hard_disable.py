from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

import backend
import backend_demo
import catalyst
from long_form import build_hr_xai
from studio_agent import provider_policy, reference_providers


def test_packaged_backend_demo_xai_paths_reject_stale_key_before_io(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("XAI_API_KEY", "stale-saved-key")

    class NoNetwork:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("backend_demo attempted provider I/O")

    monkeypatch.setattr(backend_demo.httpx, "AsyncClient", NoNetwork)

    calls = (
        lambda: backend_demo.analyze_screen_recording(str(tmp_path / "missing.mp4")),
        lambda: backend_demo.generate_demo_script({"duration": 10}),
        lambda: backend_demo.generate_ai_face(str(tmp_path / "face.png")),
    )
    for call in calls:
        with pytest.raises(provider_policy.ProviderPolicyDenied, match="denies xai"):
            asyncio.run(call())


def test_packaged_history_rewind_xai_paths_reject_stale_key_before_io(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("XAI_API_KEY", "stale-saved-key")

    with pytest.raises(provider_policy.ProviderPolicyDenied, match="denies xai"):
        build_hr_xai.XAIClient()
    with pytest.raises(provider_policy.ProviderPolicyDenied, match="denies xai"):
        build_hr_xai.gen_grok_image(
            "retired provider prompt",
            tmp_path / "still.png",
            stats={},
        )
    assert not (tmp_path / "still.png").exists()


def test_catalyst_uploaded_reference_audio_uses_fal_adapter(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    audio_path = tmp_path / "reference.mp3"
    audio_path.write_bytes(b"audio")
    calls: list[str] = []

    def fake_fal_transcribe(path: str) -> dict:
        calls.append(path)
        return {"text": "FAL transcript", "provider": "fal", "segments": []}

    monkeypatch.setattr(
        reference_providers,
        "transcribe_fal_segments",
        fake_fal_transcribe,
    )

    result = asyncio.run(catalyst._transcribe_audio_with_grok(str(audio_path)))

    assert result == "Sampled audio transcript excerpt: FAL transcript"
    assert calls == [str(audio_path)]
    backend_source = Path(backend.__file__).read_text(encoding="utf-8")
    assert "FasterWhisperModel" not in backend_source
    assert "_reference_whisper_model" not in backend_source


def test_cliplab_runpod_paths_fail_before_network() -> None:
    from cliplab.intelligence import _score_with_runpod
    from cliplab.reframe import _runpod_face_trajectory

    with pytest.raises(provider_policy.ProviderPolicyDenied, match="denies runpod"):
        asyncio.run(_score_with_runpod([], "", ""))
    with pytest.raises(provider_policy.ProviderPolicyDenied, match="denies runpod"):
        asyncio.run(_runpod_face_trajectory("missing.mp4", 0.0, 1.0))
