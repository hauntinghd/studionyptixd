from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

import skeleton_ai_router
from skeleton_ai import (
    compose,
    i2v_engine,
    scripting_grok,
    styled_stills,
    voice_auto,
    voice_fal,
    voice_xai,
)


def _router_client() -> TestClient:
    app = FastAPI()
    app.include_router(
        skeleton_ai_router.build_skeleton_ai_router(
            require_auth=lambda: {"id": "fal-policy-user"},
        )
    )
    return TestClient(app)


def test_legacy_media_models_migrate_to_explicit_fal_models() -> None:
    assert styled_stills.normalize_fal_image_model_id("grok_imagine") == "seedream_edit"
    assert (
        styled_stills.normalize_fal_image_model_id("grok-imagine-image-quality")
        == "seedream_edit"
    )
    assert (
        styled_stills.normalize_fal_image_model_id("seedream_v5_lite_modal")
        == "seedream_v5_lite"
    )
    for legacy in (
        "grok_imagine_video",
        "grok-imagine-video-1.5",
        "xai:grok-imagine-video-1.5:1080p",
    ):
        chain, model_id = i2v_engine.resolve_video_model_chain(video_model=legacy)
        assert model_id == "seedance"
        assert chain == [i2v_engine.SEEDANCE_ENDPOINT, i2v_engine.PIXVERSE_V6_ENDPOINT]
        assert all(not endpoint.startswith("xai:") for endpoint in chain)


def test_legacy_image_selection_dispatches_only_to_fal(monkeypatch, tmp_path: Path) -> None:
    calls: list[tuple[str, object]] = []
    monkeypatch.setenv("XAI_API_KEY", "saved-but-ignored")
    monkeypatch.setattr(styled_stills.render_simulation, "enabled", lambda: False)
    monkeypatch.setattr(styled_stills, "_ensure_fal", lambda: calls.append(("key", None)))
    monkeypatch.setattr(
        styled_stills,
        "_queue_result",
        lambda endpoint, payload, timeout_sec: (
            calls.append((endpoint, payload))
            or {"images": [{"url": "https://example.test/still.png"}]}
        ),
    )
    monkeypatch.setattr(
        styled_stills,
        "_download",
        lambda _url, destination: Path(destination).write_bytes(b"image" * 400),
    )

    result = styled_stills.generate_still_t2i(
        "portrait",
        tmp_path / "still.png",
        negative_prompt="bad anatomy",
        image_model_id="grok_imagine",
    )

    assert calls[0][0] == "key"
    assert calls[1][0] == "fal-ai/bytedance/seedream/v4.5/text-to-image"
    assert result["provider_transport"] == "fal"
    assert result["provider"] == "seedream_edit"
    assert result["model_migrated_from"] == "grok_imagine"


def test_legacy_video_selection_dispatches_silent_fal_request(
    monkeypatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, dict]] = []

    class FakeFalClient:
        @staticmethod
        def upload_file(_path: str) -> str:
            return "https://example.test/still.png"

    monkeypatch.setenv("XAI_API_KEY", "saved-but-ignored")
    monkeypatch.setattr(i2v_engine.render_simulation, "enabled", lambda: False)
    monkeypatch.setattr(i2v_engine, "fal_client", FakeFalClient())
    monkeypatch.setattr(i2v_engine, "_ensure_fal", lambda: "fal-key")
    monkeypatch.setattr(
        i2v_engine,
        "_verify_silent_output",
        lambda _path: {"status": "pass", "pass": True, "audio_streams": 0},
    )
    monkeypatch.setattr(
        i2v_engine.production_costs,
        "price_fal_video",
        lambda endpoint, seconds: (0.1, "test", "fal_i2v"),
    )
    monkeypatch.setattr(
        i2v_engine,
        "_queue_result",
        lambda endpoint, args, timeout_sec: (
            calls.append((endpoint, args))
            or {
                "video": {"url": "https://example.test/video.mp4"},
                "_fal_request_id": "fal-request",
            }
        ),
    )
    monkeypatch.setattr(
        i2v_engine,
        "_download",
        lambda _url, destination: Path(destination).write_bytes(b"video" * 400),
    )
    monkeypatch.setattr(compose, "strip_clip_audio", lambda _path: None)
    still = tmp_path / "still.png"
    still.write_bytes(b"image" * 400)
    output = tmp_path / "clip.mp4"

    i2v_engine.generate(
        still,
        "Slow camera push",
        output,
        video_model="grok_imagine_video",
    )

    assert [endpoint for endpoint, _args in calls] == [i2v_engine.SEEDANCE_ENDPOINT]
    assert calls[0][1]["generate_audio"] is False
    assert calls[0][1]["prompt"].startswith("SILENT visual-only")
    metadata = json.loads(
        output.with_suffix(output.suffix + ".fal.json").read_text(encoding="utf-8")
    )
    assert metadata["provider"] == "fal"
    assert metadata["video_model"] == "seedance"
    assert metadata["model_migrated_from"] == "grok_imagine_video"
    assert metadata["audio_stripped"] is True


def test_i2v_requires_fal_before_upload(monkeypatch, tmp_path: Path) -> None:
    uploaded = False

    class FakeFalClient:
        @staticmethod
        def upload_file(_path: str) -> str:
            nonlocal uploaded
            uploaded = True
            return "https://example.test/still.png"

    monkeypatch.setattr(i2v_engine.render_simulation, "enabled", lambda: False)
    monkeypatch.setattr(i2v_engine, "fal_client", FakeFalClient())
    monkeypatch.setattr(
        i2v_engine,
        "_ensure_fal",
        lambda: (_ for _ in ()).throw(i2v_engine.I2VError("missing FAL key")),
    )
    still = tmp_path / "still.png"
    still.write_bytes(b"image" * 400)

    with pytest.raises(i2v_engine.I2VError, match="missing FAL key"):
        i2v_engine.generate(still, "motion", tmp_path / "clip.mp4")
    assert uploaded is False


def test_i2v_silence_verification_rejects_any_audio_stream(monkeypatch) -> None:
    monkeypatch.setattr(
        i2v_engine.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout=json.dumps(
                {
                    "streams": [
                        {"index": 0, "codec_type": "video"},
                        {"index": 1, "codec_type": "audio"},
                    ]
                }
            ),
            stderr="",
        ),
    )

    with pytest.raises(i2v_engine.I2VError, match="still contains an audio stream"):
        i2v_engine._verify_silent_output(Path("clip.mp4"))


def test_i2v_route_guard_blocks_stale_fal_fallback(monkeypatch, tmp_path: Path) -> None:
    calls: list[str] = []

    class FakeFalClient:
        @staticmethod
        def upload_file(_path: str) -> str:
            return "https://example.test/still.png"

    monkeypatch.setattr(i2v_engine.render_simulation, "enabled", lambda: False)
    monkeypatch.setattr(i2v_engine, "fal_client", FakeFalClient())
    monkeypatch.setattr(i2v_engine, "_ensure_fal", lambda: "fal-key")
    monkeypatch.setattr(
        i2v_engine.production_costs,
        "price_fal_video",
        lambda endpoint, seconds: (0.1, "test", "fal_i2v"),
    )

    def fail_seedance(endpoint: str, _args: dict, timeout_sec: int) -> dict:
        calls.append(endpoint)
        raise i2v_engine.I2VError("content_policy_violation")

    monkeypatch.setattr(i2v_engine, "_queue_result", fail_seedance)
    still = tmp_path / "still.png"
    still.write_bytes(b"image" * 400)

    with pytest.raises(i2v_engine.I2VRouteChanged, match="route changed"):
        i2v_engine.generate(
            still,
            "motion",
            tmp_path / "clip.mp4",
            video_model="seedance",
            fallback_guard=lambda: False,
        )
    assert calls == [i2v_engine.SEEDANCE_ENDPOINT]


def test_voice_catalog_is_static_fal_only_and_hidden_without_key(monkeypatch) -> None:
    monkeypatch.delenv("FAL_AI_KEY", raising=False)
    monkeypatch.delenv("FAL_KEY", raising=False)
    assert _router_client().get("/api/skeleton-ai/voices").json() == {
        "provider": "fal_minimax",
        "configured": False,
        "voices": [],
    }

    monkeypatch.setenv("FAL_KEY", "configured-for-catalog")

    class NoNetwork:
        def subscribe(self, *_args, **_kwargs):
            raise AssertionError("voice catalog attempted provider network")

    monkeypatch.setattr(voice_fal, "fal_client", NoNetwork())
    payload = _router_client().get("/api/skeleton-ai/voices").json()
    assert payload["configured"] is True
    assert payload["voices"]
    assert all(row["provider"] == "fal_minimax" for row in payload["voices"])


def test_regeneration_motion_prompt_is_capped_at_300_characters() -> None:
    with pytest.raises(ValidationError):
        skeleton_ai_router.RegenerateSceneRequest(
            job_id="job123",
            beat_index=0,
            motion_prompt="x" * 301,
        )
    accepted = skeleton_ai_router.RegenerateSceneRequest(
        job_id="job123",
        beat_index=0,
        motion_prompt="x" * 300,
    )
    assert len(accepted.motion_prompt or "") == 300


def test_scripting_uses_direct_anthropic_and_sonnet_defaults(monkeypatch) -> None:
    calls: list[tuple[str, dict]] = []
    monkeypatch.setenv("ANTHROPIC_API_KEY", "configured-anthropic")

    class Response:
        status_code = 200
        text = ""
        headers: dict[str, str] = {}

        @staticmethod
        def raise_for_status() -> None:
            return None

        @staticmethod
        def json() -> dict:
            return {"content": [{"type": "text", "text": "finished script"}]}

    class Client:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        @staticmethod
        def post(url: str, *, headers: dict, json: dict) -> Response:
            calls.append((url, json))
            return Response()

    monkeypatch.setattr(scripting_grok.httpx, "Client", Client)
    result = scripting_grok.GrokClient().complete(
        "system",
        "user",
        temperature=0.9,
    )

    assert result == "finished script"
    assert calls[0][0] == scripting_grok.ANTHROPIC_MESSAGES_URL
    assert calls[0][1]["model"] == "claude-sonnet-5"
    assert "temperature" not in calls[0][1]


def test_fal_tts_rejects_non_https_download_before_io(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FAL_KEY", "configured-fal")

    class FakeFalClient:
        @staticmethod
        def subscribe(_endpoint: str, *, arguments: dict) -> dict:
            return {"audio": {"url": "http://example.test/voice.mp3"}}

    monkeypatch.setattr(voice_fal.render_simulation, "enabled", lambda: False)
    monkeypatch.setattr(voice_fal, "fal_client", FakeFalClient())
    monkeypatch.setattr(
        voice_fal.urllib.request,
        "urlretrieve",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("unsafe download reached urlretrieve")
        ),
    )

    with pytest.raises(RuntimeError, match="non-HTTPS or malformed"):
        voice_fal.synthesize(text="hello", out_path=tmp_path / "voice.mp3")


def test_stale_xai_voice_selection_migrates_to_fal(monkeypatch, tmp_path: Path) -> None:
    client = voice_auto.AutoVoiceClient(provider="xai")
    destination = tmp_path / "voice.mp3"
    monkeypatch.setattr(
        client._fal,
        "synthesize",
        lambda **kwargs: Path(kwargs["out_path"]),
    )

    assert client.synthesize(text="hello", out_path=destination) == destination
    assert client.provider_preference == "fal"
    assert client.migrated_from == "xai"
    assert client.last_provider == "fal"


def test_xai_voice_compatibility_boundary_fails_before_io(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XAI_API_KEY", "stale-xai-key")
    with pytest.raises(voice_xai.XAITTSError, match="disabled by Studio provider policy"):
        voice_xai.synthesize(text="hello", out_path=tmp_path / "voice.mp3")


@pytest.mark.parametrize(
    "model_id",
    ["grok-4.5", "openai/gpt-5.9", "google/gemini-2.5-flash", "openrouter/auto"],
)
def test_scripting_denies_non_anthropic_models_before_http(
    model_id: str,
    monkeypatch,
) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "configured-anthropic")

    class NoClient:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("denied model constructed an HTTP client")

    monkeypatch.setattr(scripting_grok.httpx, "Client", NoClient)
    with pytest.raises(scripting_grok.GrokAuthError, match="denies"):
        scripting_grok.GrokClient(model=model_id)


def test_owned_execution_modules_have_no_xai_network_path() -> None:
    for module in (styled_stills, i2v_engine, scripting_grok):
        source = Path(module.__file__).read_text(encoding="utf-8").lower()
        assert "api.x.ai" not in source
        assert "xai_api_key" not in source
