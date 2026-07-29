from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from skeleton_ai import compose, i2v_engine, styled_stills
from studio_agent import dictation, dictation_stream, provider_policy


def test_legacy_image_models_normalize_to_explicit_fal_models() -> None:
    assert styled_stills.normalize_fal_image_model_id("grok_imagine") == "seedream_edit"
    assert styled_stills.normalize_fal_image_model_id("grok-imagine-image-quality") == "seedream_edit"
    assert styled_stills.normalize_fal_image_model_id("seedream_v5_lite_modal") == "seedream_v5_lite"
    assert styled_stills.normalize_fal_image_model_id("seedream_v4") == "seedream_v4"


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
        lambda _url, dest: Path(dest).write_bytes(b"image" * 400),
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


def test_default_fal_video_model_is_pinned() -> None:
    """Guard the default motion lane against silent drift.

    Every other test in this area asserts the *invariant* that legacy routes
    migrate to whatever the default is. That keeps them honest across a model
    change but cannot catch the default itself moving, so pin it exactly once
    here. Changing this literal should be a deliberate product decision: motion
    is roughly three quarters of a short's provider cost.
    """
    assert i2v_engine.DEFAULT_FAL_VIDEO_MODEL == "kling_pro"
    assert provider_policy.DEFAULT_FAL_VIDEO_MODEL == i2v_engine.DEFAULT_FAL_VIDEO_MODEL


def test_default_lane_keeps_a_permissive_fallback_hop() -> None:
    """A content-policy bounce must stay recoverable on the default lane."""
    chain, _model = i2v_engine.resolve_video_model_chain(
        video_model=i2v_engine.DEFAULT_FAL_VIDEO_MODEL
    )
    assert len(chain) > 1, "the default lane needs a second hop for moderation bounces"
    assert chain[-1] == i2v_engine.PIXVERSE_V6_ENDPOINT


def test_legacy_video_models_normalize_to_default_fal_chain() -> None:
    default_model = i2v_engine.DEFAULT_FAL_VIDEO_MODEL
    expected_chain = list(i2v_engine.VIDEO_MODELS[default_model]["endpoints"])
    for legacy in (
        "grok_imagine_video",
        "grok-imagine-video-1.5",
        "xai:grok-imagine-video-1.5:1080p",
    ):
        chain, model = i2v_engine.resolve_video_model_chain(video_model=legacy)
        assert model == default_model
        assert chain == expected_chain
        assert all(not endpoint.startswith("xai:") for endpoint in chain)


def test_legacy_video_selection_dispatches_silent_fal_request(monkeypatch, tmp_path: Path) -> None:
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
        lambda _path: {"status": "pass", "pass": True, "audio_streams": []},
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
        lambda _url, dest: Path(dest).write_bytes(b"video" * 400),
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

    default_model = i2v_engine.DEFAULT_FAL_VIDEO_MODEL
    first_hop = i2v_engine.VIDEO_MODELS[default_model]["endpoints"][0]
    assert [endpoint for endpoint, _args in calls] == [first_hop]
    # Endpoints spell the audio switch differently (generate_audio vs
    # generate_audio_switch) and Kling 2.1 Pro has no audio parameter at all, so
    # assert the invariant rather than one endpoint's payload shape: no audio key
    # may ever be enabled. The unconditional strip+verify is covered by
    # audio_stripped below.
    audio_flags = {k: v for k, v in calls[0][1].items() if "audio" in k}
    assert all(value is False for value in audio_flags.values()), audio_flags
    assert calls[0][1]["prompt"].startswith("SILENT visual-only")
    metadata = json.loads(output.with_suffix(".mp4.fal.json").read_text(encoding="utf-8"))
    assert metadata["provider"] == "fal"
    assert metadata["video_model"] == default_model
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


def test_recorded_dictation_ignores_saved_xai_key_and_requires_fal(monkeypatch) -> None:
    monkeypatch.setenv("XAI_API_KEY", "saved-but-ignored")
    monkeypatch.delenv("FAL_KEY", raising=False)
    monkeypatch.setattr(dictation, "FAL_AI_KEY", "")

    with pytest.raises(RuntimeError, match="missing FAL_KEY/FAL_AI_KEY"):
        asyncio.run(dictation.transcribe_audio_bytes(b"audio", filename="line.webm"))


def test_recorded_dictation_reports_fal_provider(monkeypatch) -> None:
    monkeypatch.setenv("XAI_API_KEY", "saved-but-ignored")
    monkeypatch.setattr(dictation, "_transcribe_fal", lambda data, filename: "hello studio")

    assert asyncio.run(dictation.transcribe_audio_bytes(b"audio")) == ("hello studio", "fal")


def test_live_dictation_websocket_fails_closed_without_upstream(monkeypatch) -> None:
    class FakeWebSocket:
        def __init__(self) -> None:
            self.events: list[dict] = []
            self.closed = False

        async def send_json(self, event: dict) -> None:
            self.events.append(event)

        async def close(self) -> None:
            self.closed = True

    monkeypatch.setenv("XAI_API_KEY", "saved-but-ignored")
    websocket = FakeWebSocket()
    asyncio.run(dictation_stream.proxy_dictation_stream(websocket))

    assert websocket.events == [
        {
            "type": "error",
            "code": "live_dictation_disabled",
            "message": dictation_stream.LIVE_DICTATION_DISABLED_MESSAGE,
        }
    ]
    assert websocket.closed is True


def test_owned_media_modules_contain_no_xai_network_endpoint() -> None:
    modules = (
        styled_stills,
        i2v_engine,
        dictation,
        dictation_stream,
    )
    for module in modules:
        source = Path(module.__file__).read_text(encoding="utf-8").lower()
        assert "api.x.ai" not in source
        assert "xai_api_key" not in source

