from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from fastapi import BackgroundTasks, HTTPException

import backend
import backend_public_config
from backend_models import GenerateRequest
from skeleton_ai import voice_fal
from studio_agent import provider_policy


@pytest.mark.parametrize(
    ("field", "model_id", "capability"),
    [
        ("image_model_id", "grok_imagine", "image"),
        ("image_model_id", "imagen4_preview", "image"),
        ("image_model_id", "openai/dall-e-3", "image"),
        ("video_model_id", "grok_imagine_video", "video"),
        ("video_model_id", "veo3_fast", "video"),
        ("video_model_id", "openai/sora", "video"),
    ],
)
def test_generate_rejects_explicit_denied_model_before_readiness_or_billing(
    monkeypatch,
    field: str,
    model_id: str,
    capability: str,
) -> None:
    downstream_calls: list[str] = []

    monkeypatch.setattr(backend, "_assert_not_waitlist_only_for_non_owner", lambda _user: None)
    monkeypatch.setattr(backend, "_ensure_template_allowed", lambda _template, _user: None)
    monkeypatch.setattr(
        backend,
        "_require_studio_provider_readiness",
        lambda **_kwargs: downstream_calls.append("provider_readiness"),
    )

    async def fake_reserve(*_args, **_kwargs):
        downstream_calls.append("credit_reservation")
        return True, "test", {}

    monkeypatch.setattr(backend, "_reserve_generation_credit", fake_reserve)
    request_data = {
        "template": "story",
        "prompt": "Provider policy boundary test",
        field: model_id,
    }

    with pytest.raises(HTTPException) as caught:
        asyncio.run(
            backend._generate_short(
                GenerateRequest(**request_data),
                BackgroundTasks(),
                user={"id": "policy-user", "email": "policy@example.test"},
            )
        )

    assert caught.value.status_code == 400
    assert f"denies {capability} model {model_id}" in str(caught.value.detail)
    assert downstream_calls == []


def test_public_creative_catalog_advertises_only_fal_safe_rows(monkeypatch) -> None:
    # Seed the upstream catalog with stale denied and non-FAL rows. The public
    # payload is the final authority consumed by legacy Creative clients.
    monkeypatch.setattr(
        backend_public_config,
        "seedream_model_profiles",
        lambda **_kwargs: [
            {
                "id": "seedream_edit",
                "provider": "fal",
                "text_to_image_endpoint": "fal-ai/bytedance/seedream/v4.5/text-to-image",
                "enabled": True,
            },
            {"id": "grok_imagine", "provider": "fal", "enabled": True},
            {"id": "imagen4_preview", "provider": "google", "enabled": True},
            {"id": "seedream_v5_lite_modal", "provider": "modal", "enabled": True},
        ],
    )
    build_payload = backend_public_config.build_public_config_payload(
        maintenance_snapshot=lambda: (False, ""),
        story_art_style_count=lambda: 0,
        default_membership_plan_id=lambda: "creator",
        youtube_auth_configured=lambda: False,
        youtube_public_api_key_candidates=lambda: [],
        youtube_active_oauth_mode=lambda: "none",
    )

    payload = asyncio.run(build_payload())
    catalog = payload["creative_model_catalog"]
    image_rows = catalog["image_models"]
    video_rows = catalog["video_models"]

    assert image_rows
    assert video_rows
    assert catalog["default_image_model_id"] in {row["id"] for row in image_rows}
    assert catalog["default_video_model_id"] in {row["id"] for row in video_rows}
    for row in [*image_rows, *video_rows]:
        assert provider_policy.normalize_provider(row.get("provider")) == "fal"
        assert not provider_policy.is_denied_image_model(row.get("id"))
        assert not provider_policy.is_denied_video_model(row.get("id"))


def test_legacy_animation_fails_closed_without_fal_before_executor(monkeypatch, tmp_path: Path) -> None:
    executor_calls: list[str] = []

    async def fake_kling(*_args, **_kwargs):
        executor_calls.append("kling")
        raise AssertionError("animation executor must not run without FAL")

    async def fake_fal_queue(*_args, **_kwargs):
        executor_calls.append("fal_queue")
        raise AssertionError("animation executor must not run without FAL")

    monkeypatch.setattr(backend, "FAL_AI_KEY", "")
    monkeypatch.setattr(backend, "animate_image_kling", fake_kling)
    monkeypatch.setattr(backend, "animate_image_fal_queue_model", fake_fal_queue)

    with pytest.raises(RuntimeError, match="FAL_AI_KEY not configured"):
        asyncio.run(
            backend.animate_scene(
                str(tmp_path / "still.png"),
                "Slow camera push",
                str(tmp_path),
                scene_idx=1,
                job_ts="policy",
                video_model_id="kling21_standard",
            )
        )

    assert executor_calls == []


def test_fal_voice_generation_migrates_retired_ids_and_stale_env(monkeypatch, tmp_path: Path) -> None:
    calls: list[dict] = []

    def fake_synthesize(**kwargs) -> Path:
        calls.append(kwargs)
        output = Path(kwargs["out_path"])
        output.write_bytes(b"provider-free-audio")
        return output

    retired_voice_id = "21m00Tcm4TlvDq8ikWAM"
    monkeypatch.setenv("FAL_TTS_VOICE_ID", retired_voice_id)
    monkeypatch.setattr(voice_fal, "synthesize", fake_synthesize)

    result = asyncio.run(
        backend._generate_fal_voiceover(
            "A provider-free narration test.",
            str(tmp_path / "voice.mp3"),
            template="story",
            override_voice_id=retired_voice_id,
        )
    )

    assert result["provider"] == "fal"
    assert result["voice_id"] == voice_fal.DEFAULT_VOICE
    assert result["voice_migrated_from"] == retired_voice_id
    assert calls[0]["voice_id"] == voice_fal.DEFAULT_VOICE

