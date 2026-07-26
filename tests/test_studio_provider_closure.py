from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from studio_agent import competitor, cost_optimizer, provider_policy, reference_providers, runner, store, visual_qa
import studio_agent_router


def test_persisted_legacy_media_routes_migrate_with_versioned_audit(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path)
    session = store.create_session(
        user_id="policy-user",
        model="claude-sonnet-5",
        image_model="ernie_image",
        video_model="seedance",
    )
    path = tmp_path / f"{session['session_id']}.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw.update({
        "model": "grok-4.5",
        "image_model": "grok_imagine",
        "video_model": "grok_imagine_video",
        "provider_policy_version": "legacy",
        "active_jobs": [{
            "job_id": "job-legacy",
            "kind": "shortform",
            "image_model_id": "imagen4_preview",
            "video_model": "veo3_fast",
            "voice_provider": "xai",
            "stt_provider": "xai",
            "visual_qa_provider": "openrouter",
        }],
    })
    path.write_text(json.dumps(raw), encoding="utf-8")

    migrated = store.get_session(
        session["session_id"],
        reconcile_jobs=False,
        _prune_active_jobs=False,
    )
    assert migrated is not None
    assert migrated["model"] == "claude-sonnet-5"
    assert migrated["image_model"] == "seedream_edit"
    assert migrated["video_model"] == "seedance"
    job = migrated["active_jobs"][0]
    assert job["image_model_id"] == "seedream_edit"
    assert job["video_model"] == "seedance"
    assert job["voice_provider"] == "fal_minimax"
    assert job["stt_provider"] == "fal"
    assert job["visual_qa_provider"] == "anthropic"
    assert migrated["provider_policy_version"] == provider_policy.POLICY_VERSION
    audit = migrated["provider_policy_migrations"]
    assert {row["field_path"] for row in audit} >= {
        "model", "image_model", "video_model",
        "active_jobs[0].image_model_id", "active_jobs[0].video_model",
        "active_jobs[0].voice_provider", "active_jobs[0].stt_provider",
        "active_jobs[0].visual_qa_provider",
    }
    assert all(row["policy_version"] == provider_policy.POLICY_VERSION for row in audit)


def test_new_legacy_media_selection_is_rejected_not_silently_migrated(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path)
    session = store.create_session(user_id="policy-user", model="claude-sonnet-5")
    with pytest.raises(provider_policy.ProviderPolicyDenied):
        store.update_session(session["session_id"], image_model="grok_imagine")
    with pytest.raises(provider_policy.ProviderPolicyDenied):
        store.update_session(session["session_id"], video_model="veo3_fast")


def test_cost_optimizer_never_recommends_denied_media_routes() -> None:
    result = cost_optimizer.optimize_shortform(
        scene_count=6,
        duration_seconds=30,
        image_model_id="grok_imagine",
        video_model="grok_imagine_video",
    )

    rows = [result["active_route"], result["recommended_route"], *result["quality_cost_options"]]
    assert result["image_model_id"] == provider_policy.DEFAULT_FAL_IMAGE_MODEL
    assert result["video_model"] == provider_policy.DEFAULT_FAL_VIDEO_MODEL
    assert all(not provider_policy.is_denied_image_model(row["image_model_id"]) for row in rows)
    assert all(
        row["video_model"] == "none"
        or not provider_policy.is_denied_video_model(row["video_model"])
        for row in rows
    )


def test_models_endpoint_has_no_ghost_rows_without_provider_keys(monkeypatch) -> None:
    for name in ("ANTHROPIC_API_KEY", "FAL_KEY", "FAL_AI_KEY", "XAI_API_KEY", "OPENROUTER_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    studio_agent_router._MODELS_CACHE.clear()
    studio_agent_router._MODELS_CACHE.update({"at": 0.0, "payload": None})
    app = FastAPI()
    app.include_router(studio_agent_router.build_studio_agent_router(
        require_auth=lambda: {"id": "owner"},
        is_admin_check=lambda _user: True,
    ))

    response = TestClient(app).get("/api/studio-agent/models")

    assert response.status_code == 200
    payload = response.json()
    assert payload["models"] == []
    assert payload["image_models"] == []
    assert payload["video_models"] == []
    assert len(payload["setup_reasons"]) == 2


def test_reference_orders_ignore_stale_denied_provider_configuration(monkeypatch) -> None:
    monkeypatch.setenv("STUDIO_REFERENCE_VISION_PROVIDER_ORDER", "openrouter,google,xai,fal")
    monkeypatch.setenv("STUDIO_REFERENCE_ANALYSIS_PROVIDER_ORDER", "openrouter")
    monkeypatch.setenv("STUDIO_REFERENCE_STT_PROVIDER_ORDER", "xai")
    assert reference_providers.vision_provider_order() == ["anthropic"]
    assert reference_providers.analysis_provider_order() == ["anthropic"]
    assert reference_providers.stt_provider_order() == ["fal"]
    with pytest.raises(provider_policy.ProviderPolicyDenied):
        reference_providers.reference_openrouter_vision_model()
    with pytest.raises(provider_policy.ProviderPolicyDenied):
        reference_providers.reference_fal_analysis_model()


@pytest.mark.parametrize("provider", ["anthropic_direct", "anthropic_fallback", "anthropic"])
def test_direct_anthropic_usage_never_uses_openrouter_billing(provider: str, monkeypatch) -> None:
    monkeypatch.delenv("ANTHROPIC_PROMPT_USD_PER_M", raising=False)
    monkeypatch.delenv("ANTHROPIC_COMPLETION_USD_PER_M", raising=False)
    monkeypatch.delenv("ANTHROPIC_FALLBACK_PROMPT_USD_PER_M", raising=False)
    monkeypatch.delenv("ANTHROPIC_FALLBACK_COMPLETION_USD_PER_M", raising=False)
    monkeypatch.setattr(runner, "_sonnet_5_intro_pricing_active", lambda: True)

    prompt, completion, reason, model = runner._llm_pricing_for_provider(
        provider,
        "claude-sonnet-5",
        "claude-sonnet-5",
    )

    assert (prompt, completion) == (2.0, 10.0)
    assert reason == "studio_agent_anthropic_direct"
    assert model == "claude-sonnet-5"


def test_sonnet_five_metering_rolls_to_standard_price_after_intro(monkeypatch) -> None:
    monkeypatch.delenv("ANTHROPIC_PROMPT_USD_PER_M", raising=False)
    monkeypatch.delenv("ANTHROPIC_COMPLETION_USD_PER_M", raising=False)
    monkeypatch.setattr(runner, "_sonnet_5_intro_pricing_active", lambda: False)

    prompt, completion, reason, model = runner._llm_pricing_for_provider(
        "anthropic_direct",
        "claude-sonnet-5",
        "claude-sonnet-5",
    )

    assert (prompt, completion) == (3.0, 15.0)
    assert reason == "studio_agent_anthropic_direct"
    assert model == "claude-sonnet-5"


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


def test_scene_fingerprint_changes_when_same_size_and_mtime_content_changes(tmp_path) -> None:
    workspace = tmp_path
    still = workspace / "stills" / "b00.png"
    still.parent.mkdir(parents=True)
    still.write_bytes(b"AAAA")
    original_stat = still.stat()
    scene = {
        "index": 0,
        "sid": "b00",
        "still_rel": "stills/b00.png",
        "narration": "A letter is opened at the table.",
        "scene_action": "The host opens the sealed letter at a kitchen table.",
    }
    first = visual_qa.scene_visual_qa_fingerprint(workspace, scene)
    still.write_bytes(b"BBBB")
    os.utime(still, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
    second = visual_qa.scene_visual_qa_fingerprint(workspace, scene)
    assert first != second


@pytest.mark.parametrize(
    ("style", "required_phrase"),
    [
        ("skeleton_host", "clear glass skin"),
        ("cinematic", "foreground/midground/background"),
        ("ultra_realism", "physically credible skin"),
        ("historical_18th_century", "anachronism"),
    ],
)
def test_generic_still_applies_explicit_launch_style_rubric(
    tmp_path,
    monkeypatch,
    style: str,
    required_phrase: str,
) -> None:
    still = tmp_path / "stills" / f"{style}.png"
    still.parent.mkdir(parents=True, exist_ok=True)
    still.write_bytes(b"candidate")
    prompts: list[str] = []

    def fake_jpeg(_source: Path, target: Path) -> Path:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"jpeg")
        return target

    def fake_vision(_paths: list[str], *, prompt: str) -> dict:
        prompts.append(prompt)
        return {
            "provider": "anthropic",
            "model": "claude-sonnet-5",
            "parsed": {
                "pass": True,
                "style_match": True,
                "confidence": 0.99,
                "summary": "current asset passes",
            },
        }

    monkeypatch.setattr(visual_qa, "_qa_jpeg", fake_jpeg)
    monkeypatch.setattr(visual_qa, "_run_semantic_vision", fake_vision)
    report = visual_qa.audit_generic_still(
        still,
        scene_contract="The subject opens a letter in the requested room.",
        render_style=style,
    )
    assert report["status"] == "pass"
    assert report["pass"] is True
    assert report["render_style"] == style
    assert required_phrase.lower() in prompts[0].lower()


def test_generic_clip_requires_silence_and_motion_brief(tmp_path, monkeypatch) -> None:
    clip = tmp_path / "clips" / "b00.mp4"
    clip.parent.mkdir(parents=True)
    clip.write_bytes(b"video")
    frames = []
    frame_dir = clip.parent / ".vq_frames_b00"
    frame_dir.mkdir()
    for index in range(9):
        frame = frame_dir / f"f{index:02d}.jpg"
        frame.write_bytes(b"frame")
        frames.append(frame)
    prompts: list[str] = []
    requested_frame_counts: list[int] = []

    monkeypatch.setattr(
        visual_qa,
        "_audio_silence_report",
        lambda _path: {"status": "pass", "pass": True, "summary": "silent"},
    )
    def fake_extract(_path: Path, *, count: int) -> list[Path]:
        requested_frame_counts.append(count)
        return list(frames)

    monkeypatch.setattr(visual_qa, "_extract_clip_frames", fake_extract)

    def fake_vision(_paths: list[str], *, prompt: str) -> dict:
        prompts.append(prompt)
        return {
            "provider": "anthropic",
            "parsed": {
                "pass": True,
                "confidence": 0.95,
                "motion_brief_satisfied": True,
                "violations": [],
                "summary": "motion is visible",
            },
        }

    monkeypatch.setattr(visual_qa, "_run_semantic_vision", fake_vision)
    report = visual_qa.audit_generic_clip(
        clip,
        scene_contract="The host reacts to the letter.",
        motion_brief="A two-step recoil followed by a slow camera push.",
        render_style="cinematic",
    )
    assert report["status"] == "pass"
    assert report["audio_silence"]["pass"] is True
    assert report["frames_reviewed"] == 9
    assert requested_frame_counts == [9]
    assert "two-step recoil" in prompts[0]


def test_generic_clip_blocks_audio_or_unavailable_semantic_qa(tmp_path, monkeypatch) -> None:
    clip = tmp_path / "clips" / "b00.mp4"
    clip.parent.mkdir(parents=True)
    clip.write_bytes(b"video")
    monkeypatch.setattr(
        visual_qa,
        "_audio_silence_report",
        lambda _path: {"status": "fail", "pass": False, "summary": "audio stream present"},
    )
    report = visual_qa.audit_generic_clip(clip, scene_contract="Move slowly")
    assert report["status"] == "fail"
    assert report["pass"] is False
    assert report["violations"] == ["audio_not_silent"]
    assert visual_qa.should_block_publish({"status": "warn", "summary": "QA unavailable"}) == (
        True,
        "QA unavailable",
    )


def test_semantic_qa_calls_direct_anthropic_only(monkeypatch) -> None:
    monkeypatch.setattr(
        competitor,
        "_summarize_keyframe_visuals_anthropic",
        lambda _paths, *, prompt_text: {
            "summary": '{"pass":true,"confidence":1.0}',
            "provider": "anthropic",
        },
    )
    monkeypatch.setattr(
        competitor,
        "_summarize_keyframe_visuals_fal",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("FAL semantic QA called")),
    )
    monkeypatch.setattr(
        competitor,
        "_summarize_keyframe_visuals_openrouter",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("OpenRouter QA called")),
    )
    result = visual_qa._run_semantic_vision(["frame.jpg"], prompt="judge")
    assert result["provider"] == "anthropic"
    assert result["parsed"]["pass"] is True
