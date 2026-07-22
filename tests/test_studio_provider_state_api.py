from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from studio_agent import openrouter, provider_policy, store
import studio_agent_router


def _reset_model_cache() -> None:
    studio_agent_router._MODELS_CACHE.clear()
    studio_agent_router._MODELS_CACHE.update({"at": 0.0, "payload": None})


def _test_app() -> FastAPI:
    app = FastAPI()
    app.include_router(studio_agent_router.build_studio_agent_router(
        require_auth=lambda: {"id": "owner"},
        is_admin_check=lambda _user: True,
    ))
    return app


def test_persisted_legacy_routes_migrate_with_versioned_audit(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path)
    session = store.create_session(
        user_id="policy-user",
        model=provider_policy.DEFAULT_RUNNER_MODEL,
        image_model="ernie_image",
        video_model="seedance",
    )
    path = tmp_path / f"{session['session_id']}.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw.update({
        "model": "grok-4.5",
        "image_model": "openai/dall-e-3",
        "video_model": "google/veo3_fast",
        "provider_policy_version": "legacy",
        "active_jobs": [{
            "job_id": "job-legacy",
            "kind": "shortform",
            "image_model_id": "imagen4_preview",
            "video_model": "grok_imagine_video",
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
    assert migrated["model"] == provider_policy.DEFAULT_RUNNER_MODEL
    assert migrated["image_model"] == provider_policy.DEFAULT_FAL_IMAGE_MODEL
    assert migrated["video_model"] == provider_policy.DEFAULT_FAL_VIDEO_MODEL
    job = migrated["active_jobs"][0]
    assert job["image_model_id"] == provider_policy.DEFAULT_FAL_IMAGE_MODEL
    assert job["video_model"] == provider_policy.DEFAULT_FAL_VIDEO_MODEL
    assert job["voice_provider"] == provider_policy.DEFAULT_FAL_VOICE_PROVIDER
    assert job["stt_provider"] == "fal"
    assert job["visual_qa_provider"] == "anthropic"
    assert migrated["provider_policy_version"] == provider_policy.POLICY_VERSION
    audit = migrated["provider_policy_migrations"]
    assert {row["field_path"] for row in audit} >= {
        "model",
        "image_model",
        "video_model",
        "active_jobs[0].image_model_id",
        "active_jobs[0].video_model",
        "active_jobs[0].voice_provider",
        "active_jobs[0].stt_provider",
        "active_jobs[0].visual_qa_provider",
    }
    assert all(row["policy_version"] == provider_policy.POLICY_VERSION for row in audit)


def test_session_list_migrates_legacy_routes_before_building_sidebar_summary(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path)
    session = store.create_session(
        user_id="owner",
        model=provider_policy.DEFAULT_RUNNER_MODEL,
        image_model="ernie_image",
        video_model="seedance",
    )
    path = tmp_path / f"{session['session_id']}.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw.update({
        "model": "grok-4.5",
        "image_model": "grok_imagine_standard",
        "video_model": "grok_imagine_video",
        # Reproduce a partially migrated record: the version marker alone must
        # never allow a denied persisted route to crash a read-only endpoint.
        "provider_policy_version": provider_policy.POLICY_VERSION,
    })
    path.write_text(json.dumps(raw), encoding="utf-8")

    response = TestClient(_test_app()).get("/api/studio-agent/sessions")

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    [summary] = payload["sessions"]
    assert summary["session_id"] == session["session_id"]
    assert summary["model"] == provider_policy.DEFAULT_RUNNER_MODEL
    assert summary["image_model"] == provider_policy.DEFAULT_FAL_IMAGE_MODEL
    assert summary["video_model"] == provider_policy.DEFAULT_FAL_VIDEO_MODEL


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("model", "grok-4.5"),
        ("model", "openai/gpt-5"),
        ("model", "google/gemini-3"),
        ("image_model", "grok_imagine"),
        ("image_model", "imagen4_preview"),
        ("image_model", "openrouter/dall-e"),
        ("video_model", "grok_imagine_video"),
        ("video_model", "veo3_fast"),
        ("video_model", "openai/sora"),
    ],
)
def test_new_denied_route_selection_is_rejected(tmp_path, monkeypatch, field: str, value: str) -> None:
    monkeypatch.setattr(store, "SESSIONS_DIR", tmp_path)
    session = store.create_session(user_id="policy-user", model=provider_policy.DEFAULT_RUNNER_MODEL)

    with pytest.raises(provider_policy.ProviderPolicyDenied):
        store.update_session(session["session_id"], **{field: value})


def test_models_endpoint_has_no_ghost_rows_without_effective_keys(monkeypatch) -> None:
    monkeypatch.setattr(openrouter, "anthropic_api_key", lambda: "")
    for name in ("FAL_KEY", "FAL_AI_KEY", "XAI_API_KEY", "OPENROUTER_API_KEY", "OPENAI_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    _reset_model_cache()

    response = TestClient(_test_app()).get("/api/studio-agent/models")

    assert response.status_code == 200
    payload = response.json()
    assert payload["models"] == []
    assert payload["image_models"] == []
    assert payload["video_models"] == []
    assert payload["recommended"] == []
    assert payload["providers"] == []
    assert len(payload["setup_reasons"]) == 2
    assert payload["provider_policy"]["runner"] == ["anthropic"]
    assert payload["provider_policy"]["image"] == ["fal"]


def test_models_endpoint_filters_stale_provider_rows(monkeypatch) -> None:
    monkeypatch.setattr(openrouter, "anthropic_api_key", lambda: "anthropic-key")
    monkeypatch.setenv("FAL_KEY", "fal-key")

    async def fake_list_models():
        return [
            {"id": "claude-sonnet-5", "name": "Sonnet 5", "provider": "Anthropic", "recommended": True},
            {"id": "anthropic/claude-opus-4-8", "name": "Marketplace Claude", "provider": "Anthropic"},
            {"id": "grok-4.5", "name": "Grok", "provider": "xAI"},
            {"id": "openai/gpt-5", "name": "GPT", "provider": "OpenAI"},
        ]

    monkeypatch.setattr(openrouter, "list_models", fake_list_models)
    monkeypatch.setattr(openrouter, "build_model_catalog", lambda rows: list(rows or []))
    monkeypatch.setattr(studio_agent_router, "seedream_model_profiles", lambda **_kwargs: [
        {"id": "seedream_edit", "provider": "fal", "enabled": True},
        {"id": "grok_imagine", "provider": "fal", "enabled": True},
        {"id": "imagen4_preview", "provider": "google", "enabled": True},
    ])
    monkeypatch.setattr(studio_agent_router, "video_model_profiles", lambda **_kwargs: [
        {"id": "seedance", "provider": "fal", "enabled": True},
        {"id": "grok_imagine_video", "provider": "xAI", "enabled": True},
        {"id": "veo3_fast", "provider": "google", "enabled": True},
    ])
    _reset_model_cache()

    response = TestClient(_test_app()).get("/api/studio-agent/models")

    assert response.status_code == 200
    payload = response.json()
    assert [row["id"] for row in payload["models"]] == ["claude-sonnet-5"]
    assert [row["id"] for row in payload["image_models"]] == ["seedream_edit"]
    assert [row["id"] for row in payload["video_models"]] == ["seedance"]
    assert payload["providers"] == ["Anthropic"]


def test_false_or_unverifiable_scene_mutation_receipts_never_project_ok() -> None:
    with pytest.raises(HTTPException) as blocked:
        studio_agent_router._verified_mutation_payload(
            {"ok": False, "status": "visual_qa_failed"},
            operation="regenerate_production_scene",
        )
    assert blocked.value.status_code == 409
    assert blocked.value.detail["code"] == "mutation_postcondition_failed"

    with pytest.raises(HTTPException) as malformed:
        studio_agent_router._verified_mutation_payload(
            "not-json",
            operation="animate_production_scenes",
        )
    assert malformed.value.status_code == 502

    accepted = studio_agent_router._verified_mutation_payload(
        {"status": "running", "job_id": "job-1"},
        operation="animate_production_scenes",
    )
    assert accepted == {"status": "running", "job_id": "job-1"}


def test_frontend_dictation_and_pickers_have_no_provider_fallback_authority() -> None:
    root = Path(__file__).resolve().parents[1] / "ViralShorts-App" / "src" / "studio"
    dictation = (root / "hooks" / "useSpeechDictation.ts").read_text(encoding="utf-8")
    assert "new WebSocket" not in dictation
    assert "SpeechRecognition" not in dictation
    assert "/api/studio-agent/dictation" in dictation
    assert "provider !== 'fal'" in dictation

    picker_sources = "\n".join(
        (root / "panels" / name).read_text(encoding="utf-8")
        for name in ("AgentPanel.tsx", "CreatePanel.tsx", "LongFormPanel.tsx")
    )
    assert "FALLBACK_MODELS" not in picker_sources
    assert "FALLBACK_IMAGE_MODELS" not in picker_sources
    assert "grok_imagine_video'" not in picker_sources
