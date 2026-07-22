from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

import long_form_router
import skeleton_ai_router
from long_form import pipeline as longform_pipeline
from studio_agent import jobs as agent_jobs
from studio_agent import openrouter, runpod_bridge, tools


def _client() -> TestClient:
    def require_auth():
        return {"id": "user-1", "email": "owner@example.com"}

    app = FastAPI()
    app.include_router(skeleton_ai_router.build_skeleton_ai_router(require_auth=require_auth))
    app.include_router(
        long_form_router.build_long_form_router(
            require_auth=require_auth,
            is_admin_check=lambda _user: True,
        )
    )
    return TestClient(app)


def _longform_body() -> dict:
    return {
        "channel_key": "history_rewind",
        "outline": {
            "title": "A Test Documentary",
            "topic": "A test topic",
            "chapters": [{"title": "Chapter 1", "beats": ["Beat one"]}],
        },
        "image_model": "seedream_edit",
    }


def _mock_owned_longform_job(monkeypatch, owner_id: str = "user-1") -> None:
    monkeypatch.setattr(
        agent_jobs,
        "job_access_metadata",
        lambda job_id, kind="": {
            "exists": True,
            "job_id": job_id,
            "kind": "longform",
            "owner_id": owner_id,
        },
    )


def test_runpod_short_generate_uses_logged_contract_once(monkeypatch) -> None:
    calls: list[tuple[str, dict, dict]] = []
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: True)
    monkeypatch.setattr(
        skeleton_ai_router,
        "run_pipeline",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not execute locally")),
    )

    def execute(name, arguments, **context):
        calls.append((name, dict(arguments), dict(context)))
        return json.dumps({"ok": True, "status": "accepted", "job_id": "sf_123", "runpod_job_id": "rp-1"})

    monkeypatch.setattr(tools, "execute_tool_logged", execute)
    response = _client().post(
        "/api/skeleton-ai/generate",
        headers={"X-Idempotency-Key": "short-click-1"},
        json={
            "category": "people_blogs",
            "topic": "Test",
            "script_override": "A complete test script.",
            "video_model": "seedance",
            "image_model": "seedream_edit",
            "reference_image": "https://example.test/reference.png",
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["job_id"] == "sf_123"
    assert len(calls) == 1
    name, arguments, context = calls[0]
    assert name == "start_shortform_generate"
    assert arguments["_runpod_command_id"] == "short-click-1"
    assert arguments["reference_image"] == "https://example.test/reference.png"
    assert arguments["visual_proof_only"] is True
    assert context == {"user_id": "user-1", "content_format": "short"}


def test_runpod_short_generate_requires_key_without_local_fallback(monkeypatch) -> None:
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: True)
    monkeypatch.setattr(
        tools,
        "execute_tool_logged",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must reject before dispatch")),
    )
    monkeypatch.setattr(
        skeleton_ai_router,
        "run_pipeline",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not execute locally")),
    )

    response = _client().post(
        "/api/skeleton-ai/generate",
        json={"category": "people_blogs", "topic": "Test", "video_model": "seedance"},
    )

    assert response.status_code == 400
    assert "X-Idempotency-Key is required" in response.text


def test_uncovered_skeleton_scenes_fails_closed_when_enabled(monkeypatch) -> None:
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: True)
    monkeypatch.setattr(
        skeleton_ai_router,
        "GrokClient",
        lambda: (_ for _ in ()).throw(AssertionError("uncovered route must not start local work")),
    )

    response = _client().post(
        "/api/skeleton-ai/scenes",
        headers={"X-Idempotency-Key": "scenes-click-1"},
        json={"script": "One. Two.", "reference_image": "https://example.test/ref.png"},
    )

    assert response.status_code == 503
    assert "no idempotent RunPod parity" in response.text


def test_flag_off_short_generate_preserves_local_pipeline(monkeypatch, tmp_path: Path) -> None:
    local_calls: list[dict] = []
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: False)
    monkeypatch.setattr(skeleton_ai_router, "OUTPUT_ROOT", tmp_path)
    monkeypatch.setattr(
        tools,
        "execute_tool_logged",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("flag-off must stay local")),
    )

    def local_pipeline(**kwargs):
        local_calls.append(dict(kwargs))
        return {"video_path": "/local/video.mp4", "status": "complete"}

    monkeypatch.setattr(skeleton_ai_router, "run_pipeline", local_pipeline)
    response = _client().post(
        "/api/skeleton-ai/generate",
        json={
            "category": "people_blogs",
            "topic": "Local test",
            "script_override": "Local script.",
            "video_model": "seedance",
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["video_path"] == "/local/video.mp4"
    assert len(local_calls) == 1


def test_global_flag_alone_fails_longform_closed(monkeypatch) -> None:
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: True)
    monkeypatch.delenv("STUDIO_RUNPOD_LONGFORM_ENABLED", raising=False)
    monkeypatch.setattr(
        tools,
        "execute_tool_logged",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("separate gate must block")),
    )
    monkeypatch.setattr(
        longform_pipeline,
        "start_render",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not execute locally")),
    )

    response = _client().post(
        "/api/long-form/render-start",
        headers={"X-Idempotency-Key": "long-click-1"},
        json=_longform_body(),
    )

    assert response.status_code == 503
    assert "STUDIO_RUNPOD_LONGFORM_ENABLED" in response.text


def test_enabled_longform_start_uses_logged_contract_once(monkeypatch) -> None:
    calls: list[tuple[str, dict, dict]] = []
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: True)
    monkeypatch.setenv("STUDIO_RUNPOD_LONGFORM_ENABLED", "1")
    monkeypatch.setattr(
        longform_pipeline,
        "start_render",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not execute locally")),
    )

    def execute(name, arguments, **context):
        calls.append((name, dict(arguments), dict(context)))
        return json.dumps({"ok": True, "status": "accepted", "job_id": "lf_123", "runpod_job_id": "rp-lf-1"})

    monkeypatch.setattr(tools, "execute_tool_logged", execute)
    response = _client().post(
        "/api/long-form/render-start",
        headers={"X-Idempotency-Key": "long-click-1"},
        json=_longform_body(),
    )

    assert response.status_code == 200, response.text
    assert response.json()["job_id"] == "lf_123"
    assert len(calls) == 1
    name, arguments, context = calls[0]
    assert name == "start_longform_render"
    assert arguments["_runpod_command_id"] == "long-click-1"
    assert json.loads(arguments["chapters_json"])["title"] == "A Test Documentary"
    assert context == {"user_id": "user-1", "content_format": "long"}


def test_flag_off_longform_mutations_still_use_logged_credit_contract(
    monkeypatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, dict]] = []
    _mock_owned_longform_job(monkeypatch)
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: False)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("billable long-form route bypassed execute_tool_logged")

    monkeypatch.setattr(longform_pipeline, "start_render", forbidden)
    monkeypatch.setattr(longform_pipeline, "regenerate_still", forbidden)
    monkeypatch.setattr(longform_pipeline, "start_finalize", forbidden)
    regenerated_still = tmp_path / "scene_0001.png"
    regenerated_still.write_bytes(b"verified regenerated still")
    monkeypatch.setattr(
        longform_pipeline,
        "job_still_path",
        lambda _job_id, _scene_idx: regenerated_still,
    )

    def execute(name, arguments, **_context):
        calls.append((name, dict(arguments)))
        job_id = arguments.get("job_id") or "lf_local"
        if name == "regenerate_longform_still":
            return json.dumps({
                "ok": True,
                "job_id": job_id,
                "postcondition_verified": True,
            })
        if name == "finalize_longform_render":
            return json.dumps({
                "status": "running",
                "accepted": True,
                "complete": False,
                "job_id": job_id,
            })
        return json.dumps({"ok": True, "job_id": job_id})

    monkeypatch.setattr(tools, "execute_tool_logged", execute)
    client = _client()
    start = client.post(
        "/api/long-form/render-start",
        headers={"X-Idempotency-Key": "local-start-1"},
        json=_longform_body(),
    )
    regenerate = client.post(
        "/api/long-form/jobs/lf_local/regenerate-scene",
        headers={"X-Idempotency-Key": "local-regen-1"},
        json={"scene_idx": 1, "new_prompt": "Fix it"},
    )
    finalize = client.post(
        "/api/long-form/jobs/lf_local/finalize",
        headers={"X-Idempotency-Key": "local-finalize-1"},
    )

    assert [start.status_code, regenerate.status_code, finalize.status_code] == [200, 200, 200]
    assert [row[0] for row in calls] == [
        "start_longform_render",
        "regenerate_longform_still",
        "finalize_longform_render",
    ]
    assert [row[1]["_runpod_command_id"] for row in calls] == [
        "local-start-1",
        "local-regen-1",
        "local-finalize-1",
    ]


def test_enabled_longform_scene_and_finalize_use_logged_tools(monkeypatch) -> None:
    calls: list[tuple[str, dict]] = []
    _mock_owned_longform_job(monkeypatch)
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: True)
    monkeypatch.setenv("STUDIO_RUNPOD_LONGFORM_ENABLED", "true")

    def execute(name, arguments, **_context):
        calls.append((name, dict(arguments)))
        return json.dumps({"ok": True, "status": "accepted", "runpod_job_id": f"rp-{name}"})

    monkeypatch.setattr(tools, "execute_tool_logged", execute)
    client = _client()
    regenerate = client.post(
        "/api/long-form/jobs/lf_123/regenerate-scene",
        headers={"X-Idempotency-Key": "regen-1"},
        json={"scene_idx": 2, "new_prompt": "More dramatic light"},
    )
    finalize = client.post(
        "/api/long-form/jobs/lf_123/finalize",
        headers={"X-Idempotency-Key": "finalize-lf-123"},
    )

    assert regenerate.status_code == 200, regenerate.text
    assert finalize.status_code == 200, finalize.text
    assert [row[0] for row in calls] == ["regenerate_longform_still", "finalize_longform_render"]
    assert calls[0][1]["_runpod_command_id"] == "regen-1"
    assert calls[1][1]["_runpod_command_id"] == "finalize-lf-123"


def test_thumbnail_regeneration_uses_logged_local_credit_contract(
    monkeypatch,
) -> None:
    _mock_owned_longform_job(monkeypatch)
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: True)
    monkeypatch.setenv("STUDIO_RUNPOD_LONGFORM_ENABLED", "1")
    calls: list[tuple[str, dict, dict]] = []
    monkeypatch.setattr(
        longform_pipeline,
        "regenerate_thumbnail",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("route must not bypass execute_tool_logged")
        ),
    )

    def execute(name, arguments, **context):
        calls.append((name, dict(arguments), dict(context)))
        return json.dumps({
            "ok": True,
            "job_id": "lf_local",
            "idx": 1,
            "custom_prompt_used": True,
            "thumbnail_url": "/api/long-form/jobs/lf_local/thumbnail/1?v=123",
        })

    monkeypatch.setattr(tools, "execute_tool_logged", execute)

    response = _client().post(
        "/api/long-form/jobs/lf_local/regenerate-thumbnail/1",
        headers={"X-Idempotency-Key": "thumb-1"},
        json={"custom_prompt": "brighter"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["custom_prompt_used"] is True
    assert calls == [(
        "regenerate_longform_thumbnail",
        {
            "job_id": "lf_local",
            "idx": 1,
            "custom_prompt": "brighter",
            "_runpod_command_id": "thumb-1",
        },
        {"user_id": "user-1", "content_format": "long"},
    )]


def test_outline_and_chapter_expansion_are_priced_logged_and_idempotent(monkeypatch) -> None:
    calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(openrouter, "model_pricing", lambda _model: _async_value((3.0, 15.0)))

    def execute(name, arguments, **_context):
        calls.append((name, dict(arguments)))
        if name == "generate_longform_outline":
            return json.dumps({"outline": {"title": "Metered", "chapters": []}})
        return json.dumps({"beats": [{"beat_index": 0}]})

    monkeypatch.setattr(tools, "execute_tool_logged", execute)
    client = _client()
    outline = client.post(
        "/api/long-form/outline",
        headers={"X-Idempotency-Key": "outline-1"},
        json={"channel_key": "history_rewind", "topic": "Meter this", "model": "claude-sonnet-4-6"},
    )
    chapter = client.post(
        "/api/long-form/outline/expand-chapter",
        headers={"X-Idempotency-Key": "chapter-1"},
        json={
            "channel_key": "history_rewind",
            "outline_title": "Metered",
            "chapter": {"index": 0, "title": "One", "minutes": 1},
            "model": "claude-sonnet-4-6",
        },
    )

    assert outline.status_code == 200, outline.text
    assert chapter.status_code == 200, chapter.text
    assert [row[0] for row in calls] == ["generate_longform_outline", "expand_longform_chapter"]
    assert [row[1]["_runpod_command_id"] for row in calls] == ["outline-1", "chapter-1"]
    for _, arguments in calls:
        assert arguments["_billing_prompt_price_per_m"] == 3.0
        assert arguments["_billing_completion_price_per_m"] == 15.0
        assert arguments["_billing_input_chars"] > 0


async def _async_value(value):
    return value


def test_unpriced_outline_fails_before_tool_or_provider_execution(monkeypatch) -> None:
    monkeypatch.setattr(openrouter, "model_pricing", lambda _model: _async_value((None, None)))
    monkeypatch.setattr(
        tools,
        "execute_tool_logged",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("unpriced outline reached paid execution")
        ),
    )

    response = _client().post(
        "/api/long-form/outline",
        headers={"X-Idempotency-Key": "unpriced-outline-1"},
        json={
            "channel_key": "history_rewind",
            "topic": "Do not spend",
            "model": "unpriced/model",
        },
    )

    assert response.status_code == 503
    assert "no provider request was sent" in response.text


def test_runpod_owned_cancel_fails_closed_instead_of_claiming_local_cancel(monkeypatch) -> None:
    _mock_owned_longform_job(monkeypatch)
    monkeypatch.setattr(
        runpod_bridge,
        "get_dispatch_receipt_by_studio_job_id",
        lambda job_id: {"studio_job_id": job_id, "runpod_job_id": "rp-owned"},
    )
    monkeypatch.setattr(
        longform_pipeline,
        "cancel_render",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not cancel only local state")),
    )

    response = _client().post("/api/long-form/jobs/lf_owned/cancel")

    assert response.status_code == 409
    assert "cannot stop its remote spend" in response.text


def test_read_route_stays_local_without_idempotency_header(monkeypatch) -> None:
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: True)
    monkeypatch.setenv("STUDIO_RUNPOD_LONGFORM_ENABLED", "1")
    monkeypatch.setattr(
        tools,
        "execute_tool_logged",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("reads must not dispatch")),
    )

    response = _client().get("/api/long-form/channels")

    assert response.status_code == 200, response.text
    assert isinstance(response.json().get("channels"), list)


def test_frontend_production_calls_send_idempotency_headers() -> None:
    root = Path(__file__).resolve().parents[1] / "ViralShorts-App" / "src" / "studio"
    create_source = (root / "panels" / "CreatePanel.tsx").read_text(encoding="utf-8")
    long_source = (root / "panels" / "LongFormPanel.tsx").read_text(encoding="utf-8")

    assert create_source.count("'X-Idempotency-Key':") >= 3
    assert long_source.count("'X-Idempotency-Key':") >= 6
    assert "productionIdempotencyKey('longform-finalize', activeJobId)" in long_source
