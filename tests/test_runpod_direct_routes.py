from __future__ import annotations

import json

from fastapi import FastAPI
from fastapi.testclient import TestClient

import studio_agent_router
from studio_agent import jobs, runpod_bridge, tools


def _client() -> TestClient:
    user = {"id": "user-1"}

    def require_auth():
        return user

    app = FastAPI()
    app.include_router(
        studio_agent_router.build_studio_agent_router(
            require_auth=require_auth,
            is_admin_check=lambda _user: True,
        )
    )
    return TestClient(app)


def _snapshot(job_id: str, kind: str) -> dict:
    return {
        "job_id": job_id,
        "kind": kind,
        "status": "awaiting_approval",
        "title": "Test production",
    }


def test_enabled_direct_animate_dispatches_once_and_never_spawns_local(monkeypatch) -> None:
    calls: list[tuple[str, dict, dict]] = []

    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: True)
    monkeypatch.setattr(jobs, "get_job_snapshot", _snapshot)
    monkeypatch.setattr(
        tools,
        "spawn_animate_production_scenes",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("RunPod-enabled animate must not start a local worker")
        ),
    )

    def dispatch(name, arguments, **context):
        calls.append((name, dict(arguments), dict(context)))
        return json.dumps({"status": "queued", "runpod_job_id": "rp-1"})

    monkeypatch.setattr(tools, "execute_tool_logged", dispatch)

    response = _client().post(
        "/api/studio-agent/jobs/short-1/animate",
        headers={"X-Idempotency-Key": "click-animate-1"},
    )

    assert response.status_code == 200, response.text
    assert len(calls) == 1
    name, arguments, context = calls[0]
    assert name == "animate_production_scenes"
    assert arguments == {
        "job_id": "short-1",
        "_runpod_command_id": "click-animate-1",
    }
    assert context == {"user_id": "user-1", "content_format": "short"}


def test_enabled_direct_animate_without_idempotency_key_fails_closed(monkeypatch) -> None:
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: True)
    monkeypatch.setattr(
        tools,
        "execute_tool_logged",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("missing idempotency key must fail before dispatch")
        ),
    )
    monkeypatch.setattr(
        tools,
        "spawn_animate_production_scenes",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("missing idempotency key must never fall back locally")
        ),
    )

    response = _client().post("/api/studio-agent/jobs/short-1/animate")

    assert response.status_code == 400
    assert "X-Idempotency-Key is required" in response.text


def test_enabled_scene_approve_animate_sets_state_then_dispatches_once(monkeypatch) -> None:
    order: list[str] = []

    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: True)
    monkeypatch.setattr(jobs, "get_job_snapshot", _snapshot)

    def set_animate(job_id: str, animate: bool, scene_indices: list[int]):
        assert (job_id, animate, scene_indices) == ("short-1", True, [2])
        order.append("set_state")
        return json.dumps({"ok": True})

    def dispatch(name, arguments, **context):
        assert name == "animate_production_scenes"
        assert arguments == {
            "job_id": "short-1",
            "scene_indices": [2],
            "_runpod_command_id": "click-scene-2",
        }
        assert context == {"user_id": "user-1", "content_format": "short"}
        order.append("dispatch")
        return json.dumps({"status": "queued", "runpod_job_id": "rp-2"})

    monkeypatch.setattr(tools, "set_production_scenes_animate", set_animate)
    monkeypatch.setattr(tools, "execute_tool_logged", dispatch)
    monkeypatch.setattr(
        tools,
        "spawn_animate_production_scenes",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("RunPod-enabled approval must not start a local worker")
        ),
    )

    response = _client().post(
        "/api/studio-agent/jobs/short-1/scene/2/approval",
        headers={"X-Idempotency-Key": "click-scene-2"},
        json={"animate": True},
    )

    assert response.status_code == 200, response.text
    assert order == ["set_state", "dispatch"]


def test_flag_off_direct_animate_preserves_local_background_path(monkeypatch) -> None:
    local_calls: list[str] = []

    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: False)
    monkeypatch.setattr(jobs, "get_job_snapshot", _snapshot)
    monkeypatch.setattr(
        tools,
        "execute_tool_logged",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("flag-off animate must not dispatch to RunPod")
        ),
    )

    def spawn(job_id: str):
        local_calls.append(job_id)
        return json.dumps({"status": "running", "job_id": job_id})

    monkeypatch.setattr(tools, "spawn_animate_production_scenes", spawn)

    response = _client().post("/api/studio-agent/jobs/short-1/animate")

    assert response.status_code == 200, response.text
    assert local_calls == ["short-1"]


def test_enabled_short_scene_regenerate_dispatches_full_still_and_animation(monkeypatch) -> None:
    from long_form import pipeline as lf_pipeline

    calls: list[tuple[str, dict, dict]] = []
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: True)
    monkeypatch.setattr(lf_pipeline, "load_state", lambda _job_id: None)
    monkeypatch.setattr(jobs, "get_job_snapshot", _snapshot)
    monkeypatch.setattr(
        tools,
        "regenerate_production_scene",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("RunPod regenerate must not execute the local still-plus-animation path")
        ),
    )

    def dispatch(name, arguments, **context):
        calls.append((name, dict(arguments), dict(context)))
        return json.dumps({"status": "queued", "runpod_job_id": "rp-regen"})

    monkeypatch.setattr(tools, "execute_tool_logged", dispatch)

    response = _client().post(
        "/api/studio-agent/jobs/short-1/scene/2/regenerate",
        headers={"X-Idempotency-Key": "click-regen-2"},
    )

    assert response.status_code == 200, response.text
    assert calls == [
        (
            "regenerate_production_scene",
            {
                "job_id": "short-1",
                "scene_index": 2,
                "_runpod_command_id": "click-regen-2",
            },
            {"user_id": "user-1", "content_format": "short"},
        )
    ]


def test_enabled_short_finalize_dispatches_caption_preferences(monkeypatch) -> None:
    calls: list[tuple[str, dict, dict]] = []
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: True)
    monkeypatch.setattr(jobs, "get_job_snapshot", _snapshot)

    def dispatch(name, arguments, **context):
        calls.append((name, dict(arguments), dict(context)))
        return json.dumps({"status": "queued", "runpod_job_id": "rp-finalize"})

    monkeypatch.setattr(tools, "execute_tool_logged", dispatch)

    response = _client().post(
        "/api/studio-agent/jobs/short-1/finalize",
        params={"kind": "shortform", "captions_enabled": "false", "caption_mode": "off"},
        headers={"X-Idempotency-Key": "click-finalize-short"},
    )

    assert response.status_code == 200, response.text
    assert calls == [
        (
            "finalize_production",
            {
                "job_id": "short-1",
                "captions_enabled": False,
                "caption_mode": "off",
                "_runpod_command_id": "click-finalize-short",
            },
            {"user_id": "user-1", "content_format": "short"},
        )
    ]


def test_worker_execute_tool_forwards_short_finalize_caption_preferences(monkeypatch) -> None:
    calls: list[tuple[str, bool | None, str | None]] = []

    def finalize(job_id: str, *, captions_enabled=None, caption_mode=None):
        calls.append((job_id, captions_enabled, caption_mode))
        return json.dumps({"ok": True})

    monkeypatch.setattr(tools, "finalize_production", finalize)

    result = tools.execute_tool(
        "finalize_production",
        {"job_id": "short-1", "captions_enabled": False, "caption_mode": "off"},
        user_id="user-1",
        content_format="short",
    )

    assert json.loads(result) == {"ok": True}
    assert calls == [("short-1", False, "off")]


def test_enabled_longform_expand_dispatches_and_never_expands_locally(monkeypatch) -> None:
    from long_form import pipeline as lf_pipeline

    calls: list[tuple[str, dict, dict]] = []
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: True)
    monkeypatch.setattr(jobs, "get_job_snapshot", _snapshot)
    monkeypatch.setattr(
        lf_pipeline,
        "expand_visual_proof",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("RunPod long-form expand must not execute locally")
        ),
    )

    def dispatch(name, arguments, **context):
        calls.append((name, dict(arguments), dict(context)))
        return json.dumps({"status": "queued", "runpod_job_id": "rp-expand-long"})

    monkeypatch.setattr(tools, "execute_tool_logged", dispatch)

    response = _client().post(
        "/api/studio-agent/jobs/long-1/expand-proof",
        headers={"X-Idempotency-Key": "click-expand-long"},
    )

    assert response.status_code == 200, response.text
    assert calls == [
        (
            "expand_longform_visual_proof",
            {"job_id": "long-1", "_runpod_command_id": "click-expand-long"},
            {"user_id": "user-1", "content_format": "long"},
        )
    ]


def test_enabled_longform_finalize_dispatches_and_never_finalizes_locally(monkeypatch) -> None:
    calls: list[tuple[str, dict, dict]] = []
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: True)
    monkeypatch.setattr(jobs, "get_job_snapshot", _snapshot)
    monkeypatch.setattr(
        jobs,
        "finalize_longform_job",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("RunPod long-form finalize must not execute locally")
        ),
    )

    def dispatch(name, arguments, **context):
        calls.append((name, dict(arguments), dict(context)))
        return json.dumps({"status": "queued", "runpod_job_id": "rp-finalize-long"})

    monkeypatch.setattr(tools, "execute_tool_logged", dispatch)

    response = _client().post(
        "/api/studio-agent/jobs/long-1/finalize",
        params={"kind": "longform"},
        headers={"X-Idempotency-Key": "click-finalize-long"},
    )

    assert response.status_code == 200, response.text
    assert calls == [
        (
            "finalize_longform_render",
            {"job_id": "long-1", "_runpod_command_id": "click-finalize-long"},
            {"user_id": "user-1", "content_format": "long"},
        )
    ]


def test_runpod_owned_cancel_fails_closed_without_local_cancel(monkeypatch) -> None:
    local_calls: list[str] = []
    monkeypatch.setattr(
        runpod_bridge,
        "get_dispatch_receipt_by_studio_job_id",
        lambda _job_id: {
            "status": "accepted",
            "dispatch_id": "rpd_" + ("a" * 40),
            "runpod_job_id": "rp-active",
            "studio_job_id": "short-1",
        },
    )
    monkeypatch.setattr(
        tools,
        "cancel_shortform_job",
        lambda job_id: local_calls.append(job_id) or True,
    )

    response = _client().post("/api/studio-agent/jobs/short-1/cancel", params={"kind": "shortform"})

    assert response.status_code == 409, response.text
    assert local_calls == []
    detail = response.json()["detail"]
    assert detail["code"] == "runpod_remote_cancel_not_supported"
    assert "No local cancellation was issued" in detail["message"]
    assert "provider spend may continue" in detail["message"]
