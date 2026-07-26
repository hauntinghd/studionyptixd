from __future__ import annotations

import json

from fastapi import FastAPI
from fastapi.testclient import TestClient

import long_form_router
from long_form import pipeline as longform_pipeline
from studio_agent import jobs as agent_jobs
from studio_agent import tools
from studio_agent.execution_context import current_production_command


def _client(user_id: str) -> TestClient:
    def require_auth():
        return {"id": user_id, "email": f"{user_id}@example.test"}

    app = FastAPI()
    app.include_router(
        long_form_router.build_long_form_router(
            require_auth=require_auth,
            is_admin_check=lambda _user: True,
        )
    )
    return TestClient(app)


def test_cross_user_cannot_mutate_owned_longform_job(monkeypatch) -> None:
    monkeypatch.setattr(
        agent_jobs,
        "job_access_metadata",
        lambda job_id, kind="": {
            "exists": True,
            "job_id": job_id,
            "kind": "longform",
            "owner_id": "creator-owner",
        },
    )

    def forbidden(*_args, **_kwargs):
        raise AssertionError("cross-user request reached a production mutation")

    monkeypatch.setattr(longform_pipeline, "regenerate_still", forbidden)
    monkeypatch.setattr(longform_pipeline, "start_finalize", forbidden)
    monkeypatch.setattr(longform_pipeline, "regenerate_thumbnail", forbidden)
    monkeypatch.setattr(longform_pipeline, "cancel_render", forbidden)

    client = _client("different-creator")
    requests = [
        ("/api/long-form/jobs/lf_owned/regenerate-scene", {"json": {"scene_idx": 0}}),
        ("/api/long-form/jobs/lf_owned/finalize", {}),
        ("/api/long-form/jobs/lf_owned/regenerate-thumbnail/1", {"json": {}}),
        ("/api/long-form/jobs/lf_owned/cancel", {}),
    ]
    for path, kwargs in requests:
        response = client.post(path, **kwargs)
        assert response.status_code == 404, (path, response.text)
        assert response.json()["detail"] == "job_not_found"


def test_render_start_stamps_authenticated_owner(monkeypatch) -> None:
    captured: dict = {}
    monkeypatch.setattr(tools, "_runpod_production_enabled", lambda: False)

    def execute(name, arguments, **context):
        authority = current_production_command()
        captured["name"] = name
        captured["arguments"] = dict(arguments)
        captured["context"] = dict(context)
        captured["command_id"] = authority.command_id if authority else ""
        return json.dumps({"job_id": "lf_owner_stamped"})

    monkeypatch.setattr(tools, "execute_tool_logged", execute)
    response = _client("creator-owner").post(
        "/api/long-form/render-start",
        headers={"X-Idempotency-Key": "owner-start-1"},
        json={
            "channel_key": "history_rewind",
            "outline": {
                "title": "Ownership test",
                "chapters": [{"title": "Chapter 1", "beats": ["Beat"]}],
                "user_id": "spoofed-user",
            },
            "image_model": "seedream_edit",
        },
    )

    assert response.status_code == 200, response.text
    assert captured["name"] == "start_longform_render"
    outline = json.loads(captured["arguments"]["chapters_json"])
    assert outline["user_id"] == "creator-owner"
    assert captured["command_id"] == "owner-start-1"
    assert captured["context"] == {
        "user_id": "creator-owner",
        "content_format": "long",
        "session_id": "direct_4fbe55289d2f4f8de71451052b2cea62",
    }
