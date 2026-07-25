from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import BackgroundTasks, Depends, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.testclient import TestClient

from routes import (
    build_generation_router,
    build_longform_creative_router,
    build_media_router,
)
from studio_agent import idempotent_mutations
from studio_agent.command_execution import FileExecutionLedger


USERS = {"user-a": {"id": "user-a", "email": "a@example.test"}}


async def require_test_auth(request: Request) -> dict:
    authorization = str(request.headers.get("authorization") or "").strip()
    token = (
        authorization.removeprefix("Bearer ").strip()
        if authorization.startswith("Bearer ")
        else ""
    )
    user = USERS.get(token)
    if not user:
        raise HTTPException(401, "Authentication required")
    return dict(user)


def _headers(key: str | None = None) -> dict[str, str]:
    headers = {"Authorization": "Bearer user-a"}
    if key:
        headers["X-Idempotency-Key"] = key
    return headers


@pytest.fixture(autouse=True)
def isolated_receipt_ledger(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(
        idempotent_mutations,
        "_LEDGER",
        FileExecutionLedger(tmp_path / "legacy-production-receipts"),
    )


def test_generate_requires_key_and_replays_without_second_enqueue() -> None:
    calls: list[dict] = []

    async def generate(
        payload: dict,
        request: Request,
        user: dict = Depends(require_test_auth),
    ):
        calls.append(dict(payload))
        return {"status": "accepted", "job_id": "job-generate-1"}

    app = FastAPI()
    app.include_router(
        build_generation_router(
            require_auth=require_test_auth,
            generate_short_endpoint=generate,
        )
    )
    client = TestClient(app)

    missing = client.post(
        "/api/generate",
        json={"prompt": "ship it"},
        headers=_headers(),
    )
    assert missing.status_code == 400
    assert calls == []

    first = client.post(
        "/api/generate",
        json={"prompt": "ship it"},
        headers=_headers("generate-command-1"),
    )
    replay = client.post(
        "/api/generate",
        json={"prompt": "ship it"},
        headers=_headers("generate-command-1"),
    )

    assert first.status_code == replay.status_code == 200
    assert first.json()["job_id"] == replay.json()["job_id"] == "job-generate-1"
    assert replay.json()["idempotent_replay"] is True
    assert calls == [{"prompt": "ship it"}]

    conflict = client.post(
        "/api/generate",
        json={"prompt": "different work"},
        headers=_headers("generate-command-1"),
    )
    assert conflict.status_code == 409
    assert calls == [{"prompt": "ship it"}]


def test_direct_adapter_rejects_an_unregistered_production_tool(monkeypatch) -> None:
    calls: list[dict] = []

    async def generate(
        payload: dict,
        request: Request,
        user: dict = Depends(require_test_auth),
    ):
        calls.append(dict(payload))
        return {"status": "accepted", "job_id": "must-not-run"}

    monkeypatch.setattr(idempotent_mutations, "LOCAL_IDEMPOTENT_TOOLS", set())
    app = FastAPI()
    app.include_router(
        build_generation_router(
            require_auth=require_test_auth,
            generate_short_endpoint=generate,
        )
    )

    response = TestClient(app).post(
        "/api/generate",
        json={"prompt": "must fail closed"},
        headers=_headers("unregistered-command"),
    )

    assert response.status_code == 409
    assert "not registered" in response.text
    assert calls == []


def _media_client(calls: dict[str, int]) -> TestClient:
    async def auto_scene(*_args, **_kwargs):
        return {"ok": True}

    async def auto_regenerate(body: dict, request: Request, *, user: dict):
        calls["regenerate"] += 1
        return {
            "ok": True,
            "job_id": str(body.get("job_id") or ""),
            "scene_index": int(body.get("scene_index") or 0),
        }

    async def status(*_args, **_kwargs):
        return {"status": "complete"}

    async def download(*_args, **_kwargs):
        return {"ok": True}

    async def chatstory(*_args, **_kwargs):
        calls["chatstory"] += 1
        return {"ok": True, "output_file": "chatstory.mp4"}

    async def clone(*_args, **_kwargs):
        calls["clone"] += 1
        return {"status": "accepted", "job_id": "clone-job-1"}

    async def jobs(*_args, **_kwargs):
        return {}

    app = FastAPI()
    app.include_router(
        build_media_router(
            require_auth=require_test_auth,
            auto_scene_image_handler=auto_scene,
            auto_regenerate_scene_image_handler=auto_regenerate,
            job_status_handler=status,
            download_video_handler=download,
            render_chat_story_handler=chatstory,
            clone_video_handler=clone,
            list_jobs_handler=jobs,
        )
    )
    return TestClient(app)


@pytest.mark.parametrize(
    ("path", "request_kwargs", "counter"),
    [
        (
            "/api/auto/regenerate-scene-image",
            {"json": {"job_id": "job-1", "scene_index": 0}},
            "regenerate",
        ),
        (
            "/api/chatstory/render",
            {"data": {"payload": '{"messages":[{"text":"hello"}]}'}},
            "chatstory",
        ),
        (
            "/api/clone",
            {"data": {"topic": "A new topic", "resolution": "720p"}},
            "clone",
        ),
    ],
)
def test_media_mutations_claim_before_handler_and_replay(
    path: str,
    request_kwargs: dict,
    counter: str,
) -> None:
    calls = {"regenerate": 0, "chatstory": 0, "clone": 0}
    client = _media_client(calls)

    missing = client.post(path, headers=_headers(), **request_kwargs)
    assert missing.status_code == 400
    assert calls[counter] == 0

    command_key = f"legacy-{counter}-command"
    first = client.post(path, headers=_headers(command_key), **request_kwargs)
    replay = client.post(path, headers=_headers(command_key), **request_kwargs)

    assert first.status_code == replay.status_code == 200
    assert replay.json()["idempotent_replay"] is True
    assert calls[counter] == 1


def test_legacy_clone_upload_identity_uses_content_bytes() -> None:
    calls = {"regenerate": 0, "chatstory": 0, "clone": 0}
    client = _media_client(calls)
    headers = _headers("legacy-clone-upload-command")

    missing = client.post(
        "/api/clone",
        headers=_headers(),
        data={"topic": "Upload-bound clone"},
        files={"file": ("source.mp4", b"same-length-a", "video/mp4")},
    )
    first = client.post(
        "/api/clone",
        headers=headers,
        data={"topic": "Upload-bound clone"},
        files={"file": ("source.mp4", b"same-length-a", "video/mp4")},
    )
    replay = client.post(
        "/api/clone",
        headers=headers,
        data={"topic": "Upload-bound clone"},
        files={"file": ("source.mp4", b"same-length-a", "video/mp4")},
    )
    conflict = client.post(
        "/api/clone",
        headers=headers,
        data={"topic": "Upload-bound clone"},
        files={"file": ("source.mp4", b"same-length-b", "video/mp4")},
    )

    assert missing.status_code == 400
    assert first.status_code == replay.status_code == 200
    assert replay.json()["idempotent_replay"] is True
    assert conflict.status_code == 409
    assert "different production mutation" in conflict.text
    assert calls["clone"] == 1


def test_all_legacy_longform_and_creative_mutations_use_the_claimed_endpoint() -> None:
    calls = 0
    longform_calls = 0
    longform_upload_calls = 0

    async def unused(request: Request):
        return {"ok": True}

    async def longform_create(request: Request):
        nonlocal longform_calls
        longform_calls += 1
        return {"ok": True, "session": {"session_id": "longform-session-1"}}

    async def unused_session(session_id: str, request: Request):
        return {"ok": True, "session_id": session_id}

    async def longform_reference_upload(
        session_id: str,
        request: Request,
        reference_image: UploadFile = File(...),
    ):
        nonlocal longform_upload_calls
        longform_upload_calls += 1
        return {"ok": True, "session_id": session_id, "filename": reference_image.filename}

    async def unused_scene(
        session_id: str,
        scene_index: int,
        request: Request,
    ):
        return {"ok": True, "session_id": session_id, "scene_index": scene_index}

    async def creative_session(body: dict, request: Request):
        nonlocal calls
        calls += 1
        return {"ok": True, "session_id": "creative-session-1"}

    router = build_longform_creative_router(
        require_auth=require_test_auth,
        create_longform_session_endpoint=longform_create,
        create_longform_session_bootstrap_endpoint=unused,
        longform_reference_image_endpoint=longform_reference_upload,
        longform_character_reference_endpoint=unused_session,
        longform_scene_assignment_endpoint=unused_session,
        longform_reference_file_endpoint=unused_session,
        longform_session_status_endpoint=unused_session,
        list_longform_sessions_endpoint=unused,
        longform_preview_file_endpoint=unused_session,
        longform_chapter_action_endpoint=unused_session,
        longform_resolve_error_endpoint=unused_session,
        longform_finalize_endpoint=unused_session,
        longform_stop_session_endpoint=unused_session,
        longform_ingest_outcome_endpoint=unused_session,
        longform_auto_ingest_outcome_endpoint=unused_session,
        creative_generate_script_endpoint=creative_session,
        creative_ingest_url_endpoint=creative_session,
        creative_create_session_endpoint=creative_session,
        creative_reference_image_endpoint=creative_session,
        creative_reference_file_endpoint=unused_session,
        creative_session_status_endpoint=unused_session,
        creative_session_scene_images_endpoint=unused_session,
        creative_scene_image_endpoint=creative_session,
        creative_scene_feedback_endpoint=creative_session,
        creative_update_scene_endpoint=unused_scene,
        creative_finalize_endpoint=creative_session,
    )
    expected = {
        "/api/longform/session",
        "/api/longform/session/bootstrap",
        "/api/longform/session/{session_id}/reference-image",
        "/api/longform/session/{session_id}/character-reference",
        "/api/longform/session/{session_id}/scene-assignment",
        "/api/longform/session/{session_id}/chapter-action",
        "/api/longform/session/{session_id}/resolve-error",
        "/api/longform/session/{session_id}/finalize",
        "/api/longform/session/{session_id}/stop",
        "/api/longform/session/{session_id}/outcome",
        "/api/longform/session/{session_id}/outcome/auto",
        "/api/creative/script",
        "/api/creative/ingest-url",
        "/api/creative/session",
        "/api/creative/reference-image",
        "/api/creative/scene-image",
        "/api/creative/scene-feedback",
        "/api/creative/scene/{session_id}/{scene_index}",
        "/api/creative/finalize",
    }
    claimed = {
        route.path
        for route in router.routes
        if getattr(route.endpoint, "__production_command_contract__", None)
    }
    assert claimed == expected

    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    payload = {"template": "story", "prompt": "Build this"}

    missing_longform = client.post(
        "/api/longform/session",
        headers=_headers(),
    )
    first_longform = client.post(
        "/api/longform/session",
        headers=_headers("legacy-longform-create-command"),
    )
    replay_longform = client.post(
        "/api/longform/session",
        headers=_headers("legacy-longform-create-command"),
    )
    assert missing_longform.status_code == 400
    assert first_longform.status_code == replay_longform.status_code == 200
    assert replay_longform.json()["idempotent_replay"] is True
    assert longform_calls == 1

    upload_headers = _headers("legacy-longform-reference-command")
    missing_upload = client.post(
        "/api/longform/session/longform-session-1/reference-image",
        headers=_headers(),
        files={"reference_image": ("reference.png", b"image-bytes-a", "image/png")},
    )
    first_upload = client.post(
        "/api/longform/session/longform-session-1/reference-image",
        headers=upload_headers,
        files={"reference_image": ("reference.png", b"image-bytes-a", "image/png")},
    )
    replay_upload = client.post(
        "/api/longform/session/longform-session-1/reference-image",
        headers=upload_headers,
        files={"reference_image": ("reference.png", b"image-bytes-a", "image/png")},
    )
    conflict_upload = client.post(
        "/api/longform/session/longform-session-1/reference-image",
        headers=upload_headers,
        files={"reference_image": ("reference.png", b"image-bytes-b", "image/png")},
    )
    assert missing_upload.status_code == 400
    assert first_upload.status_code == replay_upload.status_code == 200
    assert replay_upload.json()["idempotent_replay"] is True
    assert conflict_upload.status_code == 409
    assert "different production mutation" in conflict_upload.text
    assert longform_upload_calls == 1

    missing = client.post(
        "/api/creative/session",
        json=payload,
        headers=_headers(),
    )
    assert missing.status_code == 400
    assert calls == 0

    first = client.post(
        "/api/creative/session",
        json=payload,
        headers=_headers("creative-session-command"),
    )
    replay = client.post(
        "/api/creative/session",
        json=payload,
        headers=_headers("creative-session-command"),
    )
    assert first.status_code == replay.status_code == 200
    assert replay.json()["idempotent_replay"] is True
    assert calls == 1
