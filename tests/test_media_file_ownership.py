from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.testclient import TestClient

import backend
import skeleton_ai_router
from studio_agent import runpod_bridge


def _skeleton_client(user_id: str) -> TestClient:
    async def require_auth() -> dict[str, str]:
        return {"id": user_id, "email": f"{user_id}@example.com"}

    app = FastAPI()
    app.include_router(skeleton_ai_router.build_skeleton_ai_router(require_auth=require_auth))
    return TestClient(app)


def test_skeleton_reference_files_are_creator_scoped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reference_root = tmp_path / "_references"
    reference_root.mkdir()
    monkeypatch.setattr(skeleton_ai_router, "OUTPUT_ROOT", tmp_path)
    monkeypatch.setattr(skeleton_ai_router, "REFERENCE_ROOT", reference_root)
    filename = "creator-a_abcdef123456.png"
    (reference_root / filename).write_bytes(b"private image")

    assert _skeleton_client("creator-a").get(f"/api/skeleton-ai/references/{filename}").status_code == 200
    assert _skeleton_client("creator-b").get(f"/api/skeleton-ai/references/{filename}").status_code == 404


def test_skeleton_job_status_stills_and_mutations_require_job_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(skeleton_ai_router, "OUTPUT_ROOT", tmp_path)
    monkeypatch.setattr(runpod_bridge, "get_dispatch_receipt_by_studio_job_id", lambda _job_id: None)
    job_id = "ownedjob123"
    workspace = tmp_path / job_id
    stills = workspace / "stills"
    stills.mkdir(parents=True)
    (workspace / "job_spec.json").write_text(
        json.dumps({"job_id": job_id, "user_id": "creator-a"}),
        encoding="utf-8",
    )
    (workspace / "result.json").write_text(json.dumps({"status": "complete"}), encoding="utf-8")
    (stills / "b00.png").write_bytes(b"private still")

    owner = _skeleton_client("creator-a")
    stranger = _skeleton_client("creator-b")
    assert owner.get(f"/api/skeleton-ai/jobs/{job_id}").status_code == 200
    assert owner.get(f"/api/skeleton-ai/jobs/{job_id}/stills/b00.png").status_code == 200
    assert stranger.get(f"/api/skeleton-ai/jobs/{job_id}").status_code == 404
    assert stranger.get(f"/api/skeleton-ai/jobs/{job_id}/stills/b00.png").status_code == 404
    response = stranger.post(
        "/api/skeleton-ai/scenes/regenerate",
        json={"job_id": job_id, "beat_index": 0},
    )
    assert response.status_code == 404


def test_skeleton_job_owner_write_is_durable_and_preserves_spec_fields(tmp_path: Path) -> None:
    workspace = tmp_path / "job123"
    workspace.mkdir()
    (workspace / "job_spec.json").write_text(json.dumps({"topic": "Existing topic"}), encoding="utf-8")

    skeleton_ai_router._write_job_owner(workspace, "job123", {"id": "creator-a"})

    payload = json.loads((workspace / "job_spec.json").read_text(encoding="utf-8"))
    assert payload == {"topic": "Existing topic", "job_id": "job123", "user_id": "creator-a"}


def test_longform_reference_and_preview_files_require_session_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current_user = {"id": "creator-a"}

    async def authenticate(_request):
        return dict(current_user)

    reference_dir = tmp_path / "longform_references"
    preview_dir = tmp_path / "longform_previews"
    reference_dir.mkdir()
    preview_dir.mkdir()
    reference_name = "lf_1_1234_reference.png"
    preview_name = "lf_1_1234_c01_s001.png"
    (reference_dir / reference_name).write_bytes(b"reference")
    (preview_dir / preview_name).write_bytes(b"preview")
    sessions = {
        "lf_1_1234": {
            "session_id": "lf_1_1234",
            "user_id": "creator-a",
            "reference_image_path": str(reference_dir / reference_name),
            "reference_image_public_url": f"/api/longform/reference-file/{reference_name}",
            "chapters": [
                {"scenes": [{"image_url": f"/api/longform/preview/{preview_name}?v=1"}]}
            ],
        }
    }
    monkeypatch.setattr(backend, "get_current_user_from_request", authenticate)
    monkeypatch.setattr(backend, "TEMP_DIR", tmp_path)
    monkeypatch.setattr(backend, "LONGFORM_PREVIEW_DIR", preview_dir)
    monkeypatch.setattr(backend, "_longform_sessions", sessions)
    monkeypatch.setattr(backend, "_load_longform_sessions", lambda: None)

    response = asyncio.run(backend._longform_reference_file(reference_name, request=object()))
    assert isinstance(response, FileResponse)
    response = asyncio.run(backend._longform_preview_file(preview_name, request=object()))
    assert isinstance(response, FileResponse)

    current_user["id"] = "creator-b"
    for call in (
        backend._longform_reference_file(reference_name, request=object()),
        backend._longform_preview_file(preview_name, request=object()),
    ):
        with pytest.raises(HTTPException) as exc:
            asyncio.run(call)
        assert exc.value.status_code == 404


def test_creative_reference_file_requires_session_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current_user = {"id": "creator-a"}
    reference_dir = tmp_path / "creative_references"
    reference_dir.mkdir()
    filename = "creative_123_reference.webp"
    path = reference_dir / filename
    path.write_bytes(b"reference")

    async def authenticate(_request):
        return dict(current_user)

    async def creative_session(session_id: str):
        assert session_id == "creative_123"
        return {
            "session_id": session_id,
            "user_id": "creator-a",
            "reference_image_path": str(path),
            "reference_image_public_url": f"/api/creative/reference-file/{filename}",
        }

    monkeypatch.setattr(backend, "get_current_user_from_request", authenticate)
    monkeypatch.setattr(backend, "_get_creative_session", creative_session)
    monkeypatch.setattr(backend, "TEMP_DIR", tmp_path)

    response = asyncio.run(backend._creative_reference_file(filename, request=object()))
    assert isinstance(response, FileResponse)
    current_user["id"] = "creator-b"
    with pytest.raises(HTTPException) as exc:
        asyncio.run(backend._creative_reference_file(filename, request=object()))
    assert exc.value.status_code == 404


def test_provider_conditioning_uses_private_inline_reference_not_bearer_route() -> None:
    inline = "data:image/png;base64,cHJpdmF0ZQ=="
    private_route = "https://studio.example/api/longform/reference-file/private.png"
    session = {
        "reference_image_uploaded": True,
        "reference_image_url": inline,
        "reference_image_public_url": private_route,
        "reference_lock_mode": "strict",
        "character_references": [
            {
                "character_id": "character-1",
                "name": "Character",
                "reference_image_url": inline,
                "reference_image_public_url": private_route,
            }
        ],
    }

    assert backend._longform_session_subject_reference_image_url(session) == inline
    assert backend._longform_scene_reference_bundle(
        session,
        {"assigned_character_id": "character-1"},
    )["reference_image_url"] == inline
    assert backend._resolve_reference_for_scene(session, "story", 0) == inline
