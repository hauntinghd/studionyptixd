from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from studio_agent import jobs, tools
from studio_agent_router import build_studio_agent_router


def _shortform_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, owner_id: str) -> tuple[str, Path]:
    job_id = "ownedjob123"
    monkeypatch.setattr(jobs, "ROOT", tmp_path)
    monkeypatch.setattr(jobs, "SKELETON_OUTPUT", Path("shorts"))
    workspace = tmp_path / "shorts" / job_id
    workspace.mkdir(parents=True)
    (workspace / "job_spec.json").write_text(
        json.dumps({"job_id": job_id, "user_id": owner_id}),
        encoding="utf-8",
    )
    return job_id, workspace


def test_job_ids_cannot_be_paths() -> None:
    assert jobs.valid_job_id("job_123")
    assert not jobs.valid_job_id("..")
    assert not jobs.valid_job_id("../secret")
    assert not jobs.valid_job_id("job/secret")
    assert not jobs.valid_job_id("x" * 49)


def test_shortform_owner_is_loaded_from_durable_job_spec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id, _ = _shortform_workspace(tmp_path, monkeypatch, "creator-a")

    access = jobs.job_access_metadata(job_id, "shortform")

    assert access == {
        "exists": True,
        "job_id": job_id,
        "kind": "shortform",
        "owner_id": "creator-a",
    }


def test_tool_execution_rejects_cross_creator_job_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id, _ = _shortform_workspace(tmp_path, monkeypatch, "creator-a")

    tools._enforce_tool_job_ownership(
        "list_production_scenes", {"job_id": job_id}, "creator-a"
    )
    with pytest.raises(PermissionError, match="production job not found"):
        tools._enforce_tool_job_ownership(
            "list_production_scenes", {"job_id": job_id}, "creator-b"
        )


def test_http_job_poll_hides_cross_creator_job() -> None:
    app = FastAPI()

    async def require_auth() -> dict:
        return {"id": "creator-b", "email": "b@example.com"}

    app.include_router(
        build_studio_agent_router(
            require_auth=require_auth,
            lane_access_check=lambda _user: True,
        )
    )
    access = {
        "exists": True,
        "job_id": "ownedjob123",
        "kind": "shortform",
        "owner_id": "creator-a",
    }
    with (
        patch("studio_agent.jobs.job_access_metadata", return_value=access),
        patch("studio_agent.jobs.get_job_snapshot") as snapshot,
    ):
        response = TestClient(app).get(
            "/api/studio-agent/jobs/ownedjob123?kind=shortform"
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "job_not_found"
    snapshot.assert_not_called()


def test_controlled_beta_owner_cannot_export_final_job_media() -> None:
    app = FastAPI()

    async def require_auth() -> dict:
        return {"id": "creator-a", "email": "a@example.com"}

    app.include_router(
        build_studio_agent_router(
            require_auth=require_auth,
            lane_access_check=lambda _user: True,
            export_access_check=lambda _user: False,
        )
    )
    access = {
        "exists": True,
        "job_id": "ownedjob123",
        "kind": "shortform",
        "owner_id": "creator-a",
    }
    with (
        patch("studio_agent.jobs.job_access_metadata", return_value=access),
        patch("studio_agent.jobs.resolve_media_path") as resolve_media,
    ):
        response = TestClient(app).get(
            "/api/studio-agent/jobs/ownedjob123/media?kind=shortform"
        )

    assert response.status_code == 403
    assert response.json()["detail"] == (
        "Final video export is disabled for controlled-beta accounts."
    )
    resolve_media.assert_not_called()


def test_finalize_preflight_rejects_clip_path_escape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = "preflight123"
    monkeypatch.setattr(tools, "ROOT", tmp_path)
    monkeypatch.setattr(tools, "SKELETON_OUTPUT", Path("shorts"))
    workspace = tmp_path / "shorts" / job_id
    workspace.mkdir(parents=True)
    (workspace / "scenes.json").write_text(
        json.dumps([
            {
                "index": 0,
                "sid": "b00",
                "animate": True,
                "clip_rel": "../../outside.mp4",
            }
        ]),
        encoding="utf-8",
    )
    (tmp_path / "outside.mp4").write_bytes(b"not a valid in-workspace clip")

    blocker = tools.shortform_finalize_preflight(job_id)

    assert blocker["status"] == "awaiting_animation"
    assert blocker["pending_animated_scenes"] == [0]
