from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

import long_form_router
import studio_agent_router
import zerotier_private_router
from long_form import pipeline as long_form_pipeline
from studio_agent import jobs as studio_agent_jobs
from studio_agent import render_styles


USERS = {
    "owner": {"id": "owner", "email": "owner@example.com"},
    "member": {"id": "member", "email": "member@example.com"},
}


async def require_test_auth(request: Request) -> dict:
    authorization = str(request.headers.get("authorization") or "").strip()
    token = authorization.removeprefix("Bearer ").strip() if authorization.startswith("Bearer ") else ""
    user = USERS.get(token)
    if not user:
        raise HTTPException(401, "Authentication required")
    return dict(user)


def auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture()
def long_form_media_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    mp4 = tmp_path / "final.mp4"
    thumbnail = tmp_path / "thumbnail.png"
    still = tmp_path / "still.png"
    mp4.write_bytes(b"video")
    thumbnail.write_bytes(b"thumbnail")
    still.write_bytes(b"still")
    monkeypatch.setattr(long_form_pipeline, "job_mp4_path", lambda _job_id: mp4)
    monkeypatch.setattr(long_form_pipeline, "job_thumbnail_path", lambda _job_id, _idx: thumbnail)
    monkeypatch.setattr(long_form_pipeline, "job_still_path", lambda _job_id, _idx: still)
    monkeypatch.setattr(
        studio_agent_jobs,
        "job_access_metadata",
        lambda job_id, kind: {
            "exists": job_id == "job_abc",
            "kind": kind,
            "owner_id": "owner",
        },
    )

    app = FastAPI()
    app.include_router(
        long_form_router.build_long_form_router(
            require_auth=require_test_auth,
            is_admin_check=lambda user: user.get("id") == "owner",
        )
    )
    return TestClient(app)


@pytest.mark.parametrize(
    "path",
    [
        "/api/long-form/jobs/job_abc/mp4",
        "/api/long-form/jobs/job_abc/thumbnail/1",
        "/api/long-form/jobs/job_abc/still/0",
    ],
)
def test_long_form_media_requires_owner_auth(long_form_media_client: TestClient, path: str) -> None:
    assert long_form_media_client.get(path).status_code == 401
    assert long_form_media_client.get(path, headers=auth("member")).status_code == 403
    assert long_form_media_client.get(path, headers=auth("owner")).status_code == 200


@pytest.fixture()
def zerotier_media_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    output_root = tmp_path / "zerotier"
    workspace = output_root / "job_abc"
    stills = workspace / "stills"
    stills.mkdir(parents=True)
    (stills / "scene_01.png").write_bytes(b"still")
    (workspace / "ZeroTier_job_abc.mp4").write_bytes(b"x" * 2048)
    monkeypatch.setattr(zerotier_private_router, "ZT_OUTPUT_ROOT", output_root)

    app = FastAPI()
    app.include_router(
        zerotier_private_router.build_zerotier_private_router(
            require_auth=require_test_auth,
            is_admin_user=lambda user: bool(user and user.get("id") == "owner"),
        )
    )
    return TestClient(app)


@pytest.mark.parametrize(
    "path",
    [
        "/api/zerotier-private/jobs/job_abc/stills/scene_01.png",
        "/api/zerotier-private/jobs/job_abc/mp4",
    ],
)
def test_zerotier_media_requires_owner_auth(zerotier_media_client: TestClient, path: str) -> None:
    assert zerotier_media_client.get(path).status_code == 401
    assert zerotier_media_client.get(path, headers=auth("member")).status_code == 403
    assert zerotier_media_client.get(path, headers=auth("owner")).status_code == 200


def test_style_preview_reads_are_authenticated_and_cache_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    preview_dir = tmp_path / "style-previews"
    video_dir = preview_dir / "video"
    preview_dir.mkdir()
    video_dir.mkdir()
    monkeypatch.setattr(render_styles, "STYLE_PREVIEW_DIR", preview_dir)
    monkeypatch.setattr(render_styles, "STYLE_PREVIEW_VIDEO_DIR", video_dir)

    generation_calls: list[str] = []

    def unexpected_still_generation(_key: str) -> Path:
        generation_calls.append("still")
        raise AssertionError("GET must not generate a still")

    def unexpected_video_generation(_key: str) -> Path:
        generation_calls.append("video")
        raise AssertionError("GET must not generate a video")

    monkeypatch.setattr(render_styles, "get_style_preview_path", unexpected_still_generation)
    monkeypatch.setattr(render_styles, "get_style_preview_video_path", unexpected_video_generation)

    app = FastAPI()
    app.include_router(
        studio_agent_router.build_studio_agent_router(
            require_auth=require_test_auth,
            is_admin_check=lambda user: user.get("id") == "owner",
        )
    )
    client = TestClient(app)

    still_url = "/api/studio-agent/style-preview/cinematic"
    video_url = "/api/studio-agent/style-preview/cinematic/video"
    assert client.get(still_url).status_code == 401
    assert client.get(video_url).status_code == 401
    assert client.get(still_url, headers=auth("owner")).status_code == 404
    assert client.get(video_url, headers=auth("owner")).status_code == 404
    assert generation_calls == []

    render_styles._style_preview_path("cinematic").write_bytes(b"p" * 2048)
    render_styles._style_preview_video_path("cinematic").write_bytes(b"v" * 2048)
    assert client.get(still_url, headers=auth("owner")).status_code == 200
    assert client.get(video_url, headers=auth("owner")).status_code == 200
    assert generation_calls == []


def test_style_catalog_only_advertises_cached_media(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    preview_dir = tmp_path / "style-previews"
    video_dir = preview_dir / "video"
    preview_dir.mkdir()
    video_dir.mkdir()
    monkeypatch.setattr(render_styles, "STYLE_PREVIEW_DIR", preview_dir)
    monkeypatch.setattr(render_styles, "STYLE_PREVIEW_VIDEO_DIR", video_dir)

    uncached = {item["key"]: item for item in render_styles.list_render_styles()}
    assert all("preview_url" not in item for item in uncached.values())
    assert all("preview_video_url" not in item for item in uncached.values())

    render_styles._style_preview_path("cinematic").write_bytes(b"p" * 2048)
    cached = {item["key"]: item for item in render_styles.list_render_styles()}
    assert cached["cinematic"]["preview_url"] == "/api/studio-agent/style-preview/cinematic"
    assert "preview_video_url" not in cached["cinematic"]
