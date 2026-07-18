from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.testclient import TestClient

from backend_job_payloads import build_job_status_payload, build_list_jobs_payload, job_access_allowed
from backend_media_handlers import (
    build_download_video_response,
    resolve_auto_scene_directory,
    validate_job_id_component,
)
from routes import (
    build_billing_router,
    build_generation_router,
    build_media_router,
    build_studio_utility_router,
)


ADMIN_EMAILS = {"owner@example.com"}
USERS = {
    "user-a": {"id": "user-a", "email": "a@example.com"},
    "user-b": {"id": "user-b", "email": "b@example.com"},
    "admin": {"id": "owner-id", "email": "owner@example.com"},
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


def test_retired_waitlist_route_cannot_create_a_checkout() -> None:
    calls = 0

    async def obsolete_waitlist_checkout(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return {"checkout_url": "https://example.invalid/obsolete"}

    async def unused(*_args, **_kwargs):
        return {"ok": True}

    app = FastAPI()
    app.include_router(
        build_billing_router(
            create_checkout_endpoint=unused,
            create_topup_checkout_endpoint=unused,
            paypal_return_endpoint=unused,
            paypal_webhook_endpoint=unused,
            paypal_verify_order_endpoint=unused,
            create_billing_portal_session_endpoint=unused,
            join_waitlist_endpoint=obsolete_waitlist_checkout,
            stripe_webhook_endpoint=unused,
            admin_set_plan_endpoint=unused,
            admin_cancel_subscription_endpoint=unused,
            admin_refund_credits_endpoint=unused,
            admin_grant_credits_endpoint=unused,
        )
    )

    response = TestClient(app).post(
        "/api/waitlist/join",
        json={"email": "stale@example.com", "plan": "creator", "provider": "stripe"},
    )

    assert response.status_code == 410
    assert calls == 0


def test_generate_auth_runs_before_handler() -> None:
    calls: list[dict] = []

    async def generate(payload: dict):
        calls.append(payload)
        return {"status": "accepted"}

    app = FastAPI()
    app.include_router(build_generation_router(require_auth=require_test_auth, generate_short_endpoint=generate))
    client = TestClient(app)

    response = client.post("/api/generate", json={"prompt": "private"})

    assert response.status_code == 401
    assert calls == []
    assert client.post("/api/generate", json={"prompt": "private"}, headers=auth("user-a")).status_code == 200
    assert calls == [{"prompt": "private"}]


def test_youtube_ideas_quota_route_requires_auth() -> None:
    calls = 0

    async def ideas():
        nonlocal calls
        calls += 1
        return {"ideas": []}

    async def queue_status():
        return {"waiting": 0}

    app = FastAPI()
    app.include_router(
        build_studio_utility_router(
            require_auth=require_test_auth,
            shorts_ideas_endpoint=ideas,
            queue_status_endpoint=queue_status,
        )
    )
    client = TestClient(app)

    assert client.get("/api/studio/shorts/ideas?q=test").status_code == 401
    assert calls == 0
    assert client.get("/api/studio/shorts/ideas?q=test", headers=auth("user-a")).status_code == 200
    assert calls == 1


@pytest.fixture()
def media_api(tmp_path: Path):
    output_dir = tmp_path / "output"
    scene_root = tmp_path / "scenes"
    output_dir.mkdir()
    scene_root.mkdir()

    jobs = {
        "job-a": {"status": "complete", "user_id": "user-a", "output_file": "a.mp4"},
        "job-b": {"status": "complete", "user_id": "user-b", "output_file": "b.mp4"},
        "legacy": {"status": "complete", "output_file": "legacy.mp4"},
        "auto-a": {"status": "review", "user_id": "user-a"},
    }
    persisted = {
        "1720000000_4321": {
            "status": "complete",
            "user_id": "user-a",
            "output_file": "skeleton_1720000000_4321.mp4",
        }
    }
    for name in ("a.mp4", "b.mp4", "legacy.mp4", "orphan.mp4", "skeleton_1720000000_4321.mp4"):
        (output_dir / name).write_bytes(name.encode("utf-8"))
    auto_dir = resolve_auto_scene_directory(scene_root, "auto-a", create=True)
    (auto_dir / "scene_1.png").write_bytes(b"png")

    async def get_persisted_job_state(job_id: str):
        return persisted.get(job_id)

    async def persist_job_state(job_id: str, state: dict):
        persisted[job_id] = dict(state)

    status_handler = build_job_status_payload(
        jobs_ref=jobs,
        prune_in_memory_jobs=lambda: None,
        get_persisted_job_state=get_persisted_job_state,
        record_kpi_for_job=lambda _job_id, _state: None,
        persist_job_state=persist_job_state,
        admin_emails=ADMIN_EMAILS,
    )
    list_handler = build_list_jobs_payload(jobs_ref=jobs, admin_emails=ADMIN_EMAILS)
    download_handler = build_download_video_response(
        output_dir=output_dir,
        jobs_ref=jobs,
        get_persisted_job_state=get_persisted_job_state,
        admin_emails=ADMIN_EMAILS,
    )

    async def auto_scene(job_id: str, filename: str, *, user: dict):
        try:
            normalized = validate_job_id_component(job_id)
        except ValueError:
            raise HTTPException(400, "Invalid job id")
        state = jobs.get(normalized) or persisted.get(normalized)
        if not isinstance(state, dict) or not job_access_allowed(state, user, ADMIN_EMAILS):
            raise HTTPException(404, "Image not found")
        if Path(filename).name != filename:
            raise HTTPException(400, "Invalid filename")
        path = resolve_auto_scene_directory(scene_root, normalized, create=False) / filename
        if not path.is_file():
            raise HTTPException(404, "Image not found")
        return FileResponse(path)

    async def auto_regenerate(_body: dict, _request: Request | None = None, *, user: dict):
        return {"ok": bool(user)}

    async def render_chat_story(*_args, **_kwargs):
        return {"ok": True}

    async def clone_video(*_args, **_kwargs):
        return {"ok": True}

    app = FastAPI()
    app.include_router(
        build_media_router(
            require_auth=require_test_auth,
            auto_scene_image_handler=auto_scene,
            auto_regenerate_scene_image_handler=auto_regenerate,
            job_status_handler=status_handler,
            download_video_handler=download_handler,
            render_chat_story_handler=render_chat_story,
            clone_video_handler=clone_video,
            list_jobs_handler=list_handler,
        )
    )
    return TestClient(app), jobs, scene_root


@pytest.mark.parametrize("path", ["/api/status/job-a", "/api/job/job-a", "/api/jobs", "/api/download/a.mp4"])
def test_legacy_media_routes_require_auth(media_api, path: str) -> None:
    client, _jobs, _scene_root = media_api
    assert client.get(path).status_code == 401


@pytest.mark.parametrize("path", ["/api/chatstory/render", "/api/clone"])
def test_upload_render_routes_authenticate_before_handlers(media_api, path: str) -> None:
    client, _jobs, _scene_root = media_api
    assert client.post(path).status_code == 401


@pytest.mark.parametrize("prefix", ["/api/status/", "/api/job/"])
def test_job_status_is_owner_scoped_without_existence_leak(media_api, prefix: str) -> None:
    client, _jobs, _scene_root = media_api

    assert client.get(f"{prefix}job-a", headers=auth("user-a")).status_code == 200
    denied = client.get(f"{prefix}job-a", headers=auth("user-b"))
    missing = client.get(f"{prefix}missing", headers=auth("user-b"))

    assert denied.status_code == missing.status_code == 404
    assert denied.json() == missing.json() == {"detail": "Job not found"}
    assert client.get(f"{prefix}job-a", headers=auth("admin")).status_code == 200
    assert client.get(f"{prefix}legacy", headers=auth("admin")).status_code == 200


def test_jobs_list_is_filtered_per_user_and_admin_can_audit(media_api) -> None:
    client, _jobs, _scene_root = media_api

    user_jobs = client.get("/api/jobs", headers=auth("user-a")).json()
    admin_jobs = client.get("/api/jobs", headers=auth("admin")).json()

    assert set(user_jobs) == {"job-a", "auto-a"}
    assert "output_file" not in user_jobs["job-a"]
    assert {"job-a", "job-b", "legacy", "auto-a"}.issubset(admin_jobs)


def test_download_is_owner_scoped_and_supports_persisted_jobs_and_admin(media_api) -> None:
    client, _jobs, _scene_root = media_api

    assert client.get("/api/download/a.mp4", headers=auth("user-a")).status_code == 200
    denied = client.get("/api/download/a.mp4", headers=auth("user-b"))
    missing = client.get("/api/download/missing.mp4", headers=auth("user-b"))
    assert denied.status_code == missing.status_code == 404
    assert denied.json() == missing.json() == {"detail": "Video not found"}
    assert client.get(
        "/api/download/skeleton_1720000000_4321.mp4",
        headers=auth("user-a"),
    ).status_code == 200
    assert client.get("/api/download/orphan.mp4", headers=auth("admin")).status_code == 200


def test_account_without_export_access_cannot_use_legacy_final_video_download(tmp_path: Path) -> None:
    output = tmp_path / "output"
    output.mkdir()
    (output / "beta.mp4").write_bytes(b"video")
    handler = build_download_video_response(
        output_dir=output,
        jobs_ref={
            "beta-job": {
                "job_id": "beta-job",
                "user_id": "user-a",
                "output_file": "beta.mp4",
            }
        },
        get_persisted_job_state=lambda _job_id: None,
        admin_emails=ADMIN_EMAILS,
        export_access_check=lambda _user: False,
    )
    app = FastAPI()

    @app.get("/download/{filename}")
    async def download(filename: str):
        return await handler(filename, user=USERS["user-a"])

    response = TestClient(app).get("/download/beta.mp4")

    assert response.status_code == 403
    assert response.json()["detail"] == (
        "Final video export is not available for this account."
    )


def test_auto_scene_image_requires_owner(media_api) -> None:
    client, _jobs, _scene_root = media_api

    assert client.get("/api/auto/scene-image/auto-a/scene_1.png").status_code == 401
    assert client.get(
        "/api/auto/scene-image/auto-a/scene_1.png",
        headers=auth("user-b"),
    ).status_code == 404
    assert client.get(
        "/api/auto/scene-image/auto-a/scene_1.png",
        headers=auth("user-a"),
    ).status_code == 200


@pytest.mark.parametrize(
    "path",
    [
        "/api/auto/scene-image/../scene_1.png",
        "/api/auto/scene-image/%2e%2e/scene_1.png",
        "/api/auto/scene-image/%252e%252e/scene_1.png",
    ],
)
def test_auto_scene_traversal_does_not_create_directories(media_api, path: str) -> None:
    client, _jobs, scene_root = media_api
    before = {entry.name for entry in scene_root.iterdir()}

    response = client.get(path, headers=auth("user-a"))

    assert response.status_code in {400, 404}
    assert {entry.name for entry in scene_root.iterdir()} == before
    assert not (scene_root / "%2e%2e").exists()


def test_auto_scene_read_resolver_never_mkdirs(tmp_path: Path) -> None:
    target = resolve_auto_scene_directory(tmp_path, "valid-job", create=False)
    assert target == tmp_path / "valid-job"
    assert not target.exists()
