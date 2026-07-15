from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient
from PIL import Image

import thumblab_router as thumb
from studio_agent.command_execution import FileExecutionLedger


def _app(
    tmp_path: Path,
    *,
    provider_fails: bool = False,
    commit_fails: bool = False,
    receipt_save_fails: bool = False,
    max_video_bytes: int = 1024,
):
    jobs: dict[str, dict] = {}
    events: list[str] = []

    async def require_auth(x_test_user: str | None = Header(None)):
        if not x_test_user:
            raise HTTPException(401, "Auth required")
        return {"id": x_test_user, "email": f"{x_test_user}@example.com"}

    async def persist(_job_id: str, _job: dict):
        return None

    async def llm(_system: str, _user: str, **_kwargs):
        return {
            "prompt": "A single cinematic subject with dramatic contrast and clear visual hierarchy " * 2,
            "negative_prompt": "clutter",
            "title_text": "Big Reveal",
            "style_notes": "mobile first",
        }

    async def vision(_system: str, _user: str, **_kwargs):
        return {"ctr_score": 80, "patterns": ["one subject"], "generation_directive": "high contrast"}

    async def render(_model: str, _prompt: str, output_path: str, **_kwargs):
        events.append("provider")
        if provider_fails:
            raise RuntimeError("provider unavailable")
        Image.new("RGB", (640, 360), (120, 10, 20)).save(output_path, "PNG")
        return {"provider": "seedream45", "provider_label": "Seedream 4.5"}

    async def gallery(*_args, **_kwargs):
        return [
            {
                "video_id": "abcDEF_1234",
                "title": "Example",
                "views": 123,
                "thumbnail_url": "https://i.ytimg.com/vi/abcDEF_1234/hqdefault.jpg",
                "channel_title": "Creator",
            }
        ]

    async def channels(user: dict, sync: bool = False):
        assert sync is False
        return {"channels": [{"channel_id": f"channel-{user['id']}", "title": "Mine"}]}

    def reserve(_user_id: str, _credits: int, **_kwargs):
        events.append("reserve")
        return {"reservation_id": "res_test"}

    def release(_user_id: str, _reservation_id: str, **_kwargs):
        events.append("release")
        return {}

    def commit(_user_id: str, _reservation_id: str, **_kwargs):
        events.append("commit")
        if commit_fails:
            raise RuntimeError("wallet unavailable")
        return {}

    async def probe(_path: Path):
        return 120.0

    async def extract(_source: Path, destination: Path, **_kwargs):
        destination.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (320, 180), (1, 2, 3)).save(destination, "JPEG")

    ledger = FileExecutionLedger(tmp_path / "ledger")
    if receipt_save_fails:
        ledger.save = lambda _receipt: (_ for _ in ()).throw(OSError("disk full"))  # type: ignore[method-assign]

    app = FastAPI()
    app.include_router(
        thumb.build_thumblab_router(
            require_auth=require_auth,
            jobs=jobs,
            persist_job_state=persist,
            fal_json_completion=llm,
            fal_vision_json_completion=vision,
            generate_image_fal_selected_model=render,
            youtube_fetch_public_channel_page_videos=gallery,
            list_connected_youtube_channels_for_user=channels,
            reserve_credits=reserve,
            release_reservation=release,
            commit_reservation=commit,
            storage_root=tmp_path / "thumbs",
            idempotency_ledger=ledger,
            fal_ai_key="test-key",
            pikzels_api_key="",
            max_video_bytes=max_video_bytes,
            probe_video_duration=probe,
            extract_frame_image=extract,
        )
    )
    return TestClient(app), jobs, events


def _auth(user: str = "alice") -> dict[str, str]:
    return {"X-Test-User": user}


def _generate_payload(description: str = "A billion-dollar fraud") -> dict:
    return {"mode": "describe", "description": description, "image_model": "seedream45"}


@pytest.mark.parametrize(
    ("method", "path", "kwargs"),
    [
        ("get", "/api/thumbnails/models", {}),
        ("get", "/api/thumbnails/my-channels", {}),
        ("get", "/api/thumbnails/creator-gallery?url=@creator", {}),
        ("post", "/api/thumbnails/upload-video", {"files": {"file": ("x.mp4", b"video", "video/mp4")}}),
        ("post", "/api/thumbnails/extract-frame?upload_id=vid_" + "a" * 32 + "&pct=.2", {}),
        ("get", "/api/thumbnails/frame/vid_" + "a" * 32, {}),
        ("post", "/api/thumbnails/generate", {"json": _generate_payload()}),
        ("get", "/api/thumbnails/generated/thumb_" + "a" * 32 + ".png", {}),
    ],
)
def test_all_active_thumblab_routes_require_auth(tmp_path: Path, method: str, path: str, kwargs: dict):
    client, _jobs, _events = _app(tmp_path)
    assert client.request(method, path, **kwargs).status_code == 401


def test_generation_is_idempotent_billed_before_provider_and_owner_scoped(tmp_path: Path):
    client, jobs, events = _app(tmp_path)
    headers = {**_auth(), "X-Idempotency-Key": "thumb-command-1"}
    first = client.post("/api/thumbnails/generate", headers=headers, json=_generate_payload())
    assert first.status_code == 202
    job_id = first.json()["job_id"]
    assert jobs[job_id]["status"] == "complete"
    assert events == ["reserve", "provider", "commit"]

    replay = client.post("/api/thumbnails/generate", headers=headers, json=_generate_payload())
    assert replay.status_code == 202
    assert replay.json()["job_id"] == job_id
    assert replay.json()["idempotent_replay"] is True
    assert events == ["reserve", "provider", "commit"]

    conflict = client.post("/api/thumbnails/generate", headers=headers, json=_generate_payload("different"))
    assert conflict.status_code == 409
    output_url = jobs[job_id]["output_url"]
    assert client.get(output_url, headers=_auth("alice")).status_code == 200
    assert client.get(output_url, headers=_auth("bob")).status_code == 404


def test_provider_failure_releases_credit_hold(tmp_path: Path):
    client, jobs, events = _app(tmp_path, provider_fails=True)
    response = client.post(
        "/api/thumbnails/generate",
        headers={**_auth(), "X-Idempotency-Key": "failed-command"},
        json=_generate_payload(),
    )
    assert response.status_code == 202
    assert jobs[response.json()["job_id"]]["status"] == "error"
    assert events == ["reserve", "provider", "release"]


def test_commit_failure_deletes_output_and_releases_hold(tmp_path: Path):
    client, jobs, events = _app(tmp_path, commit_fails=True)
    response = client.post(
        "/api/thumbnails/generate",
        headers={**_auth(), "X-Idempotency-Key": "commit-failure"},
        json=_generate_payload(),
    )
    assert response.status_code == 202
    job_id = response.json()["job_id"]
    assert jobs[job_id]["status"] == "error"
    assert events == ["reserve", "provider", "commit", "release"]
    assert client.get(f"/api/thumbnails/generated/{job_id}.png", headers=_auth()).status_code == 404


def test_uncommitted_thumbnail_file_is_not_served(tmp_path: Path):
    client, jobs, _events = _app(tmp_path)
    runtime = thumb._DEFAULT_RUNTIME
    assert runtime is not None
    user = {"id": "alice"}
    job_id = "thumb_" + "a" * 32
    jobs[job_id] = {"status": "generating", "user_id": "alice"}
    path = thumb._thumbnail_output_dir(runtime, user, create=True) / f"{job_id}.png"
    Image.new("RGB", (20, 20)).save(path, "PNG")
    assert client.get(f"/api/thumbnails/generated/{job_id}.png", headers=_auth()).status_code == 404


def test_owner_scoped_longform_package_thumbnail_filename_is_supported(tmp_path: Path):
    client, _jobs, _events = _app(tmp_path)
    runtime = thumb._DEFAULT_RUNTIME
    assert runtime is not None
    path = thumb._thumbnail_output_dir(runtime, {"id": "alice"}, create=True) / "longform_abc_package.png"
    Image.new("RGB", (20, 20)).save(path, "PNG")
    assert client.get("/api/thumbnails/generated/longform_abc_package.png", headers=_auth()).status_code == 200
    assert client.get("/api/thumbnails/generated/longform_abc_package.png", headers=_auth("bob")).status_code == 404


def test_receipt_save_failure_releases_hold_before_provider_work(tmp_path: Path):
    client, jobs, events = _app(tmp_path, receipt_save_fails=True)
    response = client.post(
        "/api/thumbnails/generate",
        headers={**_auth(), "X-Idempotency-Key": "receipt-failure"},
        json=_generate_payload(),
    )
    assert response.status_code == 503
    assert events == ["reserve", "release"]
    assert len(jobs) == 1
    assert next(iter(jobs.values()))["status"] == "error"


def test_upload_and_frame_artifacts_are_owner_scoped_and_bounded(tmp_path: Path):
    client, _jobs, _events = _app(tmp_path, max_video_bytes=16)
    uploaded = client.post(
        "/api/thumbnails/upload-video",
        headers=_auth("alice"),
        files={"file": ("movie.mp4", b"12345678", "video/mp4")},
    )
    assert uploaded.status_code == 200
    upload_id = uploaded.json()["upload_id"]
    assert client.post(
        f"/api/thumbnails/extract-frame?upload_id={upload_id}&pct=.2", headers=_auth("bob")
    ).status_code == 404
    extracted = client.post(
        f"/api/thumbnails/extract-frame?upload_id={upload_id}&pct=.2", headers=_auth("alice")
    )
    assert extracted.status_code == 200
    assert client.get(extracted.json()["preview_url"], headers=_auth("alice")).status_code == 200
    assert client.get(extracted.json()["preview_url"], headers=_auth("bob")).status_code == 404

    oversized = client.post(
        "/api/thumbnails/upload-video",
        headers=_auth("alice"),
        files={"file": ("huge.mp4", b"x" * 17, "video/mp4")},
    )
    assert oversized.status_code == 413


def test_gallery_rejects_non_youtube_urls(tmp_path: Path):
    client, _jobs, _events = _app(tmp_path)
    response = client.get(
        "/api/thumbnails/creator-gallery",
        params={"url": "https://evil.example/?next=youtube.com/@creator"},
        headers=_auth(),
    )
    assert response.status_code == 400


def test_generate_rejects_another_accounts_channel_before_billing(tmp_path: Path):
    client, _jobs, events = _app(tmp_path)
    payload = {**_generate_payload(), "channel_id": "channel-bob"}
    response = client.post(
        "/api/thumbnails/generate",
        headers={**_auth("alice"), "X-Idempotency-Key": "wrong-channel"},
        json=payload,
    )
    assert response.status_code == 403
    assert events == []


def test_reference_fetch_rejects_private_network_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    def blocked(*_args, **_kwargs):
        raise thumb.ProductReferenceError("private address")

    monkeypatch.setattr(thumb, "_fetch_public_resource", blocked)
    with pytest.raises(ValueError, match="not allowed"):
        asyncio.run(_download(tmp_path))


async def _download(tmp_path: Path):
    return await thumb._download_public_image("http://127.0.0.1/private.png", tmp_path / "x.png")
