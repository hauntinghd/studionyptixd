from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.testclient import TestClient

import backend
import catalyst
import cliplab_router
import skeleton_ai_router
import studio_agent_router
import upload_limits
from studio_agent import idempotent_mutations
from studio_agent.command_execution import FileExecutionLedger


class _Upload:
    def __init__(
        self,
        payload: bytes,
        *,
        filename: str = "upload.mp4",
        content_type: str = "video/mp4",
    ) -> None:
        self.payload = payload
        self.filename = filename
        self.content_type = content_type
        self.offset = 0

    async def read(self, size: int = -1) -> bytes:
        if self.offset >= len(self.payload):
            return b""
        if size < 0:
            size = len(self.payload) - self.offset
        chunk = self.payload[self.offset : self.offset + size]
        self.offset += len(chunk)
        return chunk


def _owner_dependency():
    async def require_auth() -> dict[str, object]:
        return {"id": "owner", "email": "owner@example.com", "is_admin": True}

    return require_auth


def test_content_length_preflight_rejects_before_multipart_parser() -> None:
    called = False

    async def inner(scope, receive, send):
        nonlocal called
        called = True

    middleware = upload_limits.MultipartContentLengthLimitMiddleware(inner)
    limit = upload_limits.multipart_content_length_limit("/api/studio-agent/dictation")
    assert limit is not None
    sent: list[dict] = []
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/studio-agent/dictation",
        "headers": [
            (b"content-type", b"multipart/form-data; boundary=x"),
            (b"content-length", str(limit + 1).encode("ascii")),
        ],
    }
    asyncio.run(middleware(scope, lambda: None, lambda message: _capture(sent, message)))
    assert called is False
    assert sent[0]["status"] == 413
    assert json.loads(sent[1]["body"])["max_bytes"] == limit


async def _capture(target: list[dict], message: dict) -> None:
    target.append(message)


def test_content_length_preflight_preserves_legitimate_cliplab_budget() -> None:
    called = False

    async def inner(scope, receive, send):
        nonlocal called
        called = True

    middleware = upload_limits.MultipartContentLengthLimitMiddleware(inner)
    limit = upload_limits.multipart_content_length_limit("/api/cliplab/ingest/upload")
    assert limit is not None and limit > upload_limits.MAX_CLIPLAB_VIDEO_BYTES
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/cliplab/ingest/upload",
        "headers": [
            (b"content-type", b"multipart/form-data; boundary=x"),
            (b"content-length", str(limit).encode("ascii")),
        ],
    }
    asyncio.run(middleware(scope, lambda: None, lambda message: _capture([], message)))
    assert called is True


def _request_receiver(*chunks: bytes):
    messages = [
        {
            "type": "http.request",
            "body": chunk,
            "more_body": index < len(chunks) - 1,
        }
        for index, chunk in enumerate(chunks)
    ]

    async def receive() -> dict:
        if messages:
            return messages.pop(0)
        return {"type": "http.request", "body": b"", "more_body": False}

    return receive


async def _consume_request(scope, receive, send) -> None:
    while True:
        message = await receive()
        if not message.get("more_body", False):
            break
    await send({"type": "http.response.start", "status": 204, "headers": []})
    await send({"type": "http.response.body", "body": b""})


@pytest.mark.parametrize(
    "headers",
    [
        [(b"content-type", b"multipart/form-data; boundary=x")],
        [
            (b"content-type", b"multipart/form-data; boundary=x"),
            (b"content-length", b"4"),
            (b"transfer-encoding", b"chunked"),
        ],
    ],
)
def test_actual_body_limit_rejects_lengthless_chunked_and_dishonest_lengths(
    headers: list[tuple[bytes, bytes]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(upload_limits, "multipart_content_length_limit", lambda _path: 4)
    middleware = upload_limits.MultipartContentLengthLimitMiddleware(_consume_request)
    sent: list[dict] = []
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/test-upload",
        "headers": headers,
    }

    asyncio.run(
        middleware(
            scope,
            _request_receiver(b"123", b"45"),
            lambda message: _capture(sent, message),
        )
    )

    assert sent[0]["status"] == 413
    assert json.loads(sent[1]["body"])["max_bytes"] == 4


def test_actual_body_limit_allows_body_at_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(upload_limits, "multipart_content_length_limit", lambda _path: 4)
    middleware = upload_limits.MultipartContentLengthLimitMiddleware(_consume_request)
    sent: list[dict] = []
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/test-upload",
        "headers": [(b"content-type", b"multipart/form-data; boundary=x")],
    }

    asyncio.run(
        middleware(
            scope,
            _request_receiver(b"12", b"34"),
            lambda message: _capture(sent, message),
        )
    )

    assert sent[0]["status"] == 204


def test_malformed_content_length_is_rejected_before_body_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    async def inner(scope, receive, send):
        nonlocal called
        called = True

    monkeypatch.setattr(upload_limits, "multipart_content_length_limit", lambda _path: 4)
    middleware = upload_limits.MultipartContentLengthLimitMiddleware(inner)
    sent: list[dict] = []
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/test-upload",
        "headers": [
            (b"content-type", b"multipart/form-data; boundary=x"),
            (b"content-length", b"not-a-number"),
        ],
    }
    asyncio.run(
        middleware(
            scope,
            _request_receiver(b"1234"),
            lambda message: _capture(sent, message),
        )
    )
    assert called is False
    assert sent[0]["status"] == 400


def test_actual_body_limit_interrupts_fastapi_multipart_parser(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False
    app = FastAPI()

    @app.post("/api/test-upload")
    async def upload(file: UploadFile = File(...)):
        nonlocal called
        called = True
        return {"ok": True}

    monkeypatch.setattr(upload_limits, "multipart_content_length_limit", lambda _path: 96)
    app.add_middleware(upload_limits.MultipartContentLengthLimitMiddleware)
    body = (
        b"--x\r\nContent-Disposition: form-data; name=\"file\"; filename=\"x.bin\"\r\n"
        b"Content-Type: application/octet-stream\r\n\r\n"
        + b"x" * 128
        + b"\r\n--x--\r\n"
    )
    sent: list[dict] = []
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/api/test-upload",
        "raw_path": b"/api/test-upload",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"content-type", b"multipart/form-data; boundary=x")],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }
    asyncio.run(
        app(
            scope,
            _request_receiver(body[:80], body[80:]),
            lambda message: _capture(sent, message),
        )
    )

    assert called is False
    assert sent[0]["status"] == 413


def test_backend_installs_preflight_and_preserves_studio_cors() -> None:
    limit = upload_limits.multipart_content_length_limit("/api/studio-agent/dictation")
    assert limit is not None
    response = TestClient(backend.app).post(
        "/api/studio-agent/dictation",
        content=b"x",
        headers={
            "content-type": "multipart/form-data; boundary=x",
            "content-length": str(limit + 1),
            "origin": "https://studio.nyptidindustries.com",
        },
    )
    assert response.status_code == 413
    assert response.headers.get("access-control-allow-origin") == "https://studio.nyptidindustries.com"


def test_backend_cors_allows_billable_idempotency_header() -> None:
    response = TestClient(backend.app).options(
        "/api/thumbnails/generate",
        headers={
            "origin": "https://studio.nyptidindustries.com",
            "access-control-request-method": "POST",
            "access-control-request-headers": "authorization,content-type,x-idempotency-key",
        },
    )

    assert response.status_code == 200
    assert response.headers.get("access-control-allow-origin") == "https://studio.nyptidindustries.com"
    allowed = response.headers.get("access-control-allow-headers", "").lower()
    assert "x-idempotency-key" in allowed


def test_dictation_route_returns_413_before_transcription(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(upload_limits, "MAX_DICTATION_AUDIO_BYTES", 4)
    app = FastAPI()
    app.include_router(
        studio_agent_router.build_studio_agent_router(
            require_auth=_owner_dependency(),
            is_admin_check=lambda user: True,
        )
    )
    response = TestClient(app).post(
        "/api/studio-agent/dictation",
        files={"audio": ("dictation.webm", b"12345", "audio/webm")},
    )
    assert response.status_code == 413


def test_cliplab_route_caps_and_removes_partial_video(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cliplab_router, "MAX_CLIPLAB_VIDEO_BYTES", 4)
    monkeypatch.setattr(cliplab_router, "CLIPLAB_UPLOAD_DIR", tmp_path)
    monkeypatch.setattr(
        idempotent_mutations,
        "_LEDGER",
        FileExecutionLedger(tmp_path / "command-ledger"),
    )
    async def enqueue_job(_job_id: str, _plan: str, _descriptor: dict) -> None:
        return None

    app = FastAPI()
    app.include_router(
        cliplab_router.build_cliplab_router(
            require_auth=_owner_dependency(),
            jobs={},
            enqueue_job=enqueue_job,
            fal_json_completion=lambda *args, **kwargs: {},
        )
    )
    response = TestClient(app).post(
        "/api/cliplab/ingest/upload",
        headers={"X-Idempotency-Key": "oversized-cliplab-upload-1"},
        files={"file": ("source.mp4", b"12345", "video/mp4")},
    )
    assert response.status_code == 413
    assert not list(tmp_path.rglob("vid_*"))


def test_skeleton_reference_route_returns_413(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(upload_limits, "MAX_REFERENCE_IMAGE_BYTES", 4)
    app = FastAPI()
    app.include_router(skeleton_ai_router.build_skeleton_ai_router(require_auth=_owner_dependency()))
    response = TestClient(app).post(
        "/api/skeleton-ai/reference",
        files={"reference_image": ("reference.png", b"12345", "image/png")},
    )
    assert response.status_code == 413


def test_backend_longform_and_creative_reference_reads_are_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    async def current_user(request):
        return {"id": "user"}

    async def creative_session(session_id):
        return {"session_id": session_id, "user_id": "user", "reference_lock_mode": "strict"}

    monkeypatch.setattr(backend, "get_current_user_from_request", current_user)
    monkeypatch.setattr(backend, "MAX_LONGFORM_REFERENCE_IMAGE_BYTES", 4)
    monkeypatch.setattr(backend, "_load_longform_sessions", lambda: None)
    monkeypatch.setattr(backend, "_save_longform_sessions", lambda: None)
    monkeypatch.setattr(backend, "_longform_sessions", {"session": {"session_id": "session", "user_id": "user"}})
    monkeypatch.setattr(backend, "_get_creative_session", creative_session)

    calls = [
        backend._longform_reference_image(
            "session",
            _Upload(b"12345", filename="reference.png", content_type="image/png"),
            reference_lock_mode="strict",
            request=object(),
        ),
        backend._longform_character_reference(
            "session",
            character_name="Character",
            reference_image=_Upload(b"12345", filename="character.png", content_type="image/png"),
            reference_lock_mode="strict",
            request=object(),
        ),
        backend._creative_reference_image(
            session_id="creative",
            reference_image=_Upload(b"12345", filename="creative.png", content_type="image/png"),
            reference_lock_mode="strict",
            request=object(),
        ),
    ]
    for call in calls:
        with pytest.raises(HTTPException) as exc:
            asyncio.run(call)
        assert exc.value.status_code == 413


def test_longform_bootstrap_cleans_prior_analytics_images_on_oversize(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def current_user(request):
        return {"id": "user"}

    async def no_busy_session(user_id):
        return ""

    monkeypatch.setattr(backend, "get_current_user_from_request", current_user)
    monkeypatch.setattr(backend, "_active_longform_capacity_session_id", no_busy_session)
    monkeypatch.setattr(backend, "_longform_deep_analysis_enabled", lambda user: True)
    monkeypatch.setattr(backend, "MAX_ANALYTICS_IMAGE_BYTES", 4)
    monkeypatch.setattr(backend, "TEMP_DIR", tmp_path)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            backend._create_longform_session_bootstrap(
                template="documentary",
                topic="topic",
                input_title="title",
                input_description="description",
                format_preset="documentary",
                source_url="",
                youtube_channel_id="",
                analytics_notes="",
                strategy_notes="",
                transcript_text="",
                reference_lock_mode="strict",
                auto_pipeline=False,
                target_minutes=8.0,
                language="en",
                animation_enabled=True,
                sfx_enabled=True,
                whisper_mode="subtle",
                subject_reference_image=None,
                analytics_images=[
                    _Upload(b"1234", filename="one.png", content_type="image/png"),
                    _Upload(b"12345", filename="two.png", content_type="image/png"),
                ],
                request=object(),
            )
        )
    assert exc.value.status_code == 413
    assert not list(tmp_path.rglob("lf_bootstrap_*"))


def test_catalyst_uploads_are_bounded_and_all_prior_files_are_cleaned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(catalyst, "_is_admin_user", lambda user: True)
    monkeypatch.setattr(catalyst, "CATALYST_HUB_LONGFORM_WORKSPACES", ["documentary"])
    monkeypatch.setattr(catalyst, "MAX_ANALYTICS_IMAGE_BYTES", 4)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            catalyst._catalyst_hub_reference_video_analysis_manual_for_user(
                user={"id": "owner"},
                channel_id="channel",
                workspace_id="documentary",
                analytics_images=[
                    _Upload(b"1234", filename="one.png", content_type="image/png"),
                    _Upload(b"12345", filename="two.png", content_type="image/png"),
                ],
                upload_dir=tmp_path,
            )
        )
    assert exc.value.status_code == 413
    assert not list(tmp_path.iterdir())
