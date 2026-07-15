from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from starlette.requests import Request

import auth
import backend_clone_handler
import backend_media_handlers
import studio_agent_router
from studio_agent import attachments, product_reference, store
from upload_limits import UploadTooLargeError, read_upload_limited, write_upload_limited


class _Upload:
    def __init__(self, payload: bytes, filename: str = "video.mp4") -> None:
        self.payload = payload
        self.filename = filename
        self.offset = 0

    async def read(self, size: int = -1) -> bytes:
        if self.offset >= len(self.payload):
            return b""
        if size < 0:
            size = len(self.payload) - self.offset
        chunk = self.payload[self.offset : self.offset + size]
        self.offset += len(chunk)
        return chunk


async def _reserve_clone_credit(*_args, **_kwargs):
    return True, "monthly", {"month_key": "2026-07", "credits_total_remaining": 100}


async def _refund_clone_credit(*_args, **_kwargs):
    return None


def _clone_billing_args() -> dict:
    return {
        "resolve_user_plan_for_limits": lambda user: ("creator", {}),
        "billing_active_for_user": lambda user: True,
        "is_admin_user": lambda user: False,
        "reserve_generation_credit": _reserve_clone_credit,
        "refund_generation_credit": _refund_clone_credit,
        "clone_credit_cost": 20,
    }


def _request(*, headers: list[tuple[bytes, bytes]] | None = None, query: bytes = b"") -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/protected",
            "headers": headers or [],
            "query_string": query,
        }
    )


def test_http_auth_never_accepts_query_string_tokens() -> None:
    request = _request(query=b"access_token=secret-one&token=secret-two")
    assert auth._extract_request_token(request) == ""


def test_http_auth_still_accepts_bearer_header() -> None:
    request = _request(headers=[(b"authorization", b"Bearer header-secret")])
    assert auth._extract_request_token(request) == "header-secret"


def test_bounded_upload_read_fails_before_retaining_more_than_limit() -> None:
    upload = _Upload(b"12345")
    with pytest.raises(UploadTooLargeError):
        asyncio.run(read_upload_limited(upload, max_bytes=4, chunk_bytes=2))


def test_bounded_upload_write_removes_oversized_partial_file(tmp_path: Path) -> None:
    destination = tmp_path / "partial.mp4"
    upload = _Upload(b"12345")
    with pytest.raises(UploadTooLargeError):
        asyncio.run(write_upload_limited(upload, destination, max_bytes=4, chunk_bytes=2))
    assert not destination.exists()


def test_bounded_upload_never_overwrites_or_deletes_existing_file(tmp_path: Path) -> None:
    destination = tmp_path / "existing.mp4"
    destination.write_bytes(b"keep")
    with pytest.raises(FileExistsError):
        asyncio.run(write_upload_limited(_Upload(b"new"), destination, max_bytes=4))
    assert destination.read_bytes() == b"keep"


def test_attachment_hint_is_confined_to_owned_session_storage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_root = tmp_path / "data"
    legacy_root = tmp_path / "legacy"
    monkeypatch.setattr(attachments, "APP_DATA_DIR", data_root)
    monkeypatch.setattr(attachments, "LEGACY_SKELETON_OUTPUT", legacy_root)
    monkeypatch.setattr(
        store,
        "get_session",
        lambda session_id, user_id="": {"session_id": session_id, "user_id": user_id},
    )
    owned = attachments.session_attachment_dir("session-a") / "agent_video_owned.mp4"
    owned.write_bytes(b"video")
    outside = tmp_path / "private.mp4"
    outside.write_bytes(b"secret")

    assert attachments.resolve_video_attachment_path("session-a", "user-a", hint=str(outside)) == str(owned.resolve())
    assert attachments.resolve_video_attachment_path("session-a", "user-a", hint=str(owned)) == str(owned.resolve())


def test_attachment_resolution_requires_user_owned_session(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(attachments, "APP_DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(attachments, "LEGACY_SKELETON_OUTPUT", tmp_path / "legacy")
    file_path = attachments.session_attachment_dir("other-session") / "agent_video_private.mp4"
    file_path.write_bytes(b"private")
    monkeypatch.setattr(store, "get_session", lambda *args, **kwargs: None)

    assert attachments.resolve_video_attachment_path("other-session", "wrong-user", hint=str(file_path)) == ""


def test_attachment_byte_save_enforces_hard_video_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(attachments, "APP_DATA_DIR", tmp_path)
    monkeypatch.setattr(attachments, "MAX_VIDEO_ATTACHMENT_BYTES", 4)
    with pytest.raises(UploadTooLargeError):
        attachments.save_video_attachment("session", "video.mp4", b"12345")
    assert not list(tmp_path.rglob("agent_video_*"))


def _studio_agent_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    async def require_auth() -> dict[str, object]:
        return {"id": "owner", "email": "owner@example.com", "is_admin": True}

    monkeypatch.setattr(
        store,
        "get_session",
        lambda session_id, user_id="": {"session_id": session_id, "user_id": user_id},
    )
    monkeypatch.setattr(store, "update_session", lambda *args, **kwargs: {"ok": True})
    app = FastAPI()
    app.include_router(
        studio_agent_router.build_studio_agent_router(
            require_auth=require_auth,
            is_admin_check=lambda user: True,
        )
    )
    return TestClient(app)


def test_studio_video_route_rejects_oversize_while_streaming(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(attachments, "APP_DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(attachments, "MAX_VIDEO_ATTACHMENT_BYTES", 4)
    response = _studio_agent_client(monkeypatch).post(
        "/api/studio-agent/sessions/session/attachments/video",
        files={"file": ("video.mp4", b"12345", "video/mp4")},
    )
    assert response.status_code == 413
    assert not list(tmp_path.rglob("agent_video_*"))


def test_studio_image_route_rejects_oversize_before_byte_save(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(attachments, "APP_DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(attachments, "MAX_IMAGE_ATTACHMENT_BYTES", 4)
    response = _studio_agent_client(monkeypatch).post(
        "/api/studio-agent/sessions/session/attachments/image",
        files={"file": ("image.png", b"12345", "image/png")},
    )
    assert response.status_code == 413
    assert not list(tmp_path.rglob("agent_image_*"))


def test_product_fetch_pins_validated_dns_address() -> None:
    with (
        patch.object(
            product_reference,
            "_resolve_public_url",
            return_value=("https://product.example/page", ("93.184.216.34",)),
        ),
        patch.object(
            product_reference,
            "_fetch_from_pinned_address",
            return_value={"status": 200, "headers": {"content-type": "text/html"}, "body": b"ok"},
        ) as fetch,
    ):
        result = product_reference._fetch_public_resource(
            "https://product.example/page",
            max_bytes=100,
            accept="text/html",
        )
    assert result["url"] == "https://product.example/page"
    assert fetch.call_args.args[:2] == ("https://product.example/page", "93.184.216.34")


def test_product_redirect_target_is_validated_before_second_request() -> None:
    with (
        patch.object(
            product_reference,
            "_resolve_public_url",
            side_effect=[
                ("https://product.example/page", ("93.184.216.34",)),
                product_reference.ProductReferenceError("private redirect"),
            ],
        ),
        patch.object(
            product_reference,
            "_fetch_from_pinned_address",
            return_value={
                "status": 302,
                "headers": {"location": "http://127.0.0.1/admin"},
                "body": b"",
            },
        ) as fetch,
    ):
        with pytest.raises(product_reference.ProductReferenceError, match="private redirect"):
            product_reference._fetch_public_resource(
                "https://product.example/page",
                max_bytes=100,
                accept="text/html",
            )
    assert fetch.call_count == 1


def test_product_url_rejects_reserved_address() -> None:
    with patch.object(product_reference.socket, "getaddrinfo") as lookup:
        lookup.return_value = [(None, None, None, None, ("192.0.2.1", 443))]
        with pytest.raises(product_reference.ProductReferenceError):
            product_reference._assert_public_url("https://product.example/page")


def test_clone_upload_is_streamed_with_hard_cap(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(backend_clone_handler, "MAX_CLONE_VIDEO_BYTES", 4)
    handler = backend_clone_handler.build_clone_video_handler(
        xai_api_key="x",
        elevenlabs_api_key="e",
        get_current_user_from_request=lambda request: asyncio.sleep(0, result={"id": "user"}),
        user_has_paid_access=lambda user: True,
        normalize_output_resolution=lambda value, priority_allowed=False: value,
        normalize_external_source_url=lambda value: value,
        temp_dir=tmp_path,
        jobs_ref={},
        enqueue_generation_job=lambda *args, **kwargs: asyncio.sleep(0),
        queue_full_error=RuntimeError,
        run_clone_pipeline=lambda *args, **kwargs: None,
        persist_job_state=lambda *args, **kwargs: asyncio.sleep(0),
        **_clone_billing_args(),
    )
    with pytest.raises(HTTPException) as exc:
        asyncio.run(handler(file=_Upload(b"12345"), request=object()))
    assert exc.value.status_code == 413
    assert not list(tmp_path.glob("clone_upload_*"))


def test_clone_upload_is_removed_when_queue_rejects_unexpectedly(tmp_path: Path) -> None:
    async def broken_enqueue(*args, **kwargs):
        raise ValueError("queue offline")

    jobs: dict[str, dict] = {}
    handler = backend_clone_handler.build_clone_video_handler(
        xai_api_key="x",
        elevenlabs_api_key="e",
        get_current_user_from_request=lambda request: asyncio.sleep(0, result={"id": "user"}),
        user_has_paid_access=lambda user: True,
        normalize_output_resolution=lambda value, priority_allowed=False: value,
        normalize_external_source_url=lambda value: value,
        temp_dir=tmp_path,
        jobs_ref=jobs,
        enqueue_generation_job=broken_enqueue,
        queue_full_error=RuntimeError,
        run_clone_pipeline=lambda *args, **kwargs: None,
        persist_job_state=lambda *args, **kwargs: asyncio.sleep(0),
        **_clone_billing_args(),
    )
    with pytest.raises(ValueError, match="queue offline"):
        asyncio.run(handler(file=_Upload(b"small"), request=object()))
    assert not list(tmp_path.glob("clone_upload_*"))
    assert jobs == {}


def test_chat_story_background_is_streamed_with_hard_cap(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(backend_media_handlers, "MAX_CHAT_STORY_BACKGROUND_BYTES", 4)
    handler = backend_media_handlers.build_render_chat_story_handler(
        get_current_user_from_request=lambda request: asyncio.sleep(0, result={"id": "user"}),
        chat_story_access_for_user=lambda user: True,
        is_admin_user=lambda user: False,
        temp_dir=tmp_path,
        output_dir=tmp_path,
        render_script_path=tmp_path / "missing.py",
        log=type("Log", (), {"error": lambda *args, **kwargs: None})(),
    )
    payload = '{"messages":[{"text":"hello"}]}'
    with pytest.raises(HTTPException) as exc:
        asyncio.run(
            handler(
                object(),
                payload,
                background_video=_Upload(b"12345", filename="background.mp4"),
            )
        )
    assert exc.value.status_code == 413
    assert not list(tmp_path.glob("chatstory_*"))
