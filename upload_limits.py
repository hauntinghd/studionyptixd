"""Shared bounded-upload primitives for multipart file handlers.

Starlette's :class:`UploadFile` may already be spooled by the multipart parser,
but handlers still need an explicit persistent-storage and in-memory boundary.
These helpers stream in fixed-size chunks, delete partial destinations on every
failure, and fail as soon as the configured byte limit is crossed.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Awaitable, Callable


UPLOAD_CHUNK_BYTES = 1024 * 1024
MIB = 1024 * 1024
GIB = 1024 * MIB

MAX_DICTATION_AUDIO_BYTES = 25 * MIB
MAX_REFERENCE_IMAGE_BYTES = 12 * MIB
MAX_LONGFORM_REFERENCE_IMAGE_BYTES = 8 * MIB
MAX_ANALYTICS_IMAGE_BYTES = 12 * MIB
MAX_CLIPLAB_VIDEO_BYTES = 2 * GIB
MAX_CATALYST_VIDEO_BYTES = 1 * GIB
MAX_STUDIO_ATTACHMENT_VIDEO_BYTES = 3 * GIB


# Request-body budgets include room for multipart boundaries and non-file form
# fields. They are deliberately path-specific: a single small global limit
# would break legitimate multi-hour ClipLab and Catalyst reference uploads.
MULTIPART_CONTENT_LENGTH_LIMITS: tuple[tuple[re.Pattern[str], int], ...] = (
    (re.compile(r"^/api/cliplab/ingest/upload$"), MAX_CLIPLAB_VIDEO_BYTES + 16 * MIB),
    (re.compile(r"^/api/studio-agent/dictation$"), MAX_DICTATION_AUDIO_BYTES + 1 * MIB),
    (re.compile(r"^/api/studio-agent/sessions/[^/]+/attachments/image$"), MAX_REFERENCE_IMAGE_BYTES + 1 * MIB),
    (re.compile(r"^/api/studio-agent/sessions/[^/]+/attachments/video$"), MAX_STUDIO_ATTACHMENT_VIDEO_BYTES + 16 * MIB),
    (re.compile(r"^/api/longform/session/bootstrap$"), 8 * MIB + 24 * MAX_ANALYTICS_IMAGE_BYTES + 16 * MIB),
    (re.compile(r"^/api/longform/session/[^/]+/(?:reference-image|character-reference)$"), MAX_LONGFORM_REFERENCE_IMAGE_BYTES + 1 * MIB),
    (re.compile(r"^/api/creative/reference-image$"), MAX_LONGFORM_REFERENCE_IMAGE_BYTES + 1 * MIB),
    (re.compile(r"^/api/skeleton-ai/reference$"), MAX_REFERENCE_IMAGE_BYTES + 1 * MIB),
    (
        re.compile(r"^/api/catalyst/hub/reference-video-analysis/manual$"),
        2 * MAX_CATALYST_VIDEO_BYTES + 24 * MAX_ANALYTICS_IMAGE_BYTES + 32 * MIB,
    ),
    (re.compile(r"^/api/chatstory/render$"), 512 * MIB + 12 * MIB + 1 * MIB + 8 * MIB),
    (re.compile(r"^/api/clone$"), 512 * MIB + 8 * MIB),
)


def multipart_content_length_limit(path: str) -> int | None:
    normalized = str(path or "")
    for pattern, limit in MULTIPART_CONTENT_LENGTH_LIMITS:
        if pattern.fullmatch(normalized):
            return limit
    return None


class _MultipartBodyTooLarge(Exception):
    """Private control-flow signal raised by the bounded ASGI receive wrapper."""


class MultipartContentLengthLimitMiddleware:
    """Enforce path-specific multipart budgets before Starlette spools files.

    A valid ``Content-Length`` provides an immediate rejection path. The
    wrapped ASGI ``receive`` callable also counts the bytes that actually
    arrive, closing bypasses via chunked requests, a missing length, or a
    dishonest undersized length. Per-file handler limits remain authoritative
    inside the aggregate request-body budget.
    """

    def __init__(self, app: Callable[..., Awaitable[None]]) -> None:
        self.app = app

    async def __call__(self, scope: dict[str, Any], receive: Callable, send: Callable) -> None:
        if scope.get("type") != "http" or str(scope.get("method") or "").upper() not in {"POST", "PUT", "PATCH"}:
            await self.app(scope, receive, send)
            return
        limit = multipart_content_length_limit(str(scope.get("path") or ""))
        if limit is None:
            await self.app(scope, receive, send)
            return
        def _as_bytes(value: Any) -> bytes:
            return value if isinstance(value, bytes) else str(value).encode("latin-1", errors="ignore")

        headers = {
            _as_bytes(key).lower(): _as_bytes(value)
            for key, value in list(scope.get("headers") or [])
        }
        content_type = headers.get(b"content-type", b"").decode("latin-1", errors="ignore").lower()
        if not content_type.startswith("multipart/form-data"):
            await self.app(scope, receive, send)
            return
        raw_length = headers.get(b"content-length", b"").decode("ascii", errors="ignore").strip()
        if raw_length:
            try:
                declared = int(raw_length)
            except ValueError:
                await self._reject(send, 400, "Invalid Content-Length header", limit)
                return
            if declared < 0:
                await self._reject(send, 400, "Invalid Content-Length header", limit)
                return
            if declared > limit:
                await self._reject(send, 413, f"Multipart request exceeds {limit} bytes", limit)
                return

        received = 0
        response_started = False
        body_limit_exceeded = False
        limit_response_sent = False

        async def bounded_receive() -> dict[str, Any]:
            nonlocal received, body_limit_exceeded
            message = await receive()
            if message.get("type") == "http.request":
                body = message.get("body", b"")
                if not isinstance(body, (bytes, bytearray, memoryview)):
                    body = bytes(body or b"")
                received += len(body)
                if received > limit:
                    body_limit_exceeded = True
                    raise _MultipartBodyTooLarge
            return message

        async def tracked_send(message: dict[str, Any]) -> None:
            nonlocal response_started, limit_response_sent
            if body_limit_exceeded:
                # FastAPI converts arbitrary request-body parsing exceptions
                # into its own 400 response. Suppress that response and retain
                # the semantically correct 413 from this outer boundary.
                if not limit_response_sent:
                    limit_response_sent = True
                    await self._reject(send, 413, f"Multipart request exceeds {limit} bytes", limit)
                return
            if message.get("type") == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, bounded_receive, tracked_send)
        except _MultipartBodyTooLarge:
            # FastAPI resolves multipart form dependencies before invoking the
            # endpoint, so no response should have started. If a future inner
            # ASGI app streams a response while still reading the request, do
            # not emit an invalid second response head.
            if response_started:
                raise
            if not limit_response_sent:
                limit_response_sent = True
                await self._reject(send, 413, f"Multipart request exceeds {limit} bytes", limit)
        if body_limit_exceeded and not response_started and not limit_response_sent:
            await self._reject(send, 413, f"Multipart request exceeds {limit} bytes", limit)

    @staticmethod
    async def _reject(send: Callable, status: int, detail: str, limit: int) -> None:
        body = json.dumps({"detail": detail, "max_bytes": limit}, separators=(",", ":")).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("ascii")),
                    (b"cache-control", b"no-store"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})


class UploadTooLargeError(ValueError):
    """Raised when an upload crosses its explicit byte budget."""


async def read_upload_limited(
    upload: Any,
    *,
    max_bytes: int,
    label: str = "upload",
    chunk_bytes: int = UPLOAD_CHUNK_BYTES,
) -> bytes:
    """Read an async upload without ever retaining more than ``max_bytes``.

    This is intended for APIs which still require a byte payload. Prefer
    :func:`write_upload_limited` when the next step consumes a local file.
    """
    if max_bytes <= 0 or chunk_bytes <= 0:
        raise ValueError("upload limits must be positive")
    payload = bytearray()
    while True:
        chunk = await upload.read(min(chunk_bytes, max_bytes - len(payload) + 1))
        if not chunk:
            break
        payload.extend(chunk)
        if len(payload) > max_bytes:
            raise UploadTooLargeError(f"{label} exceeds {max_bytes} bytes")
    return bytes(payload)


async def write_upload_limited(
    upload: Any,
    destination: Path,
    *,
    max_bytes: int,
    label: str = "upload",
    chunk_bytes: int = UPLOAD_CHUNK_BYTES,
) -> int:
    """Stream an async upload to a file with a hard upper bound.

    The destination is removed if the upload is empty, oversized, interrupted,
    or otherwise fails, so callers cannot accidentally process partial media.
    """
    if max_bytes <= 0 or chunk_bytes <= 0:
        raise ValueError("upload limits must be positive")
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    size = 0
    created = False
    try:
        # Callers construct unique server-side names. Exclusive creation avoids
        # following or overwriting a pre-existing symlink/file if a name ever
        # collides in a shared temporary directory.
        with destination.open("xb") as handle:
            created = True
            while True:
                chunk = await upload.read(min(chunk_bytes, max_bytes - size + 1))
                if not chunk:
                    break
                size += len(chunk)
                if size > max_bytes:
                    raise UploadTooLargeError(f"{label} exceeds {max_bytes} bytes")
                handle.write(chunk)
        if size <= 0:
            raise ValueError(f"{label} is empty")
        return size
    except BaseException:
        if created:
            destination.unlink(missing_ok=True)
        raise
