from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path

import pytest
from fastapi import HTTPException
from starlette.responses import FileResponse, JSONResponse, Response

import backend


RELEASE_VERSION = "1.0.1"
RELEASE_FILENAME = f"NYPTID-Studio_{RELEASE_VERSION}_x64-setup.exe"
RELEASE_BYTES = b"provider-free NYPTID Studio 1.0.1 release artifact\n"
RELEASE_SIGNATURE = "provider-free-updater-signature"


def _run(coroutine):
    return asyncio.run(coroutine)


@pytest.fixture
def release_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(backend, "DESKTOP_RELEASE_DIR", tmp_path)
    monkeypatch.setattr(backend, "DESKTOP_RELEASE_VERSION", RELEASE_VERSION)
    monkeypatch.setattr(backend, "DESKTOP_RELEASE_FILENAME", RELEASE_FILENAME)
    return tmp_path


def _write_release(release_dir: Path, *, declared_sha256: str | None = None) -> tuple[Path, str]:
    release_path = release_dir / RELEASE_FILENAME
    release_path.write_bytes(RELEASE_BYTES)
    actual_sha256 = hashlib.sha256(RELEASE_BYTES).hexdigest()
    release_path.with_suffix(f"{release_path.suffix}.sha256").write_text(
        declared_sha256 or actual_sha256,
        encoding="utf-8",
    )
    release_path.with_suffix(f"{release_path.suffix}.sig").write_text(
        RELEASE_SIGNATURE,
        encoding="utf-8",
    )
    return release_path, actual_sha256


def test_matching_sidecar_and_artifact_publish_release_and_downloads(release_dir: Path) -> None:
    release_path, actual_sha256 = _write_release(release_dir)

    manifest = _run(backend.desktop_release_latest())

    assert manifest["version"] == RELEASE_VERSION
    assert manifest["available"] is True
    assert manifest["sha256"] == actual_sha256
    assert manifest["published_at"]

    generic_download = _run(backend.desktop_release_download())
    versioned_download = _run(backend.desktop_release_download_versioned(RELEASE_VERSION))

    assert isinstance(generic_download, FileResponse)
    assert Path(generic_download.path) == release_path
    assert generic_download.headers["cache-control"] == "public, max-age=300"
    assert isinstance(versioned_download, FileResponse)
    assert Path(versioned_download.path) == release_path
    assert versioned_download.headers["cache-control"] == "public, max-age=31536000, immutable"


def test_mismatched_sidecar_fails_closed_and_downloads_return_404(release_dir: Path) -> None:
    _write_release(release_dir, declared_sha256="0" * 64)

    manifest = _run(backend.desktop_release_latest())

    assert manifest["available"] is False
    assert manifest["sha256"] == ""
    assert manifest["published_at"] == ""

    with pytest.raises(HTTPException) as generic_error:
        _run(backend.desktop_release_download())
    assert generic_error.value.status_code == 404

    with pytest.raises(HTTPException) as versioned_error:
        _run(backend.desktop_release_download_versioned(RELEASE_VERSION))
    assert versioned_error.value.status_code == 404


@pytest.mark.parametrize(
    ("target", "arch"),
    [
        ("linux", "x86_64"),
        ("windows", "i686"),
        ("windows", "aarch64"),
    ],
)
def test_updater_only_offers_windows_x86_64(target: str, arch: str, release_dir: Path) -> None:
    _write_release(release_dir)

    response = _run(backend.desktop_release_updater(target, arch, "0.2.3"))

    assert isinstance(response, Response)
    assert response.status_code == 204
    assert response.headers["cache-control"] == "no-store"


def test_older_windows_x86_64_client_receives_signed_1_0_1_metadata(release_dir: Path) -> None:
    release_path, _actual_sha256 = _write_release(release_dir)

    response = _run(backend.desktop_release_updater("windows", "x86_64", "0.2.3"))
    payload = json.loads(response.body)

    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert payload == {
        "version": RELEASE_VERSION,
        "pub_date": backend.datetime.fromtimestamp(
            release_path.stat().st_mtime,
            backend.timezone.utc,
        ).isoformat(),
        "url": f"https://nyptid-studio.fly.dev/api/desktop/download/{RELEASE_VERSION}",
        "signature": RELEASE_SIGNATURE,
        "notes": backend.DESKTOP_RELEASE_NOTES,
    }


def test_current_windows_x86_64_client_receives_no_update(release_dir: Path) -> None:
    _write_release(release_dir)

    response = _run(backend.desktop_release_updater("windows", "x86_64", RELEASE_VERSION))

    assert isinstance(response, Response)
    assert response.status_code == 204
    assert response.headers["cache-control"] == "no-store"


def test_invalid_current_version_is_rejected_for_supported_platform(release_dir: Path) -> None:
    _write_release(release_dir)

    with pytest.raises(HTTPException) as error:
        _run(backend.desktop_release_updater("windows", "x86_64", "not-semver"))

    assert error.value.status_code == 400
    assert error.value.detail == "Invalid Studio desktop version"


def test_versioned_download_rejects_non_current_release(release_dir: Path) -> None:
    _write_release(release_dir)

    with pytest.raises(HTTPException) as error:
        _run(backend.desktop_release_download_versioned("0.2.3"))

    assert error.value.status_code == 404
    assert error.value.detail == "Studio desktop release version was not found"
