from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import tomllib
from pathlib import Path

import pytest
from fastapi import HTTPException
from starlette.responses import FileResponse, JSONResponse, Response

import backend


ROOT = Path(__file__).resolve().parents[1]
SIGNED_FIXTURE_PATH = ROOT / "tests" / "fixtures" / "desktop_release_test_artifact.txt"
SIGNED_FIXTURE_SIGNATURE_PATH = SIGNED_FIXTURE_PATH.with_suffix(
    f"{SIGNED_FIXTURE_PATH.suffix}.sig"
)
RELEASE_VERSION = "1.0.2"
RELEASE_FILENAME = f"NYPTID-Studio_{RELEASE_VERSION}_x64-setup.exe"
RELEASE_BYTES = SIGNED_FIXTURE_PATH.read_bytes()
RELEASE_SIGNATURE = SIGNED_FIXTURE_SIGNATURE_PATH.read_text(encoding="utf-8").strip()
CANONICAL_API_URL = "https://api-studio.nyptidindustries.com"
ROTATED_UPDATER_PUBKEY_SHA256 = "7936caeec9b98979ccd1deebf7627d8234c633815c2c215eb8a5ef9a7342301b"


def _run(coroutine):
    return asyncio.run(coroutine)


@pytest.fixture
def release_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(backend, "DESKTOP_RELEASE_DIR", tmp_path)
    monkeypatch.setattr(backend, "DESKTOP_RELEASE_VERSION", RELEASE_VERSION)
    monkeypatch.setattr(backend, "DESKTOP_RELEASE_FILENAME", RELEASE_FILENAME)
    monkeypatch.setattr(backend, "_api_public_url", lambda: CANONICAL_API_URL)
    return tmp_path


def _write_release(
    release_dir: Path,
    *,
    declared_sha256: str | None = None,
    signature: str = RELEASE_SIGNATURE,
) -> tuple[Path, str]:
    release_path = release_dir / RELEASE_FILENAME
    release_path.write_bytes(RELEASE_BYTES)
    actual_sha256 = hashlib.sha256(RELEASE_BYTES).hexdigest()
    release_path.with_suffix(f"{release_path.suffix}.sha256").write_text(
        declared_sha256 or actual_sha256,
        encoding="utf-8",
    )
    release_path.with_suffix(f"{release_path.suffix}.sig").write_text(
        signature,
        encoding="utf-8",
    )
    return release_path, actual_sha256


def test_matching_sidecar_and_artifact_publish_release_and_downloads(release_dir: Path) -> None:
    release_path, actual_sha256 = _write_release(release_dir)

    manifest = _run(backend.desktop_release_latest())

    assert manifest["version"] == RELEASE_VERSION
    assert manifest["available"] is True
    assert manifest["download_url"] == f"{CANONICAL_API_URL}/api/desktop/download/{RELEASE_VERSION}"
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


def test_tampered_artifact_with_matching_checksum_fails_minisign_gate(release_dir: Path) -> None:
    release_path, _actual_sha256 = _write_release(release_dir)
    release_path.write_bytes(RELEASE_BYTES + b"tampered")
    release_path.with_suffix(f"{release_path.suffix}.sha256").write_text(
        hashlib.sha256(release_path.read_bytes()).hexdigest(),
        encoding="utf-8",
    )

    manifest = _run(backend.desktop_release_latest())

    assert manifest["available"] is False
    assert manifest["sha256"] == ""


@pytest.mark.parametrize("signature", ["not-a-tauri-signature", "", "Zg=="])
def test_malformed_or_missing_signature_fails_closed(
    release_dir: Path,
    signature: str,
) -> None:
    _write_release(release_dir, signature=signature)

    manifest = _run(backend.desktop_release_latest())

    assert manifest["available"] is False
    assert manifest["sha256"] == ""


def test_non_utf8_signature_sidecar_fails_closed(release_dir: Path) -> None:
    release_path, _actual_sha256 = _write_release(release_dir)
    release_path.with_suffix(f"{release_path.suffix}.sig").write_bytes(b"\xff\xfe")

    manifest = _run(backend.desktop_release_latest())

    assert manifest["available"] is False
    assert manifest["sha256"] == ""


def _mutate_minisign_document(
    encoded_signature: str,
    mutate,
) -> str:
    document = base64.b64decode(encoded_signature, validate=True).decode("utf-8")
    lines = document.splitlines()
    mutate(lines)
    return base64.b64encode(("\n".join(lines) + "\n").encode("utf-8")).decode("ascii")


def test_wrong_minisign_key_id_fails_closed(release_dir: Path) -> None:
    def mutate(lines: list[str]) -> None:
        payload = bytearray(base64.b64decode(lines[1], validate=True))
        payload[2] ^= 1
        lines[1] = base64.b64encode(payload).decode("ascii")

    _write_release(
        release_dir,
        signature=_mutate_minisign_document(RELEASE_SIGNATURE, mutate),
    )

    assert _run(backend.desktop_release_latest())["available"] is False


def test_unverified_trusted_comment_fails_closed(release_dir: Path) -> None:
    def mutate(lines: list[str]) -> None:
        lines[2] += "\tmodified"

    _write_release(
        release_dir,
        signature=_mutate_minisign_document(RELEASE_SIGNATURE, mutate),
    )

    assert _run(backend.desktop_release_latest())["available"] is False


def test_missing_tauri_trust_anchor_fails_closed(
    release_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_release(release_dir)
    monkeypatch.setattr(
        backend,
        "DESKTOP_UPDATER_CONFIG_PATH",
        release_dir / "missing-tauri.conf.json",
    )

    assert _run(backend.desktop_release_latest())["available"] is False


def test_supplied_real_release_artifact_verifies_like_backend() -> None:
    artifact_raw = os.getenv("NYPTID_UPDATER_VERIFY_ARTIFACT", "").strip()
    signature_raw = os.getenv("NYPTID_UPDATER_VERIFY_SIGNATURE", "").strip()
    if not artifact_raw:
        pytest.skip("set NYPTID_UPDATER_VERIFY_ARTIFACT to validate a release artifact")
    if not signature_raw:
        pytest.fail(
            "NYPTID_UPDATER_VERIFY_SIGNATURE is required when a release artifact is supplied"
        )

    artifact = Path(artifact_raw)
    signature = Path(signature_raw).read_text(encoding="utf-8").strip()
    assert backend._desktop_release_signature_is_valid(artifact, signature)


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


def test_older_windows_x86_64_client_receives_signed_1_0_2_metadata(release_dir: Path) -> None:
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
        "url": f"{CANONICAL_API_URL}/api/desktop/download/{RELEASE_VERSION}",
        "signature": RELEASE_SIGNATURE,
        "notes": backend.DESKTOP_RELEASE_NOTES,
    }


def test_pending_1_0_2_source_and_updater_contract_are_canonical() -> None:
    tauri_config = json.loads(
        (ROOT / "ViralShorts-App" / "src-tauri" / "tauri.conf.json").read_text(encoding="utf-8")
    )
    cargo_config = tomllib.loads(
        (ROOT / "ViralShorts-App" / "src-tauri" / "Cargo.toml").read_text(encoding="utf-8")
    )

    assert backend.DESKTOP_RELEASE_VERSION == RELEASE_VERSION
    assert backend.DESKTOP_RELEASE_FILENAME == RELEASE_FILENAME
    assert tauri_config["version"] == RELEASE_VERSION
    assert cargo_config["package"]["version"] == RELEASE_VERSION
    assert tauri_config["plugins"]["updater"]["endpoints"] == [
        f"{CANONICAL_API_URL}/api/desktop/updater/{{{{target}}}}/{{{{arch}}}}/{{{{current_version}}}}"
    ]
    updater_pubkey = base64.b64decode(tauri_config["plugins"]["updater"]["pubkey"], validate=True)
    assert hashlib.sha256(updater_pubkey).hexdigest() == ROTATED_UPDATER_PUBKEY_SHA256
    assert CANONICAL_API_URL in tauri_config["app"]["security"]["csp"]
    assert "nyptid-studio.fly.dev/api/desktop/updater" not in json.dumps(tauri_config)
    assert "1.0.0 and 1.0.1 require this one manual installer" in backend.DESKTOP_RELEASE_NOTES
    desktop_release_source = (
        ROOT / "ViralShorts-App" / "src" / "studio" / "lib" / "desktopRelease.ts"
    ).read_text(encoding="utf-8")
    assert "compareVersions(currentVersion, '1.0.2') < 0" in desktop_release_source


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
