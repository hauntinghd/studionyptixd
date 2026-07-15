from __future__ import annotations

import os
import json
import hashlib
import threading
from pathlib import Path
from typing import Any

import pytest

from studio_agent import runpod_storage


DISPATCH_ID = "rpd_" + "a" * 40


class FakeS3:
    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects = dict(objects or {})
        self.uploads: list[tuple[str, str, str]] = []
        self.downloads: list[tuple[str, str, str]] = []
        self.list_calls: list[dict[str, Any]] = []
        self.operations: list[tuple[str, str]] = []

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        self.uploads.append((filename, bucket, key))
        self.operations.append(("upload_file", key))
        self.objects[key] = Path(filename).read_bytes()

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, **_kwargs: Any) -> None:
        self.operations.append(("put_object", Key))
        self.objects[Key] = bytes(Body)

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        self.list_calls.append(dict(kwargs))
        prefix = str(kwargs["Prefix"])
        return {
            "Contents": [
                {"Key": key, "Size": len(value)}
                for key, value in sorted(self.objects.items())
                if key.startswith(prefix)
            ],
            "IsTruncated": False,
        }

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        self.downloads.append((bucket, key, filename))
        Path(filename).write_bytes(self.objects[key])


@pytest.fixture(autouse=True)
def storage_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("RUNPOD_NETWORK_VOLUME_ID", "volume123")
    monkeypatch.setenv("RUNPOD_NETWORK_VOLUME_REGION", "EU-RO-1")
    monkeypatch.setenv("RUNPOD_S3_ACCESS_KEY_ID", "runpod-s3-access")
    monkeypatch.setenv("RUNPOD_S3_SECRET_ACCESS_KEY", "runpod-s3-secret")
    monkeypatch.setenv("SKELETON_AI_OUTPUT_ROOT", str(tmp_path / "shorts"))
    monkeypatch.setenv("LF_OUTPUT_ROOT", str(tmp_path / "longform"))
    monkeypatch.setenv("RUNPOD_STORAGE_LEDGER_DIR", str(tmp_path / "ledger"))
    monkeypatch.delenv("RUNPOD_S3_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("RUNPOD_S3_ENDPOINT", raising=False)
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)


def test_stage_short_workspace_uses_exact_prefix_and_skips_transient_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "shorts" / "job_123"
    (workspace / "stills").mkdir(parents=True)
    (workspace / "job_spec.json").write_text('{"topic":"skeleton"}', encoding="utf-8")
    (workspace / "stills" / "scene_001.png").write_bytes(b"png")
    (workspace / "heartbeat.txt").write_text("alive", encoding="utf-8")
    (workspace / ".animate.running").write_text("1", encoding="utf-8")
    (workspace / "state.json.tmp").write_text("partial", encoding="utf-8")
    fake = FakeS3()
    monkeypatch.setattr(runpod_storage, "_s3_client", lambda _config: fake)

    result = runpod_storage.stage_job_workspace("job_123", "shortform")

    assert result["remote_prefix"] == "studio/skeleton_ai/output/job_123"
    assert result["files_uploaded"] == 2
    assert {item[2] for item in fake.uploads} == {
        "studio/skeleton_ai/output/job_123/job_spec.json",
        "studio/skeleton_ai/output/job_123/stills/scene_001.png",
    }
    assert all(item[1] == "volume123" for item in fake.uploads)
    assert fake.operations[-1] == (
        "put_object",
        "studio/skeleton_ai/output/job_123/.studio-runpod-input-manifest.json",
    )
    manifest = json.loads(fake.objects[fake.operations[-1][1]])
    assert [entry["path"] for entry in manifest["files"]] == [
        "job_spec.json",
        "stills/scene_001.png",
    ]


def test_stage_longform_workspace_uses_longform_prefix(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "longform" / "abc123"
    workspace.mkdir(parents=True)
    (workspace / "state.json").write_text("{}", encoding="utf-8")
    fake = FakeS3()
    monkeypatch.setattr(runpod_storage, "_s3_client", lambda _config: fake)

    result = runpod_storage.stage_job_workspace("abc123", "longform")

    assert result["remote_prefix"] == "studio/long_form/abc123"
    assert [item[2] for item in fake.uploads] == ["studio/long_form/abc123/state.json"]


def test_terminal_sync_downloads_atomically_once_and_replays_receipt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    prefix = "studio/skeleton_ai/output/job123"
    fake = FakeS3(
        {
            f"{prefix}/result.json": b'{"status":"complete"}',
            f"{prefix}/stills/scene_001.png": b"image",
            f"{prefix}/heartbeat.txt": b"transient",
            "studio/skeleton_ai/output/another/result.json": b"wrong-job",
        }
    )
    monkeypatch.setattr(runpod_storage, "_s3_client", lambda _config: fake)

    first = runpod_storage.sync_job_workspace("job123", "shortform", DISPATCH_ID)
    second = runpod_storage.sync_job_workspace("job123", "shortform", DISPATCH_ID)

    assert first["status"] == "synced"
    assert first["files_downloaded"] == 2
    assert second["status"] == "already_synced"
    assert second["idempotent_replay"] is True
    assert second["files_transferred_this_call"] == 0
    assert len(fake.list_calls) == 1
    assert len(fake.downloads) == 2
    workspace = tmp_path / "shorts" / "job123"
    assert (workspace / "result.json").read_bytes() == b'{"status":"complete"}'
    assert (workspace / "stills" / "scene_001.png").read_bytes() == b"image"
    assert not (workspace / "heartbeat.txt").exists()
    assert not list(workspace.rglob("*.tmp"))


def test_concurrent_sync_returns_pending_without_duplicate_downloads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    prefix = "studio/long_form/job999"
    entered = threading.Event()
    release = threading.Event()

    class BlockingS3(FakeS3):
        def download_file(self, bucket: str, key: str, filename: str) -> None:
            entered.set()
            assert release.wait(timeout=5)
            super().download_file(bucket, key, filename)

    fake = BlockingS3({f"{prefix}/state.json": b"{}"})
    monkeypatch.setattr(runpod_storage, "_s3_client", lambda _config: fake)
    owner_result: list[dict[str, Any]] = []

    owner = threading.Thread(
        target=lambda: owner_result.append(
            runpod_storage.sync_job_workspace("job999", "longform", DISPATCH_ID)
        )
    )
    owner.start()
    assert entered.wait(timeout=5)

    concurrent = runpod_storage.sync_job_workspace("job999", "longform", DISPATCH_ID)
    assert concurrent["status"] == "sync_pending"
    assert concurrent["files_transferred_this_call"] == 0

    release.set()
    owner.join(timeout=5)
    assert not owner.is_alive()
    assert owner_result[0]["status"] == "synced"
    assert len(fake.downloads) == 1


def test_receipt_failure_is_retry_safe_after_an_atomic_download(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prefix = "studio/long_form/job999"
    fake = FakeS3({f"{prefix}/state.json": b"{}"})
    monkeypatch.setattr(runpod_storage, "_s3_client", lambda _config: fake)
    real_write = runpod_storage._atomic_write_json
    attempts = 0

    def fail_once(*args: Any, **kwargs: Any) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("disk full")
        real_write(*args, **kwargs)

    monkeypatch.setattr(runpod_storage, "_atomic_write_json", fail_once)

    with pytest.raises(runpod_storage.RunPodStorageTransferError):
        runpod_storage.sync_job_workspace("job999", "longform", DISPATCH_ID)
    retry = runpod_storage.sync_job_workspace("job999", "longform", DISPATCH_ID)

    assert retry["status"] == "synced"
    assert len(fake.downloads) == 2


@pytest.mark.parametrize(
    "job_id",
    ["", "../escape", "job/escape", r"job\\escape", ".", "..", "job id"],
)
def test_job_id_traversal_and_unsafe_values_are_rejected(job_id: str) -> None:
    with pytest.raises(runpod_storage.RunPodStoragePolicyError):
        runpod_storage.stage_job_workspace(job_id, "shortform")


def test_remote_traversal_is_rejected_and_claim_is_retriable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    prefix = "studio/skeleton_ai/output/job123"
    fake = FakeS3({f"{prefix}/../outside.txt": b"escape"})
    monkeypatch.setattr(runpod_storage, "_s3_client", lambda _config: fake)

    with pytest.raises(runpod_storage.RunPodStoragePolicyError, match="traversing"):
        runpod_storage.sync_job_workspace("job123", "shortform", DISPATCH_ID)

    assert not (tmp_path / "shorts" / "outside.txt").exists()
    assert not (tmp_path / "outside.txt").exists()
    assert not (tmp_path / "ledger" / f"{DISPATCH_ID}.claim.json").exists()
    assert fake.downloads == []


def test_upload_never_follows_a_workspace_symlink(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "shorts" / "job123"
    workspace.mkdir(parents=True)
    outside = tmp_path / "secret.txt"
    outside.write_text("secret", encoding="utf-8")
    link = workspace / "reference.txt"
    try:
        os.symlink(outside, link)
    except (OSError, NotImplementedError):
        pytest.skip("symbolic links are unavailable on this host")
    (workspace / "result.json").write_text("{}", encoding="utf-8")
    fake = FakeS3()
    monkeypatch.setattr(runpod_storage, "_s3_client", lambda _config: fake)

    runpod_storage.stage_job_workspace("job123", "shortform")

    assert [item[2] for item in fake.uploads] == [
        "studio/skeleton_ai/output/job123/result.json"
    ]


def test_missing_config_fails_without_creating_a_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("RUNPOD_NETWORK_VOLUME_ID")
    monkeypatch.delenv("RUNPOD_S3_ACCESS_KEY_ID")
    monkeypatch.delenv("RUNPOD_S3_SECRET_ACCESS_KEY")
    called = False

    def unexpected_client(_config: Any) -> Any:
        nonlocal called
        called = True
        raise AssertionError("client should not be created")

    monkeypatch.setattr(runpod_storage, "_s3_client", unexpected_client)

    assert runpod_storage.configured() is False
    with pytest.raises(runpod_storage.RunPodStorageConfigurationError, match="not configured"):
        runpod_storage.assert_configured()
    with pytest.raises(runpod_storage.RunPodStorageConfigurationError):
        runpod_storage.sync_job_workspace("job123", "shortform", DISPATCH_ID)
    assert called is False


def test_generic_aws_credentials_are_never_reused_for_runpod(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RUNPOD_S3_ACCESS_KEY_ID")
    monkeypatch.delenv("RUNPOD_S3_SECRET_ACCESS_KEY")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "aws-alias-access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws-alias-secret")
    monkeypatch.setattr(runpod_storage, "_load_boto3", lambda: object())

    with pytest.raises(runpod_storage.RunPodStorageConfigurationError, match="RUNPOD_S3"):
        runpod_storage._storage_config()
    assert runpod_storage.configured() is False


def test_configured_requires_s3_client_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runpod_storage,
        "_load_boto3",
        lambda: (_ for _ in ()).throw(
            runpod_storage.RunPodStorageConfigurationError(
                "boto3 is required for RunPod S3 sync"
            )
        ),
    )

    assert runpod_storage.configured() is False
    with pytest.raises(runpod_storage.RunPodStorageConfigurationError, match="boto3"):
        runpod_storage.assert_configured()


def test_worker_manifest_reconciliation_removes_stale_files(tmp_path: Path) -> None:
    workspace = tmp_path / "shorts" / "job123"
    workspace.mkdir(parents=True)
    expected = workspace / "state.json"
    expected.write_bytes(b'{"status":"ready"}')
    stale = workspace / "stills" / "deleted-scene.png"
    stale.parent.mkdir()
    stale.write_bytes(b"stale")
    manifest = {
        "schema": "nyptid.studio.workspace.v1",
        "job_id": "job123",
        "kind": "shortform",
        "files": [
            {
                "path": "state.json",
                "size": expected.stat().st_size,
                "sha256": hashlib.sha256(expected.read_bytes()).hexdigest(),
            }
        ],
    }
    (workspace / ".studio-runpod-input-manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    result = runpod_storage.reconcile_staged_workspace("job123", "shortform")

    assert result["files_verified"] == 1
    assert result["stale_files_removed"] == 1
    assert expected.is_file()
    assert not stale.exists()
