"""Safe S3 synchronization for Studio workspaces on a RunPod network volume.

RunPod exposes a network volume as an S3-compatible bucket whose name is the
volume ID.  This module only copies one validated Studio job workspace to or
from the two production prefixes used by the RunPod worker.  It deliberately
has no delete operation and does not expose a general-purpose bucket API.

Terminal job polling can happen concurrently across API processes.  A durable
``O_EXCL`` claim plus receipt makes a completed dispatch download at most once;
other callers return ``sync_pending`` without starting another transfer.
"""
from __future__ import annotations

import json
import hashlib
import os
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterator
from urllib.parse import urlparse


_REPO_ROOT = Path(__file__).resolve().parents[1]
_JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
_DISPATCH_ID_RE = re.compile(r"^rpd_[0-9a-f]{40}$")
_VOLUME_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_REGION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9-]{1,31}$")
_KINDS = frozenset({"shortform", "longform"})
_INPUT_MANIFEST_NAME = ".studio-runpod-input-manifest.json"
_INPUT_MANIFEST_SCHEMA = "nyptid.studio.workspace.v1"

_TRANSIENT_DIR_NAMES = frozenset(
    {
        ".claim",
        ".claims",
        ".lock",
        ".locks",
        ".tmp",
        "claim",
        "claims",
        "lock",
        "locks",
        "tmp",
    }
)
_TRANSIENT_EXACT_NAMES = frozenset(
    {
        ".claim",
        ".tmp",
        ".lock",
        "claim.json",
        "heartbeat",
        "heartbeat.json",
        "heartbeat.txt",
        "lock.json",
        "reclaiming",
        _INPUT_MANIFEST_NAME,
    }
)
_TRANSIENT_SUFFIXES = (
    ".claim",
    ".claim.json",
    ".lck",
    ".lock",
    ".part",
    ".partial",
    ".running",
    ".temp",
    ".tmp",
)


class RunPodStorageError(RuntimeError):
    """Base error for RunPod workspace storage."""


class RunPodStorageConfigurationError(RunPodStorageError):
    """Required S3 credentials or volume configuration are missing."""


class RunPodStoragePolicyError(RunPodStorageError):
    """A job, key, or filesystem path violated the storage boundary."""


class RunPodStorageTransferError(RunPodStorageError):
    """The S3-compatible service failed during a copy operation."""


@dataclass(frozen=True)
class _StorageConfig:
    bucket: str
    region: str
    endpoint_url: str
    access_key_id: str
    secret_access_key: str


def _env(name: str) -> str:
    return str(os.getenv(name) or "").strip()


def _access_key_id() -> str:
    return _env("RUNPOD_S3_ACCESS_KEY_ID")


def _secret_access_key() -> str:
    return _env("RUNPOD_S3_SECRET_ACCESS_KEY")


def _endpoint_for_region(region: str) -> str:
    configured_endpoint = _env("RUNPOD_S3_ENDPOINT_URL") or _env("RUNPOD_S3_ENDPOINT")
    return configured_endpoint or f"https://s3api-{region.lower()}.runpod.io/"


def _storage_config() -> _StorageConfig:
    volume_id = _env("RUNPOD_NETWORK_VOLUME_ID")
    region = _env("RUNPOD_NETWORK_VOLUME_REGION") or "EU-RO-1"
    access_key_id = _access_key_id()
    secret_access_key = _secret_access_key()

    missing: list[str] = []
    if not volume_id:
        missing.append("RUNPOD_NETWORK_VOLUME_ID")
    if not access_key_id:
        missing.append("RUNPOD_S3_ACCESS_KEY_ID")
    if not secret_access_key:
        missing.append("RUNPOD_S3_SECRET_ACCESS_KEY")
    if missing:
        raise RunPodStorageConfigurationError(
            "RunPod network-volume S3 is not configured: " + ", ".join(missing)
        )
    if not _VOLUME_ID_RE.fullmatch(volume_id):
        raise RunPodStorageConfigurationError("RUNPOD_NETWORK_VOLUME_ID is malformed")
    if not _REGION_RE.fullmatch(region):
        raise RunPodStorageConfigurationError("RUNPOD_NETWORK_VOLUME_REGION is malformed")

    endpoint_url = _endpoint_for_region(region)
    parsed = urlparse(endpoint_url)
    if (
        parsed.scheme.lower() != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise RunPodStorageConfigurationError(
            "RunPod S3 endpoint must be an HTTPS origin without credentials, query, or fragment"
        )
    return _StorageConfig(
        bucket=volume_id,
        region=region,
        endpoint_url=endpoint_url,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
    )


def configured() -> bool:
    """Return whether credentials and the S3 client dependency are usable."""

    try:
        assert_configured()
    except RunPodStorageConfigurationError:
        return False
    return True


def assert_configured() -> None:
    """Raise a non-secret, actionable error when storage is not configured."""

    _storage_config()
    _load_boto3()


def _load_boto3() -> Any:
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover - dependency is in requirements.txt
        raise RunPodStorageConfigurationError("boto3 is required for RunPod S3 sync") from exc
    return boto3


def _s3_client(config: _StorageConfig) -> Any:
    boto3 = _load_boto3()
    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        region_name=config.region,
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
    )


def _validated_job_id(job_id: str) -> str:
    normalized = str(job_id or "").strip()
    if not _JOB_ID_RE.fullmatch(normalized):
        raise RunPodStoragePolicyError(
            "job_id must contain only letters, numbers, underscores, or hyphens"
        )
    return normalized


def _validated_kind(kind: str) -> str:
    normalized = str(kind or "").strip().lower()
    if normalized not in _KINDS:
        raise RunPodStoragePolicyError("kind must be 'shortform' or 'longform'")
    return normalized


def _validated_dispatch_id(dispatch_id: str) -> str:
    normalized = str(dispatch_id or "").strip().lower()
    if not _DISPATCH_ID_RE.fullmatch(normalized):
        raise RunPodStoragePolicyError("dispatch_id is malformed")
    return normalized


def _local_root(kind: str) -> Path:
    if kind == "shortform":
        from studio_agent.fs_paths import skeleton_output_root

        return skeleton_output_root()
    override = _env("LF_OUTPUT_ROOT")
    if override:
        return Path(override).expanduser()
    if os.name == "posix" and Path("/var/data").is_dir():
        return Path("/var/data/long_form")
    return _REPO_ROOT / "long_form" / "output"


def _workspace(job_id: str, kind: str, *, require_existing: bool) -> Path:
    root = _local_root(kind).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    if root.is_symlink():
        raise RunPodStoragePolicyError("workspace root may not be a symbolic link")
    root_resolved = root.resolve()
    candidate = root / job_id
    if candidate.is_symlink():
        raise RunPodStoragePolicyError("job workspace may not be a symbolic link")
    resolved = candidate.resolve(strict=False)
    try:
        resolved.relative_to(root_resolved)
    except ValueError as exc:
        raise RunPodStoragePolicyError("job workspace escapes its configured root") from exc
    if require_existing and not resolved.is_dir():
        raise RunPodStoragePolicyError(f"{kind} workspace does not exist for job {job_id}")
    if require_existing and resolved.is_symlink():
        raise RunPodStoragePolicyError("job workspace may not be a symbolic link")
    return resolved


def _remote_prefix(job_id: str, kind: str) -> str:
    if kind == "shortform":
        return f"studio/skeleton_ai/output/{job_id}"
    return f"studio/long_form/{job_id}"


def _is_transient(relative: PurePosixPath) -> bool:
    parts = tuple(part.lower() for part in relative.parts)
    if any(part in _TRANSIENT_DIR_NAMES for part in parts[:-1]):
        return True
    name = parts[-1] if parts else ""
    return (
        name in _TRANSIENT_EXACT_NAMES
        or name.startswith("heartbeat.")
        or name.endswith("~")
        or any(name.endswith(suffix) for suffix in _TRANSIENT_SUFFIXES)
    )


def _iter_upload_files(workspace: Path) -> Iterator[tuple[Path, PurePosixPath]]:
    workspace_resolved = workspace.resolve()
    for directory, dirnames, filenames in os.walk(workspace, topdown=True, followlinks=False):
        directory_path = Path(directory)
        safe_dirs: list[str] = []
        for dirname in sorted(dirnames):
            child = directory_path / dirname
            relative = PurePosixPath(child.relative_to(workspace).as_posix())
            if (
                child.is_symlink()
                or dirname.lower() in _TRANSIENT_DIR_NAMES
                or _is_transient(relative)
            ):
                continue
            safe_dirs.append(dirname)
        dirnames[:] = safe_dirs
        for filename in sorted(filenames):
            source = directory_path / filename
            relative = PurePosixPath(source.relative_to(workspace).as_posix())
            if source.is_symlink() or _is_transient(relative) or not source.is_file():
                continue
            try:
                source.resolve(strict=True).relative_to(workspace_resolved)
            except (OSError, ValueError) as exc:
                raise RunPodStoragePolicyError("workspace file escapes through a symbolic link") from exc
            yield source, relative


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _manifest_bytes(job_id: str, kind: str, files: list[tuple[Path, PurePosixPath]]) -> bytes:
    entries: list[dict[str, Any]] = []
    for source, relative in files:
        entries.append(
            {
                "path": relative.as_posix(),
                "size": source.stat().st_size,
                "sha256": _file_sha256(source),
            }
        )
    payload = {
        "schema": _INPUT_MANIFEST_SCHEMA,
        "job_id": job_id,
        "kind": kind,
        "generated_at": time.time(),
        "files": entries,
    }
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def stage_job_workspace(job_id: str, kind: str) -> dict[str, Any]:
    """Upload one exact workspace snapshot, committing its manifest last."""

    normalized_job_id = _validated_job_id(job_id)
    normalized_kind = _validated_kind(kind)
    config = _storage_config()
    workspace = _workspace(normalized_job_id, normalized_kind, require_existing=True)
    prefix = _remote_prefix(normalized_job_id, normalized_kind)
    client = _s3_client(config)
    files = list(_iter_upload_files(workspace))
    manifest = _manifest_bytes(normalized_job_id, normalized_kind, files)
    files_uploaded = 0
    bytes_uploaded = 0
    try:
        for source, relative in files:
            key = f"{prefix}/{relative.as_posix()}"
            client.upload_file(str(source), config.bucket, key)
            files_uploaded += 1
            try:
                bytes_uploaded += source.stat().st_size
            except OSError:
                pass
        # This object is the commit marker. The worker refuses to execute an
        # existing-job command without it and validates every declared hash,
        # so a partial upload can never be mistaken for a complete workspace.
        client.put_object(
            Bucket=config.bucket,
            Key=f"{prefix}/{_INPUT_MANIFEST_NAME}",
            Body=manifest,
            ContentType="application/json",
        )
    except RunPodStoragePolicyError:
        raise
    except Exception as exc:
        raise RunPodStorageTransferError(
            f"RunPod workspace upload failed for {normalized_kind} job {normalized_job_id}"
        ) from exc
    return {
        "ok": True,
        "status": "staged",
        "job_id": normalized_job_id,
        "kind": normalized_kind,
        "bucket": config.bucket,
        "remote_prefix": prefix,
        "files_uploaded": files_uploaded,
        "bytes_uploaded": bytes_uploaded,
        "manifest_uploaded_last": True,
        "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
    }


def _validated_manifest_relative(raw: Any) -> PurePosixPath:
    text = str(raw or "")
    if not text or text.startswith("/") or "\\" in text or "\x00" in text:
        raise RunPodStoragePolicyError("RunPod workspace manifest contains an unsafe path")
    parts = text.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise RunPodStoragePolicyError("RunPod workspace manifest contains a traversing path")
    relative = PurePosixPath(*parts)
    if _is_transient(relative):
        raise RunPodStoragePolicyError("RunPod workspace manifest contains an internal path")
    return relative


def reconcile_staged_workspace(job_id: str, kind: str) -> dict[str, Any]:
    """Make the mounted worker workspace exactly match the committed manifest."""

    normalized_job_id = _validated_job_id(job_id)
    normalized_kind = _validated_kind(kind)
    workspace = _workspace(normalized_job_id, normalized_kind, require_existing=True)
    manifest_path = workspace / _INPUT_MANIFEST_NAME
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise RunPodStoragePolicyError("Committed RunPod workspace manifest is missing")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RunPodStoragePolicyError("Committed RunPod workspace manifest is unreadable") from exc
    if not isinstance(manifest, dict) or (
        manifest.get("schema") != _INPUT_MANIFEST_SCHEMA
        or str(manifest.get("job_id") or "") != normalized_job_id
        or str(manifest.get("kind") or "") != normalized_kind
        or not isinstance(manifest.get("files"), list)
    ):
        raise RunPodStoragePolicyError("Committed RunPod workspace manifest identity is invalid")

    allowed: set[str] = set()
    for entry in manifest["files"]:
        if not isinstance(entry, dict):
            raise RunPodStoragePolicyError("Committed RunPod workspace manifest entry is invalid")
        relative = _validated_manifest_relative(entry.get("path"))
        relative_text = relative.as_posix()
        if relative_text in allowed:
            raise RunPodStoragePolicyError("Committed RunPod workspace manifest has duplicate paths")
        allowed.add(relative_text)
        source = workspace.joinpath(*relative.parts)
        if source.is_symlink() or not source.is_file():
            raise RunPodStoragePolicyError("Committed RunPod workspace file is missing")
        try:
            expected_size = int(entry.get("size"))
        except (TypeError, ValueError) as exc:
            raise RunPodStoragePolicyError("Committed RunPod workspace file size is invalid") from exc
        expected_hash = str(entry.get("sha256") or "").strip().lower()
        if source.stat().st_size != expected_size or _file_sha256(source) != expected_hash:
            raise RunPodStoragePolicyError("Committed RunPod workspace file failed integrity validation")

    removed = 0
    for directory, dirnames, filenames in os.walk(workspace, topdown=False, followlinks=False):
        directory_path = Path(directory)
        for filename in filenames:
            candidate = directory_path / filename
            relative_text = candidate.relative_to(workspace).as_posix()
            if relative_text == _INPUT_MANIFEST_NAME or relative_text in allowed:
                continue
            if candidate.is_symlink():
                raise RunPodStoragePolicyError("Mounted RunPod workspace contains a symbolic link")
            candidate.unlink()
            removed += 1
        for dirname in dirnames:
            candidate_dir = directory_path / dirname
            if candidate_dir.is_symlink():
                raise RunPodStoragePolicyError("Mounted RunPod workspace contains a symbolic link")
            try:
                candidate_dir.rmdir()
            except OSError:
                pass
    return {
        "ok": True,
        "status": "manifest_reconciled",
        "job_id": normalized_job_id,
        "kind": normalized_kind,
        "files_verified": len(allowed),
        "stale_files_removed": removed,
    }


def _ledger_dir() -> Path:
    configured_root = _env("RUNPOD_STORAGE_LEDGER_DIR")
    if configured_root:
        root = Path(configured_root).expanduser()
    else:
        app_data = _env("APP_DATA_DIR")
        root = (
            Path(app_data).expanduser() / "runpod_storage_sync" / "control_plane"
            if app_data
            else _REPO_ROOT / "data" / "runpod_storage_sync" / "control_plane"
        )
    root.mkdir(parents=True, exist_ok=True)
    return root


def _ledger_paths(dispatch_id: str) -> tuple[Path, Path]:
    root = _ledger_dir()
    return root / f"{dispatch_id}.claim.json", root / f"{dispatch_id}.receipt.json"


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def _matching_ledger_record(
    record: dict[str, Any] | None,
    *,
    dispatch_id: str,
    job_id: str,
    kind: str,
) -> bool:
    if record is None:
        return False
    if (
        str(record.get("dispatch_id") or "") != dispatch_id
        or str(record.get("job_id") or "") != job_id
        or str(record.get("kind") or "") != kind
    ):
        raise RunPodStoragePolicyError("dispatch_id is already bound to another workspace sync")
    return True


def _claim_sync(dispatch_id: str, job_id: str, kind: str) -> tuple[bool, dict[str, Any] | None]:
    claim_path, receipt_path = _ledger_paths(dispatch_id)
    receipt = _read_json(receipt_path)
    if _matching_ledger_record(
        receipt, dispatch_id=dispatch_id, job_id=job_id, kind=kind
    ):
        return False, receipt
    claim_payload = {
        "dispatch_id": dispatch_id,
        "job_id": job_id,
        "kind": kind,
        "pid": os.getpid(),
        "claimed_at": time.time(),
    }
    try:
        descriptor = os.open(str(claim_path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        # A receipt may have landed between our first read and the failed claim.
        receipt = _read_json(receipt_path)
        if _matching_ledger_record(
            receipt, dispatch_id=dispatch_id, job_id=job_id, kind=kind
        ):
            return False, receipt
        claim = _read_json(claim_path)
        _matching_ledger_record(claim, dispatch_id=dispatch_id, job_id=job_id, kind=kind)
        return False, None
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(claim_payload, handle, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        try:
            claim_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    return True, None


def _validated_remote_relative(key: str, prefix: str) -> PurePosixPath | None:
    expected = prefix + "/"
    if not key.startswith(expected):
        raise RunPodStoragePolicyError("RunPod returned a key outside the requested job prefix")
    raw = key[len(expected) :]
    if not raw or raw.endswith("/"):
        return None
    if "\\" in raw or "\x00" in raw or raw.startswith("/"):
        raise RunPodStoragePolicyError("RunPod returned an unsafe workspace key")
    raw_parts = raw.split("/")
    if any(part in {"", ".", ".."} for part in raw_parts):
        raise RunPodStoragePolicyError("RunPod returned a traversing workspace key")
    if os.name == "nt" and any(any(char in part for char in '<>:"|?*') for part in raw_parts):
        raise RunPodStoragePolicyError("RunPod returned a key invalid on this filesystem")
    relative = PurePosixPath(*raw_parts)
    if _is_transient(relative):
        return None
    return relative


def _safe_download_destination(workspace: Path, relative: PurePosixPath) -> Path:
    workspace.mkdir(parents=True, exist_ok=True)
    if workspace.is_symlink():
        raise RunPodStoragePolicyError("job workspace may not be a symbolic link")
    current = workspace
    for part in relative.parts[:-1]:
        current = current / part
        if current.is_symlink():
            raise RunPodStoragePolicyError("download path crosses a symbolic link")
        if current.exists() and not current.is_dir():
            raise RunPodStoragePolicyError("download path collides with an existing file")
        current.mkdir(exist_ok=True)
    destination = workspace.joinpath(*relative.parts)
    if destination.is_symlink():
        raise RunPodStoragePolicyError("download destination may not be a symbolic link")
    try:
        destination.resolve(strict=False).relative_to(workspace.resolve())
    except ValueError as exc:
        raise RunPodStoragePolicyError("download destination escapes the job workspace") from exc
    return destination


def _iter_remote_objects(client: Any, bucket: str, prefix: str) -> Iterator[dict[str, Any]]:
    continuation: str | None = None
    while True:
        arguments: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix + "/"}
        if continuation:
            arguments["ContinuationToken"] = continuation
        response = client.list_objects_v2(**arguments)
        if not isinstance(response, dict):
            raise RunPodStorageTransferError("RunPod S3 returned an invalid object listing")
        contents = response.get("Contents") or []
        if not isinstance(contents, list):
            raise RunPodStorageTransferError("RunPod S3 returned an invalid object listing")
        for item in contents:
            if isinstance(item, dict):
                yield item
        if not bool(response.get("IsTruncated")):
            return
        next_token = str(response.get("NextContinuationToken") or "").strip()
        if not next_token or next_token == continuation:
            raise RunPodStorageTransferError("RunPod S3 pagination did not advance")
        continuation = next_token


def _download_atomic(
    client: Any,
    *,
    bucket: str,
    key: str,
    destination: Path,
    dispatch_id: str,
) -> None:
    temporary = destination.with_name(
        f".{destination.name}.{dispatch_id[4:12]}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    try:
        client.download_file(bucket, key, str(temporary))
        if temporary.is_symlink() or not temporary.is_file():
            raise RunPodStorageTransferError("RunPod S3 did not produce a regular download file")
        # Windows rejects fsync on a read-only descriptor; r+b is portable and
        # does not alter the already downloaded content.
        with temporary.open("r+b") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass


def sync_job_workspace(job_id: str, kind: str, dispatch_id: str) -> dict[str, Any]:
    """Download a terminal RunPod workspace at most once for one dispatch."""

    normalized_job_id = _validated_job_id(job_id)
    normalized_kind = _validated_kind(kind)
    normalized_dispatch_id = _validated_dispatch_id(dispatch_id)
    config = _storage_config()
    workspace = _workspace(normalized_job_id, normalized_kind, require_existing=False)
    prefix = _remote_prefix(normalized_job_id, normalized_kind)
    claimed, prior = _claim_sync(
        normalized_dispatch_id, normalized_job_id, normalized_kind
    )
    if prior is not None:
        replay = dict(prior)
        replay.update(
            {
                "status": "already_synced",
                "idempotent_replay": True,
                "files_transferred_this_call": 0,
            }
        )
        return replay
    if not claimed:
        return {
            "ok": True,
            "status": "sync_pending",
            "pending": True,
            "idempotent_replay": True,
            "dispatch_id": normalized_dispatch_id,
            "job_id": normalized_job_id,
            "kind": normalized_kind,
            "remote_prefix": prefix,
            "files_transferred_this_call": 0,
        }

    claim_path, receipt_path = _ledger_paths(normalized_dispatch_id)
    files_downloaded = 0
    bytes_downloaded = 0
    try:
        client = _s3_client(config)
        for item in _iter_remote_objects(client, config.bucket, prefix):
            key = str(item.get("Key") or "")
            relative = _validated_remote_relative(key, prefix)
            if relative is None:
                continue
            destination = _safe_download_destination(workspace, relative)
            _download_atomic(
                client,
                bucket=config.bucket,
                key=key,
                destination=destination,
                dispatch_id=normalized_dispatch_id,
            )
            files_downloaded += 1
            try:
                bytes_downloaded += destination.stat().st_size
            except OSError:
                try:
                    bytes_downloaded += max(0, int(item.get("Size") or 0))
                except (TypeError, ValueError):
                    pass
        receipt = {
            "ok": True,
            "status": "synced",
            "pending": False,
            "idempotent_replay": False,
            "dispatch_id": normalized_dispatch_id,
            "job_id": normalized_job_id,
            "kind": normalized_kind,
            "bucket": config.bucket,
            "remote_prefix": prefix,
            "files_downloaded": files_downloaded,
            "files_transferred_this_call": files_downloaded,
            "bytes_downloaded": bytes_downloaded,
            "synced_at": time.time(),
        }
        _atomic_write_json(receipt_path, receipt)
        return receipt
    except (RunPodStoragePolicyError, RunPodStorageTransferError):
        # Each file lands through an atomic replace. Repeating an interrupted
        # terminal download is safe and necessary; retaining this claim would
        # strand the job in sync_pending forever.
        try:
            claim_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    except Exception as exc:
        try:
            claim_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise RunPodStorageTransferError(
            f"RunPod workspace download failed for {normalized_kind} job {normalized_job_id}"
        ) from exc


__all__ = [
    "RunPodStorageConfigurationError",
    "RunPodStorageError",
    "RunPodStoragePolicyError",
    "RunPodStorageTransferError",
    "assert_configured",
    "configured",
    "reconcile_staged_workspace",
    "stage_job_workspace",
    "sync_job_workspace",
]
