#!/usr/bin/env python3
"""Deterministic full-file manifests and root-owned migration attestations."""

from __future__ import annotations

import argparse
import base64
import hashlib
import os
import shutil
import re
import stat
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterator


MAGIC = b"NYPTID_STUDIO_DATA_MANIFEST_V1\n"
LINE_RE = re.compile(rb"^([0-9a-f]{64})\t([0-9]+)\t([A-Za-z0-9_-]+={0,2})\n$")
ATTESTATION_FORMAT = "1"
ALLOWED_ROLES = {"migrated-data-ready", "reverse-destination"}


class ManifestError(RuntimeError):
    pass


@dataclass(frozen=True)
class Summary:
    manifest_sha256: str
    file_count: int
    total_bytes: int


def _require_root() -> None:
    if hasattr(os, "geteuid") and os.geteuid() != 0:
        raise ManifestError("this command must run as root")


def _full_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _assert_root_private_file(path: Path) -> None:
    if not path.is_file() or path.is_symlink():
        raise ManifestError(f"required regular file is missing: {path}")
    info = path.stat()
    if hasattr(os, "geteuid") and info.st_uid != 0:
        raise ManifestError(f"file must be owned by root: {path}")
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise ManifestError(f"file must not grant group/other permissions: {path}")


def _assert_safe_data_dir(path: Path) -> Path:
    if path.is_symlink():
        raise ManifestError(f"data directory cannot be a symlink: {path}")
    resolved = path.resolve(strict=True)
    if not resolved.is_dir():
        raise ManifestError(f"data directory is not a real directory: {path}")
    if str(resolved) not in {"/var/data", "/opt/studio/data", "/srv/studio/data", "/var/lib/studio/data"}:
        raise ManifestError(f"refusing unexpected data directory: {resolved}")
    return resolved


def _assert_output_outside_data(data_dir: Path, output: Path) -> None:
    resolved_output = output.resolve(strict=False)
    try:
        resolved_output.relative_to(data_dir)
    except ValueError:
        return
    raise ManifestError("manifest/attestation output cannot live inside the data tree")


def _iter_regular_files(root: Path) -> Iterator[tuple[Path, bytes]]:
    root_bytes = os.fsencode(root)
    files: list[tuple[Path, bytes]] = []

    def visit(directory: bytes, parts: tuple[bytes, ...]) -> None:
        entries = sorted(os.scandir(directory), key=lambda entry: entry.name)
        for entry in entries:
            entry_stat = entry.stat(follow_symlinks=False)
            name = entry.name
            rel_parts = (*parts, name)
            if stat.S_ISDIR(entry_stat.st_mode):
                visit(entry.path, rel_parts)
            elif stat.S_ISREG(entry_stat.st_mode):
                files.append((Path(os.fsdecode(entry.path)), b"/".join(rel_parts)))
            else:
                display = os.fsdecode(b"/".join(rel_parts))
                raise ManifestError(f"data tree contains a symlink or special file: {display}")

    visit(root_bytes, ())
    files.sort(key=lambda item: item[1])
    yield from files


def _hash_stable_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if nofollow:
        flags |= nofollow
    elif path.is_symlink():
        raise ManifestError(f"data tree contains a symlink: {path}")
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ManifestError(f"could not safely open data file: {path}") from exc
    with os.fdopen(descriptor, "rb", buffering=0) as handle:
        before = os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ManifestError(f"data tree entry is not a regular file: {path}")
        while True:
            chunk = handle.read(8 * 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        after = os.fstat(handle.fileno())
    identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if identity_before != identity_after:
        raise ManifestError(f"file changed while being hashed: {path}")
    return digest.hexdigest(), int(after.st_size)


def _write_manifest_stream(data_dir: Path, handle: BinaryIO) -> tuple[int, int]:
    file_count = 0
    total_bytes = 0
    handle.write(MAGIC)
    for full_path, relative_bytes in _iter_regular_files(data_dir):
        sha256, size = _hash_stable_file(full_path)
        encoded_path = base64.urlsafe_b64encode(relative_bytes)
        handle.write(sha256.encode("ascii"))
        handle.write(b"\t")
        handle.write(str(size).encode("ascii"))
        handle.write(b"\t")
        handle.write(encoded_path)
        handle.write(b"\n")
        file_count += 1
        total_bytes += size
    if file_count <= 0 or total_bytes <= 0:
        raise ManifestError("data tree is empty; refusing to create a readiness proof")
    return file_count, total_bytes


def create_manifest(data_dir: Path, manifest: Path) -> Summary:
    data_dir = _assert_safe_data_dir(data_dir)
    if manifest.is_symlink():
        raise ManifestError("data manifest output cannot be a symlink")
    if manifest.exists() and not manifest.is_file():
        raise ManifestError("data manifest output is not a regular file")
    manifest = manifest.resolve(strict=False)
    _assert_output_outside_data(data_dir, manifest)
    manifest.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{manifest.name}.", dir=manifest.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as handle:
            file_count, total_bytes = _write_manifest_stream(data_dir, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, manifest)
        os.chmod(manifest, 0o600)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        temporary.unlink(missing_ok=True)
        raise
    return Summary(_full_sha256(manifest), file_count, total_bytes)


def summarize_manifest(manifest: Path) -> Summary:
    _assert_root_private_file(manifest)
    file_count = 0
    total_bytes = 0
    previous_path: bytes | None = None
    with manifest.open("rb") as handle:
        if handle.readline() != MAGIC:
            raise ManifestError("unsupported or corrupt data manifest header")
        for line in handle:
            match = LINE_RE.fullmatch(line)
            if match is None:
                raise ManifestError("malformed data manifest entry")
            size = int(match.group(2))
            try:
                relative = base64.urlsafe_b64decode(match.group(3))
            except Exception as exc:
                raise ManifestError("invalid path encoding in data manifest") from exc
            if base64.urlsafe_b64encode(relative) != match.group(3):
                raise ManifestError("non-canonical path encoding in data manifest")
            components = relative.split(b"/")
            if not relative or relative.startswith(b"/") or any(part in {b"", b".", b".."} for part in components):
                raise ManifestError("unsafe path in data manifest")
            if previous_path is not None and relative <= previous_path:
                raise ManifestError("data manifest paths are not strictly sorted")
            previous_path = relative
            file_count += 1
            total_bytes += size
    if file_count <= 0 or total_bytes <= 0:
        raise ManifestError("data manifest describes an empty data tree")
    return Summary(_full_sha256(manifest), file_count, total_bytes)


def verify_tree(data_dir: Path, manifest: Path) -> Summary:
    data_dir = _assert_safe_data_dir(data_dir)
    if manifest.is_symlink():
        raise ManifestError("data manifest cannot be a symlink")
    manifest = manifest.resolve(strict=True)
    _assert_root_private_file(manifest)
    expected = summarize_manifest(manifest)
    fd, temporary_name = tempfile.mkstemp(prefix=".studio-data-verify.", dir=manifest.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as handle:
            actual_count, actual_bytes = _write_manifest_stream(data_dir, handle)
            handle.flush()
            os.fsync(handle.fileno())
        actual_sha = _full_sha256(temporary)
        if actual_sha != expected.manifest_sha256:
            raise ManifestError(
                "data tree does not match the source manifest "
                f"(actual files={actual_count}, bytes={actual_bytes})"
            )
    finally:
        temporary.unlink(missing_ok=True)
    return expected


def _parse_attestation(path: Path) -> dict[str, str]:
    _assert_root_private_file(path)
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line or "=" not in raw_line:
            raise ManifestError("malformed data attestation")
        key, value = raw_line.split("=", 1)
        if key in values or not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
            raise ManifestError("duplicate or invalid data attestation key")
        values[key] = value
    required = {
        "ATTESTATION_FORMAT",
        "ATTESTATION_ROLE",
        "VERIFIED",
        "DATA_DIR",
        "MANIFEST_PATH",
        "MANIFEST_SHA256",
        "FILE_COUNT",
        "TOTAL_BYTES",
        "MINIMUM_FILES",
        "MINIMUM_BYTES",
        "VERIFIED_AT_EPOCH",
    }
    if set(values) != required:
        raise ManifestError("data attestation keys do not match the strict contract")
    if values["ATTESTATION_FORMAT"] != ATTESTATION_FORMAT or values["VERIFIED"] != "1":
        raise ManifestError("data attestation is not verified")
    if values["ATTESTATION_ROLE"] not in ALLOWED_ROLES:
        raise ManifestError("data attestation role is invalid")
    for key in ("FILE_COUNT", "TOTAL_BYTES", "MINIMUM_FILES", "MINIMUM_BYTES", "VERIFIED_AT_EPOCH"):
        if not values[key].isdigit():
            raise ManifestError(f"data attestation has invalid {key}")
    if not re.fullmatch(r"[0-9a-f]{64}", values["MANIFEST_SHA256"]):
        raise ManifestError("data attestation manifest hash is invalid")
    return values


def _validate_attestation_metadata(
    path: Path,
    *,
    expected_role: str | None,
    expected_data_dir: Path | None,
) -> tuple[dict[str, str], Path, Summary]:
    values = _parse_attestation(path)
    if expected_role and values["ATTESTATION_ROLE"] != expected_role:
        raise ManifestError("data attestation role does not match")
    if expected_data_dir is not None:
        resolved_data = _assert_safe_data_dir(expected_data_dir)
        if values["DATA_DIR"] != str(resolved_data):
            raise ManifestError("data attestation targets a different data directory")
    manifest = Path(values["MANIFEST_PATH"])
    if not manifest.is_absolute():
        raise ManifestError("data attestation manifest path must be absolute")
    summary = summarize_manifest(manifest)
    if summary.manifest_sha256 != values["MANIFEST_SHA256"]:
        raise ManifestError("data attestation manifest hash does not match its file")
    if summary.file_count != int(values["FILE_COUNT"]) or summary.total_bytes != int(values["TOTAL_BYTES"]):
        raise ManifestError("data attestation count/bytes do not match its manifest")
    if summary.file_count < int(values["MINIMUM_FILES"]) or summary.total_bytes < int(values["MINIMUM_BYTES"]):
        raise ManifestError("data attestation falls below its migration safety floor")
    return values, manifest, summary


def write_attestation(
    data_dir: Path,
    manifest: Path,
    attestation: Path,
    *,
    role: str,
    minimum_files: int,
    minimum_bytes: int,
) -> Summary:
    if role not in ALLOWED_ROLES:
        raise ManifestError("unsupported attestation role")
    if minimum_files <= 0 or minimum_bytes <= 0:
        raise ManifestError("migration safety floors must both be positive")
    data_dir = _assert_safe_data_dir(data_dir)
    if manifest.is_symlink():
        raise ManifestError("data manifest cannot be a symlink")
    manifest = manifest.resolve(strict=True)
    if attestation.is_symlink():
        raise ManifestError("data attestation cannot be a symlink")
    if attestation.exists() and not attestation.is_file():
        raise ManifestError("data attestation output is not a regular file")
    attestation = attestation.resolve(strict=False)
    _assert_output_outside_data(data_dir, attestation)
    summary = verify_tree(data_dir, manifest)
    if summary.file_count < minimum_files or summary.total_bytes < minimum_bytes:
        raise ManifestError("verified data is below the requested migration safety floor")
    attestation.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    lines = (
        f"ATTESTATION_FORMAT={ATTESTATION_FORMAT}\n"
        f"ATTESTATION_ROLE={role}\n"
        "VERIFIED=1\n"
        f"DATA_DIR={data_dir}\n"
        f"MANIFEST_PATH={manifest}\n"
        f"MANIFEST_SHA256={summary.manifest_sha256}\n"
        f"FILE_COUNT={summary.file_count}\n"
        f"TOTAL_BYTES={summary.total_bytes}\n"
        f"MINIMUM_FILES={minimum_files}\n"
        f"MINIMUM_BYTES={minimum_bytes}\n"
        f"VERIFIED_AT_EPOCH={int(time.time())}\n"
    )
    fd, temporary_name = tempfile.mkstemp(prefix=f".{attestation.name}.", dir=attestation.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(lines)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, attestation)
        os.chmod(attestation, 0o600)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        temporary.unlink(missing_ok=True)
        raise
    return summary


def check_attestation(path: Path, data_dir: Path, expected_role: str) -> Summary:
    _values, manifest, summary = _validate_attestation_metadata(
        path,
        expected_role=expected_role,
        expected_data_dir=data_dir,
    )
    verified = verify_tree(data_dir, manifest)
    if verified != summary:
        raise ManifestError("data changed after attestation")
    return summary


def inspect_attestation(path: Path, expected_role: str) -> Summary:
    values = _parse_attestation(path)
    if values["ATTESTATION_ROLE"] != expected_role:
        raise ManifestError("data attestation role does not match")
    file_count = int(values["FILE_COUNT"])
    total_bytes = int(values["TOTAL_BYTES"])
    if file_count < int(values["MINIMUM_FILES"]) or total_bytes < int(values["MINIMUM_BYTES"]):
        raise ManifestError("data attestation falls below its migration safety floor")
    return Summary(values["MANIFEST_SHA256"], file_count, total_bytes)


def _print_summary(summary: Summary) -> None:
    print(f"MANIFEST_SHA256={summary.manifest_sha256}")
    print(f"FILE_COUNT={summary.file_count}")
    print(f"TOTAL_BYTES={summary.total_bytes}")


def clear_reverse_destination(data_dir: Path, confirmation: str) -> Summary:
    """Clear only the mounted Fly /var/data destination before exact reverse sync."""

    if confirmation != "replace-reverse-destination-from-verified-staging":
        raise ManifestError("reverse destination clear confirmation is invalid")
    if str(data_dir) != "/var/data":
        raise ManifestError("destructive reverse destination must be exactly /var/data")
    if data_dir.is_symlink():
        raise ManifestError("reverse destination cannot be a symlink")
    resolved = data_dir.resolve(strict=True)
    if str(resolved) != "/var/data" or not resolved.is_dir():
        raise ManifestError("reverse destination did not resolve exactly to /var/data")
    if not os.path.ismount(resolved):
        raise ManifestError("/var/data is not a mounted Fly volume")
    for child in list(resolved.iterdir()):
        if child.is_symlink() or not child.is_dir():
            child.unlink()
        else:
            shutil.rmtree(child)
    directory_fd = os.open(resolved, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    return Summary(hashlib.sha256(b"").hexdigest(), 0, 0)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    create = subparsers.add_parser("create")
    create.add_argument("--data-dir", type=Path, required=True)
    create.add_argument("--manifest", type=Path, required=True)

    summary = subparsers.add_parser("summary")
    summary.add_argument("--manifest", type=Path, required=True)

    verify = subparsers.add_parser("verify")
    verify.add_argument("--data-dir", type=Path, required=True)
    verify.add_argument("--manifest", type=Path, required=True)

    attest = subparsers.add_parser("attest")
    attest.add_argument("--data-dir", type=Path, required=True)
    attest.add_argument("--manifest", type=Path, required=True)
    attest.add_argument("--attestation", type=Path, required=True)
    attest.add_argument("--role", choices=sorted(ALLOWED_ROLES), required=True)
    attest.add_argument("--minimum-files", type=int, default=1)
    attest.add_argument("--minimum-bytes", type=int, default=1)

    check = subparsers.add_parser("check")
    check.add_argument("--data-dir", type=Path, required=True)
    check.add_argument("--attestation", type=Path, required=True)
    check.add_argument("--role", choices=sorted(ALLOWED_ROLES), required=True)

    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("--attestation", type=Path, required=True)
    inspect.add_argument("--role", choices=sorted(ALLOWED_ROLES), required=True)
    clear = subparsers.add_parser("clear-reverse-destination")
    clear.add_argument("--data-dir", type=Path, required=True)
    clear.add_argument("--confirm", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        _require_root()
        if args.command == "create":
            result = create_manifest(args.data_dir, args.manifest)
        elif args.command == "summary":
            result = summarize_manifest(args.manifest)
        elif args.command == "verify":
            result = verify_tree(args.data_dir, args.manifest)
        elif args.command == "attest":
            result = write_attestation(
                args.data_dir,
                args.manifest,
                args.attestation,
                role=args.role,
                minimum_files=args.minimum_files,
                minimum_bytes=args.minimum_bytes,
            )
        elif args.command == "check":
            result = check_attestation(args.attestation, args.data_dir, args.role)
        elif args.command == "inspect":
            result = inspect_attestation(args.attestation, args.role)
        elif args.command == "clear-reverse-destination":
            result = clear_reverse_destination(args.data_dir, args.confirm)
        else:  # pragma: no cover
            raise ManifestError("unknown command")
        _print_summary(result)
        return 0
    except (ManifestError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
