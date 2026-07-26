#!/usr/bin/env python3
"""Atomically replace one exact Caddy site block without touching other sites."""
from __future__ import annotations

import argparse
import os
import re
import shutil
import stat
import tempfile
import time
from pathlib import Path


def _brace_delta(line: str) -> int:
    """Count structural braces, ignoring comments and quoted/backtick strings."""

    delta = 0
    quote = ""
    escaped = False
    for char in line:
        if escaped:
            escaped = False
            continue
        if quote:
            if char == "\\" and quote in {'"', "'"}:
                escaped = True
            elif char == quote:
                quote = ""
            continue
        if char == "#":
            break
        if char in {'"', "'", "`"}:
            quote = char
        elif char == "{":
            delta += 1
        elif char == "}":
            delta -= 1
    return delta


def _site_span(lines: list[str], hostname: str) -> tuple[int, int]:
    pattern = re.compile(rf"^\s*{re.escape(hostname)}\s*\{{\s*$")
    starts = [index for index, line in enumerate(lines) if pattern.match(line)]
    if len(starts) != 1:
        raise RuntimeError(
            f"expected exactly one {hostname} site block, found {len(starts)}"
        )
    start = starts[0]
    depth = 0
    for index in range(start, len(lines)):
        depth += _brace_delta(lines[index])
        if index == start and depth <= 0:
            raise RuntimeError("site block opening brace is malformed")
        if index > start and depth == 0:
            return start, index + 1
        if depth < 0:
            break
    raise RuntimeError(f"{hostname} site block is not balanced")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--caddyfile", required=True)
    parser.add_argument("--site-block", required=True)
    parser.add_argument("--hostname", required=True)
    parser.add_argument("--backup-dir", required=True)
    args = parser.parse_args()

    caddyfile = Path(args.caddyfile).resolve()
    site_block = Path(args.site_block).resolve()
    backup_dir = Path(args.backup_dir).resolve()
    if not caddyfile.is_file() or not site_block.is_file():
        raise SystemExit("Caddyfile and replacement site block must both exist")
    if caddyfile == site_block:
        raise SystemExit("replacement site block must not be the live Caddyfile")

    original_stat = caddyfile.stat()
    original_text = caddyfile.read_text(encoding="utf-8")
    replacement_text = site_block.read_text(encoding="utf-8").strip() + "\n"
    original_lines = original_text.splitlines(keepends=True)
    replacement_lines = replacement_text.splitlines(keepends=True)
    old_start, old_end = _site_span(original_lines, args.hostname)
    new_start, new_end = _site_span(replacement_lines, args.hostname)
    surrounding = replacement_lines[:new_start] + replacement_lines[new_end:]
    if any(line.strip() and not line.lstrip().startswith("#") for line in surrounding):
        raise SystemExit(
            "replacement file may contain only comments outside the requested site block"
        )

    updated = "".join(
        original_lines[:old_start] + replacement_lines + original_lines[old_end:]
    )
    if updated == original_text:
        print("unchanged")
        return 0

    backup_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(backup_dir, 0o700)
    backup = backup_dir / f"Caddyfile.{int(time.time())}.bak"
    if backup.exists():
        raise SystemExit("refusing to overwrite an existing Caddy backup")
    shutil.copy2(caddyfile, backup)
    os.chmod(backup, 0o600)

    fd, temporary_name = tempfile.mkstemp(
        prefix=".Caddyfile.",
        suffix=".tmp",
        dir=str(caddyfile.parent),
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(updated)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, stat.S_IMODE(original_stat.st_mode))
        if hasattr(os, "chown"):
            try:
                os.chown(temporary, original_stat.st_uid, original_stat.st_gid)
            except PermissionError:
                pass
        os.replace(temporary, caddyfile)
    finally:
        temporary.unlink(missing_ok=True)
    print(str(backup))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
