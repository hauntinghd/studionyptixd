"""Writable paths for local + Vercel (read-only bundle) + RunPod volume."""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]


def on_readonly_runtime() -> bool:
    return bool(
        os.getenv("VERCEL")
        or os.getenv("VERCEL_ENV")
        or os.getenv("AWS_LAMBDA_FUNCTION_NAME")
    )


def data_root() -> Path:
    """Durable-ish writable root. On Vercel this is /tmp until RunPod volume is wired."""
    raw = str(os.getenv("APP_DATA_DIR") or "").strip()
    if raw:
        path = Path(raw).expanduser()
    elif on_readonly_runtime():
        path = Path(tempfile.gettempdir()) / "studio_data"
    else:
        path = _REPO_ROOT / "data"
    ensure_dir(path)
    return path


def ensure_dir(path: Path) -> Path:
    try:
        path.mkdir(parents=True, exist_ok=True)
        return path
    except OSError:
        fallback = Path(tempfile.gettempdir()) / "studio_data" / path.name
        try:
            fallback.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass
        return fallback


def skeleton_output_root() -> Path:
    raw = str(os.getenv("SKELETON_AI_OUTPUT_ROOT") or "").strip()
    if raw:
        path = Path(raw)
        if not path.is_absolute():
            # Prefer writable data root on serverless instead of repo-relative.
            path = data_root() / path if on_readonly_runtime() else (_REPO_ROOT / path)
    else:
        path = data_root() / "skeleton_ai" / "output" if on_readonly_runtime() else (_REPO_ROOT / "skeleton_ai" / "output")
    return ensure_dir(path)
