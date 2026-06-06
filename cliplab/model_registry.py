"""Load trained ClipLab weights from RunPod volume."""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from cliplab.config import CLIPLAB_MODELS_DIR

_log = logging.getLogger("nyptid-studio.cliplab")

_DEFAULT_REGISTRY = {
    "version": 1,
    "virality_scorer": {
        "active": "local_llm",
        "checkpoints": {
            "runpod_custom_v1": {
                "path": "virality/v1/model.pt",
                "config": "virality/v1/config.json",
                "status": "pending_training",
            }
        },
    },
    "face_reframe": {
        "active": "opencv_face",
        "checkpoints": {
            "runpod_face_v1": {
                "path": "reframe/v1/tracker.pt",
                "config": "reframe/v1/config.json",
                "status": "pending_training",
            }
        },
    },
}


def registry_path() -> Path:
    return CLIPLAB_MODELS_DIR / "model_registry.json"


def load_registry() -> dict[str, Any]:
    path = registry_path()
    if not path.exists():
        bundled = Path(__file__).parent / "runpod" / "model_registry.json"
        if bundled.exists():
            try:
                return json.loads(bundled.read_text(encoding="utf-8"))
            except Exception:
                pass
        return dict(_DEFAULT_REGISTRY)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        _log.warning("ClipLab registry read failed: %s", str(exc)[:120])
        return dict(_DEFAULT_REGISTRY)


def checkpoint_path(model_key: str, checkpoint_id: str) -> Path | None:
    reg = load_registry()
    block = dict(reg.get(model_key) or {})
    ckpts = dict(block.get("checkpoints") or {})
    entry = dict(ckpts.get(checkpoint_id) or {})
    rel = str(entry.get("path") or "").strip()
    if not rel:
        return None
    return CLIPLAB_MODELS_DIR / rel


def active_checkpoint(model_key: str) -> tuple[str, Path | None]:
    reg = load_registry()
    block = dict(reg.get(model_key) or {})
    active = str(block.get("active") or "").strip()
    if not active:
        return "", None
    return active, checkpoint_path(model_key, active)


def registry_status() -> dict[str, Any]:
    reg = load_registry()
    virality_id, virality_path = active_checkpoint("virality_scorer")
    reframe_id, reframe_path = active_checkpoint("face_reframe")
    return {
        "registry_path": str(registry_path()),
        "volume_root": str(CLIPLAB_MODELS_DIR),
        "virality_backend": virality_id or "local_llm",
        "virality_weights_ready": bool(virality_path and virality_path.exists()),
        "reframe_backend": reframe_id or "opencv_face",
        "reframe_weights_ready": bool(reframe_path and reframe_path.exists()),
        "registry": reg,
    }
