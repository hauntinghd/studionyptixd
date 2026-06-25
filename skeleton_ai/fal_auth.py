from __future__ import annotations

import os


def require_fal_key(label: str = "fal.ai") -> str:
    key = (os.getenv("FAL_AI_KEY", "") or os.getenv("FAL_KEY", "")).strip()
    if not key:
        raise RuntimeError(f"FAL_AI_KEY or FAL_KEY is required for {label}.")
    os.environ["FAL_AI_KEY"] = key
    os.environ["FAL_KEY"] = key
    return key
