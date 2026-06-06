#!/usr/bin/env python3
"""Flip model_registry.json active backends after training."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", default=str(Path(os.getenv("STUDIO_APP_DATA_DIR", "/runpod-volume/studio")) / "cliplab/models/model_registry.json"))
    args = ap.parse_args()

    reg_path = Path(args.registry)
    if not reg_path.exists():
        raise SystemExit(f"registry not found: {reg_path}")

    data = json.loads(reg_path.read_text(encoding="utf-8"))
    data["updated_at"] = datetime.now(timezone.utc).isoformat()
    data["virality_scorer"]["active"] = "runpod_custom_v1"
    data["face_reframe"]["active"] = "runpod_face_v1"
    data["virality_scorer"]["checkpoints"]["runpod_custom_v1"]["status"] = "ready"
    data["face_reframe"]["checkpoints"]["runpod_face_v1"]["status"] = "ready"

    reg_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": "activated", "registry": str(reg_path)}))


if __name__ == "__main__":
    main()
