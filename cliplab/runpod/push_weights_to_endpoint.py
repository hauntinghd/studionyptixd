#!/usr/bin/env python3
"""Push trained ClipLab weights to RunPod inference endpoint after production training."""
from __future__ import annotations

import argparse
import base64
import json
import os
import time
from pathlib import Path

import httpx

DEFAULT_EP = os.getenv("RUNPOD_CLIPLAB_ENDPOINT_ID", "lmsndljarhrspn")


def load_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.is_file():
        return env
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.strip().startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def run_wait(api: str, ep: str, inp: dict, *, timeout: int = 1800) -> dict:
    r = httpx.post(
        f"https://api.runpod.ai/v2/{ep}/run",
        headers={"Authorization": f"Bearer {api}"},
        json={"input": inp},
        timeout=60,
    ).json()
    jid = r.get("id")
    if not jid:
        raise RuntimeError(json.dumps(r))
    t0 = time.time()
    while time.time() - t0 < timeout:
        s = httpx.get(
            f"https://api.runpod.ai/v2/{ep}/status/{jid}",
            headers={"Authorization": f"Bearer {api}"},
            timeout=60,
        ).json()
        if s.get("status") == "COMPLETED":
            return s
        if s.get("status") in {"FAILED", "CANCELLED"}:
            raise RuntimeError(json.dumps(s))
        time.sleep(6)
    raise TimeoutError(jid)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--studio-root", default=os.getenv("STUDIO_APP_DATA_DIR", "/workspace/studio"))
    ap.add_argument("--endpoint", default=DEFAULT_EP)
    ap.add_argument("--env-file", default="")
    args = ap.parse_args()

    env_path = Path(args.env_file) if args.env_file else Path(r"D:\games\asd\runpod-serverless\.runpod.env")
    env = load_env(env_path)
    api = os.getenv("RUNPOD_API_KEY") or env.get("RUNPOD_API_KEY", "")
    if not api:
        raise SystemExit("RUNPOD_API_KEY required")

    root = Path(args.studio_root)
    models = root / "cliplab" / "models"
    files: dict[str, str] = {}
    for path in models.rglob("*"):
        if path.is_file() and path.suffix in {".pt", ".json"}:
            rel = path.relative_to(models).as_posix()
            b64 = base64.b64encode(path.read_bytes()).decode()
            # Serverless workers may mount STUDIO_APP_DATA_DIR as /tmp/cliplab
            files[f"models/{rel}"] = b64
            files[f"cliplab/models/{rel}"] = b64

    if not files:
        raise SystemExit(f"No model files under {models}")

    batch: dict[str, str] = {}
    sz = 0
    for k, v in files.items():
        if sz + len(v) > 2_000_000 and batch:
            out = run_wait(api, args.endpoint, {"task": "bootstrap_weights", "files": batch})
            print("batch", json.dumps(out.get("output", out)))
            batch, sz = {}, 0
        batch[k] = v
        sz += len(v)
    if batch:
        out = run_wait(api, args.endpoint, {"task": "bootstrap_weights", "files": batch})
        print("batch", json.dumps(out.get("output", out)))

    health = run_wait(api, args.endpoint, {"task": "health"})
    print("health", json.dumps(health.get("output", health), indent=2))


if __name__ == "__main__":
    main()
