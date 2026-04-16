"""
Create a 10GB RunPod network volume for Studio backend state persistence.
Idempotent — if a volume named 'studio-backend-volume' already exists, reuses it.

After success, appends NETWORK_VOLUME_ID=<id> to runpod-serverless/.runpod.env.

Usage:
    python runpod-serverless/create_runpod_volume.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx

SCRIPT_DIR = Path(__file__).resolve().parent
RUNPOD_ENV = SCRIPT_DIR / ".runpod.env"
VOLUME_NAME = "studio-backend-volume"
VOLUME_SIZE_GB = 10
# US-GA-1 is the first-choice data center; we'll fall back automatically.
DATA_CENTER_CANDIDATES = ["US-GA-1", "US-OR-1", "US-CA-2", "EU-RO-1", "EU-SE-1"]


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    if not RUNPOD_ENV.exists():
        raise SystemExit(f"ERROR: {RUNPOD_ENV} not found. Create it from .runpod.env.example first.")
    for line in RUNPOD_ENV.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def persist_volume_id(volume_id: str) -> None:
    """Upsert NETWORK_VOLUME_ID=<id> into .runpod.env."""
    text = RUNPOD_ENV.read_text(encoding="utf-8")
    lines = text.splitlines()
    replaced = False
    for i, line in enumerate(lines):
        if line.startswith("NETWORK_VOLUME_ID="):
            lines[i] = f"NETWORK_VOLUME_ID={volume_id}"
            replaced = True
            break
    if not replaced:
        lines.append(f"NETWORK_VOLUME_ID={volume_id}")
    RUNPOD_ENV.write_text("\n".join(lines) + "\n", encoding="utf-8")


def list_volumes(api_key: str) -> list[dict]:
    query = {
        "query": """
        query {
            myself {
                networkVolumes {
                    id
                    name
                    size
                    dataCenterId
                }
            }
        }
        """
    }
    with httpx.Client(timeout=30) as c:
        r = c.post(
            f"https://api.runpod.io/graphql?api_key={api_key}",
            headers={"Content-Type": "application/json"},
            json=query,
        )
        r.raise_for_status()
        data = r.json()
    if "errors" in data:
        raise RuntimeError(f"RunPod GraphQL error: {data['errors']}")
    return list(((data.get("data") or {}).get("myself") or {}).get("networkVolumes") or [])


def create_volume(api_key: str, data_center: str) -> dict:
    # Note: RunPod's GraphQL schema uses `createNetworkVolume` with `name`, `size`, and
    # `dataCenterId` fields on the input. Per docs circa 2026-Q1.
    query = {
        "query": """
        mutation CreateVolume($input: CreateNetworkVolumeInput!) {
            createNetworkVolume(input: $input) {
                id
                name
                size
                dataCenterId
            }
        }
        """,
        "variables": {
            "input": {
                "name": VOLUME_NAME,
                "size": VOLUME_SIZE_GB,
                "dataCenterId": data_center,
            }
        },
    }
    with httpx.Client(timeout=45) as c:
        r = c.post(
            f"https://api.runpod.io/graphql?api_key={api_key}",
            headers={"Content-Type": "application/json"},
            json=query,
        )
    data = r.json()
    if "errors" in data:
        raise RuntimeError(json.dumps(data["errors"]))
    return (data.get("data") or {}).get("createNetworkVolume") or {}


def main() -> int:
    env = load_env()
    api_key = env.get("RUNPOD_API_KEY", "").strip()
    if not api_key:
        print("ERROR: RUNPOD_API_KEY missing in .runpod.env", file=sys.stderr)
        return 2

    # 1. Check if already exists (idempotency)
    try:
        existing = list_volumes(api_key)
    except Exception as e:
        print(f"WARNING: could not list volumes ({e}); proceeding to create", file=sys.stderr)
        existing = []

    for vol in existing:
        if vol.get("name") == VOLUME_NAME:
            vid = vol.get("id", "")
            print(f"Found existing volume '{VOLUME_NAME}': id={vid} size={vol.get('size')}GB dc={vol.get('dataCenterId')}")
            persist_volume_id(vid)
            print(f"Updated NETWORK_VOLUME_ID in {RUNPOD_ENV.name}")
            return 0

    # 2. Create fresh — fall back across data centers if first choice is full
    last_err = ""
    for dc in DATA_CENTER_CANDIDATES:
        print(f"Attempting createNetworkVolume in {dc}...")
        try:
            result = create_volume(api_key, dc)
        except Exception as e:
            last_err = str(e)[:200]
            print(f"  {dc} failed: {last_err}")
            continue
        if result.get("id"):
            vid = result["id"]
            print(f"[OK] Created: id={vid} size={result.get('size')}GB dc={result.get('dataCenterId')}")
            persist_volume_id(vid)
            print(f"Updated NETWORK_VOLUME_ID in {RUNPOD_ENV.name}")
            return 0

    print(f"ERROR: all data centers failed. Last error: {last_err}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
