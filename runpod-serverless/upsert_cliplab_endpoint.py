#!/usr/bin/env python3
"""Create/update RunPod serverless endpoint for ClipLab inference."""
from __future__ import annotations

import os
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

TEMPLATE_NAME = "cliplab-inference"
ENDPOINT_NAME = "cliplab-inference"


def load_envs() -> None:
    for name in (".runpod.env", ".env"):
        p = SCRIPT_DIR / name
        if not p.exists():
            continue
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def gql(api_key: str, query: str, variables: dict | None = None) -> dict:
    import httpx

    resp = httpx.post(
        "https://api.runpod.io/graphql",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"query": query, "variables": variables or {}},
        timeout=60,
    )
    data = resp.json()
    if data.get("errors"):
        print(data["errors"], file=sys.stderr)
        raise SystemExit(1)
    return data


def main() -> int:
    load_envs()
    api_key = os.environ.get("RUNPOD_API_KEY", "").strip()
    registry = os.environ.get("DOCKER_REGISTRY", "").strip()
    image_tag = os.environ.get("CLIPLAB_IMAGE_TAG", "cliplab-inference:v1.0").strip()
    volume_id = os.environ.get("NETWORK_VOLUME_ID", "").strip()
    app_data_dir = os.environ.get("STUDIO_APP_DATA_DIR", "/runpod-volume/studio").strip()

    if not api_key:
        print("ERROR: RUNPOD_API_KEY missing", file=sys.stderr)
        return 2
    if not registry:
        print("ERROR: DOCKER_REGISTRY missing", file=sys.stderr)
        return 2

    full_image = f"{registry}/{image_tag}"
    env_list = [
        {"key": "STUDIO_APP_DATA_DIR", "value": app_data_dir},
        {"key": "PYTHONPATH", "value": "/app"},
    ]

    import base64
    import subprocess

    ghcr_auth = ""
    gh_token = ""
    gh_user = ""
    if "ghcr.io" in registry:
        try:
            gh_token = subprocess.check_output(["gh", "auth", "token"], stderr=subprocess.DEVNULL, text=True).strip()
            gh_user = subprocess.check_output(
                ["gh", "api", "user", "--jq", ".login"],
                stderr=subprocess.DEVNULL,
                text=True,
                env={**os.environ, "MSYS_NO_PATHCONV": "1"},
            ).strip()
            if gh_token and gh_user:
                ghcr_auth = base64.b64encode(f"{gh_user}:{gh_token}".encode()).decode()
        except Exception as e:
            print(f"WARNING: GHCR auth unavailable ({e})")

    print(f"Image:     {full_image}")
    print(f"Volume ID: {volume_id or '<none>'}")

    registry_auth_id = ""
    if gh_token and gh_user:
        try:
            auth_data = gql(api_key, """
                mutation($input: SaveRegistryAuthInput!) {
                    saveRegistryAuth(input: $input) { id name }
                }
            """, {"input": {"name": "ghcr-cliplab", "username": gh_user, "password": gh_token}})
            registry_auth_id = auth_data["data"]["saveRegistryAuth"]["id"]
        except SystemExit:
            print("WARNING: registry auth failed")

    template_input: dict = {
        "name": TEMPLATE_NAME,
        "imageName": full_image,
        "dockerArgs": "",
        "ports": "",
        "volumeInGb": 0,
        "containerDiskInGb": 30,
        "env": env_list,
    }
    if registry_auth_id:
        template_input["containerRegistryAuthId"] = registry_auth_id

    tmpl_data = gql(api_key, """
        mutation($input: SaveTemplateInput!) {
            saveTemplate(input: $input) { id name }
        }
    """, {"input": template_input})
    template_id = tmpl_data["data"]["saveTemplate"]["id"]
    print(f"template id: {template_id}")

    ep_data = gql(api_key, """
        mutation($input: EndpointInput!) {
            saveEndpoint(input: $input) { id name }
        }
    """, {
        "input": {
            "name": ENDPOINT_NAME,
            "templateId": template_id,
            "gpuIds": "AMPERE_16",
            "networkVolumeId": volume_id,
            "locations": "",
            "idleTimeout": 5,
            "scalerType": "QUEUE_DELAY",
            "scalerValue": 4,
            "workersMin": 0,
            "workersMax": 2,
            "flashboot": True,
        }
    })
    endpoint_id = ep_data["data"]["saveEndpoint"]["id"]
    print(f"endpoint id: {endpoint_id}")
    print(f"endpoint url: https://api.runpod.ai/v2/{endpoint_id}/runsync")

    env_file = SCRIPT_DIR / ".runpod.env"
    if env_file.exists():
        lines = env_file.read_text(encoding="utf-8").splitlines()
        key = "RUNPOD_CLIPLAB_ENDPOINT_ID"
        replaced = False
        for i, line in enumerate(lines):
            if line.startswith(f"{key}=") or line.startswith(f"# {key}="):
                lines[i] = f"{key}={endpoint_id}"
                replaced = True
                break
        if not replaced:
            lines.append(f"{key}={endpoint_id}")
        env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"saved {key} to .runpod.env")

    print()
    print("Set on Studio API (Fly / main RunPod bridge):")
    print("  CLIPLAB_VIRALITY_BACKEND=runpod_custom_v1")
    print("  CLIPLAB_REFRAME_BACKEND=runpod_face_v1")
    print(f"  RUNPOD_CLIPLAB_ENDPOINT_ID={endpoint_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
