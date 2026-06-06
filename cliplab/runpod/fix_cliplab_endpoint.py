#!/usr/bin/env python3
"""Switch lmsndljarhrspn to full inference handler + push production weights."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import httpx

ENV_FILE = Path(r"D:\games\asd\runpod-serverless\.runpod.env")
CLIPLAB_EP = os.getenv("RUNPOD_CLIPLAB_ENDPOINT_ID", "lmsndljarhrspn")
APP_DATA = "/tmp/cliplab"

FULL_CMD = (
    "bash -lc 'pip install -q runpod opencv-python-headless sentence-transformers && "
    f"export STUDIO_APP_DATA_DIR={APP_DATA} && "
    "export PYTHONPATH=/tmp/cliplab/cliplab_src && "
    "cd /tmp/cliplab/cliplab_src/cliplab/runpod && python inference_handler.py'"
)


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if s and not s.startswith("#") and "=" in s:
            k, v = s.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def gql(api_key: str, query: str, variables: dict | None = None) -> dict:
    r = httpx.post(
        f"https://api.runpod.io/graphql?api_key={api_key}",
        headers={"Content-Type": "application/json"},
        json={"query": query, "variables": variables or {}},
        timeout=90,
    )
    data = r.json()
    if data.get("errors"):
        raise RuntimeError(json.dumps(data["errors"], indent=2))
    return data


def main() -> int:
    env = load_env()
    api_key = env["RUNPOD_API_KEY"]
    ep_id = env.get("RUNPOD_CLIPLAB_ENDPOINT_ID", CLIPLAB_EP)

    existing_id = None
    data = gql(api_key, "query { myself { podTemplates { id name } } }")
    for tmpl in (data.get("data") or {}).get("myself", {}).get("podTemplates") or []:
        if tmpl.get("name") == "cliplab-inf-v3":
            existing_id = tmpl.get("id")
            break

    tmpl_input: dict = {
        "name": "cliplab-inf-v3",
        "imageName": "runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel",
        "dockerArgs": FULL_CMD,
        "ports": "",
        "volumeInGb": 0,
        "containerDiskInGb": 40,
        "isServerless": True,
        "env": [
            {"key": "STUDIO_APP_DATA_DIR", "value": APP_DATA},
            {"key": "PYTHONPATH", "value": "/tmp/cliplab/cliplab_src"},
        ],
    }
    if existing_id:
        tmpl_input["id"] = existing_id

    tmpl = gql(
        api_key,
        "mutation($input: SaveTemplateInput!) { saveTemplate(input: $input) { id } }",
        {"input": tmpl_input},
    )
    template_id = tmpl["data"]["saveTemplate"]["id"]

    gql(
        api_key,
        "mutation($input: EndpointInput!) { saveEndpoint(input: $input) { id } }",
        {
            "input": {
                "id": ep_id,
                "name": "cliplab-inference",
                "templateId": template_id,
                "gpuIds": "AMPERE_16",
                "networkVolumeId": "",
                "locations": "",
                "idleTimeout": 5,
                "scalerType": "QUEUE_DELAY",
                "scalerValue": 4,
                "workersMin": 1,
                "workersMax": 2,
            }
        },
    )
    print(f"Updated endpoint {ep_id} template {template_id}")
    print("Run push_weights_to_endpoint.py on pod after workers recycle.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
