import httpx
import json
import time
from pathlib import Path

env = {}
for line in Path(r"D:\games\asd\runpod-serverless\.runpod.env").read_text().splitlines():
    if "=" in line and not line.strip().startswith("#"):
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()

api = env["RUNPOD_API_KEY"]
EP = "30lt3s0grkw5le"
TID = "bsol0g38bf"

MINIMAL = (
    'bash -lc "pip install -q runpod && python -c '
    "'import runpod; runpod.serverless.start({\"handler\": lambda e: {\"ok\": True, \"ping\": 1}})'\""
)

def gql(q, v=None):
    r = httpx.post(f"https://api.runpod.io/graphql?api_key={api}", json={"query": q, "variables": v or {}}, timeout=60).json()
    print(json.dumps(r, indent=2)[:2000])
    return r

gql(
    "mutation($input: SaveTemplateInput!) { saveTemplate(input: $input) { id } }",
    {
        "input": {
            "id": TID,
            "name": "cliplab-inference-sl",
            "imageName": "runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel",
            "dockerArgs": MINIMAL,
            "ports": "",
            "volumeInGb": 0,
            "containerDiskInGb": 30,
            "isServerless": True,
            "env": [{"key": "STUDIO_APP_DATA_DIR", "value": "/runpod-volume/studio"}],
        }
    },
)

gql(
    "mutation($input: EndpointInput!) { saveEndpoint(input: $input) { id } }",
    {
        "input": {
            "id": EP,
            "name": "cliplab-inference",
            "templateId": TID,
            "gpuIds": "AMPERE_16,-NVIDIA RTX A4500,-NVIDIA RTX 4000 Ada Generation,-NVIDIA RTX 2000 Ada Generation",
            "networkVolumeId": env["NETWORK_VOLUME_ID"],
            "locations": None,
            "idleTimeout": 60,
            "workersMin": 1,
            "workersMax": 2,
        }
    },
)

time.sleep(120)
h = httpx.get(f"https://api.runpod.ai/v2/{EP}/health", headers={"Authorization": f"Bearer {api}"}, timeout=30)
print("health endpoint", h.text)

r = httpx.post(
    f"https://api.runpod.ai/v2/{EP}/runsync",
    headers={"Authorization": f"Bearer {api}"},
    json={"input": {}},
    timeout=300,
)
print("runsync", r.status_code, r.text[:1000])
