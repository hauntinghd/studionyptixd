import httpx
import json
import subprocess
import time
from pathlib import Path

env = {}
for line in Path(r"D:\games\asd\runpod-serverless\.runpod.env").read_text().splitlines():
    if "=" in line and not line.strip().startswith("#"):
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()

api = env["RUNPOD_API_KEY"]
VOL = env["NETWORK_VOLUME_ID"]
WORKTREE = Path(r"D:\games\asd\.claude\worktrees\laughing-mclean-b5c91d")
MODELS = Path(r"C:\Users\casey\AppData\Local\Temp\cliplab_models_sync")


def gql(q, v=None):
    r = httpx.post(f"https://api.runpod.io/graphql?api_key={api}", json={"query": q, "variables": v or {}}, timeout=90).json()
    if r.get("errors"):
        raise RuntimeError(json.dumps(r["errors"], indent=2))
    return r


# CPU file browser pod with studio volume
pod = gql(
    """
    mutation($input: PodFindAndDeployOnDemandInput!) {
      podFindAndDeployOnDemand(input: $input) { id desiredStatus }
    }
    """,
    {
        "input": {
            "cloudType": "SECURE",
            "gpuCount": 1,
            "gpuTypeId": "NVIDIA RTX A4000",
            "volumeInGb": 0,
            "containerDiskInGb": 15,
            "minVcpuCount": 2,
            "minMemoryInGb": 4,
            "name": "cliplab-volume-sync",
            "imageName": "runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel",
            "dockerArgs": "sleep infinity",
            "ports": "22/tcp",
            "volumeMountPath": "/runpod-volume/studio",
            "networkVolumeId": VOL,
            "dataCenterId": "EU-RO-1",
            "startSsh": True,
            "supportPublicIp": True,
        }
    },
)["data"]["podFindAndDeployOnDemand"]
pod_id = pod["id"]
print("pod", pod_id)

# wait ssh
host = port = None
for _ in range(60):
    p = gql(
        "query($input: PodFilter!) { pod(input: $input) { desiredStatus runtime { ports { ip isIpPublic privatePort publicPort } } } }",
        {"input": {"podId": pod_id}},
    )["data"]["pod"]
    if p.get("desiredStatus") == "RUNNING":
        for pt in (p.get("runtime") or {}).get("ports") or []:
            if pt.get("privatePort") == 22 and pt.get("isIpPublic"):
                host, port = pt["ip"], int(pt["publicPort"])
                break
    if host:
        break
    time.sleep(10)

if not host:
    raise SystemExit("no ssh")

print(f"ssh {host}:{port}")

# upload cliplab tree
subprocess.run(
    ["scp", "-o", "StrictHostKeyChecking=no", "-P", str(port), "-r",
     str(MODELS), f"root@{host}:/runpod-volume/studio/cliplab/"],
    check=True,
    timeout=600,
)
subprocess.run(
    ["scp", "-o", "StrictHostKeyChecking=no", "-P", str(port), "-r",
     str(WORKTREE / "cliplab"), f"root@{host}:/runpod-volume/studio/cliplab_src/"],
    check=True,
    timeout=600,
)

subprocess.run(
    ["ssh", "-o", "StrictHostKeyChecking=no", "-p", str(port), f"root@{host}",
     "ls -la /runpod-volume/studio/cliplab/models/virality/v1/ && ls -la /runpod-volume/studio/cliplab_src/cliplab/runpod/"],
    check=True,
)

gql("mutation($input: PodTerminateInput!) { podTerminate(input: $input) }", {"input": {"podId": pod_id}})
print("volume sync done")

# Attach volume to studio + cliplab endpoints, patch studio env
for ep_id, name in [("ajt1r1estd85z8", "studio-api-ada24"), ("30lt3s0grkw5le", "cliplab-inference")]:
    endpoints = gql("query { myself { endpoints { id name templateId gpuIds networkVolumeId } } }")["data"]["myself"]["endpoints"]
    ep = next(e for e in endpoints if e["id"] == ep_id)
    gql(
        "mutation($input: EndpointInput!) { saveEndpoint(input: $input) { id networkVolumeId } }",
        {
            "input": {
                "id": ep_id,
                "name": name,
                "templateId": ep["templateId"],
                "gpuIds": ep["gpuIds"],
                "networkVolumeId": VOL,
                "idleTimeout": 60,
                "scalerType": "QUEUE_DELAY",
                "scalerValue": 4,
                "workersMin": 0 if ep_id == env["RUNPOD_ENDPOINT_ID"] else 0,
                "workersMax": 2 if ep_id != env["RUNPOD_ENDPOINT_ID"] else 10,
            }
        },
    )
    print("attached volume to", ep_id)

# patch studio template env
studio_ep = env["RUNPOD_ENDPOINT_ID"]
endpoints = gql("query { myself { endpoints { id templateId } } }")["data"]["myself"]["endpoints"]
template_id = next(e["templateId"] for e in endpoints if e["id"] == studio_ep)
tmpl = gql(
    "query($id: String!) { podTemplate(id: $id) { name imageName dockerArgs env { key value } } }",
    {"id": template_id},
)["data"]["podTemplate"]
env_list = list(tmpl.get("env") or [])
for k, v in {
    "CLIPLAB_VIRALITY_BACKEND": "runpod_custom_v1",
    "CLIPLAB_REFRAME_BACKEND": "runpod_face_v1",
    "RUNPOD_CLIPLAB_ENDPOINT_ID": "30lt3s0grkw5le",
    "STUDIO_APP_DATA_DIR": "/runpod-volume/studio",
    "APP_DATA_DIR": "/runpod-volume/studio",
}.items():
    if any(e["key"] == k for e in env_list):
        for e in env_list:
            if e["key"] == k:
                e["value"] = v
    else:
        env_list.append({"key": k, "value": v})
gql(
    "mutation($input: SaveTemplateInput!) { saveTemplate(input: $input) { id } }",
    {"input": {
        "id": template_id, "name": tmpl["name"], "imageName": tmpl["imageName"],
        "dockerArgs": tmpl.get("dockerArgs") or "", "ports": "", "volumeInGb": 0,
        "containerDiskInGb": 50, "isServerless": True, "env": env_list,
    }},
)

# cliplab full handler template
FULL = (
    "bash -lc 'pip install -q runpod opencv-python-headless sentence-transformers && "
    "export STUDIO_APP_DATA_DIR=/runpod-volume/studio && "
    "cd /runpod-volume/studio/cliplab_src/cliplab/runpod && python inference_handler.py'"
)
gql(
    "mutation($input: SaveTemplateInput!) { saveTemplate(input: $input) { id } }",
    {"input": {
        "id": "bsol0g38bf", "name": "cliplab-inference-sl",
        "imageName": "runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel",
        "dockerArgs": FULL, "ports": "", "volumeInGb": 0, "containerDiskInGb": 30,
        "isServerless": True,
        "env": [{"key": "STUDIO_APP_DATA_DIR", "value": "/runpod-volume/studio"}],
    }},
)

lines = Path(r"D:\games\asd\runpod-serverless\.runpod.env").read_text().splitlines()
if not any(l.startswith("RUNPOD_CLIPLAB_ENDPOINT_ID=") for l in lines):
    lines.append("RUNPOD_CLIPLAB_ENDPOINT_ID=30lt3s0grkw5le")
else:
    lines = ["RUNPOD_CLIPLAB_ENDPOINT_ID=30lt3s0grkw5le" if l.startswith("RUNPOD_CLIPLAB_ENDPOINT_ID=") else l for l in lines]
Path(r"D:\games\asd\runpod-serverless\.runpod.env").write_text("\n".join(lines) + "\n")
print("DONE - test health on cliplab endpoint after workers spin up")
