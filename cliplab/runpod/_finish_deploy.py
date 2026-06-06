import base64
import httpx
import json
import time
from pathlib import Path

BOOTSTRAP_PY = b'''
import base64
import os
import runpod
from pathlib import Path

V = Path(os.environ.get("STUDIO_APP_DATA_DIR", "/runpod-volume/studio"))

def handler(job):
    i = dict(job.get("input") or job or {})
    t = i.get("task", "health")
    if t == "health":
        return {"ok": True, "volume": str(V), "mode": "bootstrap"}
    if t == "bootstrap_weights":
        written = []
        for rel, b64 in dict(i.get("files") or {}).items():
            rel = str(rel).lstrip("/").replace("..", "")
            dest = V / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(base64.b64decode(b64))
            written.append(str(dest))
        return {"ok": True, "written": len(written)}
    return {"error": f"unknown task: {t}"}

runpod.serverless.start({"handler": handler})
'''

B64 = base64.b64encode(BOOTSTRAP_PY).decode()
BOOTSTRAP_CMD = (
    f"bash -lc \"pip install -q runpod && echo {B64} | base64 -d > /tmp/cliplab_boot.py && python /tmp/cliplab_boot.py\""
)

FULL_CMD = (
    "bash -lc 'pip install -q runpod opencv-python-headless sentence-transformers && "
    "export STUDIO_APP_DATA_DIR=/runpod-volume/studio && "
    "cd /runpod-volume/studio/cliplab_src/cliplab/runpod && python inference_handler.py'"
)

env = {}
for line in Path(r"D:\games\asd\runpod-serverless\.runpod.env").read_text().splitlines():
    if "=" in line and not line.strip().startswith("#"):
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()

api = env["RUNPOD_API_KEY"]
EP = "30lt3s0grkw5le"
TID = "bsol0g38bf"
WORKTREE = Path(r"D:\games\asd\.claude\worktrees\laughing-mclean-b5c91d")
MODELS = Path(r"C:\Users\casey\AppData\Local\Temp\cliplab_models_sync\models")


def gql(query, variables=None):
    r = httpx.post(
        f"https://api.runpod.io/graphql?api_key={api}",
        json={"query": query, "variables": variables or {}},
        timeout=90,
    ).json()
    if r.get("errors"):
        raise RuntimeError(json.dumps(r["errors"], indent=2))
    return r


def save_template(docker_args: str):
    gql(
        "mutation($input: SaveTemplateInput!) { saveTemplate(input: $input) { id } }",
        {
            "input": {
                "id": TID,
                "name": "cliplab-inference-sl",
                "imageName": "runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel",
                "dockerArgs": docker_args,
                "ports": "",
                "volumeInGb": 0,
                "containerDiskInGb": 30,
                "isServerless": True,
                "env": [{"key": "STUDIO_APP_DATA_DIR", "value": "/runpod-volume/studio"}],
            }
        },
    )


def runsync(inp: dict, timeout: int = 600) -> dict:
    r = httpx.post(
        f"https://api.runpod.ai/v2/{EP}/run",
        headers={"Authorization": f"Bearer {api}", "Content-Type": "application/json"},
        json={"input": inp},
        timeout=60,
    )
    r.raise_for_status()
    job = r.json()
    job_id = job.get("id")
    if not job_id:
        return job
    deadline = time.time() + timeout
    while time.time() < deadline:
        s = httpx.get(
            f"https://api.runpod.ai/v2/{EP}/status/{job_id}",
            headers={"Authorization": f"Bearer {api}"},
            timeout=60,
        )
        s.raise_for_status()
        data = s.json()
        status = data.get("status")
        if status == "COMPLETED":
            return data
        if status in {"FAILED", "CANCELLED", "TIMED_OUT"}:
            raise RuntimeError(json.dumps(data, indent=2))
        time.sleep(8)
    raise TimeoutError(f"job {job_id} not completed")


def push_tree(prefix: str, root: Path):
    batch: dict[str, str] = {}
    size = 0
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix in {".md", ".pyc"}:
            continue
        rel = f"{prefix}/{path.relative_to(root).as_posix()}"
        b64 = base64.b64encode(path.read_bytes()).decode()
        if size + len(b64) > 2_500_000 and batch:
            print("push batch", len(batch))
            out = runsync({"task": "bootstrap_weights", "files": batch})
            print(json.dumps(out, indent=2)[:800])
            batch, size = {}, 0
        batch[rel] = b64
        size += len(b64)
    if batch:
        print("push batch", len(batch))
        out = runsync({"task": "bootstrap_weights", "files": batch})
        print(json.dumps(out, indent=2)[:800])


# 1) bootstrap template + warm worker
save_template(BOOTSTRAP_CMD)
gql(
    "mutation($input: EndpointInput!) { saveEndpoint(input: $input) { id } }",
    {
        "input": {
            "id": EP,
            "name": "cliplab-inference",
            "templateId": TID,
            "gpuIds": "AMPERE_16,-NVIDIA RTX A4500,-NVIDIA RTX 4000 Ada Generation,-NVIDIA RTX 2000 Ada Generation",
            "networkVolumeId": env["NETWORK_VOLUME_ID"],
            "locations": "EU-RO-1",
            "idleTimeout": 60,
            "scalerType": "QUEUE_DELAY",
            "scalerValue": 4,
            "workersMin": 1,
            "workersMax": 2,
        }
    },
)
print("bootstrap template set, waiting 180s for worker...")
time.sleep(180)

for attempt in range(8):
    health = runsync({"task": "health"}, timeout=120)
    print(f"health attempt {attempt}:", json.dumps(health, indent=2)[:1000])
    out_h = health.get("output") or health
    if out_h.get("ok"):
        break
    time.sleep(30)
else:
    raise SystemExit("worker never became ready")

health = {"output": out_h}

push_tree("cliplab_src/cliplab", WORKTREE / "cliplab")
push_tree("cliplab/models", MODELS)

save_template(FULL_CMD)
print("full template set, waiting 90s...")
time.sleep(90)

health2 = runsync({"task": "health"})
out = health2.get("output") or health2
print("final health:", json.dumps(health2, indent=2))

# patch studio
studio_ep = env["RUNPOD_ENDPOINT_ID"]
endpoints = gql("query { myself { endpoints { id name templateId } } }")["data"]["myself"]["endpoints"]
template_id = next(e["templateId"] for e in endpoints if e["id"] == studio_ep)
tmpl = gql(
    "query($id: String!) { podTemplate(id: $id) { name imageName dockerArgs env { key value } } }",
    {"id": template_id},
)["data"]["podTemplate"]
env_list = list(tmpl.get("env") or [])
for k, v in {
    "CLIPLAB_VIRALITY_BACKEND": "runpod_custom_v1",
    "CLIPLAB_REFRAME_BACKEND": "runpod_face_v1",
    "RUNPOD_CLIPLAB_ENDPOINT_ID": EP,
}.items():
    if any(e["key"] == k for e in env_list):
        for e in env_list:
            if e["key"] == k:
                e["value"] = v
    else:
        env_list.append({"key": k, "value": v})
gql(
    "mutation($input: SaveTemplateInput!) { saveTemplate(input: $input) { id } }",
    {
        "input": {
            "id": template_id,
            "name": tmpl["name"],
            "imageName": tmpl["imageName"],
            "dockerArgs": tmpl.get("dockerArgs") or "",
            "ports": "",
            "volumeInGb": 0,
            "containerDiskInGb": 50,
            "isServerless": True,
            "env": env_list,
        }
    },
)

lines = Path(r"D:\games\asd\runpod-serverless\.runpod.env").read_text().splitlines()
key = "RUNPOD_CLIPLAB_ENDPOINT_ID"
lines = [f"{key}={EP}" if l.startswith(f"{key}=") else l for l in lines]
if not any(l.startswith(f"{key}=") for l in lines):
    lines.append(f"{key}={EP}")
Path(r"D:\games\asd\runpod-serverless\.runpod.env").write_text("\n".join(lines) + "\n")

print("\nDONE", EP)
print("virality_weights", out.get("virality_weights"))
print("reframe_weights", out.get("reframe_weights"))
