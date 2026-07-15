import base64
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
ARTIFACTS = Path(r"C:\Users\casey\AppData\Local\Temp\cliplab_models_sync")
SRC = Path(r"D:\games\asd\.claude\worktrees\laughing-mclean-b5c91d\cliplab")
# The endpoint container is an isolated, single-tenant ephemeral workspace.
DATA_ROOT = "/tmp/cliplab"  # nosec B108

BOOTSTRAP_PY = f'''
import base64, os, runpod
from pathlib import Path
ROOT = Path("{DATA_ROOT}")

def handler(job):
    i = dict(job.get("input") or job or {{}})
    t = i.get("task", "health")
    if t == "health":
        m = ROOT / "models/virality/v1/model.pt"
        return {{"ok": True, "virality_weights": m.exists(), "root": str(ROOT)}}
    if t == "bootstrap_weights":
        n = 0
        for rel, b64 in dict(i.get("files") or {{}}).items():
            dest = ROOT / str(rel).lstrip("/")
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(base64.b64decode(b64))
            n += 1
        return {{"ok": True, "written": n}}
    return {{"error": "unknown task"}}

runpod.serverless.start({{"handler": handler}})
'''

FULL_CMD = (
    f"bash -lc 'pip install -q runpod torch opencv-python-headless sentence-transformers && "
    f"export STUDIO_APP_DATA_DIR={DATA_ROOT} && "
    f"cd {DATA_ROOT}/cliplab_src/cliplab/runpod && python inference_handler.py'"
)

BOOTSTRAP_CMD = (
    "bash -lc \"pip install -q runpod && echo "
    + base64.b64encode(BOOTSTRAP_PY.encode()).decode()
    + " | base64 -d | python\""
)


def gql(q, v=None):
    r = httpx.post(f"https://api.runpod.io/graphql?api_key={api}", json={"query": q, "variables": v or {}}, timeout=90).json()
    if r.get("errors"):
        raise RuntimeError(json.dumps(r["errors"], indent=2))
    return r


def save_template(docker_args: str):
    gql("mutation($input: SaveTemplateInput!) { saveTemplate(input: $input) { id } }", {
        "input": {
            "id": TID, "name": "cliplab-inference-sl",
            "imageName": "runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel",
            "dockerArgs": docker_args, "ports": "", "volumeInGb": 0, "containerDiskInGb": 40,
            "isServerless": True, "env": [],
        }
    })


def save_endpoint(workers_min: int = 0):
    gql("mutation($input: EndpointInput!) { saveEndpoint(input: $input) { id } }", {
        "input": {
            "id": EP, "name": "cliplab-inference", "templateId": TID,
            "gpuIds": "AMPERE_16,-NVIDIA RTX A4500,-NVIDIA RTX 4000 Ada Generation,-NVIDIA RTX 2000 Ada Generation",
            "networkVolumeId": None,
            "idleTimeout": 120, "scalerType": "QUEUE_DELAY", "scalerValue": 4,
            "workersMin": workers_min, "workersMax": 2,
        }
    })


def run_wait(inp, timeout=900):
    r = httpx.post(f"https://api.runpod.ai/v2/{EP}/run", headers={"Authorization": f"Bearer {api}"}, json={"input": inp}, timeout=60)
    r.raise_for_status()
    jid = r.json()["id"]
    t0 = time.time()
    while time.time() - t0 < timeout:
        s = httpx.get(f"https://api.runpod.ai/v2/{EP}/status/{jid}", headers={"Authorization": f"Bearer {api}"}, timeout=60).json()
        st = s.get("status")
        if st == "COMPLETED":
            return s
        if st in {"FAILED", "CANCELLED", "TIMED_OUT"}:
            raise RuntimeError(json.dumps(s, indent=2))
        time.sleep(10)
    raise TimeoutError(jid)


def collect_files() -> dict[str, str]:
    out: dict[str, str] = {}
    models_root = ARTIFACTS / "models"
    for path in models_root.rglob("*"):
        if path.is_file():
            out[f"models/{path.relative_to(models_root).as_posix()}"] = base64.b64encode(path.read_bytes()).decode()
    for path in SRC.rglob("*"):
        if path.is_file() and path.suffix not in {".md", ".pyc"}:
            out[f"cliplab_src/cliplab/{path.relative_to(SRC).as_posix()}"] = base64.b64encode(path.read_bytes()).decode()
    return out


def push_files(files: dict[str, str]):
    batch, sz = {}, 0
    for k, v in files.items():
        if sz + len(v) > 2_000_000 and batch:
            print("batch", len(batch), run_wait({"task": "bootstrap_weights", "files": batch}).get("output"))
            batch, sz = {}, 0
        batch[k] = v
        sz += len(v)
    if batch:
        print("batch", len(batch), run_wait({"task": "bootstrap_weights", "files": batch}).get("output"))


save_template(BOOTSTRAP_CMD)
save_endpoint(0)
print("waiting for worker...")
time.sleep(90)
print("health", run_wait({"task": "health"}))

push_files(collect_files())
print("post-push health", run_wait({"task": "health"}))

save_template(FULL_CMD)
save_endpoint(1)
print("switched to full handler, waiting...")
time.sleep(120)
print("final health", json.dumps(run_wait({"task": "health"}), indent=2))

# Studio env patch
studio_ep = env["RUNPOD_ENDPOINT_ID"]
template_id = next(e["templateId"] for e in gql("query { myself { endpoints { id templateId } } }")["data"]["myself"]["endpoints"] if e["id"] == studio_ep)
tmpl = gql("query($id: String!) { podTemplate(id: $id) { name imageName dockerArgs env { key value } } }", {"id": template_id})["data"]["podTemplate"]
env_list = list(tmpl.get("env") or [])
updates = {
    "CLIPLAB_VIRALITY_BACKEND": "runpod_custom_v1",
    "CLIPLAB_REFRAME_BACKEND": "runpod_face_v1",
    "RUNPOD_CLIPLAB_ENDPOINT_ID": EP,
}
for k, v in updates.items():
    if not any(e["key"] == k for e in env_list):
        env_list.append({"key": k, "value": v})
    else:
        for e in env_list:
            if e["key"] == k:
                e["value"] = v
gql("mutation($input: SaveTemplateInput!) { saveTemplate(input: $input) { id } }", {"input": {
    "id": template_id, "name": tmpl["name"], "imageName": tmpl["imageName"],
    "dockerArgs": tmpl.get("dockerArgs") or "", "ports": "", "volumeInGb": 0,
    "containerDiskInGb": 50, "isServerless": True, "env": env_list,
}})

p = Path(r"D:\games\asd\runpod-serverless\.runpod.env")
lines = p.read_text().splitlines()
lines = [f"RUNPOD_CLIPLAB_ENDPOINT_ID={EP}" if l.startswith("RUNPOD_CLIPLAB_ENDPOINT_ID=") else l for l in lines]
if not any(l.startswith("RUNPOD_CLIPLAB_ENDPOINT_ID=") for l in lines):
    lines.append(f"RUNPOD_CLIPLAB_ENDPOINT_ID={EP}")
p.write_text("\n".join(lines) + "\n")
print("CLIPLAB LIVE", EP)
