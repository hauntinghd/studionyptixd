import base64
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
EP = "30lt3s0grkw5le"
TID = "bsol0g38bf"
LOCAL = Path(r"C:\Users\casey\AppData\Local\Temp\cliplab_models_sync2")
SRC = Path(r"D:\games\asd\.claude\worktrees\laughing-mclean-b5c91d\cliplab")
# The endpoint container is an isolated, single-tenant ephemeral workspace.
DATA_ROOT = "/tmp/cliplab"  # nosec B108

if LOCAL.exists():
    import shutil
    shutil.rmtree(LOCAL)
LOCAL.mkdir()
subprocess.run([
    "scp", "-o", "StrictHostKeyChecking=no", "-P", "22047", "-r",
    "root@194.68.245.4:/workspace/studio/cliplab", str(LOCAL)
], check=True, timeout=600)

print("local files:")
for p in LOCAL.rglob("*"):
    if p.is_file():
        print(" ", p.relative_to(LOCAL))

BOOT = base64.b64encode(f'''
import base64, runpod
from pathlib import Path
ROOT = Path("{DATA_ROOT}")
def handler(job):
    i = dict(job.get("input") or job or {{}})
    t = i.get("task","health")
    if t=="health":
        vp=ROOT/"models/virality/v1/model.pt"
        rp=ROOT/"models/reframe/v1/tracker.pt"
        return {{"ok":True,"virality_weights":vp.exists(),"reframe_weights":rp.exists(),"virality_path":str(vp)}}
    if t=="bootstrap_weights":
        for rel,b64 in dict(i.get("files") or {{}}).items():
            d=ROOT/str(rel).lstrip("/"); d=Path(d); d.parent.mkdir(parents=True, exist_ok=True); d.write_bytes(base64.b64decode(b64))
        return {{"ok":True}}
    return {{"error":"unknown"}}
runpod.serverless.start({{"handler":handler}})
'''.encode()).decode()

FULL = (
    f"bash -lc 'pip install -q runpod torch opencv-python-headless sentence-transformers && "
    f"export STUDIO_APP_DATA_DIR={DATA_ROOT} && cd {DATA_ROOT}/cliplab_src/cliplab/runpod && python inference_handler.py'"
)


def gql(q, v=None):
    r = httpx.post(f"https://api.runpod.io/graphql?api_key={api}", json={"query": q, "variables": v or {}}, timeout=90).json()
    if r.get("errors"):
        raise RuntimeError(json.dumps(r["errors"]))
    return r


def run_wait(inp, timeout=600):
    r = httpx.post(f"https://api.runpod.ai/v2/{EP}/run", headers={"Authorization": f"Bearer {api}"}, json={"input": inp}, timeout=60).json()
    jid = r["id"]
    t0 = time.time()
    while time.time() - t0 < timeout:
        s = httpx.get(f"https://api.runpod.ai/v2/{EP}/status/{jid}", headers={"Authorization": f"Bearer {api}"}, timeout=60).json()
        if s.get("status") == "COMPLETED":
            return s
        if s.get("status") in {"FAILED", "CANCELLED"}:
            raise RuntimeError(json.dumps(s))
        time.sleep(8)
    raise TimeoutError(jid)

# reset workers by scaling to 0 then back
gql("mutation($input: EndpointInput!) { saveEndpoint(input: $input) { id } }", {"input": {
    "id": EP, "name": "cliplab-inference", "templateId": TID,
    "gpuIds": "AMPERE_16,-NVIDIA RTX A4500,-NVIDIA RTX 4000 Ada Generation,-NVIDIA RTX 2000 Ada Generation",
    "networkVolumeId": None, "idleTimeout": 5, "scalerType": "QUEUE_DELAY", "scalerValue": 4,
    "workersMin": 0, "workersMax": 0,
}})
time.sleep(30)
gql("mutation($input: SaveTemplateInput!) { saveTemplate(input: $input) { id } }", {"input": {
    "id": TID, "name": "cliplab-inference-sl", "imageName": "runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel",
    "dockerArgs": f"bash -lc \"pip install -q runpod && echo {BOOT} | base64 -d | python\"",
    "ports": "", "volumeInGb": 0, "containerDiskInGb": 40, "isServerless": True, "env": [],
}})
gql("mutation($input: EndpointInput!) { saveEndpoint(input: $input) { id } }", {"input": {
    "id": EP, "name": "cliplab-inference", "templateId": TID,
    "gpuIds": "AMPERE_16,-NVIDIA RTX A4500,-NVIDIA RTX 4000 Ada Generation,-NVIDIA RTX 2000 Ada Generation",
    "networkVolumeId": None, "idleTimeout": 120, "scalerType": "QUEUE_DELAY", "scalerValue": 4,
    "workersMin": 0, "workersMax": 2,
}})
time.sleep(60)

files = {}
cliplab_root = LOCAL / "cliplab"
for path in cliplab_root.rglob("*"):
    if path.is_file():
        rel = path.relative_to(cliplab_root).as_posix()
        if rel.startswith("models/"):
            files[rel] = base64.b64encode(path.read_bytes()).decode()
for path in SRC.rglob("*"):
    if path.is_file() and path.suffix not in {".md", ".pyc"}:
        files[f"cliplab_src/cliplab/{path.relative_to(SRC).as_posix()}"] = base64.b64encode(path.read_bytes()).decode()

print("pushing", len(files), "files")
batch, sz = {}, 0
for k, v in files.items():
    if sz + len(v) > 2_000_000 and batch:
        print(run_wait({"task": "bootstrap_weights", "files": batch}).get("output"))
        batch, sz = {}, 0
    batch[k] = v
    sz += len(v)
if batch:
    print(run_wait({"task": "bootstrap_weights", "files": batch}).get("output"))

print("health bootstrap", run_wait({"task": "health"}))

gql("mutation($input: SaveTemplateInput!) { saveTemplate(input: $input) { id } }", {"input": {
    "id": TID, "name": "cliplab-inference-sl", "imageName": "runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel",
    "dockerArgs": FULL, "ports": "", "volumeInGb": 0, "containerDiskInGb": 40, "isServerless": True,
    "env": [{"key": "STUDIO_APP_DATA_DIR", "value": DATA_ROOT}],
}})
gql("mutation($input: EndpointInput!) { saveEndpoint(input: $input) { id } }", {"input": {
    "id": EP, "name": "cliplab-inference", "templateId": TID,
    "gpuIds": "AMPERE_16,-NVIDIA RTX A4500,-NVIDIA RTX 4000 Ada Generation,-NVIDIA RTX 2000 Ada Generation",
    "networkVolumeId": None, "idleTimeout": 120, "scalerType": "QUEUE_DELAY", "scalerValue": 4,
    "workersMin": 1, "workersMax": 2,
}})
time.sleep(120)
print("health full", json.dumps(run_wait({"task": "health"}), indent=2))
print("score test", json.dumps(run_wait({
    "task": "score_segments",
    "prompt": "hot takes",
    "segments": [{"start": 1, "end": 30, "virality_score": 70, "transcript_snippet": "this industry is lying to you"}],
    "weights_path": f"{DATA_ROOT}/models/virality/v1/model.pt",
}), indent=2))
