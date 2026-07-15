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
TID = None  # create fresh serverless template each deploy
LOCAL = Path(r"C:\Users\casey\AppData\Local\Temp\cliplab_models_sync2\cliplab")
SRC = Path(r"D:\games\asd\.claude\worktrees\laughing-mclean-b5c91d\cliplab")
# The endpoint container is an isolated, single-tenant ephemeral workspace.
ROOT = "/tmp/cliplab"  # nosec B108

WRAPPER = f'''
import base64, importlib.util, os, runpod, sys
from pathlib import Path
ROOT = Path("{ROOT}")

def _full():
    os.environ["STUDIO_APP_DATA_DIR"] = str(ROOT / "cliplab")
    p = ROOT / "cliplab_src/cliplab/runpod/inference_handler.py"
    if not p.exists():
        return None
    try:
        spec = importlib.util.spec_from_file_location("cliplab_ih", p)
        mod = importlib.util.module_from_spec(spec)
        sys.path.insert(0, str(p.parent))
        spec.loader.exec_module(mod)
        return mod.handler
    except Exception as e:
        import traceback
        return lambda job: {{"error": "import_failed", "detail": str(e), "trace": traceback.format_exc()}}

def handler(job):
    i = dict(job.get("input") or job or {{}})
    t = i.get("task", "health")
    if t == "bootstrap_weights":
        for rel, b64 in dict(i.get("files") or {{}}).items():
            dest = ROOT / str(rel).lstrip("/")
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(base64.b64decode(b64))
        return {{"ok": True, "written": len(i.get("files") or {{}})}}
    h = _full()
    if h is None:
        vp = ROOT / "cliplab/models/virality/v1/model.pt"
        return {{"ok": True, "bootstrapped": False, "virality_weights": vp.exists()}}
    return h(job)

runpod.serverless.start({{"handler": handler}})
'''

START = (
    "bash -lc \"pip install -q runpod torch opencv-python-headless sentence-transformers && echo "
    + base64.b64encode(WRAPPER.encode()).decode()
    + " | base64 -d | python\""
)


def gql(q, v=None):
    r = httpx.post(f"https://api.runpod.io/graphql?api_key={api}", json={"query": q, "variables": v or {}}, timeout=90).json()
    if r.get("errors"):
        raise RuntimeError(json.dumps(r["errors"]))
    return r


def run_wait(inp, timeout=900):
    r = httpx.post(f"https://api.runpod.ai/v2/{EP}/run", headers={"Authorization": f"Bearer {api}"}, json={"input": inp}, timeout=60).json()
    jid = r["id"]
    t0 = time.time()
    while time.time() - t0 < timeout:
        s = httpx.get(f"https://api.runpod.ai/v2/{EP}/status/{jid}", headers={"Authorization": f"Bearer {api}"}, timeout=60).json()
        if s.get("status") == "COMPLETED":
            return s
        if s.get("status") in {"FAILED", "CANCELLED"}:
            raise RuntimeError(json.dumps(s, indent=2))
        time.sleep(10)
    raise TimeoutError(jid)


if not LOCAL.exists():
    subprocess.run(["scp", "-o", "StrictHostKeyChecking=no", "-P", "22047", "-r",
                    "root@194.68.245.4:/workspace/studio/cliplab", str(LOCAL.parent)], check=True)

tmpl_in = {
    "name": "cliplab-inference-v2",
    "imageName": "runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel",
    "dockerArgs": START, "ports": "", "volumeInGb": 0, "containerDiskInGb": 50,
    "isServerless": True, "env": [],
}
tid = gql("mutation($input: SaveTemplateInput!) { saveTemplate(input: $input) { id } }", {"input": tmpl_in})["data"]["saveTemplate"]["id"]
print("template", tid)

gql("mutation($input: EndpointInput!) { saveEndpoint(input: $input) { id } }", {"input": {
    "id": EP, "name": "cliplab-inference", "templateId": tid,
    "gpuIds": "AMPERE_16,-NVIDIA RTX A4500,-NVIDIA RTX 4000 Ada Generation,-NVIDIA RTX 2000 Ada Generation",
    "networkVolumeId": None, "idleTimeout": 120, "scalerType": "QUEUE_DELAY", "scalerValue": 4,
    "workersMin": 0, "workersMax": 2,
}})
time.sleep(60)

files = {}
for path in LOCAL.rglob("*"):
    if path.is_file():
        files[f"cliplab/{path.relative_to(LOCAL).as_posix()}"] = base64.b64encode(path.read_bytes()).decode()
for path in SRC.rglob("*"):
    if path.is_file() and path.suffix not in {".md", ".pyc"}:
        files[f"cliplab_src/cliplab/{path.relative_to(SRC).as_posix()}"] = base64.b64encode(path.read_bytes()).decode()

batch, sz = {}, 0
for k, v in files.items():
    if sz + len(v) > 2_000_000 and batch:
        print("push", run_wait({"task": "bootstrap_weights", "files": batch}).get("output"))
        batch, sz = {}, 0
    batch[k] = v
    sz += len(v)
if batch:
    print("push", run_wait({"task": "bootstrap_weights", "files": batch}).get("output"))

print("health", json.dumps(run_wait({"task": "health"}), indent=2))
print("score", json.dumps(run_wait({
    "task": "score_segments",
    "prompt": "hot takes",
    "segments": [{"start": 1, "end": 30, "virality_score": 70, "transcript_snippet": "this industry is lying to you"}],
}), indent=2))

gql("mutation($input: EndpointInput!) { saveEndpoint(input: $input) { id } }", {"input": {
    "id": EP, "name": "cliplab-inference", "templateId": tid,
    "gpuIds": "AMPERE_16,-NVIDIA RTX A4500,-NVIDIA RTX 4000 Ada Generation,-NVIDIA RTX 2000 Ada Generation",
    "networkVolumeId": None, "idleTimeout": 120, "scalerType": "QUEUE_DELAY", "scalerValue": 4,
    "workersMin": 1, "workersMax": 2,
}})

studio_ep = env["RUNPOD_ENDPOINT_ID"]
template_id = next(e["templateId"] for e in gql("query { myself { endpoints { id templateId } } }")["data"]["myself"]["endpoints"] if e["id"] == studio_ep)
tmpl = gql("query($id: String!) { podTemplate(id: $id) { name imageName dockerArgs env { key value } } }", {"id": template_id})["data"]["podTemplate"]
env_list = list(tmpl.get("env") or [])
for k, v in {"CLIPLAB_VIRALITY_BACKEND": "runpod_custom_v1", "CLIPLAB_REFRAME_BACKEND": "runpod_face_v1", "RUNPOD_CLIPLAB_ENDPOINT_ID": EP}.items():
    if any(e["key"] == k for e in env_list):
        for e in env_list:
            if e["key"] == k: e["value"] = v
    else:
        env_list.append({"key": k, "value": v})
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
print("DONE", EP)
