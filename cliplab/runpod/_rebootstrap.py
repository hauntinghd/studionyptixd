"""Re-bootstrap existing endpoint lmsndljarhrspn after path fix."""
import base64
import httpx
import json
import time
from pathlib import Path

EP = "lmsndljarhrspn"
LOCAL = Path(r"C:\Users\casey\AppData\Local\Temp\cliplab_models_sync2\cliplab")
SRC = Path(r"D:\games\asd\.claude\worktrees\laughing-mclean-b5c91d\cliplab")
env = {}
for line in Path(r"D:\games\asd\runpod-serverless\.runpod.env").read_text().splitlines():
    if "=" in line and not line.strip().startswith("#"):
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
api = env["RUNPOD_API_KEY"]

def run_wait(inp, timeout=900):
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

files = {}
for path in (LOCAL / "models").rglob("*"):
    if path.is_file():
        files[f"models/{path.relative_to(LOCAL / 'models').as_posix()}"] = base64.b64encode(path.read_bytes()).decode()
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
print("score", json.dumps(run_wait({"task": "score_segments", "prompt": "hot takes", "segments": [
    {"start": 1, "end": 30, "virality_score": 70, "transcript_snippet": "this industry is lying to you"},
]}), indent=2))
