#!/usr/bin/env python3
"""Train on JJRR pod, deploy ClipLab endpoint, push weights to Studio volume."""
from __future__ import annotations

import base64
import json
import subprocess
import sys
import time
from pathlib import Path

import httpx

WORKTREE = Path(r"D:\games\asd\.claude\worktrees\laughing-mclean-b5c91d")
ENV_FILE = Path(r"D:\games\asd\runpod-serverless\.runpod.env")
JJRR_POD = ("194.68.245.4", 22047)
REMOTE_ROOT = "/workspace/studio_cliplab"


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
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


def ssh(cmd: str, timeout: int = 7200) -> None:
    host, port = JJRR_POD
    print(f"$ {cmd[:100]}...")
    subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=no", "-p", str(port), f"root@{host}", cmd],
        check=True,
        timeout=timeout,
    )


def scp_up(local: str, remote: str) -> None:
    host, port = JJRR_POD
    subprocess.run(
        ["scp", "-o", "StrictHostKeyChecking=no", "-P", str(port), "-r", local, f"root@{host}:{remote}"],
        check=True,
        timeout=600,
    )


def scp_down(remote: str, local: str) -> None:
    host, port = JJRR_POD
    subprocess.run(
        ["scp", "-o", "StrictHostKeyChecking=no", "-P", str(port), "-r", f"root@{host}:{remote}", local],
        check=True,
        timeout=600,
    )


BOOTSTRAP_CMD = (
    "bash -lc \"pip install -q runpod && python -c \\\""
    "import base64,os,runpod; from pathlib import Path;"
    "V=Path(os.getenv('STUDIO_APP_DATA_DIR','/runpod-volume/studio'));"
    "def h(e):"
    " i=(e.get('input') or e); t=i.get('task','health');"
    " if t=='health': return {'ok':True,'volume':str(V)};"
    " if t=='bootstrap_weights':"
    "  w=[];"
    "  [((V/p).parent.mkdir(parents=True,exist_ok=True) or (V/p).write_bytes(base64.b64decode(b)) or w.append(str(V/p))) for p,b in (i.get('files') or {}).items() if not '..' in str(p)];"
    "  return {'ok':True,'written':w};"
    " return {'error':'unknown task'};"
    "runpod.serverless.start({'handler':h})\\\"\""
)

FULL_CMD = (
    "bash -lc 'pip install -q runpod opencv-python-headless sentence-transformers && "
    "export STUDIO_APP_DATA_DIR={app_data} && "
    "cd /runpod-volume/studio/cliplab_src/cliplab/runpod && "
    "python inference_handler.py'"
)


def _template_id_by_name(api_key: str, name: str) -> str | None:
    data = gql(
        api_key,
        "query { myself { podTemplates { id name } } }",
    )
    for tmpl in (data.get("data") or {}).get("myself", {}).get("podTemplates") or []:
        if tmpl.get("name") == name:
            return tmpl.get("id")
    return None


def upsert_cliplab_endpoint(api_key: str, env: dict[str, str], *, docker_args: str | None = None) -> str:
    volume_id = env["NETWORK_VOLUME_ID"]
    app_data = env.get("STUDIO_APP_DATA_DIR", "/runpod-volume/studio")
    image = "runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel"
    start_cmd = docker_args or FULL_CMD.format(app_data=app_data)
    tmpl_input: dict = {
        "name": "cliplab-inference",
        "imageName": image,
        "dockerArgs": start_cmd,
        "ports": "",
        "volumeInGb": 0,
        "containerDiskInGb": 30,
        "isServerless": True,
        "env": [
            {"key": "STUDIO_APP_DATA_DIR", "value": app_data},
            {"key": "PYTHONPATH", "value": "/runpod-volume/studio/cliplab_src"},
        ],
    }
    existing = _template_id_by_name(api_key, "cliplab-inference")
    if existing:
        tmpl_input["id"] = existing
    tmpl = gql(
        api_key,
        "mutation($input: SaveTemplateInput!) { saveTemplate(input: $input) { id } }",
        {"input": tmpl_input},
    )
    template_id = tmpl["data"]["saveTemplate"]["id"]

    existing_ep = gql(
        api_key,
        "query { myself { endpoints { id name templateId } } }",
    )
    endpoint_id = None
    for ep in (existing_ep.get("data") or {}).get("myself", {}).get("endpoints") or []:
        if ep.get("name") == "cliplab-inference":
            endpoint_id = ep.get("id")
            break

    ep_input = {
        "name": "cliplab-inference",
        "templateId": template_id,
        "gpuIds": "AMPERE_16",
        "networkVolumeId": volume_id,
        "locations": "",
        "idleTimeout": 5,
        "scalerType": "QUEUE_DELAY",
        "scalerValue": 4,
        "workersMin": 0,
        "workersMax": 2,
    }
    if endpoint_id:
        ep_input["id"] = endpoint_id
    ep = gql(
        api_key,
        "mutation($input: EndpointInput!) { saveEndpoint(input: $input) { id } }",
        {"input": ep_input},
    )
    return ep["data"]["saveEndpoint"]["id"]


def push_cliplab_src(api_key: str, endpoint_id: str) -> None:
    """Upload handler source onto Studio volume via bootstrap_weights."""
    files: dict[str, str] = {}
    src_root = WORKTREE / "cliplab"
    for path in src_root.rglob("*"):
        if not path.is_file() or path.suffix in {".pyc", ".md"}:
            continue
        rel = f"cliplab_src/cliplab/{path.relative_to(src_root).as_posix()}"
        files[rel] = base64.b64encode(path.read_bytes()).decode("ascii")
    # Chunk to stay under payload limits (~few MB per request)
    batch: dict[str, str] = {}
    batch_bytes = 0
    for rel, b64 in files.items():
        size = len(b64)
        if batch_bytes + size > 3_000_000 and batch:
            _runsync(api_key, endpoint_id, {"task": "bootstrap_weights", "files": batch})
            batch, batch_bytes = {}, 0
        batch[rel] = b64
        batch_bytes += size
    if batch:
        _runsync(api_key, endpoint_id, {"task": "bootstrap_weights", "files": batch})


def push_model_weights(api_key: str, endpoint_id: str, models_dir: Path) -> None:
    files: dict[str, str] = {}
    for path in models_dir.rglob("*"):
        if path.is_file():
            rel = f"cliplab/models/{path.relative_to(models_dir).as_posix()}"
            files[rel] = base64.b64encode(path.read_bytes()).decode("ascii")
    _runsync(api_key, endpoint_id, {"task": "bootstrap_weights", "files": files})


def _runsync(api_key: str, endpoint_id: str, inp: dict) -> dict:
    r = httpx.post(
        f"https://api.runpod.ai/v2/{endpoint_id}/runsync",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"input": inp},
        timeout=300,
    )
    r.raise_for_status()
    out = r.json()
    print(json.dumps(out, indent=2)[:2000])
    return out


def patch_studio_template(api_key: str, env: dict[str, str], cliplab_ep: str) -> None:
    endpoint_id = env.get("RUNPOD_ENDPOINT_ID", "")
    if not endpoint_id:
        return
    ep = gql(api_key, "query($id: String!) { endpoint(id: $id) { templateId } }", {"id": endpoint_id})
    template_id = ep["data"]["endpoint"]["templateId"]
    tmpl = gql(
        api_key,
        "query($id: String!) { podTemplate(id: $id) { name imageName dockerArgs env { key value } } }",
        {"id": template_id},
    )["data"]["podTemplate"]
    env_list = list(tmpl.get("env") or [])
    updates = {
        "CLIPLAB_VIRALITY_BACKEND": "runpod_custom_v1",
        "CLIPLAB_REFRAME_BACKEND": "runpod_face_v1",
        "RUNPOD_CLIPLAB_ENDPOINT_ID": cliplab_ep,
    }
    for k, v in updates.items():
        found = False
        for e in env_list:
            if e["key"] == k:
                e["value"] = v
                found = True
        if not found:
            env_list.append({"key": k, "value": v})
    gql(
        api_key,
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
                "env": env_list,
            }
        },
    )


def save_env_key(key: str, val: str) -> None:
    lines = ENV_FILE.read_text(encoding="utf-8").splitlines()
    replaced = False
    for i, line in enumerate(lines):
        if line.startswith(f"{key}="):
            lines[i] = f"{key}={val}"
            replaced = True
    if not replaced:
        lines.append(f"{key}={val}")
    ENV_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-train", action="store_true", help="Skip JJRR pod training (weights already on pod)")
    args = ap.parse_args()

    env = load_env()
    api_key = env["RUNPOD_API_KEY"]

    local_models = Path(r"C:\Users\casey\AppData\Local\Temp\cliplab_models_sync")

    if not args.skip_train:
        ssh(f"mkdir -p {REMOTE_ROOT}")
        scp_up(str(WORKTREE / "cliplab"), f"{REMOTE_ROOT}/")

        train_cmd = f"""
set -euo pipefail
export STUDIO_APP_DATA_DIR=/workspace/studio
export PYTHONPATH={REMOTE_ROOT}
cd {REMOTE_ROOT}
PY=/workspace/JJRR/.venv/bin/python
PIP=/workspace/JJRR/.venv/bin/pip
sed -i 's/\\r$//' cliplab/runpod/*.sh 2>/dev/null || true
$PIP install -q torch torchvision opencv-python-headless sentence-transformers runpod
bash cliplab/runpod/setup_volume.sh
$PY cliplab/runpod/bootstrap_feedback.py
$PY cliplab/runpod/bootstrap_opencv_reframe.py
$PY cliplab/runpod/train_virality_scorer.py --epochs 5
$PY cliplab/runpod/train_face_reframe.py --epochs 10
$PY cliplab/runpod/activate_registry.py
ls -la /workspace/studio/cliplab/models/virality/v1/
"""
        ssh(train_cmd)

    if local_models.exists():
        import shutil
        shutil.rmtree(local_models)
    local_models.mkdir(parents=True)
    scp_down("/workspace/studio/cliplab", str(local_models))

    print("Creating ClipLab inference endpoint (bootstrap mode)...")
    cliplab_ep = upsert_cliplab_endpoint(api_key, env, docker_args=BOOTSTRAP_CMD)
    print(f"Endpoint: {cliplab_ep}")

    print("Uploading handler source to Studio volume (may take 2-3 min)...")
    time.sleep(45)
    push_cliplab_src(api_key, cliplab_ep)

    print("Uploading trained weights...")
    push_model_weights(api_key, cliplab_ep, local_models / "models")

    print("Switching endpoint to full inference handler...")
    upsert_cliplab_endpoint(api_key, env)

    print("Health check...")
    health = _runsync(api_key, cliplab_ep, {"task": "health"})
    out = health.get("output") or health
    if not out.get("virality_weights"):
        print("WARN: virality weights not visible yet", file=sys.stderr)

    patch_studio_template(api_key, env, cliplab_ep)
    save_env_key("RUNPOD_CLIPLAB_ENDPOINT_ID", cliplab_ep)

    print("\n=== CLIPLAB LIVE ===")
    print(f"RUNPOD_CLIPLAB_ENDPOINT_ID={cliplab_ep}")
    print("CLIPLAB_VIRALITY_BACKEND=runpod_custom_v1")
    print("CLIPLAB_REFRAME_BACKEND=runpod_face_v1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
