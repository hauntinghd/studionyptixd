import httpx
import json
from pathlib import Path

env = {}
for line in Path(r"D:\games\asd\runpod-serverless\.runpod.env").read_text().splitlines():
    if "=" in line and not line.strip().startswith("#"):
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()

api = env["RUNPOD_API_KEY"]
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

# Delete bad pod template if possible
for mut in [
    ("mutation { deleteTemplate(templateName: \"cliplab-inference\") }", {}),
]:
    try:
        r = httpx.post(
            f"https://api.runpod.io/graphql?api_key={api}",
            json={"query": mut[0], "variables": mut[1]},
            timeout=60,
        ).json()
        print("delete", json.dumps(r, indent=2))
    except Exception as e:
        print("delete err", e)

mut = "mutation($input: SaveTemplateInput!) { saveTemplate(input: $input) { id name isServerless } }"
inp = {
    "name": "cliplab-inference-sl",
    "imageName": "runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel",
    "dockerArgs": BOOTSTRAP_CMD,
    "ports": "",
    "volumeInGb": 0,
    "containerDiskInGb": 30,
    "isServerless": True,
    "env": [{"key": "STUDIO_APP_DATA_DIR", "value": "/runpod-volume/studio"}],
}
r = httpx.post(
    f"https://api.runpod.io/graphql?api_key={api}",
    json={"query": mut, "variables": {"input": inp}},
    timeout=60,
).json()
print("create", json.dumps(r, indent=2))

if r.get("data", {}).get("saveTemplate", {}).get("id"):
    tid = r["data"]["saveTemplate"]["id"]
    ep_mut = "mutation($input: EndpointInput!) { saveEndpoint(input: $input) { id name } }"
    ep_inp = {
        "name": "cliplab-inference",
        "templateId": tid,
        "gpuIds": "AMPERE_16",
        "networkVolumeId": env["NETWORK_VOLUME_ID"],
        "locations": "",
        "idleTimeout": 5,
        "scalerType": "QUEUE_DELAY",
        "scalerValue": 4,
        "workersMin": 0,
        "workersMax": 2,
    }
    er = httpx.post(
        f"https://api.runpod.io/graphql?api_key={api}",
        json={"query": ep_mut, "variables": {"input": ep_inp}},
        timeout=60,
    ).json()
    print("endpoint", json.dumps(er, indent=2))
