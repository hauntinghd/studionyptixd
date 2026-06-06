import httpx
from pathlib import Path

EP = "lmsndljarhrspn"
env = {}
for line in Path(r"D:\games\asd\runpod-serverless\.runpod.env").read_text().splitlines():
    if "=" in line and not line.strip().startswith("#"):
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
api = env["RUNPOD_API_KEY"]
studio_ep = env["RUNPOD_ENDPOINT_ID"]

def gql(q, v=None):
    r = httpx.post(f"https://api.runpod.io/graphql?api_key={api}", json={"query": q, "variables": v or {}}, timeout=90).json()
    if r.get("errors"):
        raise RuntimeError(r["errors"])
    return r

template_id = next(e["templateId"] for e in gql("query { myself { endpoints { id templateId } } }")["data"]["myself"]["endpoints"] if e["id"] == studio_ep)
tmpl = gql("query($id: String!) { podTemplate(id: $id) { name imageName dockerArgs env { key value } } }", {"id": template_id})["data"]["podTemplate"]
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
gql("mutation($input: SaveTemplateInput!) { saveTemplate(input: $input) { id } }", {"input": {
    "id": template_id, "name": tmpl["name"], "imageName": tmpl["imageName"],
    "dockerArgs": tmpl.get("dockerArgs") or "", "ports": "", "volumeInGb": 0,
    "containerDiskInGb": 50, "isServerless": True, "env": env_list,
}})
p = Path(r"D:\games\asd\runpod-serverless\.runpod.env")
lines = p.read_text().splitlines()
lines = [f"RUNPOD_CLIPLAB_ENDPOINT_ID={EP}" if l.startswith("RUNPOD_CLIPLAB_ENDPOINT_ID=") else l for l in lines]
p.write_text("\n".join(lines) + "\n")
print("patched", EP)
