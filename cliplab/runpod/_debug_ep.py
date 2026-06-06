import httpx
import json
from pathlib import Path

env = {}
for line in Path(r"D:\games\asd\runpod-serverless\.runpod.env").read_text().splitlines():
    if "=" in line and not line.strip().startswith("#"):
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()

api = env["RUNPOD_API_KEY"]

# Get studio endpoint details via REST if available
for ep_id in ["ajt1r1estd85z8", "30lt3s0grkw5le"]:
    r = httpx.get(
        f"https://api.runpod.ai/v2/{ep_id}/health",
        headers={"Authorization": f"Bearer {api}"},
        timeout=30,
    )
    print(ep_id, "health", r.status_code, r.text[:500])

# List volumes
q = "query { myself { networkVolumes { id name dataCenterId size } endpoints { id name networkVolumeId gpuIds locations workersMin } } }"
r = httpx.post(f"https://api.runpod.io/graphql?api_key={api}", json={"query": q}, timeout=60).json()
print(json.dumps(r, indent=2))
