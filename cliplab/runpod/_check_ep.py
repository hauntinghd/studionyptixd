import httpx
import json
from pathlib import Path

env = {}
for line in Path(r"D:\games\asd\runpod-serverless\.runpod.env").read_text().splitlines():
    if "=" in line and not line.strip().startswith("#"):
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()

api = env["RUNPOD_API_KEY"]
q = """
query {
  myself {
    endpoints {
      id name workersMin workersMax templateId
    }
  }
}
"""
r = httpx.post(f"https://api.runpod.io/graphql?api_key={api}", json={"query": q}, timeout=60).json()
print(json.dumps(r, indent=2))

for ep in r.get("data", {}).get("myself", {}).get("endpoints", []):
    if ep.get("name") == "cliplab-inference":
        print("cliplab endpoint", ep)
