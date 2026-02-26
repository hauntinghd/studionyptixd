import json, urllib.request

resp = urllib.request.urlopen("http://localhost:8188/object_info")
data = json.loads(resp.read())

targets = [
    "ModelSamplingWanVideo",
    "WanImageToVideo",
    "WanVideoMoEExpertSelect",
    "LatentUpscaleBy",
    "UNETLoader",
    "CLIPLoader",
    "CLIPVisionLoader",
    "VAELoader",
    "WanFunControlToVideo",
    "ModelSamplingSD3",
]

for name in targets:
    status = "YES" if name in data else "NO"
    print(f"{name}: {status}")

print(f"\nTotal nodes available: {len(data)}")
wan_nodes = [k for k in data if "wan" in k.lower()]
print(f"Wan-related nodes: {wan_nodes}")
