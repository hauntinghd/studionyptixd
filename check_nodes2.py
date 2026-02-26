import json, urllib.request

resp = urllib.request.urlopen("http://localhost:8188/object_info")
data = json.loads(resp.read())

for name in ["CLIPVisionEncode", "wanBlockSwap"]:
    node = data.get(name, {})
    if node:
        print(f"{name} inputs:")
        inputs = node.get("input", {})
        for cat in ["required", "optional"]:
            if cat in inputs:
                print(f"  {cat}:")
                for k, v in inputs[cat].items():
                    print(f"    {k}: {v}")
        print()
    else:
        print(f"{name}: NOT FOUND")
