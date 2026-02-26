import json, urllib.request

resp = urllib.request.urlopen("http://localhost:8188/object_info")
data = json.loads(resp.read())

node = data.get("WanImageToVideo", {})
print("WanImageToVideo inputs:")
inputs = node.get("input", {})
for cat in ["required", "optional"]:
    if cat in inputs:
        print(f"  {cat}:")
        for k, v in inputs[cat].items():
            print(f"    {k}: {v}")

print()
node2 = data.get("Wan22ImageToVideoLatent", {})
if node2:
    print("Wan22ImageToVideoLatent inputs:")
    inputs2 = node2.get("input", {})
    for cat in ["required", "optional"]:
        if cat in inputs2:
            print(f"  {cat}:")
            for k, v in inputs2[cat].items():
                print(f"    {k}: {v}")
