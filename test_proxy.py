import json, urllib.request

workflow = {
    "prompt": {
        "4": {
            "class_type": "CheckpointLoaderSimple",
            "inputs": {"ckpt_name": "sd_xl_base_1.0.safetensors"},
        },
        "5": {
            "class_type": "CLIPTextEncode",
            "inputs": {"text": "a skeleton", "clip": ["4", 1]},
        },
        "6": {
            "class_type": "CLIPTextEncode",
            "inputs": {"text": "bad", "clip": ["4", 1]},
        },
        "7": {
            "class_type": "EmptyLatentImage",
            "inputs": {"width": 576, "height": 1024, "batch_size": 1},
        },
        "3": {
            "class_type": "KSampler",
            "inputs": {
                "model": ["4", 0], "positive": ["5", 0], "negative": ["6", 0],
                "latent_image": ["7", 0], "seed": 42, "steps": 25,
                "cfg": 7.0, "sampler_name": "euler_ancestral",
                "scheduler": "normal", "denoise": 1.0,
            },
        },
        "8": {"class_type": "VAEDecode", "inputs": {"samples": ["3", 0], "vae": ["4", 2]}},
        "9": {"class_type": "SaveImage", "inputs": {"images": ["8", 0], "filename_prefix": "test"}},
    }
}

data = json.dumps(workflow).encode()

for url in ["http://localhost:8188/prompt", "https://ow0iv7oclt5try-8188.proxy.runpod.net/prompt"]:
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        print(f"OK {url}: {resp.read().decode()[:100]}")
    except Exception as e:
        body = e.read().decode()[:300] if hasattr(e, 'read') else str(e)
        print(f"FAIL {url}: {body}")
