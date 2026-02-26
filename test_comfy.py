import json, urllib.request

workflow = {
    "prompt": {
        "4": {
            "class_type": "CheckpointLoaderSimple",
            "inputs": {"ckpt_name": "sd_xl_base_1.0.safetensors"},
        },
        "5": {
            "class_type": "CLIPTextEncode",
            "inputs": {"text": "a 3D skeleton standing in a dark room, cinematic lighting", "clip": ["4", 1]},
        },
        "6": {
            "class_type": "CLIPTextEncode",
            "inputs": {"text": "bad quality, blurry", "clip": ["4", 1]},
        },
        "7": {
            "class_type": "EmptyLatentImage",
            "inputs": {"width": 576, "height": 1024, "batch_size": 1},
        },
        "3": {
            "class_type": "KSampler",
            "inputs": {
                "model": ["4", 0],
                "positive": ["5", 0],
                "negative": ["6", 0],
                "latent_image": ["7", 0],
                "seed": 42,
                "steps": 25,
                "cfg": 7.0,
                "sampler_name": "euler_ancestral",
                "scheduler": "normal",
                "denoise": 1.0,
            },
        },
        "8": {
            "class_type": "VAEDecode",
            "inputs": {"samples": ["3", 0], "vae": ["4", 2]},
        },
        "9": {
            "class_type": "SaveImage",
            "inputs": {"images": ["8", 0], "filename_prefix": "test"},
        },
    }
}

data = json.dumps(workflow).encode()
req = urllib.request.Request("http://localhost:8188/prompt", data=data, headers={"Content-Type": "application/json"})
try:
    resp = urllib.request.urlopen(req)
    print("SUCCESS:", resp.read().decode()[:200])
except Exception as e:
    if hasattr(e, 'read'):
        print("ERROR:", e.read().decode()[:500])
    else:
        print("ERROR:", str(e))
