"""Minimal handler to verify RunPod serverless works at all."""
import runpod

def handler(event):
    return {"status_code": 200, "body": {"ok": True, "msg": "minimal handler alive"}}

runpod.serverless.start({"handler": handler})
