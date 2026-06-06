#!/usr/bin/env python3
import httpx, json, os, time
api = os.environ["RUNPOD_API_KEY"]
ep = os.environ.get("RUNPOD_CLIPLAB_ENDPOINT_ID", "lmsndljarhrspn")
h = {"Authorization": f"Bearer {api}"}

def sync(inp, timeout=600):
    r = httpx.post(f"https://api.runpod.ai/v2/{ep}/runsync", headers=h, json={"input": inp}, timeout=timeout)
    return r.json()

for attempt in range(3):
    out = sync({"task": "health"}, timeout=600)
    body = out.get("output") or out
    print(f"attempt {attempt + 1} health", json.dumps(body, indent=2))
    if isinstance(body, dict) and body.get("virality_weights"):
        break
    time.sleep(20)

score = sync(
    {
        "task": "score_segments",
        "prompt": "hot takes",
        "segments": [
            {"start": 1, "end": 30, "virality_score": 70, "transcript_snippet": "this industry is lying to you about growth"},
            {"start": 60, "end": 90, "virality_score": 20, "transcript_snippet": "thanks for joining the webinar today"},
        ],
    },
    timeout=600,
)
print("score", json.dumps(score.get("output") or score, indent=2))
