"""Search all fal image-gen request history for glock/pistol prompts."""
from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ENV = Path(r"D:\Games\asd\.env")
BASE = "https://api.fal.ai/v1"
GUN = re.compile(r"glock|pistol|handgun|9mm|sidearm|semi.?auto.*pistol|polymer.*pistol", re.I)
IMAGE_ENDPOINTS = [
    "fal-ai/bytedance/seedream/v4.5/text-to-image",
    "fal-ai/bytedance/seedream/v4/text-to-image",
    "fal-ai/flux/dev",
    "fal-ai/flux/schnell",
    "fal-ai/flux-pro/v1.1",
    "fal-ai/grok-2-image",
    "fal-ai/nano-banana",
    "fal-ai/imagen4/preview",
    "fal-ai/recraft/v3/text-to-image",
]


def require_fal_api_url(url: str) -> str:
    value = str(url or "").strip()
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme.lower() != "https"
        or (parsed.hostname or "").lower() != "api.fal.ai"
        or parsed.username
        or parsed.password
    ):
        raise ValueError("refusing non-HTTPS or non-fal API URL")
    return value


def load_env() -> None:
    for line in ENV.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def get(path: str, params: dict) -> dict:
    key = (os.environ.get("FAL_AI_KEY") or "").strip()
    q = urllib.parse.urlencode(params, doseq=True)
    url = require_fal_api_url(f"{BASE}{path}?{q}")
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Key {key}", "Accept": "application/json"},
    )
    # The request URL is HTTPS and host-validated above.
    with urllib.request.urlopen(req, timeout=60) as r:  # nosec B310
        return json.loads(r.read().decode())


def main() -> None:
    load_env()
    hits = []
    for eid in IMAGE_ENDPOINTS:
        cursor = None
        while True:
            p = {"endpoint_id": eid, "start": "2025-01-01", "limit": 100, "expand": "payloads"}
            if cursor:
                p["cursor"] = cursor
            try:
                data = get("/models/requests/by-endpoint", p)
            except urllib.error.HTTPError:
                break
            for it in data.get("items") or []:
                blob = json.dumps(it)
                if GUN.search(blob):
                    hits.append(it)
            if not data.get("has_more") or not data.get("next_cursor"):
                break
            cursor = data["next_cursor"]

    print(f"Image-gen gun hits: {len(hits)}")
    for it in hits:
        print(json.dumps({
            "endpoint": it.get("endpoint_id"),
            "request_id": it.get("request_id"),
            "ended_at": it.get("ended_at"),
            "input": it.get("json_input"),
            "output": it.get("json_output"),
        }, indent=2)[:2000])
        print("---")


if __name__ == "__main__":
    main()
