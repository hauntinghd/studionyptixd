"""Query fal.ai request history for 3D weapon/pistol model generations."""
from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ENV_PATH = Path(r"D:\Games\asd\.env")
BASE = "https://api.fal.ai/v1"
WEAPON_RE = re.compile(
    r"pistol|handgun|glock|revolver|beretta|9mm|firearm|sidearm|"
    r"hand.?gun|sig sauer|1911|desert eagle|smith.?wesson|weapon|gun\b|rifle|ak-?47|ar-?15",
    re.I,
)

# Known fal text/image-to-3D endpoints (expand as needed)
ENDPOINTS_3D = [
    "fal-ai/hunyuan3d/v3.1/rapid/text-to-3d",
    "fal-ai/hunyuan3d/v3.1/text-to-3d",
    "fal-ai/hunyuan3d/v2/text-to-3d",
    "fal-ai/hyper3d/rodin",
    "fal-ai/trellis",
    "fal-ai/trellis/image-to-3d",
    "tripo3d/tripo/v2.5/text-to-3d",
    "tripo3d/tripo3d/h3.1/text-to-3d",
    "meshy/v5/text-to-3d",
    "meshy/v5/image-to-3d",
    "fal-ai/unique3d",
    "fal-ai/unique3d/image-to-3d",
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
    if not ENV_PATH.exists():
        return
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        v = v.strip().strip('"').strip("'")
        os.environ.setdefault(k.strip(), v)


def fal_get(path: str, params: dict | None = None) -> dict:
    key = (os.environ.get("FAL_AI_KEY") or os.environ.get("FAL_KEY") or "").strip()
    if not key:
        raise SystemExit("FAL_AI_KEY not set")
    q = urllib.parse.urlencode(params or {}, doseq=True)
    url = require_fal_api_url(f"{BASE}{path}" + (f"?{q}" if q else ""))
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Key {key}", "Accept": "application/json"},
        method="GET",
    )
    try:
        # The request URL is HTTPS and host-validated above.
        with urllib.request.urlopen(req, timeout=60) as resp:  # nosec B310
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:500]
        raise SystemExit(f"HTTP {exc.code} {path}: {body}") from exc


def search_3d_models() -> list[str]:
    ids: set[str] = set(ENDPOINTS_3D)
    for q in ("3d", "mesh", "hunyuan", "tripo", "rodin", "trellis", "meshy"):
        try:
            data = fal_get("/models", {"limit": 100, "q": q})
        except SystemExit:
            continue
        items = data.get("models") or data.get("items") or []
        for m in items:
            eid = m.get("endpoint_id") or m.get("id")
            if eid and ("3d" in eid.lower() or "mesh" in eid.lower() or "rodin" in eid.lower()):
                ids.add(eid)
    return sorted(ids)


def iter_requests(endpoint_ids: list[str], start: str = "2025-01-01") -> list[dict]:
    all_items: list[dict] = []
    for eid in endpoint_ids:
        cursor = None
        while True:
            params: dict = {
                "endpoint_id": eid,
                "start": start,
                "limit": 100,
                "status": "success",
                "expand": "payloads",
            }
            if cursor:
                params["cursor"] = cursor
            try:
                data = fal_get("/models/requests/by-endpoint", params)
            except SystemExit as exc:
                msg = str(exc)
                if "404" in msg or "not_found" in msg.lower():
                    break
                raise
            items = data.get("items") or []
            all_items.extend(items)
            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")
            if not cursor:
                break
    return all_items


def prompt_from_item(item: dict) -> str:
    inp = item.get("json_input") or {}
    if isinstance(inp, str):
        return inp
    for k in ("prompt", "text", "caption", "input_prompt"):
        if inp.get(k):
            return str(inp[k])
    return json.dumps(inp)[:300]


def urls_from_output(item: dict) -> list[str]:
    out = item.get("json_output") or {}
    urls: list[str] = []

    def walk(obj):
        if isinstance(obj, str) and (
            obj.startswith("http") and any(x in obj.lower() for x in (".glb", ".obj", ".fbx", ".zip", "fal.media", "cdn"))
        ):
            urls.append(obj)
        elif isinstance(obj, dict):
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)

    walk(out)
    return urls


def main() -> None:
    load_env()
    endpoint_ids = search_3d_models()
    print(f"Checking {len(endpoint_ids)} 3D endpoints since 2025-01-01...")
    items = iter_requests(endpoint_ids)
    print(f"Total successful 3D requests: {len(items)}")

    weapon_hits: list[dict] = []
    for item in items:
        prompt = prompt_from_item(item)
        blob = json.dumps(item.get("json_input") or {}) + json.dumps(item.get("json_output") or {})
        if WEAPON_RE.search(prompt) or WEAPON_RE.search(blob):
            weapon_hits.append(item)

    print(f"Weapon-related hits: {len(weapon_hits)}\n")
    for item in weapon_hits:
        prompt = prompt_from_item(item)
        urls = urls_from_output(item)
        print("=" * 72)
        print(f"request_id: {item.get('request_id')}")
        print(f"endpoint:   {item.get('endpoint_id')}")
        print(f"ended_at:   {item.get('ended_at')}")
        print(f"prompt:     {prompt[:200]}")
        for u in urls:
            print(f"url:        {u}")
        if not urls:
            print(f"output:     {json.dumps(item.get('json_output'), indent=2)[:600]}")

    # Also dump all prompts for manual scan if no weapon regex hits
    if not weapon_hits and items:
        print("\nNo regex weapon hits. All 3D prompts:")
        for item in items:
            print(f"- [{item.get('endpoint_id')}] {prompt_from_item(item)[:120]}")


if __name__ == "__main__":
    main()
