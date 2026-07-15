"""Dump all fal 3D request outputs and search full JSON for weapon terms."""
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
    r"hand.?gun|sig sauer|1911|desert eagle|smith.?wesson|weapon|gun\b|rifle|ak-?47|ar-?15|firearm",
    re.I,
)

ENDPOINTS_3D = [
    "fal-ai/hyper3d/rodin",
    "fal-ai/hunyuan-3d/v3.1/rapid/text-to-3d",
    "fal-ai/hunyuan-3d/v3.1/rapid/image-to-3d",
    "fal-ai/trellis",
    "fal-ai/trellis/image-to-3d",
    "tripo3d/tripo/v2.5/image-to-3d",
    "tripo3d/tripo/v2.5/text-to-3d",
    "meshy/v5/text-to-3d",
    "meshy/v5/image-to-3d",
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
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def fal_get(key: str, path: str, params: dict | None = None) -> dict:
    q = urllib.parse.urlencode(params or {}, doseq=True)
    url = require_fal_api_url(f"{BASE}{path}" + (f"?{q}" if q else ""))
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Key {key}", "Accept": "application/json"},
        method="GET",
    )
    # The request URL is HTTPS and host-validated above.
    with urllib.request.urlopen(req, timeout=60) as resp:  # nosec B310
        return json.loads(resp.read().decode("utf-8"))


def collect(key_name: str, key: str) -> list[dict]:
    items: list[dict] = []
    for eid in ENDPOINTS_3D:
        cursor = None
        while True:
            params = {
                "endpoint_id": eid,
                "start": "2024-01-01",
                "limit": 100,
                "expand": "payloads",
            }
            if cursor:
                params["cursor"] = cursor
            try:
                data = fal_get(key, "/models/requests/by-endpoint", params)
            except urllib.error.HTTPError as exc:
                if exc.code in (403, 404):
                    break
                raise
            items.extend(data.get("items") or [])
            if not data.get("has_more") or not data.get("next_cursor"):
                break
            cursor = data["next_cursor"]
    for it in items:
        it["_key"] = key_name
    return items


def extract_urls(obj) -> list[str]:
    out: list[str] = []
    if isinstance(obj, str) and obj.startswith("http"):
        out.append(obj)
    elif isinstance(obj, dict):
        for v in obj.values():
            out.extend(extract_urls(v))
    elif isinstance(obj, list):
        for v in obj:
            out.extend(extract_urls(v))
    return out


def main() -> None:
    load_env()
    keys = []
    for name in ["FAL_AI_KEY", "FAL_AI_KEY_2", "FAL_AI_KEY_3", "FAL_AI_KEY_4", "FAL_AI_KEY_5", "FAL_AI_KEY_6", "FAL_KEY"]:
        v = os.environ.get(name, "").strip()
        if v and (name, v) not in [(n, k) for n, k in keys]:
            keys.append((name, v))

    all_items: list[dict] = []
    for name, key in keys:
        try:
            batch = collect(name, key)
            print(f"{name}: {len(batch)} 3D requests")
            all_items.extend(batch)
        except urllib.error.HTTPError as exc:
            print(f"{name}: HTTP {exc.code}")

    print(f"Total: {len(all_items)} requests\n")

    hits = []
    for it in all_items:
        blob = json.dumps(it)
        if WEAPON_RE.search(blob):
            hits.append(it)

    if hits:
        print(f"=== WEAPON MATCHES ({len(hits)}) ===")
        for it in hits:
            print(json.dumps({
                "key": it.get("_key"),
                "request_id": it.get("request_id"),
                "endpoint": it.get("endpoint_id"),
                "ended_at": it.get("ended_at"),
                "input": it.get("json_input"),
                "urls": extract_urls(it.get("json_output")),
            }, indent=2))
            print()
    else:
        print("No weapon keyword matches in full JSON.\n")

    # Dump all mesh outputs for manual inspection
    print("=== ALL 3D OUTPUT URLS ===")
    for it in sorted(all_items, key=lambda x: x.get("ended_at") or ""):
        urls = [u for u in extract_urls(it.get("json_output")) if any(x in u.lower() for x in (".glb", ".obj", ".fbx", ".zip", "model", "mesh"))]
        if not urls:
            urls = extract_urls(it.get("json_output"))
        if urls:
            print(f"\n[{it.get('_key')}] {it.get('endpoint_id')} @ {it.get('ended_at')}")
            print(f"  request_id: {it.get('request_id')}")
            inp = it.get("json_input")
            if inp:
                print(f"  input: {json.dumps(inp)[:200]}")
            for u in urls[:8]:
                print(f"  {u}")

    out = Path(r"D:\recaps\fal_3d_request_dump.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(all_items, indent=2), encoding="utf-8")
    print(f"\nFull dump: {out}")


if __name__ == "__main__":
    main()
