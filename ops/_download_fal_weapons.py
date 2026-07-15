"""Download weapon GLBs from fal Rodin history."""
from __future__ import annotations

import json
import urllib.parse
import urllib.request
from pathlib import Path

DUMP = Path(r"D:\recaps\fal_3d_request_dump.json")
OUT = Path(r"D:\recaps\fal_weapon_models")

WEAPON_SRC = {
    "src_07_pistol_v1.png": "https://v3b.fal.media/files/b/0a972758/2z2VMlP_xq1lXw8VrzAXN_70677de4ae68440da65ff5cfb3b1247b.png",
    "src_08_pistol_v2.png": "https://v3b.fal.media/files/b/0a97275e/BG3D7AgW9-mwmlFgIvHOC_dc835f2525ea4f22b653d063d32a577a.png",
    "src_09_shotgun.png": "https://v3b.fal.media/files/b/0a97276b/6Swyqy3s4UCvFl25ZNRY4_dec220508c1d4e96aa426671ec9bbe68.png",
    "src_10_rifle.png": "https://v3b.fal.media/files/b/0a97277a/i_Rj2IoDfohtBUTml14AO_be3d2f92f5854dd7908307dd3abe8fc6.png",
    "src_40_pdw.png": "https://v3b.fal.media/files/b/0a972b5a/nmq1cZQ7I1Uo5ziCMbAHH_dd978db4d5664dffba3f3ef67b70f5cc.png",
}


def require_https_url(url: str) -> str:
    value = str(url or "").strip()
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme.lower() != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("refusing non-HTTPS or malformed download URL")
    return value


def urls(obj) -> list[str]:
    out: list[str] = []
    if isinstance(obj, str) and obj.startswith("http"):
        out.append(obj)
    elif isinstance(obj, dict):
        for v in obj.values():
            out.extend(urls(v))
    elif isinstance(obj, list):
        for v in obj:
            out.extend(urls(v))
    return out


def main() -> None:
    items = json.loads(DUMP.read_text(encoding="utf-8"))
    seen: set[str] = set()
    uniq = []
    for it in items:
        rid = it.get("request_id")
        if rid in seen:
            continue
        seen.add(rid)
        uniq.append(it)

    rev = {v: k for k, v in WEAPON_SRC.items()}
    OUT.mkdir(parents=True, exist_ok=True)
    manifest: list[dict] = []

    for it in uniq:
        inp = it.get("json_input") or {}
        for src in inp.get("input_image_urls") or []:
            if src not in rev:
                continue
            label = rev[src]
            glbs = [u for u in urls(it.get("json_output")) if ".glb" in u.lower()]
            entry = {
                "label": label,
                "request_id": it.get("request_id"),
                "ended_at": it.get("ended_at"),
                "endpoint": it.get("endpoint_id"),
                "source_image": src,
                "glb_urls": glbs,
            }
            manifest.append(entry)
            print(json.dumps(entry, indent=2))
            for i, u in enumerate(glbs):
                stem = label.replace(".png", "")
                dest = OUT / f"{stem}_{i}.glb"
                safe_url = require_https_url(u)
                # The download URL is validated as HTTPS above.
                urllib.request.urlretrieve(safe_url, dest)  # nosec B310
                print(f"saved {dest} ({dest.stat().st_size // 1024} KB)")

    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"\nDone — {len(manifest)} weapon jobs -> {OUT}")


if __name__ == "__main__":
    main()
