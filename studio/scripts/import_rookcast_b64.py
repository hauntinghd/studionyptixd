"""Import a Rookcast skill from browser CDP base64 extract.

Usage (after Edit opens textarea in Rookcast Skills UI):
  1. CDP: (function(){ return btoa(unescape(encodeURIComponent(document.querySelector('textarea').value))); })()
  2. python studio/scripts/import_rookcast_b64.py <cdp-json> studio/skills/<name>/SKILL.md
"""
from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    if len(sys.argv) < 3:
        print("usage: import_rookcast_b64.py <cdp-json-path> <out-rel-path>")
        raise SystemExit(1)
    cdp_path = Path(sys.argv[1])
    out = ROOT / sys.argv[2]
    data = json.loads(cdp_path.read_text(encoding="utf-8"))
    b64 = data["result"]["value"]
    text = base64.b64decode(b64).decode("utf-8")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")
    print(f"Wrote {out} ({len(text)} chars)")


if __name__ == "__main__":
    main()
