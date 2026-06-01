"""Save a CDP Runtime.evaluate textarea dump to studio/skills/."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    if len(sys.argv) < 3:
        print("usage: save_cdp_skill.py <cdp-json-path> <out-relative-path>")
        raise SystemExit(1)
    cdp_path = Path(sys.argv[1])
    out_rel = sys.argv[2]
    data = json.loads(cdp_path.read_text(encoding="utf-8"))
    text = data["result"]["value"]
    if isinstance(text, str) and text.startswith('"') and text.endswith('"'):
        text = json.loads(text)
    out = ROOT / out_rel
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")
    print(f"Wrote {out} ({len(text)} chars)")


if __name__ == "__main__":
    main()
