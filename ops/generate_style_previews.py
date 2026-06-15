"""Pre-generate Studio Agent art-style preview assets.

By default this warms the 24 Seedream 4.5 still previews. Pass --videos to
also generate one separate i2v motion clip per style.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from studio_agent.render_styles import (  # noqa: E402
    get_style_preview_path,
    get_style_preview_video_path,
    list_render_styles,
)


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate Studio Agent style preview stills/videos")
    ap.add_argument("--videos", action="store_true", help="Also generate cached i2v preview clips")
    ap.add_argument("--only", default="", help="Comma-separated style keys to generate")
    args = ap.parse_args()

    wanted = {x.strip() for x in args.only.split(",") if x.strip()}
    styles = [s for s in list_render_styles() if not wanted or s["key"] in wanted]
    if not styles:
        print("No matching styles.")
        return 1

    print(f"Generating {len(styles)} style preview still(s). videos={bool(args.videos)}")
    for idx, style in enumerate(styles, start=1):
        key = str(style["key"])
        label = str(style["label"])
        print(f"[{idx}/{len(styles)}] {label} ({key}) still...")
        still = get_style_preview_path(key)
        print(f"    {still}")
        if args.videos:
            print(f"[{idx}/{len(styles)}] {label} ({key}) video...")
            video = get_style_preview_video_path(key)
            print(f"    {video}")

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
