#!/usr/bin/env python3
"""Seed synthetic cliplab_feedback.jsonl when Studio feedback is sparse."""
from __future__ import annotations

import argparse
import json
import os
import random
from pathlib import Path

SAMPLES = [
    ("find pricing objections", "the pricing model is the biggest blocker for teams our size", True, True, 88),
    ("find pricing objections", "honestly we switched because the old plan was too expensive", True, False, 79),
    ("find pricing objections", "weather was nice that week", False, False, 22),
    ("best hot takes", "this industry is lying to you about growth hacks", True, True, 92),
    ("best hot takes", "nobody talks about the churn problem", True, False, 84),
    ("best hot takes", "let me read the sponsor copy", False, False, 18),
    ("emotional moments", "I almost quit twice before we hit product market fit", True, True, 90),
    ("emotional moments", "my co-founder and I had a fight on launch day", True, False, 81),
    ("emotional moments", "here is the agenda for today", False, False, 15),
    ("contrarian clips", "everyone says post daily but that is wrong for B2B", True, True, 86),
    ("contrarian clips", "SEO is not dead it just changed shape", True, False, 77),
    ("contrarian clips", "thanks for joining the webinar", False, False, 20),
]


def main() -> None:
    root = Path(os.getenv("STUDIO_APP_DATA_DIR", "/runpod-volume/studio"))
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(root / "cliplab/datasets/cliplab_feedback.jsonl"))
    ap.add_argument("--min-rows", type=int, default=12)
    args = ap.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    existing = []
    if out_path.exists():
        for line in out_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                existing.append(json.loads(line))

    rows = list(existing)
    rng = random.Random(42)
    i = 0
    while len(rows) < args.min_rows:
        prompt, snippet, kept, published, score = SAMPLES[i % len(SAMPLES)]
        start = float(rng.randint(30, 900))
        end = start + float(rng.randint(12, 45))
        rows.append(
            {
                "prompt": prompt,
                "transcript_snippet": snippet,
                "segment_start": start,
                "segment_end": end,
                "virality_score": score,
                "kept": kept,
                "published": published,
                "edited_hook": "",
                "channel_id": "bootstrap",
                "source_video_id": f"bootstrap_{len(rows):03d}",
            }
        )
        i += 1

    with out_path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")

    print(json.dumps({"status": "ok", "rows": len(rows), "out": str(out_path)}))


if __name__ == "__main__":
    main()
