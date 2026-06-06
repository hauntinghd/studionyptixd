#!/usr/bin/env python3
"""Mass-generate cliplab_feedback.jsonl for production virality reranker training."""
from __future__ import annotations

import argparse
import json
import os
import random
from pathlib import Path

PROMPTS = [
    "find pricing objections",
    "find every hot take",
    "emotional story peaks",
    "contrarian clips",
    "funny moments",
    "product reveal reactions",
    "debate / argument segments",
    "actionable tips under 60s",
    "controversy or spicy takes",
    "before/after transformation",
    "founder struggle stories",
    "customer success quotes",
    "technical explainers that hook",
    "roasts and callouts",
    "surprise plot twists",
    "motivational peaks",
    "skepticism → belief arc",
    "numbers / stats that shock",
    "relatable pain points",
    "call-to-action moments",
]

POSITIVE_SNIPPETS = [
    "the pricing model is the biggest blocker for teams our size — we almost churned twice",
    "this industry is lying to you about growth hacks and nobody wants to say it out loud",
    "I almost quit twice before we hit product market fit — here's what changed",
    "everyone says post daily but that is completely wrong for B2B SaaS",
    "wait — he just said revenue doubled in 90 days with zero ad spend",
    "that's the moment I realized the entire strategy was backwards",
    "honestly this is the part investors never ask about but it matters more than TAM",
    "she called me out on stage and I had no comeback — brutal but fair",
    "the demo broke live and we turned it into our best sales week ever",
    "stop optimizing vanity metrics — retention is the only scoreboard",
    "I tested this for 30 days and the results were not what I expected",
    "this one habit added six figures without hiring anyone new",
    "the contrarian take: your funnel is fine, your offer is the problem",
    "when he said 'we fired half the team' the room went silent",
    "here's the clip where he admits the product almost died in beta",
    "nobody talks about the churn problem until it's too late",
    "this is why your thumbnails get clicks but your watch time dies",
    "the hook is simple: we were wrong for eighteen months straight",
    "if you only watch one minute, make it this — pure gold",
    "the stat at the end rewired how I think about compounding",
]

NEGATIVE_SNIPPETS = [
    "thanks for joining the webinar today we'll get started in a moment",
    "let me read the sponsor copy real quick before we continue",
    "here is the agenda for today's episode nothing spicy yet",
    "weather was nice that week so we moved the meeting outdoors",
    "please like and subscribe before we jump into housekeeping",
    "technical difficulties — we'll be right back after this break",
    "as I mentioned in the last episode same intro again",
    "buffering on my end can everyone hear me okay",
    "let me pull up slide seventeen of forty-two",
    "small talk about coffee while we wait for guests",
]

NEUTRAL_SNIPPETS = [
    "we'll cover three topics today starting with market overview",
    "the platform update ships next quarter with minor UI tweaks",
    "our guest has a background in enterprise sales",
    "let me give context before the main segment",
    "this section is background for newcomers",
]


def _row(
    rng: random.Random,
    prompt: str,
    snippet: str,
    *,
    kept: bool,
    published: bool,
    score: int,
    vid: str,
) -> dict:
    start = float(rng.randint(15, 3600))
    dur = float(rng.randint(8, 55))
    hook = ""
    if kept and rng.random() > 0.6:
        words = snippet.split()[:6]
        hook = " ".join(words).upper() if words else ""
    return {
        "prompt": prompt,
        "transcript_snippet": snippet,
        "segment_start": start,
        "segment_end": start + dur,
        "virality_score": score,
        "kept": kept,
        "published": published,
        "edited_hook": hook,
        "hook_text": hook,
        "channel_id": f"bootstrap_{rng.randint(1, 200):03d}",
        "source_video_id": vid,
    }


def generate(count: int, seed: int) -> list[dict]:
    rng = random.Random(seed)
    rows: list[dict] = []
    for i in range(count):
        prompt = rng.choice(PROMPTS)
        roll = rng.random()
        if roll < 0.32:
            snippet = rng.choice(NEGATIVE_SNIPPETS)
            kept, published, score = False, False, rng.randint(8, 28)
        elif roll < 0.72:
            snippet = rng.choice(POSITIVE_SNIPPETS)
            kept, published, score = True, False, rng.randint(68, 88)
        else:
            snippet = rng.choice(POSITIVE_SNIPPETS)
            kept, published, score = True, True, rng.randint(85, 98)
        # Inject variation
        if rng.random() > 0.5:
            snippet = snippet + " " + rng.choice(["", " — watch this.", " (clip this).", ""])
        rows.append(
            _row(
                rng,
                prompt,
                snippet,
                kept=kept,
                published=published,
                score=score,
                vid=f"mass_{i:06d}",
            )
        )
    return rows


def merge_existing(path: Path, rows: list[dict]) -> list[dict]:
    if not path.exists():
        return rows
    seen = set()
    merged: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        key = (row.get("source_video_id"), row.get("segment_start"))
        if key not in seen:
            seen.add(key)
            merged.append(row)
    for row in rows:
        key = (row.get("source_video_id"), row.get("segment_start"))
        if key not in seen:
            seen.add(key)
            merged.append(row)
    return merged


def main() -> None:
    root = Path(os.getenv("STUDIO_APP_DATA_DIR", "/runpod-volume/studio"))
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(root / "cliplab/datasets/cliplab_feedback.jsonl"))
    ap.add_argument("--count", type=int, default=8000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--merge", action="store_true", help="Keep existing real feedback rows")
    args = ap.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows = generate(args.count, args.seed)
    if args.merge:
        rows = merge_existing(out_path, rows)

    with out_path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    pos = sum(1 for r in rows if r.get("published"))
    neg = sum(1 for r in rows if r.get("kept") is False)
    print(json.dumps({"status": "ok", "rows": len(rows), "published": pos, "negative": neg, "out": str(out_path)}))


if __name__ == "__main__":
    main()
