#!/usr/bin/env python3
"""Mass-generate cliplab_reframe.jsonl with realistic synthetic face trajectories."""
from __future__ import annotations

import argparse
import json
import math
import os
import random
from pathlib import Path


def _trajectory_talking_head(rng: random.Random, *, frames: int, fps: float = 30.0) -> list[dict]:
    """Speaker drifts slightly while talking — common podcast setup."""
    cx0, cy0 = 960 + rng.uniform(-80, 80), 520 + rng.uniform(-40, 40)
    amp_x, amp_y = rng.uniform(20, 90), rng.uniform(10, 50)
    freq = rng.uniform(0.3, 1.2)
    face_w = rng.uniform(180, 280)
    out: list[dict] = []
    for i in range(frames):
        t = i / fps
        cx = cx0 + amp_x * math.sin(t * freq * 2 * math.pi)
        cy = cy0 + amp_y * math.cos(t * freq * 1.3 * 2 * math.pi)
        jitter = rng.uniform(-8, 8)
        out.append(
            {
                "t": round(t, 3),
                "cx": cx + jitter,
                "cy": cy + jitter * 0.5,
                "face_w": face_w + rng.uniform(-15, 15),
                "face_h": face_w + rng.uniform(-10, 10),
                "confidence": round(rng.uniform(0.78, 0.97), 3),
            }
        )
    return out


def _trajectory_pan(rng: random.Random, *, frames: int, fps: float = 30.0) -> list[dict]:
    """Camera pan — face moves across frame."""
    x_start = rng.uniform(400, 700)
    x_end = rng.uniform(1100, 1500)
    cy = rng.uniform(450, 600)
    face_w = rng.uniform(150, 240)
    out: list[dict] = []
    for i in range(frames):
        t = i / fps
        alpha = i / max(frames - 1, 1)
        cx = x_start + (x_end - x_start) * alpha
        out.append(
            {
                "t": round(t, 3),
                "cx": cx,
                "cy": cy + rng.uniform(-12, 12),
                "face_w": face_w,
                "face_h": face_w * rng.uniform(0.95, 1.05),
                "confidence": round(rng.uniform(0.72, 0.94), 3),
            }
        )
    return out


def _trajectory_zoom(rng: random.Random, *, frames: int, fps: float = 30.0) -> list[dict]:
    """Slow zoom in on face."""
    cx, cy = 960 + rng.uniform(-60, 60), 540 + rng.uniform(-30, 30)
    w_start = rng.uniform(120, 180)
    w_end = rng.uniform(220, 320)
    out: list[dict] = []
    for i in range(frames):
        t = i / fps
        alpha = i / max(frames - 1, 1)
        fw = w_start + (w_end - w_start) * alpha
        out.append(
            {
                "t": round(t, 3),
                "cx": cx + rng.uniform(-5, 5),
                "cy": cy + rng.uniform(-5, 5),
                "face_w": fw,
                "face_h": fw,
                "confidence": round(rng.uniform(0.8, 0.96), 3),
            }
        )
    return out


def _trajectory_multi_switch(rng: random.Random, *, frames: int, fps: float = 30.0) -> list[dict]:
    """Two speakers — jump cut between face positions."""
    a = (rng.uniform(600, 800), rng.uniform(480, 560))
    b = (rng.uniform(1050, 1250), rng.uniform(480, 560))
    face_w = rng.uniform(160, 230)
    switch_every = max(frames // rng.randint(3, 6), 8)
    out: list[dict] = []
    for i in range(frames):
        t = i / fps
        cx, cy = a if (i // switch_every) % 2 == 0 else b
        out.append(
            {
                "t": round(t, 3),
                "cx": cx + rng.uniform(-6, 6),
                "cy": cy + rng.uniform(-6, 6),
                "face_w": face_w,
                "face_h": face_w,
                "confidence": round(rng.uniform(0.75, 0.93), 3),
            }
        )
    return out


GENERATORS = [_trajectory_talking_head, _trajectory_pan, _trajectory_zoom, _trajectory_multi_switch]


def generate(count: int, seed: int, *, frames: int, duration_sec: float) -> list[dict]:
    rng = random.Random(seed)
    fps = frames / duration_sec
    rows: list[dict] = []
    for i in range(count):
        gen = GENERATORS[i % len(GENERATORS)]
        traj = gen(rng, frames=frames, fps=fps)
        rows.append(
            {
                "video_path": f"/workspace/studio/cliplab/exports/synthetic_mass_{i:05d}.mp4",
                "frames": traj,
                "crop_mode": "9:16",
                "source": "synthetic_mass_bootstrap",
            }
        )
    return rows


def main() -> None:
    root = Path(os.getenv("STUDIO_APP_DATA_DIR", "/runpod-volume/studio"))
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(root / "cliplab/datasets/cliplab_reframe.jsonl"))
    ap.add_argument("--count", type=int, default=1200)
    ap.add_argument("--frames", type=int, default=48)
    ap.add_argument("--duration-sec", type=float, default=45.0)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--merge", action="store_true")
    args = ap.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows = generate(args.count, args.seed, frames=args.frames, duration_sec=args.duration_sec)
    if args.merge and out_path.exists():
        existing = [json.loads(l) for l in out_path.read_text(encoding="utf-8").splitlines() if l.strip()]
        rows = existing + rows

    with out_path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")

    print(json.dumps({"status": "ok", "rows": len(rows), "frames_per_clip": args.frames, "out": str(out_path)}))


if __name__ == "__main__":
    main()
