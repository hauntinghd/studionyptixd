#!/usr/bin/env python3
"""Bootstrap cliplab_reframe.jsonl from OpenCV face tracking on export videos."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import cv2


def _studio_root() -> Path:
    return Path(os.getenv("STUDIO_APP_DATA_DIR", "/runpod-volume/studio"))


def _track_video(video_path: str, *, duration_sec: float = 45.0) -> list[dict]:
    cascade = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return []
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    max_frames = int(duration_sec * fps)
    frames: list[dict] = []
    i = 0
    while i < max_frames:
        ok, frame = cap.read()
        if not ok:
            break
        if i % 3 != 0:
            i += 1
            continue
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        faces = cascade.detectMultiScale(gray, 1.08, 4, minSize=(48, 48))
        if len(faces):
            x, y, fw, fh = max(faces, key=lambda f: f[2] * f[3])
            frames.append(
                {
                    "t": round(i / fps, 3),
                    "cx": float(x + fw / 2),
                    "cy": float(y + fh / 2),
                    "face_w": float(fw),
                    "face_h": float(fh),
                    "confidence": 0.85,
                }
            )
        i += 1
    cap.release()
    return frames


def main() -> None:
    root = _studio_root()
    ap = argparse.ArgumentParser()
    ap.add_argument("--exports-dir", default=str(root / "cliplab/exports"))
    ap.add_argument("--out", default=str(root / "cliplab/datasets/cliplab_reframe.jsonl"))
    ap.add_argument("--max-videos", type=int, default=50)
    ap.add_argument("--duration-sec", type=float, default=45.0)
    args = ap.parse_args()

    exports = Path(args.exports_dir)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    videos = sorted(exports.glob("**/*.mp4"))[: args.max_videos]
    if not videos:
        rows = []
        for i in range(4):
            frames = []
            for j in range(24):
                t = j * 0.5
                cx = 960 + 40 * ((j % 5) - 2)
                cy = 540 + 20 * ((j % 3) - 1)
                frames.append({"t": t, "cx": cx, "cy": cy, "face_w": 210, "face_h": 210, "confidence": 0.88})
            rows.append(
                {
                    "video_path": str(root / f"cliplab/exports/synthetic_{i:03d}.mp4"),
                    "frames": frames,
                    "crop_mode": "9:16",
                    "source": "synthetic_bootstrap",
                }
            )
        with out_path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")
        print(json.dumps({"status": "synthetic", "rows": len(rows), "out": str(out_path)}))
        return

    rows = []
    for vp in videos:
        frames = _track_video(str(vp), duration_sec=args.duration_sec)
        if len(frames) < 4:
            continue
        rows.append({"video_path": str(vp), "frames": frames, "crop_mode": "9:16", "source": "opencv_bootstrap"})

    with out_path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")

    print(json.dumps({"status": "ok", "rows": len(rows), "out": str(out_path)}))


if __name__ == "__main__":
    main()
