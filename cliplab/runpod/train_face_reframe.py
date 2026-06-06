#!/usr/bin/env python3
"""Train ClipLab face trajectory refiner on cliplab_reframe.jsonl."""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_RUNPOD = Path(__file__).resolve().parent
if str(_RUNPOD) not in sys.path:
    sys.path.insert(0, str(_RUNPOD))

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset

from models_torch import TrajectoryRefiner


class TrajectoryDS(Dataset):
    def __init__(self, rows: list[dict], seq_len: int = 32) -> None:
        self.samples: list[torch.Tensor] = []
        self.targets: list[torch.Tensor] = []
        for row in rows:
            frames = list(row.get("frames") or [])
            if len(frames) < 4:
                continue
            # Normalize time within clip
            t0 = float(frames[0].get("t") or 0)
            t_end = float(frames[-1].get("t") or t0 + 1)
            span = max(t_end - t0, 0.001)
            step = max(len(frames) // seq_len, 1)
            seq_in, seq_tgt = [], []
            for fr in frames[::step][:seq_len]:
                t_norm = (float(fr.get("t", 0)) - t0) / span
                cx = float(fr.get("cx", 960)) / 1920.0
                cy = float(fr.get("cy", 540)) / 1080.0
                fw = float(fr.get("face_w", 200)) / 1920.0
                fh = float(fr.get("face_h", 200)) / 1080.0
                conf = float(fr.get("confidence", 0.8))
                # Input: noisy version (bootstrap simulates detector jitter)
                jitter = 0.02
                seq_in.append([t_norm, cx + jitter, cy - jitter, fw, conf])
                seq_tgt.append([cx, cy, fw, fh])
            if len(seq_in) >= 4:
                self.samples.append(torch.tensor(seq_in, dtype=torch.float32))
                self.targets.append(torch.tensor(seq_tgt, dtype=torch.float32))
        if not self.samples:
            raise ValueError("no valid trajectory sequences")

    def __len__(self) -> int:
        return len(self.samples)

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor]:
        return self.samples[idx], self.targets[idx]


def collate(batch: list[tuple[torch.Tensor, torch.Tensor]]) -> tuple[torch.Tensor, torch.Tensor]:
    max_len = max(b[0].shape[0] for b in batch)
    xs, ys = [], []
    for x, y in batch:
        pad = max_len - x.shape[0]
        if pad > 0:
            x = torch.cat([x, x[-1:].repeat(pad, 1)], dim=0)
            y = torch.cat([y, y[-1:].repeat(pad, 1)], dim=0)
        xs.append(x)
        ys.append(y)
    return torch.stack(xs), torch.stack(ys)


def load_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default=os.getenv("CLIPLAB_REFRAME_DATASET", str(Path(os.getenv("STUDIO_APP_DATA_DIR", "/runpod-volume/studio")) / "cliplab/datasets/cliplab_reframe.jsonl")))
    ap.add_argument("--out", default=str(Path(os.getenv("STUDIO_APP_DATA_DIR", "/runpod-volume/studio")) / "cliplab/models/reframe/v1"))
    ap.add_argument("--epochs", type=int, default=10)
    ap.add_argument("--batch-size", type=int, default=8)
    ap.add_argument("--lr", type=float, default=1e-3)
    args = ap.parse_args()

    rows = load_rows(Path(args.dataset))
    if len(rows) < 2:
        raise SystemExit(f"Need >= 2 reframe rows, got {len(rows)}. Run bootstrap_opencv_reframe.py first.")

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    ds = TrajectoryDS(rows)
    dl = DataLoader(ds, batch_size=min(args.batch_size, len(ds)), shuffle=True, collate_fn=collate)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = TrajectoryRefiner().to(device)
    opt = torch.optim.AdamW(model.parameters(), lr=args.lr)
    loss_fn = nn.SmoothL1Loss()

    model.train()
    for epoch in range(args.epochs):
        total = 0.0
        for x, y in dl:
            x, y = x.to(device), y.to(device)
            pred = model(x)
            loss = loss_fn(pred, y)
            opt.zero_grad()
            loss.backward()
            opt.step()
            total += float(loss.item())
        print(f"epoch {epoch + 1}/{args.epochs} loss={total / max(len(dl), 1):.4f}")

    ckpt = {"state_dict": model.state_dict(), "model_type": "face_reframe_tracker_v1"}
    torch.save(ckpt, out_dir / "tracker.pt")

    config = {
        "model_type": "face_reframe_tracker_v1",
        "dataset_rows": len(rows),
        "sequence_samples": len(ds),
        "epochs": args.epochs,
        "output_aspect": "9:16",
    }
    (out_dir / "config.json").write_text(json.dumps(config, indent=2), encoding="utf-8")
    print(json.dumps({"status": "trained", "out": str(out_dir), "rows": len(rows)}))


if __name__ == "__main__":
    main()
