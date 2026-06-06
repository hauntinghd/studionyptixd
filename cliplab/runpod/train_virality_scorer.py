#!/usr/bin/env python3
"""Train ClipLab virality reranker on cliplab_feedback.jsonl."""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_RUNPOD = Path(__file__).resolve().parent
if str(_RUNPOD) not in sys.path:
    sys.path.insert(0, str(_RUNPOD))

from models_torch import TextEmbedder, ViralityReranker, label_from_row, numeric_features

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset


class FeedbackDS(Dataset):
    def __init__(self, rows: list[dict], embedder: TextEmbedder) -> None:
        self.rows = rows
        self.embedder = embedder
        texts = [
            f"{r.get('prompt', '')} [SEP] {r.get('transcript_snippet', '')}" for r in rows
        ]
        self.embs = embedder.encode(texts)
        self.numerics = torch.tensor(
            [
                numeric_features(
                    duration=max(0.0, float(r.get("segment_end", 0)) - float(r.get("segment_start", 0))),
                    snippet=str(r.get("transcript_snippet") or ""),
                    hook_text=str(r.get("edited_hook") or r.get("hook_text") or ""),
                    llm_score=float(r.get("virality_score") or 50),
                    kept=1.0 if r.get("kept") else 0.0,
                    published=1.0 if r.get("published") else 0.0,
                )
                for r in rows
            ],
            dtype=torch.float32,
        )
        self.labels = torch.tensor([label_from_row(r) / 100.0 for r in rows], dtype=torch.float32)

    def __len__(self) -> int:
        return len(self.rows)

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        return self.embs[idx], self.numerics[idx], self.labels[idx]


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
    ap.add_argument("--dataset", default=os.getenv("CLIPLAB_FEEDBACK_DATASET", str(Path(os.getenv("STUDIO_APP_DATA_DIR", "/runpod-volume/studio")) / "cliplab/datasets/cliplab_feedback.jsonl")))
    ap.add_argument("--out", default=str(Path(os.getenv("STUDIO_APP_DATA_DIR", "/runpod-volume/studio")) / "cliplab/models/virality/v1"))
    ap.add_argument("--epochs", type=int, default=5)
    ap.add_argument("--batch-size", type=int, default=32)
    ap.add_argument("--lr", type=float, default=1e-3)
    args = ap.parse_args()

    rows = load_rows(Path(args.dataset))
    if len(rows) < 8:
        raise SystemExit(f"Need >= 8 feedback rows, got {len(rows)}. Bootstrap or collect Studio feedback first.")

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    hidden = 512 if len(rows) >= 1000 else 256
    embedder = TextEmbedder()
    # Hold out 10% for validation when dataset is large
    rng = __import__("random").Random(42)
    shuffled = list(rows)
    rng.shuffle(shuffled)
    val_n = max(1, len(shuffled) // 10) if len(shuffled) >= 100 else 0
    val_rows = shuffled[:val_n] if val_n else []
    train_rows = shuffled[val_n:] if val_n else shuffled

    ds = FeedbackDS(train_rows, embedder)
    val_ds = FeedbackDS(val_rows, embedder) if val_rows else None
    dl = DataLoader(ds, batch_size=min(args.batch_size, len(ds)), shuffle=True)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = ViralityReranker(embed_dim=embedder.dim, hidden=hidden).to(device)
    opt = torch.optim.AdamW(model.parameters(), lr=args.lr)
    loss_fn = nn.MSELoss()

    val_dl = None
    if val_ds is not None:
        val_dl = DataLoader(val_ds, batch_size=min(args.batch_size, len(val_ds)), shuffle=False)

    best_val = float("inf")
    best_state = None

    model.train()
    for epoch in range(args.epochs):
        total = 0.0
        for emb, num, lab in dl:
            emb, num, lab = emb.to(device), num.to(device), lab.to(device)
            pred = torch.sigmoid(model(emb, num))
            loss = loss_fn(pred, lab)
            opt.zero_grad()
            loss.backward()
            opt.step()
            total += float(loss.item())
        train_loss = total / max(len(dl), 1)

        val_loss = None
        if val_dl is not None:
            model.eval()
            vtotal = 0.0
            with torch.no_grad():
                for emb, num, lab in val_dl:
                    emb, num, lab = emb.to(device), num.to(device), lab.to(device)
                    pred = torch.sigmoid(model(emb, num))
                    vtotal += float(loss_fn(pred, lab).item())
            val_loss = vtotal / max(len(val_dl), 1)
            if val_loss < best_val:
                best_val = val_loss
                best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
            model.train()

        msg = f"epoch {epoch + 1}/{args.epochs} train_loss={train_loss:.4f}"
        if val_loss is not None:
            msg += f" val_loss={val_loss:.4f}"
        print(msg)

    if best_state is not None:
        model.load_state_dict(best_state)

    ckpt = {
        "state_dict": model.state_dict(),
        "embed_dim": embedder.dim,
        "numeric_dim": 8,
        "model_type": "virality_reranker_v1",
    }
    torch.save(ckpt, out_dir / "model.pt")

    config = {
        "model_type": "virality_reranker_v1",
        "dataset_rows": len(rows),
        "train_rows": len(train_rows),
        "val_rows": len(val_rows),
        "epochs": args.epochs,
        "embed_dim": embedder.dim,
        "hidden": hidden,
        "best_val_loss": best_val if best_val < float("inf") else None,
        "label": "published>kept>virality_score",
    }
    (out_dir / "config.json").write_text(json.dumps(config, indent=2), encoding="utf-8")
    print(json.dumps({"status": "trained", "out": str(out_dir), "rows": len(rows)}))


if __name__ == "__main__":
    main()
