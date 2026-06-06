"""ClipLab PyTorch models — virality reranker + face trajectory refiner."""
from __future__ import annotations

import math
from typing import Any

import torch
import torch.nn as nn


class ViralityReranker(nn.Module):
    """MLP on [text_embedding | numeric_features] -> virality score 0-100."""

    def __init__(self, embed_dim: int = 384, numeric_dim: int = 8, hidden: int = 256) -> None:
        super().__init__()
        self.embed_dim = embed_dim
        self.numeric_dim = numeric_dim
        inp = embed_dim + numeric_dim
        self.mlp = nn.Sequential(
            nn.Linear(inp, hidden),
            nn.ReLU(),
            nn.Dropout(0.15),
            nn.Linear(hidden, hidden // 2),
            nn.ReLU(),
            nn.Linear(hidden // 2, 1),
        )

    def forward(self, text_emb: torch.Tensor, numeric: torch.Tensor) -> torch.Tensor:
        x = torch.cat([text_emb, numeric], dim=-1)
        return self.mlp(x).squeeze(-1)  # logits; use sigmoid * 100 at inference


class TrajectoryRefiner(nn.Module):
    """GRU refines OpenCV face detections -> smooth crop centers."""

    def __init__(self, input_dim: int = 5, hidden: int = 64, layers: int = 2) -> None:
        super().__init__()
        self.gru = nn.GRU(input_dim, hidden, num_layers=layers, batch_first=True)
        self.head = nn.Linear(hidden, 4)  # cx, cy, face_w, face_h

    def forward(self, seq: torch.Tensor) -> torch.Tensor:
        out, _ = self.gru(seq)
        return self.head(out)


def numeric_features(
    *,
    duration: float,
    snippet: str,
    hook_text: str,
    llm_score: float,
    kept: float,
    published: float,
) -> list[float]:
    words = len((snippet or "").split())
    hook_len = len((hook_text or "").split())
    return [
        min(duration / 60.0, 1.0),
        min(words / 120.0, 1.0),
        min(hook_len / 12.0, 1.0),
        llm_score / 100.0,
        kept,
        published,
        1.0 if "?" in (snippet or "") else 0.0,
        1.0 if "!" in (hook_text or snippet or "") else 0.0,
    ]


def label_from_row(row: dict[str, Any]) -> float:
    if row.get("published"):
        return 95.0
    if row.get("kept") is False:
        return 15.0
    if row.get("kept"):
        return 82.0
    return float(row.get("virality_score") or 50.0)


class TextEmbedder:
    """Lazy sentence-transformers embedder with hash fallback."""

    def __init__(self, dim: int = 384) -> None:
        self.dim = dim
        self._model = None

    def _load(self) -> None:
        if self._model is not None:
            return
        try:
            from sentence_transformers import SentenceTransformer  # noqa: PLC0415

            self._model = SentenceTransformer("all-MiniLM-L6-v2")
            self.dim = 384
        except Exception:
            self._model = None

    def encode(self, texts: list[str]) -> torch.Tensor:
        self._load()
        if self._model is not None:
            import numpy as np  # noqa: PLC0415

            arr = self._model.encode(texts, normalize_embeddings=True, show_progress_bar=False)
            return torch.tensor(np.asarray(arr, dtype="float32"))
        # Hash fallback
        out = []
        for t in texts:
            vec = [0.0] * self.dim
            for i, ch in enumerate((t or "")[:512]):
                vec[i % self.dim] += (ord(ch) % 31) / 31.0
            norm = math.sqrt(sum(v * v for v in vec)) or 1.0
            out.append([v / norm for v in vec])
        return torch.tensor(out, dtype=torch.float32)
