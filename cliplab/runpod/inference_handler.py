#!/usr/bin/env python3
"""Archived ClipLab model inference source.

The exported handler permanently rejects serverless execution. Model-loading
and inference helpers remain available as offline research source.
"""
from __future__ import annotations

import base64
import os
import sys
from pathlib import Path
from typing import Any

_RUNPOD = Path(__file__).resolve().parent
if str(_RUNPOD) not in sys.path:
    sys.path.insert(0, str(_RUNPOD))

import cv2
import numpy as np
import torch

from models_torch import (
    TextEmbedder,
    TrajectoryRefiner,
    ViralityReranker,
    numeric_features,
)

VOLUME = Path(os.getenv("STUDIO_APP_DATA_DIR", "/runpod-volume/studio"))
if (VOLUME / "models").is_dir():
    MODELS = VOLUME / "models"
else:
    MODELS = VOLUME / "cliplab" / "models"

_VIRALITY: ViralityReranker | None = None
_VIRALITY_EMB: TextEmbedder | None = None
_VIRALITY_PATH: str | None = None
_REFINER: TrajectoryRefiner | None = None
_REFINER_PATH: str | None = None
_DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")


def _torch_load(path: str) -> dict:
    try:
        checkpoint = torch.load(path, map_location=_DEVICE, weights_only=True)
    except TypeError as exc:
        raise RuntimeError(
            "safe checkpoint loading requires a PyTorch version with weights_only support"
        ) from exc
    if not isinstance(checkpoint, dict):
        raise ValueError("checkpoint must contain a weights-only state dictionary")
    return checkpoint


def _load_virality(weights_path: str) -> tuple[ViralityReranker, TextEmbedder]:
    global _VIRALITY, _VIRALITY_EMB, _VIRALITY_PATH
    if _VIRALITY is not None and _VIRALITY_PATH == weights_path:
        return _VIRALITY, _VIRALITY_EMB  # type: ignore[return-value]
    ckpt = _torch_load(weights_path)
    embed_dim = int(ckpt.get("embed_dim") or 384)
    numeric_dim = int(ckpt.get("numeric_dim") or 8)
    model = ViralityReranker(embed_dim=embed_dim, numeric_dim=numeric_dim).to(_DEVICE)
    model.load_state_dict(ckpt["state_dict"])
    model.eval()
    embedder = TextEmbedder(dim=embed_dim)
    _VIRALITY, _VIRALITY_EMB, _VIRALITY_PATH = model, embedder, weights_path
    return model, embedder


def _load_refiner(weights_path: str) -> TrajectoryRefiner:
    global _REFINER, _REFINER_PATH
    if _REFINER is not None and _REFINER_PATH == weights_path:
        return _REFINER
    ckpt = _torch_load(weights_path)
    model = TrajectoryRefiner().to(_DEVICE)
    model.load_state_dict(ckpt["state_dict"])
    model.eval()
    _REFINER, _REFINER_PATH = model, weights_path
    return model


def _score_segments(inp: dict[str, Any]) -> dict[str, Any]:
    weights = str(inp.get("weights_path") or MODELS / "virality/v1/model.pt")
    if not Path(weights).exists():
        return {"error": f"weights missing: {weights}"}

    model, embedder = _load_virality(weights)
    prompt = str(inp.get("prompt") or "")
    segments = list(inp.get("segments") or [])
    if not segments:
        return {"segments": []}

    texts, numerics, metas = [], [], []
    for row in segments:
        if not isinstance(row, dict):
            continue
        snippet = str(row.get("transcript_snippet") or row.get("hook_text") or "")
        hook = str(row.get("hook_text") or "")
        dur = max(0.0, float(row.get("end", 0)) - float(row.get("start", 0)))
        texts.append(f"{prompt} [SEP] {snippet}")
        numerics.append(
            numeric_features(
                duration=dur,
                snippet=snippet,
                hook_text=hook,
                llm_score=float(row.get("virality_score") or 50),
                kept=0.0,
                published=0.0,
            )
        )
        metas.append(row)

    if not texts:
        return {"segments": segments}

    with torch.no_grad():
        emb = embedder.encode(texts).to(_DEVICE)
        num = torch.tensor(numerics, dtype=torch.float32, device=_DEVICE)
        scores = (torch.sigmoid(model(emb, num)) * 100.0).cpu().tolist()

    out = []
    for row, score in zip(metas, scores):
        merged = dict(row)
        merged["virality_score"] = round(float(score), 1)
        merged["model_source"] = "runpod_custom_v1"
        out.append(merged)
    out.sort(key=lambda r: float(r.get("virality_score") or 0), reverse=True)
    return {"segments": out}


def _detect_largest_face(frame: np.ndarray, cascade: cv2.CascadeClassifier) -> tuple[float, float, float, float, float] | None:
    h, w = frame.shape[:2]
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    faces = cascade.detectMultiScale(gray, scaleFactor=1.08, minNeighbors=4, minSize=(48, 48))
    if len(faces) == 0:
        return None
    x, y, fw, fh = max(faces, key=lambda f: f[2] * f[3])
    return (x + fw / 2, y + fh / 2, float(fw), float(fh), 0.85)


def _reframe_trajectory(inp: dict[str, Any]) -> dict[str, Any]:
    video_path = str(inp.get("video_path") or "")
    if not video_path or not Path(video_path).exists():
        return {"error": f"video not found: {video_path}"}

    weights = str(inp.get("weights_path") or MODELS / "reframe/v1/tracker.pt")
    if not Path(weights).exists():
        return {"error": f"weights missing: {weights}"}

    start_sec = float(inp.get("start_sec") or 0)
    duration_sec = float(inp.get("duration_sec") or 0)
    refiner = _load_refiner(weights)
    cascade = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return {"error": "could not open video"}
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    frame_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 1920)
    frame_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 1080)
    if start_sec > 0:
        cap.set(cv2.CAP_PROP_POS_MSEC, start_sec * 1000)
    max_frames = int((duration_sec or 3600) * fps) if duration_sec else int(3600 * fps)

    raw_seq: list[list[float]] = []
    times: list[float] = []
    frame_i = read = 0
    sample_every = 3

    while read < max_frames:
        ok, frame = cap.read()
        if not ok:
            break
        if frame_i % sample_every != 0:
            frame_i += 1
            continue
        t = start_sec + frame_i / fps
        det = _detect_largest_face(frame, cascade)
        if det:
            cx, cy, fw, fh, conf = det
            t_norm = (t - start_sec) / max(duration_sec or (max_frames / fps), 0.001)
            raw_seq.append([t_norm, cx / frame_w, cy / frame_h, fw / frame_w, conf])
            times.append(t)
        frame_i += 1
        read += 1
    cap.release()

    if len(raw_seq) < 2:
        return {"trajectory": []}

    with torch.no_grad():
        x = torch.tensor([raw_seq], dtype=torch.float32, device=_DEVICE)
        pred = refiner(x)[0].cpu().tolist()

    trajectory = []
    for t, (cx_n, cy_n, fw_n, fh_n) in zip(times, pred):
        trajectory.append(
            {
                "t": round(t, 3),
                "cx": round(cx_n * frame_w, 1),
                "cy": round(cy_n * frame_h, 1),
                "face_w": round(fw_n * frame_w, 1),
                "face_h": round(fh_n * frame_h, 1),
                "confidence": 0.9,
            }
        )
    return {"trajectory": trajectory}


def _legacy_inference_handler(event: dict) -> dict:
    inp = dict(event.get("input") or event or {})
    task = str(inp.get("task") or "health").strip().lower()

    if task == "health":
        reg = MODELS / "model_registry.json"
        return {
            "ok": True,
            "volume": str(VOLUME),
            "device": str(_DEVICE),
            "registry_exists": reg.exists(),
            "virality_weights": (MODELS / "virality/v1/model.pt").exists(),
            "reframe_weights": (MODELS / "reframe/v1/tracker.pt").exists(),
        }
    if task == "score_segments":
        return _score_segments(inp)
    if task == "reframe_trajectory":
        return _reframe_trajectory(inp)
    if task == "bootstrap_weights":
        return _bootstrap_weights(inp)
    return {"error": f"unknown task: {task}"}


def _bootstrap_weights(inp: dict[str, Any]) -> dict[str, Any]:
    """Write base64-encoded files onto the shared volume (one-time deploy helper)."""
    files = dict(inp.get("files") or {})
    if not files:
        return {"error": "files dict required"}
    written = []
    for rel_path, b64 in files.items():
        rel = str(rel_path).lstrip("/").replace("..", "")
        dest = VOLUME / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(base64.b64decode(b64))
        written.append(str(dest))
    return {"ok": True, "written": written}


def handler(_event: dict) -> dict:
    """Reject every legacy serverless invocation without inspecting its input."""

    return {
        "ok": False,
        "status": "rejected",
        "error": "runpod_retired",
        "detail": "ClipLab RunPod execution is permanently retired; production is Contabo-owned.",
    }


if __name__ == "__main__":
    import runpod

    runpod.serverless.start({"handler": handler})
