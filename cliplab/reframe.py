"""Face-aware 9:16 reframe with OpenCV + RunPod custom tracker hook."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
from pathlib import Path

import cv2
import httpx
import numpy as np

from cliplab.config import (
    CLIPLAB_OUTPUT_FPS,
    CLIPLAB_OUTPUT_HEIGHT,
    CLIPLAB_OUTPUT_WIDTH,
    REFRAME_BACKEND,
    RUNPOD_CLIPLAB_URL,
)
from cliplab.model_registry import active_checkpoint
from cliplab.models import FaceTrajectoryPoint
from studio_agent import provider_policy

_log = logging.getLogger("nyptid-studio.cliplab.reframe")

_SMOOTH_ALPHA = 0.22
_SAMPLE_EVERY_N = 3  # frames


class FaceTracker:
    """Detect + smooth face center trajectory for vertical crop."""

    def __init__(self) -> None:
        cascade_path = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
        self._cascade = cv2.CascadeClassifier(cascade_path)
        self._dnn_net = None
        self._try_load_yunet()

    def _try_load_yunet(self) -> None:
        try:
            model = Path(cv2.data.haarcascades).parent / "opencv_face_detector_uint8.pb"
            config = Path(cv2.data.haarcascades).parent / "opencv_face_detector.pbtxt"
            if model.exists() and config.exists():
                self._dnn_net = cv2.dnn.readNetFromTensorflow(str(model), str(config))
        except Exception:
            self._dnn_net = None

    def _detect_largest_face(self, frame: np.ndarray) -> tuple[float, float, float, float, float] | None:
        h, w = frame.shape[:2]
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        faces = self._cascade.detectMultiScale(gray, scaleFactor=1.08, minNeighbors=4, minSize=(48, 48))
        if len(faces) == 0 and self._dnn_net is not None:
            blob = cv2.dnn.blobFromImage(frame, 1.0, (300, 300), (104.0, 177.0, 123.0))
            self._dnn_net.setInput(blob)
            detections = self._dnn_net.forward()
            best = None
            best_area = 0
            for i in range(detections.shape[2]):
                conf = float(detections[0, 0, i, 2])
                if conf < 0.55:
                    continue
                box = detections[0, 0, i, 3:7] * np.array([w, h, w, h])
                x1, y1, x2, y2 = box.astype(int)
                area = (x2 - x1) * (y2 - y1)
                if area > best_area:
                    best_area = area
                    best = (x1, y1, x2 - x1, y2 - y1, conf)
            if best:
                x, y, fw, fh, conf = best
                return (x + fw / 2, y + fh / 2, fw, fh, conf)
            return None
        if len(faces) == 0:
            return None
        x, y, fw, fh = max(faces, key=lambda f: f[2] * f[3])
        return (x + fw / 2, y + fh / 2, float(fw), float(fh), 0.85)

    def track_video(self, video_path: str, *, start_sec: float = 0, duration_sec: float = 0) -> list[FaceTrajectoryPoint]:
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            return []
        fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
        if start_sec > 0:
            cap.set(cv2.CAP_PROP_POS_MSEC, start_sec * 1000)
        max_frames = int((duration_sec or 3600) * fps) if duration_sec else int(3600 * fps)

        trajectory: list[FaceTrajectoryPoint] = []
        smooth_cx = smooth_cy = None
        frame_i = 0
        read = 0

        while read < max_frames:
            ok, frame = cap.read()
            if not ok:
                break
            if frame_i % _SAMPLE_EVERY_N != 0:
                frame_i += 1
                continue
            t = start_sec + frame_i / fps
            det = self._detect_largest_face(frame)
            if det:
                cx, cy, fw, fh, conf = det
                if smooth_cx is None:
                    smooth_cx, smooth_cy = cx, cy
                else:
                    smooth_cx = smooth_cx * (1 - _SMOOTH_ALPHA) + cx * _SMOOTH_ALPHA
                    smooth_cy = smooth_cy * (1 - _SMOOTH_ALPHA) + cy * _SMOOTH_ALPHA
                trajectory.append(
                    FaceTrajectoryPoint(
                        t=round(t, 3), cx=smooth_cx, cy=smooth_cy,
                        face_w=fw, face_h=fh, confidence=conf,
                    )
                )
            frame_i += 1
            read += 1
        cap.release()
        return trajectory


async def _runpod_face_trajectory(video_path: str, start_sec: float, duration_sec: float) -> list[FaceTrajectoryPoint]:
    provider_policy.assert_provider_allowed(
        "runpod",
        provider_policy.I2V_CAPABILITY,
    )
    runpod_key = os.getenv("RUNPOD_API_KEY", "").strip()
    if not runpod_key or not RUNPOD_CLIPLAB_URL:
        return []
    active_id, weights = active_checkpoint("face_reframe")
    if not weights or not weights.exists():
        return []
    try:
        async with httpx.AsyncClient(timeout=300) as client:
            resp = await client.post(
                RUNPOD_CLIPLAB_URL,
                headers={"Authorization": f"Bearer {runpod_key}"},
                json={
                    "input": {
                        "task": "reframe_trajectory",
                        "video_path": video_path,
                        "start_sec": start_sec,
                        "duration_sec": duration_sec,
                        "weights_path": str(weights),
                    }
                },
            )
        if resp.status_code != 200:
            return []
        data = resp.json()
        output = dict((data.get("output") or data) if isinstance(data, dict) else {})
        rows = list(output.get("trajectory") or [])
        return [FaceTrajectoryPoint(**r) for r in rows if isinstance(r, dict)]
    except Exception as exc:
        _log.warning("RunPod face trajectory failed: %s", str(exc)[:200])
        return []


def _crop_x_for_frame(src_w: int, src_h: int, cx: float) -> int:
    crop_w = int(src_h * 9 / 16)
    crop_w = min(crop_w, src_w)
    x = int(cx - crop_w / 2)
    return max(0, min(x, src_w - crop_w))


def build_segment_crop_filter(trajectory: list[FaceTrajectoryPoint], src_w: int, src_h: int) -> str:
    """Static crop from median face position (polished for talking-head)."""
    crop_w = int(src_h * 9 / 16)
    crop_w = min(crop_w, src_w)
    crop_h = src_h
    if trajectory:
        cx = float(np.median([p.cx for p in trajectory]))
        cy = float(np.median([p.cy for p in trajectory]))
        x = _crop_x_for_frame(src_w, src_h, cx)
        # slight vertical bias to keep face above center (Shorts safe zone)
        y = max(0, min(int(cy - crop_h * 0.42), src_h - crop_h))
    else:
        x = (src_w - crop_w) // 2
        y = 0
    return (
        f"crop={crop_w}:{crop_h}:{x}:{y},"
        f"scale={CLIPLAB_OUTPUT_WIDTH}:{CLIPLAB_OUTPUT_HEIGHT},"
        f"fps={CLIPLAB_OUTPUT_FPS}"
    )


def probe_video_size(video_path: str) -> tuple[int, int, float]:
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height", "-show_entries", "format=duration",
             "-of", "json", video_path],
            capture_output=True, text=True, check=False,
        )
        data = json.loads(r.stdout or "{}")
        streams = list(data.get("streams") or [])
        w = int((streams[0] or {}).get("width") or 1920) if streams else 1920
        h = int((streams[0] or {}).get("height") or 1080) if streams else 1080
        dur = float((data.get("format") or {}).get("duration") or 0)
        return w, h, dur
    except Exception:
        return 1920, 1080, 0.0


async def compute_trajectory(
    video_path: str,
    *,
    start_sec: float = 0,
    duration_sec: float = 0,
    backend: str = "",
) -> list[FaceTrajectoryPoint]:
    use = backend or REFRAME_BACKEND
    if use == "runpod_face_v1":
        pts = await _runpod_face_trajectory(video_path, start_sec, duration_sec)
        if pts:
            return pts
    tracker = FaceTracker()
    return await asyncio.to_thread(
        tracker.track_video, video_path, start_sec=start_sec, duration_sec=duration_sec,
    )
