"""Provider-free render simulation helpers.

This module exists so load tests can exercise Studio render stages without
touching FAL, uploading files, or debiting real provider spend.
"""
from __future__ import annotations

import base64
import os
import subprocess
import time
from pathlib import Path


def env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def enabled() -> bool:
    return env_bool("STUDIO_RENDER_SIMULATION_MODE", False)


def sleep(stage: str = "") -> None:
    try:
        scale = max(0.0, float(os.getenv("STUDIO_RENDER_SIMULATION_SLEEP_SCALE", "0.15") or 0.15))
    except Exception:
        scale = 0.15
    defaults = {
        "still": 0.35,
        "i2v": 0.8,
        "voice": 0.25,
        "audio": 0.25,
    }
    time.sleep(defaults.get(stage, 0.25) * scale)


def _tiny_png_bytes() -> bytes:
    return base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADElEQVR42mNk+M9QDwADggGOSHzRgAAAAABJRU5ErkJggg=="
    )


def write_still(out_path: Path, *, label: str = "Studio simulation") -> Path:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    sleep("still")
    try:
        from PIL import Image, ImageDraw

        image = Image.new("RGB", (720, 1280), (8, 14, 24))
        draw = ImageDraw.Draw(image)
        draw.rectangle((48, 48, 672, 1232), outline=(0, 220, 255), width=4)
        draw.text((80, 120), label[:80], fill=(230, 250, 255))
        draw.text((80, 170), "provider-free dry run", fill=(120, 220, 180))
        image.save(out_path, "PNG")
    except Exception:
        out_path.write_bytes(_tiny_png_bytes())
    return out_path


def write_audio(out_path: Path, *, duration_sec: float = 2.0) -> Path:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    sleep("voice")
    duration = max(0.25, min(30.0, float(duration_sec or 2.0)))
    cmd = [
        "ffmpeg",
        "-y",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        f"anullsrc=channel_layout=stereo:sample_rate=32000",
        "-t",
        f"{duration:.3f}",
        "-c:a",
        "libmp3lame",
        "-q:a",
        "5",
        str(out_path),
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True)
    except Exception:
        out_path.write_bytes(b"SIMULATED_AUDIO")
    return out_path


def write_video(out_path: Path, *, still_path: Path | None = None, duration_sec: float = 2.0) -> Path:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    sleep("i2v")
    duration = max(0.25, min(30.0, float(duration_sec or 2.0)))
    if still_path and Path(still_path).is_file() and Path(still_path).stat().st_size > 1024:
        cmd = [
            "ffmpeg",
            "-y",
            "-loglevel",
            "error",
            "-loop",
            "1",
            "-i",
            str(still_path),
            "-t",
            f"{duration:.3f}",
            "-vf",
            "scale=720:1280,format=yuv420p",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            str(out_path),
        ]
    else:
        cmd = [
            "ffmpeg",
            "-y",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "color=c=0x08121f:s=720x1280:r=24",
            "-t",
            f"{duration:.3f}",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-pix_fmt",
            "yuv420p",
            str(out_path),
        ]
    try:
        subprocess.run(cmd, check=True, capture_output=True)
    except Exception:
        out_path.write_bytes(b"SIMULATED_VIDEO")
    return out_path
