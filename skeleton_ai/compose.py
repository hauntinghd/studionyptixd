"""
ffmpeg compose for Skeleton AI shorts.

Stages:
  1. Pre-render each scene clip individually with -t exact duration + 2-tier
     captions burned in + watermark. (Avoids xfade/concat-filter frame loss
     bug we hit on Olympus rebuild.)
  2. concat demuxer for the spine — byte-copy, no filter graph involved.
  3. Mux narration audio over the silent video.
  4. (Optional) loudnorm pass to -14 LUFS.

Output: 720x1280 / 30fps / H.264 + AAC mono.
"""
from __future__ import annotations
import subprocess
from pathlib import Path
from . import captions as cap


def probe_duration(path: Path) -> float:
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
        capture_output=True, text=True,
    )
    try:
        return float(r.stdout.strip())
    except ValueError:
        return 0.0


def trim_with_captions(
    src_clip: Path,
    out_path: Path,
    *,
    duration_sec: float,
    narration_text: str,
    width: int = 720,
    height: int = 1280,
    fps: int = 30,
) -> Path:
    """Trim a scene clip to exact duration and burn captions + watermark."""
    out_path = Path(out_path)
    if out_path.exists() and out_path.stat().st_size > 1024:
        return out_path

    phrases = cap.split_into_phrases(narration_text)
    timed = cap.time_phrases(phrases, duration_sec)

    drawtexts = [cap.caption_drawtext(p, width=width) for p in timed]
    drawtexts.append(cap.watermark_drawtext())

    vf = (
        f"scale={width}:{height}:force_original_aspect_ratio=decrease,"
        f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:black,fps={fps},"
        f"{','.join(drawtexts)}"
    )

    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", str(src_clip),
        "-t", f"{duration_sec:.3f}",
        "-vf", vf,
        "-an",
        "-c:v", "libx264", "-preset", "medium", "-crf", "20",
        "-pix_fmt", "yuv420p",
        str(out_path),
    ]
    out_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(cmd, check=True)
    return out_path


def concat_demuxer(trimmed_clips: list[Path], out_silent: Path, work_dir: Path) -> Path:
    """Concatenate trimmed clips byte-for-byte. No re-encode."""
    out_silent = Path(out_silent)
    work_dir = Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    concat_list = work_dir / "concat.txt"
    with open(concat_list, "w") as f:
        for c in trimmed_clips:
            f.write(f"file '{Path(c).as_posix()}'\n")
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-f", "concat", "-safe", "0", "-i", str(concat_list),
        "-c", "copy",
        str(out_silent),
    ], check=True)
    return out_silent


def mux_narration(silent_video: Path, narration_audio: Path, out_path: Path) -> Path:
    """Mux narration audio onto the silent video."""
    out_path = Path(out_path)
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", str(silent_video),
        "-i", str(narration_audio),
        "-map", "0:v:0", "-map", "1:a:0",
        "-c:v", "copy",
        "-c:a", "aac", "-b:a", "192k",
        "-shortest",
        str(out_path),
    ]
    subprocess.run(cmd, check=True)
    return out_path
