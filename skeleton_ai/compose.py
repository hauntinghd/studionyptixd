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


def has_audio(path: Path) -> bool:
    r = subprocess.run(
        [
            "ffprobe", "-v", "error",
            "-select_streams", "a:0",
            "-show_entries", "stream=codec_type",
            "-of", "csv=p=0",
            str(path),
        ],
        capture_output=True,
        text=True,
    )
    return "audio" in (r.stdout or "").lower()


def strip_clip_audio(path: Path) -> Path:
    """Force i2v clips silent. Provider audio (Grok talk/music) is discarded;
    Studio narration is muxed later via FAL MiniMax voiceover."""
    path = Path(path)
    if not path.is_file() or path.stat().st_size < 1024:
        return path
    if not has_audio(path):
        return path
    tmp = path.with_suffix(path.suffix + ".silent.tmp.mp4")
    try:
        subprocess.run(
            [
                "ffmpeg", "-y", "-loglevel", "error",
                "-i", str(path),
                "-c:v", "copy",
                "-an",
                str(tmp),
            ],
            check=True,
        )
        if tmp.is_file() and tmp.stat().st_size > 1024:
            tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)
    return path


def trim_with_captions(
    src_clip: Path,
    out_path: Path,
    *,
    duration_sec: float,
    narration_text: str,
    width: int = 720,
    height: int = 1280,
    fps: int = 30,
    watermark_text: str = "Studio",
    caption_mode: str = "word",
    captions_enabled: bool = True,
    preserve_source_audio: bool = False,
    force: bool = False,
) -> Path:
    """Trim a scene clip to exact duration and burn captions + watermark.

    Uses -filter_script:v to load the filter chain from a file — this avoids
    the catastrophic shell-escaping problem with drawtext filters when the
    text contains apostrophes, em-dashes, etc.
    """
    out_path = Path(out_path)
    if not force and out_path.exists() and out_path.stat().st_size > 1024:
        return out_path

    caption_mode = "word" if str(caption_mode or "").lower() in {"word", "single_word", "one_word"} else "phrase"
    drawtexts = []
    if captions_enabled:
        phrases = cap.split_into_phrases(narration_text, max_words=1 if caption_mode == "word" else 3)
        timed = cap.time_phrases(phrases, duration_sec)
        drawtexts = [cap.caption_drawtext(p, width=width, caption_mode=caption_mode) for p in timed]
    drawtexts.append(cap.watermark_drawtext(watermark_text=watermark_text))

    vf = (
        f"scale={width}:{height}:force_original_aspect_ratio=decrease,"
        f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:black,fps={fps},"
        f"{','.join(drawtexts)}"
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    filter_script = out_path.parent / f"{out_path.stem}_filter.txt"
    filter_script.write_text(vf, encoding="utf-8")

    source_has_audio = has_audio(src_clip)
    cmd = ["ffmpeg", "-y", "-loglevel", "error", "-i", str(src_clip)]
    if preserve_source_audio and not source_has_audio:
        cmd.extend([
            "-f", "lavfi",
            "-t", f"{duration_sec:.3f}",
            "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",
        ])
    cmd.extend([
        "-t", f"{duration_sec:.3f}",
        "-filter_script:v", str(filter_script),
        "-c:v", "libx264", "-preset", "medium", "-crf", "20",
        "-pix_fmt", "yuv420p",
    ])
    if preserve_source_audio and source_has_audio:
        cmd.extend(["-map", "0:v:0", "-map", "0:a:0"])
        cmd.extend(["-c:a", "aac", "-b:a", "128k"])
    elif preserve_source_audio:
        cmd.extend(["-map", "0:v:0", "-map", "1:a:0"])
        cmd.extend(["-c:a", "aac", "-b:a", "128k"])
    else:
        cmd.append("-an")
    cmd.append(str(out_path))
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
    """Mux FAL voiceover onto silent picture. Never keep provider i2v audio."""
    out_path = Path(out_path)
    # Belt-and-suspenders: strip any leftover Grok/provider talk track first.
    strip_clip_audio(silent_video)
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
