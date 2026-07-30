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
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any, Iterable
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


def decode_audio_clock(src_path: Path, out_path: Path) -> Path:
    """Decode narration to Studio's stable 48 kHz mono alignment clock."""
    src_path = Path(src_path)
    out_path = Path(out_path)
    if not src_path.is_file() or src_path.stat().st_size <= 0:
        raise RuntimeError(f"narration audio is missing: {src_path}")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "ffmpeg", "-y", "-loglevel", "error",
            "-i", str(src_path),
            "-map", "0:a:0",
            "-af", "asetpts=PTS-STARTPTS",
            "-ar", "48000",
            "-ac", "1",
            "-c:a", "pcm_s16le",
            str(out_path),
        ],
        check=True,
    )
    return out_path


def _file_sha256(path: Path | None) -> str:
    if path is None or not Path(path).is_file():
        return ""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _render_fingerprint(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _cached_render_matches(out_path: Path, cache_path: Path, fingerprint: str) -> bool:
    if not out_path.is_file() or out_path.stat().st_size <= 1024 or not cache_path.is_file():
        return False
    try:
        cached = json.loads(cache_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    return bool(isinstance(cached, dict) and cached.get("fingerprint") == fingerprint)


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
    caption_mode: str = "phrase",
    captions_enabled: bool = True,
    preserve_source_audio: bool = False,
    audio_path: Path | None = None,
    verified_word_timings: Iterable[dict[str, Any]] | None = None,
    alignment_cache_path: Path | None = None,
    force: bool = False,
) -> Path:
    """Trim a scene clip to exact duration and burn captions + watermark.

    Uses -filter_script:v to load the filter chain from a file — this avoids
    the catastrophic shell-escaping problem with drawtext filters when the
    text contains apostrophes, em-dashes, etc.
    """
    src_clip = Path(src_clip)
    out_path = Path(out_path)
    audio_path = Path(audio_path) if audio_path else None
    caption_mode = "word" if str(caption_mode or "").lower() in {"word", "single_word", "one_word"} else "phrase"
    duration_sec = max(0.0, float(duration_sec or 0.0))
    if duration_sec <= 0.0:
        raise RuntimeError("scene duration must be greater than zero")
    if audio_path:
        audio_duration = probe_duration(audio_path)
        if audio_duration > 0.0:
            # Narration is the canonical edit clock.  Picture is padded/trimmed
            # to this value rather than shortening speech to provider video.
            duration_sec = audio_duration

    verified = [dict(row) for row in list(verified_word_timings or []) if isinstance(row, dict)]
    if captions_enabled and caption_mode == "word" and not verified:
        if not audio_path:
            raise cap.CaptionTimingError("word captions require the scene narration audio")
        from studio_agent.caption_alignment import align_audio_words

        verified = align_audio_words(
            audio_path,
            cache_path=Path(alignment_cache_path) if alignment_cache_path else out_path.with_suffix(".alignment.json"),
        )

    drawtexts = []
    timed: list[cap.CaptionPhrase] = []
    timing_source = "disabled"
    if captions_enabled:
        timed, timing_source = cap.build_timed_captions(
            narration_text,
            duration_sec,
            caption_mode=caption_mode,
            verified_word_timings=verified,
        )
        drawtexts = [cap.caption_drawtext(p, width=width, caption_mode=caption_mode) for p in timed]
    drawtexts.append(cap.watermark_drawtext(watermark_text=watermark_text))

    render_payload = {
        "version": 2,
        "source_sha256": _file_sha256(src_clip),
        "audio_sha256": _file_sha256(audio_path),
        "duration_sec": round(duration_sec, 6),
        "narration_text": narration_text,
        "width": int(width),
        "height": int(height),
        "fps": int(fps),
        "watermark_text": watermark_text,
        "caption_mode": caption_mode,
        "captions_enabled": bool(captions_enabled),
        "preserve_source_audio": bool(preserve_source_audio),
        "timing_source": timing_source,
        "verified_word_timings": verified,
    }
    fingerprint = _render_fingerprint(render_payload)
    render_cache = out_path.with_suffix(out_path.suffix + ".render.json")
    if not force and _cached_render_matches(out_path, render_cache, fingerprint):
        return out_path

    vf = (
        "setpts=PTS-STARTPTS,"
        f"scale={width}:{height}:force_original_aspect_ratio=decrease,"
        f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:black,fps={fps},"
        f"tpad=stop_mode=clone:stop_duration={duration_sec:.6f},"
        f"trim=duration={duration_sec:.6f},setpts=PTS-STARTPTS,"
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
        cmd.extend([
            "-af", f"asetpts=PTS-STARTPTS,apad,atrim=duration={duration_sec:.6f}",
            "-c:a", "aac", "-b:a", "128k",
        ])
    elif preserve_source_audio:
        cmd.extend(["-map", "0:v:0", "-map", "1:a:0"])
        cmd.extend([
            "-af", f"asetpts=PTS-STARTPTS,apad,atrim=duration={duration_sec:.6f}",
            "-c:a", "aac", "-b:a", "128k",
        ])
    else:
        cmd.append("-an")
    cmd.extend(["-movflags", "+faststart"])
    cmd.append(str(out_path))
    subprocess.run(cmd, check=True)
    cues = [
        {
            "text": item.text,
            "start": round(item.start_sec, 4),
            "end": round(min(duration_sec, item.start_sec + item.duration_sec), 4),
        }
        for item in timed
    ]
    out_path.with_suffix(out_path.suffix + ".captions.json").write_text(
        json.dumps(
            {
                "version": 2,
                "enabled": bool(captions_enabled),
                "mode": caption_mode if captions_enabled else "off",
                "timing_source": timing_source,
                "duration_sec": duration_sec,
                "cues": cues,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    render_cache.write_text(
        json.dumps({"version": 2, "fingerprint": fingerprint, "inputs": render_payload}, indent=2),
        encoding="utf-8",
    )
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


#: Music sits well under the voice. This is the bed level *before* ducking, so
#: the audible level during speech is lower again.
MUSIC_BED_GAIN = 0.16
MUSIC_FADE_IN_SEC = 1.5
MUSIC_FADE_OUT_SEC = 2.0


def _music_bed_filters(duration_sec: float, music_gain: float) -> str:
    """Build the ducked music bed graph mixed under the narration.

    The bed is sidechained to the voice rather than set to a fixed level: a
    static mix either buries the music between lines or fights the narration
    during them. Ducking gives the bed presence in the gaps and gets out of the
    way when anyone is speaking, which is what "low background music" actually
    means in a finished video.
    """
    gain = max(0.01, float(music_gain))
    fade_out_start = max(0.0, float(duration_sec) - MUSIC_FADE_OUT_SEC)
    return (
        # Two copies of the voice: one to mix, one to drive the ducker.
        "[1:a]asetpts=PTS-STARTPTS,asplit=2[voice][key];"
        f"[2:a]asetpts=PTS-STARTPTS,atrim=duration={duration_sec:.6f},"
        # Keep the bed out of the vocal-presence and sub-bass ranges entirely.
        "highpass=f=60,lowpass=f=7000,"
        f"volume={gain:.3f},"
        f"afade=t=in:st=0:d={MUSIC_FADE_IN_SEC:.3f},"
        f"afade=t=out:st={fade_out_start:.6f}:d={MUSIC_FADE_OUT_SEC:.3f}[bed];"
        # ratio/attack/release tuned so the bed dips quickly under a line and
        # recovers smoothly in the pause rather than pumping between words.
        "[bed][key]sidechaincompress=threshold=0.030:ratio=8:attack=15:release=350[ducked];"
        # normalize=0 is required: amix otherwise divides every input by the
        # input count, so simply adding a music bed would drop the narration by
        # ~6dB. The voice must sound identical whether or not music is present.
        "[voice][ducked]amix=inputs=2:duration=first:dropout_transition=0:normalize=0,"
        "alimiter=limit=0.95[aout]"
    )


def mux_narration(
    silent_video: Path,
    narration_audio: Path,
    out_path: Path,
    *,
    fps: int = 30,
    caption_phrases: Iterable[cap.CaptionPhrase] | None = None,
    music_track: Path | None = None,
    music_gain: float = MUSIC_BED_GAIN,
) -> Path:
    """Mux narration without ever shortening it to the provider video clock."""
    out_path = Path(out_path)
    duration_sec = probe_duration(narration_audio)
    if duration_sec <= 0.0:
        raise RuntimeError("cannot mux narration with an unreadable audio clock")
    music_path = Path(music_track) if music_track else None
    use_music = bool(
        music_path and music_path.is_file() and music_path.stat().st_size > 0
    )
    # Belt-and-suspenders: strip any leftover Grok/provider talk track first.
    strip_clip_audio(silent_video)
    video_filters = [
        "setpts=PTS-STARTPTS",
        f"fps={int(fps)}",
        f"tpad=stop_mode=clone:stop_duration={duration_sec:.6f}",
        f"trim=duration={duration_sec:.6f}",
        "setpts=PTS-STARTPTS",
    ]
    for phrase in list(caption_phrases or []):
        video_filters.append(cap.caption_drawtext(phrase, caption_mode="word"))
    filter_script = out_path.with_suffix(out_path.suffix + ".mux-filter.txt")
    filter_script.parent.mkdir(parents=True, exist_ok=True)
    filter_script.write_text(",".join(video_filters), encoding="utf-8")
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", str(silent_video),
        "-i", str(narration_audio),
    ]
    if use_music:
        # Loop the bed so a short track still covers the whole video; the
        # atrim in the graph cuts it back to the narration clock.
        cmd += ["-stream_loop", "-1", "-i", str(music_path)]
        audio_filter_script = out_path.with_suffix(out_path.suffix + ".mix-filter.txt")
        audio_filter_script.write_text(
            _music_bed_filters(duration_sec, music_gain), encoding="utf-8"
        )
        cmd += [
            "-map", "0:v:0", "-map", "[aout]",
            "-filter_script:v", str(filter_script),
            "-filter_complex_script", str(audio_filter_script),
        ]
    else:
        cmd += [
            "-map", "0:v:0", "-map", "1:a:0",
            "-filter_script:v", str(filter_script),
            "-af", "asetpts=PTS-STARTPTS",
        ]
    cmd += [
        "-c:v", "libx264", "-preset", "medium", "-crf", "20", "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "192k",
        "-t", f"{duration_sec:.6f}",
        "-movflags", "+faststart",
        str(out_path),
    ]
    subprocess.run(cmd, check=True)
    return out_path
