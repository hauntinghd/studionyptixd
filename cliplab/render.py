"""Extract + reframe + caption render for ClipLab clips."""
from __future__ import annotations

import asyncio
import logging
import os
import subprocess
from pathlib import Path

from cliplab.config import (
    CLIPLAB_OUTPUT_FPS,
    CLIPLAB_OUTPUT_HEIGHT,
    CLIPLAB_OUTPUT_WIDTH,
    CLIPLAB_RENDER_DIR,
)
from cliplab.models import ClipSegment, TranscriptCue
from cliplab.reframe import build_segment_crop_filter, compute_trajectory, probe_video_size

_log = logging.getLogger("nyptid-studio.cliplab.render")


def _caption_font_option() -> str:
    candidates = [
        os.getenv("CLIPLAB_CAPTION_FONT", "").strip(),
        r"C:\Windows\Fonts\arialbd.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
        "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf",
        "/usr/share/fonts/opentype/noto/NotoSans-Bold.ttf",
    ]
    for raw in candidates:
        if not raw:
            continue
        path = Path(raw)
        if not path.is_file():
            continue
        escaped = path.as_posix().replace(":", r"\:").replace("'", r"\'")
        return f"fontfile='{escaped}':"
    return ""


def _cues_in_range(cues: list[TranscriptCue], start: float, end: float) -> list[TranscriptCue]:
    out: list[TranscriptCue] = []
    for c in cues:
        if c.end < start or c.start > end:
            continue
        out.append(c)
    return out


def _caption_filter_chain(
    cues: list[TranscriptCue],
    *,
    width: int = 1080,
    style: str = "karaoke",
) -> str:
    """Karaoke-style phrase captions using drawtext (filter_script file recommended for prod)."""
    if not cues:
        return ""
    del width
    selected_style = str(style or "karaoke").strip().lower()
    font = _caption_font_option()
    if selected_style == "minimal":
        typography = f"{font}fontsize=42:fontcolor=white:borderw=2:bordercolor=black"
        y_expr = "h*0.78"
    elif selected_style == "empire":
        typography = f"{font}fontsize=56:fontcolor=0xF2C94C:borderw=5:bordercolor=black"
        y_expr = "h*0.72"
    else:
        typography = f"{font}fontsize=58:fontcolor=white:borderw=5:bordercolor=black"
        y_expr = "h*0.72"
    parts: list[str] = []
    for cue in cues:
        text = (
            str(cue.text or "")
            .replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace(":", "\\:")
            .replace(",", "\\,")
        )
        if not text:
            continue
        parts.append(
            f"drawtext=text='{text[:120]}':{typography}:expansion=none:"
            f"x=(w-text_w)/2:y={y_expr}:enable='between(t\\,{max(0,cue.start):.3f}\\,{cue.end:.3f})'"
        )
    return ",".join(parts[:24])  # cap filter complexity


def build_remix_filter(
    *,
    background_mode: str,
    style_preset: str,
    edit_intensity: str,
    caption_style: str,
    cues: list[TranscriptCue] | None = None,
    burn_captions: bool = True,
) -> str:
    """Build the complete vertical treatment graph for Remix Lab."""
    style = str(style_preset or "clean_viral").strip().lower()
    intensity = str(edit_intensity or "medium").strip().lower()
    treatments = {
        "clean_viral": (1.04, 1.08, 0.005),
        "empire": (1.08, 0.96, -0.005),
        "empire_magnates": (1.08, 0.96, -0.005),
        "documentary": (1.08, 0.88, -0.01),
        "streamer": (1.05, 1.14, 0.005),
        "high_energy": (1.09, 1.20, 0.01),
    }
    contrast, saturation, brightness = treatments.get(style, treatments["clean_viral"])
    sharpen = {"low": 0.25, "medium": 0.5, "high": 0.8}.get(intensity, 0.5)
    treatment = (
        f"eq=contrast={contrast:.3f}:saturation={saturation:.3f}:brightness={brightness:.3f},"
        f"unsharp=5:5:{sharpen:.2f}:5:5:0.0,fps={CLIPLAB_OUTPUT_FPS},format=yuv420p"
    )
    captions = ""
    if burn_captions and cues:
        caption_chain = _caption_filter_chain(cues, style=caption_style)
        if caption_chain:
            captions = f",{caption_chain}"
    if str(background_mode or "blur").strip().lower() == "solid":
        return (
            f"[0:v]scale={CLIPLAB_OUTPUT_WIDTH}:{CLIPLAB_OUTPUT_HEIGHT}:force_original_aspect_ratio=decrease,"
            f"pad={CLIPLAB_OUTPUT_WIDTH}:{CLIPLAB_OUTPUT_HEIGHT}:(ow-iw)/2:(oh-ih)/2:color=black,"
            f"{treatment}{captions}[vout]"
        )
    return (
        "[0:v]split=2[bgsrc][fgsrc];"
        f"[bgsrc]scale={CLIPLAB_OUTPUT_WIDTH}:{CLIPLAB_OUTPUT_HEIGHT}:force_original_aspect_ratio=increase,"
        f"crop={CLIPLAB_OUTPUT_WIDTH}:{CLIPLAB_OUTPUT_HEIGHT},gblur=sigma=28[bg];"
        f"[fgsrc]scale={CLIPLAB_OUTPUT_WIDTH}:{CLIPLAB_OUTPUT_HEIGHT}:force_original_aspect_ratio=decrease[fg];"
        f"[bg][fg]overlay=(W-w)/2:(H-h)/2:shortest=1,{treatment}{captions}[vout]"
    )


async def render_remix_short(
    video_path: str,
    video_id: str,
    *,
    job_id: str,
    cues: list[TranscriptCue] | None = None,
    style_preset: str = "clean_viral",
    caption_style: str = "bold",
    edit_intensity: str = "medium",
    background_mode: str = "blur",
    burn_captions: bool = True,
) -> dict:
    """Render a real, playable 9:16 Remix Lab artifact with preserved audio."""
    safe_video_id = "".join(ch for ch in str(video_id or "") if ch.isalnum() or ch in "_-")[:96]
    safe_job_id = "".join(ch for ch in str(job_id or "") if ch.isalnum() or ch in "_-")[:96]
    if not safe_video_id or not safe_job_id:
        raise ValueError("invalid ClipLab remix identifier")
    source = Path(video_path)
    if not source.is_file():
        raise FileNotFoundError("ClipLab remix source does not exist")
    out_dir = CLIPLAB_RENDER_DIR / safe_video_id
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{safe_job_id}.mp4"
    out = out_dir / filename
    filter_graph = build_remix_filter(
        background_mode=background_mode,
        style_preset=style_preset,
        edit_intensity=edit_intensity,
        caption_style=caption_style,
        cues=cues,
        burn_captions=burn_captions,
    )
    cmd = [
        "ffmpeg", "-y", "-i", str(source),
        "-filter_complex", filter_graph,
        "-map", "[vout]", "-map", "0:a?",
        "-c:v", "libx264", "-preset", "medium", "-crf", "19",
        "-c:a", "aac", "-b:a", "192k", "-ar", "48000",
        "-movflags", "+faststart", "-shortest", str(out),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0 or not out.is_file() or out.stat().st_size <= 0:
        out.unlink(missing_ok=True)
        raise RuntimeError(f"ClipLab remix render failed: {stderr.decode(errors='ignore')[-500:]}")
    _, _, duration = probe_video_size(str(out))
    return {
        "path": str(out),
        "filename": filename,
        "duration_sec": duration,
        "style_preset": style_preset,
        "caption_style": caption_style,
        "edit_intensity": edit_intensity,
        "background_mode": background_mode,
        "captions_burned": bool(burn_captions and cues),
    }


async def render_clip(
    video_path: str,
    segment: ClipSegment,
    out_path: str,
    *,
    cues: list[TranscriptCue] | None = None,
    burn_captions: bool = True,
    reframe_backend: str = "",
) -> str:
    start = max(0.0, float(segment.start))
    end = max(start + 1.0, float(segment.end))
    duration = end - start

    src_w, src_h, _ = probe_video_size(video_path)
    trajectory = await compute_trajectory(
        video_path, start_sec=start, duration_sec=duration, backend=reframe_backend,
    )
    vf = build_segment_crop_filter(trajectory, src_w, src_h)
    if burn_captions and cues:
        cap = _caption_filter_chain(_cues_in_range(cues, start, end))
        if cap:
            vf = f"{vf},{cap}"

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg", "-y", "-ss", f"{start:.3f}", "-i", video_path,
        "-t", f"{duration:.3f}",
        "-vf", vf,
        "-c:v", "libx264", "-preset", "medium", "-crf", "20",
        "-c:a", "aac", "-b:a", "192k", "-ar", "48000",
        "-movflags", "+faststart",
        "-pix_fmt", "yuv420p",
        str(out),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0 or not out.exists():
        raise RuntimeError(f"Clip render failed: {stderr.decode()[-300:]}")
    return str(out)


async def render_clips_batch(
    video_path: str,
    video_id: str,
    segments: list[ClipSegment],
    indices: list[int],
    *,
    cues: list[TranscriptCue] | None = None,
    burn_captions: bool = True,
    reframe_backend: str = "",
) -> list[dict]:
    out_dir = CLIPLAB_RENDER_DIR / video_id
    out_dir.mkdir(parents=True, exist_ok=True)
    rendered: list[dict] = []
    for idx in indices:
        if idx < 0 or idx >= len(segments):
            continue
        seg = segments[idx]
        fname = f"clip_{idx:03d}_{int(seg.start)}_{int(seg.end)}.mp4"
        path = str(out_dir / fname)
        try:
            await render_clip(
                video_path, seg, path,
                cues=cues, burn_captions=burn_captions, reframe_backend=reframe_backend,
            )
            rendered.append({
                "index": idx,
                "path": path,
                "filename": fname,
                "start": seg.start,
                "end": seg.end,
                "virality_score": seg.virality_score,
            })
        except Exception as exc:
            _log.warning("Clip %s render failed: %s", idx, str(exc)[:200])
            rendered.append({"index": idx, "error": str(exc)[:200]})
    return rendered
