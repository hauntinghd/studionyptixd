"""Extract + reframe + caption render for ClipLab clips."""
from __future__ import annotations

import asyncio
import logging
import subprocess
from pathlib import Path

from cliplab.config import CLIPLAB_RENDER_DIR
from cliplab.models import ClipSegment, TranscriptCue
from cliplab.reframe import build_segment_crop_filter, compute_trajectory, probe_video_size

_log = logging.getLogger("nyptid-studio.cliplab.render")


def _cues_in_range(cues: list[TranscriptCue], start: float, end: float) -> list[TranscriptCue]:
    out: list[TranscriptCue] = []
    for c in cues:
        if c.end < start or c.start > end:
            continue
        out.append(c)
    return out


def _caption_filter_chain(cues: list[TranscriptCue], *, width: int = 1080) -> str:
    """Karaoke-style phrase captions using drawtext (filter_script file recommended for prod)."""
    if not cues:
        return ""
    parts: list[str] = []
    for cue in cues:
        text = str(cue.text or "").replace("'", "\\'").replace(":", "\\:").replace(",", "\\,")
        if not text:
            continue
        parts.append(
            f"drawtext=text='{text[:120]}':fontsize=42:fontcolor=white:borderw=3:bordercolor=black:"
            f"x=(w-text_w)/2:y=h*0.72:enable='between(t\\,{max(0,cue.start):.3f}\\,{cue.end:.3f})'"
        )
    return ",".join(parts[:24])  # cap filter complexity


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
                "score_breakdown": dict(seg.score_breakdown or {}),
                "hook_text": seg.hook_text,
                "why_it_matches": seg.why_it_matches,
                "visual_notes": seg.visual_notes,
                "audio_notes": seg.audio_notes,
                "narrative_role": seg.narrative_role,
                "retention_reason": seg.retention_reason,
                "edit_plan": list(seg.edit_plan or []),
            })
        except Exception as exc:
            _log.warning("Clip %s render failed: %s", idx, str(exc)[:200])
            rendered.append({"index": idx, "error": str(exc)[:200]})
    return rendered


def _clean_filter_text(value: str) -> str:
    return (
        str(value or "")
        .replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace(":", "\\:")
        .replace(",", "\\,")
        .replace("[", "\\[")
        .replace("]", "\\]")
    )


def _remix_caption_filter(cues: list[TranscriptCue], *, style: str = "bold") -> str:
    if not cues:
        return ""
    style_key = str(style or "bold").strip().lower()
    font_path = ""
    for candidate in ("C:/Windows/Fonts/arialbd.ttf", "C:/Windows/Fonts/arial.ttf"):
        if Path(candidate).exists():
            font_path = candidate.replace("\\", "/").replace(":", "\\:")
            break
    if style_key == "minimal":
        fontsize, y, borderw, fontcolor = 38, "h*0.76", 2, "white"
    elif style_key == "empire":
        fontsize, y, borderw, fontcolor = 48, "h*0.70", 4, "white"
    else:
        fontsize, y, borderw, fontcolor = 46, "h*0.72", 4, "white"
    parts: list[str] = []
    for cue in cues[:80]:
        text = _clean_filter_text(str(cue.text or "").strip()[:96])
        if not text:
            continue
        font_arg = f"fontfile='{font_path}':" if font_path else ""
        parts.append(
            "drawtext="
            f"{font_arg}text='{text}':fontsize={fontsize}:fontcolor={fontcolor}:"
            f"borderw={borderw}:bordercolor=black@0.92:"
            "box=1:boxcolor=black@0.28:boxborderw=18:"
            f"x=(w-text_w)/2:y={y}:"
            f"enable='between(t\\,{max(0.0, float(cue.start)):.3f}\\,{max(float(cue.end), float(cue.start) + 0.12):.3f})'"
        )
    return ",".join(parts)


def _remix_color_filters(style_preset: str, intensity: str) -> str:
    style = str(style_preset or "clean_viral").strip().lower()
    level = str(intensity or "medium").strip().lower()
    contrast = 1.08
    saturation = 1.08
    brightness = 0.0
    if style in {"documentary", "empire", "empire_magnates"}:
        contrast, saturation, brightness = 1.16, 1.05, -0.015
    elif style in {"high_energy", "streamer"}:
        contrast, saturation, brightness = 1.18, 1.18, 0.005
    elif style in {"clean", "clean_viral"}:
        contrast, saturation, brightness = 1.1, 1.1, 0.0
    if level == "low":
        contrast = 1.0 + ((contrast - 1.0) * 0.45)
        saturation = 1.0 + ((saturation - 1.0) * 0.45)
    elif level == "high":
        contrast = 1.0 + ((contrast - 1.0) * 1.35)
        saturation = 1.0 + ((saturation - 1.0) * 1.25)
    return f"eq=contrast={contrast:.3f}:saturation={saturation:.3f}:brightness={brightness:.3f}"


def _remix_video_filter(
    *,
    src_w: int,
    src_h: int,
    cues: list[TranscriptCue],
    style_preset: str,
    caption_style: str,
    edit_intensity: str,
    background_mode: str,
    burn_captions: bool,
) -> str:
    color = _remix_color_filters(style_preset, edit_intensity)
    mode = str(background_mode or "blur").strip().lower()
    if mode == "solid":
        base = (
            "[0:v]scale=1080:1920:force_original_aspect_ratio=decrease,"
            "pad=1080:1920:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1[base]"
        )
    else:
        base = (
            "[0:v]split=2[bg][fg];"
            "[bg]scale=1080:1920:force_original_aspect_ratio=increase,"
            "crop=1080:1920,gblur=sigma=32,eq=brightness=-0.06:saturation=0.82[back];"
            "[fg]scale=1080:1920:force_original_aspect_ratio=decrease,setsar=1[front];"
            "[back][front]overlay=(W-w)/2:(H-h)/2[base]"
        )
    filters = f"{base};[base]{color},unsharp=5:5:0.55:3:3:0.22"
    if str(edit_intensity or "").strip().lower() in {"medium", "high"}:
        filters += ",vignette=PI/5"
    cap = _remix_caption_filter(cues, style=caption_style) if burn_captions else ""
    if cap:
        filters += f",{cap}"
    return f"{filters}[vout]"


async def remix_short_video(
    video_path: str,
    video_id: str,
    out_path: str,
    *,
    cues: list[TranscriptCue] | None = None,
    style_preset: str = "clean_viral",
    caption_style: str = "bold",
    edit_intensity: str = "medium",
    background_mode: str = "blur",
    burn_captions: bool = True,
) -> dict:
    src_w, src_h, _ = probe_video_size(video_path)
    cues = list(cues or [])
    vf = _remix_video_filter(
        src_w=src_w,
        src_h=src_h,
        cues=cues,
        style_preset=style_preset,
        caption_style=caption_style,
        edit_intensity=edit_intensity,
        background_mode=background_mode,
        burn_captions=burn_captions,
    )
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg", "-y", "-i", video_path,
        "-filter_complex", vf,
        "-map", "[vout]",
        "-map", "0:a?",
        "-c:v", "libx264", "-preset", "medium", "-crf", "19",
        "-c:a", "aac", "-b:a", "192k", "-ar", "48000",
        "-movflags", "+faststart",
        "-pix_fmt", "yuv420p",
        "-shortest",
        str(out),
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0 or not out.exists():
        raise RuntimeError(f"Remix render failed: {stderr.decode(errors='ignore')[-500:]}")
    return {
        "path": str(out),
        "filename": out.name,
        "video_id": video_id,
        "style_preset": style_preset,
        "caption_style": caption_style,
        "edit_intensity": edit_intensity,
        "background_mode": background_mode,
        "burn_captions": bool(burn_captions),
        "source_width": src_w,
        "source_height": src_h,
        "caption_cues": len(cues),
    }
