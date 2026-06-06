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
            })
        except Exception as exc:
            _log.warning("Clip %s render failed: %s", idx, str(exc)[:200])
            rendered.append({"index": idx, "error": str(exc)[:200]})
    return rendered
