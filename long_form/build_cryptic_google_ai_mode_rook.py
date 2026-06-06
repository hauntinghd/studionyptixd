"""CrypticScience — Rook-style Google AI Mode (avatar + motion graphics + source proof).

Three visual layers:
  1. Aurora talking-head host (~45%)
  2. Motion graphics stats/headlines (~30%)
  3. Verified source proof B-roll — browser citations from Google I/O posts (~25%)

NOT generic stock B-roll. Matches Rook/RookCast format from qxvumPV5ims.

Run:
  python long_form/build_cryptic_google_ai_mode_rook.py
  python long_form/build_cryptic_google_ai_mode_rook.py --force
  python long_form/build_cryptic_google_ai_mode_rook.py --preview   # hook only, test look
  python long_form/build_cryptic_google_ai_mode_rook.py --backend aurora
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import fal_client

from long_form.cryptic_google_ai_mode_rook_beats import (
    AVATAR_MOTION,
    BEATS,
    HOST_IMAGE_PROMPT,
    STABLE_AVATAR_PROMPT,
    TITLE,
)

_AVATAR_CHAPTERS = {
    "01_hook": "Cold open",
    "03_what_is": "What AI Mode is",
    "06_live_today": "Live today",
    "09_agents": "Search agents",
    "12_generative": "Generative UI",
    "14_personal": "Personal Intelligence",
    "17_searchers": "For searchers",
    "19_creators": "For creators",
    "21_limits": "What Google did not say",
    "23_cta": "Close",
}
from long_form.pipeline import SEEDREAM_URL, _download, _fal_post
from long_form.v5_pipeline import _silence_kill, _two_pass_loudnorm

OUT = Path(r"D:/recaps/cryptic_science/google_ai_mode_rook_v3")
DL = Path.home() / "Downloads"
W, H = 1920, 1080

VOICE_ID = "onwK4e9ZLuTAKqWW03F9"
VOICE_MODEL = "eleven_multilingual_v2"
VOICE_SPEED = 0.94

BACKENDS = {
    "aurora": "fal-ai/creatify/aurora",
    "stable": "fal-ai/stable-avatar",
}
# Aurora hard-limits audio to 60s; stable-avatar allows ~5 min per call.
AURORA_MAX_AUDIO_SEC = 55.0
STABLE_MAX_AUDIO_SEC = 280.0


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    for p in (Path(r"D:\Games\asd\.env"), ROOT / ".env"):
        if p.exists():
            load_dotenv(p)
            break


def _ensure_fal() -> None:
    key = os.environ.get("FAL_AI_KEY", "").strip()
    if not key:
        raise RuntimeError("FAL_AI_KEY missing")
    os.environ["FAL_KEY"] = key


def _ffprobe_dur(p: Path) -> float:
    from long_form.pipeline import _ffprobe_dur as fd
    return fd(p)


def _gen_host_still(out: Path, force: bool) -> Path:
    if not force and out.exists() and out.stat().st_size > 4096:
        return out
    data = _fal_post(
        SEEDREAM_URL,
        {
            "prompt": HOST_IMAGE_PROMPT[:3500],
            "negative_prompt": (
                "giant microphone foreground, boom arm blocking face, golden hour side light, "
                "lens flare, bookshelf clutter, cartoon, anime, deformed, ugly, blurry, watermark, "
                "hands visible, wide shot, full body"
            ),
            "image_size": {"width": 1920, "height": 1080},
            "num_images": 1,
            "seed": 881204,
        },
        timeout_s=240,
    )
    url = (data.get("images") or [{}])[0].get("url")
    if not url:
        raise RuntimeError(data)
    _download(url, out)
    print(f"  [host] {out.name} (1920x1080)")
    return out


def _gen_vo(text: str, out: Path, force: bool) -> Path:
    if not force and out.exists() and out.stat().st_size > 1024:
        return out
    from skeleton_ai.voice_elevenlabs import ElevenLabsClient

    key = os.environ.get("ELEVENLABS_STUDIO_API_KEY") or os.environ.get("ELEVENLABS_API_KEY")
    el = ElevenLabsClient(api_key=key)
    if out.exists():
        out.unlink()
    el.synthesize(
        text, out,
        voice_id=VOICE_ID, model_id=VOICE_MODEL, speed=VOICE_SPEED,
        stability=0.58, similarity_boost=0.82, style=0.12,
    )
    return out


def _avatar_arguments(backend: str, host_url: str, audio_url: str) -> dict:
    if backend == "aurora":
        return {
            "image_url": host_url,
            "audio_url": audio_url,
            "prompt": AVATAR_MOTION,
            "resolution": "720p",
            "guidance_scale": 1.0,
            "audio_guidance_scale": 2.5,
        }
    return {
        "image_url": host_url,
        "audio_url": audio_url,
        "prompt": STABLE_AVATAR_PROMPT,
        "aspect_ratio": "16:9",
        "guidance_scale": 5,
        "audio_guidance_scale": 5,
        "num_inference_steps": 50,
        "perturbation": 0.05,
    }


def _gen_static_host_clip(host: Path, duration: float, out: Path) -> None:
    """Ken Burns host still — fallback when fal credits unavailable."""
    frames = max(1, int(duration * 30))
    vf = (
        f"scale={W}:{H}:force_original_aspect_ratio=increase,crop={W}:{H},"
        f"zoompan=z='min(1.0+on*0.00015,1.05)':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':"
        f"d={frames}:s={W}x{H}:fps=30,"
        f"eq=contrast=1.04:saturation=1.06,unsharp=5:5:0.35"
    )
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error", "-loop", "1", "-i", str(host),
        "-vf", vf, "-t", f"{duration:.3f}", "-an",
        "-c:v", "libx264", "-preset", "slow", "-crf", "15", "-pix_fmt", "yuv420p",
        str(out),
    ], check=True)
    print(f"  [avatar] static fallback {out.name} ({duration:.1f}s)")


def _gen_avatar_clip(
    host: Path, audio: Path, out: Path, *, backend: str, force: bool,
) -> Path:
    if not force and out.exists() and out.stat().st_size > 10000:
        return out
    endpoint = BACKENDS[backend]
    img_url = fal_client.upload_file(str(host))
    aud_url = fal_client.upload_file(str(audio))
    print(f"  [avatar] {endpoint} ({_ffprobe_dur(audio):.1f}s audio)...")
    result = fal_client.subscribe(endpoint, arguments=_avatar_arguments(backend, img_url, aud_url))
    vurl = ""
    if isinstance(result, dict):
        vid = result.get("video")
        if isinstance(vid, dict):
            vurl = vid.get("url", "")
        elif isinstance(vid, str):
            vurl = vid
        vurl = vurl or result.get("video_url", "")
    if not vurl:
        raise RuntimeError(result)
    _download(vurl, out)
    print(f"  [avatar] saved {out.name} ({_ffprobe_dur(out):.1f}s)")
    return out


def _slice_avatar(master: Path, start: float, duration: float, out: Path) -> None:
    vf = (
        f"scale={W}:{H}:force_original_aspect_ratio=decrease,"
        f"pad={W}:{H}:(ow-iw)/2:(oh-ih)/2:black,"
        f"eq=contrast=1.04:saturation=1.06,unsharp=5:5:0.4,fps=30"
    )
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-ss", f"{start:.3f}", "-i", str(master),
        "-vf", vf, "-t", f"{duration:.3f}", "-an",
        "-c:v", "libx264", "-preset", "slow", "-crf", "15", "-pix_fmt", "yuv420p",
        str(out),
    ], check=True)


def _render_motion(beat: dict, out: Path, *, force: bool = False) -> Path:
    from long_form.motion_graphics.stat_card import (
        ChecklistCard,
        CompareCard,
        CounterCard,
        NewsCard,
        PercentageCard,
        SourceProofCard,
        TimelineCard,
    )

    args = dict(beat.get("mg_args") or {})
    dur = float(beat.get("duration_sec") or 8.0)
    kind = beat.get("mg", "counter")
    card_map = {
        "counter": CounterCard,
        "news": NewsCard,
        "percentage": PercentageCard,
        "timeline": TimelineCard,
        "checklist": ChecklistCard,
        "compare": CompareCard,
        "source_proof": SourceProofCard,
    }
    cls = card_map.get(kind)
    if cls is None:
        raise ValueError(kind)
    if not force and out.exists() and out.stat().st_size > 10000:
        print(f"  [motion] reuse {out.name}")
        return out
    card = cls(duration_sec=dur, width=W, height=H, **args)
    card.render(out, crf=15)
    print(f"  [motion] {out.name} ({dur}s - {kind})")
    return out


def _fit_video(in_mp4: Path, duration: float, out: Path) -> None:
    clip = _ffprobe_dur(in_mp4)
    vf = f"scale={W}:{H}:force_original_aspect_ratio=decrease,pad={W}:{H}:(ow-iw)/2:(oh-ih)/2:black,fps=30"
    target = max(1.0, duration)
    if clip + 0.05 >= target:
        subprocess.run([
            "ffmpeg", "-y", "-loglevel", "error", "-i", str(in_mp4),
            "-vf", vf, "-t", f"{target:.3f}", "-an",
            "-c:v", "libx264", "-preset", "slow", "-crf", "16", "-pix_fmt", "yuv420p",
            str(out),
        ], check=True)
    else:
        pts = target / max(clip, 0.1)
        subprocess.run([
            "ffmpeg", "-y", "-loglevel", "error", "-i", str(in_mp4),
            "-vf", f"{vf},setpts={pts:.6f}*PTS", "-fps_mode", "vfr",
            "-t", f"{target:.3f}", "-an",
            "-c:v", "libx264", "-preset", "slow", "-crf", "16", "-pix_fmt", "yuv420p",
            str(out),
        ], check=True)


def _mux(va: Path, au: Path, out: Path, duration: float) -> None:
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", str(va), "-i", str(au),
        "-map", "0:v", "-map", "1:a",
        "-c:v", "copy", "-c:a", "aac", "-b:a", "256k",
        "-t", f"{duration:.3f}",
        str(out),
    ], check=True)


def _concat_audio(paths: list[Path], out: Path) -> None:
    if len(paths) == 1:
        shutil.copy2(paths[0], out)
        return
    inputs: list[str] = []
    for p in paths:
        inputs.extend(["-i", str(p)])
    n = len(paths)
    filt = "".join(f"[{i}:a]" for i in range(n)) + f"concat=n={n}:v=0:a=1[outa]"
    subprocess.run(
        ["ffmpeg", "-y", "-loglevel", "error", *inputs,
         "-filter_complex", filt, "-map", "[outa]",
         "-c:a", "libmp3lame", "-b:a", "192k", str(out)],
        check=True,
    )


def _is_static_fallback_part(path: Path) -> bool:
    """Ken Burns fallback parts are 1080p video-only; Aurora parts include audio."""
    if not path.exists() or path.stat().st_size < 10_000:
        return False
    probe = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "stream=codec_type,width",
         "-of", "json", str(path)],
        capture_output=True, text=True, check=True,
    )
    data = json.loads(probe.stdout)
    streams = data.get("streams") or []
    has_audio = any(s.get("codec_type") == "audio" for s in streams)
    if has_audio:
        return False
    for s in streams:
        if s.get("codec_type") == "video" and int(s.get("width") or 0) >= 1920:
            return True
    return False


def _scan_static_fallback_parts(chunk_dir: Path) -> list[int]:
    out: list[int] = []
    for part in sorted(chunk_dir.glob("part_*.mp4")):
        idx = int(part.stem.split("_")[1])
        if _is_static_fallback_part(part):
            out.append(idx)
    return out


def _chapter_lines_from_timeline(beats: list[dict], timeline_durs: list[float]) -> str:
    lines: list[str] = []
    t = 0.0
    for beat, dur in zip(beats, timeline_durs):
        bid = beat["id"]
        if beat["type"] == "avatar" and bid in _AVATAR_CHAPTERS:
            m, s = int(t // 60), int(t % 60)
            lines.append(f"{m}:{s:02d} — {_AVATAR_CHAPTERS[bid]}")
        t += dur
    return "\n".join(lines)


def _write_upload_pack(
    meta: dict,
    *,
    beats: list[dict],
    timeline_durs: list[float],
) -> None:
    sources_path = ROOT / "long_form" / "cryptic_google_ai_mode_sources.json"
    sources = json.loads(sources_path.read_text(encoding="utf-8"))
    primary_links = "\n".join(f"- {s['title']}: {s['url']}" for s in sources["primary_sources"])
    chapters = _chapter_lines_from_timeline(beats, timeline_durs)
    meta = {
        **meta,
        "upload_title": TITLE,
        "voice": {"id": VOICE_ID, "name": "Daniel", "model": VOICE_MODEL},
        "verified_sources": sources,
        "upload_desc": (
            "What Google actually announced for AI Mode at I/O 2026 — live today vs coming this summer. "
            "No hype, no pitch. Primary sources only.\n\n"
            f"PRIMARY SOURCES (May 19, 2026):\n{primary_links}\n\n"
            f"CHAPTERS:\n{chapters}\n\n"
            "Format: verified explainer with on-screen source citations.\n\n"
            "Comment what to explain next.\n#Google #AIMode #GoogleIO2026 #Search #TechNews"
        ),
        "upload_tags": (
            "Google AI Mode, Google I/O 2026, AI search, Google Search changes, "
            "Gemini 3.5 Flash, AI Overviews, information agents, Google Antigravity, "
            "tech explainer, verified news, CrypticScience"
        ),
        "pinned_comment": (
            "What should we break down next — AI agents, generative UI in Search, "
            "or what this means for creators? Top comment picks the next video."
        ),
        "category": "Science & Technology",
    }
    (OUT / "upload_pack.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")


def _repair_avatar_chunks(
    host: Path,
    master_audio: Path,
    master_raw: Path,
    *,
    chunk_indices: list[int],
    backend: str,
) -> tuple[Path, list[str]]:
    """Re-render specific Aurora chunks, restitch master, refresh affected avatar scenes."""
    max_sec = AURORA_MAX_AUDIO_SEC if backend == "aurora" else STABLE_MAX_AUDIO_SEC
    chunk_dir = master_raw.parent / f"{master_raw.stem}_chunks"
    chunks = _split_audio_chunks(master_audio, chunk_dir, max_sec)
    parts: list[Path] = []
    repaired: list[str] = []

    for i, chunk in enumerate(chunks):
        part = chunk_dir / f"part_{i:02d}.mp4"
        if i in chunk_indices:
            part.unlink(missing_ok=True)
            print(f"  [repair] re-rendering chunk {i} ({_ffprobe_dur(chunk):.1f}s)...")
            _gen_avatar_clip(host, chunk, part, backend=backend, force=True)
            repaired.append(part.name)
        elif part.exists() and part.stat().st_size > 500_000:
            print(f"  [repair] keep {part.name} ({_ffprobe_dur(part):.1f}s)")
        else:
            raise FileNotFoundError(f"Missing avatar part {part} — run full build or repair that chunk")
        parts.append(part)

    if master_raw.exists():
        master_raw.unlink()
    _concat_videos(parts, master_raw)
    print(f"  [repair] master restitched {master_raw.name} ({_ffprobe_dur(master_raw):.1f}s)")

    audio_dir = OUT / "audio"
    scenes = OUT / "scenes"
    clips = OUT / "clips"
    avatar_clean, _, avatar_offsets = _prepare_avatar_audio(BEATS, audio_dir, force=False)
    chunk_starts = [i * max_sec for i in chunk_indices]
    chunk_end = max(chunk_starts) + max_sec + 1.0 if chunk_starts else 0.0
    chunk_start = min(chunk_starts) if chunk_starts else 0.0

    avatar_idx = 0
    for beat in BEATS:
        if beat["type"] != "avatar":
            continue
        bid = beat["id"]
        clean = avatar_clean[avatar_idx]
        dur = _ffprobe_dur(clean)
        offset = avatar_offsets[bid]
        avatar_idx += 1
        if offset + dur <= chunk_start or offset >= chunk_end:
            continue
        mux = scenes / f"{bid}.mp4"
        av_fit = clips / f"{bid}_avatar_fit.mp4"
        print(f"  [repair] re-slice {bid} @ {offset:.1f}s ({dur:.1f}s)")
        _slice_avatar(master_raw, offset, dur, av_fit)
        _mux(av_fit, clean, mux, dur)

    return master_raw, repaired


def _split_audio_chunks(src: Path, out_dir: Path, max_sec: float) -> list[Path]:
    """Split long narration into fal-safe chunks (Aurora max 60s)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    total = _ffprobe_dur(src)
    chunks: list[Path] = []
    t = 0.0
    i = 0
    while t < total - 0.05:
        dur = min(max_sec, total - t)
        out = out_dir / f"chunk_{i:02d}.mp3"
        subprocess.run([
            "ffmpeg", "-y", "-loglevel", "error",
            "-ss", f"{t:.3f}", "-i", str(src),
            "-t", f"{dur:.3f}",
            "-c:a", "libmp3lame", "-b:a", "192k", str(out),
        ], check=True)
        chunks.append(out)
        t += dur
        i += 1
    return chunks


def _concat_timeline_video(parts: list[Path], durs: list[float], out: Path) -> None:
    """Re-encode timeline concat with exact per-clip durations (avoids -c copy drift)."""
    if len(parts) != len(durs):
        raise ValueError("parts/durs length mismatch")
    if len(parts) == 1:
        subprocess.run([
            "ffmpeg", "-y", "-loglevel", "error",
            "-i", str(parts[0]),
            "-t", f"{durs[0]:.3f}",
            "-vf", (
                f"scale={W}:{H}:force_original_aspect_ratio=decrease,"
                f"pad={W}:{H}:(ow-iw)/2:(oh-ih)/2:black,fps=30"
            ),
            "-an", "-c:v", "libx264", "-preset", "fast", "-crf", "16", "-pix_fmt", "yuv420p",
            str(out),
        ], check=True)
        return
    inputs: list[str] = []
    for p in parts:
        inputs.extend(["-i", str(p)])
    n = len(parts)
    scale_parts = "".join(
        f"[{i}:v:0]trim=duration={durs[i]:.3f},setpts=PTS-STARTPTS,"
        f"scale={W}:{H}:force_original_aspect_ratio=decrease,"
        f"pad={W}:{H}:(ow-iw)/2:(oh-ih)/2:black,fps=30,setsar=1[v{i}];"
        for i in range(n)
    )
    concat_in = "".join(f"[v{i}]" for i in range(n))
    filt = scale_parts + f"{concat_in}concat=n={n}:v=1:a=0[outv]"
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error", *inputs,
        "-filter_complex", filt, "-map", "[outv]",
        "-c:v", "libx264", "-preset", "fast", "-crf", "16", "-pix_fmt", "yuv420p",
        str(out),
    ], check=True)


def _assemble_final(
    *,
    scene_paths: list[Path],
    audio_paths: list[Path],
    timeline_durs: list[float],
    preview: bool,
    force: bool,
) -> Path:
    """Mux existing scene clips + audio into final MP4 (no fal spend)."""
    audio_dir = OUT / "audio"
    vlist = OUT / "concat_v.txt"
    vlist.write_text("\n".join(f"file '{p.as_posix()}'" for p in scene_paths), encoding="utf-8")
    video_only = OUT / ("preview_video.mp4" if preview else "video_only.mp4")
    if force and video_only.exists():
        video_only.unlink()
    _concat_timeline_video(scene_paths, timeline_durs, video_only)
    v_dur = _ffprobe_dur(video_only)
    print(f"  [video] {video_only.name} ({v_dur:.1f}s)")

    alist = OUT / "concat_a.txt"
    alist.write_text("\n".join(f"file '{p.as_posix()}'" for p in audio_paths), encoding="utf-8")
    vo_full = audio_dir / ("preview_full.mp3" if preview else "full.mp3")
    if force and vo_full.exists():
        vo_full.unlink()
    _concat_audio(audio_paths, vo_full)
    a_dur = _ffprobe_dur(vo_full)
    print(f"  [audio] {vo_full.name} ({a_dur:.1f}s)")

    vo_norm = audio_dir / ("preview_loudnorm.mp3" if preview else "full_loudnorm.mp3")
    if force and vo_norm.exists():
        vo_norm.unlink()
    _two_pass_loudnorm(vo_full, vo_norm)
    a_norm_dur = _ffprobe_dur(vo_norm)
    print(f"  [loudnorm] {vo_norm.name} ({a_norm_dur:.1f}s)")

    final_name = "CrypticScience_Google_AI_Mode_Rook_PREVIEW.mp4" if preview else "CrypticScience_Google_AI_Mode_Rook_v3.mp4"
    final = OUT / final_name
    if abs(v_dur - a_norm_dur) > 0.5:
        print(f"  [warn] A/V delta {abs(v_dur - a_norm_dur):.1f}s — padding shorter stream")
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", str(video_only), "-i", str(vo_norm),
        "-map", "0:v", "-map", "1:a",
        "-c:v", "libx264", "-preset", "slow", "-crf", "16",
        "-c:a", "aac", "-b:a", "256k",
        "-shortest",
        "-movflags", "+faststart",
        str(final),
    ], check=True)
    final_dur = _ffprobe_dur(final)
    print(f"  [final] {final.name} ({final_dur:.1f}s)")
    return final


def _concat_videos(paths: list[Path], out: Path) -> None:
    """Concat avatar parts — video-only re-encode for clean timing."""
    if len(paths) == 1:
        shutil.copy2(paths[0], out)
        return
    inputs: list[str] = []
    for p in paths:
        inputs.extend(["-i", str(p)])
    n = len(paths)
    scale_parts = "".join(
        f"[{i}:v:0]scale={W}:{H}:force_original_aspect_ratio=decrease,"
        f"pad={W}:{H}:(ow-iw)/2:(oh-ih)/2,fps=30,setsar=1[v{i}];"
        for i in range(n)
    )
    concat_in = "".join(f"[v{i}]" for i in range(n))
    filt = scale_parts + f"{concat_in}concat=n={n}:v=1:a=0[outv]"
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error", *inputs,
        "-filter_complex", filt, "-map", "[outv]",
        "-c:v", "libx264", "-preset", "fast", "-crf", "16", "-pix_fmt", "yuv420p",
        str(out),
    ], check=True)


def _gen_avatar_master(
    host: Path, master_audio: Path, out: Path, *, backend: str, force: bool,
    static_fallback: bool = False,
) -> Path:
    audio_dur = _ffprobe_dur(master_audio)
    if out.exists() and out.stat().st_size > 50000 and not force:
        vid_dur = _ffprobe_dur(out)
        if abs(vid_dur - audio_dur) < 3.0:
            print(f"  [avatar] reuse master {out.name} ({vid_dur:.1f}s)")
            return out
        print(f"  [avatar] stale master ({vid_dur:.1f}s vs audio {audio_dur:.1f}s) - regenerating")
        out.unlink(missing_ok=True)

    max_sec = AURORA_MAX_AUDIO_SEC if backend == "aurora" else STABLE_MAX_AUDIO_SEC
    chunk_dir = out.parent / f"{out.stem}_chunks"

    if audio_dur <= max_sec + 0.5:
        return _gen_avatar_clip(host, master_audio, out, backend=backend, force=force)

    chunks = _split_audio_chunks(master_audio, chunk_dir, max_sec)
    print(f"  [avatar] {audio_dur:.1f}s -> {len(chunks)} chunks (max {max_sec:.0f}s each)")
    parts: list[Path] = []
    for i, chunk in enumerate(chunks):
        part = chunk_dir / f"part_{i:02d}.mp4"
        if part.exists() and part.stat().st_size > 500_000 and not force:
            print(f"  [avatar] reuse {part.name} ({_ffprobe_dur(part):.1f}s)")
            parts.append(part)
            continue
        if force and part.exists():
            part.unlink()
        if static_fallback:
            _gen_static_host_clip(host, _ffprobe_dur(chunk), part)
        else:
            try:
                _gen_avatar_clip(host, chunk, part, backend=backend, force=force)
            except Exception as exc:
                print(f"  [avatar] fal failed ({exc}) - static fallback")
                _gen_static_host_clip(host, _ffprobe_dur(chunk), part)
        parts.append(part)
    _concat_videos(parts, out)
    print(f"  [avatar] master stitched {out.name} ({_ffprobe_dur(out):.1f}s)")
    return out


def _prepare_avatar_audio(
    beats: list[dict], audio_dir: Path, *, force: bool,
) -> tuple[list[Path], list[float], dict[str, float]]:
    """Return per-beat clean audio, durations, and avatar offset in master track."""
    clean_paths: list[Path] = []
    durations: list[float] = []
    avatar_offsets: dict[str, float] = {}
    master_t = 0.0

    for beat in beats:
        bid = beat["id"]
        if beat["type"] == "avatar":
            raw = audio_dir / f"vo_{bid}_raw.mp3"
            clean = audio_dir / f"vo_{bid}_clean.mp3"
            if force:
                clean.unlink(missing_ok=True)
            _gen_vo(beat["narration"], raw, force)
            _silence_kill(raw, clean)
            dur = _ffprobe_dur(clean)
            avatar_offsets[bid] = master_t
            master_t += dur
            clean_paths.append(clean)
            durations.append(dur)
            print(f"  {bid} avatar vo={dur:.1f}s (master@{avatar_offsets[bid]:.1f}s)")
        else:
            dur = float(beat.get("duration_sec") or 6.0)
            durations.append(dur)

    return clean_paths, durations, avatar_offsets


def _collect_timeline(beats: list[dict], audio: Path, scenes: Path, clips: Path) -> tuple[list[Path], list[Path], list[float]]:
    """Gather existing scene clips + audio for assemble-only (no fal)."""
    scene_paths: list[Path] = []
    audio_paths: list[Path] = []
    timeline_durs: list[float] = []
    avatar_clean, _, _ = _prepare_avatar_audio(beats, audio, force=False)
    avatar_idx = 0
    for beat in beats:
        bid = beat["id"]
        if beat["type"] == "avatar":
            clean = avatar_clean[avatar_idx]
            dur = _ffprobe_dur(clean)
            avatar_idx += 1
            mux = scenes / f"{bid}.mp4"
            if not mux.exists():
                raise FileNotFoundError(f"Missing avatar scene {mux}")
            scene_paths.append(mux)
            audio_paths.append(clean)
            timeline_durs.append(dur)
        else:
            dur = float(beat.get("duration_sec") or 6.0)
            silent = clips / f"{bid}_silent.mp4"
            if not silent.exists():
                raise FileNotFoundError(f"Missing motion clip {silent}")
            sil = audio / f"sil_{bid}.mp3"
            if not sil.exists():
                subprocess.run([
                    "ffmpeg", "-y", "-loglevel", "error",
                    "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo",
                    "-t", f"{dur:.3f}", "-c:a", "libmp3lame", "-b:a", "192k",
                    str(sil),
                ], check=True)
            scene_paths.append(silent)
            audio_paths.append(sil)
            timeline_durs.append(dur)
    return scene_paths, audio_paths, timeline_durs


def build(
    *,
    force: bool = False,
    preview: bool = False,
    backend: str = "aurora",
    static_fallback: bool = False,
    assemble_only: bool = False,
    repair_chunks: list[int] | None = None,
) -> Path:
    if backend not in BACKENDS:
        raise ValueError(f"backend must be one of {list(BACKENDS)}")

    _load_env()
    beats = BEATS[:2] if preview else BEATS

    OUT.mkdir(parents=True, exist_ok=True)
    assets = OUT / "assets"
    audio = OUT / "audio"
    clips = OUT / "clips"
    scenes = OUT / "scenes"
    for d in (assets, audio, clips, scenes):
        d.mkdir(exist_ok=True)

    if assemble_only:
        print("  [mode] assemble-only — re-muxing from cached scenes (no fal spend)")
        scene_paths, audio_paths, timeline_durs = _collect_timeline(beats, audio, scenes, clips)
        chunk_dir = clips / "avatar_master_aurora_chunks"
        static_parts = _scan_static_fallback_parts(chunk_dir) if chunk_dir.exists() else []
        if static_parts:
            print(f"  [warn] static fallback chunks still present: {static_parts}")
        final = _assemble_final(
            scene_paths=scene_paths,
            audio_paths=audio_paths,
            timeline_durs=timeline_durs,
            preview=preview,
            force=True,
        )
        total = sum(timeline_durs)
        master_dur = _ffprobe_dur(audio / "avatar_master.mp3") if (audio / "avatar_master.mp3").exists() else total
        meta = {
            "title": TITLE,
            "format": "rook_avatar_motion_graphics_source_proof",
            "reference": "https://www.youtube.com/watch?v=qxvumPV5ims",
            "avatar_backend": backend,
            "static_fallback_used": bool(static_parts),
            "static_fallback_chunks": static_parts,
            "total_sec": _ffprobe_dur(final),
            "timeline_sec": total,
            "avatar_master_sec": master_dur,
            "beats": len(beats),
            "voice": VOICE_ID,
            "assemble_only": True,
            "preview": preview,
        }
        _write_upload_pack(meta, beats=beats, timeline_durs=timeline_durs)
        final_name = final.name
        shutil.copy2(final, DL / final_name)
        print(f"\nDONE {final} ({_ffprobe_dur(final)/60:.1f} min, $0 fal) -> Downloads")
        return final

    if repair_chunks is not None:
        _ensure_fal()
        host_path = assets / "host_v2.png"
        if not host_path.exists():
            host_path = assets / "host.png"
        master_audio = audio / "avatar_master.mp3"
        if not master_audio.exists():
            raise FileNotFoundError("avatar_master.mp3 missing — run full build first")
        master_raw = clips / f"avatar_master_{backend}.mp4"
        _, repaired = _repair_avatar_chunks(
            host_path, master_audio, master_raw,
            chunk_indices=repair_chunks, backend=backend,
        )
        print(f"  [repair] done: {', '.join(repaired)}")
        return build(assemble_only=True, backend=backend, preview=preview)

    if not static_fallback:
        _ensure_fal()

    host_path = assets / "host_v2.png"
    if not host_path.exists():
        host_path = assets / "host.png"
    host = _gen_host_still(host_path, force and not host_path.exists())

    avatar_clean, _, avatar_offsets = _prepare_avatar_audio(beats, audio, force=force)
    master_audio = audio / "avatar_master.mp3"
    if force and master_audio.exists():
        master_audio.unlink()
    _concat_audio(avatar_clean, master_audio)
    master_dur = _ffprobe_dur(master_audio)
    print(f"  [master] {len(avatar_clean)} avatar segments, {master_dur:.1f}s total")

    master_raw = clips / f"avatar_master_{backend}.mp4"
    _gen_avatar_master(
        host, master_audio, master_raw, backend=backend, force=force,
        static_fallback=static_fallback,
    )

    scene_paths: list[Path] = []
    audio_paths: list[Path] = []
    timeline_durs: list[float] = []
    avatar_idx = 0

    for beat in beats:
        bid = beat["id"]
        if beat["type"] == "avatar":
            clean = avatar_clean[avatar_idx]
            dur = _ffprobe_dur(clean)
            avatar_idx += 1
            mux = scenes / f"{bid}.mp4"
            if not force and mux.exists() and mux.stat().st_size > 500_000:
                print(f"  [scene] reuse {mux.name}")
            else:
                offset = avatar_offsets[bid]
                av_fit = clips / f"{bid}_avatar_fit.mp4"
                _slice_avatar(master_raw, offset, dur, av_fit)
                _mux(av_fit, clean, mux, dur)
            scene_paths.append(mux)
            audio_paths.append(clean)
            timeline_durs.append(dur)
        else:
            dur = float(beat.get("duration_sec") or 6.0)
            silent = clips / f"{bid}_silent.mp4"
            if not force and silent.exists() and silent.stat().st_size > 10_000:
                print(f"  [scene] reuse {silent.name}")
            else:
                mg = clips / f"{bid}_mg.mp4"
                _render_motion(beat, mg, force=force)
                _fit_video(mg, dur, silent)
            scene_paths.append(silent)
            sil = audio / f"sil_{bid}.mp3"
            subprocess.run([
                "ffmpeg", "-y", "-loglevel", "error",
                "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo",
                "-t", f"{dur:.3f}", "-c:a", "libmp3lame", "-b:a", "192k",
                str(sil),
            ], check=True)
            audio_paths.append(sil)
            timeline_durs.append(dur)

    final = _assemble_final(
        scene_paths=scene_paths,
        audio_paths=audio_paths,
        timeline_durs=timeline_durs,
        preview=preview,
        force=force,
    )

    total = _ffprobe_dur(final)
    est_usd = round(master_dur * (0.14 if backend == "aurora" else 0.10), 2)
    chunk_dir = clips / f"avatar_master_{backend}_chunks"
    static_parts = _scan_static_fallback_parts(chunk_dir) if chunk_dir.exists() else []
    meta = {
        "title": TITLE,
        "format": "rook_avatar_motion_graphics_source_proof",
        "reference": "https://www.youtube.com/watch?v=qxvumPV5ims",
        "avatar_backend": backend,
        "static_fallback_used": static_fallback or bool(static_parts),
        "static_fallback_chunks": static_parts,
        "total_sec": total,
        "timeline_sec": sum(timeline_durs),
        "avatar_master_sec": master_dur,
        "beats": len(beats),
        "voice": VOICE_ID,
        "est_fal_avatar_usd": est_usd,
        "preview": preview,
    }
    _write_upload_pack(meta, beats=beats, timeline_durs=timeline_durs)
    shutil.copy2(final, DL / final.name)
    print(f"\nDONE {final} ({total/60:.1f} min, ~${est_usd} avatar) -> Downloads")
    return final


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--preview", action="store_true", help="Hook + first stat card only")
    ap.add_argument("--backend", choices=list(BACKENDS), default="aurora")
    ap.add_argument(
        "--static-fallback",
        action="store_true",
        help="Use Ken Burns host still for missing fal chunks (no fal credits needed)",
    )
    ap.add_argument(
        "--assemble-only",
        action="store_true",
        help="Re-mux final from cached scenes/clips only (no fal spend)",
    )
    ap.add_argument(
        "--repair-chunks",
        type=str,
        default="",
        help="Comma-separated Aurora chunk indices to re-render (e.g. 7,8), then re-assemble",
    )
    args = ap.parse_args()
    repair = [int(x.strip()) for x in args.repair_chunks.split(",") if x.strip()]
    build(force=args.force, preview=args.preview, backend=args.backend,
          static_fallback=args.static_fallback, assemble_only=args.assemble_only,
          repair_chunks=repair if repair else None)
