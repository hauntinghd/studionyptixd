"""CrypticScience — Google AI Mode explainer v2 (HQ Pexels + natural ElevenLabs VO).

Run:
  python long_form/build_cryptic_google_ai_mode.py
  python long_form/build_cryptic_google_ai_mode.py --force
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from long_form.cryptic_google_ai_mode_script import SCENES, TITLE
from long_form.pipeline import _download
from long_form.v5_pipeline import _silence_kill, _two_pass_loudnorm

OUT = Path(r"D:/recaps/cryptic_science/google_ai_mode_io2026")
DL = Path.home() / "Downloads"
W, H = 1920, 1080
FONT = "C\\:/Windows/Fonts/segoeuib.ttf"
FONT_REG = "C\\:/Windows/Fonts/segoeui.ttf"

# Daniel — Steady Broadcaster (informative / educational)
VOICE_ID = "onwK4e9ZLuTAKqWW03F9"
VOICE_MODEL = "eleven_multilingual_v2"
VOICE_SPEED = 0.94


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    for p in (Path(r"D:\Games\asd\.env"), ROOT / ".env"):
        if p.exists():
            load_dotenv(p)
            break


def _ffprobe_dur(p: Path) -> float:
    from long_form.pipeline import _ffprobe_dur as fd
    return fd(p)


def _esc(text: str) -> str:
    return text.replace("'", "\u2019").replace(":", r"\:").replace(",", r"\,")


def _pexels_search(query: str, *, page: int = 1) -> list[dict]:
    key = os.environ.get("PEXELS_API_KEY", "").strip()
    if not key:
        raise RuntimeError("PEXELS_API_KEY missing")
    qs = urllib.parse.urlencode({
        "query": query,
        "orientation": "landscape",
        "size": "large",
        "per_page": 20,
        "page": page,
    })
    url = f"https://api.pexels.com/videos/search?{qs}"
    req = urllib.request.Request(
        url,
        headers={"Authorization": key, "User-Agent": "CrypticScience/2.0"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return (json.loads(resp.read().decode()).get("videos") or [])


def _best_file(video: dict, *, min_h: int = 1080) -> dict | None:
    files = [f for f in (video.get("video_files") or []) if "mp4" in str(f.get("file_type", "")).lower()]
    files.sort(key=lambda f: (int(f.get("height") or 0), int(f.get("width") or 0)), reverse=True)
    for f in files:
        if int(f.get("height") or 0) >= min_h:
            return f
    return files[0] if files else None


def _fetch_one(query: str, out: Path, used: set[int]) -> dict:
    for page in (1, 2, 4):
        for vid in _pexels_search(query, page=page):
            vid_id = int(vid.get("id") or 0)
            if vid_id in used or float(vid.get("duration") or 0) < 5:
                continue
            f = _best_file(vid)
            if not f:
                continue
            _download(str(f["link"]), out)
            user = vid.get("user") or {}
            used.add(vid_id)
            return {
                "pexels_id": vid_id,
                "pexels_url": vid.get("url", ""),
                "photographer": user.get("name", "Pexels"),
                "query": query,
                "height": int(f.get("height") or 0),
            }
    raise RuntimeError(f"No 1080p+ clip for: {query}")


def _fetch_scene_clips(scene: dict, clips_dir: Path, used: set[int], force: bool) -> tuple[list[Path], list[dict]]:
    metas: list[dict] = []
    paths: list[Path] = []
    queries = [scene["pexels_query"]]
    if scene.get("pexels_query_b"):
        queries.append(scene["pexels_query_b"])

    for qi, query in enumerate(queries):
        tag = "a" if qi == 0 else "b"
        out = clips_dir / f"{scene['id']}_{tag}_raw.mp4"
        meta_path = clips_dir / f"{scene['id']}_{tag}_pexels.json"
        if not force and out.exists() and meta_path.exists() and out.stat().st_size > 4096:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            used.add(int(meta["pexels_id"]))
            paths.append(out)
            metas.append(meta)
            continue
        meta = _fetch_one(query, out, used)
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        paths.append(out)
        metas.append(meta)
        print(f"  [pexels] {scene['id']}_{tag} id={meta['pexels_id']} {meta['height']}p")
    return paths, metas


def _gen_vo(text: str, out: Path, force: bool) -> None:
    if not force and out.exists() and out.stat().st_size > 1024:
        return
    from skeleton_ai.voice_elevenlabs import ElevenLabsClient

    key = os.environ.get("ELEVENLABS_STUDIO_API_KEY") or os.environ.get("ELEVENLABS_API_KEY")
    el = ElevenLabsClient(api_key=key)
    if out.exists():
        out.unlink()
    el.synthesize(
        text,
        out,
        voice_id=VOICE_ID,
        model_id=VOICE_MODEL,
        speed=VOICE_SPEED,
        stability=0.58,
        similarity_boost=0.82,
        style=0.12,
    )
    print(f"  [vo] {out.name} ({VOICE_MODEL})")


def _overlay_vf(scene: dict, *, num_frames: int) -> str:
    cap = _esc(scene["caption"])
    chapter = _esc(scene.get("chapter", ""))
    src = _esc(scene.get("source_line", ""))
    return (
        f"scale={W}:{H}:force_original_aspect_ratio=increase,"
        f"crop={W}:{H},"
        f"zoompan=z='min(zoom+0.00035,1.06)':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':"
        f"d={num_frames}:s={W}x{H}:fps=30,"
        f"eq=contrast=1.05:brightness=0.02:saturation=1.08,"
        f"drawbox=x=0:y=0:w={W}:h=140:color=0x0A0A0F@0.72:t=fill,"
        f"drawtext=fontfile='{FONT}':text='{cap}':fontsize=58:fontcolor=white:"
        f"bordercolor=black:borderw=3:x=72:y=42,"
        f"drawtext=fontfile='{FONT_REG}':text='{chapter}':fontsize=30:fontcolor=0xB0B8C4:"
        f"x=72:y=98,"
        f"drawbox=x=0:y={H - 80}:w={W}:h=80:color=0x0A0A0F@0.78:t=fill,"
        f"drawtext=fontfile='{FONT_REG}':text='{src}':fontsize=26:fontcolor=0x7EB8FF:"
        f"x=72:y={H - 56}"
    )


def _trim_clip(raw: Path, duration: float, scene: dict, out: Path) -> None:
    target = max(3.0, duration)
    num_frames = max(90, int(target * 30) + 2)
    vf = _overlay_vf(scene, num_frames=num_frames)
    clip_dur = _ffprobe_dur(raw)
    if clip_dur + 0.08 >= target:
        subprocess.run([
            "ffmpeg", "-y", "-loglevel", "error", "-i", str(raw),
            "-vf", vf, "-t", f"{target:.3f}", "-an", "-pix_fmt", "yuv420p",
            "-c:v", "libx264", "-preset", "slow", "-crf", "16",
            str(out),
        ], check=True)
    else:
        pts = target / max(clip_dur, 0.1)
        subprocess.run([
            "ffmpeg", "-y", "-loglevel", "error", "-i", str(raw),
            "-vf", f"{vf},setpts={pts:.6f}*PTS", "-fps_mode", "vfr",
            "-t", f"{target:.3f}", "-an", "-pix_fmt", "yuv420p",
            "-c:v", "libx264", "-preset", "slow", "-crf", "16",
            str(out),
        ], check=True)


def _render_scene(clips: list[Path], scene: dict, duration: float, out: Path) -> None:
    if len(clips) == 1:
        _trim_clip(clips[0], duration, scene, out)
        return
    half = duration / 2.0
    a = out.with_name(out.stem + "_a.mp4")
    b = out.with_name(out.stem + "_b.mp4")
    _trim_clip(clips[0], half + 0.35, scene, a)
    _trim_clip(clips[1], half + 0.35, scene, b)
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", str(a), "-i", str(b),
        "-filter_complex",
        f"[0:v][1:v]xfade=transition=fade:duration=0.5:offset={half:.3f}[v]",
        "-map", "[v]", "-t", f"{duration:.3f}",
        "-c:v", "libx264", "-preset", "slow", "-crf", "16", "-pix_fmt", "yuv420p",
        str(out),
    ], check=True)
    a.unlink(missing_ok=True)
    b.unlink(missing_ok=True)


def _chapter_lines(durations: list[float], labels: list[str]) -> str:
    lines = []
    t = 0.0
    for d, label in zip(durations, labels):
        m, s = int(t // 60), int(t % 60)
        lines.append(f"{m}:{s:02d} — {label}")
        t += d
    return "\n".join(lines)


def _concat_with_crossfade(scene_mp4s: list[Path], durations: list[float], out: Path) -> None:
    if len(scene_mp4s) == 1:
        shutil.copy2(scene_mp4s[0], out)
        return
    fade = 0.4
    inputs: list[str] = []
    for p in scene_mp4s:
        inputs.extend(["-i", str(p)])
    parts = []
    cum = durations[0]
    prev = "[0:v]"
    for i in range(1, len(scene_mp4s)):
        offset = max(0.1, cum - fade)
        nxt = f"[v{i}]" if i < len(scene_mp4s) - 1 else "[vout]"
        parts.append(f"{prev}[{i}:v]xfade=transition=fade:duration={fade}:offset={offset:.3f}{nxt}")
        prev = nxt
        cum += durations[i] - fade
    fc = ";".join(parts)
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error", *inputs,
        "-filter_complex", fc, "-map", "[vout]",
        "-c:v", "libx264", "-preset", "slow", "-crf", "16", "-pix_fmt", "yuv420p",
        str(out),
    ], check=True)


def build(*, force: bool = False) -> Path:
    _load_env()
    OUT.mkdir(parents=True, exist_ok=True)
    clips_dir = OUT / "clips"
    audio = OUT / "audio"
    scenes_dir = OUT / "scenes"
    for d in (clips_dir, audio, scenes_dir):
        d.mkdir(exist_ok=True)

    if force:
        for p in audio.glob("vo_*"):
            p.unlink(missing_ok=True)
        for p in clips_dir.glob("*_raw.mp4"):
            p.unlink(missing_ok=True)

    sources = json.loads(
        (ROOT / "long_form" / "cryptic_google_ai_mode_sources.json").read_text(encoding="utf-8")
    )

    vo_parts: list[Path] = []
    durations: list[float] = []
    used: set[int] = set()
    attributions: list[dict] = []

    for sc in SCENES:
        raw_vo = audio / f"vo_{sc['id']}_raw.mp3"
        clean = audio / f"vo_{sc['id']}_clean.mp3"
        if force and clean.exists():
            clean.unlink()
        _gen_vo(sc["narration"], raw_vo, force)
        _silence_kill(raw_vo, clean)
        dur = _ffprobe_dur(clean)
        vo_parts.append(clean)
        durations.append(dur)
        wc = len(sc["narration"].split())
        print(f"  {sc['id']} vo={dur:.1f}s ({wc} words)")

    scene_mp4s: list[Path] = []
    for i, sc in enumerate(SCENES):
        clip_paths, metas = _fetch_scene_clips(sc, clips_dir, used, force)
        for m in metas:
            attributions.append({"scene": sc["id"], **m})
        scene_v = scenes_dir / f"{sc['id']}.mp4"
        _render_scene(clip_paths, sc, durations[i], scene_v)
        mux = scenes_dir / f"{sc['id']}_mux.mp4"
        subprocess.run([
            "ffmpeg", "-y", "-loglevel", "error",
            "-i", str(scene_v), "-i", str(vo_parts[i]),
            "-map", "0:v", "-map", "1:a",
            "-c:v", "copy", "-c:a", "aac", "-b:a", "256k",
            "-t", f"{durations[i]:.3f}",
            str(mux),
        ], check=True)
        scene_mp4s.append(mux)

    vo_list = audio / "concat.txt"
    vo_list.write_text("\n".join(f"file '{p.as_posix()}'" for p in vo_parts), encoding="utf-8")
    vo_full = audio / "narration_full.mp3"
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-f", "concat", "-safe", "0", "-i", str(vo_list),
        "-c", "copy", str(vo_full),
    ], check=True)
    vo_norm = audio / "narration_loudnorm.mp3"
    if force and vo_norm.exists():
        vo_norm.unlink()
    _two_pass_loudnorm(vo_full, vo_norm)

    silent = OUT / "video_only.mp4"
    _concat_with_crossfade(scene_mp4s, durations, silent)

    final = OUT / "CrypticScience_Google_AI_Mode_Explained_v2.mp4"
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", str(silent), "-i", str(vo_norm),
        "-map", "0:v", "-map", "1:a",
        "-c:v", "libx264", "-preset", "slow", "-crf", "16",
        "-c:a", "aac", "-b:a", "256k", "-shortest",
        "-movflags", "+faststart",
        str(final),
    ], check=True)

    total = sum(durations)
    chapter_labels = [sc.get("chapter", sc["id"]) for sc in SCENES]
    chapters = _chapter_lines(durations, chapter_labels)
    primary_links = "\n".join(f"- {s['title']}: {s['url']}" for s in sources["primary_sources"])
    pexels_credit = "\n".join(
        f"- Video by {a['photographer']} (Pexels): {a['pexels_url']}" for a in attributions
    )
    meta = {
        "title": TITLE,
        "upload_title": "What Google AI Mode Actually Changes (Verified — I/O 2026)",
        "total_sec": total,
        "voice": {"id": VOICE_ID, "name": "Daniel", "model": VOICE_MODEL},
        "quality": "1080p CRF16 slow preset, dual B-roll, crossfades, Ken Burns",
        "est_fal_usd": 0.0,
        "verified_sources": sources,
        "upload_desc": (
            "What Google actually announced for AI Mode at I/O 2026 — live today vs coming this summer. "
            "No hype, no pitch. Primary sources only.\n\n"
            f"PRIMARY SOURCES (May 19, 2026):\n{primary_links}\n\n"
            f"CHAPTERS:\n{chapters}\n\n"
            f"{pexels_credit}\n\nVideos provided by Pexels.\n\n"
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
    shutil.copy2(final, DL / "CrypticScience_Google_AI_Mode_Explained_v2.mp4")
    print(f"\nDONE {final} ({total/60:.1f} min) voice=Daniel -> Downloads")
    return final


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="Regenerate VO and refetch B-roll")
    args = ap.parse_args()
    build(force=args.force)
