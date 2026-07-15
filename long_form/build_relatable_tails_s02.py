"""RelatableTails S02 — decision-maker Short with real Pexels cat B-roll ($0 fal).

Run:
  python long_form/build_relatable_tails_s02.py
"""
from __future__ import annotations

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

from long_form.pipeline import _download
from long_form.v5_pipeline import _silence_kill, _two_pass_loudnorm

OUT = Path(r"D:/recaps/relatable_tails/s02_cat_love_pexels")
DL = Path.home() / "Downloads"
TITLE = "Does Your Cat Love You — Or Just The Food?"
SERIES = "RelatableTails Verdict #1"

SCENES = [
    {
        "id": "01_hook",
        "caption": "does your cat love you?",
        "narration": "Does your cat love you — or just the person who feeds them?",
        "pexels_query": "cat staring camera close up",
        "overlay": None,
    },
    {
        "id": "02_verdict",
        "caption": "the decision maker says...",
        "narration": (
            "The decision maker says: if they only cuddle when the bowl is empty — "
            "that's not love. That's a transaction."
        ),
        "pexels_query": "cat eating food bowl",
        "overlay": "decision_no",
    },
    {
        "id": "03_cta",
        "caption": "comment your cat's name",
        "narration": (
            "Comment your cat's name below. We'll decide if they love you next. "
            "Follow RelatableTails for verdict number two."
        ),
        "pexels_query": "cat paw cute",
        "overlay": None,
    },
]

FONT = "C\\:/Windows/Fonts/arialbd.ttf"


def _require_https_url(url: str, *, expected_host: str | None = None) -> str:
    value = str(url or "").strip()
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme.lower() != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("refusing non-HTTPS or malformed URL")
    if expected_host and parsed.hostname.lower() != expected_host.lower():
        raise ValueError(f"refusing unexpected URL host: {parsed.hostname}")
    return value


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


def _pexels_search(query: str, *, page: int = 1) -> list[dict]:
    key = os.environ.get("PEXELS_API_KEY", "").strip()
    if not key:
        raise RuntimeError("PEXELS_API_KEY missing")
    qs = urllib.parse.urlencode({
        "query": query,
        "orientation": "portrait",
        "size": "medium",
        "per_page": 15,
        "page": page,
    })
    url = _require_https_url(
        f"https://api.pexels.com/videos/search?{qs}",
        expected_host="api.pexels.com",
    )
    req = urllib.request.Request(
        url,
        headers={"Authorization": key, "User-Agent": "RelatableTails/1.0 (NYPTID Studio)"},
    )
    # The request URL is HTTPS and host-validated above.
    with urllib.request.urlopen(req, timeout=60) as resp:  # nosec B310
        data = json.loads(resp.read().decode())
    return data.get("videos") or []


def _pick_mp4_link(video: dict) -> str:
    files = video.get("video_files") or []
    mp4s = [f for f in files if "mp4" in str(f.get("file_type", "")).lower()]
    mp4s.sort(key=lambda f: (int(f.get("height") or 0), int(f.get("width") or 0)), reverse=True)
    if not mp4s:
        raise RuntimeError(f"No mp4 files for pexels video {video.get('id')}")
    return str(mp4s[0]["link"])


def _fetch_pexels_clip(scene: dict, clips_dir: Path, used_ids: set[int]) -> tuple[Path, dict]:
    out = clips_dir / f"{scene['id']}_raw.mp4"
    meta_path = clips_dir / f"{scene['id']}_pexels.json"
    if out.exists() and meta_path.exists() and out.stat().st_size > 4096:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        used_ids.add(int(meta["pexels_id"]))
        return out, meta

    for page in (1, 2, 3):
        videos = _pexels_search(scene["pexels_query"], page=page)
        for vid in videos:
            vid_id = int(vid.get("id") or 0)
            if vid_id in used_ids:
                continue
            dur = float(vid.get("duration") or 0)
            if dur < 3:
                continue
            link = _pick_mp4_link(vid)
            _download(link, out)
            user = vid.get("user") or {}
            meta = {
                "pexels_id": vid_id,
                "pexels_url": vid.get("url", ""),
                "photographer": user.get("name", "Pexels Contributor"),
                "photographer_url": user.get("url", ""),
                "query": scene["pexels_query"],
            }
            meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
            used_ids.add(vid_id)
            print(f"  [pexels] {scene['id']} id={vid_id} by {meta['photographer']!a}")
            return out, meta
    raise RuntimeError(f"No unused Pexels clip for query: {scene['pexels_query']}")


def _gen_vo(text: str, out: Path) -> Path:
    if out.exists() and out.stat().st_size > 1024:
        return out
    from skeleton_ai.voice_elevenlabs import ElevenLabsClient

    key = os.environ.get("ELEVENLABS_STUDIO_API_KEY") or os.environ.get("ELEVENLABS_API_KEY")
    el = ElevenLabsClient(api_key=key)
    el.synthesize(text, out, voice_id="nPczCjzI2devNBz1zQrb", speed=1.05)
    print(f"  [vo] {out.name}")
    return out


def _esc(text: str) -> str:
    return text.replace("'", "\u2019").replace(":", r"\:").replace(",", r"\,")


def _overlay_filters(caption: str, overlay: str | None) -> str:
    cap_fs = 64 if len(caption) <= 22 else 48
    parts = [
        "scale=720:1280:force_original_aspect_ratio=increase",
        "crop=720:1280",
        "fps=30",
        f"drawtext=fontfile='{FONT}':text='{_esc(SERIES)}':fontsize=28:fontcolor=white:"
        f"bordercolor=black:borderw=3:x=w-text_w-24:y=24",
        f"drawtext=fontfile='{FONT}':text='{_esc(caption)}':fontsize={cap_fs}:fontcolor=white:"
        f"bordercolor=black:borderw=5:x=(w-text_w)/2:y=h*0.08",
    ]
    if overlay == "decision_no":
        parts += [
            f"drawbox=x=60:y=420:w=600:h=420:color=black@0.55:t=fill",
            f"drawtext=fontfile='{FONT}':text='DECISION MAKER':fontsize=36:fontcolor=white:"
            f"bordercolor=black:borderw=3:x=(w-text_w)/2:y=460",
            f"drawtext=fontfile='{FONT}':text='NO':fontsize=120:fontcolor=0xFF4444:"
            f"bordercolor=black:borderw=8:x=(w-text_w)/2:y=560",
            f"drawtext=fontfile='{FONT}':text='(its complicated)':fontsize=28:fontcolor=0xCCCCCC:"
            f"bordercolor=black:borderw=2:x=(w-text_w)/2:y=720",
        ]
    return ",".join(parts)


def _fit_clip(raw: Path, duration: float, caption: str, overlay: str | None, out: Path) -> None:
    clip_dur = _ffprobe_dur(raw)
    vf = _overlay_filters(caption, overlay)
    target = max(2.0, duration)
    if clip_dur + 0.05 >= target:
        subprocess.run([
            "ffmpeg", "-y", "-loglevel", "error", "-ss", "0", "-i", str(raw),
            "-t", f"{target:.3f}", "-vf", vf, "-an",
            "-c:v", "libx264", "-preset", "medium", "-crf", "19", "-pix_fmt", "yuv420p",
            str(out),
        ], check=True)
    else:
        pts = target / max(clip_dur, 0.1)
        subprocess.run([
            "ffmpeg", "-y", "-loglevel", "error", "-i", str(raw),
            "-vf", f"{vf},setpts={pts:.6f}*PTS", "-fps_mode", "vfr",
            "-t", f"{target:.3f}", "-an",
            "-c:v", "libx264", "-preset", "medium", "-crf", "19", "-pix_fmt", "yuv420p",
            str(out),
        ], check=True)


def build() -> Path:
    _load_env()
    OUT.mkdir(parents=True, exist_ok=True)
    clips_dir = OUT / "clips"
    audio = OUT / "audio"
    scenes_dir = OUT / "scenes"
    for d in (clips_dir, audio, scenes_dir):
        d.mkdir(exist_ok=True)

    vo_parts: list[Path] = []
    durations: list[float] = []
    attributions: list[dict] = []
    used_pexels: set[int] = set()

    for sc in SCENES:
        raw = audio / f"vo_{sc['id']}_raw.mp3"
        clean = audio / f"vo_{sc['id']}_clean.mp3"
        _gen_vo(sc["narration"], raw)
        _silence_kill(raw, clean)
        dur = _ffprobe_dur(clean)
        vo_parts.append(clean)
        durations.append(dur)
        print(f"  beat {sc['id']} vo={dur:.2f}s")

    scene_mp4s: list[Path] = []
    for sc in SCENES:
        raw_clip, pex_meta = _fetch_pexels_clip(sc, clips_dir, used_pexels)
        attributions.append({"scene": sc["id"], **pex_meta})
        scene_v = scenes_dir / f"{sc['id']}.mp4"
        _fit_clip(raw_clip, durations[len(scene_mp4s)], sc["caption"], sc.get("overlay"), scene_v)
        muxed = scenes_dir / f"{sc['id']}_mux.mp4"
        subprocess.run([
            "ffmpeg", "-y", "-loglevel", "error",
            "-i", str(scene_v), "-i", str(vo_parts[len(scene_mp4s)]),
            "-map", "0:v", "-map", "1:a",
            "-c:v", "copy", "-c:a", "aac", "-b:a", "192k",
            "-t", f"{durations[len(scene_mp4s)]:.3f}",
            str(muxed),
        ], check=True)
        scene_mp4s.append(muxed)

    vo_list = audio / "concat.txt"
    vo_list.write_text("\n".join(f"file '{p.as_posix()}'" for p in vo_parts), encoding="utf-8")
    vo_full = audio / "narration_full.mp3"
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-f", "concat", "-safe", "0", "-i", str(vo_list),
        "-c", "copy", str(vo_full),
    ], check=True)
    vo_norm = audio / "narration_loudnorm.mp3"
    _two_pass_loudnorm(vo_full, vo_norm)

    vlist = OUT / "video_concat.txt"
    vlist.write_text("\n".join(f"file '{p.as_posix()}'" for p in scene_mp4s), encoding="utf-8")
    silent = OUT / "video_only.mp4"
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-f", "concat", "-safe", "0", "-i", str(vlist),
        "-c", "copy", str(silent),
    ], check=True)

    final = OUT / "RelatableTails_S02_CatLove_Pexels.mp4"
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", str(silent), "-i", str(vo_norm),
        "-map", "0:v", "-map", "1:a",
        "-c:v", "libx264", "-preset", "medium", "-crf", "19",
        "-c:a", "aac", "-b:a", "192k", "-shortest",
        "-movflags", "+faststart",
        str(final),
    ], check=True)

    credit_lines = []
    for a in attributions:
        credit_lines.append(
            f"Video by {a['photographer']} on Pexels ({a['pexels_url']})"
        )
    meta = {
        "title": TITLE,
        "series": SERIES,
        "scenes": SCENES,
        "durations": durations,
        "total_sec": sum(durations),
        "est_fal_usd": 0.0,
        "source": "pexels",
        "attributions": attributions,
        "upload_title": TITLE,
        "upload_desc": (
            "Comment your cat's name — we'll decide if they love you in Verdict #2.\n\n"
            + "\n".join(credit_lines)
            + "\n\nVideos provided by Pexels.\n"
            "#cat #cats #relatable #petshorts #RelatableTails"
        ),
        "pinned_comment": "Drop your cat's name. Top comment becomes Verdict #2.",
    }
    (OUT / "upload_pack.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    shutil.copy2(final, DL / "RelatableTails_S02_CatLove_Pexels.mp4")
    print(f"\nDONE {final} ({sum(durations):.1f}s) -> Downloads")
    return final


if __name__ == "__main__":
    build()
