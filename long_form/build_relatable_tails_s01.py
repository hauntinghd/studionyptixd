"""RelatableTails S01 — one decision-maker Short (~$0.08 fal + ElevenLabs).

Retention-first: hook frame 0, no dead air, verdict by 8s, comment/subscribe last 3s.

Run:
  python long_form/build_relatable_tails_s01.py
"""
from __future__ import annotations

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

from long_form.pipeline import SEEDREAM_URL, _download, _fal_post
from long_form.v5_pipeline import LTX_13B_ENDPOINT, _silence_kill, _two_pass_loudnorm

OUT = Path(r"D:/recaps/relatable_tails/s01_cat_love")
DL = Path.home() / "Downloads"
TITLE = "Does Your Cat Love You — Or Just The Food?"

# 3 beats — VO drives timing; clips trim/stretch to match.
SCENES = [
    {
        "id": "01_hook",
        "caption": "does your cat love you?",
        "narration": "Does your cat love you — or just the person who feeds them?",
        "visual": (
            "Vertical 9:16 YouTube Short. Close-up orange tabby cat staring directly "
            "at camera, big eyes, cozy living room, soft window light. Bold white text "
            "space top third. Relatable pet meme aesthetic, clean bright, NOT cartoon."
        ),
        "motion": "subtle cat head tilt, slow push-in, gentle blink, vertical short",
    },
    {
        "id": "02_verdict",
        "caption": "the decision maker says...",
        "narration": (
            "The decision maker says: if they only cuddle when the bowl is empty — "
            "that's not love. That's a transaction."
        ),
        "visual": (
            "Same orange tabby cat, mystical glowing decision wheel overlay with "
            "YES and NO labels, fortune teller candles soft blur background, vertical 9:16."
        ),
        "motion": "decision wheel slow spin glow pulse, cat still staring, vertical short",
    },
    {
        "id": "03_cta",
        "caption": "comment your cat's name",
        "narration": (
            "Comment your cat's name below. We'll decide if they love you next. "
            "Follow RelatableTails for verdict number two."
        ),
        "visual": (
            "Orange tabby cat paw reaching toward camera, subscribe reminder UI subtle, "
            "warm cozy aesthetic, vertical 9:16 short."
        ),
        "motion": "paw tap motion slow, warm push-in, vertical short",
    },
]

NEG = "blur, low quality, deformed, ugly, text watermark, human face, scary, blood"


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


def _gen_still(scene: dict, out: Path) -> Path:
    if out.exists() and out.stat().st_size > 1024:
        return out
    data = _fal_post(
        SEEDREAM_URL,
        {
            "prompt": scene["visual"][:3500],
            "negative_prompt": NEG,
            "image_size": {"width": 720, "height": 1280},
            "num_images": 1,
            "seed": 880501,
        },
        timeout_s=240,
    )
    url = (data.get("images") or [{}])[0].get("url")
    if not url:
        raise RuntimeError(data)
    _download(url, out)
    print(f"  [still] {out.name}")
    return out


def _gen_clip(still: Path, motion: str, out: Path, sec: int = 6) -> Path:
    if out.exists() and out.stat().st_size > 1024:
        return out
    url = fal_client.upload_file(str(still))
    result = fal_client.subscribe(
        LTX_13B_ENDPOINT,
        arguments={
            "image_url": url,
            "prompt": f"{motion}. Relatable pet short, stable, no deformation.",
            "negative_prompt": NEG,
            "resolution": "720p",
            "num_frames": sec * 24,
            "frame_rate": 24,
            "aspect_ratio": "9:16",
        },
    )
    vurl = ""
    if isinstance(result, dict):
        vurl = (result.get("video") or {}).get("url") or result.get("video_url", "")
    if not vurl:
        raise RuntimeError(result)
    _download(vurl, out)
    print(f"  [ltx] {out.name}")
    return out


def _gen_vo(text: str, out: Path) -> Path:
    if out.exists() and out.stat().st_size > 1024:
        return out
    from skeleton_ai.voice_elevenlabs import ElevenLabsClient

    key = os.environ.get("ELEVENLABS_STUDIO_API_KEY") or os.environ.get("ELEVENLABS_API_KEY")
    el = ElevenLabsClient(api_key=key)
    el.synthesize(text, out, voice_id="nPczCjzI2devNBz1zQrb", speed=1.05)
    print(f"  [vo] {out.name}")
    return out


def _stretch(in_mp4: Path, out_mp4: Path, target: float) -> None:
    clip = _ffprobe_dur(in_mp4)
    target = max(2.0, target)
    if target <= clip + 0.05:
        subprocess.run([
            "ffmpeg", "-y", "-loglevel", "error", "-i", str(in_mp4),
            "-t", f"{target:.3f}", "-an",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
            str(out_mp4),
        ], check=True)
    else:
        pts = target / clip
        subprocess.run([
            "ffmpeg", "-y", "-loglevel", "error", "-i", str(in_mp4),
            "-vf", f"setpts={pts:.6f}*PTS", "-fps_mode", "vfr",
            "-t", f"{target:.3f}", "-an",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
            str(out_mp4),
        ], check=True)


def _scene_video(clip: Path, caption: str, duration: float, out: Path) -> None:
    cap = caption.replace("'", "\u2019").replace(":", r"\:").replace(",", r"\,")
    font = "C\\:/Windows/Fonts/arialbd.ttf"
    fs = 64 if len(caption) <= 18 else 50
    vf = (
        f"scale=720:1280:force_original_aspect_ratio=decrease,"
        f"pad=720:1280:(ow-iw)/2:(oh-ih)/2:black,fps=30,"
        f"drawtext=fontfile='{font}':text='{cap}':fontsize={fs}:fontcolor=white:"
        f"bordercolor=black:borderw=5:x=(w-text_w)/2:y=h*0.08"
    )
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error", "-i", str(clip),
        "-vf", vf, "-t", f"{duration:.3f}", "-an",
        "-c:v", "libx264", "-preset", "medium", "-crf", "19", "-pix_fmt", "yuv420p",
        str(out),
    ], check=True)


def build() -> Path:
    _load_env()
    _ensure_fal()
    OUT.mkdir(parents=True, exist_ok=True)
    stills = OUT / "stills"
    clips = OUT / "clips"
    audio = OUT / "audio"
    scenes_dir = OUT / "scenes"
    for d in (stills, clips, audio, scenes_dir):
        d.mkdir(exist_ok=True)

    # Per-scene VO first (timing authority)
    vo_parts: list[Path] = []
    durations: list[float] = []
    for i, sc in enumerate(SCENES):
        raw = audio / f"vo_{sc['id']}_raw.mp3"
        clean = audio / f"vo_{sc['id']}_clean.mp3"
        _gen_vo(sc["narration"], raw)
        _silence_kill(raw, clean)
        dur = _ffprobe_dur(clean)
        vo_parts.append(clean)
        durations.append(dur)
        print(f"  beat {sc['id']} vo={dur:.2f}s")

    # One still + one LTX per beat (3 x $0.08 = $0.24) — use 1 still + 1 ltx for beat1, reuse for 2/3
    # Budget save: 1 still + 1 ltx only ($0.08), reuse clip for all beats
    still_path = stills / "hero_cat.png"
    _gen_still(SCENES[0], still_path)
    base_clip = clips / "hero_ltx.mp4"
    _gen_clip(still_path, SCENES[0]["motion"], base_clip, sec=8)

    scene_mp4s: list[Path] = []
    for i, sc in enumerate(SCENES):
        stretched = clips / f"{sc['id']}_stretched.mp4"
        _stretch(base_clip, stretched, durations[i])
        scene_v = scenes_dir / f"{sc['id']}.mp4"
        _scene_video(stretched, sc["caption"], durations[i], scene_v)
        # mux scene vo
        muxed = scenes_dir / f"{sc['id']}_mux.mp4"
        subprocess.run([
            "ffmpeg", "-y", "-loglevel", "error",
            "-i", str(scene_v), "-i", str(vo_parts[i]),
            "-map", "0:v", "-map", "1:a",
            "-c:v", "copy", "-c:a", "aac", "-b:a", "192k",
            "-t", f"{durations[i]:.3f}",
            str(muxed),
        ], check=True)
        scene_mp4s.append(muxed)

    # concat VO
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

    # concat video
    vlist = OUT / "video_concat.txt"
    vlist.write_text("\n".join(f"file '{p.as_posix()}'" for p in scene_mp4s), encoding="utf-8")
    silent = OUT / "video_only.mp4"
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-f", "concat", "-safe", "0", "-i", str(vlist),
        "-c", "copy", str(silent),
    ], check=True)

    final = OUT / "RelatableTails_S01_CatLove.mp4"
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", str(silent), "-i", str(vo_norm),
        "-map", "0:v", "-map", "1:a",
        "-c:v", "libx264", "-preset", "medium", "-crf", "19",
        "-c:a", "aac", "-b:a", "192k", "-shortest",
        "-movflags", "+faststart",
        str(final),
    ], check=True)

    meta = {
        "title": TITLE,
        "scenes": SCENES,
        "durations": durations,
        "total_sec": sum(durations),
        "est_fal_usd": 0.08,
        "upload_title": TITLE,
        "upload_desc": (
            "Comment your cat's name — we'll decide if they love you in the next verdict.\n"
            "#cat #cats #relatable #petshorts #RelatableTails"
        ),
        "pinned_comment": "Drop your cat's name. Top comment becomes Verdict #2.",
    }
    (OUT / "upload_pack.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    shutil.copy2(final, DL / "RelatableTails_S01_CatLove.mp4")
    print(f"\nDONE {final} ({sum(durations):.1f}s) -> Downloads")
    return final


if __name__ == "__main__":
    build()
