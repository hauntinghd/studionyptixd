"""PB Lies — Operation Mockingbird LTX animation pass.

Animates approved cast-kit stills → 720p clips via LTX 13B distilled (~$0.04 each).

Run:
  python long_form/build_pb_mockingbird_animate.py           # all 36 clips
  python long_form/build_pb_mockingbird_animate.py --limit 3 # smoke test
  python long_form/build_pb_mockingbird_animate.py --scene scene_01_classified_desk
  python long_form/build_pb_mockingbird_animate.py --force   # re-render existing
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from long_form.pb_lies_cast_kit import STILLS_DIR
from long_form.pb_lies_mockingbird_scenes import MOCKINGBIRD_SCENES
from long_form.v5_pipeline import EM_LTX_FPS, LTX_13B_ENDPOINT, _download

OUT_ROOT = Path(r"D:/recaps/pb_lies/mockingbird_s01")
CLIPS_DIR = OUT_ROOT / "clips"
DOWNLOADS = Path.home() / "Downloads" / "PB_Lies_Mockbird_Clips"
CLIP_SEC = max(5, min(12, int(os.environ.get("PB_LTX_CLIP_SEC", "10"))))
COST_PER_CLIP = 0.04

MOTION_BY_BEAT: dict[str, str] = {
    "cold_open": "slow push-in on desk and documents, subtle forensic red glow pulse",
    "chapter_card": "slow dolly toward title wall, minimal figure movement",
    "character_intro": "slow orbit left, subtle rim light shift on figure",
    "evidence": "slow pan right, figure hands subtly adjusting papers",
    "environment": "slow parallax push-in, ambient room depth",
    "mechanism": "subtle screen glow animation, gentle data flicker on displays",
    "transition": "figure slow forward walk, corridor dolly follow",
    "number_card": "slow push-in emphasizing floating number card UI",
    "modern_beat": "monitor glow pulse, subtle typing hand motion",
    "legacy": "slow push-in through newsroom depth",
    "pause": "near-static hold with micro push-in only",
    "callback": "slow push-in matching cold open, folder edge glow",
    "outro": "figure hand placing folder on desk, slow deliberate motion",
}

PB_IDENTITY = (
    "faceless white 3D mannequin preserved, same mesh topology, "
    "no facial features appearing, documentary Nod Map style"
)


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
    key = (os.environ.get("FAL_AI_KEY") or os.environ.get("FAL_KEY") or "").strip()
    if not key:
        raise RuntimeError("FAL_AI_KEY not set")
    os.environ["FAL_KEY"] = key


def _still_path(spec: dict) -> Path:
    vid = spec.get("variant") or "white_suit_default"
    p = STILLS_DIR / f"{spec['id']}_{vid}.png"
    if not p.exists():
        legacy = STILLS_DIR / f"{spec['id']}.png"
        if legacy.exists():
            return legacy
        raise FileNotFoundError(f"Missing still: {p}")
    return p


def _motion_for(spec: dict) -> str:
    beat = str(spec.get("beat") or "evidence")
    return MOTION_BY_BEAT.get(beat, MOTION_BY_BEAT["evidence"])


def _gen_pb_clip(still: Path, motion: str, out: Path) -> Path:
    if out.exists() and out.stat().st_size > 1024:
        return out
    import fal_client

    _ensure_fal()
    num_frames = CLIP_SEC * EM_LTX_FPS
    image_url = fal_client.upload_file(str(still))
    full_prompt = (
        f"{motion}. Documentary cinematography, subtle realistic motion, "
        f"no camera wobble, no subject deformation, stable composition, "
        f"cinematic 3D investigative documentary, {PB_IDENTITY}"
    )
    neg = (
        "blur, distort, low quality, static noise, face morphing, anime, cartoon, "
        "subject deformation, flicker, warping, jitter, eyes mouth nose appearing, "
        "real human face, character changing, duplicate limbs"
    )
    last_err = ""
    for attempt in range(2):
        try:
            result = fal_client.subscribe(
                LTX_13B_ENDPOINT,
                arguments={
                    "image_url": image_url,
                    "prompt": full_prompt,
                    "negative_prompt": neg,
                    "resolution": "720p",
                    "num_frames": num_frames,
                    "frame_rate": EM_LTX_FPS,
                    "aspect_ratio": "16:9",
                },
            )
            video_url = ""
            if isinstance(result, dict):
                video = result.get("video") or {}
                if isinstance(video, dict):
                    video_url = video.get("url") or ""
                video_url = video_url or result.get("video_url", "")
            if not video_url:
                last_err = f"no video url: {str(result)[:200]}"
                time.sleep(3 + attempt * 5)
                continue
            _download(video_url, out, timeout_s=180)
            print(f"  [ltx] {out.name} ({out.stat().st_size // 1024} KB, {CLIP_SEC}s)")
            return out
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            time.sleep(3 + attempt * 5)
    raise RuntimeError(f"LTX failed for {still.name}: {last_err}")


def animate_scene(key: str, *, force: bool = False) -> float:
    spec = MOCKINGBIRD_SCENES[key]
    still = _still_path(spec)
    out = CLIPS_DIR / f"{spec['id']}.mp4"
    CLIPS_DIR.mkdir(parents=True, exist_ok=True)
    if force and out.exists():
        out.unlink()
    ch = spec.get("chapter", "?")
    print(f"=== Animate {key} ch={ch} ({CLIP_SEC}s LTX) ===")
    motion = _motion_for(spec)
    _gen_pb_clip(still, motion, out)
    DOWNLOADS.mkdir(parents=True, exist_ok=True)
    shutil.copy2(out, DOWNLOADS / out.name)
    return COST_PER_CLIP


def animate_all(*, limit: int | None = None, force: bool = False) -> float:
    keys = sorted(MOCKINGBIRD_SCENES.keys(), key=lambda k: MOCKINGBIRD_SCENES[k]["id"])
    if limit:
        keys = keys[:limit]
    cost = 0.0
    print(f"=== Mockingbird LTX ({len(keys)} clips × ${COST_PER_CLIP}) ===")
    for key in keys:
        spec = MOCKINGBIRD_SCENES[key]
        out = CLIPS_DIR / f"{spec['id']}.mp4"
        if not force and out.exists() and out.stat().st_size > 1024:
            print(f"  [skip] {out.name}")
            continue
        cost += animate_scene(key, force=force)
    return cost


def _write_manifest() -> None:
    rows = []
    for key in sorted(MOCKINGBIRD_SCENES.keys(), key=lambda k: MOCKINGBIRD_SCENES[k]["id"]):
        spec = MOCKINGBIRD_SCENES[key]
        clip = CLIPS_DIR / f"{spec['id']}.mp4"
        rows.append({
            "scene_key": key,
            "id": spec["id"],
            "chapter": spec.get("chapter"),
            "beat": spec.get("beat"),
            "variant": spec.get("variant"),
            "still": str(_still_path(spec)),
            "clip": str(clip) if clip.exists() else None,
            "motion": _motion_for(spec),
        })
    meta = {
        "clip_sec": CLIP_SEC,
        "fps": EM_LTX_FPS,
        "cost_per_clip": COST_PER_CLIP,
        "clips_dir": str(CLIPS_DIR),
        "downloads": str(DOWNLOADS),
        "scenes": rows,
    }
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    (OUT_ROOT / "animate_manifest.json").write_text(
        json.dumps(meta, indent=2), encoding="utf-8"
    )


def main() -> None:
    _load_env()
    ap = argparse.ArgumentParser(description="PB Lies Mockingbird LTX animation")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--scene", type=str, default=None)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    cost = 0.0
    if args.scene:
        if args.scene not in MOCKINGBIRD_SCENES:
            raise SystemExit(f"Unknown scene: {args.scene}")
        cost = animate_scene(args.scene, force=args.force)
    else:
        cost = animate_all(limit=args.limit, force=args.force)

    _write_manifest()
    print(f"\n=== DONE — est fal ~${cost:.2f} | clips -> {CLIPS_DIR} ===")
    print(f"Downloads: {DOWNLOADS}")


if __name__ == "__main__":
    main()
