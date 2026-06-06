"""PB Lies — finish Operation Mockingbird (VO sync + SFX + compose).

Uses pre-rendered LTX clips + locked narration script. Each clip is trimmed or
slow-stretched to match fal MiniMax VO duration exactly (ElevenLabs fallback
if credits available).

Run:
  python long_form/build_pb_mockingbird_finish.py
  python long_form/build_pb_mockingbird_finish.py --resync   # local ffmpeg only
  python long_form/build_pb_mockingbird_finish.py --skip-sfx # VO + mux only
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

from long_form.pb_lies_mockingbird_scenes import MOCKINGBIRD_SCENES
from long_form.pb_lies_mockingbird_script import (
    NARRATION,
    SILENT_SCENE_SEC,
    SFX_BY_BEAT,
    TITLE,
    _narration_for,
)
from long_form.pipeline import _ffprobe_dur, _slugify

OUT_ROOT = Path(r"D:/recaps/pb_lies/mockingbird_s01")
CLIPS_DIR = OUT_ROOT / "clips"
AUDIO_VO = OUT_ROOT / "audio" / "vo"
AUDIO_SFX = OUT_ROOT / "audio" / "sfx"
SCENES_DIR = OUT_ROOT / "scenes"
FPS = 30
VOICE_SPEED = 0.92
ELEVENLABS_VOICE = "nPczCjzI2devNBz1zQrb"  # Brian


def _gen_vo(text: str, out_raw: Path) -> None:
    if out_raw.exists() and out_raw.stat().st_size > 1024:
        return
    from skeleton_ai.voice_elevenlabs import ElevenLabsClient, ElevenLabsAuthError

    # Prefer paid studio key if set; fall back to ELEVENLABS_API_KEY.
    el_key = (
        os.environ.get("ELEVENLABS_STUDIO_API_KEY")
        or os.environ.get("ELEVENLABS_API_KEY")
        or ""
    ).strip()
    try:
        el = ElevenLabsClient(api_key=el_key)
        el.synthesize(text, out_raw, voice_id=ELEVENLABS_VOICE, speed=VOICE_SPEED)
        return
    except ElevenLabsAuthError as e:
        print(f"  [warn] ElevenLabs failed ({e}) — falling back to MiniMax")
    from long_form.v5_pipeline import _gen_em_vo

    _gen_em_vo(text, out_raw, voice_id="English_Trustworthy_Man", speed=VOICE_SPEED)


def _needs_audio_rebuild(clean: Path, final: Path, target_sec: float) -> bool:
    if not final.exists() or final.stat().st_size < 4096:
        return True
    final_dur = _ffprobe_dur(final)
    return abs(final_dur - target_sec) > 0.35


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    for p in (Path(r"D:\Games\asd\.env"), ROOT / ".env"):
        if p.exists():
            load_dotenv(p)
            break


def _scene_keys() -> list[str]:
    return sorted(MOCKINGBIRD_SCENES.keys(), key=lambda k: MOCKINGBIRD_SCENES[k]["id"])


def _sid(idx: int, spec: dict) -> str:
    return f"{idx:02d}_{spec['id']}"


def _gen_silence(out_raw: Path, sec: float) -> None:
    if out_raw.exists() and out_raw.stat().st_size > 512:
        return
    out_raw.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
        "-f", "lavfi", "-i", "anullsrc=r=44100:cl=mono",
        "-t", f"{sec:.2f}",
        "-c:a", "libmp3lame", "-b:a", "192k",
        str(out_raw),
    ]
    subprocess.run(cmd, check=True, capture_output=True)


def _sfx_prompt(spec: dict, narration: str) -> str:
    beat = str(spec.get("beat") or "")
    if beat in SFX_BY_BEAT:
        return SFX_BY_BEAT[beat]
    from long_form.v5_pipeline import _derive_sfx_prompt

    visual = spec.get("prompt_delta", "")
    return _derive_sfx_prompt(visual, narration)


def _process_scene(
    idx: int,
    key: str,
    *,
    skip_sfx: bool = False,
    force: bool = False,
) -> Path | None:
    from long_form.v5_pipeline import (
        _build_scene_mp4,
        _gen_scene_sfx,
        _mix_vo_sfx_loudnorm,
        _silence_kill,
        _stretch_video_to_duration,
    )

    spec = MOCKINGBIRD_SCENES[key]
    sid = _sid(idx, spec)
    narration = _narration_for(key)

    raw_clip = CLIPS_DIR / f"{spec['id']}.mp4"
    if not raw_clip.exists():
        print(f"  [skip] missing clip {raw_clip.name}")
        return None

    vo_raw = AUDIO_VO / f"vo_{sid}_raw.mp3"
    vo_clean = AUDIO_VO / f"vo_{sid}_clean.mp3"
    AUDIO_VO.mkdir(parents=True, exist_ok=True)

    if force or not vo_clean.exists() or vo_clean.stat().st_size < 512:
        if vo_raw.exists():
            vo_raw.unlink(missing_ok=True)
        if vo_clean.exists():
            vo_clean.unlink(missing_ok=True)
        if narration:
            print(f"  [vo] {spec['id']} ({len(narration.split())} words)")
            _gen_vo(narration, vo_raw)
            _silence_kill(vo_raw, vo_clean)
        else:
            sec = SILENT_SCENE_SEC.get(key, 4.0)
            print(f"  [silence] {spec['id']} ({sec}s)")
            _gen_silence(vo_raw, sec)
            if vo_clean.exists():
                vo_clean.unlink(missing_ok=True)
            shutil.copy2(vo_raw, vo_clean)

    vo_dur = max(2.0, _ffprobe_dur(vo_clean))

    stretched = CLIPS_DIR / f"{spec['id']}_stretched.mp4"
    _stretch_video_to_duration(raw_clip, stretched, vo_dur, force=force)

    sfx_path = AUDIO_SFX / f"sfx_{sid}.mp3"
    AUDIO_SFX.mkdir(parents=True, exist_ok=True)
    if skip_sfx:
        # VO-only bed: copy clean vo as final audio path input
        audio_final = AUDIO_VO / f"audio_{sid}.mp3"
        if force or not audio_final.exists():
            shutil.copy2(vo_clean, audio_final)
    else:
        if force or not sfx_path.exists() or sfx_path.stat().st_size < 1024:
            prompt = _sfx_prompt(spec, narration)
            _gen_scene_sfx(prompt, vo_dur + 1.0, sfx_path)
        audio_final = AUDIO_VO / f"audio_{sid}.mp3"
        if force or _needs_audio_rebuild(vo_clean, audio_final, vo_dur):
            if audio_final.exists():
                audio_final.unlink(missing_ok=True)
            _mix_vo_sfx_loudnorm(vo_clean, sfx_path, audio_final, duration_sec=vo_dur)

    scene_mp4 = SCENES_DIR / f"scene_{sid}.mp4"
    SCENES_DIR.mkdir(parents=True, exist_ok=True)
    _build_scene_mp4(stretched, audio_final, scene_mp4, fps=FPS, duration_sec=vo_dur)

    actual = _ffprobe_dur(scene_mp4)
    drift = abs(actual - vo_dur)
    flag = " OK" if drift < 0.2 else f" DRIFT {drift:.2f}s"
    print(f"  [mux] {scene_mp4.name} vo={vo_dur:.2f}s scene={actual:.2f}s{flag}")
    return scene_mp4


def _write_sync_report(scene_rows: list[dict]) -> None:
    (OUT_ROOT / "sync_report.json").write_text(
        json.dumps({"title": TITLE, "fps": FPS, "scenes": scene_rows}, indent=2),
        encoding="utf-8",
    )


def finish(*, skip_sfx: bool = False, resync: bool = False, force: bool = False) -> Path:
    from long_form.v5_pipeline import _final_concat_v5

    if resync:
        force = True

    keys = _scene_keys()
    scene_mp4s: list[Path] = []
    rows: list[dict] = []
    total_vo = 0.0

    print(f"=== Finish Mockingbird ({len(keys)} scenes) ===")
    for idx, key in enumerate(keys, start=1):
        spec = MOCKINGBIRD_SCENES[key]
        sid = _sid(idx, spec)
        if not resync:
            scene_mp4 = _process_scene(idx, key, skip_sfx=skip_sfx, force=force)
        else:
            scene_mp4 = SCENES_DIR / f"scene_{sid}.mp4"
            vo_clean = AUDIO_VO / f"vo_{sid}_clean.mp3"
            raw_clip = CLIPS_DIR / f"{spec['id']}.mp4"
            if not vo_clean.exists() or not raw_clip.exists():
                print(f"  [skip resync] {key}")
                continue
            scene_mp4 = _process_scene(idx, key, skip_sfx=skip_sfx, force=True)

        if scene_mp4 and scene_mp4.exists():
            scene_mp4s.append(scene_mp4)
            vo_dur = _ffprobe_dur(AUDIO_VO / f"vo_{sid}_clean.mp3")
            total_vo += vo_dur
            rows.append({
                "index": idx,
                "key": key,
                "id": spec["id"],
                "chapter": spec.get("chapter"),
                "beat": spec.get("beat"),
                "vo_sec": round(vo_dur, 3),
                "scene_mp4_sec": round(_ffprobe_dur(scene_mp4), 3),
                "narration_words": len(NARRATION.get(key, "").split()),
            })

    if not scene_mp4s:
        raise RuntimeError("No scene MP4s assembled")

    title_slug = _slugify(TITLE)
    out_mp4 = OUT_ROOT / f"LongForm_{title_slug}.mp4"
    for old in OUT_ROOT.glob("LongForm_*.mp4"):
        if old != out_mp4:
            old.unlink(missing_ok=True)

    print(f"\n=== Concat {len(scene_mp4s)} scenes (~{total_vo/60:.1f} min VO) ===")
    _final_concat_v5(scene_mp4s, out_mp4, fade_out_sec=3.0, fps=FPS)

    dur = _ffprobe_dur(out_mp4)
    _write_sync_report(rows)

    dl = Path.home() / "Downloads" / "PB_Lies_Operation_Mockingbird.mp4"
    shutil.copy2(out_mp4, dl)
    print(f"\n=== DONE ===")
    print(f"Final: {out_mp4} ({dur/60:.1f} min, {out_mp4.stat().st_size//1024//1024} MB)")
    print(f"Downloads: {dl}")
    print(f"Sync report: {OUT_ROOT / 'sync_report.json'}")
    return out_mp4


def main() -> None:
    _load_env()
    ap = argparse.ArgumentParser(description="Finish PB Lies Mockingbird episode")
    ap.add_argument("--resync", action="store_true", help="Re-stretch/mux only (no new VO/SFX)")
    ap.add_argument("--skip-sfx", action="store_true")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    finish(skip_sfx=args.skip_sfx, resync=args.resync, force=args.force)


if __name__ == "__main__":
    main()
