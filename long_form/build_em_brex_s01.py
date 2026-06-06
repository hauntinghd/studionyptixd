"""Empire Magnates S01 — Bre-X (v5 Stage 1: chapters + still gallery).

Uses approved yellow-porcelain cast kit + Creator Doctrine scripts.
Stage 1 stops at awaiting_approval (~$5 fal). Finalize separately:

  python long_form/build_em_brex_s01.py --finalize

Run from repo root:
  python long_form/build_em_brex_s01.py
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

OUT_ROOT = Path(r"D:/recaps/empire_magnates/brex_s01")
JOB_ID = "brex_s01"
TOPIC = (
    "Bre-X Minerals gold mining fraud — Busang jungle site, fake core samples, "
    "1997 collapse, $6 billion market cap wiped, Michael de Guzman, David Walsh"
)
TARGET_MINUTES = 20
SCENES_PER_CHAPTER = 12
TITLE = "March 19, 1997: When Bre-X Lost $6 Billion"


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    for p in (
        Path(r"D:\Games\asd\.env"),
        ROOT / ".env",
        ROOT.parents[2] / ".env",
    ):
        if p.exists():
            load_dotenv(p)
            break


def _ensure_fal() -> None:
    key = os.getenv("FAL_AI_KEY", "").strip() or os.getenv("FAL_KEY", "").strip()
    if not key:
        raise RuntimeError("FAL_AI_KEY not set — add credits at fal.ai and check .env")
    os.environ["FAL_KEY"] = key


async def _run_stage1() -> None:
    os.environ["LF_OUTPUT_ROOT"] = str(OUT_ROOT)

    from long_form.pipeline import compute_render_cost
    from long_form.prompts.channels import channel_outline_prompt_extras, get_channel
    from long_form.scripting import generate_outline
    from long_form.v5_pipeline import run_v5_episode_pipeline
    from skeleton_ai.scripting_grok import GrokClient

    channel = get_channel("empire_magnates")
    job_dir = OUT_ROOT / JOB_ID
    job_dir.mkdir(parents=True, exist_ok=True)
    outline_path = job_dir / "outline.json"

    grok = GrokClient()
    if outline_path.exists() and outline_path.stat().st_size > 64:
        outline = json.loads(outline_path.read_text(encoding="utf-8"))
        print(f"[cache] outline: {outline_path}")
    else:
        print("Generating outline (fal any-llm)...")
        outline = generate_outline(
            grok,
            channel["system_prompt"],
            topic=TOPIC,
            target_minutes=TARGET_MINUTES,
            title_template_block=channel_outline_prompt_extras("empire_magnates"),
        )
        outline["title"] = outline.get("title") or TITLE
        outline_path.write_text(json.dumps(outline, indent=2, ensure_ascii=True), encoding="utf-8")
        print(f"Outline saved: {outline_path}")

    cost = compute_render_cost(
        channel, outline, scenes_per_chapter_override=SCENES_PER_CHAPTER
    )
    n_ch = len(outline.get("chapters") or [])
    print(f"Title: {outline.get('title')}")
    print(f"Chapters: {n_ch}  scenes/chapter: {SCENES_PER_CHAPTER}  total scenes: {cost['n_scenes']}")
    print(f"Est fal spend — stage 1: ${cost['stage_1_usd']}  stage 2: ${cost['stage_2_usd']}  total: ${cost['total_usd']}")
    print(f"Output: {job_dir}")
    print("Running v5 Stage 1 (chapters + stills)...")

    await run_v5_episode_pipeline(
        JOB_ID,
        channel,
        outline,
        scenes_per_chapter=SCENES_PER_CHAPTER,
    )
    print(f"\nStage 1 done — review stills in {job_dir / 'stills'}")
    print("When approved: python long_form/build_em_brex_s01.py --finalize")


async def _run_finalize(*, reclip: bool = False) -> None:
    os.environ["LF_OUTPUT_ROOT"] = str(OUT_ROOT)
    os.environ.setdefault("EM_LTX_CLIP_SEC", "12")
    job_dir = OUT_ROOT / JOB_ID
    if reclip:
        import shutil
        for sub in ("clips", "scenes", "audio"):
            d = job_dir / sub
            if d.exists():
                shutil.rmtree(d)
                print(f"Cleared {d} for re-clip at {os.environ['EM_LTX_CLIP_SEC']}s")
        # Reset state so finalize re-runs scene assembly
        state_path = job_dir / "state.json"
        if state_path.exists():
            import json
            st = json.loads(state_path.read_text(encoding="utf-8"))
            st["phase"] = "awaiting_approval"
            st.pop("mp4_path", None)
            state_path.write_text(json.dumps(st, indent=2, ensure_ascii=True), encoding="utf-8")

    from long_form.v5_pipeline import finalize_v5_episode_pipeline

    print(f"Finalizing {JOB_ID} (LTX {os.environ.get('EM_LTX_CLIP_SEC', '12')}s clips + VO + compose)...")
    await finalize_v5_episode_pipeline(JOB_ID)
    _copy_upload_pack()
    print(f"Done. Check {job_dir}")


def _resync_local() -> None:
    """Re-cut existing 12s LTX clips to VO + recompose. No fal spend."""
    import json
    import shutil

    from long_form.pipeline import _ffprobe_dur, _slugify
    from long_form.v5_pipeline import (
        EM_LTX_CLIP_SEC,
        _build_scene_mp4,
        _derive_sfx_prompt,
        _final_concat_v5,
        _gen_scene_sfx,
        _mix_vo_sfx_loudnorm,
        _stretch_video_to_duration,
    )

    job_dir = OUT_ROOT / JOB_ID
    state = json.loads((job_dir / "state.json").read_text(encoding="utf-8"))
    outline = state.get("outline") or {}
    fps = 60
    scene_brief_records = state.get("scene_briefs") or []
    clips = job_dir / "clips"
    vo = job_dir / "audio" / "vo"
    sfx = job_dir / "audio" / "sfx"
    scenes = job_dir / "scenes"
    sfx.mkdir(parents=True, exist_ok=True)
    scenes.mkdir(parents=True, exist_ok=True)

    for stretched in clips.glob("*_stretched.mp4"):
        stretched.unlink(missing_ok=True)
    for scene_mp4 in scenes.glob("*.mp4"):
        scene_mp4.unlink(missing_ok=True)
    for old in job_dir.glob("LongForm_*.mp4"):
        old.unlink(missing_ok=True)

    scene_mp4s: list[Path] = []
    for rec in scene_brief_records:
        ci = int(rec["chapter_index"])
        li = int(rec["local_idx"])
        gi = int(rec["global_idx"])
        sb = rec["brief"]
        sid = f"{ci:02d}_{li:02d}_{gi:04d}"
        raw_clip = clips / f"clip_{sid}.mp4"
        if not raw_clip.exists():
            print(f"  skip scene {gi}: missing {raw_clip.name}")
            continue
        vo_clean = vo / f"vo_{sid}_clean.mp3"
        if not vo_clean.exists():
            print(f"  skip scene {gi}: missing VO")
            continue
        vo_dur = max(2.0, _ffprobe_dur(vo_clean))

        stretched = clips / f"clip_{sid}_stretched.mp4"
        _stretch_video_to_duration(raw_clip, stretched, vo_dur, force=True)

        sfx_path = sfx / f"sfx_{sid}.mp3"
        if not sfx_path.exists() or sfx_path.stat().st_size < 1024:
            sfx_prompt = _derive_sfx_prompt(sb.get("scene_prompt", ""), sb.get("narration", ""))
            _gen_scene_sfx(sfx_prompt, vo_dur + 1.0, sfx_path)

        audio_final = vo / f"audio_{sid}.mp3"
        if not audio_final.exists() or audio_final.stat().st_size < 4096:
            _mix_vo_sfx_loudnorm(vo_clean, sfx_path, audio_final, duration_sec=vo_dur)

        scene_mp4 = scenes / f"scene_{sid}.mp4"
        _build_scene_mp4(stretched, audio_final, scene_mp4, fps=fps, duration_sec=vo_dur)
        scene_mp4s.append(scene_mp4)
        if len(scene_mp4s) % 8 == 0:
            print(f"  resynced {len(scene_mp4s)}/{len(scene_brief_records)} scenes...")

    title_slug = _slugify(outline.get("title", "brex"))
    out_mp4 = job_dir / f"LongForm_{title_slug}.mp4"
    _final_concat_v5(scene_mp4s, out_mp4, fade_out_sec=3.0, fps=fps)
    state["phase"] = "done"
    state["mp4_path"] = f"{JOB_ID}/{out_mp4.name}"
    state["mp4_duration_sec"] = _ffprobe_dur(out_mp4)
    state["mp4_size_bytes"] = out_mp4.stat().st_size
    (job_dir / "state.json").write_text(json.dumps(state, indent=2, ensure_ascii=True), encoding="utf-8")
    print(f"Resynced {len(scene_mp4s)} scenes @ {EM_LTX_CLIP_SEC}s LTX -> VO trim/slow-mo")
    _copy_upload_pack()


def _copy_upload_pack() -> None:
    """Copy MP4 + approved yellow-porcelain thumbs to Downloads."""
    import shutil

    from long_form.em_yellow_cast_kit import APPROVED_DIR

    dl = Path.home() / "Downloads"
    job_dir = OUT_ROOT / JOB_ID
    mp4s = sorted(job_dir.glob("LongForm_*.mp4"), key=lambda p: p.stat().st_mtime, reverse=True)
    if mp4s:
        dest = dl / "EM_BreX_March_19_1997_When_Bre-X_Lost_6_Billion.mp4"
        shutil.copy2(mp4s[0], dest)

    thumb_map = {
        "EM_yellow_thumb_brex.png": "thumb_archetype_brex.png",
        "EM_yellow_scene_01_presentation.png": "scene_archetype_presentation.png",
        "EM_yellow_scene_02_walk.png": "scene_archetype_walk.png",
    }
    for out_name, src_name in thumb_map.items():
        src = APPROVED_DIR / src_name
        if src.exists():
            shutil.copy2(src, dl / out_name)

    upload = dl / "EM_BreX_UPLOAD.txt"
    upload.write_text(
        "TITLE:\n"
        "March 19, 1997: When Bre-X Lost $6 Billion\n\n"
        "DESCRIPTION:\n"
        "March 19, 1997. A geologist boards a helicopter in Borneo and never lands. "
        "Six days later, the largest gold deposit in history vanishes.\n\n"
        "Loophole Files - fraud mechanisms explained honestly.\n\n"
        "THUMBNAIL (use one of these - NOT auto-generated thumb_1/2/3):\n"
        "  EM_yellow_thumb_brex.png          (primary - Bre-X hologram + 6B badge)\n"
        "  EM_yellow_scene_01_presentation.png\n"
        "  EM_yellow_scene_02_walk.png\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="EM Bre-X v5 build")
    parser.add_argument(
        "--finalize",
        action="store_true",
        help="Run Stage 2 after still gallery approval",
    )
    parser.add_argument(
        "--reclip",
        action="store_true",
        help="With --finalize: delete clips/scenes and re-render i2v (e.g. longer clips)",
    )
    parser.add_argument(
        "--resync",
        action="store_true",
        help="Re-cut existing 12s clips to VO locally (no fal) + recompose",
    )
    parser.add_argument(
        "--copy-pack",
        action="store_true",
        help="Copy MP4 + approved thumbs to Downloads only",
    )
    args = parser.parse_args()

    _load_env()
    _ensure_fal()
    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    if args.copy_pack:
        _copy_upload_pack()
        print("Upload pack copied to Downloads.")
    elif args.resync:
        os.environ.setdefault("EM_LTX_CLIP_SEC", "12")
        _resync_local()
    elif args.finalize:
        asyncio.run(_run_finalize(reclip=args.reclip))
    else:
        asyncio.run(_run_stage1())


if __name__ == "__main__":
    main()
