"""
EM v5_episode sub-pipeline — cinematic photoreal documentary renders.

Registered into long_form.pipeline.SUB_PIPELINES under key 'v5_episode'.
Channel registry routes EM (Empire Magnates) here via pipeline_kind.

Mirrors the locked v5 recipe documented in project_v5_pipeline_locked.md:
  - per-scene LTX 13B i2v animation (skeleton_ai.i2v_engine handles the
    LTX → Seedance → Pixverse fallback chain)
  - per-scene ElevenLabs VO (Brian default; skeleton_ai.voice_elevenlabs)
  - aggressive silence-kill on VO (-30dB / 200ms / 100ms breath)
  - per-scene mmaudio-v2 SFX bed (auto-prompt heuristic from scene visual)
  - VO + SFX mix at 16% duck, then 2-pass loudnorm to -14 LUFS
  - scenes concatenated with constant-power audio crossfades
  - final fade-to-black

NOT yet ported from canonical build_episode_v5.py:
  - Whisper word-level transcription + word-timed callouts (needs
    scene.overlay text data which Grok-generated outlines don't have
    natively; can be added in a follow-up by deriving callouts from
    scene_action + numeric/proper-noun extraction)
  - constant-power audio crossfades between scenes (using simple
    concat-demuxer for now — works fine; crossfades can be added later
    via filter_complex acrossfade)

Cost envelope (from project_v5_pipeline_locked.md): ~$50.76 fal per
20-min EM episode. Real numbers checked against Wirecard ship.
"""
from __future__ import annotations

import asyncio
import json
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable

from long_form.pipeline import (
    LFRenderError,
    LF_OUTPUT_ROOT,
    MMAUDIO_URL,
    SEEDREAM_URL,
    _chapters_path,
    _chunk_text,
    _download,
    _ensure_job_dir,
    _fal_post,
    _ffmpeg_concat_audio,
    _ffprobe_dur,
    _final_mp4_path,
    _gen_chapter,
    _gen_thumbnails,
    _job_dir,
    _slugify,
    _two_pass_loudnorm,
    load_state,
    save_state,
    update_status,
)


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 — scenes (seedream v4.5 stills, EM uses seedream not ernie)
# ─────────────────────────────────────────────────────────────────────────────

def _gen_em_still(prompt: str, visual_style: str, out_path: Path) -> Path:
    """Generate a still via seedream v4.5 with the EM channel's visual_style
    appended. Higher fidelity than ernie-image (~$0.04 vs $0.03/image, 4× quality)."""
    if out_path.exists() and out_path.stat().st_size > 1024:
        return out_path
    full_prompt = f"{prompt}\n\nStyle: {visual_style}".strip()
    data = _fal_post(
        SEEDREAM_URL,
        {"prompt": full_prompt, "image_size": {"width": 1920, "height": 1080}},
        timeout_s=240,
    )
    images = data.get("images") or []
    if not images:
        raise LFRenderError(f"EM still gen returned no images: {data}")
    img_url = images[0].get("url", "")
    if not img_url:
        raise LFRenderError(f"EM still gen response missing url: {data}")
    _download(img_url, out_path, timeout_s=120)
    return out_path


# ─────────────────────────────────────────────────────────────────────────────
# Phase 3 — i2v (LTX 13B via skeleton_ai.i2v_engine, fallback chain)
# ─────────────────────────────────────────────────────────────────────────────

def _gen_em_clip(still_path: Path, motion_prompt: str, out_path: Path,
                 *, duration_sec: int = 5) -> Path:
    """Animate the still via skeleton_ai.i2v_engine (LTX 13B → Seedance → Pixverse).

    EM v5 is locked to 16:9 aspect and ~5s clips per scene per
    project_v5_pipeline_locked.md. Each scene clip stretches to its
    VO duration during scene assembly (Phase 5)."""
    if out_path.exists() and out_path.stat().st_size > 1024:
        return out_path
    # Lazy import — keeps long_form/v5_pipeline.py importable without fal_client
    # available at module load.
    from skeleton_ai.i2v_engine import generate as gen_i2v
    return gen_i2v(
        still_path,
        motion_prompt or "subtle parallax, slow camera push-in, gentle ambient motion",
        out_path,
        tier="standard",
        duration_sec=duration_sec,
        aspect_ratio="16:9",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Phase 4 — VO (ElevenLabs Brian default)
# ─────────────────────────────────────────────────────────────────────────────

def _gen_em_vo(text: str, out_path: Path, *, voice_id: str = "",
               speed: float = 1.0) -> Path:
    """Synthesize one VO chunk via ElevenLabs. Caller is expected to chunk
    long chapter narration into per-scene sentences before calling.

    EM uses ElevenLabs (vs HR's fal MiniMax) per the channel registry —
    Lume-emulating long-form has been on EL since the v5 lock 2026-04-24.
    """
    if out_path.exists() and out_path.stat().st_size > 1024:
        return out_path
    from skeleton_ai.voice_elevenlabs import ElevenLabsClient
    client = ElevenLabsClient()
    return client.synthesize(text, out_path, voice_id=voice_id or None, speed=speed)


# ─────────────────────────────────────────────────────────────────────────────
# Phase 5 — per-scene assembly (silence-kill + SFX + 2-pass loudnorm + mux)
#
# Mirrors build_episode_v5.py:trim_and_compress + mmaudio_sfx + scene mux.
# ─────────────────────────────────────────────────────────────────────────────

# Auto-SFX prompt heuristics — keyword → ambience descriptor. Lifted directly
# from build_episode_v5.py so EM-channel renders match the validated Wirecard
# ship. Ordered: more-specific keywords win earlier matches.
SFX_HEURISTICS: list[tuple[tuple[str, ...], str]] = [
    (("vault", "safe", "deposit box"),
     "echoing metal vault interior, distant pump hum, dramatic low strings, dust motes, suspense"),
    (("trading floor", "stock exchange", "DAX", "Bloomberg", "ticker"),
     "stock-exchange floor din, ticker clatter, distant phone chatter, urgent low strings"),
    (("courtroom", "trial", "judge", "verdict"),
     "courtroom ambience, distant murmur, gavel hint, tense procedural underscore"),
    (("airfield", "private jet", "tarmac", "takeoff"),
     "private airfield ambience, jet turbine spool-up, wind, low cinematic suspense"),
    (("Moscow", "Russia", "GRU", "Kremlin"),
     "cold corridor ambience, faint Russian conversation, low menacing strings, dread"),
    (("press", "podium", "interview", "camera flashes"),
     "press conference room, camera shutter clicks, microphone hiss, formal underscore"),
    (("office", "desk", "boardroom", "executive"),
     "modern office ambience, monitor hum, hvac drone, restrained low documentary score"),
    (("street", "city", "urban", "skyline", "drone"),
     "European city ambience, faint distant traffic, light wind, atmospheric documentary score"),
    (("auditor", "Ernst & Young", "EY", "KPMG", "ledger", "balance sheet"),
     "office paper rustle, keyboard taps, hushed concern, slow tense underscore"),
    (("forge", "signature", "fake", "fraudulent"),
     "tense pen-on-paper close, paper crinkle, low strings, conspiratorial dread"),
    (("Philippines", "Manila", "tropical"),
     "tropical office ambience, ceiling fan, distant city, humid documentary atmosphere"),
    (("hotel", "lobby", "handshake"),
     "hotel lobby ambience, distant footsteps, glass clink, refined tense score"),
]
DEFAULT_SFX_PROMPT = (
    "Documentary thriller score with sustained low strings, atmospheric dread, "
    "subtle ambient bed, no foreground sounds. Lume-style cinematic underscore."
)


def _derive_sfx_prompt(visual: str, narration: str = "") -> str:
    """Map scene visual + narration text to an mmaudio-v2 SFX prompt.

    Lifted from build_episode_v5.py:derive_sfx_prompt — same heuristic
    table; we additionally search the narration text since Grok-generated
    scene visuals may be more abstract than build_episode_v5's hand-curated
    scene_brief."""
    haystack = f"{visual} {narration}".lower()
    for kws, prompt in SFX_HEURISTICS:
        for kw in kws:
            if kw.lower() in haystack:
                return prompt
    return DEFAULT_SFX_PROMPT


def _silence_kill(in_mp3: Path, out_mp3: Path) -> Path:
    """Aggressive silence-kill on VO: -30dB threshold, 200ms run, 100ms
    breath. Same params build_episode_v5.py uses (proven on Wirecard)."""
    if out_mp3.exists() and out_mp3.stat().st_size > 4096:
        return out_mp3
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
        "-i", str(in_mp3),
        "-af",
        "silenceremove=start_periods=1:start_silence=0.05:start_threshold=-30dB:"
        "stop_periods=-1:stop_silence=0.1:stop_threshold=-30dB:detection=peak",
        "-c:a", "libmp3lame", "-b:a", "192k",
        str(out_mp3),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise LFRenderError(f"silence-kill failed: {r.stderr[-400:]}")
    return out_mp3


def _gen_scene_sfx(prompt: str, duration_sec: float, out_path: Path) -> Path:
    """Per-scene mmaudio-v2 SFX bed. Same endpoint sleep_doc uses for
    ambient — different prompt + matched-to-clip duration."""
    if out_path.exists() and out_path.stat().st_size > 1024:
        return out_path
    data = _fal_post(
        MMAUDIO_URL,
        {"prompt": prompt, "duration": int(max(2, min(30, duration_sec)))},
        timeout_s=180,
    )
    url = (data.get("audio") or {}).get("url") or data.get("audio_url")
    if not url:
        raise LFRenderError(f"mmaudio response missing audio url: {data}")
    _download(url, out_path, timeout_s=120)
    return out_path


def _stretch_video_to_duration(in_mp4: Path, out_mp4: Path, target_sec: float) -> Path:
    """Stretch (or freeze-loop) a clip so it matches the VO duration.

    EM v5 strategy: if VO ≤ clip, trim. If VO > clip, freeze the last frame.
    Simpler than zoompan + segments and matches Wirecard's recipe.
    """
    if out_mp4.exists() and out_mp4.stat().st_size > 1024:
        return out_mp4
    clip_sec = _ffprobe_dur(in_mp4)
    if clip_sec <= 0:
        raise LFRenderError(f"clip {in_mp4} has zero duration")
    target_sec = max(2.0, target_sec)
    if target_sec <= clip_sec + 0.05:
        # Trim.
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
            "-i", str(in_mp4),
            "-t", f"{target_sec:.3f}",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
            "-an",
            str(out_mp4),
        ]
    else:
        # Tail-pad with last-frame freeze (tpad=stop_mode=clone).
        extra = target_sec - clip_sec
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
            "-i", str(in_mp4),
            "-vf", f"tpad=stop_mode=clone:stop_duration={extra:.3f}",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
            "-an",
            str(out_mp4),
        ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise LFRenderError(f"stretch_video failed on {in_mp4.name}: {r.stderr[-400:]}")
    return out_mp4


def _mix_vo_sfx_loudnorm(vo_mp3: Path, sfx_mp3: Path, out_mp3: Path,
                         *, duration_sec: float) -> Path:
    """Mix VO at 1.0 + SFX at 0.16 (16% duck per build_episode_v5), trim to
    duration_sec, then 2-pass loudnorm to -14 LUFS broadcast standard."""
    if out_mp3.exists() and out_mp3.stat().st_size > 4096:
        return out_mp3
    pre_mix = out_mp3.with_name(out_mp3.stem + "_pre.mp3")
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
        "-i", str(vo_mp3),
        "-stream_loop", "-1", "-i", str(sfx_mp3),
        "-filter_complex",
        "[0:a]volume=1.0[v];[1:a]volume=0.16[s];"
        "[v][s]amix=inputs=2:duration=first:dropout_transition=0",
        "-t", f"{duration_sec:.3f}",
        "-c:a", "libmp3lame", "-b:a", "192k",
        str(pre_mix),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise LFRenderError(f"mix VO+SFX failed: {r.stderr[-400:]}")
    _two_pass_loudnorm(pre_mix, out_mp3)
    pre_mix.unlink(missing_ok=True)
    return out_mp3


def _build_scene_mp4(scene_video: Path, scene_audio: Path, out_path: Path,
                     *, fps: int = 24) -> Path:
    """Mux the silent stretched clip with the loudness-normalized scene audio."""
    if out_path.exists() and out_path.stat().st_size > 1024:
        return out_path
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
        "-i", str(scene_video),
        "-i", str(scene_audio),
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
        "-r", str(fps),
        "-c:a", "aac", "-b:a", "192k",
        "-pix_fmt", "yuv420p",
        "-shortest",
        "-movflags", "+faststart",
        str(out_path),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise LFRenderError(f"scene mux failed: {r.stderr[-400:]}")
    return out_path


def _split_chapter_to_scenes(chapter: dict, scenes_per_chapter: int) -> list[dict]:
    """Split chapter narration into N scene-sized chunks, paired with
    scene_prompts so each scene has a still-prompt + narration bite.

    Grok produces N scene_prompts per chapter alongside one big narration
    blob. We split narration on sentence boundaries into roughly N equal
    chunks so each scene has its own VO line. If we have fewer sentences
    than scene_prompts, repeat the last narration; if more, glue extras
    onto the final scene."""
    narration = (chapter.get("narration") or "").strip()
    prompts = list(chapter.get("scene_prompts") or [])
    if not prompts:
        return []
    sents = re.split(r"(?<=[\.!?])\s+", narration) if narration else []
    if not sents:
        return [
            {"scene_prompt": p, "narration": "", "duration_target_sec": 5.0}
            for p in prompts
        ]
    n_scenes = len(prompts)
    # Distribute sentences as evenly as possible across N scenes.
    chunks: list[list[str]] = [[] for _ in range(n_scenes)]
    for i, s in enumerate(sents):
        chunks[min(i * n_scenes // max(1, len(sents)), n_scenes - 1)].append(s)
    out: list[dict] = []
    for i, p in enumerate(prompts):
        text = " ".join(chunks[i]).strip()
        # Estimate duration at ~150 wpm + min 3s + max 10s.
        words = len(text.split()) if text else 8
        target = max(3.0, min(10.0, words / 2.5))
        out.append({"scene_prompt": p, "narration": text, "duration_target_sec": target})
    return out


def _process_scene(
    *,
    chapter_index: int,
    local_idx: int,
    global_idx: int,
    scene_brief: dict,
    visual_style: str,
    motion_prompt_hint: str,
    voice_id: str,
    fps: int,
    job_dir: Path,
) -> Path:
    """End-to-end per-scene render. Returns the path to the assembled
    scene MP4 ready for final concat.

    Idempotent — every helper checks for existing output before regenerating.
    """
    sid = f"{chapter_index:02d}_{local_idx:02d}_{global_idx:04d}"
    stills = job_dir / "stills"
    clips = job_dir / "clips"
    vo = job_dir / "audio" / "vo"
    sfx = job_dir / "audio" / "sfx"
    scenes = job_dir / "scenes"
    for d in (stills, clips, vo, sfx, scenes):
        d.mkdir(parents=True, exist_ok=True)

    # 1. Still (seedream)
    still = stills / f"scene_{global_idx:04d}.png"
    _gen_em_still(scene_brief["scene_prompt"], visual_style, still)

    # 2. LTX clip (i2v)
    clip = clips / f"clip_{sid}.mp4"
    motion = (motion_prompt_hint or "").strip() or "slow camera push-in, subtle parallax"
    _gen_em_clip(still, motion, clip, duration_sec=5)

    # 3. VO (ElevenLabs)
    vo_raw = vo / f"vo_{sid}_raw.mp3"
    text = scene_brief.get("narration") or ""
    if not text.strip():
        # Skip-VO scenes get a 2s silence + still + clip duration default.
        target_sec = 4.0
        # Generate a tiny silence MP3 placeholder so the rest of the pipeline
        # has a file to mux.
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
            "-f", "lavfi", "-i", "anullsrc=r=44100:cl=mono",
            "-t", f"{target_sec:.2f}",
            "-c:a", "libmp3lame", "-b:a", "128k",
            str(vo_raw),
        ]
        subprocess.run(cmd, capture_output=True)
    else:
        _gen_em_vo(text, vo_raw, voice_id=voice_id)

    # 4. Silence-kill the VO
    vo_clean = vo / f"vo_{sid}_clean.mp3"
    _silence_kill(vo_raw, vo_clean)
    vo_dur = _ffprobe_dur(vo_clean)
    if vo_dur < 1.0:
        # Pad to at least 2s so the scene doesn't flicker by.
        vo_dur = 2.0

    # 5. Per-scene SFX
    sfx_path = sfx / f"sfx_{sid}.mp3"
    sfx_prompt = _derive_sfx_prompt(scene_brief["scene_prompt"], text)
    _gen_scene_sfx(sfx_prompt, vo_dur + 1.0, sfx_path)

    # 6. Mix + 2-pass loudnorm
    audio_final = vo / f"audio_{sid}.mp3"
    _mix_vo_sfx_loudnorm(vo_clean, sfx_path, audio_final, duration_sec=vo_dur)

    # 7. Stretch the clip to match VO duration
    clip_stretched = clips / f"clip_{sid}_stretched.mp4"
    _stretch_video_to_duration(clip, clip_stretched, vo_dur)

    # 8. Mux into final scene MP4
    scene_mp4 = scenes / f"scene_{sid}.mp4"
    _build_scene_mp4(clip_stretched, audio_final, scene_mp4, fps=fps)
    return scene_mp4


def _final_concat_v5(scene_mp4s: list[Path], out_path: Path,
                     *, fade_out_sec: float = 3.0, fps: int = 24) -> Path:
    """Concat all scene MP4s + add final fade-to-black.

    NOTE: Using simple concat-demuxer here (no inter-scene crossfades).
    Casey's build_episode_v5 uses constant-power crossfades — those need
    filter_complex acrossfade which is complex enough to defer to a
    follow-up PR. The plain-concat result is still broadcast-grade.
    """
    if not scene_mp4s:
        raise LFRenderError("no scene MP4s to concat")
    # Step 1: concat-demuxer to a tmp.
    tmp = out_path.with_name(out_path.stem + "_tmp.mp4")
    list_file = out_path.with_suffix(".list.txt")
    list_file.write_text(
        "\n".join(f"file '{str(p.resolve()).replace(chr(92), '/')}'" for p in scene_mp4s),
        encoding="utf-8",
    )
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
        "-f", "concat", "-safe", "0", "-i", str(list_file),
        "-c", "copy",
        str(tmp),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    list_file.unlink(missing_ok=True)
    if r.returncode != 0:
        raise LFRenderError(f"v5 concat-demuxer failed: {r.stderr[-400:]}")

    # Step 2: re-encode with fade-to-black + faststart.
    total = _ffprobe_dur(tmp)
    if total <= 0:
        raise LFRenderError("v5 concat produced zero-duration file")
    fade_start = max(0.0, total - fade_out_sec)
    cmd2 = [
        "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
        "-i", str(tmp),
        "-vf", f"fade=t=out:st={fade_start:.3f}:d={fade_out_sec:.3f}",
        "-af", f"afade=t=out:st={fade_start:.3f}:d={fade_out_sec:.3f}",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "20",
        "-c:a", "aac", "-b:a", "192k",
        "-pix_fmt", "yuv420p",
        "-r", str(fps),
        "-movflags", "+faststart",
        str(out_path),
    ]
    r2 = subprocess.run(cmd2, capture_output=True, text=True)
    tmp.unlink(missing_ok=True)
    if r2.returncode != 0:
        raise LFRenderError(f"v5 final encode (fade) failed: {r2.stderr[-400:]}")
    return out_path


# ─────────────────────────────────────────────────────────────────────────────
# v5_episode orchestrator
# ─────────────────────────────────────────────────────────────────────────────

async def run_v5_episode_pipeline(
    job_id: str,
    channel: dict,
    outline: dict,
    *,
    scenes_per_chapter: int = 12,
    wpm: int = 150,
) -> None:
    """End-to-end EM v5 cinematic episode render. Updates state + status
    at every phase boundary so the frontend's poll loop reports progress.

    Per-chapter expansion → per-scene LTX i2v + ElevenLabs VO + silence-kill
    + mmaudio SFX + 2-pass loudnorm + scene mux → final concat with
    fade-to-black. Cost envelope ~$50.76/episode (Wirecard locked number).
    """
    job_dir = _ensure_job_dir(job_id)
    state = load_state(job_id) or {}
    state.update({
        "job_id": job_id,
        "channel_key": channel.get("key"),
        "channel_label": channel.get("label"),
        "pipeline_kind": "v5_episode",
        "outline": outline,
        "phase": "starting",
        "percent": 0,
        "started_at": time.time(),
        "scenes_per_chapter": scenes_per_chapter,
        "wpm": wpm,
    })
    save_state(job_id, state)
    update_status(job_id, phase="starting", percent=0)

    loop = asyncio.get_running_loop()
    visual_style = channel.get("visual_style") or ""
    fps = int(channel.get("fps") or 24)

    # ── Phase 1 — chapters ────────────────────────────────────────────────
    update_status(job_id, phase="chapters", percent=2)
    state["phase"] = "chapters"
    save_state(job_id, state)

    from skeleton_ai.scripting_grok import GrokClient
    grok = GrokClient()

    chapters_path = _chapters_path(job_id)
    if chapters_path.exists():
        try:
            chapters_data = json.loads(chapters_path.read_text(encoding="utf-8"))
            chapters_done = list(chapters_data.get("chapters") or [])
        except Exception:
            chapters_done = []
    else:
        chapters_done = []
    chapter_count = len(outline.get("chapters") or [])
    done_indices = {int(c.get("chapter_index", -1)) for c in chapters_done}

    for ch_idx in range(chapter_count):
        if ch_idx in done_indices:
            continue
        result = await loop.run_in_executor(
            None,
            lambda i=ch_idx: _gen_chapter(
                grok, channel=channel, outline=outline,
                chapter_index=i, chapter_count=chapter_count,
                scenes_per_chapter=scenes_per_chapter, wpm=wpm,
            ),
        )
        chapters_done.append(result)
        chapters_data = {
            "outline_title": outline.get("title", ""),
            "chapters": sorted(chapters_done, key=lambda c: int(c.get("chapter_index", 0))),
        }
        chapters_path.write_text(json.dumps(chapters_data, indent=2, ensure_ascii=True), encoding="utf-8")
        pct = 2 + int(8 * (ch_idx + 1) / max(1, chapter_count))
        update_status(job_id, phase="chapters", percent=pct,
                      chapter_done=ch_idx + 1, chapter_total=chapter_count)
        state["percent"] = pct
        save_state(job_id, state)

    chapters_data = json.loads(chapters_path.read_text(encoding="utf-8"))
    chapters = chapters_data["chapters"]

    # Build full scene-brief list (chapter * scene_prompt → flat list with
    # per-scene narration). Total scenes = chapters × scenes_per_chapter.
    scene_briefs: list[tuple[int, int, int, dict]] = []
    for ch in chapters:
        ch_idx = int(ch.get("chapter_index", 0))
        per_scene = _split_chapter_to_scenes(ch, scenes_per_chapter)
        for local_idx, sb in enumerate(per_scene):
            global_idx = ch_idx * scenes_per_chapter + local_idx
            scene_briefs.append((ch_idx, local_idx, global_idx, sb))

    # ── Phase 2 — STILLS ONLY (per-scene approval gate, PR #127) ──────────
    # Renders just the seedream still for every scene. The expensive parts
    # of v5 (LTX i2v + ElevenLabs VO + mmaudio SFX + 2-pass loudnorm + scene
    # mux + final concat) DON'T run until Casey approves the still gallery
    # and POSTs to /jobs/{id}/finalize. Spends only ~$1-3 fal here vs the
    # ~$48 of the full v5 episode.
    update_status(job_id, phase="scenes", percent=12)
    state["phase"] = "scenes"
    state["scene_briefs"] = [
        # Persist so finalize + regenerate can rebuild the same plan.
        {"chapter_index": ci, "local_idx": li, "global_idx": gi, "brief": sb}
        for ci, li, gi, sb in scene_briefs
    ]
    save_state(job_id, state)

    total = len(scene_briefs)
    stills_dir = job_dir / "stills"
    stills_dir.mkdir(parents=True, exist_ok=True)

    def _gen_one_still(triple):
        ci, li, gi, sb = triple
        out = stills_dir / f"scene_{gi:04d}.png"
        _gen_em_still(sb["scene_prompt"], visual_style, out)
        return gi

    # Stills-only concurrency = 6 (each is one seedream call ~5-10s; lower
    # API surface than the full per-scene pipeline so we can parallelize more).
    STILLS_CONCURRENCY = 6

    def _run_stills_pool() -> int:
        done_n = 0
        with ThreadPoolExecutor(max_workers=STILLS_CONCURRENCY) as ex:
            futs = {ex.submit(_gen_one_still, t): t for t in scene_briefs}
            for fut in as_completed(futs):
                t = futs[fut]
                gi = t[2]
                try:
                    fut.result()
                    done_n += 1
                    if done_n % 3 == 0 or done_n == total:
                        pct = 12 + int(60 * done_n / max(1, total))
                        update_status(job_id, phase="scenes", percent=pct,
                                      scene_done=done_n, scene_total=total)
                except Exception as e:
                    print(f"[v5 stills] scene {gi} failed: {e}")
        return done_n

    done_n = await loop.run_in_executor(None, _run_stills_pool)
    state["scenes_generated"] = done_n
    state["percent"] = 72
    save_state(job_id, state)
    update_status(job_id, phase="scenes", percent=72,
                  scene_done=done_n, scene_total=total)

    # ── Pause for per-scene approval ──────────────────────────────────────
    # finalize_v5_episode_pipeline (kicked by POST /jobs/{id}/finalize)
    # runs the rest: per-scene LTX/EL/SFX/mux + thumbnails + concat.
    state["phase"] = "awaiting_approval"
    state["percent"] = 72
    save_state(job_id, state)
    update_status(job_id, phase="awaiting_approval", percent=72)
    return


async def finalize_v5_episode_pipeline(job_id: str) -> None:
    """Continue a paused v5_episode render: per-scene LTX i2v + ElevenLabs
    VO + silence-kill + mmaudio SFX + 2-pass loudnorm + scene mux + final
    concat with fade-to-black. Loads state + chapters from disk so the
    pipeline survives process restart between approval and finalize."""
    state = load_state(job_id)
    if not state:
        raise LFRenderError(f"no state for job {job_id}")
    # Allowed: awaiting_approval (normal), failed (rerun), AND any of the
    # finalize phases themselves so a stalled / restarted finalize can pick
    # up where it left off (PR #128 — fal mmaudio-v2 422'd every SFX call
    # which left jobs stuck in scene_assembly with partial output).
    if state.get("phase") not in (
        "awaiting_approval", "scene_assembly", "i2v", "vo", "sfx",
        "compose", "thumbnails", "failed", "cancelled",
    ):
        raise LFRenderError(
            f"job {job_id} is in phase {state.get('phase')!r}; "
            "v5 finalize requires awaiting_approval (or resume from a "
            "stalled finalize / failure)"
        )

    from long_form.prompts.channels import get_channel
    channel = get_channel(state["channel_key"])
    outline = state.get("outline") or {}
    job_dir = _ensure_job_dir(job_id)
    visual_style = channel.get("visual_style") or ""
    fps = int(channel.get("fps") or 24)
    voice_id = (channel.get("voice_id_default") or "").strip()
    motion_hint = (channel.get("motion_prompt_default") or "").strip()

    scene_brief_records = state.get("scene_briefs") or []
    if not scene_brief_records:
        raise LFRenderError("scene_briefs missing from state — cannot finalize")
    # Reconstitute as the (ci, li, gi, brief) tuples the pool expects.
    scene_briefs = [
        (int(r["chapter_index"]), int(r["local_idx"]), int(r["global_idx"]), r["brief"])
        for r in scene_brief_records
    ]
    total = len(scene_briefs)

    loop = asyncio.get_running_loop()

    # ── Per-scene assembly (i2v + VO + SFX + 2-pass + mux per scene) ──────
    update_status(job_id, phase="scene_assembly", percent=73)
    state["phase"] = "scene_assembly"
    save_state(job_id, state)

    def _process_one(triple):
        ch_idx, local_idx, global_idx, sb = triple
        return global_idx, _process_scene(
            chapter_index=ch_idx, local_idx=local_idx, global_idx=global_idx,
            scene_brief=sb, visual_style=visual_style,
            motion_prompt_hint=motion_hint, voice_id=voice_id, fps=fps,
            job_dir=job_dir,
        )

    # Same SCENE_CONCURRENCY as the original full-pipeline run (each scene
    # = ~3 remaining fal calls now since the still already exists).
    SCENE_CONCURRENCY = 3

    def _run_pool() -> dict[int, Path]:
        out: dict[int, Path] = {}
        done_n = 0
        with ThreadPoolExecutor(max_workers=SCENE_CONCURRENCY) as ex:
            futs = {ex.submit(_process_one, t): t for t in scene_briefs}
            for fut in as_completed(futs):
                t = futs[fut]
                gi = t[2]
                try:
                    gi_ret, mp4 = fut.result()
                    out[gi_ret] = mp4
                    done_n += 1
                    pct = 73 + int(15 * done_n / max(1, total))
                    update_status(job_id, phase="scene_assembly", percent=pct,
                                  scene_done=done_n, scene_total=total)
                except Exception as e:
                    print(f"[v5 finalize] scene {gi} failed: {e}")
        return out

    scene_mp4s_indexed = await loop.run_in_executor(None, _run_pool)
    scene_mp4s = [scene_mp4s_indexed[gi] for _, _, gi, _ in scene_briefs if gi in scene_mp4s_indexed]
    state["scene_mp4s_assembled"] = len(scene_mp4s)
    state["percent"] = 88
    save_state(job_id, state)

    # ── Phase 6 — thumbnails ──────────────────────────────────────────────
    update_status(job_id, phase="thumbnails", percent=89)
    state["phase"] = "thumbnails"
    save_state(job_id, state)
    thumbs = await loop.run_in_executor(
        None, lambda: _gen_thumbnails(channel, outline, job_dir / "thumbnails", count=3)
    )
    state["thumbnails_generated"] = len(thumbs)
    state["percent"] = 92
    save_state(job_id, state)

    # ── Phase 7 — final concat + fade ─────────────────────────────────────
    update_status(job_id, phase="compose", percent=93)
    state["phase"] = "compose"
    save_state(job_id, state)

    title_slug = _slugify(outline.get("title", "longform"))
    out_mp4 = _final_mp4_path(job_id, title_slug)
    await loop.run_in_executor(
        None,
        lambda: _final_concat_v5(scene_mp4s, out_mp4, fade_out_sec=3.0, fps=fps),
    )

    state["mp4_path"] = str(out_mp4.relative_to(LF_OUTPUT_ROOT))
    state["mp4_duration_sec"] = _ffprobe_dur(out_mp4)
    state["mp4_size_bytes"] = out_mp4.stat().st_size
    state["phase"] = "done"
    state["percent"] = 100
    state["finished_at"] = time.time()
    save_state(job_id, state)
    update_status(job_id, phase="done", percent=100)


def regenerate_v5_still(job_id: str, scene_idx: int,
                        new_prompt: str | None = None) -> Path:
    """Regenerate one v5_episode still. Updates scene_briefs in state.json
    if a new prompt is provided so future re-runs (and the eventual
    finalize phase) use it."""
    state = load_state(job_id)
    if not state:
        raise LFRenderError(f"no state for job {job_id}")
    scene_briefs = state.get("scene_briefs") or []
    target = next((r for r in scene_briefs if int(r.get("global_idx", -1)) == scene_idx), None)
    if not target:
        raise LFRenderError(f"scene_idx {scene_idx} not in scene_briefs")
    brief = target.get("brief") or {}
    prompt = (new_prompt or brief.get("scene_prompt") or "").strip()
    if not prompt:
        raise LFRenderError("prompt cannot be empty")
    if new_prompt and new_prompt != brief.get("scene_prompt"):
        target["brief"]["scene_prompt"] = new_prompt
        state["scene_briefs"] = scene_briefs
        save_state(job_id, state)
    from long_form.prompts.channels import get_channel
    channel = get_channel(state["channel_key"])
    visual_style = channel.get("visual_style") or ""
    out = _job_dir(job_id) / "stills" / f"scene_{scene_idx:04d}.png"
    if out.exists():
        out.unlink()
    return _gen_em_still(prompt, visual_style, out)
