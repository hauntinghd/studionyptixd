"""
ZeroTier (Private) render pipeline.

Takes the Grok-generated 8-beat script JSON and renders the final MP4 using
the validated canonical pipeline:

  - seedream v4.5 cel-shaded comic stills (one per scene)
  - LTX 13B i2v with Pixverse V6 fallback (skeleton_ai.i2v_engine handles this)
  - MiniMax `English_Trustworthy_Man` narration
  - ffmpeg compose with lowercase per-scene drawtext captions + concat-demuxer
    + final mux

Mirrors `D:/recaps/ZeroTier/short14/build_short14.py` (which Casey already
validated end-to-end on 2026-05-07) but as a callable that takes the script
JSON instead of a hard-coded SCENES dict.

Phase 2b ships this synchronously — request-response pattern. The HTTP
request stays open ~5-10 minutes while rendering. Future Phase 2b.5 adds
a job-queue wrapper for non-blocking renders.
"""
from __future__ import annotations
import os
import re
import json
import subprocess
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import httpx
import fal_client


SEEDREAM_URL = "https://fal.run/fal-ai/bytedance/seedream/v4.5/text-to-image"
TTS_ENDPOINT = "fal-ai/minimax/speech-02-hd"

# Visual style block — matches build_short14.py exactly so renders are
# visually consistent with Casey's already-uploaded shorts.
COMIC_STYLE = (
    "Modern DC comics dynamic art style. Bold confident black linework, "
    "high-contrast cel-shaded coloring, strong primary color blocks, dramatic "
    "chiaroscuro lighting, kinetic action-pose composition, speed lines and "
    "motion streaks where appropriate. Comic-panel cinematic framing. "
    "Aesthetic reference: Jim Lee Justice League, Francis Manapul Flash run, "
    "Greg Capullo Batman. Premium digital comic illustration grade. "
    "NOT photorealistic, NOT photograph. Comic-book art only. "
)

WALLY_CHAR = (
    "Wally West (red Flash costume — red bodysuit with golden lightning bolt "
    "insignia centered on chest, golden boots, red cowl with golden lightning "
    "earpieces, white eye lenses, athletic adult build). "
)

NEG_STILL = (
    "text, watermark, logo, blur, low quality, deformed hands, extra fingers, "
    "facial features wrong, multiple heads, child, "
    "photograph, photo, photorealistic 3D render, plasticky skin, AI photo, "
    "skeleton, anatomical bones, ribcage"
)
NEG_VIDEO = (
    "blur, low quality, jitter, warping, flicker, text overlays, "
    "frozen pose, identity drift"
)


class ZTRenderError(RuntimeError):
    pass


def _slugify(s: str, max_len: int = 16) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", str(s or "").lower()).strip("_")
    return (s[:max_len].rstrip("_")) or "scene"


def _ensure_fal():
    key = os.getenv("FAL_AI_KEY", "").strip()
    if not key:
        raise ZTRenderError("FAL_AI_KEY not set in env")
    os.environ["FAL_KEY"] = key
    return key


def _normalize_scenes(script_json: Any) -> tuple[list[dict], str]:
    """Parse the Grok output into a normalized list of scene dicts.

    Returns (scenes, title). Each scene has:
        id, caption, narration, duration, visual_prompt, motion_prompt
    """
    if isinstance(script_json, str):
        try:
            data = json.loads(script_json)
        except Exception as e:
            raise ZTRenderError(f"script JSON unparseable: {e}; raw start: {script_json[:200]}")
    elif isinstance(script_json, dict):
        data = script_json
    else:
        raise ZTRenderError(f"script must be JSON object or string, got {type(script_json).__name__}")

    title = str(data.get("title", "") or "").strip() or "ZeroTier Short"
    raw_scenes = list(data.get("scenes") or [])
    if not raw_scenes:
        raise ZTRenderError("script has no scenes")

    out: list[dict] = []
    for i, raw in enumerate(raw_scenes):
        if not isinstance(raw, dict):
            continue
        narration = str(raw.get("narration", "") or "").strip()
        if not narration:
            continue
        caption_raw = str(raw.get("text_overlay", "") or "").strip().lower()
        caption = re.sub(r"\s+", " ", caption_raw)[:60] or _slugify(narration[:30])
        visual = str(raw.get("visual_description", "") or "").strip()
        if not visual:
            visual = narration  # fall back to narration as visual seed
        duration = float(raw.get("duration_sec", 0) or 0) or 4.0
        duration = max(2.5, min(8.0, duration))
        sid = f"{i:02d}_{_slugify(caption or narration[:30])}"

        # Build the seedream prompt: COMIC_STYLE + WALLY_CHAR (if Wally
        # appears in the visual) + the visual description from Grok.
        wally_prefix = WALLY_CHAR if "wally" in visual.lower() else ""
        full_visual_prompt = (
            f"{COMIC_STYLE}Vertical 9:16 frame. {wally_prefix}{visual}"
        )

        # Motion prompt: lift the visual_description verbatim — Pixverse
        # handles long descriptive prompts well.
        motion_prompt = visual

        out.append({
            "id": sid,
            "caption": caption,
            "narration": narration,
            "duration": duration,
            "visual_prompt": full_visual_prompt,
            "motion_prompt": motion_prompt,
        })

    if not out:
        raise ZTRenderError("no usable scenes after normalization")
    return out, title


def _save_url(url: str, path: Path) -> None:
    if path.exists() and path.stat().st_size > 1024:
        return
    with httpx.stream("GET", url, timeout=180) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=1024 * 256):
                f.write(chunk)


def _gen_still(scene: dict, stills_dir: Path) -> Path:
    out = stills_dir / f"{scene['id']}.png"
    if out.exists() and out.stat().st_size > 1024:
        return out
    payload = {
        "prompt": scene["visual_prompt"][:3500],
        "negative_prompt": NEG_STILL,
        "image_size": {"width": 720, "height": 1280},
        "num_images": 1,
        "guidance_scale": 5.5,
        "num_inference_steps": 50,
        "enable_safety_checker": True,
    }
    fal_key = os.environ["FAL_KEY"]
    with httpx.Client(timeout=240, follow_redirects=True) as c:
        r = c.post(SEEDREAM_URL, json=payload, headers={
            "Authorization": f"Key {fal_key}",
            "Content-Type": "application/json",
        })
    if r.status_code not in (200, 201):
        raise ZTRenderError(f"seedream {r.status_code} for {scene['id']}: {r.text[:300]}")
    images = r.json().get("images") or []
    if not images:
        raise ZTRenderError(f"seedream returned no images for {scene['id']}")
    _save_url(images[0]["url"], out)
    return out


def _gen_clip_pixverse(scene: dict, still_path: Path, clips_dir: Path) -> Path:
    """Pixverse V6 i2v at 720p 5s 9:16 — proven fallback (LTX has been
    intermittently degraded in 2026-05). Pixverse pricing: $0.045/sec at
    720p without audio = ~$0.225/clip."""
    out = clips_dir / f"{scene['id']}.mp4"
    if out.exists() and out.stat().st_size > 1024:
        return out
    PIXVERSE = "https://queue.fal.run/fal-ai/pixverse/v6/image-to-video"
    fal_key = os.environ["FAL_KEY"]
    HDR = {"Authorization": f"Key {fal_key}", "Content-Type": "application/json"}
    image_url = fal_client.upload_file(str(still_path))
    sub_r = httpx.post(PIXVERSE, headers=HDR, json={
        "prompt": scene["motion_prompt"][:1500],
        "image_url": image_url,
        "aspect_ratio": "9:16",
        "resolution": "720p",
        "duration": "5",
        "negative_prompt": NEG_VIDEO,
    }, timeout=30)
    if sub_r.status_code != 200:
        raise ZTRenderError(f"pixverse submit {sub_r.status_code}: {sub_r.text[:200]}")
    sub = sub_r.json()
    deadline = time.time() + 240
    while time.time() < deadline:
        s = httpx.get(sub["status_url"], headers=HDR, timeout=15).json()
        st = s.get("status")
        if st == "COMPLETED":
            rr = httpx.get(sub["response_url"], headers=HDR, timeout=30).json()
            v = (rr.get("video") or {}).get("url") or rr.get("video_url")
            _save_url(v, out)
            return out
        if st in ("FAILED", "ERROR"):
            raise ZTRenderError(f"pixverse {st} for {scene['id']}: {s}")
        time.sleep(5)
    raise ZTRenderError(f"pixverse timeout for {scene['id']} after 240s")


def _gen_vo(scenes: list[dict], vo_dir: Path) -> Path:
    out = vo_dir / "narration.mp3"
    if out.exists() and out.stat().st_size > 1024:
        return out
    full_text = " ".join(s["narration"] for s in scenes).strip()
    result = fal_client.subscribe(
        TTS_ENDPOINT,
        arguments={
            "text": full_text,
            "voice_setting": {
                "voice_id": "English_Trustworthy_Man",
                "speed": 0.95,
                "emotion": "neutral",
            },
            "audio_setting": {
                "sample_rate": 32000,
                "bitrate": 128000,
                "format": "mp3",
            },
        },
    )
    audio_url = (
        result.get("audio", {}).get("url")
        if isinstance(result.get("audio"), dict)
        else result.get("audio_url")
    )
    if not audio_url:
        raise ZTRenderError(f"minimax returned no audio: {result}")
    _save_url(audio_url, out)
    return out


def _compose(scenes: list[dict], clips: list[Path], vo: Path,
             workspace: Path, out_path: Path) -> Path:
    trimmed_dir = workspace / "trimmed"
    trimmed_dir.mkdir(exist_ok=True)
    trimmed: list[Path] = []
    for scene, clip in zip(scenes, clips):
        sid = scene["id"]
        duration = scene["duration"]
        caption = scene["caption"]
        trimmed_path = trimmed_dir / f"{sid}.mp4"
        # Escape single quotes + colons + commas for ffmpeg drawtext.
        caption_text = (
            caption.replace("\\", "\\\\")
                   .replace("'", "’")  # smart-quote substitution; visually identical
                   .replace(":", r"\:")
                   .replace(",", r"\,")
        )
        font_path = "C\\:/Windows/Fonts/arialbd.ttf"
        clean_len = len(caption)
        font_size = 68 if clean_len <= 16 else (56 if clean_len <= 22 else 46)
        drawtext = (
            f"drawtext=fontfile='{font_path}':text='{caption_text}':"
            f"fontsize={font_size}:fontcolor=white:bordercolor=black:borderw=5:"
            f"x=(w-text_w)/2:y=h*0.82"
        )
        cmd = [
            "ffmpeg", "-y", "-loglevel", "error",
            "-i", str(clip),
            "-t", f"{duration}",
            "-vf", (
                f"scale=720:1280:force_original_aspect_ratio=decrease,"
                f"pad=720:1280:(ow-iw)/2:(oh-ih)/2:black,fps=30,{drawtext}"
            ),
            "-an",
            "-c:v", "libx264",
            "-preset", "medium",
            "-crf", "20",
            "-pix_fmt", "yuv420p",
            str(trimmed_path),
        ]
        subprocess.run(cmd, check=True, capture_output=True)
        trimmed.append(trimmed_path)

    concat_list = workspace / "concat.txt"
    with open(concat_list, "w", encoding="utf-8") as f:
        for t in trimmed:
            f.write(f"file '{t.as_posix()}'\n")
    silent = workspace / "silent_combined.mp4"
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-f", "concat", "-safe", "0", "-i", str(concat_list),
        "-c", "copy",
        str(silent),
    ], check=True, capture_output=True)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", str(silent),
        "-i", str(vo),
        "-map", "0:v:0", "-map", "1:a:0",
        "-c:v", "copy",
        "-c:a", "aac", "-b:a", "192k",
        "-shortest",
        str(out_path),
    ], check=True, capture_output=True)
    return out_path


def render_stills_only(
    *,
    script_json: Any,
    workspace: Path,
) -> dict:
    """Phase 4.5a: render stills only. Returns scene metadata + per-scene
    still paths so the user can preview + approve before animation burns
    Pixverse credits.

    Stills cache by scene_id, so a regen of one scene only re-pays for that
    scene (the others stay cached).
    """
    _ensure_fal()
    workspace = Path(workspace)
    stills_dir = workspace / "stills"
    stills_dir.mkdir(parents=True, exist_ok=True)

    scenes, title = _normalize_scenes(script_json)

    # Persist the normalized scenes so finalize can read them later.
    scenes_path = workspace / "scenes.json"
    scenes_path.write_text(
        json.dumps({"title": title, "scenes": scenes}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    stills: list[Path] = [None] * len(scenes)  # type: ignore
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(_gen_still, s, stills_dir): i for i, s in enumerate(scenes)}
        for fut in as_completed(futs):
            stills[futs[fut]] = fut.result()

    return {
        "title": title,
        "scene_count": len(scenes),
        "scenes": [
            {
                "scene_index": i,
                "scene_id": s["id"],
                "caption": s["caption"],
                "narration": s["narration"],
                "duration": s["duration"],
                "visual_prompt_preview": (s["visual_prompt"][:200] + ("…" if len(s["visual_prompt"]) > 200 else "")),
                "still_filename": stills[i].name,
            }
            for i, s in enumerate(scenes)
        ],
        "fal_cost_estimate_usd_so_far": round(len(scenes) * 0.04, 2),
    }


def regenerate_one_still(
    *,
    workspace: Path,
    scene_index: int,
    custom_prompt: str | None = None,
) -> dict:
    """Phase 4.5a: regenerate a SINGLE still for an existing job. Reads the
    persisted scenes.json, optionally overrides the visual_prompt with the
    user's edit, deletes the cached still, and re-renders.
    """
    _ensure_fal()
    workspace = Path(workspace)
    scenes_path = workspace / "scenes.json"
    if not scenes_path.exists():
        raise ZTRenderError(f"scenes.json not found in {workspace}; run render-stills first")
    raw = json.loads(scenes_path.read_text(encoding="utf-8"))
    scenes = raw.get("scenes") or []
    if scene_index < 0 or scene_index >= len(scenes):
        raise ZTRenderError(f"scene_index {scene_index} out of range (have {len(scenes)} scenes)")

    scene = dict(scenes[scene_index])
    if custom_prompt and custom_prompt.strip():
        scene["visual_prompt"] = (
            f"{COMIC_STYLE}Vertical 9:16 frame. {custom_prompt.strip()[:3000]}"
        )
        # Update persisted scenes.json so subsequent renders use the new prompt.
        scenes[scene_index]["visual_prompt"] = scene["visual_prompt"]
        scenes_path.write_text(
            json.dumps({"title": raw.get("title", ""), "scenes": scenes}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    stills_dir = workspace / "stills"
    stills_dir.mkdir(parents=True, exist_ok=True)
    out = stills_dir / f"{scene['id']}.png"
    if out.exists():
        out.unlink()  # force regen
    new_path = _gen_still(scene, stills_dir)
    return {
        "scene_index": scene_index,
        "scene_id": scene["id"],
        "still_filename": new_path.name,
        "fal_cost_estimate_usd": 0.04,
    }


def render_finalize(
    *,
    workspace: Path,
    final_filename: str = "ZeroTier_short.mp4",
) -> dict:
    """Phase 4.5a: given an existing job with all 8 stills approved, run
    the i2v + TTS + compose stages to produce the final MP4.

    Reads scenes.json + stills/ from the workspace.
    """
    _ensure_fal()
    workspace = Path(workspace)
    scenes_path = workspace / "scenes.json"
    if not scenes_path.exists():
        raise ZTRenderError(f"scenes.json not found in {workspace}; run render-stills first")
    raw = json.loads(scenes_path.read_text(encoding="utf-8"))
    scenes = raw.get("scenes") or []
    title = raw.get("title", "ZeroTier short")
    stills_dir = workspace / "stills"
    clips_dir = workspace / "clips"
    vo_dir = workspace / "vo"
    clips_dir.mkdir(exist_ok=True)
    vo_dir.mkdir(exist_ok=True)

    # Verify all stills exist
    stills: list[Path] = []
    for s in scenes:
        p = stills_dir / f"{s['id']}.png"
        if not p.exists():
            raise ZTRenderError(f"still missing for scene {s['id']} — generate first")
        stills.append(p)

    # i2v in parallel
    clips: list[Path] = [None] * len(scenes)  # type: ignore
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(_gen_clip_pixverse, scenes[i], stills[i], clips_dir): i for i in range(len(scenes))}
        for fut in as_completed(futs):
            clips[futs[fut]] = fut.result()

    vo = _gen_vo(scenes, vo_dir)
    final_mp4 = workspace / final_filename
    _compose(scenes, clips, vo, workspace, final_mp4)

    fal_cost_est = round(
        len(scenes) * 0.225 + 0.10, 2  # Pixverse + MiniMax (stills already paid)
    )

    return {
        "mp4_path": str(final_mp4),
        "title": title,
        "scene_count": len(scenes),
        "duration_total_sec": round(sum(s["duration"] for s in scenes), 1),
        "fal_cost_estimate_usd": fal_cost_est,
        "scenes": [{"id": s["id"], "caption": s["caption"], "duration": s["duration"]} for s in scenes],
    }


def render_zerotier_short(
    *,
    script_json: Any,
    workspace: Path,
    final_filename: str = "ZeroTier_short.mp4",
) -> dict:
    """Run the full canonical ZeroTier render pipeline (monolithic).

    Returns a dict with keys: mp4_path (Path), title, duration_total_sec,
    scene_count, fal_cost_estimate_usd.
    """
    _ensure_fal()
    workspace = Path(workspace)
    stills_dir = workspace / "stills"
    clips_dir = workspace / "clips"
    vo_dir = workspace / "vo"
    for d in (workspace, stills_dir, clips_dir, vo_dir):
        d.mkdir(parents=True, exist_ok=True)

    scenes, title = _normalize_scenes(script_json)

    # Stills: parallel up to 4 workers (matches build_short14.py). seedream
    # is fast (~30-50s each) so this gets the whole batch done in ~60s.
    stills: list[Path] = [None] * len(scenes)  # type: ignore
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(_gen_still, s, stills_dir): i for i, s in enumerate(scenes)}
        for fut in as_completed(futs):
            stills[futs[fut]] = fut.result()

    # Clips: parallel up to 3 workers (Pixverse handles 3 concurrent fine).
    clips: list[Path] = [None] * len(scenes)  # type: ignore
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(_gen_clip_pixverse, scenes[i], stills[i], clips_dir): i for i in range(len(scenes))}
        for fut in as_completed(futs):
            clips[futs[fut]] = fut.result()

    vo = _gen_vo(scenes, vo_dir)

    final_mp4 = workspace / final_filename
    _compose(scenes, clips, vo, workspace, final_mp4)

    # Cost estimate: ~$0.04 seedream still + ~$0.225 Pixverse clip + $0.10
    # MiniMax (TTS, ~600 chars). Roughly $2.20 for 8 scenes.
    fal_cost_est = round(
        len(scenes) * 0.04 + len(scenes) * 0.225 + 0.10, 2
    )

    return {
        "mp4_path": str(final_mp4),
        "title": title,
        "scene_count": len(scenes),
        "duration_total_sec": round(sum(s["duration"] for s in scenes), 1),
        "fal_cost_estimate_usd": fal_cost_est,
        "scenes": [{"id": s["id"], "caption": s["caption"], "duration": s["duration"]} for s in scenes],
    }
