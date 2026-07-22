"""
ZeroTier (Private) render pipeline.

Takes Studio's generated scene JSON and renders the final MP4 using
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
import shutil
import subprocess
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import httpx
import fal_client


SEEDREAM_URL = "https://fal.run/fal-ai/bytedance/seedream/v4.5/text-to-image"

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
        caption_raw = str(raw.get("text_overlay", "") or "").strip()
        # Scene 1 = Pope hook: ALL CAPS, center-frame overlay in compose.
        if i == 0:
            caption = re.sub(r"\s+", " ", caption_raw.upper())[:60] or _slugify(narration[:30]).upper()
        else:
            caption = re.sub(r"\s+", " ", caption_raw.lower())[:60] or _slugify(narration[:30])
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
            "panel_image": str(raw.get("panel_image", "") or "").strip(),
            "comic_ref": str(raw.get("comic_ref", "") or "").strip(),
        })

    if not out:
        raise ZTRenderError("no usable scenes after normalization")
    return out, title


def _probe_duration_sec(path: Path) -> float:
    """Return duration in seconds via ffprobe, or 0.0 if unparseable.

    PR #149 — distinguishes real animated clips (5s expected from
    Pixverse) from corrupt single-frame "MP4s" Pixverse occasionally
    returns when its queue glitches. A real clip is 5.0±0.2s; a
    corrupt one is usually <0.5s or unparseable.
    """
    try:
        res = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(path),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if res.returncode != 0:
            return 0.0
        return float(res.stdout.strip() or 0.0)
    except Exception:
        return 0.0


def _validate_clip_or_delete(path: Path, *, sid: str, expected_sec: float = 5.0) -> None:
    """Verify a Pixverse-downloaded clip is a real video (>= 3s).

    PR #149 — if the clip is corrupt or too short, delete it so the
    cache check at _gen_clip_pixverse re-runs i2v on the next attempt
    instead of returning the junk file. Then raise ZTRenderError so
    the caller knows which scene failed.
    """
    dur = _probe_duration_sec(path)
    # Real Pixverse 5s clips probe at 4.8-5.2s. Corrupt single-frame
    # outputs probe at 0.04s or fail to parse entirely. Set threshold
    # at 3.0s so we don't reject legitimate 4s clips from the rare
    # short-duration request.
    if dur < 3.0:
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass
        raise ZTRenderError(
            f"pixverse returned a corrupt clip for {sid}: probed duration "
            f"{dur:.2f}s (expected ~{expected_sec}s). Cache cleared — retry "
            f"will re-run Pixverse on this scene."
        )


def _save_url(url: str | None, path: Path, *, source: str = "?") -> None:
    """Download a remote URL to disk.

    PR #148 — guard against None URLs. Pixverse / mmaudio / minimax all
    have a thin edge case where the queue reports COMPLETED but the
    response payload is missing the video/audio URL field (transient
    fal-side glitch). Without a guard, `httpx.stream("GET", None, ...)`
    raises 'TypeError: Invalid type for url. Expected str or httpx.URL,
    got <class NoneType>: None' — opaque to Casey, who just sees the
    polling timeout. This explicit check converts that into a clean
    ZTRenderError naming WHICH step failed so the resume button knows
    where to retry.
    """
    if path.exists() and path.stat().st_size > 1024:
        return
    if not url or not isinstance(url, str):
        raise ZTRenderError(
            f"missing download url from {source}: got {type(url).__name__} "
            f"(typically fal returned COMPLETED with no video/audio field — "
            f"retry the same step usually works)"
        )
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
    _save_url(images[0].get("url"), out, source=f"seedream still {scene['id']}")
    return out


def _sanitize_motion_via_sonnet(original: str, scene_id: str) -> str:
    """PR #155 — when Pixverse rejects a motion_prompt for
    content_policy_violation, ask Sonnet (via fal any-llm) to rewrite
    it as a tableau-safe version: same narrative intent, no death /
    violence / dying / graphic action verbs. Returns the rewritten
    prompt, or the original if Sonnet call fails (then Ken Burns
    fallback takes over).
    """
    try:
        from skeleton_ai.scripting_grok import GrokClient
        grok = GrokClient()
        system = (
            "You rewrite Pixverse i2v motion prompts that got rejected "
            "by the content checker. Output a SAFE version that:\n"
            "  - Removes any death imagery (dying, last breath, "
            "    deathbed, expiring, passing).\n"
            "  - Removes graphic violence verbs (kills, slays, "
            "    shatters, destroys, rips, shreds).\n"
            "  - Reframes as TABLEAU motion — subtle physical movement, "
            "    no kinetic action, no implied harm.\n"
            "  - Preserves the emotional weight via atmospheric/visual "
            "    cues (wind ripples a banner, golden light shifts, dust "
            "    settles, a hand slowly lifts, eyes close peacefully).\n"
            "  - Keeps a 5-second timeframe and 9:16 vertical context.\n\n"
            "Output ONLY the rewritten motion prompt (one paragraph, "
            "40-100 words). No quotes, no JSON, no preamble."
        )
        user = f"Rewrite this rejected motion prompt as tableau-safe:\n\n{original}"
        out = grok.complete(system, user, max_tokens=400, temperature=0.4)
        cleaned = (out or "").strip().strip('"').strip("'")
        if cleaned and len(cleaned) >= 20:
            print(
                f"  [{scene_id}] motion sanitized via Sonnet — "
                f"new length {len(cleaned)}c",
                flush=True,
            )
            return cleaned
    except Exception as e:
        print(
            f"  [{scene_id}] sonnet sanitize failed ({type(e).__name__}: "
            f"{str(e)[:100]}) — using original",
            flush=True,
        )
    return original


def _ffmpeg_ken_burns_clip(still_path: Path, out_path: Path,
                            duration_sec: float = 5.0) -> Path:
    """PR #155 — final fallback when Pixverse refuses a motion prompt
    twice. Generates a slow Ken Burns pan from the still so the scene
    isn't lost entirely. Same approach we used to rescue the Anti-
    Monitor render's flagged scene 2.
    """
    fps = 30
    total_frames = int(duration_sec * fps)
    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-loop", "1", "-i", str(still_path),
        "-vf", (
            f"scale=1440:2560,"
            f"zoompan="
            f"z='min(1.0+on/{total_frames}*0.06,1.06)':"
            f"x='iw/2-(iw/zoom/2)+on*0.4':"
            f"y='ih/2-(ih/zoom/2)':"
            f"d=1:fps={fps}:s=720x1280"
        ),
        "-t", str(duration_sec),
        "-c:v", "libx264", "-preset", "medium", "-crf", "20",
        "-pix_fmt", "yuv420p", "-an", str(out_path),
    ]
    subprocess.run(cmd, check=True, capture_output=True)
    return out_path


def _pixverse_submit_and_poll(motion_prompt: str, image_url: str,
                                scene_id: str) -> str | None:
    """PR #155 — extracted Pixverse submit + poll loop. Returns the
    video URL on success, or None on content_policy_violation. Raises
    ZTRenderError on any other Pixverse failure (network, FAILED,
    timeout, etc.) so callers can distinguish "retry possible" from
    "infrastructure broken".
    """
    PIXVERSE = "https://queue.fal.run/fal-ai/pixverse/v6/image-to-video"
    fal_key = os.environ["FAL_KEY"]
    HDR = {"Authorization": f"Key {fal_key}", "Content-Type": "application/json"}
    sub_r = httpx.post(PIXVERSE, headers=HDR, json={
        "prompt": motion_prompt[:1500],
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
            if not v:
                # Inspect the rr body — content_policy_violation has a
                # specific shape (detail = list of dicts with
                # type='content_policy_violation').
                detail = rr.get("detail") or []
                is_content_policy = False
                if isinstance(detail, list) and detail:
                    first = detail[0] if isinstance(detail[0], dict) else {}
                    if first.get("type") == "content_policy_violation":
                        is_content_policy = True
                if is_content_policy:
                    print(
                        f"  [{scene_id}] pixverse content_policy_violation",
                        flush=True,
                    )
                    return None  # signal caller to sanitize+retry
                raise ZTRenderError(
                    f"pixverse COMPLETED but returned no video url for "
                    f"{scene_id}; response keys={list(rr.keys())}; "
                    f"raw={str(rr)[:240]}"
                )
            return v
        if st in ("FAILED", "ERROR"):
            raise ZTRenderError(f"pixverse {st} for {scene_id}: {s}")
        time.sleep(5)
    raise ZTRenderError(f"pixverse timeout for {scene_id} after 240s")


def _gen_clip_pixverse(scene: dict, still_path: Path, clips_dir: Path) -> Path:
    """Pixverse V6 i2v at 720p 5s 9:16 — proven fallback (LTX has been
    intermittently degraded in 2026-05). Pixverse pricing: $0.045/sec at
    720p without audio = ~$0.225/clip.

    PR #155 — resilient against Pixverse content_policy_violation:
      1. Try with original motion_prompt
      2. On 422 content-policy, Sonnet-sanitize the prompt and retry
      3. If still rejected, generate a ffmpeg Ken Burns pan from the
         still (always works, no animation but better than no scene)
    """
    out = clips_dir / f"{scene['id']}.mp4"
    if out.exists() and out.stat().st_size > 1024:
        # PR #149 — even cached clips must validate. A corrupt clip
        # from a previous failed run that wrote >1024 bytes would
        # otherwise be silently re-used. _validate_clip_or_delete
        # raises + deletes if too short, forcing this call to fall
        # through and re-run Pixverse below.
        try:
            _validate_clip_or_delete(out, sid=scene["id"])
            return out
        except ZTRenderError:
            # File was deleted; fall through to fresh Pixverse run
            pass

    sid = scene["id"]
    image_url = fal_client.upload_file(str(still_path))
    original_motion = scene["motion_prompt"]

    # Attempt 1: original motion_prompt
    video_url = _pixverse_submit_and_poll(original_motion, image_url, sid)
    if video_url:
        _save_url(video_url, out, source=f"pixverse clip {sid}")
        _validate_clip_or_delete(out, sid=sid)
        return out

    # Attempt 2: Sonnet-sanitized motion_prompt
    sanitized = _sanitize_motion_via_sonnet(original_motion, sid)
    if sanitized != original_motion:
        video_url = _pixverse_submit_and_poll(sanitized, image_url, sid)
        if video_url:
            _save_url(video_url, out, source=f"pixverse clip {sid} (sanitized)")
            _validate_clip_or_delete(out, sid=sid)
            return out

    # Fallback: ffmpeg Ken Burns pan on the still. Loses motion variety
    # for this one scene but guarantees the render completes.
    print(
        f"  [{sid}] pixverse rejected twice — falling back to ffmpeg "
        f"Ken Burns pan from the still",
        flush=True,
    )
    duration = float(scene.get("duration", scene.get("duration_sec", 5.0)) or 5.0)
    _ffmpeg_ken_burns_clip(still_path, out, duration_sec=duration)
    return out


def _gen_clip_ken_burns(scene: dict, still_path: Path, clips_dir: Path) -> Path:
    """Ken Burns only — no Pixverse spend. Tier A budget renders."""
    out = clips_dir / f"{scene['id']}.mp4"
    if out.exists() and out.stat().st_size > 1024:
        try:
            _validate_clip_or_delete(out, sid=scene["id"])
            return out
        except ZTRenderError:
            pass
    duration = float(scene.get("duration", scene.get("duration_sec", 5.0)) or 5.0)
    _ffmpeg_ken_burns_clip(still_path, out, duration_sec=duration)
    return out



# ────────────────────────────────────────────────────────────────────
# Sound design — per-scene mmaudio-v2 SFX heuristics ported from
# Empire Magnates v5. Pattern: keyword-match the scene's visual prompt
# to a domain-specific ambient descriptor, generate SFX via mmaudio,
# mix under narration with 2-pass loudnorm to -14 LUFS broadcast.
# ────────────────────────────────────────────────────────────────────
SFX_HEURISTICS_ZT: list[tuple[tuple[str, ...], str]] = [
    (("speed force", "lightning", "running", "sprinting", "blur", "vibrate", "racing"),
     "electric crackle, energy hum, kinetic motion whoosh, dramatic low strings"),
    (("death", "dying", "spectre", "ghost", "soul", "afterlife", "grave", "funeral", "casket"),
     "ethereal whisper, ambient pad, distant wind, mortality bed, somber strings"),
    (("crisis", "anti-monitor", "multiverse", "infinite earths", "reality"),
     "cosmic rumble, glass breaking, reality fracturing, dramatic chaotic strings"),
    (("black hole", "event horizon", "singularity", "cosmos", "universe", "void"),
     "deep space hum, low cosmic rumble, gravitational drone, time dilation pad"),
    (("apartment", "kitchen", "couch", "living room", "interior", "doorway"),
     "interior room tone, soft hum, distant city ambience, intimate setting"),
    (("city", "manhattan", "street", "skyline", "rooftop", "asphalt"),
     "urban ambience, distant traffic, wind, atmospheric city bed"),
    (("fight", "punch", "battle", "clash", "collision", "lunge", "explode"),
     "impact hits, action thud, kinetic energy strikes, dramatic action underscore"),
    (("daughter", "linda", "wife", "family", "embrace", "love", "wedding", "born"),
     "warm strings, gentle ambient, hopeful arc, intimate emotional bed, soft pad"),
    (("forgive", "memory", "regret", "sacrifice", "remember", "tear"),
     "soft cello, warm pad, contemplative reverb, emotional bed"),
    (("hospital", "birth", "newborn", "baby"),
     "warm room ambience, gentle string motif, hopeful underscore, soft pad"),
    (("courtroom", "trial", "verdict"),
     "courtroom ambience, distant murmur, tense procedural underscore"),
    (("chess", "thinking", "calculate", "decision"),
     "tense thinking pad, soft ticking clock, contemplative low strings"),
    (("crowd", "stadium", "rally", "audience"),
     "crowd murmur, distant cheers, atmospheric ambience"),
    (("mirror", "reflection", "doppelganger", "twin"),
     "ethereal pad, reverb, eerie ambient, dramatic tension"),
    (("storm", "rain", "thunder"),
     "rainfall, distant thunder, atmospheric storm bed, dramatic low strings"),
]

DEFAULT_SFX_ZT = (
    "Cinematic comic-book underscore, dramatic low strings, atmospheric "
    "energy, hopeful tension, hero motif, no melody dominance, no vocals"
)


def _derive_sfx_prompt(scene: dict) -> str:
    """Pick a per-scene SFX prompt by keyword-matching the visual prompt
    + motion description against ZT-specific heuristics."""
    haystack = (
        str(scene.get("visual_prompt", "") or "") + " " +
        str(scene.get("motion_prompt", "") or "") + " " +
        str(scene.get("narration", "") or "")
    ).lower()
    for keywords, descriptor in SFX_HEURISTICS_ZT:
        if any(k in haystack for k in keywords):
            return descriptor + ". No dialogue, ambient bed only, music score."
    return DEFAULT_SFX_ZT + ". No dialogue, ambient bed only."


def _gen_scene_sfx(scene: dict, clip_path: Path, sfx_dir: Path) -> Path:
    """mmaudio-v2 per-scene SFX bed. Takes the silent video clip + a
    keyword-derived ambient prompt, returns the SFX as an MP3 file."""
    sid = scene["id"]
    out = sfx_dir / f"{sid}.mp3"
    if out.exists() and out.stat().st_size > 1024:
        return out
    duration = float(scene.get("duration", 4.0) or 4.0)
    prompt = _derive_sfx_prompt(scene)
    upload_url = fal_client.upload_file(str(clip_path))
    result = fal_client.subscribe(
        "fal-ai/mmaudio-v2",
        arguments={
            "video_url": upload_url,
            "prompt": prompt[:1500],
            "duration": round(duration, 1),
            "cfg_strength": 4.5,
            "num_inference_steps": 25,
        },
    )
    sfx_video_url = (result.get("video") or {}).get("url") or result.get("video_url")
    # PR #148 — already had a None-check here, but harden the error
    # message + dump response keys so we can diagnose if it ever fires.
    if not sfx_video_url:
        raise ZTRenderError(f"no video in mmaudio result for {sid}: {result}")
    tmp = sfx_dir / f"{sid}_tmp.mp4"
    _save_url(sfx_video_url, tmp, source=f"mmaudio sfx {sid}")
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", str(tmp), "-vn",
        "-c:a", "libmp3lame", "-b:a", "192k",
        str(out),
    ], check=True, capture_output=True)
    try:
        tmp.unlink()
    except Exception:
        pass
    return out


def _two_pass_loudnorm(in_path: Path, out_path: Path) -> None:
    """Broadcast-grade -14 LUFS normalize. First pass measures, second
    applies the linear correction. Falls back to single-pass on parse fail."""
    measure = subprocess.run(
        ["ffmpeg", "-y", "-i", str(in_path), "-af",
         "loudnorm=I=-14:LRA=7:TP=-2:print_format=json", "-f", "null", "-"],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
    )
    candidates = re.findall(r"\{[^{}]*\"input_i\"[^{}]*\}", measure.stderr)
    if not candidates:
        subprocess.run([
            "ffmpeg", "-y", "-i", str(in_path),
            "-af", "loudnorm=I=-14:LRA=7:TP=-2",
            "-c:a", "libmp3lame", "-b:a", "192k", str(out_path),
        ], check=True, capture_output=True)
        return
    s = json.loads(candidates[-1])
    subprocess.run([
        "ffmpeg", "-y", "-i", str(in_path),
        "-af",
        f"loudnorm=I=-14:LRA=7:TP=-2:"
        f"measured_I={s['input_i']}:measured_LRA={s['input_lra']}:"
        f"measured_TP={s['input_tp']}:measured_thresh={s['input_thresh']}:"
        f"offset={s['target_offset']}:linear=true:print_format=summary",
        "-c:a", "libmp3lame", "-b:a", "192k", str(out_path),
    ], check=True, capture_output=True)


def _build_full_audio_track(
    scenes: list[dict],
    clips_dir: Path,
    sfx_dir: Path,
    vo_path: Path,
    workspace: Path,
) -> Path:
    """Generate per-scene SFX in parallel, concat into a single SFX track,
    mix narration on top at static levels (VO 1.0, SFX 0.22 ≈ -13 dB),
    apply 2-pass loudnorm to -14 LUFS broadcast standard."""
    sfx_dir.mkdir(exist_ok=True)

    # Per-scene mmaudio in parallel (3 concurrent — fal soft-cap considerate)
    sfx_paths_by_sid: dict[str, Path] = {}
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {}
        for scene in scenes:
            clip_path = clips_dir / f"{scene['id']}.mp4"
            if not clip_path.exists():
                continue
            futs[ex.submit(_gen_scene_sfx, scene, clip_path, sfx_dir)] = scene["id"]
        for fut in as_completed(futs):
            sid = futs[fut]
            try:
                sfx_paths_by_sid[sid] = fut.result()
            except Exception as e:
                # SFX is enhancement, not critical — log + continue without it
                # for that one scene (silence in that scene's bed).
                print(f"  [sfx:fail] {sid}: {e}", flush=True)

    # Concat in scene order (insert silence for missing scenes so timing aligns)
    sfx_concat = workspace / "sfx_concat.mp3"
    sfx_list = workspace / "sfx_list.txt"
    with open(sfx_list, "w", encoding="utf-8") as f:
        for scene in scenes:
            p = sfx_paths_by_sid.get(scene["id"])
            if p and p.exists():
                f.write(f"file '{p.as_posix()}'\n")
            # else: this scene has no SFX, skip (slight desync — acceptable
            # since amix duration=longest pads with VO anyway)
    if not sfx_paths_by_sid:
        # No SFX generated at all — fall back to just the narration track,
        # don't fail the render.
        return vo_path
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-f", "concat", "-safe", "0", "-i", str(sfx_list),
        "-c:a", "libmp3lame", "-b:a", "192k",
        str(sfx_concat),
    ], check=True, capture_output=True)

    # Mix: VO (1.0, full volume) + SFX (0.22 ≈ -13 dB underscore)
    mixed = workspace / "mixed_audio.mp3"
    subprocess.run([
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", str(vo_path),
        "-i", str(sfx_concat),
        "-filter_complex",
        "[0:a]volume=1.0[vo];"
        "[1:a]volume=0.22[sfx];"
        "[vo][sfx]amix=inputs=2:duration=longest:dropout_transition=2[mixed]",
        "-map", "[mixed]",
        "-c:a", "libmp3lame", "-b:a", "192k",
        str(mixed),
    ], check=True, capture_output=True)

    # 2-pass loudnorm to -14 LUFS broadcast standard
    final_audio = workspace / "final_audio.mp3"
    _two_pass_loudnorm(mixed, final_audio)
    return final_audio


def _gen_vo(scenes: list[dict], vo_dir: Path) -> Path:
    out = vo_dir / "narration.mp3"
    if out.exists() and out.stat().st_size > 1024:
        return out
    from skeleton_ai import voice_fal

    full_text = " ".join(s["narration"] for s in scenes).strip()
    try:
        return voice_fal.synthesize(
            text=full_text,
            out_path=out,
            voice_id=voice_fal.DEFAULT_VOICE,
            speed=0.95,
        )
    except Exception as exc:
        raise ZTRenderError(f"FAL MiniMax narration failed: {exc}") from exc


def _compose(scenes: list[dict], clips: list[Path], vo: Path,
             workspace: Path, out_path: Path) -> Path:
    trimmed_dir = workspace / "trimmed"
    trimmed_dir.mkdir(exist_ok=True)
    trimmed: list[Path] = []
    for scene_idx, (scene, clip) in enumerate(zip(scenes, clips)):
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
        is_hook = scene_idx == 0
        # Pope hook: center 80% safe zone, large caps. Body: lower third.
        if is_hook:
            font_size = 72 if clean_len <= 18 else (58 if clean_len <= 28 else 48)
            y_expr = "(h-text_h)/2"
        else:
            font_size = 68 if clean_len <= 16 else (56 if clean_len <= 22 else 46)
            y_expr = "h*0.75"
        drawtext = (
            f"drawtext=fontfile='{font_path}':text='{caption_text}':"
            f"fontsize={font_size}:fontcolor=white:bordercolor=black:borderw=5:"
            f"x=(w-text_w)/2:y={y_expr}"
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
    # Phase 4.6: per-scene mmaudio SFX bed mixed under narration with
    # 2-pass loudnorm. Falls back to vo-only if any of the SFX steps
    # fail (sound design is enhancement, not blocker).
    sfx_dir = workspace / "sfx"
    try:
        audio_track = _build_full_audio_track(scenes, clips_dir, sfx_dir, vo, workspace)
    except Exception as e:
        print(f"  [sfx-mix:fail] falling back to vo-only: {e}", flush=True)
        audio_track = vo

    final_mp4 = workspace / final_filename
    _compose(scenes, clips, audio_track, workspace, final_mp4)

    # PR #149 — final MP4 sanity check. The render_finalize result
    # gets reported as "status: ready" with this MP4's URL, which
    # auto-fires metadata-gen + .txt download on the frontend. If the
    # compose produced a near-zero-duration MP4 (e.g. corrupt input
    # clips all stitched together), the user gets a .txt for a video
    # that "didn't animate". Validate here so we raise loudly instead.
    expected_total = sum(s["duration"] for s in scenes)
    actual_total = _probe_duration_sec(final_mp4)
    if actual_total < expected_total * 0.7:  # 30% slack for encoder rounding
        raise ZTRenderError(
            f"compose produced an MP4 too short to be the real render: "
            f"got {actual_total:.1f}s, expected ~{expected_total:.1f}s. "
            f"Per-clip durations: " + ", ".join(
                f"{c.name}={_probe_duration_sec(c):.1f}s" for c in clips
            )
        )

    fal_cost_est = round(
        len(scenes) * 0.225 + 0.10 + len(scenes) * 0.05, 2  # Pixverse + MiniMax + mmaudio (stills already paid)
    )

    return {
        "mp4_path": str(final_mp4),
        "title": title,
        "scene_count": len(scenes),
        "duration_total_sec": round(actual_total, 1),
        "fal_cost_estimate_usd": fal_cost_est,
        "scenes": [{"id": s["id"], "caption": s["caption"], "duration": s["duration"]} for s in scenes],
    }


def render_ken_burns_short(
    *,
    script_json: Any,
    workspace: Path,
    final_filename: str = "ZeroTier_short.mp4",
) -> dict:
    """Tier A render: Seedream stills + Ken Burns pans + MiniMax VO only.

    Skips Pixverse i2v and mmaudio SFX to stay under ~$0.35 for a 4-scene short.
    """
    from zerotier_private.pope_doctrine import validate_pope_script

    _ensure_fal()
    workspace = Path(workspace)
    stills_dir = workspace / "stills"
    clips_dir = workspace / "clips"
    vo_dir = workspace / "vo"
    for d in (workspace, stills_dir, clips_dir, vo_dir):
        d.mkdir(parents=True, exist_ok=True)

    raw_data = script_json if isinstance(script_json, dict) else json.loads(script_json)
    for issue in validate_pope_script(raw_data):
        print(f"  [pope:warn] {issue}", flush=True)

    scenes, title = _normalize_scenes(raw_data)
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

    clips: list[Path] = [None] * len(scenes)  # type: ignore
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {
            ex.submit(_gen_clip_ken_burns, scenes[i], stills[i], clips_dir): i
            for i in range(len(scenes))
        }
        for fut in as_completed(futs):
            clips[futs[fut]] = fut.result()

    vo = _gen_vo(scenes, vo_dir)
    final_mp4 = workspace / final_filename
    _compose(scenes, clips, vo, workspace, final_mp4)

    expected_total = sum(s["duration"] for s in scenes)
    actual_total = _probe_duration_sec(final_mp4)
    if actual_total < expected_total * 0.7:
        raise ZTRenderError(
            f"compose produced an MP4 too short: got {actual_total:.1f}s, "
            f"expected ~{expected_total:.1f}s"
        )

    fal_cost_est = round(len(scenes) * 0.04 + 0.10, 2)

    return {
        "mp4_path": str(final_mp4),
        "title": title,
        "scene_count": len(scenes),
        "duration_total_sec": round(actual_total, 1),
        "fal_cost_estimate_usd": fal_cost_est,
        "scenes": [{"id": s["id"], "caption": s["caption"], "duration": s["duration"]} for s in scenes],
    }


def _resolve_panel_path(scene: dict, panels_dir: Path) -> Path:
    """Resolve a scene's panel_image to an on-disk file under panels_dir."""
    name = str(scene.get("panel_image", "") or "").strip()
    if not name:
        raise ZTRenderError(
            f"scene {scene['id']} missing panel_image — drop scans in {panels_dir} "
            f"and set panel_image in script.json"
        )
    candidate = Path(name)
    if candidate.is_absolute() and candidate.exists():
        return candidate
    for p in (panels_dir / name, panels_dir / candidate.name):
        if p.exists() and p.stat().st_size > 1024:
            return p
    raise ZTRenderError(
        f"panel not found for {scene['id']}: {name!r} (looked in {panels_dir})"
    )


def render_comic_panel_short(
    *,
    script_json: Any,
    workspace: Path,
    final_filename: str = "ZeroTier_short.mp4",
    vo_provider: str = "fal_minimax",
) -> dict:
    """Tier P: real comic panel scans + Ken Burns + VO only (~$0.10 fal).

    No Seedream, Pixverse, or mmaudio. User supplies panel JPEG/PNG per scene
    in workspace/panels/ (see panel_image in script JSON).

    ``vo_provider`` remains a compatibility input. Every value is normalized
    onto the sole supported narration capability, FAL MiniMax.
    """
    from zerotier_private.pope_doctrine import validate_pope_script

    workspace = Path(workspace)
    panels_dir = workspace / "panels"
    stills_dir = workspace / "stills"
    clips_dir = workspace / "clips"
    vo_dir = workspace / "vo"
    for d in (workspace, panels_dir, stills_dir, clips_dir, vo_dir):
        d.mkdir(parents=True, exist_ok=True)

    raw_data = script_json if isinstance(script_json, dict) else json.loads(script_json)
    for issue in validate_pope_script(raw_data):
        print(f"  [pope:warn] {issue}", flush=True)

    scenes, title = _normalize_scenes(raw_data)
    missing = [s["id"] for s in scenes if not s.get("panel_image")]
    if missing:
        raise ZTRenderError(
            f"Tier P requires panel_image on every scene; missing: {missing}"
        )

    scenes_path = workspace / "scenes.json"
    scenes_path.write_text(
        json.dumps({"title": title, "scenes": scenes, "render_tier": "P_panels"}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    stills: list[Path] = []
    for scene in scenes:
        panel = _resolve_panel_path(scene, panels_dir)
        still_out = stills_dir / f"{scene['id']}{panel.suffix.lower()}"
        if not still_out.exists() or still_out.stat().st_mtime < panel.stat().st_mtime:
            shutil.copy2(panel, still_out)
        stills.append(still_out)
        ref = scene.get("comic_ref") or panel.name
        print(f"  [panel] {scene['id']} <- {ref}", flush=True)

    clips: list[Path] = [None] * len(scenes)  # type: ignore
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {
            ex.submit(_gen_clip_ken_burns, scenes[i], stills[i], clips_dir): i
            for i in range(len(scenes))
        }
        for fut in as_completed(futs):
            clips[futs[fut]] = fut.result()

    requested_voice_provider = str(vo_provider or "").strip().lower()
    voice_provider_migrated = requested_voice_provider not in {
        "",
        "fal",
        "fal_minimax",
        "minimax",
    }
    vo = _gen_vo(scenes, vo_dir)
    fal_cost_est = 0.10

    final_mp4 = workspace / final_filename
    _compose(scenes, clips, vo, workspace, final_mp4)

    expected_total = sum(s["duration"] for s in scenes)
    actual_total = _probe_duration_sec(final_mp4)
    if actual_total < expected_total * 0.7:
        raise ZTRenderError(
            f"compose produced an MP4 too short: got {actual_total:.1f}s, "
            f"expected ~{expected_total:.1f}s"
        )

    credits = [s.get("comic_ref") for s in scenes if s.get("comic_ref")]
    (workspace / "comic_credits.txt").write_text(
        "Panel sources:\n" + "\n".join(f"- {c}" for c in credits) + "\n",
        encoding="utf-8",
    )

    return {
        "mp4_path": str(final_mp4),
        "title": title,
        "scene_count": len(scenes),
        "duration_total_sec": round(actual_total, 1),
        "fal_cost_estimate_usd": fal_cost_est,
        "voice_provider": "fal_minimax",
        "voice_provider_migrated": voice_provider_migrated,
        "render_tier": "P_panels",
        "comic_credits": credits,
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
    # Phase 4.6: per-scene mmaudio SFX + loudnorm. See render_finalize for
    # the same logic.
    sfx_dir = workspace / "sfx"
    try:
        audio_track = _build_full_audio_track(scenes, clips_dir, sfx_dir, vo, workspace)
    except Exception as e:
        print(f"  [sfx-mix:fail] falling back to vo-only: {e}", flush=True)
        audio_track = vo

    final_mp4 = workspace / final_filename
    _compose(scenes, clips, audio_track, workspace, final_mp4)

    # Cost estimate: $0.04 seedream + $0.225 Pixverse + $0.10 MiniMax + $0.05 mmaudio per scene
    fal_cost_est = round(
        len(scenes) * 0.04 + len(scenes) * 0.225 + 0.10 + len(scenes) * 0.05, 2
    )

    return {
        "mp4_path": str(final_mp4),
        "title": title,
        "scene_count": len(scenes),
        "duration_total_sec": round(sum(s["duration"] for s in scenes), 1),
        "fal_cost_estimate_usd": fal_cost_est,
        "scenes": [{"id": s["id"], "caption": s["caption"], "duration": s["duration"]} for s in scenes],
    }
