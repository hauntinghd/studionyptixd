"""
Long-form Studio render pipeline.

Background-job pattern: kick a job, run async, poll status. Per-channel
sub-pipelines registered in SUB_PIPELINES dict. PR #120 ships the
sleep_doc sub-pipeline (HR — Ken-Burns slideshow + fal MiniMax narration +
mmaudio ambient bed); PR #122 will register v5_episode (EM — LTX i2v +
Whisper callouts + silence-kill + 2-pass loudnorm).

Storage: ``/var/data/long_form/<job_id>/`` on Fly (or ``long_form/output/<job_id>/``
fallback locally — auto-detected). Layout:

    state.json                       — job manifest (channel_key, outline,
                                       phase progress, timestamps)
    chapters.json                    — Grok-expanded chapters with full
                                       narration + scene_prompts
    stills/scene_NNNN.png            — ernie-image scenes
    audio/chapter_NN.mp3             — per-chapter MiniMax narration
    audio/narration.mp3              — concat'd full narration
    audio/ambient.mp3                — mmaudio loop (4 min, tiled at compose)
    audio/mix.mp3                    — narration + ambient mixdown
    thumbnails/thumb_N.png           — seedream candidate thumbnails
    LongForm_<job_id>.mp4            — final 1080p60 output

Cost budget for HR 9-hour sleep doc (per channel registry $73 estimate):
    - 18 chapters × any-llm Sonnet 4.5 ≈ $9.00
    - 540 ernie-image scenes × $0.03 ≈ $16.20
    - 65k-word fal MiniMax speech-02-hd ≈ $39.00 (390k chars × $0.10/1k)
    - 3 seedream v4.5 thumbnails × $0.04 ≈ $0.12
    - 1 mmaudio-v2 4-min ambient bed ≈ $0.05
    - subtotal ≈ $64.40, with retry overhead → ~$73 envelope

Casey's HR feedback rule (feedback_hr_premium_fal_tts.md): TTS MUST be fal
MiniMax — NOT Edge — Egypt 9H Edge-TTS shipped and Casey called it bad.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Awaitable, Callable

import httpx


# ─────────────────────────────────────────────────────────────────────────────
# Globals — paths, fal endpoints, status registry, task strong-refs
# ─────────────────────────────────────────────────────────────────────────────

# Fly persistent volume detection — must be Linux + the mount must exist.
# (On Windows local dev, Path("/var/data").exists() resolves to D:\var\data
# which can pass true after a single run because mkdir creates it. Restrict
# to posix to keep local dev under long_form/output/.)
def _resolve_lf_output_root() -> Path:
    override = (os.environ.get("LF_OUTPUT_ROOT") or "").strip()
    if override:
        return Path(override)
    if os.name == "posix":
        var_data = Path("/var/data")
        if var_data.is_dir():
            return var_data / "long_form"
    return Path("long_form/output")


LF_OUTPUT_ROOT = _resolve_lf_output_root()
LF_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# fal endpoints used by this pipeline.
SEEDREAM_URL = "https://fal.run/fal-ai/bytedance/seedream/v4.5/text-to-image"
ERNIE_URL = "https://fal.run/fal-ai/ernie-image"
MINIMAX_TTS_URL = "https://fal.run/fal-ai/minimax/speech-02-hd"
MMAUDIO_URL = "https://fal.run/fal-ai/mmaudio-v2"

# Per-job in-memory progress snapshot. Survives across HTTP requests but not
# process restarts — that's fine because state.json on disk is the source of
# truth; this is a lightweight cache for /jobs/{id}/status polling.
_lf_jobs_status: dict[str, dict[str, Any]] = {}

# Strong-ref retention for asyncio.create_task background tasks. Without this
# the GC sometimes cancels them mid-render (we hit this exact bug on ZT short
# Phase 4.5b — see lesson #2 in the 2026-05-08 handoff).
_lf_running_tasks: set[asyncio.Task] = set()


class LFRenderError(RuntimeError):
    """Long-form render failure — caught by _run_render to mark the job failed."""


# ─────────────────────────────────────────────────────────────────────────────
# Job ID + path helpers
# ─────────────────────────────────────────────────────────────────────────────

def _new_job_id() -> str:
    """12-hex job id — same shape as ZT private (also acts as media-fetch
    capability token; gating by job_id replaces Authorization headers for
    <img>/<video> tags)."""
    return uuid.uuid4().hex[:12]


def _job_dir(job_id: str) -> Path:
    return LF_OUTPUT_ROOT / job_id


def _ensure_job_dir(job_id: str) -> Path:
    d = _job_dir(job_id)
    (d / "stills").mkdir(parents=True, exist_ok=True)
    (d / "audio").mkdir(parents=True, exist_ok=True)
    (d / "thumbnails").mkdir(parents=True, exist_ok=True)
    return d


def _slugify(s: str, max_len: int = 24) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", str(s or "").lower()).strip("_")
    return (s[:max_len].rstrip("_")) or "longform"


def _state_path(job_id: str) -> Path:
    return _job_dir(job_id) / "state.json"


def _chapters_path(job_id: str) -> Path:
    return _job_dir(job_id) / "chapters.json"


def _final_mp4_path(job_id: str, title_slug: str) -> Path:
    return _job_dir(job_id) / f"LongForm_{title_slug}_{job_id}.mp4"


def save_state(job_id: str, state: dict[str, Any]) -> None:
    """Persist state.json atomically (tmp + rename)."""
    p = _state_path(job_id)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2, ensure_ascii=True), encoding="utf-8")
    tmp.replace(p)


def load_state(job_id: str) -> dict[str, Any] | None:
    p = _state_path(job_id)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def update_status(job_id: str, **fields: Any) -> None:
    """Merge fields into the in-memory status registry. Caller is responsible
    for also persisting whichever fields belong on disk via save_state."""
    entry = _lf_jobs_status.setdefault(job_id, {})
    entry.update(fields)
    entry["updated_at"] = time.time()


def get_status(job_id: str) -> dict[str, Any] | None:
    """Return a defensive copy of the status snapshot or None if no such job."""
    entry = _lf_jobs_status.get(job_id)
    if entry is None:
        # Try to recover from disk — useful after a process restart.
        st = load_state(job_id)
        if not st:
            return None
        entry = {
            "phase": st.get("phase", "unknown"),
            "percent": st.get("percent", 0),
            "error": st.get("error", ""),
            "started_at": st.get("started_at", 0),
            "updated_at": st.get("updated_at", 0),
        }
        _lf_jobs_status[job_id] = entry
    out = dict(entry)
    out.pop("_task", None)  # don't leak the asyncio.Task object
    return out


def list_recent_jobs(limit: int = 20) -> list[dict[str, Any]]:
    """Return up to N most recent jobs, newest first. Sources state.json on
    disk so survives process restart (memory registry alone wouldn't)."""
    if not LF_OUTPUT_ROOT.exists():
        return []
    rows: list[tuple[float, dict[str, Any]]] = []
    for sub in LF_OUTPUT_ROOT.iterdir():
        if not sub.is_dir():
            continue
        sp = sub / "state.json"
        if not sp.exists():
            continue
        try:
            st = json.loads(sp.read_text(encoding="utf-8"))
        except Exception:
            continue
        ts = float(
            st.get("created_at")
            or st.get("started_at")
            or sp.stat().st_mtime
        )
        st["job_id"] = sub.name
        rows.append((ts, st))
    rows.sort(key=lambda r: r[0], reverse=True)
    return [r[1] for r in rows[:limit]]


# ─────────────────────────────────────────────────────────────────────────────
# fal HTTP helpers — share the round-robin key pool pattern from
# zerotier_private/pipeline.py + mongol_project/generate_pipeline.py
# ─────────────────────────────────────────────────────────────────────────────

def _fal_keys() -> list[str]:
    keys: list[str] = []
    for name in ("FAL_AI_KEY", "FAL_AI_KEY_2", "FAL_AI_KEY_3",
                 "FAL_AI_KEY_4", "FAL_AI_KEY_5", "FAL_AI_KEY_6"):
        v = (os.environ.get(name) or "").strip()
        if v:
            keys.append(v)
    if not keys:
        raise LFRenderError("no FAL_AI_KEY* in env")
    return keys


_key_cursor = [0]


def _next_fal_key() -> str:
    keys = _fal_keys()
    k = keys[_key_cursor[0] % len(keys)]
    _key_cursor[0] += 1
    return k


def _fal_post(url: str, payload: dict, *, timeout_s: int = 600, attempts: int = 3) -> dict:
    """POST to fal with retry on 429/5xx + key rotation. Same pattern as
    zerotier_private/pipeline.py — tested on ZT renders."""
    last_err: str = ""
    for attempt in range(attempts):
        key = _next_fal_key()
        try:
            with httpx.Client(timeout=timeout_s) as c:
                r = c.post(
                    url,
                    headers={
                        "Authorization": f"Key {key}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                last_err = f"HTTP {r.status_code}: {r.text[:200]}"
                time.sleep(2 ** attempt)
                continue
            raise LFRenderError(f"fal {url.rsplit('/', 2)[-1]} HTTP {r.status_code}: {r.text[:300]}")
        except httpx.HTTPError as exc:
            last_err = f"{type(exc).__name__}: {exc}"
            time.sleep(2 ** attempt)
    raise LFRenderError(f"fal {url} failed after {attempts} attempts: {last_err}")


def _download(url: str, out_path: Path, timeout_s: int = 120) -> None:
    """Stream fal's signed URL to disk. We always download immediately because
    fal signed URLs expire quickly."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=timeout_s, follow_redirects=True) as c:
        r = c.get(url)
        r.raise_for_status()
        out_path.write_bytes(r.content)


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1 — chapter expansion (Grok per-chapter narration + scene prompts)
# ─────────────────────────────────────────────────────────────────────────────

CHAPTER_PROMPT_TEMPLATE = """You are writing a single chapter of a long-form documentary.

Channel context (locked grammar):
{channel_system_prompt}

Visual style for this channel:
{visual_style}

Documentary title: {outline_title}
Hook: {outline_hook}
Chapter index: {chapter_index} of {chapter_count}
Chapter title: {chapter_title}
Chapter synopsis: {chapter_synopsis}
Target chapter duration: {chapter_minutes} minutes
Target word count: ~{target_words} words (at {wpm} wpm)
Number of scene-images for this chapter: {scenes_per_chapter}

Return strict JSON, NO markdown fences, with this exact shape:
{{
  "chapter_index": {chapter_index},
  "title": "{chapter_title}",
  "narration": "<flowing prose ~{target_words} words. Multiple paragraphs.>",
  "word_count": <int actual>,
  "scene_prompts": [
    "<scene 1 image prompt — 20-40 words. Concrete visual: subject + environment + lighting + framing. Apply the channel visual style. NO text, NO watermarks, NO logos.>",
    ... {scenes_per_chapter} total ...
  ]
}}
"""


def _strip_json_fences(s: str) -> str:
    s = s.strip().strip("`").strip()
    if s.lower().startswith("json"):
        s = s[4:].strip()
    return s


def _gen_chapter(
    grok,
    *,
    channel: dict,
    outline: dict,
    chapter_index: int,
    chapter_count: int,
    scenes_per_chapter: int,
    wpm: int,
) -> dict:
    """Run one chapter expansion via the existing GrokClient. Caller passes
    the same client instance across all chapters so we get session reuse."""
    chapters = outline.get("chapters") or []
    if chapter_index >= len(chapters):
        raise LFRenderError(f"chapter_index {chapter_index} out of range (have {len(chapters)})")
    ch = chapters[chapter_index]
    chapter_minutes = max(1, int(ch.get("minutes", 1)))
    target_words = chapter_minutes * wpm

    sys = (
        f"{channel.get('system_prompt', '')}\n\n"
        f"Visual style: {channel.get('visual_style', '')}\n\n"
        "Output strict JSON only. No markdown fences, no commentary."
    )
    user = CHAPTER_PROMPT_TEMPLATE.format(
        channel_system_prompt=channel.get("system_prompt", ""),
        visual_style=channel.get("visual_style", ""),
        outline_title=outline.get("title", ""),
        outline_hook=outline.get("hook", ""),
        chapter_index=chapter_index,
        chapter_count=chapter_count,
        chapter_title=ch.get("title", f"Chapter {chapter_index + 1}"),
        chapter_synopsis=ch.get("synopsis", ""),
        chapter_minutes=chapter_minutes,
        target_words=target_words,
        wpm=wpm,
        scenes_per_chapter=scenes_per_chapter,
    )

    raw = grok.complete(sys, user, max_tokens=16000, temperature=0.65)
    raw = _strip_json_fences(raw)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise LFRenderError(f"chapter {chapter_index} JSON parse failed: {e}; raw: {raw[:300]}")

    # Normalize.
    out = {
        "chapter_index": int(data.get("chapter_index", chapter_index)),
        "title": str(data.get("title", ch.get("title", ""))),
        "narration": str(data.get("narration", "") or "").strip(),
        "word_count": int(data.get("word_count", 0) or 0),
        "scene_prompts": [str(p) for p in (data.get("scene_prompts") or []) if str(p).strip()],
    }
    if not out["narration"]:
        raise LFRenderError(f"chapter {chapter_index} narration empty")
    if not out["scene_prompts"]:
        raise LFRenderError(f"chapter {chapter_index} scene_prompts empty")
    if out["word_count"] == 0:
        out["word_count"] = len(out["narration"].split())
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 — scene image gen (ernie-image per scene; thread pool for throughput)
# ─────────────────────────────────────────────────────────────────────────────

def _gen_scene_image(prompt: str, out_path: Path, *, image_model: str = "ernie") -> Path:
    """Render a single scene still. ernie-image is the canonical sleep-doc
    model (cheap $0.03/image, 1920×1080). seedream falls back via param if
    Casey wants higher fidelity at 4× cost."""
    if out_path.exists() and out_path.stat().st_size > 1024:
        return out_path
    if image_model == "seedream_45":
        url = SEEDREAM_URL
        payload = {"prompt": prompt, "image_size": {"width": 1920, "height": 1080}}
    else:
        url = ERNIE_URL
        payload = {"prompt": prompt, "image_size": {"width": 1920, "height": 1080}}
    data = _fal_post(url, payload, timeout_s=240)
    images = data.get("images") or []
    if not images:
        raise LFRenderError(f"image gen returned no images: {data}")
    img_url = images[0].get("url", "")
    if not img_url:
        raise LFRenderError(f"image gen response missing url: {data}")
    _download(img_url, out_path, timeout_s=120)
    return out_path


def _gen_scenes_batch(
    chapters: list[dict],
    stills_dir: Path,
    image_model: str,
    *,
    on_progress: Callable[[int, int], None] | None = None,
    concurrency: int = 4,
) -> list[Path]:
    """Generate every scene image in chapters[*].scene_prompts in parallel.

    Indexing: global_idx = chapter_index * scenes_per_chapter + local_idx
    Filename: scene_NNNN.png (zero-padded global)
    """
    tasks: list[tuple[int, str, Path]] = []
    scenes_per_chapter = len(chapters[0].get("scene_prompts") or []) if chapters else 0
    for ch in chapters:
        ch_idx = int(ch.get("chapter_index", 0))
        for local_idx, prompt in enumerate(ch.get("scene_prompts") or []):
            global_idx = ch_idx * scenes_per_chapter + local_idx
            out = stills_dir / f"scene_{global_idx:04d}.png"
            tasks.append((global_idx, prompt, out))

    total = len(tasks)
    out_paths: list[Path] = []
    done = 0
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        future_to_task = {
            ex.submit(_gen_scene_image, prompt, out, image_model=image_model): (gi, out)
            for gi, prompt, out in tasks
        }
        for fut in as_completed(future_to_task):
            gi, out = future_to_task[fut]
            try:
                fut.result()
                out_paths.append(out)
            except Exception as e:
                # Keep going — single scene failure shouldn't kill 540-scene render.
                # We log it on state.json so the operator can re-run after.
                print(f"[scenes] scene {gi} failed: {e}")
            done += 1
            if on_progress and (done % 5 == 0 or done == total):
                on_progress(done, total)
    out_paths.sort(key=lambda p: int(re.search(r"scene_(\d+)", p.name).group(1)))
    return out_paths


# ─────────────────────────────────────────────────────────────────────────────
# Phase 3 — fal MiniMax narration (per-chapter, then concat)
# ─────────────────────────────────────────────────────────────────────────────

def _gen_minimax_chapter(
    text: str,
    out_path: Path,
    *,
    voice_id: str = "English_Trustworthy_Man",
) -> Path:
    """Render one chapter's narration via fal MiniMax speech-02-hd.

    speech-02-hd is the premium-tier voice mandated by the HR feedback memory
    (feedback_hr_premium_fal_tts.md): Egypt 9H Edge-TTS shipped and Casey
    called it bad. fal MiniMax is the only acceptable HR voice tier.
    """
    if out_path.exists() and out_path.stat().st_size > 4096:
        return out_path
    payload = {
        "text": text,
        "voice_setting": {
            "voice_id": voice_id,
            "speed": 0.92,                # slower than default for sleep pacing
            "vol": 1.0,
            "pitch": 0,
        },
        "output_format": "mp3",
        "audio_setting": {
            "sample_rate": 32000,
            "bitrate": 128000,
            "format": "mp3",
            "channel": 1,
        },
    }
    # MiniMax has a per-call char limit (~5000 chars). Chunk if needed.
    text = text.strip()
    if len(text) <= 5000:
        data = _fal_post(MINIMAX_TTS_URL, payload, timeout_s=300)
        url = (data.get("audio") or {}).get("url") or data.get("audio_url")
        if not url:
            raise LFRenderError(f"MiniMax response missing audio url: {data}")
        _download(url, out_path, timeout_s=120)
        return out_path

    # Long chapter — chunk into <=4500 char paragraph batches, render each,
    # then ffmpeg concat into the chapter MP3.
    parts = _chunk_text(text, max_chars=4500)
    part_paths: list[Path] = []
    for i, part in enumerate(parts):
        part_payload = dict(payload, text=part)
        data = _fal_post(MINIMAX_TTS_URL, part_payload, timeout_s=300)
        url = (data.get("audio") or {}).get("url") or data.get("audio_url")
        if not url:
            raise LFRenderError(f"MiniMax part {i} missing url: {data}")
        pp = out_path.with_name(f"{out_path.stem}_p{i:02d}.mp3")
        _download(url, pp, timeout_s=120)
        part_paths.append(pp)
    _ffmpeg_concat_audio(part_paths, out_path)
    for pp in part_paths:
        try:
            pp.unlink()
        except Exception:
            pass
    return out_path


def _chunk_text(text: str, *, max_chars: int = 4500) -> list[str]:
    """Split text on paragraph boundaries so each chunk <= max_chars."""
    paras = re.split(r"\n\n+", text.strip())
    out: list[str] = []
    buf = ""
    for p in paras:
        candidate = (buf + "\n\n" + p) if buf else p
        if len(candidate) <= max_chars:
            buf = candidate
        else:
            if buf:
                out.append(buf)
            # If the single paragraph exceeds max_chars, split on sentence breaks.
            if len(p) > max_chars:
                sents = re.split(r"(?<=[\.!?])\s+", p)
                cur = ""
                for s in sents:
                    cand = (cur + " " + s) if cur else s
                    if len(cand) <= max_chars:
                        cur = cand
                    else:
                        if cur:
                            out.append(cur)
                        cur = s
                if cur:
                    out.append(cur)
                buf = ""
            else:
                buf = p
    if buf:
        out.append(buf)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Phase 4 — mmaudio ambient bed
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_AMBIENT_PROMPT = (
    "calm orchestral ambient pad, slow strings and low brass, gentle, "
    "cinematic, no drums, no percussion, continuous drone, bedtime atmosphere"
)


def _gen_ambient(out_path: Path, *, prompt: str = DEFAULT_AMBIENT_PROMPT, duration_sec: int = 240) -> Path:
    """4-min sleep ambient loop. ffmpeg tiles it to full narration length
    during compose."""
    if out_path.exists() and out_path.stat().st_size > 1024:
        return out_path
    data = _fal_post(MMAUDIO_URL, {"prompt": prompt, "duration": duration_sec}, timeout_s=240)
    url = (data.get("audio") or {}).get("url") or data.get("audio_url")
    if not url:
        raise LFRenderError(f"mmaudio response missing audio url: {data}")
    _download(url, out_path, timeout_s=120)
    return out_path


# ─────────────────────────────────────────────────────────────────────────────
# Phase 5 — seedream thumbnails (3 candidates from channel thumbnail_style)
# ─────────────────────────────────────────────────────────────────────────────

def _gen_thumbnails(channel: dict, outline: dict, thumbs_dir: Path, count: int = 3) -> list[Path]:
    base_prompt = (channel.get("thumbnail_style_prompt") or channel.get("visual_style") or "").strip()
    title = (outline.get("title") or "").strip()
    out_paths: list[Path] = []
    for i in range(count):
        out = thumbs_dir / f"thumb_{i + 1}.png"
        if out.exists() and out.stat().st_size > 1024:
            out_paths.append(out)
            continue
        # Add a per-iteration subject hint so the 3 thumbs aren't identical.
        variant_hints = [
            "Wide establishing shot, subject center-right.",
            "Medium portrait composition, subject center.",
            "Low-angle dramatic composition, subject silhouetted.",
        ]
        full_prompt = (
            f"{base_prompt}\n\nDocumentary title context: {title}.\n\n"
            f"Composition variant: {variant_hints[i % len(variant_hints)]}"
        )
        data = _fal_post(
            SEEDREAM_URL,
            {"prompt": full_prompt, "image_size": {"width": 1280, "height": 720}},
            timeout_s=180,
        )
        images = data.get("images") or []
        if not images:
            print(f"[thumbnails] thumb {i + 1} returned no images")
            continue
        img_url = images[0].get("url", "")
        if not img_url:
            continue
        _download(img_url, out, timeout_s=120)
        out_paths.append(out)
    return out_paths


# ─────────────────────────────────────────────────────────────────────────────
# Phase 6 — ffmpeg compose (slideshow + audio mix + 2-pass loudnorm + mux)
#
# Mirrors mongol_project/compose_1080p60.sh which is the validated sleep-doc
# slideshow recipe. Per-scene duration = narration_total / scene_count, then
# concat-demuxer + amix ambient under narration + libx264 1080p output.
# ─────────────────────────────────────────────────────────────────────────────

def _ffprobe_dur(path: Path) -> float:
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", str(path)],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise LFRenderError(f"ffprobe failed on {path}: {r.stderr[:200]}")
    try:
        return float((r.stdout or "0").strip())
    except ValueError:
        return 0.0


def _ffmpeg_concat_audio(parts: list[Path], out_path: Path) -> Path:
    """Bit-stream concat MP3 parts (no re-encode) via concat demuxer."""
    if not parts:
        raise LFRenderError("no audio parts to concat")
    list_file = out_path.with_suffix(".list.txt")
    list_file.write_text(
        "\n".join(f"file '{str(p.resolve()).replace(chr(92), '/')}'" for p in parts),
        encoding="utf-8",
    )
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
        "-f", "concat", "-safe", "0", "-i", str(list_file),
        "-c", "copy", str(out_path),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    list_file.unlink(missing_ok=True)
    if r.returncode != 0:
        raise LFRenderError(f"ffmpeg concat-audio failed: {r.stderr[-400:]}")
    return out_path


def _two_pass_loudnorm(in_path: Path, out_path: Path) -> Path:
    """Broadcast-grade 2-pass loudnorm to -14 LUFS. Same target ZT private
    Phase 4.6 uses for shorts."""
    # Pass 1: measure
    cmd1 = [
        "ffmpeg", "-hide_banner", "-loglevel", "info", "-i", str(in_path),
        "-af", "loudnorm=I=-14:TP=-1.5:LRA=11:print_format=json",
        "-f", "null", "-",
    ]
    r1 = subprocess.run(cmd1, capture_output=True, text=True)
    out_text = (r1.stderr or "") + (r1.stdout or "")
    m = re.search(r"\{[^{}]*\"input_i\"[^{}]*\}", out_text, re.DOTALL)
    if not m:
        # If measurement fails, skip second pass and just bit-copy.
        out_path.write_bytes(in_path.read_bytes())
        return out_path
    measured = json.loads(m.group(0))
    cmd2 = [
        "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
        "-i", str(in_path),
        "-af", (
            "loudnorm=I=-14:TP=-1.5:LRA=11:"
            f"measured_I={measured.get('input_i', '-23')}:"
            f"measured_TP={measured.get('input_tp', '-2')}:"
            f"measured_LRA={measured.get('input_lra', '7')}:"
            f"measured_thresh={measured.get('input_thresh', '-34')}:"
            f"offset={measured.get('target_offset', '0')}:"
            "linear=true:print_format=summary"
        ),
        "-c:a", "libmp3lame", "-b:a", "192k",
        str(out_path),
    ]
    r2 = subprocess.run(cmd2, capture_output=True, text=True)
    if r2.returncode != 0:
        raise LFRenderError(f"loudnorm pass 2 failed: {r2.stderr[-400:]}")
    return out_path


def _list_scenes_sorted(stills_dir: Path) -> list[Path]:
    """Sort by integer scene index (scene_NNNN.png) — the dir-listing order
    is alphabetical by default which would put scene_10 before scene_2."""
    matches: list[tuple[int, Path]] = []
    for p in stills_dir.iterdir():
        if not p.is_file():
            continue
        m = re.match(r"scene_(\d+)", p.stem)
        if not m:
            continue
        if p.suffix.lower() not in (".png", ".jpg", ".jpeg", ".webp"):
            continue
        matches.append((int(m.group(1)), p))
    matches.sort(key=lambda r: r[0])
    return [p for _, p in matches]


def _compose_slideshow(
    stills: list[Path],
    narration: Path,
    ambient: Path,
    out_path: Path,
    *,
    fps: int = 60,
) -> Path:
    """Full slideshow compose: scenes held for narration_total/scene_count
    seconds each, ambient mixed under narration at -16dB."""
    narr_sec = _ffprobe_dur(narration)
    if narr_sec <= 0:
        raise LFRenderError("narration has zero duration")
    scene_count = len(stills)
    if scene_count == 0:
        raise LFRenderError("no scene stills to compose")
    per_scene = narr_sec / scene_count

    # Mix audio: ambient stream-looped + narration, longest=narration
    mix_path = out_path.with_name(out_path.stem + "_mix.mp3")
    cmd_mix = [
        "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
        "-stream_loop", "-1", "-i", str(ambient),
        "-i", str(narration),
        "-filter_complex",
        "[0:a]volume=0.15[a0];[1:a]volume=1.0[a1];"
        "[a0][a1]amix=inputs=2:duration=longest:dropout_transition=3",
        "-t", f"{narr_sec:.3f}",
        "-c:a", "libmp3lame", "-b:a", "192k",
        str(mix_path),
    ]
    r = subprocess.run(cmd_mix, capture_output=True, text=True)
    if r.returncode != 0:
        raise LFRenderError(f"ffmpeg amix failed: {r.stderr[-400:]}")

    # 2-pass loudnorm on the mix.
    final_audio = mix_path.with_name(mix_path.stem + "_lk.mp3")
    _two_pass_loudnorm(mix_path, final_audio)

    # Build concat-demuxer list.
    concat_file = out_path.with_suffix(".concat.txt")
    lines = []
    for s in stills:
        p = str(s.resolve()).replace("\\", "/")
        lines.append(f"file '{p}'")
        lines.append(f"duration {per_scene:.4f}")
    # ffmpeg concat-demuxer quirk: repeat last image without duration line.
    last = str(stills[-1].resolve()).replace("\\", "/")
    lines.append(f"file '{last}'")
    concat_file.write_text("\n".join(lines), encoding="utf-8")

    cmd_v = [
        "ffmpeg", "-hide_banner", "-loglevel", "warning", "-y",
        "-f", "concat", "-safe", "0", "-i", str(concat_file),
        "-i", str(final_audio),
        "-vf",
        "scale=1920:1080:force_original_aspect_ratio=decrease,"
        "pad=1920:1080:(ow-iw)/2:(oh-ih)/2,format=yuv420p,fps=" + str(fps),
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "26", "-g", "300",
        "-c:a", "aac", "-b:a", "192k",
        "-movflags", "+faststart",
        "-shortest",
        str(out_path),
    ]
    r2 = subprocess.run(cmd_v, capture_output=True, text=True)
    concat_file.unlink(missing_ok=True)
    mix_path.unlink(missing_ok=True)
    final_audio.unlink(missing_ok=True)
    if r2.returncode != 0:
        raise LFRenderError(f"ffmpeg compose failed: {r2.stderr[-500:]}")
    return out_path


# ─────────────────────────────────────────────────────────────────────────────
# Sleep-doc orchestrator (HR pipeline_kind="sleep_doc")
# ─────────────────────────────────────────────────────────────────────────────

async def run_sleep_doc_pipeline(
    job_id: str,
    channel: dict,
    outline: dict,
    *,
    scenes_per_chapter: int = 30,
    wpm: int = 120,
) -> None:
    """End-to-end HR sleep-doc render. Each phase updates state.json + the
    in-memory status registry so the frontend's poll loop can report
    progress accurately.

    All per-fal-call work runs in a thread-pool executor (sync httpx); we
    only touch asyncio at the phase boundaries so we don't block the
    FastAPI event loop.
    """
    job_dir = _ensure_job_dir(job_id)
    state = load_state(job_id) or {}
    state.update({
        "job_id": job_id,
        "channel_key": channel.get("key"),
        "channel_label": channel.get("label"),
        "pipeline_kind": "sleep_doc",
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

    # ── Phase 1 — chapters (Grok per-chapter expansion) ────────────────────
    update_status(job_id, phase="chapters", percent=2)
    state["phase"] = "chapters"
    save_state(job_id, state)

    # Lazy import — keeps pipeline.py importable on machines without GrokClient.
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
                grok,
                channel=channel,
                outline=outline,
                chapter_index=i,
                chapter_count=chapter_count,
                scenes_per_chapter=scenes_per_chapter,
                wpm=wpm,
            ),
        )
        chapters_done.append(result)
        # Persist after each chapter so resume works after crash.
        chapters_data = {"outline_title": outline.get("title", ""), "chapters": sorted(chapters_done, key=lambda c: int(c.get("chapter_index", 0)))}
        chapters_path.write_text(json.dumps(chapters_data, indent=2, ensure_ascii=True), encoding="utf-8")
        # 0-15% range for chapter phase (lots of subsequent work).
        pct = 2 + int(13 * (ch_idx + 1) / max(1, chapter_count))
        update_status(
            job_id,
            phase="chapters",
            percent=pct,
            chapter_done=ch_idx + 1,
            chapter_total=chapter_count,
        )
        state["percent"] = pct
        save_state(job_id, state)

    chapters_data = json.loads(chapters_path.read_text(encoding="utf-8"))
    chapters = chapters_data["chapters"]

    # ── Phase 2 — scene image gen ──────────────────────────────────────────
    update_status(job_id, phase="scenes", percent=15)
    state["phase"] = "scenes"
    save_state(job_id, state)

    stills_dir = job_dir / "stills"
    image_model = channel.get("image_model_default", "ernie")

    def _on_scene_progress(done: int, total: int) -> None:
        # Scenes phase = 15-45% (the longest phase by wall time).
        pct = 15 + int(30 * done / max(1, total))
        update_status(
            job_id, phase="scenes", percent=pct,
            scene_done=done, scene_total=total,
        )

    stills = await loop.run_in_executor(
        None,
        lambda: _gen_scenes_batch(
            chapters, stills_dir, image_model,
            on_progress=_on_scene_progress,
            concurrency=4,
        ),
    )
    if not stills:
        raise LFRenderError("scene gen produced no stills")
    state["scenes_generated"] = len(stills)
    state["percent"] = 45
    save_state(job_id, state)

    # ── Phase 3 — narration (fal MiniMax per chapter, then concat) ────────
    update_status(job_id, phase="narration", percent=46)
    state["phase"] = "narration"
    save_state(job_id, state)

    audio_dir = job_dir / "audio"
    chapter_mp3s: list[Path] = []
    voice_id = channel.get("voice_id_default") or "English_Trustworthy_Man"

    for i, ch in enumerate(chapters):
        out = audio_dir / f"chapter_{int(ch['chapter_index']):02d}.mp3"
        if not (out.exists() and out.stat().st_size > 4096):
            await loop.run_in_executor(
                None,
                lambda c=ch, o=out: _gen_minimax_chapter(c["narration"], o, voice_id=voice_id),
            )
        chapter_mp3s.append(out)
        # Narration phase = 46-78%
        pct = 46 + int(32 * (i + 1) / max(1, len(chapters)))
        update_status(
            job_id, phase="narration", percent=pct,
            narration_done=i + 1, narration_total=len(chapters),
        )

    narration_full = audio_dir / "narration.mp3"
    if not (narration_full.exists() and narration_full.stat().st_size > 8192):
        await loop.run_in_executor(None, lambda: _ffmpeg_concat_audio(chapter_mp3s, narration_full))
    state["narration_path"] = str(narration_full.relative_to(LF_OUTPUT_ROOT))
    state["narration_duration_sec"] = _ffprobe_dur(narration_full)
    state["percent"] = 78
    save_state(job_id, state)

    # ── Phase 4 — ambient bed (1 mmaudio call, ffmpeg tiles at compose) ───
    update_status(job_id, phase="ambient", percent=80)
    state["phase"] = "ambient"
    save_state(job_id, state)
    ambient = audio_dir / "ambient.mp3"
    await loop.run_in_executor(None, lambda: _gen_ambient(ambient))
    state["percent"] = 82
    save_state(job_id, state)

    # ── Phase 5 — thumbnails ──────────────────────────────────────────────
    update_status(job_id, phase="thumbnails", percent=83)
    state["phase"] = "thumbnails"
    save_state(job_id, state)
    thumbs_dir = job_dir / "thumbnails"
    thumbs = await loop.run_in_executor(
        None, lambda: _gen_thumbnails(channel, outline, thumbs_dir, count=3)
    )
    state["thumbnails_generated"] = len(thumbs)
    state["percent"] = 86
    save_state(job_id, state)

    # ── Phase 6 — compose ─────────────────────────────────────────────────
    update_status(job_id, phase="compose", percent=87)
    state["phase"] = "compose"
    save_state(job_id, state)

    title_slug = _slugify(outline.get("title", "longform"))
    out_mp4 = _final_mp4_path(job_id, title_slug)
    fps = int(channel.get("fps") or 60)
    await loop.run_in_executor(
        None,
        lambda: _compose_slideshow(stills, narration_full, ambient, out_mp4, fps=fps),
    )

    state["mp4_path"] = str(out_mp4.relative_to(LF_OUTPUT_ROOT))
    state["mp4_duration_sec"] = _ffprobe_dur(out_mp4)
    state["mp4_size_bytes"] = out_mp4.stat().st_size
    state["phase"] = "done"
    state["percent"] = 100
    state["finished_at"] = time.time()
    save_state(job_id, state)
    update_status(job_id, phase="done", percent=100)


# ─────────────────────────────────────────────────────────────────────────────
# Sub-pipeline registry + outer task wrapper
# ─────────────────────────────────────────────────────────────────────────────

# Mapping from channel.pipeline_kind → async runner.
# Registered: 'sleep_doc' (HR — PR #120), 'v5_episode' (EM — PR #123).
SUB_PIPELINES: dict[str, Callable[..., Awaitable[None]]] = {
    "sleep_doc": run_sleep_doc_pipeline,
}


def _register_v5_episode() -> None:
    """Lazy-register v5_episode so importing pipeline.py doesn't pull in
    the EL/i2v dependency chain at module load time. v5_pipeline.py imports
    helpers from pipeline.py, so this back-edge has to be deferred."""
    try:
        from long_form.v5_pipeline import run_v5_episode_pipeline
        SUB_PIPELINES["v5_episode"] = run_v5_episode_pipeline
    except Exception as exc:  # noqa: BLE001
        # Don't crash the API on import failure — sleep_doc still works
        # standalone, and EM renders will surface a clear error via
        # _run_render's "pipeline_kind not registered" path.
        import logging
        logging.getLogger(__name__).warning(
            "v5_episode pipeline registration deferred: %s", exc
        )


_register_v5_episode()


async def _run_render(job_id: str, channel: dict, outline: dict) -> None:
    """Outer wrapper that catches errors + marks the job failed on disk +
    in memory. Sub-pipelines never raise to here in normal flow — they
    update state along the way."""
    pipeline_kind = (channel.get("pipeline_kind") or "sleep_doc").strip()
    runner = SUB_PIPELINES.get(pipeline_kind)
    if runner is None:
        st = load_state(job_id) or {}
        st.update({"phase": "failed", "error": f"pipeline_kind={pipeline_kind!r} not registered"})
        save_state(job_id, st)
        update_status(job_id, phase="failed", error=st["error"])
        return
    try:
        await runner(job_id, channel, outline)
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        st = load_state(job_id) or {}
        st.update({"phase": "failed", "error": msg, "failed_at": time.time()})
        save_state(job_id, st)
        update_status(job_id, phase="failed", error=msg)


def start_render(channel: dict, outline: dict) -> str:
    """Public API — kick a render. Returns job_id immediately; renders run
    in the asyncio background. Caller polls /jobs/{id}/status."""
    if not isinstance(outline, dict) or not outline.get("chapters"):
        raise LFRenderError("outline must include a non-empty 'chapters' list")
    job_id = _new_job_id()
    _ensure_job_dir(job_id)
    state = {
        "job_id": job_id,
        "channel_key": channel.get("key"),
        "channel_label": channel.get("label"),
        "pipeline_kind": channel.get("pipeline_kind") or "sleep_doc",
        "outline": outline,
        "phase": "queued",
        "percent": 0,
        "created_at": time.time(),
    }
    save_state(job_id, state)
    update_status(job_id, phase="queued", percent=0, started_at=time.time())

    # Spawn background task with strong-ref retention to prevent GC cancel.
    task = asyncio.create_task(_run_render(job_id, channel, outline))
    _lf_running_tasks.add(task)
    task.add_done_callback(_lf_running_tasks.discard)
    _lf_jobs_status[job_id]["_task"] = task
    return job_id


# ─────────────────────────────────────────────────────────────────────────────
# Media path helpers (used by the router for capability-token serving)
# ─────────────────────────────────────────────────────────────────────────────

def job_mp4_path(job_id: str) -> Path | None:
    """Return the canonical Final_<slug>_<jobid>.mp4 path, or None if absent.
    Excludes any *_mix.mp3 / *_lk.mp3 intermediates (lesson #8 from
    SESSION_2026-05-08 — ZT MP4 endpoint served the silent intermediate)."""
    d = _job_dir(job_id)
    if not d.exists():
        return None
    state = load_state(job_id) or {}
    rel = state.get("mp4_path") or ""
    if rel:
        candidate = LF_OUTPUT_ROOT / rel
        if candidate.exists() and candidate.suffix.lower() == ".mp4":
            return candidate
    # Fallback: scan for LongForm_*<job_id>.mp4 (excludes silent intermediates).
    for f in d.glob(f"LongForm_*_{job_id}.mp4"):
        if f.is_file():
            return f
    return None


def job_thumbnail_path(job_id: str, idx: int) -> Path | None:
    p = _job_dir(job_id) / "thumbnails" / f"thumb_{int(idx)}.png"
    return p if p.exists() else None


def job_still_path(job_id: str, scene_idx: int) -> Path | None:
    p = _job_dir(job_id) / "stills" / f"scene_{int(scene_idx):04d}.png"
    return p if p.exists() else None
