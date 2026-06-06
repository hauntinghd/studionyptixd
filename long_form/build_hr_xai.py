#!/usr/bin/env python3
"""
History Rewind — local build pipeline (no fal.ai credits).

  Script  → xAI chat (grok-3-mini by default)
  Stills  → Grok Imagine Quality @ 1K ($0.05/image)
  Voice   → ElevenLabs custom sleep voice
  Video   → ffmpeg Ken-Burns slideshow, duration = narration length

Winner title pattern enforced (NOT "Rise and Fall | Full Documentary").

Usage:
  cd D:\\Games\\asd\\.claude\\worktrees\\laughing-mclean-b5c91d
  python -m long_form.build_hr_xai --topic "The Khmer Empire" --pilot
  python -m long_form.build_hr_xai --slug khmer_empire --images-only
  python -m long_form.build_hr_xai --slug khmer_empire --compose-only

Env (load from D:/Games/asd/.env):
  XAI_API_KEY
  ELEVENLABS_API_KEY
  HR_ELEVENLABS_VOICE_ID  (default Zu82ovvOlZd6iX0xbEzd)
  HR_XAI_TEXT_MODEL       (default grok-3-mini)
  HR_GROK_IMAGE_MODEL     (default grok-imagine-image-quality)
  HR_IMAGE_COST_USD       (default 0.05)
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dotenv import load_dotenv

# Repo root + shared env
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
load_dotenv(Path("D:/Games/asd/.env"))

sys.path.insert(0, str(ROOT))

from long_form.pipeline import (  # noqa: E402
    CHAPTER_PROMPT_TEMPLATE,
    _compose_slideshow,
    _ffmpeg_concat_audio,
    _ffprobe_dur,
    _gen_chapter,
    _list_scenes_sorted,
    _strip_json_fences,
    _two_pass_loudnorm,
)
from long_form.prompts.channels import CHANNELS, channel_outline_prompt_extras  # noqa: E402
from long_form.scripting import generate_outline  # noqa: E402

OUTPUT_ROOT = Path(os.environ.get("HR_OUTPUT_ROOT", "D:/recaps/history_rewind"))
XAI_BASE = "https://api.x.ai/v1"
DEFAULT_TEXT_MODEL = os.environ.get("HR_XAI_TEXT_MODEL", "grok-3-mini")
DEFAULT_IMAGE_MODEL = os.environ.get("HR_GROK_IMAGE_MODEL", "grok-imagine-image-quality")
IMAGE_COST_USD = float(os.environ.get("HR_IMAGE_COST_USD", "0.05"))
ELEVEN_VOICE = os.environ.get("HR_ELEVENLABS_VOICE_ID", "onwK4e9ZLuTAKqWW03F9")  # Daniel — sleep doc, works on free API
ELEVEN_VOICE_FALLBACK = os.environ.get("HR_ELEVENLABS_VOICE_FALLBACK", "JBFqnCBsd6RMkjVDRZzb")  # George
ELEVEN_MODEL = os.environ.get("HR_ELEVENLABS_MODEL", "eleven_turbo_v2_5")
WPM = 120
SCENES_PER_MINUTE = 1  # ~60s Ken Burns hold per still


class XAIClient:
    """Thin xAI chat wrapper — same .complete() shape as GrokClient."""

    def __init__(self, model: str | None = None) -> None:
        key = (os.environ.get("XAI_API_KEY") or "").strip()
        if not key:
            sys.exit("XAI_API_KEY missing — set in D:/Games/asd/.env")
        self.api_key = key
        self.model = model or DEFAULT_TEXT_MODEL
        try:
            from openai import OpenAI  # noqa: PLC0415
        except ImportError:
            sys.exit("pip install openai")
        self._client = OpenAI(api_key=self.api_key, base_url=XAI_BASE)

    def complete(
        self,
        system: str,
        user: str,
        *,
        max_tokens: int = 4000,
        temperature: float = 0.65,
    ) -> str:
        resp = self._client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            max_tokens=max_tokens,
            temperature=temperature,
        )
        content = resp.choices[0].message.content or ""
        return content.strip()


def _slugify(s: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")
    return s[:60] or "history_rewind"


def _job_dir(slug: str) -> Path:
    d = OUTPUT_ROOT / slug
    d.mkdir(parents=True, exist_ok=True)
    return d


def _visual_prefix(channel: dict) -> str:
    return (
        f"{channel.get('visual_style', '')}. "
        "Photoreal cinematic 16:9, period-accurate, soft warm lighting, "
        "no text, no watermark, no logos, no modern elements. "
    )


def gen_grok_image(prompt: str, out_path: Path, *, stats: dict) -> Path:
    """Grok Imagine Quality 1K still."""
    if out_path.exists() and out_path.stat().st_size > 8_000:
        print(f"  [img] cached {out_path.name}")
        return out_path
    import base64  # noqa: PLC0415
    from openai import OpenAI  # noqa: PLC0415

    client = OpenAI(api_key=os.environ["XAI_API_KEY"], base_url=XAI_BASE)
    full_prompt = prompt.strip()
    for attempt in range(4):
        try:
            resp = client.images.generate(
                model=DEFAULT_IMAGE_MODEL,
                prompt=full_prompt,
                n=1,
                response_format="b64_json",
                extra_body={"aspect_ratio": "16:9", "resolution": "1k"},
            )
            b64 = resp.data[0].b64_json if resp.data else ""
            if not b64:
                raise RuntimeError("no b64_json in response")
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(base64.b64decode(b64))
            stats["images"] = stats.get("images", 0) + 1
            stats["image_usd"] = stats.get("image_usd", 0.0) + IMAGE_COST_USD
            kb = out_path.stat().st_size // 1024
            print(f"  [img] {out_path.name}  ({kb} KB)  spend≈${stats['image_usd']:.2f}")
            return out_path
        except Exception as exc:
            if attempt < 3:
                wait = 2 + attempt * 3
                print(f"  [img] retry {attempt + 1} ({exc}), wait {wait}s")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError("image gen failed")


def gen_images_batch(
    chapters: list[dict],
    stills_dir: Path,
    channel: dict,
    *,
    stats: dict,
    concurrency: int = 16,
    max_images: int | None = None,
) -> list[Path]:
    prefix = _visual_prefix(channel)
    tasks: list[tuple[int, str, Path]] = []
    scenes_per = len(chapters[0].get("scene_prompts") or []) if chapters else 0
    for ch in chapters:
        ch_idx = int(ch.get("chapter_index", 0))
        for local_i, prompt in enumerate(ch.get("scene_prompts") or []):
            gidx = ch_idx * scenes_per + local_i if scenes_per else local_i
            out = stills_dir / f"scene_{gidx:04d}.png"
            tasks.append((gidx, prefix + prompt.strip(), out))

    if max_images is not None:
        tasks = tasks[:max_images]

    total = len(tasks)
    print(f"Generating {total} stills @ ${IMAGE_COST_USD:.2f} each (est ${total * IMAGE_COST_USD:.2f})")
    done_paths: list[Path] = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futs = {
            pool.submit(gen_grok_image, prompt, path, stats=stats): (gidx, path)
            for gidx, prompt, path in tasks
        }
        done = 0
        for fut in as_completed(futs):
            gidx, path = futs[fut]
            try:
                fut.result()
                done_paths.append(path)
            except Exception as exc:
                print(f"  [img] scene {gidx} FAILED: {exc}")
            done += 1
            if done % 5 == 0 or done == total:
                print(f"  [img] progress {done}/{total}")
    done_paths.sort(key=lambda p: int(re.search(r"scene_(\d+)", p.name).group(1)))
    return done_paths


def _elevenlabs_remaining() -> int:
    import httpx  # noqa: PLC0415

    key = (os.environ.get("ELEVENLABS_API_KEY") or "").strip()
    if not key:
        return 0
    r = httpx.get(
        "https://api.elevenlabs.io/v1/user/subscription",
        headers={"xi-api-key": key},
        timeout=30,
    )
    if r.status_code != 200:
        return 0
    d = r.json()
    return max(0, int(d.get("character_limit") or 0) - int(d.get("character_count") or 0))


def gen_elevenlabs_chapter(text: str, out_path: Path, *, allow_edge_fallback: bool = True) -> Path:
    """ElevenLabs narration: Tim → Daniel (premade) → edge-tts if quota/plan blocks."""
    import httpx  # noqa: PLC0415

    key = (os.environ.get("ELEVENLABS_API_KEY") or "").strip()
    if not key:
        sys.exit("ELEVENLABS_API_KEY missing")
    if out_path.exists() and out_path.stat().st_size > 4096:
        print(f"  [tts] cached {out_path.name}")
        return out_path

    text = text.strip()
    remaining = _elevenlabs_remaining()
    voices = [ELEVEN_VOICE, ELEVEN_VOICE_FALLBACK]

    def _write_edge() -> None:
        import asyncio  # noqa: PLC0415
        import edge_tts  # noqa: PLC0415

        voice = os.environ.get("HR_EDGE_VOICE", "en-US-GuyNeural")

        async def _run() -> None:
            comm = edge_tts.Communicate(text, voice, rate="-18%")
            await comm.save(str(out_path))

        print(f"  [tts] edge fallback ({voice}) — EL quota/plan limit")
        asyncio.run(_run())

    if remaining < len(text) and allow_edge_fallback:
        _write_edge()
        print(f"  [tts] {out_path.name}  ({out_path.stat().st_size // 1024} KB)")
        return out_path

    out_path.parent.mkdir(parents=True, exist_ok=True)
    headers = {"xi-api-key": key, "Content-Type": "application/json"}
    body_base = {
        "model_id": ELEVEN_MODEL,
        "voice_settings": {
            "stability": 0.62,
            "similarity_boost": 0.82,
            "style": 0.08,
            "use_speaker_boost": True,
        },
    }

    def _try_voice(voice_id: str, chunk: str) -> bytes | None:
        with httpx.Client(timeout=300) as client:
            r = client.post(
                f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}",
                headers=headers,
                json={**body_base, "text": chunk},
            )
            if r.status_code == 200:
                return r.content
            if r.status_code in (401, 402, 429):
                return None
            r.raise_for_status()
        return None

    chunks = [text] if len(text) <= 9500 else _chunk_narration(text, 9000)
    parts: list[Path] = []
    for i, chunk in enumerate(chunks):
        audio: bytes | None = None
        used_voice = ELEVEN_VOICE
        for vid in voices:
            audio = _try_voice(vid, chunk)
            if audio:
                used_voice = vid
                break
        if audio is None:
            if allow_edge_fallback:
                if len(chunks) == 1:
                    _write_edge()
                    print(f"  [tts] {out_path.name}  ({out_path.stat().st_size // 1024} KB)")
                    return out_path
                import asyncio  # noqa: PLC0415
                import edge_tts  # noqa: PLC0415

                part = out_path.with_name(f"{out_path.stem}_p{i:02d}.mp3")
                voice = os.environ.get("HR_EDGE_VOICE", "en-US-GuyNeural")

                async def _chunk() -> None:
                    comm = edge_tts.Communicate(chunk, voice, rate="-18%")
                    await comm.save(str(part))

                print(f"  [tts] chunk {i} edge fallback")
                asyncio.run(_chunk())
                parts.append(part)
                continue
            raise RuntimeError("ElevenLabs failed and edge fallback disabled")
        part = out_path if len(chunks) == 1 else out_path.with_name(f"{out_path.stem}_p{i:02d}.mp3")
        part.write_bytes(audio)
        if len(chunks) > 1:
            parts.append(part)
        print(f"  [tts] {part.name} via {used_voice[:8]}…  ({len(audio) // 1024} KB)")

    if len(chunks) > 1:
        _ffmpeg_concat_audio(parts, out_path)
        for p in parts:
            p.unlink(missing_ok=True)
    print(f"  [tts] {out_path.name}  ({out_path.stat().st_size // 1024} KB)")
    return out_path


def _chunk_narration(text: str, max_chars: int) -> list[str]:
    paras = re.split(r"\n\n+", text.strip())
    out: list[str] = []
    buf = ""
    for p in paras:
        cand = (buf + "\n\n" + p) if buf else p
        if len(cand) <= max_chars:
            buf = cand
        else:
            if buf:
                out.append(buf)
            buf = p
    if buf:
        out.append(buf)
    return out


def _runtime_badge(seconds: float) -> str:
    """YouTube thumb badge text from actual runtime (e.g. '1 HOUR', '8 HOURS')."""
    total_min = int(round(seconds / 60))
    if total_min < 60:
        return f"{max(1, total_min)} MIN"
    h, m = divmod(total_min, 60)
    if m == 0:
        return f"{h} HOUR" if h == 1 else f"{h} HOURS"
    if h == 1:
        return "1 HOUR"  # keep badge clean; title carries exact length
    return f"{h} HOURS"


def gen_thumbnail(
    topic: str,
    channel: dict,
    out_path: Path,
    stats: dict,
    *,
    runtime_sec: float | None = None,
) -> Path:
    badge = _runtime_badge(runtime_sec) if runtime_sec else "1 HOUR"
    style = (channel.get("thumbnail_style_prompt") or "").replace("9 HOURS", badge)
    prompt = (
        f"{style} "
        f"Topic: {topic}. Bold 3-word title overlay (e.g. 'KHMER EMPIRE'). "
        f"The corner badge MUST read exactly '{badge}' — NOT '9 HOURS'."
    )
    if out_path.exists():
        out_path.unlink()
    return gen_grok_image(prompt, out_path, stats=stats)


def _natural_target_minutes(pilot: bool) -> int:
    return 18 if pilot else 0  # 0 = let outline decide length


def run_outline(
    xai: XAIClient,
    channel: dict,
    topic: str,
    *,
    pilot: bool,
) -> dict:
    target = _natural_target_minutes(pilot)
    if pilot:
        target_minutes = target
    else:
        target_minutes = 360  # soft ceiling for outline pass; chapters can be trimmed

    extras = channel_outline_prompt_extras("history_rewind")
    sys_addendum = (
        extras
        + "\n\nDURATION RULE: Do NOT pad to 9 hours. Choose chapter count and "
        "minutes so the topic is told completely at ~120 wpm calm sleep pacing. "
        "Typical docs run 4–8 hours but shorter is fine if the story is complete."
    )
    channel_copy = dict(channel)
    channel_copy["system_prompt"] = channel_copy.get("system_prompt", "") + sys_addendum

    outline = generate_outline(
        xai,  # type: ignore[arg-type]
        channel_copy["system_prompt"],
        topic=topic,
        target_minutes=target_minutes if pilot else 480,
        title_template_block=extras,
    )
    if pilot and outline.get("chapters"):
        outline["chapters"] = outline["chapters"][:1]
        outline["chapters"][0]["minutes"] = max(
            15, min(int(outline["chapters"][0].get("minutes", 18)), 20)
        )
    outline["target_duration_sec"] = sum(
        int(c.get("minutes", 0)) * 60 for c in outline.get("chapters") or []
    )
    return outline


def run_chapters(
    xai: XAIClient,
    channel: dict,
    outline: dict,
    job: Path,
    *,
    chapter_indices: list[int] | None = None,
) -> list[dict]:
    chapters_path = job / "chapters.json"
    existing: list[dict] = []
    if chapters_path.exists():
        existing = json.loads(chapters_path.read_text(encoding="utf-8"))

    ch_list = outline.get("chapters") or []
    indices = chapter_indices if chapter_indices is not None else list(range(len(ch_list)))
    out_by_idx: dict[int, dict] = {int(c.get("chapter_index", i)): c for i, c in enumerate(existing)}

    for idx in indices:
        if idx in out_by_idx and out_by_idx[idx].get("narration"):
            print(f"[chapter {idx}] cached")
            continue
        ch = ch_list[idx]
        minutes = max(1, int(ch.get("minutes", 15)))
        scenes_per = max(8, minutes * SCENES_PER_MINUTE)
        print(f"[chapter {idx}] expanding ({minutes} min, {scenes_per} scenes)...")
        data = _gen_chapter(
            xai,  # type: ignore[arg-type]
            channel=channel,
            outline=outline,
            chapter_index=idx,
            chapter_count=len(ch_list),
            scenes_per_chapter=scenes_per,
            wpm=WPM,
        )
        out_by_idx[idx] = data

    merged = [out_by_idx[i] for i in sorted(out_by_idx)]
    chapters_path.write_text(json.dumps(merged, indent=2), encoding="utf-8")
    return merged


def run_tts(chapters: list[dict], job: Path) -> Path:
    audio_dir = job / "audio"
    audio_dir.mkdir(exist_ok=True)
    parts: list[Path] = []
    for ch in chapters:
        idx = int(ch.get("chapter_index", 0))
        out = audio_dir / f"chapter_{idx:02d}.mp3"
        gen_elevenlabs_chapter(ch.get("narration", ""), out)
        parts.append(out)
    narr = audio_dir / "narration.mp3"
    if len(parts) == 1:
        if narr != parts[0]:
            narr.write_bytes(parts[0].read_bytes())
    else:
        _ffmpeg_concat_audio(parts, narr)
    lk = audio_dir / "narration_lk.mp3"
    _two_pass_loudnorm(narr, lk)
    return lk


def _silent_ambient(duration_sec: float, out_path: Path) -> Path:
    """Minimal bed — silence placeholder (no fal mmaudio)."""
    if out_path.exists():
        return out_path
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
        "-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo",
        "-t", f"{max(1.0, duration_sec):.3f}",
        "-c:a", "libmp3lame", "-b:a", "128k",
        str(out_path),
    ]
    subprocess.run(cmd, check=True)
    return out_path


def _format_runtime_hours(seconds: float) -> str:
    h = max(1, round(seconds / 3600))
    return f"{h} Hour{'s' if h != 1 else ''}"


def _fix_title_runtime(title: str, seconds: float) -> str:
    runtime = _format_runtime_hours(seconds)
    title = re.sub(r"\|\s*History for Sleep\s*\|\s*\d+\s*Hours?\s*$", "", title, flags=re.I)
    title = re.sub(r"\|\s*\d+\s*Hours?\s*$", "", title, flags=re.I)
    for bad in ("Rise and Fall", "Full Documentary", "Complete History"):
        title = re.sub(re.escape(bad), "", title, flags=re.I)
    title = re.sub(r"\s{2,}", " ", title).replace(" | |", " |").strip(" |")
    return f"{title} | History for Sleep | {runtime}"


def run_compose(job: Path, outline: dict, fps: int = 30) -> Path:
    stills = _list_scenes_sorted(job / "stills")
    narr = job / "audio" / "narration_lk.mp3"
    if not narr.exists():
        narr = job / "audio" / "narration.mp3"
    dur = _ffprobe_dur(narr)
    ambient = job / "audio" / "ambient.mp3"
    _silent_ambient(dur, ambient)
    out = job / f"{job.name}_final.mp4"
    _compose_slideshow(stills, narr, ambient, out, fps=fps)

    meta = {
        "title": _fix_title_runtime(outline.get("title", job.name), dur),
        "duration_sec": dur,
        "duration_hms": time.strftime("%H:%M:%S", time.gmtime(dur)),
        "scene_count": len(stills),
        "output": str(out),
    }
    (job / "upload_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    topic = outline.get("title", job.name).split("|")[0].strip()
    gen_thumbnail(topic, CHANNELS["history_rewind"], job / "thumbnail.png", {"images": 0, "image_usd": 0.0}, runtime_sec=dur)
    print(f"Thumbnail badge: {_runtime_badge(dur)}")
    print(f"\nDONE  {out}")
    print(f"Title: {meta['title']}")
    print(f"Runtime: {meta['duration_hms']}  ({len(stills)} scenes)")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="History Rewind XAI build pipeline")
    ap.add_argument("--topic", default="The Khmer Empire — Angkor's rise and the jungle reclaiming the temples")
    ap.add_argument("--slug", default="", help="output folder name (default: slugified topic)")
    ap.add_argument("--pilot", action="store_true", help="single ~18min chapter proof")
    ap.add_argument("--outline-only", action="store_true")
    ap.add_argument("--chapters-only", action="store_true")
    ap.add_argument("--images-only", action="store_true")
    ap.add_argument("--tts-only", action="store_true")
    ap.add_argument("--compose-only", action="store_true")
    ap.add_argument("--thumbnail-only", action="store_true")
    ap.add_argument("--max-images", type=int, default=0, help="cap image spend (0=all)")
    ap.add_argument("--resume", action="store_true", help="skip outline if outline.json exists")
    ap.add_argument("--chapter", type=int, default=-1, help="only expand chapter N")
    args = ap.parse_args()

    channel = CHANNELS["history_rewind"]
    slug = args.slug or _slugify(args.topic)
    job = _job_dir(slug)
    stats: dict = {"images": 0, "image_usd": 0.0}

    outline_path = job / "outline.json"
    if args.compose_only or args.tts_only or args.images_only or args.thumbnail_only:
        if not outline_path.exists():
            sys.exit(f"Missing {outline_path} — run outline first")
        outline = json.loads(outline_path.read_text(encoding="utf-8"))
    else:
        outline = None

    if args.thumbnail_only:
        meta_path = job / "upload_meta.json"
        dur = float(json.loads(meta_path.read_text()).get("duration_sec", 0)) if meta_path.exists() else 0.0
        if dur <= 0:
            narr = job / "audio" / "narration_lk.mp3"
            if not narr.exists():
                narr = job / "audio" / "narration.mp3"
            dur = _ffprobe_dur(narr) if narr.exists() else 3600.0
        topic = outline.get("title", args.topic).split("|")[0].strip()
        print(f"=== THUMBNAIL ({_runtime_badge(dur)}) ===")
        gen_thumbnail(topic, channel, job / "thumbnail.png", stats, runtime_sec=dur)
        print(f"Saved {job / 'thumbnail.png'}")
        spend_path = job / "xai_spend.json"
        prev = json.loads(spend_path.read_text()) if spend_path.exists() else {}
        prev["thumbnail_regen_usd"] = IMAGE_COST_USD
        spend_path.write_text(json.dumps(prev, indent=2), encoding="utf-8")
        return

    xai = XAIClient()

    if not args.images_only and not args.tts_only and not args.compose_only:
        if args.resume and outline_path.exists():
            outline = json.loads(outline_path.read_text(encoding="utf-8"))
            print(f"=== RESUME outline from {outline_path} ({len(outline.get('chapters') or [])} chapters) ===")
        else:
            print(f"=== OUTLINE  topic={args.topic!r}  pilot={args.pilot} ===")
            outline = run_outline(xai, channel, args.topic, pilot=args.pilot)
            outline_path.write_text(json.dumps(outline, indent=2), encoding="utf-8")
            print(f"Title: {outline.get('title')}")
            print(f"Chapters: {len(outline.get('chapters') or [])}")
            if args.outline_only:
                print(f"Saved {outline_path}")
                return

    if args.chapters_only or (
        not args.images_only and not args.tts_only and not args.compose_only
    ):
        assert outline is not None
        ch_idx = [args.chapter] if args.chapter >= 0 else None
        print("=== CHAPTERS ===")
        run_chapters(xai, channel, outline, job, chapter_indices=ch_idx)

    chapters = json.loads((job / "chapters.json").read_text(encoding="utf-8"))

    if args.images_only or (
        not args.tts_only and not args.compose_only and not args.outline_only and not args.chapters_only
    ):
        cap = args.max_images if args.max_images > 0 else None
        if cap:
            est = cap * IMAGE_COST_USD
            print(f"=== IMAGES (capped {cap}, est ${est:.2f}) ===")
        else:
            n = sum(len(c.get("scene_prompts") or []) for c in chapters)
            print(f"=== IMAGES ({n} scenes, est ${n * IMAGE_COST_USD:.2f}) ===")
        stills_dir = job / "stills"
        gen_images_batch(
            chapters, stills_dir, channel, stats=stats, max_images=cap
        )
        print(f"Image spend tracked: ${stats.get('image_usd', 0):.2f}")
        print("(Thumbnail generated after compose with correct runtime badge.)")

    if args.tts_only or (
        not args.images_only and not args.compose_only and not args.outline_only and not args.chapters_only
    ):
        print("=== TTS (ElevenLabs) ===")
        run_tts(chapters, job)

    if args.compose_only or (
        not args.outline_only and not args.chapters_only and not args.images_only and not args.tts_only and not args.thumbnail_only
    ):
        print("=== COMPOSE ===")
        run_compose(job, outline, fps=int(channel.get("fps") or 30))

    spend_path = job / "xai_spend.json"
    prev = {}
    if spend_path.exists():
        prev = json.loads(spend_path.read_text(encoding="utf-8"))
    prev.update(stats)
    spend_path.write_text(json.dumps(prev, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
