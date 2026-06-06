"""Transcription with word-level cues for ClipLab."""
from __future__ import annotations

import asyncio
import json
import logging
import re
import subprocess
from pathlib import Path

import httpx

from cliplab.config import CLIPLAB_TRANSCRIPT_DIR
from cliplab.models import TranscriptCue, TranscriptWord

_log = logging.getLogger("nyptid-studio.cliplab.transcribe")

_VTT_TS = re.compile(
    r"(?P<h1>\d{2}):(?P<m1>\d{2}):(?P<s1>\d{2})[\.,](?P<ms1>\d{3})\s*-->\s*"
    r"(?P<h2>\d{2}):(?P<m2>\d{2}):(?P<s2>\d{2})[\.,](?P<ms2>\d{3})"
)


def _ts_to_sec(h: str, m: str, s: str, ms: str) -> float:
    return int(h) * 3600 + int(m) * 60 + int(s) + int(ms) / 1000.0


def parse_vtt_cues(raw_vtt: str) -> list[TranscriptCue]:
    cues: list[TranscriptCue] = []
    blocks = re.split(r"\n\s*\n", str(raw_vtt or ""))
    for block in blocks:
        lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
        if len(lines) < 2:
            continue
        ts_line = next((ln for ln in lines if "-->" in ln), "")
        m = _VTT_TS.search(ts_line)
        if not m:
            continue
        start = _ts_to_sec(m["h1"], m["m1"], m["s1"], m["ms1"])
        end = _ts_to_sec(m["h2"], m["m2"], m["s2"], m["ms2"])
        text_lines = [
            re.sub(r"<[^>]+>", " ", ln)
            for ln in lines
            if "-->" not in ln and not ln.isdigit() and not ln.startswith("WEBVTT")
        ]
        text = re.sub(r"\s+", " ", " ".join(text_lines)).strip()
        if not text:
            continue
        words_raw = text.split()
        dur = max(0.05, end - start)
        step = dur / max(1, len(words_raw))
        words = [
            TranscriptWord(text=w, start=round(start + i * step, 3), end=round(start + (i + 1) * step, 3))
            for i, w in enumerate(words_raw)
        ]
        cues.append(TranscriptCue(start=start, end=end, text=text, words=words))
    return cues


async def extract_audio_mp3(video_path: str, out_path: str, *, max_sec: float = 0) -> str:
    cmd = ["ffmpeg", "-y", "-i", video_path, "-vn", "-ac", "1", "-ar", "16000", "-acodec", "libmp3lame", "-q:a", "6"]
    if max_sec > 0:
        cmd.extend(["-t", str(max_sec)])
    cmd.append(out_path)
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    await proc.communicate()
    return out_path if Path(out_path).exists() else ""


async def transcribe_with_fal_whisper(audio_path: str, fal_key: str) -> list[TranscriptCue]:
    """Fal whisper with segment timestamps when FAL_AI_KEY is set."""
    if not fal_key or not Path(audio_path).exists():
        return []
    try:
        import fal_client
        url = fal_client.upload_file(audio_path)
        async with httpx.AsyncClient(timeout=600) as client:
            resp = await client.post(
                "https://fal.run/fal-ai/whisper",
                headers={"Authorization": f"Key {fal_key}"},
                json={"audio_url": url, "task": "transcribe", "chunk_level": "segment"},
            )
        if resp.status_code != 200:
            return []
        data = resp.json()
        chunks = list(data.get("chunks") or data.get("segments") or [])
        cues: list[TranscriptCue] = []
        for row in chunks:
            if not isinstance(row, dict):
                continue
            text = str(row.get("text") or "").strip()
            if not text:
                continue
            start = float(row.get("timestamp") or row.get("start") or 0)
            end = float(row.get("end") or start + max(1.0, len(text.split()) * 0.35))
            words_raw = text.split()
            step = max(0.05, (end - start) / max(1, len(words_raw)))
            words = [
                TranscriptWord(text=w, start=round(start + i * step, 3), end=round(start + (i + 1) * step, 3))
                for i, w in enumerate(words_raw)
            ]
            cues.append(TranscriptCue(start=start, end=end, text=text, words=words))
        return cues
    except Exception as exc:
        _log.warning("Fal whisper failed: %s", str(exc)[:200])
        return []


async def transcribe_video(
    video_path: str,
    video_id: str,
    *,
    vtt_text: str = "",
    fal_key: str = "",
) -> dict:
    """Return transcript JSON on disk + cues list."""
    out_json = CLIPLAB_TRANSCRIPT_DIR / f"{video_id}.json"
    cues: list[TranscriptCue] = []
    source = "vtt"

    if vtt_text.strip():
        cues = parse_vtt_cues(vtt_text)
    if not cues:
        audio_path = str(CLIPLAB_TRANSCRIPT_DIR / f"{video_id}.mp3")
        await extract_audio_mp3(video_path, audio_path)
        if fal_key:
            cues = await transcribe_with_fal_whisper(audio_path, fal_key)
            source = "fal_whisper" if cues else source
        if not cues and Path(audio_path).exists():
            # Last resort: single-chunk placeholder so LLM can still run on partial audio path
            dur = probe_duration(video_path)
            cues = [TranscriptCue(start=0, end=dur, text="(transcript pending — re-run with captions or Fal key)", words=[])]

    payload = {
        "video_id": video_id,
        "source": source,
        "cues": [c.model_dump() for c in cues],
        "duration_sec": max((c.end for c in cues), default=probe_duration(video_path)),
    }
    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return {"path": str(out_json), "cues": cues, "source": source}


def load_transcript(video_id: str) -> dict:
    path = CLIPLAB_TRANSCRIPT_DIR / f"{video_id}.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def transcript_plain(cues: list[TranscriptCue], *, max_chars: int = 120_000) -> str:
    lines = []
    total = 0
    for cue in cues:
        line = f"[{cue.start:.1f}-{cue.end:.1f}] {cue.text}"
        total += len(line)
        if total > max_chars:
            break
        lines.append(line)
    return "\n".join(lines)


def probe_duration(path: str) -> float:
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", path],
            capture_output=True, text=True, check=False,
        )
        return float(r.stdout.strip() or 0)
    except Exception:
        return 0.0
