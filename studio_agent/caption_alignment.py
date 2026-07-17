"""Verified word alignment and ASS generation shared by Studio compositors."""
from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Iterable

import httpx


WHISPER_ENDPOINT = "https://fal.run/fal-ai/whisper"


class CaptionAlignmentError(RuntimeError):
    pass


def _sec(value: Any) -> float:
    if isinstance(value, bool) or value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return 0.0
    return 0.0


def _times(row: dict[str, Any]) -> tuple[float, float]:
    stamp = row.get("timestamp")
    if isinstance(stamp, (list, tuple)):
        start = _sec(stamp[0] if stamp else 0)
        end = _sec(stamp[1] if len(stamp) > 1 else start)
    else:
        start = _sec(row.get("start", stamp))
        end = _sec(row.get("end", start))
    return max(0.0, start), max(start, end)


def parse_word_chunks(payload: dict[str, Any]) -> list[dict[str, Any]]:
    chunks = payload.get("chunks") or payload.get("words") or payload.get("segments") or []
    words: list[dict[str, Any]] = []
    for row in chunks:
        if not isinstance(row, dict):
            continue
        text = str(row.get("text") or row.get("word") or "").strip()
        if not text:
            continue
        start, end = _times(row)
        tokens = text.split()
        if not tokens:
            continue
        span = max(0.03, end - start)
        step = span / len(tokens)
        for index, token in enumerate(tokens):
            words.append({
                "text": token,
                "start": round(start + index * step, 4),
                "end": round(start + (index + 1) * step, 4),
            })
    validate_word_timings(words)
    return words


def validate_word_timings(words: Iterable[dict[str, Any]], *, duration_sec: float | None = None) -> None:
    prior_end = 0.0
    count = 0
    for row in words:
        start = _sec(row.get("start"))
        end = _sec(row.get("end"))
        if not math.isfinite(start) or not math.isfinite(end) or end <= start:
            raise CaptionAlignmentError("caption alignment returned an invalid word interval")
        if start + 0.08 < prior_end:
            raise CaptionAlignmentError("caption alignment returned non-monotonic words")
        if duration_sec is not None and end > float(duration_sec) + 0.08:
            raise CaptionAlignmentError("caption alignment extends past the final audio clock")
        prior_end = max(prior_end, end)
        count += 1
    if count == 0:
        raise CaptionAlignmentError("caption alignment returned no timed words")


def _audio_fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def align_audio_words(
    audio_path: Path,
    *,
    cache_path: Path | None = None,
    language: str = "en",
) -> list[dict[str, Any]]:
    """Return cached or FAL-Whisper-verified word timestamps.

    This is called only during an actual captioned production. Unit/release
    tests mock it and never contact a provider.
    """
    audio_path = Path(audio_path)
    if not audio_path.is_file() or audio_path.stat().st_size <= 512:
        raise CaptionAlignmentError("final narration audio is missing")
    fingerprint = _audio_fingerprint(audio_path)
    if cache_path and cache_path.is_file():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if cached.get("audio_sha256") == fingerprint:
                words = list(cached.get("words") or [])
                validate_word_timings(words)
                return words
        except Exception:
            pass

    fal_key = str(os.getenv("FAL_AI_KEY") or os.getenv("FAL_KEY") or "").strip()
    if not fal_key:
        raise CaptionAlignmentError("FAL_AI_KEY is required for verified caption alignment")
    import fal_client

    os.environ["FAL_KEY"] = fal_key
    audio_url = str(fal_client.upload_file(str(audio_path)) or "").strip()
    if not audio_url.startswith("https://"):
        raise CaptionAlignmentError("FAL upload returned no secure audio URL")
    with httpx.Client(timeout=600.0) as client:
        response = client.post(
            WHISPER_ENDPOINT,
            headers={"Authorization": f"Key {fal_key}"},
            json={
                "audio_url": audio_url,
                "task": "transcribe",
                "chunk_level": "word",
                "language": language,
            },
        )
    if response.status_code != 200:
        raise CaptionAlignmentError(f"caption alignment failed ({response.status_code}): {response.text[:240]}")
    try:
        payload = response.json()
    except Exception as exc:
        raise CaptionAlignmentError(f"caption alignment returned invalid JSON: {exc}") from exc
    words = parse_word_chunks(payload if isinstance(payload, dict) else {})
    if cache_path:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(
            json.dumps({
                "version": 1,
                "timing_source": "fal_whisper_word",
                "audio_sha256": fingerprint,
                "words": words,
            }, indent=2),
            encoding="utf-8",
        )
    return words


def group_word_cues(words: list[dict[str, Any]], *, mode: str = "word") -> list[dict[str, Any]]:
    normalized = "word" if str(mode or "").lower() == "word" else "phrase"
    if normalized == "word":
        return [dict(row) for row in words]
    cues: list[dict[str, Any]] = []
    group: list[dict[str, Any]] = []
    for row in words:
        if group and (_sec(row.get("start")) - _sec(group[-1].get("end")) > 0.45):
            cues.append(_merge_group(group))
            group = []
        group.append(row)
        if len(group) >= 3 or str(row.get("text") or "").rstrip().endswith((".", "!", "?", ",")):
            cues.append(_merge_group(group))
            group = []
    if group:
        cues.append(_merge_group(group))
    return cues


def _merge_group(group: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "text": " ".join(str(row.get("text") or "").strip() for row in group).strip(),
        "start": _sec(group[0].get("start")),
        "end": _sec(group[-1].get("end")),
    }


def _ass_time(seconds: float) -> str:
    value = max(0.0, float(seconds or 0.0))
    hours = int(value // 3600)
    minutes = int((value % 3600) // 60)
    secs = value % 60
    return f"{hours}:{minutes:02d}:{secs:05.2f}"


def _ass_text(text: str) -> str:
    return str(text or "").replace("\\", "").replace("{", "(").replace("}", ")").replace("\n", " ").upper()


def write_ass(
    cues: list[dict[str, Any]],
    out_path: Path,
    *,
    width: int,
    height: int,
    font_name: str = "Noto Sans",
) -> Path:
    validate_word_timings(cues)
    header = f"""[Script Info]
ScriptType: v4.00+
PlayResX: {int(width)}
PlayResY: {int(height)}
WrapStyle: 2

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Studio,{font_name},58,&H00FFFFFF,&H000088FF,&H00101010,&H90000000,-1,0,0,0,100,100,0,0,1,5,1,2,80,80,80,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""
    lines = [
        f"Dialogue: 0,{_ass_time(_sec(row.get('start')))},{_ass_time(_sec(row.get('end')))},Studio,,0,0,0,,{_ass_text(str(row.get('text') or ''))}"
        for row in cues
    ]
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(header + "\n".join(lines) + "\n", encoding="utf-8")
    return out_path


def write_caption_manifest(
    path: Path,
    *,
    words: list[dict[str, Any]],
    cues: list[dict[str, Any]],
    mode: str,
) -> None:
    Path(path).write_text(
        json.dumps({
            "version": 1,
            "enabled": True,
            "timing_source": "fal_whisper_word",
            "mode": mode,
            "words": words,
            "cues": cues,
        }, indent=2),
        encoding="utf-8",
    )
