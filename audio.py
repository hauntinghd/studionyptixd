from __future__ import annotations

import asyncio
import logging
import subprocess
import time
from pathlib import Path

import httpx
from fastapi import HTTPException

from backend_catalog import (
    RESOLUTION_CONFIGS,
    SUPPORTED_LANGUAGES,
    TEMPLATE_SFX_STYLES,
    TEMPLATE_VOICE_SETTINGS,
)
from backend_image_prompts import TEMPLATE_SFX_PROMPTS
from backend_settings import DISABLE_ALL_SFX, ELEVENLABS_API_KEY, TEMP_DIR

log = logging.getLogger("nyptid-studio")

DEFAULT_ELEVENLABS_VOICES = [
    {"voice_id": "EXAVITQu4vr4xnSDxMaL", "name": "Sarah", "category": "premade", "description": "Warm, upbeat female", "gender": "female", "accent": "american", "age": "young", "preview_url": ""},
    {"voice_id": "FGY2WhTYpPnrIDTdsKH5", "name": "Laura", "category": "premade", "description": "Neutral narration", "gender": "female", "accent": "american", "age": "young", "preview_url": ""},
    {"voice_id": "XB0fDUnXU5powFXDhCwa", "name": "Charlotte", "category": "premade", "description": "Calm storytelling", "gender": "female", "accent": "british", "age": "young", "preview_url": ""},
    {"voice_id": "pNInz6obpgDQGcFmaJgB", "name": "Adam", "category": "premade", "description": "Confident male narrator", "gender": "male", "accent": "american", "age": "middle_aged", "preview_url": ""},
    {"voice_id": "onwK4e9ZLuTAKqWW03F9", "name": "Daniel", "category": "premade", "description": "Clear educational tone", "gender": "male", "accent": "british", "age": "middle_aged", "preview_url": ""},
]
EDGE_TTS_DEFAULT_VOICES = {
    "en": {"female": "en-US-JennyNeural", "male": "en-US-GuyNeural"},
    "es": {"female": "es-ES-ElviraNeural", "male": "es-ES-AlvaroNeural"},
    "fr": {"female": "fr-FR-DeniseNeural", "male": "fr-FR-HenriNeural"},
    "de": {"female": "de-DE-KatjaNeural", "male": "de-DE-ConradNeural"},
    "it": {"female": "it-IT-ElsaNeural", "male": "it-IT-DiegoNeural"},
    "pt": {"female": "pt-BR-FranciscaNeural", "male": "pt-BR-AntonioNeural"},
    "hi": {"female": "hi-IN-SwaraNeural", "male": "hi-IN-MadhurNeural"},
    "ja": {"female": "ja-JP-NanamiNeural", "male": "ja-JP-KeitaNeural"},
}
_voice_catalog_cache = {"ts": 0.0, "source": "unknown", "provider_ok": False, "count": 0, "warning": "not_checked"}


def _fallback_voice_catalog() -> list[dict]:
    return [dict(v) for v in DEFAULT_ELEVENLABS_VOICES]


def _cache_voice_catalog(source: str, provider_ok: bool, count: int, warning: str = ""):
    _voice_catalog_cache["ts"] = time.time()
    _voice_catalog_cache["source"] = source
    _voice_catalog_cache["provider_ok"] = bool(provider_ok)
    _voice_catalog_cache["count"] = max(0, int(count or 0))
    _voice_catalog_cache["warning"] = warning or ""


async def _fetch_voice_catalog() -> tuple[list[dict], str, bool, str]:
    if not ELEVENLABS_API_KEY:
        warning = "ElevenLabs API key not configured; using fallback voices."
        return _fallback_voice_catalog(), "fallback", False, warning
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                "https://api.elevenlabs.io/v1/voices",
                headers={"xi-api-key": ELEVENLABS_API_KEY},
            )
            resp.raise_for_status()
            data = resp.json()
        voices = []
        for voice in data.get("voices", []):
            voices.append({
                "voice_id": voice["voice_id"],
                "name": voice.get("name", "Unknown"),
                "category": voice.get("category", ""),
                "description": voice.get("labels", {}).get("description", ""),
                "gender": voice.get("labels", {}).get("gender", ""),
                "accent": voice.get("labels", {}).get("accent", ""),
                "age": voice.get("labels", {}).get("age", ""),
                "preview_url": voice.get("preview_url", ""),
            })
        if voices:
            return voices, "elevenlabs", True, ""
        warning = "ElevenLabs returned zero voices; using fallback voices."
        return _fallback_voice_catalog(), "fallback", False, warning
    except Exception as exc:
        warning = f"ElevenLabs voice catalog unavailable ({type(exc).__name__}); using fallback voices."
        log.warning(warning)
        return _fallback_voice_catalog(), "fallback", False, warning


def _default_elevenlabs_voice_priority() -> list[str]:
    return [
        "pNInz6obpgDQGcFmaJgB",
        "EXAVITQu4vr4xnSDxMaL",
        "onwK4e9ZLuTAKqWW03F9",
        "XB0fDUnXU5powFXDhCwa",
        "FGY2WhTYpPnrIDTdsKH5",
    ]


async def _resolve_elevenlabs_voice_candidates(requested_voice_id: str = "") -> tuple[str, list[str], set[str]]:
    voices, source, provider_ok, warning = await _fetch_voice_catalog()
    available_ids = {
        str(voice.get("voice_id", "") or "").strip()
        for voice in voices
        if str(voice.get("voice_id", "") or "").strip()
    }
    requested = str(requested_voice_id or "").strip()
    ordered_candidates: list[str] = []

    def add_candidate(voice_id: str):
        vid = str(voice_id or "").strip()
        if vid and vid not in ordered_candidates:
            ordered_candidates.append(vid)

    add_candidate(requested)
    for default_voice_id in _default_elevenlabs_voice_priority():
        add_candidate(default_voice_id)

    if provider_ok and available_ids:
        filtered = [voice_id for voice_id in ordered_candidates if voice_id in available_ids]
        if not filtered:
            filtered = list(available_ids)
        selected_voice_id = filtered[0]
        if requested and requested not in available_ids:
            log.warning(
                "Requested ElevenLabs voice %s is unavailable in live catalog (%s voices, source=%s, warning=%s). Falling back to %s.",
                requested,
                len(available_ids),
                source,
                warning,
                selected_voice_id,
            )
        return selected_voice_id, filtered, available_ids

    selected_voice_id = requested or _default_elevenlabs_voice_priority()[0]
    return selected_voice_id, ordered_candidates, available_ids


def _is_retryable_elevenlabs_voice_error(status_code: int) -> bool:
    return int(status_code or 0) in {401, 403, 404}


def _edge_tts_rate_percent(speed: float) -> str:
    pct = int(round((float(speed) - 1.0) * 100))
    pct = max(-50, min(50, pct))
    return f"{pct:+d}%"


def _resolve_edge_tts_voice(language: str = "en", requested_voice_id: str = "", preferred_gender: str = "") -> str:
    lang_key = str(language or "en").split("-", 1)[0].lower()
    gender_key = "female" if str(preferred_gender or "").lower().startswith("f") else "male"
    catalog_by_id = {voice["voice_id"]: voice for voice in DEFAULT_ELEVENLABS_VOICES}
    requested_meta = catalog_by_id.get(str(requested_voice_id or "").strip(), {})
    if str(requested_meta.get("gender", "")).lower().startswith("f"):
        gender_key = "female"
    elif str(requested_meta.get("gender", "")).lower().startswith("m"):
        gender_key = "male"
    voice_map = EDGE_TTS_DEFAULT_VOICES.get(lang_key) or EDGE_TTS_DEFAULT_VOICES["en"]
    return voice_map.get(gender_key) or EDGE_TTS_DEFAULT_VOICES["en"]["female"]


async def _generate_voiceover_with_edge_tts(
    text: str,
    output_path: str,
    *,
    language: str = "en",
    requested_voice_id: str = "",
    preferred_gender: str = "",
    speed: float = 1.0,
) -> dict:
    import edge_tts

    edge_voice = _resolve_edge_tts_voice(
        language=language,
        requested_voice_id=requested_voice_id,
        preferred_gender=preferred_gender,
    )
    rate = _edge_tts_rate_percent(speed)
    communicate = edge_tts.Communicate(text=text, voice=edge_voice, rate=rate)
    await communicate.save(output_path)
    if not Path(output_path).exists() or Path(output_path).stat().st_size <= 0:
        raise RuntimeError("Edge TTS fallback produced no audio output")
    log.warning(
        "Using Edge TTS fallback voice %s for requested ElevenLabs voice %s",
        edge_voice,
        requested_voice_id or "(default)",
    )
    return {"audio_path": output_path, "word_timings": [], "voice_id": edge_voice, "provider": "edge_tts"}


async def generate_voiceover(
    text: str,
    output_path: str,
    template: str = "random",
    override_voice_id: str = "",
    language: str = "en",
    override_speed: float | None = None,
) -> dict:
    """Generate voiceover with word-level timestamps for caption sync."""
    vs = TEMPLATE_VOICE_SETTINGS.get(template, {})
    requested_voice_id = override_voice_id if override_voice_id else vs.get("voice_id", "pNInz6obpgDQGcFmaJgB")
    speed = float(override_speed) if override_speed is not None else float(vs.get("speed", 1.0))
    lang_cfg = SUPPORTED_LANGUAGES.get(language, SUPPORTED_LANGUAGES["en"])
    tts_model = lang_cfg["model"]
    voice_settings = {
        "stability": vs.get("stability", 0.5),
        "similarity_boost": vs.get("similarity_boost", 0.75),
        "style": vs.get("style", 0.3),
        "speed": max(0.8, min(1.35, speed)),
    }
    if not ELEVENLABS_API_KEY:
        log.warning("ElevenLabs API key not configured; using Edge TTS fallback for template=%s", template)
        return await _generate_voiceover_with_edge_tts(
            text=text,
            output_path=output_path,
            language=language,
            requested_voice_id=requested_voice_id,
            preferred_gender=str(vs.get("gender", "")),
            speed=speed,
        )
    resolved_voice_id, voice_candidates, available_voice_ids = await _resolve_elevenlabs_voice_candidates(
        requested_voice_id
    )
    last_error: Exception | None = None
    data: dict = {}
    used_voice_id = resolved_voice_id

    async with httpx.AsyncClient(timeout=120) as client:
        for candidate_voice_id in voice_candidates:
            if available_voice_ids and candidate_voice_id not in available_voice_ids:
                continue
            used_voice_id = candidate_voice_id
            url = f"https://api.elevenlabs.io/v1/text-to-speech/{candidate_voice_id}/with-timestamps"
            resp = await client.post(
                url,
                headers={
                    "xi-api-key": ELEVENLABS_API_KEY,
                    "Content-Type": "application/json",
                },
                json={
                    "text": text,
                    "model_id": tts_model,
                    "voice_settings": voice_settings,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                break
            if _is_retryable_elevenlabs_voice_error(resp.status_code):
                body_preview = (resp.text or "")[:200]
                log.warning(
                    "ElevenLabs voice %s rejected timestamped TTS with %s. Retrying fallback voice. Body: %s",
                    candidate_voice_id,
                    resp.status_code,
                    body_preview,
                )
                last_error = httpx.HTTPStatusError(
                    f"Retryable ElevenLabs voice failure ({resp.status_code})",
                    request=resp.request,
                    response=resp,
                )
                continue
            resp.raise_for_status()
        else:
            if last_error:
                log.warning("All ElevenLabs timestamp voices failed; falling back to Edge TTS. Last error: %s", last_error)
                return await _generate_voiceover_with_edge_tts(
                    text=text,
                    output_path=output_path,
                    language=language,
                    requested_voice_id=requested_voice_id,
                    preferred_gender=str(vs.get("gender", "")),
                    speed=speed,
                )
            raise HTTPException(502, "ElevenLabs voice generation failed before audio could be generated.")

    import base64 as b64mod
    audio_b64 = data.get("audio_base64", "")
    if audio_b64:
        audio_bytes = b64mod.b64decode(audio_b64)
        with open(output_path, "wb") as f:
            f.write(audio_bytes)
    else:
        log.warning("No audio_base64 in timestamps response for voice %s, falling back to standard endpoint", used_voice_id)
        fallback_audio: bytes | None = None
        last_fallback_error: Exception | None = None
        async with httpx.AsyncClient(timeout=120) as client:
            for candidate_voice_id in voice_candidates:
                if available_voice_ids and candidate_voice_id not in available_voice_ids:
                    continue
                fallback_resp = await client.post(
                    f"https://api.elevenlabs.io/v1/text-to-speech/{candidate_voice_id}",
                    headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
                    json={
                        "text": text,
                        "model_id": tts_model,
                        "voice_settings": {
                            "stability": vs.get("stability", 0.5),
                            "similarity_boost": vs.get("similarity_boost", 0.75),
                            "style": vs.get("style", 0.3),
                        },
                    },
                )
                if fallback_resp.status_code == 200:
                    fallback_audio = fallback_resp.content
                    used_voice_id = candidate_voice_id
                    break
                if _is_retryable_elevenlabs_voice_error(fallback_resp.status_code):
                    body_preview = (fallback_resp.text or "")[:200]
                    log.warning(
                        "ElevenLabs voice %s rejected fallback TTS with %s. Retrying next voice. Body: %s",
                        candidate_voice_id,
                        fallback_resp.status_code,
                        body_preview,
                    )
                    last_fallback_error = httpx.HTTPStatusError(
                        f"Retryable ElevenLabs fallback voice failure ({fallback_resp.status_code})",
                        request=fallback_resp.request,
                        response=fallback_resp,
                    )
                    continue
                fallback_resp.raise_for_status()
        if fallback_audio is None:
            if last_fallback_error:
                log.warning("All ElevenLabs standard fallback voices failed; using Edge TTS instead. Last error: %s", last_fallback_error)
                return await _generate_voiceover_with_edge_tts(
                    text=text,
                    output_path=output_path,
                    language=language,
                    requested_voice_id=requested_voice_id,
                    preferred_gender=str(vs.get("gender", "")),
                    speed=speed,
                )
            raise HTTPException(502, "ElevenLabs fallback voice generation failed before audio could be generated.")
        with open(output_path, "wb") as f:
            f.write(fallback_audio)
        return {"audio_path": output_path, "word_timings": [], "voice_id": used_voice_id}

    word_timings = _extract_word_timings(text, data.get("alignment", {}))
    log.info(f"Voiceover generated with {len(word_timings)} word timings using voice {used_voice_id}: {output_path}")
    return {"audio_path": output_path, "word_timings": word_timings, "voice_id": used_voice_id}


async def _voice_provider_snapshot(force_refresh: bool = False) -> dict:
    age_sec = time.time() - float(_voice_catalog_cache.get("ts", 0.0))
    if force_refresh or age_sec > 60.0:
        voices, source, provider_ok, warning = await _fetch_voice_catalog()
        _cache_voice_catalog(source, provider_ok, len(voices), warning)
        age_sec = 0.0
    return {
        "source": str(_voice_catalog_cache.get("source", "unknown")),
        "provider_ok": bool(_voice_catalog_cache.get("provider_ok", False)),
        "count": int(_voice_catalog_cache.get("count", 0) or 0),
        "warning": str(_voice_catalog_cache.get("warning", "") or ""),
        "age_sec": round(max(0.0, age_sec), 1),
    }

def _extract_word_timings(original_text: str, alignment: dict) -> list:
    """Convert ElevenLabs character-level alignment into word-level timings."""
    chars = alignment.get("characters", [])
    char_starts = alignment.get("character_start_times_seconds", [])
    char_ends = alignment.get("character_end_times_seconds", [])

    if not chars or not char_starts or not char_ends:
        return []
    if len(chars) != len(char_starts) or len(chars) != len(char_ends):
        return []

    words = []
    current_word = ""
    word_start = None

    for i, ch in enumerate(chars):
        if ch in (" ", "\n", "\t"):
            if current_word:
                words.append({
                    "word": current_word,
                    "start": word_start,
                    "end": char_ends[i - 1] if i > 0 else char_starts[i],
                })
                current_word = ""
                word_start = None
        else:
            if word_start is None:
                word_start = char_starts[i]
            current_word += ch

    if current_word and word_start is not None:
        words.append({
            "word": current_word,
            "start": word_start,
            "end": char_ends[-1],
        })

    return words

def _seconds_to_ass_timestamp(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    cs = int((seconds % 1) * 100)
    return f"{h}:{m:02d}:{s:02d}.{cs:02d}"

def generate_ass_subtitles(word_timings: list, output_path: str, resolution: str = "720p",
                           video_width: int = 0, video_height: int = 0, template: str = "") -> str:
    """Generate an ASS subtitle file with rapid single-word captions.
    Each word appears individually, large and bold, changing rapidly with every spoken word.
    High-retention viral TikTok/Reels style -- one word at a time, rapid fire.
    Supports both portrait (shorts) and landscape (product demo) layouts.
    """
    if video_width and video_height:
        res_w = video_width
        res_h = video_height
        is_landscape = res_w > res_h
    else:
        cfg = RESOLUTION_CONFIGS.get(resolution, RESOLUTION_CONFIGS.get("720p", {}))
        res_w = int(cfg.get("output_width", 720) or 720)
        res_h = int(cfg.get("output_height", 1280) or 1280)
        is_landscape = res_w > res_h

    skeleton_pro_style = (template == "skeleton" and not is_landscape)
    is_1080 = str(resolution).startswith("1080p")

    if skeleton_pro_style:
        font_size = 84 if is_1080 else 60
        outline = 3 if is_1080 else 2
        shadow = 2
        margin_v = int(res_h * 0.14)
        spacing = 0
        scale_xy = 100
        primary = "&H00FFFFFF"
        secondary = "&H00E7F4FF"
        outline_color = "&H00303030"
        back_color = "&H70000000"
    elif is_landscape:
        font_size = max(36, int(res_h * 0.045))
        outline = 3
        shadow = 1
        margin_v = int(res_h * 0.08)
        spacing = 2
        scale_xy = 105
        primary = "&H00FFFFFF"
        secondary = "&H000000FF"
        outline_color = "&H00000000"
        back_color = "&H96000000"
    else:
        font_size = 72 if resolution == "1080p" else 52
        outline = 5 if resolution == "1080p" else 4
        shadow = 2
        margin_v = int(res_h * 0.25)
        spacing = 2
        scale_xy = 105
        primary = "&H00FFFFFF"
        secondary = "&H000000FF"
        outline_color = "&H00000000"
        back_color = "&H96000000"

    header = f"""[Script Info]
Title: NYPTID Captions
ScriptType: v4.00+
PlayResX: {res_w}
PlayResY: {res_h}
WrapStyle: 0

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Word,Noto Sans,{font_size},{primary},{secondary},{outline_color},{back_color},-1,0,0,0,{scale_xy},{scale_xy},{spacing},0,1,{outline},{shadow},2,20,20,{margin_v},1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""

    def ts_to_ass(seconds: float) -> str:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)
        cs = int((seconds % 1) * 100)
        return f"{h}:{m:02d}:{s:02d}.{cs:02d}"

    # Landscape demos often have faster narration pacing than shorts.
    # Use shorter per-word minimum display to keep captions visually in sync.
    MIN_DISPLAY = 0.12 if is_landscape else 0.25

    timed = []
    for wt in word_timings:
        word = wt["word"].strip()
        if not word:
            continue
        timed.append({"word": word, "start": wt["start"], "end": wt["end"]})

    events = []
    for i, wt in enumerate(timed):
        start = wt["start"]
        natural_end = wt["end"]
        next_start = timed[i + 1]["start"] if i + 1 < len(timed) else natural_end + 0.5
        end = max(natural_end, start + MIN_DISPLAY)
        # Prevent overlap with the next word so captions don't visually trail speech.
        max_end = (next_start - 0.01) if (i + 1 < len(timed)) else (natural_end + 0.5)
        end = min(end, max_end)
        # If speech is very fast, allow shorter windows rather than forcing laggy overlap.
        if end <= start:
            end = start + 0.04

        # Preserve natural casing for skeleton "editorial" look, keep uppercase for other templates.
        clean_word = wt["word"].replace("\\", "").replace("{", "").replace("}", "")
        safe_word = clean_word if skeleton_pro_style else clean_word.upper()

        if skeleton_pro_style:
            # Subtle pop/fade reads closer to hand-edited NLE captions.
            pop_in = r"{\blur0.6\fad(35,45)\fscx100\fscy100\t(0,90,\fscx104\fscy104)\t(90,170,\fscx100\fscy100)}"
        else:
            pop_in = r"{\fscx130\fscy130\t(0,60,\fscx105\fscy105)}"
        events.append(
            f"Dialogue: 0,{ts_to_ass(start)},{ts_to_ass(end)},Word,,0,0,0,,{pop_in}{safe_word}"
        )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(header)
        f.write("\n".join(events))
        f.write("\n")

    log.info(f"ASS subtitle file generated: {output_path} ({len(events)} single-word captions)")
    return output_path

def generate_ass_scene_subtitles(
    scenes: list,
    output_path: str,
    resolution: str = "720p",
    video_width: int = 0,
    video_height: int = 0,
    template: str = "",
) -> str:
    if video_width and video_height:
        res_w = int(video_width)
        res_h = int(video_height)
        is_landscape = res_w > res_h
    else:
        cfg = RESOLUTION_CONFIGS.get(resolution, RESOLUTION_CONFIGS.get("720p", {}))
        res_w = int(cfg.get("output_width", 720) or 720)
        res_h = int(cfg.get("output_height", 1280) or 1280)
        is_landscape = res_w > res_h

    skeleton_pro_style = (template == "skeleton" and not is_landscape)
    is_1080 = str(resolution).startswith("1080p")

    if skeleton_pro_style:
        font_size = 84 if is_1080 else 60
        outline = 3 if is_1080 else 2
        shadow = 2
        margin_v = int(res_h * 0.14)
        spacing = 0
        scale_xy = 100
        primary = "&H00FFFFFF"
        secondary = "&H00E7F4FF"
        outline_color = "&H00303030"
        back_color = "&H70000000"
    elif is_landscape:
        font_size = max(36, int(res_h * 0.045))
        outline = 3
        shadow = 1
        margin_v = int(res_h * 0.08)
        spacing = 2
        scale_xy = 105
        primary = "&H00FFFFFF"
        secondary = "&H000000FF"
        outline_color = "&H00000000"
        back_color = "&H96000000"
    else:
        font_size = 72 if resolution == "1080p" else 52
        outline = 5 if resolution == "1080p" else 4
        shadow = 2
        margin_v = int(res_h * 0.25)
        spacing = 2
        scale_xy = 105
        primary = "&H00FFFFFF"
        secondary = "&H000000FF"
        outline_color = "&H00000000"
        back_color = "&H96000000"

    header = f"""[Script Info]
Title: NYPTID Captions
ScriptType: v4.00+
PlayResX: {res_w}
PlayResY: {res_h}
WrapStyle: 0

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Word,Noto Sans,{font_size},{primary},{secondary},{outline_color},{back_color},-1,0,0,0,{scale_xy},{scale_xy},{spacing},0,1,{outline},{shadow},2,20,20,{margin_v},1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""

    def split_caption_chunks(text: str) -> list[str]:
        clean = " ".join(str(text or "").split())
        if not clean:
            return []
        words = clean.split(" ")
        max_words = 6 if skeleton_pro_style else 5
        max_chars = 34 if skeleton_pro_style else 28
        chunks: list[str] = []
        current: list[str] = []
        for word in words:
            candidate = current + [word]
            candidate_text = " ".join(candidate)
            if current and (len(candidate) > max_words or len(candidate_text) > max_chars):
                chunks.append(" ".join(current))
                current = [word]
            else:
                current = candidate
        if current:
            chunks.append(" ".join(current))
        return chunks[:3]

    events: list[str] = []
    cursor = 0.0
    for raw_scene in scenes or []:
        scene = dict(raw_scene or {})
        try:
            duration = max(0.6, float(scene.get("duration_sec", 5.0) or 5.0))
        except Exception:
            duration = 5.0
        caption_source = (
            str(scene.get("text_overlay", "") or "").strip()
            or str(scene.get("narration", "") or "").strip()
        )
        chunks = split_caption_chunks(caption_source)
        if not chunks:
            cursor += duration
            continue
        chunk_duration = max(0.35, duration / max(len(chunks), 1))
        for idx, chunk in enumerate(chunks):
            start = cursor + (idx * chunk_duration)
            end = (cursor + duration - 0.04) if idx == len(chunks) - 1 else (cursor + ((idx + 1) * chunk_duration) - 0.04)
            if end <= start:
                end = start + 0.2
            clean_chunk = chunk.replace("\\", "").replace("{", "").replace("}", "")
            safe_chunk = clean_chunk if skeleton_pro_style else clean_chunk.upper()
            if skeleton_pro_style:
                pop_in = r"{\blur0.6\fad(40,60)\fscx100\fscy100\t(0,90,\fscx104\fscy104)\t(90,180,\fscx100\fscy100)}"
            else:
                pop_in = r"{\fscx118\fscy118\t(0,70,\fscx105\fscy105)}"
            events.append(
                f"Dialogue: 0,{_seconds_to_ass_timestamp(start)},{_seconds_to_ass_timestamp(end)},Word,,0,0,0,,{pop_in}{safe_chunk}"
            )
        cursor += duration

    if not events:
        return ""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(header)
        f.write("\n".join(events))
        f.write("\n")

    log.info(f"ASS scene subtitle file generated: {output_path} ({len(events)} timed caption beats)")
    return output_path

def _audio_track_exists(path: str) -> bool:
    p = Path(str(path or "").strip())
    return bool(p and p.exists() and p.stat().st_size > 0)

async def _mix_ambience_tracks(
    sfx_track: str,
    bgm_track: str,
    output_path: str,
    *,
    sfx_gain: float = 1.0,
    bgm_gain: float = 0.55,
) -> str:
    """Mix scene SFX and background music into one ambience track."""
    has_sfx = _audio_track_exists(sfx_track)
    has_bgm = _audio_track_exists(bgm_track)
    if not has_sfx and not has_bgm:
        return ""

    if has_sfx and has_bgm:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            sfx_track,
            "-i",
            bgm_track,
            "-filter_complex",
            (
                f"[0:a]volume={max(0.1, float(sfx_gain or 1.0)):.3f}[sfx];"
                f"[1:a]volume={max(0.05, float(bgm_gain or 0.55)):.3f},highpass=f=40,lowpass=f=6500[bgm];"
                "[sfx][bgm]amix=inputs=2:duration=longest:dropout_transition=2,"
                "alimiter=limit=0.93,apad=pad_dur=0.8[aout]"
            ),
            "-map",
            "[aout]",
            "-c:a",
            "libmp3lame",
            "-ar",
            "44100",
            "-ac",
            "1",
            output_path,
        ]
    else:
        src = sfx_track if has_sfx else bgm_track
        # Normalize to one consistent MP3 stream for downstream ffmpeg graph.
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            src,
            "-af",
            "apad=pad_dur=0.8",
            "-c:a",
            "libmp3lame",
            "-ar",
            "44100",
            "-ac",
            "1",
            output_path,
        ]

    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    await proc.communicate()
    if proc.returncode == 0 and _audio_track_exists(output_path):
        return output_path
    return ""


async def _generate_catalyst_bgm_track(
    total_duration_sec: float,
    output_path: str,
    music_profile: str = "",
    whisper_mode: str = "subtle",
    format_preset: str = "",
) -> str:
    duration = max(6.0, float(total_duration_sec or 0.0))
    fade_out_start = max(0.0, duration - 2.0)
    seed_clip = str(TEMP_DIR / f"bgm_seed_{int(time.time() * 1000)}.mp3")
    profile = str(music_profile or "").strip().lower()
    whisper_hint = "subtle whisper texture" if whisper_mode == "cinematic" else ("barely-there dark air" if whisper_mode == "subtle" else "clean no-whisper texture")
    prompt_map = {
        "cinematic_dark_tension": "Instrumental cinematic dark-tension music bed, ominous pulse, restrained trailer swell, no vocals, no speech, built to sit under narration.",
        "documentary_tension": "Instrumental premium documentary tension music bed, polished pulse, executive trailer texture, subtle low-end movement, no vocals, no speech, built for YouTube narration.",
        "kinetic_recap_bed": "Instrumental high-pressure recap music bed, rhythmic pulse, sharp energy, trailer percussion texture, no vocals, no speech, built to stay under narration.",
        "story_pulse_bed": "Instrumental cinematic story-channel bed, emotional pulse, controlled tension, light trailer texture, no vocals, no speech, built under narration.",
        "precision_explainer_bed": "Instrumental precision explainer music bed, clean pulse, polished documentary texture, restrained movement, no vocals, no speech, built under narration.",
    }
    bgm_prompt = prompt_map.get(profile) or prompt_map.get("documentary_tension")
    bgm_prompt = f"{bgm_prompt} {whisper_hint}. Format: {format_preset or 'documentary'}."
    try:
        if ELEVENLABS_API_KEY:
            async with httpx.AsyncClient(timeout=90) as client:
                resp = await client.post(
                    "https://api.elevenlabs.io/v1/sound-generation",
                    headers={
                        "xi-api-key": ELEVENLABS_API_KEY,
                        "Content-Type": "application/json",
                    },
                    json={
                        "text": bgm_prompt,
                        "duration_seconds": 22.0,
                        "prompt_influence": 0.64,
                    },
                )
            if resp.status_code in (200, 201):
                with open(seed_clip, "wb") as f:
                    f.write(resp.content)
                if _audio_track_exists(seed_clip):
                    cmd = [
                        "ffmpeg",
                        "-y",
                        "-stream_loop",
                        "-1",
                        "-i",
                        seed_clip,
                        "-t",
                        str(duration),
                        "-af",
                        f"afade=t=in:st=0:d=0.8,afade=t=out:st={fade_out_start}:d=2.0,apad=pad_dur=0.8",
                        "-ar",
                        "44100",
                        "-ac",
                        "1",
                        output_path,
                    ]
                    proc = await asyncio.create_subprocess_exec(
                        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                    )
                    await proc.communicate()
                    if proc.returncode == 0 and _audio_track_exists(output_path):
                        return output_path
        low_freq = 62 if "dark" in profile else (74 if "recap" in profile else 68)
        high_freq = 95 if "story" in profile else (112 if "recap" in profile else 88)
        noise_amp = "0.030" if "recap" in profile else ("0.022" if "story" in profile else "0.026")
        cmd = [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            f"sine=frequency={low_freq}:sample_rate=44100:duration={duration}",
            "-f",
            "lavfi",
            "-i",
            f"sine=frequency={high_freq}:sample_rate=44100:duration={duration}",
            "-f",
            "lavfi",
            "-i",
            f"anoisesrc=color=pink:amplitude={noise_amp}:sample_rate=44100:duration={duration}",
            "-filter_complex",
            (
                "[0:a]volume=0.15[a0];"
                "[1:a]volume=0.08,lowpass=f=1200[a1];"
                "[2:a]highpass=f=120,lowpass=f=4200,volume=0.07[a2];"
                f"[a0][a1][a2]amix=inputs=3:duration=longest:normalize=0,"
                f"afade=t=in:st=0:d=0.8,afade=t=out:st={fade_out_start}:d=2.0,apad=pad_dur=0.8[aout]"
            ),
            "-map",
            "[aout]",
            "-ar",
            "44100",
            "-ac",
            "1",
            output_path,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await proc.communicate()
        if proc.returncode == 0 and _audio_track_exists(output_path):
            return output_path
    except Exception as e:
        log.warning(f"Catalyst BGM generation failed (non-fatal): {e}")
    finally:
        Path(seed_clip).unlink(missing_ok=True)
    return ""


def _sfx_enabled() -> bool:
    return not DISABLE_ALL_SFX


async def generate_scene_sfx(
    visual_description: str,
    duration_sec: float,
    output_path: str,
    template: str = "",
    scene_index: int = -1,
    total_scenes: int = 0,
    force: bool = False,
) -> str:
    """Generate scene SFX with ElevenLabs Sound Effects API."""
    if ((not force) and (not _sfx_enabled())) or not ELEVENLABS_API_KEY:
        return ""

    style_hint = TEMPLATE_SFX_STYLES.get(template, "cinematic ambient atmosphere")
    transition_palette = [
        "smooth whoosh transition",
        "dramatic cinematic hit with sub bass",
        "quick snap transition accent",
        "soft zoom swell with airy tail",
        "blur sweep transition texture",
    ]
    visual_lower = str(visual_description or "").lower()
    if scene_index == 0:
        dynamic_layer = "strong opening hook impact, attention-grabbing stinger"
    elif total_scenes > 0 and scene_index == (total_scenes - 1):
        dynamic_layer = "final payoff impact, satisfying outro resolve"
    elif scene_index >= 0:
        dynamic_layer = transition_palette[scene_index % len(transition_palette)]
    else:
        dynamic_layer = "cinematic transition accent"
    detail_layers: list[str] = []
    if any(token in visual_lower for token in ["brain", "science", "medical", "anatomy", "lab", "neural", "memory"]):
        detail_layers.append("clean digital pulses, subtle lab hum, glossy interface sweeps")
    if any(token in visual_lower for token in ["business", "money", "market", "finance", "factory", "machine", "strategy"]):
        detail_layers.append("premium documentary whooshes, restrained sub hits, sharp executive trailer accents")
    if any(token in visual_lower for token in ["war", "killed", "attack", "crime", "danger", "dark", "mystery", "terror", "secret"]):
        detail_layers.append("tense low drone, metallic stress texture, ominous rise and impact")
    if any(token in visual_lower for token in ["map", "city", "location", "timeline", "history", "explainer"]):
        detail_layers.append("motion-graphic swells, crisp HUD ticks, soft transition suction")
    if not detail_layers:
        detail_layers.append("cinematic documentary sweeteners, subtle risers, polished transition texture")
    detail_layers.append("no speech, no vocals, no melody-led music bed")
    sfx_prompt = (
        f"{style_hint}, {dynamic_layer}, {', '.join(detail_layers)}, "
        f"matching visual: {visual_description[:240]}"
    )

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                "https://api.elevenlabs.io/v1/sound-generation",
                headers={
                    "xi-api-key": ELEVENLABS_API_KEY,
                    "Content-Type": "application/json",
                },
                json={
                    "text": sfx_prompt,
                    "duration_seconds": min(max(duration_sec, 0.8), 22.0),
                    "prompt_influence": 0.72,
                },
            )
            if resp.status_code != 200:
                log.warning(f"ElevenLabs SFX failed ({resp.status_code}): {resp.text[:200]}")
                return ""
            with open(output_path, "wb") as f:
                f.write(resp.content)
            if Path(output_path).stat().st_size > 0:
                log.info(f"SFX generated: {output_path} ({Path(output_path).stat().st_size / 1024:.0f} KB)")
                return output_path
            return ""
    except Exception as e:
        log.warning(f"SFX generation failed (non-fatal): {e}")
        return ""


def _probe_audio_duration_seconds(audio_path: str) -> float:
    """Best-effort audio duration probe using ffprobe."""
    try:
        proc = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                audio_path,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return float((proc.stdout or "0").strip() or 0)
    except Exception:
        return 0.0


def _probe_video_duration_seconds(video_path: str) -> float:
    """Best-effort video duration probe using ffprobe."""
    try:
        proc = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                video_path,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return float((proc.stdout or "0").strip() or 0)
    except Exception:
        return 0.0


def _rebalance_scene_durations_for_audio(
    scenes: list,
    audio_path: str,
    *,
    minimum_scene_duration: float = 3.5,
    ending_buffer_sec: float = 0.45,
) -> list[float]:
    """Stretch scene durations so the final video covers the voice track."""
    if not scenes:
        return []
    safe_min = max(0.5, float(minimum_scene_duration or 3.5))
    audio_duration = max(0.0, _probe_audio_duration_seconds(audio_path))
    base_durations: list[float] = []
    for index, raw_scene in enumerate(scenes):
        try:
            duration = float((raw_scene or {}).get("duration_sec", safe_min))
        except Exception:
            duration = safe_min
        duration = max(safe_min, min(duration, 12.0))
        if index == len(scenes) - 1:
            duration += max(0.2, float(ending_buffer_sec or 0.45))
        base_durations.append(duration)
    if audio_duration <= 0.1:
        return [round(duration, 2) for duration in base_durations]
    target_total = max(sum(base_durations), audio_duration + max(0.2, float(ending_buffer_sec or 0.45)))
    current_total = sum(base_durations)
    if target_total <= current_total + 0.05:
        return [round(duration, 2) for duration in base_durations]
    extra = target_total - current_total
    weights = [max(1.0, duration) for duration in base_durations]
    weight_total = sum(weights) or float(len(base_durations))
    adjusted: list[float] = []
    allocated = 0.0
    for index, duration in enumerate(base_durations):
        if index == len(base_durations) - 1:
            share = max(0.0, extra - allocated)
        else:
            share = extra * (weights[index] / weight_total)
            allocated += share
        adjusted.append(round(duration + share, 2))
    return adjusted


def _build_atempo_filter_chain(speed: float) -> str:
    """Build an ffmpeg atempo chain for any positive speed ratio."""
    if speed <= 0:
        return "atempo=1.0"
    parts = []
    remaining = float(speed)
    while remaining > 2.0:
        parts.append("atempo=2.0")
        remaining /= 2.0
    while remaining < 0.5:
        parts.append("atempo=0.5")
        remaining /= 0.5
    parts.append(f"atempo={remaining:.6f}")
    return ",".join(parts)


async def _quintuple_check_scene_sfx(
    scenes: list,
    sfx_paths: list[str],
    template: str,
    job_id: str = "",
) -> list[str]:
    """Quintuple-check scene SFX alignment and retry mismatched clips once."""
    fixed = list(sfx_paths or [])
    while len(fixed) < len(scenes):
        fixed.append("")

    for i, scene in enumerate(scenes):
        expected = float(scene.get("duration_sec", 5) or 5)
        sfx = fixed[i] if i < len(fixed) else ""
        ok_exists = bool(sfx and Path(sfx).exists() and Path(sfx).stat().st_size > 0)
        actual = _probe_audio_duration_seconds(sfx) if ok_exists else 0.0
        ok_duration = ok_exists and abs(actual - expected) <= 1.5
        ok_order = i < len(fixed)
        ok_scene = bool(scene.get("visual_description", "").strip())
        ok_nonempty_prompt = ok_scene

        if ok_exists and ok_duration and ok_order and ok_scene and ok_nonempty_prompt:
            continue

        retry_out = str(TEMP_DIR / (f"{job_id}_sfx_retry_{i}.mp3" if job_id else f"sfx_retry_{i}_{int(time.time()*1000)}.mp3"))
        desc = " ".join(
            part
            for part in [
                str(scene.get("visual_description", "") or "").strip(),
                f"SFX direction: {str(scene.get('sfx_direction', '') or '').strip()}."
                if str(scene.get("sfx_direction", "") or "").strip()
                else "",
                f"Engagement purpose: {str(scene.get('engagement_purpose', '') or '').strip()}."
                if str(scene.get("engagement_purpose", "") or "").strip()
                else "",
            ]
            if part
        ).strip()
        retry = await generate_scene_sfx(desc, expected, retry_out, template=template, scene_index=i, total_scenes=len(scenes))
        retry_ok = bool(retry and Path(retry).exists() and Path(retry).stat().st_size > 0)
        retry_dur = _probe_audio_duration_seconds(retry) if retry_ok else 0.0
        if retry_ok and abs(retry_dur - expected) <= 1.5:
            fixed[i] = retry
            log.info(f"[{job_id}] SFX scene {i+1} realigned on retry ({retry_dur:.2f}s vs expected {expected:.2f}s)")
        else:
            fixed[i] = ""
            log.warning(f"[{job_id}] SFX scene {i+1} failed alignment checks; using silence pad")

    return fixed


async def generate_sfx_for_scene(scene_desc: str, template: str, duration_sec: float, output_path: str) -> str:
    """Generate a sound effect for a scene using ElevenLabs Sound Effects API."""
    if not _sfx_enabled() or not ELEVENLABS_API_KEY:
        return ""
    base_sfx = TEMPLATE_SFX_PROMPTS.get(template, "Cinematic dramatic transition impact hit with bass")
    sfx_prompt = f"{base_sfx}. Scene: {scene_desc[:150]}"
    sfx_duration = min(max(duration_sec, 0.5), 22.0)

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                "https://api.elevenlabs.io/v1/sound-generation",
                headers={"xi-api-key": ELEVENLABS_API_KEY, "Content-Type": "application/json"},
                json={
                    "text": sfx_prompt,
                    "duration_seconds": sfx_duration,
                    "prompt_influence": 0.4,
                },
            )
            if resp.status_code not in (200, 201):
                log.warning(f"SFX generation failed ({resp.status_code}): {resp.text[:200]}")
                return ""
            with open(output_path, "wb") as f:
                f.write(resp.content)
        log.info(f"SFX generated: {output_path} ({Path(output_path).stat().st_size / 1024:.0f} KB)")
        return output_path
    except Exception as e:
        log.warning(f"SFX generation error (non-fatal): {e}")
        return ""
