"""
URL-to-script ingestion for the Remix Script feature.

Given a TikTok / YouTube / Instagram Reel / Shorts URL, pulls the video's
captions (VTT) via yt-dlp and distills them into a plain-text transcript the
user can paste into the Studio Script tab.

yt-dlp supports 1000+ sites — if the site has embedded subtitles or YouTube's
auto-captions, we can read them without burning any API quota (yt-dlp scrapes
the public endpoints, not the YouTube Data API v3).

Public surface:
    async def ingest_url_transcript(url: str, *, language: str = "en") -> dict

Returns a dict matching the shape the /api/creative/ingest-url endpoint returns.
Never raises — errors become `{"ok": False, "error": "..."}` so the FastAPI
handler can turn them into 4xx without exception-message leaks.
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

import httpx

try:
    import yt_dlp  # type: ignore
except Exception:  # noqa: BLE001
    yt_dlp = None  # type: ignore

log = logging.getLogger("nyptid-studio")

TRANSCRIPT_CHAR_LIMIT = 12000  # plenty for any short-form source
YT_DLP_TIMEOUT_SEC = 30.0
SUBTITLE_FETCH_TIMEOUT_SEC = 15.0

# Hosts we explicitly advertise support for. yt-dlp handles many more, but we
# keep the allow-list tight so users get predictable behavior.
SUPPORTED_HOST_PATTERNS = (
    re.compile(r"(?:^|\.)youtube\.com$", re.IGNORECASE),
    re.compile(r"(?:^|\.)youtu\.be$", re.IGNORECASE),
    re.compile(r"(?:^|\.)tiktok\.com$", re.IGNORECASE),
    re.compile(r"(?:^|\.)instagram\.com$", re.IGNORECASE),
)


def _host_supported(url: str) -> bool:
    try:
        from urllib.parse import urlparse
        host = urlparse(str(url or "").strip()).hostname or ""
    except Exception:
        return False
    return any(p.search(host) for p in SUPPORTED_HOST_PATTERNS)


def _pick_subtitle_candidate(info: dict, language: str = "en") -> tuple[str, str]:
    """
    Walk info['subtitles'] then info['automatic_captions'] looking for the
    preferred language. Returns (url, ext); ext is usually "vtt" for YouTube.
    """
    preferred: list[str] = []
    lang = str(language or "en").strip().lower()
    if lang:
        preferred.extend([lang, f"{lang}-us", f"{lang}-en"])
    preferred.extend(["en-us", "en", "en-gb"])
    seen: set[str] = set()
    preferred = [p for p in preferred if not (p in seen or seen.add(p))]

    for pool_name in ("subtitles", "automatic_captions"):
        pool = info.get(pool_name) or {}
        if not isinstance(pool, dict):
            continue
        for wanted in preferred:
            variants = [wanted]
            if "-" in wanted:
                variants.append(wanted.split("-", 1)[0])
            for variant in variants:
                entries = pool.get(variant)
                if not isinstance(entries, list):
                    continue
                # Prefer vtt (easy to parse) over anything else.
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    url = str(entry.get("url", "") or "").strip()
                    ext = str(entry.get("ext", "") or "").strip().lower()
                    if url and ext == "vtt":
                        return url, ext
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    url = str(entry.get("url", "") or "").strip()
                    ext = str(entry.get("ext", "") or "").strip().lower()
                    if url:
                        return url, ext or "unknown"
    return "", ""


def _parse_vtt_text(raw_text: str, max_chars: int = TRANSCRIPT_CHAR_LIMIT) -> str:
    """
    Strip WebVTT cues, timestamps, and markup → clean prose transcript.
    Dedupes consecutive identical lines (YouTube auto-captions often repeat).
    """
    # VTT metadata keys that appear before the first cue — skip them entirely.
    # These are legal in WebVTT before any cue block; YouTube emits at least "Kind"
    # and "Language". We only drop them in the pre-cue region so an actual caption
    # that happens to contain a colon isn't eaten mid-transcript.
    VTT_META_KEYS = {"kind", "language", "styles", "region"}
    out: list[str] = []
    last: str = ""
    seen_first_cue = False
    for raw in str(raw_text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("WEBVTT") or line.startswith("NOTE "):
            continue
        if "-->" in line:
            seen_first_cue = True
            continue
        if re.match(r"^\d+$", line):
            continue
        if not seen_first_cue:
            # Before any cue, skip VTT metadata header lines like "Kind: captions".
            head = line.split(":", 1)[0].strip().lower()
            if head in VTT_META_KEYS:
                continue
        # Strip HTML-ish tags and entities.
        line = re.sub(r"<[^>]+>", " ", line)
        line = re.sub(r"&[a-zA-Z#0-9]+;", " ", line)
        line = re.sub(r"\s+", " ", line).strip()
        if not line or line == last:
            continue
        out.append(line)
        last = line
    joined = " ".join(out).strip()
    if len(joined) > max_chars:
        joined = joined[:max_chars].rsplit(" ", 1)[0] + "…"
    return joined


def _extract_with_yt_dlp(url: str) -> dict[str, Any]:
    """
    Synchronous yt-dlp wrapper. Called from an executor because yt-dlp blocks.
    """
    if yt_dlp is None:
        raise RuntimeError("yt-dlp is not installed on this server")
    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "extract_flat": False,
        "writesubtitles": False,
        "writeautomaticsub": False,
        # Tell yt-dlp what subtitle languages to list (does not download them).
        "subtitleslangs": ["en", "en-US", "en-GB"],
        "socket_timeout": 20,
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=False) or {}


async def ingest_url_transcript(url: str, *, language: str = "en") -> dict[str, Any]:
    """
    Main entry point.

    Returns a dict:
        ok: bool
        error: str                    # only when ok == False
        title: str
        source: str                   # e.g. "youtube", "tiktok", "instagram", "unknown"
        duration_sec: int
        url: str                      # canonical URL (from yt-dlp)
        transcript: str               # plain text, clipped at TRANSCRIPT_CHAR_LIMIT
        description: str              # video description (may be empty)
        warning: str                  # non-fatal info (e.g. "only auto-captions available")
    """
    clean_url = str(url or "").strip()
    if not clean_url:
        return {"ok": False, "error": "Missing URL"}
    if not re.match(r"^https?://", clean_url, re.IGNORECASE):
        clean_url = f"https://{clean_url}"
    if not _host_supported(clean_url):
        return {"ok": False, "error": "Only YouTube, TikTok, and Instagram URLs are supported right now."}
    if yt_dlp is None:
        return {"ok": False, "error": "Transcript extractor is not available on this server."}

    try:
        info = await asyncio.wait_for(
            asyncio.to_thread(_extract_with_yt_dlp, clean_url),
            timeout=YT_DLP_TIMEOUT_SEC,
        )
    except asyncio.TimeoutError:
        log.warning(f"Remix URL extract timed out: {clean_url}")
        return {"ok": False, "error": "Video metadata fetch timed out. Try a shorter clip or a different URL."}
    except Exception as e:  # noqa: BLE001
        # Keep the error message short so we don't leak internal details.
        short = str(e)[:160].replace("\n", " ")
        log.warning(f"Remix URL extract failed for {clean_url}: {short}")
        if "Sign in to confirm" in short or "age" in short.lower():
            return {"ok": False, "error": "That video requires sign-in or is age-gated — we can't read its transcript."}
        if "Private video" in short:
            return {"ok": False, "error": "That video is private."}
        return {"ok": False, "error": "Could not load that URL. Make sure it points directly to the video and try again."}

    subtitle_url, subtitle_ext = _pick_subtitle_candidate(info, language=language)
    transcript = ""
    warning = ""
    # Decide whether the captions we found were user-uploaded or auto-generated.
    # (Subtitle pool is checked first, so if we matched from automatic_captions,
    # the subtitles pool was empty or missing the language.)
    if subtitle_url:
        used_auto = True
        subtitles_pool = info.get("subtitles") or {}
        if isinstance(subtitles_pool, dict):
            for lang_key, entries in subtitles_pool.items():
                for entry in (entries or []) if isinstance(entries, list) else []:
                    if not isinstance(entry, dict):
                        continue
                    if str(entry.get("url", "") or "").strip() == subtitle_url:
                        used_auto = False
                        break
        if used_auto:
            warning = "Used auto-captions — review for accuracy before rendering."
        try:
            async with httpx.AsyncClient(timeout=SUBTITLE_FETCH_TIMEOUT_SEC, follow_redirects=True) as client:
                resp = await client.get(subtitle_url)
                resp.raise_for_status()
                raw_text = resp.text
        except Exception as e:  # noqa: BLE001
            log.warning(f"Remix subtitle fetch failed: {e}")
            return {"ok": False, "error": "We found captions but couldn't download them. Try another URL."}
        if subtitle_ext == "vtt" or "-->" in raw_text:
            transcript = _parse_vtt_text(raw_text)
        else:
            transcript = re.sub(r"\s+", " ", raw_text).strip()[:TRANSCRIPT_CHAR_LIMIT]

    if not transcript:
        # Last-resort fallback: use the description if the caller will find it useful.
        description = str(info.get("description", "") or "").strip()
        if description:
            return {
                "ok": False,
                "error": "No captions available on that video. Try a video that has captions or subtitles.",
                "title": str(info.get("title", "") or ""),
                "description": description[:TRANSCRIPT_CHAR_LIMIT],
            }
        return {"ok": False, "error": "No captions available on that video. Try a video that has captions or subtitles."}

    extractor_key = str(info.get("extractor_key", "") or info.get("extractor", "") or "").lower()
    if "youtube" in extractor_key:
        source = "youtube"
    elif "tiktok" in extractor_key:
        source = "tiktok"
    elif "instagram" in extractor_key:
        source = "instagram"
    else:
        source = extractor_key or "unknown"

    return {
        "ok": True,
        "title": str(info.get("title", "") or ""),
        "source": source,
        "duration_sec": int(info.get("duration") or 0),
        "url": str(info.get("webpage_url", "") or clean_url),
        "transcript": transcript,
        "description": str(info.get("description", "") or "")[:2000],
        "warning": warning,
    }
