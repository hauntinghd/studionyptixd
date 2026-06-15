"""
Two-tier caption renderer for Skeleton AI shorts.

Per canonical spec:
  - ORANGE bold = key data ($amount, age, name, "USA", "NASCAR", "HAMILTON")
  - WHITE bold  = filler narration words
  - Black 5-6px stroke on all text
  - Komika Axis-style font, all caps
  - Bottom 25% of frame, centered
  - 1-3 words at a time, ~0.5-1 sec per phrase

Plus "Cryptic Science" watermark in retro stencil at bottom-center.

Output: drawtext filter strings ready to splice into ffmpeg compose.
"""
from __future__ import annotations
import re
from dataclasses import dataclass

# Default font on Windows; deployment image will need to ship this font.
DEFAULT_FONT = "C\\:/Windows/Fonts/arialbd.ttf"
WATERMARK_TEXT = "ZeroTier"  # Updated for ZeroTier channel (was Cryptic Science in tests)

# Patterns that mark a word as Tier-1 KEY DATA (orange).
KEY_PATTERNS = [
    re.compile(r"^\$[\d,.]+(?:[KMB])?$", re.I),    # $135,000  $1.5M
    re.compile(r"^\d+(?:,\d{3})*$"),                 # 50000  1,000,000
    re.compile(r"^\d+%$"),                            # 25%
    re.compile(r"^age\s*\d+$", re.I),                # Age 24
    re.compile(r"^year\s*\d+$", re.I),               # Year 5
    re.compile(r"^\d+\s*years?$", re.I),             # 4 years
]

# Specific tokens that are always orange (named entities).
KEY_LITERALS = {
    "USA", "FBI", "CIA", "NASA", "NASCAR", "F1",
    "MARVEL", "DC", "ROME", "SPARTA", "EGYPT",
}


@dataclass
class CaptionPhrase:
    text: str           # the words shown (1-3 of them, all caps)
    start_sec: float    # when it appears
    duration_sec: float # how long it stays
    is_key: bool        # True → orange tier, False → white tier


def is_key_word(word: str) -> bool:
    """Returns True if a word should render as orange (Tier 1)."""
    upper = word.strip().upper()
    if upper in KEY_LITERALS:
        return True
    return any(p.match(word.strip()) for p in KEY_PATTERNS)


def _token_weight(word: str) -> int:
    """Hyphenated data like 28-year-old should occupy the whole caption beat."""
    clean = word.strip()
    if re.search(r"\d+\s*-\s*year\s*-\s*old", clean, re.I):
        return max_words_for_numeric_phrase()
    if "-" in clean and any(ch.isdigit() for ch in clean):
        return 2
    return 1


def max_words_for_numeric_phrase() -> int:
    return 3


def split_into_phrases(text: str, max_words: int = 3) -> list[str]:
    """Break a sentence into phrase chunks of <= max_words for caption display."""
    max_words = max(1, int(max_words or 3))
    words = text.strip().split()
    out: list[str] = []
    i = 0
    while i < len(words):
        # If the next word is a key word, group it alone or with one connector.
        if max_words == 1:
            out.append(words[i].upper())
            i += 1
        elif is_key_word(words[i]) or _token_weight(words[i]) >= max_words:
            out.append(words[i].upper())
            i += 1
        else:
            # Group up to max_words consecutive non-key words.
            chunk: list[str] = []
            weight = 0
            while i < len(words) and weight < max_words and not is_key_word(words[i]):
                w = _token_weight(words[i])
                if chunk and weight + w > max_words:
                    break
                chunk.append(words[i].upper())
                weight += w
                i += 1
            if chunk:
                out.append(" ".join(chunk))
    return out


def time_phrases(phrases: list[str], total_duration: float) -> list[CaptionPhrase]:
    """Distribute caption beats across total_duration.

    Word-by-word captions drift badly if every token receives equal time. TTS
    naturally lingers on longer words, hyphenated numbers, and sentence-final
    punctuation, so weight those beats instead of using a flat interval.
    """
    if not phrases:
        return []
    weights: list[float] = []
    for phrase in phrases:
        words = phrase.replace("-", " ").split()
        char_weight = max(1.0, len(re.sub(r"[^A-Z0-9]", "", phrase.upper())) / 5.2)
        punct_pause = 0.75 if re.search(r"[.!?]$", phrase.strip()) else 0.35 if re.search(r"[,;:]$", phrase.strip()) else 0.0
        numeric_weight = 0.45 if any(ch.isdigit() for ch in phrase) else 0.0
        weights.append(max(1.0, len(words) * 0.55 + char_weight + punct_pause + numeric_weight))
    total_weight = max(1, sum(weights))
    min_phrase = 0.16 if len(phrases) >= 12 else 0.36
    variable_total = max(float(total_duration) - (min_phrase * len(phrases)), 0.0)
    out: list[CaptionPhrase] = []
    t = 0.0
    for p, weight in zip(phrases, weights):
        is_key = any(is_key_word(w) for w in p.split())
        dur = min_phrase + variable_total * (weight / total_weight)
        out.append(CaptionPhrase(text=p, start_sec=t, duration_sec=dur, is_key=is_key))
        t += dur
    return out


def _esc(text: str) -> str:
    """Sanitize + escape text for ffmpeg drawtext.

    drawtext escaping is hellish with apostrophes — every workaround breaks on
    Windows shell quoting. Easiest fix: strip apostrophes/quotes/em-dashes/etc
    from caption text outright. Captions are short anyway (1-3 words) — losing
    a contraction is fine.
    """
    # Replace problematic Unicode + ASCII special chars.
    repl = {
        "‘": "", "’": "",       # smart single quotes
        "“": "", "”": "",       # smart double quotes
        "–": "-", "—": "-",     # en/em dash
        "…": "...",                   # ellipsis
        "'": "", '"': "",                  # ASCII quotes
        "%": "",                           # ffmpeg drawtext treats % specially
        "\\": "",                          # backslash itself
    }
    for src, dst in repl.items():
        text = text.replace(src, dst)
    # Now escape only the ffmpeg-special chars that remain.
    return text.replace(":", "\\:").replace(",", "\\,")


def caption_drawtext(
    phrase: CaptionPhrase,
    *,
    width: int = 720,
    font: str = DEFAULT_FONT,
    caption_mode: str = "phrase",
) -> str:
    """Build an ffmpeg drawtext filter for one CaptionPhrase."""
    fontcolor = "#FF8800" if phrase.is_key else "white"
    n = len(phrase.text)
    if caption_mode == "word":
        size = 82 if n <= 10 else 72
    elif n <= 12:
        size = 96
    elif n <= 20:
        size = 76
    else:
        size = 60
    enable = (
        f"between(t\\,{phrase.start_sec:.3f}\\,"
        f"{phrase.start_sec + phrase.duration_sec:.3f})"
    )
    return (
        f"drawtext=fontfile='{font}':text='{_esc(phrase.text)}':"
        f"fontsize={size}:fontcolor={fontcolor}:"
        f"bordercolor=black:borderw=6:"
        f"x=(w-text_w)/2:y=h*0.78:enable='{enable}'"
    )


def watermark_drawtext(font: str = DEFAULT_FONT, watermark_text: str = WATERMARK_TEXT) -> str:
    """Persistent channel watermark at bottom-center (ZeroTier for this channel)."""
    return (
        f"drawtext=fontfile='{font}':text='{_esc(watermark_text)}':"
        f"fontsize=36:fontcolor=white@0.85:"
        f"bordercolor=black@0.7:borderw=3:"
        f"x=(w-text_w)/2:y=h*0.93"
    )
