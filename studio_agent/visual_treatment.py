"""Script-aware visual treatment planning and deterministic motion graphics.

The planner is intentionally deterministic at render time: language models may
write a story, but they must not invent evidence cards, dates, or numbers.  A
scene is only routed to graphics when its narration itself supplies a concrete
fact that graphics clarify.  Catalyst can later calibrate the scoring from
channel outcomes without changing that safety rule.
"""
from __future__ import annotations

import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any


_YEAR = re.compile(r"\b(?:18|19|20)\d{2}\b")
_NUMBER = re.compile(r"\b\d+(?:\.\d+)?\s*(?:%|million|billion|thousand|years?|days?|hours?)\b", re.I)
_EVIDENCE = re.compile(r"\b(?:timeline|map|route|border|document|file|record|court|evidence|report|data|statistic|percent|law|policy|election|prison|fugitive)\b", re.I)


def _clean(value: str, limit: int = 110) -> str:
    value = re.sub(r"\s+", " ", str(value or "")).strip(" .")
    return value[:limit].rstrip()


def choose_visual_treatment(
    narration: str,
    *,
    index: int,
    total: int,
    channel_key: str = "",
    skeleton_host: bool = False,
    motion_graphics_requested: bool = False,
) -> dict[str, Any]:
    """Choose one concrete visual modality for a script beat.

    Hero beats are protected for cinematic storytelling.  Factual beats become
    local, reproducible graphics only when a viewer benefits from seeing the
    relation rather than another generic presenter shot.

    When the creator explicitly requested motion graphics during expand intake,
    mid-beat factual scenes get a lower threshold so overrides are not ignored.
    """
    text = _clean(narration, 360)
    low = text.lower()
    score = 0
    graphic_type = ""
    years = [int(v) for v in _YEAR.findall(text)]
    number = _NUMBER.search(text)
    if years:
        score += 4
        graphic_type = "timeline"
    if number:
        score += 4
        graphic_type = "stat"
    if _EVIDENCE.search(text):
        score += 3
        graphic_type = graphic_type or "evidence"
    if motion_graphics_requested and score > 0:
        score += 2
    # Keep the opening, turning point, and ending emotionally cinematic.
    hero = index == 0 or index == max(0, total - 1) or any(
        needle in low for needle in ("but then", "the truth", "everything changed", "finally", "revealed")
    )
    if hero:
        treatment = {
            "kind": "cinematic", "role": "hero", "reason": "hook_or_turning_point",
            "catalyst_score": score, "source_text": text,
        }
        if skeleton_host:
            # A subtle body-origin effect is safe for important moments.  It is
            # never an eye/chest object, text overlay, or floating graphic.
            treatment["motion_effect"] = (
                "One brief, low-intensity cyan pulse travels along the existing spine and ribs, "
                "then fades; no eye glow, symbols, particles, or new anatomy"
            )
        return treatment
    graphic_threshold = 3 if motion_graphics_requested else 4
    if score >= graphic_threshold:
        return {
            "kind": "motion_graphic", "role": "clarifier", "graphic_type": graphic_type or "evidence",
            "reason": "script_contains_a_concrete_fact_or_relationship", "catalyst_score": score,
            "source_text": text, "channel_key": str(channel_key or ""),
        }
    return {
        "kind": "cinematic", "role": "support", "reason": "character_or_environment_storytelling",
        "catalyst_score": score, "source_text": text,
    }


def plan_visual_treatments(
    beats: list[dict[str, Any]],
    *,
    channel_key: str = "",
    skeleton_host: bool = False,
    motion_graphics_requested: bool = False,
) -> list[dict[str, Any]]:
    total = max(1, len(beats))
    return [
        choose_visual_treatment(
            str(beat.get("narration") or beat.get("text") or ""), index=i, total=total,
            channel_key=channel_key, skeleton_host=skeleton_host,
            motion_graphics_requested=motion_graphics_requested,
        )
        for i, beat in enumerate(beats)
    ]


def _render_vertical_card(treatment: dict[str, Any], output: Path, *, duration_sec: float, fps: int, width: int, height: int) -> Path:
    """Native 9:16 evidence card; all copy comes from the approved script."""
    from PIL import Image, ImageDraw
    from long_form.motion_graphics import get_font

    text = _clean(str(treatment.get("source_text") or ""), 130)
    kind = str(treatment.get("graphic_type") or "evidence").upper()
    years = _YEAR.findall(text)
    number = _NUMBER.search(text)
    focal = years[-1] if years else (number.group(0).upper() if number else "CASE FILE")
    words = text.split()
    lines: list[str] = []
    line = ""
    probe = Image.new("RGB", (width, height))
    measure = ImageDraw.Draw(probe)
    body_font = get_font("bold", max(34, width // 23))
    for word in words:
        candidate = f"{line} {word}".strip()
        if measure.textbbox((0, 0), candidate, font=body_font)[2] > width - 140 and line:
            lines.append(line)
            line = word
        else:
            line = candidate
    if line:
        lines.append(line)
    lines = lines[:4]
    total = max(1, int(round(duration_sec * fps)))
    with tempfile.TemporaryDirectory(prefix="studio_vertical_card_") as temp:
        frames = Path(temp)
        for index in range(total):
            progress = index / max(1, total - 1)
            image = Image.new("RGB", (width, height), (5, 8, 14))
            draw = ImageDraw.Draw(image)
            # Structural grid and accent make this an intentional editorial beat.
            draw.rectangle((0, 0, width, 18), fill=(20, 190, 220))
            draw.text((70, 105), f"NYPTID / {kind}", font=get_font("bold", max(26, width // 32)), fill=(120, 220, 240))
            alpha_y = int(360 - min(1.0, progress * 2.2) * 80)
            draw.text((70, alpha_y), focal, font=get_font("display", max(120, width // 5)), fill=(245, 250, 255))
            line_y = 760
            draw.line((70, line_y, width - 70, line_y), fill=(45, 185, 215), width=4)
            for line_index, line_text in enumerate(lines):
                reveal = max(0.0, min(1.0, (progress - 0.28 - line_index * 0.10) / 0.25))
                if reveal:
                    draw.text((70, 860 + line_index * 76), line_text, font=body_font, fill=(225, 232, 240))
            draw.text((70, height - 125), "SCRIPT-SOURCED VISUAL EXPLAINER", font=get_font("regular", max(22, width // 42)), fill=(110, 130, 150))
            image.save(frames / f"frame_{index:06d}.png")
        result = subprocess.run(
            ["ffmpeg", "-hide_banner", "-loglevel", "warning", "-y", "-framerate", str(fps), "-i", str(frames / "frame_%06d.png"), "-c:v", "libx264", "-preset", "veryfast", "-crf", "18", "-pix_fmt", "yuv420p", "-r", str(fps), str(output)],
            capture_output=True, text=True,
        )
    if result.returncode != 0:
        raise RuntimeError(f"vertical motion-graphic render failed: {result.stderr[-300:]}")
    return output


def render_motion_graphic_clip(
    treatment: dict[str, Any], output: Path, *, duration_sec: float, fps: int = 24,
    width: int = 1920, height: int = 1080,
) -> Path:
    """Render a fact card locally; no image/video model, no fabricated text."""
    from long_form.motion_graphics import CounterCard, NewsCard, TimelineCard

    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if height > width:
        return _render_vertical_card(treatment, output, duration_sec=duration_sec, fps=fps, width=width, height=height)
    text = _clean(str(treatment.get("source_text") or ""), 150)
    kind = str(treatment.get("graphic_type") or "evidence")
    card_width, card_height = width, height
    if kind == "timeline":
        years = [int(value) for value in _YEAR.findall(text)] or [2000, 2005, 2010, 2015, 2020]
        years = sorted(dict.fromkeys(years))[:6]
        card = TimelineCard(years=years, event_year=years[-1], event_label=_clean(text, 52), duration_sec=duration_sec, fps=fps, width=card_width, height=card_height)
    elif kind == "stat":
        match = _NUMBER.search(text)
        raw = match.group(0) if match else "1"
        value = float(re.search(r"\d+(?:\.\d+)?", raw).group(0))
        suffix = "%" if "%" in raw else ""
        card = CounterCard(final_value=value, suffix=suffix, label=_clean(text, 65), duration_sec=duration_sec, fps=fps, width=card_width, height=card_height)
    else:
        card = NewsCard(publisher="NYT", section="CASE FILE", headline=_clean(text, 125), duration_sec=duration_sec, fps=fps, width=card_width, height=card_height)
    return card.render(output)
