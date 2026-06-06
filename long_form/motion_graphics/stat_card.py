"""Lume-style motion-graphics stat cards.

Decoded from the 2026-05-16 Lume episode (Candice McCoy, $60K/day):
roughly 60% of that 18-minute video is pure motion graphics — stat
reveals, NPR-style news cards, timeline animations, source attributions.
At our scale that lane was costing ~$0.08/scene through LTX. These
classes render the same look for $0 and ship 60fps MP4 ready to mux
into the episode pipeline.

Each card is a dataclass: customize fields, call .render(path), get an
MP4 back. Internal loop is pure Pillow → frames → ffmpeg compile.
"""
from __future__ import annotations

import math
import os
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Tuple

from PIL import Image, ImageDraw, ImageFilter, ImageFont

# ──────────────────────────────────────────────────────────────────────
# Font resolution — tries Windows, Linux (Fly), macOS in order.
# ──────────────────────────────────────────────────────────────────────
_FONT_CANDIDATES = {
    "bold": [
        "C:/Windows/Fonts/arialbd.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
        "/Library/Fonts/Arial Bold.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
    ],
    "regular": [
        "C:/Windows/Fonts/arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans.ttf",
        "/Library/Fonts/Arial.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
    ],
    "display": [
        "C:/Windows/Fonts/impact.ttf",
        "C:/Windows/Fonts/arialbd.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/Library/Fonts/Impact.ttf",
    ],
    "serif_bold": [
        "C:/Windows/Fonts/georgiab.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSerif-Bold.ttf",
        "/Library/Fonts/Georgia Bold.ttf",
    ],
}

_FONT_CACHE: dict = {}


def get_font(role: str, size: int) -> ImageFont.FreeTypeFont:
    """Return a Pillow font for the given role + size, cached."""
    key = (role, size)
    if key in _FONT_CACHE:
        return _FONT_CACHE[key]
    for path in _FONT_CANDIDATES.get(role, []):
        if Path(path).exists():
            font = ImageFont.truetype(path, size)
            _FONT_CACHE[key] = font
            return font
    # Fallback: any bold variant we have
    for path in _FONT_CANDIDATES.get("bold", []):
        if Path(path).exists():
            font = ImageFont.truetype(path, size)
            _FONT_CACHE[key] = font
            return font
    raise RuntimeError(
        f"No font found for role '{role}' size {size}. "
        f"Tried: {_FONT_CANDIDATES.get(role, [])}"
    )


# ──────────────────────────────────────────────────────────────────────
# Easing helpers
# ──────────────────────────────────────────────────────────────────────
def ease_out_quad(t: float) -> float:
    t = max(0.0, min(1.0, t))
    return 1 - (1 - t) ** 2


def ease_in_out_cubic(t: float) -> float:
    t = max(0.0, min(1.0, t))
    if t < 0.5:
        return 4 * t * t * t
    return 1 - ((-2 * t + 2) ** 3) / 2


def ease_out_back(t: float) -> float:
    """Slight overshoot — useful for text snap-in."""
    t = max(0.0, min(1.0, t))
    c1 = 1.70158
    c3 = c1 + 1
    return 1 + c3 * (t - 1) ** 3 + c1 * (t - 1) ** 2


def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * max(0.0, min(1.0, t))


def fade_window(t: float, start: float, dur: float) -> float:
    """Return eased 0..1 progress for a fade-in starting at `start`,
    lasting `dur` seconds within the normalized 0..1 timeline."""
    if dur <= 0:
        return 1.0 if t >= start else 0.0
    return ease_out_quad((t - start) / dur)


def fit_font(
    draw: ImageDraw.ImageDraw,
    text: str,
    target_width: int,
    role: str = "bold",
    max_size: int = 380,
    min_size: int = 60,
    step: int = 20,
) -> ImageFont.FreeTypeFont:
    """Pick the largest font size for `role` whose rendered width of
    `text` fits inside `target_width`. Falls back to `min_size` if even
    that overflows. Use this anywhere a single line of dynamic-length
    text needs to live inside a fixed canvas (counter values, big
    titles, etc.)."""
    size = max_size
    last = None
    while size >= min_size:
        font = get_font(role, size)
        w = draw.textbbox((0, 0), text, font=font)[2]
        if w <= target_width:
            return font
        last = font
        size -= step
    return last or get_font(role, min_size)


# ──────────────────────────────────────────────────────────────────────
# Base StatCard — handles the frame-render-then-ffmpeg pipeline.
# ──────────────────────────────────────────────────────────────────────
@dataclass
class StatCard:
    duration_sec: float = 5.0
    fps: int = 60
    width: int = 1920
    height: int = 1080
    background: Tuple[int, int, int] = (12, 18, 32)  # Lume dark navy
    # Optional subtle vignette darkening at the edges
    vignette: bool = True

    def _draw_background(self, img: Image.Image) -> None:
        if not self.vignette:
            return
        # Radial darken overlay
        overlay = Image.new("L", (self.width, self.height), 0)
        odraw = ImageDraw.Draw(overlay)
        cx, cy = self.width // 2, self.height // 2
        max_r = int(math.hypot(cx, cy))
        for i in range(8):
            inner = max_r - i * 80
            alpha = int(20 + i * 8)
            odraw.ellipse(
                [cx - inner, cy - inner, cx + inner, cy + inner],
                fill=alpha,
            )
        overlay = ImageOps_invert(overlay)
        overlay = overlay.filter(ImageFilter.GaussianBlur(radius=80))
        black = Image.new("RGB", (self.width, self.height), (0, 0, 0))
        img.paste(black, (0, 0), overlay)

    def _render_frame(self, t: float, frame_idx: int) -> Image.Image:
        """Subclasses override this. `t` is normalized 0..1."""
        raise NotImplementedError

    def render(
        self,
        output_path: Path,
        ffmpeg_exe: str = "ffmpeg",
        crf: int = 18,
    ) -> Path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        total_frames = max(1, int(round(self.duration_sec * self.fps)))

        with tempfile.TemporaryDirectory(prefix="statcard_") as tmp:
            tmp_dir = Path(tmp)
            for i in range(total_frames):
                t = i / max(1, total_frames - 1)
                img = self._render_frame(t, i)
                img.save(tmp_dir / f"frame_{i:06d}.png", optimize=False)

            subprocess.run(
                [
                    ffmpeg_exe, "-y", "-loglevel", "error",
                    "-framerate", str(self.fps),
                    "-i", str(tmp_dir / "frame_%06d.png"),
                    "-c:v", "libx264",
                    "-preset", "veryfast",
                    "-crf", str(crf),
                    "-pix_fmt", "yuv420p",
                    "-r", str(self.fps),
                    str(output_path),
                ],
                check=True,
            )
        return output_path


def ImageOps_invert(img: Image.Image) -> Image.Image:
    """Tiny invert for L-mode images without importing ImageOps."""
    return Image.eval(img, lambda v: 255 - v)


# ──────────────────────────────────────────────────────────────────────
# PercentageCard — "70% OF ALL ERC CLAIMS / HAD UNACCEPTABLE RISK"
# ──────────────────────────────────────────────────────────────────────
@dataclass
class PercentageCard(StatCard):
    percentage: float = 70.0
    subtitle: str = ""           # e.g. "OF ALL ERC CLAIMS"
    body: str = ""               # e.g. "HAD UNACCEPTABLE RISK"
    source: str = ""             # e.g. "IRS"
    accent_color: Tuple[int, int, int] = (235, 70, 90)  # Lume red
    suffix: str = "%"            # change to "" for raw counter, "x" for multiplier

    def _render_frame(self, t: float, frame_idx: int) -> Image.Image:
        img = Image.new("RGB", (self.width, self.height), self.background)
        self._draw_background(img)
        draw = ImageDraw.Draw(img, "RGBA")

        # Animated counter 0 → percentage (eases over t ∈ [0.05, 0.55])
        count_t = fade_window(t, 0.05, 0.50)
        displayed = self.percentage * count_t
        # Render as int if it's whole, else 1-decimal
        if abs(displayed - round(displayed)) < 0.05 and self.percentage == int(self.percentage):
            pct_str = f"{int(round(displayed))}{self.suffix}"
        else:
            pct_str = f"{displayed:.1f}{self.suffix}"

        # ── Glow halo behind percentage ──
        glow_t = fade_window(t, 0.10, 0.40)
        if glow_t > 0:
            glow_layer = Image.new("RGBA", (self.width, self.height), (0, 0, 0, 0))
            gdraw = ImageDraw.Draw(glow_layer)
            cx, cy = self.width // 2, self.height // 2 - 40
            for r in range(520, 200, -40):
                alpha = int(28 * glow_t * (1 - (r - 200) / 360))
                gdraw.ellipse(
                    [cx - r, cy - r // 2, cx + r, cy + r // 2],
                    fill=(*self.accent_color, max(0, alpha)),
                )
            glow_layer = glow_layer.filter(ImageFilter.GaussianBlur(radius=40))
            img.paste(glow_layer, (0, 0), glow_layer)
            draw = ImageDraw.Draw(img, "RGBA")  # re-bind after paste

        # ── Big percentage text ── (auto-shrink to fit canvas with 200px margin)
        pct_font = fit_font(
            draw, pct_str, target_width=self.width - 200,
            role="bold", max_size=380, min_size=120,
        )
        pbox = draw.textbbox((0, 0), pct_str, font=pct_font)
        pw, ph = pbox[2] - pbox[0], pbox[3] - pbox[1]
        px = (self.width - pw) // 2 - pbox[0]
        py = (self.height - ph) // 2 - 100 - pbox[1]
        # Slight scale-in (back ease) for snap
        snap_t = ease_out_back(min(1.0, max(0.0, (t - 0.05) / 0.35)))
        if snap_t < 0.999:
            # Render to a temp layer, scale, paste
            tmp = Image.new("RGBA", (self.width, self.height), (0, 0, 0, 0))
            tdraw = ImageDraw.Draw(tmp)
            tdraw.text((px, py), pct_str, font=pct_font, fill=(*self.accent_color, 255))
            scale = max(0.7, min(1.05, 0.7 + 0.35 * snap_t))
            new_w = int(self.width * scale)
            new_h = int(self.height * scale)
            tmp = tmp.resize((new_w, new_h), Image.LANCZOS)
            ox = (self.width - new_w) // 2
            oy = (self.height - new_h) // 2
            img.paste(tmp, (ox, oy), tmp)
            draw = ImageDraw.Draw(img, "RGBA")
        else:
            draw.text((px, py), pct_str, font=pct_font, fill=(*self.accent_color, 255))

        # ── Subtitle (white, bold, below) ──
        sub_t = fade_window(t, 0.35, 0.30)
        if sub_t > 0 and self.subtitle:
            sub_size = 72
            sub_font = get_font("bold", sub_size)
            sbox = draw.textbbox((0, 0), self.subtitle, font=sub_font)
            sw = sbox[2] - sbox[0]
            sx = (self.width - sw) // 2 - sbox[0]
            sy = py + ph + 60
            draw.text(
                (sx, sy), self.subtitle, font=sub_font,
                fill=(255, 255, 255, int(255 * sub_t)),
            )
            sy_after = sy + (sbox[3] - sbox[1]) + 24

            if self.body:
                body_size = 48
                body_font = get_font("regular", body_size)
                bbox = draw.textbbox((0, 0), self.body, font=body_font)
                bw = bbox[2] - bbox[0]
                bx = (self.width - bw) // 2 - bbox[0]
                draw.text(
                    (bx, sy_after), self.body, font=body_font,
                    fill=(210, 215, 225, int(255 * sub_t)),
                )

        # ── Source attribution (bottom-left, last to appear) ──
        if self.source:
            src_t = fade_window(t, 0.60, 0.30)
            if src_t > 0:
                line_y = self.height - 110
                draw.line(
                    [(60, line_y), (self.width - 60, line_y)],
                    fill=(70, 85, 110, int(180 * src_t)),
                    width=1,
                )
                src_font = get_font("regular", 32)
                label = "SOURCE: "
                draw.text(
                    (60, line_y + 22), label, font=src_font,
                    fill=(110, 150, 210, int(255 * src_t)),
                )
                lbox = draw.textbbox((0, 0), label, font=src_font)
                lw = lbox[2] - lbox[0]
                draw.text(
                    (60 + lw, line_y + 22), self.source, font=src_font,
                    fill=(210, 215, 225, int(255 * src_t)),
                )

        return img


# ──────────────────────────────────────────────────────────────────────
# NewsCard — NPR-style headline reconstruction
# ──────────────────────────────────────────────────────────────────────
@dataclass
class NewsCard(StatCard):
    publisher: str = "NPR"
    section: str = "ECONOMY"
    headline: str = ""
    highlight: str = ""          # phrase inside headline to yellow-highlight
    date_str: str = ""           # "APRIL 16, 2020 · 4:18 PM ET"
    show_attribution: str = ""   # "HEARD ON ALL THINGS CONSIDERED"
    show_accent_color: Tuple[int, int, int] = (220, 30, 40)
    card_color: Tuple[int, int, int] = (235, 238, 245)
    text_color: Tuple[int, int, int] = (20, 24, 35)

    def _draw_publisher_logo(self, draw: ImageDraw.ImageDraw, cx: int, cy: int) -> None:
        """NPR-style RGB letter blocks. Generic for any 3-letter publisher."""
        letters = (self.publisher[:3].upper() or "NPR")
        # Three boxes: red / black / blue
        colors = [(220, 30, 40), (20, 20, 20), (30, 110, 220)]
        box_w, box_h = 50, 50
        gap = 4
        total_w = 3 * box_w + 2 * gap
        x0 = cx - total_w // 2
        font = get_font("bold", 36)
        for i, ch in enumerate(letters.ljust(3)[:3]):
            bx = x0 + i * (box_w + gap)
            color = colors[i % len(colors)]
            draw.rectangle([bx, cy - box_h // 2, bx + box_w, cy + box_h // 2], fill=color)
            tbox = draw.textbbox((0, 0), ch, font=font)
            tw, th = tbox[2] - tbox[0], tbox[3] - tbox[1]
            tx = bx + (box_w - tw) // 2 - tbox[0]
            ty = cy - th // 2 - tbox[1]
            draw.text((tx, ty), ch, font=font, fill=(255, 255, 255))

    def _render_frame(self, t: float, frame_idx: int) -> Image.Image:
        img = Image.new("RGB", (self.width, self.height), self.background)
        self._draw_background(img)
        draw = ImageDraw.Draw(img, "RGBA")

        # ── Card slide-in ──
        card_t = ease_out_quad(min(1.0, max(0.0, (t - 0.05) / 0.40)))
        card_w = int(self.width * 0.72)
        card_h = int(self.height * 0.62)
        cx = (self.width - card_w) // 2
        # Slide up from below
        slide_offset = int((1.0 - card_t) * 140)
        cy = (self.height - card_h) // 2 + slide_offset

        if card_t <= 0:
            return img

        # Card body with rounded corners + drop shadow
        shadow = Image.new("RGBA", (self.width, self.height), (0, 0, 0, 0))
        sdraw = ImageDraw.Draw(shadow)
        sdraw.rounded_rectangle(
            [cx + 6, cy + 14, cx + card_w + 6, cy + card_h + 14],
            radius=24, fill=(0, 0, 0, int(140 * card_t)),
        )
        shadow = shadow.filter(ImageFilter.GaussianBlur(radius=18))
        img.paste(shadow, (0, 0), shadow)
        draw = ImageDraw.Draw(img, "RGBA")

        draw.rounded_rectangle(
            [cx, cy, cx + card_w, cy + card_h],
            radius=24, fill=(*self.card_color, int(255 * card_t)),
        )

        # ── Publisher logo (centered top) ──
        self._draw_publisher_logo(draw, cx + card_w // 2, cy + 80)

        # ── Section label (small, gray) ──
        sec_font = get_font("bold", 28)
        sec_y = cy + 145
        draw.text(
            (cx + 60, sec_y), self.section.upper(), font=sec_font,
            fill=(110, 118, 135, int(255 * card_t)),
        )

        # ── Headline with optional yellow highlight ──
        head_font = get_font("bold", 56)
        text_t = ease_out_quad(min(1.0, max(0.0, (t - 0.30) / 0.45)))
        if text_t > 0:
            # Word-wrap manually
            words = self.headline.split()
            lines: List[str] = []
            line = ""
            max_text_w = card_w - 120
            for w in words:
                trial = (line + " " + w).strip()
                tw = draw.textbbox((0, 0), trial, font=head_font)[2]
                if tw > max_text_w and line:
                    lines.append(line)
                    line = w
                else:
                    line = trial
            if line:
                lines.append(line)

            line_y = cy + 200
            for ln in lines:
                # Highlight phrase if present in this line
                if self.highlight and self.highlight in ln:
                    pre, _, post = ln.partition(self.highlight)
                    pre_w = draw.textbbox((0, 0), pre, font=head_font)[2]
                    hi_w = draw.textbbox((0, 0), self.highlight, font=head_font)[2]
                    # Yellow highlight rect
                    draw.rectangle(
                        [cx + 60 + pre_w - 4, line_y + 8,
                         cx + 60 + pre_w + hi_w + 4, line_y + 70],
                        fill=(252, 230, 60, int(255 * text_t)),
                    )
                    draw.text(
                        (cx + 60, line_y), ln, font=head_font,
                        fill=(*self.text_color, int(255 * text_t)),
                    )
                else:
                    draw.text(
                        (cx + 60, line_y), ln, font=head_font,
                        fill=(*self.text_color, int(255 * text_t)),
                    )
                line_y += 78

        # ── Date + show attribution (bottom) ──
        attr_t = ease_out_quad(min(1.0, max(0.0, (t - 0.55) / 0.35)))
        if attr_t > 0 and (self.date_str or self.show_attribution):
            small_font = get_font("regular", 28)
            attr_y = cy + card_h - 110
            if self.date_str:
                draw.text(
                    (cx + 60, attr_y), self.date_str, font=small_font,
                    fill=(110, 118, 135, int(255 * attr_t)),
                )
            if self.show_attribution:
                attr_y2 = attr_y + 44
                label = "HEARD ON "
                draw.text(
                    (cx + 60, attr_y2), label, font=small_font,
                    fill=(110, 118, 135, int(255 * attr_t)),
                )
                lbox = draw.textbbox((0, 0), label, font=small_font)
                lw = lbox[2] - lbox[0]
                draw.text(
                    (cx + 60 + lw, attr_y2),
                    self.show_attribution.upper(), font=small_font,
                    fill=(*self.show_accent_color, int(255 * attr_t)),
                )

        return img


# ──────────────────────────────────────────────────────────────────────
# TimelineCard — year markers with an event arrow + label
# ──────────────────────────────────────────────────────────────────────
@dataclass
class TimelineCard(StatCard):
    years: List[int] = field(default_factory=lambda: [2020, 2021, 2022, 2023, 2024, 2025])
    event_year: Optional[int] = None     # which year gets the highlighted marker
    event_label: str = ""                # text above the arrow
    line_color: Tuple[int, int, int] = (180, 188, 205)
    text_color: Tuple[int, int, int] = (210, 215, 225)
    inactive_year_color: Tuple[int, int, int] = (120, 128, 145)
    active_year_color: Tuple[int, int, int] = (255, 255, 255)

    def _render_frame(self, t: float, frame_idx: int) -> Image.Image:
        img = Image.new("RGB", (self.width, self.height), self.background)
        self._draw_background(img)
        draw = ImageDraw.Draw(img, "RGBA")

        # ── Horizontal line draws left-to-right ──
        line_y = self.height // 2 + 80
        line_left = 120
        line_right = self.width - 120
        line_t = ease_in_out_cubic(min(1.0, t / 0.55))
        line_end = line_left + int((line_right - line_left) * line_t)
        draw.line([(line_left, line_y), (line_end, line_y)],
                  fill=self.line_color, width=2)

        # ── Year markers + labels (fade in as line passes each) ──
        # Unified layout: all years render BELOW the line (matches Lume
        # frame_025 of Candice McCoy ep). Active year gets brighter
        # color + slightly larger dot but stays in the same row.
        n = max(1, len(self.years) - 1)
        marker_font_inactive = get_font("regular", 44)
        marker_font_active = get_font("bold", 48)
        for i, yr in enumerate(self.years):
            xi = line_left + int((line_right - line_left) * (i / n))
            local_t = (line_t - (i / n)) / max(0.001, 1.0 / n / 1.5)
            local_t = max(0.0, min(1.0, local_t))
            if local_t <= 0:
                continue
            is_event = (self.event_year is not None and yr == self.event_year)
            color = self.active_year_color if is_event else self.inactive_year_color
            r = 10 if is_event else 6
            draw.ellipse(
                [xi - r, line_y - r, xi + r, line_y + r],
                fill=(*color, int(255 * local_t)),
                outline=(*color, int(255 * local_t)),
            )
            # All year labels below the line, same baseline.
            font = marker_font_active if is_event else marker_font_inactive
            ybox = draw.textbbox((0, 0), str(yr), font=font)
            yw = ybox[2] - ybox[0]
            draw.text(
                (xi - yw // 2 - ybox[0], line_y + 28),
                str(yr), font=font,
                fill=(*color, int(255 * local_t)),
            )

        # ── Event arrow + label (above the highlighted year) ──
        if self.event_year is not None and self.event_label:
            ev_t = ease_out_quad(min(1.0, max(0.0, (t - 0.55) / 0.35)))
            if ev_t > 0:
                try:
                    ev_idx = self.years.index(self.event_year)
                except ValueError:
                    ev_idx = 0
                ex = line_left + int((line_right - line_left) * (ev_idx / n))
                # Vertical line up + arrow head
                arrow_top = line_y - 200
                arrow_bottom = line_y - 30
                draw.line([(ex, arrow_top), (ex, arrow_bottom)],
                          fill=(*self.active_year_color, int(255 * ev_t)), width=2)
                # Arrowhead
                ah = 14
                draw.polygon(
                    [(ex - ah // 2, arrow_bottom - ah),
                     (ex + ah // 2, arrow_bottom - ah),
                     (ex, arrow_bottom)],
                    fill=(*self.active_year_color, int(255 * ev_t)),
                )
                # Event label
                ev_font = get_font("bold", 44)
                lbox = draw.textbbox((0, 0), self.event_label, font=ev_font)
                lw = lbox[2] - lbox[0]
                draw.text(
                    (ex - lw // 2 - lbox[0], arrow_top - 60),
                    self.event_label, font=ev_font,
                    fill=(180, 188, 205, int(255 * ev_t)),
                )

        return img


# ──────────────────────────────────────────────────────────────────────
# CounterCard — animated big-number reveal ("$59 B", "1,227 returns")
# ──────────────────────────────────────────────────────────────────────
@dataclass
class CounterCard(StatCard):
    final_value: float = 59.0
    prefix: str = "$"            # e.g. "$" or ""
    suffix: str = " B"           # e.g. " B", " MILLION", " RETURNS"
    label: str = ""              # subtitle below the number
    source: str = ""
    decimals: int = 0
    accent_color: Tuple[int, int, int] = (255, 255, 255)
    use_thousands_sep: bool = True

    def _format(self, v: float) -> str:
        if self.decimals == 0:
            n_str = f"{int(round(v)):,}" if self.use_thousands_sep else f"{int(round(v))}"
        else:
            fmt = f"{{:,.{self.decimals}f}}" if self.use_thousands_sep else f"{{:.{self.decimals}f}}"
            n_str = fmt.format(v)
        return f"{self.prefix}{n_str}{self.suffix}"

    def _render_frame(self, t: float, frame_idx: int) -> Image.Image:
        img = Image.new("RGB", (self.width, self.height), self.background)
        self._draw_background(img)
        draw = ImageDraw.Draw(img, "RGBA")

        # Counter ramp 0 → final_value (eases over t ∈ [0.10, 0.70])
        cnt_t = fade_window(t, 0.10, 0.60)
        cur_val = self.final_value * cnt_t
        text = self._format(cur_val)
        # Use the FINAL string for sizing so we pick a font that holds
        # the final value (otherwise the counter "snaps" smaller as more
        # digits appear during the ramp-up animation).
        final_text = self._format(self.final_value)

        num_font = fit_font(
            draw, final_text, target_width=self.width - 200,
            role="bold", max_size=340, min_size=120,
        )
        nbox = draw.textbbox((0, 0), text, font=num_font)
        nw, nh = nbox[2] - nbox[0], nbox[3] - nbox[1]
        nx = (self.width - nw) // 2 - nbox[0]
        ny = (self.height - nh) // 2 - 80 - nbox[1]
        draw.text(
            (nx, ny), text, font=num_font,
            fill=(*self.accent_color, 255),
        )

        # Label below
        lab_t = fade_window(t, 0.50, 0.30)
        if lab_t > 0 and self.label:
            lab_font = get_font("bold", 56)
            lbox = draw.textbbox((0, 0), self.label, font=lab_font)
            lw = lbox[2] - lbox[0]
            lx = (self.width - lw) // 2 - lbox[0]
            ly = ny + nh + 60
            draw.text(
                (lx, ly), self.label, font=lab_font,
                fill=(220, 225, 235, int(255 * lab_t)),
            )

        # Source
        if self.source:
            src_t = fade_window(t, 0.65, 0.30)
            if src_t > 0:
                line_y = self.height - 110
                draw.line(
                    [(60, line_y), (self.width - 60, line_y)],
                    fill=(70, 85, 110, int(180 * src_t)),
                    width=1,
                )
                src_font = get_font("regular", 32)
                label = "SOURCE: "
                draw.text(
                    (60, line_y + 22), label, font=src_font,
                    fill=(110, 150, 210, int(255 * src_t)),
                )
                lbox = draw.textbbox((0, 0), label, font=src_font)
                lw = lbox[2] - lbox[0]
                draw.text(
                    (60 + lw, line_y + 22), self.source, font=src_font,
                    fill=(210, 215, 225, int(255 * src_t)),
                )
        return img


# ──────────────────────────────────────────────────────────────────────
# ChecklistCard — staggered bullet reveals (Lume "what shipped" slides)
# ──────────────────────────────────────────────────────────────────────
@dataclass
class ChecklistCard(StatCard):
    title: str = ""
    items: List[str] = field(default_factory=list)
    source: str = ""
    accent_color: Tuple[int, int, int] = (80, 160, 255)
    check_color: Tuple[int, int, int] = (60, 210, 120)

    def _render_frame(self, t: float, frame_idx: int) -> Image.Image:
        img = Image.new("RGB", (self.width, self.height), self.background)
        self._draw_background(img)
        draw = ImageDraw.Draw(img, "RGBA")

        title_t = fade_window(t, 0.05, 0.25)
        if title_t > 0 and self.title:
            title_font = get_font("bold", 64)
            tbox = draw.textbbox((0, 0), self.title, font=title_font)
            tw = tbox[2] - tbox[0]
            draw.text(
                (self.width // 2 - tw // 2 - tbox[0], 120 - tbox[1]),
                self.title, font=title_font,
                fill=(255, 255, 255, int(255 * title_t)),
            )

        item_font = get_font("regular", 46)
        start_y = 260
        row_h = 96
        for i, item in enumerate(self.items[:5]):
            item_t = fade_window(t, 0.18 + i * 0.12, 0.22)
            if item_t <= 0:
                continue
            y = start_y + i * row_h
            cx, cy = 140, y + 28
            draw.ellipse(
                [cx - 22, cy - 22, cx + 22, cy + 22],
                fill=(*self.check_color, int(200 * item_t)),
            )
            draw.text((cx - 10, cy - 16), "v", font=get_font("bold", 28),
                      fill=(255, 255, 255, int(255 * item_t)))
            slide = int((1.0 - item_t) * 40)
            draw.text(
                (200 + slide, y), item, font=item_font,
                fill=(230, 235, 245, int(255 * item_t)),
            )
            draw.line(
                [(120, y + row_h - 12), (self.width - 120, y + row_h - 12)],
                fill=(50, 65, 90, int(120 * item_t)), width=1,
            )

        if self.source:
            src_t = fade_window(t, 0.72, 0.22)
            if src_t > 0:
                line_y = self.height - 110
                draw.line(
                    [(60, line_y), (self.width - 60, line_y)],
                    fill=(70, 85, 110, int(180 * src_t)), width=1,
                )
                src_font = get_font("regular", 32)
                label = "SOURCE: "
                draw.text((60, line_y + 22), label, font=src_font,
                          fill=(110, 150, 210, int(255 * src_t)))
                lw = draw.textbbox((0, 0), label, font=src_font)[2]
                draw.text((60 + lw, line_y + 22), self.source, font=src_font,
                          fill=(210, 215, 225, int(255 * src_t)))
        return img


# ──────────────────────────────────────────────────────────────────────
# CompareCard — two-column FREE vs PAID (Rook finance explainer style)
# ──────────────────────────────────────────────────────────────────────
@dataclass
class CompareCard(StatCard):
    headline: str = "WHAT COSTS WHAT"
    left_title: str = "FREE"
    right_title: str = "PRO / ULTRA"
    left_items: List[str] = field(default_factory=list)
    right_items: List[str] = field(default_factory=list)
    source: str = ""
    left_color: Tuple[int, int, int] = (60, 210, 120)
    right_color: Tuple[int, int, int] = (235, 180, 60)

    def _draw_column(
        self, draw: ImageDraw.ImageDraw, x: int, w: int, y0: int,
        title: str, items: List[str], color: Tuple[int, int, int], t: float,
        delay: float,
    ) -> None:
        col_t = fade_window(t, delay, 0.30)
        if col_t <= 0:
            return
        draw.rounded_rectangle(
            [x, y0, x + w, y0 + 520],
            radius=20, fill=(22, 30, 48, int(230 * col_t)),
            outline=(*color, int(180 * col_t)), width=3,
        )
        title_font = get_font("bold", 52)
        draw.text((x + 40, y0 + 36), title, font=title_font,
                  fill=(*color, int(255 * col_t)))
        item_font = get_font("regular", 34)
        for i, item in enumerate(items[:4]):
            it = fade_window(t, delay + 0.08 + i * 0.08, 0.18)
            if it <= 0:
                continue
            iy = y0 + 120 + i * 88
            draw.ellipse(
                [x + 36, iy + 8, x + 56, iy + 28],
                fill=(*color, int(200 * it)),
            )
            draw.text((x + 72, iy), item, font=item_font,
                      fill=(220, 225, 235, int(255 * it)))

    def _render_frame(self, t: float, frame_idx: int) -> Image.Image:
        img = Image.new("RGB", (self.width, self.height), self.background)
        self._draw_background(img)
        draw = ImageDraw.Draw(img, "RGBA")

        head_t = fade_window(t, 0.05, 0.20)
        if head_t > 0 and self.headline:
            head_font = get_font("bold", 58)
            head = self.headline
            hbox = draw.textbbox((0, 0), head, font=head_font)
            hw = hbox[2] - hbox[0]
            draw.text(
                (self.width // 2 - hw // 2 - hbox[0], 90 - hbox[1]),
                head, font=head_font, fill=(255, 255, 255, int(255 * head_t)),
            )

        col_w = int(self.width * 0.38)
        gap = int(self.width * 0.06)
        y0 = 200
        lx = (self.width - 2 * col_w - gap) // 2
        rx = lx + col_w + gap
        self._draw_column(draw, lx, col_w, y0, self.left_title,
                          self.left_items, self.left_color, t, 0.20)
        self._draw_column(draw, rx, col_w, y0, self.right_title,
                          self.right_items, self.right_color, t, 0.32)

        if self.source:
            src_t = fade_window(t, 0.75, 0.20)
            if src_t > 0:
                line_y = self.height - 110
                draw.line([(60, line_y), (self.width - 60, line_y)],
                          fill=(70, 85, 110, int(180 * src_t)), width=1)
                src_font = get_font("regular", 32)
                label = "SOURCE: "
                draw.text((60, line_y + 22), label, font=src_font,
                          fill=(110, 150, 210, int(255 * src_t)))
                lw = draw.textbbox((0, 0), label, font=src_font)[2]
                draw.text((60 + lw, line_y + 22), self.source, font=src_font,
                          fill=(210, 215, 225, int(255 * src_t)))
        return img


# ──────────────────────────────────────────────────────────────────────
# SourceProofCard — verified source B-roll (browser citation + quote)
# ──────────────────────────────────────────────────────────────────────
@dataclass
class SourceProofCard(StatCard):
    url: str = "https://blog.google/"
    site_label: str = "blog.google"
    page_title: str = ""
    author: str = ""
    date_str: str = ""
    quote: str = ""
    highlights: List[str] = field(default_factory=list)
    badge: str = "VERIFIED SOURCE"

    def _wrap_lines(
        self, draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont, max_w: int,
    ) -> List[str]:
        words = text.split()
        lines: List[str] = []
        line = ""
        for w in words:
            trial = (line + " " + w).strip()
            if draw.textbbox((0, 0), trial, font=font)[2] > max_w and line:
                lines.append(line)
                line = w
            else:
                line = trial
        if line:
            lines.append(line)
        return lines

    def _render_frame(self, t: float, frame_idx: int) -> Image.Image:
        img = Image.new("RGB", (self.width, self.height), self.background)
        self._draw_background(img)
        draw = ImageDraw.Draw(img, "RGBA")

        reveal = fade_window(t, 0.05, 0.25)
        zoom = 1.0 + 0.04 * ease_in_out_cubic(min(1.0, max(0.0, (t - 0.20) / 0.75)))

        bw = int(self.width * 0.78 * zoom)
        bh = int(self.height * 0.72 * zoom)
        bx = (self.width - bw) // 2
        by = (self.height - bh) // 2 + int((1.0 - reveal) * 60)

        if reveal <= 0:
            return img

        shadow = Image.new("RGBA", (self.width, self.height), (0, 0, 0, 0))
        sdraw = ImageDraw.Draw(shadow)
        sdraw.rounded_rectangle(
            [bx + 8, by + 16, bx + bw + 8, by + bh + 16],
            radius=18, fill=(0, 0, 0, int(160 * reveal)),
        )
        shadow = shadow.filter(ImageFilter.GaussianBlur(radius=16))
        img.paste(shadow, (0, 0), shadow)
        draw = ImageDraw.Draw(img, "RGBA")

        draw.rounded_rectangle(
            [bx, by, bx + bw, by + bh], radius=18,
            fill=(28, 32, 42, int(255 * reveal)),
            outline=(70, 85, 110, int(200 * reveal)), width=2,
        )

        chrome_h = 56
        draw.rectangle([bx, by, bx + bw, by + chrome_h], fill=(45, 50, 62, int(255 * reveal)))
        for i, col in enumerate([(255, 95, 86), (255, 189, 46), (39, 201, 63)]):
            draw.ellipse([bx + 20 + i * 28, by + 18, bx + 36 + i * 28, by + 34], fill=(*col, 255))

        url_x, url_y = bx + 120, by + 14
        url_w = bw - 140
        draw.rounded_rectangle(
            [url_x, url_y, url_x + url_w, url_y + 28], radius=8,
            fill=(22, 26, 34, int(255 * reveal)),
        )
        url_font = get_font("regular", 20)
        short_url = self.url.replace("https://", "").replace("www.", "")
        if len(short_url) > 72:
            short_url = short_url[:69] + "..."
        draw.text((url_x + 12, url_y + 4), short_url, font=url_font,
                  fill=(170, 180, 195, int(255 * reveal)))

        badge_font = get_font("bold", 22)
        badge_t = fade_window(t, 0.15, 0.20)
        if badge_t > 0:
            bb = draw.textbbox((0, 0), self.badge, font=badge_font)
            bwid = bb[2] - bb[0] + 24
            draw.rounded_rectangle(
                [bx + bw - bwid - 16, by + chrome_h + 16, bx + bw - 16, by + chrome_h + 52],
                radius=8, fill=(40, 120, 80, int(220 * badge_t)),
            )
            draw.text(
                (bx + bw - bwid - 4, by + chrome_h + 22), self.badge, font=badge_font,
                fill=(255, 255, 255, int(255 * badge_t)),
            )

        cx, cy = bx + 48, by + chrome_h + 70
        content_w = bw - 96
        title_font = get_font("serif_bold", 42)
        body_font = get_font("regular", 30)
        meta_font = get_font("regular", 24)

        text_t = fade_window(t, 0.25, 0.35)
        if text_t > 0 and self.page_title:
            for ln in self._wrap_lines(draw, self.page_title, title_font, content_w):
                draw.text((cx, cy), ln, font=title_font, fill=(255, 255, 255, int(255 * text_t)))
                cy += 52

        if self.author or self.date_str:
            meta = " · ".join(x for x in (self.author, self.date_str) if x)
            draw.text((cx, cy + 8), meta, font=meta_font, fill=(130, 145, 170, int(255 * text_t)))
            cy += 48

        cy += 16
        draw.line([(cx, cy), (cx + content_w, cy)], fill=(60, 75, 95, int(180 * text_t)), width=1)
        cy += 28

        quote_t = fade_window(t, 0.40, 0.40)
        if quote_t > 0 and self.quote:
            lines = self._wrap_lines(draw, f'"{self.quote}"', body_font, content_w)
            for ln in lines:
                x = cx
                if self.highlights:
                    for phrase in self.highlights:
                        if phrase.lower() in ln.lower():
                            idx = ln.lower().find(phrase.lower())
                            pre = ln[:idx]
                            mid = ln[idx:idx + len(phrase)]
                            pre_w = draw.textbbox((0, 0), pre, font=body_font)[2] if pre else 0
                            mid_w = draw.textbbox((0, 0), mid, font=body_font)[2]
                            draw.rectangle(
                                [x + pre_w - 2, cy + 2, x + pre_w + mid_w + 2, cy + 36],
                                fill=(252, 230, 60, int(200 * quote_t)),
                            )
                            break
                draw.text((x, cy), ln, font=body_font, fill=(220, 228, 240, int(255 * quote_t)))
                cy += 42

        foot_t = fade_window(t, 0.70, 0.25)
        if foot_t > 0:
            foot_font = get_font("regular", 26)
            cite = f"PRIMARY SOURCE: {self.url}"
            draw.text((cx, by + bh - 52), cite, font=foot_font,
                      fill=(110, 150, 210, int(255 * foot_t)))

        return img
